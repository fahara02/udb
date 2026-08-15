#!/usr/bin/env python3
"""UDB embedding inference sidecar.

The broker stays model-free. It emits `udb.embedding.work.v1` payloads containing
only row identity, text, model id, and vector routing. This sidecar turns one work
payload into the body a trusted sidecar can submit to the broker's internal
`ReportEmbedding` callback.

The default provider is deterministic and dependency-free for local/container
smoke. Production providers resolve their endpoints and credentials from the
model's Vault reference; credential-shaped keys in broker work payloads are
rejected here so secrets cannot creep into the event contract.
"""

from __future__ import annotations

import hashlib
import http.server
import json
import math
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


DEFAULT_PORT = 8080
MAX_BODY_BYTES = 512 * 1024
DEFAULT_DIMS = 16
MAX_DIMS = 4096
MAX_BATCH = 256
VAULT_CACHE_TTL_SECONDS = 60
FORBIDDEN_WORK_KEYS = {
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "credentials",
    "key",
    "password",
    "secret",
    "token",
}


class SidecarError(Exception):
    def __init__(self, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class WorkItem:
    tenant_id: str
    project_id: str
    source: str
    row_pk: str
    text: str
    model_id: str
    model_name: str
    model_version: str
    target_collection: str
    work_item_id: str
    job_id: str
    chunk_hash: str
    token_count: int
    dimensions: int
    provider: str
    provider_endpoint_ref: str
    output_dtype: str
    task_type: str
    normalize: bool
    vector_name: str
    parent_text: str
    char_start: int
    char_end: int
    token_start: int
    token_end: int
    contextual_retrieval: bool
    late_chunking: bool


_vault_cache: dict[tuple[str, str, str], tuple[float, dict[str, Any]]] = {}
_vault_lock = threading.Lock()


def provider_name() -> str:
    return os.environ.get("UDB_EMBED_PROVIDER", "deterministic").strip().lower()


def configured_dims() -> int:
    raw = os.environ.get("UDB_EMBED_DIMS", str(DEFAULT_DIMS)).strip()
    try:
        dims = int(raw)
    except ValueError as exc:
        raise SidecarError(f"UDB_EMBED_DIMS must be an integer, got {raw!r}", 500) from exc
    if dims <= 0 or dims > MAX_DIMS:
        raise SidecarError(f"UDB_EMBED_DIMS must be in 1..{MAX_DIMS}", 500)
    return dims


def check_no_credentials(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).strip().lower().replace("-", "_")
            if normalized in FORBIDDEN_WORK_KEYS:
                raise SidecarError(f"work payload contains forbidden credential key at {path}.{key}")
            check_no_credentials(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            check_no_credentials(child, f"{path}[{index}]")


def required_str(mapping: dict[str, Any], key: str) -> str:
    value = str(mapping.get(key, "")).strip()
    if not value:
        raise SidecarError(f"field {key!r} is required")
    return value


def parse_work(raw: bytes) -> WorkItem:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SidecarError(f"request body must be JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise SidecarError("request body must be a JSON object")
    check_no_credentials(value)
    dimensions = int(value.get("dimensions") or configured_dims())
    if dimensions <= 0 or dimensions > MAX_DIMS:
        raise SidecarError(f"dimensions must be in 1..{MAX_DIMS}")
    endpoint_ref = str(value.get("provider_endpoint_ref", "")).strip()
    if endpoint_ref and not endpoint_ref.startswith("vault://"):
        raise SidecarError("provider_endpoint_ref must use vault://")
    return WorkItem(
        tenant_id=required_str(value, "tenant_id"),
        project_id=required_str(value, "project_id"),
        source=required_str(value, "source"),
        row_pk=required_str(value, "row_pk"),
        text=required_str(value, "text"),
        model_id=required_str(value, "model_id"),
        model_name=str(value.get("model_name") or value.get("model_id", "")).strip(),
        model_version=str(value.get("model_version", "")).strip(),
        target_collection=str(value.get("target_collection", "")).strip(),
        work_item_id=str(value.get("work_item_id", "")).strip(),
        job_id=str(value.get("job_id", "")).strip(),
        chunk_hash=str(value.get("chunk_hash", "")).strip(),
        token_count=int(value.get("token_count") or len(required_str(value, "text").split())),
        dimensions=dimensions,
        provider=str(value.get("provider") or provider_name()).strip().lower(),
        provider_endpoint_ref=endpoint_ref,
        output_dtype=str(value.get("output_dtype") or "FLOAT32").strip().upper(),
        task_type=str(value.get("task_type") or "DOCUMENT").strip().upper(),
        normalize=bool(value.get("normalize", True)),
        vector_name=str(value.get("vector_name", "")).strip(),
        parent_text=str(value.get("parent_text", "")),
        char_start=int(value.get("char_start") or 0),
        char_end=int(value.get("char_end") or 0),
        token_start=int(value.get("token_start") or 0),
        token_end=int(value.get("token_end") or 0),
        contextual_retrieval=bool(value.get("contextual_retrieval", False)),
        late_chunking=bool(value.get("late_chunking", False)),
    )


def normalize_vector(values: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in values)) or 1.0
    return [round(value / norm, 8) for value in values]


def deterministic_embed(text: str, model_id: str, dims: int, normalize: bool = True) -> list[float]:
    seed = f"{model_id}\0{text}".encode("utf-8")
    values: list[float] = []
    counter = 0
    while len(values) < dims:
        digest = hashlib.sha256(seed + counter.to_bytes(4, "big")).digest()
        for offset in range(0, len(digest), 4):
            if len(values) >= dims:
                break
            raw = int.from_bytes(digest[offset : offset + 4], "big", signed=False)
            values.append((raw / 2_147_483_647.5) - 1.0)
        counter += 1
    return normalize_vector(values) if normalize else [round(value, 8) for value in values]


def post_json(url: str, body: dict[str, Any], headers: dict[str, str] | None = None) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(body, separators=(",", ":")).encode("utf-8"),
        headers={"Content-Type": "application/json", **(headers or {})},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            decoded = json.loads(response.read().decode("utf-8") or "{}")
    except (urllib.error.URLError, json.JSONDecodeError) as exc:
        raise SidecarError(f"provider request failed: {exc}", 502) from exc
    if not isinstance(decoded, dict):
        raise SidecarError("provider response must be a JSON object", 502)
    return decoded


def vault_cache_key(tenant_id: str, project_id: str, reference: str) -> tuple[str, str, str]:
    return (tenant_id.strip(), project_id.strip(), reference.strip())


def vault_resolver_headers() -> dict[str, str]:
    token = os.environ.get("UDB_VAULT_RESOLVER_TOKEN", "").strip()
    if not token:
        raise SidecarError(
            "UDB_VAULT_RESOLVER_TOKEN is required to authenticate Vault resolution",
            500,
        )
    return {"Authorization": f"Bearer {token}"}


def resolve_vault_reference(
    reference: str,
    tenant_id: str,
    project_id: str,
) -> dict[str, Any]:
    if not reference:
        raise SidecarError("production embedding providers require provider_endpoint_ref", 500)
    if not tenant_id.strip() or not project_id.strip():
        raise SidecarError("Vault resolution requires tenant_id and project_id", 500)
    cache_key = vault_cache_key(tenant_id, project_id, reference)
    now = time.monotonic()
    with _vault_lock:
        cached = _vault_cache.get(cache_key)
        if cached and cached[0] > now:
            return cached[1]
    resolver = os.environ.get("UDB_VAULT_RESOLVER_URL", "").strip()
    if not resolver:
        raise SidecarError("UDB_VAULT_RESOLVER_URL is required to resolve vault references", 500)
    resolved = post_json(
        resolver,
        {
            "reference": reference,
            "tenant_id": tenant_id,
            "project_id": project_id,
        },
        vault_resolver_headers(),
    )
    secret = resolved.get("data", resolved)
    if not isinstance(secret, dict):
        raise SidecarError("vault resolver returned an invalid secret envelope", 502)
    with _vault_lock:
        _vault_cache[cache_key] = (now + VAULT_CACHE_TTL_SECONDS, secret)
    return secret


def contextualized_input(work: WorkItem, secret: dict[str, Any] | None = None) -> str:
    if not work.contextual_retrieval:
        return work.text
    if not work.parent_text.strip():
        raise SidecarError("contextual retrieval requires parent_text", 422)
    if work.provider == "deterministic":
        context = " ".join(work.parent_text.split()[:96])
    else:
        endpoint = str((secret or {}).get("contextualizer_endpoint", "")).strip()
        if not endpoint:
            raise SidecarError("vault secret must contain contextualizer_endpoint", 502)
        response = post_json(endpoint, {
            "model": str((secret or {}).get("contextualizer_model", "")),
            "document": work.parent_text,
            "chunk": work.text,
            "max_context_tokens": 100,
        }, {"Authorization": f"Bearer {str((secret or {}).get('api_key', ''))}"})
        context = str(response.get("context", "")).strip()
        if not context:
            raise SidecarError("contextualizer returned no context", 502)
    return f"Context: {context}\n\nChunk: {work.text}"


def late_chunk_vector(work: WorkItem, secret: dict[str, Any] | None = None) -> list[float]:
    if not work.parent_text.strip() or work.token_end <= work.token_start:
        raise SidecarError("late chunking requires parent_text and valid token boundaries", 422)
    parent_tokens = work.parent_text.split()
    if work.token_start < 0 or work.token_end > len(parent_tokens):
        raise SidecarError("late chunk token boundaries are outside parent_text", 422)
    if work.provider == "deterministic":
        token_vectors = [
            deterministic_embed(
                " ".join(parent_tokens[: index + 1]), work.model_id, work.dimensions, False
            )
            for index in range(work.token_start, work.token_end)
        ]
        pooled = [sum(vector[index] for vector in token_vectors) / len(token_vectors) for index in range(work.dimensions)]
        return normalize_vector(pooled) if work.normalize else pooled
    endpoint = str((secret or {}).get("late_chunking_endpoint", "")).strip()
    if not endpoint:
        raise SidecarError("vault secret must contain late_chunking_endpoint", 502)
    response = post_json(endpoint, {
        "model": work.model_name,
        "input": work.parent_text,
        "boundaries": [{"token_start": work.token_start, "token_end": work.token_end}],
        "dimensions": work.dimensions,
    }, {"Authorization": f"Bearer {str((secret or {}).get('api_key', ''))}"})
    vectors = response.get("vectors")
    vector = vectors[0] if isinstance(vectors, list) and vectors else response.get("vector")
    if not isinstance(vector, list) or len(vector) != work.dimensions:
        raise SidecarError("late chunking provider returned a vector with the wrong dimensions", 502)
    values = [float(value) for value in vector]
    return normalize_vector(values) if work.normalize else values


def embed_work(work: WorkItem) -> list[float]:
    if work.provider == "deterministic":
        if work.late_chunking:
            return late_chunk_vector(work)
        return deterministic_embed(
            contextualized_input(work), work.model_id, work.dimensions, work.normalize
        )
    if work.provider not in {"openai", "openai-compatible", "azure-openai"}:
        raise SidecarError(f"unsupported embedding provider {work.provider!r}", 422)
    secret = resolve_vault_reference(
        work.provider_endpoint_ref,
        work.tenant_id,
        work.project_id,
    )
    endpoint = str(secret.get("endpoint", "")).rstrip("/")
    api_key = str(secret.get("api_key", ""))
    if not endpoint or not api_key:
        raise SidecarError("vault secret must contain endpoint and api_key", 502)
    if work.late_chunking:
        return late_chunk_vector(work, secret)
    provider_input = contextualized_input(work, secret)
    response = post_json(
        f"{endpoint}/embeddings",
        {
            "model": work.model_name,
            "input": provider_input,
            "dimensions": work.dimensions,
            "encoding_format": "float",
            "input_type": work.task_type.lower(),
            "output_dtype": work.output_dtype.lower(),
        },
        {"Authorization": f"Bearer {api_key}"},
    )
    data = response.get("data")
    vector = data[0].get("embedding") if isinstance(data, list) and data else None
    if not isinstance(vector, list) or len(vector) != work.dimensions:
        raise SidecarError("provider returned a vector with the wrong dimensions", 502)
    values = [float(value) for value in vector]
    return normalize_vector(values) if work.normalize else values


def build_report(work: WorkItem) -> dict[str, Any]:
    return {
        "tenant_id": work.tenant_id,
        "source_name": work.source,
        "row_pk": work.row_pk,
        "vector": embed_work(work),
        "model": work.model_id,
        "dims": work.dimensions,
        "work_item_id": work.work_item_id,
        "chunk_hash": work.chunk_hash,
        "token_count": work.token_count,
        "vector_name": work.vector_name,
    }


def build_failure(work: WorkItem, error: Exception, retryable: bool = True) -> dict[str, Any]:
    return {
        "tenant_id": work.tenant_id,
        "work_item_id": work.work_item_id,
        "error": str(error),
        "retryable": retryable,
        "error_code": type(error).__name__,
    }


def rerank(body: dict[str, Any]) -> dict[str, Any]:
    provider = os.environ.get("UDB_RERANK_PROVIDER", "deterministic").strip().lower()
    candidates = body.get("candidates")
    if not isinstance(candidates, list):
        raise SidecarError("candidates must be an array")
    if provider != "deterministic":
        reference = os.environ.get("UDB_RERANK_VAULT_REF", "").strip()
        secret = resolve_vault_reference(
            reference,
            required_str(body, "tenant_id"),
            required_str(body, "project_id"),
        )
        endpoint = str(secret.get("rerank_endpoint") or secret.get("endpoint") or "").strip()
        api_key = str(secret.get("api_key", ""))
        if not endpoint or not api_key:
            raise SidecarError("rerank vault secret must contain endpoint and api_key", 502)
        response = post_json(endpoint, {
            "model": str(body.get("model") or secret.get("rerank_model") or ""),
            "query": required_str(body, "query"),
            "documents": [str(candidate.get("text", "")) for candidate in candidates],
            "top_n": int(body.get("top_n") or len(candidates)),
        }, {"Authorization": f"Bearer {api_key}"})
        ranked = response.get("results")
        if not isinstance(ranked, list):
            raise SidecarError("rerank provider returned no results", 502)
        results = []
        for item in ranked:
            if not isinstance(item, dict):
                continue
            index = int(item.get("index", -1))
            if 0 <= index < len(candidates):
                results.append({"id": str(candidates[index].get("id", "")), "score": float(item.get("relevance_score") or item.get("score") or 0.0)})
        return {"results": results}
    query_tokens = set(re.findall(r"[\w]+", required_str(body, "query").lower()))
    results = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        text_tokens = set(re.findall(r"[\w]+", str(candidate.get("text", "")).lower()))
        lexical = len(query_tokens & text_tokens) / max(1, len(query_tokens | text_tokens))
        dense = float(candidate.get("score") or 0.0)
        results.append({"id": str(candidate.get("id", "")), "score": 0.65 * lexical + 0.35 * dense})
    results.sort(key=lambda item: item["score"], reverse=True)
    top_n = int(body.get("top_n") or len(results))
    return {"results": results[: max(1, top_n)]}


def parse_document(body: dict[str, Any]) -> dict[str, Any]:
    text = str(body.get("text", ""))
    parser_reference = os.environ.get("UDB_DOCUMENT_PARSER_VAULT_REF", "").strip()
    if parser_reference:
        secret = resolve_vault_reference(
            parser_reference,
            required_str(body, "tenant_id"),
            required_str(body, "project_id"),
        )
        parser_endpoint = str(secret.get("parser_endpoint") or secret.get("endpoint") or "").strip()
        api_key = str(secret.get("api_key", ""))
        if not parser_endpoint or not api_key:
            raise SidecarError("parser vault secret must contain parser_endpoint and api_key", 502)
        parsed = post_json(parser_endpoint, {
            "text": text,
            "storage_object_ref": str(body.get("storage_object_ref", "")),
            "content_type": str(body.get("content_type", "")),
            "layout_aware": True,
        }, {"Authorization": f"Bearer {api_key}"})
        text = str(parsed.get("markdown") or parsed.get("text") or "")
    if not text:
        storage_ref = required_str(body, "storage_object_ref")
        fetch_url = os.environ.get("UDB_STORAGE_FETCH_URL", "").strip()
        if not fetch_url:
            raise SidecarError("UDB_STORAGE_FETCH_URL is required for storage-backed parsing", 500)
        fetched = post_json(fetch_url, {"storage_object_ref": storage_ref})
        text = str(fetched.get("text", ""))
    if not text.strip():
        raise SidecarError("document parser produced no text", 422)
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    content_hash = hashlib.sha256(" ".join(text.split()).encode("utf-8")).hexdigest()
    return {
        "tenant_id": required_str(body, "tenant_id"),
        "document_id": required_str(body, "document_id"),
        "job_id": required_str(body, "job_id"),
        "text": text,
        "content_hash": content_hash,
    }


class Handler(http.server.BaseHTTPRequestHandler):
    server_version = "udb-embedding-sidecar/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write(
            "%s - - [%s] %s\n"
            % (self.client_address[0], self.log_date_time_string(), fmt % args)
        )

    def write_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/healthz":
            try:
                dims = configured_dims()
            except SidecarError as exc:
                self.write_json(exc.status, {"ok": False, "error": str(exc)})
                return
            self.write_json(
                200,
                {"ok": True, "provider": provider_name(), "dims": dims},
            )
            return
        self.write_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path not in {"/embed", "/v1/embed", "/embed-batch", "/v1/embed-batch", "/rerank", "/v1/rerank", "/parse", "/v1/parse"}:
            self.write_json(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("content-length", "0"))
        except ValueError:
            self.write_json(400, {"error": "invalid Content-Length"})
            return
        if length <= 0 or length > MAX_BODY_BYTES:
            self.write_json(413, {"error": "request body size is invalid"})
            return
        try:
            raw = self.rfile.read(length)
            if self.path.endswith("/rerank") or self.path == "/rerank":
                value = json.loads(raw.decode("utf-8"))
                self.write_json(200, rerank(value))
                return
            if self.path.endswith("/parse") or self.path == "/parse":
                value = json.loads(raw.decode("utf-8"))
                self.write_json(200, {"report_parsed_document_request": parse_document(value)})
                return
            if "embed-batch" in self.path:
                value = json.loads(raw.decode("utf-8"))
                items = value.get("items") if isinstance(value, dict) else None
                if not isinstance(items, list) or not items or len(items) > MAX_BATCH:
                    raise SidecarError(f"items must contain 1..{MAX_BATCH} work payloads")
                reports = [build_report(parse_work(json.dumps(item).encode("utf-8"))) for item in items]
                self.write_json(200, {"status": "embedded", "declared_capacity": MAX_BATCH, "report_embedding_batch_request": {"tenant_id": reports[0]["tenant_id"], "items": reports, "declared_capacity": MAX_BATCH}})
                return
            work = parse_work(raw)
            try:
                report = build_report(work)
            except Exception as exc:
                self.write_json(502, {"status": "failed", "report_embedding_failure_request": build_failure(work, exc)})
                return
            self.write_json(
                200,
                {
                    "status": "embedded",
                    "provider": provider_name(),
                    "target_collection": work.target_collection,
                    "report_embedding_request": report,
                },
            )
        except SidecarError as exc:
            self.write_json(exc.status, {"error": str(exc)})
        except Exception as exc:  # Provider libraries often raise broad exceptions.
            self.write_json(502, {"error": f"embedding failed: {exc}"})


def main() -> None:
    port = int(os.environ.get("PORT", str(DEFAULT_PORT)))
    bind = os.environ.get("HOST", "0.0.0.0")
    httpd = http.server.ThreadingHTTPServer((bind, port), Handler)
    print(
        json.dumps(
            {
                "event": "udb_embedding_sidecar_started",
                "provider": provider_name(),
                "dims": configured_dims(),
                "port": port,
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
    httpd.serve_forever()


if __name__ == "__main__":
    main()
