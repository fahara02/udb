#!/usr/bin/env python3
"""UDB embedding inference sidecar.

The broker stays model-free. It emits `udb.embedding.work.v1` payloads containing
only row identity, text, model id, and vector routing. This sidecar turns one work
payload into the body a trusted sidecar can submit to the broker's internal
`ReportEmbedding` callback.

The default provider is deterministic and dependency-free for local/container
smoke. Production deployments should replace `embed_text` with a provider module
that uses sidecar-local model credentials; credential-shaped keys in broker work
payloads are rejected here so secrets cannot creep into the event contract.
"""

from __future__ import annotations

import hashlib
import http.server
import json
import math
import os
import sys
from dataclasses import dataclass
from typing import Any


DEFAULT_PORT = 8080
MAX_BODY_BYTES = 512 * 1024
DEFAULT_DIMS = 16
MAX_DIMS = 4096
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
    source: str
    row_pk: str
    text: str
    model_id: str
    target_collection: str


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
    return WorkItem(
        tenant_id=required_str(value, "tenant_id"),
        source=required_str(value, "source"),
        row_pk=required_str(value, "row_pk"),
        text=required_str(value, "text"),
        model_id=required_str(value, "model_id"),
        target_collection=str(value.get("target_collection", "")).strip(),
    )


def embed_text(text: str, model_id: str, dims: int) -> list[float]:
    provider = provider_name()
    if provider != "deterministic":
        raise SidecarError(
            "only UDB_EMBED_PROVIDER=deterministic is built into this sidecar image",
            500,
        )
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
    norm = math.sqrt(sum(value * value for value in values)) or 1.0
    return [round(value / norm, 8) for value in values]


def build_report(work: WorkItem) -> dict[str, Any]:
    dims = configured_dims()
    return {
        "tenant_id": work.tenant_id,
        "source_name": work.source,
        "row_pk": work.row_pk,
        "vector": embed_text(work.text, work.model_id, dims),
        "model": work.model_id,
        "dims": dims,
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
        if self.path not in {"/embed", "/v1/embed"}:
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
            work = parse_work(self.rfile.read(length))
            report = build_report(work)
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
