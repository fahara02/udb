#!/usr/bin/env python3
"""Smoke-test the UDB embedding sidecar contract."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SIDECAR = ROOT / "sidecars" / "embedding" / "embedding_sidecar.py"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def request_json(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"} if body is not None else {},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8") or "{}")


def wait_for_health(base_url: str, proc: subprocess.Popen[bytes] | None, deadline: float) -> int:
    last_error = ""
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"sidecar exited early with status {proc.returncode}")
        try:
            status, payload = request_json("GET", f"{base_url}/healthz")
            if status == 200 and payload.get("ok") is True:
                return int(payload.get("dims", 0))
            last_error = f"status={status} payload={payload}"
        except Exception as exc:  # noqa: BLE001 - startup diagnostics
            last_error = str(exc)
        time.sleep(0.1)
    raise TimeoutError(f"sidecar did not become healthy: {last_error}")


def assert_report(payload: dict[str, Any], dims: int, work: dict[str, Any] | None = None) -> str:
    if payload.get("status") != "embedded":
        raise RuntimeError(f"unexpected status payload: {payload}")
    report = payload.get("report_embedding_request")
    if not isinstance(report, dict):
        raise RuntimeError(f"missing report_embedding_request: {payload}")
    expected = {
        "tenant_id": (work or {}).get("tenant_id", "tenant-a"),
        "source_name": (work or {}).get("source", "contacts"),
        "row_pk": (work or {}).get("row_pk", "contact-1"),
        "model": (work or {}).get("model_id", "deterministic-v1"),
        "dims": dims,
    }
    for key, value in expected.items():
        if report.get(key) != value:
            raise RuntimeError(f"report field {key!r} mismatch: {report}")
    vector = report.get("vector")
    if not isinstance(vector, list) or len(vector) != dims:
        raise RuntimeError(f"vector dimension mismatch: {report}")
    if any(not isinstance(value, (int, float)) for value in vector):
        raise RuntimeError(f"vector contains non-numeric values: {vector}")
    return json.dumps(report, sort_keys=True, separators=(",", ":"))


def expect_runtime_error(label: str, fn: Any) -> None:
    try:
        fn()
    except RuntimeError:
        return
    raise AssertionError(f"selftest did not reject {label}")


def run_selftest() -> None:
    report = {
        "tenant_id": "tenant-a",
        "source_name": "contacts",
        "row_pk": "contact-1",
        "model": "deterministic-v1",
        "dims": 3,
        "vector": [0.1, 0.2, 0.3],
    }
    payload = {"status": "embedded", "report_embedding_request": report}
    encoded = assert_report(payload, 3)
    if json.loads(encoded)["vector"] != report["vector"]:
        raise AssertionError("selftest report encoding changed vector payload")

    bad_dims = {"status": "embedded", "report_embedding_request": dict(report, dims=4)}
    expect_runtime_error("dimension mismatch", lambda: assert_report(bad_dims, 3))

    bad_vector = {
        "status": "embedded",
        "report_embedding_request": dict(report, vector=[0.1, "bad", 0.3]),
    }
    expect_runtime_error("non-numeric vector", lambda: assert_report(bad_vector, 3))

    bad_status = {"status": "queued", "report_embedding_request": report}
    expect_runtime_error("unexpected status", lambda: assert_report(bad_status, 3))

    spec = importlib.util.spec_from_file_location("udb_embedding_sidecar_security", SIDECAR)
    if spec is None or spec.loader is None:
        raise AssertionError("could not load embedding sidecar for security selftest")
    sidecar = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = sidecar
    spec.loader.exec_module(sidecar)
    if sidecar.vault_cache_key("tenant-a", "project-a", "vault://provider") == sidecar.vault_cache_key(
        "tenant-b", "project-a", "vault://provider"
    ):
        raise AssertionError("Vault cache key is not tenant-scoped")
    if sidecar.vault_cache_key("tenant-a", "project-a", "vault://provider") == sidecar.vault_cache_key(
        "tenant-a", "project-b", "vault://provider"
    ):
        raise AssertionError("Vault cache key is not project-scoped")

    saved_resolver = os.environ.get("UDB_VAULT_RESOLVER_URL")
    saved_token = os.environ.get("UDB_VAULT_RESOLVER_TOKEN")
    calls: list[tuple[str, dict[str, Any], dict[str, str]]] = []
    try:
        os.environ["UDB_VAULT_RESOLVER_URL"] = "https://vault-resolver.invalid/resolve"
        os.environ["UDB_VAULT_RESOLVER_TOKEN"] = "resolver-test-token"
        sidecar._vault_cache.clear()

        def fake_post(url: str, body: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
            calls.append((url, body, headers))
            return {"data": {"endpoint": "https://provider.invalid", "api_key": "redacted"}}

        sidecar.post_json = fake_post
        sidecar.resolve_vault_reference("vault://provider", "tenant-a", "project-a")
        sidecar.resolve_vault_reference("vault://provider", "tenant-a", "project-a")
        sidecar.resolve_vault_reference("vault://provider", "tenant-a", "project-b")
        if len(calls) != 2:
            raise AssertionError("Vault cache did not isolate project scope or reuse identical scope")
        if calls[0][1] != {
            "reference": "vault://provider",
            "tenant_id": "tenant-a",
            "project_id": "project-a",
        }:
            raise AssertionError(f"Vault resolver did not receive canonical scope: {calls[0][1]}")
        if calls[0][2].get("Authorization") != "Bearer resolver-test-token":
            raise AssertionError("Vault resolver call was not authenticated")
    finally:
        sidecar._vault_cache.clear()
        if saved_resolver is None:
            os.environ.pop("UDB_VAULT_RESOLVER_URL", None)
        else:
            os.environ["UDB_VAULT_RESOLVER_URL"] = saved_resolver
        if saved_token is None:
            os.environ.pop("UDB_VAULT_RESOLVER_TOKEN", None)
        else:
            os.environ["UDB_VAULT_RESOLVER_TOKEN"] = saved_token


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test the UDB embedding sidecar")
    parser.add_argument("--selftest", action="store_true", help="run no-network report-shape checks")
    parser.add_argument("--url", help="Existing sidecar base URL, for example http://127.0.0.1:58090")
    args = parser.parse_args()

    if args.selftest:
        run_selftest()
        print("embedding sidecar smoke selftest passed")
        return 0

    dims = 12
    proc: subprocess.Popen[bytes] | None = None
    if args.url:
        base_url = args.url.rstrip("/")
    else:
        port = free_port()
        base_url = f"http://127.0.0.1:{port}"
        env = os.environ.copy()
        env.update(
            {
                "HOST": "127.0.0.1",
                "PORT": str(port),
                "UDB_EMBED_PROVIDER": "deterministic",
                "UDB_EMBED_DIMS": str(dims),
            }
        )
        proc = subprocess.Popen(
            [sys.executable, str(SIDECAR)],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    try:
        health_dims = wait_for_health(base_url, proc, time.monotonic() + 10)
        if health_dims > 0:
            dims = health_dims
        work = {
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "source": "contacts",
            "row_pk": "contact-1",
            "text": "Ada Lovelace wrote the first algorithm.",
            "model_id": "deterministic-v1",
            "target_collection": "contacts_vec",
            "work_item_id": "work-contact-1",
            "job_id": "job-contact-1",
            "chunk_hash": "hash-contact-1",
            "token_count": 7,
            "dimensions": dims,
            "provider": "deterministic",
        }
        status, payload = request_json("POST", f"{base_url}/embed", work)
        if status != 200:
            raise RuntimeError(f"embed failed: status={status} payload={payload}")
        first_report = assert_report(payload, dims, work)

        status, payload = request_json("POST", f"{base_url}/embed", work)
        if status != 200:
            raise RuntimeError(f"second embed failed: status={status} payload={payload}")
        second_report = assert_report(payload, dims, work)
        if first_report != second_report:
            raise RuntimeError("deterministic provider returned different vectors for the same work")

        report = json.loads(first_report)
        for key in ("work_item_id", "chunk_hash", "token_count"):
            if report.get(key) != work[key]:
                raise RuntimeError(f"durable work identity {key!r} was not echoed")

        contextual = dict(work, row_pk="contact-context", work_item_id="work-context",
                          contextual_retrieval=True, parent_text="Ada worked on mathematics and engines.",
                          char_start=0, char_end=len(work["text"]), token_start=0, token_end=7)
        status, contextual_payload = request_json("POST", f"{base_url}/embed", contextual)
        if status != 200:
            raise RuntimeError(f"contextual embed failed: status={status} payload={contextual_payload}")
        assert_report(contextual_payload, dims, contextual)

        late = dict(work, row_pk="contact-late", work_item_id="work-late", late_chunking=True,
                    parent_text="Ada designed an engine algorithm for computation.",
                    text="engine algorithm", token_start=3, token_end=5, token_count=2)
        status, late_payload = request_json("POST", f"{base_url}/embed", late)
        if status != 200:
            raise RuntimeError(f"late chunk embed failed: status={status} payload={late_payload}")
        assert_report(late_payload, dims, late)

        status, batch_payload = request_json("POST", f"{base_url}/embed-batch", {
            "items": [dict(work, row_pk="batch-1", work_item_id="work-batch-1"),
                      dict(work, row_pk="batch-2", work_item_id="work-batch-2")]
        })
        batch = batch_payload.get("report_embedding_batch_request")
        if status != 200 or not isinstance(batch, dict) or len(batch.get("items", [])) != 2:
            raise RuntimeError(f"batch embed contract failed: status={status} payload={batch_payload}")

        status, rerank_payload = request_json("POST", f"{base_url}/rerank", {
            "tenant_id": "tenant-a", "project_id": "project-a",
            "query": "first algorithm", "top_n": 2,
            "candidates": [{"id": "weak", "text": "weather report", "score": 0.9},
                           {"id": "strong", "text": "first algorithm", "score": 0.5}],
        })
        if status != 200 or rerank_payload.get("results", [{}])[0].get("id") != "strong":
            raise RuntimeError(f"rerank contract failed: status={status} payload={rerank_payload}")

        status, parse_payload = request_json("POST", f"{base_url}/parse", {
            "tenant_id": "tenant-a", "project_id": "project-a",
            "document_id": "doc-1", "job_id": "job-1",
            "text": "<h1>Title</h1><p>Useful body.</p>",
        })
        parsed = parse_payload.get("report_parsed_document_request")
        if status != 200 or not isinstance(parsed, dict) or "Useful body" not in parsed.get("text", ""):
            raise RuntimeError(f"parser contract failed: status={status} payload={parse_payload}")

        rejected = dict(work)
        rejected["api_key"] = "must-not-cross-the-broker-event"
        status, payload = request_json("POST", f"{base_url}/embed", rejected)
        if status != 400 or "credential" not in str(payload.get("error", "")):
            raise RuntimeError(f"credential-shaped key was not rejected: status={status} payload={payload}")

        print(json.dumps({"ok": True, "provider": "deterministic", "dims": dims}, separators=(",", ":")))
        return 0
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
