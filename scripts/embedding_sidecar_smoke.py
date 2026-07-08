#!/usr/bin/env python3
"""Smoke-test the UDB embedding sidecar contract."""

from __future__ import annotations

import argparse
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


def assert_report(payload: dict[str, Any], dims: int) -> str:
    if payload.get("status") != "embedded":
        raise RuntimeError(f"unexpected status payload: {payload}")
    report = payload.get("report_embedding_request")
    if not isinstance(report, dict):
        raise RuntimeError(f"missing report_embedding_request: {payload}")
    expected = {
        "tenant_id": "tenant-a",
        "source_name": "contacts",
        "row_pk": "contact-1",
        "model": "deterministic-v1",
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
            "source": "contacts",
            "row_pk": "contact-1",
            "text": "Ada Lovelace wrote the first algorithm.",
            "model_id": "deterministic-v1",
            "target_collection": "contacts_vec",
        }
        status, payload = request_json("POST", f"{base_url}/embed", work)
        if status != 200:
            raise RuntimeError(f"embed failed: status={status} payload={payload}")
        first_report = assert_report(payload, dims)

        status, payload = request_json("POST", f"{base_url}/embed", work)
        if status != 200:
            raise RuntimeError(f"second embed failed: status={status} payload={payload}")
        second_report = assert_report(payload, dims)
        if first_report != second_report:
            raise RuntimeError("deterministic provider returned different vectors for the same work")

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
