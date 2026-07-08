#!/usr/bin/env python3
"""Smoke-test the notification provider sidecar.

This is intentionally sidecar-scoped. The broker delivery worker reuses the
WebhookService SSRF guard, so local/private HTTP endpoints are not valid broker
targets. This smoke proves the adapter contract the broker will call once an
operator deploys the sidecar behind an allowed HTTPS endpoint.
"""

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
SIDECAR = ROOT / "sidecars" / "notify" / "notify_sidecar.py"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def request_json(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], dict[str, Any]]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return resp.status, dict(resp.headers.items()), payload
    except urllib.error.HTTPError as exc:
        payload = json.loads(exc.read().decode("utf-8") or "{}")
        return exc.code, dict(exc.headers.items()), payload


def wait_for_health(base_url: str, proc: subprocess.Popen[bytes], deadline: float) -> None:
    last_error = ""
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"sidecar exited early with status {proc.returncode}")
        try:
            status, _headers, payload = request_json("GET", f"{base_url}/healthz")
            if status == 200 and payload.get("ok") is True:
                return
            last_error = f"status={status} payload={payload}"
        except Exception as exc:  # noqa: BLE001 - diagnostics for smoke startup
            last_error = str(exc)
        time.sleep(0.1)
    raise TimeoutError(f"sidecar did not become healthy: {last_error}")


def provider_message_id_from(headers: dict[str, str]) -> str:
    return next(
        (value for key, value in headers.items() if key.lower() == "x-provider-message-id"),
        "",
    )


def assert_send_response(status: int, headers: dict[str, str], payload: dict[str, Any]) -> str:
    provider_message_id = provider_message_id_from(headers)
    if status != 200:
        raise RuntimeError(f"send failed: status={status} payload={payload}")
    if payload.get("status") != "sent" or payload.get("provider") != "smtp":
        raise RuntimeError(f"unexpected send payload: {payload}")
    if not provider_message_id or provider_message_id != payload.get("provider_message_id"):
        raise RuntimeError(
            f"missing/mismatched x-provider-message-id: header={provider_message_id!r} payload={payload}"
        )
    return provider_message_id


def expect_runtime_error(label: str, fn: Any) -> None:
    try:
        fn()
    except RuntimeError:
        return
    raise AssertionError(f"selftest did not reject {label}")


def run_selftest() -> None:
    payload = {
        "status": "sent",
        "provider": "smtp",
        "provider_message_id": "smtp-dryrun-123",
    }
    message_id = assert_send_response(
        200,
        {"X-Provider-Message-Id": "smtp-dryrun-123"},
        payload,
    )
    if message_id != "smtp-dryrun-123":
        raise AssertionError("selftest provider message id changed")

    expect_runtime_error(
        "missing provider message header",
        lambda: assert_send_response(200, {}, payload),
    )
    expect_runtime_error(
        "provider message id mismatch",
        lambda: assert_send_response(
            200,
            {"x-provider-message-id": "different"},
            payload,
        ),
    )
    expect_runtime_error(
        "unexpected provider status",
        lambda: assert_send_response(
            200,
            {"x-provider-message-id": "smtp-dryrun-123"},
            dict(payload, status="queued"),
        ),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test the UDB notify sidecar")
    parser.add_argument("--selftest", action="store_true", help="run no-network send-response checks")
    parser.add_argument("--url", help="Existing sidecar base URL, for example http://127.0.0.1:58080")
    args = parser.parse_args()

    if args.selftest:
        run_selftest()
        print("notification sidecar smoke selftest passed")
        return 0

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
                "UDB_NOTIFY_PROVIDER": "smtp",
                "UDB_NOTIFY_DRY_RUN": "1",
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
        if proc is not None:
            wait_for_health(base_url, proc, time.monotonic() + 10)
        status, headers, payload = request_json(
            "POST",
            f"{base_url}/send",
            {
                "to": "ops@example.com",
                "subject": "UDB notify sidecar smoke",
                "body": "dry-run delivery",
            },
            {
                "Authorization": "Bearer smoke-credential",
                "Content-Type": "application/json",
            },
        )
        provider_message_id = assert_send_response(status, headers, payload)
        print(
            json.dumps(
                {
                    "ok": True,
                    "provider": payload["provider"],
                    "provider_message_id": provider_message_id,
                },
                separators=(",", ":"),
            )
        )
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
