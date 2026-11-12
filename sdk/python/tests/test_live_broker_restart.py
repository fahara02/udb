"""Live broker-restart recovery (parity with the Go SDK's TestLiveBrokerRestartRecovery).

Skipped unless ``UDB_LIVE_SDK_TESTS=1`` and ``UDB_LIVE_BROKER_RESTART_CMD`` names a
command (run with ``sh -c``) that restarts the broker on the same addresses and
KEEPS its database (sessions, users and tenant ids survive); a reset that recreates
the database is a different scenario.

The same ``connect_enterprise`` session must reconnect by itself and keep working on
the data plane (GetCapabilities) and the native control plane (ValidateSession).
While the broker is down a call may fail UNAVAILABLE; any other failure, or no
recovery within 90s, fails the test.
"""

from __future__ import annotations

import os
import subprocess
import time
from typing import Any, Callable

import grpc
import pytest

from udb_client._reconnect import status_code
from udb_client.enterprise import EnterpriseConfig, connect_enterprise

RESTART_CMD = os.getenv("UDB_LIVE_BROKER_RESTART_CMD", "").strip()
RECOVERY_S = 90.0

pytestmark = pytest.mark.skipif(
    os.getenv("UDB_LIVE_SDK_TESTS") != "1" or not RESTART_CMD,
    reason="requires a live broker and UDB_LIVE_BROKER_RESTART_CMD",
)


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise AssertionError(f"{name} is required when UDB_LIVE_SDK_TESTS=1")
    return value


def test_live_broker_restart_recovery() -> None:
    sess = connect_enterprise(
        EnterpriseConfig(
            target=required_env("UDB_GRPC_TARGET"),
            auth_target=required_env("UDB_AUTH_GRPC_TARGET"),
            username=required_env("UDB_LIVE_USERNAME"),
            password=required_env("UDB_LIVE_PASSWORD"),
            tenant_code=required_env("UDB_LIVE_TENANT"),
            project_id=required_env("UDB_LIVE_PROJECT"),
            purpose="python.live.broker-restart",
            deadline=5.0,
        )
    )
    with sess:
        probes: list[tuple[str, Callable[[], Any]]] = [
            ("data GetCapabilities", lambda: sess.data.get_capabilities(metadata=sess.data_metadata())),
            (
                "native ValidateSession",
                lambda: sess.auth.validate_session(sess.bearer(), metadata=sess.native_metadata()),
            ),
        ]
        for name, call in probes:
            try:
                call()
            except Exception as exc:  # noqa: BLE001 - reported with its status
                pytest.fail(f"before restart, {name}: code={status_code(exc)} err={exc}")

        subprocess.run(["sh", "-c", RESTART_CMD], check=True)

        for name, call in probes:
            deadline = time.monotonic() + RECOVERY_S
            while True:
                try:
                    call()
                    break
                except Exception as exc:  # noqa: BLE001 - only UNAVAILABLE is tolerated
                    code = status_code(exc)
                    if code != grpc.StatusCode.UNAVAILABLE:
                        pytest.fail(f"after restart, {name} on the same session: code={code} err={exc}")
                    if time.monotonic() > deadline:
                        pytest.fail(f"after restart, {name} stayed UNAVAILABLE for 90s: {exc}")
                    time.sleep(0.25)
        assert sess.refresh_error() is None, "the session reports a credential failure after the restart"
