#!/usr/bin/env python3
"""Restart the workflow-owned benchmark broker in place, keeping its database.

Used as `UDB_LIVE_BROKER_RESTART_CMD` by the SDK live tests that prove a client
recovers from a broker restart without being recreated. The broker is stopped
with SIGTERM and started again with the same binary, arguments, environment and
working directory (read from /proc, in memory only: credentials never reach a
log). The database is left as it is, so tenants, users and sessions survive the
restart exactly as they would in production. Exits 0 once both listeners report
SERVING again; the PID file is updated for the workflow's cleanup.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

PID_PATH = Path("/tmp/udb-bench.pid")
LOG_PATH = Path("/tmp/udb-restart-proof.log")
EXPECTED_ARGS = ["serve", "proto", "", "127.0.0.1:50051"]


def fail(message: str) -> None:
    print(f"restart_bench_broker: {message}", file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    try:
        import grpc
    except ImportError:
        fail("grpcio is required to wait for the restarted broker")

    pid = int(PID_PATH.read_text().strip())
    if pid <= 1:
        fail("invalid workflow-owned broker PID")
    proc = Path(f"/proc/{pid}")
    argv = [os.fsdecode(v) for v in (proc / "cmdline").read_bytes().split(b"\0")[:-1]]
    expected = os.environ.get("UDB_BENCH_BIN")
    if not argv or argv[1:] != EXPECTED_ARGS or (
        expected and Path(argv[0]).resolve() != Path(expected).resolve()
    ):
        fail("PID does not identify this workflow's broker")
    broker_env = dict(
        os.fsdecode(v).split("=", 1) for v in (proc / "environ").read_bytes().split(b"\0") if v
    )
    broker_cwd = os.readlink(proc / "cwd")

    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + 20
    while proc.exists():
        try:
            state = (proc / "stat").read_text().rsplit(")", 1)[1].split()[0]
        except FileNotFoundError:
            break
        if state == "Z":
            break
        if time.monotonic() >= deadline:
            fail("broker did not stop within 20 s")
        time.sleep(0.1)
    # The listeners must be free before the new process binds them.
    for port in (50051, 50061):
        port_deadline = time.monotonic() + 20
        while True:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    pass
            except OSError:
                break
            if time.monotonic() >= port_deadline:
                fail(f"port {port} still in use after the broker stopped")
            time.sleep(0.1)

    with LOG_PATH.open("ab") as output:
        broker = subprocess.Popen(
            argv,
            cwd=broker_cwd,
            env=broker_env,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    PID_PATH.write_text(f"{broker.pid}\n")

    channels = [
        grpc.insecure_channel(broker_env.get(name, default))
        for name, default in (
            ("UDB_GRPC_TARGET", "127.0.0.1:50051"),
            ("UDB_AUTH_GRPC_TARGET", "127.0.0.1:50061"),
        )
    ]
    try:
        deadline = time.monotonic() + 120
        while True:
            if broker.poll() is not None:
                fail("restarted broker exited before it was ready")
            ready = True
            for channel in channels:
                try:
                    # Empty request; HealthCheckResponse status=SERVING.
                    ready = (
                        channel.unary_unary("/grpc.health.v1.Health/Check")(b"", timeout=1)
                        == b"\x08\x01"
                        and ready
                    )
                except grpc.RpcError:
                    ready = False
            if ready:
                break
            if time.monotonic() >= deadline:
                fail("restarted broker listeners did not report SERVING within 120 s")
            time.sleep(0.5)
    finally:
        for channel in channels:
            channel.close()
    print("restart_bench_broker: broker restarted with the same database and settings")


if __name__ == "__main__":
    main()
