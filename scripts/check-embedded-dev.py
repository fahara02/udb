#!/usr/bin/env python3
"""CI proof for embedded startup, shared Go conformance and persisted restart."""
import hashlib
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid


PORTS = (54329, 50051, 50061)


def start(command, project, environment, log):
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    return subprocess.Popen(command, cwd=project, env=environment, stdout=log,
                            stderr=log, creationflags=flags)


def assert_stopped(process):
    if process.wait(timeout=45) != 0:
        raise RuntimeError("embedded dev shutdown failed")
    for port in PORTS:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                raise RuntimeError(f"embedded shutdown left port {port} listening")
        except OSError:
            pass


def interrupt(process):
    process.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT)
    assert_stopped(process)


def interrupted_download(command, environment, log):
    started, finish = threading.Event(), threading.Event()

    class SlowArchive(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "1")
            self.end_headers()
            started.set()
            finish.wait(timeout=60)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), SlowArchive)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory(prefix="udb-download-interrupt-") as temp:
            project = Path(temp)
            test_env = environment | {
                "UDB_HOME": str(project / "cache"),
                "UDB_POSTGRES_BINARIES_URL": f"http://127.0.0.1:{server.server_port}",
            }
            process = start(command, project, test_env, log)
            try:
                if not started.wait(timeout=30):
                    raise RuntimeError("embedded dev never began its download")
                interrupt(process)
                if (project / ".udb" / "dev" / "pgdata" / "postmaster.pid").exists():
                    raise RuntimeError("interrupted download started PostgreSQL")
                print("embedded dev: interruption cancelled an in-progress download")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
    finally:
        finish.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def interrupted_startup(command, environment, log):
    with tempfile.TemporaryDirectory(prefix="udb-startup-interrupt-") as temp:
        project = Path(temp)
        proto = project / "proto"
        proto.mkdir()
        test_env = environment | {"UDB_PROTO_ROOT": str(proto)}
        process = start(command, project, test_env, log)
        try:
            pid = project / ".udb" / "dev" / "pgdata" / "postmaster.pid"
            deadline = time.monotonic() + 60
            while not pid.exists():
                if process.poll() is not None:
                    raise RuntimeError("embedded dev exited before PostgreSQL started")
                if time.monotonic() >= deadline:
                    raise RuntimeError("PostgreSQL did not start for the interruption proof")
                time.sleep(0.05)
            interrupt(process)
            if pid.exists():
                raise RuntimeError("startup interruption left PostgreSQL running")
            print("embedded dev: startup interruption stopped PostgreSQL and its broker")
        finally:
            if process.poll() is None:
                subprocess.run([command[0], "dev", "down", "--embedded"],
                               cwd=project, env=test_env, stdout=log, stderr=log, timeout=45)
                process.wait(timeout=45)


def run_go_contract(repo, environment, proof, selector, phase, timeout):
    command = ["go", "test", "./udbtest", "-run", "^" + selector + "$", "-json", "-count=1", "-timeout", str(timeout - 60) + "s"]
    result = subprocess.run(command, cwd=repo / "sdk" / "go", env=environment,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    diagnostics = Path(os.environ["RUNNER_TEMP"]) / "udb-embedded-go-receipts"
    diagnostics.mkdir(exist_ok=True)
    (diagnostics / (phase + ".jsonl")).write_bytes(result.stdout)
    (diagnostics / (phase + ".stderr")).write_bytes(result.stderr)
    receipt = go_contract_receipt(result.stdout, result.stderr, result.returncode, selector, phase)
    proof.setdefault("go_contracts", []).append(receipt)


def go_contract_receipt(stdout, stderr, returncode, selector, phase):
    if len(stdout) > 8 * 1024 * 1024 or len(stderr) > 1024 * 1024:
        raise RuntimeError("Go contract diagnostics exceeded the receipt bound")
    events = [json.loads(line) for line in stdout.decode("utf-8").splitlines() if line.strip()]
    if any(not isinstance(event, dict) for event in events):
        raise RuntimeError("Go contract receipt must contain JSON objects")
    package = "github.com/fahara02/udb/sdk/go/udbtest"
    if any(event.get("Test") and event.get("Package") != package for event in events):
        raise RuntimeError("Go contract test receipt belongs to a foreign package")
    starts = [i for i, event in enumerate(events) if event.get("Action") == "run" and event.get("Test") == selector]
    passes = [i for i, event in enumerate(events) if event.get("Action") == "pass" and event.get("Test") == selector]
    refused = [event for event in events if event.get("Action") in {"skip", "fail"}]
    package_pass = [i for i, event in enumerate(events) if event.get("Action") == "pass" and not event.get("Test")]
    if returncode or len(starts) != 1 or len(passes) != 1 or refused or len(package_pass) != 1:
        raise RuntimeError("missing, skipped, failed or duplicated actual Go contract")
    if events[package_pass[0]].get("Package") != package or not starts[0] < passes[0] < package_pass[0]:
        raise RuntimeError("Go contract completion order or package is invalid")
    return {
        "phase": phase, "selector": selector, "package": package, "exit_code": returncode,
        "named_started": len(starts), "named_passed": len(passes), "skipped_or_failed": len(refused),
        "package_passed": len(package_pass), "jsonl_sha256": hashlib.sha256(stdout).hexdigest(),
        "stderr_sha256": hashlib.sha256(stderr).hexdigest(),
    }


def receipt_selftest():
    package, selector = "github.com/fahara02/udb/sdk/go/udbtest", "TestOwnedContract"
    def event(action, test=None):
        value = {"Action": action, "Package": package}
        if test is not None:
            value["Test"] = test
        return value
    good = [event("start"), event("run", selector), event("pass", selector), event("pass")]
    def encoded(events):
        return b"".join((json.dumps(item) + "\n").encode() for item in events)
    for events in [good, good[:2] + [event("run", selector + "/owned"), event("pass", selector + "/owned")] + good[2:]]:
        assert go_contract_receipt(encoded(events), b"", 0, selector, "control")["named_passed"] == 1
    negative = [
        (b"", b"", 0),
        (encoded(good), b"", 1),
        (encoded(good[:1] + good[2:]), b"", 0),
        (encoded(good[:2] + good[3:]), b"", 0),
        (encoded(good[:-1]), b"", 0),
        (encoded(good[:2] + [event("run", selector)] + good[2:]), b"", 0),
        (encoded(good[:3] + [event("pass", selector)] + good[3:]), b"", 0),
        (encoded(good + [event("pass")]), b"", 0),
        (encoded(good[:2] + [event("skip", selector + "/hidden")] + good[2:]), b"", 0),
        (encoded(good + [event("fail")]), b"", 0),
        (encoded([good[0], good[2], good[1], good[3]]), b"", 0),
        (encoded([good[0], good[1], good[3], good[2]]), b"", 0),
        (encoded([good[0], good[1] | {"Package": "foreign"}, *good[2:]]), b"", 0),
        (encoded([*good[:3], good[3] | {"Package": "foreign"}]), b"", 0),
        (encoded(good + [[]]), b"", 0),
        (b"invalid-json\n", b"", 0),
        (b"\xff\n", b"", 0),
        (b"x" * (8 * 1024 * 1024 + 1), b"", 0),
        (encoded(good), b"x" * (1024 * 1024 + 1), 0),
    ]
    for stdout, stderr, returncode in negative:
        try:
            go_contract_receipt(stdout, stderr, returncode, selector, "control")
        except (RuntimeError, ValueError, UnicodeDecodeError):
            continue
        raise AssertionError("invalid Go contract receipt was accepted")
    print(json.dumps({"positive_controls": 2, "negative_controls": len(negative), "application_execution": False}))



def main(proof):
    def phase(name):
        if proof["current_phase"] != "prepare":
            proof["completed_phases"].append(proof["current_phase"])
        proof["current_phase"] = name

    repo = Path(__file__).resolve().parents[1]
    binary = Path(sys.argv[1]).resolve()
    # The broker intentionally prints first-start credentials. Raw diagnostics
    # remain private on the ephemeral runner; only named receipts are uploaded.
    log_path = Path(os.environ["RUNNER_TEMP"]) / "udb-embedded-dev.private.log"
    with binary.open("rb") as source:
        proof["binary_sha256"] = hashlib.file_digest(source, "sha256").hexdigest()
    proof["source_commit"] = os.environ.get("GITHUB_SHA", "")
    with tempfile.TemporaryDirectory(prefix="udb-embedded-") as temp, log_path.open("w") as log:
        project = Path(temp)
        environment = os.environ.copy()
        environment["UDB_HOME"] = str(project / "cache")
        proto = project / "proto"
        proto.mkdir()
        environment["UDB_PROTO_ROOT"] = str(proto)
        for key in ("UDB_PG_DSN", "DATABASE_URL", "UDB_SESSION_HASH_SECRET", "UDB_JWT_PRIVATE_KEY", "UDB_JWT_PUBLIC_KEY", "UDB_ENCRYPTION_KEY", "UDB_EMBEDDED_PG_PASSWORD", "UDB_DEV_ADMIN_SECRET", "UDB_EMBEDDED_PG_ARCHIVE"):
            environment.pop(key, None)
        marker = project / ".udb" / "dev" / "bootstrap.json"
        command = [str(binary), "dev", "up", "--embedded"]
        phase("interrupted-download")
        interrupted_download(command, environment, log)
        first_marker = None
        peer_tenant = str(uuid.uuid4())
        persisted_row_id = str(uuid.uuid4())
        for iteration in range(2):
            phase(f"start-{iteration + 1}")
            process = start(command, project, environment, log)
            try:
                deadline = time.monotonic() + 600
                while True:
                    if process.poll() is not None:
                        raise RuntimeError(f"embedded dev exited during startup: {process.returncode}; see {log_path}")
                    ready = marker.exists()
                    if ready:
                        try:
                            for port in (50051, 50061):
                                with socket.create_connection(("127.0.0.1", port), timeout=1):
                                    pass
                            break
                        except OSError:
                            pass
                    if time.monotonic() >= deadline:
                        raise RuntimeError("embedded dev did not become ready within 10 minutes")
                    time.sleep(0.5)
                contents = marker.read_bytes()
                if first_marker is None:
                    first_marker = contents
                elif contents != first_marker:
                    raise RuntimeError("restart replaced the persisted admin credentials")
                credentials = json.loads(contents)
                peer_username = "udbtest-embedded-peer"
                peer_password = "UdbTestPeer#2026Pass"
                phase(f"peer-bootstrap-{iteration + 1}")
                subprocess.run([str(binary), "auth", "bootstrap", "user", "--username", peer_username,
                                "--email", peer_username + "@udbtest.invalid", "--password", peer_password,
                                "--tenant", peer_tenant, "--project", "default"],
                               cwd=project, env=environment | {"UDB_PG_DSN": credentials["dsn"]},
                               stdout=log, stderr=log, check=True, timeout=60)
                test_env = environment | {
                    "UDB_LIVE_SDK_TESTS": "1",
                    "UDB_LIVE_CDC_TESTS": "1",
                    "UDB_GRPC_TARGET": "127.0.0.1:50051",
                    "UDB_AUTH_GRPC_TARGET": "127.0.0.1:50061",
                    "UDB_LIVE_USERNAME": credentials["username"],
                    "UDB_LIVE_PASSWORD": credentials["password"],
                    "UDB_LIVE_TENANT": credentials["tenant_id"],
                    "UDB_LIVE_PROJECT": "default",
                    "UDB_LIVE_PEER_USERNAME": peer_username,
                    "UDB_LIVE_PEER_PASSWORD": peer_password,
                    "UDB_LIVE_PEER_TENANT": peer_tenant,
                }
                phase(f"persisted-row-{iteration + 1}")
                restart_env = test_env | {
                    "UDB_EMBEDDED_RESTART_TESTS": "1",
                    "UDB_EMBEDDED_RESTART_ROW_ID": persisted_row_id,
                    "UDB_EMBEDDED_RESTART_PHASE": "seed" if iteration == 0 else "verify",
                }
                run_go_contract(repo, restart_env, proof, "TestEmbeddedRestartPreservesCommittedRow",
                                f"persisted-row-{iteration + 1}", 180)
                phase(f"shared-table-{iteration + 1}")
                run_go_contract(repo, test_env, proof, "TestLiveBrokerMeetsTheTableContract",
                                f"shared-table-{iteration + 1}", 660)
                phase(f"shutdown-{iteration + 1}")
                subprocess.run([str(binary), "dev", "down", "--embedded"], cwd=project,
                               env=environment, stdout=log, stderr=log, check=True, timeout=30)
                assert_stopped(process)
                print(f"embedded dev startup/restart {iteration + 1}: shared Go contract passed, processes stopped")
            finally:
                if process.poll() is None:
                    subprocess.run([str(binary), "dev", "down", "--embedded"], cwd=project,
                                   env=environment, stdout=log, stderr=log, timeout=30)
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
        phase("interrupted-startup")
        interrupted_startup(command, environment, log)
        helper_env = environment | {
            "UDB_EMBEDDED_SDK_TESTS": "1",
            "UDB_TEST_BINARY": str(binary),
        }
        phase("sdk-start-pair")
        run_go_contract(repo, helper_env, proof, "TestEmbeddedBrokerMeetsTheTableContract",
                        "sdk-start-pair", 750)
        print("udbtest.Start: the shared fake/live contract passed through the real embedded helper")
        phase("complete")


if __name__ == "__main__":
    if sys.argv[1:] == ["--selftest"]:
        receipt_selftest()
        raise SystemExit(0)
    receipt = {"status": "running", "current_phase": "prepare", "completed_phases": []}
    try:
        main(receipt)
        receipt["status"] = "success"
    except Exception as error:
        # CalledProcessError includes full argv (including the bootstrap password)
        # in its text. Never print it or copy the broker log into public artifacts.
        receipt["status"] = "failure"
        receipt["error_type"] = type(error).__name__
        returncode = getattr(error, "returncode", None)
        if isinstance(returncode, int):
            receipt["exit_code"] = returncode
        print(f"embedded dev failed during {receipt['current_phase']}: {receipt['error_type']}")
    finally:
        public_log = Path(os.environ["RUNNER_TEMP"]) / "udb-embedded-dev.log"
        public_log.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    if receipt["status"] != "success":
        raise SystemExit(1)
