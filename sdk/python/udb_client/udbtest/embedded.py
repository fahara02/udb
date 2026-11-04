"""The real broker with its embedded PostgreSQL, for tests the fake cannot answer."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import uuid

from ..enterprise import EnterpriseSession, connect_enterprise

# The broker's shortest shared authz snapshot reload period.
_AUTHZ_RELOAD_S = 5.0
_STARTUP_TIMEOUT_S = 600.0
_LOGIN_TIMEOUT_S = 30.0
_STOP_TIMEOUT_S = 45.0
_INHERITED = ("UDB_HOME", "UDB_POSTGRES_BINARIES_URL", "UDB_EMBEDDED_PG_ARCHIVE")
_LISTENERS = ("UDB_GRPC_ADDR", "UDB_AUTH_GRPC_ADDR", "UDB_WEBRTC_GRPC_ADDR", "UDB_HTTP_ADDR", "UDB_METRICS_ADDR")


class EmbeddedBroker:
    """A running embedded broker. :attr:`client` is its tenant administrator, and
    :attr:`peer` a second tenant's administrator when started with ``start_pair``. Use
    it as a context manager, or call :meth:`close`; that stops the clients, the broker
    and PostgreSQL. Bootstrap credentials stay in its private project directory.
    """

    def __init__(self, proto_root: str, policy_file: str, with_peer: bool):
        self.client: EnterpriseSession | None = None
        self.peer: EnterpriseSession | None = None
        self._project = tempfile.TemporaryDirectory(prefix="udbtest-")
        self._closed = False
        project = Path(self._project.name)
        self._binary = _resolve_binary()
        if not proto_root:
            proto_root = str(project / "proto")
            os.mkdir(proto_root, 0o700)
        proto_root = os.path.abspath(proto_root)
        # Avoid inheriting another project's listeners, database DSNs or credentials.
        # Acquisition settings alone may be shared between isolated test brokers.
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.upper().startswith("UDB_") and key.upper() != "DATABASE_URL"
        }
        for key in _INHERITED:
            if key in os.environ:
                environment[key] = os.environ[key]
        environment["UDB_PROTO_ROOT"] = proto_root
        pg_address, *addresses = _reserve_ports(1 + len(_LISTENERS))
        pg_port = pg_address.rsplit(":", 1)[1]
        listeners = dict(zip(_LISTENERS, addresses))
        environment.update(listeners)
        self._environment = environment
        self._pg_address = pg_address
        self._listeners = listeners
        self._log_path = project / "broker.log"
        self._log = open(self._log_path, "xb")
        os.chmod(self._log_path, 0o600)
        self._process = subprocess.Popen(
            [self._binary, "dev", "up", "--embedded", "--pg-port", pg_port],
            cwd=project,
            env=environment,
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )
        try:
            credentials = self._wait_for_bootstrap(project)
            peer_login = self._provision_peer(project, credentials) if with_peer else None
            if policy_file:
                tenants = [credentials["tenant_id"]]
                if peer_login is not None:
                    tenants.append(peer_login["tenant"])
                for tenant in tenants:
                    self._apply_policy(project, os.path.abspath(policy_file), tenant, credentials["dsn"])
            # The admin, the peer and the policies are written offline, straight to
            # PostgreSQL, and the broker sees them only through its shared authz
            # snapshot. A login before the next reload gets a token with no roles or
            # scopes, so wait for two reloads before the first one.
            time.sleep(2 * _AUTHZ_RELOAD_S + 1)
            self.client = self._connect(credentials["username"], credentials["password"], credentials["tenant_id"])
            if peer_login is not None:
                self.peer = self._connect(peer_login["username"], peer_login["password"], peer_login["tenant"])
        except BaseException:
            self.close()
            raise

    def _wait_for_bootstrap(self, project: Path) -> dict[str, str]:
        marker = project / ".udb" / "dev" / "bootstrap.json"
        deadline = time.monotonic() + _STARTUP_TIMEOUT_S
        while True:
            try:
                credentials = json.loads(marker.read_text(encoding="utf-8"))
                if credentials.get("password") and credentials.get("tenant_id"):
                    return credentials
            except (OSError, ValueError):
                pass
            exit_code = self._process.poll()
            if exit_code is not None:
                raise RuntimeError(
                    f"udbtest.start: broker exited during startup: {exit_code} (private log: {self._log_path})"
                )
            if time.monotonic() > deadline:
                raise RuntimeError(f"udbtest.start: startup exceeded 10 minutes (private log: {self._log_path})")
            time.sleep(0.2)

    def _provision_peer(self, project: Path, credentials: dict[str, str]) -> dict[str, str]:
        username = f"udbtest-peer-{uuid.uuid4()}"
        password = f"UdbTestPeer#{uuid.uuid4()}"
        tenant = str(uuid.uuid4())
        # The peer is created offline against the dev cluster, so it needs the secrets
        # the dev broker hashes passwords and encrypts with.
        environment = dict(self._environment)
        environment["UDB_PG_DSN"] = credentials["dsn"]
        for name, file_name in (("UDB_SESSION_HASH_SECRET", "session-secret"), ("UDB_ENCRYPTION_KEY", "encryption-key")):
            environment[name] = (project / ".udb" / "dev" / file_name).read_text(encoding="utf-8").strip()
        self._run(
            [self._binary, "auth", "bootstrap", "user", "--username", username,
             "--email", f"{username}@udbtest.invalid", "--password", password,
             "--tenant", tenant, "--project", "default"],
            project, environment, "provision peer tenant",
        )
        return {"username": username, "password": password, "tenant": tenant}

    def _apply_policy(self, project: Path, policy_file: str, tenant: str, dsn: str) -> None:
        environment = dict(self._environment)
        environment["UDB_PG_DSN"] = dsn
        self._run(
            [self._binary, "policy", "apply", "-f", policy_file, "--tenant", tenant],
            project, environment, f"apply policy file for tenant {tenant}",
        )

    def _run(self, command: list[str], project: Path, environment: dict[str, str], what: str) -> None:
        try:
            subprocess.run(
                command, cwd=project, env=environment, stdout=self._log, stderr=subprocess.STDOUT,
                timeout=60, check=True,
            )
        except (subprocess.SubprocessError, OSError) as error:
            raise RuntimeError(f"udbtest.start: {what}: {type(error).__name__} (private log: {self._log_path})") from None

    def _connect(self, username: str, password: str, tenant: str) -> EnterpriseSession:
        return connect_enterprise(
            target=self._listeners["UDB_GRPC_ADDR"],
            auth_target=self._listeners["UDB_AUTH_GRPC_ADDR"],
            username=username,
            password=password,
            tenant_code=tenant,
            project_id="default",
            purpose="udbtest.embedded",
            deadline=_LOGIN_TIMEOUT_S,
        )

    def close(self) -> None:
        """Stop the clients, the broker and PostgreSQL, and remove the project."""
        if self._closed:
            return
        self._closed = True
        for session in (self.client, self.peer):
            if session is not None:
                session.close()
        problem = ""
        project = self._project.name
        try:
            subprocess.run(
                [self._binary, "dev", "down", "--embedded"], cwd=project, env=self._environment,
                stdout=self._log, stderr=subprocess.STDOUT, timeout=_STOP_TIMEOUT_S, check=True,
            )
        except (subprocess.SubprocessError, OSError) as error:
            problem = f"stop embedded broker: {type(error).__name__}"
        try:
            self._process.wait(timeout=_STOP_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait()
            problem = problem or "embedded broker did not stop within 45 seconds"
        self._log.close()
        for address in (self._pg_address, self._listeners["UDB_GRPC_ADDR"], self._listeners["UDB_AUTH_GRPC_ADDR"]):
            host, port = address.rsplit(":", 1)
            try:
                socket.create_connection((host, int(port)), timeout=1).close()
            except OSError:
                continue
            problem = problem or f"shutdown left {address} listening"
        if problem:
            # Keep the private log for diagnosis instead of deleting it with the project.
            self._project.cleanup = lambda: None  # type: ignore[method-assign]
            raise RuntimeError(f"udbtest.start: {problem} (private log: {self._log_path})")
        self._project.cleanup()

    def __enter__(self) -> "EmbeddedBroker":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _resolve_binary() -> str:
    binary = os.environ.get("UDB_TEST_BINARY") or shutil.which("udb")
    if not binary:
        raise RuntimeError("udbtest.start: install an embedded-capable udb or set UDB_TEST_BINARY")
    return os.path.abspath(binary)


def _reserve_ports(count: int) -> list[str]:
    # Hold every reservation until all are selected, so listeners in the same child
    # cannot receive the same ephemeral port.
    held = []
    try:
        for _ in range(count):
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            held.append(listener)
        return ["127.0.0.1:%d" % listener.getsockname()[1] for listener in held]
    finally:
        for listener in held:
            listener.close()


def start(proto_root: str = "", policy_file: str = "") -> EmbeddedBroker:
    """Run the installed real broker with its embedded PostgreSQL and log in as the
    bootstrapped test administrator. ``UDB_TEST_BINARY`` selects a broker binary;
    otherwise ``udb`` on PATH is used. It must support ``dev up --embedded``. This never
    builds a broker or runs Docker.

    ``proto_root`` names the application's proto directory; empty creates an empty
    project using the broker's native models. ``policy_file`` is applied through the
    real ``udb policy apply`` for the bootstrapped tenant. Each call owns a temporary
    project and separate loopback ports. PostgreSQL downloads use the ordinary shared
    cache or ``UDB_HOME``, ``UDB_POSTGRES_BINARIES_URL`` and ``UDB_EMBEDDED_PG_ARCHIVE``.

    Use the result as a context manager::

        with udbtest.start() as broker:
            run_table(lambda tenant: broker.client)
    """
    return EmbeddedBroker(proto_root, policy_file, False)


def start_pair(proto_root: str = "", policy_file: str = "") -> EmbeddedBroker:
    """:func:`start` with two independently provisioned tenant administrators, for the
    full shared isolation contract."""
    return EmbeddedBroker(proto_root, policy_file, True)
