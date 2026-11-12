"""Recovery after a broker restart (parity with the Go SDK's reconnect_test.go).

An in-process broker is stopped and started again on the same port; every start
is a new generation that has lost the previous one's sessions, so the bearers and
refresh tokens it issued are refused. The same client must keep working without
being recreated, a credential the restarted broker refuses must fail closed
without a sign-in per call, and a bearer the client does not own must never be
renewed or resent.
"""

from __future__ import annotations

import threading
import time
from concurrent import futures
from typing import Any, Callable

import grpc
import pytest

from udb_client import UdbConfig, UdbProject
from udb_client._reconnect import status_code
from udb_client.enterprise import EnterpriseConfig, connect_enterprise

authn_pb = pytest.importorskip("udb.core.authn.services.v1.core_pb2")
authn_grpc = pytest.importorskip("udb.core.authn.services.v1.authn_service_pb2_grpc")
cache_pb = pytest.importorskip("udb.core.cache.services.v1.cache_service_pb2")
cache_grpc = pytest.importorskip("udb.core.cache.services.v1.cache_service_pb2_grpc")

TENANT = "11111111-1111-4111-8111-111111111111"
PROJECT = "22222222-2222-4222-8222-222222222222"


def _principal() -> Any:
    return authn_pb.Principal(
        principal_id="principal-1",
        subject="gateway",
        user_id="user-1",
        tenant_id=TENANT,
        project_id=PROJECT,
        scopes=["udb:cache:increment"],
    )


class RestartBroker(authn_grpc.AuthnServiceServicer, cache_grpc.CacheServiceServicer):
    """An in-process broker that can be stopped and started again on the same port."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._server: grpc.Server | None = None
        self.port = 0
        self.gen = 0
        self.logins = 0
        self.exchanges = 0
        self.refreshes = 0
        self.increments = 0
        self.refused_bearers: list[str] = []
        #: Makes Login, RefreshToken and the key exchange refuse.
        self.refuse_sign_in = False
        self.start()

    @property
    def target(self) -> str:
        return f"127.0.0.1:{self.port}"

    def start(self) -> None:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        authn_grpc.add_AuthnServiceServicer_to_server(self, server)
        cache_grpc.add_CacheServiceServicer_to_server(self, server)
        deadline = time.monotonic() + 5
        while True:
            port = server.add_insecure_port(f"127.0.0.1:{self.port}")
            if port or time.monotonic() > deadline:
                break
            time.sleep(0.05)
        assert port, f"could not listen on 127.0.0.1:{self.port} again"
        self.port = port
        server.start()
        self._server = server

    def stop(self) -> None:
        """Take the broker down: every connection is closed and nothing listens."""
        server, self._server = self._server, None
        if server is not None:
            server.stop(None).wait()

    def restart(self) -> None:
        """Bring the broker back on the same port as a new generation."""
        with self._lock:
            self.gen += 1
        self.start()

    def _issued(self, kind: str, n: int) -> str:
        return f"g{self.gen}-{kind}-{n}"

    def _current(self, token: str) -> bool:
        return token.startswith(f"g{self.gen}-")

    # ── AuthnService ──────────────────────────────────────────────────────
    def Login(self, request: Any, context: Any) -> Any:
        with self._lock:
            self.logins += 1
            n, refuse = self.logins, self.refuse_sign_in
        if refuse:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid credential")
        return authn_pb.LoginResponse(
            access_token=self._issued("access", n),
            refresh_token=self._issued("refresh", n),
            session_id=self._issued("session", n),
            access_token_expires_in=3600,
        )

    def Authenticate(self, request: Any, context: Any) -> Any:
        expires = int(time.time()) + 3600
        if request.api_key:
            with self._lock:
                self.exchanges += 1
                n, refuse = self.exchanges, self.refuse_sign_in
            if refuse:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid API key")
            return authn_pb.AuthnResponse(
                access_token=self._issued("key", n), expires_at_unix=expires, principal=_principal()
            )
        return authn_pb.AuthnResponse(expires_at_unix=expires, principal=_principal())

    def RefreshToken(self, request: Any, context: Any) -> Any:
        with self._lock:
            self.refreshes += 1
            n, refuse = self.refreshes, self.refuse_sign_in
        if refuse or not self._current(request.refresh_token):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "refresh token is not known")
        return authn_pb.RefreshTokenResponse(
            access_token=self._issued("refreshed", n),
            refresh_token=self._issued("refresh-next", n),
            access_token_expires_in=3600,
        )

    # ── CacheService ──────────────────────────────────────────────────────
    def Increment(self, request: Any, context: Any) -> Any:
        """The request-limit counter of the incident: a mutation that is neither
        read-only nor replay-safe. Authentication runs before it applies."""
        headers = dict(context.invocation_metadata())
        bearer = headers.get("authorization", "").removeprefix("Bearer ")
        with self._lock:
            if not self._current(bearer):
                self.refused_bearers.append(bearer)
                refused = True
            else:
                self.increments += 1
                refused = False
                value = self.increments
        if refused:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid credential")
        return cache_pb.IncrementResponse(value=value)


@pytest.fixture
def broker():
    b = RestartBroker()
    try:
        yield b
    finally:
        b.stop()


def _increment(channel: Any, metadata: Callable[[], Any]) -> Any:
    stub = cache_grpc.CacheServiceStub(channel)
    request = cache_pb.IncrementRequest(namespace="limits", key="gateway", delta=1)
    return stub.Increment(request, metadata=metadata().to_grpc_metadata(), timeout=5)


def _increment_after_restart(channel: Any, metadata: Callable[[], Any], within: float = 10.0) -> BaseException | None:
    """Call Increment until the restarted broker answers. While the channel
    reconnects the call may fail UNAVAILABLE (a mutation is never repeated on its
    own); any other outcome is returned at once."""
    deadline = time.monotonic() + within
    while True:
        try:
            _increment(channel, metadata)
            return None
        except Exception as exc:  # noqa: BLE001 - classified below
            if status_code(exc) != grpc.StatusCode.UNAVAILABLE:
                return exc
            if time.monotonic() > deadline:
                raise AssertionError(f"broker did not become reachable again: {exc}") from exc
            time.sleep(0.05)


def _enterprise(broker: RestartBroker) -> Any:
    return connect_enterprise(
        EnterpriseConfig(
            target=broker.target, username="gateway", password="secret", tenant_code="acme", deadline=2.0
        )
    )


def _service_account(broker: RestartBroker) -> UdbProject:
    return UdbProject(UdbConfig(target=broker.target, tenant_id=TENANT, api_key="svc-key", deadline=2.0))


def test_broker_restart_enterprise_session_recovers_without_recreate(broker) -> None:
    with _enterprise(broker) as sess:
        _increment(sess.native_channel, sess.native_metadata)
        before = sess.bearer()

        broker.stop()
        with pytest.raises(Exception) as down:
            _increment(sess.native_channel, sess.native_metadata)
        assert status_code(down.value) == grpc.StatusCode.UNAVAILABLE
        # Let the channel fail a few dials, growing its reconnect backoff.
        time.sleep(1.5)
        broker.restart()

        assert _increment_after_restart(sess.native_channel, sess.native_metadata) is None
        assert broker.logins == 2, "the session signs in again after its refresh token was refused"
        assert broker.increments == 2, "the refused increment is neither applied nor repeated twice"
        assert ["Bearer " + b for b in broker.refused_bearers] == [before]
        after = sess.bearer()
        assert after != before and after.startswith("Bearer g1-"), after
        assert sess.refresh_error() is None


def test_broker_restart_service_account_reexchanges_key(broker) -> None:
    project = _service_account(broker)
    try:
        metadata = lambda: project.metadata  # noqa: E731
        _increment(project._ctl_channel, metadata)
        broker.stop()
        time.sleep(0.5)
        broker.restart()

        assert _increment_after_restart(project._ctl_channel, metadata) is None
        assert (broker.exchanges, broker.increments, len(broker.refused_bearers)) == (2, 2, 1)
        assert project.credential_error() is None
    finally:
        project.close()


def test_connect_enterprise_waits_for_restarting_broker(broker) -> None:
    broker.stop()
    back = threading.Timer(0.2, broker.restart)
    back.start()
    try:
        _enterprise(broker).close()
    finally:
        back.join()


def test_broker_restart_refused_credential_fails_closed(broker) -> None:
    """A broker that, after its restart, no longer accepts the session's credential
    gets one sign-in attempt and the call fails UNAUTHENTICATED; the refused call is
    never applied and never resent with the refused bearer."""
    with _enterprise(broker) as sess:
        broker.stop()
        broker.refuse_sign_in = True
        broker.restart()

        err = _increment_after_restart(sess.native_channel, sess.native_metadata)
        assert status_code(err) == grpc.StatusCode.UNAUTHENTICATED, err
        # Later calls are refused locally while the session is unauthorized; they
        # never turn into one sign-in attempt per call.
        for _ in range(20):
            with pytest.raises(Exception) as refused:
                _increment(sess.native_channel, sess.native_metadata)
            assert status_code(refused.value) == grpc.StatusCode.UNAUTHENTICATED
        assert sess.refresh_error() is not None
        assert broker.increments == 0
        assert len(broker.refused_bearers) == 1, "the refused bearer was resent"
        assert 1 <= broker.logins - 1 <= 2, f"{broker.logins - 1} sign-in attempts after the refusal"


def test_broker_restart_refused_key_fails_closed(broker) -> None:
    project = _service_account(broker)
    try:
        metadata = lambda: project.metadata  # noqa: E731
        broker.stop()
        broker.refuse_sign_in = True
        broker.restart()

        err = _increment_after_restart(project._ctl_channel, metadata)
        assert status_code(err) == grpc.StatusCode.UNAUTHENTICATED, err
        for _ in range(20):
            with pytest.raises(Exception) as refused:
                _increment(project._ctl_channel, metadata)
            assert status_code(refused.value) == grpc.StatusCode.UNAUTHENTICATED
        assert project.credential_error() is not None
        assert broker.increments == 0
        assert len(broker.refused_bearers) == 1, "the refused bearer was resent"
        assert 1 <= broker.exchanges - 1 <= 2, f"{broker.exchanges - 1} key exchanges after the refusal"
    finally:
        project.close()


def test_broker_refusal_of_unowned_bearer_is_not_repeated(broker) -> None:
    """A bearer the client does not own (a configured bearer, a delegated as_user
    bearer) is never renewed or resent after a refusal."""
    static = UdbProject(UdbConfig(target=broker.target, tenant_id=TENANT, bearer_token="g0-static"))
    try:
        with _enterprise(broker) as sess:
            broker.stop()
            broker.restart()

            err = _increment_after_restart(static._ctl_channel, lambda: static.metadata)
            assert status_code(err) == grpc.StatusCode.UNAUTHENTICATED, err
            err = _increment_after_restart(sess.native_channel, lambda: sess.as_user("g0-person"))
            assert status_code(err) == grpc.StatusCode.UNAUTHENTICATED, err
            assert (broker.logins, broker.increments, len(broker.refused_bearers)) == (1, 0, 2)
    finally:
        static.close()
