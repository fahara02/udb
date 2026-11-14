"""UdbProject.for_tenant (parity with the Go SDK's tenant_exchange_test.go).

A service connected to its home tenant gets a project in a member tenant whose
short-lived exchanged token (no refresh token) is exchanged again before it
expires, and at once when the broker refuses it; while that re-exchange is
refused, calls fail locally instead of resending the refused token.
"""

from __future__ import annotations

import threading
import time
from concurrent import futures
from typing import Any

import grpc
import pytest

from udb_client import UdbConfig, UdbProject
from udb_client._reconnect import status_code

authn_pb = pytest.importorskip("udb.core.authn.services.v1.core_pb2")
authn_service_pb = pytest.importorskip("udb.core.authn.services.v1.authn_service_pb2")
authn_grpc = pytest.importorskip("udb.core.authn.services.v1.authn_service_pb2_grpc")
cache_pb = pytest.importorskip("udb.core.cache.services.v1.cache_service_pb2")
cache_grpc = pytest.importorskip("udb.core.cache.services.v1.cache_service_pb2_grpc")

HOME_TENANT = "11111111-1111-4111-8111-111111111111"
MEMBER_TENANT = "22222222-2222-4222-8222-222222222222"
MEMBER_PROJECT = "33333333-3333-4333-8333-333333333333"
SERVICE_USER = "44444444-4444-4444-8444-444444444444"


def _principal(tenant: str, project: str) -> Any:
    return authn_pb.Principal(
        principal_id="svc", subject="svc", tenant_id=tenant, project_id=project, user_id=SERVICE_USER
    )


class ExchangeBroker(authn_grpc.AuthnServiceServicer, cache_grpc.CacheServiceServicer):
    """Issues a home bearer for an API key, mints member-tenant tokens for that
    home bearer, and serves an Increment that accepts only the newest member token."""

    def __init__(self, ttl: float = 3600.0) -> None:
        self._lock = threading.Lock()
        self.ttl = ttl
        self.home_bearer = ""
        self.member_token = ""
        self.exchanges = 0
        self.exchange_attempts = 0
        self.exchange_tenants: list[list[str]] = []
        self.increment_tenants: list[list[str]] = []
        self.refused = 0
        self.refuse_exchange = False
        self.wrong_tenant = ""
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        authn_grpc.add_AuthnServiceServicer_to_server(self, self._server)
        cache_grpc.add_CacheServiceServicer_to_server(self, self._server)
        self.port = self._server.add_insecure_port("127.0.0.1:0")
        self._server.start()

    @property
    def target(self) -> str:
        return f"127.0.0.1:{self.port}"

    def stop(self) -> None:
        self._server.stop(None).wait()

    def Authenticate(self, request: Any, context: Any) -> Any:
        with self._lock:
            if request.api_key == "svc-key":
                self.home_bearer = "home-key"
                return authn_pb.AuthnResponse(
                    access_token=self.home_bearer,
                    expires_at_unix=int(time.time()) + 3600,
                    principal=_principal(HOME_TENANT, ""),
                )
            ttl = self.ttl
        if request.bearer_token.startswith("member-"):
            return authn_pb.AuthnResponse(
                expires_at_unix=int(time.time() + ttl), principal=_principal(MEMBER_TENANT, MEMBER_PROJECT)
            )
        context.abort(grpc.StatusCode.UNAUTHENTICATED, "unknown credential")

    def ExchangeServiceTokenForTenant(self, request: Any, context: Any) -> Any:
        headers = context.invocation_metadata()
        with self._lock:
            self.exchange_attempts += 1
            self.exchange_tenants.append([v for k, v in headers if k == "x-tenant-id"])
            authorized = dict(headers).get("authorization") == f"Bearer {self.home_bearer}"
            refuse = self.refuse_exchange
            if authorized and not refuse:
                self.exchanges += 1
                self.member_token = f"member-{self.exchanges}"
            token, ttl, wrong = self.member_token, self.ttl, self.wrong_tenant
        if not authorized:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "the exchange requires the home bearer")
        if refuse:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "no active membership")
        return authn_service_pb.ExchangeServiceTokenForTenantResponse(
            access_token=token,
            expires_at_unix=int(time.time() + ttl),
            tenant_id=wrong or request.target_tenant_id,
            project_id=MEMBER_PROJECT,
            membership_id="membership-1",
        )

    def Increment(self, request: Any, context: Any) -> Any:
        headers = context.invocation_metadata()
        with self._lock:
            self.increment_tenants.append([v for k, v in headers if k == "x-tenant-id"])
            accepted = dict(headers).get("authorization") == f"Bearer {self.member_token}"
            if not accepted:
                self.refused += 1
        if not accepted:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid credential")
        return cache_pb.IncrementResponse(value=1)


def _home(broker: ExchangeBroker) -> UdbProject:
    return UdbProject(UdbConfig(target=broker.target, tenant_id=HOME_TENANT, api_key="svc-key", deadline=2.0))


def _increment(project: UdbProject) -> Any:
    stub = cache_grpc.CacheServiceStub(project._ctl_channel)
    request = cache_pb.IncrementRequest(namespace="limits", key="gateway", delta=1)
    return stub.Increment(request, metadata=project.metadata.to_grpc_metadata(), timeout=5)


@pytest.fixture
def broker():
    b = ExchangeBroker()
    try:
        yield b
    finally:
        b.stop()


def test_for_tenant_exchanges_the_home_bearer_and_keeps_the_member_token_fresh(broker) -> None:
    broker.ttl = 3.0
    home = _home(broker)
    member = None
    try:
        member = home.for_tenant(MEMBER_TENANT, MEMBER_PROJECT)
        assert member.metadata.tenant_id == MEMBER_TENANT
        assert member.metadata.project_id == MEMBER_PROJECT
        assert member.metadata.user_id == SERVICE_USER
        assert member.principal().tenant_id == MEMBER_TENANT
        assert member.bearer_expires_at() is not None
        assert home.metadata.tenant_id == HOME_TENANT, "for_tenant changed the home identity"

        for _ in range(3):
            _increment(member)
        assert broker.exchanges == 1, "three calls on a fresh token exchanged again"
        assert broker.increment_tenants[0] == [MEMBER_TENANT]
        assert broker.exchange_tenants[0] == [HOME_TENANT]

        # A 3s token renews at 80% of its life: a re-exchange before it expires.
        deadline = time.monotonic() + 6
        while broker.exchanges < 2:
            assert time.monotonic() < deadline, "the member token was not exchanged again before it expired"
            time.sleep(0.05)
        _increment(member)
        assert member.credential_error() is None
        assert broker.refused == 0, "a member call carried a stale token"
    finally:
        if member is not None:
            member.close()
        home.close()


def test_for_tenant_refused_token_fails_closed_while_the_reexchange_backs_off(broker) -> None:
    home = _home(broker)
    member = None
    try:
        member = home.for_tenant(MEMBER_TENANT)
        with broker._lock:
            broker.member_token = "revoked"
            broker.refuse_exchange = True
        with pytest.raises(Exception) as first:
            _increment(member)
        # The renewal's own refusal is kept: the membership is gone.
        assert status_code(first.value) in (
            grpc.StatusCode.UNAUTHENTICATED,
            grpc.StatusCode.PERMISSION_DENIED,
        ), first.value
        for _ in range(20):
            with pytest.raises(Exception) as later:
                _increment(member)
            assert status_code(later.value) == grpc.StatusCode.UNAUTHENTICATED, later.value
        assert member.credential_error() is not None
        assert broker.refused == 1, "the refused member token was resent"
        assert broker.exchange_attempts <= 4, f"{broker.exchange_attempts} exchange attempts: one per call"

        with broker._lock:
            broker.refuse_exchange = False
        deadline = time.monotonic() + 10
        while member.credential_error() is not None:
            assert time.monotonic() < deadline, f"no recovery: {member.credential_error()}"
            time.sleep(0.05)
        _increment(member)
    finally:
        if member is not None:
            member.close()
        home.close()


def test_for_tenant_refuses_another_tenant_and_a_non_canonical_id(broker) -> None:
    broker.wrong_tenant = "55555555-5555-4555-8555-555555555555"
    home = _home(broker)
    try:
        with pytest.raises(Exception, match="returned a token for tenant"):
            home.for_tenant(MEMBER_TENANT)
        for bad in ("", "acme", "ABCDEF01-2345-4678-89AB-CDEF01234567"):
            with pytest.raises(Exception, match="canonical"):
                home.for_tenant(bad)
        assert broker.exchange_attempts == 1, "a non-canonical tenant reached the broker"
    finally:
        home.close()
