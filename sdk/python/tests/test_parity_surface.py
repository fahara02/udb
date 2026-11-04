"""The session, pool, environment, builder and adapter surface, on the wire.

Everything here talks over a real local gRPC server, so headers and refusals are
checked as the broker would receive them. The broker's own verification, tenant
checks and revocation are proven by ``test_live_parity_surface.py``.
"""

from __future__ import annotations

import asyncio
import copy
import threading
import time
from concurrent import futures
from datetime import datetime, timezone

import grpc
import grpc.aio
import pytest
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.timestamp_pb2 import Timestamp

from udb.core.authn.services.v1 import authn_service_pb2_grpc as authn_grpc
from udb.core.authn.services.v1 import core_pb2 as authn_pb
from udb.core.common.v1 import db_pb2
from udb.core.config.services.v1 import config_service_pb2 as config_pb
from udb.core.config.services.v1 import config_service_pb2_grpc as config_grpc
from udb.core.livequery.services.v1 import livequery_service_pb2 as lq_pb
from udb.core.livequery.services.v1 import livequery_service_pb2_grpc as lq_grpc
from udb.core.lock.services.v1 import lock_service_pb2 as lock_pb
from udb.core.lock.services.v1 import lock_service_pb2_grpc as lock_grpc
from udb.core.storage.entity.v1 import file_pb2
from udb.core.storage.services.v1 import storage_service_pb2 as storage_pb
from udb.core.storage.services.v1 import storage_service_pb2_grpc as storage_grpc
from udb.entity.v1 import error_pb2, stores_pb2, types_pb2, vector_pb2
from udb.sdk.live.v1 import sdk_live_pb2
from udb.services.v1 import data_broker_pb2_grpc as data_grpc
from udb_client import (
    EnterpriseConfig,
    EnterpriseSession,
    Expect,
    Metadata,
    TenantSessionPool,
    TenantState,
    UdbAsyncProject,
    UdbConfig,
    UdbConfigurationError,
    UdbCredentialBindingError,
    UdbCredentialError,
    UdbError,
    UdbNoDeltaFeed,
    UdbProject,
    UdbRpcError,
    UdbTenantError,
    api_key_tenant_pool,
    apply_column_defaults,
    config_from_env,
    connect_enterprise,
    connect_enterprise_async,
    connect_from_env,
    connect_from_env_async,
    graph_label,
    lq_eq,
    lq_in,
    lq_is_null,
    max_len,
    topic,
    unique_keys,
)
from udb_client.adapters import (
    UdbAsyncServerInterceptor,
    UdbServerInterceptor,
    correlation_id,
    from_context,
    metadata_from_context,
    tenant_id as request_tenant_id,
)
from udb_client.exceptions import UdbNotFound

TENANT = "11111111-1111-4111-8111-111111111111"
PROJECT = "22222222-2222-4222-8222-222222222222"
USER = "33333333-3333-4333-8333-333333333333"
OTHER = "44444444-4444-4444-8444-444444444444"
TIMEOUT = 5.0


def _principal(**overrides) -> authn_pb.Principal:
    values = dict(
        principal_id=USER,
        subject=USER,
        user_id=USER,
        service_identity="svc",
        tenant_id=TENANT,
        project_id=PROJECT,
        scopes=["udb:read", "udb:write"],
    )
    values.update(overrides)
    return authn_pb.Principal(**values)


class Authn(authn_grpc.AuthnServiceServicer):
    """Refresh tokens are single-use, as on the broker."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.logins = 0
        self.refreshes = 0
        self.exchanges = 0
        self.minted = 0
        self.live_refresh = ""
        self.presented: list[str] = []
        self.ttl = 900
        self.principal = _principal()
        self.refuse_refresh = False
        self.refuse_login = False
        self.refresh_delay = 0.0
        self.mfa = False
        self.login_requests: list[authn_pb.LoginRequest] = []
        self.login_headers: list[dict[str, str]] = []
        self.key_principals: dict[str, authn_pb.Principal] = {}
        self.validated: list[str] = []
        self.valid = True
        self.headers: list[dict[str, str]] = []

    def _mint(self) -> tuple[str, str]:
        self.minted += 1
        self.live_refresh = f"refresh-{self.minted}"
        return f"access-{self.minted}", self.live_refresh

    def Login(self, request, context):
        with self.lock:
            self.logins += 1
            self.login_requests.append(request)
            self.login_headers.append(dict(context.invocation_metadata()))
            if self.mfa:
                return authn_pb.LoginResponse(mfa_required=True)
            if self.refuse_login or request.password != "correct":
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid credential")
            access, refresh = self._mint()
            return authn_pb.LoginResponse(
                access_token=access,
                refresh_token=refresh,
                session_id=f"session-{self.minted}",
                access_token_expires_in=self.ttl,
            )

    def Authenticate(self, request, context):
        if request.api_key:
            with self.lock:
                self.exchanges += 1
            principal = self.key_principals.get(request.api_key)
            if principal is None:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid key")
            return authn_pb.AuthnResponse(
                access_token=f"key-bearer-{request.api_key}-{self.exchanges}",
                expires_at_unix=int(time.time()) + self.ttl,
                principal=principal,
            )
        return authn_pb.AuthnResponse(
            principal=self.principal, expires_at_unix=int(time.time()) + self.ttl
        )

    def RefreshToken(self, request, context):
        time.sleep(self.refresh_delay)
        with self.lock:
            self.refreshes += 1
            self.presented.append(request.refresh_token)
            if self.refuse_refresh or request.refresh_token != self.live_refresh:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid credential")
            access, refresh = self._mint()
            return authn_pb.RefreshTokenResponse(
                access_token=access, refresh_token=refresh, access_token_expires_in=self.ttl
            )

    def ValidateToken(self, request, context):
        self.validated.append(request.token)
        self.headers.append(dict(context.invocation_metadata()))
        expires = Timestamp(seconds=2_000_000_000)
        if not self.valid:
            return authn_pb.ValidateTokenResponse(valid=False)
        return authn_pb.ValidateTokenResponse(
            valid=True,
            user_id=USER,
            tenant_id=TENANT,
            project_id=PROJECT,
            session_public_id="sesspub_1",
            scopes=["udb:read"],
            roles=["reader"],
            expires_at=expires,
            principal=_principal(),
        )


class Data(data_grpc.DataBrokerServicer):
    def __init__(self) -> None:
        self.headers: list[dict[str, str]] = []
        self.calls: list[tuple[str, object]] = []
        self.run_id = "run-1"
        self.approval_token = "approval-1"
        self.points: list[vector_pb2.VectorPoint] = []

    def Select(self, request, context):
        self.headers.append(dict(context.invocation_metadata()))
        return types_pb2.RecordSet()

    def PlanMigration(self, request, context):
        self.calls.append(("plan", request))
        return types_pb2.MigrationPlanResponse(run_id=self.run_id)

    def ApproveMigrationPlan(self, request, context):
        self.calls.append(("approve", request))
        return types_pb2.MigrationStatusResponse(
            run_id=request.run_id, approval_token=self.approval_token
        )

    def ApplyMigration(self, request, context):
        self.calls.append(("apply", request))
        if request.approval_token != self.approval_token:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "wrong token")
        return types_pb2.MigrationStatusResponse(run_id=request.run_id, state="APPLIED")

    def GraphQuery(self, request, context):
        self.calls.append(("graph", request))
        record = types_pb2.GraphResultSet().records.add()
        record.update({"note_id": "n-1"})
        return types_pb2.GraphResultSet(records=[record])

    def VectorHybridSearch(self, request, context):
        self.calls.append(("vector", request))
        return vector_pb2.VectorSet(points=self.points)


class Config(config_grpc.ConfigServiceServicer):
    def __init__(self) -> None:
        self.requests: list[config_pb.EvaluateFlagsRequest] = []
        self.headers: list[dict[str, str]] = []

    def EvaluateFlags(self, request, context):
        self.requests.append(request)
        self.headers.append(dict(context.invocation_metadata()))
        known = {"on": config_pb.FlagValue(bool_value=True), "off": config_pb.FlagValue(bool_value=False)}
        return config_pb.EvaluateFlagsResponse(
            values={key: known[key] for key in request.keys if key in known}
        )


class Live(lq_grpc.LiveQueryServiceServicer):
    def __init__(self) -> None:
        self.script: list = []
        self.requests: list[lq_pb.SubscribeRequest] = []
        self.headers: list[dict[str, str]] = []
        self.times: list[float] = []

    def Subscribe(self, request, context):
        self.requests.append(copy.copy(request))
        self.headers.append(dict(context.invocation_metadata()))
        self.times.append(time.monotonic())
        step = self.script.pop(0) if self.script else None
        if step is None:
            while context.is_active():
                time.sleep(0.01)
            return
        yield from step(context)


class Storage(storage_grpc.StorageServiceServicer):
    def __init__(self) -> None:
        self.requests: list[storage_pb.DeleteFileRequest] = []

    def DeleteFile(self, request, context):
        self.requests.append(request)
        return storage_pb.DeleteFileResponse()


class Lock(lock_grpc.LockServiceServicer):
    def __init__(self) -> None:
        self.requests: list = []

    def AcquireLock(self, request, context):
        self.requests.append(request)
        return lock_pb.AcquireLockResponse(acquired=True, fencing_token=7)


class Broker:
    """A local gRPC server standing in for the broker's listeners."""

    def __init__(self, health: bytes = b"\x08\x01") -> None:
        self.authn = Authn()
        self.data = Data()
        self.config = Config()
        self.live = Live()
        self.storage = Storage()
        self.lock = Lock()
        self.health = health
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
        authn_grpc.add_AuthnServiceServicer_to_server(self.authn, self.server)
        data_grpc.add_DataBrokerServicer_to_server(self.data, self.server)
        config_grpc.add_ConfigServiceServicer_to_server(self.config, self.server)
        lq_grpc.add_LiveQueryServiceServicer_to_server(self.live, self.server)
        storage_grpc.add_StorageServiceServicer_to_server(self.storage, self.server)
        lock_grpc.add_LockServiceServicer_to_server(self.lock, self.server)
        self.server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    "grpc.health.v1.Health",
                    {
                        "Check": grpc.unary_unary_rpc_method_handler(
                            lambda _request, _context: self.health,
                            request_deserializer=lambda data: data,
                            response_serializer=lambda data: data,
                        )
                    },
                ),
            )
        )
        self.target = f"127.0.0.1:{self.server.add_insecure_port('127.0.0.1:0')}"
        self.server.start()

    def stop(self) -> None:
        self.server.stop(None)


@pytest.fixture
def broker():
    served = Broker()
    yield served
    served.stop()


def _connect(broker: Broker, **overrides) -> EnterpriseSession:
    values = dict(
        target=broker.target,
        username="admin",
        password="correct",
        tenant_code="acme",
        project_id="default",
        deadline=TIMEOUT,
    )
    values.update(overrides)
    return connect_enterprise(**values)


def _wait_until(condition, *, timeout: float = TIMEOUT) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return condition()


def _select(session) -> None:
    session.data.select(message_type="udb.sdk.live.v1.SdkLiveRecord")


# ── tenant state ────────────────────────────────────────────────────────────
def test_tenant_state_rejects_everything_but_a_canonical_uuid_and_stays_unverified() -> None:
    state = TenantState(tenant_code="acme")
    mixed_case = "AAAAAAAA-1111-4111-8111-111111111111"
    for bad in ("", "   ", "acme", mixed_case, TENANT.replace("-", "")):
        with pytest.raises(UdbTenantError):
            state.adopt(bad)
        assert state.canonical_id == "" and state.is_verified is False
    with pytest.raises(UdbTenantError, match="not verified"):
        state.get_verified()
    state.adopt(f"  {TENANT} ")
    assert state.get_verified() == TENANT


def test_tenant_state_validation_names_both_values_and_passes_an_empty_record() -> None:
    state = TenantState()
    with pytest.raises(UdbTenantError, match="not verified"):
        state.validate_tenant_id("")
    state.adopt(TENANT)
    state.validate_tenant_id("")
    state.validate_tenant_id(TENANT)
    with pytest.raises(UdbTenantError) as refused:
        state.validate_tenant_id("acme")
    assert "acme" in str(refused.value) and TENANT in str(refused.value)


# ── enterprise session ──────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "missing", [{"target": ""}, {"username": ""}, {"password": ""}]
)
def test_connect_enterprise_requires_target_username_password(missing) -> None:
    values = dict(target="127.0.0.1:1", username="u", password="p")
    values.update(missing)
    with pytest.raises(UdbConfigurationError, match="requires target, username, and password"):
        connect_enterprise(**values)


def test_enterprise_config_repr_hides_the_password() -> None:
    assert "hunter2" not in repr(EnterpriseConfig(target="t", username="u", password="hunter2"))


def test_connect_enterprise_adopts_the_verified_principal_not_the_hints(broker) -> None:
    with _connect(broker) as session:
        assert session.canonical_tenant_id == TENANT
        assert session.canonical_project_id == PROJECT
        assert session.tenant_state.get_verified() == TENANT
        assert session.tenant_state.tenant_code == "acme"
        meta = session.metadata
        assert (meta.tenant_id, meta.project_id, meta.user_id) == (TENANT, PROJECT, USER)
        assert meta.service_identity == "svc" and tuple(meta.scopes) == ("udb:read", "udb:write")
        assert session.principal().principal_id == USER
        assert session.bearer() == "Bearer access-1"
        assert isinstance(session.bearer_expires_at(), datetime)
        session.principal().tenant_id = "mutated"
        assert session.principal().tenant_id == TENANT
    request = broker.authn.login_requests[0]
    assert request.tenant_hint == "acme" and request.project_hint == "default"
    assert "authorization" not in broker.authn.login_headers[0]


def test_connect_enterprise_without_a_code_sends_an_empty_body_hint_and_default_header(broker) -> None:
    with _connect(broker, tenant_code="", project_id=""):
        pass
    assert broker.authn.login_requests[0].tenant_hint == ""
    assert broker.authn.login_headers[0]["x-tenant-id"] == "default"


def test_connect_enterprise_refused_login_raises_the_broker_status(broker) -> None:
    with pytest.raises(UdbRpcError) as refused:
        _connect(broker, password="wrong")
    assert refused.value.code == grpc.StatusCode.UNAUTHENTICATED


def test_connect_enterprise_mfa_required_is_an_error_not_an_empty_session(broker) -> None:
    broker.authn.mfa = True
    with pytest.raises(UdbError, match="MFA required: True"):
        _connect(broker)


def test_connect_enterprise_refuses_a_principal_with_a_non_canonical_tenant(broker) -> None:
    broker.authn.principal = _principal(tenant_id="acme")
    with pytest.raises(UdbTenantError, match="not a canonical"):
        _connect(broker)


def test_validate_tenant_fails_fast(broker) -> None:
    with _connect(broker) as session:
        session.validate_tenant(TENANT)
        session.validate_tenant("")
        with pytest.raises(UdbTenantError, match="mismatch"):
            session.validate_tenant(OTHER)


def test_repeated_refreshes_present_each_rotated_token_and_calls_carry_the_newest_bearer(broker) -> None:
    with _connect(broker) as session:
        for expected in range(2, 5):
            session.refresh_if_needed(force=True)
            assert session.bearer() == f"Bearer access-{expected}"
            _select(session)
            assert broker.data.headers[-1]["authorization"] == f"Bearer access-{expected}"
            assert broker.data.headers[-1]["x-tenant-id"] == TENANT
        assert broker.authn.presented == ["refresh-1", "refresh-2", "refresh-3"]
        assert broker.authn.logins == 1
        assert session.refresh_error() is None


def test_derived_metadata_keeps_following_the_current_bearer(broker) -> None:
    with _connect(broker) as session:
        derived = session.metadata.with_purpose("billing")
        session.refresh_if_needed(force=True)
        session.data.select(message_type="m", metadata=derived)
        assert broker.data.headers[-1]["authorization"] == "Bearer access-2"
        assert broker.data.headers[-1]["x-purpose"] == "billing"


def test_background_refresher_renews_a_short_lived_bearer_repeatedly(broker) -> None:
    broker.authn.ttl = 2
    with _connect(broker) as session:
        assert _wait_until(lambda: broker.authn.refreshes >= 2, timeout=6.0)
        assert session.refresh_error() is None
        assert session.bearer() != "Bearer access-1"
    assert broker.authn.presented[:2] == ["refresh-1", "refresh-2"]


def test_close_stops_the_refresher_and_later_calls_are_cancelled_locally(broker) -> None:
    broker.authn.ttl = 2
    session = _connect(broker)
    thread = session._refresh_thread
    session.close()
    session.close()
    assert not thread.is_alive()
    sent = len(broker.data.headers)
    with pytest.raises(UdbCredentialError) as refused:
        _select(session)
    assert refused.value.code == grpc.StatusCode.CANCELLED
    assert len(broker.data.headers) == sent


def test_a_stale_bearer_refreshes_on_demand_and_concurrent_callers_share_one_refresh(broker) -> None:
    with _connect(broker) as session:
        broker.authn.refresh_delay = 0.2
        session._token.issued_at = time.time() - 899
        session._token.expires_at = time.time() + 1  # within the refresh margin
        results: list[str] = []
        threads = [
            threading.Thread(target=lambda: results.append(session._current_bearer())) for _ in range(8)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert results == ["Bearer access-2"] * 8
        assert broker.authn.refreshes == 1


def test_a_refused_refresh_logs_in_again_with_the_same_credentials(broker) -> None:
    with _connect(broker) as session:
        broker.authn.refuse_refresh = True
        session.refresh_if_needed(force=True)
        assert broker.authn.logins == 2
        assert session.bearer() == "Bearer access-2"
        assert session.refresh_error() is None
        _select(session)
        assert broker.data.headers[-1]["authorization"] == "Bearer access-2"


def test_transient_failure_while_valid_is_refused_but_not_poison(broker) -> None:
    with _connect(broker) as session:
        broker.authn.refuse_refresh = True
        broker.authn.refuse_login = True
        session._token.issued_at = time.time() - 890
        session._token.expires_at = time.time() + 10  # inside the margin, still valid
        with pytest.raises(UdbCredentialError):
            _select(session)
        assert session.refresh_error() is not None
        assert session.credential_error() is None
        # A failed attempt may have spent the refresh token, so recovery logs in.
        broker.authn.refuse_login = False
        presented = len(broker.authn.presented)
        logins = broker.authn.logins
        session.refresh_if_needed()
        assert len(broker.authn.presented) == presented
        assert broker.authn.logins == logins + 1
        assert session.refresh_error() is None


def test_an_expired_unrefreshable_session_is_poisoned_and_refuses_locally(broker) -> None:
    with _connect(broker) as session:
        broker.authn.refuse_refresh = True
        broker.authn.refuse_login = True
        session._token.expires_at = time.time() - 1
        sent = len(broker.data.headers)
        with pytest.raises(UdbCredentialError) as first:
            _select(session)
        assert first.value.code == grpc.StatusCode.UNAUTHENTICATED
        with pytest.raises(UdbCredentialError, match="session not authorized"):
            _select(session)
        with pytest.raises(UdbCredentialError):
            session.data_metadata()
        with pytest.raises(UdbCredentialError):
            session.native_metadata()
        with pytest.raises(UdbCredentialError):
            session.ready()
        assert isinstance(session.credential_error(), UdbCredentialError)
        assert len(broker.data.headers) == sent


@pytest.mark.parametrize(
    "changed",
    [
        {"tenant_id": OTHER},
        {"project_id": OTHER},
        {"user_id": OTHER},
        {"service_identity": "other"},
        {"scopes": ["udb:admin"]},
        {"principal_id": OTHER},
        {"subject": OTHER},
        {"account_kind": 2},
    ],
)
def test_a_renewal_as_a_different_principal_is_terminal(broker, changed) -> None:
    with _connect(broker) as session:
        broker.authn.principal = _principal(**changed)
        with pytest.raises(UdbCredentialBindingError):
            session.refresh_if_needed(force=True)
        session._record_credential_error(session._terminal_error)
        # Scope order alone would not have been a change; this is terminal now.
        logins = broker.authn.logins
        broker.authn.principal = _principal()
        with pytest.raises(UdbCredentialBindingError):
            session.refresh_if_needed(force=True)
        assert broker.authn.logins == logins, "a binding failure must not re-login"
        sent = len(broker.data.headers)
        with pytest.raises(UdbCredentialError):
            _select(session)
        assert len(broker.data.headers) == sent


def test_scope_order_and_duplicates_are_not_an_identity_change(broker) -> None:
    with _connect(broker) as session:
        broker.authn.principal = _principal(scopes=["udb:write", "udb:read", "udb:read"])
        session.refresh_if_needed(force=True)
        assert session.refresh_error() is None


def test_lock_service_defaults_the_canonical_tenant_and_keeps_an_explicit_one(broker) -> None:
    with _connect(broker) as session:
        locks = session.lock_service()
        acquired = locks.acquire(lock_pb.AcquireLockRequest(lock_name="l", owner_id="o", lease_ttl_seconds=5))
        assert acquired.fencing_token == 7
        locks.acquire(lock_pb.AcquireLockRequest(tenant_id=OTHER, lock_name="l", owner_id="o"))
        assert [r.tenant_id for r in broker.lock.requests] == [TENANT, OTHER]
        assert session.native_channel is session._ctl_channel


def test_raw_stub_on_the_native_channel_uses_the_session_metadata(broker) -> None:
    with _connect(broker) as session:
        stub = config_grpc.ConfigServiceStub(session.native_channel)
        stub.EvaluateFlags(
            config_pb.EvaluateFlagsRequest(tenant_id=TENANT, keys=["on"]),
            metadata=session.native_metadata().to_grpc_metadata(),
            timeout=TIMEOUT,
        )
        assert broker.config.headers[-1]["authorization"] == "Bearer access-1"


# ── as_user / validate_session ──────────────────────────────────────────────
def test_as_user_sends_only_the_user_bearer_and_the_connected_scope(broker) -> None:
    broker.authn.key_principals["svc-key"] = _principal(user_id="")
    with UdbProject(UdbConfig(target=broker.target, tenant_id=TENANT, api_key="svc-key", deadline=TIMEOUT)) as project:
        before = project.metadata
        for bearer in ("user-token", "Bearer user-token", "  Bearer user-token "):
            project.data.select(message_type="m", metadata=project.as_user(bearer))
            headers = broker.data.headers[-1]
            assert headers["authorization"] == "Bearer user-token"
            assert headers["x-tenant-id"] == TENANT
            assert headers["x-udb-project-id"] == PROJECT
            for hint in ("x-user-id", "x-service-identity", "x-scopes"):
                assert headers.get(hint, "") == "", hint
            assert "x-api-key" not in headers
        assert project.metadata == before
        project.data.select(message_type="m")
        assert broker.data.headers[-1]["authorization"].startswith("Bearer key-bearer-")
        assert broker.data.headers[-1]["x-service-identity"] == "svc"


def test_as_user_keeps_request_audit_values_but_not_identity_from_the_given_metadata(broker) -> None:
    with UdbProject(
        UdbConfig(target=broker.target, tenant_id=TENANT, project_id=PROJECT, purpose="p", deadline=TIMEOUT)
    ) as project:
        request = Metadata(
            tenant_id=OTHER,
            project_id=OTHER,
            purpose="delegated-purpose",
            correlation_id="delegated-correlation",
            user_id=OTHER,
            scopes=("udb:admin",),
            api_key="raw",
            consistency="strong",
        )
        delegated = project.as_user("user-token", metadata=request)
        assert delegated.tenant_id == TENANT and delegated.project_id == PROJECT
        assert delegated.purpose == "delegated-purpose"
        assert delegated.correlation_id == "delegated-correlation"
        assert delegated.user_id == "" and tuple(delegated.scopes) == () and delegated.api_key == ""
        assert delegated.consistency == "strong"
        again = project.as_user("second", metadata=delegated)
        assert again.purpose == "delegated-purpose" and again.bearer_token == "second"


@pytest.mark.parametrize("empty", ["", "   ", "Bearer ", "Bearer    "])
def test_as_user_refuses_an_empty_bearer_rather_than_fall_back_to_the_service(broker, empty) -> None:
    with UdbProject(UdbConfig(target=broker.target, tenant_id=TENANT, deadline=TIMEOUT)) as project:
        with pytest.raises(UdbCredentialError, match="delegated bearer is required") as refused:
            project.as_user(empty)
        assert refused.value.code == grpc.StatusCode.UNAUTHENTICATED


def test_as_user_reaches_native_facades_with_the_user_bearer_not_the_service_bearer(broker) -> None:
    broker.authn.key_principals["svc-key"] = _principal()
    with UdbProject(UdbConfig(target=broker.target, tenant_id=TENANT, api_key="svc-key", deadline=TIMEOUT)) as project:
        project.flags.evaluate("on", metadata=project.as_user("user-token"))
        assert broker.config.headers[-1]["authorization"] == "Bearer user-token"
        assert broker.config.headers[-1].get("x-service-identity", "") == ""


def test_as_user_on_a_session_keeps_the_bearer_and_the_owners_poison_check(broker) -> None:
    with _connect(broker) as session:
        delegated = session.as_user("user-token")
        session.refresh_if_needed(force=True)
        session.data.select(message_type="m", metadata=session.data_metadata(delegated))
        assert broker.data.headers[-1]["authorization"] == "Bearer user-token"
        broker.authn.refuse_refresh = broker.authn.refuse_login = True
        session._token.expires_at = time.time() - 1
        with pytest.raises(UdbCredentialError):
            _select(session)
        sent = len(broker.data.headers)
        with pytest.raises(UdbCredentialError):
            session.data.select(message_type="m", metadata=delegated)
        assert len(broker.data.headers) == sent


def test_validate_session_returns_the_verified_fields_and_strips_the_prefix(broker) -> None:
    with UdbProject(UdbConfig(target=broker.target, tenant_id=TENANT, deadline=TIMEOUT)) as project:
        user = project.auth.validate_session("Bearer tok")
        assert broker.authn.validated == ["tok"]
        assert (user.public_id, user.user_id, user.tenant_id, user.project_id) == (
            "sesspub_1", USER, TENANT, PROJECT,
        )
        assert user.scopes == ("udb:read",) and user.roles == ("reader",)
        assert user.expires_at == datetime.fromtimestamp(2_000_000_000, timezone.utc)
        assert user.principal.user_id == USER


def test_validate_session_refuses_an_invalid_token_as_not_found(broker) -> None:
    broker.authn.valid = False
    with UdbProject(UdbConfig(target=broker.target, tenant_id=TENANT, deadline=TIMEOUT)) as project:
        with pytest.raises(UdbNotFound, match="not valid"):
            project.auth.validate_session("tok")


# ── environment contract ────────────────────────────────────────────────────
_ENV_NAMES = (
    "TARGET AUTH_TARGET WEBRTC_TARGET TENANT_ID PROJECT_ID PROJECT PURPOSE API_KEY BEARER "
    "SCOPES SERVICE_IDENTITY DEADLINE TLS TLS_SERVER_NAME"
).split()


@pytest.fixture
def env(monkeypatch):
    for name in _ENV_NAMES:
        monkeypatch.delenv(f"PARITY_{name}", raising=False)

    def set_env(**values):
        for name, value in values.items():
            monkeypatch.setenv(f"PARITY_{name}", value)

    return set_env


def test_config_from_env_reports_every_problem_at_once(env) -> None:
    env(AUTH_TARGET="nohost", DEADLINE="soon")
    with pytest.raises(UdbConfigurationError) as refused:
        config_from_env("PARITY_")
    message = str(refused.value)
    for needle in (
        "PARITY_TARGET is not set",
        "PARITY_TENANT_ID is not set",
        "PARITY_PURPOSE is not set",
        "PARITY_API_KEY (or PARITY_BEARER) is not set",
        "PARITY_AUTH_TARGET must be host:port",
        "PARITY_DEADLINE must be a positive duration",
    ):
        assert needle in message


@pytest.mark.parametrize("deadline", ["0s", "-5s", "10", "5x", "s"])
def test_config_from_env_refuses_a_bad_deadline(env, deadline) -> None:
    env(TARGET="h:1", TENANT_ID=TENANT, PURPOSE="p", API_KEY="k", DEADLINE=deadline)
    with pytest.raises(UdbConfigurationError, match="DEADLINE"):
        config_from_env("PARITY_")


def test_config_from_env_reads_the_contract(env) -> None:
    env(
        TARGET="h:1",
        AUTH_TARGET="h:2",
        WEBRTC_TARGET="h:3",
        TENANT_ID=TENANT,
        PROJECT="legacy-name",
        PURPOSE="p",
        API_KEY="k",
        SCOPES="a,b c\ta",
        SERVICE_IDENTITY="svc",
        DEADLINE="1m30s",
        TLS="TRUE",
        TLS_SERVER_NAME="broker.internal",
    )
    config = config_from_env("PARITY_")
    assert (config.target, config.auth_target, config.webrtc_target) == ("h:1", "h:2", "h:3")
    assert config.project_id == "legacy-name"
    assert tuple(config.scopes) == ("a", "b", "c")
    assert config.deadline == 90.0
    assert config.tls is True
    assert config.channel_options == (("grpc.ssl_target_name_override", "broker.internal"),)
    assert config.service_identity == "svc"


def test_config_from_env_prefers_project_id_and_tolerates_defaults(env) -> None:
    env(TARGET="h:1", TENANT_ID=TENANT, PURPOSE="p", BEARER="b", PROJECT_ID="new", PROJECT="old", TLS="1")
    config = config_from_env("PARITY_")
    assert config.project_id == "new" and config.tls is True and config.bearer_token == "b"
    assert config.channel_options is None and config.deadline == 30.0


def test_connect_from_env_refuses_both_credentials(env) -> None:
    env(TARGET="h:1", TENANT_ID=TENANT, PURPOSE="p", API_KEY="k", BEARER="b")
    with pytest.raises(UdbConfigurationError, match="mutually exclusive"):
        connect_from_env("PARITY_")


def test_connect_from_env_exchanges_the_key_and_verifies(broker, env) -> None:
    broker.authn.key_principals["svc-key"] = _principal()
    env(TARGET=broker.target, TENANT_ID=TENANT, PURPOSE="p", API_KEY="svc-key")
    with connect_from_env(
        "PARITY_",
        Expect(tenant_id=TENANT, project_id=PROJECT, service_identity="svc", required_scopes=("UDB:READ",)),
    ) as project:
        assert project.principal().tenant_id == TENANT
        assert project.metadata.bearer_token.startswith("key-bearer-")
        assert project.bearer_expires_at() > datetime.now(timezone.utc)
        project.ready()


def test_connect_from_env_closes_the_connection_on_a_failed_expectation(broker, env, monkeypatch) -> None:
    broker.authn.key_principals["svc-key"] = _principal()
    env(TARGET=broker.target, TENANT_ID=TENANT, PURPOSE="p", API_KEY="svc-key")
    closed: list[bool] = []
    original = UdbProject.close
    monkeypatch.setattr(UdbProject, "close", lambda self: (closed.append(True), original(self))[1])
    with pytest.raises(UdbConfigurationError, match="does not match"):
        connect_from_env("PARITY_", Expect(tenant_id=OTHER))
    assert closed


def test_connect_from_env_closes_when_the_key_is_refused(broker, env, monkeypatch) -> None:
    env(TARGET=broker.target, TENANT_ID=TENANT, PURPOSE="p", API_KEY="unknown")
    closed: list[bool] = []
    original = UdbProject.close
    monkeypatch.setattr(UdbProject, "close", lambda self: (closed.append(True), original(self))[1])
    with pytest.raises(UdbRpcError):
        connect_from_env("PARITY_")
    assert closed


def test_verify_reports_every_mismatch_at_once(broker) -> None:
    broker.authn.key_principals["k"] = _principal()
    with UdbProject(UdbConfig(target=broker.target, tenant_id=TENANT, api_key="k", deadline=TIMEOUT)) as project:
        with pytest.raises(UdbConfigurationError) as refused:
            project.verify(
                Expect(tenant_id=OTHER, project_id="p2", service_identity="s2", required_scopes=("x", "udb:read"))
            )
        message = str(refused.value)
        assert "tenant is" in message and "project is" in message and "service identity is" in message
        assert "lacks scopes x " in message and "udb:read" not in message.split("lacks scopes")[1]
        project.verify(None)
        project.verify(Expect())


def test_verify_on_a_raw_bearer_needs_nothing_expected(broker) -> None:
    with UdbProject(UdbConfig(target=broker.target, tenant_id=TENANT, bearer_token="b", deadline=TIMEOUT)) as project:
        project.verify(Expect())
        assert project.principal() is None and project.bearer_expires_at() is None
        with pytest.raises(UdbConfigurationError, match="no verified principal"):
            project.verify(Expect(tenant_id=TENANT))


def test_principal_is_a_copy_that_follows_renewal(broker) -> None:
    broker.authn.key_principals["k"] = _principal()
    with UdbProject(UdbConfig(target=broker.target, tenant_id=TENANT, api_key="k", deadline=TIMEOUT)) as project:
        project.principal().tenant_id = "mutated"
        assert project.principal().tenant_id == TENANT
        broker.authn.key_principals["k"] = _principal(username="renamed")
        project.authenticate_api_key_and_adopt("k")
        assert project.principal().username == "renamed"


def test_credentials_set_later_supersede_the_exchanged_keys_introspection(broker) -> None:
    broker.authn.key_principals["k"] = _principal()
    with UdbProject(UdbConfig(target=broker.target, tenant_id=TENANT, api_key="k", deadline=TIMEOUT)) as project:
        assert project.principal() is not None and project.bearer_expires_at() is not None
        project._api_key_last_error = RuntimeError("old failure")
        project.set_credentials(bearer_token="explicit")
        assert project.principal() is None and project.bearer_expires_at() is None
        assert project.credential_error() is None
        with pytest.raises(UdbConfigurationError, match="no verified principal"):
            project.verify(Expect(tenant_id=TENANT))


def test_live_query_on_a_closed_session_stops_instead_of_reconnecting(broker) -> None:
    session = _connect(broker)
    query = session.live_query("x.v1.Row", backoff=0.01)
    session.close()
    with pytest.raises(UdbCredentialError) as closed:
        list(query)
    assert closed.value.code == grpc.StatusCode.CANCELLED
    assert broker.live.requests == []


@pytest.mark.parametrize(
    ("health", "expected"),
    [(b"", "UNKNOWN"), (b"\x08\x02", "NOT_SERVING"), (b"\x08\x03", "SERVICE_UNKNOWN")],
)
def test_ready_refuses_a_broker_that_is_not_serving(health, expected) -> None:
    served = Broker(health=health)
    try:
        with UdbProject(UdbConfig(target=served.target, tenant_id=TENANT, deadline=TIMEOUT)) as project:
            with pytest.raises(UdbError, match=f"broker is {expected}"):
                project.ready()
    finally:
        served.stop()


def test_ready_passes_repeatedly_and_reports_a_failing_credential_and_a_dead_broker(broker) -> None:
    broker.authn.key_principals["k"] = _principal()
    project = UdbProject(UdbConfig(target=broker.target, tenant_id=TENANT, api_key="k", deadline=TIMEOUT))
    for _ in range(3):
        project.ready()
    project._api_key_last_error = RuntimeError("exchange refused")
    assert project.credential_error() is project._api_key_last_error
    with pytest.raises(UdbCredentialError, match="credential refresh is failing"):
        project.ready()
    project._api_key_last_error = None
    broker.stop()
    with pytest.raises(UdbRpcError) as refused:
        project.ready(timeout=0.5)
    assert refused.value.code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)
    project.close()
    with pytest.raises(UdbCredentialError) as closed:
        project.ready()
    assert closed.value.code == grpc.StatusCode.CANCELLED


# ── API-key renewal ─────────────────────────────────────────────────────────
def _key_project(broker, **overrides) -> UdbProject:
    broker.authn.key_principals["k"] = _principal(**overrides)
    return UdbProject(UdbConfig(target=broker.target, tenant_id=OTHER, api_key="k", deadline=TIMEOUT))


def _make_due(project: UdbProject) -> None:
    project._api_key_issued_at = time.time() - 890
    project._api_key_expires_at = time.time() + 10


def _key_bearer(project: UdbProject, broker) -> str:
    project.data.select(message_type="m")
    return broker.data.headers[-1]["authorization"]


def test_api_key_adoption_takes_the_whole_identity_from_the_principal(broker) -> None:
    with _key_project(broker, user_id="", service_identity="svc-1", scopes=["udb:read"]) as project:
        meta = project.metadata
        assert (meta.tenant_id, meta.project_id, meta.user_id) == (TENANT, PROJECT, "")
        assert meta.service_identity == "svc-1" and tuple(meta.scopes) == ("udb:read",)


def test_api_key_call_renews_on_demand_repeatedly_and_shares_one_exchange(broker) -> None:
    with _key_project(broker) as project:
        for round_number in range(2, 5):
            _make_due(project)
            exchanges = broker.authn.exchanges
            results: list[str] = []
            threads = [
                threading.Thread(target=lambda: results.append(dict(project._ctl_metadata(None))["authorization"]))
                for _ in range(8)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            assert broker.authn.exchanges == exchanges + 1, f"round {round_number}"
            assert len(set(results)) == 1 and results[0] == _key_bearer(project, broker)
        assert broker.authn.exchanges == 4
        assert project.credential_error() is None


def test_api_key_bearer_not_due_needs_no_exchange(broker) -> None:
    with _key_project(broker) as project:
        for _ in range(3):
            _key_bearer(project, broker)
        assert broker.authn.exchanges == 1


def test_derived_api_key_metadata_follows_the_renewed_bearer(broker) -> None:
    with _key_project(broker) as project:
        derived = project.metadata.with_purpose("billing")
        _make_due(project)
        project.data.select(message_type="m", metadata=derived)
        assert broker.data.headers[-1]["authorization"].endswith("-2")
        assert broker.data.headers[-1]["x-purpose"] == "billing"


@pytest.mark.parametrize(
    "changed",
    [
        {"tenant_id": OTHER},
        {"project_id": OTHER},
        {"user_id": OTHER},
        {"service_identity": "other"},
        {"scopes": ["udb:admin"]},
        {"principal_id": OTHER},
        {"subject": OTHER},
        {"account_kind": 2},
    ],
)
def test_api_key_renewal_as_another_principal_fails_closed_until_it_matches(broker, changed) -> None:
    with _key_project(broker) as project:
        pinned = project.metadata
        broker.authn.key_principals["k"] = _principal(**changed)
        _make_due(project)
        sent = len(broker.data.headers)
        with pytest.raises(UdbCredentialError, match="renew"):
            project.data.select(message_type="m")
        assert isinstance(project.credential_error(), UdbCredentialBindingError)
        # The binding failure refuses locally, without another exchange or call.
        exchanges = broker.authn.exchanges
        with pytest.raises(UdbCredentialError):
            project.data.select(message_type="m")
        assert broker.authn.exchanges == exchanges and len(broker.data.headers) == sent
        assert project.metadata == pinned, "a refused renewal must not change identity"
        with pytest.raises(UdbCredentialError):
            project.ready()
        # The background retry recovers as soon as the broker verifies the same identity.
        broker.authn.key_principals["k"] = _principal()
        project._api_key_renewal_tick()
        assert project.credential_error() is None
        assert _key_bearer(project, broker).startswith("Bearer key-bearer-")


def test_api_key_scope_order_is_not_an_identity_change(broker) -> None:
    with _key_project(broker) as project:
        broker.authn.key_principals["k"] = _principal(scopes=["udb:write", "udb:read", "udb:read"])
        _make_due(project)
        _key_bearer(project, broker)
        assert project.credential_error() is None


def test_explicit_adoption_of_another_key_replaces_the_pin(broker) -> None:
    with _key_project(broker) as project:
        broker.authn.key_principals["k2"] = _principal(tenant_id=OTHER, principal_id=OTHER)
        project.authenticate_api_key_and_adopt("k2")
        assert project.metadata.tenant_id == OTHER and project.principal().principal_id == OTHER
        _make_due(project)
        _key_bearer(project, broker)
        assert project.credential_error() is None and project.metadata.tenant_id == OTHER


def test_exchange_without_expiry_or_principal_is_refused(broker) -> None:
    class NoExpiry(Authn):
        def Authenticate(self, request, context):
            return authn_pb.AuthnResponse(access_token="t", principal=_principal())

    class NoPrincipal(Authn):
        def Authenticate(self, request, context):
            return authn_pb.AuthnResponse(access_token="t", expires_at_unix=int(time.time()) + 900)

    class Expired(Authn):
        def Authenticate(self, request, context):
            return authn_pb.AuthnResponse(access_token="t", expires_at_unix=1, principal=_principal())

    for servicer, message in ((NoExpiry, "no bearer expiry"), (NoPrincipal, "no verified principal"), (Expired, "expired")):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        authn_grpc.add_AuthnServiceServicer_to_server(servicer(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            with pytest.raises(UdbConfigurationError, match=message):
                UdbProject(UdbConfig(target=f"127.0.0.1:{port}", tenant_id=TENANT, api_key="k", deadline=TIMEOUT))
        finally:
            server.stop(None)


def test_closed_api_key_project_refuses_locally(broker) -> None:
    project = _key_project(broker)
    project.close()
    with pytest.raises(UdbCredentialError) as refused:
        project.data.select(message_type="m")
    assert refused.value.code == grpc.StatusCode.CANCELLED


def test_explicit_credentials_end_on_demand_renewal(broker) -> None:
    with _key_project(broker) as project:
        project.set_credentials(bearer_token="explicit")
        _make_due(project)
        assert _key_bearer(project, broker) == "Bearer explicit"
        assert broker.authn.exchanges == 1


# ── login_and_adopt_tenant ──────────────────────────────────────────────────
def test_login_and_adopt_takes_the_whole_identity_and_replaces_hints(broker) -> None:
    config = UdbConfig(
        target=broker.target,
        tenant_id="hint-tenant",
        user_id="hint-user",
        service_identity="hint-svc",
        scopes=("hint:scope",),
        deadline=TIMEOUT,
    )
    with UdbProject(config) as project:
        for _ in range(3):
            authn = project.login_and_adopt_tenant("admin", "correct")
            meta = project.metadata
            assert authn.principal.user_id == USER
            assert (meta.tenant_id, meta.project_id, meta.user_id) == (TENANT, PROJECT, USER)
            assert meta.service_identity == "svc" and tuple(meta.scopes) == ("udb:read", "udb:write")
            assert (project.config.user_id, project.config.service_identity) == (USER, "svc")
            assert "authorization" not in broker.authn.login_headers[-1], "login presents no old bearer"
            project.data.select(message_type="m")
            headers = broker.data.headers[-1]
            assert headers["x-user-id"] == USER and headers["x-scopes"] == "udb:read,udb:write"
        # A principal with an empty user clears the configured one rather than keep a hint.
        broker.authn.principal = _principal(user_id="", scopes=[])
        project.login_and_adopt_tenant("admin", "correct")
        assert project.metadata.user_id == "" and tuple(project.metadata.scopes) == ()


def test_login_and_adopt_refuses_a_failed_login_and_keeps_the_identity(broker) -> None:
    with UdbProject(UdbConfig(target=broker.target, tenant_id="hint", deadline=TIMEOUT)) as project:
        with pytest.raises(UdbRpcError) as refused:
            project.login_and_adopt_tenant("admin", "wrong")
        assert refused.value.code == grpc.StatusCode.UNAUTHENTICATED
        assert project.metadata.tenant_id == "hint" and project.metadata.bearer_token == ""
        broker.authn.mfa = True
        with pytest.raises(UdbError, match="MFA required: True"):
            project.login_and_adopt_tenant("admin", "correct")
        assert project.metadata.tenant_id == "hint"


def test_login_supersedes_an_exchanged_key(broker) -> None:
    with _key_project(broker) as project:
        project.login_and_adopt_tenant("admin", "correct")
        assert project.principal() is None
        _make_due(project)
        assert _key_bearer(project, broker) == "Bearer access-1"
        assert broker.authn.exchanges == 1


# ── async connection forms ──────────────────────────────────────────────────
def test_connect_enterprise_async_sends_with_the_sessions_current_bearer(broker) -> None:
    async def scenario() -> None:
        project = await connect_enterprise_async(
            target=broker.target, username="admin", password="correct", deadline=TIMEOUT
        )
        session = project.credential_owner
        assert isinstance(session, EnterpriseSession)
        assert project.metadata.tenant_id == TENANT and project.principal().tenant_id == TENANT
        for expected in range(1, 4):
            await project.data.select(message_type="m")
            assert broker.data.headers[-1]["authorization"] == f"Bearer access-{expected}"
            await project.ready()
            assert await project.flags.enabled("on") is True
            session.refresh_if_needed(force=True)
        session.validate_tenant(TENANT)
        await project.close()
        with pytest.raises(UdbCredentialError):
            session.ready()

    asyncio.run(scenario())


def test_connect_enterprise_async_raises_the_brokers_refusal(broker) -> None:
    with pytest.raises(UdbRpcError) as refused:
        asyncio.run(connect_enterprise_async(target=broker.target, username="u", password="wrong"))
    assert refused.value.code == grpc.StatusCode.UNAUTHENTICATED


def test_connect_from_env_async_exchanges_verifies_and_renews(broker, env) -> None:
    broker.authn.key_principals["svc-key"] = _principal()
    env(TARGET=broker.target, TENANT_ID=TENANT, PURPOSE="p", API_KEY="svc-key")

    async def scenario() -> None:
        async with await connect_from_env_async("PARITY_", Expect(tenant_id=TENANT)) as project:
            project.verify(Expect(project_id=PROJECT))
            with pytest.raises(UdbConfigurationError, match="does not match"):
                project.verify(Expect(project_id=OTHER))
            assert project.bearer_expires_at() is not None
            for _ in range(3):
                _make_due(project.credential_owner)
                exchanges = broker.authn.exchanges
                await project.data.select(message_type="m")
                assert broker.authn.exchanges == exchanges + 1
                assert broker.data.headers[-1]["authorization"].startswith("Bearer key-bearer-")
            delegated = project.as_user("user-token")
            await project.data.select(message_type="m", metadata=delegated)
            assert broker.data.headers[-1]["authorization"] == "Bearer user-token"
            broker.authn.key_principals["svc-key"] = _principal(tenant_id=OTHER)
            _make_due(project.credential_owner)
            with pytest.raises(UdbCredentialError):
                await project.data.select(message_type="m")
            with pytest.raises(UdbCredentialError):
                await project.ready()
            with pytest.raises(UdbCredentialError):
                await project.data.select(message_type="m", metadata=delegated)

    asyncio.run(scenario())


def test_connect_from_env_async_refuses_a_wrong_principal(broker, env) -> None:
    broker.authn.key_principals["svc-key"] = _principal()
    env(TARGET=broker.target, TENANT_ID=TENANT, PURPOSE="p", API_KEY="svc-key")
    with pytest.raises(UdbConfigurationError, match="does not match"):
        asyncio.run(connect_from_env_async("PARITY_", Expect(tenant_id=OTHER)))


def test_async_project_from_a_pooled_project_leaves_the_pool_owner_open(broker) -> None:
    broker.authn.key_principals["k"] = _principal()

    async def scenario(pool) -> None:
        project = await asyncio.to_thread(pool.get, TENANT)
        async with UdbAsyncProject.from_project(project, owns=False) as view:
            await view.data.select(message_type="m")
        assert project.credential_error() is None
        project.ready()

    with api_key_tenant_pool(UdbConfig(target=broker.target, deadline=TIMEOUT), {TENANT: "k"}) as pool:
        asyncio.run(scenario(pool))


def test_async_verify_without_an_owner_needs_nothing_expected(broker) -> None:
    async def scenario() -> None:
        async with UdbAsyncProject(UdbConfig(target=broker.target, tenant_id=TENANT, deadline=TIMEOUT)) as project:
            project.verify(Expect())
            assert project.principal() is None and project.credential_error() is None
            with pytest.raises(UdbConfigurationError, match="no verified principal"):
                project.verify(Expect(tenant_id=TENANT))

    asyncio.run(scenario())


# ── tenant pool ─────────────────────────────────────────────────────────────
class _Client:
    def __init__(self, tenant: str = "") -> None:
        self.tenant = tenant
        self.closed = threading.Event()
        self.error = None

    def credential_error(self):
        return self.error

    def close(self) -> None:
        self.closed.set()


def test_pool_shares_one_connect_across_concurrent_first_calls() -> None:
    calls: list[str] = []

    def connect(tenant):
        calls.append(tenant)
        time.sleep(0.1)
        return _Client(tenant)

    with TenantSessionPool(connect) as pool:
        results: list = []
        threads = [threading.Thread(target=lambda: results.append(pool.get(TENANT))) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert calls == [TENANT]
        assert all(result is results[0] for result in results)
        assert pool.get(TENANT) is results[0]
        pool.get(OTHER)
        assert pool.tenants() == sorted([TENANT, OTHER])


def test_pool_replaces_a_client_whose_credential_is_failing_and_closes_it() -> None:
    clients: list[_Client] = []

    def connect(tenant):
        clients.append(_Client(tenant))
        return clients[-1]

    with TenantSessionPool(connect) as pool:
        first = pool.get(TENANT)
        first.error = RuntimeError("refresh failing")
        second = pool.get(TENANT)
        assert second is not first
        assert first.closed.wait(TIMEOUT)
        assert not second.closed.is_set()


def test_pool_raises_the_connect_error_to_every_waiter_and_retries_next_time() -> None:
    attempts: list[int] = []

    def connect(_tenant):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("dial failed")
        return _Client()

    with TenantSessionPool(connect) as pool:
        with pytest.raises(RuntimeError, match="dial failed"):
            pool.get(TENANT)
        assert pool.get(TENANT) is not None
        assert pool.tenants() == [TENANT]


def test_pool_refuses_a_connect_that_returns_nothing() -> None:
    with TenantSessionPool(lambda _tenant: None) as pool:
        with pytest.raises(UdbError, match="returned no client"):
            pool.get(TENANT)


def test_pool_timeout_by_the_initiator_abandons_the_connect_and_closes_the_late_client() -> None:
    release = threading.Event()
    late = _Client()
    calls: list[int] = []

    def connect(_tenant):
        calls.append(1)
        if len(calls) == 1:
            release.wait(TIMEOUT)
            return late
        return _Client()

    with TenantSessionPool(connect) as pool:
        with pytest.raises(TimeoutError):
            pool.get(TENANT, timeout=0.05)
        # The abandoned dial no longer blocks a new generation.
        replacement = pool.get(TENANT, timeout=TIMEOUT)
        assert replacement is not late
        release.set()
        assert late.closed.wait(TIMEOUT)
        assert pool.get(TENANT) is replacement


def test_pool_timeout_by_a_follower_leaves_the_connect_running() -> None:
    release = threading.Event()
    results: list = []

    def connect(_tenant):
        release.wait(TIMEOUT)
        return _Client()

    with TenantSessionPool(connect) as pool:
        initiator = threading.Thread(target=lambda: results.append(pool.get(TENANT)))
        initiator.start()
        assert _wait_until(lambda: TENANT in pool._pending)
        with pytest.raises(TimeoutError):
            pool.get(TENANT, timeout=0.05)
        release.set()
        initiator.join()
        assert pool.get(TENANT) is results[0]


def test_pool_evict_closes_and_refuses_a_pending_dial_and_reconnects() -> None:
    clients: list[_Client] = []
    release = threading.Event()

    def connect(tenant):
        release.wait(TIMEOUT)
        clients.append(_Client(tenant))
        return clients[-1]

    pool = TenantSessionPool(connect)
    errors: list[BaseException] = []

    def waiter():
        try:
            pool.get(TENANT)
        except BaseException as error:  # noqa: BLE001 - recorded for the assertion
            errors.append(error)

    thread = threading.Thread(target=waiter)
    thread.start()
    assert _wait_until(lambda: TENANT in pool._pending)
    pool.evict(TENANT)
    thread.join(TIMEOUT)
    assert "evicted" in str(errors[0])
    release.set()
    assert _wait_until(lambda: bool(clients) and clients[0].closed.is_set())
    assert pool.tenants() == []
    second = pool.get(TENANT)
    pool.evict(TENANT)
    assert second.closed.is_set()
    pool.close()


def test_pool_close_closes_every_client_and_refuses_later_gets() -> None:
    pool = TenantSessionPool(_Client)
    a, b = pool.get(TENANT), pool.get(OTHER)
    pool.close()
    assert a.closed.is_set() and b.closed.is_set()
    with pytest.raises(UdbError, match="closed"):
        pool.get(TENANT)


def test_api_key_pool_refuses_a_tenant_without_a_key_and_owns_its_key_map(broker) -> None:
    broker.authn.key_principals["k"] = _principal()
    keys = {TENANT: "k", OTHER: ""}
    with api_key_tenant_pool(UdbConfig(target=broker.target, deadline=TIMEOUT), keys) as pool:
        keys.clear()
        project = pool.get(TENANT)
        assert project.principal().tenant_id == TENANT
        assert pool.get(TENANT) is project
        assert broker.authn.exchanges == 1
        with pytest.raises(UdbConfigurationError, match="no API key"):
            pool.get(OTHER)
        with pytest.raises(UdbConfigurationError, match="no API key"):
            pool.get("unknown-tenant")


def test_api_key_pool_refuses_a_key_that_verifies_as_another_tenant(broker) -> None:
    broker.authn.key_principals["k"] = _principal(tenant_id=OTHER)
    with api_key_tenant_pool(UdbConfig(target=broker.target, deadline=TIMEOUT), {TENANT: "k"}) as pool:
        with pytest.raises(UdbConfigurationError, match="does not match"):
            pool.get(TENANT)
        assert pool.tenants() == []


# ── live query ──────────────────────────────────────────────────────────────
def _snapshot(*rows: str):
    def step(_context):
        yield lq_pb.SubscribeResponse(snapshot=lq_pb.LiveQuerySnapshot(rows_json=list(rows)))

    return step


def _project(broker, **overrides) -> UdbProject:
    values = dict(target=broker.target, tenant_id=TENANT, project_id=PROJECT, deadline=TIMEOUT)
    values.update(overrides)
    return UdbProject(UdbConfig(**values))


def _no_feed(_context_or_none=None):
    def step(context):
        yield from ()
        trailer = error_pb2.ErrorDetail(capability_required="cdc_change_feed").SerializeToString()
        context.set_trailing_metadata((("udb-error-detail-bin", trailer),))
        context.abort(grpc.StatusCode.FAILED_PRECONDITION, "no feed")

    return step


def test_live_query_builds_the_request_and_delivers_snapshot_then_changes(broker) -> None:
    def changes(_context):
        yield lq_pb.SubscribeResponse(heartbeat=lq_pb.LiveQueryHeartbeat())
        yield lq_pb.SubscribeResponse(change=lq_pb.LiveQueryChange())  # legacy keepalive
        yield lq_pb.SubscribeResponse(
            change=lq_pb.LiveQueryChange(
                op=lq_pb.LIVE_QUERY_CHANGE_OP_INSERT,
                row_json='{"record_id":"r2","revision":"5"}',
                event_id="e1",
            )
        )

    broker.live.script = [
        lambda ctx: (yield from (*_snapshot('{"record_id":"r1"}')(ctx), *changes(ctx))),
    ]
    with _project(broker) as project:
        query = project.live_query(
            sdk_live_pb2.SdkLiveRecord,
            where=[lq_eq("payload", "x"), lq_is_null("blob_ref")],
            any_of=[[lq_in("record_id", "a", "b")]],
            snapshot_limit=5,
        )
        events = []
        for event in query:
            events.append(event)
            if len(events) == 2:
                query.cancel()
    request = broker.live.requests[0]
    assert request.tenant_id == TENANT and request.project_id == PROJECT
    assert request.message_type == "udb.sdk.live.v1.SdkLiveRecord" and request.snapshot_limit == 5
    assert [p.field for p in request.filters] == ["payload", "blob_ref"]
    assert list(request.any_of[0].predicates[0].values) == ["a", "b"]
    assert events[0].is_snapshot and events[0].resumed is False
    assert events[0].snapshot[0].record_id == "r1"
    assert not events[1].is_snapshot and events[1].event_id == "e1"
    assert events[1].row.record_id == "r2" and events[1].row.revision == 5
    assert query.last_event_id == "e1"


def test_live_query_reconnects_resuming_after_the_last_delivered_change(broker) -> None:
    def first(_context):
        yield from _snapshot('{"a":1}')(_context)
        yield lq_pb.SubscribeResponse(
            change=lq_pb.LiveQueryChange(op=lq_pb.LIVE_QUERY_CHANGE_OP_UPDATE, row_json='{"a":2}', event_id="e9")
        )

    def second(context):
        yield from _snapshot('{"a":2}')(context)

    broker.live.script = [first, second]
    observed: list[Exception] = []
    with _project(broker) as project:
        query = project.live_query("x.v1.Row", backoff=0.01, on_error=observed.append)
        events = []
        for event in query:
            events.append(event)
            if len(events) == 3:
                query.cancel()
    assert events[0].snapshot == [{"a": 1}] and events[1].row == {"a": 2}
    assert events[2].resumed is True and events[2].snapshot == [{"a": 2}]
    assert broker.live.requests[0].since_event_id == ""
    assert "x-udb-livequery-resume" not in broker.live.headers[0]
    assert broker.live.requests[1].since_event_id == "e9"
    assert broker.live.headers[1]["x-udb-livequery-resume"] == "e9"
    assert "reconnecting" in str(observed[0]) and "x.v1.Row" in str(observed[0])


def test_live_query_since_event_id_resumes_an_earlier_process(broker) -> None:
    broker.live.script = [_snapshot()]
    with _project(broker) as project:
        query = project.live_query("x.v1.Row", since_event_id="e5")
        for _event in query:
            query.cancel()
    assert broker.live.requests[0].since_event_id == "e5"


def test_live_query_without_a_feed_raises_after_the_snapshot(broker) -> None:
    def step(context):
        yield from _snapshot('{"a":1}')(context)
        yield from _no_feed()(context)

    broker.live.script = [step]
    with _project(broker) as project:
        events = []
        with pytest.raises(UdbNoDeltaFeed, match="change feed"):
            for event in project.live_query("x.v1.Row"):
                events.append(event)
    assert len(events) == 1 and len(broker.live.requests) == 1


@pytest.mark.parametrize(
    "code",
    [
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.NOT_FOUND,
        grpc.StatusCode.UNIMPLEMENTED,
        grpc.StatusCode.FAILED_PRECONDITION,
    ],
)
def test_live_query_refusals_are_not_retried(broker, code) -> None:
    def step(context):
        yield from ()
        context.abort(code, "refused")

    broker.live.script = [step]
    with _project(broker) as project:
        with pytest.raises(UdbRpcError) as refused:
            list(project.live_query("x.v1.Row", backoff=0.01))
    assert refused.value.code == code and not isinstance(refused.value, UdbNoDeltaFeed)
    assert len(broker.live.requests) == 1


def test_live_query_honors_the_brokers_retry_window(broker) -> None:
    def step(context):
        yield from ()
        trailer = error_pb2.ErrorDetail(retryable=True, retry_after_ms=300).SerializeToString()
        context.set_trailing_metadata((("udb-error-detail-bin", trailer),))
        context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "admission window")

    broker.live.script = [step, _snapshot()]
    with _project(broker) as project:
        query = project.live_query("x.v1.Row", backoff=0.01)
        for _event in query:
            query.cancel()
    assert broker.live.times[1] - broker.live.times[0] >= 0.29


def test_live_query_undecodable_rows_stop_the_view(broker) -> None:
    for rows in ("[1,2]", "not json"):
        broker.live.script = [_snapshot(rows)]
        with _project(broker) as project:
            with pytest.raises(UdbError, match="not a JSON object"):
                list(project.live_query("x.v1.Row"))


def test_live_query_run_stops_when_the_handler_raises_and_cancels_the_stream(broker) -> None:
    broker.live.script = [_snapshot('{"a":1}')]
    with _project(broker) as project:
        query = project.live_query("x.v1.Row")

        def handle(_event):
            raise KeyError("handler bug")

        with pytest.raises(KeyError):
            query.run(handle)
    assert len(broker.live.requests) == 1


def test_live_query_cancel_from_another_thread_ends_a_quiet_stream(broker) -> None:
    with _project(broker) as project:
        query = project.live_query("x.v1.Row")
        done = threading.Event()
        worker = threading.Thread(target=lambda: (list(query), done.set()))
        worker.start()
        assert _wait_until(lambda: broker.live.requests)
        query.cancel()
        assert done.wait(TIMEOUT)


def test_live_query_on_a_session_reconnects_with_the_current_bearer(broker) -> None:
    def step(context):
        yield from ()
        context.abort(grpc.StatusCode.UNAVAILABLE, "restarting")

    broker.live.script = [step, _snapshot()]
    with _connect(broker) as session:
        seen = []

        def on_error(_error):
            seen.append(True)
            session.refresh_if_needed(force=True)

        query = session.live_query("x.v1.Row", backoff=0.01, on_error=on_error)
        for _event in query:
            query.cancel()
    assert seen
    assert broker.live.headers[0]["authorization"] == "Bearer access-1"
    assert broker.live.headers[1]["authorization"] == "Bearer access-2"


# ── graph and vector builders ───────────────────────────────────────────────
def test_graph_from_builds_the_typed_traversal(broker) -> None:
    with _project(broker) as project:
        request = (
            project.graph_from(file_pb2.File, "f-1")
            .out("RELATED", "CITES")
            .depth(1, 2)
            .labels("Note")
            .where("archived", "false")
            .where_edge("weight", "1")
            .limit(25)
            .with_edges()
            .request()
        )
    traversal = request.traversal
    assert traversal.start_label == graph_label(file_pb2.File) != ""
    assert traversal.start_id == "f-1"
    assert traversal.direction == stores_pb2.GRAPH_TRAVERSAL_DIRECTION_OUTGOING
    assert list(traversal.relationship_types) == ["RELATED", "CITES"]
    assert (traversal.min_depth, traversal.max_depth, traversal.limit) == (1, 2, 25)
    assert traversal.node_property_equals["archived"] == "false"
    assert traversal.relationship_property_equals["weight"] == "1"
    assert traversal.return_relationships is True and request.read_only is True and request.limit == 25
    assert request.resource.backend == "neo4j"
    assert request.resource.message_type == file_pb2.File.DESCRIPTOR.full_name


def test_graph_direction_and_run_and_missing_start(broker) -> None:
    with _project(broker) as project:
        rows = project.graph_from(file_pb2.File, "f-1").in_("A").both("B").run()
        assert rows == [{"note_id": "n-1"}]
        sent = broker.data.calls[-1][1]
        assert sent.traversal.direction == stores_pb2.GRAPH_TRAVERSAL_DIRECTION_BOTH
        assert list(sent.traversal.relationship_types) == ["A", "B"]
        assert sent.context.tenant_id == TENANT
        calls = len(broker.data.calls)
        with pytest.raises(UdbConfigurationError, match="start node id"):
            project.graph_from(file_pb2.File, "").run()
        assert len(broker.data.calls) == calls


def test_vectors_hybrid_builds_the_request(broker) -> None:
    with _project(broker) as project:
        request = (
            project.vectors_hybrid("notes")
            .text("rotate keys")
            .near([0.1, 0.2])
            .weights(0.7, 0.3)
            .filter({"topic": "security"})
            .limit(5)
            .request()
        )
        defaults = project.vectors_hybrid("notes").request()
    assert request.collection == "notes" and request.text_query == "rotate keys"
    assert len(request.vector) == 2 and request.limit == 5 and request.with_payload is True
    assert request.fusion_weights[0] == pytest.approx(0.7)
    assert request.filter["topic"] == "security"
    assert defaults.limit == 10 and defaults.with_payload is True


def test_vectors_hybrid_refuses_an_empty_search_and_decodes_hits(broker) -> None:
    point = vector_pb2.VectorPoint(id="p1", score=0.5)
    point.payload.update({"record_id": "r1", "unknown": "dropped"})
    broker.data.points = [point]
    with _project(broker) as project:
        calls = len(broker.data.calls)
        with pytest.raises(UdbConfigurationError, match="near\\(vector\\) and/or text\\(query\\)"):
            project.vectors_hybrid("notes").run()
        assert len(broker.data.calls) == calls
        typed = project.vectors_hybrid("notes", sdk_live_pb2.SdkLiveRecord).text("q").run()
        assert typed[0].id == "p1" and typed[0].score == pytest.approx(0.5)
        assert typed[0].row.record_id == "r1"
        assert typed[0].payload == {"record_id": "r1", "unknown": "dropped"}
        untyped = project.vectors_hybrid("notes").near([0.1]).run()
        assert untyped[0].row == {"record_id": "r1", "unknown": "dropped"}


def test_vector_hit_that_does_not_decode_names_the_point(broker) -> None:
    point = vector_pb2.VectorPoint(id="p1")
    point.payload.update({"revision": "not-a-number"})
    broker.data.points = [point]
    with _project(broker) as project:
        with pytest.raises(UdbError, match="vector hit p1"):
            project.vectors_hybrid("notes", sdk_live_pb2.SdkLiveRecord).text("q").run()


# ── flags, migrations, delete mode, generated ───────────────────────────────
def test_flags_evaluate_sends_scope_and_attrs_and_omits_unknown_keys(broker) -> None:
    with _project(broker) as project:
        values = project.flags.evaluate("on", "missing", attrs={"user_id": "u"})
        assert set(values) == {"on"} and values["on"].bool_value is True
        request = broker.config.requests[-1]
        assert request.tenant_id == TENANT and list(request.keys) == ["on", "missing"]
        assert request.context.project_id == PROJECT and request.context.attributes["user_id"] == "u"
        for _ in range(3):
            assert project.flags.enabled("on") is True
        assert project.flags.enabled("off") is False
        assert project.flags.enabled("missing") is False


def test_apply_current_chains_plan_approve_apply_with_the_body_token(broker) -> None:
    with _project(broker) as project:
        status = project.admin.apply_current(PROJECT)
    assert status.state == "APPLIED" and status.run_id == "run-1"
    steps = [name for name, _ in broker.data.calls]
    assert steps == ["plan", "approve", "apply"]
    assert broker.data.calls[0][1].project_id == PROJECT
    assert broker.data.calls[1][1].run_id == "run-1"
    assert broker.data.calls[2][1].approval_token == "approval-1"


def test_apply_current_refuses_a_plan_without_a_run_id(broker) -> None:
    broker.data.run_id = ""
    with _project(broker) as project:
        with pytest.raises(UdbError, match="no run id"):
            project.admin.apply_current(PROJECT)
    assert [name for name, _ in broker.data.calls] == ["plan"]


def test_delete_file_mode_carries_the_mode_and_reason(broker) -> None:
    with _project(broker) as project:
        project.storage.delete_file_mode("f-1", storage_pb.DELETE_MODE_HARD, reason="gdpr")
        project.storage.delete_file_mode("f-2", storage_pb.DELETE_MODE_SOFT)
        project.storage.delete_file("f-3")
    hard, soft, default = broker.storage.requests
    assert (hard.tenant_id, hard.file_id, hard.mode, hard.reason) == (
        TENANT, "f-1", storage_pb.DELETE_MODE_HARD, "gdpr",
    )
    assert soft.mode == storage_pb.DELETE_MODE_SOFT and soft.reason == ""
    assert default.mode == storage_pb.DELETE_MODE_UNSPECIFIED


def test_generated_shares_channels_and_follows_metadata(broker) -> None:
    with _connect(broker) as session:
        generated = session.generated
        assert session.generated is generated
        generated.ConfigService.evaluate_flags(config_pb.EvaluateFlagsRequest(tenant_id=TENANT, keys=["on"]))
        assert broker.config.headers[-1]["authorization"] == "Bearer access-1"
        session.refresh_if_needed(force=True)
        generated.ConfigService.evaluate_flags(config_pb.EvaluateFlagsRequest(tenant_id=TENANT, keys=["on"]))
        assert broker.config.headers[-1]["authorization"] == "Bearer access-2"
        session.bind_metadata(session.metadata.with_purpose("rebound"))
        generated.DataBroker.select(types_pb2.SelectRequest(message_type="m"))
        assert broker.data.headers[-1]["x-purpose"] == "rebound"


def test_generated_routes_the_data_plane_and_native_services_to_their_listeners() -> None:
    data_listener, native_listener = Broker(), Broker()
    try:
        with UdbProject(
            UdbConfig(
                target=data_listener.target,
                auth_target=native_listener.target,
                tenant_id=TENANT,
                deadline=TIMEOUT,
            )
        ) as project:
            project.generated.DataBroker.select(types_pb2.SelectRequest(message_type="m"))
            project.generated.ConfigService.evaluate_flags(
                config_pb.EvaluateFlagsRequest(tenant_id=TENANT, keys=["on"])
            )
        assert len(data_listener.data.headers) == 1 and native_listener.data.headers == []
        assert len(native_listener.config.requests) == 1 and data_listener.config.requests == []
    finally:
        data_listener.stop()
        native_listener.stop()


# ── schema reflection ───────────────────────────────────────────────────────
_FIXTURES = iter(range(1_000_000))


def _entity(configure):
    """A copy of SdkLiveRecord whose annotations ``configure`` rewrites, registered
    under its own name."""
    model = descriptor_pb2.FileDescriptorProto()
    sdk_live_pb2.SdkLiveRecord.DESCRIPTOR.file.CopyToProto(model)
    number = next(_FIXTURES)
    model.name = f"parity/fixture_{number}.proto"
    model.package = f"parity.fixture{number}"
    entity = next(m for m in model.message_type if m.name == "SdkLiveRecord")
    del model.message_type[:]
    model.message_type.add().CopyFrom(entity)
    entity = model.message_type[0]
    table = entity.options.Extensions[db_pb2.pg_table]
    del table.indexes[:]

    def column(name):
        field = next(f for f in entity.field if f.name == name)
        return field.options.Extensions[db_pb2.pg_column]

    configure(column, table)
    pool = descriptor_pool.Default()
    pool.Add(model)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName(f"{model.package}.SdkLiveRecord"))


def test_unique_keys_follow_effective_declared_constraints() -> None:
    def configure(column, table):
        column("lookup_key").column_name = "lookup_sql"
        column("record_id").column_name = "stored_id"
        column("payload").column_name = "payload_sql"
        column("payload").index.CopyFrom(
            db_pb2.IndexOptions(index_name="field_composite", unique=True, columns=["payload_sql", "revision"])
        )
        table.indexes.extend(
            [
                db_pb2.IndexOptions(index_name="duplicate", unique=True, columns=["lookup_sql"]),
                db_pb2.IndexOptions(index_name="reversed_duplicate", unique=True, columns=["revision", "payload"]),
                db_pb2.IndexOptions(index_name="composite", unique=True, composite_fields=["revision, lookup_sql"]),
                db_pb2.IndexOptions(index_name="pk", unique=True, columns=["stored_id"]),
                db_pb2.IndexOptions(index_name="partial", unique=True, columns=["payload"], where_clause="revision > 0"),
                db_pb2.IndexOptions(
                    index_name="expression",
                    unique=True,
                    columns=["date_trunc('day', revision)", "lower(payload)"],
                ),
            ]
        )

    message = _entity(configure)
    want = [["lookup_key"], ["payload", "revision"], ["revision", "lookup_key"]]
    keys = unique_keys(message)
    assert keys == want
    keys[0][0] = "mutated"
    assert unique_keys(message) == want
    assert unique_keys(message()) == want


def test_unique_keys_include_the_partition_column_and_exclude_partial_only() -> None:
    def configure(_column, table):
        table.partition_strategy = db_pb2.PARTITION_STRATEGY_HASH
        table.partition_column = "project_id"
        table.indexes.add(unique=True, columns=["payload"], where_clause="revision > 0")

    assert unique_keys(_entity(configure)) == [["lookup_key", "project_id"]]


@pytest.mark.parametrize("ambiguous", [False, True])
def test_unique_keys_refuse_unknown_and_ambiguous_columns(ambiguous) -> None:
    def configure(column, table):
        name = "missing_column"
        if ambiguous:
            name = "lookup_key"
            column("payload").column_name = name
        table.indexes.add(unique=True, columns=[name])

    with pytest.raises(UdbConfigurationError, match="unique key column"):
        unique_keys(_entity(configure))


def test_unique_keys_refuse_repeated_components_and_no_message() -> None:
    def configure(_column, table):
        table.indexes.add(unique=True, columns=["revision", "revision"])

    with pytest.raises(UdbConfigurationError, match="repeats field"):
        unique_keys(_entity(configure))
    with pytest.raises(UdbConfigurationError, match="no message"):
        unique_keys(None)


@pytest.mark.parametrize(
    ("alias", "normal"),
    [
        ("ExternalCode", "external_code"),
        ("_External__Code_", "external_code"),
        ("TOTPEnabled", "totp_enabled"),
        ("URLValue", "url_value"),
        ("some-field", "some_field"),
        (" __Mixed UpperCase__ ", "mixed_upper_case"),
    ],
)
def test_unique_keys_match_the_parser_alias_normalization(alias, normal) -> None:
    def configure(column, table):
        lookup = column("lookup_key")
        lookup.unique = False
        lookup.column_name = alias
        lookup.index.CopyFrom(db_pb2.IndexOptions(unique=True, columns=[alias]))
        table.indexes.add(unique=True, columns=[normal])

    assert unique_keys(_entity(configure)) == [["lookup_key"]]


def test_max_len_reads_a_varchar_bound_only() -> None:
    def configure(column, _table):
        column("payload").sql_type = "VARCHAR(64)"
        column("lookup_key").sql_type = "TEXT"
        column("record_id").sql_type = "VARCHAR(abc)"

    message = _entity(configure)
    assert max_len(message, "payload") == 64
    assert max_len(message(), "payload") == 64
    assert max_len(message, "lookup_key") == 0
    assert max_len(message, "record_id") == 0
    assert max_len(message, "no_such_field") == 0


def test_apply_column_defaults_sets_bools_and_integers_only() -> None:
    def configure(column, _table):
        column("revision").default_value = " 7 "
        column("payload").default_value = "ignored-for-text"

    message = _entity(configure)()
    apply_column_defaults(message)
    assert message.revision == 7 and message.payload == ""


@pytest.mark.parametrize("bad", ["x", "1.5", "99999999999999999999", "0x10"])
def test_apply_column_defaults_refuses_a_default_that_does_not_parse(bad) -> None:
    def configure(column, _table):
        column("revision").default_value = bad

    with pytest.raises(UdbConfigurationError, match="revision default"):
        apply_column_defaults(_entity(configure)())


def test_topic_comes_from_the_event_contract() -> None:
    assert topic(sdk_live_pb2.SdkLiveRecord) == ""


# ── gRPC server interceptors ────────────────────────────────────────────────
def _echo_server(behaviors: dict, interceptor) -> tuple[grpc.Server, grpc.Channel]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), interceptors=[interceptor])
    server.add_generic_rpc_handlers(
        (grpc.method_handlers_generic_handler("parity.Echo", behaviors),)
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, grpc.insecure_channel(f"127.0.0.1:{port}")


def _inbound(**extra) -> list[tuple[str, str]]:
    headers = dict(
        {
            "x-tenant-id": TENANT,
            "x-user-id": USER,
            "x-purpose": "billing",
            "x-correlation-id": "corr-1",
            "x-service-identity": "svc",
            "x-udb-project-id": PROJECT,
            "x-udb-client-catalog-version": "1.0.0",
            "x-scopes": "a,,b",
            "authorization": "Bearer never-lifted",
        }
    )
    headers.update(extra)
    return [(key, value) for key, value in headers.items() if value is not None]


def test_server_interceptor_exposes_the_context_on_every_handler_shape() -> None:
    seen: list = []

    def unary(_request, _context):
        seen.append(metadata_from_context())
        return b"ok"

    def server_stream(_request, _context):
        # A generator body runs after the handler returned to the framework.
        seen.append(request_tenant_id())
        yield b"1"
        seen.append(correlation_id())
        yield b"2"

    def client_stream(requests, _context):
        list(requests)
        seen.append(request_tenant_id())
        return b"ok"

    def bidi(requests, _context):
        for _ in requests:
            seen.append(request_tenant_id())
            yield b"x"

    plain = {"request_deserializer": bytes, "response_serializer": bytes}
    server, channel = _echo_server(
        {
            "Unary": grpc.unary_unary_rpc_method_handler(unary, **plain),
            "ServerStream": grpc.unary_stream_rpc_method_handler(server_stream, **plain),
            "ClientStream": grpc.stream_unary_rpc_method_handler(client_stream, **plain),
            "Bidi": grpc.stream_stream_rpc_method_handler(bidi, **plain),
        },
        UdbServerInterceptor(),
    )
    try:
        metadata = _inbound()
        channel.unary_unary("/parity.Echo/Unary")(b"", metadata=metadata)
        meta = seen.pop()
        assert meta.tenant_id == TENANT and meta.user_id == USER and meta.purpose == "billing"
        assert meta.correlation_id == "corr-1" and meta.service_identity == "svc"
        assert meta.project_id == PROJECT and meta.client_catalog_version == "1.0.0"
        assert tuple(meta.scopes) == ("a", "b")
        assert meta.bearer_token == "" and meta.api_key == ""
        assert list(channel.unary_stream("/parity.Echo/ServerStream")(b"", metadata=metadata)) == [b"1", b"2"]
        assert seen == [TENANT, "corr-1"]
        seen.clear()
        channel.stream_unary("/parity.Echo/ClientStream")(iter([b"a"]), metadata=metadata)
        assert seen == [TENANT]
        seen.clear()
        list(channel.stream_stream("/parity.Echo/Bidi")(iter([b"a", b"b"]), metadata=metadata))
        assert seen == [TENANT, TENANT]
    finally:
        channel.close()
        server.stop(None)
    assert from_context() is None


def test_server_interceptor_falls_back_to_the_request_id_and_handles_no_metadata() -> None:
    seen: list = []

    def unary(_request, _context):
        seen.append(metadata_from_context())
        return b"ok"

    plain = {"request_deserializer": bytes, "response_serializer": bytes}
    server, channel = _echo_server(
        {"Unary": grpc.unary_unary_rpc_method_handler(unary, **plain)}, UdbServerInterceptor()
    )
    try:
        call = channel.unary_unary("/parity.Echo/Unary")
        call(b"", metadata=[("x-request-id", "req-1")])
        call(b"")
        with pytest.raises(grpc.RpcError) as unknown:
            channel.unary_unary("/parity.Echo/Missing")(b"")
        assert unknown.value.code() == grpc.StatusCode.UNIMPLEMENTED
    finally:
        channel.close()
        server.stop(None)
    assert seen[0].correlation_id == "req-1"
    assert (seen[1].tenant_id, seen[1].correlation_id, tuple(seen[1].scopes)) == ("", "", ())
    assert seen[1].service_identity == "" and seen[1].project_id == ""


def test_async_server_interceptor_keeps_concurrent_requests_apart() -> None:
    async def scenario() -> list[str]:
        seen: list[str] = []

        async def unary(_request, _context):
            before = request_tenant_id()
            await asyncio.sleep(0.05)
            seen.append(before if before == request_tenant_id() else "leaked")
            return b"ok"

        server = grpc.aio.server(interceptors=[UdbAsyncServerInterceptor()])
        server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    "parity.Echo",
                    {
                        "Unary": grpc.unary_unary_rpc_method_handler(
                            unary, request_deserializer=bytes, response_serializer=bytes
                        )
                    },
                ),
            )
        )
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            call = channel.unary_unary("/parity.Echo/Unary")
            await asyncio.gather(
                call(b"", metadata=[("x-tenant-id", TENANT)]),
                call(b"", metadata=[("x-tenant-id", OTHER)]),
            )
        await server.stop(None)
        return seen

    assert sorted(asyncio.run(scenario())) == sorted([TENANT, OTHER])


# ── async forms ─────────────────────────────────────────────────────────────
def test_async_project_surface_on_the_wire(broker) -> None:
    point = vector_pb2.VectorPoint(id="p1", score=0.25)
    point.payload.update({"record_id": "r1"})
    broker.data.points = [point]
    broker.live.script = [_snapshot('{"a":1}')]

    async def scenario() -> None:
        async with UdbAsyncProject(
            UdbConfig(target=broker.target, tenant_id=TENANT, project_id=PROJECT, deadline=TIMEOUT)
        ) as project:
            await project.ready()
            assert await project.flags.enabled("on") is True
            assert await project.flags.enabled("missing") is False
            status = await project.admin.apply_current(PROJECT)
            assert status.state == "APPLIED"
            assert await project.graph_from(file_pb2.File, "f-1").run() == [{"note_id": "n-1"}]
            with pytest.raises(UdbConfigurationError):
                await project.graph_from(file_pb2.File, "").run()
            hits = await project.vectors_hybrid("notes", sdk_live_pb2.SdkLiveRecord).text("q").run()
            assert hits[0].row.record_id == "r1"
            with pytest.raises(UdbConfigurationError):
                await project.vectors_hybrid("notes").run()
            delegated = project.as_user("Bearer user-token")
            assert delegated.bearer_token == "user-token" and delegated.user_id == ""
            with pytest.raises(UdbCredentialError):
                project.as_user(" ")
            await project.data.select(message_type="m", metadata=delegated)
            query = project.live_query("x.v1.Row")
            async for event in query:
                assert event.snapshot == [{"a": 1}]
                query.cancel()

    asyncio.run(scenario())
    assert broker.data.headers[-1]["authorization"] == "Bearer user-token"
    assert broker.live.requests[0].message_type == "x.v1.Row"


def test_async_flags_and_live_query_use_a_separate_auth_target() -> None:
    data_listener, native_listener = Broker(), Broker()

    def refused(context):
        yield from ()
        context.abort(grpc.StatusCode.PERMISSION_DENIED, "no")

    native_listener.live.script = [refused]

    async def scenario() -> None:
        async with UdbAsyncProject(
            UdbConfig(
                target=data_listener.target,
                auth_target=native_listener.target,
                tenant_id=TENANT,
                deadline=TIMEOUT,
            )
        ) as project:
            assert await project.flags.enabled("on") is True
            with pytest.raises(UdbRpcError) as denied:
                async for _event in project.live_query("x.v1.Row"):
                    pass
            assert denied.value.code == grpc.StatusCode.PERMISSION_DENIED

    try:
        asyncio.run(scenario())
        assert len(native_listener.config.requests) == 1 and data_listener.config.requests == []
    finally:
        data_listener.stop()
        native_listener.stop()
