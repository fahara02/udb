"""Live conformance for the session, pool, environment and builder surface.

Skipped unless ``UDB_LIVE_SDK_TESTS=1``. Runs against a Postgres-only broker, the
same posture as CI's "SDK refresh-token rotation (live)" job. A backend that posture
does not have (a graph store, a vector store, an object store) skips only that
part, with the reason stated.

Every test repeats the action and proves a refusal, because a client bug that only
shows on the second use (a spent refresh token, a stale bearer) passes a single-use
test. The service account, API key, flags and locks are created here and removed
afterwards. The operator's own sessions are left alone: only a session this test
logged in is ever revoked.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from concurrent import futures
from dataclasses import replace

import grpc
import pytest

from udb.core.apikey.services.v1 import core_pb2 as apikey_pb
from udb.core.authn.entity.v1 import enums_pb2 as authn_enums
from udb.core.authn.services.v1 import authn_service_pb2 as authn_service_pb
from udb.core.authn.services.v1 import core_pb2 as authn_pb
from udb.core.common.v1 import types_pb2 as common_pb
from udb.core.config.services.v1 import config_service_pb2 as config_pb
from udb.core.lock.entity.v1 import lock_pb2
from udb.core.lock.services.v1 import lock_service_pb2 as lock_pb
from udb.core.storage.entity.v1 import enums_pb2 as file_enums
from udb.core.storage.entity.v1 import file_pb2
from udb.core.storage.services.v1 import storage_service_pb2 as storage_pb
from udb.entity.v1 import types_pb2
from udb_client import (
    EnterpriseSession,
    Expect,
    Metadata,
    UdbAsyncProject,
    UdbConfig,
    UdbCredentialBindingError,
    UdbProject,
    UdbConfigurationError,
    UdbCredentialError,
    UdbError,
    UdbNoDeltaFeed,
    UdbRpcError,
    UdbTenantError,
    api_key_tenant_pool,
    connect_enterprise,
    connect_enterprise_async,
    connect_from_env,
    connect_from_env_async,
    lq_eq,
)
from udb_client.adapters import UdbServerInterceptor, metadata_from_context
from udb_client.exceptions import UdbNotFound

pytestmark = pytest.mark.skipif(
    os.getenv("UDB_LIVE_SDK_TESTS") != "1",
    reason="requires live UDB broker",
)

REPEATS = 3
TIMEOUT = 15.0
SERVICE_PASSWORD = "CorrectHorse1!"
LOCK_ENTITY = "udb.core.lock.entity.v1.Lock"


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise AssertionError(f"{name} is required when UDB_LIVE_SDK_TESTS=1")
    return value


def _targets() -> tuple[str, str]:
    target = required_env("UDB_GRPC_TARGET")
    return target, os.getenv("UDB_AUTH_GRPC_TARGET", "").strip() or target


def _connect(**overrides) -> EnterpriseSession:
    target, auth_target = _targets()
    values = dict(
        target=target,
        auth_target=auth_target,
        username=required_env("UDB_LIVE_USERNAME"),
        password=required_env("UDB_LIVE_PASSWORD"),
        tenant_code=required_env("UDB_LIVE_TENANT"),
        project_id=required_env("UDB_LIVE_PROJECT"),
        purpose="python.live.parity",
        deadline=TIMEOUT,
    )
    values.update(overrides)
    return connect_enterprise(**values)


def _operator_context(session: EnterpriseSession) -> common_pb.RequestContext:
    meta = session.metadata
    return common_pb.RequestContext(
        user_id=meta.user_id,
        principal_id=meta.user_id,
        purpose=meta.purpose,
        tenant=common_pb.TenantContext(tenant_id=meta.tenant_id, project_id=meta.project_id),
    )


def _native(session: EnterpriseSession):
    return session.native_metadata().to_grpc_metadata()


@pytest.fixture(scope="module")
def operator():
    session = _connect()
    yield session
    session.close()


@pytest.fixture(scope="module")
def service(operator):
    """A real SERVICE_ACCOUNT with a narrow ``udb:read`` grant and its own API key."""
    meta = operator.metadata
    authn = operator.auth.authn
    context = _operator_context(operator)
    name = f"py-parity-{uuid.uuid4().hex}"
    created = authn.CreateUser(
        authn_pb.CreateUserRequest(
            username=name,
            email=f"{name}@example.invalid",
            password=SERVICE_PASSWORD,
            tenant_id=meta.tenant_id,
            project_id=meta.project_id,
            full_name="Python parity service account",
            account_kind=authn_enums.ACCOUNT_KIND_SERVICE_ACCOUNT,
            context=context,
        ),
        metadata=_native(operator),
        timeout=TIMEOUT,
    ).user
    user_id = created.user_id
    authn.ChangeUserStatus(
        authn_pb.ChangeUserStatusRequest(
            user_id=user_id,
            new_status=authn_enums.USER_STATUS_ACTIVE,
            reason="python live parity activation",
            context=context,
        ),
        metadata=_native(operator),
        timeout=TIMEOUT,
    )
    authn.CreateServiceAccountGrant(
        authn_service_pb.CreateServiceAccountGrantRequest(
            tenant_id=meta.tenant_id,
            project_id=meta.project_id,
            user_id=user_id,
            service_identity=name,
            approved_scopes=["udb:read"],
            reason="python live parity narrow read fixture",
        ),
        metadata=_native(operator),
        timeout=TIMEOUT,
    )
    key = operator.apikey.CreateApiKey(
        apikey_pb.CreateApiKeyRequest(
            name=name,
            owner_id=user_id,
            scopes=["udb:read"],
            context=common_pb.RequestContext(
                user_id=user_id,
                principal_id=user_id,
                tenant=common_pb.TenantContext(tenant_id=meta.tenant_id, project_id=meta.project_id),
            ),
        ),
        metadata=_native(operator),
        timeout=TIMEOUT,
    )
    assert key.plain_key, "the broker returned no API key"
    yield {"name": name, "user_id": user_id, "key": key.plain_key}
    operator.apikey.RevokeApiKey(
        apikey_pb.RevokeApiKeyRequest(
            key_id=key.key.key_id, revoke_reason="python live parity cleanup", context=context
        ),
        metadata=_native(operator),
        timeout=TIMEOUT,
    )
    authn.RevokeServiceAccountGrant(
        authn_service_pb.RevokeServiceAccountGrantRequest(
            tenant_id=meta.tenant_id, user_id=user_id, reason="python live parity cleanup"
        ),
        metadata=_native(operator),
        timeout=TIMEOUT,
    )
    authn.ChangeUserStatus(
        authn_pb.ChangeUserStatusRequest(
            user_id=user_id,
            new_status=authn_enums.USER_STATUS_DEACTIVATED,
            reason="python live parity cleanup",
            context=context,
        ),
        metadata=_native(operator),
        timeout=TIMEOUT,
    )


def _service_env(monkeypatch, service, operator, **overrides) -> str:
    prefix = "PARITY_LIVE_"
    target, auth_target = _targets()
    values = dict(
        TARGET=target,
        AUTH_TARGET=auth_target,
        TENANT_ID=operator.canonical_tenant_id,
        PROJECT_ID=operator.canonical_project_id,
        PURPOSE="python.live.parity",
        API_KEY=service["key"],
        DEADLINE="15s",
    )
    values.update(overrides)
    for name, value in values.items():
        monkeypatch.setenv(prefix + name, value)
    return prefix


# ── enterprise session ──────────────────────────────────────────────────────
def test_live_enterprise_session_adopts_a_verified_tenant_and_refreshes_repeatedly(operator) -> None:
    with _connect() as session:
        assert session.canonical_tenant_id == operator.canonical_tenant_id
        assert session.canonical_tenant_id != required_env("UDB_LIVE_TENANT")
        assert session.tenant_state.is_verified
        assert session.principal().tenant_id == session.canonical_tenant_id
        session.validate_tenant(session.canonical_tenant_id)
        with pytest.raises(UdbTenantError, match="mismatch"):
            session.validate_tenant(required_env("UDB_LIVE_TENANT"))

        previous = session.bearer()
        for attempt in range(1, REPEATS + 1):
            session.refresh_if_needed(force=True)
            bearer = session.bearer()
            assert bearer != previous, f"refresh {attempt} did not replace the bearer"
            assert session.refresh_error() is None, f"refresh {attempt} recorded a failure"
            verified = session.auth.authn.ValidateToken(
                authn_pb.ValidateTokenRequest(
                    token=bearer.removeprefix("Bearer "), token_type=authn_enums.TOKEN_TYPE_JWT_ACCESS
                ),
                metadata=session.native_metadata().to_grpc_metadata(),
                timeout=TIMEOUT,
            )
            assert verified.valid, f"refresh {attempt}: the session's call did not validate"
            assert session.flags.enabled(f"py.parity.absent.{uuid.uuid4().hex}") is False
            previous = bearer
        session.ready()


def test_live_enterprise_session_refuses_a_wrong_password_and_missing_fields() -> None:
    with pytest.raises(UdbRpcError) as refused:
        _connect(password="definitely-not-the-password")
    assert refused.value.code == grpc.StatusCode.UNAUTHENTICATED, refused.value.details
    with pytest.raises(UdbConfigurationError, match="requires target, username, and password"):
        _connect(username="")


def test_live_enterprise_session_logs_in_again_after_its_session_is_revoked() -> None:
    with _connect() as session:
        for attempt in range(1, REPEATS + 1):
            meta = session.metadata
            revoked = session.auth.authn.RevokeSession(
                authn_pb.RevokeSessionRequest(
                    session_id=session._token.session_id,
                    principal_id=meta.user_id,
                    revoke_reason="python live parity re-login proof",
                    context=_operator_context(session),
                ),
                metadata=_native(session),
                timeout=TIMEOUT,
            )
            assert revoked.revoked_count == 1, f"attempt {attempt}: expected to revoke one session"
            spent = session.bearer()
            session.refresh_if_needed(force=True)
            assert session.bearer() != spent, f"attempt {attempt}: the bearer did not change"
            assert session.refresh_error() is None
            assert session.flags.enabled(f"py.parity.absent.{uuid.uuid4().hex}") is False
            assert session.credential_error() is None


def test_live_enterprise_session_refuses_locally_after_close() -> None:
    session = _connect()
    session.flags.enabled("py.parity.absent")
    session.close()
    session.close()
    with pytest.raises(UdbCredentialError) as refused:
        session.flags.enabled("py.parity.absent")
    assert refused.value.code == grpc.StatusCode.CANCELLED
    with pytest.raises(UdbCredentialError):
        session.ready()


def test_live_lock_service_defaults_the_tenant_and_refuses_a_stale_fencing_token(operator) -> None:
    locks = operator.lock_service()
    name = f"py-parity-{uuid.uuid4().hex}"
    previous = 0
    for attempt in range(1, REPEATS + 1):
        acquired = locks.acquire(
            lock_pb.AcquireLockRequest(lock_name=name, owner_id="py-parity", lease_ttl_seconds=60)
        )
        assert acquired.acquired, f"attempt {attempt}: lock was not acquired"
        assert acquired.fencing_token > previous, "the fencing token must increase"
        previous = acquired.fencing_token
        locks.renew(
            lock_pb.RenewLockRequest(
                lock_name=name, owner_id="py-parity", fencing_token=acquired.fencing_token, lease_ttl_seconds=60
            )
        )
        found = locks.get(lock_pb.GetLockRequest(lock_name=name))
        assert found.found and found.lock.tenant_id == operator.canonical_tenant_id
        try:
            stale = locks.release(
                lock_pb.ReleaseLockRequest(
                    lock_name=name, owner_id="py-parity", fencing_token=acquired.fencing_token - 1
                )
            )
        except UdbRpcError:
            stale = None
        assert stale is None or not getattr(stale, "released", False), "a stale token released the lock"
        locks.release(
            lock_pb.ReleaseLockRequest(
                lock_name=name, owner_id="py-parity", fencing_token=acquired.fencing_token
            )
        )


# ── environment, verify, ready, credential introspection ────────────────────
def test_live_connect_from_env_exchanges_verifies_and_reports_ready(monkeypatch, operator, service) -> None:
    prefix = _service_env(monkeypatch, service, operator)
    expect = Expect(
        tenant_id=operator.canonical_tenant_id,
        project_id=operator.canonical_project_id,
        service_identity=service["name"],
        required_scopes=("UDB:READ",),
    )
    for attempt in range(1, REPEATS + 1):
        with connect_from_env(prefix, expect) as project:
            principal = project.principal()
            assert principal.service_identity == service["name"], f"attempt {attempt}"
            assert principal.principal_id == service["user_id"]
            assert list(principal.scopes) == ["udb:read"]
            assert project.bearer_expires_at() is not None
            assert project.credential_error() is None
            project.ready()
            project.ready()


def test_live_connect_from_env_refuses_a_wrong_principal_and_a_bad_key(monkeypatch, operator, service) -> None:
    prefix = _service_env(monkeypatch, service, operator)
    with pytest.raises(UdbConfigurationError) as refused:
        connect_from_env(
            prefix,
            Expect(service_identity="someone-else", required_scopes=("udb:admin",)),
        )
    assert "service identity is" in str(refused.value) and "udb:admin" in str(refused.value)
    prefix = _service_env(monkeypatch, service, operator, API_KEY="udbk_not_a_real_key")
    with pytest.raises(UdbRpcError) as bad_key:
        connect_from_env(prefix)
    assert bad_key.value.code in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.PERMISSION_DENIED)
    monkeypatch.delenv(prefix + "PURPOSE")
    with pytest.raises(UdbConfigurationError, match="PURPOSE is not set"):
        connect_from_env(prefix)


# ── API-key renewal ─────────────────────────────────────────────────────────
def test_live_api_key_call_renews_on_demand_and_pins_identity(monkeypatch, operator, service) -> None:
    prefix = _service_env(monkeypatch, service, operator)
    with connect_from_env(prefix) as project:
        identity = project.metadata
        # Stop the background loop so each renewal below can only be on demand.
        project._api_key_stop.set()
        project._api_key_thread.join(timeout=5)
        project._api_key_stop.clear()
        previous = dict(project.metadata.to_grpc_metadata())["authorization"]
        for attempt in range(1, REPEATS + 1):
            project._api_key_issued_at = 1.0
            project._api_key_expires_at = time.time() + 5  # due, still valid
            time.sleep(1.1)  # expiry has one-second resolution, so a new bearer differs
            current = dict(project.metadata.to_grpc_metadata())["authorization"]
            assert current != previous, f"attempt {attempt}: the call did not renew on demand"
            assert project.credential_error() is None
            verified = project.auth.authenticate_bearer(current.removeprefix("Bearer "))
            assert verified.principal.service_identity == service["name"]
            assert project.metadata == replace(identity, bearer_token=project.metadata.bearer_token)
            previous = current

        # A pin the broker's principal does not match must refuse locally.
        pinned = project._api_key_pinned
        project._api_key_pinned = replace(pinned, service_identity="someone-else")
        project._api_key_issued_at = 1.0
        project._api_key_expires_at = time.time() + 5
        with pytest.raises(UdbCredentialError):
            project.metadata.to_grpc_metadata()
        assert isinstance(project.credential_error(), UdbCredentialBindingError)
        with pytest.raises(UdbCredentialError):
            project.ready()
        project._api_key_pinned = pinned
        project._api_key_renewal_tick()
        assert project.credential_error() is None
        project.ready()

        # A key the broker refuses is a local refusal, never the old bearer.
        project._api_key_secret = "udbk_not_a_real_key"
        project._api_key_issued_at = 1.0
        project._api_key_expires_at = time.time() + 5
        with pytest.raises(UdbCredentialError) as refused:
            project.metadata.to_grpc_metadata()
        assert refused.value.code in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.PERMISSION_DENIED)
        assert "udbk_not_a_real_key" not in str(refused.value)


# ── login_and_adopt_tenant ──────────────────────────────────────────────────
def test_live_login_and_adopt_takes_the_whole_verified_identity() -> None:
    target, auth_target = _targets()
    config = UdbConfig(
        target=target,
        auth_target=auth_target,
        tenant_id=required_env("UDB_LIVE_TENANT"),
        project_id=required_env("UDB_LIVE_PROJECT"),
        user_id="hint-user",
        service_identity="hint-service",
        scopes=("hint:scope",),
        purpose="python.live.parity",
        deadline=TIMEOUT,
    )
    with UdbProject(config) as project:
        with pytest.raises(UdbRpcError) as refused:
            project.login_and_adopt_tenant(required_env("UDB_LIVE_USERNAME"), "definitely-not-the-password")
        assert refused.value.code == grpc.StatusCode.UNAUTHENTICATED, refused.value.details
        assert project.metadata.user_id == "hint-user", "a refused login must not change identity"
        for _ in range(REPEATS):
            principal = project.login_and_adopt_tenant(
                required_env("UDB_LIVE_USERNAME"), required_env("UDB_LIVE_PASSWORD")
            ).principal
            meta = project.metadata
            assert meta.tenant_id == principal.tenant_id != required_env("UDB_LIVE_TENANT")
            assert meta.project_id == principal.project_id
            assert meta.user_id == principal.user_id != "hint-user"
            assert meta.service_identity == principal.service_identity != "hint-service"
            assert tuple(meta.scopes) == tuple(principal.scopes)
            assert project.flags.enabled(f"py.parity.absent.{uuid.uuid4().hex}") is False


# ── async connection forms ──────────────────────────────────────────────────
def test_live_connect_enterprise_async(operator) -> None:
    target, auth_target = _targets()
    values = dict(
        target=target,
        auth_target=auth_target,
        username=required_env("UDB_LIVE_USERNAME"),
        tenant_code=required_env("UDB_LIVE_TENANT"),
        project_id=required_env("UDB_LIVE_PROJECT"),
        purpose="python.live.parity",
        deadline=TIMEOUT,
    )

    async def scenario() -> None:
        with pytest.raises(UdbRpcError) as refused:
            await connect_enterprise_async(password="definitely-not-the-password", **values)
        assert refused.value.code == grpc.StatusCode.UNAUTHENTICATED, refused.value.details
        project = await connect_enterprise_async(password=required_env("UDB_LIVE_PASSWORD"), **values)
        session = project.credential_owner
        assert project.metadata.tenant_id == operator.canonical_tenant_id
        for attempt in range(1, REPEATS + 1):
            previous = session.bearer()
            await asyncio.to_thread(session.refresh_if_needed, force=True)
            assert session.bearer() != previous, f"refresh {attempt} did not replace the bearer"
            await project.ready()
            assert await project.flags.enabled(f"py.parity.absent.{uuid.uuid4().hex}") is False
        await project.close()
        with pytest.raises(UdbCredentialError):
            session.ready()

    asyncio.run(scenario())


def test_live_connect_from_env_async(monkeypatch, operator, service) -> None:
    prefix = _service_env(monkeypatch, service, operator)

    async def scenario() -> None:
        with pytest.raises(UdbConfigurationError, match="does not match"):
            await connect_from_env_async(prefix, Expect(service_identity="someone-else"))
        for _ in range(REPEATS):
            async with await connect_from_env_async(prefix, Expect(service_identity=service["name"])) as project:
                assert project.principal().principal_id == service["user_id"]
                assert project.bearer_expires_at() is not None
                await project.ready()
                await project.ready()

    asyncio.run(scenario())


# ── tenant pool ─────────────────────────────────────────────────────────────
def test_live_api_key_tenant_pool_shares_refuses_and_reconnects(operator, service) -> None:
    target, auth_target = _targets()
    tenant = operator.canonical_tenant_id
    base = UdbConfig(
        target=target,
        auth_target=auth_target,
        project_id=operator.canonical_project_id,
        purpose="python.live.parity",
        service_identity="",
        deadline=TIMEOUT,
    )
    with api_key_tenant_pool(base, {tenant: service["key"]}) as pool:
        first = pool.get(tenant)
        for _ in range(REPEATS):
            assert pool.get(tenant) is first
        first.ready()
        assert pool.tenants() == [tenant]
        with pytest.raises(UdbConfigurationError, match="no API key"):
            pool.get(str(uuid.uuid4()))
        pool.evict(tenant)
        assert pool.tenants() == []
        second = pool.get(tenant)
        assert second is not first
        second.ready()

    # A key used for a tenant it does not belong to must never yield a client.
    foreign = str(uuid.uuid4())
    with api_key_tenant_pool(base, {foreign: service["key"]}) as pool:
        with pytest.raises((UdbConfigurationError, UdbRpcError)):
            pool.get(foreign)
        assert pool.tenants() == []


# ── per-request user ────────────────────────────────────────────────────────
def test_live_as_user_uses_the_users_authority_and_never_falls_back(monkeypatch, operator, service) -> None:
    meta = operator.metadata
    person = operator.auth.login(
        required_env("UDB_LIVE_USERNAME"),
        required_env("UDB_LIVE_PASSWORD"),
        device_name="py-parity-as-user",
        metadata=Metadata(
            tenant_id=meta.tenant_id,
            project_id=meta.project_id,
            purpose=meta.purpose,
            correlation_id="py-parity-as-user",
        ),
    )
    assert person.access_token and person.session_id
    prefix = _service_env(monkeypatch, service, operator)
    with connect_from_env(prefix) as project:
        before = project.metadata
        # This RPC needs authority the narrow service grant lacks, so succeeding
        # under delegation cannot be explained by the service's own bearer.
        with pytest.raises(UdbRpcError) as own:
            project.auth.validate_session(person.access_token)
        assert own.value.code == grpc.StatusCode.PERMISSION_DENIED, own.value.details

        for token in [person.access_token] * REPEATS + [f"Bearer {person.access_token}"]:
            delegated = project.auth.validate_session(
                person.access_token, metadata=project.as_user(token)
            )
            assert delegated.user_id == meta.user_id
            assert delegated.tenant_id == meta.tenant_id
            assert delegated.public_id
        assert project.metadata == before

        for empty in ("", "  ", "Bearer "):
            with pytest.raises(UdbCredentialError, match="delegated bearer is required"):
                project.as_user(empty)
        with pytest.raises(UdbRpcError) as invalid:
            project.auth.validate_session(
                person.access_token, metadata=project.as_user("not-a-valid-udb-token")
            )
        assert invalid.value.code in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.PERMISSION_DENIED)

        revoked = operator.auth.authn.RevokeSession(
            authn_pb.RevokeSessionRequest(
                session_id=person.session_id,
                principal_id=meta.user_id,
                revoke_reason="python live parity as_user proof",
                context=_operator_context(operator),
            ),
            metadata=_native(operator),
            timeout=TIMEOUT,
        )
        assert revoked.revoked_count == 1
        with pytest.raises(UdbRpcError) as after:
            project.auth.validate_session(
                person.access_token, metadata=project.as_user(person.access_token)
            )
        assert after.value.code == grpc.StatusCode.UNAUTHENTICATED, after.value.details
        # The service's own identity still works and is still narrow.
        project.ready()
        with pytest.raises(UdbRpcError) as still_own:
            project.auth.validate_session(person.access_token)
        assert still_own.value.code == grpc.StatusCode.PERMISSION_DENIED


def test_live_validate_session_reports_a_garbage_token_as_invalid(operator) -> None:
    token = operator.bearer()
    for _ in range(REPEATS):
        user = operator.auth.validate_session(token)
        assert user.user_id == operator.metadata.user_id
        assert user.tenant_id == operator.canonical_tenant_id
        assert user.expires_at is not None
    with pytest.raises((UdbNotFound, UdbRpcError)):
        operator.auth.validate_session("not-a-valid-udb-token")


# ── flags ───────────────────────────────────────────────────────────────────
def test_live_flags_evaluate_put_values_and_refuse_a_foreign_tenant(operator) -> None:
    key = f"py.parity.{uuid.uuid4().hex}"
    config = operator.generated.ConfigService
    tenant, project = operator.canonical_tenant_id, operator.canonical_project_id

    def put(enabled: bool) -> None:
        config.put_flag(
            config_pb.PutFlagRequest(
                tenant_id=tenant,
                project_id=project,
                flag_key=key,
                value=config_pb.FlagValue(bool_value=True),
                enabled=enabled,
                rollout_percentage=100,
                rollout_context_key="user_id",
                metadata_json="{}",
            )
        )

    assert operator.flags.enabled(key) is False, "a flag that does not exist is off"
    put(True)
    try:
        for _ in range(REPEATS):
            assert operator.flags.enabled(key, {"user_id": "u-1"}) is True
        values = operator.flags.evaluate(key, f"{key}.absent")
        assert set(values) == {key} and values[key].bool_value is True
        put(False)
        assert operator.flags.enabled(key) is False
        with pytest.raises(UdbRpcError) as foreign:
            operator.flags.evaluate(key, metadata=replace(operator.metadata, tenant_id=str(uuid.uuid4())))
        assert foreign.value.code in (
            grpc.StatusCode.PERMISSION_DENIED,
            grpc.StatusCode.INVALID_ARGUMENT,
            grpc.StatusCode.UNAUTHENTICATED,
        ), foreign.value.details
    finally:
        config.delete_flag(config_pb.DeleteFlagRequest(tenant_id=tenant, project_id=project, flag_key=key))
    assert operator.flags.enabled(key) is False


# ── migrations ──────────────────────────────────────────────────────────────
def test_live_apply_current_runs_the_whole_chain_repeatedly_and_refuses_another_project(operator) -> None:
    project = operator.canonical_project_id or required_env("UDB_LIVE_PROJECT")
    with pytest.raises(UdbRpcError):
        operator.admin.apply_current(f"py-parity-no-such-project-{uuid.uuid4().hex}")
    run_ids: set[str] = set()
    for attempt in range(1, REPEATS + 1):
        try:
            status = operator.admin.apply_current(project)
        except UdbRpcError as error:
            if error.code == grpc.StatusCode.FAILED_PRECONDITION and "ACTIVE project catalog" in error.details:
                pytest.skip(f"this broker has no ACTIVE catalog for project {project!r}: {error.details}")
            raise
        assert status.run_id, f"attempt {attempt}: ApplyMigration returned no run id"
        assert status.state, f"attempt {attempt}: ApplyMigration returned no state"
        run_ids.add(status.run_id)
    assert len(run_ids) == REPEATS, "each apply_current must be its own migration run"


# ── live query ──────────────────────────────────────────────────────────────
def test_live_query_delivers_the_snapshot_repeatedly_and_refuses_an_unknown_entity(operator) -> None:
    locks = operator.lock_service()
    name = f"py-parity-{uuid.uuid4().hex}"
    acquired = locks.acquire(
        lock_pb.AcquireLockRequest(lock_name=name, owner_id="py-parity", lease_ttl_seconds=120)
    )
    assert acquired.acquired
    no_feed = os.getenv("UDB_CDC_ENABLED", "").lower() == "false"
    try:
        for message, read_name in (
            (LOCK_ENTITY, lambda row: row["lock_name"]),
            (lock_pb2.Lock, lambda row: row.lock_name),
            (LOCK_ENTITY, lambda row: row["lock_name"]),
        ):
            reconnects: list[Exception] = []
            query = operator.live_query(
                message,
                where=[lq_eq("lock_name", name)],
                snapshot_limit=10,
                backoff=0.1,
                # A reconnect cancels, so a transient failure cannot hang the test.
                on_error=lambda error: (reconnects.append(error), query.cancel()),
            )
            events: list = []
            if no_feed:
                with pytest.raises(UdbNoDeltaFeed):
                    for event in query:
                        events.append(event)
            else:
                for event in query:
                    events.append(event)
                    query.cancel()
            assert not reconnects, f"unexpected reconnect: {reconnects[:1]}"
            assert events and events[0].is_snapshot and events[0].resumed is False
            assert [read_name(row) for row in events[0].snapshot] == [name]

        # A filter matching nothing is an empty snapshot, not an error.
        query = operator.live_query(LOCK_ENTITY, where=[lq_eq("lock_name", f"{name}-absent")])
        events = []
        try:
            for event in query:
                events.append(event)
                query.cancel()
        except UdbNoDeltaFeed:
            pass
        assert events and events[0].snapshot == []

        attempts: list[Exception] = []
        query = operator.live_query(
            f"udb.does.not.exist.v1.Entity{uuid.uuid4().hex}",
            on_error=lambda error: (attempts.append(error), query.cancel()),
        )
        with pytest.raises(UdbRpcError):
            list(query)
        assert not attempts, f"an unknown entity must not be retried: {attempts[:1]}"
    finally:
        locks.release(
            lock_pb.ReleaseLockRequest(lock_name=name, owner_id="py-parity", fencing_token=acquired.fencing_token)
        )


# ── graph and vector builders ───────────────────────────────────────────────
def test_live_graph_and_vector_builders(operator) -> None:
    # The refusals are local and hold in every posture.
    for _ in range(REPEATS):
        with pytest.raises(UdbConfigurationError, match="start node id"):
            operator.graph_from(file_pb2.File, "").run()
        with pytest.raises(UdbConfigurationError, match="near"):
            operator.vectors_hybrid("py_parity").run()

    enabled = set(operator.data.get_capabilities().enabled_backends)
    ran = []
    if "neo4j" in enabled:
        for _ in range(REPEATS):
            assert isinstance(
                operator.graph_from(file_pb2.File, str(uuid.uuid4())).out().limit(1).run(), list
            )
        ran.append("graph")
    if enabled & {"qdrant", "weaviate", "pinecone", "milvus", "pgvector"}:
        for _ in range(REPEATS):
            try:
                hits = operator.vectors_hybrid("py_parity_absent").text("query").limit(1).run()
            except UdbRpcError as error:
                assert error.code != grpc.StatusCode.UNKNOWN, error.details
            else:
                assert isinstance(hits, list)
        ran.append("vector")
    if len(ran) < 2:
        pytest.skip(
            f"this broker has no graph or vector store for the served half "
            f"(enabled backends: {sorted(enabled)}); ran: {ran or 'local refusals only'}"
        )


# ── delete mode ─────────────────────────────────────────────────────────────
def test_live_delete_file_mode_reaches_the_broker_and_refuses_a_missing_file(operator) -> None:
    storage = operator.storage
    with pytest.raises(UdbRpcError) as malformed:
        storage.delete_file_mode("not-a-uuid", storage_pb.DELETE_MODE_HARD)
    assert malformed.value.code == grpc.StatusCode.INVALID_ARGUMENT, malformed.value.details
    for mode in (storage_pb.DELETE_MODE_SOFT, storage_pb.DELETE_MODE_HARD):
        with pytest.raises(UdbRpcError) as missing:
            storage.delete_file_mode(str(uuid.uuid4()), mode, reason="python live parity")
        assert missing.value.code == grpc.StatusCode.NOT_FOUND, missing.value.details

    for mode in (storage_pb.DELETE_MODE_SOFT, storage_pb.DELETE_MODE_HARD, storage_pb.DELETE_MODE_SOFT):
        try:
            registered = storage.register_upload(
                f"py-parity-{uuid.uuid4().hex}.txt", content_type="text/plain", size_bytes=4
            )
        except UdbRpcError as error:
            pytest.skip(f"this broker cannot reserve an upload without an object store: {error.details}")
        if not registered.file_id:
            pytest.skip(f"this broker reserved no file row: {registered.error.message}")
        storage.delete_file_mode(registered.file_id, mode, reason="python live parity")
        try:
            found = storage.get_file(registered.file_id)
        except UdbRpcError as error:
            assert error.code == grpc.StatusCode.NOT_FOUND, error.details
        else:
            assert "DELETED" in file_enums.FileStatus.Name(found.file.status), "the file is still live"


# ── generated client ────────────────────────────────────────────────────────
def test_live_generated_client_follows_the_session_bearer(operator) -> None:
    with _connect() as session:
        generated = session.generated
        assert session.generated is generated
        for attempt in range(1, REPEATS + 1):
            session.refresh_if_needed(force=True)
            token = session.bearer().removeprefix("Bearer ")
            verified = generated.AuthnService.validate_token(
                authn_pb.ValidateTokenRequest(token=token, token_type=authn_enums.TOKEN_TYPE_JWT_ACCESS)
            )
            assert verified.valid, f"refresh {attempt}: generated call carried a stale bearer"
        assert generated.DataBroker.get_capabilities(types_pb2.CapabilitiesRequest()).enabled_backends
        session.close()
        with pytest.raises(UdbCredentialError):
            generated.AuthnService.validate_token(
                authn_pb.ValidateTokenRequest(token=token, token_type=authn_enums.TOKEN_TYPE_JWT_ACCESS)
            )


# ── gRPC server interceptor ─────────────────────────────────────────────────
def test_live_server_interceptor_reads_the_headers_a_real_session_sends(operator) -> None:
    seen: list = []

    def unary(_request, _context):
        seen.append(metadata_from_context())
        return b"ok"

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2), interceptors=[UdbServerInterceptor()])
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
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        call = channel.unary_unary("/parity.Echo/Unary")
        for _ in range(REPEATS):
            call(b"", metadata=operator.metadata.to_grpc_metadata(), timeout=TIMEOUT)
            meta = seen.pop()
            assert meta.tenant_id == operator.canonical_tenant_id
            assert meta.user_id == operator.metadata.user_id
            assert meta.purpose == "python.live.parity"
            assert meta.correlation_id
            assert meta.bearer_token == "", "a credential must not be lifted into the context"
        call(b"", timeout=TIMEOUT)
        assert seen.pop().tenant_id == ""
    finally:
        channel.close()
        server.stop(None)


# ── async forms ─────────────────────────────────────────────────────────────
def test_live_async_project_surface(operator) -> None:
    target, auth_target = _targets()
    meta = operator.metadata

    async def scenario() -> None:
        def config(bearer: str) -> UdbConfig:
            return UdbConfig(
                target=target,
                auth_target=auth_target,
                tenant_id=meta.tenant_id,
                project_id=meta.project_id,
                user_id=meta.user_id,
                service_identity=meta.service_identity,
                scopes=tuple(meta.scopes),
                purpose=meta.purpose,
                bearer_token=bearer,
                deadline=TIMEOUT,
            )

        async with UdbAsyncProject(config(operator.bearer().removeprefix("Bearer "))) as project:
            for _ in range(REPEATS):
                await project.ready()
                assert await project.flags.enabled(f"py.parity.absent.{uuid.uuid4().hex}") is False
            with pytest.raises(UdbConfigurationError):
                await project.graph_from(file_pb2.File, "").run()
            with pytest.raises(UdbConfigurationError):
                await project.vectors_hybrid("py_parity").run()
            with pytest.raises(UdbCredentialError):
                project.as_user("")
            attempts: list[Exception] = []
            query = project.live_query(
                f"udb.does.not.exist.v1.Entity{uuid.uuid4().hex}",
                on_error=lambda error: (attempts.append(error), query.cancel()),
            )
            with pytest.raises(UdbRpcError):
                async for _event in query:
                    pass
            assert not attempts
        async with UdbAsyncProject(config("not-a-valid-udb-token")) as project:
            with pytest.raises(UdbError) as refused:
                await project.flags.evaluate("py.parity.absent")
            assert getattr(refused.value, "code", None) in (
                grpc.StatusCode.UNAUTHENTICATED,
                grpc.StatusCode.PERMISSION_DENIED,
            )

    asyncio.run(scenario())
