"""Live service-account delegation conformance.

Skipped unless ``UDB_LIVE_SDK_TESTS=1``; CI's "SDK service accounts (live)"
job runs it against the broker it just built, on every push to main, after
``scripts/provision-service-account-live.py`` has created the identities it
reads from the environment.

It proves, through this SDK, the served behavior UDB 0.6.4 added:

1. ``Authenticate`` names the account (``Principal.username``).
2. A service account's bearer carries its ordinary bound roles and its
   ``udb:authz:check-access-for`` scope, but never an admin-segment role.
3. A service account holding ``udb:authz:check-access-for`` gets a
   ``CheckAccess`` evaluated for an active person of its tenant
   (``evaluated_subject``), and is refused for an unknown user id.
4. ``ExchangeServiceTokenForTenant`` turns the home bearer into a short-lived
   token of another tenant that approved a membership, and that token works
   for an authenticated call in the target tenant; a tenant with no
   membership is refused.
5. A phone-only ``CreateUser`` creates a PENDING_VERIFICATION account that a
   ``PHONE_VERIFICATION`` OTP activates.

Every authenticated call asserts the CANONICAL tenant UUID the bearer was
issued for, never the human tenant code.
"""

from __future__ import annotations

import os
import secrets
import time
import uuid
from dataclasses import replace

import grpc
import pytest

from udb.core.authn.entity.v1 import enums_pb2 as authn_enum_pb
from udb.core.authn.services.v1 import authn_service_pb2 as authn_service_pb
from udb.core.authn.services.v1 import authn_service_pb2_grpc as authn_grpc
from udb.core.authn.services.v1 import core_pb2 as authn_pb2
from udb.core.authz.services.v1 import core_pb2 as authz_pb2
from udb_client.auth import UdbAuthClient
from udb_client.exceptions import UdbRpcError
from udb_client.metadata import Metadata

pytestmark = pytest.mark.skipif(
    os.getenv("UDB_LIVE_SDK_TESTS") != "1",
    reason="requires live UDB broker",
)

CHECK_OBJECT = "sdk-live-chart"
CHECK_ACTION = "read"
CHECK_ACCESS_SCOPE = "udb:authz:check-access"
CHECK_ACCESS_FOR_SCOPE = "udb:authz:check-access-for"
#: Admin-segment role codes a service bearer must never carry.
NEVER_ON_SERVICE_BEARER = ("platform_admin", "organization_owner")
#: Ceiling of an exchanged token's lifetime (15 minutes) plus clock skew.
EXCHANGED_TOKEN_MAX_SECONDS = 15 * 60 + 60
PHONE_PASSWORD = "SdkLivePhone#2026"
TIMEOUT = 15.0


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise AssertionError(f"{name} is required when UDB_LIVE_SDK_TESTS=1")
    return value


def test_live_service_account_delegation_conformance() -> None:
    target = required_env("UDB_GRPC_TARGET")
    auth_target = os.getenv("UDB_AUTH_GRPC_TARGET", "").strip() or target
    username = required_env("UDB_LIVE_USERNAME")
    password = required_env("UDB_LIVE_PASSWORD")
    tenant_hint = required_env("UDB_LIVE_TENANT")
    project_hint = required_env("UDB_LIVE_PROJECT")
    service_username = required_env("UDB_LIVE_SERVICE_USERNAME")
    service_password = required_env("UDB_LIVE_SERVICE_PASSWORD")
    service_user_id = required_env("UDB_LIVE_SERVICE_USER_ID")
    service_role = required_env("UDB_LIVE_SERVICE_ROLE")
    withheld_role = required_env("UDB_LIVE_SERVICE_WITHHELD_ROLE")
    delegated_user_id = required_env("UDB_LIVE_DELEGATED_USER_ID")
    target_tenant = required_env("UDB_LIVE_TARGET_TENANT_ID")
    target_project = required_env("UDB_LIVE_TARGET_PROJECT")
    unapproved_tenant = required_env("UDB_LIVE_UNAPPROVED_TENANT_ID")

    # Login and Authenticate are public: the tenant/project here are hints.
    hints = Metadata(
        tenant_id=tenant_hint,
        project_id=project_hint,
        purpose="python.live.service-accounts",
        correlation_id=f"python-live-service-accounts-{uuid.uuid4().hex}",
        service_identity="python.sdk.live",
    )
    auth = UdbAuthClient(auth_target, hints, timeout=TIMEOUT)
    channel = grpc.insecure_channel(auth_target)
    try:
        authn = authn_grpc.AuthnServiceStub(channel)

        # ── 1. Principal.username ─────────────────────────────────────────
        human_login = auth.login(username, password, device_name="python-live-service-accounts")
        assert human_login.access_token, "step 1: the human login returned no access token"
        human = auth.authenticate_bearer(human_login.access_token).principal
        assert human.username.lower() == username.lower(), (
            f"step 1: Authenticate principal.username is {human.username!r}, expected {username!r}"
        )
        home_tenant = human.tenant_id
        assert home_tenant, "step 1: the human principal carries no canonical tenant_id"
        human_meta = replace(
            hints,
            tenant_id=home_tenant,
            project_id=human.project_id or project_hint,
            bearer_token=human_login.access_token,
        )

        # ── 2. The service bearer's roles, scopes and username ────────────
        service_login = auth.login(
            service_username, service_password, device_name="python-live-service-accounts"
        )
        assert service_login.access_token, "step 2: the service-account login returned no access token"
        service = auth.authenticate_bearer(service_login.access_token).principal
        assert service.username.lower() == service_username.lower(), (
            f"step 2: service principal.username is {service.username!r}, expected {service_username!r}"
        )
        assert service.user_id == service_user_id, (
            f"step 2: service principal.user_id is {service.user_id!r}, expected {service_user_id!r}"
        )
        assert service.account_kind == authn_enum_pb.ACCOUNT_KIND_SERVICE_ACCOUNT, (
            "step 2: service principal.account_kind is "
            f"{authn_enum_pb.AccountKind.Name(service.account_kind)}, expected ACCOUNT_KIND_SERVICE_ACCOUNT"
        )
        assert service.tenant_id == home_tenant, (
            f"step 2: service principal.tenant_id is {service.tenant_id!r}, expected home tenant {home_tenant!r}"
        )
        roles = list(service.roles)
        assert service_role in roles, f"step 2: the service bearer lacks bound role {service_role!r}; roles={roles}"
        for forbidden in (withheld_role, *NEVER_ON_SERVICE_BEARER):
            assert forbidden not in roles, (
                f"step 2: the service bearer carries admin-segment role {forbidden!r}; roles={roles}"
            )
        scopes = list(service.scopes)
        assert CHECK_ACCESS_FOR_SCOPE in scopes, (
            f"step 2: the service bearer lacks scope {CHECK_ACCESS_FOR_SCOPE!r}; scopes={scopes}"
        )
        service_meta = replace(
            hints,
            tenant_id=home_tenant,
            project_id=service.project_id or project_hint,
            bearer_token=service_login.access_token,
        )

        # ── 3. Delegated CheckAccess ──────────────────────────────────────
        delegated = auth.check_access(
            authz_pb2.CheckAccessRequest(
                user_id=delegated_user_id, object=CHECK_OBJECT, action=CHECK_ACTION
            ),
            metadata=service_meta,
        )
        assert delegated.evaluated_subject == delegated_user_id, (
            f"step 3: delegated CheckAccess evaluated {delegated.evaluated_subject!r}, "
            f"expected the person {delegated_user_id!r}"
        )
        unknown_user = str(uuid.uuid4())
        with pytest.raises(UdbRpcError) as unknown:
            auth.check_access(
                authz_pb2.CheckAccessRequest(user_id=unknown_user, object=CHECK_OBJECT, action=CHECK_ACTION),
                metadata=service_meta,
            )
        assert unknown.value.code == grpc.StatusCode.PERMISSION_DENIED, (
            f"step 3: delegated CheckAccess for unknown user {unknown_user} returned "
            f"{unknown.value.code.name}: {unknown.value.details}"
        )

        # ── 4. Tenant token exchange ──────────────────────────────────────
        exchanged = authn.ExchangeServiceTokenForTenant(
            authn_service_pb.ExchangeServiceTokenForTenantRequest(
                target_tenant_id=target_tenant,
                target_project_id=target_project,
                requested_scopes=[CHECK_ACCESS_SCOPE],
            ),
            metadata=service_meta.to_grpc_metadata(),
            timeout=TIMEOUT,
        )
        now = int(time.time())
        assert exchanged.access_token, "step 4: ExchangeServiceTokenForTenant returned no access token"
        assert exchanged.tenant_id == target_tenant, (
            f"step 4: exchanged tenant_id is {exchanged.tenant_id!r}, expected {target_tenant!r}"
        )
        assert exchanged.project_id == target_project, (
            f"step 4: exchanged project_id is {exchanged.project_id!r}, expected {target_project!r}"
        )
        assert list(exchanged.scopes) == [CHECK_ACCESS_SCOPE], (
            f"step 4: exchanged scopes are {list(exchanged.scopes)}, expected [{CHECK_ACCESS_SCOPE!r}]"
        )
        assert exchanged.membership_id, "step 4: the exchanged token names no membership_id"
        assert now < exchanged.expires_at_unix <= now + EXCHANGED_TOKEN_MAX_SECONDS, (
            f"step 4: exchanged expires_at_unix {exchanged.expires_at_unix} is not within "
            f"({now}, {now + EXCHANGED_TOKEN_MAX_SECONDS}]"
        )

        target_meta = replace(
            hints,
            tenant_id=target_tenant,
            project_id=target_project,
            bearer_token=exchanged.access_token,
        )
        # The tenant/project hints of this Authenticate are the target's.
        exchanged_principal = auth.authenticate_bearer(
            exchanged.access_token, metadata=replace(target_meta, bearer_token="")
        ).principal
        assert exchanged_principal.tenant_id == target_tenant, (
            f"step 4: the exchanged principal's tenant_id is {exchanged_principal.tenant_id!r}, "
            f"expected {target_tenant!r}"
        )
        assert exchanged_principal.user_id == service_user_id, (
            f"step 4: the exchanged principal's user_id is {exchanged_principal.user_id!r}, "
            f"expected the service account {service_user_id!r}"
        )

        # An authenticated call IN the target tenant. CheckAccess requires a
        # user_id; naming the caller itself keeps it claim-bound (the exchanged
        # token carries no delegation scope).
        in_target = auth.check_access(
            authz_pb2.CheckAccessRequest(
                user_id=service_user_id, object=CHECK_OBJECT, action=CHECK_ACTION
            ),
            metadata=target_meta,
        )
        assert in_target.evaluated_subject == service_user_id, (
            f"step 4: CheckAccess in the target tenant evaluated {in_target.evaluated_subject!r}, "
            f"expected the service account {service_user_id!r}"
        )

        with pytest.raises(grpc.RpcError) as unapproved:
            authn.ExchangeServiceTokenForTenant(
                authn_service_pb.ExchangeServiceTokenForTenantRequest(
                    target_tenant_id=unapproved_tenant,
                    target_project_id=target_project,
                ),
                metadata=service_meta.to_grpc_metadata(),
                timeout=TIMEOUT,
            )
        assert unapproved.value.code() == grpc.StatusCode.PERMISSION_DENIED, (
            f"step 4: exchange into unapproved tenant {unapproved_tenant} returned "
            f"{unapproved.value.code().name}: {unapproved.value.details()}"
        )

        # ── 5. Phone-only CreateUser activated by a phone OTP ─────────────
        phone_username = f"sdk-live-phone-python-{secrets.token_hex(6)}"
        phone = "+1555" + "".join(secrets.choice("0123456789") for _ in range(7))
        human_headers = human_meta.to_grpc_metadata
        created = authn.CreateUser(
            authn_pb2.CreateUserRequest(
                username=phone_username,
                phone=phone,
                password=PHONE_PASSWORD,
                tenant_id=home_tenant,
                project_id=human.project_id or project_hint,
            ),
            metadata=human_headers(),
            timeout=TIMEOUT,
        )
        user = created.user
        assert user.user_id, "step 5: phone-only CreateUser returned no user_id"
        assert user.email == "", f"step 5: phone-only user has email {user.email!r}, expected none"
        assert user.phone == phone, f"step 5: created user.phone is {user.phone!r}, expected {phone!r}"
        assert user.status == authn_enum_pb.USER_STATUS_PENDING_VERIFICATION, (
            f"step 5: created user.status is {authn_enum_pb.UserStatus.Name(user.status)}, "
            "expected USER_STATUS_PENDING_VERIFICATION"
        )
        assert created.otp_id, "step 5: phone-only CreateUser returned no otp_id"

        sent = authn.SendOTP(
            authn_pb2.SendOTPRequest(
                user_id=user.user_id, otp_type=authn_enum_pb.OTP_TYPE_PHONE_VERIFICATION
            ),
            metadata=human_headers(),
            timeout=TIMEOUT,
        )
        assert sent.dev_otp_code, (
            "step 5: SendOTP(PHONE_VERIFICATION) echoed no dev_otp_code (is UDB_OTP_DEV_ECHO=1 set?)"
        )
        assert sent.otp_id, "step 5: SendOTP(PHONE_VERIFICATION) returned no otp_id"
        verified = authn.VerifyOTP(
            authn_pb2.VerifyOTPRequest(otp_id=sent.otp_id, code=sent.dev_otp_code),
            metadata=human_headers(),
            timeout=TIMEOUT,
        )
        assert verified.verified, "step 5: VerifyOTP on the phone code did not verify"
        assert verified.otp_type == authn_enum_pb.OTP_TYPE_PHONE_VERIFICATION, (
            f"step 5: VerifyOTP otp_type is {authn_enum_pb.OTPType.Name(verified.otp_type)}, "
            "expected OTP_TYPE_PHONE_VERIFICATION"
        )
        fetched = authn.GetUser(
            authn_pb2.GetUserRequest(user_id=user.user_id),
            metadata=human_headers(),
            timeout=TIMEOUT,
        ).user
        assert fetched.status == authn_enum_pb.USER_STATUS_ACTIVE, (
            f"step 5: after VerifyOTP user.status is {authn_enum_pb.UserStatus.Name(fetched.status)}, "
            "expected USER_STATUS_ACTIVE"
        )
        assert fetched.phone == phone, f"step 5: GetUser phone is {fetched.phone!r}, expected {phone!r}"
    finally:
        channel.close()
        auth.close()
