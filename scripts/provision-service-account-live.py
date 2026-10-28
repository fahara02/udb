#!/usr/bin/env python3
"""Provision the service-account fixtures the SDK live conformance tests share.

CI's "SDK service accounts (live)" job runs this ONCE, after the broker is up
and before any SDK test, so every SDK proves the same served behavior against
the same identities instead of re-implementing provisioning seven times.

It needs two offline-bootstrapped administrators (created by `udb auth
bootstrap user` before the broker starts):

- ``UDB_LIVE_USERNAME`` in ``UDB_LIVE_TENANT``: the service account's HOME
  tenant administrator;
- ``UDB_LIVE_TARGET_ADMIN_USERNAME`` in ``UDB_LIVE_TARGET_TENANT``: the
  administrator of the tenant the account is approved to act in.

Through the broker's public RPCs, as those administrators, it creates:

1. a service account ``UDB_LIVE_SERVICE_USERNAME`` (password
   ``UDB_LIVE_SERVICE_PASSWORD``) in the home tenant, activated, with a typed
   grant carrying exactly the scopes the conformance tests exercise:
   ``udb:authz:check-access``, ``udb:authz:check-access-for`` and
   ``udb:authn:exchange-tenant-token``;
2. two roles bound to the account: ``UDB_LIVE_SERVICE_ROLE`` (an ordinary
   role a service bearer carries) and ``UDB_LIVE_SERVICE_WITHHELD_ROLE`` (an
   admin-segment code a service bearer must never carry);
3. an ACTIVE person in the home tenant, the subject of delegated CheckAccess;
4. a ServiceAccountTenantMembership approved by the TARGET tenant's
   administrator, letting the account act in the target tenant with
   ``udb:authz:check-access``.

It appends to ``$GITHUB_ENV`` (or prints, outside Actions):

- ``UDB_LIVE_SERVICE_USER_ID``: the service account;
- ``UDB_LIVE_DELEGATED_USER_ID``: the person;
- ``UDB_LIVE_TARGET_TENANT_ID``: the target tenant's canonical UUID;
- ``UDB_LIVE_UNAPPROVED_TENANT_ID``: a tenant id with no membership.

The output holds identifiers only, never a credential.
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import replace

import grpc
from google.protobuf import timestamp_pb2

from udb.core.authn.entity.v1 import enums_pb2 as authn_enums
from udb.core.authn.services.v1 import authn_service_pb2 as authn_service_pb
from udb.core.authn.services.v1 import authn_service_pb2_grpc as authn_grpc
from udb.core.authn.services.v1 import core_pb2 as authn_pb
from udb.core.authz.services.v1 import authz_service_pb2_grpc as authz_grpc
from udb.core.authz.services.v1 import core_pb2 as authz_pb
from udb_client.auth import UdbAuthClient
from udb_client.metadata import Metadata

#: Scopes of the service account's typed grant: the caller's own CheckAccess,
#: the literal delegation scope, and the tenant-token exchange scope.
SERVICE_SCOPES = (
    "udb:authz:check-access",
    "udb:authz:check-access-for",
    "udb:authn:exchange-tenant-token",
)
#: What the target tenant approves the account for.
MEMBERSHIP_SCOPES = ("udb:authz:check-access",)
#: The delegated subject's password (it never logs in; CreateUser requires one).
PERSON_PASSWORD = "SdkLivePerson#2026"
TIMEOUT = 20.0


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def admin_session(auth_target: str, username: str, password: str, tenant: str, project: str,
                  purpose: str) -> tuple[Metadata, str, str]:
    """Log an administrator in; return (authenticated metadata, user id, canonical tenant)."""
    meta = Metadata(tenant_id=tenant, project_id=project, purpose=purpose,
                    correlation_id=f"{purpose}-{uuid.uuid4().hex}",
                    service_identity="ci.sdk.service-accounts", client_catalog_version="")
    with UdbAuthClient(auth_target, meta, timeout=TIMEOUT) as auth:
        login = auth.login(username, password, device_name=purpose)
        if not login.access_token:
            raise RuntimeError(f"{username}: login returned no access token")
        principal = auth.authenticate_bearer(login.access_token).principal
    if not principal.tenant_id or not principal.user_id:
        raise RuntimeError(f"{username}: the verified principal has no tenant or user id")
    authed = replace(meta, tenant_id=principal.tenant_id,
                     project_id=principal.project_id or project, bearer_token=login.access_token)
    return authed, principal.user_id, principal.tenant_id


def create_active_user(authn, headers, *, username: str, password: str, tenant: str, project: str,
                       kind: int, full_name: str) -> str:
    created = authn.CreateUser(authn_pb.CreateUserRequest(
        username=username, email=f"{username}@example.invalid", password=password,
        tenant_id=tenant, project_id=project, full_name=full_name, account_kind=kind),
        metadata=headers, timeout=TIMEOUT)
    user_id = created.user.user_id
    if not user_id:
        raise RuntimeError(f"CreateUser({username}) returned no user id")
    # An administrator-provisioned account has no inbox in CI: activate it the
    # way `udb auth bootstrap user` does.
    authn.ChangeUserStatus(authn_pb.ChangeUserStatusRequest(
        user_id=user_id, new_status=authn_enums.USER_STATUS_ACTIVE,
        reason="sdk service-account conformance fixture"), metadata=headers, timeout=TIMEOUT)
    return user_id


def bind_role(authz, headers, *, code: str, user_id: str, actor: str, tenant: str, project: str) -> None:
    role = authz.CreateRole(authz_pb.CreateRoleRequest(
        name=f"SDK live {code}", description="sdk service-account conformance role",
        created_by=actor, role_code=code, domain=tenant, tenant_id=tenant, project_id=project),
        metadata=headers, timeout=TIMEOUT).role
    if not role.role_id:
        raise RuntimeError(f"CreateRole({code}) returned no role id")
    authz.AssignRole(authz_pb.AssignRoleRequest(
        user_id=user_id, role_id=role.role_id, domain=tenant, assigned_by=actor,
        tenant_id=tenant, project_id=project), metadata=headers, timeout=TIMEOUT)


def export(values: dict[str, str]) -> None:
    lines = "".join(f"{key}={value}\n" for key, value in values.items())
    github_env = os.getenv("GITHUB_ENV", "")
    if github_env:
        with open(github_env, "a", encoding="utf-8") as handle:
            handle.write(lines)
    print(lines, end="")


def main() -> int:
    auth_target = required_env("UDB_AUTH_GRPC_TARGET")
    project = required_env("UDB_LIVE_PROJECT")
    target_project = os.getenv("UDB_LIVE_TARGET_PROJECT", "").strip() or project
    home_meta, home_admin_id, home_tenant = admin_session(
        auth_target, required_env("UDB_LIVE_USERNAME"), required_env("UDB_LIVE_PASSWORD"),
        required_env("UDB_LIVE_TENANT"), project, "ci.sdk.service-accounts.home")
    target_meta, _target_admin_id, target_tenant = admin_session(
        auth_target, required_env("UDB_LIVE_TARGET_ADMIN_USERNAME"),
        required_env("UDB_LIVE_TARGET_ADMIN_PASSWORD"), required_env("UDB_LIVE_TARGET_TENANT"),
        target_project, "ci.sdk.service-accounts.target")
    if home_tenant == target_tenant:
        raise RuntimeError("the home and target administrators share one tenant")
    service_username = required_env("UDB_LIVE_SERVICE_USERNAME")
    service_role = required_env("UDB_LIVE_SERVICE_ROLE")
    withheld_role = required_env("UDB_LIVE_SERVICE_WITHHELD_ROLE")

    with grpc.insecure_channel(auth_target) as channel:
        authn = authn_grpc.AuthnServiceStub(channel)
        authz = authz_grpc.AuthzServiceStub(channel)
        home = home_meta.to_grpc_metadata()

        service_id = create_active_user(
            authn, home, username=service_username,
            password=required_env("UDB_LIVE_SERVICE_PASSWORD"), tenant=home_tenant,
            project=project, kind=authn_enums.ACCOUNT_KIND_SERVICE_ACCOUNT,
            full_name="SDK live service account")
        grant = authn.CreateServiceAccountGrant(authn_service_pb.CreateServiceAccountGrantRequest(
            tenant_id=home_tenant, user_id=service_id,
            service_identity=f"spiffe://ci.udb/{service_username}", project_id=project,
            approved_scopes=list(SERVICE_SCOPES), reason="sdk service-account conformance"),
            metadata=home, timeout=TIMEOUT).grant
        if grant.status and grant.status.upper() != "ACTIVE":
            raise RuntimeError(f"the typed grant is {grant.status}, not ACTIVE")
        for code in (service_role, withheld_role):
            bind_role(authz, home, code=code, user_id=service_id, actor=home_admin_id,
                      tenant=home_tenant, project=project)

        person_id = create_active_user(
            authn, home, username=f"{service_username}-person", password=PERSON_PASSWORD,
            tenant=home_tenant, project=project, kind=authn_enums.ACCOUNT_KIND_PERSON,
            full_name="SDK live delegated person")

        expires = timestamp_pb2.Timestamp(seconds=int(time.time()) + 24 * 3600)
        membership = authn.CreateServiceAccountTenantMembership(
            authn_service_pb.CreateServiceAccountTenantMembershipRequest(
                target_tenant_id=target_tenant, user_id=service_id, home_tenant_id=home_tenant,
                target_project_id=target_project, approved_scopes=list(MEMBERSHIP_SCOPES),
                reason="sdk service-account conformance", expires_at=expires),
            metadata=target_meta.to_grpc_metadata(), timeout=TIMEOUT).membership
        if membership.status != "ACTIVE" or not membership.membership_id:
            raise RuntimeError(f"the tenant membership is {membership.status!r}, not ACTIVE")

    export({
        "UDB_LIVE_SERVICE_USER_ID": service_id,
        "UDB_LIVE_DELEGATED_USER_ID": person_id,
        "UDB_LIVE_TARGET_TENANT_ID": target_tenant,
        "UDB_LIVE_UNAPPROVED_TENANT_ID": str(uuid.uuid4()),
    })
    print(f"Provisioned service account {service_id} (home {home_tenant}) with a membership in "
          f"{target_tenant}; delegated subject {person_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
