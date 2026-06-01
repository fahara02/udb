"""Admin-flow variant — the full native-services provisioning sequence from
Python, symmetric with the Go example: register a user, define RBAC
(role → assignment → policy), verify the access check, and mint an API key the
consumer example (`main.py`) can then authenticate.

Uses the generated service stubs directly (the `UdbAuthClient` wrapper covers the
consumer surface — authenticate/can/native_access — while provisioning is an
admin concern that drives the raw Authn/Authz/ApiKey RPCs).

Prereqs: a running broker, `grpcio`, and the generated stubs
(`buf generate` → sdk/python/gen). Run:
    UDB_TARGET=127.0.0.1:50051 python admin.py
It prints `export UDB_API_KEY=...` — copy that to run any consumer example.
"""

from __future__ import annotations

import os
import sys
import time

_SDK = os.path.join(os.path.dirname(__file__), "..", "..", "..", "sdk", "python")
sys.path.insert(0, _SDK)
sys.path.insert(0, os.path.join(_SDK, "gen"))

import grpc  # noqa: E402

from udb_client.metadata import Metadata  # noqa: E402
from udb.core.authn.services.v1 import authn_service_pb2_grpc, core_pb2 as authn  # noqa: E402
from udb.core.authz.services.v1 import authz_service_pb2_grpc, core_pb2 as authz  # noqa: E402
from udb.core.apikey.services.v1 import apikey_service_pb2_grpc, core_pb2 as apikey  # noqa: E402

TARGET = os.getenv("UDB_TARGET", "127.0.0.1:50051")


def main() -> None:
    meta = Metadata(
        tenant_id="acme",
        project_id="billing",
        purpose="control-plane",
        correlation_id="native-py-admin",
        scopes=("udb:*",),
        service_identity="examples.native-py-admin",
    )
    md = meta.to_grpc_metadata()
    suffix = str(int(time.time() * 1000))

    with grpc.insecure_channel(TARGET) as ch:
        authn_stub = authn_service_pb2_grpc.AuthnServiceStub(ch)
        authz_stub = authz_service_pb2_grpc.AuthzServiceStub(ch)
        apikey_stub = apikey_service_pb2_grpc.ApiKeyServiceStub(ch)

        # ── Step 1: register a user ──────────────────────────────────────────
        user = authn_stub.CreateUser(
            authn.CreateUserRequest(
                username=f"alice_{suffix}",
                email=f"alice_{suffix}@example.com",
                password="CorrectHorse1!",
                tenant_id="acme",
                full_name="Alice Example",
                project_id="billing",
            ),
            metadata=md,
        ).user
        print(f"1) registered user {user.user_id} ({user.username})")

        # ── Step 2: RBAC — role → assignment → allow policy ──────────────────
        role = authz_stub.CreateRole(
            authz.CreateRoleRequest(
                name=f"Reader {suffix}",
                role_code=f"reader_{suffix}",
                created_by=user.user_id,
                domain="acme",
                tenant_id="acme",
                project_id="billing",
            ),
            metadata=md,
        ).role
        authz_stub.AssignRole(
            authz.AssignRoleRequest(
                user_id=user.user_id, role_id=role.role_id, domain="acme",
                assigned_by=user.user_id, tenant_id="acme", project_id="billing",
            ),
            metadata=md,
        )
        authz_stub.PutAuthzPolicy(
            authz.PutAuthzPolicyRequest(
                policy=authz.AuthzPolicyRecord(
                    id=f"policy-{role.role_code}", enabled=True, effect="allow",
                    tenant="acme", project="billing", role=role.role_code,
                    action="data.select", resource="invoice",
                )
            ),
            metadata=md,
        )
        print(f"2) role {role.role_code} assigned to user; allow policy on invoice/data.select added")

        # ── Step 3: verify the access check ──────────────────────────────────
        allow = authz_stub.CheckAccess(
            authz.CheckAccessRequest(
                user_id=user.user_id, domain="acme", tenant_id="acme",
                project_id="billing", object="invoice", action="data.select",
            ),
            metadata=md,
        ).allowed
        deny = authz_stub.CheckAccess(
            authz.CheckAccessRequest(
                user_id=user.user_id, domain="acme", tenant_id="acme",
                project_id="billing", object="invoice", action="data.delete",
            ),
            metadata=md,
        ).allowed
        print(f"3) check data.select={allow} ; data.delete={deny}")

        # ── Step 4: mint an API key for the consumer example ─────────────────
        created = apikey_stub.CreateApiKey(
            apikey.CreateApiKeyRequest(
                name="native-py-admin-key", owner_id=user.user_id, scopes=["data:read"],
            ),
            metadata=md,
        )
        print(f"4) minted dev API key → export UDB_API_KEY={created.plain_key}")


if __name__ == "__main__":
    main()
