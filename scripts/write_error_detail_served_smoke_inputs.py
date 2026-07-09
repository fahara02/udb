#!/usr/bin/env python3
"""Write live Authn ErrorDetail served-smoke proof fixtures.

The workflow consumes these files after launching a real broker. It logs in
through AuthnService, creates a throwaway user, sends the first OTP, and writes
request JSON that proves:

* a body validation error carries a single `phone` field violation; and
* a second SendOTP call crosses the served gRPC boundary with quota detail.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PY_SDK = ROOT / "sdk" / "python"
PY_GEN = PY_SDK / "gen"
for path in (PY_SDK, PY_GEN):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import grpc  # type: ignore  # noqa: E402
from udb.core.authn.entity.v1 import enums_pb2 as authn_enums_pb2  # noqa: E402
from udb.core.authn.services.v1 import authn_service_pb2_grpc as authn_grpc  # noqa: E402
from udb.core.authn.services.v1 import core_pb2 as authn_pb2  # noqa: E402
from udb.core.common.v1 import types_pb2 as common_pb2  # noqa: E402
from udb_client.auth import UdbAuthClient  # noqa: E402
from udb_client.metadata import Metadata  # noqa: E402


PASSWORD = "ErrorDetail#2026Pass"
OTP_TYPE = authn_enums_pb2.OTP_TYPE_SENSITIVE_OPERATION
PROOF_PURPOSE = "error-detail-served-smoke"


@dataclass(frozen=True)
class AuthProof:
    tenant_id: str
    project_id: str
    bearer: str


def _json_dump(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def authenticate(target: str, username: str, password: str, tenant_hint: str, project: str) -> AuthProof:
    metadata = Metadata(
        tenant_id=tenant_hint,
        project_id=project,
        purpose=PROOF_PURPOSE,
        correlation_id="error-detail-served-smoke-login",
        service_identity=PROOF_PURPOSE,
    )
    auth = UdbAuthClient(target, metadata, timeout=15.0)
    login = auth.login(username, password, device_name="error-detail-served-smoke")
    if not login.access_token:
        raise RuntimeError(f"login for {username!r} returned no access_token")
    principal = auth.authenticate_bearer(login.access_token)
    tenant_id = getattr(principal.principal, "tenant_id", "") or tenant_hint
    if not tenant_id:
        raise RuntimeError(f"authenticate_bearer for {username!r} returned no tenant_id")
    return AuthProof(tenant_id=tenant_id, project_id=project, bearer=login.access_token)


def write_inputs(out_dir: Path, auth_target: str, auth: AuthProof) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    nonce = uuid.uuid4().hex[:12]
    metadata = (
        ("authorization", f"Bearer {auth.bearer}"),
        ("x-tenant-id", auth.tenant_id),
        ("x-udb-project-id", auth.project_id),
        ("x-purpose", PROOF_PURPOSE),
        ("x-correlation-id", f"error-detail-served-smoke-{nonce}"),
        ("x-request-id", f"error-detail-served-smoke-{nonce}"),
        ("x-scopes", "udb:admin"),
        ("x-service-identity", PROOF_PURPOSE),
    )
    stub = authn_grpc.AuthnServiceStub(grpc.insecure_channel(auth_target))
    username = f"error-detail-{nonce}"
    created = stub.CreateUser(
        authn_pb2.CreateUserRequest(
            username=username,
            email=f"{username}@example.com",
            password=PASSWORD,
            tenant_id=auth.tenant_id,
            project_id=auth.project_id,
            full_name="ErrorDetail Served Smoke",
        ),
        metadata=metadata,
        timeout=15.0,
    )
    user_id = created.user.user_id
    if not user_id:
        raise RuntimeError("CreateUser returned no user_id")

    context = common_pb2.RequestContext(
        tenant=common_pb2.TenantContext(tenant_id=auth.tenant_id, project_id=auth.project_id),
        correlation_id=f"error-detail-otp-seed-{nonce}",
        purpose=PROOF_PURPOSE,
    )
    seeded = stub.SendOTP(
        authn_pb2.SendOTPRequest(
            user_id=user_id,
            otp_type=OTP_TYPE,
            correlation_id=f"error-detail-otp-seed-{nonce}",
            context=context,
        ),
        metadata=metadata,
        timeout=15.0,
    )
    if not seeded.otp_id:
        raise RuntimeError("first SendOTP returned no otp_id")

    request_context = {
        "tenant": {"tenant_id": auth.tenant_id, "project_id": auth.project_id},
        "correlation_id": f"error-detail-proof-{nonce}",
        "purpose": PROOF_PURPOSE,
    }
    _json_dump(
        out_dir / "validation.json",
        {
            "user_id": user_id,
            "phone": "",
            "context": request_context,
        },
    )
    _json_dump(
        out_dir / "quota.json",
        {
            "user_id": user_id,
            "otp_type": "OTP_TYPE_SENSITIVE_OPERATION",
            "correlation_id": f"error-detail-otp-proof-{nonce}",
            "context": request_context,
        },
    )
    (out_dir / "header.txt").write_text(
        f"authorization: Bearer {auth.bearer}\n"
        f"x-tenant-id: {auth.tenant_id}\n"
        f"x-udb-project-id: {auth.project_id}\n"
        f"x-purpose: {PROOF_PURPOSE}\n"
        f"x-correlation-id: error-detail-served-smoke-{nonce}\n"
        f"x-request-id: error-detail-served-smoke-{nonce}\n"
        "x-scopes: udb:admin\n"
        f"x-service-identity: {PROOF_PURPOSE}\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--auth-target", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--project", default="default")
    args = parser.parse_args()

    auth = authenticate(args.auth_target, args.username, args.password, args.tenant, args.project)
    write_inputs(args.out_dir, args.auth_target, auth)
    print(f"wrote ErrorDetail served-smoke proof inputs to {args.out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
