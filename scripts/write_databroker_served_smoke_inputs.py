#!/usr/bin/env python3
"""Write live DataBroker served-smoke proof fixtures.

The generated files are consumed by the idempotency and retry-safe workflow
smokes. They are deterministic request bodies, but the tenant ids and bearer
headers come from a real AuthnService login so the proof still runs through the
served broker security path.
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

from udb_client.auth import UdbAuthClient  # noqa: E402
from udb_client.metadata import Metadata  # noqa: E402


MESSAGE_TYPE = "udb.sdk.live.v1.SdkLiveRecord"
PROOF_PURPOSE = "served-smoke-proof"
PROOF_SCOPES = "udb:admin"


@dataclass(frozen=True)
class AuthProof:
    tenant_id: str
    project_id: str
    bearer: str


def _json_dump(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _context(tenant_id: str, project_id: str) -> dict[str, object]:
    return {
        "tenant_id": tenant_id,
        "project_id": project_id,
        "purpose": PROOF_PURPOSE,
        "scopes": [PROOF_SCOPES],
    }


def _record(tenant_id: str, project_id: str, record_id: str, payload: str) -> dict[str, object]:
    return {
        "record_id": record_id,
        "tenant_id": tenant_id,
        "project_id": project_id,
        "lookup_key": record_id,
        "payload": payload,
    }


def _upsert(
    tenant_id: str,
    project_id: str,
    record_id: str,
    payload: str,
    *,
    idempotency_key: str = "",
) -> dict[str, object]:
    request: dict[str, object] = {
        "context": _context(tenant_id, project_id),
        "message_type": MESSAGE_TYPE,
        "record_json_object": _record(tenant_id, project_id, record_id, payload),
        "return_record": True,
    }
    if idempotency_key:
        request["idempotency_key"] = idempotency_key
    return request


def _delete(tenant_id: str, project_id: str, record_id: str, *, idempotency_key: str) -> dict[str, object]:
    return {
        "context": _context(tenant_id, project_id),
        "message_type": MESSAGE_TYPE,
        "filter": {
            "record_id": record_id,
            "tenant_id": tenant_id,
            "project_id": project_id,
        },
        "idempotency_key": idempotency_key,
    }


def _select(tenant_id: str, project_id: str, record_id: str) -> dict[str, object]:
    return {
        "context": _context(tenant_id, project_id),
        "message_type": MESSAGE_TYPE,
        "filter": {
            "record_id": record_id,
            "tenant_id": tenant_id,
            "project_id": project_id,
        },
        "limit": 1,
    }


def authenticate(
    target: str,
    username: str,
    password: str,
    tenant_hint: str,
    project_id: str,
    *,
    purpose: str,
) -> AuthProof:
    metadata = Metadata(
        tenant_id=tenant_hint,
        project_id=project_id,
        purpose=purpose,
        correlation_id=purpose,
        service_identity=purpose,
    )
    auth = UdbAuthClient(target, metadata, timeout=15.0)
    login = auth.login(username, password, device_name=purpose)
    if not login.access_token:
        raise RuntimeError(f"login for {username!r} returned no access_token")
    principal = auth.authenticate_bearer(login.access_token)
    tenant_id = getattr(principal.principal, "tenant_id", "") or tenant_hint
    if not tenant_id:
        raise RuntimeError(f"authenticate_bearer for {username!r} returned no tenant_id")
    return AuthProof(tenant_id=tenant_id, project_id=project_id, bearer=login.access_token)


def write_inputs(out_dir: Path, primary: AuthProof, tenant2: AuthProof) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    nonce = uuid.uuid4().hex[:12]

    replay_record = f"served-replay-{nonce}"
    tenant2_record = replay_record
    batch_record = f"served-batch-{nonce}"
    fail_record = f"served-fail-closed-{nonce}"
    retry_record = f"served-retry-{nonce}"

    _json_dump(
        out_dir / "upsert.json",
        _upsert(primary.tenant_id, primary.project_id, replay_record, "first", idempotency_key=f"idem-{nonce}"),
    )
    _json_dump(
        out_dir / "tenant2-upsert.json",
        _upsert(tenant2.tenant_id, tenant2.project_id, tenant2_record, "first", idempotency_key=f"idem-{nonce}"),
    )
    _json_dump(
        out_dir / "batch-upsert.json",
        [
            _upsert(primary.tenant_id, primary.project_id, batch_record, "batch-a", idempotency_key=f"batch-{nonce}"),
            _upsert(primary.tenant_id, primary.project_id, batch_record, "batch-b", idempotency_key=f"batch-{nonce}"),
        ],
    )
    _json_dump(
        out_dir / "fail-closed-upsert.json",
        _upsert(primary.tenant_id, primary.project_id, fail_record, "fail-closed", idempotency_key=f"fail-{nonce}"),
    )
    _json_dump(out_dir / "fail-closed-select.json", _select(primary.tenant_id, primary.project_id, fail_record))
    _json_dump(out_dir / "keyless-upsert.json", _upsert(primary.tenant_id, primary.project_id, fail_record, "fail-closed"))
    _json_dump(
        out_dir / "retry-upsert.json",
        _upsert(primary.tenant_id, primary.project_id, retry_record, "retry", idempotency_key=f"retry-{nonce}"),
    )
    _json_dump(
        out_dir / "retry-delete.json",
        _delete(primary.tenant_id, primary.project_id, retry_record, idempotency_key=f"retry-{nonce}"),
    )
    (out_dir / "header.txt").write_text(_headers(primary, "databroker-served-smoke-primary"), encoding="utf-8")
    (out_dir / "tenant2-header.txt").write_text(_headers(tenant2, "databroker-served-smoke-tenant2"), encoding="utf-8")


def _headers(proof: AuthProof, correlation_id: str) -> str:
    return (
        f"authorization: Bearer {proof.bearer}\n"
        f"x-tenant-id: {proof.tenant_id}\n"
        f"x-udb-project-id: {proof.project_id}\n"
        f"x-purpose: {PROOF_PURPOSE}\n"
        f"x-correlation-id: {correlation_id}\n"
        f"x-request-id: {correlation_id}\n"
        f"x-scopes: {PROOF_SCOPES}\n"
        "x-service-identity: databroker-served-smoke\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--auth-target", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--tenant2-username", required=True)
    parser.add_argument("--tenant2-password", required=True)
    parser.add_argument("--tenant2", required=True)
    parser.add_argument("--project", default="default")
    parser.add_argument("--tenant2-project")
    args = parser.parse_args()
    tenant2_project = args.tenant2_project or f"{args.project}-tenant2"

    primary = authenticate(
        args.auth_target,
        args.username,
        args.password,
        args.tenant,
        args.project,
        purpose="databroker-served-smoke-primary",
    )
    tenant2 = authenticate(
        args.auth_target,
        args.tenant2_username,
        args.tenant2_password,
        args.tenant2,
        tenant2_project,
        purpose="databroker-served-smoke-tenant2",
    )
    write_inputs(args.out_dir, primary, tenant2)
    print(f"wrote DataBroker served-smoke proof inputs to {args.out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
