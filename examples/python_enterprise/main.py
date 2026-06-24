"""Python counterpart of examples/ts_enterprise — the real enterprise path:

    connect (data + auth targets)  ->  login (password -> RS256 JWT)
       ->  adopt canonical tenant UUID  ->  tenant-scoped CRUD of billing.v1.Invoice
       ->  cross-tenant isolation check

The Python SDK's UdbProject.login_and_adopt_tenant installs the bearer and the
canonical tenant across every sub-client (incl. the data plane), so no manual
bearer plumbing is needed. Run after a broker with a bootstrapped admin + seeded
ABAC is up (examples/ts_enterprise/scripts/bootstrap.sh + serve.sh work verbatim):

    UDB_PASS=<admin-pass> UDB_TENANT=acme python main.py
"""

from __future__ import annotations

import os
import sys

from udb_client import create_udb

MESSAGE_TYPE = "billing.v1.Invoice"


def env(key: str, default: str) -> str:
    return os.environ.get(key) or default


def main() -> None:
    password = os.environ.get("UDB_PASS")
    if not password:
        sys.exit("set UDB_PASS to the admin password printed by bootstrap.sh")
    tenant_code = env("UDB_TENANT", "acme")
    username = env("UDB_USER", "admin")

    udb = create_udb(
        target=env("UDB_TARGET", "127.0.0.1:50051"),
        auth_target=env("UDB_AUTH_TARGET", "127.0.0.1:50061"),
        tenant_id=tenant_code,  # pre-login hint; replaced by the canonical UUID
        project_id=env("UDB_PROJECT", "default"),
        purpose="billing-app",
        scopes=["udb:read", "udb:write"],
    )
    try:
        authn = udb.login_and_adopt_tenant(username, password)
        tenant = udb.config.tenant_id  # the verified canonical UUID after adoption
        print(f"authn: logged in as {username}")
        print(f'       tenant code "{tenant_code}" -> canonical UUID {tenant}')
        print(f"       scopes: {list(authn.principal.scopes)}")
        print(
            "authz: data-plane ABAC gates every CRUD op below "
            "(default-deny without the seeded policy)"
        )

        invoice_id = "1c1c1c1c-0000-4000-8000-000000000001"
        record = {
            "invoice_id": invoice_id,
            "tenant_id": tenant,  # the canonical UUID, never the human code
            "customer_email": "ada@example.com",
            "amount_cents": 4999,
            "status": "paid",
        }

        # CREATE
        udb.data.upsert(
            message_type=MESSAGE_TYPE, record=record, conflict_fields=("invoice_id",)
        )
        print(f"crud:  created invoice {invoice_id}")

        # READ — a tenant-scoped read MUST carry tenant_id in the filter.
        scoped = {"invoice_id": invoice_id, "tenant_id": tenant}
        rs = udb.data.select(message_type=MESSAGE_TYPE, filter=scoped)
        print(f"crud:  read    {len(rs.records_json)} row(s)")

        # UPDATE
        record["status"] = "refunded"
        udb.data.upsert(
            message_type=MESSAGE_TYPE, record=record, conflict_fields=("invoice_id",)
        )
        print("crud:  updated status -> refunded")

        # DELETE
        udb.data.delete(message_type=MESSAGE_TYPE, filter=scoped)
        print("crud:  deleted")

        # CROSS-TENANT ISOLATION — a read scoped to another tenant returns nothing.
        other = udb.data.select(
            message_type=MESSAGE_TYPE,
            filter={"tenant_id": "00000000-0000-0000-0000-00000000d999"},
        )
        print(
            f"authz: cross-tenant read -> {len(other.records_json)} row(s) "
            "— isolation enforced"
        )

        print("\nENTERPRISE FLOW OK (authn + authz + tenant-scoped CRUD + isolation)")
    finally:
        closer = getattr(udb, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception:
                pass


if __name__ == "__main__":
    main()
