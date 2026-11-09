"""docker_nginx example client: everything goes through nginx on one TLS address.

    login (native AuthnService via /udb.core.*)  ->  adopt the canonical tenant UUID
      ->  one more native call (AuthzService.Authorize with the bearer)
      ->  write + read one shop.v1.Order (DataBroker via /udb.services.v1.DataBroker/)

The data plane and the native control plane share one target, so the SDK opens
a single TLS channel and nginx routes each call by its gRPC path.

Settings (environment):
    UDB_PASS      admin password (admin.env, written by scripts/setup.sh)  required
    UDB_USER      admin username                                           admin
    UDB_TENANT    human tenant code                                        acme
    UDB_PROJECT   project id                                               default
    UDB_TARGET    nginx address                                            localhost:443
    UDB_CA_CERT   CA that signed nginx's certificate                       certs/ca.crt
"""

from __future__ import annotations

import json
import os
import sys
import uuid

from udb_client import create_udb

MESSAGE_TYPE = "shop.v1.Order"


def env(key: str, default: str) -> str:
    return os.environ.get(key) or default


def main() -> int:
    password = os.environ.get("UDB_PASS")
    if not password:
        print("set UDB_PASS (it is in admin.env, written by scripts/setup.sh)", file=sys.stderr)
        return 2
    username = env("UDB_USER", "admin")
    tenant_code = env("UDB_TENANT", "acme")
    target = env("UDB_TARGET", "localhost:443")
    with open(env("UDB_CA_CERT", "certs/ca.crt"), "rb") as handle:
        ca_pem = handle.read()

    udb = create_udb(
        target=target,
        auth_target=target,  # same address: nginx routes /udb.core.* to the native plane
        tenant_id=tenant_code,  # pre-login hint; replaced by the canonical UUID
        project_id=env("UDB_PROJECT", "default"),
        purpose="shop-app",
        scopes=["udb:read", "udb:write"],
        tls=True,
        root_certificates=ca_pem,
    )
    try:
        print(f"connect: {target} (TLS, verified against the example CA)")

        # Native: AuthnService.Login + Authenticate, then adopt the tenant UUID.
        authn = udb.login_and_adopt_tenant(username, password)
        tenant = udb.config.tenant_id
        print(f"login:   {username} -> tenant \"{tenant_code}\" = {tenant}")
        print(f"         scopes {list(authn.principal.scopes)}")

        # Native: AuthzService.Authorize with the bearer just issued. The answer
        # comes from the same policy_rules the data plane enforces below.
        allowed, decision = udb.auth.can(MESSAGE_TYPE, "Select", use_cache=False)
        print(
            f"native:  Authorize({MESSAGE_TYPE}, Select) -> allowed={allowed}"
            + (f" ({decision.deny_reason})" if decision.deny_reason else "")
        )

        # Data plane: write one order, read it back by key + tenant.
        order_id = str(uuid.uuid4())
        udb.data.upsert(
            message_type=MESSAGE_TYPE,
            record={
                "order_id": order_id,
                "tenant_id": tenant,  # the canonical UUID, never the human code
                "customer_email": "ada@example.com",
                "total_cents": 4200,
                "status": "placed",
            },
            conflict_fields=("order_id",),
        )
        print(f"write:   {MESSAGE_TYPE} {order_id}")

        result = udb.data.select(
            message_type=MESSAGE_TYPE,
            filter={"order_id": order_id, "tenant_id": tenant},
        )
        rows = [json.loads(raw) for raw in result.records_json]
        if len(rows) != 1 or rows[0].get("total_cents") not in (4200, "4200"):
            print(f"read:    expected the order just written, got {rows}", file=sys.stderr)
            return 1
        print(f"read:    status={rows[0].get('status')} total_cents={rows[0].get('total_cents')}")

        print("\nOK")
        return 0
    finally:
        udb.close()


if __name__ == "__main__":
    sys.exit(main())
