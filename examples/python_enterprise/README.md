# UDB enterprise Python example

The Python counterpart of [`../ts_enterprise`](../ts_enterprise) and
[`../go_enterprise`](../go_enterprise) — the **real** enterprise path:

```
connect (data + auth targets)  ->  login (password -> RS256 JWT)  ->  verify bearer
   ->  adopt canonical tenant UUID  ->  tenant-scoped CRUD of billing.v1.Invoice
   ->  cross-tenant isolation check
```

`UdbProject.login_and_adopt_tenant` installs the bearer + canonical tenant across
every sub-client (data plane included), so there is no manual bearer plumbing.
The SDK also now auto-attaches `x-request-id`/`x-correlation-id`, so native RPCs
don't fail "request context required".

## Run

```bash
pip install udb-client

# 1. Bring up a broker with a bootstrapped admin + seeded ABAC. The ts_enterprise
#    scripts do exactly this and can be reused verbatim:
( cd ../ts_enterprise && ./scripts/bootstrap.sh && ./scripts/serve.sh )   # serve.sh stays foreground

# 2. In another terminal:
set -a; source ../ts_enterprise/secrets/enterprise.env; set +a   # or set UDB_* yourself
UDB_PASS="$UDB_ADMIN_PASSWORD" UDB_TENANT=acme python main.py
```

Env (all optional except `UDB_PASS`): `UDB_TARGET` (default `127.0.0.1:50051`),
`UDB_AUTH_TARGET` (`127.0.0.1:50061`), `UDB_USER` (`admin`), `UDB_TENANT`
(`acme`), `UDB_PROJECT` (`default`).

The row is sent as a plain dict (`record_json`), so no codegen step is needed;
`proto/billing/v1/invoice.proto` is the schema the broker serves.
