# UDB enterprise Go example

The Go counterpart of [`../ts_enterprise`](../ts_enterprise) — the **real**
enterprise path, in one `ConnectEnterprise` call:

```
connect (data + auth targets)  →  login (password → RS256 JWT)  →  verify bearer
   →  adopt canonical tenant UUID  →  tenant-scoped CRUD of billing.v1.Invoice
   →  cross-tenant isolation check
```

It uses the Go SDK helpers added for critic §11/§13/§14/§15:
- `udbclient.ConnectEnterprise` — dials, logs in, verifies, adopts the canonical
  tenant UUID, and carries the bearer on every call (`DataContext`/`NativeContext`).
- `EnterpriseSession.CanonicalTenantID` — the verified UUID (never the human code).
- `EnterpriseSession.ValidateTenant` — fails fast if a record's `tenant_id` differs.
- The SDK now auto-attaches `x-request-id`/`x-correlation-id`, so native RPCs
  don't fail "request context required".

## Run

```bash
# 1. Bring up a broker with a bootstrapped admin + seeded ABAC. The ts_enterprise
#    scripts do exactly this and can be reused verbatim:
( cd ../ts_enterprise && ./scripts/bootstrap.sh && ./scripts/serve.sh )   # serve.sh stays in the foreground

# 2. In another terminal, run this example (UDB_PASS = the admin password from bootstrap.sh):
set -a; source ../ts_enterprise/secrets/enterprise.env; set +a   # or set UDB_* yourself
UDB_PASS="$UDB_ADMIN_PASSWORD" UDB_TENANT=acme go run .
```

Env (all optional except `UDB_PASS`): `UDB_TARGET` (default `127.0.0.1:50051`),
`UDB_AUTH_TARGET` (`127.0.0.1:50061`), `UDB_USER` (`admin`), `UDB_TENANT`
(`acme`), `UDB_PROJECT` (`default`).

The row is sent as `record_json` (a plain map), so no `buf`/`protoc` step is
needed; `proto/billing/v1/invoice.proto` is the schema the broker serves. For a
typed row, generate one with `protoc-gen-go` from the same proto.
