# UDB enterprise TypeScript example

A **standalone** project (it installs `@udb_plus/sdk` from npm — no source
imports) that runs the **real** enterprise path against a UDB broker:

```
provision  →  authn (password → RS256 JWT)  →  authz (default-deny ABAC)
           →  tenant-scoped CRUD of a billing.Invoice  →  isolation check
```

Unlike a "hello world", it faces the friction real deployments hit — and shows
how each is resolved:

| Real-world friction | How this example handles it |
|---|---|
| The SDK is a dependency, not a file you import | `npm install @udb_plus/sdk`, `import { UdbProject } from "@udb_plus/sdk"` |
| Auth lives on a **separate** listener (`:50061`), not the data port (`:50051`) | client sets `authTarget`; broker sets `UDB_AUTH_GRPC_ADDR` |
| Login needs JWT keys + session/password secrets | `bootstrap.sh` generates an RS256 keypair + HMAC secrets |
| The JWT tenant claim is a **canonical UUID**, not the code `"acme"` | client verifies the bearer and adopts `principal.tenant_id` |
| Authorization is **default-deny**; the org-owner role alone is not enough | `bootstrap.sh` seeds a real ABAC policy (`UDB_ABAC_POLICIES_JSON`) |
| Two authz surfaces (data-plane ABAC vs control-plane Casbin) disagree | documented below; the example relies on the authoritative data-plane gate |
| Tenant-scoped reads must filter on `tenant_id` | every `select`/`delete` carries the adopted tenant UUID |
| The row type duplicates the proto | `Invoice` is **generated** from `invoice.proto` (ts-proto), not hand-written |

> The dev counterpart (header-scopes, default-allow, no login) is intentionally
> *not* what this is. For the broker-side hardening reference see
> [`../../docs/enterprise-deployment.md`](../../docs/enterprise-deployment.md).

---

## Prerequisites
- **Docker** (PostgreSQL), **Node 18+**, **openssl**, and **buf** (for type-gen).
- The **`udb` CLI** — `cargo build --release --bin udb`, or set `UDB_CLI` to a
  binary. The scripts auto-find `udb` on `PATH` / `target/{release,debug}`.

## Run it

```bash
# 1. Postgres (the enterprise DB is created on a dedicated database name).
cd ../php_quickstart && docker compose up -d && cd ../ts_enterprise
docker exec udb-php-quickstart-postgres-1 psql -U udb -d udb -c "CREATE DATABASE udb_enterprise;" || true

# 2. Provision: RS256 keys, HMAC secrets, the admin user (bound to
#    organization_owner → udb:*), and the seeded ABAC policy. Writes secrets/.
./scripts/bootstrap.sh

# 3. Start the broker in ENTERPRISE mode (keep this terminal open).
#    Wait for "UDB DataBroker is ready".
./scripts/serve.sh

# 4. (second terminal) Install the SDK + generate the Invoice type, then run.
npm install
npm run gen                       # udb proto export + buf generate → gen/billing/v1/invoice.ts
set -a; source secrets/enterprise.env; set +a
npm start
```

Expected output:
```
authn: logged in as admin
       tenant code "acme" → canonical UUID 00000000-0000-0000-0000-0000000d0001
       scopes: ["udb:admin","udb:*"]
authz: data-plane ABAC gates every CRUD op below (default-deny without the seeded policy)
crud:  created invoice 1c1c1c1c-0000-4000-8000-000000000001
crud:  read    {"invoice_id":"…","tenant_id":"…","amount_cents":4999,"status":"paid",…}
crud:  updated {…,"status":"refunded",…}
crud:  deleted; remaining rows = 0
authz: cross-tenant read (tenant …d9999) → 0 row(s) — isolation enforced

ENTERPRISE FLOW OK (authn + authz + tenant-scoped CRUD + isolation)
```

---

## The two authorization surfaces (a real UDB gotcha)

UDB authorizes in **two** independent places, and they are not the same engine:

1. **Data-plane ABAC** — gates every `Select`/`Upsert`/`Delete`. Built from an
   ABAC policy snapshot (`UDB_ABAC_POLICIES_JSON`, or the `udb_abac_policies`
   table). **This is the authoritative gate on your data, and it is
   default-deny** — without a seeded policy, even an `organization_owner` admin
   gets `PERMISSION_DENIED`. `bootstrap.sh` seeds it.
2. **Control-plane Casbin** — what the SDK's `udb.authz.can/require` query
   (`AuthzService.Check`), a governance model over roles + `policy_rules`. It can
   say DENY while the data-plane says ALLOW until *its* rules are seeded too.

This example relies on (1) — the gate that actually runs on each CRUD call.
Seeding (2) to match is a governance exercise (`udb auth policy …`).

## Going further: TLS / mTLS

This example uses plaintext on localhost with real JWT auth (a valid hardened
shape is to terminate TLS at an edge proxy). For in-broker TLS, set `UDB_TLS_*`
(and `UDB_MTLS_*`) on the broker and pass `tls: { rootCerts, privateKey,
certChain }` to `UdbProject.connect`. See `docs/enterprise-deployment.md`.

## Files
- `proto/billing/v1/invoice.proto` — your tenant-scoped table (the only schema you write).
- `gen/billing/v1/invoice.ts` — **generated** `Invoice` type (do not edit).
- `src/main.ts` — the authn → authz → CRUD flow.
- `scripts/bootstrap.sh` — one-time provisioning (keys, secrets, admin, ABAC policy).
- `scripts/serve.sh` — start the broker in enterprise mode.
