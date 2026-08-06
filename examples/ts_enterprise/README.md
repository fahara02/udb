# Connect to UDB from TypeScript — the enterprise path

> Part of the enterprise trio (Go / Python / TypeScript — the same program in
> three languages). Start with the shared **[ENTERPRISE_GUIDE.md](../ENTERPRISE_GUIDE.md)**
> for the flow, the one run procedure, and how the three compare.

This is the real, no-shortcuts way to go from *username + password* to *doing
tenant-scoped work* against a UDB broker in TypeScript: log in, get a JWT, and
run tenant-scoped CRUD that the broker actually authorizes. It's a **standalone**
project — it installs `@udb_plus/sdk` from npm and imports it like any other
dependency, no source paths into this repo.

If you only remember one thing: **you log in with the human tenant *code* (e.g.
`acme`), but you do all your data work with the canonical tenant *UUID* the
broker hands back.** Mixing those two up is the single most common UDB
integration bug — your reads quietly return nothing, because row-level security
compares UUIDs and `"acme"` is not a UUID. The login flow below resolves the code
to its UUID for you; you adopt that UUID and use it everywhere after.

Unlike a "hello world", this example faces the friction real deployments hit:

- The **auth control plane is on a separate listener** (`:50061`), not the data
  port (`:50051`). Call `login` on the data port and you get `UNIMPLEMENTED`.
- **Authorization is default-deny.** Without a seeded access policy, even an
  org-owner admin gets `PERMISSION_DENIED` on every CRUD call. `bootstrap.sh`
  seeds a real policy so you can watch it work.
- The **row type is generated from the proto**, not hand-written — so your code
  and the schema the broker serves can't drift apart.

> This is the hardened shape (real JWT login, default-deny). The dev counterpart
> — header-scopes, default-allow, no login — is intentionally *not* what this is.
> For broker-side hardening, see
> [`../../docs/enterprise-deployment.md`](../../docs/enterprise-deployment.md).

## The 30-second version

`UdbProject.connectEnterprise({ …username, password })` is the one-call path (the
parity of Go's `ConnectEnterprise`): it connects, logs in, verifies the bearer,
adopts the canonical tenant UUID, and starts a background bearer refresher — all
in one `await`. This example instead runs the four underlying steps by hand,
because a tenant-scoped **read** needs that canonical UUID *in your own hands* for
the filter, and `authenticateBearer` is how you get it back. They're short:

```ts
import { UdbProject } from "@udb_plus/sdk";

// 1. Connect. Two listeners: data plane + the separate auth plane.
const udb = await UdbProject.connect({
  target:     "127.0.0.1:50051", // DataBroker (public) listener
  authTarget: "127.0.0.1:50061", // auth (native-bearer) listener
  tenantId:   "acme",            // the HUMAN code you know up front
  projectId:  "default",
  purpose:    "billing-app",
});

// 2. Log in → RS256 JWT.
const login = await udb.login({ username: "admin", password: process.env.UDB_PASS! });

// 3. Verify the bearer and read the VERIFIED tenant UUID out of it.
const verified = await udb.auth.authenticateBearer(login.access_token);
const tenant = verified.principal.tenant_id;   // the canonical UUID — never "acme"
udb.setTenant(tenant);                          // adopt it on every outbound call

// 4. Do tenant-scoped work. tenant_id is the UUID RLS matches on.
const invoices = udb.data.table("billing.v1.Invoice", { key: ["invoice_id"] });
await invoices.upsert({ invoice_id: "inv-1001", tenant_id: tenant, amount_cents: 4200, status: "paid" });
```

No `protoc` step at call time: rows go as plain objects and the broker validates
them against the proto it serves. The generated `Invoice` type just gives you
compile-time field checking.

## What the full example does, in order

[`src/main.ts`](src/main.ts) walks the whole flow and prints each stage:

1. **Connect** to both listeners. The data plane is `:50051`; the native auth
   control plane is `:50061`. The SDK dials both — you don't wire two clients.
2. **Authn** — a real password login returns an RS256 JWT (`login.access_token`).
   If the broker has no signing key, you'll see a clear error instead of a
   mystery failure.
3. **Adopt the canonical tenant** — `authenticateBearer` verifies the token and
   returns the *principal*, including the real tenant UUID and scopes. From here
   on, `udb.setTenant(uuid)` means the human code `"acme"` is only a memory; the
   UUID is the truth. A tenant-scoped RPC rejects a mismatched tenant, so this
   step is not optional.
4. **CRUD** a tenant-scoped `billing.Invoice`: create, read, update, delete —
   each carrying the adopted UUID as `tenant_id`. A tenant-scoped read *requires*
   `tenant_id` in the filter; the broker rejects an unscoped read outright.
5. **Isolation check** — it re-creates the row, then asks for a *different*
   tenant's data and shows you it comes back empty. That proves the guard is
   real rather than asking you to take it on faith.

## Run it

**Prerequisites:** Docker (for PostgreSQL), Node 18+, `openssl`, and `buf` (to
generate the row type). You also need the **`udb` CLI** — build it with
`cargo build --release --bin udb`, or point `UDB_CLI` at a binary. The scripts
auto-find `udb` on `PATH` and under `target/{release,debug}`.

```bash
# 1. Postgres. php_quickstart's compose already maps host :55432 → container
#    :5432 (the port bootstrap.sh expects); the enterprise DB is a separate name.
cd ../php_quickstart && docker compose up -d && cd ../ts_enterprise
docker exec udb-php-quickstart-postgres-1 psql -U udb -d udb -c "CREATE DATABASE udb_enterprise;" || true

# 2. Provision: RS256 keys, HMAC secrets, the admin user (bound to
#    organization_owner → udb:*), and a seeded policy_rules rule. Writes secrets/.
./scripts/bootstrap.sh

# 3. Start the broker in ENTERPRISE mode (keep this terminal open).
#    Wait for "UDB DataBroker is ready".
./scripts/serve.sh

# 4. (second terminal) Install the SDK, generate the Invoice type, run.
npm install
npm run gen                       # udb proto export + buf generate → gen/billing/v1/invoice.ts
set -a; source secrets/enterprise.env; set +a
npm start
```

Expected output:

```
transport: plaintext
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

### Configuration

`bootstrap.sh` writes these into `secrets/enterprise.env`; you `source` that
before `npm start`. The client (`src/main.ts`) reads only these five — the rest
of the env file is for the broker. Everything except `UDB_PASS` has a default:

| Env var | What it is | Default |
|---|---|---|
| `UDB_PASS` | admin password (from `bootstrap.sh`) | **required** |
| `UDB_TARGET` | DataBroker (public) address | `127.0.0.1:50051` |
| `UDB_AUTH_TARGET` | auth (native-bearer) address | `127.0.0.1:50061` |
| `UDB_USER` | username to log in as | `admin` |
| `UDB_TENANT` | human tenant code | `acme` |

## Common mistakes this example is designed to prevent

- **Using the tenant code in filters.** Always use the UUID from
  `verified.principal.tenant_id` after `udb.setTenant(...)`, never `"acme"`.
  RLS matches on the UUID, so a human code silently returns zero rows instead of
  erroring — the most confusing way to fail.
- **Logging in on the wrong port.** `login` lives on the auth listener
  (`:50061`), not the data port. Point `authTarget` at it.
- **Expecting the org-owner role to be enough.** Authorization is default-deny:
  the data plane evaluates a Casbin policy from the `udb_authz.policy_rules`
  governance table on *every* CRUD call, and without a matching rule even
  `udb:*` gets `PERMISSION_DENIED`. `bootstrap.sh` seeds a rule into
  `policy_rules` (via `udb policy-seed`) so `Select`/`Upsert`/`Delete` are
  allowed for this tenant.
- **Hand-writing the row shape.** The `Invoice` type is generated from
  `invoice.proto` with `buf generate`, using **snake_case** field names
  (`invoice_id`, `amount_cents`, …) so it matches the JSON the SDK's `data.table`
  puts on the wire. Rename a field to camelCase and the row won't round-trip —
  keep the generated names.
- **Forgetting isolation is server-enforced.** You don't police tenant
  boundaries in app code; UDB's row-level security does. Step 5 proves it.

## One authorization engine (a real UDB gotcha)

UDB authorizes through **one** Casbin engine over the `udb_authz.policy_rules`
governance table — the same rules drive both the data-plane `authorize()` gate
on every `Select`/`Upsert`/`Delete` and the control-plane `AuthzService.Check`
that answers the SDK's `udb.authz.can` / `udb.authz.require` queries. It is
**default-deny**:

- The data plane reads a **shared, PG-warmed snapshot** of `policy_rules`
  (revision-fenced): it evaluates the real RPC action the broker submits —
  `Select` / `Upsert` / `Delete` — against the rules for your tenant. With no
  matching rule, even `udb:*` gets `PERMISSION_DENIED`.
- You configure policy per-tenant at runtime via `AuthzService.CreatePolicyRule`
  (effective immediately — the snapshot cache invalidates on mutation and a
  warmer reloads), or offline via **`udb policy-seed`** (emits INSERTs into
  `udb_authz.policy_rules`, piped to psql). `bootstrap.sh` uses the offline path.

Because it is one engine over one table, seeding a rule that allows
`Select`/`Upsert`/`Delete` for this tenant is all this example needs — there is
no separate ABAC lane to keep in sync. (`UDB_ABAC_DEFAULT_ALLOW=true` is the
dev-only escape hatch that flips the default to allow.)

## Going further: TLS / mTLS

This example runs plaintext on localhost with real JWT auth (terminating TLS at
an edge proxy is a valid hardened shape). For in-broker TLS, set `UDB_TLS_*` (and
`UDB_MTLS_*`) on the broker and pass `tls: { rootCerts, certChain, privateKey }`
to `UdbProject.connect` — `src/main.ts` already reads `UDB_TLS_CA` /
`UDB_TLS_CLIENT_CERT` / `UDB_TLS_CLIENT_KEY` and switches to mTLS when they're
set. See [`../../docs/enterprise-deployment.md`](../../docs/enterprise-deployment.md).

## Files

- `proto/billing/v1/invoice.proto` — your tenant-scoped table (the only schema you write).
- `gen/billing/v1/invoice.ts` — the **generated** `Invoice` type (do not edit).
- `src/main.ts` — the connect → authn → authz → CRUD → isolation flow.
- `scripts/bootstrap.sh` — one-time provisioning (keys, secrets, admin, policy_rules rule).
- `scripts/serve.sh` — start the broker in enterprise mode.
- `scripts/serve-mtls.sh` — the same, with in-broker TLS/mTLS.
</content>
</invoke>
