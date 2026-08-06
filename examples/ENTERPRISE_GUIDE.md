# UDB enterprise examples — one guide, three languages

`go_enterprise`, `python_enterprise`, and `ts_enterprise` are the **same program
in three languages**. Each one walks the real enterprise path end to end:

1. **Log in** with a username + password against the auth listener → an RS256 JWT.
2. **Adopt the canonical tenant UUID** the broker verifies from that JWT (never
   the human code you typed).
3. **CRUD a tenant-scoped table** (`billing.v1.Invoice`) that the broker
   authorizes on every call (default-deny).
4. **Prove tenant isolation** — a read scoped to another tenant returns nothing.

Pick the language you work in; they all do the identical thing and print the
identical lines.

## The one thing to remember

> You **log in** with the human tenant *code* (`acme`), but you do **all data
> work** with the canonical tenant *UUID* the broker hands back.

Row-level security compares UUIDs. `"acme"` is not a UUID, so a filter on the
code silently returns zero rows — the single most common UDB integration bug.
Every example adopts the verified UUID right after login and uses it everywhere.

## Two listeners

- **Data plane** — `127.0.0.1:50051` (the public DataBroker). Your CRUD goes here.
- **Auth plane** — `127.0.0.1:50061` (the native-bearer listener). `login` goes
  here. Calling `login` on `:50051` returns `UNIMPLEMENTED` — that mix-up is the
  #2 stumble.

## Run it (one procedure for all three)

The broker bootstrap (offline admin + a seeded default-deny access policy) lives
in `ts_enterprise/scripts` and is language-agnostic — every example reuses it.

```bash
# Terminal 1 — bring up a broker (serve.sh stays in the foreground):
cd examples/ts_enterprise
./scripts/bootstrap.sh    # mints the admin user + seeds the allow policy; prints UDB_PASS
./scripts/serve.sh        # serves proto/billing in enterprise mode

# Terminal 2 — run ANY of the three. Reuse the env bootstrap.sh wrote:
set -a; source examples/ts_enterprise/secrets/enterprise.env; set +a

cd examples/go_enterprise      && UDB_TENANT=acme go run .
# or
cd examples/python_enterprise  && pip install udb-client && UDB_TENANT=acme python main.py
# or
cd examples/ts_enterprise      && npm install && npm run gen && npm start
```

All three read the same environment; only `UDB_PASS` is required.

| Env var | What it is | Default |
|---|---|---|
| `UDB_PASS` | admin password (printed by `bootstrap.sh`) | **required** |
| `UDB_TARGET` | DataBroker (public) address | `127.0.0.1:50051` |
| `UDB_AUTH_TARGET` | auth (native-bearer) address | `127.0.0.1:50061` |
| `UDB_USER` | username to log in as | `admin` |
| `UDB_TENANT` | human tenant code | `acme` |
| `UDB_PROJECT` | project id | `default` |

## The 30-second form, per language

**Go** — one call does connect + login + verify + adopt:

```go
session, _ := udbclient.ConnectEnterprise(ctx, udbclient.EnterpriseConfig{
    Target: "127.0.0.1:50051", AuthTarget: "127.0.0.1:50061",
    Username: "admin", Password: os.Getenv("UDB_PASS"),
    TenantCode: "acme", ProjectID: "default",
})
defer session.Close()
tenant := session.CanonicalTenantID // the verified UUID — use this everywhere
inv := session.Udb.Entity("billing.v1.Invoice", udbclient.Key("invoice_id"))
inv.Upsert(session.DataContext(ctx), map[string]any{"invoice_id": "inv-1", "tenant_id": tenant, "amount_cents": 4200})
```

**Python** — `create_udb` + `login_and_adopt_tenant`:

```python
udb = create_udb(target="127.0.0.1:50051", auth_target="127.0.0.1:50061",
                 tenant_id="acme", project_id="default", purpose="billing-app")
authn = udb.login_and_adopt_tenant("admin", os.environ["UDB_PASS"])
tenant = udb.config.tenant_id  # the verified canonical UUID
udb.data.upsert(message_type="billing.v1.Invoice",
                record={"invoice_id": "inv-1", "tenant_id": tenant, "amount_cents": 4200},
                conflict_fields=("invoice_id",))
```

**TypeScript** — `connectEnterprise` is the one-call path; `main.ts` shows the four
underlying steps so the canonical UUID and scopes are visible as you go:

```ts
const udb = await UdbProject.connectEnterprise({
  target: "127.0.0.1:50051", authTarget: "127.0.0.1:50061",
  tenantId: "acme", projectId: "default", purpose: "billing-app",
  username: "admin", password: process.env.UDB_PASS!,
});
const invoices = udb.data.table("billing.v1.Invoice", { key: ["invoice_id"] });
await invoices.upsert({ invoice_id: "inv-1", tenant_id: tenant, amount_cents: 4200 });
```

## Authorization is default-deny

Without a seeded access policy, even the org-owner admin gets `PERMISSION_DENIED`
on every CRUD call — that is the point. `bootstrap.sh` seeds three allow rules
(`Select` / `Upsert` / `Delete`) into the `udb_authz.policy_rules` governance
table (Casbin). The data plane reads a PG-warmed snapshot of that table, so the
same rule governs the SDK's `authz.can`/`require` and the server-side gate.

## No `buf`/`protoc` step required (except TS types)

Go and Python send rows as a plain map/dict (`record_json`); the broker validates
them against `proto/billing/v1/invoice.proto`. The TypeScript example additionally
generates a typed `Invoice` from that same proto (`npm run gen`) so its rows are
compile-time checked — the schema and your code cannot drift.

## Where each example lives

| Example | Entry point | SDK dependency |
|---|---|---|
| [`go_enterprise`](go_enterprise) | `main.go` | `github.com/fahara02/udb/sdk/go` |
| [`python_enterprise`](python_enterprise) | `main.py` | `udb-client` (PyPI) |
| [`ts_enterprise`](ts_enterprise) | `src/main.ts` | `@udb_plus/sdk` (npm) — also owns `scripts/` |
