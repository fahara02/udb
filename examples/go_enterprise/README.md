# Connect to UDB from Go — the enterprise path

> Part of the enterprise trio (Go / Python / TypeScript — the same program in
> three languages). Start with the shared **[ENTERPRISE_GUIDE.md](../ENTERPRISE_GUIDE.md)**
> for the flow, the one run procedure, and how the three compare.

This is the shortest safe way to go from *username + password* to *doing
tenant-scoped work* against a UDB broker, in one call: `ConnectEnterprise`.

If you only remember one thing: **log in with the human tenant *code* (e.g.
`acme`), but do all your data work with the canonical tenant *UUID* the broker
hands back.** Mixing those two up is the single most common UDB integration bug —
your reads quietly return nothing (row-level security compares UUIDs), or your
writes stamp a tenant value the database can't use. `ConnectEnterprise` makes the
switch for you and refuses to let you use the unverified code by accident.

## The 30-second version

```go
session, err := udbclient.ConnectEnterprise(ctx, udbclient.EnterpriseConfig{
    Target:     "127.0.0.1:50051", // the DataBroker (public) listener
    AuthTarget: "127.0.0.1:50061", // the auth (native-bearer) listener (defaults to Target)
    Username:   "admin",
    Password:   os.Getenv("UDB_PASS"),
    TenantCode: "acme",            // the HUMAN code you know up front
    ProjectID:  "default",
})
if err != nil {
    log.Fatal(err)
}
defer session.Close()

// This is the value you use everywhere — the VERIFIED canonical UUID,
// never "acme".
tenant := session.CanonicalTenantID

// Every data call goes through DataContext(ctx): it attaches your bearer token
// and the verified identity. Native services (Auth, Notification, …) use
// NativeContext(ctx) the same way.
inv := session.Udb.Entity("billing.v1.Invoice", udbclient.Key("invoice_id"))
_, err = inv.Upsert(session.DataContext(ctx), map[string]any{
    "invoice_id": "inv-1001",
    "tenant_id":  tenant, // the canonical UUID — this is what RLS matches
    "amount":     4200,
})
```

That's it. No `buf`/`protoc` step: rows go as a plain `map[string]any`
(`record_json`), and the broker validates them against the proto it serves
(`proto/billing/v1/invoice.proto`). Want a typed struct instead? Generate one
from that same proto with `protoc-gen-go` — the API is identical.

## What `ConnectEnterprise` actually does, in order

1. **Dials both listeners** — the data plane (`DataTarget`) and the auth plane
   (`AuthTarget`). UDB serves auth on a separate port; you don't have to wire two
   clients yourself.
2. **Logs in** with your username/password and the `TenantCode`/`ProjectID`
   hints, and gets back an RS256 JWT.
3. **Verifies the bearer** and reads the *principal* — the identity the broker
   actually authenticated, including the real tenant UUID.
4. **Adopts the canonical tenant** — stores that verified UUID as
   `CanonicalTenantID` and marks the session verified. From here on, the human
   code is only a memory; the UUID is the truth.
5. **Attaches the bearer + request/correlation IDs** to every context it hands
   you (`DataContext`, `NativeContext`), so calls don't fail with "request
   context required".

The full example in [`main.go`](main.go) also does a **cross-tenant isolation
check** at the end — it proves that a record carrying a *different* tenant's UUID
is rejected, so you can see the guard working rather than take it on faith.

## Run it

You need a broker with a bootstrapped admin and seeded access policy. The
TypeScript example next door already scripts exactly that, so reuse it:

```bash
# Terminal 1 — bring up a broker (serve.sh stays in the foreground):
cd ../ts_enterprise
./scripts/bootstrap.sh   # creates the admin user + seeds the access policy
./scripts/serve.sh

# Terminal 2 — run this example. UDB_PASS is the admin password bootstrap.sh printed:
cd ../go_enterprise
set -a; source ../ts_enterprise/secrets/enterprise.env; set +a   # or export UDB_* yourself
UDB_PASS="$UDB_ADMIN_PASSWORD" UDB_TENANT=acme go run .
```

### Configuration

Everything except `UDB_PASS` has a sensible default:

| Env var | What it is | Default |
|---|---|---|
| `UDB_PASS` | admin password (from `bootstrap.sh`) | **required** |
| `UDB_TARGET` | DataBroker (public) address | `127.0.0.1:50051` |
| `UDB_AUTH_TARGET` | auth (native-bearer) address | `127.0.0.1:50061` |
| `UDB_USER` | username to log in as | `admin` |
| `UDB_TENANT` | human tenant code | `acme` |
| `UDB_PROJECT` | project id | `default` |

## Common mistakes this example is designed to prevent

- **Using the tenant code in filters.** Always use `session.CanonicalTenantID`.
  If you try to `Adopt` a value that isn't a canonical lowercase UUID, the SDK
  rejects it up front instead of letting a bad identity leak into a query.
- **Calling a bare context.** Use `session.DataContext(ctx)` /
  `session.NativeContext(ctx)`; a plain `ctx` has no bearer and the broker will
  reject it.
- **Forgetting isolation is server-enforced.** You don't have to police tenant
  boundaries in your app — UDB's row-level security does.
  `session.ValidateTenant(recordTenantID)` gives you a fast client-side check on
  top, but the broker is the real guard.
