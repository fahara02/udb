# Connect to UDB from Python — the enterprise path

This is the shortest safe way to go from *username + password* to *doing
tenant-scoped work* against a UDB broker: `create_udb(...)` then one call,
`login_and_adopt_tenant`.

If you only remember one thing: **log in with the human tenant *code* (e.g.
`acme`), but do all your data work with the canonical tenant *UUID* the broker
hands back.** Mixing those two up is the single most common UDB integration bug —
your reads quietly return zero rows (row-level security compares UUIDs, not
codes), or your writes stamp a tenant value the database can't match.
`login_and_adopt_tenant` does the swap for you: it logs in, reads the verified
identity, and replaces the code you started with by the real UUID everywhere.

## The 30-second version

```python
from udb_client import create_udb

udb = create_udb(
    target="127.0.0.1:50051",       # the DataBroker (public) listener
    auth_target="127.0.0.1:50061",  # the auth (native-bearer) listener
    tenant_id="acme",               # the HUMAN code you know up front (a hint)
    project_id="default",
    purpose="billing-app",
    scopes=["udb:read", "udb:write"],
)

authn = udb.login_and_adopt_tenant("admin", os.environ["UDB_PASS"])

# This is the value you use everywhere — the VERIFIED canonical UUID, never "acme".
tenant = udb.config.tenant_id

# Every data call now carries your bearer token and the verified identity;
# login_and_adopt_tenant wired that into udb.data for you.
udb.data.upsert(
    message_type="billing.v1.Invoice",
    record={
        "invoice_id": "inv-1001",
        "tenant_id": tenant,   # the canonical UUID — this is what RLS matches
        "amount_cents": 4999,
        "status": "paid",
    },
    conflict_fields=("invoice_id",),
)
```

That's it. No `buf`/`protoc` step: rows go as a plain dict (`record_json`), and
the broker validates them against the proto it serves
(`proto/billing/v1/invoice.proto`). Want typed messages instead? Generate them
from that same proto — the API is identical.

## What `login_and_adopt_tenant` actually does, in order

1. **Logs in** over the auth listener with your username/password and the
   `tenant_id`/`project_id` hints you passed to `create_udb`, and gets back an
   RS256 JWT.
2. **Reads the *principal*** — the identity the broker actually authenticated,
   including the real tenant UUID and your granted scopes
   (`authn.principal.scopes`).
3. **Adopts the canonical tenant** — stores that verified UUID as
   `udb.config.tenant_id`. From here on, the human code is only a memory; the
   UUID is the truth.
4. **Installs the bearer** across every sub-client, including the data plane
   (`udb.data`). The SDK also auto-attaches `x-request-id` / `x-correlation-id`,
   so native RPCs don't fail with "request context required" — you never plumb a
   token by hand.

The full example in [`main.py`](main.py) then runs a complete CRUD cycle
(create → read → update → delete) and ends with a **cross-tenant isolation
check**: it reads with a *different* tenant's UUID and shows zero rows come back,
so you can watch the guard work rather than take it on faith.

## Run it

You need a broker with a bootstrapped admin and a seeded access policy. The
TypeScript example next door already scripts exactly that, so reuse it:

```bash
pip install udb-client

# Terminal 1 — bring up a broker (serve.sh stays in the foreground):
cd ../ts_enterprise
./scripts/bootstrap.sh   # creates the admin user + seeds the access policy
./scripts/serve.sh

# Terminal 2 — run this example. UDB_PASS is the admin password bootstrap.sh printed:
cd ../python_enterprise
set -a; source ../ts_enterprise/secrets/enterprise.env; set +a   # or export UDB_* yourself
UDB_PASS="$UDB_ADMIN_PASSWORD" UDB_TENANT=acme python main.py
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

- **Using the tenant code in filters.** Always read `udb.config.tenant_id` after
  login and use that UUID. A human code like `acme` in a `filter` matches
  nothing, because row-level security compares canonical UUIDs.
- **Forgetting to scope your reads.** A tenant-scoped read must include
  `tenant_id` in its `filter` (see the `select` calls in `main.py`). Leave it
  out and you're relying on the server's default scope instead of asking for
  what you mean.
- **Forgetting isolation is server-enforced.** You don't police tenant
  boundaries in your app — UDB's row-level security does. The cross-tenant read
  at the end of `main.py` returns zero rows to prove it.
