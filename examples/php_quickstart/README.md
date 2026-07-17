# UDB PHP Quickstart

Learn UDB from zero in three tiny PHP scripts. You define a table as a **proto**,
and the broker turns it into a real Postgres table plus a typed gRPC API — you
never hand-write DDL, migrations, or a data-access layer. Each script adds
exactly one idea and is meant to be run in order:

| # | Script | What it adds |
|---|--------|--------------|
| 1 | `01_crud.php` | Create / read / update / delete one row (`customer.proto`) |
| 2 | `02_authz.php` | The broker enforces `udb:read` / `udb:write` scopes on every call |
| 3 | `03_relations.php` | A second related table + a real query (filter, sort, limit) |

You hand-write only the protos and these three scripts. Everything else — the UDB
contract protos, the PHP models, the database tables — is generated for you.

> Small sibling of `examples/php_arbitary_project`. Start here; go there for
> vectors, object storage, caching, and a richer schema.

## Setup once, run in ~30 seconds

**You need:** Docker, PHP 8.1+ with the `grpc` and `protobuf` extensions
(`pecl install grpc protobuf` — or use the [container fallback](#no-local-grpc-extension)),
`buf` ≥ 1.x, and the `udb` CLI (`cargo build --release --bin udb`, or point
`$env:UDB_CLI` at a release binary — the scripts find it either way).

```powershell
# 1. Bring the UDB contract protos into proto/udb/** so your protos can import them
./scripts/export-protos.ps1

# 2. Generate the PHP models (buf) and install the SDK (composer)
./scripts/generate.ps1

# 3. Start the data backends: Postgres on :55432, Redis on :56379
docker compose up -d

# 4. Run the broker — keep this terminal open. It serves proto/shop and
#    force-syncs the schema on boot, so shop.customers / shop.orders just appear.
./scripts/serve-broker.ps1
```

Then, in a second terminal:

```powershell
$env:UDB_TARGET = "127.0.0.1:50051"
php 01_crud.php
php 02_authz.php
php 03_relations.php
```

Each script prints its steps and ends with `CRUD OK` / `AUTHZ OK` / `RELATIONS OK`.

> **Port 55432 taken?** Windows reserves some high ports. Pick another with
> `$env:UDB_POSTGRES_PORT = "15432"` **before** `docker compose up -d` *and*
> before running the broker (the broker reads the same variable).
>
> The Postgres image (`Dockerfile.postgres`) is `postgres:16-alpine` plus
> `pg_partman`, which the broker's control-plane schemas need. The first `up`
> compiles that extension once.

## What each example teaches

**1 — CRUD.** `customer.proto` declares one table. One message = one table, and
its fully-qualified name (`shop.v1.Customer`) is the key you pass on every call.
The broker turns these annotations:

```proto
message Customer {
  option (udb.core.common.v1.table) = { table_name: "customers" schema_name: "shop" is_table: true };
  string customer_id    = 1 [(udb.core.common.v1.column) = { sql_type: "UUID" primary_key: true default_value: "gen_random_uuid()" }];
  string email          = 2 [(udb.core.common.v1.column) = { sql_type: "VARCHAR(320)" not_null: true unique: true }];
  string full_name      = 3 [(udb.core.common.v1.column) = { sql_type: "TEXT" }];
  int64  loyalty_points = 4 [(udb.core.common.v1.column) = { sql_type: "BIGINT" not_null: true default_value: "0" }];
}
```

into a real `"shop"."customers"` table (UUID PK, unique email, a
broker-managed `created_at`), then serves `$client->upsert()`,
`->select()`, `->delete()` against it. Because the script owns the primary key
and passes `conflict_fields: ['customer_id']`, the same call inserts or updates
and re-runs are idempotent. (Upserting on a non-PK column needs a declared unique
index.)

**2 — authorization.** Identical CRUD calls — the only thing that changes is the
`scopes` in the request metadata. The broker enforces, on every call: a write
(`Upsert`/`Delete`) needs `udb:write`, a read (`Select`) needs `udb:read`. Your
code checks nothing; the broker returns gRPC `INVALID_ARGUMENT` with a clear
message (`scope udb:write is required`) when a scope is missing. The script runs
one allowed and one denied identity for each so you watch the guard work.

**3 — relations & queries.** `order.proto` adds an `orders` table that references
a customer by id. The script creates one customer, places several orders, then
runs a real query — `filter` (a `Struct` of column → value), `sort`
(`new Sort(['field' => 'amount_cents', 'descending' => true])`), and `limit`. The
response gives you both `getRecordsJson()` (raw JSON) and structured `getRows()`,
whose `getFields()` map is keyed by column name. Deterministic ids make re-runs
idempotent, so the output never changes.

## Configuration

Every script and the broker read a small set of environment variables:

| Env var | What it is | Default |
|---|---|---|
| `UDB_TARGET` | broker (public) address the scripts dial | `127.0.0.1:50051` |
| `UDB_POSTGRES_PORT` | host port for the Postgres container | `55432` |
| `UDB_REDIS_PORT` | host port for the Redis container | `56379` |
| `UDB_CLI` | path to a `udb` release binary (else the scripts build/find it) | auto |

## Common mistakes this prevents

- **Calling native services on the public port.** Example 2 shows the
  authorization an app client uses on `:50051`: per-request **scopes**. The rest
  of UDB's control plane — `AuthnService` (login, users, MFA), `AuthzService`
  (roles, policies), `ApiKeyService`, plus Tenant/Notification/Analytics — is
  deliberately bound to a separate internal listener (default `:50061`, the
  public port + 10) behind a control-plane bearer. It's meant for a trusted
  gateway, not arbitrary clients. Call those RPCs on `:50051` and you get
  `Unimplemented`. Wiring that plane up (minting an admin bearer, seeding
  policies) is beyond this quickstart — see
  [`../../docs/native-services.md`](../../docs/native-services.md).
- **Passing a bare request.** Every call needs `UdbMetadata` (tenant, user,
  scopes, project). All three scripts use tenant code `quickstart` consistently,
  so the broker resolves it to the same tenant every time — keep it consistent or
  your rows land under a different tenant and reads come back empty.
- **Forgetting the runtime extensions.** The SDK installs without them, but a
  live run needs PHP's `grpc` + `protobuf` extensions.

## Regenerating & cleaning up

Everything under `proto/udb/`, `third_party/`, `gen/`, and `vendor/` is generated
and git-ignored. Rebuild from scratch:

```powershell
./scripts/export-protos.ps1   # proto/udb/** + third_party/
./scripts/generate.ps1        # gen/** + vendor/
```

Stop the stack with `docker compose down` (add `-v` to wipe the database).

## No local gRPC extension

Run a script inside a container that already has the extensions:

```powershell
docker run --rm -it -v "${PWD}:/app" -w /app --network host php:8.3-cli bash -lc `
  "pecl install grpc protobuf >/dev/null && docker-php-ext-enable grpc protobuf && php 01_crud.php"
```

## Verify the broker yourself

`.probe-unimplemented.sh` calls every RPC over gRPC reflection and prints which
are reachable on the public port:

```powershell
bash .probe-unimplemented.sh 127.0.0.1:50051 | sort
```

## Where to go next

- `examples/php_arbitary_project` — vectors, object storage, caching, batch RPCs, a richer schema.
- `udb sdk generate --lang php` — regenerate the SDK itself from the proto descriptor set.
