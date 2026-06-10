# UDB PHP Quickstart

Learn UDB from the absolute basics, in three small steps. Each example adds
**one** concept on top of the previous one and is meant to be run in order:

| # | Script | New concept | Proto |
|---|--------|-------------|-------|
| 1 | `01_crud.php` | Create / read / update / delete one row | `customer.proto` |
| 2 | `02_authz.php` | Authorization — the broker enforces operation scopes | (same `Customer`) |
| 3 | `03_relations.php` | A second, related table + querying many rows | `order.proto` |

You hand-write only the protos and the three scripts. Everything else
(`proto/udb/**`, the PHP models, the database tables) is generated.

> This is the small sibling of `examples/php_arbitary_project`. Start here; go
> there when you want vectors, object storage, and a richer schema.

---

## One-time setup

Do this once; all three examples reuse it.

### Prerequisites
- **Docker** — for PostgreSQL + Redis.
- **The UDB CLI** — `cargo build --release --bin udb`, or set
  `$env:UDB_CLI` to a release binary. The scripts find it automatically.
- **buf** ≥ 1.x.
- **PHP 8.1+** with the **`grpc`** and **`protobuf`** extensions for the live run
  (`pecl install grpc protobuf`). No extension? See [Running without a local
  gRPC extension](#running-without-a-local-grpc-extension).

### 1. Bring in the UDB protos
```powershell
./scripts/export-protos.ps1
```
`udb proto export` writes the UDB annotation contract into `proto/udb/**` so your
protos can `import "udb/core/common/v1/db.proto"`.

### 2. Generate the PHP models + install the SDK
```powershell
./scripts/generate.ps1
```
`buf generate --path proto/shop` emits PHP **only** for your protos
(`gen/PhpQuickstart/Shop/V1/Customer.php`, `Order.php`). The UDB request/response
classes (`Udb\Entity\V1\*`) ship inside the `fahara02/udb-laravel` SDK that
`composer install` pulls in.

### 3. Start the data backends
```powershell
docker compose up -d        # PostgreSQL (:55432) + Redis (:56379)
```
> Postgres is built from `Dockerfile.postgres` (stock `postgres:16-alpine` plus
> `pg_partman`, which the broker's control-plane schemas need). The first
> `up` compiles the extension once.
>
> If port `55432` is taken (Windows reserves some high ports), pick another:
> `$env:UDB_POSTGRES_PORT="15432"` before `docker compose up -d` **and** before
> running the broker.

### 4. Run the broker (keep this terminal open)
```powershell
./scripts/serve-broker.ps1
```
It serves `proto/shop`, connects to the containers, and **force-syncs the schema
on boot** — so `shop.customers` and `shop.orders` are created from your protos
automatically. No manual migration step.

You're ready. Open a second terminal for the examples and set the target once:
```powershell
$env:UDB_TARGET = "127.0.0.1:50051"
```

---

## Example 1 — basic CRUD

`customer.proto` declares one table. One message == one table; the
fully-qualified name `shop.v1.Customer` is the key you pass on every call.

```proto
message Customer {
  option (udb.core.common.v1.table) = { table_name: "customers" schema_name: "shop" is_table: true };
  string customer_id    = 1 [(udb.core.common.v1.column) = { sql_type: "UUID" primary_key: true default_value: "gen_random_uuid()" }];
  string email          = 2 [(udb.core.common.v1.column) = { sql_type: "VARCHAR(320)" not_null: true unique: true }];
  string full_name      = 3 [(udb.core.common.v1.column) = { sql_type: "TEXT" }];
  int64  loyalty_points = 4 [(udb.core.common.v1.column) = { sql_type: "BIGINT" not_null: true default_value: "0" }];
}
```

→ the broker creates exactly:
```sql
CREATE TABLE "shop"."customers" (
  "customer_id"    UUID DEFAULT gen_random_uuid(),
  "email"          VARCHAR(320) NOT NULL,
  "full_name"      TEXT,
  "loyalty_points" BIGINT DEFAULT 0 NOT NULL,
  "created_at"     TIMESTAMPTZ DEFAULT now() NOT NULL,
  CONSTRAINT "pk_customers" PRIMARY KEY ("customer_id")
);
```

Run it:
```powershell
php 01_crud.php
```
```
created   affected_rows=1
read      rows=1  {"created_at":"…","customer_id":"…","email":"ada@example.com","full_name":"Ada Lovelace","loyalty_points":100}
updated   rows=1  {…,"full_name":"Augusta Ada King","loyalty_points":250}
deleted   rows=0

CRUD OK
```

Key points:
- The whole loop is `$client->upsert()`, `$client->select()`, `$client->delete()`.
- We **own the primary key** (`customer_id`) and pass `conflict_fields: ["customer_id"]`, so the same call inserts or updates and re-runs are idempotent. Upserting on a non-PK column requires a declared unique index.
- `record_json` is the row; columns with defaults (`created_at`) are filled by the database when omitted.

---

## Example 2 — authorization (operation scopes)

Same table, same operations — the only thing that changes is the **scopes** in
the request metadata. The broker enforces, on every call:

- a write (`Upsert`/`Delete`) requires the **`udb:write`** scope,
- a read (`Select`) requires the **`udb:read`** scope.

Your code does not check anything — the broker does, returning
`INVALID_ARGUMENT` with a clear message when the scope is missing.

```powershell
php 02_authz.php
```
```
read-only identity, Upsert:
  denied as expected — gRPC 3: scope udb:write is required
read-write identity, Upsert:
  allowed — affected_rows=1
read-only identity, Select:
  allowed — rows=1
write-only identity, Select:
  denied as expected — gRPC 3: scope udb:read is required

AUTHZ OK
```

In a real app the scopes come from your authenticated principal; here we just
vary them to watch the broker allow and deny. See [Authorization & identity —
the full picture](#authorization--identity--the-full-picture) for where policies
and login fit in.

---

## Example 3 — relationships & queries

`order.proto` adds a second table that references a customer by id:

```proto
message Order {
  option (udb.core.common.v1.table) = { table_name: "orders" schema_name: "shop" is_table: true };
  string order_id     = 1 [(udb.core.common.v1.column) = { sql_type: "UUID" primary_key: true default_value: "gen_random_uuid()" }];
  string customer_id  = 2 [(udb.core.common.v1.column) = { sql_type: "UUID" not_null: true }];
  int64  amount_cents = 3 [(udb.core.common.v1.column) = { sql_type: "BIGINT" not_null: true default_value: "0" }];
  string status       = 4 [(udb.core.common.v1.column) = { sql_type: "VARCHAR(32)" not_null: true default_value: "'pending'" }];
}
```

The script creates one customer, places several orders for them, then runs a
real query — filter by customer, sort by amount, limit the result:

```powershell
php 03_relations.php
```
```
customer upserted: 0a0a0a0a-0000-4000-8000-000000000001
orders upserted: 4
top 3 orders for customer (amount desc):
  order=…0002  amount_cents=4999  status=paid
  order=…0003  amount_cents=2500  status=paid
  order=…0001  amount_cents=1299  status=paid

RELATIONS OK
```

Re-running the script is safe — it upserts the same rows (deterministic ids), so
the output never changes.

Key points:
- A `SelectRequest` can carry a `filter` (Struct of column → value), `sort`
  (`[new Sort(['field' => …, 'descending' => true])]`), and `limit`.
- The response gives you both `recordsJson` (raw JSON strings) and structured
  `rows` whose `getFields()` map is keyed by column name.

---

## Authorization & identity — the full picture

Example 2 shows the part of authorization an **application client** uses on the
public port: the broker checks the request's **scopes** on every data call.

Two further pieces live in UDB's **native control plane** — `AuthnService`
(login, users, sessions, MFA…), `AuthzService` (roles, policies, decisions),
`ApiKeyService`, plus Tenant/Notification/Analytics. These are **deliberately
bound to a separate internal listener** (default `127.0.0.1:<public_port+10>`,
i.e. `:50061`) behind a control-plane bearer, *not* the public DataBroker port —
they are meant for a trusted PEP/gateway, not arbitrary clients. Calling them on
`:50051` returns `Unimplemented`.

The maintained native-service overview is in
[`../../docs/native-services.md`](../../docs/native-services.md). Wiring up
that control plane (minting an admin bearer, seeding policies) is an advanced
topic beyond this quickstart.

---

## Regenerating / cleaning up

Everything under `proto/udb/`, `third_party/`, `gen/`, and `vendor/` is generated
and git-ignored. Rebuild from scratch:
```powershell
./scripts/export-protos.ps1   # proto/udb/** + third_party/
./scripts/generate.ps1        # gen/** + vendor/
```
Stop the stack with `docker compose down` (add `-v` to wipe the database).

## Running without a local gRPC extension

Run a script in a container that has the extension:
```powershell
docker run --rm -it -v "${PWD}:/app" -w /app --network host php:8.3-cli bash -lc `
  "pecl install grpc protobuf >/dev/null && docker-php-ext-enable grpc protobuf && php 01_crud.php"
```

## Verifying the broker yourself

`.probe-unimplemented.sh` calls every RPC over gRPC reflection and prints which
are reachable on the public port:
```powershell
bash .probe-unimplemented.sh 127.0.0.1:50051 | sort
```

## Where to go next
- `examples/php_arbitary_project` — vectors, object storage, caching, batch RPCs, richer schema.
- `udb sdk generate --lang php` — regenerate the SDK itself from the proto descriptor set.
