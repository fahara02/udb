# UDB PHP Quickstart

The smallest end-to-end UDB example in PHP: **one table, basic CRUD**, then the
**same code with native authn + authz** layered on top.

If you've seen `examples/php_arbitary_project`, this is the stripped-down
sibling — no vectors, no object storage, no MFA, one proto message. Start here,
then graduate to that example when you need the rest.

You write exactly **three** things; everything else is generated:

| You write | Generated for you |
|-----------|-------------------|
| `proto/shop/v1/customer.proto` — one table | `proto/udb/**`, `third_party/` (via `udb proto export`) |
| `01_crud.php` — create / read / update / delete | `gen/PhpQuickstart/**` (via `buf generate`) |
| `02_crud_with_auth.php` — same CRUD + login + permission checks | the `shop`.`customers` table (by the broker on boot) |

## Prerequisites

- **Docker** — for PostgreSQL + Redis.
- **The UDB CLI** — build it once with `cargo build --release --bin udb-proto-parser`,
  or set `$env:UDB_CLI` to a downloaded release binary. The scripts find it
  automatically (see `scripts/_udb.ps1`).
- **buf** — `buf --version` ≥ 1.x.
- **PHP 8.1+** with the **`grpc`** and **`protobuf`** extensions for the live run
  (`pecl install grpc protobuf`). No extension? Use the Docker run shown at the
  bottom instead.

---

## Example 1 — basic CRUD

### 1. Bring in the UDB protos

```powershell
./scripts/export-protos.ps1
```

This runs `udb proto export --out proto`, which writes the UDB annotation
contract and broker wire surface into `proto/udb/**` and vendors `google/api`
into `third_party/`. Your `customer.proto` imports one file from it:

```proto
import "udb/core/common/v1/db.proto";
```

### 2. Look at the table you're defining

`proto/shop/v1/customer.proto` — one message, annotated with the real UDB
options. One message == one table:

```proto
message Customer {
  option (udb.core.common.v1.table) = {
    table_name: "customers"  schema_name: "shop"  is_table: true
  };
  string customer_id = 1 [(udb.core.common.v1.column) = {
    column_name: "customer_id"  sql_type: "UUID"  primary_key: true
    default_value: "gen_random_uuid()"
  }];
  string email = 2 [(udb.core.common.v1.column) = {
    column_name: "email"  sql_type: "VARCHAR(320)"  not_null: true  unique: true
  }];
  // … full_name, loyalty_points, created_at …
}
```

That compiles, through UDB, to exactly this PostgreSQL:

```sql
CREATE TABLE IF NOT EXISTS "shop"."customers" (
    "customer_id"    UUID DEFAULT gen_random_uuid(),
    "email"          VARCHAR(320) NOT NULL,
    "full_name"      TEXT,
    "loyalty_points" BIGINT DEFAULT 0 NOT NULL,
    "created_at"     TIMESTAMPTZ DEFAULT now() NOT NULL,
    CONSTRAINT "pk_customers" PRIMARY KEY ("customer_id")
);
CREATE UNIQUE INDEX "uidx_shop_customers_email" ON "shop"."customers" ("email");
```

(Preview it yourself any time with `udb sql proto`.)

### 3. Generate the PHP model + install the SDK

```powershell
./scripts/generate.ps1
```

`buf generate --path proto/shop` emits PHP **only** for `customer.proto` →
`gen/PhpQuickstart/Shop/V1/Customer.php`. The UDB message/service classes are
not regenerated — they ship inside the `fahara02/udb-laravel` SDK that
`composer install` pulls in.

### 4. Start the data backends

```powershell
docker compose up -d        # PostgreSQL (:55432) + Redis (:56379)
```

### 5. Run the broker (keep this terminal open)

```powershell
./scripts/serve-broker.ps1
```

It serves `proto/` and connects to the containers above. On boot it force-syncs
the schema, so `shop`.`customers` is created from your proto automatically — no
migration step to run by hand.

### 6. Run the client

```powershell
$env:UDB_TARGET = "127.0.0.1:50051"
php 01_crud.php
```

Expected output:

```
created   affected_rows=1
read      rows=1  {"customer_id":"…","email":"ada@example.com","full_name":"Ada Lovelace","loyalty_points":100,…}
updated   rows=1  {…,"full_name":"Augusta Ada King","loyalty_points":250,…}
deleted   rows=0

CRUD OK
```

That's the whole loop: `$client->upsert()`, `$client->select()`,
`$client->delete()` — each carrying a `UdbMetadata` (tenant / user / purpose /
scopes) the broker requires on every call.

---

## Example 2 — the same CRUD, with native authn + authz

`02_crud_with_auth.php` is `01_crud.php` plus two ideas, using the broker's
**native** auth services (no external IdP):

1. **Authn** — exchange a UDB **API key** for a verified `Principal`
   (`$auth->authenticateApiKey(...)`), and build the request metadata from the
   scopes the broker actually granted.
2. **Authz** — before each operation, ask the policy engine for a decision
   (`$auth->can($resource, 'read'|'write')`) and stop on denial.

### 1. Mint an API key (broker must be running)

```powershell
./scripts/seed-api-key.ps1
```

This calls the broker's `ApiKeyService` and prints JSON containing a one-time
`plain_key`. Copy it:

```powershell
$env:UDB_TARGET  = "127.0.0.1:50051"
$env:UDB_API_KEY = "<the plain_key value>"
```

### 2. Run the client

```powershell
php 02_crud_with_auth.php
```

Expected output (with the default-allow broker from step 5):

```
authenticated  user=php-quickstart  method=api_key  scopes=[udb:read,udb:write]
authorized action=write effect=ALLOW
created    affected_rows=1
authorized action=read  effect=ALLOW
read       rows=1  {…"email":"ada@example.com"…}
authorized action=write effect=ALLOW
deleted    email=ada@example.com

AUTH + CRUD OK
```

### 3. (Optional) See the policy engine actually enforce

By default the quickstart broker runs with `UDB_ABAC_DEFAULT_ALLOW=true`, so
every decision is `ALLOW`. To watch it deny and then allow, restart the broker
with default-deny and seed an explicit policy:

```powershell
# stop the broker (Ctrl+C), then:
$env:UDB_ABAC_DEFAULT_ALLOW = "false"
./scripts/serve-broker.ps1
```

```powershell
# in another terminal — grant the principal write on the customers table:
$env:UDB_GRPC_TARGET = "127.0.0.1:50051"; $env:UDB_TENANT_ID = "quickstart"
udb auth policy put --subject php-quickstart --action write `
    --resource shop.v1.Customer --effect ALLOW --tenant quickstart
```

Run `php 02_crud_with_auth.php` before seeding the policy and you'll see
`DENIED action=write …`; run it after and the same call is `authorized`. The
PHP code never changed — the broker's policy did.

---

## Regenerating / cleaning up

Everything under `proto/udb/`, `third_party/`, `gen/`, and `vendor/` is
generated (and git-ignored). To rebuild from scratch:

```powershell
./scripts/export-protos.ps1   # proto/udb/** + third_party/
./scripts/generate.ps1        # gen/** + vendor/
```

Stop the stack with `docker compose down` (add `-v` to wipe the database).

## No local PHP gRPC extension?

Run the client in a container that has it (mirrors `php_arbitary_project`):

```powershell
docker run --rm -it -v "${PWD}:/app" -w /app `
  --network host php:8.3-cli bash -lc `
  "pecl install grpc protobuf >/dev/null && docker-php-ext-enable grpc protobuf && \
   php -d extension=grpc -d extension=protobuf 01_crud.php"
```

## Where to go next

- `examples/php_arbitary_project` — the full tour: vectors, object storage,
  caching, batch RPCs, and a richer multi-table schema.
- `udb sdk generate --lang php` — regenerate the SDK itself from the proto
  descriptor set.
