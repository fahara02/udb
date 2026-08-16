# Bring your own proto to UDB — a standalone Go project

This example answers one question: **how do I use UDB with *my* domain schema
instead of some built-in one, without vendoring the UDB source tree?**

You write an ordinary proto (`acme.billing.v1`), and it drives both sides at
once: `buf` turns it into Go structs your app imports, and the UDB CLI turns the
*same* proto into database migrations. Then a single Go client exercises all
three storage planes UDB gives you from one schema — relational rows, vector
search, and object storage — against a broker you start with Docker Compose. No
UDB checkout required: the CLI comes from a GitHub Release or a Docker image.

## The 30-second version

```powershell
# 1. Generate Go models (buf) + DB migrations (UDB) from proto/
.\scripts\generate.ps1 -Runner docker

# 2. Start the broker + its backing stores, then sync the schema in
.\scripts\serve.ps1
.\scripts\bootstrap.ps1 -Runner docker

# 3. Run the smoke client
$env:UDB_TARGET = "127.0.0.1:50051"
go run .
```

You should see log lines for a relational create/read/update/delete, a vector
upsert + search, and an object write/read + presigned URL — one client, one
proto, three planes.

## What the client actually does

`acme_billing.go` opens one plaintext gRPC connection and attaches request
metadata (tenant, scopes, project) once via `udbclient.New`. Every call reuses
that through `client.Context(ctx)`. In order, it:

1. **Relational CRUD** on `acme.billing.v1.Product` — upserts a row (conflict key
   `product_id`), selects it back, updates it, deletes it, and asserts the row
   count each time. Rows travel as JSON (`record_json`) validated against the
   proto the broker serves, so there's no hand-written SQL.
2. **Vector upsert + search** on the `acme_products` collection (768-dim, cosine —
   declared right in the proto's `vector_store` option). It writes one point and
   searches for it.
3. **Object storage** on the `acme-billing-documents` bucket — streams a small
   text object up with `PutObject`, reads it back with `GetObject` and checks the
   bytes match, then mints a short-lived presigned `GET` URL.

The interesting part is that all three come from the *same* `Product` /
`BillingDocument` messages in `proto/acme/billing/v1/acme_billing_v1.proto`. The
`table`, `vector_store`, and `object_store` options on those messages are what
tell UDB to provision a Postgres table, a Qdrant collection, and an S3/MinIO
bucket for you.

## Project layout

| Path | Purpose |
|------|---------|
| `proto/` | Your ACME Billing schema — the single source both `buf` and UDB read |
| `gen/` | Go structs `buf generate` produces from `proto/` |
| `db_ops/` | Migrations UDB generates from `proto/`; committed so you can read the DDL without running anything |
| `docker-compose.yml` | Local Postgres, Redis, Qdrant, MinIO, and the UDB broker |
| `scripts/` | Thin wrappers that call the UDB CLI as an external binary or Docker image |
| `acme_billing.go` | The Go SDK smoke client shown above |

## Generating from proto

`generate.ps1` runs `buf generate` and then asks the UDB CLI to build `db_ops/`
from `proto/` (`sync-migrations proto --backend all --force-bootstrap`). Pick how
the CLI is sourced with `-Runner`:

```powershell
# Docker image (no local binary needed):
.\scripts\generate.ps1 -Runner docker

# A GitHub Release binary instead:
$env:UDB_RUNNER = "release"
.\scripts\generate.ps1
```

If the Release asset name for your platform differs from what the script guesses,
point it straight at the download URL:

```powershell
$env:UDB_CLI_URL = "https://github.com/fahara02/udb/releases/download/<version>/<asset>"
.\scripts\generate.ps1 -Runner docker
```

Testing against a binary you built from a UDB checkout? Point the scripts at it —
but note the Docker runner runs the CLI *inside* a Linux container, so a Windows
`.exe` path won't execute there; give it a URL to a Linux binary via
`UDB_CLI_URL` instead.

```powershell
cargo build --release --bin udb
$env:UDB_CLI = "E:/Projects/udb/target/release/udb.exe"
.\scripts\generate.ps1 -Runner auto
```

## Configuration

| Env var | What it is | Default |
|---|---|---|
| `UDB_TARGET` | broker address the Go client dials | `localhost:50051` |
| `UDB_RUNNER` | how to source the CLI: `auto`, `docker`, or `release` | `auto` |
| `UDB_CLI` | path to a local `udb` binary (used by `auto`) | unset |
| `UDB_CLI_URL` | exact URL (or path) to a CLI binary to download | unset |
| `UDB_VERSION` | Release tag to pull (`latest` or e.g. `v0.5.16`) | `latest` |
| `UDB_GITHUB_REPO` | repo to fetch releases from | `fahara02/udb` |

`-Runner auto` tries, in order: `$UDB_CLI`, a `udb` on your `PATH`, then Docker.

## Checks

```powershell
buf lint
go test ./...
go build -o NUL .
```

## Common mistakes this example is designed to prevent

- **Skipping generate before serve.** The broker provisions storage from the
  migrations in `db_ops/`, and your Go code imports the structs in `gen/`. Both
  come out of `generate.ps1` — run it before `serve.ps1`/`go run .` or the schema
  and the models won't line up.
- **A short message type.** UDB matches rows by the *fully-qualified* proto name.
  The client uses `"acme.billing.v1.Product"`, not `"Product"` — the package
  prefix is load-bearing.
- **A Windows CLI path inside Docker.** `-Runner docker` runs the CLI in a Linux
  container. Feed it a Linux binary via `UDB_CLI_URL`, not a local `.exe`.
- **Drift between buf and UDB.** Both read `proto/`. Change a field in one place
  and regenerate both (that's all `generate.ps1` does), so your Go structs and
  the database schema never disagree.
