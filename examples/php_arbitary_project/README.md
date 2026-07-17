# Run UDB against YOUR own proto — from PHP

This example teaches one thing: **UDB doesn't force you onto a built-in schema.
Point it at a proto you wrote (`acme.billing.v1` here), and it becomes a real
multi-store backend for it** — relational rows in Postgres, vectors in Qdrant,
objects in MinIO — all reachable from a single PHP client. And it's fully
standalone: no UDB source checkout required. The proto is yours, the CLI and SDK
come from GitHub, and everything runs on local Docker.

## The 30-second version

```powershell
# 1. Generate PHP models + DB migrations from proto/ (uses Docker for the CLI):
.\scripts\generate.ps1 -Runner docker

# 2. Start Postgres + Redis + Qdrant + MinIO + the broker, then apply the schema:
.\scripts\serve.ps1
.\scripts\bootstrap.ps1 -Runner docker

# 3. Run the client against the broker (needs the PHP grpc extension):
$env:UDB_TARGET = "127.0.0.1:50051"
php acme_billing.php
```

No local PHP with the gRPC extension? Skip step 3 and run the whole client
inside a container that already has it:

```powershell
docker compose --profile smoke run --rm --build smoke
```

That's the loop. `acme_billing.php` then exercises the full surface against your
proto: relational create/read/update/delete, a vector upsert + search, and an
object put/read/presign — each one verified, so a green run means the broker
really honored your schema end to end.

## How the pieces fit

You bring the proto; three tools turn it into a running system.

- **`proto/acme/billing/v1/acme_billing_v1.proto`** — your schema. `Product`,
  `Invoice`, and friends, annotated with UDB table/column/vector/object options.
  Everything downstream is generated from this one file.
- **`buf`** generates the PHP message classes into `gen/` (that's where the
  `Product` class the client imports comes from).
- **The `udb` CLI** reads the same proto and writes `db_ops/` — the per-backend
  migrations (Postgres SQL, the Qdrant collection, the MinIO bucket, Redis
  cache config). It's committed here so you can read the generated DDL without
  running anything.
- **The PHP SDK** (`fahara02/udb-laravel`, pulled from GitHub by Composer) is
  what `acme_billing.php` talks through: `UdbClient` + `UdbMetadata`.

## Step by step

### 1. Generate

`generate.ps1` does three things in order: `buf generate` (PHP models),
`composer install` (the SDK), then asks the CLI to write `db_ops/` from your
proto. Pick where the CLI comes from with `-Runner`:

```powershell
# Docker — downloads the latest release CLI and runs it inside Compose:
.\scripts\generate.ps1 -Runner docker

# GitHub Release binary directly (no Docker):
$env:UDB_RUNNER = "release"
.\scripts\generate.ps1
```

Testing a CLI you built from source? Point the scripts straight at it:

```powershell
cargo build --release --bin udb
$env:UDB_CLI = "E:/Projects/udb/target/release/udb.exe"
.\scripts\generate.ps1 -Runner auto
```

For Docker-based pre-release testing, set `UDB_CLI_URL` to a URL for the **Linux**
binary. A Windows path won't run inside the Linux Compose container — see
[Common mistakes](#common-mistakes-this-prevents).

### 2. Start the broker and apply the schema

```powershell
.\scripts\serve.ps1                    # brings up Postgres, Redis, Qdrant, MinIO, broker
.\scripts\bootstrap.ps1 -Runner docker # force-syncs the proto schema into the backends
```

`serve.ps1` waits for the broker container to reach `running` and dumps its logs
if it doesn't, so a failed start is visible instead of silent.

### 3. Run the client

```powershell
$env:UDB_TARGET = "127.0.0.1:50051"
php acme_billing.php
```

The client runs relational CRUD (with a select after each write to prove it),
then a 768-dimension vector upsert + search, then an object put/read/presign
round-trip against the MinIO bucket. Want per-operation timings?

```powershell
docker compose --profile smoke run --rm --no-deps -e UDB_SHOW_TIMINGS=true smoke php84 acme_billing.php
```

## Configuration

The client reads three env vars; everything else has a working default baked into
`docker-compose.yml`.

| Env var | What it is | Default |
|---|---|---|
| `UDB_TARGET` | broker address the client dials | `127.0.0.1:50051` |
| `UDB_SHOW_TIMINGS` | print per-operation timings | `false` |
| `UDB_WARMUP` | warm the client before the first call | `true` |

Tooling scripts (`generate.ps1`, `bootstrap.ps1`, `udb.ps1`) read a few more:

| Env var | What it is | Default |
|---|---|---|
| `UDB_RUNNER` | where the CLI comes from: `auto`, `docker`, or `release` | `auto` |
| `UDB_CLI` | path to a local `udb` binary (used by `auto`) | unset |
| `UDB_CLI_URL` | URL (or path) to a specific CLI binary | latest Linux release |
| `UDB_VERSION` | release tag to fetch | `latest` |
| `UDB_GITHUB_REPO` | repo to fetch the CLI from | `fahara02/udb` |

## Common mistakes this prevents

- **Live calls need the PHP `grpc` extension.** Without it `php acme_billing.php`
  can't open a channel. If you can't install it locally, use the Docker `smoke`
  profile — it ships PHP with prebuilt `grpc`/`protobuf` extensions.
- **A Windows CLI path can't run inside the Linux container.** For the Docker
  runners, `UDB_CLI_URL` must point at a Linux binary (a URL or a Linux-native
  file), not `udb.exe`. Use `-Runner auto` with `UDB_CLI` for a Windows binary
  instead.
- **Rate limiting is off here on purpose.** The Compose broker sets
  `UDB_RATE_LIMIT_ENABLED=false` so local benchmarks don't measure Redis
  rate-limit overhead. Leave it enabled in production unless another gateway
  enforces limits.

## Performance notes

- Keep one `UdbClient` per long-lived worker/container and call
  `$client->warmup($metadata)` before user traffic (the client does this by
  default; set `UDB_WARMUP=false` to skip it).
- This client is deliberately correctness-heavy — it does an extra select after
  every write and touches all three stores. Real app flows should drop
  verification reads they don't need, and use batch or transaction RPCs instead
  of one process/RPC per row.

## Checks

```powershell
buf lint
composer validate --no-check-publish
php -l acme_billing.php
```
