# Talk to UDB from Python — with your own proto

This shows the whole loop for a *real* app: you bring your own proto
(`acme.billing.v1`), UDB turns it into database migrations, and the Python SDK
does live relational + vector + object-storage work against a running broker —
all in a standalone `uv` project with **no UDB source checkout required**.

If you only take one thing away: UDB is proto-driven. You define
`Product` once in `proto/acme/billing/v1/acme_billing_v1.proto`, and the same
proto drives the Postgres migration UDB generates *and* the Python model your
client sends. Change the proto, regenerate, and both sides stay in sync.

## The 30-second version

Everything is pre-generated and committed, so you can go straight to a live run:

```powershell
.\scripts\serve.ps1                    # docker compose: Postgres, Redis, Qdrant, MinIO, broker
.\scripts\bootstrap.ps1 -Runner docker # apply the generated migrations to those stores
uv sync                                # install udb-client + deps into a local venv
$env:UDB_TARGET = "127.0.0.1:50051"
uv run python acme_billing.py
```

`acme_billing.py` then runs, against the local broker:

- relational **create / read / update / delete** of a `Product`,
- a **vector** upsert + similarity search (`acme_products` collection),
- an **object** put / get / presigned-URL round-trip (`acme-billing-documents` bucket),

and prints a line per check. Any failed assertion aborts with a clear message.

## What's in here

| Path | Purpose |
|------|---------|
| `proto/acme/billing/v1/acme_billing_v1.proto` | Your entity definitions — the single source of truth |
| `gen/` | Python protobuf models generated from that proto (committed) |
| `db_ops/` | Migrations UDB generated from the proto, per backend (committed, so you can read them) |
| `docker-compose.yml` | Local Postgres, Redis, Qdrant, MinIO, and the UDB broker |
| `scripts/` | Thin wrappers that call UDB as an external CLI or Docker image |
| `acme_billing.py` | The SDK client that exercises the full surface |
| `tests/` | Offline checks on the generated model + SDK metadata |

## Regenerating (only if you change the proto)

`gen/` and `db_ops/` ship committed, so a fresh clone runs without this step. If
you edit the proto, regenerate both:

```powershell
.\scripts\generate.ps1 -Runner docker
```

This runs `grpcio-tools` (via `uv run --no-project`) to rebuild the Python models,
then asks the UDB CLI to regenerate `db_ops/` from `proto/`.

Instead of Docker, you can point the CLI at a GitHub release binary:

```powershell
$env:UDB_RUNNER = "release"
.\scripts\generate.ps1
```

…or at a binary you built from a UDB source checkout:

```powershell
cargo build --bin udb
$env:UDB_CLI = "E:/Projects/udb/target/debug/udb.exe"
.\scripts\generate.ps1 -Runner auto
```

## Tests and type-check

Fully offline — no broker needed:

```powershell
uv sync --extra dev
uv run pytest
uv run pyrefly check
```

## Configuration

Client behavior (`acme_billing.py`):

| Env var | What it is | Default |
|---|---|---|
| `UDB_TARGET` | broker gRPC address | `127.0.0.1:50051` |
| `UDB_SHOW_TIMINGS` | print per-operation timings | `false` |
| `UDB_WARMUP` | warm the client connection first | `true` |

How the scripts find the `udb` CLI:

| Env var | What it is | Default |
|---|---|---|
| `UDB_RUNNER` | `auto`, `docker`, or `release` | `auto` |
| `UDB_CLI` | path to a local `udb` binary (used by `auto`) | — |
| `UDB_CLI_URL` | direct URL/path to a CLI binary to download | latest linux release |
| `UDB_VERSION` | release tag to fetch | `latest` |

The broker listens on `50051` (gRPC) and `50052` (metrics); compose also exposes
the backing stores on high ports (see `docker-compose.yml`) so they don't clash
with anything you already run locally.

## A note on tenants

This example runs the broker in a relaxed local mode (header-supplied scopes and
a permissive policy) and sends a plain tenant string, `acme-org-1`, in the
request metadata. That keeps the demo simple. On a real, secured broker the rule
changes: **row-level security compares canonical tenant *UUIDs*, not the human
tenant code.** If you carry a friendly code like `acme` into your filters there,
your reads quietly come back empty and your writes stamp a value the database
can't match. On a locked-down broker, always use the verified tenant UUID the
broker hands back after auth — never the human code.
