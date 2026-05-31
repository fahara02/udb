# Operations

This document replaces the old scattered topology, runbook, performance, load,
and disaster-recovery notes.

## Configuration

Start with:

```powershell
Copy-Item .env.example .env.local
```

Required values for a normal broker are `APP_ENV`, `UDB_ENV`,
`UDB_APP_NAME`, `UDB_PG_INSTANCES`, `UDB_PG_DSN_PRIMARY`, `UDB_PG_DSN`,
Postgres pool settings, and `UDB_2PC_ENABLED`. Optional backends are enabled by
feature plus env. See [../.env.example](../.env.example).

Dotenv precedence:

1. OS environment
2. `.env.<APP_ENV>`
3. `.env.local`
4. `.env.prod`
5. `.env`

## Startup Checklist

1. Fill `.env.local` or production secrets.
2. Run `cargo run --bin udb-proto-parser -- doctor --human`.
3. Generate or verify system DDL:

```powershell
cargo run --bin udb-proto-parser -- system-ddl
```

4. Run migrations or `db_ops` sync for the selected backend.
5. Start the broker:

```powershell
cargo run --bin udb-proto-parser -- serve --addr 0.0.0.0:50051
```

## Deployment Topology

Production deployments should separate:

- UDB broker pods/processes.
- Postgres canonical/system catalog.
- Optional backend services.
- Kafka/schema registry for CDC.
- Metrics scrape endpoint.
- Secret manager or environment injection.

Run more than one broker instance only when shared catalog, CDC, and saga
coordination tables are available and migrations are controlled by a single
rollout path.

## Reload And Rotation

The runtime has config reload machinery and connection manager support for
shadow replacement. Treat live reload as an operator drill:

1. Start traffic against the broker.
2. Change a non-secret config value or rotate a backend DSN in staging.
3. Trigger the reload path used by your deployment wrapper.
4. Confirm old clients drain, new clients serve, and metrics stay healthy.
5. Record latency and error deltas.

Do not rely on undocumented live reload semantics for secret rotation until
that drill passes in the target environment.

## Backup And Restore

Canonical state is the catalog/system store plus canonical backend rows.
Projection and cache backends are rebuildable.

Restore sequence:

1. Restore Postgres/system catalog.
2. Restore canonical backend data.
3. Start UDB with CDC paused if possible.
4. Run `doctor --probe`.
5. Rebuild projections and object/vector indexes from canonical state.
6. Resume CDC and run smoke/load checks.

## Incident Playbooks

| Incident | First action | Follow-up |
|---|---|---|
| Optional backend unavailable | Confirm feature/env and circuit-breaker state | Decide fail-closed vs degraded mode |
| CDC lag | Check Kafka, offsets, DLQ, WAL lag | Replay or quarantine DLQ events |
| Saga compensation stuck | List sagas, inspect retry state | Retry compensation or mark reviewed |
| Migration failure | Stop rollout, inspect migration run | Roll back catalog or re-apply phased plan |
| Tenant noisy neighbor | Inspect channel/fairness metrics | Tune tenant/project/channel limits |

## Load Profiles

Keep these profiles as live acceptance tests, not static claims:

- `read-heavy`
- `write-heavy`
- `mixed-projection`
- `tenant-noisy-neighbor`
- `backend-outage`
- `reload-during-traffic`
- `multi-project-smoke`

Example:

```powershell
$env:UDB_HOST = "localhost:50051"
$env:CONCURRENCY = "50"
$env:TOTAL_REQUESTS = "10000"
$env:PROFILE = "read-heavy"
.\scripts\load_test.ps1
```

Record p50, p95, p99, error rate, queue waits, and backend pool utilization for
each production-like run.
