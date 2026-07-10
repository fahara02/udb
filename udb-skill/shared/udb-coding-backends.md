# UDB backends — per-engine quirks an agent MUST know (companion to udb-coding)

Not database tutorials. This is the UDB-specific delta for each of the 18
backends: how UDB uses it, the RLS/tenant posture, the canonical-store tier, and
the exact traps already hit (live-DB conformance + audit findings). When you
touch `src/runtime/executors/<b>.rs`, `src/ir/compile/<b>.rs`,
`src/runtime/canonical_store/<b>*.rs`, or `src/backend/mod.rs`, read the row
first. Canonical copy: `udb-skill/shared/udb-coding-backends.md`.

## Cross-backend rules (apply to every engine)

- **Tenant isolation** = the shared resolver `ir::compile::util::resolve_tenant_column`
  (honors `is_tenant_column`, `tenant_id`, `_tenant_id`, legacy `org_id`/
  `institution_id`) → predicate injected by `ir::compile::<backend>`. The
  generic-dispatch executor path must EITHER inject via the compiler OR report
  its `enforce()` posture as `Advisory` — never claim `Enforced` on a raw
  passthrough (that was a critical cross-tenant lie on ClickHouse + Mongo).
- **Capability matrix** (`src/backend/mod.rs::BackendKind`): `supports_*` flags
  and the canonical-store TIER (`dev/single-node` | `system-store-capable` |
  `HA-canonical`) must match reachable runtime behavior. Don't flip a flag true
  without a wired path + a test.
- **Canonical-store leases/outbox-seq** must be cluster-atomic to be
  `HA-canonical`. Process-local `tokio::Mutex` is NOT atomicity. Current tier
  truth (maintainer decision — FULL-canonical, never projection-only, do not
  re-litigate): Postgres is HA-canonical; **ClickHouse is full-canonical via
  Keeper-backed KeeperMap leases** (fails CLOSED when Keeper is absent);
  **Elasticsearch is full-canonical via native CAS**
  (`if_seq_no`/`if_primary_term`); **Qdrant/Weaviate/Pinecone have NO usable
  CAS primitive and fail closed** (`cas_unsupported`) for system-store roles —
  their data-plane vector role is unaffected. All nine canonical stores run
  live conformance (`conformance_live_tests.rs`) 9/9.
- **Generic SQL executors** must gate raw SQL through
  `helpers::validate_read_sql`/`validate_mutation_sql` (single statement + verb
  allowlist). Any executor skipping this weakens the read/write guarantee.
- **Session tenant context** lives in `src/runtime/backend_context.rs`; it is
  defense-in-depth on TOP of the injected predicate, not a replacement.

## SQL family

### Postgres (`executors/postgres.rs`, `canonical_store/*`, the reference backend)
- The ONLY proven **HA-canonical** store: real RLS (`set_request_local_settings`
  installs `app.current_tenant_id` etc.; tables with `enable_rls`), advisory
  leases via `pg_try_advisory_xact_lock`, durable token = LSN, monotonic outbox
  seq. The legacy read path (`planning/broker/build_select_query_plan`) and join
  fusion both live here.
- Traps already hit: join fusion ran `fetch_all(&pool)` with NO transaction ⇒
  RLS GUCs absent ⇒ leak (fixed: wrap in `pool.begin()` + settings). `$like`
  ESCAPE must render a ONE-char escape under `standard_conforming_strings=on`
  (`ESCAPE '\'`, not `'\\'`). Outbox uuid casts and 2PC `COMMIT/ROLLBACK
  PREPARED` semantics are PG-specific. **Promoted-primary fence trap (fixed):**
  `wait_for_token` read `pg_last_wal_replay_lsn()` via COALESCE — a PROMOTED
  primary retains a stale non-null replay LSN, so the fence gated on garbage;
  the durable-position read must branch on `pg_is_in_recovery()`. **Aggregate
  RLS-GUC trap (fixed twice):** ANY aggregate/SUM read on an RLS table must run
  in a tx that installs `app.current_tenant_id` first — a bare pool read
  matches 0 rows and silently under-reports (shipped in metering QueryUsage and
  the storage quota gate). **Typed-bind trap (fixed):** `bind_one`-style
  helpers must bind uuid/timestamptz as their real types — PG has no implicit
  text→uuid cast (42804), and a bind-type collision on a prepared statement
  poisons the pooled connection.
- 2PC: the live `XaCoordinator` participant; write-ahead ledger row precedes
  `COMMIT PREPARED`; presumed-abort sweep only aborts xids with NO ledger row.

### MySQL (`executors/mysql.rs`, `mysql_migration_audit.rs`)
- `system-store-capable`. Generic SQL gated like PG. Dynamic-credential issuance
  uses `CREATE USER … ; GRANT …` (vs PG `CREATE ROLE`).
- Traps: MySQL **8.4** renamed replication primitives — use `SHOW BINARY LOG
  STATUS` (not `SHOW MASTER STATUS`) and `SOURCE_POS_WAIT` (not
  `MASTER_POS_WAIT`); these bit live conformance. XA: `XaMysqlParticipant` +
  `MysqlInDoubtParticipant` must be CONSTRUCTED/REGISTERED, not just defined —
  a capability lie shipped when `supports_xa: true` had no constructor.
  **`XA RECOVER` protocol trap (fixed):** MySQL refuses `XA RECOVER` over the
  prepared-statement protocol (err 1295 ER_UNSUPPORTED_PS) — it must run as a
  text-protocol query (`pool.fetch_all("XA RECOVER")`, not `sqlx::query(...)`);
  crash recovery silently never worked until this was fixed.

### SQLite (`executors/sqlite.rs`)
- `dev/single-node` only. Generic SQL gated. Used heavily in tests — but test
  DB files go under `std::env::temp_dir()` with Drop cleanup, NEVER the repo
  root (stray `.udb-outbox-ha-*.db` files got committed once).

### MSSQL / SQL Server (`executors/mssql.rs`)
- `system-store-capable`. Session context via `sp_set_session_context`.
- **Trap (high-severity, fixed):** `@read_only = 1` makes a SESSION_CONTEXT key
  immutable for the CONNECTION lifetime — and tiberius connections are POOLED,
  so the 2nd request on a reused connection failed. Do NOT set `@read_only`;
  the broker is the sole per-request writer. No `sp_reset_connection` in the
  pool path.
- Durability token: must be `MIN_ACTIVE_ROWVERSION()`/`@@DBTS`, not wall-clock
  (a wall-clock token makes `wait_for_token` return instantly ⇒ vacuous fence).
- **Deferred-compilation trap (fixed):** SQL Server resolves column names at
  COMPILE time, so a guarded backfill (`IF COL_LENGTH(...) IS NOT NULL UPDATE …
  new_col …`) still fails code-207 when the column is absent — wrap the guarded
  statement in `EXEC('…')` so compilation is deferred to execution.

### ClickHouse (`executors/clickhouse.rs`, `canonical_store/clickhouse.rs`,
`ir/compile/clickhouse.rs`)
- **Full-canonical via ClickHouse Keeper.** Advisory leases live in a
  **KeeperMap** table (`udb_keeper_advisory_leases`) — real distributed CAS;
  `ensure_advisory_lease_table` FAILS CLOSED when Keeper is not configured
  (the old ReplacingMergeTree lease CAS was non-atomic — "two acquirers can
  each believe they won" — and is exactly why the Keeper lock exists).
  Projection/saga/admin-audit/migration mutations are fenced under the same
  Keeper leases. Never downgrade this to projection-only (maintainer decision).
- **Trap (critical, fixed):** generic `query()` ran caller SQL verbatim with NO
  tenant predicate and NO SETTINGS while `enforce()` claimed Enforced. Either
  inject via `ir::compile::clickhouse` + per-query SETTINGS, or report Advisory.
  Also: generic ClickHouse `query`/`mutate` historically skipped the
  read/mutation SQL allowlist that PG/MySQL/SQLite enforce — apply it.

## Document / wide-column / graph

### MongoDB (`executors/mongodb.rs`, `ir/compile/mongodb.rs`)
- Tenant scoping = `_tenant_id`/`_project_id` ANDed into filters via
  `ir::compile::mongodb::and_with_context`. NoSQL → no SQL allowlist, but the
  filter-injection rule is identical.
- **Trap (critical, fixed):** generic `query`/`mutate` passed the caller's
  `filter` verbatim to `find/update/delete` with ZERO `_tenant_id` injection
  while `enforce()` claimed Enforced. Reuse `and_with_context`; stamp writes.

### Cassandra (`executors/cassandra.rs`)
- Wide-column; lease via LWT (`IF NOT EXISTS` compare-and-set loop). Mind CQL's
  lack of cross-partition transactions when reasoning about atomicity.

### Neo4j (`canonical_store/neo4j.rs`)
- **Trap (fixed):** Community Edition can't create uniqueness CONSTRAINTS (only
  RANGE indexes), so a bare lease `MERGE` racing two acquirers can both create a
  node and both win. Pre-seed a tombstone lease node and UPDATE it (mirror the
  counter-node pre-seed); expire via property update, don't `DELETE` (delete
  reopens the fresh-MERGE race each cycle).

## KV / cache / vector / object

### Redis / Memcached (`canonical_store/redis.rs`, `executors/memcached.rs`)
- Redis is `system-store-capable` ONLY with AOF on — the canonical store
  fail-closes on `aof_enabled:0` (CI compose runs `redis-server --appendonly
  yes`). **Lease trap (fixed):** same-owner refresh must be a Lua
  check-and-`PEXPIRE` (atomic), NOT GET-then-SETEX (a race lets another owner's
  fresh acquire get overwritten). Keep TTLs in milliseconds (PX), not truncated
  seconds. Use `SCAN`, never `KEYS`. The cluster jti denylist (token kill) also
  rides Redis with bounded TTL — fail-OPEN to the durable PG check on Redis
  outage (this is the one place availability beats freshness, documented).

### Vector stores: Qdrant / Pinecone / Weaviate / Elasticsearch
(`canonical_store/qdrant.rs`, `vector_system.rs`)
- **Full-canonical is the target posture (maintainer decision), with per-engine
  honesty:** **Elasticsearch** has real native CAS
  (`if_seq_no`/`if_primary_term` conditional writes; missing seq/term = hard
  error) and hosts system state. **Qdrant** (1.16+ may change this) and
  **Weaviate/Pinecone** (terminally) expose no usable CAS primitive — their
  system-store CAS paths return `cas_unsupported` and FAIL CLOSED
  (`try_acquire_advisory_lease` unconditionally errors; no last-write-wins
  fallback, ever). The data-plane vector role (search/upsert) is unaffected.
- Asset EMBED upserts and SearchService write here. Shared system-record logic
  lives in `system_store.rs` (`JsonSystemRecordAdapter`) — Qdrant/vector must
  DELEGATE, not re-copy (≈2,400 lines were triplicated; the audit flagged it).
- **Trap (fixed):** KV-style system stores rewrote a whole JSON-array membership
  index on every insert and never pruned terminal tasks (O(n) per enqueue,
  unbounded growth). Prune terminal ids; bound/TTL the sets.

### S3 / MinIO (object storage)
- Backs StorageService (presigned PUT/GET, GC) and asset byte IO. Defaults are
  named consts (`DEFAULT_OBJECT_BUCKET = "udb-storage"` in `native_helpers.rs`)
  — don't re-hardcode `"minio"`/bucket literals (asset byte steps did, drifting
  from storage's consts). CI must `mc mb --ignore-existing local/udb-storage`
  before live tests or they fail NoSuchBucket. Heavy media work (transcode) runs
  in SIDECARS via presigned URLs + work events — never link codecs into the
  broker.

## When adding a NEW backend

1. `BackendKind` variant + capability flags + canonical-store TIER (honest —
   default `dev/single-node` until proven).
2. `executors/<b>.rs` (`enforce()` posture truthful; generic SQL gated if SQL).
3. `ir/compile/<b>.rs` Compiler impl with tenant injection; add to the ONE
   feature matrix (`compile_for_backend`); add cross-backend test fixtures —
   semantic divergence is a compile error/`Unsupported`, never silent.
4. Canonical-store impl ONLY if it can host system state — delegate shared logic
   to `system_store.rs`; prove leases/seq atomic before claiming a higher tier.
5. Live conformance entry in `canonical_store/conformance_live_tests.rs`
   (env-gated DSN) + a compose service (with a REAL healthcheck, not `true`).
6. Plugin conformance (`backend/plugin.rs`) reflects ACTUAL wiring, not aspiration.
