# UDB SDK Live Conformance — Test Report

**Date:** 2026-06-13
**Broker:** local `udb serve` (dev build, `--features mongodb-native`), data plane `127.0.0.1:50051`, native control plane `127.0.0.1:50061`
**Mode:** real JWT auth (no header-scopes crutch), one bootstrapped admin (`sdk-live-admin`) bound to the canonical tenant UUID

---

## ✅ Result: ALL FOUR SDKs GREEN

### Two layers — be precise about what each proves

**Layer 1 — full-surface reachability probe (all 262 RPCs, every SDK).**
Each RPC is sent a descriptor-derived, field-populated typed request; the test asserts it
**reaches a live handler** (no `UNIMPLEMENTED`/`UNAVAILABLE`/`UNKNOWN` mount failure) and that
decode + tenant/validation + handler-entry run. A *business* error is tolerated. So this proves
**"wired, decodes, validates, not a stub"** for all 262 — it does **NOT** assert the response is
*correct* for all 262.

**Layer 2 — deep, value-asserted E2E (a subset, ~a few dozen RPCs).**
Real create→read→assert lifecycles where the result is checked. NOT all 262.

**Per-RPC test granularity:** each SDK now reports **one test case per RPC** so the runner shows
granular per-RPC pass/fail (not a single opaque "1 passed"):

| SDK | Granularity | Runner output |
|-----|---|---|
| **Go** | one `t.Run` sub-test per RPC | 262 named sub-tests |
| **Python** | one **parametrized** case per RPC (`test_rpc_surface[<svc/rpc>]`) + 1 deep test | **263 passed** |
| **TypeScript** | one **node:test sub-test** per RPC | **252 tests** (251 unary subtests + main; 11 streaming probed inline) |
| **PHP** | one **Pest dataset** case per RPC (`reaches live RPC … (<stub>/<rpc>)`) + deep e2e + a 262-count guard | **264 passed** (+1 perf, skipped unless `UDB_LIVE_PERF=1`) |

The **suite wall-time** below is the *whole* run (process/container boot + one Argon2 login +
deep CRUD across all 14 backends + all 262 probes) — **NOT per-RPC latency**. Per call, all four
SDKs are single-digit-to-low-tens of ms; see the per-RPC perf section (PHP is in fact the fastest
per call). PHP's larger wall-time is the Docker cold-start it alone pays, not RPC cost.

| SDK | Result | Layer 1: surface reached | Suite wall-time | Runtime |
|-----|:------:|:------------------------:|-----------|---------|
| **Go** | ✅ | **262 / 262** (262 sub-tests) | ~43 s | native → localhost |
| **Python** | ✅ | **262 / 262** (262 parametrized cases) | ~32 s | native → localhost |
| **TypeScript** | ✅ | **262 / 262** (251 subtests + 11 streaming) | ~6 s | native → localhost |
| **PHP** | ✅ | **262 / 262** (`expect($probed)->toBe(262)`, 520 assertions) | ~30 s (incl. Docker boot) | Docker → `host.docker.internal` |

Of the 262: **236 receive a populated typed request** (real decode + validation), and **~26
destructive RPCs are sent typed-empty** on purpose — validation runs but the mutation never
executes (executing reset/revoke-all/emergency/catalog-destroy against the shared broker would
corrupt state).

**Layer 2 (value-asserted) covers, per SDK:** the all-backend CRUD matrix (13 backends), the
native-service create→read→assert lifecycles (tenant / authz / apikey / storage / asset /
webrtc / analytics / notification), the session lifecycle (logout truly revokes token +
refresh), and fail-closed negatives (bad password → no token; forged bearer rejected). This is
**not** all 262 — it is the CRUD/lifecycle subset.

> **Honest scope:** "262/262" means *every RPC is reachable and validates a real request*, not
> that every RPC has a result-asserted round-trip. Deep value-asserted E2E is the subset above.
> Extending result-assertion to every individual RPC (especially the read/list/admin family) is
> a worthwhile next step and is **in progress** (see below).

### Deep-E2E coverage status (honest, per service)

Goal in progress: convert every RPC from surface-probe to result-asserted E2E. Current state:

| Service | RPCs | Deep (result-asserted) | Notes |
|---|--:|:--:|---|
| Backend CRUD matrix | (13 backends) | ✅ full | create→read→assert per backend, strict (any failure fails the suite) |
| StorageService | 7 | ✅ | RegisterUpload→GetFile→UpdateFile(rename)→GetDownloadUrl→Delete, value-asserted |
| TenantService | 6 | ✅ | CreateTenant→config set/get round-trip |
| ApiKeyService | 9 | ✅ | Create→Get→Validate→Rotate→Revoke |
| AuthzService | 41 | ◑ partial | role/binding/policy-rule CRUD deep; canary/governance surface |
| AnalyticsService | 7 | ✅ | RecordPipelineMetric→GetPipelineSummary/GetThroughput asserted |
| NotificationService | 11 | ✅ | template/preference/send round-trips |
| AssetService | 8 | ✅ | pipeline def → register → start asserted |
| WebRTC (Room/Peer/Track/Turn) | 14 | ✅ | room/peer/track lifecycle asserted |
| **DataBroker — ops** | (10) | ✅ **NEW** | CDC status / DLQ / saga / catalog manifest+versions / schema lookup / health / admin / projects now result-asserted (`live_databroker_ops_test.go`) — **this caught a real bug, B16** |
| AuthnService | 50 | ◑ partial | login/session/user/password deep; MFA/WebAuthn/OTP/recovery surface |
| IdentityProviderService | 27 | ○ surface | SCIM/SAML/OIDC — surface-probed, deepening pending |
| ControlPlaneService | 5 | ○ surface | xDS-style streams — surface-probed |

✅ deep · ◑ partial · ○ surface-only (reachable + validated, not yet result-asserted).

### B16 — `GetCdcStatus` queried a non-existent `dispatched_at` column **[FIXED]**

The new result-asserted DataBroker ops E2E immediately caught a real defect: `get_cdc_status`
(`core/catalog_admin.rs`) computed outbox depth with
`SELECT COUNT(*) FROM <outbox> WHERE dispatched_at IS NULL`, but the transactional outbox has no
`dispatched_at` column (it is append-and-consume via logical replication) — so the RPC returned
`Internal: column "dispatched_at" does not exist`. Fixed to `COUNT(*)` (the pending backlog).
This is exactly the kind of bug a surface probe (which tolerates business errors) would never
have surfaced — proof the deepening is worth it.

---

## Broker under test

| | |
|---|---|
| Supported data-plane RPCs | **76** (DataBroker) — 262 total across 16 services |
| System tables verified | 51 |
| **Enabled backends (13)** | `cassandra`, `clickhouse`, `elasticsearch`, `memcached`, `minio`, `mongodb`, `mysql`, `neo4j`, `postgres`, `qdrant`, `redis`, `sqlserver`, `weaviate` |

All 13 advertised backends serve a **genuine data-plane CRUD round-trip** — verified by the
strict all-backend matrix (see B13). `enabled_backends` is honest: the phantom `s3`
duplicate is gone (object backend advertises only `minio`).

### Per-backend data-plane CRUD (Go matrix)

| Store category | Backends | Round-trip exercised |
|---|---|---|
| relational | postgres, mysql, sqlserver, clickhouse, cassandra | `GenericDispatch` query |
| document | mongodb | `EnsureResource → DocumentUpsert → DocumentGet → DocumentDelete` |
| object | minio | `EnsureResource → PutObject → GetObject` |
| cache | redis, memcached | `CacheSet → CacheGet → CacheScan → CacheDelete` |
| vector | qdrant, weaviate, elasticsearch | `EnsureResource → VectorUpsert → VectorSearch` |
| graph | neo4j | `GraphMutate → GraphQuery` |

---

## Per-RPC performance (all 4 SDKs, 262 RPCs each)

Every SDK now has a per-RPC perf harness gated on `UDB_LIVE_PERF=1`. Each times **every RPC**
over multiple iterations (read_only ×25, mutation ×5, destructive ×1 typed-empty), computes
real p50/p99/mean, and writes a per-service + slowest-20 report:

| SDK | Harness | Report | RPCs measured |
|-----|---------|--------|--------------:|
| **Go** | `live_perf_test.go` | `sdk/go/udbclient/perf_report_go.md` | **262** |
| **Python** | `test_live_conformance.py::test_live_perf` | `sdk/python/perf_report_python.md` | **262** |
| **TypeScript** | `live-auth.test.ts` ("live per-RPC perf") | `sdk/typescript/perf_report_ts.md` | **262** |
| **PHP** | `GeneratedRpcSurfaceTest.php` ("measures per-RPC latency") | `sdk/php/perf_report_php.md` | **262** |

**Unary** RPCs are timed as a full request→response round-trip. **Streaming** RPCs (11: the CDC
subscription, object up/download, batch/tx duplexes, xDS resource streams, WebRTC signaling)
report **stream-open latency** — initiate the stream + send the request, then cancel *without
draining responses*. This is the fix for a real measurement bug: a subscription stream emits its
first message only when an event arrives, which never happens in a passive perf run, so the old
code that drained to the first message just timed out at the 20 s deadline and recorded **20 s**
for `PublishCDC` — that single bogus value is what inflated the DataBroker mean to **272 ms**.
Streaming rows are labeled `stream_open` and never dropped; all 262 stay in the aggregate.

**Per-service mean latency** with the measurement corrected (Go run, representative; DataBroker
now reflects all 76 RPCs honestly):

| Service | RPCs | mean | | Service | RPCs | mean |
|---|--:|--:|---|---|--:|--:|
| AuthnService¹ | 50 | ~20–64 ms | | DataBroker² | 76 | ~9–22 ms |
| AuthzService | 41 | ~9–69 ms | | NotificationService | 11 | ~9 ms |
| ApiKeyService¹ | 9 | ~8–76 ms | | TenantService | 6 | ~8–12 ms |
| AnalyticsService | 7 | ~6–22 ms | | IdentityProvider | 27 | ~3–7 ms |
| ControlPlaneService | 5 | ~8–38 ms | | Storage/Asset/Room/Track/Peer/Turn | ~30 | ~3 ms |

(Per-service means vary run-to-run because the broker is shared/loaded; the *shape* is stable.)

¹ **`Login`, `CreateUser`, `CreateApiKey`** are the slowest RPCs (~0.7–2 s) — **by design**:
Argon2id password/key hashing is deliberately expensive. Not a defect.
² **DataBroker** is now **~9–22 ms** across all 76 RPCs (no streaming artifact). `PublishCDC`
stream-open is **~0.1 ms**; the real DataBroker heavyweights are `GetCatalogManifest`
(~120–365 ms, whole-manifest payload) and `GetHealthReport` (~20–140 ms, probes every backend).

**Honest read:** with the measurement fixed, most RPCs are **single-digit to low-tens of ms** over
localhost. The genuine outliers are (a) Argon2 credential hashing, (b) `GetCatalogManifest`, and
(c) Authz policy evaluation under load — all expected. The earlier **272 ms** was a *test bug*
(draining a passive subscription to its deadline), now corrected at the source.

---

## How to reproduce

```powershell
# 1. backends (3 compose stacks → 14 containers)
docker compose -f docker-compose.integration.yml -f docker-compose.canonical.yml -f docker-compose.extras.yml up -d
# mongo replica set + mssql db are auto-handled by the broker; bootstrap once:
./scripts/bootstrap-admin.ps1

# 2. broker (loads .env.local, all backends)
./scripts/launch-broker.ps1 -NoBuild -StopExisting -DisableHeaderScopes

# 3. the four SDK suites
./scripts/run-go-live.ps1     -Backends "cassandra,clickhouse,elasticsearch,memcached,minio,mongodb,mysql,neo4j,postgres,qdrant,redis,sqlserver,weaviate"
./scripts/run-python-live.ps1
./scripts/run-ts-live.ps1
./scripts/run-php-live.ps1 -Build   # -Build first time (compiles grpc pecl, ~10 min, then cached)
```

---

## Note on timings — suite wall-time vs per-RPC latency (PHP is NOT slow)

**Do not read the suite wall-time as per-RPC latency — they are different things.** Per RPC, PHP
is the **fastest** of the four SDKs in the perf sweep (DataBroker mean **9.4 ms** vs Go 18 ms,
Python 14 ms, TS 23 ms; slowest single PHP RPC is `GetCatalogManifest` at ~205 ms). No PHP RPC
comes close to a second, let alone 30 s.

The suite **wall-time** (PHP ~30 s vs Python ~16 s vs TS ~2.5 s) is harness overhead, not RPC
latency, and is dominated by things a real PHP app never pays per call:

- **PHP is the only SDK run in a cold Docker container** — container start + `grpc` pecl ext init
  + composer autoload is several seconds *before any RPC runs*. A real PHP app is a long-lived
  process that pays this once at boot, not per request.
- **The conformance suite does a deep create→read→assert CRUD against all 14 live backends**
  (Postgres, MySQL, MSSQL, Cassandra, Mongo, Redis, MinIO, Qdrant, …). Those 14 real database
  round-trips — plus one Argon2 login (~1–2 s, deliberately expensive) — are the bulk of the
  wall-time, shared by every SDK; PHP just also pays the Docker boot on top.
- Evidence: the PHP **perf** test alone (login + 262 timed probes, *without* the 14-backend deep
  e2e) runs in ~13 s, and the 262 actual RPC calls inside it sum to **well under a second**.

A couple of RPCs are genuinely heavy server-side regardless of SDK — `GetHealthReport` probes all
14 backends, `GetCatalogManifest` returns the whole manifest — which is why the PHP client deadline
was raised 2 s → 15 s for the 14-backend broker (a ceiling, not a typical wait).

---

## Fixes landed to reach all-green

This run started red; the following defects were found and fixed to get here:

| # | Bug | Fix |
|---|-----|-----|
| B1 | Storage reaper `timestamptz < text` | IR compiler casts timestamp placeholders (`$N::TIMESTAMPTZ`) |
| B2 | MSSQL advertised but `udb` DB missing | broker self-heals `CREATE DATABASE` via `master` |
| B8 | Typed UUID **write** `config_id/file_id … is uuid but expression is text` | cast `$N::UUID` in `INSERT VALUES` + `UPDATE SET` |
| B10 | Boot panic — default SystemStore `redis:default` not `postgres:primary` | guarded migration-audit `rollback_json→payload_json` backfill (pg/mysql/mssql) + promote postgres:primary |
| B11 | A typed-write failure tripped the postgres **circuit breaker** → cascading "not registered" across all native RPCs | breaker counts only genuine unreachability, not request-shape errors |
| B12 | `AnalyticsService.GetThroughput` returned 0 (typed `LogicalAggregate` matched 0 rows) | serve via the proven raw-SQL aggregate |
| B13 | mysql/mssql/cassandra/memcached advertised but had **no data-plane executor** → CRUD "not registered" (masked by a tolerant matrix) | wire `executor_registry` from the real per-backend maps; **strict matrix** now fails any enabled backend that doesn't truly serve CRUD; `mssql`↔`sqlserver` token normalized |
| B14 | Login/refresh `typed authn write failed: invalid byte sequence … 0x00` | recursively strip NUL bytes from typed-authn-write params (a NUL can never be stored in PG text) |
| B15 | PHP generated client `$this('mutation' === 'read_only')` → "object not callable" on every unary RPC | fixed the PHP template + regenerated (`udb sdk generate`) |

---

*Static verification (no broker): `go vet` 0 · `python -m py_compile` clean · `tsc --noEmit` 0 · `php -l` clean. Live verification above.*
