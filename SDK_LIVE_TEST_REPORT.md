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
| **PHP** | single `it()` (520 assertions) — per-RPC dataset pending (Pest needs a cached-login data provider) | 1 test, 520 assertions |

| SDK | Result | Layer 1: surface reached | Wall time | Runtime |
|-----|:------:|:------------------------:|-----------|---------|
| **Go** | ✅ | **262 / 262** (262 sub-tests) | ~43 s | native → localhost |
| **Python** | ✅ | **262 / 262** (262 parametrized cases) | ~32 s | native → localhost |
| **TypeScript** | ✅ | **262 / 262** (251 subtests + 11 streaming) | ~6 s | native → localhost |
| **PHP** | ✅ | **262 / 262** (`expect($probed)->toBe(262)`, 520 assertions) | 30.3 s | Docker → `host.docker.internal` |

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
| **Go** | `live_perf_test.go` | `sdk/go/udbclient/perf_report_go.md` | 262 |
| **Python** | `test_live_conformance.py::test_live_perf` | `sdk/python/perf_report_python.md` | 262 |
| **TypeScript** | `live-auth.test.ts` ("live per-RPC perf") | `sdk/typescript/perf_report_ts.md` | 251 (11 streaming excluded) |
| **PHP** | `GeneratedRpcSurfaceTest.php` ("measures per-RPC latency") | `sdk/php/perf_report_php.md` | 262 |

The four agree on the shape: most RPCs **3–40 ms**; DataBroker's mean is skewed only because
Go/Python/PHP include the open-ended `PublishCDC` *subscription stream* (timed at its 20 s
deadline) — TS excludes streaming RPCs from its unary loop, so its DataBroker mean is ~13 ms.
The Go breakdown (representative) follows; the other three reports carry the same per-service
and slowest-20 tables.

**Per-service mean latency** (mean of per-RPC means):

| Service | RPCs | mean | | Service | RPCs | mean |
|---|--:|--:|---|---|--:|--:|
| DataBroker¹ | 76 | 272 ms | | ControlPlane | 5 | 11 ms |
| ApiKeyService² | 9 | 150 ms | | Notification | 11 | 9 ms |
| AnalyticsService | 7 | 142 ms | | TenantService | 6 | 9 ms |
| AuthnService² | 50 | 39 ms | | AssetService | 8 | 4 ms |
| AuthzService | 41 | 29 ms | | StorageService | 7 | 4 ms |
| IdentityProvider | 27 | 3.5 ms | | Room/Track/Peer/Turn/Signal | 15 | ~3 ms |

¹ **DataBroker's mean is skewed by `PublishCDC`** — an open-ended CDC *subscription stream* that
legitimately blocks (timed at the 20s deadline). Excluding it, DataBroker RPCs are single-digit
to low-100s ms; `GetCatalogManifest` (~172 ms) and the analytics aggregates (~130 ms) are the
real heavyweights.
² **`Login` (~810 ms), `CreateUser` (~715 ms), `CreateApiKey` (~1.25 s)** are slow **by design** —
Argon2id password/key hashing (deliberately expensive). Not a defect.

**Honest read:** most RPCs are **3–40 ms** over localhost. The outliers are (a) the CDC
subscription stream, (b) Argon2 credential hashing, (c) analytics aggregation queries, and
(d) `GetCatalogManifest` (whole-manifest payload). These match expectations.

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

## Note on timings — why PHP is slower

PHP (30 s) vs Python (16 s) / TS (2.5 s) is **not** a broker issue:

- **PHP is the only SDK run inside Docker** — it reaches the host broker via
  `host.docker.internal`, so every one of the 262 RPCs crosses the Docker NAT bridge. The
  others run natively and hit `localhost`.
- **PHP gRPC is synchronous** (the `grpc` pecl extension) with higher per-call overhead and no
  HTTP/2 multiplexing reuse, where Go pipelines over one channel.
- **The PHP probe populates every request field via reflection** (`set*`), heavier than the
  compiled Go/TS population.
- A few RPCs are genuinely heavy server-side — `GetHealthReport` probes all 14 backends and
  `GetCatalogManifest` returns the whole manifest; these dominate the tail (and are why the
  PHP client deadline was raised from 2 s → 15 s for the 14-backend broker).

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
