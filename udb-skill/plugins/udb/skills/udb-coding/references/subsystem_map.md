# UDB subsystem map — understand the codebase, navigate fast

A curated, agent-friendly map of every UDB subsystem. It is the "understand the
codebase and navigate" companion to the generated `codebase-map.md` (which is
the exhaustive symbol/dependency index — read THIS first for orientation, then
`codebase-map.md` to locate a specific symbol). All paths are under `src/`
unless noted. Entry points are real `pub` names you can grep.

> **Line-of-code rule:** never read a whole file. Read its module-doc (`///` at
> the top of `mod.rs` / the main file), then grep the entry point you need.
> When this file and the code disagree, trust the code and report the drift.

---

## 1. Orientation — the three planes

| Plane | Listener | Port (default) | Enforced by | `src/` home |
|---|---|---|---|---|
| **Data plane** (`DataBroker` CRUD + typed stores) | public | `:50051` | Casbin (`udb_authz.policy_rules`) | `runtime/core`, `planning`, `ir`, `runtime/executors` |
| **Native / control plane** (descriptor-declared services) | loopback | `:50061` (`+10`) | token scopes (`endpoint_security`) | `runtime/service`, `runtime/authn`, `runtime/authz` |
| **WebRTC peer plane** (media) | loopback | `:50071` | scopes on peer/signaling | `runtime/service/webrtc_service`, `runtime/signalling` |

**The one-line data flow:** gRPC request → **neutral IR** (`LogicalRead/Write/…`)
→ `compile_for_backend` → `CompiledRendering` (`Sql`/`Json`/`KeyValue`/`Object`)
→ `ExecutionPlan` legs → `DispatchExecutor` (backend driver) → **canonical store**
(PG/MySQL/SQLite/MSSQL) → outbox/projection/CDC side effects.

---

## 2. Subsystem index (quick table)

| # | Subsystem | One-liner | Key paths |
|---|---|---|---|
| 1 | **DataBrokerRuntime** | Centra runtime: pools, dispatch, routing, CRUD/tx entry | `runtime/core/*` |
| 2 | **IR compiler** | Neutral op → backend wire form | `ir/*`, `ir/compile/*` |
| 3 | **SQL dialect** | ~90%-shared SQL lowerer for PG/MySQL/SQLite/MSSQL | `ir/compile/sql_dialect.rs` |
| 4 | **Executors** | Stateless per-backend leaf I/O adapters | `runtime/executors/*` |
| 5 | **Backend identity/plugins** | BackendKind tiers/roles/capabilities + plugin registry | `backend/{kind,mod,plugin}.rs`, `backend/plugins/*` |
| 6 | **Backend context enforcement** | RLS/tenant posture per backend (SET LOCAL, prefixes, params) | `runtime/backend_context.rs` |
| 7 | **Canonical store** | Durability token + outbox + advisory lease trait | `runtime/canonical_store/*` |
| 8 | **System store** | ProjectionTask/Saga/AdminAudit/MigrationAudit contracts | `runtime/canonical_store/system_store.rs` |
| 9 | **Vector plane** | Gated SystemStores for Qdrant/Pinecone/Weaviate/ES | `runtime/canonical_store/{vector_plane,vector_system}.rs` |
| 10 | **Connection manager** | Per-(project,backend,instance,role) pools + tenant leases | `runtime/connection_manager.rs` |
| 11 | **Consistency / fence** | ConsistencyMode + WriteReceipt + ReadFence (RYW) | `runtime/{consistency,consistency_fence}.rs`, `replica.rs` |
| 12 | **Catalog / schema registry** | Multi-project manifests + client catalog_version negotiation | `runtime/{catalog,schema_registry}.rs`, `system.rs` |
| 13 | **Request pipeline** | Named request phases (security→…→audit) for observability | `runtime/pipeline.rs` |
| 14 | **Projection engine** | Sync document/vector/graph from canonical PG writes | `runtime/projection/*`, `drift_reconciliation.rs` |
| 15 | **CDC / outbox** | Change-capture → transactional outbox → Kafka | `runtime/cdc/*` |
| 16 | **XA / saga** | Distributed 2PC + saga compensation | `runtime/{xa,xa_*,saga,saga_compensators}.rs` |
| 17 | **Native registry / method-security** | Which services are native + per-RPC scope gate | `runtime/service/{native_registry,method_security}.rs` |
| 18 | **Authn** | Password/session/API-key/JWT/TOTP/WebAuthn/OIDC primitives | `runtime/authn/*`, `runtime/service/auth_service/authn/*` |
| 19 | **Authz / Casbin** | Policy engine over `udb_authz.policy_rules` | `runtime/authz/*` |
| 20 | **Credential layer** | Bearer/API-key/mTLS resolution at transport | `runtime/credential_layer.rs` |
| 21 | **Native services** | Descriptor-declared gRPC services (Vault, Storage, WebRTC, …) | `runtime/service/<svc>/` |
| 22 | **Migration / DDL** | Proto → diff → plan → apply → ledger | `migration/*`, `control/lifecycle.rs` |
| 23 | **Descriptor manifest** | Decodes embedded FileDescriptorSet → runtime contract | `runtime/descriptor_manifest.rs` |
| 24 | **Control lifecycle** | Startup FSM + migration plan-approval gate | `control/*` |
| 25 | **Config / env** | `UdbConfig` from yaml + `.env*` + env vars | `runtime/config/*` |
| 26 | **CLI** | `udb` binary dispatch (doctor/auth/init/codegen/…) | `cli/*` |
| 27 | **SDK codegen** | Render SDKs/manifests from the descriptor contract | `cli/{sdk_gen,native_app}.rs`, `runtime/sdk_manifest.rs` |
| 28 | **Parser / AST** | Hand-written proto → `ProtoSchema`/`ProtoFileAst` | `parser/*`, `schema/*` |
| 29 | **Planning broker** | Build select/upsert/delete/tx/vector/object plans | `planning/*` |
| 30 | **Observability** | Metrics/OTEL/tracing/SLO/preflight | `runtime/{metrics,observability,otel,slo,preflight}.rs` |
| 31 | **Security / encryption** | TLS/mTLS, AES-GCM-SIV envelope encryption, redaction | `runtime/{security,encryption}.rs`, `proto_redaction.rs` |
| 32 | **WASM / portable** | Edge subset + browser bridge (hand-rolled memory ABI) | `crates/udb-portable`, `crates/udb-wasm` |

---

## 3. Data plane — the write→read→project→stream spine

### 3.1 DataBrokerRuntime (the god-object)
- **Purpose:** central runtime owning every backend pool, the canonical-store
  registry, circuit breakers, routing counters, CRUD + transaction dispatch.
- **Key files:** `runtime/core/mod.rs` (struct + `encrypt_secret_at_rest`),
  `core/accessors.rs` (most accessors: `backend_executor`,
  `resolve_backend_selector`, `pg_read_pool_routed`, `enforce_read_fence`),
  `core/probe_dispatch.rs` (`enqueue_outbox_event`), `core/tx_object.rs`
  (`begin_tx`), `core/{catalog_admin,tenant_purge,pagination,native_store}.rs`.
- **Entry points:** `DataBrokerRuntime`, `DataBrokerRuntime::begin_tx`,
  `backend_executor()`, `CircuitBreakerState`, `default_system_stores()`.
- **Gotcha:** encryption-at-rest is fail-**open** in dev (plaintext when no key)
  but fail-closed in `fail_closed_mode()`.

### 3.2 Neutral IR + compiler
- **Neutral IR** (`ir/{mod,value,filter,projection,operations,plan,cache_key,raw_dispatch}.rs`):
  pure backend-agnostic ops. `LogicalUpdate`/`LogicalDelete` **require a filter**
  (no unbounded path). `LogicalRecord` is a `BTreeMap` (deterministic cache key).
  `LogicalResourceOp::validate_identifiers` is the allowlist anti-injection guard.
- **Compiler** (`ir/compile/mod.rs`): `Compiler` trait + `compile_for_backend`.
  `CompiledRendering` is one enum (`Sql`/`Json`/`KeyValue`/`Object`). Postgres
  always compiled-in; others feature-gated. `compile_for_backend` returns `None`
  (≠ `OperationNotSupported`) when the backend isn't compiled in.
- **Gotcha:** `CompileContext.tenant_id` is NOT injected by the compiler — the
  runtime sets transaction-local settings before executing. `enforce_tenant_scope`
  + tenant-scoped table + no tenant → `CompileError::TenantScopeRequired`.

### 3.3 SQL dialect + executors
- **SQL dialect** (`ir/compile/sql_dialect.rs`): `SqlCompiler<D>` shared by
  PG/MySQL/SQLite/MSSQL. Rejects `col = NULL` (`Malformed` → use `IsNull`).
  `cast_compare_placeholder` for PG `uuid = $1::UUID`.
- **Executors** (`runtime/executors/`): trait surface (`BackendExecutor`,
  `MutationExecutor`, `SearchExecutor`, `ObjectExecutor`, …) + `DispatchExecutor`
  enum (`handle.rs`) → per-backend driver. Return `tonic::Status` so gRPC codes
  survive dispatch. `ObjectExecutor::delete_object` refuses by default (no silent
  no-op on a non-object-store).
- **Backend identity** (`backend/kind.rs`, `backend/mod.rs`): `BackendKind`,
  `tier()`, `role()`, `capabilities()`. Only `Canonical`-role backends host system
  tables / durability anchors. `mssql.as_str() == "sqlserver"` (locked by test).
- **Backend plugins** (`backend/plugin.rs`, `backend/plugins/*`): `Backend` trait
  + `all_plugins()`. **Adding a backend = one plugin module + one entry in
  `plugins::all()`** — no edits in dispatch/generation/CLI.
- **Context enforcement** (`runtime/backend_context.rs`): PG uses `SET LOCAL`
  (never `SET SESSION` — pooling-safe), Mongo/ES use filter/prefix params, KV
  backends prepend key prefixes. `ContextEffect::{Enforced,Advisory,Unsupported}`.

### 3.4 Consistency, catalog, connection
- **Consistency** (`runtime/consistency.rs`, `consistency_fence.rs`):
  `ConsistencyMode` (7 pinned variants), `WriteReceipt` (WAL LSN + outbox seq +
  projection task ids), `ReadFence`/`StaleReadWarning`. **Postgres-only** — the
  fence is anchored to PG WAL. `replica.rs`: `PgReplicaManager` +
  `REPLICA_BOUNDED` (fails over to primary, never silently stale).
- **Catalog** (`runtime/catalog.rs`, `schema_registry.rs`, `system.rs`):
  `CatalogManager` (per-project active manifests, ArcSwap), `SchemaRegistry`
  negotiates `catalog_version`, `SystemCatalogConfig` = single source for all
  system-table relation names (monad: outbox, saga, xa_ledger, projection_tasks,
  idempotency_keys, row_revisions…).
- **Connection manager** (`runtime/connection_manager.rs`): `ConnectionManager`,
  `ClientState`, `PoolingMode`, `acquire_tenant_connection`. Tenant-scoped
  leases = fail-closed tenant guard at the pool layer.

### 3.5 Projection + drift + CDC
- **Projection** (`runtime/projection/mod.rs`, `drift_reconciliation.rs`):
  durable `udb_projection_tasks` table (at-least-once, idempotent, retry, DLQ);
  `ProjectionEngine::{enqueue_write_tasks_tx, replay_*}`; `DriftScanner` +
  `repair_drift`. **Gotcha:** `ReconciliationWorker` is opt-in, off by default;
  the idempotency key MUST include `source_checksum` or an updated matching row
  is deduped and the projection goes stale.
- **CDC** (`runtime/cdc/*`): per-backend change-capture → transactional outbox
  (`insert_outbox_row` — the ONE canonical INSERT) → Kafka (exactly-once
  transactional mode) with DLQ/retry/epoch-fencing/redaction. `UDB_CDC_ENABLED=false`
  is a true full-stop (gates outbox WRITE and tailer).

### 3.6 XA / saga
- `runtime/{xa.rs,xa_postgres.rs,xa_recovery.rs}`: `XaCoordinator` PREPARE→vote→
  COMMIT/ROLLBACK; `xa_recovery` drives in-doubt xids from `udb_xa_ledger` /
  `pg_prepared_xacts` / `XA RECOVER`. PG requires `max_prepared_transactions>0`.
- `runtime/saga.rs`, `saga_compensators.rs`: durable saga ledger + autonomous
  recovery with plugin-aware `BackendCompensator`s. **Gotcha:** compensation is
  NOT retried forever — `QuarantinePolicy` moves a saga to `manual_review`.

---

## 4. Native / control plane

### 4.1 Registry, wiring, enforcement
- **Native registry** (`runtime/service/native_registry.rs`): derives native
  services from the descriptor's `native_service` annotations (no hardcoded
  list). `native_service_enabled` = mounted (enabled AND listener AND no missing
  deps) — stricter than `enabled`.
- **Method-security** (`runtime/service/method_security.rs`): the tower layer
  enforcing `endpoint_security` (scopes/roles/tenant-required/CSRF/internal-only/
  AAL/rate) per gRPC path. `ADMIN_SCOPES = ["*","udb:*","udb:admin","udb:auth:admin"]`.
  `AUTH_MODE_PUBLIC` bootstrap RPCs (Authenticate/Login/GetJwks/…)
  bypass the admin token but get a bootstrap rate limit.
- **Credential layer** (`runtime/credential_layer.rs`): one async pass resolving
  bearer JWT / scoped API keys / mTLS cert→grant. Fail-closed against stale/revoked
  grants; API-key scopes can never exceed the live grant.
- **Native worker host** (`runtime/service/native_runtime.rs`): leader-elected
  singleton-worker boilerplate (`NativeWorkerHost::spawn_while_leader`), composes
  `runtime/singleton.rs` leases.
- **Native entity store** (`runtime/service/native_entity_store.rs`): backend-neutral
  KV persistence for services; many services still raw-SQL through documented
  P4 "escape hatches" (the neutral IR can't express them).

### 4.2 Authn
- **Primitives** (`runtime/authn/mod.rs` + `totp.rs`, `mfa_challenge.rs`,
  `revocation.rs`, `signing_keys.rs`, `token_family.rs`): library-free sessions/
  API keys (stored as keyed HMAC digests), Argon2id passwords, encrypted TOTP.
  Postgres-backed, fail-closed without a pool.
- **AuthnService** (`runtime/service/auth_service/authn/*`): `Authenticate`,
  `Login`, session lifecycle, JWT + refresh, TOTP MFA, CSRF, user CRUD, WebAuthn,
  OIDC, API-key lifecycle, service grants + mTLS cert bindings. Offline
  `bootstrap_admin_user`; `served_bootstrap_admin` is single-use across restarts
  (durable marker + `UDB_ALLOW_SERVED_BOOTSTRAP`).
- **Grants** (`runtime/service/auth_service/grants.rs`): immutable service
  identity (`service_account_grants` + `certificate_bindings`); forbidden scopes
  (wildcard/admin/owner) can NEVER be granted to a service; a scope-less grant is
  rejected.

### 4.3 Authz
- **Casbin engine** (`runtime/authz/casbin_engine.rs`, `mod.rs`): `AuthzSnapshot`
  over `udb_authz.policy_rules`; RBAC+ABAC+ReBAC+explicit-deny+required-scopes.
  Deny decided in Rust before Casbin (Rust effector can't deny-override
  reliably). `required_scopes` refine an Allow ONLY. Empty-policy message names
  `udb_authz.policy_rules` and points at `udb authz seed`.
- **Policy engine seam** (`runtime/authz/policy_engine.rs`): pluggable
  `decide/explain/lint/bundle` so a future Cedar/OPA can slot in.
- **Signed bundles** (`runtime/authz/bundle.rs`): HMAC-SHA256-signed snapshot for
  SDK-local `can()`; scoped to tenant/project (no cross-tenant leak).
- **Native access** (`runtime/authz/native_access.rs`): projects an allowed
  Decision into a short-lived restricted PG role + DSN + `app.current_*` session
  vars for the native fast path. UDB does NOT create roles — operator provisions.

### Native-service inventory
Each is a proto-driven gRPC service, no in-memory store. Read the per-service
`mod.rs` doc before touching it.

| Service | Purpose | Entry points | Gotcha |
|---|---|---|---|
| TenantService | `udb_tenant.*` CRUD | `CreateTenant`/`ListTenants`/`GetTenant` | tenant from verified claim, never body |
| VaultService (flagship) | KV + Transit + Seal secrets | `VaultService` KV/Transit/Seal RPCs | seal fails closed if master key unavailable |
| StorageService | object metadata + presigned writes | `RegisterFile`/`GeneratePresignedUrl`/`DeleteFile` | v1 is metadata only; bytes via presigned URL |
| AssetService | asset CRUD + processing pipelines | 8 RPCs + `consumer`/`execution` submodules | auto-trigger orchestration |
| WebRTC | rooms/peers/tracks, TURN, signaling | `WebRTC`, `SignalingService` | only transient signaling is in-memory |
| LockService | distributed locks + fencing | `AcquireLock`/`ReleaseLock` | lock name from verified claim tenant |
| CacheService | self-invalidating cache | `cache_get/set/delete/scan` | Redis `SCAN` not `KEYS` |
| ConfigService | feature flags | `EvaluateFlag`/`eval::evaluate_flag` | resolve-once TTL, no worker |
| MeteringService | usage + quotas | `CheckQuota`/`record_usage` | metering NEVER fails the metered request |
| EmbeddingService | vectors (sidecar-only inference) | `RegisterSource`/`Backfill`/`Retrieve` | inference in sidecars only |
| LiveQueryService | snapshot + CDC delta streaming | `Subscribe` | tenant isolation enforced twice |
| SearchService | one search box (FT/vector/hybrid, RRF) | `Search`/`HybridSearch` | mediated dispatch only |
| SchedulerService | durable cron/one-shot | `SchedulerService` RPCs | uses `WORKER_SCHEDULER_TICK` lease |
| NotificationService | multi-channel notifications | `NotificationService` RPCs | templates/tenant hybrid |
| AnalyticsService | pipeline metrics/throughput | `GetThroughput` etc. | |
| BackupService | tenant backup/restore | `BackupService` RPCs | gated by tenant-movement scope |
| WebhookService | outbound webhooks | `WebhookService` RPCs | `WORKER_WEBHOOK_DELIVERY` lease |
| WorkflowService | long-running workflows/sagas | `WorkflowService` RPCs | `WORKER_*` leases |
| ControlPlaneService | xDS-style policy/config push | `StreamResources` | backends pushed before their policies |
| IdentityProviderService | SAML/SCIM/OIDC federation | `IdentityProviderService` RPCs | SCIM HTTP off by default |
| Analytics / Cache / … | (19 services total beyond Authn/Authz/ApiKey) | per `runtime/service/<svc>/mod.rs` | |

---

## 5. Cross-cutting

### 5.1 Descriptor manifest + protocol
- `runtime/descriptor_manifest.rs`: decodes the build-embedded FileDescriptorSet
  into the full contract (services/RPCs/messages/endpoint+column security).
  Custom-option extensions are hand-decoded (prost drops them). Fail-closed.
- `build.rs` → `protocol/*` (`include!` of `OUT_DIR/protocol.rs`) +
  `udb_descriptor.bin` → `descriptor_manifest` → codegen. **Never edit `OUT_DIR`**
  (`// @generated`).

### 5.2 Migration + control lifecycle
- `migration/*`: parse → checksum → `diff_manifests`/`diff_all_backends` → plan →
  `apply_artifacts[_audited|_phased]` → tracker ledger. `ApplyError` — missing
  targets fail closed. `phase_runner` won't auto-retry a failed phase.
- `control/lifecycle.rs`: `run_startup_lifecycle` FSM; **the migration plan-approval
  gate** blocks APPLYING unless the live diff exactly matches the approved
  exported plan. `canonical_migration_changes` diffs against the PRIOR manifest
  (clean run = no-op).

### 5.3 Config, init, CLI, codegen
- Config (`runtime/config/*`): `UdbConfig` from `configs/database.yaml` + `.env*`
  + env. Default DDL concurrency is **serial (1)** (parallel bootstrap races PG
  catalog rows). `UDB_ENV=production` forces TLS/mTLS regardless of `=false`.
- Init (`init/*`): presentation-free `build_init_plan`/`apply_init_plan` FSM;
  `revert_last_init` escape hatch.
- CLI (`cli/*`): `run()` dispatch. Broken-pipe → silent exit(0). rustls
  CryptoProvider must be installed before any TLS config.
- Codegen (`cli/sdk_gen.rs`, `cli/native_app.rs`, `runtime/sdk_manifest.rs`):
  render SDKs/manifests from the descriptor contract. Use
  `descriptor_manifest`, never re-decode `prost_types::*Options`.

### 5.4 Parser, planning, observability, security
- Parser (`parser/*`, `schema/*`): hand-written lexer + `ProtoFileAst`
  (codegen) + `ProtoSchema` (catalog). `parse_file`/`parse_directory` use `std::fs`
  — NOT wasm; the portable surface is `parse_proto_source`/`parse_ast_source`.
  `schema_checksum` is the catalog-negotiation source of truth.
- Planning (`planning/*`): `build_select_query_plan`, `build_upsert_plan`, … —
  pure planning before any executor runs. `backend` identity lives at
  `crate::backend` (don't reintroduce a parallel registry).
- Observability (`runtime/{metrics,observability,otel,slo,preflight}.rs`): metric
  label cardinality caps (`UDB_METRIC_LABEL_MAX_LEN=64`,
  `UDB_METRIC_MAX_DISTINCT_LABELS=512`); `init_otel` before `init_observability`.
- Security (`runtime/security.rs`, `encryption.rs`, `proto_redaction.rs`):
  `is_production()` = `UDB_ENV=production` OR (`tls_required && svc_id_required`);
  AES-256-GCM-SIV envelope encryption with rotation; generated `RedactStorageOnly`
  for `OUTPUT_VIEW_STORAGE_ONLY` fields.

### 5.5 WASM / portable
- `crates/udb-portable` = WASM/edge-safe subset (no tokio/sqlx/tonic/fs); pulls
  server source via `#[path]` (no duplication). `crates/udb-wasm` = browser
  bridge with a hand-rolled `(ptr<<32)|len` memory ABI (`udb_alloc`/`udb_free`;
  JS must `udb_free`). No `wasm-bindgen`.

---

## 6. How to navigate (the recipe)

1. **Identify the layer** from §2's index table (data-plane core / compiler /
   executor / canonical store / native service / cross-cutting).
2. **Read the subsystem's section here** for purpose + entry points + gotcha.
3. **Grep the entry point** in `codebase-map.md` to see the module-doc + every
   surrounding symbol, then grep it in `src/`.
4. **For a data-plane feature:** follow §3's spine: IR op → compiler → rendering →
   executor → canonical store → outbox/projection.
5. **For a native-service feature:** `runtime/service/<svc>/mod.rs` doc → handler →
   the shared helpers (`native_helpers.rs`) → registry/method-security wiring.
6. **For authz:** the three surfaces (data=Casbin policy row, native=scope,
   legacy ABAC=dead) — never mix them.

## 7. Key invariants an agent must hold

- **Fail closed** dominates: descriptor decode, tenant scoping, missing apply
  targets, scope-less grants, Vault unseal, mandatory-TLS production.
- **Tenant isolation** is enforced on reads AND writes; on no-RLS SQL backends it
  is injected at compile time (`scoped_where_clause`) and/or via `SET LOCAL`.
- **Never hand-edit generated files** (`OUT_DIR`, `udb_descriptor.bin`,
  `docs/generated/*`, `codebase-map.md`).
- **`data.*` vs real action tokens** — the data plane matches RPC method names
  (`Select`/`Upsert`/`Delete`/`Update`/`BulkCas`), never `data.select`.
- **Two authz surfaces, two cures** — a data deny needs a policy row
  (`udb authz seed`); a native deny needs a scope on the grant.
