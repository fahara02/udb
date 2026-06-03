# UDB Capability Matrix Master Plan

This is the implementation plan for making UDB's backend capability matrix full
featured without creating parallel systems. The rule for every item in this file
is simple: reuse the runtime, compiler, executor, canonical-store, service, and
proto surfaces already present in the repository before adding new code.

The goal is not to mark every backend as canonical. The goal is to make every
backend expose the maximum truthful capability set it can support:

- durable databases become canonical stores only after they satisfy the shared
  UDB system-state contract;
- search, cache, vector, analytics, and object systems get richer native and
  projection functionality without pretending to host strongly coordinated UDB
  system state;
- `GetCapabilities`, `udb doctor`, `compat-matrix`, SDK metadata, runtime
  admission, and documentation all read the same generated matrix;
- native services become the operational control plane for auth, tenants,
  backend access, secrets, policy simulation, notifications, analytics,
  quotas, and workflow automation.

## Non-Negotiables

- Do not copy the Postgres, MySQL, or SQLite canonical store into each new
  backend. Extract shared pieces first.
- Do not create per-service SQL islands. Native services must use shared store
  traits and the canonical `SystemStores` bundle where possible.
- Do not advertise a capability until a compiler path, executor path, runtime
  admission path, and test path prove it.
- Do not promote Memcached or object stores to canonical storage unless their
  durability and coordination contract changes materially. Redis and the
  vector/search family may be canonical only through the compiled
  `SystemStores` adapters and startup durability checks in `src/backend/mod.rs`.
- Do not use a hidden Postgres sidecar to claim that another backend is
  canonical. If the backend cannot hold UDB system state itself, mark it as
  projection or auxiliary.
- Do not add new hand-maintained docs tables for capabilities. Docs should be
  generated or checked from `src/backend/mod.rs` and the plugin inventory.

## Current Code To Reuse

| Existing code | Reuse for | Extension needed |
|---|---|---|
| `src/backend/mod.rs` | Backend identity, roles, current `BackendCapability`, generated matrix | Add V2 nested capability structs while keeping V1 fields for compatibility |
| `src/backend/plugin.rs` and `src/backend/plugins/*.rs` | Per-backend plugin inventory and runtime support state | Add capability evidence fields and optional canonical-driver evidence |
| `src/runtime/canonical_store/system_store.rs` | Shared traits for projection tasks, saga, admin audit, migration audit | Keep as contract boundary; add smaller helper traits only if tests require them |
| `src/runtime/canonical_store/conformance.rs` | Canonical-store contract tests | Expand into backend-parameterized conformance suites and live-test feature gates |
| `src/runtime/canonical_store/dialect.rs` | Shared SQL helper functions | Promote into a full SQL canonical dialect with DDL, JSON, locks, claim, pagination, and upsert rendering |
| `src/runtime/canonical_store/{postgres,mysql,sqlite,mssql}.rs` | SQL canonical implementations | Keep common behavior in shared SQL helpers; leave backend-specific dialect logic thin |
| `src/runtime/core/mod.rs` | Canonical store registry and `register_full_canonical_store` | Register new stores only after conformance is wired |
| `src/runtime/core/setup_data.rs` | Backend setup and canonical registration | Keep feature and topology gates centralized; object-store canonical candidates stay refused until their feasibility profile is proven |
| `src/runtime/executors/mod.rs` | Executor traits and dispatch model | Avoid adding direct backend branches outside the executor registry |
| `src/runtime/executors/handle.rs` | `DispatchExecutor` routing | Reuse plugin `DispatchFactory`; add operations through traits before match arms |
| `src/runtime/executors/mssql.rs` | SQL Server query, mutation, resource, session-context behavior | Keep Tiberius canonical behavior covered by live conformance |
| `src/runtime/executors/mongodb.rs` | MongoDB native driver, transactions, change streams, resource lifecycle | Keep canonical registration behind `mongodb-native` and majority semantics |
| `src/runtime/executors/cassandra.rs` | CQL executor using `scylla` | Keep LWT/quorum canonical behavior explicit in capability evidence |
| `src/runtime/executors/neo4j.rs` | Cypher executor | Keep graph-native system model and ReBAC backing store evidence together |
| `src/runtime/executors/{s3,minio,gcs,azureblob}.rs` | Object executor path | Add consistent streaming, multipart, presign, metadata, retention, and lifecycle capability reporting |
| `src/ir/compile/*.rs` | Backend typed operation compilers | Capability V2 evidence must point at compiler support, not just executor availability |
| `src/runtime/consistency.rs` and `src/runtime/consistency_fence.rs` | Read fence and consistency model | Add backend durability-token evidence and admission tests |
| `src/runtime/cdc/source.rs` | CDC source model | Attach CDC capability details per backend |
| `src/runtime/xa.rs` | 2PC/XA gating | Keep V2 capability checks fail-fast before side effects |
| `src/runtime/service/auth_service/*` | Authn, Authz, API key, event, mapping stores | Expand service features through existing modules and shared events |
| `src/runtime/service/tenant_service` | Tenant/project control plane | Extend to org/env/residency/quota policies |
| `src/runtime/service/notification_service` | Notification control plane | Extend to template/provider/subscription workflow |
| `src/runtime/service/analytics_service` | Admin and analytics summaries | Extend into capability health, usage, lag, quota, and anomaly rollups |
| `proto/udb/core/**` | Native service proto packages | Add new RPCs in existing packages before creating new packages |
| `proto/udb/services/v1/data_broker.proto` | Public broker API | Add versioned capabilities response fields without breaking existing clients |
| `crates/udb-portable` | WASM/edge-safe parser, schema cache, and client-side planning support | Keep SDK metadata and capability negotiation portable; do not pull runtime SDKs into this crate |

## Dependency And SDK Decisions

This section is intentionally concrete. A dependency can be added only when it
reduces custom protocol code, exposes a feature UDB cannot safely implement
itself, and fits the feature-gated build model in `Cargo.toml`.

| Area | Current repo state | Decision | Reason |
|---|---|---|---|
| Postgres | `sqlx`, `tokio-postgres` | Keep both | `sqlx` backs normal SQL and migrations; `tokio-postgres` already supports lower-level replication/CDC paths |
| MySQL / SQLite | `sqlx/mysql`, `sqlx/sqlite` behind features | Keep | Already integrated with feature gating and canonical stores |
| SQL Server | `tiberius = 0.12`, `tokio-util`, lazy round-robin client pool in `MssqlClient` | Keep; do not add ODBC | Tiberius is the existing pure-Rust TDS path and avoids native ODBC installation; add AAD token support through `AuthMethod::AADToken` before introducing another driver |
| MongoDB | HTTP Data API path plus optional `mongodb-driver = 3.7` through `mongodb-native` | Use native driver for canonical only | Full canonical needs sessions, transactions, topology checks, change streams, and retryable writes; the HTTP Data API path stays projection/administrative |
| Cassandra / Scylla | `scylla = 0.13` | Upgrade assessment required before canonical work | Scylla's Rust driver has moved past 1.0; run migration-guide review and live tests before building LWT-heavy canonical logic on the older API |
| Neo4j | HTTP transactional Cypher API through `reqwest`; no Bolt crate | Keep HTTP for projection; evaluate Bolt only for canonical | Canonical graph storage may need causal bookmarks and transaction behavior that are easier with Bolt. Add `neo4rs` behind `neo4j-bolt` only if HTTP cannot satisfy conformance |
| ClickHouse | HTTP/SQL executor through existing code | Keep | ClickHouse remains append/analytics profile; no new driver should be added until mutable canonical semantics are proven |
| Redis | `redis = 0.27` with Tokio | Keep | Good fit for cache, rate limit, streams, and auxiliary locks; not canonical authority |
| S3 / MinIO | `aws-config`, `aws-sdk-s3` | Keep official AWS SDK | Required for streaming bodies, multipart upload, presign, checksums, retries, and S3-compatible endpoint support |
| GCS | `google-cloud-storage = 0.22` | Keep | Official Google Rust storage client exists in repo and supports upload/download/integrity behavior |
| Azure Blob | `azure_core`, `azure_storage`, `azure_storage_blobs = 0.21` | Do not blindly upgrade | Newer Azure Rust storage crates may require Rust 1.88 while UDB declares Rust 1.85; upgrade only with an MSRV decision |
| Unified object abstraction | none | Optional: evaluate OpenDAL first, `object_store` second | UDB already needs cloud-specific features. OpenDAL can reduce duplicate common object code; keep official SDK adapters for presign, legal hold, retention, and provider-specific controls |
| Qdrant / Weaviate / Pinecone / Elasticsearch | REST executors over `reqwest` plus shared vector/search `SystemStores` adapter | Keep REST unless an official Rust SDK materially improves typed capability evidence | Do not add SDKs just for convenience; canonical claims must stay tied to adapter conformance and startup checks |
| Kafka / CDC | `rdkafka`, vendored Postgres replication helpers | Keep | Already feature-gated and tied to outbox/CDC paths |
| gRPC / SDK generation | `tonic`, `prost`, `tonic-build`, descriptor reflection | Keep; add SDK generation around existing protos | Public SDKs must come from `proto/udb/**`, not hand-written JSON wrappers |
| Rust embedded SDK | `src/embedded.rs`, generated tonic clients, `udb-portable` | Expand | Provide `udb::sdk` convenience clients that wrap generated clients and portable capability negotiation |
| Browser/edge SDK | `crates/udb-portable` | Expand without runtime deps | Keep it `serde`/`sha2` class only; publish WASM parser/capability-cache APIs separately |
| Authz policy | `casbin = 2`, existing Casbin engine/cache | Keep Casbin; optionally add Cedar after ADR | Cedar has a Rust implementation and schema validation value, but it should be a second policy backend, not a replacement for existing Casbin code |
| OPA/Rego | none | Do not embed OPA in phase 1 | OPA Wasm can work, but it adds bundle/runtime complexity. Treat as later optional policy backend after Casbin/Cedar interfaces are stable |
| OIDC | `jsonwebtoken`, `ExternalJwtProvider` | Add `openidconnect` behind `oidc` feature | OIDC discovery, JWKS refresh, issuer/audience validation, and provider metadata should not be hand-rolled |
| WebAuthn/passkeys | none | Add `webauthn-rs` behind `webauthn` feature | Server-side WebAuthn ceremonies are security-sensitive and should use a maintained WebAuthn library |
| SCIM | none | Implement the SCIM model in proto/service code first | Do not assume a mature Rust SCIM server crate. Build typed resources, validation, and conformance tests around UDB native services |
| Observability | `prometheus`, `opentelemetry`, `tracing`, `tower` | Keep | Capability health, driver probes, and latency histograms should reuse existing metrics/tracing stack |
| Protobuf bytes optimization | `prost`, `tonic-build`, `bytes` already present | Use `prost_build::Config::bytes` for selected protocol `bytes` fields | Prost can generate `bytes::Bytes` instead of `Vec<u8>` for protobuf bytes fields; use it for object chunks and binary cell payloads after compatibility tests |
| Record batch / columnar transport | none | Add UDB-native `RecordBatchV2` first; evaluate Arrow IPC/Flight later | Arrow has strong streaming/columnar standards, but UDB needs a backward-compatible broker protocol first |
| SIMD base64 | `base64` scalar crate | Evaluate `base64-simd` behind `simd-codecs` | It provides runtime CPU feature detection by default and targets a real current hot path: binary payload/base64 conversion |
| SIMD JSON | `serde_json` | Evaluate `simd-json` only for large generic-dispatch/document payloads | It uses unsafe SIMD and falls back when x86 AVX2/SSE4.2 are unavailable; do not put it on all JSON paths without profiling |
| CRC/checksum acceleration | `sha2` and ad hoc hashes | Evaluate `crc32fast` for non-cryptographic chunk checksums only | CRC is useful for transport integrity; it must not replace admin audit hash chains or security hashes |

### Dependency Admission Checklist

Before adding a crate:

1. Add a `Cargo.toml` feature that compiles it out of slim builds.
2. Confirm MSRV compatibility with UDB's current Rust version.
3. Add a small adapter module under the relevant existing surface:
   `executors`, `canonical_store`, `auth_service`, or `runtime/service`.
4. Add refusal-path tests for disabled features.
5. Add a capability evidence entry pointing at the adapter and tests.
6. Update `cargo deny` or equivalent supply-chain review once that tool exists.

Do not add a dependency that only moves code from one UDB module to another. The
dependency must bring protocol correctness, a maintained SDK surface, security
logic, or measurable performance/operability value.

## SDK Plan

SDKs must be generated or layered on existing generated code. Hand-written SDKs
are allowed only as thin ergonomic wrappers.

### Rust SDK

Reuse:

- `tonic` generated clients from `proto/udb/**`;
- `src/embedded.rs`;
- `crates/udb-portable`;
- `BackendCapabilityMatrixEntry` and future V2 capability structs.

Deliverables:

1. Add `src/sdk/mod.rs` with thin clients:
   - `DataClient`;
   - `ObjectClient`;
   - `VectorClient`;
   - `AuthnClient`;
   - `AuthzClient`;
   - `TenantClient`;
   - `AdminClient`.
2. Each wrapper accepts a generated tonic client and never bypasses gRPC
   authorization or capability admission.
3. Add `CapabilityCache` backed by `udb-portable::SchemaCache` so clients can
   fail fast before sending impossible backend operations.
4. Add retry helpers only for idempotent requests and only when request metadata
   has an idempotency key.
5. Keep streaming object APIs as streaming; do not buffer full objects in SDK
   helpers.

### TypeScript / Browser SDK

Reuse:

- `proto/udb/**` as source;
- `crates/udb-portable` compiled to WASM for schema parsing and capability
  negotiation;
- gRPC-Web or Connect-style transport generated from proto definitions.

Deliverables:

1. Generate types from proto files.
2. Publish a portable capability cache and request planner.
3. Add browser-safe object upload helpers using presigned URLs, not direct
   backend credentials.
4. Keep native-service admin operations out of browser defaults unless an
   explicit admin transport is configured.

### Go / Python SDKs

Reuse:

- `proto/udb/**`;
- service descriptors exposed through reflection;
- generated client stubs.

Deliverables:

1. Generate clients, examples, and smoke tests from proto.
2. Provide capability-matrix helpers, not custom transport code.
3. Keep auth metadata injection consistent across SDKs.

### Proto Tooling

Add a proto tooling phase only after the V2 capability schema stabilizes:

- evaluate `buf` for proto lint, breaking-change checks, and SDK generation;
- keep `tonic-build` as the Rust build path unless `buf` demonstrably improves
  reproducibility;
- add descriptor-set snapshot tests so native service additions cannot silently
  disappear from reflection.

## Protocol V2 Plan

The current protocol is flexible, but the hot path still spends work converting
between protobuf `Struct`, `serde_json::Value`, base64 strings, and buffered
executor strings. This section defines a practical V2 protocol that keeps V1
working while giving high-volume clients a cheaper path.

Current code facts:

- `proto/udb/services/v1/data_broker.proto` exposes unary and streaming RPCs,
  including `Select`, `BatchSelect`, `PutObject`, `GetObject`, typed data-plane
  RPCs, admin RPCs, and native-service entry points.
- `src/runtime/executor_utils.rs` converts `prost_types::Struct` to and from
  `serde_json::Value`.
- `src/runtime/executors/mod.rs` has `query_stream`, but the default path wraps
  a fully buffered `String`.
- `src/runtime/service/handlers_data.rs` still base64-encodes object bytes for
  generic `get_object` dispatch.
- `src/runtime/core/setup_data.rs` has real object streaming for typed S3
  `GetObject`, but typed `PutObject` still collects chunks before upload.

### V2 Compatibility Rules

1. Keep every existing V1 RPC and message field.
2. Add new messages and RPCs with `V2` suffixes or additive optional fields.
3. Never change protobuf field numbers or wire types.
4. Reserve removed field numbers and names.
5. Add client negotiation before using any V2-only encoding.
6. SDKs must default to V1 until `GetProtocolSupport` or `GetCapabilities`
   confirms V2 support.
7. V2 must be useful without backend changes; backend-specific acceleration is
   a later optimization.

### Protocol Negotiation

Add a compact protocol support response under `proto/udb/entity/v1/admin.proto`
or a new `proto/udb/entity/v1/protocol.proto`:

```proto
message ProtocolSupport {
  string min_protocol_version = 1;
  string max_protocol_version = 2;
  repeated string encodings = 3; // struct_json, record_batch_v2, arrow_ipc
  repeated string compression = 4; // identity, gzip, zstd when supported
  uint32 max_unary_request_bytes = 5;
  uint32 max_unary_response_bytes = 6;
  uint32 preferred_stream_chunk_bytes = 7;
  bool supports_query_stream = 8;
  bool supports_bytes_zero_copy = 9;
  map<string, BackendProtocolSupport> backend = 10;
}
```

Implementation path:

1. Extend `GetCapabilities` first so no new endpoint is required.
2. Add `GetProtocolSupport` only if the capabilities response becomes too large
   or clients need cheap startup negotiation.
3. Add SDK `CapabilityCache`/`ProtocolSupportCache` in `crates/udb-portable`
   so browser/edge clients can make the same decisions as server SDKs.

### RecordBatchV2

Add a typed row-batch response beside `RecordSet`. Do not remove `RecordSet`.

Recommended shape:

```proto
message RecordBatchV2 {
  string message_type = 1;
  string schema_version = 2;
  repeated ColumnBatch columns = 3;
  uint32 row_count = 4;
  string next_page_token = 5;
  udb.entity.v1.RequestContext request_context = 6;
}

message ColumnBatch {
  string field_name = 1;
  uint32 field_id = 2;
  ColumnType type = 3;
  bytes null_bitmap = 4;
  oneof values {
    Int64Column int64_values = 10;
    DoubleColumn double_values = 11;
    BoolColumn bool_values = 12;
    StringColumn string_values = 13;
    BytesColumn bytes_values = 14;
    JsonColumn json_values = 15;
  }
}
```

Why this is grounded:

- UDB already knows message schema and field metadata from the proto manifest.
- SQL row conversion already probes typed values in `executor_utils.rs`.
- Columnar batches reduce per-cell `Struct` allocation for large scans.
- V1 `RecordSet` remains the fallback for small requests and dynamic clients.

Acceptance:

- Add `SelectV2` returning `stream RecordBatchV2` or add an encoding flag to
  `Select` only if SDK compatibility stays clean.
- Add `BatchSelectV2` only after `SelectV2` is stable.
- Unit-test conversion from SQL rows to both `RecordSet` and `RecordBatchV2`.
- Benchmark 1k, 10k, and 100k row reads against current `RecordSet`.
- Expose `record_batch_v2` in protocol negotiation only when tests pass.

### Bytes And Object Streaming

Use protobuf `bytes` fields and `bytes::Bytes` where the Rust server can avoid
copying. Prost supports configuring `bytes` fields to generate `Bytes` instead
of `Vec<u8>` via `prost_build::Config::bytes`.

Implementation path:

1. Identify high-volume `bytes` fields:
   - `Chunk.data`;
   - binary SQL cells;
   - object metadata checksums;
   - optional encoded `RecordBatchV2` payloads.
2. Add a build.rs/prost configuration plan that generates `Bytes` for selected
   fields without breaking SDK generation.
3. Update typed `PutObject` to stream provider uploads instead of collecting all
   chunks first.
4. Keep inline object caps for generic dispatch; generic JSON dispatch is not a
   bulk object transport.
5. Standardize object streaming across S3/MinIO/GCS/Azure Blob through
   `ObjectExecutor::get_object_stream` and a future `put_object_stream`.

Acceptance:

- Large object upload/download never allocates the full object in the broker.
- Generic dispatch refuses large inline object bodies and points users to typed
  streaming object RPCs.
- SDKs expose streaming APIs without buffering by default.

### Compression

gRPC supports compression negotiation, but compression should be explicit in
UDB capability output because it changes CPU and latency behavior.

Plan:

1. Expose accepted compression algorithms in `ProtocolSupport`.
2. Add server config for compression on large responses only.
3. Do not compress small messages by default.
4. Add per-RPC metrics:
   - uncompressed bytes;
   - compressed bytes;
   - compression CPU time;
   - compression algorithm.

Acceptance:

- Compression can be enabled by SDK or server policy.
- Capability output shows whether a deployment accepts compressed requests.
- Benchmarks show compression helps for large result sets before defaulting it
  on.

### Error Details And Retry Semantics

Add typed error details so SDKs stop parsing strings:

- `error_code`;
- `backend`;
- `operation`;
- `capability_required`;
- `retryable`;
- `retry_after_ms`;
- `policy_decision_id`;
- `schema_version`;
- `request_id`;
- `correlation_id`.

Use these for capability refusal, stale schema, quota, backpressure, policy
denial, backend unavailable, saga retry, and DLQ replay errors.

### Protocol V2 Milestones

1. Add protocol support fields to capabilities.
2. Add typed error details.
3. Add `Bytes` generation for selected fields and prove SDK compatibility.
4. Add `SelectV2`/`RecordBatchV2`.
5. Add streaming query executor path that does not buffer a full JSON string.
6. Add typed object upload streaming for S3/MinIO first, then GCS/Azure.
7. Add SDK negotiation and fallback.
8. Add performance dashboards and documentation generated from capabilities.

## Native Acceleration And Assembly Plan

This plan treats handwritten assembly as the last step, not the first. The
grounded order is:

1. Remove protocol-level allocations and buffering.
2. Profile hot paths on Windows and Unix.
3. Use maintained SIMD crates or `core::arch` intrinsics where they fit.
4. Add handwritten `asm!` only for a small proven kernel that cannot be expressed
   cleanly with intrinsics and has tests on every supported target.

### Platform Support Goal

UDB acceleration must support both Windows and Unix. That means every optimized
kernel needs a scalar fallback plus OS/architecture gates.

| Target family | UDB support policy | Implementation path |
|---|---|---|
| Windows x86_64 MSVC | Required | scalar fallback plus optional SSE4.2/AVX2/AVX-512 through `core::arch::x86_64` and `is_x86_feature_detected!` |
| Linux x86_64 GNU/MUSL | Required | same x86 dispatch as Windows; CI should cover GNU and one MUSL build if releases target MUSL |
| macOS x86_64 | Supported | same x86 dispatch; no Windows-specific assumptions |
| Windows ARM64 / Arm64EC | Supported where Rust target is supported | scalar fallback plus optional AArch64/NEON via `core::arch::aarch64` and `is_aarch64_feature_detected!` where available |
| Linux AArch64 | Required for ARM servers | scalar fallback plus NEON/AES/SHA feature-gated paths where available |
| macOS AArch64 | Required for Apple Silicon dev machines | scalar fallback plus AArch64 intrinsics; avoid assuming Linux feature detection behavior |
| Other architectures | Build must not break | scalar fallback only unless a maintainer adds a tested target |

Rust facts:

- Inline assembly is stable for x86/x86-64, ARM, AArch64/Arm64EC, RISC-V,
  LoongArch, and s390x according to the Rust Reference.
- `core::arch`/`std::arch` expose architecture intrinsics.
- x86 runtime detection is available through `is_x86_feature_detected!`.
- AArch64 runtime detection is available through `is_aarch64_feature_detected!`,
  but feature availability and OS reporting must be tested per target.
- Portable SIMD in the standard library is not a stable UDB baseline for the
  current Rust toolchain, so do not make `std::simd` a required dependency path.

### Candidate Hot Kernels

Only optimize kernels that appear in profiles. Current code and reviews point
to these candidates:

| Kernel | Current source | First fix | SIMD/assembly option |
|---|---|---|---|
| Base64 decode for generic object bodies | `executor_utils::object_bytes_from_json` | Avoid base64 for bulk object transport; use streaming bytes | Evaluate `base64-simd` behind `simd-codecs` |
| Base64 encode for generic object get / binary SQL cells | `handlers_data.rs`, `executor_utils::base64_cell` | Keep generic payloads small; use typed bytes fields | Evaluate `base64-simd`; do not hand-write first |
| JSON parse for generic dispatch | `executor_utils::parse_sql_dispatch`, REST/object parsers | Reduce generic JSON dispatch on typed hot paths | Evaluate `simd-json` only for large payloads |
| Audit page hash verification | canonical admin-audit stores | Batch verification and avoid allocation first | Intrinsics only if hashing library lacks CPU acceleration |
| Non-cryptographic chunk checksum | future object streaming metadata | Add CRC only for transport integrity | Evaluate `crc32fast` |
| Vector rerank / distance math | future local rerank path | Prefer backend-native vector search first | Add AVX2/NEON intrinsics only if UDB does local rerank |
| UTF-8 validation | dynamic JSON/document inputs | Let parser validate first | Evaluate `simdutf8` only if validation is isolated and hot |

Do not optimize:

- database network I/O;
- policy or capability paths that still rebuild snapshots;
- row conversion that can be eliminated by `RecordBatchV2`;
- object paths that should be streaming instead of inline.

### Module Layout

Add acceleration as a small isolated module:

```text
src/runtime/accel/
  mod.rs
  base64.rs
  checksum.rs
  json.rs
  vector.rs
  detect.rs
  scalar.rs
  x86_64.rs
  aarch64.rs
```

Rules:

1. Public APIs are safe Rust wrappers.
2. Unsafe code lives only in arch-specific modules.
3. Every optimized function has a scalar implementation with identical tests.
4. Runtime dispatch is cached once per process, not checked inside tight loops.
5. Features:
   - `simd-codecs`;
   - `simd-json`;
   - `simd-checksum`;
   - `simd-vector`;
   - `asm-kernels` for rare handwritten assembly.
6. `asm-kernels` is off by default.
7. CI must compile scalar-only on every platform.
8. CI must compile optimized features on Windows x86_64, Linux x86_64, and
   Linux AArch64 before release.

### Handwritten Assembly Admission Gate

Do not add assembly because a path is "hot" in theory. Assembly is allowed only
when all of these are true:

1. A benchmark shows the kernel is a top CPU consumer after protocol and
   allocation fixes.
2. A maintained crate or `core::arch` intrinsic path cannot meet the target.
3. The assembly kernel is smaller and clearer than equivalent intrinsics.
4. The scalar fallback remains available.
5. The function has property tests against random inputs.
6. The function has cross-platform tests:
   - Windows x86_64;
   - Linux x86_64;
   - at least one Unix AArch64 target;
   - scalar fallback.
7. The feature is opt-in and documented in `GetCapabilities`/doctor output if
   it affects runtime behavior.

### Benchmark Plan

Add criterion benches before adding optimized code:

- `benches/protocol_record_batch.rs`;
- `benches/protocol_struct_recordset.rs`;
- `benches/object_base64.rs`;
- `benches/object_streaming.rs`;
- `benches/generic_dispatch_json.rs`;
- `benches/audit_hash_chain.rs`;
- `benches/vector_rerank.rs` if local rerank is implemented.

Metrics:

- allocations per request;
- peak resident bytes for large object upload/download;
- rows/sec for `RecordSet` vs `RecordBatchV2`;
- bytes/sec for base64 encode/decode;
- CPU time by RPC and backend;
- p50/p95/p99 latency;
- compression ratio and CPU cost.

Acceptance:

- No SIMD/assembly feature may regress scalar correctness tests.
- No optimized path may be the only implementation.
- Protocol V2 must show a larger win than a low-level kernel before low-level
  code is prioritized.
- Windows and Unix compile jobs must pass before enabling any acceleration
  feature in a release build.

## Target Backend Roles

| Backend | Current role | Target role | Why |
|---|---|---|---|
| Postgres | Canonical | Canonical reference | Best reference for system tables, leases, outbox, saga, audit, read fences, RLS |
| MySQL / MariaDB | Canonical | Canonical | Keep parity with Postgres through shared SQL canonical core |
| SQLite | Canonical | Embedded/dev canonical | Keep constrained; no HA claims |
| SQL Server | Canonical | Canonical | Tiberius-backed system store, transactions, locks, session context, mature backup story |
| MongoDB | Projection by default; canonical with `mongodb-native` | Canonical only with native driver and majority topology | Native transactions, change streams, collection validators |
| Cassandra / Scylla | Canonical in `cassandra` builds | Tunable canonical | LWT and quorum provide the system-state coordination contract |
| Neo4j | Canonical in `neo4j` builds | Graph-native canonical | Strong for relationship auth and graph workloads; transactional Cypher backs system state |
| ClickHouse | Canonical append/analytics profile in `clickhouse` builds | Append-canonical analytics profile | Excellent append/audit/analytics; multi-writer CAS caveat remains documented |
| Elasticsearch | Canonical in `elasticsearch` builds | Search/vector-system canonical adapter | Shared search-system adapter stores UDB system state in a dedicated index |
| Redis | Canonical in `redis` builds after durable AOF startup checks | Durable cache/stream canonical profile | Useful for rate limits and mirrors; canonical role depends on AOF/fsync durability |
| Memcached | Projection/cache | Cache only | No durable coordination model |
| S3 / MinIO / Azure Blob / GCS | Projection/object; canonical candidates | Object plane plus feasibility profile | Streaming, metadata, lifecycle, retention, presign; canonical gating remains conditional writes, fencing, listing recovery, and multipart safety |
| Qdrant / Weaviate / Pinecone | Canonical in matching vector builds | Vector-system canonical adapter | Dedicated collection/class/namespace plus serialized adapter operations back system-state contracts |

## Capability Matrix V2

Keep the current public `BackendCapability` fields for compatibility, then add a
versioned nested matrix. The nested matrix should be generated from backend
plugins and exposed through `GetCapabilities`, `udb-proto-parser compat-matrix`,
`udb doctor`, and SDK metadata.

Proposed Rust shape:

```rust
pub struct BackendCapabilityV2 {
    pub backend: BackendKind,
    pub tier: BackendTier,
    pub role: BackendRole,
    pub support_state: BackendSupportState,
    pub canonical: Option<CanonicalCapability>,
    pub data_plane: DataPlaneCapability,
    pub typed_rpc: TypedRpcCapability,
    pub generic_dispatch: GenericDispatchCapability,
    pub resource_lifecycle: ResourceLifecycleCapability,
    pub security: BackendSecurityCapability,
    pub consistency: ConsistencyCapability,
    pub native_access: NativeAccessCapability,
    pub observability: ObservabilityCapability,
    pub operations: OperationalCapability,
    pub evidence: CapabilityEvidence,
}
```

### Canonical Capability

Required fields:

- `profile`: `full`, `embedded`, `tunable`, `append_only`, or `none`.
- `system_tables`: creates and upgrades all required system state.
- `leases`: acquire, renew, release, fence token, expiration cleanup.
- `outbox`: append, claim, ack, retry, DLQ, publish evidence.
- `saga_store`: create, claim step, complete, compensate, retry, inspect.
- `projection_tasks`: enqueue, claim batch, checkpoint, retry, reconcile.
- `migration_audit`: operation ledger, phase evidence, failure records.
- `admin_audit`: append-only events, hash-chain verification.
- `read_fences`: read-your-writes or stronger by durability token.
- `durability_token`: monotonic write receipt with comparison semantics.
- `cdc_source`: WAL/binlog/change stream/log tail/resume token details.
- `backup_restore`: PITR/snapshot/export story for UDB system state.
- `conformance`: unit and live-test evidence id.

Admission rule: `BackendRole::Canonical` or `BackendRole::Both` requires
`canonical.profile != none` and conformance evidence. `append_only` cannot be
used for leases or saga admission.

### Data Plane Capability

Fields:

- `read`: typed read, filters, projection, pagination, count strategy.
- `write`: insert, update, upsert, delete, batch write, idempotency key.
- `aggregate`: group, having, window, approximate count, backend restrictions.
- `search`: text, hybrid, vector, filters, facets, ranking.
- `object`: get stream, put stream, multipart, presign, metadata, lifecycle,
  retention, legal hold, checksums.
- `cache`: TTL, scan, compare-and-set, counters, distributed locks, streams.
- `document`: nested update, array update, schema validation, change stream.
- `graph`: nodes, edges, path search, relationship predicates, graph write.
- `analytics`: columnar scans, materialized views, rollups, partition pruning.

### Typed RPC Capability

Every public RPC should have one of these states per backend:

- `native`: implemented through a backend-specific executor or compiler path.
- `compiled`: implemented by IR compiler plus executor.
- `canonical_only`: implemented through the canonical store, not the data plane.
- `unsupported`: rejected before side effects with typed capability error.
- `degraded`: implemented with documented restrictions.

The matrix must include evidence for:

- `Create`, `Read`, `Update`, `Delete`, `Query`, `Search`, `Aggregate`.
- `EnsureResource`, `DropResource`, `ListResources`, `DescribeResource`.
- `BeginTransaction`, `CommitTransaction`, `RollbackTransaction`.
- Object streaming RPCs and metadata RPCs.
- Native-service RPCs when they can operate on backend-native access.

### Security Capability

Fields:

- `tenant_isolation`: native RLS/session context/compiler filter/manual filter.
- `project_isolation`: same detail as tenant isolation.
- `native_access`: scoped users, session variables, temporary credentials,
  role grants, revocation, audit.
- `encryption`: TLS, at-rest status, field encryption, key manager support.
- `secret_strategy`: DSN redaction, secret reference, rotation support.
- `policy_enforcement`: Casbin/OPA/native predicate/compiler predicate.
- `audit`: admin audit, data access audit, auth event audit.

### Consistency Capability

Fields:

- `transactions`: none, single statement, single resource, multi statement,
  distributed.
- `isolation_levels`: backend-supported levels and UDB default.
- `two_phase_commit`: true only if `src/runtime/xa.rs` can validate it.
- `read_fence`: none, token, snapshot, causal, strict.
- `claim_semantics`: skip locked, compare-and-set, LWT, findOneAndUpdate,
  advisory lock, graph lock, external.
- `clock_dependency`: server time, hybrid logical clock, client time, vector.
- `failure_recovery`: replay, idempotency, DLQ, manual repair.

### Operational Capability

Fields:

- `health_probe`: ping, version, permissions, schema/system-state drift.
- `doctor`: DSN validation, privilege check, capability evidence, warnings.
- `backup_restore`: snapshot, PITR, export/import, restore validation.
- `migration`: create, alter, drop, online migration, rollback limitations.
- `observability`: metrics, tracing labels, slow query, audit events.
- `quota`: storage, operations, throughput, tenants, projects.
- `cost`: approximate storage and operation accounting.

## Capability Matrix Maintenance Steps

The V2 capability structs, V1 derivation, `BackendKind::capability_matrix_entry`,
`GetCapabilities`, `udb doctor`, and `udb-proto-parser compat-matrix` are now
wired to the same source-of-truth matrix in `src/backend/mod.rs`.

1. Keep `BackendCapabilityEvidence` in `src/backend/plugin.rs` populated with
   compiler module, executor module, canonical-store module, tests, feature
   flag, and known limitations.
2. Update each plugin in `src/backend/plugins/*.rs` through shared helpers. Do
   not duplicate long structs in every plugin.
3. Preserve existing `BackendCapabilityDescriptor` fields when adding any new
   protocol capability detail.
4. Keep matrix consistency tests in `src/backend/mod.rs`:
   - every supported operation has compiler or executor evidence;
   - every canonical backend has `SystemStores` evidence;
   - every `supports_xa` backend is accepted by `src/runtime/xa.rs`;
   - every native access claim has a `BackendContextEnforcer` story.
5. Add docs generation or docs validation so README tables cannot drift.

Acceptance:

- `GetCapabilities` exposes V2 without removing V1 fields.
- `cargo test backend::` covers matrix invariants.
- `cargo run --bin udb-proto-parser -- compat-matrix` and `udb doctor` report
  the same backend role, support state, and limitations.

## Shared Canonical Store Core

Before adding new canonical drivers, split common logic out of the existing SQL
stores.

New modules:

- `src/runtime/canonical_store/sql_core.rs`
- `src/runtime/canonical_store/sql_schema.rs`
- `src/runtime/canonical_store/sql_claim.rs`
- `src/runtime/canonical_store/sql_audit.rs`
- `src/runtime/canonical_store/sql_projection.rs`
- `src/runtime/canonical_store/sql_saga.rs`
- `src/runtime/canonical_store/sql_migration.rs`

Extend `src/runtime/canonical_store/dialect.rs` into a trait with:

- identifier quoting;
- positional/named placeholders;
- current timestamp expression;
- JSON encode/decode and JSON path support;
- binary/hash column type;
- monotonic token strategy;
- create-table-if-not-exists syntax;
- upsert/merge syntax;
- pagination syntax;
- lock and claim syntax;
- retry/deadline timestamp math;
- advisory lock or equivalent;
- migration transaction behavior.

Refactor sequence:

1. Add the SQL dialect trait while leaving current stores unchanged.
2. Move read-only helper rendering first: pagination, filter predicates, summary
   buckets, identifiers.
3. Move DDL rendering for UDB system tables into `sql_schema.rs`.
4. Move outbox claim/ack/DLQ logic into a shared function parameterized by
   dialect and executor adapter.
5. Move projection task claim/retry/checkpoint logic.
6. Move saga step claim/compensation logic.
7. Move admin audit and migration audit insert/list logic.
8. Convert Postgres to the shared core.
9. Convert MySQL.
10. Convert SQLite.
11. Only after those pass, add SQL Server.

Acceptance:

- No behavior change for Postgres/MySQL/SQLite after each conversion.
- `src/runtime/canonical_store/conformance.rs` passes for SQLite locally.
- Existing Postgres/MySQL live tests remain opt-in by env var.
- SQL Server implementation adds less code than a copy of any existing store.

## Canonical Driver P0: SQL Server

Target files:

- `src/runtime/canonical_store/mssql.rs`
- `src/runtime/canonical_store/sql_core.rs`
- `src/runtime/canonical_store/dialect.rs`
- `src/runtime/core/setup_data.rs`
- `src/backend/mod.rs`
- `src/backend/plugins/mssql.rs`
- `src/runtime/executors/mssql.rs`

Reuse:

- `MssqlExecutor` and `MssqlClient`.
- Existing T-SQL compiler behavior.
- `BackendContextEnforcer` with `SESSION_CONTEXT`.
- `ResourceAdminExecutor` and compiled resource operations.
- Shared SQL canonical core after extraction.
- Existing lazy round-robin Tiberius client slots; do not add another pool
  crate unless measurements show this pool is the blocker.

Implementation plan:

1. Add `MssqlCanonicalDialect` with:
   - `IDENTITY` or sequence-backed durability tokens;
   - `rowversion` where useful for optimistic concurrency;
   - `MERGE` or safe `UPDATE then INSERT` upsert pattern;
   - `TOP`/`OFFSET FETCH` pagination;
   - `SYSUTCDATETIME()` timestamps;
   - `varbinary(32)` for hash-chain values.
2. Implement UDB system schema using the shared SQL schema renderer.
3. Implement lease acquisition with either:
   - `sp_getapplock` for named locks; or
   - a lock table using `UPDLOCK, HOLDLOCK` inside a transaction.
   Pick one path and test failure recovery.
4. Implement outbox claim with `READPAST, UPDLOCK, ROWLOCK` and an expiration
   condition.
5. Implement saga step claim and projection task claim using the same claim
   helper as outbox.
6. Implement migration/admin audit through shared SQL audit modules.
7. Implement durability token reads from a monotonic sequence or committed
   ledger row.
8. Add an explicit Tiberius transaction helper before using multi-statement
   canonical updates. Keep MARS disabled unless a measured workload requires it.
9. Add AAD/managed identity support through Tiberius token auth as a separate
   native-access improvement; do not block SQL auth canonical conformance on it.
10. Register with `register_full_canonical_store` in `setup_data.rs` only when
   `UDB_MSSQL_DSN` is configured and `ensure_system_tables` succeeds.
11. Update `BackendRole` and V2 canonical profile only after conformance passes.

Tests:

- Unit tests for generated T-SQL claim/upsert SQL.
- SQL Server live conformance behind `UDB_MSSQL_DSN`.
- Permission failure test through `udb doctor`.
- Read fence test using `src/runtime/consistency_fence.rs`.

## Canonical Driver P0: MongoDB

Target files:

- `src/runtime/canonical_store/mongodb.rs`
- `src/runtime/executors/mongodb.rs`
- `src/runtime/core/setup_data.rs`
- `src/backend/mod.rs`
- `src/backend/plugins/mongodb.rs`

Reuse:

- Native MongoDB driver path in `MongoDbExecutor::new_native`.
- Existing transaction support in `MongoDbExecutor`.
- `json_to_document` and `document_to_json` conversion helpers.
- Change-stream support already present in executor code.
- Resource lifecycle helpers for collection/index creation.
- HTTP Atlas Data API executor only for projection/admin paths. It must not be
  used for canonical state because the canonical plan needs native sessions,
  transactions, topology checks, and change-stream resume tokens.

Semantics gate:

- Full canonical profile requires replica set or sharded cluster with
  transactions enabled.
- Standalone MongoDB must be `projection` or `degraded`; it cannot host full
  canonical state.

Implementation plan:

1. Add `MongoCanonicalStore` implementing `CanonicalStore` and `SystemStores`.
2. Create collections:
   - `udb_system_meta`;
   - `udb_leases`;
   - `udb_outbox`;
   - `udb_saga`;
   - `udb_projection_tasks`;
   - `udb_migration_audit`;
   - `udb_admin_audit`.
3. Add JSON schema validation for required fields and index definitions.
4. Use sessions and transactions for multi-document system-state updates.
5. Ensure every operation inside a transaction is chained to the same session.
   Do not run parallel MongoDB operations inside one transaction.
6. Implement lease acquisition using `findOneAndUpdate` with an expiry predicate
   and monotonic fencing counter.
7. Implement outbox and projection claims with `findOneAndUpdate`, status,
   claim owner, claim deadline, retry count, and stable sort order.
8. Implement saga state with transaction-protected step updates.
9. Implement durability token from a monotonic counter document plus operation
   timestamp/resume token metadata.
10. Implement CDC capability from change streams and resume tokens.
11. Add tenant/project filters to every collection query.
12. Register full canonical store in `setup_data.rs` only after topology and
   transaction checks pass.

Tests:

- Pure unit tests for BSON shape conversion and status transitions.
- Live conformance behind `UDB_MONGODB_DSN`.
- Negative test: standalone MongoDB reports degraded/no full canonical.
- Change-stream resume-token test where server supports it.

## Canonical Driver P1: Cassandra / Scylla

Target files:

- `src/runtime/canonical_store/cassandra.rs`
- `src/runtime/executors/cassandra.rs`
- `src/ir/compile/cassandra.rs`
- `src/backend/plugins/cassandra.rs`
- `src/backend/mod.rs`

Reuse:

- `CassandraClient` and `CassandraExecutor`.
- CQL compiler and tenant/project predicate injection.
- LWT awareness already documented in the compiler.
- Existing `scylla` feature gate, but first run a driver upgrade assessment
  because UDB currently pins an older pre-1.x API.

Semantics:

- Mark as `canonical.profile = tunable`, not full, unless tests prove strict
  read fences under chosen consistency levels.
- Require explicit consistency settings for system tables.
- Prefer `QUORUM` for normal reads/writes and LWT for claim transitions.

Implementation plan:

1. Upgrade or consciously pin the `scylla` driver after reading the migration
   guide and running executor smoke tests.
2. Add a system-store consistency config for read/write/serial consistency.
3. Model UDB system state by tenant/project plus bucketed partition keys.
4. Use LWT (`IF` predicates) for lease, claim, ack, and saga transitions.
5. Use `timeuuid` or a dedicated sequence table for operation ordering.
6. Store outbox in buckets to avoid hot partitions.
7. Make retry and DLQ scans partition-aware.
8. Document which operations are eventually consistent and which are fenced.
9. Add capability warnings when consistency is lower than quorum.
10. Register as canonical only when the configured profile passes conformance.

Tests:

- CQL generation tests for LWT transitions.
- Live conformance behind `UDB_CASSANDRA_DSN`.
- Tunable consistency matrix test.
- Hot-partition guard tests for generated table keys.

## Canonical Driver P1: Neo4j

Target files:

- `src/runtime/canonical_store/neo4j.rs`
- `src/runtime/executors/neo4j.rs`
- `src/ir/compile/neo4j.rs`
- `src/backend/plugins/neo4j.rs`
- `src/runtime/service/auth_service/authz.rs`

Reuse:

- Existing Neo4j HTTP transactional API executor and Cypher compiler.
- Authz snapshot and relationship-oriented authorization surfaces.

Driver gate:

- Keep the current HTTP executor for projection and graph CRUD.
- Before canonical promotion, prove that the HTTP transactional API can provide
  the needed transaction, bookmark/causal-read, and retry semantics.
- If it cannot, add a Bolt driver behind a new `neo4j-bolt` feature and route
  only the canonical store through that adapter. `neo4rs` is the practical Rust
  candidate, but it is not an official Neo4j driver, so the plan must include a
  maintenance-risk review.

Implementation plan:

1. Represent system objects as labeled nodes with unique constraints:
   - `UdbLease`;
   - `UdbOutboxEvent`;
   - `UdbSaga`;
   - `UdbProjectionTask`;
   - `UdbMigrationAudit`;
   - `UdbAdminAudit`.
2. Use graph transactions for claim and state transitions.
3. Use bookmarks or transaction metadata as durability evidence. If neither is
   available through the chosen driver path, do not mark Neo4j canonical.
4. Build ReBAC native store for authz relationship tuples.
5. Enforce tenant/project properties on all system nodes.
6. Add graph-specific explain paths to policy simulation.
7. Mark canonical only after audit, saga, outbox, lease, and projection
   conformance pass.

Tests:

- Cypher constraint creation tests.
- Live conformance behind `UDB_NEO4J_DSN`.
- ReBAC authorization tuple tests.
- Bookmark/read-fence tests.

## Canonical Driver P2: ClickHouse Append Profile

Target files:

- `src/runtime/canonical_store/clickhouse.rs`
- `src/runtime/executors/clickhouse.rs`
- `src/ir/compile/clickhouse.rs`
- `src/backend/plugins/clickhouse.rs`

Reuse:

- Existing ClickHouse compiler and executor.
- Analytics service rollups.

Semantics:

- Do not claim full canonical state by default.
- Use `canonical.profile = append_only` only for append ledger, admin audit,
  migration evidence, analytics rollups, and event retention.
- Leases, saga claims, and mutable projection tasks need a separate safe claim
  mechanism before full canonical is considered.

Implementation plan:

1. Add append-only audit tables with partitioning and retention.
2. Add analytics rollup tables for capability health and runtime usage.
3. Add event ledger ingestion from outbox mirror.
4. Add materialized views for admin/reporting queries.
5. Keep mutable system-state methods unsupported unless Keeper-backed locking
   is implemented and tested.

Tests:

- Append/read/admin audit tests.
- Capability matrix test that ClickHouse does not advertise full canonical.
- Rollup correctness tests.

## Projection And Auxiliary Backend Feature Plan

These backends should become more useful without being promoted to canonical.

### Object Stores: S3, MinIO, Azure Blob, GCS

Reuse:

- `ObjectExecutor` and existing object executor modules.
- Current direct typed object APIs and streaming behavior.
- Official SDKs already in `Cargo.toml`: `aws-sdk-s3`, `google-cloud-storage`,
  and Azure storage crates.

Plan:

1. Standardize `get_object_stream` across all object backends.
2. Add `put_object_stream` to avoid full-body buffering on uploads.
3. Add multipart upload capability with backend-specific limits.
4. Add presigned URL generation.
5. Add metadata/tags/retention/legal-hold operations where supported.
6. Add server-side encryption configuration reporting.
7. Add object lifecycle policy management through `ResourceAdminExecutor`.
8. Add checksum verification and range-read support.
9. Evaluate OpenDAL only for common read/write/list/stat/delete plumbing after
   the official SDK paths are feature-complete. Keep provider-specific SDK
   adapters for presign, retention, legal hold, and cloud IAM features.
10. Do not adopt `object_store` unless analytics/DataFusion integration becomes
   a first-class requirement; it is strong for table/analytics ecosystems but
   does not remove UDB's need for provider-specific controls.

Capability fields:

- `object.streaming_download`;
- `object.streaming_upload`;
- `object.multipart`;
- `object.presign`;
- `object.retention`;
- `object.metadata`;
- `object.range_read`;
- `object.checksum`.

### Vector Stores: Qdrant, Weaviate, Pinecone, Elasticsearch

Reuse:

- Existing vector/search compiler paths.
- Search executor trait.

Plan:

1. Normalize vector index creation through resource lifecycle operations.
2. Add hybrid search capability details: text, vector, rerank, filters.
3. Add collection/schema introspection.
4. Add batch upsert and delete-by-filter with idempotency keys.
5. Add embedding dimension checks before write.
6. Add vector distance metric reporting.
7. Add index health and rebuild status to `udb doctor`.

### Redis

Reuse:

- Redis executor and cache surfaces.

Plan:

1. Expose TTL, counters, CAS, rate-limit, and stream capability fields.
2. Add Redis Streams as optional outbox mirror, not canonical outbox.
3. Add distributed lock as auxiliary only, never canonical lease authority.
4. Add keyspace scan safety limits and warnings.
5. Add per-tenant key prefix enforcement.

### Elasticsearch

Reuse:

- Search compiler/executor.

Plan:

1. Add index template management.
2. Add reindex job lifecycle.
3. Add alias swap for zero-downtime projection rebuilds.
4. Add ingest pipeline management.
5. Add search explain and highlighting capability flags.
6. Add shard/replica health to doctor.

## Native Services Master Plan

Native services should expand through `proto/udb/core/**`,
`src/runtime/service/**`, and shared event/audit stores. New service features
must reuse `NativeModel`, canonical stores, auth event sink, tenant/project
context, and backend capability checks.

### Existing Native Service Surface

Do not add a new native-service framework. The current structure is already the
framework:

| Service | Proto file | Runtime file | Existing RPC surface to extend |
|---|---|---|---|
| Authn | `proto/udb/core/authn/services/v1/authn_service.proto` | `src/runtime/service/auth_service/authn.rs` | user CRUD, OTP, login, refresh, logout, token validation, session CRUD, CSRF, MFA enroll/confirm |
| Authz | `proto/udb/core/authz/services/v1/authz_service.proto` | `src/runtime/service/auth_service/authz.rs` | authorize/check, batch checks, role CRUD, policy CRUD, role bindings, relationships, lint, native access, policy bundle, audit/list |
| API key | `proto/udb/core/apikey/services/v1/apikey_service.proto` | `src/runtime/service/auth_service/apikey.rs` | create/get/list/update/revoke/validate keys, usage stats |
| Tenant | `proto/udb/core/tenant/services/v1/tenant_service.proto` | `src/runtime/service/tenant_service/mod.rs` | create/get/list/update tenant, get/update tenant config |
| Notification | `proto/udb/core/notification/services/v1/notification_service.proto` | `src/runtime/service/notification_service/mod.rs` | send/get/list/retry notifications, template CRUD, delivery stats, preferences |
| Analytics | `proto/udb/core/analytics/services/v1/analytics_service.proto` | `src/runtime/service/analytics_service/mod.rs` | pipeline metrics, summaries, executor performance, reconciliation analytics, throughput, SLA, snapshots |

Runtime wiring to reuse:

- `src/runtime/service/mod.rs` mounts the public `DataBroker` and the isolated
  native control-plane listener.
- `UDB_FILE_DESCRIPTOR_SET` is already used for reflection. Any new proto must
  show up there.
- `build_auth_services`, `build_tenant_service`,
  `build_notification_service`, and `build_analytics_service` are the
  construction pattern for native services.
- `src/runtime/native_catalog.rs` provides `NativeModel`, `native_schemas`,
  `merge_native`, `native_service_catalog_ddl`, `native_relation`, and model
  projection helpers.
- `src/runtime/service/auth_service/events.rs` provides `AuthEventSink` and
  outbox/Kafka event integration.
- `src/runtime/service/auth_service/mappings.rs` contains conversion and
  pagination helpers that should not be duplicated.
- `src/runtime/service/auth_service/tests/support.rs` provides live-test
  migration, cleanup, service constructors, outbox setup, default tenant
  seeding, and `assert_native_table_columns`.

Native-service acceptance pattern:

1. Add entity fields in `proto/udb/core/<service>/entity/v1`.
2. Add request/response messages in `services/v1/core.proto`.
3. Add RPCs in the existing service proto where possible.
4. Regenerate prost/tonic output through the existing build path.
5. Extend `NativeModel` mapping instead of hand-writing unrelated table DDL.
6. Add one schema-from-proto test using `assert_native_table_columns`.
7. Add one service roundtrip test through the generated service trait.
8. Emit admin/auth events through existing sinks.

### Authn Service

Current reuse points:

- `src/runtime/service/auth_service/authn.rs`
- `src/runtime/service/auth_service/events.rs`
- `src/runtime/service/auth_service/mappings.rs`
- `SecurityConfig`
- `ExternalJwtProvider`
- `validate_bearer_token`

Dependency choices:

- Keep `jsonwebtoken` for existing JWT validation and simple external-provider
  tokens.
- Add `openidconnect` behind an `oidc` feature for OIDC discovery, provider
  metadata, JWKS rotation, issuer/audience validation, and token introspection
  plumbing.
- Add `webauthn-rs` behind a `webauthn` feature for passkey/WebAuthn
  ceremonies. Do not implement WebAuthn challenge verification manually.
- Do not add a SCIM crate until a maintained server-side Rust implementation is
  selected. Model SCIM users/groups in proto first and test against SCIM 2.0
  behavior.

Plan:

1. OIDC provider registry:
   - issuer, audience, JWKS URL, clock skew, claim mapping;
   - provider health and JWKS refresh status;
   - per-tenant provider allowlist.
2. SCIM user/group provisioning:
   - create/update/deactivate users;
   - group membership sync;
   - audit every external change.
3. WebAuthn/passkeys:
   - credential registration;
   - challenge issuance;
   - device inventory;
   - recovery flow.
4. Session/device risk:
   - IP/device fingerprint;
   - impossible travel events;
   - stale session invalidation;
   - forced logout by tenant/project.
5. Service accounts and workload identities:
   - scoped identities;
   - token exchange;
   - rotation policy;
   - last-used evidence.
6. Account recovery and lockout:
   - recovery codes;
   - temporary lockout;
   - breached-password marker;
   - admin unlock workflow.

Acceptance:

- Auth events go through existing `AuthEventSink`.
- Provider config is tenant-scoped and auditable.
- Authn RPCs refuse unregistered provider ids before side effects.

### Authz Service

Current reuse points:

- `src/runtime/service/auth_service/authz.rs`
- `AuthzSnapshot`
- Casbin engine/cache code
- `GetNativeAccess`
- policy bundle and audit surfaces

Dependency choices:

- Keep `casbin` as the first policy engine because it is already present and
  wired through UDB authz code.
- Add a policy-engine trait before adding another engine:
  `evaluate`, `batch_evaluate`, `explain`, `lint`, `bundle_hash`.
- Consider Cedar only after the trait exists, because Cedar's Rust
  implementation and schema validation are useful for analyzable tenant policy.
- Treat OPA/Rego Wasm as a later integration, not an embedded default. It needs
  bundle compilation, Wasm runtime management, and separate operational
  controls.

Plan:

1. Policy simulation:
   - explain allow/deny;
   - list matched policies;
   - show subject/resource/context inputs;
   - dry-run future policy bundle.
2. Policy lifecycle:
   - draft, approve, publish, rollback;
   - bundle diff;
   - signature/hash evidence;
   - tenant-scoped policy version pinning.
3. Relationship-based authorization:
   - tuple write/read APIs;
   - graph backend adapter for Neo4j;
   - SQL tuple adapter for canonical stores;
   - expansion limits to prevent expensive graph walks.
4. Separation-of-duty constraints:
   - static conflicts;
   - dynamic session conflicts;
   - approval quorum requirements.
5. Native access expansion:
   - Postgres RLS grants;
   - MySQL session variable enforcement;
   - SQL Server `SESSION_CONTEXT`;
   - MongoDB tenant/project filters and scoped roles;
   - Neo4j tenant/project node predicates.
6. Cache invalidation:
   - bundle revocation;
   - push invalidation;
   - audit proof that stale cache was rejected.

Acceptance:

- Every simulation result includes policy version.
- Native access claims require backend capability evidence.
- ReBAC adapters share the same service trait.

### API Key Service

Current reuse points:

- `src/runtime/service/auth_service/apikey.rs`
- `ApiKeyStore`
- usage statistics and status pagination
- `AuthEventSink`

Plan:

1. Fine-grained scopes:
   - allowed RPCs;
   - allowed backends;
   - allowed tenants/projects;
   - allowed resource names and prefixes.
2. Quotas and rate limits:
   - per-minute, per-day, burst, storage, bytes;
   - backend-specific quotas;
   - soft and hard limits.
3. Network and binding controls:
   - allowed IP/CIDR;
   - mTLS cert binding;
   - user-agent/app binding;
   - workload identity binding.
4. Rotation:
   - staged key rotation;
   - emergency revoke;
   - quarantine suspicious key;
   - last-used and unused-key reporting.
5. Audit and anomaly:
   - unusual backend usage;
   - denied operation events;
   - quota threshold events.

Acceptance:

- Quota decisions are emitted as auth events.
- Revoked/quarantined keys fail before reaching data-plane handlers.
- Usage stats share analytics rollup code.

### Tenant Service

Current reuse points:

- `src/runtime/service/tenant_service`
- tenant/project bindings in runtime catalog code
- `NativeModel`

Plan:

1. Organization/project/environment hierarchy.
2. Tenant lifecycle:
   - create;
   - activate;
   - suspend;
   - archive;
   - delete with retention policy.
3. Residency policy:
   - allowed regions;
   - allowed backends;
   - data movement approval.
4. Quotas and budgets:
   - operations;
   - storage;
   - object bytes;
   - vector counts;
   - monthly spend estimate.
5. Backend allowlist:
   - per-tenant backend set;
   - per-project overrides;
   - capability restrictions.
6. Config approval workflow:
   - draft;
   - diff;
   - approve;
   - rollback.

Acceptance:

- Tenant status is checked in runtime admission.
- Residency violations are typed errors.
- Tenant config changes emit admin audit records.

### Notification Service

Current reuse points:

- `src/runtime/service/notification_service`
- outbox/event infrastructure

Dependency choices:

- Do not add provider SDKs first. Define `NotificationProvider` adapters and
  use `reqwest` for webhook/email-provider HTTP APIs until a provider-specific
  SDK is proven necessary.
- Use SecretService references for provider credentials before adding providers.

Plan:

1. Provider registry:
   - email;
   - SMS;
   - webhook;
   - Slack/Teams-style channels;
   - tenant-scoped credentials via Secret Service.
2. Template lifecycle:
   - version;
   - locale;
   - preview;
   - approve;
   - rollback.
3. Subscriptions:
   - user preferences;
   - digest schedule;
   - suppression windows;
   - unsubscribe audit.
4. Delivery:
   - retry;
   - DLQ;
   - provider callback;
   - bounce/suppression events.
5. Inbox:
   - native in-app notifications;
   - read/unread;
   - archive;
   - search.

Acceptance:

- Delivery jobs use canonical outbox or projection task store.
- Provider secrets are never stored inline.
- Template publish requires version and audit evidence.

### Analytics Service

Current reuse points:

- `src/runtime/service/analytics_service`
- admin summary and catalog admin code
- canonical audit/projection/saga stores

Dependency choices:

- Reuse `prometheus`, `opentelemetry`, `tracing`, and existing metric labels.
- Do not add DataFusion/Arrow in the first analytics phase. ClickHouse append
  profile and existing rollup tables are a simpler fit for UDB's current
  runtime.
- Revisit Arrow/DataFusion only if UDB needs local columnar query execution over
  exported audit/projection files.

Plan:

1. Capability health:
   - enabled/degraded/unsupported operations;
   - failed conformance evidence;
   - doctor warnings over time.
2. Usage/cost rollups:
   - RPC count;
   - backend operation count;
   - object bytes;
   - vector writes/searches;
   - estimated cost by tenant/project.
3. Auth analytics:
   - login success/failure;
   - key usage;
   - policy denials;
   - risky sessions.
4. Pipeline health:
   - CDC lag;
   - outbox backlog;
   - saga backlog;
   - projection drift;
   - DLQ trends.
5. Quota/anomaly:
   - threshold forecasts;
   - unusual spikes;
   - noisy tenant detection.

Acceptance:

- Rollups are generated from existing audit/event stores.
- Analytics queries avoid scanning raw logs for every request.
- ClickHouse append profile can accelerate this but cannot be required.

## New Native Services

### BackendAdminService

Purpose:

- register backend instances;
- validate DSNs;
- run backend doctor;
- expose capability/degradation state;
- manage resource lifecycle policies.

Reuse:

- `src/backend/plugins/*.rs`;
- `src/runtime/core/setup_data.rs`;
- `src/cli/doctor.rs`;
- `ResourceAdminExecutor`;
- capability V2 evidence.

First RPCs:

- `RegisterBackendInstance`;
- `ValidateBackendInstance`;
- `ProbeBackendInstance`;
- `ListBackendCapabilities`;
- `SetBackendPolicy`;
- `GetBackendHealth`;

### SecretService

Purpose:

- store references to secrets;
- rotate credentials;
- resolve DSNs and provider credentials at runtime without storing plaintext in
  config or docs.

Reuse:

- runtime config secret redaction;
- backend setup helpers;
- auth event/audit stores.

First RPCs:

- `CreateSecretRef`;
- `RotateSecret`;
- `GetSecretMetadata`;
- `BindSecretToBackend`;
- `RevokeSecret`;

### SchemaRegistryService

Purpose:

- manage logical schemas, resource contracts, versioning, compatibility, and
  migration plans across backends.

Reuse:

- generation and manifest code;
- `NativeModel`;
- migration audit store;
- resource lifecycle executor.

First RPCs:

- `RegisterSchema`;
- `CheckCompatibility`;
- `PlanMigration`;
- `ApproveMigration`;
- `GetSchemaVersion`;

### WorkflowService

Purpose:

- coordinate long-running operations: migrations, projection rebuilds, key
  rotations, policy approvals, notifications, and repair jobs.

Reuse:

- saga store;
- projection task store;
- outbox;
- admin audit.

First RPCs:

- `StartWorkflow`;
- `GetWorkflow`;
- `ApproveStep`;
- `CancelWorkflow`;
- `RetryWorkflow`;

### DataQualityService

Purpose:

- define validation rules, run checks, store findings, and produce remediation
  tasks.

Reuse:

- IR compilers;
- query executor;
- analytics service;
- projection task store.

First RPCs:

- `CreateRule`;
- `RunCheck`;
- `ListFindings`;
- `CreateRemediationTask`;
- `MarkFindingResolved`;

### LineageService

Purpose:

- track data movement from canonical stores to projections, objects, search
  indexes, vectors, and analytics rollups.

Reuse:

- projection tasks;
- CDC source model;
- admin audit;
- migration audit;

First RPCs:

- `RecordLineageEvent`;
- `GetResourceLineage`;
- `GetProjectionDrift`;
- `ExplainDataMovement`;

### ConnectorService

Purpose:

- manage external connectors and sync jobs for SaaS, files, event streams, and
  databases.

Reuse:

- BackendAdminService instance registry;
- SecretService;
- WorkflowService;
- projection tasks;
- outbox.

First RPCs:

- `RegisterConnector`;
- `ValidateConnector`;
- `StartSync`;
- `GetSyncStatus`;
- `PauseSync`;
- `ResumeSync`;

## Practical Milestones

### Phase 0: De-Island Existing Code

Deliverables:

1. Add SQL canonical core modules.
2. Convert Postgres/MySQL/SQLite to shared SQL helpers incrementally.
3. Expand `canonical_store/conformance.rs` to run all `SystemStores` contracts
   through one reusable harness.
4. Add V2 capability structs with no public behavior change.
5. Add capability evidence helpers to plugins.
6. Add matrix invariant tests.

Exit criteria:

- Existing tests pass.
- No capability output changes except optional V2 fields.
- SQL canonical store duplication decreases.

### Phase 1: Capability Matrix V2 Plumbing

Deliverables:

1. V2 capability structs in Rust.
2. Proto additions for versioned capability details.
3. `GetCapabilities` response includes V2.
4. `udb doctor` and `compat-matrix` read V2.
5. Runtime admission uses V2 for new checks but V1 remains mapped.
6. Documentation generation/validation added.

Exit criteria:

- V1 clients still work.
- Capability docs are generated or checked from code.
- Unsupported operations fail before side effects.

### Phase 2: SQL Server Canonical

Deliverables:

1. `MssqlCanonicalDialect`.
2. `MssqlCanonicalStore`.
3. Full `SystemStores` implementation.
4. Registration in `setup_data.rs`.
5. V2 canonical profile and evidence.
6. Live conformance behind `UDB_MSSQL_DSN`.

Exit criteria:

- SQL Server passes canonical conformance.
- `udb doctor` reports missing permissions clearly.
- SQL Server moves from projection to canonical only with evidence.

### Phase 3: MongoDB Canonical

Deliverables:

1. `MongoCanonicalStore`.
2. Collection validators and indexes.
3. Transaction/topology gate.
4. Change-stream CDC capability.
5. Full conformance behind `UDB_MONGODB_DSN`.

Exit criteria:

- Replica set/sharded MongoDB can be canonical.
- Standalone MongoDB reports degraded/projection.
- Read fences have tested semantics.

### Phase 4: Native Backend Admin, Secrets, And Provider Registries

Deliverables:

1. `BackendAdminService` proto and runtime module.
2. `SecretService` proto and runtime module.
3. Provider registry model reused by Authn and Notification.
4. Admin audit for all registration/rotation changes.
5. Doctor uses BackendAdmin internals where possible.

Exit criteria:

- Backend registration is no longer only env-var driven.
- Secret material is never echoed in capability or doctor output.
- Provider config is tenant scoped.

### Phase 5: Cassandra And Neo4j Canonical Profiles

Deliverables:

1. Cassandra tunable canonical store.
2. Neo4j graph-native canonical store.
3. ReBAC store adapter for Neo4j and SQL canonical stores.
4. Conformance suites and capability limitations.

Exit criteria:

- Each backend reports exact limitations.
- No full-canonical claim unless strict conformance passes.
- Authz can use Neo4j relationship graph through a shared trait.

### Phase 6: ClickHouse Append Profile And Projection Parity

Deliverables:

1. ClickHouse append-only audit/analytics profile.
2. Projection parity improvements for object/vector/search/cache backends.
3. Capability fields for streaming, multipart, hybrid search, TTL, streams,
   templates, reindex, and index health.

Exit criteria:

- ClickHouse accelerates analytics without claiming lease/saga authority.
- Projection backends expose precise useful features.

### Phase 7: Native Service Expansion

Deliverables:

1. Authn OIDC/SCIM/WebAuthn/service-account features.
2. Authz simulation/lifecycle/ReBAC/native-access expansion.
3. API key quota/rotation/anomaly controls.
4. Tenant lifecycle/residency/quota policies.
5. Notification provider/template/subscription/delivery features.
6. Analytics capability/usage/auth/pipeline/quota rollups.
7. Workflow, schema, lineage, data-quality, connector services.

Exit criteria:

- Services reuse shared stores and event sinks.
- Long-running work uses saga/projection/outbox primitives.
- Policy, tenant, and backend decisions are auditable.

### Phase 8: SDK, CLI, Docs, And Operational Polish

Deliverables:

1. SDK metadata generated from V2 capabilities.
2. CLI commands for backend registration, doctor, secrets, policies, schemas,
   workflows, and capability inspection.
3. Docs generated from code for capability tables.
4. Upgrade guide for V1 to V2 capability clients.
5. Example deployment profiles:
   - embedded SQLite;
   - Postgres reference;
   - SQL Server enterprise;
   - MongoDB document;
   - Cassandra distributed;
   - Neo4j graph;
   - ClickHouse analytics.

Exit criteria:

- New users can inspect exact capabilities before using a backend.
- Operators can see limitations and remediation steps.
- Docs cannot drift from code silently.

### Phase 9: Protocol V2 And Native Acceleration

Deliverables:

1. Protocol negotiation in `GetCapabilities` or `GetProtocolSupport`.
2. Typed error details for capability, policy, quota, schema, retry, and backend
   failures.
3. `RecordBatchV2` and streaming `SelectV2` behind negotiation.
4. Selected protobuf `bytes` fields generated as `bytes::Bytes` in Rust after
   SDK compatibility tests.
5. Provider-backed object upload streaming without full-body buffering.
6. Criterion benches for current `RecordSet`, `RecordBatchV2`, object streams,
   base64, generic JSON dispatch, audit hash verification, and vector rerank
   if local rerank exists.
7. `runtime/accel` module with scalar-first APIs and optional SIMD crates.
8. Windows and Unix CI coverage for scalar and optimized builds.

Exit criteria:

- V1 clients keep working unchanged.
- SDKs negotiate V2 and fall back to V1 automatically.
- `RecordBatchV2` beats `RecordSet` on large typed reads in benchmarks.
- Object streaming avoids full object buffering for upload and download.
- No assembly is merged unless it passes the handwritten assembly admission
  gate.
- Windows x86_64, Linux x86_64, and Linux AArch64 compile optimized feature
  sets before release.

## Test Strategy

Local tests:

- capability matrix invariant tests;
- compiler capability rejection tests;
- SQL dialect rendering tests;
- canonical store conformance with SQLite;
- auth/native-service unit tests;
- doc generation validation.
- protocol V2 encode/decode compatibility tests;
- V1/V2 SDK negotiation fallback tests;
- scalar-vs-SIMD property tests for any accelerated kernel.

Live tests behind env vars:

- `UDB_POSTGRES_DSN`;
- `UDB_MYSQL_DSN`;
- `UDB_MSSQL_DSN`;
- `UDB_MONGODB_DSN`;
- `UDB_CASSANDRA_DSN`;
- `UDB_NEO4J_DSN`;
- `UDB_CLICKHOUSE_DSN`;
- object/vector/search backend DSNs as available.

Required acceptance commands for code phases:

```powershell
cargo test --lib
cargo run --bin udb-proto-parser -- compat-matrix
cargo run --bin udb -- doctor
```

Protocol/performance acceptance commands:

```powershell
cargo bench --bench protocol_record_batch
cargo bench --bench object_streaming
cargo bench --bench object_base64
cargo test --features simd-codecs --lib
```

For live backend phases, use backend-specific env vars and run only that live
suite so unrelated unavailable infrastructure does not block progress.

## Definition Of Done

A backend feature is done only when:

- the capability matrix has precise fields and limitations;
- the compiler or executor path exists;
- runtime admission checks the capability before side effects;
- `GetCapabilities`, `udb doctor`, and `compat-matrix` agree;
- tests cover success and refusal paths;
- docs are generated or validated from code;
- no new duplicate store or service island was created.

A canonical driver is done only when:

- it implements `CanonicalStore` and `SystemStores`;
- it is registered through `register_full_canonical_store`;
- it passes the shared conformance harness;
- it has a durability-token/read-fence story;
- it has backup/restore and permission checks in doctor;
- it emits admin/migration audit records;
- its limitations are represented in V2 capability output.

A protocol or acceleration feature is done only when:

- V1 compatibility is preserved;
- SDK fallback behavior is tested;
- capability/protocol negotiation exposes the feature;
- scalar fallback exists;
- Windows and Unix builds compile;
- benchmarks show the optimization is worth the maintenance cost;
- unsafe code is isolated and documented if SIMD intrinsics or assembly are
  used.

## Research Sources

External driver/library assumptions in this plan were checked against current
public documentation on 2026-06-02:

- MongoDB Rust driver transactions and sessions:
  https://www.mongodb.com/docs/drivers/rust/current/crud/transactions/
- Tiberius SQL Server Rust driver:
  https://docs.rs/tiberius/latest/tiberius/
- ScyllaDB Rust driver:
  https://rust-driver.docs.scylladb.com/
- Neo4j Rust `neo4rs` crate:
  https://docs.rs/neo4rs/latest/neo4rs/
- AWS SDK for Rust:
  https://docs.aws.amazon.com/sdk-for-rust/
- Google Cloud Storage Rust client:
  https://docs.cloud.google.com/rust/quickstart-storage
- Azure Rust SDK release notes:
  https://azure.github.io/azure-sdk/releases/2026-02/rust.html
- Apache OpenDAL:
  https://opendal.apache.org/
- Rust `object_store` crate:
  https://docs.rs/object_store/latest/object_store/
- `webauthn-rs`:
  https://docs.rs/webauthn-rs/latest/webauthn_rs/
- `openidconnect`:
  https://docs.rs/openidconnect/latest/openidconnect/
- Cedar policy language:
  https://docs.cedarpolicy.com/
- OPA Wasm:
  https://www.openpolicyagent.org/docs/wasm
- Rust inline assembly reference:
  https://dev-doc.rust-lang.org/reference/inline-assembly.html
- Rust architecture intrinsics:
  https://doc.rust-lang.org/stable/core/arch/
- Rust x86 runtime feature detection:
  https://dev-doc.rust-lang.org/beta/std/arch/macro.is_x86_feature_detected.html
- Rust AArch64 runtime feature detection:
  https://doc.rust-lang.org/std/arch/macro.is_aarch64_feature_detected.html
- Prost `bytes::Bytes` generation:
  https://docs.rs/prost-build/latest/prost_build/struct.Config.html
- gRPC compression:
  https://grpc.io/docs/guides/compression/
- Tonic gRPC implementation docs:
  https://docs.rs/tonic/latest/tonic/
- `base64-simd`:
  https://docs.rs/base64-simd/latest/base64_simd/
- `simd-json`:
  https://docs.rs/simd-json/latest/simd_json/
- `crc32fast`:
  https://docs.rs/crc32fast/latest/crc32fast/
- Apache Arrow IPC:
  https://arrow.apache.org/docs/format/Columnar.html
- Apache Arrow Flight:
  https://arrow.apache.org/docs/format/Flight.html
