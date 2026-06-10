# UDB Rust stack — how THIS repo uses the toolchain (companion to udb-coding)

Not a Rust tutorial. This is the delta an agent needs: which crates UDB uses,
the repo's idioms for each, and the traps already hit here. Canonical copy:
`udb-skill/shared/udb-coding-rust-stack.md` (mirrored into the Claude skill's
`references/rust-stack.md`).

## Toolchain & workspace

- Workspace: root crate `udb` (lib + `udb` bin from `src/main.rs`),
  `crates/udb-portable` (shares parser/IR/descriptor source via `#[path]`
  includes — never copies; fs-dependent entry points stay un-shared),
  `crates/udb-wasm` (wasm-bindgen cdylib for the playground).
- `build.rs` runs protoc over `proto/udb/**` and embeds the
  `FileDescriptorSet`; generated prost types land in OUT_DIR. Changing a proto
  ⇒ next build regenerates Rust types automatically — but the COMMITTED
  artifacts (contract baseline, manifest JSON, buf stubs, SDK clients) need the
  regen protocol from the main skill.
- **Feature flags** gate optional surface: per-backend IR compilers, `ws-signalling`,
  `asset-image`, `kms`-style provider deps, `openssl`-gated WebAuthn attestation.
  Optional heavy deps MUST stay optional (compile time is 10–30 min already).
- Windows: `rdkafka-sys` builds librdkafka via CMake. If the build fails with
  `Could not create named generator Visual Studio <N>`, the cmake on `PATH` is
  older than the installed Visual Studio and lacks that generator — point the
  `CMAKE` env var at the cmake bundled with your VS install (under
  `…/CommonExtensions/Microsoft/CMake/CMake/bin/cmake.exe`), or upgrade the PATH
  cmake. Piped cargo hides exit codes — read `${PIPESTATUS[0]}`. `cargo check`
  does NOT compile `#[cfg(test)]` code.

## tokio (async runtime)

- Background work = **leader-elected singleton workers** (`runtime/singleton.rs`:
  `WORKER_*` consts, `run_while_leader`, `WORKER_SINGLETON_LEASE_TTL`), spawned
  in `service/mod.rs`. Never a bare `tokio::spawn(loop)` on every replica.
- Fan-out streaming uses `tokio::sync::broadcast` (see `SignalingHub` — and its
  lesson: a broadcast channel with live receivers never drains; targeted
  termination needs an addressed frame like `HubFrame::PeerClosed`).
- Deadlines: `tokio::time::timeout` around per-item futures; on timeout you must
  RESET any persistent state you marked in-flight (the CDC `publishing` lesson).
- **Never block the executor:** no `std::fs`, no synchronous network, and no
  repeated `std::env::var` (process-wide lock!) inside request paths. Resolve
  env/config ONCE — `OnceLock`/`LazyLock` or constructor fields
  (`descriptor_contract_manifest_static` and `channels.rs` are the references).
- Cross-task request context: task-locals (the method-security
  `VerifiedClaimContext`), not globals.

## tonic / tower (gRPC)

- Cross-cutting enforcement lives in a tower layer (`MethodSecurityLayer`), NOT
  in handlers — but the layer sees **headers only**; anything body-carried
  (tenant/owner ids) must be re-bound to the claim inside the handler.
- Peer info: `request.extensions()` `TcpConnectInfo` (remote_addr) — used by the
  `internal_grpc_only` loopback check. Metadata keys are the `x-…` contract.
- **Status code conventions in this repo:** `invalid_argument` = malformed
  input; `failed_precondition` = wrong state/disabled feature/sealed (clients
  shouldn't blind-retry); `aborted` = CAS/version conflict (client re-reads and
  retries); `permission_denied` = scope/tenant/policy; `resource_exhausted` =
  rate/quota/backpressure; `unauthenticated` = missing/bad credential. NEVER
  `unimplemented` for not-yet-ready features (SDK retry classifiers treat it
  differently) — use `failed_precondition` + honest degraded health.
- Server streaming uses `async_stream`; per-item admission permits must live
  INSIDE the stream (`native_helpers::execute_stream_batch_item`).

## prost / proto3 semantics (trap-dense)

- Plain scalar fields have NO presence: absent `bool`/`int` decodes as
  `false`/`0`. For partial-update RPCs declare fields `optional` (synthetic
  oneof ⇒ `Option<T>` in Rust) and bind with COALESCE — the `is_public` bug
  shipped because a plain bool silently reset visibility on every update.
- Adding `optional` to an existing field is wire-compatible BUT changes the
  descriptor (synthetic oneof) ⇒ contract baseline regen + generated clients
  change shape (`Option<bool>`/pointer types) — sweep ALL Rust constructors of
  that message (`is_public: true` → `Some(true)`).
- Never bind a raw `Option<T>` into a NOT NULL column — `unwrap_or(default)`
  at the bind site, with a test.

## sqlx (Postgres-first data access)

- Runtime queries are mostly `sqlx::query`/`query_as` with `$n` binds against
  pool handles; multi-statement work uses explicit `pool.begin()` transactions.
- **Transactional outbox**: the event INSERT rides the same transaction as the
  mutation. Keep it that way — routing outbox writes through an abstraction
  must not break the shared-transaction property.
- Partial updates: `SET col = COALESCE($n, col)` driven by presence
  (`Option<T>` binds), mirroring the proto rule above.
- Claim/queue patterns: `SELECT … FOR UPDATE SKIP LOCKED` (scheduler/poll
  loops); `ON CONFLICT (…) DO NOTHING/UPDATE` for idempotent inserts.
- RLS defense-in-depth: queries that rely on Postgres RLS must run inside a
  transaction where `set_request_local_settings` installed the `app.*` GUCs —
  a bare `fetch_all(&pool)` silently skips RLS (the join-fusion leak).
- Tests touching SQL behavior are env-gated live tests (DSN chain in the main
  skill); never an in-memory fake of the store.

## rdkafka (Kafka)

- Producer: delivery futures must be awaited for proof; `producer_epoch` fences
  zombie producers across restarts. Exactly-once modes track per-row
  `delivery_state` — any new failure path must reset rows it marked.
- Consumer (the blessed pattern — copy `spawn_storage_finalized_consumer`):
  `auto.offset.reset=earliest`, `enable.auto.commit=false`, commit ONLY after
  the handler succeeds, idempotent handlers (correlation id).
- Topics are versioned dot names `udb.<svc>.<entity>.<verb>.v1` — never
  slash/colon, always a partition key.

## Error handling & logging

- Domain engines return typed errors; the service adapter maps to tonic
  `Status` at the boundary (one mapping site, not scattered).
- No `unwrap()`/`expect()` on serving paths fed by request input. `expect()` is
  acceptable only for startup invariants that should abort the process
  (fail-closed boot, e.g. descriptor decode).
- `tracing` for logs; NEVER log secrets/payloads — credential-bearing structs
  get manual redacting `Debug` impls (pattern: `connection_manager.rs`,
  `encryption.rs`). Assume any `{:?}` may reach production logs.

## Testing idioms

- Unit tests in-module (`mod tests` at the bottom — the codebase-map indexer
  also relies on this convention).
- Claim-context harness: `method_security::scope_claim_context_for_test` +
  `test_claim_context` to drive handlers as an authenticated caller.
- Live tests: `#[ignore = "requires live …"]` + env-gated DSNs; auth-plane
  suites serialize on `live_auth_db_lock`; SQLite-file tests write under
  `std::env::temp_dir()` (never the repo root) with Drop-guard cleanup.
- The strongest test asks: "would reverting the fix make this fail?" — string
  asserts on SQL and enum checks usually can't say yes.
