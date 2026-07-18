---
name: udb-coding
description: Contribute code TO the UDB repository — deep codebase knowledge (the proto→descriptor→runtime pipeline, data-plane and native-service request lifecycles, CDC/outbox, canonical stores, XA/sagas, IR compilers, migration gates) plus the house coding doctrine (no code islands, reuse shared helpers, no hardcodes/stubs, fail closed). Use when implementing UDB plan/fix items (master_todo.md, fix plans), writing or reviewing Rust in src/runtime|ir|parser|generation|migration|control|backend|cli, adding a native service or backend, or asking where something lives in the UDB codebase.
allowed-tools: Read, Grep, Glob, Bash
---

# UDB Coding

UDB is a **proto-driven multi-database broker** (Rust, tonic/prost, sqlx): one
annotated proto contract derives DB schema, per-RPC security enforcement, **27
native services / 344 RPCs (77 DataBroker + 267 native)** across 18 backends,
six SDKs, CLI and docs. This skill carries the codebase map AND the house
doctrine for changing it safely. Current code/SDK baseline is **0.4.0** (wire protocol `1.0.0`).

**Full guide (read on demand): [references/udb-coding.md](references/udb-coding.md)** —
the proto→runtime pipeline, both request lifecycles file-by-file, the CDC/
canonical-store/XA/IR/migration subsystem mechanics with real function names,
the native-service inventory, test/CI layout, the ten directives, the
10-question flaw catalog, and the new-service recipe.

**To locate any symbol/file: read `docs/generated/codebase-map.md` first**
(generated module-dependency graphs + per-file public-symbol index, CI
freshness-gated — never stale), then grep the canonical name it gives you.

**Companion references (read the relevant one before coding in that area):**
- [references/rust-stack.md](references/rust-stack.md) — how THIS repo uses
  tokio/tonic/tower/prost/sqlx/rdkafka (idioms + traps), not a Rust tutorial.
- [references/backends.md](references/backends.md) — per-engine quirks for all
  18 backends (RLS/tenant posture, canonical-store tier, the live-DB + audit
  traps). Read the row before touching any `executors/<b>.rs`,
  `ir/compile/<b>.rs`, or `canonical_store/<b>*.rs`.
These carry the **UDB-specific delta** of each technology — rely on your own
generic Rust/SQL/engine knowledge for everything else.

## Architecture in five lines (hold this)
1. **One spine:** `proto/udb/**` → build.rs-embedded FileDescriptorSet →
   `descriptor_manifest.rs` (OnceLock, fail-closed panic on bad decode) → drives
   method_security, native routing, DDL generation, SDK manifest, docs. Never
   hand-add a Rust id list — derive from the descriptor.
2. **Two listeners:** public broker (health/reflection/DataBroker only) and the
   native control-plane listener (loopback, port+10) hosting all native services.
3. **Every request:** tower `MethodSecurityLayer` (headers only → installs
   `VerifiedClaimContext`) → handler (MUST bind body tenant/owner to the claim —
   reads included) → fair admission (`channels.rs` / `admit_on`) → dispatch.
4. **Every mutation event:** transactional outbox → CDC tailer → Kafka, with
   journal (replay source), DLQ-insert-before-mark, fail-closed tenant-scoped
   subscriber streams.
5. **Every background job:** leader-elected (`singleton.rs` `WORKER_*` consts,
   `run_while_leader`) — never a bare interval loop on every replica.

## Startup path & enterprise config (read before touching `serve()`/config)
- **0.3.7 CLI/preflight surface:** `cli/help.rs` handles `udb --help`,
  `udb help <cmd>`, per-command `--help`, `udb --version`, and near-miss
  suggestions before heavy startup side effects. `Command::Requirements` emits
  the manifest-derived backend contract and exits non-zero for missing fatal
  requirements. `doctor --enterprise` is manifest-aware and reports required
  backend gaps before `serve()` hits them.
- **Backend init is SERIAL and order-dependent.** The `all_plugins()` register loop
  (`runtime/core/setup_data.rs`) registers each backend in order; "first registered
  wins" picks the default SystemStores — so **do NOT parallelize it** (correctness
  bug). Each `register()` is wrapped in `tokio::time::timeout(UDB_BACKEND_STARTUP_PROBE_SECS,
  …)` (default 8) so a configured-but-unreachable backend (e.g. MongoDB's ~30s
  server-selection) degrades to "unavailable" instead of stalling boot. A backend
  only registers if it has an explicit DSN — but the binary **`load_project_dotenv()`
  walks UP from CWD** (`cli/mod.rs`), so a stray repo `.env` silently injects
  backend DSNs and slows startup; don't add default-localhost DSNs.
- **Fast restart:** `UDB_STARTUP_SKIP_IF_UNCHANGED=true` (`control/lifecycle.rs`)
  skips generate/apply/provision/verify when the persisted manifest checksum is
  unchanged — addresses the remote-DB (~2 min) re-bootstrap, NOT backend probing.
- **One-shot preflight:** `runtime/preflight.rs` `evaluate(&config, addr)` reports
  ALL unmet enterprise prereqs at once (encryption/password/session/auth-plane/
  redis/authz); wired into `serve()` startup AND `udb doctor --enterprise`.
- **ONE authz engine (Casbin):** both the data-plane `authorize()` and the
  control-plane `AuthzService.Check` decide via Casbin over roles/`policy_rules`
  (default-DENY). The data plane reads a shared, PG-warmed snapshot of
  `policy_rules`; there is no separate env-JSON ABAC lane. Production force-sets
  TLS+mTLS in `ServiceSettings::apply_security_posture`
  (warns when it overrides an explicit `=false`); TLS env = `UDB_TLS_CERT_PEM|PATH`,
  `UDB_TLS_KEY_PEM|PATH`, `UDB_MTLS_CLIENT_CA_PEM|PATH`.
- **0.3.7 auth/crypto fixes:** DataBroker bearer auth now returns a clearer
  `UNAUTHENTICATED` when a caller sends only `x-api-key`; the data plane still
  requires JWT bearer or mTLS. `runtime/encryption.rs` accepts 32-byte base64,
  64-char hex/`0x` hex, or raw 32-byte keys and names the configured key source
  in decode errors; reuse that decoder rather than adding a parallel one.

## The ten directives (audit-derived)
1. Proto is the source of truth (+ regen protocol after proto changes).
2. Reuse before you write — grep `native_helpers.rs`, `singleton.rs`,
   `ir/compile/util.rs`, `system_store.rs` first (duplicate helpers caused a
   real cross-tenant leak).
3. No code islands — wire-in over delete; verify callers with
   `grep -rn "<fn>" src/ crates/ tests/ build.rs` (cfg(test) doesn't count).
4. No capability lies — posture/matrix/health/docs must match the serving path.
5. No hardcodes — named const, or env resolved ONCE (OnceLock); versions via
   `env!("CARGO_PKG_VERSION")`.
6. No stubs — real impl, or typed `failed_precondition` + degraded health.
7. Fail closed — unresolvable tenant/scope/key/policy ⇒ reject; reads get the
   same claim binding as mutations.
8. No in-memory stores (even tests) — canonical store/PG; live env-gated tests
   via the DSN chain in the reference.
9. Tests call the served path — would reverting the fix fail the test?
10. Cargo discipline — batch edits; one check per wave, one
    `cargo test --workspace --lib` at the end; `${PIPESTATUS[0]}`; check skips
    test code; Windows needs `CMAKE` pointed at the VS-bundled cmake.

## Before claiming DONE
Run the reference's 10-question flaw catalog (island? duplicate? hardcode?
stub? fail-open? capability lie? mirror test? unbounded? hot-path waste?
fence?). Append `— DONE (date): <what> (file:line)`. Honest `[~]` beats a
false `[x]` — claims are re-audited adversarially against source.

## Guardrails
- Stay inside the assigned file fence; out-of-fence edits are REPORTED, never
  made. Never hand-edit generated files (`docs/generated/**`, `sdk/*/gen/**`,
  generated clients) — they come from `udb native …` / `buf` / `udb sdk generate`.
- Copy the named in-repo pattern; don't redesign it in passing.
- Line numbers drift — locate symbols by name; when doc and code disagree,
  trust the code and report the drift.
