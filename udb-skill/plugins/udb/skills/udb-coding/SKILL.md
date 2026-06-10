---
name: udb-coding
description: Contribute code TO the UDB repository — deep codebase knowledge (the proto→descriptor→runtime pipeline, data-plane and native-service request lifecycles, CDC/outbox, canonical stores, XA/sagas, IR compilers, migration gates) plus the house coding doctrine (no code islands, reuse shared helpers, no hardcodes/stubs, fail closed). Use when implementing UDB plan/fix items (master_todo.md, fix plans), writing or reviewing Rust in src/runtime|ir|parser|generation|migration|control|backend|cli, adding a native service or backend, or asking where something lives in the UDB codebase.
allowed-tools: Read, Grep, Glob, Bash
---

# UDB Coding

UDB is a **proto-driven multi-database broker** (Rust, tonic/prost, sqlx): one
annotated proto contract derives DB schema, per-RPC security enforcement, ~17
native services / 260+ RPCs across 18 backends, six SDKs, CLI and docs. This
skill carries the codebase map AND the house doctrine for changing it safely.

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
