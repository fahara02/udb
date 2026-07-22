# UDB 0.4.19 — Execution Plan

Status: IN PROGRESS · Predecessor: `v0.4.18` (`main@b30cabcf`, broker boots,
full CI incl. live suite green) · Author: maintainer.

## Implementation log (branch: refactor/modularize-services)

**Thrust A (reliability) — COMPLETE (local `cargo check` green, zero new warnings):**
- Block 1 — **A1** ✅ `sqlx_error_to_status` classifies transient transport (PoolClosed/
  PoolTimedOut/Io) AND connection-loss SQLSTATEs (08*/57P0x) as retryable `Unavailable`;
  test rewritten.
- Block 2 — **A2–A5** ✅ `store_string_from_status` tags retryable across the
  `Result<_,String>` resolver boundary; `grants.rs` 5 sites tag; enforcement
  decode-and-branch at `security.rs:1623`, `method_security.rs` bearer + api-key
  (`STORE_UNAVAILABLE` reason). Mixed-`Err` invariant preserved (outage→Unavailable,
  bad-token→Unauthenticated).
- Block 3 — **A6** ✅ split cert flag: new `certificate_resolution_unavailable`,
  retryable branch at both cert enforcement sites.
- Block 4 — **A8** ⏳ (building) live per-listener readiness-refresh task
  (`handlers_meta.rs` + `credential_layer::auth_plane_pg_reachable`); downgrade-only,
  `boot_ready && live_pg_ok`, flap grace; per-node, no leader election.
- **A7** ✅ verified no-code (authn DB path already tags/decodes; crypto paths terminal).
- **D2** ✅ verified no-code (`23505 → AlreadyExists` already classified).
- **D1** ⏸ deferred pending a live repro (won't touch live-green auth SQL speculatively).

**Thrust B (codegen) — foundation done, verified:**
- Block 5 — **B1** ✅ `--project-proto <dir>` input path: `entity_manifest_from_proto_dir`
  parses consumer `.proto` source (namespace-agnostic annotations) →
  `CatalogManifest::from_schemas` → typed entities; wired at `generate()`
  (`sdk_gen.rs`). RPC surface stays UDB's. `cargo check` green.
- Block 6 — **B2** ✅ `EntityColumnDescriptor` (proto_type/sql_type/not_null/is_array/
  enum_values/is_json*/exclude_from_insert/is_blind_index/is_pii) on `EntityDescriptor`,
  populated from `ManifestColumn`. `cargo check` green.
- **B3** ⧗ specced (litmus §4b/§4c) — typed Go marshalling generator into the
  CONSUMER package (`render_go_entity_marshal` + `{{ENTITY_GO_MARSHAL}}` + a
  consumer-package output file). The piece that deletes `record.go`/`udb_record.go`/
  `udb_helpers.go` + the marshalling half of each `*_udb.go`. NOT yet implemented.
- **B3** ✅ (was ⧗) — `render_go_entities_file` in `sdk_gen.rs` emits typed
  `<Entity>ToUDBRecord`/`FromUDBRow` + once-per-file coercion helpers into the
  CONSUMER's Go package (aggregated imports, enum short-token via computed prefix,
  Timestamp↔RFC3339Nano), wired at the render loop + `--go-package` flag. `cargo
  check` green (Block 7). NOTE: the Rust emitter is verified; the GENERATED Go's
  `go build` correctness is the litmus check, which needs the binary (env-blocked).
- **B5** ✅ assessed — mostly ALREADY PRESENT: the Go template exposes
  `Error.DetailBin` (raw ErrorDetail) and `DefaultRetryConfig.RetryOnCodes =
  [Unavailable, ResourceExhausted]` + `retryableForRPC`, so a generated Go client
  ALREADY auto-retries `codes.Unavailable` — precisely what A1–A8 now returns for a
  DB outage (end-to-end: outage → Unavailable → auto-retry, not an auth failure).
  Residue: typed field accessors (`FieldViolations`, `Kind`) need the ErrorDetail
  proto type the template deliberately avoids importing (dependency trade-off) —
  deferred; low value given `DetailBin` is exposed, and unverifiable without the
  binary.

**D4** ✅ (release atomicity) — `release-binaries.yml`: per-platform matrix now
attaches to a `draft: true` release; the final `manifest` job flips `draft: false`,
so the Releases API never exposes a published release with a partial asset set. YAML
valid (`draft` is a documented `softprops/action-gh-release@v2` input).

**Litmus (ambutest/LITMUS.md) — grounding COMPLETE:** baseline confirmed by running
the real binary (metadata + `map[string]any` generic Repository, UDB-entities-only);
boilerplate cataloged + quantified (~12–15k of ~23–25k LOC eliminable); before/after
+ conversion contract specced.

**Remaining for 0.4.19 must-cut:** B3 + B5 + D4 (release workflow, no cargo) +
version bump + push-only live-suite validation + the binary-driven litmus diff
(blocked intermittently by the local build-process killer; B1/B2 verified via
`cargo check`).

---

## 0. North Star — what 0.4.19 achieves

Two thrusts, one goal: **make UDB trustworthy to run and trivial to consume.**

1. **Trustworthy failure semantics (finish UDB-DB-READINESS-001).** A dependency
   outage (Postgres unreachable, pool exhausted, connection lost) must NEVER be
   surfaced to a caller as an authentication/authorization failure. It is a
   *retryable* `UNAVAILABLE`, and the broker's readiness must flip so orchestrators
   route away. 0.4.18 fixed exactly **one of five** credential channels; 0.4.19
   fixes the systemic root and the remaining four, plus the runtime readiness flip.

2. **"Point your proto, get a working typed client."** A consumer today hand-writes
   ~23–25k LOC of glue (typed CRUD, proto↔record marshalling, CAS/merge helpers,
   enum shims, tenant injection, error mapping) — and *emulates SQL in its host
   language* (expression-UPDATE, aggregates, ORDER BY/LIMIT/ranges, multi-row tx,
   >500-row paging) because the data plane lacks those verbs. 0.4.19:
   - **B**: wires `udb sdk generate` to the **consumer's** protos so the existing
     per-entity CRUD generator emits their typed repositories/marshalling/etc.
   - **C**: makes the **data plane own the query capabilities** the consumer
     emulates — so generated code calls real server verbs, and the correctness
     caveats (lost-update counter race, page-cap worker stall) disappear at the
     source, not behind a template.

**Serialisation:** every work item is IDed `R19-<thrust><n>` (A=reliability,
B=codegen, C=data-plane capability, D=hardening). Dependencies are explicit.
Each item is specified as **Achieves / Do / Where / Blast radius / Definition of
Done (DoD)**. A global release DoD is in §6.

**Sequencing (critical path):** `R19-A1 → A2 → {A3,A4,A5,A6,A7}`; `R19-C*` land
before `R19-B4`'s "generate against real capabilities"; `R19-B1/B2` unblock all of
B; A and the C/B tracks are independent and can run in parallel.

**Non-negotiable gates (apply to every item):** ONE cargo build at a time; the new
`embedded_native_manifest_passes_startup_manifest_validation` lib gate must stay
green (the broker must boot); the push-only live Postgres/canonical-store suite
must be green on `main` before tagging; version bump via `versions.json` +
`node scripts/check-versions.mjs --fix`; NATIVE_CONTRACT_VERSION bumped + baseline
regenerated only if the contract surface changes.

---

## Thrust A — Reliability: outage ≠ auth failure (UDB-DB-READINESS-001, full)

Evidence: error-code audit (2026-07-21). The async credential layer stores each
resolver outcome as an opaque `Result<_, String>`; on a store outage resolvers
return `Err(...)`, on a genuine denial they return `Ok(false)`/`Ok(None)`. The sync
enforcement layers collapse `Err` to a terminal auth denial. 0.4.18 fixed only the
data-plane API-key channel (`reconcile_api_key_principal` → `retryable_status`).

### R19-A1 — Retryable classification at the sqlx→Status root
- **Achieves:** pool/connection failures become a *classifiable* retryable signal
  instead of a flat `Internal`, so every downstream path can route an outage to
  `UNAVAILABLE`. This is the systemic enabler for A2–A7.
- **Do:** classify transient failures in **both** arms of `sqlx_error_to_status`:
  1. **Non-database arm** (`executor_utils.rs:685-688`, currently `internal_status`):
     `sqlx::Error::PoolTimedOut`, `PoolClosed`, `Io(_)`, and connection-closed
     protocol errors → retryable.
  2. **Database-error arm** (`:582`): connection-loss SQLSTATEs that arrive AS
     database errors and today fall through to `Internal` at `:683` — Postgres
     class `08*` (connection exception: `08000/08003/08006/08001/08004`),
     `57P01` (admin_shutdown), `57P02` (crash_shutdown), `57P03` (cannot_connect_now)
     → retryable. Add these as explicit `match code` arms BEFORE the fall-through.
  Retryable = `retryable_status(context, op, retry_after_ms, msg)` (`Code::Unavailable`,
  `kind = RETRYABLE`, `retryable = true`). Keep genuine logic errors
  (23xxx/22xxx/42xxx already handled) unchanged.
- **Where:** `src/runtime/executor_utils.rs::sqlx_error_to_status` — the DB-error
  `match code.as_ref()` block (585-645, add transient arms) AND the non-DB tail
  (685-688). Add helper `fn is_transient_transport_error(&sqlx::Error) -> bool`.
- **Blast radius:** every auth-path DB read AND every data-plane executor error
  funnels here. Wide but low-risk (only *adds* a retryable classification for
  cases currently mislabeled `Internal`). Update the pinning test
  `sqlx_non_database_error_preserves_internal_code_with_detail` (it currently pins
  `PoolClosed → Internal`) to assert `PoolClosed/PoolTimedOut/Io → Unavailable,
  retryable`; keep a non-transient case asserting `Internal`.
- **DoD:** unit tests: PoolTimedOut/PoolClosed/Io → `Unavailable`+retryable; a
  logic error → `Internal`+non-retryable. Error-detail posture guard green. No
  bare `Status::unavailable` introduced (use `retryable_status`).

### R19-A2 — Preserve the retryable signal through the credential resolvers
- **Achieves:** the enforcement layers can distinguish an infra outage from a
  genuine auth denial on the **bearer** and **cert** channels (whose `Err` is
  *mixed* — it also carries "invalid token"/"not backed by a grant", which must
  stay `Unauthenticated`). Prevents the category-2 bug (retrying a truly-bad token
  forever).
- **Do:** change the resolver outcome type from `Result<_, String>` to carry a
  code, e.g. `Result<_, ResolveError>` where `ResolveError { retryable: bool, msg }`
  (or reuse `tonic::Status` end-to-end). Resolvers set `retryable=true` only when
  the underlying error is transient-transport (A1). `Ok(false)/Ok(None)` denials
  are unchanged.
- **Where:** `src/runtime/credential_layer.rs` (`resolve_credentials` and the
  bearer/api-key/cert arms, ~lines 223–347) and `src/runtime/service/auth_service/grants.rs`
  resolver helpers that stringify infra errors (`validate_service_principal_against_grant`
  ~1742, `resolve_certificate_grant` ~1833) — thread the classified code instead of
  `error.to_string()`. Update the `PreresolvedCredentials` field types in
  `credential_layer.rs`.
- **Blast radius:** the credential-layer ↔ security.rs/method_security.rs contract.
  Contained to the auth plane; every consumer of `PreresolvedCredentials` updates
  its match arms. Requires re-checking the `#[cfg(test)]` fixtures in security.rs +
  method_security.rs that construct `PreresolvedCredentials`.
- **DoD:** resolver returns retryable=true only for transient-transport; existing
  auth unit + live tests green; the mixed-Err bearer path can now branch.

### R19-A3 — Data-plane service-bearer store outage → retryable
- **Achieves:** a healthy service-identity JWT no longer looks revoked during a DB
  blip on the DataBroker hot path.
- **Do:** at the bearer `Some(Err(_))` arm, branch on the A2 retryable flag:
  transient → `retryable_status("authn","service_grant_validate",…)`; genuine
  invalid/not-granted → keep `Status::unauthenticated("invalid bearer token")`.
- **Where:** `src/runtime/security.rs:1623`.
- **Blast radius:** data-plane bearer auth only (plain user JWTs never enter the
  grant-validation branch). Low.
- **DoD:** a unit/integration test: transient resolver Err → `Unavailable`; a
  genuine invalid-bearer Err → `Unauthenticated`. Live suite green.

### R19-A4 — Native control-plane service-bearer store outage → retryable
- **Achieves:** same guarantee on the native listener.
- **Do:** mirror A3 at the native enforcement bearer arm.
- **Where:** `src/runtime/service/method_security.rs:1087-1091`.
- **Blast radius:** native control-plane bearer only. Low.
- **DoD:** unit test transient→Unavailable / invalid→Unauthenticated; live green.

### R19-A5 — Native API-key store outage → retryable (mirror the 0.4.18 fix)
- **Achieves:** an `x-api-key` request during an outage returns retryable
  `Unavailable`, not a misleading missing-bearer `Unauthenticated`.
- **Do:** stop coercing the api-key `Err` to `None` with `.ok()`; on a store Err
  (provably infra-only for this channel) return
  `retryable_status("authn","api_key_validate",…)` before the missing-bearer arm.
- **Where:** `src/runtime/service/method_security.rs:1003-1006` (the `.ok()`),
  guarded before `:1074-1081`.
- **Blast radius:** native api-key path only. Low; api-key `Err` is infra-only
  (all logical denials are `Ok(None)`), so no category-2 risk.
- **DoD:** unit test: api-key resolver Err → `Unavailable`; unknown key `Ok(None)`
  → `Unauthenticated`. Live green.

### R19-A6 — mTLS cert-binding: split outage from deps-absent
- **Achieves:** an mTLS service client hitting a DB blip gets retryable
  `Unavailable`; a genuinely unconfigured/absent-deps deployment stays terminal.
- **Do:** replace the single `certificate_resolution_failed` flag with two signals
  (e.g. `certificate_resolution_unavailable` for DB-outage vs
  `certificate_resolution_unconfigured` for deps-absent). Route the outage flag to
  `retryable_status`; keep deps-absent terminal.
- **Where:** set in `src/runtime/credential_layer.rs:341` (outage) vs `:347`
  (deps-absent); consumed in `src/runtime/security.rs:1440` and
  `src/runtime/service/method_security.rs:1443`.
- **Blast radius:** mTLS path + the `PreresolvedCredentials` flag shape. Contained.
- **DoD:** unit test: DB-outage cert resolution → `Unavailable`; deps-absent →
  terminal. Live mtls test green.

### R19-A7 — Direct authn RPCs return `Unavailable` on outage
- **Achieves:** `Login`/`Authenticate`/`ValidateToken` (and siblings) return a
  retryable `Unavailable` during a Postgres outage instead of `Internal`, so a
  client can tell "retry me" from "server bug". Falls out of A1 for most paths —
  verify each handler's error mapping honours the new classification.
- **Do:** audit the authn RPC handlers' `map_err`/`?` paths; ensure the A1
  classification propagates (they route through `sqlx_error_to_status` /
  `grants_internal_status`). Fix any that hard-wrap to `Internal`.
- **Where:** `src/runtime/service/auth_service/authn/{login,core,mod}.rs`,
  `grants.rs:144-147` (`map_err → sqlx_error_to_status`).
- **Blast radius:** authn RPC surface. Medium; relies on A1.
- **DoD:** an integration test that runs an authn RPC against a torn-down pool and
  asserts `Unavailable`+retryable (env-gated live test). Live green.

### R19-A8 — Runtime PostgreSQL readiness flip
- **Achieves:** the broker marks itself **not-ready** when Postgres becomes
  unreachable at runtime (today it only gates at startup), so orchestrators stop
  routing traffic — the second half of UDB-DB-READINESS-001.
- **Do:** add a lightweight background PG health probe that updates a shared
  readiness state consumed by the health/readiness RPC + listener readiness. On
  loss → not-ready; on recovery → ready. Do NOT tear down the process (avoid
  crash-loops); just report unready.
- **Where:** health/readiness path (`src/runtime/.../health*` / the
  `GetHealthReport` handler) + a probe task spawned in `serve()`
  (`src/runtime/serve` / control-plane lifecycle). Confirm exact anchors in a spike.
- **Blast radius:** serve lifecycle + health reporting. Medium; new background task
  — must be leader-safe and cheap (single pooled ping on an interval).
- **DoD:** an env-gated live test: kill PG → readiness flips to not-ready within N
  seconds → restore PG → ready. No crash-loop.

---

## Thrust B — Codegen: consume the customer's proto

Decisive finding (DX survey): `udb sdk generate` **already** contains the per-entity
`Repository`/`UnitOfWork`/query-builder generator (`@@UDB_ENTITY_BEGIN`), but it is
wired only to UDB's compiled-in `udb_descriptor` and has **no consumer-proto input**.
The load-bearing change is small.

### R19-B1 — `--project-descriptor` / `--project-proto` input path
- **Achieves:** `udb sdk generate` can build its entity/rpc manifest from an
  arbitrary descriptor set (the consumer's), not just the embedded one.
- **Do:** add `--project-descriptor <path.binpb>` (and `--project-proto <dir>` that
  internally runs `buf build` to a temp descriptor) to the SDK selector. Feed the
  bytes into the **existing** `descriptor_contract_manifest_from_bytes`. The
  load-bearing surface is `entity_manifest_from_bytes()` — a consumer proto
  contributes **entities** (messages carrying `pg_table`/`pg_column`), NOT new
  services: `rpc_manifest()` composes the embedded manifest + `native_registry`
  (`sdk_manifest.rs:167-175`), and consumer entities have no native-service entry,
  so the **RPC surface stays UDB's own DataBroker/control-plane RPCs**. The
  generated per-entity repos CALL those existing RPCs (Select/Upsert/Delete + the
  Thrust-C verbs) with the consumer's typed messages. A `--project-only` toggle
  decides whether to emit only consumer entities or compose with the embedded set.
- **Where:** flag: `src/cli/args.rs` (`SdkSelector` / `Command::Sdk`, ~1116-1131);
  load: `src/runtime/descriptor_manifest.rs:412`
  (`descriptor_contract_manifest_from_bytes`); manifest builders:
  `src/runtime/sdk_manifest.rs` (`entity_manifest`/`rpc_manifest`); FSM:
  `src/cli/sdk_gen.rs:56-93`.
- **✅ De-risked (litmus, 2026-07-22):** consumer protos use their OWN annotation
  namespace (`acme.common.v1.table/column/pii`), NOT UDB's. The parser matches
  namespace-agnostically with an empty `ParserConfig.proto_namespace` (the default):
  `option_kind` does `needle("table") || needle("pg_table")` (`options.rs:713`), so
  bare `.table`/`.column` are recognized. Thus B1 parses real consumer protos via
  `parse_proto_source(src, path, &ParserConfig::default())` →
  `CatalogManifest::from_schemas` → entities. (PII/encrypted SCALAR options are
  namespace-pinned to bare-or-`udb.core.common.v1.` at `options.rs:733-762`; a
  consumer's namespaced `pii` needs a parser tweak or the udb namespace — a B3-plus
  refinement, does not block core table/column/entity generation.) The catalog is
  built from `.proto` SOURCE (not descriptor bytes) — so the flag is
  `--project-proto <dir>`, and `entity_manifest()` (which reads only the compiled-in
  `native_manifest()`) gets a sibling `entity_manifest_from_proto_dir`.
- **⚠ Verified caveat (blocks naive reuse):** `build_contract_manifest`
  (`descriptor_manifest.rs:419,425`) hard-filters `if !package.starts_with("udb")
  { continue }`, so a consumer proto (`acme.*`) is dropped. B1 must add a
  package-prefix parameter (or an unfiltered variant) — e.g.
  `build_contract_manifest_filtered(set, accept: impl Fn(&str)->bool)` — and route
  `--project-descriptor` through it with the consumer's package prefix, WITHOUT
  changing the embedded-manifest path (which must keep the `udb` filter). This is
  the real load-bearing change, not just the flag.
- **Blast radius:** SDK codegen only; **zero** runtime/broker impact. Additive flag.
- **DoD:** `udb sdk generate --project-descriptor acme.binpb --lang go --out /tmp`
  emits a client whose entity manifest contains the consumer's message types;
  `sdk list-langs`/existing embedded-only generation unchanged (golden diff clean).

### R19-B2 — Surface per-column proto types + column flags on `EntityDescriptor`
- **Achieves:** entity templates can emit typed structs and typed
  `FromProto/IntoProto` instead of `map[string]any`.
- **Do:** promote per-column info from `ManifestColumn` onto `EntityDescriptor`:
  proto field type, `exclude_from_insert`, `pii`, `encrypted_security`, enum type
  ref, nullability.
- **Where:** `src/runtime/sdk_manifest.rs` (`EntityDescriptor` ~115-141;
  `ManifestColumn` source), and the template context builder in `sdk_gen.rs`.
- **Blast radius:** codegen manifest shape; templates read new fields. No runtime.
- **DoD:** the generated context for an entity exposes typed columns + flags;
  templates compile with them.

### R19-B3 — Emit typed per-entity Repository + marshalling for consumer entities
- **Achieves:** deletes the consumer's Category 1/2/3 boilerplate (~6.5k LOC of
  `*_udb.go` repos + marshalling + duplicated decode/merge/CAS helpers).
- **Do:** run the existing `@@UDB_ENTITY` Repository/UoW/query-builder templates
  over the consumer entities (from B1), using B2's typed columns to generate typed
  `FromProto/IntoProto`, typed accessors, and the merge/CAS helper **once** per
  entity (not copy-pasted). Do this per language (Go first — the driving consumer).
- **Where:** `sdk-templates/<lang>/**` (the `@@UDB_ENTITY_BEGIN` blocks, e.g.
  `sdk-templates/python/udb_client/generated_client.py.tmpl:1230-1622`; add/verify
  the Go template equivalent).
- **Blast radius:** templates only. The generated *output* is what consumers adopt.
- **DoD:** for a sample consumer proto, generated Go compiles and round-trips
  Upsert/Select/Delete against a live broker (env-gated); replaces a hand-written
  `*_udb.go` with zero behavioural diff on a golden entity.
- **Depends on:** B1, B2. RLS-aware injection + CAS depend on C (see B4).

### R19-B4 — Generate against real server capabilities (RLS injection, CAS, ordering, aggregates)
- **Achieves:** generated repositories call **server** verbs (Thrust C) for
  tenant/project injection, merged/CAS update, ordering/ranges, aggregates — so the
  consumer stops emulating SQL in Go (Category 4) and the correctness caveats are
  gone.
- **Do:** extend the entity templates to emit methods that use the new data-plane
  capabilities: `UpdateWhere(expr)`, `Count/Aggregate`, `Select` with
  ORDER BY/LIMIT/range/IS NULL, keyset pagination, and transactional multi-row
  batch. Auto-inject `tenant_field`/`project_field` filters on every op from the
  entity's tenant/project metadata.
- **Where:** `sdk-templates/<lang>/**` entity templates; consumes C1–C5.
- **Blast radius:** templates; correctness now inherited from the server.
- **DoD:** generated repo performs an atomic counter increment via server
  expression-UPDATE (no read-modify-write race), an ordered+paged Select beyond
  500 rows, and a multi-row tx — all in generated code, verified live.
- **Depends on:** C1–C5, B3.

### R19-B5 — Ship the Go `ErrorDetail` decoder (parity with Python/TS)
- **Achieves:** Go consumers read structured `ErrorDetail` (retryable, kind,
  field_violations, retry_after_ms) — removes the fragile substring→code mapper
  (Category 8) and lets generated code branch on retryable (ties to Thrust A).
- **Do:** generate/ship the Go trailer decoder the Python/TS templates already
  emit; expose typed accessors (IsRetryable, FieldViolations, RetryAfter).
- **Where:** `sdk-templates/go/**` (mirror the Python/TS ErrorDetail decode);
  `sdk/go/udbclient`.
- **Blast radius:** Go SDK only.
- **DoD:** Go SDK decodes the `udb-error-detail-bin` trailer; a retryable server
  error is observable as retryable in Go; conformance parity test added.

### R19-B6 — Generate the RequestContext / metadata builder
- **Achieves:** one generated builder fills the typed `RequestContext` AND the gRPC
  headers from a single source — deletes Category 5's duplicated context plumbing.
- **Do:** emit a per-op context helper; rely on the GO-006 fix (shipped 0.4.17 —
  `Context()` merges request-scoped Purpose/CorrelationID/ClientCatalogVersion) so
  there is a single write target.
- **Where:** `sdk-templates/<lang>/**`; SDK client `Context()`.
- **Blast radius:** templates + SDK client. Low.
- **DoD:** two concurrent generated calls carry distinct correlation IDs/purposes
  with no shared-client mutation (the GO-006 regression test extended to generated
  code).

---

## Thrust C — Data-plane capabilities UDB must OWN

**Second-pass reality check (grounded in `proto/udb/entity/v1/relational.proto` +
`src/runtime/service/handlers_data.rs`):** most of the scaffolding the first draft
called "net-new" **already exists** — this thrust is smaller and lower-risk than it
looked:

| Capability | Current state (verified) | Real 0.4.19 gap |
|---|---|---|
| Ordering | `SelectRequest.sort` (field 7, `repeated Sort`) exists | verify executor honors it on `select_inner`/`select_v2` across backends; NULLS ordering |
| Limit / pagination | `SelectRequest.limit` (5), `page_token` (6); `RecordSet.next_page_token` (32), `total_count` (33) exist | executor **cap/stall past ~500 rows** — make keyset walk deterministic (C4) |
| CAS update | `UpsertRequest.expected` (field 9, UDB-GO-005) exists — reject-and-retry | **no atomic server-side arithmetic** → add expression-UPDATE (C1) |
| Multi-row tx | `BeginTx` (stream) + `BatchUpsert` (stream) RPCs exist | **atomicity contract** unclear/awkward → enforce + ergonomic batch (C3) |
| Aggregates | `AnalyticalQuery` RPC exists (OLAP path) | not ergonomic for OLTP entities → expose Count/Sum on the relational path (C5) |
| Filter predicates | `SelectRequest.filter` / `DeleteRequest.filter` are **equality-only `google.protobuf.Struct`** | **THE core gap** → structured non-equality predicates (C2) |

So the load-bearing net-new work is just **C1 (expression-UPDATE)** and **C2
(predicate types)**; C3/C4/C5 are mostly executor-honoring + semantics + wiring on
verbs that already exist. Anchors: handlers `select_inner:85`, `select_v2_inner:151`,
`upsert_inner:293`, `delete_inner:400` in `handlers_data.rs`; IR→SQL compiler in
`crates/udb-portable` (18-backend); typed binds in `src/runtime/postgres_helpers.rs`.
Any item that adds proto fields/RPCs follows the full addition blast radius (buf
stubs, typed clients, native contract/docs, GOLDEN, bench bodies/count pins,
authn/authz inventory, http-api-style, rustfmt) — see
`[[udb-proto-rpc-addition-blast-radius]]`. **Proto changes are additive only**
(new field numbers, never renumber/reuse) so existing SDKs keep working.

### R19-C2 — Non-equality filter predicates (THE Category-4 killer, net-new)
- **Achieves:** removes the bulk of the consumer's Go-side filtering emulation —
  `<,<=,>,>=`, `BETWEEN`, `IN`, `IS NULL`/`IS NOT NULL`, prefix/`LIKE`, and `AND/OR`
  groups — because today `filter` is an equality-only `Struct`.
- **Do:** add a structured predicate to `SelectRequest`/`DeleteRequest` **alongside**
  the existing `filter` (new field number; the `Struct` equality filter keeps
  working). A `Predicate { field, op, values[] }` + `PredicateGroup { AND|OR,
  predicates[] }` tree; compile to parameterised `WHERE` in the IR; tenant/RLS
  scoped; typed binds (mirror `postgres_helpers::bind_one`, never text-cast — see
  `[[udb-pg-bind-text-cast-trap]]`).
- **Where:** `proto/udb/entity/v1/relational.proto` `SelectRequest` (add field 9) +
  `DeleteRequest` (add field 5); IR in `crates/udb-portable`; per-backend emitter;
  `handlers_data.rs::select_inner:85`/`delete_inner:400`.
- **Blast radius:** read+delete path across 18 backends + proto-addition blast
  radius. Medium-large; land PG first behind capability advertisement.
- **DoD:** range/IN/IS-NULL/AND-OR Select returns correct rows on PG + canonical
  stores; equality-`Struct` filter behaviour byte-identical (golden); live
  conformance rows added; capability advertised in `GetCapabilities`.

### R19-C1 — Expression-UPDATE (atomic `SET col = expr`, net-new)
- **Achieves:** kills the lost-update counter race at the source — `counter =
  counter + 1`, conditional SET run in **one server statement**, not
  Select→+1→Upsert and not a CAS retry loop. (Complements, not replaces, the
  existing `expected` CAS: CAS is optimistic reject-and-retry; this is a
  server-side arithmetic mutate.)
- **Do:** add a set-expression form (column ← scalar expression over columns/params:
  `+ - * /`, coalesce, array-append) — either new fields on `UpsertRequest` or a new
  `UpdateWhere` request; compile to `UPDATE ... SET col = <expr> WHERE <predicate>`;
  tenant/RLS scoped; reuse C2's predicate for the WHERE.
- **Where:** `relational.proto` (`UpsertRequest` new fields OR new `UpdateWhere`
  RPC on `data_broker.proto`); IR in `crates/udb-portable`; per-backend emitter;
  `handlers_data.rs::upsert_inner:293` (or a new handler).
- **Blast radius:** data-plane write path across 18 backends + proto/RPC-addition
  blast radius. Large; capability-flagged, PG first. Guard the expression grammar
  (allow-list of ops/functions — no arbitrary SQL injection surface).
- **DoD:** a concurrent atomic increment shows **zero** lost updates (live
  concurrency test on PG); expression grammar is a closed allow-list; CDC/projection
  still fire for expression updates; capability advertised.
- **Depends on:** C2 (WHERE predicate).

### R19-C3 — Atomic multi-row writes (enforce on existing RPCs)
- **Achieves:** the consumer stops per-row loops + partial-failure hazards; a batch
  either all-commits or all-rolls-back.
- **Do:** `BeginTx` (stream Mutation→TxStatus) and `BatchUpsert` (stream) already
  exist — **define and enforce the atomicity contract**: run a `BatchUpsert`/mixed
  batch in a single tx with all-or-nothing semantics, or document `BeginTx` as the
  atomic path and give it an ergonomic non-streaming convenience wrapper. No new
  verb unless the audit shows `BeginTx` can't express it.
- **Where:** `handlers_data.rs` (upsert/batch/tx handling) + `src/runtime` tx
  object (`core/tx_object.rs`); proto only if a convenience RPC is added.
- **Blast radius:** write path + tx semantics. Medium; correctness-sensitive —
  define isolation + partial-failure contract explicitly.
- **DoD:** a fault-injected batch of N writes either all commit or all roll back
  (live test); documented atomicity guarantee.

### R19-C4 — Pagination past the page cap (fix the stall on existing fields)
- **Achieves:** kills the ~500-row page-cap worker stall — large sets walk
  deterministically via cursor.
- **Do:** `limit`/`page_token`/`next_page_token` already exist — make the executor
  honor keyset pagination past the cap: order-key + last-seen cursor, stable under
  concurrent inserts (no dup/skip). Fix the hard cap in `select_inner`/`select_v2`.
- **Where:** `handlers_data.rs::select_inner:85`/`select_v2_inner:151`; IR
  (`ORDER BY key + WHERE key > cursor`) in `crates/udb-portable`.
- **Blast radius:** read path; mostly executor. Medium. Depends on C2 ordering being
  honored.
- **DoD:** a >500-row table fully walked via cursors with no stall, no dup/skip
  under concurrent inserts (live test).

### R19-C5 — Aggregates on the OLTP relational path
- **Achieves:** `COUNT/SUM/MIN/MAX/AVG` (+ optional GROUP BY) in one tenant-scoped
  call for entity tables — removes client full-scan aggregation.
- **Do:** first assess whether `AnalyticalQuery` can serve OLTP entities ergonomically;
  if not (OLAP/ClickHouse-oriented), add an aggregate projection form to the Select
  path (aggregate over the C2 predicate, optional GROUP BY).
- **Where:** `AnalyticalQuery` assessment first; else `relational.proto` Select +
  IR/compiler + `handlers_data.rs`.
- **Blast radius:** read path + possible proto addition. Medium; PG first.
- **DoD:** `Count(predicate)` returns the correct tenant-scoped count in one call;
  live test; capability advertised.
- **Depends on:** C2 (predicate).

---

## Thrust D — Hardening (from the adversarial audit)

> **Implementation findings (2026-07-22):** A7 = **verified, no code** — the authn
> DB path already tags transient store errors (`authn/mod.rs:1462,1521` via
> `sqlx_error_to_tagged_string`) and decodes them (`core.rs:1075/1079/1297` via
> `status_from_store_string`), so A1's classification flows through; the remaining
> `Unauthenticated("invalid credential")` sites are JWT/OIDC crypto + external-IdP
> HTTP (`login.rs`, `mod.rs:2547-2581`), correctly terminal. D2 = **already handled**
> — `create_external_user` maps via `sqlx_error_to_status` (`idp/store.rs:1057`),
> which classifies `23505 → AlreadyExists` (typed, deterministic), so its DoD is met
> with no change. D1 = **deferred pending a live repro** (see below).

### R19-D1 — token_family RLS-GUC fallback — DEFERRED (no confirmed repro)
- **Finding:** `token_families` does have `enable_rls: true` (`token_family.proto:35`),
  but the `mint/rotate/inspect/revoke` raw-SQL paths are **symmetric** (read and
  write run in the same no-GUC context on the same subsystem) and the push-only live
  suite exercises refresh/rotate **green** on 0.4.18 — no zero-row anomaly observed.
  Installing a GUC / adding an explicit tenant filter to working, live-green,
  security-sensitive auth SQL on a *speculative* audit flag risks breaking it for no
  confirmed benefit. Hold until a live reproduction exists (bug-fix doctrine:
  confirm the root cause before touching working code).
- **Achieves (if confirmed):** closes a possible metering-style zero-row under-read on the
  `token_family.rs` raw-SQL *fallback* path (RLS-enabled table, GUC possibly not
  installed).
- **Do:** verify the fallback path; if it reads an RLS table without installing
  `app.current_tenant_id`, either install the GUC in-tx or add an explicit
  `WHERE tenant_id = $1` (defense-in-depth, mirroring the metering fix).
- **Where:** `src/runtime/service/auth_service/authn/token_family.rs` (raw-SQL
  fallback).
- **Blast radius:** token-family reads. Small.
- **DoD:** the fallback either installs the GUC or filters by tenant; a live test
  proves no cross-tenant/zero-row anomaly.

### R19-D2 — idp create_external_user ON CONFLICT target
- **Achieves:** avoids a data-dependent `23505` surfacing from the partial-unique
  **email** index when the upsert only handles the username conflict.
- **Do:** handle both unique constraints (username + partial-unique email) or scope
  the conflict target correctly.
- **Where:** `src/runtime/service/auth_service/idp/store.rs::create_external_user`.
- **Blast radius:** IdP external-user upsert. Small.
- **DoD:** an email-collision upsert returns a typed, deterministic error (not a
  raw 23505).

### R19-D4 — Release atomicity: no *visible* release until all assets land
- **Context (verified on v0.4.18):** the release already publishes all 5 platform
  binaries + per-file `.sha256` **and** `manifest.json` + `manifest.json.sha256`.
  UDB-REL-003 ("no broker/manifest assets") was a **timing artifact** — the consumer
  polled the Releases API during the ~40-min binary matrix and saw a partial set.
  So the manifest half is DONE; the only real gap is the *visibility window*.
- **Achieves:** a consumer's Releases-API poll can never observe a UDB release with a
  partial asset set — it's either absent or complete.
- **Do:** make the GitHub Release atomic w.r.t. assets: create it as a **draft** (or
  upload all assets to a staging area) and flip it to **published/latest** only
  after the full binary matrix + manifest step succeed. Do not mark the release
  visible/latest while `build (udb-*)` jobs are still running.
- **Where:** `.github/workflows/release.yml` — the Release-creation vs asset-upload
  vs publish ordering (draft → upload matrix outputs + manifest → publish). Validate
  locally with actionlint (never run git — maintainer owns CI/commit).
- **Blast radius:** release workflow only; no runtime/source. Preserve release
  integrity: reorder gating ONLY — NEVER move a tag or swap an existing asset.
- **DoD:** on the next release, the Releases API returns the release only once all 5
  platform binaries + `.sha256` + `manifest.json` + `manifest.json.sha256` are
  attached; no partial-asset window; actionlint clean.

### R19-D3 — (watch, not fix) low-severity notes
- Global/empty-tenant ("admin") API-key metadata readable by any authenticated
  caller (RPC already requires `udb:admin`, `key_hash` redacted) —
  `apikey.rs::enforce_caller_tenant`. Decide whether to tighten to admin-only read.
- 5s `validate_bearer_token_cached` TTL can honor a just-revoked token briefly —
  documented/bounded; leave unless a consumer needs sub-second revocation.

---

## 6. Global Definition of Done (release gate)

1. **Broker boots:** `embedded_native_manifest_passes_startup_manifest_validation`
   green; `udb serve` starts against an empty PostGIS DB (add a CI serve-boot smoke
   if not already covered).
2. **Full CI green on `main`**, including the push-only live Postgres/canonical-store
   suite (auth grant/binding/api-key/mTLS lifecycles + the new C-capability live
   rows).
3. **Reliability:** an env-gated outage integration test proves every credential
   channel (A3–A7) and the direct authn RPCs return retryable `Unavailable` on a
   torn-down pool, and readiness flips (A8) — while genuine bad credentials stay
   `Unauthenticated` (no category-2 regression).
4. **Codegen:** `udb sdk generate --project-descriptor <consumer.binpb>` emits a
   compiling, live-round-tripping typed client for a sample consumer proto that
   replaces a hand-written repository with no behavioural diff; Go `ErrorDetail`
   decode + conformance parity.
5. **Capabilities:** C1–C5 advertised in `GetCapabilities`, live-verified on PG +
   canonical stores, with the correctness caveats (counter race, page stall)
   demonstrably gone.
6. **Release mechanics:** version → `0.4.19` via `versions.json` +
   `check-versions.mjs --fix`; artifacts regenerated (native-contract, swagger,
   typed clients, bench docs, inventory, codebase-map, contract-baseline);
   NATIVE_CONTRACT_VERSION bumped + baseline regenerated (C adds RPCs → contract
   changes); tag `v0.4.19` + `sdk/go/v0.4.19` on the green `main` HEAD; Release
   workflow publishes all registries.

## 7. Suggested cut / sequencing

- **0.4.19 (must):** Thrust A (A1–A8, incl. the readiness flip) + D1/D2/D4 +
  B1/B2/B3/B5. Ships trustworthy failure semantics AND the first real
  "point-your-proto → typed client" (typed CRUD/marshalling/enum/error-decode) —
  the biggest DX win, no data-plane risk.
- **0.4.20 (large):** Thrust C + B4/B6. C2 (predicate) and C1 (expression-UPDATE)
  are the net-new multi-backend work; C3/C4/C5 are executor-honoring + semantics on
  verbs that already exist, so they can land incrementally. Landing C after the
  codegen input path (B1-B3) means B4 generates against each capability as it lands.
  Phase behind capability flags, PG-first, then canonical stores.

Rationale: A + B1–B3/B5 are surgical and independently valuable; C is a genuine
feature program. Splitting keeps 0.4.19 shippable while committing to the full
vision.

---

## 8. `PreresolvedCredentials` ripple (Thrust A implementation contract)

A2/A6 change the shape of `PreresolvedCredentials` (`credential_layer.rs:101-127`).
Every consumer of it must be updated in the **same** change or the build breaks —
enumerate before editing (this is the mechanical part per
`[[udb-plan-must-be-execution-grounded]]`):

- **Producer:** `CredentialResolveLayer` / `resolve_credentials` in
  `credential_layer.rs` (sets `bearer`, `api_key`, `certificate_*`, and the new
  `certificate_resolution_unavailable` vs `_unconfigured`).
- **Consumers (sync enforcement):** `security_from_request` in
  `src/runtime/security.rs` (bearer arm ~1623, cert arm ~1440) and
  `method_security.rs` (bearer ~1087, api-key ~1003/1074, cert ~1443).
- **Field-type change:** if A2 switches `bearer`/`api_key` from `Result<_, String>`
  to a classified error type, update the struct, both resolvers, ALL match arms
  above, and the `#[cfg(test)]` fixtures that construct `PreresolvedCredentials`
  (grep the test modules in security.rs + method_security.rs).
- **Invariant to preserve:** the bearer channel's `Err` is **mixed** (infra outage
  AND genuine invalid-token) — only the infra-classified case becomes `Unavailable`;
  invalid-token stays `Unauthenticated`. The api-key channel's `Err` is infra-only
  (`Ok(None)` = clean miss), so it may map `Err → Unavailable` unconditionally.

## 9. Per-change-class CI-gate checklist (run locally before pushing)

Linux-only gates a green Windows build misses (`[[udb-linux-only-ci-gates]]`):

- **Any `src/runtime/service/**` change (all of Thrust A, C):**
  `python scripts/check-error-detail-posture.py` (pins per-service operation-string
  tokens — A's `retryable_status` ops must be registered) AND
  `python scripts/generate-codebase-map.py --check` (any new pub symbol).
- **Posture guard:** no bare `Status::unavailable`/`Status::internal` — use
  `retryable_status(...)` / `internal_status(...)`.
- **Proto/RPC additions (C2 predicate, C1/C5 if new RPC):** the full
  `[[udb-proto-rpc-addition-blast-radius]]` sweep — buf stubs regen, typed clients,
  native contract + docs, GOLDEN service-set snapshot, bench bodies/skeleton/count
  pins, authn/authz inventory, http-api-style exception report, rustfmt. Bump
  `NATIVE_CONTRACT_VERSION` (`descriptor_diff.rs`) + regenerate the contract baseline.
- **Version bump (`[[udb-version-bump-flow]]`):** edit `versions.json` →
  `node scripts/check-versions.mjs --fix` (propagates to Cargo.toml + all SDKs +
  swagger + native-contract udb_version). NEVER hand-edit Cargo.toml alone.
- **`.github/` changes (D4):** actionlint + shellcheck to 0 findings; never run git
  (`[[udb-ci-local-validation]]` — maintainer owns commit/CI/branch-protection).
- **Cargo discipline (`[[udb-cargo-single-run-discipline]]`):** ONE cargo at a time,
  never concurrent, never kill a healthy build. All code edits first, then one
  `cargo check`, one `cargo test` at the end.
- **Consumer-name hygiene:** no consumer/customer project names in
  source/tests/protos/bench-docs — neutral `acme.*` / Vehicle / Invoice only.

## 10. Test matrix

| Item | Unit | Live (push-only) | Notes |
|---|---|---|---|
| A1 | classify PoolClosed/PoolTimedOut/Io + SQLSTATE 08*/57P0x → Unavailable; logic err → Internal | — | rewrite `sqlx_non_database_error_preserves_internal_code_with_detail` |
| A2 | resolver retryable=true only for transient | — | fixtures for mixed-Err bearer |
| A3–A7 | transient→Unavailable / invalid→Unauthenticated per channel | outage vs torn-down pool per channel + authn RPCs | env-gated PG teardown |
| A8 | — | kill PG → readiness not-ready → restore → ready; no crash-loop | leader-safe probe |
| B1–B3,B5 | golden: embedded-only output unchanged | generated Go repo round-trips CRUD; ErrorDetail decode parity | sample `acme` proto |
| C1 | expression grammar allow-list | concurrent atomic increment: zero lost updates | CDC still fires |
| C2 | predicate→WHERE compile per backend | range/IN/IS-NULL/AND-OR correctness; equality-Struct byte-identical | conformance rows |
| C3 | — | fault-injected batch all-or-nothing | isolation documented |
| C4 | — | >500-row walk, no dup/skip under concurrent insert | keyset stable |
| C5 | — | Count(predicate) correct + tenant-scoped | assess AnalyticalQuery first |
| D1 | GUC-installed or tenant-filtered | no cross-tenant/zero-row anomaly | fallback path |
| D2 | email-collision → typed error | — | ON CONFLICT target |
| D4 | actionlint clean | next release: no partial asset set; manifest+sha present | integrity preserved |

## 11. Open spikes (resolve before coding the affected item)

1. **A8 anchors:** exact `GetHealthReport` handler + where `serve()` spawns
   background tasks / holds the readiness cell (not yet pinned).
2. **C5:** does `AnalyticalQuery` already serve OLTP-entity aggregates ergonomically?
   Read its handler + proto before deciding net-new vs reuse.
3. **C3:** can `BeginTx` already express an all-or-nothing mixed batch, or is a
   convenience RPC warranted? Read `core/tx_object.rs` + the BeginTx handler.
4. **C1:** new fields on `UpsertRequest` vs a dedicated `UpdateWhere` RPC — decide by
   how CDC/projection/audit hook the write (they key off the upsert path today).
5. **B2:** confirm `ManifestColumn` exposes the proto field type + `exclude_from_insert`
   in a form the template context can consume (fields observed: `encrypted`,
   `security`, `searchable_encrypted`, `sql_type`; verify proto-type presence).

## 12. Verification status of the anchors in this plan

Confirmed by direct read this pass: `sqlx_error_to_status` structure + the
`PoolClosed` pinning test (`executor_utils.rs:581,2950`); `PreresolvedCredentials`
shape + `certificate_resolution_failed` single-flag (`credential_layer.rs:101-127,
341,347`); `descriptor_contract_manifest_from_bytes` (`descriptor_manifest.rs:412`);
`EntityDescriptor`/`rpc_manifest`/`entity_manifest` (`sdk_manifest.rs:116,167,336`);
`SelectRequest`/`UpsertRequest`/`DeleteRequest` fields incl. existing
`sort`/`limit`/`page_token`/`expected` and the equality-only `Struct` filter
(`relational.proto:36-73`); data-plane handlers
(`handlers_data.rs:85,151,293,400`). Still marked spike (§11): A8 readiness anchors,
AnalyticalQuery reuse, BeginTx atomicity, C1 write-path shape, ManifestColumn
proto-type field.
