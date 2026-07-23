# UDB 0.4.20 + roadmap — 0.4.20 bug-fix release, then a multi-release program

> **SCOPE, IN ONE SENTENCE (read this before anything else): 0.4.20 ships
> Block 0 ONLY — Z-1, Z-2, Z-3, B-1, plus the already-landed Z-10. Everything
> else in this document is 0.4.21+.**
>
> Z-7 (`PurgeTenant` fail-closed) and Z-8 (provisioning deadlock) are S1/S2
> security items and are **explicitly deferred to 0.4.21**, accepting the
> documented risk: both have consumer-side mitigations in place (Z-7 — the
> identity was suspended via `ChangeUserStatus`; Z-8 — provisioning is blocked
> fail-closed, not bypassed). Neither is a data-loss or silent-corruption class,
> which is the bar Block 0 items had to clear. If that risk is not acceptable,
> promote them into a 0.4.20.1 hotfix rather than widening 0.4.20.
>
> This document is a **program**, not a release: ~62 discrete items across 11
> blocks. Earlier revisions presented the whole as "the 0.4.20 plan," which is
> the same over-promise pattern the plan diagnoses in UDB itself.

## Evidence bar — read before trusting any severity in this document

Findings carry one of two labels. **Assigning S1 to an unreproduced claim is not
permitted.**

- **[VERIFIED]** — read directly against the tree during this work, anchor
  points at the exact code the claim rests on.
- **[HYPOTHESIS]** — sourced from a subagent audit with anchors, consistent with
  the code but **not independently re-derived**. Plausible, unproven, and to be
  reproduced before its severity is accepted or work is scheduled against it.

Applying that bar honestly to this document's own registers.

**[VERIFIED — read directly, by me]**

| ID | What I read | Result |
|---|---|---|
| F-1 | `build_audit_event` callers; `Phase::EmitAudit`; `security.rs:430` gate | **CONFIRMED** — dead path behind a live production gate |
| F-2 | `idempotency_claim_sql:2756-2776` + `:2731-2735` | **CONFIRMED** — one statement ⇒ one READ COMMITTED snapshot; T2's `ON CONFLICT DO NOTHING` unblocks after T1 commits but the `UNION ALL` read still runs on the pre-commit snapshot ⇒ zero rows ⇒ `fetch_optional` `None` ⇒ **INTERNAL, non-retryable** |
| X-1 | `executor_utils::cache_key:1362-1385` signature | **CONFIRMED** — no `limit`, no `sort` params |
| X-10 | `build_join_fusion_sql:113-240` | **CONFIRMED** — only `LIMIT`; `request.sort` never referenced |
| S-1 | `ir/compile/postgres.rs` `context_predicates` occurrences + `compile_update:509-600` | **CONFIRMED** — single occurrence at `:716` (aggregate only); update's `WHERE` is `render_where(&op.filter)` alone |
| R-6 | `handlers_meta.rs:757` → `credential_layer.rs:170` | **CONFIRMED** — auth-plane pool gates `HealthPlane::DataBroker` |
| Z-1/Z-2/Z-7 | consumer reproductions + `tenant_purge.rs:296-321` | **CONFIRMED** |

**[REFUTED — my own hypothesis, wrong]**

| ID | What I read | Result |
|---|---|---|
| X-11 | `build_join_fusion_sql:183-197` | **REFUTED for the relational path.** It iterates **every** joined table and pushes a per-table tenant predicate, hard-errors when a table requires a tenant column and lacks one, and refuses an empty `tenant_id` up front. This *is* traverse-level enforcement. The Ent-analogy leak I inferred does not exist here. The concern survives **only** on the IR/dispatch read path (`compile_read`, no `context_predicates`), which is admin-gated and already tracked as S-2. |

X-11 is the cautionary case: it was **my own reasoning-by-analogy** from the Ent
research, carried into an S1-adjacent security narrative, and it was wrong. Had
it shipped as a finding it would have sent someone hunting a leak that isn't
there — and worse, cast doubt on the findings that are real.

**[VERIFIED — second pass, read directly by me]**

Every remaining register item was then read against the tree. Results:

| ID | Evidence read | Result |
|---|---|---|
| F-3 | `tx_object.rs:261` (`Vec<PendingSagaStep>`), push `:585`, flush `:606/:754/:824/:845/:868` | **CONFIRMED** — buffer is in-memory; the only durable writes are the five later flush points, none at push time |
| F-4 | `engine_tail.rs:1226`, `:1282`, heartbeat `:1297-1300` | **CONFIRMED** — `HOSTNAME` → `"unknown"`; heartbeat `WHERE lock_key=$1 AND holder_host=$2` matches for *both* unset-hostname brokers. No fencing token |
| F-5 | `engine_tail.rs:1468-1471` | **CONFIRMED** — `SELECT … WHERE delivery_state='pending' ORDER BY event_seq ASC LIMIT n`, **no `FOR UPDATE SKIP LOCKED`** |
| F-6 | `engine_tail.rs:2279` (broadcast) vs `~:2306` (journal INSERT), `~:2330` `return` | **CONFIRMED** — broadcast precedes the journal write; INSERT failure returns without acking |
| F-8 | `projection/mod.rs:1353` | **CONFIRMED** — `enabled: false` |
| F-9 | `src/runtime/cdc/` | **CONFIRMED, starker than reported** — **17** uses of `SystemCatalogConfig::default()`, **0** of `::current()` |
| R-1 | `grpc-timeout` grep across `src/` | **CONFIRMED** — only `embedding_service/{handlers,retrieval}.rs`; nothing on the data plane |
| R-2 | `channels.rs:684-700` | **CONFIRMED** — `base` permit is acquired first and **held** while each scoped semaphore is awaited with the same full timeout. No breaker in `channels.rs`/`setup_data.rs` |
| R-3 | `tx_object.rs`, `core/mod.rs` | **CONFIRMED** — zero `SET TRANSACTION`, `isolation_level`, or `SAVEPOINT` |
| R-4 | `core/mod.rs:722-731` | **CONFIRMED** — `TwoPhase` rejects any op that is not `upsert`/`delete` |
| R-5 | `canonical_store/postgres_projection.rs:241-243`, `:252-254` | **CONFIRMED** — `ORDER BY created_at … FOR UPDATE SKIP LOCKED`; disjoint claims, **no per-key ordering**. ⚠ *plan anchor was wrong*: the file is under `canonical_store/`, not `projection/` |
| R-7 | `probe_dispatch.rs:894` | **CONFIRMED** — bare `tonic::Status::new(...)`, no `ErrorDetail` |
| X-2 | `setup_data.rs:583` vs `:596` | **CONFIRMED** — cache-hit `return Ok((…, None))` precedes `enforce_read_fence`, and discards the warning |
| X-3 | planner `broker/mod.rs:458/565/1001` (`normalize_filter_keys`) vs runtime `setup_data.rs:527` (`struct_to_json`) + `:625` (`filter_bind_values(&filter)`) | **CONFIRMED** — planner compiles normalized keys, runtime binds the **raw** filter |
| X-4 | `broker/mod.rs:1534-1537` | **CONFIRMED** — `matches!(… "$and" \| "$or" \| "and" \| "or")` recurses identically, so a tenant predicate inside `$or` satisfies the isolation check at `:615` without isolating |
| S-2 | `executors/postgres.rs:156-176` | **CONFIRMED** — `pg_rows_to_json` has no masking, decryption, or scope reference |
| S-3 | `service/mod.rs:241` | **CONFIRMED** — `$self.authorize(&security, "*", $method)`, literal `"*"` |

**[HYPOTHESIS — the only claim still unproven]** — the *causal* link from X-1 to
the reported >500-row stall. The cache-key omission is verified; that it *caused*
the consumer's stall is not, and needs a live reproduction before being stated as
cause.

**Net after two passes: 26 findings verified against source, 1 refuted (X-11,
mine), 1 anchor corrected (R-5), 1 claim still hypothesis (X-1 causation).**

---

## Execution Plan (revision 4)

Status: REVALIDATED DRAFT · Predecessor `v0.4.19` (`fix/cat003-0418@aa44eb4b`, **not
released**) · Supersedes revisions 1–2.

Evidence base: **eight independent sweeps** — three capability audits, three
adversarial validation passes that attacked this plan's own design, and two
web-research passes on comparable platforms and API standards. Revision 1 had one
item proven unsound; revision 3 records two further corrections to my own prior
claims (§1.3).

---

## 0. Executive summary

UDB's stated claim is a *universal data plane* that reduces consumer code. The
audits show a system whose **server-side construction discipline is genuinely
excellent** — zero bare `tonic::Status` across eight status codes, a typed error
envelope, a proto-driven service registry, real admission control — sitting
behind a **contract that repeatedly promises more than the code delivers**, and a
**consumption layer that drops most of what the server carefully produces**.

Three structural themes, in priority order:

1. **Capability lies.** Fields advertised in proto and ignored by the executor;
   docs asserting behavior the code does not implement. Worst instance: **the
   data-plane audit sink is fully configured, production-gated, and has no
   emitter at all** (§2 F-1). A consumer builds on these and fails in
   production. This class outranks every feature request.
2. **The consumption gap.** The server emits a rich typed `ErrorDetail`; Go
   exposes it as raw bytes with no accessor, PHP may silently drop it, and the
   agent-facing skill documents a Go method that does not exist. Meanwhile
   `udb sdk generate` — the flagship consumer command — **cannot run outside a
   UDB source checkout** because templates are not embedded.
3. **Extensibility is process-boundary, not code-boundary.** That is a coherent
   and defensible architecture (and matches the standing modular-monolith
   decision), but it is undocumented as a contract, and the one place it leaks —
   the notification service's single hardcoded wire shape — forces a bespoke
   sidecar per provider.

Ahead of all three sits **Thrust Z**: field-verified consumer blockers from
`latest_udb_bug.md` and `bug_report_23_7_26_codegen_feedback.md`. Two of them
(**Z-1** proto3 `optional` fails to compile, **Z-2** NULL silently rewritten as
`""` against unique indexes and CHECK constraints) make the 0.4.19 codegen
output unusable or unsafe *right now*, and **Z-0** — 0.4.19 is unpushed — means
no fix reaches a consumer whose policy is official binaries only.

The plan therefore sequences: **unblock the consumer → stop lying → close the
consumption gap → make extensibility a declared contract → then add net-new
verbs.**

A note on that ordering, since it inverts the original brief: the consumer's own
verdict on the codegen shape is *"correct, works live"* — they compiled it, unit
tested it, and round-tripped a real broker row. The bottleneck is not capability
design. It is that the good design is stuck behind two emitter bugs and an
unreleased binary.

### 0.1 The one immediate answer

**The SSL Wireless OTP case needs no plan item and no fork.**
`src/runtime/authn/mod.rs:3859` (`deliver_otp`) already POSTs
`{channel, address, code, otp_type, user_id}` to `UDB_OTP_DELIVERY_WEBHOOK_URL`
with an optional auth header, deliberately gateway-neutral; called from
`auth_service/authn/mfa.rs:141`. Stand up a translating endpoint and forward.
This is precisely Supabase's Send SMS Hook shape (platform generates the OTP,
consumer owns delivery) — see §3.2.

**Caveat that must be decided:** it **fails open** (`:3854-3858`) — delivery
failure never blocks OTP issuance. For a security factor that is a real posture
question, tracked as **E-4**.

---

## 1. Method and self-correction

### 1.1 Passes run

| Pass | Question | Outcome |
|---|---|---|
| Audit ×3 | SDK wiring · data-plane capability · bench surface | capability matrix (§2 A) |
| Validation ×3 | Does the filter rewrite break isolation? Are sibling paths left lying? Does the IR path enforce what CRUD enforces? | killed one item, found X1–X10 |
| Research ×2 | How do comparable platforms let consumers inject logic? What makes a generated SDK pleasant? | §3 architecture, §4 standards |
| Deep pass | Native-service seams · SDK/template/error UX · data-plane non-functionals | F-, S-, T- registers |

### 1.2 The rule this plan enforces

Every item carries an **Edit zone** (all linked files/modules) and a **Wiring &
reuse** clause. The recurring failure mode in this codebase is not bad logic —
it is *a second path that skips the first path's guarantees*
(`PostgresExecutor::mutate` vs `setup_data::upsert` is the canonical example).
Reviewers should reject any diff that reimplements CDC/outbox/projection/
idempotency rather than reusing §5.0's helper set.

### 1.3 Corrections to my own earlier claims — recorded deliberately

- **Revision 1 proposed exposing expression-UPDATE via `GenericDispatch`. Unsound.**
  `compile_update` injects **no** tenant predicate (`ir/compile/postgres.rs`:
  `context_predicates` appears once, at `:716`, inside `compile_aggregate` only),
  and the dispatch writer emits no CDC/outbox/projection/receipt/idempotency/
  encryption/cache-invalidation. Since the broker runs as table owner, RLS would
  not have saved it. Redesigned as **C-1**.
- **I claimed page tokens need not be signed.** Wrong per **AIP-158**: tokens
  **must be opaque and not user-parseable**; base64 is explicitly called out as
  insufficient. Corrected in **P-1**.
- **My own 0.4.19 A8 readiness work probes the wrong pool.**
  `handlers_meta.rs:757` → `credential_layer.rs:170` probes the **auth-plane**
  pool, yet gates `HealthPlane::DataBroker`. The DataBroker `pg_pool`, routed
  project backends, and every non-PG backend are never re-probed. Tracked as
  **R-6**; it is a defect in work I shipped, not an inherited one.

---

## 2. Findings register

Severity: **S1** = silent wrong data / security / compliance · **S2** = advertised
capability absent · **S3** = ergonomic or correctness debt.

### A. Data-plane capability matrix (verified)

| Capability | Verdict | Anchor |
|---|---|---|
| `$eq $ne $gt $gte $lt $lte $like $ilike $in $is_null $not_null`, nested `$and`/`$or` | **supported, correctly parenthesized** | `planning/broker/helpers.rs:4-25`; `broker/mod.rs:1677-1696`, `:1733-1766` |
| `BETWEEN` | missing (use `$gte`+`$lte`) | absent from `helpers.rs:5-23` |
| `NOT` | IR-only, unreachable from wire | `ir/filter.rs:27` vs `broker/mod.rs:1956-1988` |
| `sort` | supported — **except join-fusion** | `broker/mod.rs:646-661` |
| `limit` | supported; silent clamp (**AIP-158-correct**) | `setup_data.rs:512-521` |
| CAS `expected` on Upsert | **real and well-built** (FOR UPDATE, in-tx, decrypts before compare) | `setup_data.rs:854, 1030-1135` |
| CAS on Delete | missing | `relational.proto:68-73` |
| `BeginTx` | real single-PG tx, **non-interactive** | `tx_object.rs:177-182, :231` |
| Expression-UPDATE | missing on CRUD; PG-only in IR, **unsafe to expose as-is** | `ir/compile/postgres.rs:509`; `ir/compile/mod.rs:119-125` |
| Aggregates via Select | missing | no field in `SelectRequest` |

### B. Capability lies — contract (S1/S2)

| ID | Finding | Anchor |
|---|---|---|
| L-1 | `page_token` read nowhere; `next_page_token` never set — **dead wire both ends** | `relational.proto:42`; zero refs in `src/` |
| L-2 | `total_count` = page length, not total | `core/mod.rs:2338` |
| L-3 | `RecordSet.rows` = N **empty** structs on live path, **populated** on cache path — same query differs hit vs miss | `core/mod.rs:2335` vs `executor_utils.rs:1152-1162` |
| L-4 | `BatchUpsert` **not atomic** — per-item txs | `handlers_data.rs:378-387` |
| L-5 | `AnalyticalQuery.parameters` / `page_token` / `dry_run` / `stats` never read; raw SQL passthrough with **no tenant injection** | `handlers_stores.rs:520-565`, `:547` |
| L-6 | `LogicalUpdate.require_affected` declared, never consumed | `ir/operations.rs:106` |
| L-7 | Predicate grammar exists only in Rust; proto declares a bare `Struct` | `relational.proto:39`, `:71` |

### C. Shipped defects — read/cache path (S1)

| ID | Finding | Anchor |
|---|---|---|
| X-1 | `cache_key` ignores `limit` **and** `sort` → `LIMIT 10` and `LIMIT 1000` share one entry for the 300s TTL. Planner memo key *does* include them — the two caches disagree on query identity. **Hypothesis (must reproduce): a contributor to the reported ">500-row stall."** | `executor_utils.rs:1362-1385` vs `broker/mod.rs:368-397` |
| X-2 | Cache hit returns **before** `enforce_read_fence` → skips consistency fence, drops stale-read warning | `setup_data.rs:583` vs `:595-605` |
| X-3 | Planner compiles the **normalized** filter; runtime binds from the **raw** one → wrong value bound to wrong column when a `field_name`/`column_name` alias reorders lexicographically (BTreeMap ordering). Bridge path immune; planner fallback is not | `broker/mod.rs:565` vs `setup_data.rs:524`, `:625` |
| X-4 | Tenant predicate inside `$or` **satisfies** the isolation check without isolating | `broker/mod.rs:1520-1546`, checks `:615`, `:624` |
| X-10 | `select_join_fusion` **discards `sort` entirely** — no `ORDER BY` emitted | `postgres_helpers.rs:235-237` |
| X-11 | **Open question, must verify:** does relation `Include`/edge traversal inject the tenant predicate into *nested* queries? Ent's traverse-vs-intercept split exists precisely because a top-level-only tenant filter **leaks through traversals**. If UDB filters only the root query, this is a cross-tenant read leak | `broker/mod.rs`, join-fusion path — **unverified** |

### D. Latent security holes (S1, currently admin-gated)

| ID | Finding | Anchor |
|---|---|---|
| S-1 | IR `compile_update` injects **no tenant predicate**; only `compile_aggregate` does. Sole barrier is RLS — which fails open because **the broker runs as table owner** | `ir/compile/postgres.rs:716` vs `:509`, `:567-583` |
| S-2 | Dispatch reads bypass PII masking and decryption: `MIN(ssn)` returns plaintext, `SUM` over an encrypted column aggregates ciphertext, `group_by` leaks masked group keys | `executors/postgres.rs:156-175` vs `core/mod.rs:2317-2324` |
| S-3 | `GenericDispatch` authorizes against literal `"*"` → **every per-table ABAC Deny is inert**; no `decision_id` threaded, so audit loses authz linkage | `service/mod.rs:241` vs `handlers_data.rs:417`, `:421` |

### E. Data-plane non-functional gaps (S1/S2)

| ID | Finding | Anchor |
|---|---|---|
| **F-1** | **No data-plane audit exists.** `build_audit_event` has **exactly one caller in the repo: `tests/parser_tests.rs:1041`.** No emitter reads `config.audit_sink.kind`. `Phase::EmitAudit` is a label with no body. Yet production validation **fails** unless `UDB_AUDIT_SINK_URL` is set — enforcing a sink with no consumer. **No mutation is audited on any configuration.** | `planning/broker/mod.rs:1431`; `config/backends.rs:427-500`; `pipeline.rs:61`; `security.rs:430` |
| **F-2** | Idempotency returns **`INTERNAL`, non-retryable** on genuine concurrent duplicates — the exact case it exists for. Under READ COMMITTED, T2's `ON CONFLICT DO NOTHING` unblocks after T1 commits but the `UNION ALL SELECT` still runs on T2's **pre-commit snapshot** → no row → `idempotency_dedup_claim_shape` | `setup_data.rs:2754-2773`, `:2733`; doc claim `:2648-2655` |
| **F-3** | Saga ledger written **after** the crash window it protects. `pending_saga_steps` is an in-memory `Vec` flushed only post-commit. Crash between the S3 PUT and commit → orphaned object/vector writes, **zero ledger rows**, recovery worker finds nothing. Comments at `:433`/`:459` promise durability the code writes later | `tx_object.rs:261`, `:585`, `:754`, `:824` |
| **F-4** | CDC leader lease has **no fencing token**. Holder identity is `HOSTNAME` defaulting to `"unknown"` — two brokers with `HOSTNAME` unset both heartbeat as `"unknown"` and the `WHERE lock_key AND holder_host` heartbeat **succeeds for both** → split-brain, two tailers. Plus ≤10s dual-publisher overlap on steal | `engine_tail.rs:1226`, `:1276`, `:1297-1300`, `:1242` |
| **F-5** | Outbox tail `SELECT` has **no `FOR UPDATE SKIP LOCKED`**; its safety rests entirely on the leader lock that F-4 shows can admit two holders → duplicate publishes. The comment asserting single-poller safety is conditionally false | `engine_tail.rs:1455-1456`, `:1469-1471` |
| **F-6** | **Event loss:** `broadcast_tx` fires **before** the journal INSERT; on INSERT failure the code `return`s without acking. Kafka and live subscribers have the event; the journal — the only replay source — does not. A reconnecting `PublishCDC` subscriber **never sees it** | `engine_tail.rs:2280` vs `:2305-2330`, replay `:1069-1071`, `:1112-1127` |
| **F-7** | Idempotency keys grow **unbounded** — no sweeper, no TTL. The `tenant_created` index exists as if a purge were designed and never written. CDC *does* have one (`engine_tail.rs:989`) | `system.rs:279`, `:1227` |
| **F-8** | `ReconciliationWorker` (DEAD_LETTER repair) defaults **`enabled: false`** while the module doc asserts the repair as unconditional fact. Out of the box an exhausted projection is never repaired | `projection/mod.rs:15` vs `:1353`, `service/mod.rs:2076` |
| **F-9** | ~20 CDC sites use `SystemCatalogConfig::default()` where the write path uses `::current()` → any `system_schema`/table-name override points writer and tailer at **different relations**; CDC silently stops | `engine_dlq.rs:100`; `engine_tail.rs:990`, `:1071`, `:1322`, `:2249` |
| **R-1** | **No deadline propagation** on the data plane — only `embedding_service/retrieval.rs:269` reads `grpc-timeout`. An abandoned client leaves a 30s backend query holding an admission permit | `executor_utils.rs:35-42` |
| **R-2** | **No circuit breaker** in the executor path; base permit is **held while queueing** through four scoped semaphores each with the full timeout → worst-case admission latency **4× documented**, and a dead backend costs `4×queue_timeout + 30s` per request | `channels.rs:684-739` |
| **R-3** | No isolation-level control, no savepoints, no true interactive tx (stream drained before `begin()`). Serializable is unreachable via API | `tx_object.rs:177-182`; no `SET TRANSACTION` in `src/` |
| **R-4** | 2PC refuses `vector_upsert`/`put_object` → **never covers the multi-backend case it exists for**; PG+MySQL only | `core/mod.rs:722-728`; `tx_object.rs:652-694` |
| **R-5** | Projection has **no per-key ordering** — SKIP LOCKED lets worker B apply task N+1 before worker A applies N; last-write-wins is non-deterministic | `postgres_projection.rs:233-254` |
| **R-6** | Readiness refresh probes the **auth-plane** pool but gates `HealthPlane::DataBroker` (**my 0.4.19 defect**) | `handlers_meta.rs:757`; `credential_layer.rs:170` |
| **R-7** | Two residual untyped statuses: `probe_dispatch.rs:894` carries **no `ErrorDetail`**; `tagged_status_to_typed_status` drops `ErrorDetail` for `Aborted`/`ResourceExhausted`/`NotFound`/`PermissionDenied`/`DeadlineExceeded`, contradicting its own comment | `probe_dispatch.rs:894`; `executor_utils.rs:865` vs `:828-831` |

### F. Consumer-facing SDK / docs gaps (S1/S2/S3)

| ID | Finding | Anchor |
|---|---|---|
| **T-1** | **`udb sdk generate` is unusable outside a UDB source checkout.** `--templates` defaults to CWD-relative `"sdk-templates"`; **no embedding** (`include_str!`/`include_dir`/`RustEmbed` count = 0). The flagship consumer command requires cloning the broker repo | `args.rs:1133-1134`; `sdk_gen.rs:856-861` |
| **T-2** | **Go consumers get no typed errors.** `DetailBin []byte` with **no accessor**; `Error()` renders `[+error-detail 42B]`. Consumer must import `entityv1` and `proto.Unmarshal` by hand | `generated_client.go.tmpl:169-175`, `:178`, `:185` |
| **T-3** | **Docs promise an API that does not exist:** the skill states "`Error.Detail()` in Go" — following it is a compile error. "Every SDK decodes it" is false for Go, conditional for PHP and Java | `udb-skill/shared/using-udb.md:245-249` |
| **T-4** | **51 raw `Status::unauthenticated` sites, no typed helper** → the first error every integration hits carries **no `ErrorDetail`**, contradicting the doc's own "don't pattern-match messages" guidance | `security.rs:1280,1289,1427,1456,1560,1650,1672,1687,1779,1831,1844`; `login.rs:160` |
| **T-5** | **No remediation field** in `ErrorDetail` — no `google.rpc.Help` equivalent. Remediation is prose, and the skill's error table is a hand-maintained substitute that will drift | `error.proto:55-81`; `using-udb.md:427-441` |
| **T-6** | No single canonical machine `reason`: the stable token is spread across `kind`/`capability_required`/`policy_decision_id`, and `sanitized_error_detail` blanks two of them per-kind. Violates AIP-193 (`ErrorInfo` mandatory, one `reason`) | `error.proto:32-53`; `executor_utils.rs:234-292` |
| **T-7** | **Transactions absent from every SDK template** (0/6). `UnitOfWork` is hand-written Go only | template matrix, §2 F |
| **T-8** | **Native-service facades exist in zero languages via codegen** — six independent hand-maintained implementations (`project.ts` 67KB, `UdbProject.php` 22KB, `media.go`, …) | `sdk/*` hand-written siblings |
| **T-9** | "Per-language templating" is Rust-side hardcoding: six `{{ENTITY_<LANG>_TYPE}}` placeholders + six relation-accessor emitters as Rust format strings. A 7th language means editing the generator | `sdk_gen.rs:2070-2145`, `:2251-2263` |
| **T-10** | **B3 emitter defects reported by the consumer:** fails to compile on any proto using `optional` (G-1); **silently corrupts NULL semantics by writing `""`** (G-2) | `bug_report_23_7_26_codegen_feedback.md` |
| **T-11** | Typed-entity/ORM feature (`--project-proto`, `udb_entities_gen.go`, `orm scaffold`) is **absent from the agent skill** — zero hits | `udb-skill/shared/using-udb.md` |
| **T-12** | SDK auto-retries `RESOURCE_EXHAUSTED`; **AIP-194 warns against it** (quota/billing). Only `UNAVAILABLE` should auto-retry | `DefaultRetryConfig.RetryOnCodes` |
| **T-13** | Java `fieldViolations()` uses reflection `findFieldByName` and **silently returns empty** if absent; PHP drops the detail entirely behind `property_exists` | `GeneratedClientSupport.java:203-236`; `GeneratedClient.php.tmpl:699-723` |
| **T-14** | 8 of 10 examples never show a transaction; **none** show typed error decoding. ~40MB of committed build artifacts in `examples/` | `examples/` |
| **B-1** | **`benches/hotpath_bench.rs` did not compile at HEAD** — benched `bi::AbacPolicy`/`rebuild_authz_snapshot_from_abac`, an ABAC model replaced wholesale by the v2 engine; **neither symbol exists in `src/`**. The Criterion gate cannot have run since, so no `bench_snapshot.py` number since then is trustworthy. **Fixed this session**; left P-3b coverage debt | `hotpath_bench.rs:262` (HEAD); v2 model `runtime/authz/mod.rs:187`, `:288` |

### G. Extensibility (S2/S3)

| ID | Finding | Anchor |
|---|---|---|
| **E-1** | Notification hardcodes **one wire shape for every provider** — always `POST {to,subject,body}` + bearer. The `provider` field (`"TWILIO"`) is **only a log label**, never matched on. Any differing API needs a bespoke sidecar. *Deliberate* per `delivery.rs:11-15* | `delivery.rs:400-409`, `:343`, `:154` |
| **E-2** | `Backend` trait is well-designed and object-safe, with a test proving an external type can implement it — but `all_plugins()` is a `#[cfg]`-gated static list and **there is no `register_backend()`**. The seam exists; the registry is welded shut | `backend/plugin.rs:369`, `:450`, `:592`; `plugins/mod.rs:64` |
| **E-3** | **No before-hook anywhere.** The tower stack is one closure of concrete in-tree types; no config or API appends a consumer layer. Only attachable behavior is *after* and *out-of-process* (webhook CDC) | `service/mod.rs:2190-2205`, `:2934`, `:3023` |
| **E-4** | OTP delivery **fails open** — delivery failure never blocks issuance | `authn/mod.rs:3854-3858` |
| **E-5** | `crates/udb-wasm` is a **browser `cdylib` for the parser, not a plugin runtime**. No host functions, no guest execution, no sandbox. The name misleads | `crates/udb-wasm` |

---

## 3. Architecture decision — extensibility

### 3.1 Standing constraint

The recorded decision is **"toggleable modular monolith; go out-of-process only
when a concrete driver appears."** UDB already has the foundations: a
descriptor-derived registry (`native_registry.rs:16`, `:163`), per-service config
gates, and zero synchronous inter-service calls. This plan does **not** reopen
that; it makes the existing process-boundary model a *declared contract*.

### 3.2 What comparable systems do

| System | Mechanism | In/Out of process | Failure semantics |
|---|---|---|---|
| Hasura | Actions (sync), Event Triggers (async), Remote Schemas | **Out** | Events at-least-once + retries + `Retry-After`; never blocks the mutation |
| **Supabase** | **Auth Hooks** — one contract, two transports (`pg-functions://` 2s, HTTP 5s) | Both | **Fail closed**; 429/503 retryable ×3; Standard Webhooks HMAC; 20KB cap |
| Appwrite | Compiled-in provider list; Functions as escape hatch | Out | **Anti-pattern**: custom provider ⇒ fork + upstream PR |
| Directus | `filter` (blocking, can modify/reject) vs `action` (fire-and-forget) | In, opt-in sandbox | Capability scopes + egress allowlist, deny-by-default |
| Kong | Lua in-process; external plugin servers (MessagePack-RPC over UDS); WASM | Both | Kong's own engineer: external plugins carry "significant communications overhead" |
| **Envoy** | WASM (in) · `ext_authz` (narrow gRPC, **200ms default**) · `ext_proc` (bidi stream, may mutate headers/body/trailers) | Both | Explicit per-extension `failure_mode_allow` |
| Temporal | Activities as pluggable side effects; interceptors chain | Out | At-least-once ⇒ idempotency is the contract; retry policy declarative |

Two lessons dominate. **Supabase's Send SMS Hook is the exact precedent for the
OTP driver** — and UDB already implements its shape. **Appwrite is the
anti-pattern** UDB must avoid for notification providers (E-1), because a
compiled-in adapter list forces exactly the fork the consumer is trying to dodge.

### 3.3 Decision — the tier model

| Tier | Mechanism | Use for | Verdict |
|---|---|---|---|
| 0 | Rust trait objects, in-tree | first-party providers | exists (`Backend`); keep |
| **1** | **Declarative request/response templates in provider config** | **provider swaps — the 80% case, incl. SSL Wireless SMS** | **ADD (E-1 fix)** |
| **2** | **Out-of-process gRPC hook, `ext_authz`/`ext_proc` shaped** | consumer flow logic: validate, route, reject | **ADD (E-6), design only in 0.4.20** |
| 3 | WASM | hot pure transforms | **defer** — no host exists; `udb-wasm` is unrelated |
| 4 | Async signed webhooks | after-the-fact reactions | exists (webhook service, HMAC) |
| — | Native dylib (`libloading`) | — | **REJECT permanently.** Rust has no stable ABI, and a panic across the C ABI is UB that aborts the process. A consumer's malformed-response panic would take down the broker |

**Non-negotiables adopted** (each borrowed from a system above): name blocking vs
non-blocking in the API itself (Directus `filter`/`action`); per-hook explicit
fail-open/closed defaulting to **closed** for security hooks (Envoy); short
mandatory timeouts enforced broker-side; typed retryable-vs-terminal errors in
the hook response (Supabase); sign every outbound payload (Standard Webhooks);
capability scopes with an **egress allowlist**, deny-by-default (Directus); and
**version the hook contract independently with semver** (Backstage) — while
heeding Grafana's signature-bypass advisory, which arose precisely from coupling
signature verification to version resolution.

---

## 4. Standards adopted as the design contract

Cited so these are conventions we *conform to*, not invent.

- **AIP-158 (pagination)** — `page_size`/`page_token` → `next_page_token`; tokens
  **opaque, URL-safe, not user-parseable** (base64 alone insufficient); oversized
  page size **must coerce, not error** (UDB's clamp is already correct);
  empty `next_page_token` is the **only** end-of-collection signal; **adding
  pagination later is breaking** — so every list RPC carries the fields from day
  one.
- **AIP-160 (filtering)** — a string filter grammar for console/CLI; malformed ⇒
  `INVALID_ARGUMENT`. Note `OR` binds tighter than `AND` there; the typed builder
  must use explicit nesting so precedence never arises.
- **AIP-193 (errors)** — `ErrorInfo` **mandatory on every error**, each detail
  type at most once; `reason` UPPER_SNAKE_CASE, `domain` globally unique; **all
  request-specific info that appears in the message must also be in `metadata`**
  (that is what makes messages safely mutable); permission checked **before**
  existence.
- **AIP-194 (retry)** — auto-retry **`UNAVAILABLE` only**; never auto-retry
  transactional requests; `RESOURCE_EXHAUSTED` generally must not be auto-retried.
- **AIP-134 (update)** — `update_mask` optional; response is the resource; etag
  mismatch ⇒ **`ABORTED`**.
- **`google.rpc` detail payloads** — `ErrorInfo`, `RetryInfo`, `BadRequest.
  FieldViolation{field,description,reason}`, `PreconditionFailure`, `QuotaFailure`,
  `ResourceInfo`, `RequestInfo`, **`Help.links`** (the missing remediation field,
  T-5), `LocalizedMessage`.
- **Relay Cursor Connections** — for any GraphQL/BFF surface.
- **Ent hooks + interceptors** — write-path hooks, read-path interceptors, and
  critically the **`TraverseFunc` vs `InterceptFunc` split**: tenant predicates
  must be injected at **every traversal step**, not just the root query. This is
  the direct model for X-11.

---

## 5. Items

### 5.0 The shared helper set (reuse contract)

Any new write verb MUST reuse, from `setup_data.rs::upsert:763`:
`set_request_local_settings:791` · idempotency claim/replay `:796-836` ·
`normalize_record_keys:840` · CAS `enforce_upsert_precondition:849` ·
`encrypt_record_for_table:851` · projection `enqueue_write_tasks_tx:925` ·
CDC `emit_cdc_outbox_on_mutation:941` · `build_write_receipt:951` ·
idempotency persist `:995` · post-commit `cache_delete_pattern:1013`.

`PostgresExecutor::mutate` (`executors/postgres.rs:201-289`) does **none** of
these — its own header says so. Never route a tenant-facing write through it.

**RecordSet producers — exactly two.** `rows_to_record_set` (`core/mod.rs:2277`)
and `cached_record_set` (`executor_utils.rs:1150`). Every RecordSet-shape change
lands in **both**, or hit and miss disagree. `SelectV2` inherits free
(`executor_utils.rs:1355-1356`); `BatchSelect` inherits per stream item. The
relational read path is **Postgres-only** — keep keyset logic in the planner/IR so
a future backend inherits rather than reimplements.

---

### Thrust F — Integrity (S1; do first, independent of features)

#### F-1 · Implement the data-plane audit emitter — or fail honestly
- **Achieves:** closes the largest compliance lie in the system.
- **Justification:** `build_audit_event`'s only caller is a test. Operators set
  `UDB_AUDIT_SINK=postgres`, pass `validate()`, and get nothing. Production
  validation *enforces* a sink with no consumer.
- **Do:** wire an emitter at the mutation chokepoint consuming
  `config.audit_sink.kind` (stdout/file/kafka/postgres). If it cannot ship,
  **remove the production gate and the config surface** so no operator believes
  they are audited.
- **RESOLVE FIRST — which store is canonical.** Saying "reuse
  `AdminAuditStore`/`MigrationAuditStore`" names *two* patterns for a *third*
  path, which is precisely how a third parallel implementation gets written —
  the §5.0 trap this plan otherwise forbids. **Decision required before any
  code:** pick one as canonical, converge the other onto it, and have the
  data-plane emitter reuse that single store. If they genuinely cannot converge
  (different schemas/retention), say so in writing and document why three audit
  paths is the correct end state — but do not leave the choice to whoever picks
  up the ticket.
- **Also dead, and worse than first stated:** `send_audit_log`
  (`security.rs:430`, the HTTP emitter) has **zero call sites**. So there are two
  independent dead audit paths behind one live production gate.
- **Edit zone:** `src/planning/broker/mod.rs:1431` · `src/planning/pipeline.rs:61`
  · `src/runtime/config/backends.rs:427-500`, `:566-582` ·
  `src/runtime/config/mod.rs:1069`, `:1845`, `:1981` · `src/runtime/security.rs:430`
  · `src/runtime/core/setup_data.rs` (upsert/delete chokepoints) ·
  `src/runtime/core/catalog_admin.rs:2434` (pattern to reuse) · `src/runtime/metrics.rs:803`.
- **DoD:** every data-plane mutation produces an audit record on each configured
  sink; a test asserts non-empty output per `AuditSinkKind`; **or** the config
  surface is gone.

#### F-2 · Idempotency must not return `INTERNAL` on concurrent duplicates
- **Do:** single-statement claim that always returns a row —
  `ON CONFLICT (dedup_key) DO UPDATE SET dedup_key = EXCLUDED.dedup_key
  RETURNING (xmax = 0) AS inserted, response_json` — or re-`SELECT` in a fresh
  statement after the conflict.
- **Edit zone:** `setup_data.rs:2711-2740`, SQL `:2754-2773`, error `:2733`, doc
  `:2648-2655`; callers `:808` (upsert), `:1274` (delete).
- **DoD:** two concurrent identical keyed writes → one applies, the other returns
  the **original response body**, never `INTERNAL`. Concurrency test required.

#### F-3 · Record saga steps *before* the external side effect
- **Edit zone:** `tx_object.rs:261` (buffer), `:426`/`:447` (cap), `:585` (push),
  `:600-606` (compensate), `:754`, `:824`, `:845`, `:868`; comments `:433`, `:459`.
- **Also fix F-3b:** beyond `MAX_COMPENSATIONS_PER_TX` the overflowed object's
  **identity is never recorded** — only a counter. Persist identity or refuse the tx.
- **DoD:** kill -9 between an S3 PUT and commit leaves a recoverable ledger row.

#### F-4/F-5 · Fence the CDC leader lease; make the tail claim safe
- **Do:** per-process UUID + monotonic epoch as holder identity (never
  `HOSTNAME`-or-`"unknown"`); fence the Kafka `transactional_id` on the epoch;
  add `FOR UPDATE SKIP LOCKED` to the tail select so correctness does not rest
  solely on the lease; close the ≤10s dual-publisher overlap.
- **Edit zone:** `cdc/engine_tail.rs:1212-1270`, `:1226`, `:1242`, `:1276`,
  `:1297-1300`, `:1455-1456`, `:1469-1471`; `cdc/kafka_tx.rs`; `cdc/mod.rs:291-294`.
- **DoD:** two brokers with `HOSTNAME` unset cannot both hold the lease (test);
  no duplicate publishes under induced split-brain.

#### F-6 · Journal before broadcast
- **Edit zone:** `engine_tail.rs:2280` (broadcast), `:2305-2330` (INSERT/return),
  replay `:1069-1071`, `:1112-1127`.
- **DoD:** an induced journal-INSERT failure never leaves an event visible to
  live subscribers but absent from replay.

#### F-7 · Idempotency-key retention sweeper
- **Edit zone:** `system.rs:279`, `:1227` (the index that anticipated this);
  mirror `cdc/engine_tail.rs:989` `run_journal_retention_sweep`.

#### F-8 · `ReconciliationWorker` default — enable or correct the doc
- **Edit zone:** `projection/mod.rs:15` (doc), `:650`, `:1353`; `service/mod.rs:2076`.

#### F-9 · One `SystemCatalogConfig` resolution rule
- **Do:** `::current()` everywhere; ban `::default()` in CDC paths with a posture check.
- **Edit zone:** `cdc/engine_dlq.rs:100`; `cdc/engine_tail.rs:990`, `:1071`,
  `:1322`, `:2249` (+ ~15 more); reference `setup_data.rs:800`, `tx_object.rs:634`.

#### X-1…X-4, X-10 · Read/cache-path defects
- **X-1 cache key:** add `limit`, `sort`, cursor. **Trap:** `cache_invalidation_pattern:1387`
  globs positionally — append, or update it in the same change. Mirror
  `select_plan_cache_key` (`broker/mod.rs:368-397`); do not invent a second convention.
  **Edit zone:** `executor_utils.rs:1362-1385`, `:1387`; `setup_data.rs:568-576`.
- **X-2 fence on hit:** reorder. **Edit zone:** `setup_data.rs:577-584`, `:595-605`.
- **X-3 bind normalized:** return the normalized filter on `QueryPlan` or normalize
  before binding — one source of truth. **Edit zone:** `broker/mod.rs:565`;
  `setup_data.rs:524`, `:625`; `postgres_helpers.rs:732-760`.
- **X-4 top-level tenant predicate:** track "appears in a mandatory conjunction",
  not "appears at all"; mirror on the bridged path. **Edit zone:**
  `broker/mod.rs:1520-1546`, `:615`, `:624`, `:459-471`. **May reject requests that
  pass today — see §8 Q2.**
- **X-10 join-fusion sort:** emit `ORDER BY`. **Edit zone:** `postgres_helpers.rs:113-237`;
  `setup_data.rs:687-752`.

#### X-11 · Verify tenant scoping through relation traversal *(spike, blocking P-1)*
- **Justification:** Ent separates traverse- from intercept-hooks precisely
  because a root-only tenant filter leaks through edge traversals. If UDB's
  `Include`/join-fusion nested queries are not tenant-scoped, this is a
  cross-tenant read leak of the same family as S-1/S-2.
- **Do:** determine whether nested relation queries inject the tenant predicate.
  If not, fix at the traversal step, not the root.
- **Edit zone:** `broker/mod.rs` relation planning; `postgres_helpers.rs:113`;
  `setup_data.rs:687` (`select_join_fusion`); `sdk_gen.rs` relation emitters.
- **DoD:** a nested `Include` across tenants returns zero rows **with RLS disabled**.

---

### Thrust P — Pagination and response honesty

#### P-1 · Keyset pagination as a filter rewrite
- **Justification:** validated safe — `filter_columns`, `compile_filter_predicates`,
  `collect_filter_values`, and `logical_filter_from_planner_json` all recurse
  symmetrically through `$and`/`$or`; binds are order-matched; the neutral-IR
  bridge **fails closed** to `plan.sql` rather than diverging.
- **Do:** total order (caller `sort` + PK tiebreakers) → decode token → emit
  lexicographic `$or`/`$and` over `$eq/$gt/$lt` only → combine under `$and` with
  the tenant predicate kept in the top-level conjunction (X-4) → encode last row's
  keys as `next_page_token`.
- **Token:** **opaque, signed/encrypted, not user-parseable (AIP-158)**; bind
  `tenant_id` + `message_type` and validate on decode.
- **Edit zone:** new `src/runtime/core/pagination.rs` · `setup_data.rs:505-550`,
  `:659` · `core/mod.rs:2337-2343` · `executor_utils.rs:1150-1170` (cache twin) ·
  `broker/mod.rs:646-668` · `ir/projection.rs:77-87` (cursor already modeled) ·
  `relational.proto:42` (doc only).
- **Correctness risks:** **NULL cursor values** — the bridge rejects null
  comparisons (`sql_dialect.rs:256-264`) and falls back to `plan.sql`, which emits
  `col < NULL` → UNKNOWN → **zero rows silently**; guard nullable sort keys.
  **Projection omitting a key field** → fail loudly, never return an empty token.
  Depends on X-1 (or pages collide across limits) and X-10 (join-fusion order).
- **DoD:** >2000-row table walks with no dup/skip under concurrent inserts;
  `next_page_token` empty **only** on the last page; cursor present on hit *and*
  miss; non-paginated Selects byte-identical (golden).

#### P-2 · Real `total_count`, populated `rows`
- **Do:** `COUNT(*) OVER ()` as a synthetic column — exact total **before LIMIT**,
  **one** round trip (supersedes revision 1's second query); strip before
  building. Populate `ProtoRow.fields` via the **same** `json_to_prost_value`
  `cached_record_set` already uses.
- **Edit zone:** `broker/mod.rs:631-640` (projection) · `core/mod.rs:2292-2342` ·
  `executor_utils.rs:1002`, `:1150-1170` · `relational.proto:31`, `:33`.
- **Risks:** strip the synthetic column on **both** paths; `SELECT *` vs explicit
  projection differ; **wire-size grows** (`rows` duplicates `records_json`).
- **MEASURED (P-3 bench, 8000-record sample):** populating `rows` is a **~3.3×
  cost** on the row-build path — `records_json` only = **20.2 ms** (207 MiB/s),
  `records_json` + populated `rows` = **67.4 ms** (62 MiB/s). The extra cost is
  the per-row proto field map (`json_to_prost_value` per column). This sharpens
  §8 Q3: on a wide read this triples the conversion cost to duplicate data most
  SDKs ignore in favor of `records_json`. **Recommend populating `rows` only on
  an explicit request flag**, defaulting off — the live/cache disagreement (L-3)
  is then fixed by making the *cache* path also skip `rows` unless the flag is
  set, rather than by paying 3.3× on every read.

#### P-3 · Repair and extend the bench harness
- **B-1 root cause (fixed this session):** `benches/hotpath_bench.rs` did not
  compile at HEAD. It benched `bi::AbacPolicy` / `rebuild_authz_snapshot_from_abac`
  — an ABAC shape replaced wholesale by the v2 engine
  (`runtime::authz::AuthzPolicy` / `Effect` / `AuthzSnapshot`). **Neither the type
  nor the function exists anywhere in `src/`.** Since the Criterion harness could
  not build, the bench-integrity gate cannot have been run for some time, and no
  `bench_snapshot.py` number since then is trustworthy.
- **Done:** dead group removed (not reconstructed — its subject no longer
  exists), `bench_authz_and_scope_maps` renamed `bench_scope_maps`,
  `AUTHZ_POLICY_COUNTS` dropped.
- **P-3b (follow-up, coverage debt this created):** re-add authz coverage over
  the **v2 decision path**. `AuthzSnapshot` exposes **no public method** —
  `effective_roles`/`policy_matches`/`decision_id` are all private — so this needs
  a deliberate `bench_internals` shim over the real per-request decision, sized by
  policy count. Worth doing: `[[udb-perf-review]]` flags per-request authz rebuild
  as a known hot spot, and it is currently unmeasured.
  **Edit zone:** `src/runtime/authz/mod.rs:288-340` · `src/bench_internals.rs` ·
  `benches/hotpath_bench.rs`.
- **Then:** the added `record_set_rows` group measures P-2's per-row cost.
  Honest scope: it excludes decryption, masking and `PgRow`→JSON, because
  `rows_to_record_set` is private and takes `Vec<PgRow>` — exposing it would widen
  visibility purely for benchmarking, which `bench_internals.rs:1-12` warns against.
- **Edit zone:** `benches/hotpath_bench.rs:262` (the break), `:129`, `:391`
  (added group) · `src/bench_internals.rs` · `scripts/bench_snapshot.py:17-20`.
- **DoD:** `cargo bench --features bench-internals --bench hotpath_bench` compiles
  and runs; a before/after snapshot quantifies P-2 and lands in the release notes.

---

### Thrust T — Consumer experience (highest claim-vs-reality gap)

#### T-1 · Embed templates in the binary
- **Justification:** **the single biggest consumer gap.** The flagship command
  requires cloning the broker repo. Precisely: **`sdk-templates/` specifically is
  not embedded** — `sdk_gen.rs` contains zero `include_dir!`/`include_str!`/
  `RustEmbed`. (An earlier revision stated that count as *global*, which is
  false: `native_catalog.rs:24,33,40` already embeds the proto catalog via
  `include_dir!`. Corrected — and it makes this item **easier**, since the
  pattern to copy already exists in-tree.)
- **Do:** embed `sdk-templates/` following `native_catalog.rs:24` verbatim;
  `--templates` overrides. Keep `first_unresolved_template_token`
  (`sdk_gen.rs:1546-1562`).
- **Edit zone:** `src/cli/sdk_gen.rs:856-861`, `:1484` · `src/cli/args.rs:1133-1134`,
  `:1173-1174` · `Cargo.toml` · `sdk-templates/**` · release packaging.
- **DoD:** `udb sdk generate` works from an installed binary in an empty dir.

#### T-2/T-3 · Go typed errors + fix the docs that promise them
- **Do:** decode `ErrorDetail` in the Go template with typed accessors matching
  the documented `Detail()`; expose `field_violations`, `retryable`,
  `retry_after_ms`. Fix the skill's false claims.
- **Edit zone:** `sdk-templates/go/udbclient/generated_client.go.tmpl:169-185` +
  generated twin `sdk/go/udbclient/generated_client.go` (hand-synced; CI must
  confirm byte-equality) · `udb-skill/shared/using-udb.md:245-249`, `:427-441` ·
  `udb-skill/` wrappers + `sync_skills.py`.

#### T-4 · Typed `unauthenticated` helper (51 sites)
- **Justification:** cheapest high-value fix in the audit — the first error every
  integration hits is the only untyped family left in an otherwise disciplined server.
- **Do:** add an `unauthenticated_status(reason, …)` constructor beside the
  existing typed helpers; migrate all sites; distinguish *rotate the key* from
  *back off* (T-4 example 7 conflates three causes).
- **Edit zone:** `executor_utils.rs` (new helper near `:298-520`) ·
  `security.rs:1280,1289,1427,1456,1560,1650,1672,1687,1779,1831,1844` ·
  `auth_service/authn/login.rs:160` · `scripts/check-error-detail-posture.py`
  (add a gate forbidding raw `Status::unauthenticated`).

#### T-5/T-6 · Align `ErrorDetail` with AIP-193
- **Do:** add a single canonical `reason` (UPPER_SNAKE_CASE, closed generated
  enum) + `metadata` map + a **`help`/remediation** field. Everything
  request-specific in the message must also appear in `metadata`.
- **Edit zone:** `proto/udb/entity/v1/error.proto:25-81` ·
  `executor_utils.rs:133-153`, `:234-292` (sanitizer) · all 6 templates ·
  `udb-skill/shared/using-udb.md:427-441` (the hand-maintained table this replaces).
- **Blast radius:** proto-additive; touches every SDK decoder. Coordinate with T-2.

#### T-7/T-8 · Transactions and native facades in the templates
- **Do:** generate a transaction API in **all six** languages (see §4/§5.4 shape),
  and generate native-service facades instead of six hand-maintained copies.
- **Edit zone:** all six `sdk-templates/*` · `sdk_gen.rs` block markers `:1563-1568`,
  placeholders `:1810-1858`, `:2232-2290` · hand-written siblings to retire
  (`sdk/typescript/project.ts`, `sdk/php/src/UdbProject.php`, `sdk/go/udbclient/media.go`, …).
- **Blast radius:** **Large.** Sequence after T-1.

#### T-10 · Fix the shipped B3 emitter defects *(urgent — consumer-blocking)*
- **G-1:** generated Go fails to compile for any proto using `optional`.
  **G-2:** NULL semantics silently corrupted by writing `""`.
- **Edit zone:** `sdk_gen.rs:950` (`render_go_entities_file`), `:1106`
  (`go_to_record_stmt`), `:1128` (`go_from_row_stmt`) ·
  `sdk_manifest.rs:116-157` (needs a presence/`optional` flag) · litmus
  `ambutest/after/udb_entities_gen.go`.
- **DoD:** a proto using `optional` compiles; an absent value round-trips as NULL,
  never `""`.

#### T-12 · Retry policy per AIP-194
- **Do:** auto-retry `UNAVAILABLE` only; drop `RESOURCE_EXHAUSTED`; never
  auto-retry transactional RPCs; ship a `WithTxRetry`-style helper that replays
  the whole block on `ABORTED` so users don't hand-roll it.
- **Edit zone:** all six templates' retry config · `sdk-templates/go/...tmpl:105`
  · TS `:60-62`.

#### T-11/T-14 · Docs, skills, examples aligned to reality
- **Do:** document `--project-proto`/`udb_entities_gen`/`orm scaffold` in the
  skill; add a transactions example and an error-decoding example in **every**
  language; purge ~40MB of committed binaries.
- **Edit zone:** `udb-skill/shared/*.md` + `sync_skills.py` · `docs/orm-scaffold.md`
  · `examples/**` · `.gitignore`.

---

### Thrust E — Extensibility as a declared contract

#### E-1 · Declarative provider request templates *(the notification gap)*
- **Justification:** honors `delivery.rs:11-15` ("provider SDKs stay in
  sidecars") while removing the per-provider sidecar tax. This is Tier 1, and
  Hasura Actions' request/response transformation is the precedent.
- **Do:** extend the provider envelope with request template (method, path,
  headers, body mapping), auth scheme, and success/failure + retryable predicates.
  Validate at config load; add a `dry-run` preview (template errors are otherwise
  runtime-only and would fail every send).
- **Edit zone:** `notification_service/delivery.rs:61` (`NotificationDeliveryProvider`),
  `:154` (parse), `:343` (select), `:400-409` (dispatch), `:292` (outbox), `:305`
  (events) · `webhook_service::resolve_and_validate_target` (SSRF guard — **reuse,
  never reimplement**) · `notification_service/tests.rs:615`.
- **DoD:** a provider whose API is not `{to,subject,body}` works with config only;
  SSRF guard still applies; malformed template fails at load, not at send.

#### E-6 · Flow-hook contract *(design + proto in 0.4.20; implement 0.4.21)*
- **Do:** specify a gRPC `HookService` — `ext_authz`-shaped unary for allow/deny,
  `ext_proc`-shaped for mutation — with `Continue | Mutate | Reject | Respond`;
  per-hook fail-open/closed (**closed by default** for security hooks); mandatory
  short timeouts (Envoy's 200ms anchor); signed payloads; egress allowlist;
  semver'd independently of the platform.
- **Edit zone (design):** new `proto/udb/core/hook/**` · `service/mod.rs:2190-2205`
  (where a hook layer would attach) · `config/mod.rs` (registration) ·
  `native_registry.rs:16`, `:163`.

#### E-2 · Decide the `Backend` registry posture
- **Do:** either add a real `register_backend()` (and version the trait), or
  **document that backends are compile-time and the toy-plugin test is
  aspirational**. The current half-state misleads.
- **Edit zone:** `backend/plugin.rs:369`, `:450`, `:551`, `:592` · `plugins/mod.rs:64`
  · `Cargo.toml` features.

#### E-4 · OTP fail-open posture
- **Decide** whether OTP delivery failure should block issuance (§8 Q5).
- **Edit zone:** `authn/mod.rs:3854-3859` · `auth_service/authn/mfa.rs:141`.

#### E-5 · Rename or document `udb-wasm`
- Clarify it is a parser `cdylib`, not a plugin runtime.

---

### Thrust C — Net-new verbs (REDESIGNED; last)

#### C-0 · Scope + ABAC prerequisite *(blocking C-1/C-2)*
- Narrow scopes (`udb:data:update`, `udb:data:aggregate`) — **do not widen
  `udb:dispatch`**; authorize against the concrete table (`handlers_data.rs:417`
  pattern); thread `decision_id` (`:421`). Fixes S-3 for any reused path.
- **Edit zone:** `handlers_data.rs:453-470`, `:417-421` · `service/mod.rs:234-241`.

#### C-1 · Expression-UPDATE as a first-class CRUD verb
- **Do:** (1) fix **S-1** — AND `context_predicates` into `compile_update` exactly
  as `compile_aggregate` does; (2) add `setup_data::update` as a **sibling of
  `upsert`** reusing §5.0 wholesale; (3) **CDC after-image** — an increment has no
  post-image, so populate `return_fields` (`postgres.rs:585-595`) and feed the
  returned row, or the event is malformed; (4) closed allow-list grammar; block
  `Set`/`Coalesce` on tenant/project/PK system columns; (5) advertise
  **Postgres-only**.
- **Edit zone:** `ir/compile/postgres.rs:509`, `:525-529`, `:567-583`, `:585-595`,
  `:716-722` · `ir/compile/mod.rs:119-125` · new `setup_data::update` ·
  `handlers_data.rs` · `relational.proto` · `backend/mod.rs:591-596` ·
  `handlers_meta.rs:143-160`.
- **DoD:** concurrent increment shows **zero** lost updates; CDC/projection/
  receipt/cache-invalidation all fire and are asserted; **cross-tenant update
  rejected with RLS disabled**.

#### C-2 · Relational aggregates
- **Do:** fix **S-2** first — reject at compile time any aggregate whose `field`
  or `group_by` resolves to a masked or encrypted column (fail closed). Then
  surface `LogicalAggregate` over the §2 A predicate. Add row/timeout limits: an
  unfiltered `COUNT(DISTINCT x)` is a tenant-triggerable table scan and the
  channel breaker throttles concurrency, not per-query cost.
- **Edit zone:** `ir/compile/postgres.rs:651`, `:716` · `ir/operations.rs:239-301`,
  `:242`, `:251` · `core/mod.rs:2317-2324` (masking semantics to mirror) ·
  `handlers_data.rs:664-667`.

#### G-2 · Conditional delete
- **Wiring:** `enforce_upsert_precondition` takes `&UpsertRequest`
  (`setup_data.rs:1030-1037`) — **extract a request-agnostic core**; do not copy.
- **Edit zone:** `relational.proto:68-73` (field 5) · `setup_data.rs:1030-1135`,
  `:849`, `:1274`.

#### B-1 · Per-RPC bench surface *(mandatory if C-1/C-2 add RPCs — CI red until done)*
- Coverage is **forced**: `gen-bench-bodies-json.mjs:141` fails unless rows equal
  `AllRPCs`; `bench_manifest_test.go:126,170,228,231,257`; CI `ci.yml:117-125`.
- **Ordered procedure + every count pin:** regenerate
  `docs/generated/udb-native-contract.json` → `sdk/go/udbclient/generated_client.go:28`
  → `gen-bench-bodies-skeleton.mjs --write` → **hand-fill col5** (strict JSON,
  `<seed:KEY>`, no literal `|`) → `docs/bench-bodies/<service>.md:2|3` →
  `gen-bench-bodies-json.mjs` → `gen-sdk-benchmark-docs.mjs` →
  `check-bench-harness-posture.py:571`, `:592` (`:79` if notification) → add the
  **old** count strings to forbidden lists `:578-585`, `:600-607`. New service ⇒
  the `28 services` figure moves too.

---

### Thrust Z — Consumer-reported bug register (release-blocking)

Source: `latest_udb_bug.md` (consolidated index, 2026-07-23) and
`bug_report_23_7_26_codegen_feedback.md` (0.4.19 codegen adoption pilot —
compiled, unit-tested, and **live round-tripped against the running v0.4.18
broker**). These are field-verified against official checksum-verified binaries;
AmbuLife compiled no UDB source. **This thrust outranks Thrusts P/T/E/C** — every
item is a live consumer blocker.

#### Z-0 · Release 0.4.19 *(G-7 — blocks everything else)*
- **Achieves:** unblocks a consumer whose adoption is fully staged and waiting.
- **Justification:** `latest_udb_bug.md:13` — 0.4.19 is unpushed/unreleased; the
  consumer's post-CAT-003 policy is **official checksum+manifest binaries only,
  no source builds**, so no codegen fix reaches them until a release exists. They
  commit to regenerating, byte-diffing against the vendored trace, and reporting
  divergence same-day — which makes this the fastest available validation loop
  for all of Thrust T.
- **Do:** land Z-1/Z-2 first (they change generated output), then push + release.
  D4 (draft-until-complete asset publishing) is already in place, so the 0.4.18
  asset-less window will not repeat.
- **Edit zone:** `versions.json` + `node scripts/check-versions.mjs --fix` ·
  `.github/workflows/release-binaries.yml` (D4 already landed) · `sdk/go/vX.Y.Z` tag.
- **DoD:** official v0.4.19 (or 0.4.20) release with all assets + `sdk/go` tag
  resolving through the Go proxy.

#### Z-1 · proto3 `optional` presence breaks compilation *(G-1 — BUG, S1)*
- **Justification:** **the generated file does not compile** for any consumer
  proto using `optional`. `user.proto` fields 35/36 map to Go `*string`; the
  emitter assigns plain `string`. This is a hard stop on adoption.
- **Do:** add a presence flag to `EntityColumnDescriptor` (or derive from proto3
  `optional`) and emit the pointer-aware branch:
  write `if m.CreatedBy != nil { r["created_by"] = m.GetCreatedBy() }`;
  read `if s, ok := row["created_by"].(string); ok { m.CreatedBy = &s }`.
  This matches the §4b contract's existing "unset optional (presence) omitted" row.
- **Edit zone:** `src/runtime/sdk_manifest.rs:116-157` (`EntityColumnDescriptor`
  — add presence; populate from `ManifestColumn`) · `src/cli/sdk_gen.rs:1106`
  (`go_to_record_stmt`), `:1128` (`go_from_row_stmt`), `:950`
  (`render_go_entities_file`) · `src/generation/manifest/**` (presence must
  survive parse→manifest) · litmus `ambutest/after/udb_entities_gen.go`.
- **DoD:** a proto using `optional` generates code that `go build`s; unset
  presence is omitted from the record, not written as zero.
- **Hygiene:** the litmus artifact lives **outside this repo** (a sibling
  checkout, not an in-tree path). Nothing from it — no path, no consumer package
  name — may reach `src/`, `tests/`, protos, golden files, or bench docs; those
  use neutral `acme.*` only. The consumer names in *this* document are confined
  to the root integration reports, which is where the rule permits them.

#### Z-2 · NULL silently rewritten as `""` *(G-2 — S1, data corruption)*
- **Justification:** worse than an ergonomic wart. The broker returns SQL NULL as
  an **absent row key**; `ToUDBRecord` re-encodes it as `""`. On the consumer's
  users table, `""` is rejected by format CHECKs and — critically — **unique
  indexes treat two `""` rows as duplicates while two NULLs coexist**. A consumer
  using the generated full-record write corrupts NULL semantics **silently**.
- **Do:** B2 already carries `not_null`. For `not_null == false` string columns
  without presence, **omit the key when the Go value is the zero value** — option
  (a) in the report, which is what a hand-written repository must do anyway
  (their `nullableString`). Document the asymmetry in the generated file header
  regardless.
- **Edit zone:** `sdk_gen.rs:1106` (`go_to_record_stmt`) · `sdk_manifest.rs:145`
  (`not_null`) · generated header text in `render_go_entities_file:977-985`.
- **Correctness risk:** omitting a zero value means a consumer can no longer
  *intentionally* write `""` to a nullable column via the full-record path. Call
  this out in the header and provide an explicit escape (e.g. a `Set` variant) —
  do not leave it ambiguous.
- **DoD:** a NULL column round-trips NULL→absent→NULL; a live unique-index test
  with two NULL rows coexists.

#### Z-3 · Timestamp wire-form canonicalization *(G-3)*
- **Justification:** the broker returns `2026-07-21T17:09:46.790197+00:00`
  (offset form); the generated `udbAsTime` parses only RFC3339Nano. Consumers
  needed a six-layout ladder for DATE columns and other renderings. The report's
  own recommendation — and mine — is to **canonicalize broker-side** (always
  RFC3339Nano UTC `Z`) rather than widen every generated parser in six languages.
  The current form also breaks textual CAS comparisons for consumers comparing
  timestamp strings rather than instants.
- **Do:** canonicalize the broker's JSON wire form and **document it as a
  contract**; widen the generated parser to the ladder as a transitional safety net.
- **Edit zone:** `src/runtime/core/mod.rs:2307-2325` (`row_value_to_json` — the
  emission point) · `postgres_helpers.rs` (type rendering) · `sdk_gen.rs`
  `go_coercion_helpers` (`udbAsTime`) · all six templates · native contract docs.
- **Blast radius:** **consumer-visible wire change** — any consumer parsing the
  offset form keeps working (RFC3339Nano is a superset for readers), but string
  comparisons change. Flag in release notes.
- **DoD:** every timestamp/date column emits one canonical form; a live test
  across DATE and TIMESTAMP columns asserts it.

#### Z-4 · Broker-side blind-index derivation *(G-4 — feature, high leverage)*
- **Justification:** AEAD ciphertext is randomized, so equality lookup on an
  encrypted column is impossible. Consumers hand-derive keyed-HMAC `*_idx`
  columns **and must remember never to filter on the plaintext column — a filter
  on `mobile_number` silently matches nothing** (live-verified). That silent
  mismatch is a foot-gun of exactly the class this plan exists to remove. The
  protos already declare everything needed (`pii`, `encrypted_security`, `*_idx`).
- **Do:** tenant-scoped keyed HMAC on write + **transparent rewrite of equality
  filters to the index column**. Until then, at minimum: emit a warning comment on
  encrypted columns' marshalling lines, and **reject (not silently drop) an
  equality filter on an encrypted plaintext column**.
- **Edit zone:** `src/runtime/core/setup_data.rs:851` (`encrypt_record_for_table`)
  · `core/mod.rs:2317-2321` (decrypt path) · `planning/broker/mod.rs` filter
  compilation (the rewrite) · `sdk_manifest.rs:154-156` (`is_blind_index`,
  `is_pii`) · `sdk_gen.rs` emitters · `generation/manifest/**`.
- **Blast radius:** Large — crypto + filter compilation. Stage: fail-closed
  rejection first (small, immediate foot-gun removal), derivation second.
- **DoD:** an equality filter on an encrypted column either transparently matches
  via the blind index **or** is refused with a typed error naming the index
  column — never silently returns zero rows.

#### Z-5 · Emit merge/CAS helpers once; SDK owns RecordSet decode *(G-5)*
- **Justification:** `mergeUDBRecord` / `expectedUDBValues` / `isUDBCASConflict`
  are copy-pasted across 4+ consumer packages and are entity-independent. So is
  `decodeUDBRecordSet` (RecordSet JSON/struct rows → `[]map[string]any`), which
  consumers hand-write whenever they drop to raw `Data.Select`.
- **Do:** move the merge/CAS helpers into the Go SDK `udbclient` (entity-independent
  — better than per-package emission), and have the SDK own RecordSet decode.
- **Edit zone:** `sdk-templates/go/udbclient/generated_client.go.tmpl` +
  generated twin · `sdk/go/udbclient/` (hand-written helpers) · `sdk_gen.rs`
  (stop-gap per-package emission if SDK placement slips).
- **Note:** this is the same decode that L-3/P-2 changes — sequence **after** P-2
  so the helper is written once against the final RecordSet shape.

#### Z-6 · Typed `Entity` needs paged/sorted/projected Select *(G-6)*
- **Justification:** `udbclient.Entity.Select` takes **only an equality map**, so
  every listing endpoint bypasses the typed handle and hand-builds `structpb`
  filters plus hand decode. The report calls this "the largest remaining
  per-entity boilerplate" once typed repositories exist.
- **Do:** `Entity.SelectPage(ctx, where, SelectOptions{Fields, Sort, Limit,
  PageToken})` returning rows + next token + total — **the consumer's own
  suggested signature**, and it is exactly P-1/P-2's output surface.
- **Edit zone:** `sdk/go/udbclient/entity.go:158,190,209` · all six templates ·
  `sdk_gen.rs` entity block `:2232-2290`.
- **Depends on:** P-1 (cursor), P-2 (total), T-7 (template transactions).
- **⚠ FRAGILITY WARNING — the most exposed item in this plan.** Z-6 is the
  consumer's most-wanted ergonomic deliverable ("the largest remaining
  per-entity boilerplate"), yet it sits at the end of a **four-deep chain**:
  X-11 (unverified tenant-traversal spike) → X-1/X-10 → P-1 → P-2 → Z-6. If
  X-11 proves to be a real cross-tenant leak it becomes an S1 that consumes the
  release, and Z-6 slips indefinitely. **Do not commit Z-6 to the consumer as a
  dated deliverable.** If it must be de-risked, the fallback is to ship
  `SelectPage` over `limit`+`sort` only (both already work) and add the cursor
  once P-1 lands — a smaller promise that does not depend on the spike.

#### Z-7 · `PurgeTenant` reports success while the principal can still log in *(UDB-TENANT-011, S1 security)*
- **Justification:** field-verified: purge returned `tenant_denylisted=true`,
  `total_deleted=0`, `principals_denylisted=0`, and the bootstrap user
  **authenticated again immediately**. Only a separate `ChangeUserStatus(SUSPENDED)`
  stopped it. An operator believes a GDPR/right-to-be-forgotten purge revoked the
  tenant while its owner still mints sessions.
- **Root cause (grounded):** the purge deletes **data-plane** rows scoped by
  `tenant_column` and then denylists **existing tokens** via a cutoff at
  `now_unix` (`tenant_purge.rs:296-321`). It never deactivates **control-plane
  identity records**, so a fresh password login mints a token *after* the cutoff
  and passes. `principal_ids` being empty also yields `principals_denylisted=0` —
  the control-plane principals were never enumerated.
- **Do:** enumerate and deactivate/delete control-plane identities (users, API
  keys, grants, sessions) for the tenant inside the purge transaction; make the
  response counts reflect reality; and **fail closed** — if control-plane identity
  data cannot be purged, the RPC must error rather than return success.
- **Edit zone:** `src/runtime/core/tenant_purge.rs:233` (`purge_tenant`), `:280-290`
  (tx commit), `:296-321` (denylist), `:87-89` (response counts) ·
  `src/runtime/service/tenant_service/handlers.rs:160` ·
  `src/runtime/service/auth_service/apikey.rs` (key revocation) ·
  `auth_service/authn/core.rs` (user status) · `authn/mod.rs` (principal stores).
- **DoD:** after a successful purge, password login for every purged principal is
  **rejected**; counts are non-zero and accurate; a purge that cannot remove
  identity data returns an error, never success.

#### Z-8 · Bootstrap admin cannot create service accounts *(UDB-AUTH-013, S2 provisioning deadlock)*
- **Justification:** method-level authz **allows** `AuthnService/CreateUser`, then
  the nested admin-mutation decision **denies** `authn.user.create` on
  `authn.CreateUser` with "denied by Casbin PERM model". The two layers disagree
  for a legitimately bootstrapped organization owner, so no exact-identity
  service-account key can be issued. The consumer's only alternatives would be
  reusing another service's key, misrepresenting `service_identity`, or weakening
  authz — all of which violate the intended model, so they are correctly blocked.
- **Do:** make the bootstrap owner's grant carry `authn.user.create`, **or**
  provide an official offline `udb auth bootstrap service-account` path. Whichever
  is chosen, the **two authorization layers must agree** — a method-level allow
  followed by a nested deny is the actual defect.
- **Edit zone:** `auth_service/authn/core.rs:400`, `:420`, `:453`, `:479`, `:932-940`
  (the nested decision) · `src/runtime/authz/casbin_engine.rs:349` (denial text) ·
  `service/method_security.rs:829` (outer decision) · default policy seed /
  `udb auth bootstrap` CLI.
- **Related:** `udb auth migrate-grants` matched **zero** CreateUser-provisioned
  legacy accounts on a real deployment — either broaden its selection or document
  that API-created accounts need direct grant creation.
- **DoD:** a bootstrapped owner creates a `SERVICE_ACCOUNT` with an exact
  immutable `service_identity` through official APIs; outer and nested decisions
  agree; a negative test pins that a non-owner still cannot.

#### Z-9 · Live DB-loss drill *(UDB-DB-READINESS-001 — verification, not code)*
- The fix shipped in 0.4.18/0.4.19; **the live drill is still pending**, and
  **R-6 shows the readiness refresh probes the wrong pool**. Run the drill only
  after R-6 lands, or it will pass for the wrong reason.
- **Edit zone:** verification only — plus R-6 (`handlers_meta.rs:757`,
  `credential_layer.rs:170`).

#### Z-10 · Already fixed this session, pending release
- **UDB-AUTH-010** (ListApiKeys returned the key prefix as `name`, breaking
  reconcile-by-name provisioning → create/reconcile deadlock) — fixed at
  `apikey.rs:722`, +2 tests. **Not yet `cargo check`-verified.**
- **UDB-GO-012** (request-scoped metadata dropped on native calls) — fixed via
  `MergeRequestScopedAudit`; Go suite green.

Both ship with **Z-0**.

---

## 6. Dependency graph

```
F-1 audit ─┐ (independent, S1)
F-2 idem   ├─ Thrust F: no feature depends on these; they gate RELEASE
F-3..F-9   ┘
X-1 cachekey ──> P-1 ──> P-2 ──> T-7 (typed tx in SDK) ──> G1 typed repo
X-10 sort ────> P-1
X-11 traversal spike ──> P-1 (blocking: may be a tenant leak)
B-1 bench repair ──> P-3
T-1 embed templates ──> T-2/T-7/T-8/T-10 (all codegen work)
T-5/T-6 error contract ──> T-2 (Go decode) ──> docs T-3/T-11
C-0 scopes+ABAC ──> C-1, C-2
S-1 ──> C-1        S-2 ──> C-2
C-1/C-2 ──> B-1 (bench pins)
E-1 provider templates (independent)   E-6 design (independent)

── Thrust Z (consumer-blocking, highest priority) ──────────────────────────
Z-1 optional ─┬─> Z-0 RELEASE ──> consumer byte-diff validation loop
Z-2 NULL      ┘        (all later codegen work validates through this loop)
Z-7 purge fail-closed ── independent, S1 security
Z-8 provisioning deadlock ── independent, S2
Z-3 timestamp canonicalization ──> P-2 (same emission point, core/mod.rs:2307)
Z-4 blind index ── stage A fail-closed (independent) ─> stage B derivation (late)
P-1 + P-2 ──> Z-6 Entity.SelectPage ──> Z-5 merge/CAS helpers
R-6 ──> Z-9 live DB-loss drill   (drill before R-6 passes for the wrong reason)
```

**Critical path:** X-1 → P-1 → P-2 → T-7/G1. **Longest pole:** C-1. **Release
gates:** all of Thrust F.

---

## 7. Sequencing

| Block | Items | Cargo | Rationale |
|---|---|---|---|
| **0** ✅ | **Z-1, Z-2, Z-3, B-1** (+ Z-10) | 1 | **0.4.20 bug-fix release.** Z-1 stopped compilation outright; Z-2 corrupted NULL silently; B-1 meant the bench harness itself did not build |
| **0b** | **Z-0 release 0.4.20** | — | Unblocks a staged adoption **and** buys a same-day byte-diff validation loop for all later codegen work |
| 1 | X-11 spike, B-1 bench repair | 1 | Both may change scope; cheap. X-11 may be a tenant leak |
| 2 | Z-7, Z-8, F-1, F-2, F-9, R-6, R-7 | 1 | Security + compliance: purge fail-closed, provisioning deadlock, audit emitter, idempotency race |
| 3 | F-3, F-4, F-5, F-6, F-7, F-8 | 1 | Event-system integrity as one coherent change |
| 4 | X-1, X-2, X-3, X-4, X-10 | 1 | Read/cache defects |
| 5 | P-1, P-2, P-3, Z-3 | 1 | Pagination + response honesty + wire canonicalization + measurement |
| 6 | T-1, T-4, Z-4 (fail-closed stage) | 1 | Template embedding; typed auth errors; blind-index foot-gun removal |
| 7 | T-5, T-6, T-2, T-3, T-11, T-12 | 1 | Error contract, then every decoder + docs |
| 8 | E-1, E-2, E-4, E-5, E-6 (design) | 1 | Extensibility as a declared contract |
| 9 | T-7, T-8, Z-5, Z-6, G1 typed repo, D1 | 1–2 | Template enrichment; Z-5/Z-6 depend on P-1/P-2 |
| 10 | C-0, C-1, C-2, G-2, Z-4 (derivation), B-1 | 2+ | Net-new verbs; riskiest, last |

**Release split (maintainer decision, 2026-07-23):**
- **0.4.20 = BUG-FIX + quick wins (in progress).** Blocks 0–0b: Z-1, Z-2, Z-3,
  B-1, plus the fixes already landed (Z-10: AUTH-010, GO-012). No 0.4.19 tag was
  ever cut — `versions.json` still read `0.4.18` and neither `v0.4.19` nor
  `sdk/go/v0.4.19` exists locally or on the remote — so 0.4.20 is a clean first
  release of this work. **No tag is moved; release integrity is preserved.**
- **0.4.21 = the upgrades.** Everything else in this plan, blocks 1–10, in the
  order below.

Rationale for shipping 0.4.20 narrow: the consumer is blocked on a binary, their
policy is official checksum-verified artifacts only, and they have committed to
regenerating and byte-diffing same-day. That makes a small, fast release the
highest-leverage move available — it unblocks them *and* buys an external
validation loop for every later codegen change.

Option B (net-new verbs) is **not dropped** — it is sequenced behind the
integrity work. Every block 2–5 item corrects an already-published contract, and
adding verbs on top of an unaudited, event-losing, silently-mis-caching data
plane compounds exactly the problem this plan exists to fix. The consumer's own
report makes the same argument from the other side: they are blocked on a
release, not on new capabilities.

**Environment dependency:** blocks 8–9 need buf regeneration (Docker
`bufbuild/buf:1.65.0`, 13 pinned plugins) and native-contract regeneration via
the `udb` binary; local binary builds are killed by the environment's process
killer, so those steps likely land in CI. Blocks 0–7 are unaffected.

---

## 8. Global DoD and open questions

**DoD**
- No proto field advertised and ignored — honored or `INVALID_ARGUMENT`. Add a
  posture check to prevent recurrence.
- No doc asserting behavior the code lacks (T-3 class). Add a docs-vs-API check.
- Proto changes **additive only** (headroom verified: `SelectRequest` next free 9,
  `DeleteRequest` 5, `RecordSet` 5, `UpsertRequest` 10).
- **Every security fix proven with the DB's own protection disabled**, so the test
  proves the application-layer predicate, not RLS.
- **No second path** — reviewers reject diffs reimplementing §5.0.
- Linux-only gates before push: `check-error-detail-posture.py`,
  `generate-codebase-map.py --check`. Version bump via `versions.json` +
  `check-versions.mjs --fix`. One `cargo check` per block.

**Open questions**
1. **Sequencing** — 0.4.20 = blocks 0–6 as recommended, or wider?
2. **X-4** — enforcing top-level tenant predicates may reject `$or` filters that
   pass today: warn-then-enforce across two releases, or break immediately?
3. **P-2** — `rows` duplicates `records_json` on the wire. Accept the payload
   growth, or populate only on request?
4. **F-1** — implement the audit emitter this release, or remove the config
   surface and production gate until it exists? (Shipping neither is not an option.)
5. **E-4** — should OTP delivery failure block issuance (fail closed)?
6. **L-4** — `BatchUpsert` atomic (head-of-line blocking on one admission permit)
   or documented non-atomic?
7. **R-3** — expose isolation level / savepoints / true interactive tx? Large, but
   "transactions work flawlessly" is not true without at least isolation control.
8. **E-2** — real `register_backend()`, or document backends as compile-time?
9. **Z-0 scope** — cut 0.4.19 with only Z-1/Z-2/Z-10 as recommended, or hold the
   release until more lands? (Recommendation: cut immediately. The consumer
   regenerates and byte-diffs same-day, which is the fastest external validation
   loop available for every later codegen change.)
10. **Z-2 escape hatch** — omitting zero values means a consumer can no longer
    intentionally write `""` to a nullable column via the full-record path. Ship
    an explicit `Set`-style override, or document the restriction?
11. **Z-3** — canonicalize the broker timestamp wire form (my recommendation, and
    the reporter's) or widen the parser in all six SDKs? Canonicalizing is a
    consumer-visible wire change.
12. **Z-4 staging** — ship fail-closed rejection of equality filters on encrypted
    columns immediately (removes the silent-mismatch foot-gun), with transparent
    blind-index derivation later?
13. **Z-8** — grant `authn.user.create` to the bootstrap owner, or add an offline
    `udb auth bootstrap service-account` command? Either way the two authorization
    layers must be made to agree.

---

## 9. Shipped in 0.4.20 (bug-fix release)

- **Z-1 · proto3 `optional` presence** — threaded end to end:
  `db_parser.rs:409-420` (capture the `optional` label, previously discarded) →
  `schema/ast.rs` `ProtoColumn.has_presence` → `manifest/mod.rs`
  `ManifestColumn.has_presence` → `manifest/build.rs` `column_from_proto` →
  `sdk_manifest.rs` `EntityColumnDescriptor.has_presence` → `sdk_gen.rs`
  pointer-aware `go_to_record_stmt`/`go_from_row_stmt` (scalars **and** enums).
  **Both new struct fields are `#[serde(skip)]`** — the manifest JSON feeds
  `checksum_sha256` and UDB's own protos use `optional` in 3 files, so
  serializing would have changed embedded-manifest checksums and tripped the
  startup validation gate. Codegen reads the flag in-process from freshly parsed
  source, which never round-trips through JSON. +3 tests.
- **Z-2 · NULL vs `""`** — a nullable string without presence now omits the key
  when empty, so SQL NULL round-trips as NULL. Numeric zero still writes (only
  strings carry the ambiguity), and NOT NULL strings are unchanged. The
  restriction is documented in the generated file header, with the escape stated:
  declare the field `optional` to store a genuine empty string. +2 tests.
- **Z-3 · timestamp layout ladder** — generated `udbAsTime` now tries
  RFC3339Nano → RFC3339 → the four observed broker/DATE renderings instead of
  silently zeroing a value it could not parse. (Broker-side canonicalization
  remains the better long-term fix — deferred to 0.4.21.)
- **B-1 · bench harness restored** — see P-3. Left P-3b coverage debt.
- **Z-10** — AUTH-010 (`apikey.rs:722`) and GO-012 (`MergeRequestScopedAudit`).
- **Version** — `versions.json` 0.4.18 → 0.4.20, propagated to 103 files via
  `check-versions.mjs --fix`.

## 9b. Landed toward 0.4.21 this session (Rust-only, verified, cargo-check-green)

Each item was **read against source before editing** (per the evidence bar), then
implemented, `cargo check`-verified, and committed on `fix/cat003-0418`. Tests
added where a unit test is meaningful; live-PG concurrency assertions (F-2, X-4
negative path) remain env-gated and run in CI.

| Commit | Items | Notes |
|---|---|---|
| `608c2bec` | **F-2, X-1, X-2** | idempotency `DO UPDATE` blocking claim; cache key folds in limit/sort; read fence ahead of cache-hit return |
| `06c96875` | **X-4, F-8** | `mandatory_and_columns` (descends `$and` not `$or`) on all 4 isolation sites; DEAD_LETTER repair doc corrected |
| `4439a6d9` | **F-6, R-7, F-9(1 site)** | CDC journal-before-broadcast; `capability_status_with_code`; the one unambiguous-prod `default()→current()` |
| `eb9cc778` | **T-4 (DataBroker)** | `unauthenticated_status` helper + 12 security.rs sites, specific reason tokens |
| `fe85ebc4` | **T-4 (Authn login)** | 15 login.rs sites, **anti-enumeration-safe uniform tokens** for the deliberately-vague messages |
| `ee624353` | **X-3, F-4, F-9(all), F-7** | bind normalized filter (both sites); per-process `{hostname}-{uuid}` leader lease; all 16 CDC `default()→current()`; idempotency retention sweeper on the leader tick |
| `9b955a5d` | **G-2** | conditional delete (CAS-on-Delete): `DeleteRequest.expected` + shared `enforce_cas_precondition` core + PK-equality guard; **full proto→buf→regen pipeline** exercised (6 SDK stubs + descriptor) |
| `07b46251` | **R-6** | readiness refresh probes the DataBroker plane's OWN pool (was the auth-plane pool for every plane); shared `pg_pool_reachable` |

**Behavioral changes to flag at release:** X-4 now **rejects** requests that put
the tenant/project predicate inside `$or` (previously a silent isolation hole);
X-1 invalidates existing read-cache entries (cold cache after deploy).

**Environment correction (2026-07-23):** an earlier revision claimed buf/Docker
regen was unavailable here. That was asserted, never tested — and it is FALSE:
`buf 1.65.0` (the pinned version) and Docker Desktop are both available, 18G
disk free. Proto-changing items are therefore doable in-environment via the real
pipeline; they are no longer "blocked", only larger. The list below is now
ordered by SIZE/RISK, not by a false environment gate.

**Total landed this session: 24 items** — both flagship consumer features END-TO-END (G-2 conditional delete: server CAS core + `WithDeleteExpected`; P-1 keyset pagination: `pagination.rs` + wiring + `Entity.SelectPage`); + 3 audit false-gaps corrected (X-11, T-2, T-3); + every CI-gate regression fixed. (13 integrity + T-4 both
halves + X-3/F-4/F-9/F-7 + G-2 the first full proto-pipeline feature).

**NOT YET done — larger or needing live infra (ordered by tractability):**
- **F-1** audit emitter (Large; needs the canonical-store decision first)
- **F-3** saga-ledger-before-side-effect (correctness-sensitive tx reorder)
- **F-5** tail `FOR UPDATE SKIP LOCKED` — NOT a one-liner: needs transactional
  row-claiming (holding a PG lock across the Kafka publish), a real tx-model
  change. F-4 (leader fencing, DONE) is the more correct split-brain fix anyway
  (split-brain-critical; needs a live two-broker test)
- **F-7** idempotency-key retention sweeper (new background worker)
- **F-9** the remaining ~16 CDC `default()→current()` sites (test/prod interleaving
  needs per-site reading)
- **X-3** bind-from-normalized-filter (planner)
- **C-1/C-2/G-2/P-1** expression-UPDATE, aggregates, conditional delete, keyset
  pagination — all require **proto + buf regen (Docker, 13 pinned plugins)** and,
  for C-1/C-2, 18-backend IR work + the S-1 tenant-predicate fix
- **T-5/T-6/T-7/T-8** error-contract proto, six-language
  transaction + facade codegen
- **E-1/E-6** provider request templates + the flow-hook gRPC contract
- **Z-4** broker-side blind-index derivation (crypto + filter rewrite)

## 10. Already landed earlier (provenance, not plan items)

- **Native request-scoped metadata** — `MergeRequestScopedAudit` in
  `sdk/go/udbclient/client.go`, used by `client.go`, `auth.go:47`, and
  `outgoingContext` in both the template and its generated twin. +2 tests; Go
  suite green. Fixes the reported native-Storage correlation defect.
- **ListApiKeys name/description** — `api_key_to_pb` (`apikey.rs:722`) returned the
  key prefix as `name` and an empty `description`, breaking reconcile-by-name
  provisioning. +2 tests. **Not yet `cargo check`-verified.**
- **`record_set_rows` bench group** added (`hotpath_bench.rs:129`, `:391`) —
  **blocked by the pre-existing B-1 breakage**, not yet runnable.
