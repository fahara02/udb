# The two Postgres SQL paths — real function-level analysis (item 2.4)

You rejected the word **"legacy"** and you were right to. I read both paths end to end and
listed every function. This is the evidence, not a label.

## First, the two facts that kill the "legacy" story

1. **Both paths were born in the *same* initial commit** (`bc92a914`, 2026-05-31). Neither
   aged out of the other. There has been no "v1 era then a rewrite." They have coexisted
   from line one of the repo.
2. **They sit behind two *different* RPCs**, not one feature replacing another:
   - **PATH A** serves `DataBroker.Select / Upsert / Delete` — the simple typed CRUD the
     SDKs call. Entry: `setup_data.rs::select` (line 402) and `::upsert` (line 610).
   - **PATH B** serves `DataBroker.ExecuteBackendOperation` — the generic cross-backend RPC.
     Entry: `handlers_data.rs::execute_backend_operation` (line 487) →
     `compile_neutral_ir_dispatch` (line 658). For the **other 17 backends B is the only
     path**; for Postgres it *also* exists and overlaps A.

So this is **"data-plane specialist (A) vs cross-backend generalist (B), built in parallel,
never welded"** — exactly the multi-stream pattern you described. The proof of "never welded"
is mechanical: in `handlers_data.rs` the IR branch returns `Ok(None)` when the request has no
`ir` envelope, so **Postgres CRUD silently defaults to A and B never runs** unless a caller
opts in. Two correct paths, no forcing function to merge them.

## The table — every function, aligned by responsibility

| Responsibility | PATH A — data-plane planner (file::fn) | PATH B — IR compiler (file::fn) | My comment |
|---|---|---|---|
| **Entry / caller** | `build_select_query_plan` (planning/broker/mod.rs:400), `build_upsert_plan` (:574), `build_delete_plan` (:739) — called by `setup_data.rs::select`:402 / `::upsert`:610, `tx_object.rs`:196, `build_transaction_plan`:813 | `compile_for_backend` (ir/compile/mod.rs:207) via `compile_neutral_ir_dispatch` (handlers_data.rs:658), `compile_ir_payload`:733, `compile_logical_{read,write,update,aggregate,delete}_dispatch`:819–916 | Different RPCs, not rivals. A = typed CRUD; B = generic op. Overlap is **Postgres-only**. |
| **SELECT gen** | `build_select_query_plan_uncached`:427 | `PostgresCompiler::compile_read` (postgres.rs:242) | **True overlap.** Both emit `SELECT … FROM … WHERE`. A from a `SelectRequest` (Struct filter); B from a typed `LogicalRead`. |
| **UPSERT gen** | `build_upsert_plan`:574 (+ `is_update_excluded_column`, `conflict_target_is_unique` in helpers.rs) | `compile_write`:321 (+ `validate_unique_conflict_target`:172, `partition_aware_fields`:129) | **True overlap** — both emit `INSERT … ON CONFLICT … DO UPDATE`. This is the exact path we just bug-fixed on the **A** side. |
| **DELETE gen** | `build_delete_plan`:739 | `compile_delete`:567 | **True overlap.** Both require a filter and fail closed without one. |
| **Filter / predicate compile** | `compile_filter_predicates`:1362, `compile_filter_group`:1432, `compile_column_predicate`:1467, `unescape_like_pattern`:1611 | `Pg::render_where` + `wrap_value_for_op`:57 + `cast_compare_placeholder`:80 | **Duplicated logic.** Two filter compilers. Both special-case UUID/timestamptz placeholder casts — i.e. the *same casting rule lives in two places* (the class of bug we hit in binding). Prime drift risk. |
| **Field/column alias resolution** | `column_resolver`/`resolve_column`/`normalize_filter_keys`/`normalize_record_keys` (helpers.rs:65–141), `allowed_columns`:52 | `column_for`, `logical_field_name`:116, `field_set`:108 | **Duplicated.** Both map proto `field_name` → physical `column_name`. |
| **Tenant / project scoping** | `tenant_column`:1268, `project_column`:1276, validated inside `build_*_plan`; PG relies on RLS `SET LOCAL` at exec | `CompileContext::with_tenant`:402 / `with_project`:407; `util::append_context_predicates` (non-SQL backends) | A *validates & errors* on missing tenant; B *carries* it in context. For PG both lean on RLS. `append_context_predicates` is **B-only** and only used for ClickHouse/Cassandra/Mongo. |
| **Plan caching** | `build_select_query_plan`:400 + `select_plan_cache_key`:362 + bounded 512 `OnceLock` cache | — none — | **A-ONLY.** Retiring A loses plan caching unless it moves to the wrapper. |
| **Scope / purpose auth** | `has_scope`:1317 (`udb:read`), `validate_write_context` (helpers.rs:27) | — none (assumes IR built post-authz) — | **A-ONLY.** B trusts authz already happened. |
| **PII / encrypted column policy** | `build_select_query_plan_uncached`:462 excludes `is_pii`/`is_encrypted` from implicit `SELECT *`; `masked_columns`:1289 | — none (projection passed through verbatim) — | **A-ONLY and security-relevant.** Must be preserved in any merge. |
| **Cache-policy + audit metadata** | `build_cache_policy_plan`:869, `build_audit_event`:1137, `audit_event_type` on the plan | — none (`CompiledRendering` is just `{sql, params}`) — | **A-ONLY.** A's `QueryPlan` carries operational metadata B doesn't model. |
| **Parameter binding (exec)** | `postgres_helpers.rs::bind_values`/`bind_one`/`record_values` (binds JSON by manifest `sql_type` — **where our timestamptz/uuid bug lived**) | `compiled_rendering_to_dispatch` (handlers_data.rs:916) → executor; params are typed `LogicalValue` | **Two binding layers.** A binds untyped JSON by sql_type; B binds typed `LogicalValue`. The cast bug could exist in one and not the other — that's the cost of duplication. |
| **Backend reach** | Postgres / SQL only (`effective_sql_backend`:1604) | **All 18 backends** (`compile_for_backend` match, mod.rs:207) | **B's whole reason to exist.** A structurally cannot target Mongo/ClickHouse/vector/etc. |
| **Capability refusal** | string errors pushed into `plan.errors` | typed `CompileError::OperationNotSupported` / `OperatorUnsupported` (mod.rs:552) | B has the cleaner, typed refusal model; A is stringly-typed. |

## Why it happened (the honest reading)

The squashed initial commit hides authorship order, but the *shape* is unambiguous: one
work-stream owned the **typed data-plane CRUD** (A — rich: caching, authz, PII, audit,
cache-policy, JSON binding) and another owned the **neutral-IR cross-backend compiler** (B —
lean, typed, 18-backend, no data-plane concerns). They were never merged because **each works
on its own and Postgres defaults to A** (B only fires on an explicit `ir` envelope). Nothing
forced a reconciliation, so the overlap (SELECT/UPSERT/DELETE SQL emission for Postgres)
quietly persisted. That is the parallel-development residue you suspected — confirmed by the
same-commit birth, the duplicated filter/cast/alias logic, and the A-default / B-opt-in wiring.

## So the correct action is a MERGE, not a "retire"

"Retire the legacy planner" is wrong because **A is a superset of B for the data plane** — it
carries caching, scope/purpose auth, PII exclusion, audit, and cache-policy that B simply does
not have. Deleting A deletes those behaviors. The doctrine-correct move (no duplication, no
feature loss, wire-in over delete):

1. Make **B the single SQL emitter** for Postgres SELECT/UPSERT/DELETE (one place that builds SQL).
2. **Move A's value-adds** (plan cache, `has_scope`/purpose checks, PII/`is_encrypted`
   exclusion, `build_cache_policy_plan`, `build_audit_event`) into the **thin data-plane
   wrapper** that calls B — so they are preserved, not dropped.
3. **Collapse the duplicated sub-logic**: one filter compiler, one alias resolver, one
   UUID/timestamptz cast rule, one binding layer (the duplication that produced our bind bug).
4. **Prove `A-SQL ≡ B-SQL` on live Postgres** (extend the cross-backend fixtures to a live PG
   run) *before* removing A's SQL-gen — so the merge can't silently change behavior.

Net: not "kill the old one," but "**two SQL generators become one, and the data-plane features
that only A had survive in the wrapper.**" That's item 2.4 reframed honestly. It pairs with
Phase 2.1 (make the IR path the default instead of opt-in) — do 2.1 first, then this merge.
