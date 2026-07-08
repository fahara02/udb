# Simple-Client-Code — Execution TODO

Sequenced from `simple_client_code_todo.md` (42 adversarially-verified items).
Goal: client code "just works" — broker owns state — without perf/integrity loss.
Doctrine: batch edits, one `cargo check` per wave, fail-closed, reuse helpers,
proto changes go through regen (never hand-edit generated files).

## Wave 1 — pure-Rust broker fixes (no proto regen) — IN PROGRESS

- [ ] **Embedded read-fence forward** — `src/embedded.rs` `request_with_context`:
  forward `context.read_fence_json` as `x-udb-read-fence` (today embedded callers
  silently drop a struct-set fence). One line; `insert_ascii` early-returns empty.
- [ ] **Preserve policy ids across governance activation** — `governance_activate.rs`
  re-INSERT binds the document's own `p.id` via
  `COALESCE(NULLIF($id,'')::uuid, gen_random_uuid())` (keep DELETE-all invariant;
  validate empty-after-coalesce + within-doc dup ids, fail closed). Removes the
  `-getpolrule` isolated-project bench hack.

## Wave 2 — small proto+handler fixes (regen) — cheapest client wins

- [ ] Migration `approval_token` returned in the response **body** (not just header).
- [ ] `RegisterUploadResponse.expires_at` + an `ALREADY_FINALIZED` typed signal.
- [ ] `StartPipelineResponse.steps` populated (avoid the chained GetPipeline read).
- [ ] SCIM group lookup by stable KEY (remove client workaround).

## Wave 3 — identity authority from the verified claim (fail-closed + deletes body boilerplate)

- [ ] `executor_utils::merge_context` scopes metadata-wins (close body-scope override).
- [ ] Authz `Authorize`/`CheckAccess`/`GetNativeAccess` bind authority from claim.
- [ ] Governance actor (subject/tenant/scopes/break-glass) from claim.
- [ ] Authn WRITE paths derive omitted body tenant/project from claim.

## Wave 4 — SDK helpers (consume the above)

- [ ] Typed write-receipt + read-fence helper (read-your-writes without polling).
- [ ] Typed error-detail decode → retry branching.
- [ ] Contract metadata (`operation_kind` in JSON; lifecycle/idempotency options).
- [ ] `storage.uploadFile` facade + bound entity/table APIs + TS typed layer.

## Wave 5 — bench/harness + docs (lock-in)

- [ ] Kill the two `GenericDispatch` internal-table seeds (device via Login `device_id`;
      env-gated FAILED notification log).
- [ ] Descriptor-driven fixture planning; mock-transport facade proof; contract-freshness CI.

See `simple_client_code_todo.md` for full per-item anchors, ratings, guardrails,
and the Dropped/Refuted list (don't redo already-shipped/unsound items).
