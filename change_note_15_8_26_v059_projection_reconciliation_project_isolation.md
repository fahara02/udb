# Release 0.5.9 change note: project-scoped projection execution and repair

Date: 2026-08-15  
Validation: Source-complete; CI pending

Projection reconciliation, materialization, drift detection, and drift repair
now treat `project_id` as a first-class routing and authority identity.

## Changed behavior

- Dead-letter groups are keyed by
  `(project_id, source_table, target_backend, target_instance)`.
- Requeue operations require and match the exact `project_id` on Postgres,
  MySQL, SQLite, MSSQL, MongoDB, Neo4j, Cassandra, ClickHouse, Redis, Qdrant,
  and the shared vector-system adapters (including Elasticsearch CAS updates).
- Reconciliation reports and logs include the repaired project.
- Reconciliation source replay now resolves current exact catalogs from the
  retained `CatalogManager` on every pass, observes later activation/reload,
  skips replay while catalog authority is stale, and reads each project's
  canonical source only through its project-bound PostgreSQL instance.
- Claimed materialization rows carry their persisted manifest checksum. The
  worker skips stale-authority passes and requires an exact ACTIVE project
  catalog with the same checksum before executing a task.
- Authority-rejected tasks remain terminal and are excluded from automatic
  dead-letter groups/requeue, preventing an obsolete manifest task from
  bouncing forever between reconciliation and materialization.
- Legacy dead letters with an empty project, and groups without fresh exact
  project catalog authority, remain parked instead of being requeued.
- Generic, Redis/cache, and object targets validate the requested instance
  against the row project and its read/write role. Non-default projects no
  longer inherit process-default clients when no bound target exists.
- `ScanProjectionDrift` routes its source PostgreSQL read and target probes by
  the claimed project. Drift repair enqueues only payloads from that scan's
  project-scoped samples, preserving the project and active manifest checksum
  in the canonical task ledger.
- The canonical projection-store conformance suite covers two projects sharing
  the same source and target tuple and asserts that repairing project A cannot
  make project B claimable; it also asserts every backend returns the persisted
  manifest checksum on claim.
- Unit regressions cover live catalog activation/reload, stale authority,
  manifest mismatch, exact project read/write target selection, missing
  project identity, and repair sample substitution.

## Compatibility

This is an internal Rust trait/type contract change. No protobuf or generated
SDK contract changes are required. Existing projection task storage already
contains `project_id` and `manifest_checksum`; no schema migration is
introduced by this correction.

## CI proof required

- `cargo test --lib canonical_store::conformance::tests::sqlite_projection_task_store_satisfies_contract`
- `cargo test --lib projection::tests::reconciliation_catalog_selection_is_live_and_fails_closed_when_stale`
- `cargo test --lib runtime::core::outbox_envelope_tests::projection_routing_requires_project_bound_instance`
- `cargo test --lib drift_reconciliation::tests::repair_uses_only_payloads_from_the_claimed_project_scan`
- `cargo test --lib drift_reconciliation::tests::drift_worker_fails_closed_before_probing_without_project`
- `cargo test --lib runtime::service::handlers_admin::tests::projection_drift_`
- Run the existing env-gated canonical-store conformance job covering all
  configured live backends, including
  `canonical_store::conformance_live_tests`.

No local Cargo/build/test/rustfmt command was run.
