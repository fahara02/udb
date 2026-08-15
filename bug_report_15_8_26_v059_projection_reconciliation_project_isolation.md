# Release 0.5.9 bug report: projection workers crossed project-store boundaries

Date: 2026-08-15  
Severity: Critical  
Status: Fixed in source; CI validation pending

## Summary

Projection dead-letter reconciliation grouped tasks only by source table,
target backend, and target instance. Projection materialization trusted a
startup/default backend authority after claiming a row, and the served drift
scanner read both its canonical source and projection targets through default
clients. When two projects shared logical resource names, reconciliation,
materialization, or drift repair could therefore touch the wrong project's
physical store.

This violated the project-store boundary even though projection task rows
already carried a non-null `project_id`.

## Root cause

- `DeadLetterGroup` omitted `project_id`.
- SQL, document, graph, wide-column, ClickHouse, Redis, and shared JSON
  canonical-store implementations grouped dead letters without `project_id`.
- `ProjectionTaskStore::requeue_dead_letter_by_source` did not accept a project
  identity, so backend update predicates could not bind one.
- `ReconciliationWorker` reports did not identify the project being repaired.
- `ReconciliationWorker` captured one startup manifest/project, so later
  activation or durable catalog reload was invisible; it could also replay
  source rows after catalog authority became stale.
- Claimed projection rows dropped the manifest checksum before dispatch, so a
  task produced under an obsolete project catalog could still execute.
- Once catalog validation was added, blindly requeueing its terminal authority
  rejection would create a permanent dead-letter/requeue loop.
- Generic mutation, Redis/cache, and object projection paths accepted the
  process-default client when no exact project-bound instance was proven.
- `ScanProjectionDrift` loaded source rows from `ProjectionEngine`'s default
  PostgreSQL pool and target observations through project-agnostic dispatch.
  Repair then re-read the row from that default source instead of using the
  sample captured for the claimed project scan.

## Resolution

- `DeadLetterGroup` now carries `project_id`, and every backend includes it in
  its grouping key.
- `requeue_dead_letter_by_source` now requires `project_id`; every update,
  filter, scan, CAS mutation, or JSON-row rewrite matches it exactly.
- `ReconciliationWorker` passes the discovered project's identity into the
  repair operation and includes it in reports and structured logs.
- The worker retains the live `CatalogManager`, skips source replay while its
  authority is stale, and resolves every active project through
  `active_project_ids` plus exact `active_exact_for` on each pass.
- Reconciliation source replay resolves the current project's read-capable
  PostgreSQL instance on every pass; its repair tasks still enter the canonical
  projection ledger with that exact `project_id`.
- Every claimed task now carries `manifest_checksum`. Materialization skips a
  stale-authority pass and, immediately before dispatch, requires a non-empty
  project, an exact ACTIVE catalog, and an equal non-empty checksum.
- Catalog-authority rejections are parked with a stable terminal prefix.
  Canonical-store dead-letter grouping and source requeue exclude those rows,
  preventing reconciliation from cycling a task whose provenance can never
  match; an operator must replace or migrate it under the active catalog.
- Projection target resolution validates explicit instances against the row's
  project and read/write capability. Non-default projects cannot fall back to
  an unlabeled default client. Generic mutations carry `RequestContext`;
  Redis/cache and object mutations use exact project-aware clients.
- The served drift scan passes `project_id` into source loading and its worker.
  Source PostgreSQL and every supported target probe resolve only through the
  project's read-capable instances. Repair enqueues the source payload captured
  by that same scan and refuses a divergent key absent from the sample set.
- The shared projection-store conformance contract now creates identical
  source/backend/instance dead letters in two projects, repairs one project,
  and proves the other remains dead-lettered. The same contract is exercised
  by SQLite unit CI and the env-gated live canonical-store backends.
- A unit regression proves stale authority yields no replay catalogs and that
  later activation and reload are visible without restarting the worker.
- Routing tests prove both projection reads and writes reject a different or
  unbound project. Drift tests prove a missing project fails before probing and
  repair cannot substitute a row absent from the claimed project's samples.
- The cross-backend conformance contract proves authority-rejected dead letters
  remain parked and outside automatic repair groups.

## Validation posture

Per the requested resource constraint, no local Cargo command, build, test, or
rustfmt was run. Static call-site and backend-implementation inspection plus
`git diff --check` are the local validation boundary. Cargo unit and live
canonical-store conformance remain required in CI.
