# UDB v0.5.7 Workflow project scope downgrade

Date: 2026-08-14
Correction implemented: 2026-08-15
Status: corrected in source; GitHub CI pending
Affected service: `udb.core.workflow.services.v1.WorkflowService`

## Summary

Workflow rows store `project_id` as UUID, while authenticated metadata can carry
a human project code such as `default`. The served read/mutation helper handles
that type mismatch by converting every non-UUID project claim to an empty string.
The SQL predicate interprets empty as tenant-wide.

A project-bound caller whose validated project is a code therefore receives
tenant-wide Get/List/Cancel/Signal access, including workflows belonging to
sibling projects. This is an authorization downgrade, not a harmless query
normalization.

## Confirmed served paths

- `workflow_project_bind` returns empty for an empty or non-UUID value.
- `workflow_scope_predicate` uses `($project = '' OR project_id = $project)`.
- GetWorkflow, ListWorkflows, CancelWorkflow, and SignalWorkflow all apply that
  pair to the project resolved from metadata/claims.
- Their comments explicitly describe a non-UUID project code as degrading to
  tenant-wide.
- StartWorkflow casts its body project directly to UUID, so the same valid
  customer project-code shape can instead fail at insert time. Read and write
  behavior are inconsistent.

## Correction implemented

- `workflow_project_bind` now returns a typed validation result: empty authority
  remains intentional tenant-wide access, a UUID remains exact project scope,
  and every other non-empty value is rejected before pool/SQL access.
- StartWorkflow resolves claim/header authority first and persists it when the
  redundant body field is empty. All other RPCs resolve the same authority and
  retain the existing exact optional-project predicate.
- CancelWorkflow and SignalWorkflow load the durable row project and use it in
  both the transactional outbox envelope and domain payload. Tenant-wide
  operators no longer erase project lineage from their mutation events.
- Canonical textual-project/UUID convergence remains a separate schema
  compatibility migration. It cannot be implemented by widening an
  unrepresentable authenticated project to tenant scope.

## Verification log

- Source trace completed through request metadata projection, scope validation,
  project normalization, all workflow predicates, and the UUID schema/insert.
- Added a no-pool served-path unit regression proving all five RPCs reject a
  code-shaped project instead of reaching tenant-wide access, plus direct binder
  and predicate guards.
- Added an ignored live Postgres two-project regression covering Start/Get/List,
  cross-project Cancel/Signal denial, tenant-wide compatibility, and durable
  Signaled/Cancelled event project lineage.
- Reused one live-test outbox reset helper across Scheduler and Workflow instead
  of creating another service-local fixture.
- Per operator direction, no local Cargo build or test was run. GitHub CI and
  the ignored live Postgres regression remain pending.
