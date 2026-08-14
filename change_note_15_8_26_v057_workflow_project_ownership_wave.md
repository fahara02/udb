# UDB v0.5.7 change note: Workflow project ownership wave

Date: 2026-08-15
Status: source complete; GitHub CI pending

## Changed

- Workflow project binding no longer converts a non-empty non-UUID authority to
  empty tenant-wide scope; the RPC now fails closed before database access.
- StartWorkflow persists the effective claim/header project when its body omits
  the redundant project field.
- GetWorkflow, ListWorkflows, CancelWorkflow, and SignalWorkflow continue to use
  one shared exact optional-project predicate, now fed only validated UUID scope.
- Cancel/Signal read the stored row project and retain it in their transactional
  outbox envelope and payload, including for tenant-wide administrators.
- The live Scheduler and Workflow tests share one native outbox fixture helper.

## Regression coverage

- Added no-pool served-path coverage proving code-shaped project authority is
  rejected by all five Workflow RPCs.
- Added binder/predicate unit guards and an ignored live Postgres two-project
  regression for UUID scope isolation, claim/header persistence, Cancel/Signal
  denial, event lineage, and tenant-wide compatibility.

## Verification

- Manual call-site/outbox review and pre-stage `git diff --check`: passed.
- Local Cargo build/test: deliberately not run because the operator requires
  CI-only compilation and testing due local hardware limits.
- GitHub CI: pending after an isolated Workflow commit is pushed.
