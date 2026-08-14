# UDB v0.5.7 change note: Scheduler project ownership wave

Date: 2026-08-14
Status: source complete; GitHub CI pending

## Changed

- Scheduler now resolves project authority from the validated claim first and
  validates the UUID representation required by ScheduledJob persistence.
- Non-empty textual project codes that cannot be represented by the current
  UUID-backed Scheduler schema now fail at the RPC boundary; they never widen
  to tenant scope or reach PostgreSQL as an invalid UUID cast.
- CreateJob persists that effective project even when a scoped caller omits the
  request-body project field.
- GetJob, ListJobs, DeleteJob, PauseJob, and ResumeJob apply one shared optional
  project-ownership predicate before returning or changing state.
- Delete/Pause/Resume return the affected row's stored project inside their
  existing transaction and put it into both the outbox envelope and payload.
- Tenant-scoped credentials with no project retain intentional tenant-wide
  administration; project-scoped mismatches retain ordinary not-found errors.

## Regression coverage

- Added unit coverage for fail-fast non-UUID project authority and the shared
  SQL predicate shape.
- Added an ignored live Postgres two-project served-path regression for
  create/get/list/delete/pause/resume, event lineage, and tenant-wide behavior.

## Verification

- Fast call-site audit and pre-stage `git diff --check`: passed.
- Local Cargo build/test: deliberately not run because the operator required
  CI-only compilation and testing due local hardware limits.
- GitHub CI: pending after the isolated Scheduler commit is pushed.
