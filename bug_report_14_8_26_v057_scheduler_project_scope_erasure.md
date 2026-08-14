# UDB v0.5.7 SchedulerService project-scope erasure

Date: 2026-08-14
Status: corrected in source; GitHub CI pending
Affected service: `udb.core.scheduler.services.v1.SchedulerService`
Impact: project-bound callers can inspect or mutate sibling-project jobs

## Summary

`CreateJob` validates and stores `project_id`, and the leader tick preserves it
in fire/dead-letter envelopes. The five remaining RPCs accept no project field,
call `validate_request_scope(..., "")`, and query/mutate by tenant plus job id (or
tenant alone for List). The validated claim/header project is never applied as a
row predicate.

A project-A caller inside tenant T can therefore list project-B jobs, fetch a
known project-B job, and delete/pause/resume it. Delete, pause, and resume also
emit envelopes with an empty project even though the durable row has one, so
downstream lifecycle consumers lose the job's canonical scope.

## Confirmed served paths

- `CreateJob` binds `project_id` into ScheduledJob and the created event inside
  one transaction after `validate_request_scope`.
- `GetJob` filters only `(job_id, tenant_id, deleted_at)`.
- `ListJobs` counts and pages only by tenant/status.
- `DeleteJob`, `PauseJob`, and `ResumeJob` update only by `(job_id, tenant_id)`
  plus lifecycle status/deletion predicates.
- Those three mutation events pass `project_id = ""` to the transactional
  outbox helper instead of returning/locking the row's stored project.
- `run_scheduler_tick_once` does select the durable project and uses it in fired
  and dead-letter envelopes, proving the lineage is available and expected.

## Schema compatibility decision

ScheduledJob declares `project_id` as PostgreSQL `UUID`, while UDB request
metadata can carry canonical textual project identifiers. This is the same
representation mismatch confirmed in Asset and Storage. This security repair
does not weaken the row predicate to accommodate an unresolvable text code:
Scheduler accepts UUID project authority, rejects any other non-empty value at
the RPC boundary, and preserves empty authority only for intentional
tenant-wide operators. Converging the wider routing identifier and UUID-backed
native schemas remains a separate compatibility/migration concern; it cannot be
allowed to reopen cross-project access.

## Positive evidence retained

The scheduler mutation and firing paths otherwise use strong transaction
boundaries: create quota/row/event, delete/pause/resume plus event, and due-job
advance plus fired/dead event co-commit. The tick uses leader election and
`FOR UPDATE SKIP LOCKED`, and its per-occurrence idempotency key is stable. This
finding does not invalidate those properties.

## Correction implemented

- For project-bound claims, add exact stored-project predicates to Get/List and
  all mutations before pagination or state change. Tenant-bound no-project
  callers may retain intentional tenant-wide administration.
- Make mutation SQL return the stored project and place it in the same
  transactional event envelope.
- Add same-tenant project-A/project-B live tests for all five affected RPCs and
  event payload/envelope scope.
- Fail closed before SQL when non-empty project authority cannot be represented
  by the current UUID-backed ScheduledJob schema. Track any future identifier
  convergence as an explicit schema migration, not a predicate bypass.

## Verification log

- Source-traced all six handlers, ScheduledJob schema, mutation transactions,
  due-job claim/tick, and event construction.
- Confirmed state/event atomicity for the scheduler paths reviewed; the defect is
  project authorization and project identity loss, not a generic tick race.
- Added one claim-first Scheduler project resolver. Non-empty project authority
  is normalized as UUID before SQL, matching the persisted ScheduledJob column;
  invalid textual values now fail at the RPC boundary instead of becoming a
  database cast error. Empty authority remains intentional tenant-wide access.
- Get/List and Delete/Pause/Resume share the same optional exact-project SQL
  predicate. Mutation statements return the stored project from the affected
  row and use it in both the transactional envelope and domain payload.
- CreateJob persists the effective claim/header project when the request body
  omits the redundant field.
- Added unit guards for UUID authority and predicate shape plus an ignored live
  Postgres two-project served-path regression covering create/get/list, all
  three mutations, tenant-wide compatibility, and pause-event project lineage.
- Per operator direction, no local Cargo build or test was run. GitHub CI and
  the ignored live Postgres regression remain pending.
