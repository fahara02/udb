# UDB v0.5.7 an unknown CDC cursor triggers an unbounded shared-journal replay

Date: 2026-08-14
Status: correction implemented; live retained-journal verification pending
Affected path: `DataBroker.PublishCDC`

## Summary

A caller-supplied `since_event_id` that parses as a UUID but is not present in
the retained journal is silently mapped to the Unix epoch. `PublishCDC` then
loops over the shared journal in 1,000-row queries until it reaches the current
tail, filtering tenant/project only after reading each row. Because the stream
also releases its admission permit at construction, any CDC subscriber can
create many full-history scans with random UUIDs.

## Confirmed served path

- The public request accepts any non-empty `since_event_id` string without
  ownership, topic, age, or existence validation.
- `stream_cdc` parses the value as UUID and looks up only `published_at` by the
  globally unique event ID; the query is not bound to requested topic, tenant,
  or project.
- `Ok(None)` and every lookup error both select `(Unix epoch, empty event id)`.
- Initial replay repeatedly calls the unscoped shared-journal query with
  `LIMIT 1000` until a returned output vector is short. Tenant/project/topic
  filtering happens in Rust after the database has read the global rows.
- Unlike `journal_replay_for_scope`, this path has no total scanned-row/page/time
  ceiling. A valid stale cursor pruned by retention takes the same path as a
  random attacker-selected UUID.
- A syntactically invalid cursor instead follows the fresh-subscription branch
  and starts at the newest row, making malformed and unknown cursors
  inconsistently observable.

## Consequences

- A low-privilege CDC subscriber can amplify one cheap RPC into a retained-
  history scan over every tenant's shared journal.
- Multiple concurrent random cursors can impose sustained PostgreSQL read and
  JSON-decode load without occupying CDC admission capacity.
- A transient cursor-lookup database error silently changes requested resume
  semantics into full replay rather than producing a retryable failure.
- Clients cannot distinguish a pruned cursor from a valid resume and may receive
  an unexpectedly large replay.

## Required correction

- Return a typed invalid/pruned-cursor status when the supplied UUID cannot be
  resolved; do not treat lookup failure as data absence.
- Bind cursor resolution to a signed/versioned cursor containing topic pattern,
  tenant, project, journal watermark, and retention generation, or validate the
  referenced row against the requesting scope before accepting it.
- Bound replay by scanned rows, wall time, and pages, and surface an explicit
  continuation/watermark when the bound is reached.
- Hold the stream admission guard for the full replay and live lifetime.
- Add served tests for malformed, random-valid, pruned, foreign-scope, and
  database-failure cursor cases plus a scan-bound assertion on a mixed-tenant
  journal.

## Verification log

- Traced cursor parsing/resolution, replay SQL, loop termination, and per-row
  filtering in `CdcEngine::stream_cdc`.
- Cursor parsing and retained-row resolution now complete before the lazy stream
  is returned. Malformed cursors return `INVALID_ARGUMENT`, unknown/pruned UUIDs
  return `NOT_FOUND`, and journal dependency failures return a retryable status;
  none select Unix epoch.
- The stream admission guard now covers replay and live delivery.
- PostgreSQL and Kafka-enabled library checks passed. The live CDC test contains
  malformed and unknown cursor assertions; execution is delegated to CI.
