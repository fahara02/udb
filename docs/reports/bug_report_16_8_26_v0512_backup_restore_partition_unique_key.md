# Bug report: v0.5.12 Backup restore partition-aware unique key

Date: 2026-08-16
Release: 0.5.12
Status: fixed in source; GitHub CI and successor benchmark verification pending
Severity: post-release benchmark blocker
Affected path: `BackupService.RestoreTenant`
Evidence: v0.5.11 benchmark run `31956785039`

## Symptom

All four live SDK benchmarks reached the real restore handler and received
`FAILED_PRECONDITION`:

`unique column udb_notification.notification_logs.created_at uses unsupported restore type 'timestamp with time zone'`

This was a product restore-planning defect, not an SDK body, seed-order, or
language-specific failure.

## Root cause

`NotificationLog` declares `log_id` as its primary key and `created_at` as its
monthly PostgreSQL partition key. PostgreSQL requires every primary/unique key
on a partitioned table to contain the partition key, and UDB's SQL generator
therefore emits the live composite primary key `(log_id, created_at)`.

Cross-tenant restore correctly selected `log_id` for a trusted UUID remap from
the active manifest. Its live-index probe then flattened the same composite key
into individual columns, selected the remaining `created_at` member as though it
were independently unique, and rejected `TIMESTAMPTZ` because it has no safe
identity allocator.

## Correction

The manifest and live unique-index planners now retain key boundaries. If any
member of a composite key is protected by a trusted restore remap, the whole
key is collision-protected and auxiliary members are preserved. Thus a fresh
UUID `log_id` makes `(log_id, created_at)` distinct without fabricating or
changing the timestamp.

Collision safety remains fail closed:

- a live key containing the target tenant column remains tenant-scoped;
- an uncovered unique key is still inspected and must obtain the existing
  trusted text/UUID or owned-sequence remap authority;
- an independently unique `TIMESTAMPTZ` column is still rejected with
  `FAILED_PRECONDITION`;
- PostgreSQL expression keys (`attnum=0`) remain visible and are rejected before
  restore inserts unless
  another ordinary member of that exact key already has a trusted remap;
- a foreign-key member protects a unique child key only when the exact parent
  value map exists; unconditional maps are preallocated for every table row so
  nullable self-references do not depend on row order and are rebound after all
  rows exist, while missing, non-null-deferred, or later/cyclic parent authority
  fails before insert;
- partial-index predicates are batch-evaluated on a stable reconstructed target
  row until a bounded fixed point, so a value outside the predicate is preserved
  rather than needlessly rewritten and unstable predicates fail closed;
- bounded text identities must retain an alphabetic prefix plus a complete
  128-bit encoding (at least 33 characters); shorter columns fail instead of
  truncating every nonce into the same prefix;
- inserts remain ordinary transactional inserts, so no conflict-ignore or
  overwrite behavior was introduced.

## Coverage and required evidence

Focused unit coverage preserves manifest composite and partial-index groups,
keeps conditional plans from authorizing unconditional keys, proves later
unconditional remaps participate in predicate evaluation, requires exact
preallocated self/parent mappings, and rejects bounded text widths that cannot
retain full entropy. A standalone unique `created_at` remains rejected.
The served live Backup regression now seeds a real partitioned NotificationLog
plus two mutually self-referencing Users with empty partial-index email values.
It adds a live expression/ordinary composite probe index, backs up through
PostgreSQL and MinIO, restores to a fresh tenant, and asserts that `log_id`
changes while `created_at` is preserved, empty emails remain empty, bounded
usernames satisfy their lexical check, and both `created_by` references point to
the restored—not source—user identities.

GitHub CI must run:

`cargo test --lib restore_remap_tests`

and the ignored live filter:

`UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_backup_restore_remaps_owned_bigserial_identity -- --ignored --nocapture`

The next post-release four-SDK benchmark must report `BackupService/RestoreTenant`
as `OK`. No local Cargo/build/test/rustfmt/codegen was run.
