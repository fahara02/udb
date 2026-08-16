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

The live unique-index planner now retains key boundaries. If any member of a
composite live key is already protected by a trusted restore remap, the whole
key is collision-protected and auxiliary members are preserved. Thus a fresh
UUID `log_id` makes `(log_id, created_at)` distinct without fabricating or
changing the timestamp.

Collision safety remains fail closed:

- a live key containing the target tenant column remains tenant-scoped;
- an uncovered unique key is still inspected and must obtain the existing
  trusted text/UUID or owned-sequence remap authority;
- an independently unique `TIMESTAMPTZ` column is still rejected with
  `FAILED_PRECONDITION`;
- inserts remain ordinary transactional inserts, so no conflict-ignore or
  overwrite behavior was introduced.

## Coverage and required evidence

Focused unit coverage proves that `(log_id, created_at)` is covered by the
trusted `log_id` remap while a standalone unique `created_at` remains rejected.
The served live Backup regression now seeds a real partitioned NotificationLog,
backs up its tenant through PostgreSQL and MinIO, restores to a fresh tenant,
and asserts that `log_id` changes while `created_at` is preserved.

GitHub CI must run:

`cargo test --lib restore_remap_tests`

and the ignored live filter:

`UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_backup_restore_remaps_owned_bigserial_identity -- --ignored --nocapture`

The next post-release four-SDK benchmark must report `BackupService/RestoreTenant`
as `OK`. No local Cargo/build/test/rustfmt/codegen was run.
