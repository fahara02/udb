# Change note: v0.5.12 Backup partition-aware restore planning

Date: 2026-08-16
Release: 0.5.12
Status: implemented; GitHub CI verification pending

`BackupService.RestoreTenant` now evaluates live PostgreSQL unique indexes as
keys instead of treating every member of a composite key as independently
unique. This unblocks partitioned relations whose generated primary or unique
key includes an auxiliary partition column.

For `udb_notification.notification_logs`, the active catalog selects the UUID
`log_id` for a trusted cross-tenant remap. The live database expands that primary
key to `(log_id, created_at)` because `created_at` is the monthly partition key.
Once `log_id` is remapped, restore preserves `created_at`; it no longer attempts
to synthesize a timestamp identity.

The change does not relax collision handling. A standalone unique timestamp or
any other uncovered unsupported unique type continues to fail with
`FAILED_PRECONDITION`, numeric identities still require their exact owned
sequence, and restored rows continue through transactional PostgreSQL inserts
without conflict-ignore or overwrite behavior.

Coverage extends the pure restore-planner tests and the served PostgreSQL+MinIO
Backup regression. CI must run `cargo test --lib restore_remap_tests` and
`UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_backup_restore_remaps_owned_bigserial_identity -- --ignored --nocapture`.
The successor four-SDK post-release benchmark must replace v0.5.11 run
`31956785039`'s four `FAILED_PRECONDITION` rows with `OK`. No proto, generated
contract, SDK, or migration change is required.
