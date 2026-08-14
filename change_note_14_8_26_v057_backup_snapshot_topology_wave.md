# UDB v0.5.7 backup snapshot/topology safety wave

Date: 2026-08-14
Status: implementation and PostgreSQL-only compile complete; full CI/live verification pending

## Why this changed

The served BackupService captured one startup Postgres pool and one startup
manifest, then exported each tenant table in a separate autocommit statement.
It could therefore report `COMPLETED` for a cross-table-inconsistent or
wrong-project backup. Object backend/bucket overrides were stored only inside
the manifest that later readers first tried to locate through mutable process
defaults. Retention also discarded the manifest/journal reference after partial
provider failures.

## Source changes

- Backup and restore now resolve an explicitly active project catalog and a
  concrete canonical Postgres write instance at operation start.
- Unknown project IDs no longer inherit the default catalog. Catalogs whose
  backup tables have zero/multiple write owners, a non-relational/non-Postgres
  owner, or more than one canonical Postgres instance fail closed. Multi-instance
  fuzzy completion is deliberately not advertised as an atomic backup.
- Export performs every tenant-table read inside one explicit PostgreSQL
  `REPEATABLE READ READ ONLY` transaction. The immutable manifest records the
  project/catalog identifiers, selected instance, transaction snapshot,
  snapshot WAL LSN, and snapshot start/read-completion timestamps.
- A `RUNNING` BackupRun row is persisted before object writes. Its
  `metadata_json` contains the exact backend, bucket, manifest key, project,
  catalog checksum, Postgres instance, and snapshot provenance; completion
  updates that same durable identity.
- GetBackup, RestoreTenant, and retention locate objects only through that
  durable run metadata. Legacy rows without immutable location metadata surface
  an explicit migration-required failure rather than guessing current defaults.
- Restore preflights exact project, catalog checksum, and canonical instance
  compatibility against both the run journal and manifest before writing rows.
- Retention verifies the manifest checksum, stops on the first provider error,
  preserves the manifest/journal for retry, deletes the manifest only after all
  table artifacts, and counts a run as pruned only when the journal delete
  actually returned a row.
- Named manual and scheduled policies now pass their stored object destination
  into the shared execution path (the previously prepared partial correction is
  included in this wave).

## Deliberate limits still open

- Export still materializes one whole tenant table before encryption/upload;
  bounded row framing, incremental AEAD, streaming multipart upload, and abort
  recovery remain tracked by
  `bug_report_14_8_26_v057_backup_export_unbounded_memory.md`.
- A project spanning multiple canonical Postgres instances is refused. A future
  coordinated watermark/snapshot protocol is required before that topology can
  be completed honestly.
- Restore data commit, completed journal transition, and outbox event are not
  yet one atomic transaction. The new pre-movement `RUNNING` record makes the
  outcome recoverable/diagnosable but does not by itself close reconciliation.
- BackupPolicy is tenant-scoped and does not yet carry a project ID, so scheduled
  multi-project policy enumeration remains a separate contract migration.

## Verification

- Targeted rustfmt: passed for all touched BackupService modules.
- `git diff --check -- src/runtime/service/backup_service`: passed.
- `cargo check --lib --no-default-features --features postgres -j 2`: passed
  twice, including the final formatted/staged source (existing warnings only;
  5m18s cold-ish and 2m05s incremental on the constrained local host).
- The targeted lib-test link was stopped after 10 minutes with no diagnostic
  while `rustc` remained active; no local test-pass claim is made from that
  attempt. GitHub CI is used for the test binary and full feature matrix.
- GitHub CI: will be the authoritative full Rust/all-feature validation after
  the current CDC run completes.
- Isolated `python scripts/generate-codebase-map.py --check` against exactly the
  staged CDC + Backup tree: passed. The generated map repair is included without
  staging the broader dirty-worktree variant.
- Live concurrent-writer, alternate-target restart, and two-project/two-instance
  workflows remain required before declaring the operational findings closed.
