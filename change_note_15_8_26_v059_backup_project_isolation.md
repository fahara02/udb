# UDB v0.5.9 Backup same-tenant project isolation

Date: 2026-08-15
Release: 0.5.9
Status: implemented; GitHub compile/unit/live verification pending

## Why this changed

Backup already pinned table movement to an active project topology, but its
durable run and policy state remained tenant-only. On a shared Postgres native
store, two projects in the same tenant could collide on policy names, list each
other's state, influence scheduling, or prune each other's backups.

## Source changes

- Added first-class non-null project ownership, combined tenant/project RLS,
  project-aware indexes/conflict keys, and public project fields for BackupRun
  and BackupPolicy.
- Pinned policy/run CRUD and named-policy lookup to the explicitly active project
  and retained the project in run journals and outbox evidence.
- Scoped raw export SQL to the manifest project column when a table declares
  project isolation. Restore now validates the manifest column, project-scopes
  freshness probes, rejects foreign-project artifact rows, and rewrites the
  project field alongside the target tenant field.
- Enumerated enabled policies separately through every active project's exact
  canonical Backup store. Scheduled backups, most-recent-run reads, retention
  selection, object deletion, and journal deletion now retain that project.
- Added pure schema/query/provenance/raw-export/restore-row guards and an ignored
  served Postgres regression for one tenant across two active projects.
- Ordered same-name RLS policy replacement as reviewed drop-before-create so
  the migration can atomically replace the tenant-only predicate without a
  duplicate-policy failure.
- Regenerated the OpenAPI document and all checked-in Backup SDK contracts for
  Go, Python, TypeScript, PHP, Java, and C#.

## Migration posture

The new columns are non-null but use an empty migration sentinel so existing
rows never acquire authority in the `default` project by assumption. Every new
serving-path write supplies a resolved, explicitly active project. RLS, explicit
query filters, and maintenance scans exclude blank legacy rows.

- BackupRun rows may be backfilled only when `metadata_json.project_id` is
  present, valid, and agrees with the immutable topology metadata.
- BackupPolicy rows cannot be inferred safely from current data; operators must
  map them to a project explicitly or leave them quarantined/disabled.

## Verification

- Passed locally without Cargo: direct `rustfmt --check`, `buf lint`, Node syntax
  checks for both codegen postprocessors, and `git diff --check`.
- Completed the same generated-contract sequence used by CI:
  `buf generate --include-imports`, OpenAPI postprocessing, and SDK
  postprocessing.
- No local Cargo/build/test command was run; GitHub CI remains authoritative.
- Focused served verification:
  `gh workflow run live-quick.yml --ref <branch> -f filter='live_postgres_backup_same_tenant_project_isolation'`.
