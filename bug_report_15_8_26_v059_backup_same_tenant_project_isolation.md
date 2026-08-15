# UDB v0.5.9 correction: Backup rows were not project-scoped

Date: 2026-08-15
Affected release: 0.5.8
Target correction: 0.5.9
Status: fixed in source; GitHub compile/unit/live verification pending
Affected paths: BackupRun/BackupPolicy schema, BackupService CRUD, export/restore, scheduled backup, retention

## Summary

Backup operations resolve an explicitly active request project and pin its
catalog and Postgres instance, but the durable BackupRun and BackupPolicy rows
carry no first-class project column. Their RLS policies, query filters, unique
keys, worker scans, and retention bounds are tenant-only. Two projects in one
tenant that share a physical native-service store can therefore see or mutate
each other's backup control state.

## Confirmed served path

- BackupRun and BackupPolicy declare tenant-only RLS/table security and have no
  project column. BackupPolicy is unique only by `(tenant_id, policy_name)`.
- run/policy Get/List/Delete builders filter by tenant and logical id/name only.
- PutBackupPolicy can reuse and overwrite another project's same-named policy.
- ListBackups/ListBackupPolicies return all same-tenant rows from a shared store.
- GetBackup checks project provenance only when `object_prefix` is non-empty, so
  RUNNING/FAILED/legacy rows can cross the project boundary.
- scheduled policy enumeration loses project identity and constructs a default-
  project context; most-recent-run and retention scans pool all project runs.
- one project's retention policy can select another project's encrypted backup
  objects and journal row for deletion.
- raw export SQL filters project-isolated customer tables only by tenant, so one
  project's artifact can contain another project's rows; restore freshness and
  row rewriting likewise omit the manifest-declared project column.

## Consequences

- Same-tenant project administrators can discover another project's backup runs
  and policies or overwrite/delete a same-named policy.
- a scheduled backup may run against the default project rather than the project
  that owns the policy.
- due checks and retention caps can be suppressed or triggered by another
  project's runs, including cross-project artifact deletion.
- routing projects to different physical stores masks the defect but also causes
  the default-store worker scan to miss non-default project policies entirely.

## Required correction

- Add non-null first-class `project_id` columns, combined tenant+project RLS,
  project table-security metadata, and tenant+project indexes/conflict keys.
- Persist and return project identity; include it in every run/policy read, list,
  upsert, completion, and delete operation.
- Resolve an explicitly active project before every policy operation and named
  policy lookup. Restore must read the source run inside that same project.
- Enumerate enabled policies per explicitly active project and exact native-store
  binding; carry project through due checks, scheduled contexts, retention scans,
  topology checks, and journal deletion.
- Quarantine legacy rows fail closed. The migration sentinel is blank, never
  `default`: BackupRun can be backfilled only from validated immutable metadata;
  BackupPolicy requires explicit operator mapping because no authoritative
  historical project exists.
- Add query-shape/schema unit guards and a served same-tenant/two-project live
  regression covering same-name policy CRUD plus RUNNING/FAILED run isolation.

## Implemented correction

- BackupRun and BackupPolicy now have non-null first-class `project_id`, combined
  tenant+project RLS/table security, project-leading indexes, and
  tenant+project logical conflict keys. Public run/policy views expose the owner.
- All policy/run reads, lists, writes, completions, deletes, restore lookups, and
  durable outbox payloads retain the resolved active project.
- Named-policy backup resolution happens only after the request project is
  explicitly activated and pinned. Export adds a manifest-derived project
  predicate to every project-isolated table; restore validates the manifest
  project-column contract, verifies each row's project, rewrites it to the
  pinned project, and performs project-scoped freshness probes.
- The leader-elected maintenance driver enumerates each active project's exact
  canonical Backup store, carries project identity through due checks and
  scheduled backups, and constrains retention selection, topology, object
  deletion, and journal deletion to that project. It refuses a partial scan if
  the global bounded policy cap is exceeded.
- Blank legacy rows remain non-authoritative: request and worker filters require
  a nonblank active project, and immutable run location decoding additionally
  requires the first-class project to equal `metadata_json.project_id`.
- Migration diff ordering now drops a changed same-name RLS policy before
  recreating it; this prevents the project-isolation policy upgrade from
  colliding with the still-live tenant-only policy. The destructive drop remains
  review-gated.
- Unit guards cover schema security/index contracts, explicit store filters and
  conflict authority, raw export/maintenance SQL, restore-row project rejection,
  RLS replacement ordering, and legacy/mismatched run provenance. The ignored
  served live regression uses one tenant and two active
  projects to prove same-name policy CRUD and run Get/List isolation, including
  quarantine of a blank-project row.
- Proto-derived OpenAPI and all checked-in Go/Python/TypeScript/PHP/Java/C#
  Backup contract stubs were regenerated with the repository's exact CI
  postprocessing sequence.

## Verification log

- Source audit completed against commit `51286ed93ff989ddacbae16f4d738ac999ebd321`.
- `buf lint` passed.
- Direct `rustfmt --check` passed for every edited Rust source (without Cargo).
- `buf generate --include-imports`, `openapi-postprocess.mjs`, and
  `sdk-codegen-postprocess.mjs` completed; `git diff --check` passed.
- Local Cargo/build/test execution is prohibited for this correction; GitHub CI
  is the authoritative compile and live verification path.

Required CI:

- `.github/workflows/ci.yml`: `quick-gate`, Rust build/unit jobs, and
  `Proto (buf)` generated-contract drift.
- `.github/workflows/live-quick.yml` dispatch filter:
  `live_postgres_backup_same_tenant_project_isolation`.
