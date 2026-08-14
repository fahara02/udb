# UDB v0.5.7 tenant backup covers one default project/instance world

Date: 2026-08-14
Status: project-bound fail-closed correction implemented; CI/live topology proof pending
Affected paths: `BackupService.StartTenantBackup`, `RestoreTenant`, scheduled backup

## Summary

Backup is advertised as a tenant-wide movement operation, but its raw table scan
and restore are bound at service construction to one Postgres pool and one
default catalog manifest. The pool is resolved with an empty project and the
manifest is `DataBrokerService.manifest`, not the caller project's active catalog
or an inventory of all tenant projects/instances. A tenant whose data is routed
to another project Postgres instance can receive a COMPLETED backup that never
read that data; restore writes only into the same captured default pool.

## Confirmed served path

- `build_backup_service` calls
  `native_store_pool_for_service("backup", true, "")`, permanently selecting an
  instance with an empty project routing key.
- It copies `self.manifest` into `BackupServiceImpl` once. It never calls
  `catalog.active_for(authenticated_project)` and is not refreshed on catalog
  activation/runtime reload.
- `run_tenant_backup` enumerates `plan_tenant_purge(svc.manifest)` and executes
  every raw `SELECT` on `svc.pg_pool`, regardless of request project, target
  instance, or where normal entity writes were routed.
- `RestoreTenant` begins its cross-table transaction on the same captured pool.
- Scheduled backups construct a context with tenant only, so they necessarily
  use the same default catalog/instance world.

## Consequences

- A backup can be reported complete while omitting customer rows stored on a
  project-routed Postgres instance.
- Catalog activation can add or change tenant tables while backup continues to
  enumerate the retired startup manifest.
- Restore can populate a different database from the one the target project
  actually serves, producing a false successful disaster-recovery exercise.
- Counts, checksums, and encryption prove only the subset scanned; they cannot
  reveal a missing instance or table world.

## Required correction

- Define the tenant-wide topology contract explicitly: enumerate every project
  and canonical data instance that may own the tenant, or require a project-bound
  backup and persist that project as part of the run identity.
- Resolve the current catalog and data pool(s) at operation start, not service
  construction, and record catalog checksum plus instance identities in the
  manifest/journal.
- Reject a backup if topology discovery is incomplete or any required instance
  cannot participate; never silently fall back to default.
- Restore to the recorded/resolved target topology with a preflight proving that
  all destination instances and catalog versions are compatible.
- Add a live two-project/two-instance test that writes distinct rows through
  normal routing and proves backup/restore includes exactly the intended worlds.

## Verification log

- Traced service construction, raw export queries, restore transaction creation,
  scheduled context construction, and catalog ownership.
- Backup/restore now resolve the explicitly active request project, its current
  catalog, and one concrete canonical Postgres write instance at operation
  start. Unknown-project default-catalog fallback, ambiguous/non-Postgres write
  owners, and multi-instance fuzzy snapshots are refused.
- Restore requires the recorded project, catalog checksum, and instance to match
  its operation-start topology before any row write.
- The contract is now explicitly project-bound. Scheduled policy records still
  lack a project field, and live two-project/two-instance proof remains pending.
