# UDB v0.5.7 non-default backup objects cannot be rediscovered safely

Date: 2026-08-14
Status: source correction implemented; legacy migration and live restart proof pending
Affected paths: backup detail, restore, and retention

## Summary

Backup permits explicit object backend/bucket overrides and writes them only
inside `manifest.json`. The durable BackupRun row records the object prefix but
not the backend or bucket. Every later operation must fetch the manifest before
it can learn those values, yet it attempts that first fetch only against the
current process defaults. A successful backup written to an alternate target is
therefore invisible to GetBackup detail, unrestorable, and unsafe to retain.

## Confirmed served path

- Export resolves request overrides, writes ciphertext and the manifest to that
  selected target, but `journal_run` persists only prefix/checksum/counts.
- `BackupRun` has no object-backend or object-bucket columns.
- `GetBackup` builds the manifest GET with `svc.object_bucket` and
  `svc.object_backend`; on failure it silently returns the summary with empty
  table/exclusion detail.
- `RestoreTenant` likewise reads the manifest from service defaults. Only after
  parsing it does the code switch to the backend/bucket recorded inside it.
- Retention has the same bootstrap dependency and cannot discover alternate
  artifacts when the default-target GET fails.
- A later deployment default change breaks discovery even when the original run
  used what was then the default.

## Consequences

- The documented override can produce a completed backup that the public restore
  API cannot restore.
- Backup-detail responses hide location failure as empty detail.
- Retention can lose track of alternate-target objects, leaving encrypted tenant
  data outside the configured retention lifecycle.
- Disaster recovery depends on mutable process environment rather than durable
  run identity.

## Required correction

- Add immutable backend, instance (if applicable), bucket, project/topology, and
  manifest key fields to BackupRun and populate them before artifact writes.
- Resolve Get/Restore/Retention exclusively from the stored run target; treat
  missing legacy location as an explicit migration/operator condition.
- Validate that the selected target is an allowed backup destination and record
  a non-secret provider/version identifier.
- Return a retryable object-store error from GetBackup rather than empty detail
  when a manifest is expected but cannot be read.
- Add a live alternate-bucket backup/get/restore/retention test plus a restart
  test with changed process defaults.

## Verification log

- Traced target selection, manifest/journal fields, and every manifest bootstrap
  read.
- New runs persist backend, bucket, manifest key, project, catalog checksum, and
  canonical Postgres instance in BackupRun metadata before artifact writes.
- GetBackup, RestoreTenant, and retention use that location exclusively. A
  legacy row without it fails with an explicit migration-required policy status;
  mutable process defaults are no longer a locator.
- Alternate-target and changed-default restart workflows remain pending.
