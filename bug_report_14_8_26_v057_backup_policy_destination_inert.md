# UDB v0.5.7 backup policy destinations do not control backup execution

Date: 2026-08-14
Status: partially corrected in this audit; remaining run-location durability open
Affected paths: manual and scheduled tenant backup

## Summary

`BackupPolicy` durably stores `object_backend` and `object_bucket`, and the public
`StartTenantBackupRequest.policy_name` promises to use those fields. Neither
execution path does. Manual backup never reads `policy_name`; scheduled policy
enumeration does not project the destination columns and deliberately passes
empty overrides, causing every scheduled backup to use process-wide storage
defaults.

## Confirmed served path

- The request proto says a non-empty `policy_name` selects that policy's object
  backend/bucket.
- `start_tenant_backup` validates tenant, builds context, and calls
  `run_tenant_backup` with only request `object_backend` and `object_bucket`.
  `req.policy_name` and `req.metadata_json` are never read.
- `PutBackupPolicy` persists `object_backend` and `object_bucket`, and Get/List
  return them, so the API makes the configuration appear active.
- `EnabledBackupPolicy`, its native model, and enumeration SQL omit both object
  target fields.
- `run_scheduled_backups_once` calls `run_tenant_backup(..., "", "", ...)`,
  explicitly selecting service defaults instead of the policy destination.

## Consequences

- Operators can configure and read back a compliance/residency backup target
  while artifacts are written to another bucket/backend.
- Manual callers naming a policy receive no error and no indication that it was
  ignored.
- Scheduled and manual executions of the same named policy can land in different
  locations based on unrelated process environment and request overrides.
- Request metadata is also discarded instead of being journaled for audit.

## Required correction

- Resolve a named policy tenant-safely before starting work; fail if absent,
  disabled, malformed, or incompatible with explicit overrides.
- Include destination fields in scheduled policy enumeration and pass the
  resolved target into the shared execution path.
- Define precedence and reject conflicting policy/request targets rather than
  silently choosing one.
- Persist selected policy ID/name, metadata, backend, and bucket in the run
  journal and manifest.
- Add served manual and leader-worker tests proving that named-policy artifacts
  reach the configured target and that an unknown/disabled policy fails closed.

## Correction applied in this audit

- Manual `StartTenantBackup` now resolves a non-empty policy name in the verified
  tenant/project context and uses its backend/bucket when explicit request
  overrides are absent; an unknown name fails with typed not-found status.
- Enabled-policy enumeration now projects backend/bucket, and the scheduled
  leader path passes those durable fields to the shared backup executor.
- The SQL-shape regression now requires both destination aliases.
- Request `metadata_json`, disabled-policy semantics, immutable run target fields,
  and live alternate-target workflow coverage were previously open under this
  report and `bug_report_14_8_26_v057_backup_object_target_unresolvable.md`.
- This wave now persists the resolved backend, bucket, and manifest key in the
  RUNNING/COMPLETED run journal before object writes. Request metadata and
  disabled-policy semantics still remain open.

## Verification log

- Traced the request fields, policy persistence/projection, and both callers of
  `run_tenant_backup`.
- No production data was mutated. Source correction is targeted-rustfmt and
  `git diff --check` clean. Both the all-feature and PostgreSQL-only targeted
  test invocations exceeded their bounded compile windows without emitting a
  diagnostic/result; live object-store proof remains pending.
