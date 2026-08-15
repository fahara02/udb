# Bug report: v0.5.7 restore freshness counted its own journal

Date: 2026-08-15
Affected release: 0.5.7
Severity: critical restore availability / false fail-closed

## Observed

`BackupService.RestoreTenant` persisted its target-scoped `RUNNING` restore
journal before beginning the authoritative fresh-target transaction. The
freshness planner then inspected every tenant-owned relation, found that same
row in the descriptor-backed `BackupRun` table, and refused an otherwise empty
target with:

`restore target tenant already holds 1 row(s) (in ...backup_runs)`

The v0.5.7 post-release SDK benchmark reproduced the defect against the released
binary with a new random target tenant UUID.

## Impact

Every restore whose manifest includes the native backup journal can fail before
importing data, even when the destination tenant is genuinely fresh. Removing
the whole journal relation from freshness checks would be unsafe because an
older backup or restore run is evidence that the target has prior state.

## Root cause

The transaction-local freshness guard correctly moved after durable restore
journaling, but its query did not distinguish the current restore operation's
own bookkeeping row from pre-existing target state.

## Required correction

For the descriptor-resolved `BackupRun` relation only, exclude the current
`restore_id` from the target probe. Keep every older run visible to the guard and
leave every tenant-authored relation unchanged. Resolve the journal relation and
id column from the native proto model rather than hardcoding SQL identifiers.
