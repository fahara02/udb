# UDB v0.5.7 backup retention drops references after failed object deletion

Date: 2026-08-14
Status: source correction implemented; provider-fault/live verification pending
Affected path: leader-elected backup retention

## Summary

Retention treats object and journal deletion as unrelated best-effort actions.
It deletes the manifest last even when one or more table objects failed to
delete, then attempts to delete the BackupRun row regardless. It also reports
the run as pruned whether the journal deletion succeeded. A transient provider
failure can therefore turn an over-retention backup into invisible orphaned
tenant ciphertext with no durable retry inventory.

## Confirmed served path

- `delete_run_objects` logs and continues after each failed table-object delete.
- It then attempts manifest deletion unconditionally, destroying the only list
  of remaining object keys even after partial failure.
- If manifest GET fails, it still tries default-target manifest deletion and
  returns without a failure status.
- `prune_tenant_backups` always calls `delete_run_journal_row` afterward; that
  helper logs and swallows failure.
- The caller increments `runs_pruned` unconditionally for the selected run, so
  worker output can claim successful pruning when objects and/or the row remain.

## Consequences

- Encrypted customer data can survive beyond its retention policy without any
  journal/manifest reference that later sweeps can use.
- Partial deletion cannot be deterministically resumed because the manifest may
  already be gone.
- Compliance metrics and operator logs overstate successful deletion.
- Repeated sweeps either cannot find the orphan or repeatedly attempt a row that
  was counted as pruned already.

## Required correction

- Persist a deletion state machine/claim on the run and retain the manifest plus
  journal until every listed object deletion is confirmed idempotently.
- Treat manifest/object access errors as retryable retention failures, not
  permission to erase ownership metadata.
- Delete the manifest only after all artifacts are absent; delete/finalize the
  run row atomically with a durable retention audit event.
- Return actual outcomes and metrics for selected, objects deleted, retrying,
  finalized, and permanently failed runs.
- Add provider-fault tests for manifest GET, one middle object, manifest DELETE,
  and journal finalization, proving the next pass can recover each state.

## Verification log

- Traced candidate selection, object/manifest deletion order, journal deletion,
  and outcome accounting.
- Retention now resolves the immutable run location, verifies the manifest
  checksum, stops on any object error, deletes the manifest only after every
  listed artifact, and deletes/counts the journal row only after object cleanup
  succeeds. Failed passes retain both recovery references for retry.
- Provider-fault injection and live object-store verification remain pending.
