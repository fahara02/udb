# UDB v0.5.7 backup restore false-failure and journal gap

Date: 2026-08-14
Status: partially corrected; atomic completion/reconciliation remains open
Affected service: `udb.core.backup.services.v1.BackupService`
Real client evidence: release direct-RPC only; no Ambulife restore workflow found

## Summary

`RestoreTenant` commits the complete cross-table tenant restore transaction
before it writes the BackupRun journal. If the later journal write fails, the RPC
returns an error even though tenant data is already restored. A safe client retry
then hits the fresh-target guard and is refused because the target is no longer
empty. No durable restore outcome or declared event identifies what happened.

The export path has the analogous orchestration gap: it uploads all encrypted
artifacts and the integrity manifest before writing its journal. A journal
failure returns an error and leaves an unindexed, retention-invisible backup
prefix. Both completed/restored events are separate best-effort inserts after
journaling.

## Confirmed served paths

- Restore verifies every artifact, begins one SQL transaction, inserts all rows,
  and commits it.
- Only after commit does it allocate a restore id and call `journal_run(...).await?`.
- A journal error propagates from the RPC after the irreversible tenant-data
  commit; `emit_event` and the success response are skipped.
- Retrying against that target fails `ensure_target_is_fresh_in`, so ordinary
  idempotent recovery is impossible.
- Export uploads table ciphertext objects and `manifest.json`, then journals;
  journal failure leaves valid objects with no list/retention record.
- `journal_run` is described as best-effort but returns an error, producing a
  false-failure contract rather than either best-effort success or atomic
  durability.

## Required correction

- Allocate a stable operation/idempotency id before movement and persist a
  STARTED journal record before side effects.
- For restore, commit restored rows and COMPLETED journal state in the same
  Postgres transaction, with the declared outbox event in that transaction as
  well. On restart, reconcile STARTED operations by target/idempotency key.
- For export, write a STARTED journal before object uploads, record the manifest
  only after all artifacts exist, atomically transition the journal to COMPLETED
  with its event, and let retention/recovery discover abandoned prefixes.
- Return the original completed outcome on a matching retry; reject only a
  different operation targeting a live tenant.
- Add injected journal/outbox failure tests on both sides of the restore commit
  and a crash/restart export reconciliation test.

## Verification log

- Source trace completed across export object writes, checksummed manifest,
  restore integrity/decryption, cross-table transaction, freshness guard,
  journal helper, event helper, and retention discovery.
- Export and restore now allocate and persist a `RUNNING` journal identity before
  object/data movement, including immutable source/target topology metadata.
  A later completion failure therefore leaves a durable recovery inventory
  instead of an unindexed prefix or wholly unexplained restored tenant.
- Data commit, COMPLETED transition, and outbox event are still not one atomic
  transaction, and no reconciler/idempotent retry contract is implemented. This
  report remains open pending those changes and injected-failure proof.
