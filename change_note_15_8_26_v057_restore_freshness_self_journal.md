# Change note: restore freshness excludes only its current journal

Date: 2026-08-15
Affected release: 0.5.7

## Changed

- `RestoreTenant` now identifies the native `BackupRun` relation through the
  descriptor-backed model.
- Its transactional freshness query excludes only the current restore operation
  id from that relation.
- Prior backup/restore journal rows still make a destination non-fresh; no tenant
  data relation and no older control-plane history is ignored.
- A unit regression pins descriptor-backed journal recognition and rejects a
  similarly named table or a relation in another schema.
- The generated codebase map now includes the new restore-journal classifier.

## Verification

- No local Cargo/build/test command is run, per operator direction.
- GitHub CI must compile and run the unit regression.
- The post-release SDK benchmark must drive `RestoreTenant` against a fresh UUID
  destination and record `OK`; its aggregate gate must remain at zero failed RPCs.

Because v0.5.7 is immutable and already published, release-binary evidence must
continue to identify this defect for v0.5.7; the correction belongs to the next
patch release rather than silently replacing the published asset.
