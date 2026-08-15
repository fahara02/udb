# Bug report: PHP benchmark seed failure aborted the complete RPC report

Date: 2026-08-15
Affected release: 0.5.8 benchmark verification
Severity: release-evidence integrity
Target correction: 0.5.9

## Observed

Benchmark run `31886239424` reached the PHP full-surface benchmark with the
project lacking an explicitly active catalog. `BackupService/StartTenantBackup`
could not create the `backup_id` fixture, but the raw PHP seed helper converted
the non-OK gRPC result to `null` without retaining or logging its status code and
details.

The reflective sweep later tried to hydrate a documented Backup request that
contains `<seed:backup_id>`. `phpResolveManifestSeeds()` correctly rejected the
missing value, but that exception occurred before the report writer. The PHP
report therefore became a zero-RPC harness-error fallback instead of a complete
381-RPC result, hiding the seed failure's original authority error and all later
PHP RPC outcomes.

## Root cause

- The raw seed helper returned `null` for every non-OK status instead of raising
  a status-bearing seed error.
- Fixture state could represent only successful values, not a known failed
  prerequisite and its provenance.
- Request bodies were constructed before the measurement error boundary, so a
  missing known prerequisite aborted the entire sweep.
- The shared result collector did not recognize a fatal `SEED_BLOCKED` harness
  status.

## Impact

A single prerequisite failure destroyed the complete PHP benchmark artifact and
replaced a precise server-side failure with a secondary missing-fixture
exception. The aggregate benchmark could not distinguish an uninvoked dependent
RPC from a harness crash, and later unrelated PHP RPCs were never measured.

## Required correction

- Retain and log the original gRPC code and details from failed raw seed calls.
- Record `backup_id` as blocked by `BackupService/StartTenantBackup` when that
  seed fails or returns an invalid successful response.
- Emit every manifest-dependent RPC as a zero-iteration `SEED_BLOCKED` failure,
  with the seed source and original status, while continuing unrelated RPCs.
- Keep unknown manifest refs fail-closed so the benchmark still detects fixture
  and documentation drift.
- Require one emitted sample for every reflected RPC and teach the shared
  collector that `SEED_BLOCKED` is a countable, fatal harness status.

## Acceptance evidence

The focused PHP tests cover status/detail retention, dependency-selective
blocking, and recovery after a later successful seed. The live benchmark must
emit the full current reflected surface (381 RPCs at this revision), including
seed-blocked Backup rows if the active-catalog prerequisite is still absent.
