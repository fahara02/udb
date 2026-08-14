# UDB v0.5.7 post-merge CDC live fixtures bypass startup initialization

Date: 2026-08-15
Status: correction implemented; GitHub CI verification pending
Affected surface: CDC/auth event live tests and general native-service live-test bootstrap

## Observed failure

Post-merge `main` CI run `31849518924` passed the 22-test IR compiler live
golden stage, then the broad native-service live stage reported 140 passed and
5 failed. The failed CDC/auth/notification tests either retained their outbox
row, missed the journal/DLQ row, or received `Unavailable` before validating a
malformed replay cursor. The same stage also repeatedly logged that
`udb_system.udb_cdc_lock_log` did not exist.

## Root cause

The CDC topic-policy hardening intentionally changed `CdcEngine::new` to install
an unavailable policy snapshot. Served startup calls `load_topic_policies`
before exposing or starting the engine, but five direct-engine live fixtures
still called publish/stream methods immediately after construction. Those
operations correctly failed closed before reaching the behavior each test was
meant to verify.

Separately, the general native-service live fixture drops every `udb_*` schema
and reapplies only native proto DDL. It did not restore the shared system
catalog used by singleton worker leases, unlike served startup and the native
auth live fixture.

## Required correction

- Make every affected direct-engine fixture load one complete topic-policy
  generation before publishing or streaming.
- Keep `CdcEngine::new` fail closed; do not make an unloaded policy snapshot
  look available.
- Recreate the system catalog after general native-service fixture cleanup and
  native DDL application, before worker hosts start.
- Verify the focused CDC/event live paths and the full post-merge CI suite in
  GitHub Actions. No local Cargo build or test is permitted for this correction.

## Regression evidence

Reverting the policy-load calls makes the five tests stop at the unavailable
snapshot. Reverting system-catalog bootstrap returns the missing singleton-lock
relation errors after fixture cleanup. Both changes exercise real PostgreSQL
and Kafka paths rather than an in-memory substitute.
