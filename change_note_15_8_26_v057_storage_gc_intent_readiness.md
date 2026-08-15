# Change note: storage GC-intent readiness lifetime

Date: 2026-08-15
Release: 0.5.7 follow-up

## Change

- Replace the process-global storage GC-ledger readiness cell with a cell owned
  by each `StorageServiceImpl`/PostgreSQL-pool binding.
- Share readiness across clones of the same service instance while preventing
  one database or a prior schema lifetime from authorizing another.
- Add a live PostgreSQL regression that rebuilds all UDB schemas between two
  service instances and requires the second instance to recreate
  `udb_storage.gc_intents`.

## Verification policy

No local Cargo build or test is run because the user requires CI-only
validation. GitHub Actions must pass the focused GC-ledger and storage project
ownership tests, full pull-request checks, and the complete post-merge `main`
workflow before `v0.5.7` is tagged or published.
