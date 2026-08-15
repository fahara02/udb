# UDB v0.5.7 storage GC-intent readiness leaks across database lifetimes

Date: 2026-08-15
Status: correction implemented; GitHub CI verification pending
Affected surface: hard `StorageService.DeleteFile`, GC-intent worker, multi-database service instances

## Observed failure

Post-merge `main` CI run `31852794944` fixed the prior five CDC failures and
completed the IR stage with 22 passing tests. The broad native-service stage
then finished with 144 passed and one failure:
`live_postgres_storage_project_ownership_isolation`. Its cross-project hard
delete returned `INTERNAL` instead of `NOT_FOUND` because
`udb_storage.gc_intents` did not exist.

## Root cause

`StorageServiceImpl::ensure_gc_intents_table` guarded the operational ledger DDL
with one process-global static `OnceCell`. An earlier service instance created
the table and permanently marked the process ready. Live-test cleanup then
dropped every `udb_*` schema, but a later service instance skipped its DDL
because the global cell remained initialized.

The same ownership was unsafe beyond tests: the first database/pool used by a
process could mark the GC ledger ready for a distinct routed database that had
never received the table.

## Required correction

- Scope GC-ledger readiness to the `StorageServiceImpl` and its bound PostgreSQL
  pool, shared only by clones of that instance.
- Reset readiness whenever the builder binds a PostgreSQL pool.
- Keep DDL idempotent and fail closed; do not add an every-request catalog probe.
- Add a live regression that initializes one service, rebuilds all UDB schemas,
  initializes a second service, and proves the ledger exists.
- Verify the new regression, project-ownership test, pull-request CI, and the
  complete post-merge native suite through GitHub Actions only.
