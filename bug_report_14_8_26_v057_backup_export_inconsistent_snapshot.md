# UDB v0.5.7 backup export is not a coherent database snapshot

Date: 2026-08-14
Status: source correction implemented; compile/CI and live concurrent-writer proof pending
Affected path: `BackupService.StartTenantBackup`

## Summary

Tenant tables are exported through independent autocommit SELECT statements.
There is no transaction, repeatable-read snapshot, LSN/fence, or application
write barrier spanning the table set. Concurrent writes can therefore make one
COMPLETED backup contain mutually inconsistent table versions even though every
artifact checksum verifies.

## Confirmed served path

- `run_tenant_backup` receives a `PgPool`, not a transaction/connection snapshot.
- It loops over the purge plan and calls `fetch_all(pool)` separately for each
  relation.
- Artifact upload happens between table reads, extending the window during which
  later tables can observe a newer database state.
- The manifest records only counts/checksums and `fk_ordered`; it carries no
  PostgreSQL snapshot, WAL LSN, start/end fence, or consistency warning.
- The run is always journaled as `COMPLETED` when all individual statements and
  uploads succeed.

## Consequences

- Parent/child, ledger/balance, workflow/saga, and security state can be restored
  from different points in time.
- Integrity verification proves bytes were not changed after export, not that
  the bytes ever represented one valid database state.
- A DR test can pass handler-level checks and still restore invariant-breaking
  data.

## Required correction

- Export all Postgres tables under one explicit repeatable-read, read-only
  transaction/snapshot, with a documented strategy for long snapshots and WAL
  pressure.
- For multi-instance topology, coordinate a fence/snapshot watermark per
  participant and record whether the result is atomic or intentionally fuzzy.
- Persist catalog checksum, snapshot identifiers/LSNs, and start/end timestamps
  in the immutable run manifest and journal.
- Validate restored cross-table invariants before commit where feasible.
- Add a live concurrent-writer test that would produce split parent/child state
  under autocommit and proves the corrected snapshot cannot.

## Verification log

- Traced the entire per-table read/upload loop and found no encompassing database
  transaction or snapshot export/import mechanism.
- Correction now establishes one explicit PostgreSQL `REPEATABLE READ READ ONLY`
  transaction before the first table read and uses it for the complete table
  set. The manifest/journal record the transaction snapshot, WAL LSN,
  project/catalog checksums, concrete Postgres instance, and start/read-complete
  timestamps.
- Multi-instance projects are refused rather than mislabeled atomic; coordinated
  multi-instance snapshot support remains open.
- Targeted rustfmt and diff checks pass. Compile/CI and the required live
  concurrent-writer proof are pending; this report is not operationally closed.
