# UDB v0.5.7 change note: Storage project ownership wave

Date: 2026-08-14
Status: source complete; GitHub CI pending

## Changed

- Bound every native Storage file lookup and list to the verified caller
  project when one is present, while retaining intentional tenant-wide access
  for tenant-scoped credentials.
- Made `RegisterUpload` persist claim-first project authority so omitting the
  body field cannot create an unowned tenant-wide record.
- Preserved tenant-only physical placement and native context semantics;
  project ownership is a logical authorization predicate, not a store-routing
  change.
- Bound finalize, URL mint/reissue, byte streaming, get, update, soft delete,
  hard delete, and list to the same shared ownership predicate.
- Protected HARD-delete idempotency replay and race-winner resolution from
  cross-project outcome disclosure.

## Regression coverage

- Added pure filter-shape coverage proving project predicates are included for
  scoped callers and omitted for intentional tenant-wide callers.
- Added an ignored live Postgres two-project served-path regression covering
  every Storage capability class and the tenant-wide compatibility case.

## Verification

- Fast source/call-site audit and `git diff --check`: passed.
- Local Cargo build/test: deliberately not run because the operator required
  CI-only compilation and testing due local hardware limits.
- GitHub CI: pending after the isolated commit is pushed.
- Initial CI run `31824506981`: compile/test lanes started, while `quick-gate`
  found one import-wrap-only `cargo fmt --check` difference. The exact emitted
  diff was applied without running local Cargo; replacement CI is pending.
