# UDB v0.5.11 correction: cross-tenant restore reused numeric identities

Date: 2026-08-16
Affected release: 0.5.10
Target correction: 0.5.11
Status: fixed in source; GitHub compile/unit/live verification pending
Affected path: `BackupService.RestoreTenant`

## Failure

Release benchmark run `31941904203` restored a tenant while the source tenant
remained in the same PostgreSQL database. Restore failed on
`udb_authz.policy_tuples_pkey`: the exported `BIGSERIAL` `policy_tuple_id` was
inserted unchanged into the target tenant and collided with the live source row.

The remap pipeline handled only JSON strings and generated replacements only
for UUID/character/text columns. PostgreSQL exports integral columns through
`row_to_json` as JSON numbers, so numeric primary keys were silently skipped.
The parent-FK remapper likewise recognized only string values.

## Security and integrity impact

- Cross-tenant restore was unavailable for backups containing a globally unique
  numeric identity while the source row remained present.
- Retrying could not succeed because the collision was deterministic.
- Using `ON CONFLICT`, overwriting the source row, deleting the source, or
  accepting an operator-provided sequence name would violate source/target
  isolation and is not an acceptable correction.
- A replacement identity without a durable old-to-new map would leave child
  foreign keys pointing at the source identity or fail their constraints.

## Correction

- Unique-column authority is inspected from the exact live PostgreSQL catalog
  through the restore transaction.
- Integral unique values are remappable only when `pg_depend` proves that a real
  sequence is owned internally/automatically by the exact table column. The
  sequence name comes from `pg_catalog`, is passed as a bound `regclass`, and
  `nextval` executes inside the restore transaction.
- Missing sequence ownership, missing catalog columns, incompatible scalar
  shapes, and unsupported unique types fail closed with a topology refusal.
- Remaps retain JSON scalar types and separate numeric/text key namespaces.
  Numeric parent identities therefore rewrite numeric child foreign keys using
  the same durable in-operation old-to-new map.
- Restore continues to use plain `INSERT`; there is no conflict overwrite or
  source deletion. PostgreSQL sequence increments may leave safe gaps if the
  transaction rolls back, but allocated values are concurrency-safe and never
  reused as target authority.
- Empty backup artifacts do not demand a remap authority because no value will
  be inserted.

No proto, generated contract, or schema migration changed.

## Regression coverage

- Pure unit coverage proves numeric and textual map keys cannot alias, numeric
  child FK values remain numbers, and unowned integral identities fail closed.
- Ignored live regression
  `live_postgres_backup_restore_remaps_owned_bigserial_identity` drives the real
  StartTenantBackup and RestoreTenant handlers against PostgreSQL and MinIO,
  keeps the source `PolicyTuple`, restores into a fresh target tenant under an
  explicit unbound platform authority, and proves the target gets a different
  sequence-owned identity.

## Verification posture

No local Cargo, build, test, rustfmt, or codegen command was run. Static diff,
SQL placeholder, call-site, and whitespace checks were performed. Required CI:

- `.github/workflows/ci.yml` quick/unit/native live gates.
- `.github/workflows/live-quick.yml` with filter
  `live_postgres_backup_restore_remaps_owned_bigserial_identity`.
