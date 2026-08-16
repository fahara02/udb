# UDB v0.5.11 Backup numeric restore authority

Date: 2026-08-16
Release: 0.5.11
Status: implemented; GitHub CI verification pending

`BackupService.RestoreTenant` now allocates fresh PostgreSQL serial/identity
values when a cross-tenant restore keeps the source rows in the same physical
database. This closes the `policy_tuples` BIGSERIAL collision observed in the
v0.5.10 release benchmark.

The restore does not trust the proto type spelling, request data, or a configured
sequence name. It resolves the live column type and exact sequence ownership
from `pg_catalog` inside the restore transaction, binds that catalog-produced
name as `regclass`, and calls `nextval`. Integral unique columns without a
sequence owned by that exact column are refused fail closed. Unsupported unique
types and active-catalog/live-schema disagreement are also refused.

Old-to-new remaps now preserve JSON scalar types, so a numeric parent identity
also rewrites numeric child foreign keys. Source rows remain untouched, target
rows use normal inserts, and no conflict-overwrite path was introduced.

Coverage includes pure authority/FK mapping guards and the ignored live
`live_postgres_backup_restore_remaps_owned_bigserial_identity` regression over
the real Backup handlers, PostgreSQL, encryption, and MinIO. No proto/codegen or
database migration is required. Local Cargo/build/test/rustfmt/codegen was not
run; GitHub CI is authoritative.

PR CI run `31943894065` found formatting-only drift in the two Backup files and
published repair artifact `ci-rustfmt-repair-1` (artifact `9262760140`). The
artifact patch was applied verbatim; no local formatter was run. Compilation,
unit coverage, and the focused live regression remain CI-gated on the resulting
commit.
