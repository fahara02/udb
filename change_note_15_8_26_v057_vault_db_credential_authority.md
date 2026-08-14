# UDB v0.5.7 change note — tenant-bound Vault database credentials

Date: 2026-08-15
Status: implemented; GitHub CI pending

## Changed

- Replaced the disabled `GenerateDatabaseCredentials` path with a read-only
  PostgreSQL credential authority bound to the verified tenant, project, active
  runtime instance, physical database, policy revision, and explicit relations.
- Removed global parent-role delegation. Generated logins have no memberships
  or administrative/RLS-bypass attributes and receive only direct `SELECT`
  grants.
- Added fixed-literal restrictive RLS policies per generated login. Custom GUCs
  are installed only as compatibility defaults and are not the authorization
  boundary.
- Added fail-closed target/relation/public-authority audits and persisted
  physical target plus effective policy SHA-256 provenance with the lease.
- Made role cleanup policy-aware so expiry/revocation can remove dependent RLS
  policies and direct grants before dropping the login.
- Updated the live SDK reset configuration from legacy parent roles to explicit
  tenant/project/database/relation bindings.

## Evidence added

- Unit guards reject legacy `parent_role`, non-SELECT delegation, and any
  tenant/project/instance binding mismatch.
- `vault_db_credentials_live_enforce_fixed_tenant_and_project_after_guc_change`
  invokes the served Vault RPC, connects with the issued credential, changes
  both scope GUCs to foreign values, and still cannot see foreign rows.
- GitHub `native-integration` supplies the authority configuration; the focused
  workflow filter is `vault_db_credentials_live` in `live-quick.yml`.

## Verification

No local Cargo build or test was run, per operator instruction. Compile, unit,
and live PostgreSQL execution evidence must come from GitHub CI.
