# UDB v0.5.7 Vault database credentials are not tenant-bound at PostgreSQL

Date: 2026-08-14
Status: unsafe issuance disabled fail closed; tenant-bound credential feature remains unimplemented
Affected path: `VaultService.GenerateDatabaseCredentials`

## Summary

The request and lease are labelled with the verified tenant, but the generated
Postgres login is simply made a member of an operator-configured parent role.
The global role-alias configuration has no tenant/project allowlist, and the
login receives no fixed tenant/project session settings or other row-scope
restriction. Any tenant credential granted the RPC's method scope can request
any configured alias and receive the parent role's database privileges.

## Confirmed served path

- `UDB_VAULT_DB_ROLES_JSON` entries contain only global `role_name`,
  `parent_role`, and optional max TTL.
- Request authorization validates the body tenant and method scope, then selects
  the role by caller-supplied alias; no tenant/project binding is evaluated.
- Role creation executes `CREATE ROLE <login> ... IN ROLE <parent_role>`.
- No `ALTER ROLE ... SET app.current_tenant_id/project_id`, tenant-specific DB
  role, restricted proxy, or credential broker accompanies the membership.
- Lease `tenant_id` is bookkeeping only; it does not constrain what the returned
  username can query after connecting directly to PostgreSQL.

## Consequences

- A project/tenant-scoped Vault caller may obtain privileges intended for a
  different tenant or for cluster administration.
- RLS policies that depend on UDB-installed request settings may deny everything
  or be bypassed by parent-role attributes; neither result matches the labelled
  tenant credential contract.
- Audit rows can assert tenant ownership that the physical login does not enforce.

## Required correction

- Bind every dynamic role alias to explicit tenant/project selectors and maximum
  delegated privileges, fail closed on an unbound request, and prohibit
  BYPASSRLS/superuser/owner inheritance.
- Prefer a credential broker/proxy or tenant-specific least-privilege role that
  fixes trusted session scope server-side; do not rely on client-set GUCs.
- Preflight and continuously audit parent role attributes/grants before enabling
  issuance.
- Persist physical database/instance and effective policy revision on the lease.
- Add live negative tests showing a generated login cannot read another tenant or
  set/clear its own scope.

## 2026-08-14 correction

`GenerateDatabaseCredentials` no longer creates a PostgreSQL login. After the
existing seal, verified-tenant, alias-shape, authorization, and admission gates,
it returns a typed `FailedPrecondition` requiring
`tenant_bound_database_credential_authority`.

This is deliberate fail-closed behavior, not a claim that `ALTER ROLE SET
app.current_tenant_id` is sufficient: ordinary custom PostgreSQL GUCs are
caller-changeable and cannot be the immutable authorization boundary promised by
the lease label. The removed served path therefore cannot grant a global parent
role to a tenant caller.

The functional replacement remains open: a trusted credential broker/proxy or
database-native tenant-specific least-privilege role model, policy revision and
physical target persistence, continuous parent/grant audit, and cross-tenant
negative live tests. Until those exist, the endpoint is intentionally
non-serving rather than unsafe.

## Verification log

- Traced global role config, request selection, generated CREATE ROLE SQL, and
  lease schema.
- Added a unit guard asserting the typed fail-closed capability response.
- `cargo check --lib --no-default-features --features postgres -j 2` passed
  locally after the correction (warnings only).
- Focused Vault unit execution was terminated after the local linker remained
  CPU-bound for more than ten minutes; no test result is claimed. GitHub CI is
  pending for this wave; no production data was mutated.
