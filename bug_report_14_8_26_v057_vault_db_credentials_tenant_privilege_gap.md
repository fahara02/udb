# UDB v0.5.7 Vault database credentials are not tenant-bound at PostgreSQL

Date: 2026-08-14
Status: tenant/project-bound read-only PostgreSQL credential authority implemented; GitHub CI pending
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

## 2026-08-15 functional correction

`GenerateDatabaseCredentials` now uses a database-native, direct-grant authority
instead of a global parent role:

- `UDB_VAULT_DB_ROLES_JSON` aliases must bind one exact tenant, project,
  canonical runtime instance, physical database, declared policy revision, and
  a bounded relation list with explicit tenant/project columns. Legacy
  `parent_role` entries and unknown fields are rejected.
- The served handler first resolves and pins the active project through
  `resolve_project_store`; the alias selectors must match that verified context
  before role DDL begins, and `current_database()` must match the configured
  physical database.
- The first production-safe capability is intentionally read-only. Only
  `SELECT` is accepted. The generated role is `NOINHERIT`, `NOBYPASSRLS`, has no
  administrative attributes and no memberships, receives direct relation
  grants, and is audited after creation.
- Each allowed relation is required to expose the configured tenant/project
  columns and a permissive read policy. Issuance enables and forces RLS, then
  adds a per-login `AS RESTRICTIVE` policy with fixed tenant/project literals.
  Role-level `app.current_*` settings remain compatibility defaults, but a
  caller changing them cannot relax the restrictive predicate.
- Issuance refuses databases with PUBLIC data privileges or non-extension
  PUBLIC `SECURITY DEFINER` functions, which could otherwise provide an
  authority path outside the explicit relation list.
- Lease metadata records the canonical instance, database, server address and
  port, declared policy revision, SHA-256 of the effective alias configuration,
  exact relations, and generated policy names. The legacy `parent_role` lease
  field is stored empty because no parent membership exists.
- Cleanup now removes every policy that references the generated role, drops
  direct grants, and only then drops the login, so expiry/revocation does not
  strand a role behind RLS dependencies.

An ignored live PostgreSQL test invokes the actual `VaultService` trait method,
connects using the returned username/password, proves only the bound row is
visible, changes both caller-controlled GUC hints to foreign scope, and proves
foreign-tenant and foreign-project rows remain invisible. The main native-live
job supplies the exact authority config; `live-quick` includes the focused
`vault_db_credentials_live` filter.

## Verification log

- Traced global role config, request selection, generated CREATE ROLE SQL, and
  lease schema.
- Added parser/binding guards for legacy parent roles, unsafe privileges, and
  tenant/project/instance drift.
- Added the served-path live negative test and wired it into the GitHub
  native-integration and `live-quick` environments.
- Per operator instruction, no local Cargo build or test was run for the
  2026-08-15 functional wave. GitHub CI is pending; no production database was
  mutated.
