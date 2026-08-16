# Bug report: Asset readback and Authz mutation scope authority

Date: 2026-08-16
Affected release: 0.5.10
Target correction: 0.5.11
Severity: benchmark/release-evidence blocker with tenant-isolation impact
Evidence: benchmark run `31941904203`, job `95152213168`, artifact `9262324159`, release SHA `b98c8be97c745b904d9922e7a4a84f246635a14e`

## Observed

The retained v0.5.10 SDK benchmark artifact reports the same four fatal rows in
both Go and Python:

- `AssetService/GetAsset` returned `NOT_FOUND` for the `asset_id` just returned
  by the successful seed `RegisterAsset` call.
- `AuthzService/DeletePolicyRule`, `DeleteRole`, and `RevokeRole` returned
  `INTERNAL`. The wrapped neutral-IR error was `tenant_scope_required` for
  `PolicyRule`, `Role`, and `UserRole`, respectively.

The failures repeat across independently implemented SDK harnesses and are not
caused by manifest ordering or randomization. The previously failing Authn
Logout/MFA/WebAuthn operations, Vault dynamic-database credential lifecycle,
and DataBroker ApplyMigration all report `OK` in Go and Python (and across all
four benchmark SDKs), so those earlier regressions are not reopened here.

## Root cause

`RegisterAsset` correctly resolved an effective project from the validated
bearer/header metadata when the request body's optional `project_id` was empty,
but persisted the raw empty body value as SQL `NULL`. `GetAsset` and
`ListAssets` correctly resolved the same verified project and added it to their
read filters. The returned id was therefore immediately invisible on the same
served project path, violating the declared method readback contract.

The three Authz request messages identify their target by UUID and rely on the
endpoint's required tenant metadata rather than repeating `tenant_id` in the
body. Their handlers discarded the request metadata and submitted neutral
mutations under `RequestContext::default()`. The neutral compiler correctly
refused to lower a tenant-scoped mutation without concrete tenant authority;
the handler then wrapped that validation error as `INTERNAL`.

## Correction

- `RegisterAsset` persists and emits the canonical project resolved in its
  native request context. A genuinely tenant-wide caller still persists a null
  project, while a project-scoped caller can immediately read the returned id.
- Authz identifier mutations recover the claim-first verified tenant through
  the shared native metadata helper and create a tenant-only compiler context.
  The same context scopes the Role soft delete and its UserRole cascade.
- Missing tenant authority fails closed with a typed permission denial. The
  Authz tables remain on their declared tenant-isolation contract; an
  `x-udb-project-id` header is not promoted into a project predicate for
  `Role`, `PolicyRule`, or `UserRole`.
- The benchmark sends the live project explicitly so all four SDKs exercise the
  project-scoped write/read path. The served live regression separately keeps
  the body project empty to prove the supported verified-metadata fallback.
  The stale UUID-column explanation is removed; the column is `VARCHAR(120)`.

## Regression coverage and acceptance

Focused live coverage proves:

- blank-body `RegisterAsset` plus a non-empty verified project is immediately
  visible through `GetAsset` and `ListAssets` on that project;
- the same asset is not visible to another project;
- same-tenant policy, role, and assignment deletion/revocation succeeds; and
- a foreign tenant cannot mutate any of those targets by UUID.

Required CI filters:

- `cargo test --all-features mutation_context_tests`
- `cargo test --all-features --lib live_postgres_asset_read_after_write -- --ignored --nocapture`
- `cargo test --all-features --lib live_postgres_authz_admin_crud_and_audit_lifecycle -- --ignored --nocapture`
- `cargo test --all-features --lib live_postgres_authz_role_policy_roundtrip -- --ignored --nocapture`
- the reusable SDK benchmark with Go and Python enabled, requiring `GetAsset`,
  `DeletePolicyRule`, `DeleteRole`, and `RevokeRole` to report `OK`.

No local Cargo command, build, test, formatter, code generation, workflow
dispatch, or benchmark was run. Verification before CI is limited to static
inspection and `git diff --check`.
