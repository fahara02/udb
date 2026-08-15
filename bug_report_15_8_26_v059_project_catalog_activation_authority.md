# Bug report: served catalog activation crossed project authority

Date: 2026-08-15
Affected release: 0.5.8
Target correction: 0.5.9
Severity: critical project-isolation and availability boundary

## Observed

`ActivateCatalog` and `RollbackCatalog` committed the requested project's row
in `udb_system.catalog_versions`, then loaded the process-global last proto
manifest and called the one-argument in-memory activation compatibility helper.
That helper always targets the `default` project. A customer project could
therefore have a durable `ACTIVE` row while the served catalog map still had no
entry for it.

The merge audit also found a shared claim-authority drift: broad action scopes
(`*`, `udb:*`, `udb:admin`, and `udb:auth:admin`) were classified as platform
authority even when the verified credential carried a concrete tenant/project.
The common body guard and API-key handlers therefore returned before comparing
the body or stored record with that claim, allowing a project-bound broad-admin
credential to act as if it were unbound platform authority.

The NotFound recovery path compounded the defect by consulting `active_for`,
which intentionally falls back to `default`; an unactivated project could be
reported as active even though authority-sensitive services correctly refused
it. Durable `RollbackCatalog` was also an alias for normal activation and could
only select `STAGED` rows, so it could not reactivate a prior `ROLLED_BACK`
catalog at all.

## Impact

- Backup, Vault, tenant purge, and other exact-project paths fail closed with
  `backup_project_catalog_not_active` after an apparently successful activation.
- Reads that accept the catalog manager's legacy fallback can observe the
  default project's schema instead of the requested project's schema.
- Rollback cannot restore a prior catalog and can return misleading recovery
  results.
- A durable/in-memory split survives retries unless the exact project row is
  explicitly reloaded and published.
- Catalog-neutral-looking metadata and migration RPCs accepted body project
  identifiers that were not consistently bound to the authenticated project.
- Migration planning selected a raw `ACTIVE` row without proving its binding,
  reload event, checksum kind, payload integrity, or compatibility evidence.
- The first exact-loader draft required non-empty binding and reload evidence,
  but did not prove that the binding evidence equalled the catalog evidence or
  that the reload row named the same version and checksum. A split/stale proof
  row could therefore satisfy the structural join without representing the
  exact transition being served.
- Migration planning introspected `information_schema` on the canonical system
  pool and apply executed PostgreSQL DDL on that same pool. A project routed to
  a different physical PostgreSQL instance could therefore receive a plan for,
  and mutations against, the control/default database.
- An unknown `project_routing_mode` token silently parsed as `permissive`, so an
  operator typo could authorize every customer project to use an unlabeled
  default backend instance.
- Startup hydration could mark authority fresh before the asynchronous reload
  listener had connected and subscribed.
- Capabilities, health, message-schema discovery, admin summary, and projection
  drift accepted body project selectors under generic tenant-admin scope. They
  could expose another project's catalog/control metadata, and health treated a
  raw unproven `ACTIVE` row as healthy authority.
- API-key get/list/create/update/revoke/rotate/usage/emergency paths reused the
  same broad-scope classification, so their new record-project checks could be
  bypassed by a project-bound admin or wildcard token.

## Required correction

- Load the exact durable `ACTIVE` catalog row by project after commit; never use
  the global proto-schema history for served activation.
- Stage and publish that row with `activate_catalog_for(project_id, ...)`.
- Add an exact catalog accessor and use it for authority decisions and fallback
  responses.
- Make durable rollback transition an explicitly selected `ROLLED_BACK` row to
  `ACTIVE`; reject an empty rollback selector so retries cannot toggle versions.
- Fail closed when durable stage succeeds but node-local staging fails.
- Keep `GetCatalogManifest` project-aware and refuse an unactivated project;
  bootstrap must supply its release manifest explicitly to `StageCatalog`
  rather than reading another project's default authority.
- Bind catalog, project, migration, capabilities, and health body project IDs
  to the authenticated project; require explicit platform authority for a
  cross-project operation.
- Centralize cross-tenant/project authority: explicit platform roles or the
  exact `udb:platform_admin` scope may cross a bound claim; generic admin and
  wildcard scopes remain action privileges inside any non-empty claim scope.
  Preserve their legacy platform-operator behavior only for deliberately
  tenant/project-unbound identities.
- Make migration planning consume the same proven exact ACTIVE loader used by
  hydration and serving, and fail instead of producing an empty plan when no
  proven authority exists.
- Require equality across catalog, binding, and reload transition provenance:
  catalog id, project, version, checksum, compatibility level, and compatibility
  evidence must all describe the same successful activation/rollback/reload.
  Apply the invariant in single-project reads, load-all hydration, transition
  baselines, upgrade generation, and both directions of the startup audit.
- Keep migration run/operation ledgers on the canonical system pool, but resolve
  the project's exact write-capable PostgreSQL instance for schema introspection
  and physical DDL. Persist the catalog id/checksum and a credential-free hash
  of target routing plus physical database identity; re-resolve and CAS-verify
  every value under the catalog transition lock before apply.
- Reject unknown routing-mode values during startup validation and make the
  runtime parser fall back to strict denial if validation is bypassed. Preserve
  only the documented empty/permissive aliases, strict aliases, and a
  non-empty `strict_with_default:<project>` form.
- Connect and subscribe the durable reload listener synchronously, then hydrate
  again before catalog-dependent workers or serving can begin.
- Derive capabilities, schema discovery, admin summary, health, and drift scan
  only from an exact active target. Report the manifest's verified semantic
  schema checksum, and make health fail when raw ACTIVE history disagrees with
  the proven binding/reload authority.
- Canonicalize an empty body/security project to the documented `default`
  authority, always compare an explicit body project with the resolved security
  project, and permit cross-project use only with the exact
  `udb:platform_admin` authority outside a verified transport claim.
- Compute manifest-integrity evidence from deterministic serialization of the
  decoded `CatalogManifest` at stage, readback, and legacy upgrade. Keep the raw
  request checksum as a separate public selector and the semantic DDL checksum
  as separate schema evidence.
- Make the error-detail posture gate follow the shared typed project resolver
  and current durable error seams instead of demanding deleted per-handler
  duplicates or obsolete in-memory activation failure messages.
- When activation or rollback changes the ACTIVE catalog, atomically advance
  the target row's current validation baseline and compatibility evidence with
  the exact binding and reload record. Idempotent replay must return its stored
  response without rewriting a later transition.

## Evidence

The v0.5.8 post-release benchmark run `31886239424` authenticated project
`00000000-0000-4000-8000-000000000057`. The release binary returned successful
identity bootstrap but Backup and Vault calls failed because that exact project
was absent from the in-memory active map. Source inspection identified the
default-project activation helper in both served success paths and the global
`load_last_manifest()` call immediately before it.

The first combined v0.5.9 CI build additionally found that catalog-startup
refactoring passed the owned `Arc<DataBrokerRuntime>` snapshot directly to the
CDC constructor, whose contract is a borrowed runtime. This was a compile-time
integration defect, not a reason to weaken either catalog freshness or CDC
startup ordering; the snapshot must stay alive locally and be borrowed for the
awaited constructor call.

The next CI library run compiled and passed 2,700 tests before exposing 12
related metadata/embedded failures. `CapabilitiesRequest`,
`HealthReportRequest`, and message-schema requests document an empty project as
context/default, but the shared resolver rejected empty security/body projects.
It also checked cross-project mismatch only while a verified-claim task-local
was installed, so trusted in-process coverage observed `FailedPrecondition`
instead of the required `PermissionDenied` for a mismatched bound project.

Focused live run `31895088437` then failed its first StageCatalog readback with
`catalog_provenance_invalid`. The stored integrity digest was calculated from an
untyped JSON `Value` before PostgreSQL JSONB persistence, then recalculated from
JSONB text after readback. That representation is not the durable canonical
contract; JSONB may normalize object/numeric representation even when the
decoded `CatalogManifest` is identical. The error-detail posture guard also
still required project-specific helpers and pre-reconciliation error strings
that had been replaced by the shared project resolver and durable transition
errors.

The next quick gate found two final stale migration tokens in the same posture
list: raw manifest load/parse operations no longer exist because migration
planning consumes the exact proven catalog record. The guard must instead pin
transaction begin, project authority lock, and exact catalog-id validation.

After typed-manifest integrity fixed initial readback, focused run
`31896157075` progressed through concurrent activation and rollback, then found
one remaining split: rollback stored its new compatibility evidence in the
project binding and reload record while the reactivated catalog row retained
its original stage-time baseline/evidence. The next StageCatalog correctly
detected that ACTIVE/binding mismatch.

Focused GitHub run `31897092671` passed after that correction, proving the full
serialized authority regression against PostgreSQL and Kafka on the pushed
revision.

After merge, push-only main run `31901655082` exercised the broader ignored
native suite and found two stale served-auth fixtures. The CDC authorization
lifetime and data-only API-key CRUD tests authenticated project `billing` but
constructed only the default catalog, so the production fail-closed check
correctly returned `catalog_project_not_active`. The fixtures must explicitly
stage and activate their served manifest for `billing`; default fallback must
not be restored.

Focused GitHub runs `31903188510` and `31903230512` passed the corrected CDC
authorization-lifetime and data-only API-key CRUD fixtures independently.

The subsequent main/fixture merge audit statically traced
`VerifiedClaimContext::is_cross_tenant_admin` into the common body tenant,
project, and owner guards and into the API-key service's duplicated predicate.
It confirmed that the existing project-isolation tests used ordinary data
scopes, while a project-bound `udb:auth:admin` emergency-revoke test already
expressed the missing negative contract and would reach the wrong failure path
until the authority predicate was corrected.

## Regression coverage

The ignored live Postgres regression
`live_postgres_catalog_authority_end_to_end` now covers the complete durable
authority sequence in one serialized fixture: repeated system-catalog startup,
stage replay and conflicting reuse, two-instance concurrent activation with one
`ACTIVE` winner, stale rollback replay without state toggling, restart hydration
on two independent broker instances, and a negative served-path assertion that
a project-scoped admin cannot stage another project's catalog. The same fixture
now asserts that migration plans persist exact catalog/physical-target
provenance and that superseding the catalog makes an approved stale plan fail
before any operation ledger row can leave `PENDING`.

Focused unit regressions cover all four broad admin/wildcard scopes on bound and
unbound verified claims, retain explicit platform-role/scope behavior, and drive
API-key create preflight, get, list filtering/paging, update, revoke, rotate, and
emergency revoke through the corrected project boundary. Local Cargo/test
remains disabled; CI is the required execution proof.

The final main reconciliation briefly over-corrected the duplicated API-key
project guard: it denied every nonempty foreign project after the tenant check,
including callers that the centralized predicate had verified through an
explicit platform role or `udb:platform_admin` scope. The project guard now
reuses that exact predicate. Bound broad action scopes remain project-local,
while genuine platform authority consistently crosses tenant and project.

The same final audit found that empty-tenant/global API-key records returned
before this predicate ran. Global records now require the centralized platform
predicate as well: explicit platform roles/scopes and deliberately unbound
operators remain valid, while tenant/project-bound broad admin claims are
denied even when they know a global key identifier. Async service regressions
exercise global-key get, update, revoke, and rotate denial plus an explicit
platform-scope positive read.

CI run `31906893924` compiled all targets and tests, then reported only two
stale tenant-service assertions: they still expected a tenant-bound
`udb:admin` scope to list or parent across tenants. The corrected regressions
keep that broad scope tenant-local and use explicit `udb:platform_admin` for
the cross-tenant positive path.
