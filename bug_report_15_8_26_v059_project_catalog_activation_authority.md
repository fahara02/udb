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
