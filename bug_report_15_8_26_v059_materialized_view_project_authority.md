# Bug report: materialized-view lifecycle bypassed project database authority

Date: 2026-08-15
Affected release: 0.5.8
Target correction: 0.5.9
Severity: critical project-store isolation boundary

## Observed

`CreateMaterializedView`, the per-view TTL refresher, and the startup refresh
scheduler executed against `DataBrokerRuntime`'s default PostgreSQL pool. The
request already carried a canonical project, and customer project catalogs
could be hydrated correctly, but neither creation nor refresh resolved that
project's routed PostgreSQL write authority. Startup also captured only the
default manifest, so later project activation and replica reconciliation never
updated the refresh set.

## Impact

- a customer project could create or refresh a same-named view in the default
  database instead of its canonical project store;
- a catalog activated after broker startup never gained scheduled refresh;
- a removed or superseded catalog could leave its captured refresh loop alive;
- multi-project deployments could report healthy catalog authority while the
  materialized-view subsystem continued using stale/default authority.

## Required correction

Resolve the exact project's healthy PostgreSQL write instance before materialized
view DDL or refresh, honor and revalidate an explicit instance selector, and
execute against that pinned pool. A blank target must not collapse to the
literal `primary` instance. Schema authority must also reject multiple eligible
write owners instead of round-robin selecting different physical databases.

- Route materialized-view DDL and refresh as writes using the request/project
  context and the runtime's project-aware PostgreSQL selector.
- Resolve the exact active project catalog on every scheduled pass; do not
  capture the default manifest at startup.
- Pin the selected PostgreSQL instance for the created view and for each exact
  project/catalog generation; a weighted router must not create on one write
  instance and refresh on another.
- Skip all scheduled work while catalog authority is stale and preserve
  project identity in failure telemetry.
- Keep the legacy direct refresh helper explicitly scoped to the `default`
  project rather than allowing it to become an implicit customer fallback.

## Verification

No local Cargo, build, or test command was run by operator direction. GitHub CI
must compile all targets and exercise project-routing and catalog-authority live
coverage before release.
