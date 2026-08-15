# Change note: v0.5.9 materialized-view project routing

Date: 2026-08-15
Release: 0.5.9

## Changed

- Materialized-view create and refresh resolve the canonical project PostgreSQL
  write target and execute on its pinned pool; they no longer use blank-target
  primary-read routing that could select the default physical database.
- The schema-authority resolver fails closed when a project has multiple
  eligible PostgreSQL write owners; migrations and materialized-view DDL cannot
  round-robin across physical databases.

- Materialized-view creation now leases the project-routed PostgreSQL write
  pool instead of executing against the process default pool.
- Per-view TTL refresh retains the originating project and resolves its routed
  write pool on every execution, revalidating the same instance used to create
  the view.
- Startup refresh is now a catalog-aware supervisor. Each pass reads the
  current exact project catalog set, incorporates later activation/reload, and
  refuses work while durable catalog authority is stale. It pins one write
  instance per exact project/catalog generation instead of load-balancing DDL
  between physical databases.
- Refresh diagnostics identify the project, schema, view, and consecutive
  failure count.

## Verification

- No local Cargo/build/test command was run, per operator direction.
- GitHub CI must run formatting, compilation, unit tests, and the focused live
  catalog/project-routing lanes.
