# Change note: v0.5.9 Notification exact project authority

Notification persistence is now selected per verified tenant/project request
instead of retaining a startup/default Postgres pool.

## Changed behavior

- Get/List/Retry/Report/Stats/Template/Preference RPCs resolve the exact active
  project's physical native store.
- ReportDelivery rejects a protobuf project that differs from authenticated
  metadata and cannot relabel another project's delivery event.
- Notification workers enumerate active projects and scan/mutate only the
  selected project's rows.
- templates, preferences, logs, and delivery attempts have first-class project
  ownership, tenant+project RLS, and project-aware conflict/mutation predicates.
- template and preference raw upserts run with request-local RLS settings;
  delivery attempt, parent status, and outbox writes remain atomic.
- unknown projects fail closed instead of inheriting the default catalog/store.

Legacy requests without a project continue to name the explicit `default`
project. Blank ownership in pre-v0.5.9 rows is quarantined rather than served.

## Validation posture

Static diff checks passed. Local Cargo/build/test/rustfmt was intentionally not
run; the unit, `http-client`, and ignored live PostgreSQL filters recorded in the
companion bug report must run in CI.
