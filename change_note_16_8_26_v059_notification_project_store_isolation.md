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

CI run `31906253806` produced and supplied the applied repair artifacts:

- `ci-rustfmt-repair-1`, SHA-256
  `273DF3D519F091D3A434962BDBF673A9A55265078E9E1518CB40FBC44DCBB351`;
- `ci-sdk-codegen-repair-1`, SHA-256
  `03B6EE008A359B5D4C175E2B0E7E56BB311ACD31D18420295288F28D64E3FBF6`.

That run's slim compile and successor run `31906505380` identified and drove the
owned capability message/operation signature corrections documented in the
companion bug report. No local Cargo, build, test, code generation, or rustfmt
command was used for those repairs.

Focused live-quick run `31908446993` subsequently proved the served split-store
and shared-store paths reached their assertions, then exposed a fixture-only RLS
proof defect: its raw shared-database inspection reused the integration
superuser, which PostgreSQL permits to bypass even forced RLS. The fixture now
switches those deliberately project-unfiltered reads to a unique non-login,
non-superuser, non-`BYPASSRLS` role with least-privilege Notification read
grants, asserts the generated tables have enabled and forced RLS, and removes the
role and grants during teardown. Production Notification behavior is unchanged.

The focused `served_notification_pins_all_paths_to_each_project_instance`
live-quick filter remains CI-only and must be rerun after this fixture correction.
