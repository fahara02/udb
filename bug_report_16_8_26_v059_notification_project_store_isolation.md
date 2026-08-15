# Bug report: v0.5.9 Notification project-store isolation

Date: 2026-08-16

## Severity

Critical release blocker.

## Problem

`SendNotification` could use a request-selected native store while Get, Retry,
ReportDelivery, delivery stats, templates, preferences, and the delivery worker
retained or acquired a startup/default Postgres pool. A same-tenant request could
therefore read or mutate another project's state, or split a log mutation from its
transactional outbox row. ReportDelivery validated only tenant identity and did
not bind its protobuf RequestContext project to verified metadata.

Physical routing alone was also insufficient: NotificationTemplate,
NotificationPreference, NotificationLog, and NotificationDeliveryAttempt did not
all carry first-class project ownership. Two projects sharing one Postgres database
could collide on template/preference/attempt conflict keys and delivery-time
opt-out/status operations.

## Root cause

- `NotificationServiceImpl` retained one optional pool selected with an empty
  project at service construction.
- raw SQL handlers used that pool independently from typed native dispatch.
- the worker scanned one arbitrary pool rather than all active project stores.
- unknown projects could inherit permissive default routing without an exact live
  catalog check.
- template, preference, and attempt row schemas/business keys were tenant-only or
  global, and raw mutation predicates omitted project.

## Resolution

- every RPC normalizes the request project, requires an exactly active catalog,
  resolves one project-selected Postgres authority, and pins its backend instance
  in the RequestContext;
- ReportDelivery and Retry bind protobuf/header project scope before store access;
- raw template/preference/attempt/retry/report transactions install tenant+project
  request-local RLS settings;
- template/preference/log/attempt reads, unique keys, upserts, resets,
  suppressions, delivery-time opt-out checks, and status transitions include
  project ownership;
- the delivery leader iterates active projects and each worker scan includes an
  exact `NotificationLog.project_id` predicate;
- log/attempt/outbox mutations remain in one transaction;
- blank legacy ownership remains quarantined and is never selected by serving
  paths.

## Regression proof

Added unit guards for ReportDelivery header/body mismatch, missing live catalog,
unknown-project default fallback, project-scoped template selection, preference
lookup, attempt reset, and suppression. Added an ignored real-tonic PostgreSQL
regression covering:

- two projects on two independently provisioned instances;
- two active projects sharing one physical PostgreSQL database;
- same tenant/user/event/channel template and preference isolation;
- project-owned logs, delivery attempts, stats, and transactional outbox rows;
- cross-project GetNotification denial.

No local Cargo/build/test was run because the task requires CI-only validation.

## Required CI proof

```text
cargo test --lib runtime::service::notification_service::tests
cargo test --features http-client --lib runtime::service::notification_service
UDB_LIVE_AUTH_TESTS=1 cargo test --lib served_notification_pins_all_paths_to_each_project_instance -- --ignored --nocapture
UDB_LIVE_AUTH_TESTS=1 cargo test --lib notification_events_live -- --ignored --nocapture
```
