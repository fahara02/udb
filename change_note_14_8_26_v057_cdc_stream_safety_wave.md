# UDB v0.5.7 CDC subscriber stream safety correction

Date: 2026-08-14
Status: implementation in progress

## Scope

This change wave corrects the served `DataBroker.PublishCDC` path for:

- credential/policy authorization that otherwise survives indefinitely;
- fair-admission permits, inflight metrics, and deadlines that otherwise end
  before the lazy response stream starts;
- unknown resume cursors that otherwise rewind to the Unix epoch;
- durable-journal SQL/decode failures that otherwise look like an idle stream.
- startup-only/fail-open topic policies and the ingress/publisher wildcard and
  project-scope matcher drift.

## Chosen contract

- A CDC subscription has a bounded server-enforced lifetime. The bound is the
  configured CDC channel timeout and is clamped to the verified credential's
  expiry. At the bound, the stream ends with an authentication error and the
  client must reconnect, which re-runs credential, revocation, tenant, project,
  and current authorization gates.
- The scoped channel permit and inflight accounting live inside the returned
  stream and are released on normal completion, error, timeout, or client
  cancellation.
- A supplied cursor must be a valid UUID and must resolve to a retained journal
  row. Unknown/pruned cursors return `NOT_FOUND`; journal dependency failures
  return a retryable error. No supplied cursor may silently select epoch.
- Journal rows are fully decoded before the cursor advances. SQL or decode
  failure terminates the stream instead of being logged-and-skipped.
- Topic policies load as one strict, immutable ArcSwap generation, include
  disabled rows so disabling the final policy remains deny-by-default, reload at
  `UDB_CDC_TOPIC_POLICY_RELOAD_INTERVAL_MS`, and publish generation/age/
  availability Prometheus gauges. Startup load failure keeps the CDC engine
  unavailable; runtime reload failure swaps an explicit fail-closed sentinel.
- Publisher, stream replay/live delivery, and ingress use the same wildcard and
  tenant/project scope predicates. Open streams re-check the current generation
  on every journal/broadcast cycle and terminate when a disable, ownership move,
  or reload failure invalidates their admission.

## Primary-source basis

- RFC 7662: an active token is unexpired, unrevoked, and valid for the protected
  resource; all applicable token-state checks must be performed.
- gRPC deadline/cancellation guidance: streams have no implicit deadline and
  server applications must stop long-running work when the call is cancelled.

## Verification

- `cargo check --lib --no-default-features --features postgres -j 2`: passed
  (warnings remain in the pre-existing dirty worktree).
- `cargo test --lib --no-default-features --features postgres -j 2
  cdc_stream_budget -- --nocapture`: 3 passed, 0 failed.
- `cargo test --lib --no-default-features --features postgres -j 2
  topic_policy -- --nocapture`: 1 passed, 0 failed.
- `cargo test --lib --no-default-features --features postgres -j 2
  stream_policy_ -- --nocapture`: 2 passed, 0 failed.
- `cargo test --lib --no-default-features --features postgres -j 2
  disabled_final_policy -- --nocapture`: 1 passed, 0 failed.
- `cargo check --lib --no-default-features --features postgres,kafka -j 2`:
  passed, including Kafka publisher and stream branches.
- The local Kafka test harness could not finish on the constrained workstation:
  Windows returned error 112 while archiving `aws-sdk-s3` after the E: drive
  reached 77 MiB free. `cargo clean -p udb` removed 10.0 GiB of regenerable
  build artifacts; the retried resource-heavy test was stopped at the user's
  direction and delegated to GitHub CI.
- Full-feature CI and live Postgres/Kafka verification remain pending. This note
  must not be read as fully tested until those results are appended.
