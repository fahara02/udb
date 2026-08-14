# UDB v0.5.7 PublishCDC keeps obsolete authorization for the stream lifetime

Date: 2026-08-14
Status: correction implemented; CI/live revocation verification pending
Affected path: `DataBroker.PublishCDC`

## Summary

`PublishCDC` authenticates and authorizes only while opening the server stream.
The returned stream can then run forever using cloned scopes, tenant/project
text, topic matcher, and the topic-policy snapshot captured at subscription
time. It never revalidates token expiry or revocation, user/tenant suspension,
project removal, or a new deny policy. An already-open customer stream therefore
continues receiving newly published events after its access has been withdrawn.

## Confirmed served path

- `publish_cdc_inner` calls `security_from_request` and `authorize` once before
  it constructs the response stream.
- It passes cloned `security.scopes`, `tenant_id`, and `project_id` into
  `CdcEngine::stream_cdc`; no credential or authorization handle survives.
- `stream_cdc` snapshots the active topic strings from `self.topic_policies` and
  captures all of those values inside a `'static` stream closure.
- Both journal replay and the permanent broadcast/journal loop filter only with
  those captured strings. Neither path calls authentication, revocation, tenant
  gating, catalog/project existence, or the current Authz snapshot again.
- `SecurityContext` itself does not retain the verified JWT expiry, so even a
  cheap expiry deadline cannot be derived after the handler returns.
- There is no maximum stream lifetime that forces periodic reconnect and fresh
  authentication.

## Consequences

- Revoking an API key/token or suspending a user/tenant does not stop an open CDC
  data feed.
- Removing a policy or project can deny new connections while old connections
  keep receiving events under the retired authorization world.
- Policy rollout behavior differs by connection age and broker replica, making
  incident containment and access audits unreliable.

## Required correction

- Bind each stream to a revocable credential/principal identity and token expiry.
- Re-check the current credential/tenant/project gate and current authorization
  decision periodically and on policy/revocation generation changes; terminate
  with `UNAUTHENTICATED` or `PERMISSION_DENIED` when access is no longer valid.
- Refresh topic-policy ownership from a versioned shared snapshot rather than
  capturing a startup vector for the stream lifetime.
- Define a bounded maximum connection age as a backstop, shorter than the
  revocation-detection objective.
- Add served tests that open a stream, revoke the token/API key, suspend the
  tenant or remove the allow policy, publish another matching event, and assert
  that no post-revocation event is delivered.

## Verification log

- Traced `publish_cdc_inner` through `CdcEngine::stream_cdc`, its initial replay,
  broadcast fast path, and durable journal polling loop.
- Verified credentials now retain their expiry, every stream has a bounded
  server lifetime, and reconnect re-runs credential/revocation/tenant/project
  and method-authorization gates.
- The stream also reads the current atomically reloaded topic-policy generation
  on every replay/live cycle and terminates after a disable, ownership change,
  or policy-store failure.
- PostgreSQL and Kafka-enabled library checks passed; focused credential and
  policy revocation tests passed. Served token/API-key revocation and live
  Postgres/Kafka tests are delegated to CI and remain pending.
