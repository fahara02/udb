# UDB v0.5.7 PublishCDC keeps obsolete authorization for the stream lifetime

Date: 2026-08-14 (implementation follow-up 2026-08-15)
Status: full credential/policy lifetime correction implemented; GitHub CI pending
Affected path: `DataBroker.PublishCDC`

## Summary

`PublishCDC` authenticates and authorizes only while opening the server stream.
The returned stream can then run forever using cloned scopes, tenant/project
text, topic matcher, and the topic-policy snapshot captured at subscription
time. It never revalidates token expiry or revocation, user/tenant suspension,
project removal, or a new deny policy. An already-open customer stream therefore
continues receiving newly published events after its access has been withdrawn.

## Original confirmed served path

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

## Correction implemented

- The real Tower credential layer now attaches a redacted
  `CredentialRevalidator` containing the original bearer/API-key/certificate
  evidence. Secret values are never exposed by `Debug` or audit lineage.
- Bearers are rechecked through the same fully wired `AuthnServiceImpl` used by
  native auth: signature/expiry plus current user status, issuing session, JTI
  denylist, and typed service-account grant. API keys and certificate bindings
  are re-resolved through their canonical durable stores and current grants.
- A revalidated credential must retain its original credential type/id,
  subject, tenant, project, auth method, effective service identity, and scope
  set. Identity or scope drift closes the stream and requires fresh admission;
  this keeps the CDC engine's scope-derived privilege/topic filter aligned with
  the outer authorization context. Expiry and API-key rate-limit state are
  refreshed while the lineage remains stable.
- `PublishCDC` now rechecks credential state, the tenant suspension gate, and
  the latest shared Casbin `AuthzSnapshot` every five seconds. The same canonical
  baseline CDC read-scope gate used at stream admission is rerun after credential
  refresh, so scope attenuation cannot be hidden by an allow rule without a
  `required_scopes` clause. A revoked credential ends with `UNAUTHENTICATED`, a
  narrowed scope or withdrawn policy ends with `PERMISSION_DENIED`, and a
  temporarily unavailable credential store returns a retryable dependency
  status. The existing credential-expiry/channel deadline remains the
  maximum-lifetime backstop.
- The CDC engine already reads its atomically reloaded topic-policy generation
  during replay and live polling; disabling or moving topic ownership therefore
  terminates existing streams as well as rejecting new ones.
- DataBroker-only startup installs the same fully configured Authn validator as
  the full native listener without mounting native RPCs. Long-lived validation
  no longer depends on which listener topology was selected.

## Verification log

- Added a live served-path Postgres/Kafka test that opens real gRPC CDC streams
  through `CredentialResolveLayer`, then independently revokes the bearer's
  issuing session, revokes the API key, and withdraws the shared Casbin policy.
  Each stream must terminate within the recheck bound without delivering an
  event after the authority change.
- Added a unit assertion that retained revalidation evidence is redacted from
  debug output.
- Added a unit assertion that initial admission and periodic revalidation share
  the same mandatory CDC read-scope predicate.
- Added a unit assertion that scope comparison is order-insensitive but detects
  real narrowing/widening, which terminates the old authorization context.
- `git diff --check` passes for the implementation snapshot.
- Per user direction, no local Cargo build or test was run on the constrained
  workstation. Quick/full-feature and live Postgres/Kafka verification are
  delegated to GitHub CI; this report must not be read as CI-verified until the
  run links and conclusions are appended.
- GitHub CI run `31832101820` reached the Rust build matrix but its `quick-gate`
  stopped at `cargo fmt --check` with seven formatting-only diffs. Those exact
  CI-produced changes were applied in the follow-up commit; no local Cargo
  command was used. The same run's postgres-only slim build and Clippy job passed
  before the replacement push; replacement-run full compilation and tests remain
  pending.
- Replacement GitHub CI `31832696224` passed the complete standard matrix,
  including Linux/Windows all-target builds, test/bench compilation, the UDB
  library suite, slim postgres build, Clippy, SDKs, and drift guards.
- The first isolated live run `31833649489` compiled the live harness and started
  the real dependency stack, then failed before CDC admission because
  `DataBrokerRuntime::from_config` republished the test's default security block
  and erased the JWT verifier installed before token minting. The test now carries
  the same explicit signer/verifier config into runtime construction. A
  replacement isolated live run is pending; no stream-revocation pass is claimed
  from the failed run.
