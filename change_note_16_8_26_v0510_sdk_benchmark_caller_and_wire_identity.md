# Change note: SDK benchmark caller and wire identity hardening

Date: 2026-08-16
Release: 0.5.10

## Changed

- The PHP live session now retains the authenticated principal subject and
  derives the same stable attribution UUID used by the served authz runtime.
- PHP fixture state separates verified caller identity from disposable target
  users. Authz role, assignment, policy, and governance seeds now use caller
  attribution while target-oriented request fields remain target-oriented.
- Authz seed failures retain their original gRPC provenance for dependent role,
  policy, and policy-draft identifiers.
- Known seed blocks and unknown body-hydration errors now produce fatal rows
  with positive timing and iteration evidence instead of aborting the complete
  PHP report. Per-iteration body factories are also exception-contained.
- TypeScript resolves one strict canonical generated path per measured RPC and
  writes its canonical service/method identity in all unary, CDC, first-response
  streaming, and stream-open sample paths.
- Focused regressions cover PHP caller/target separation, stable attribution,
  authz prerequisite evidence, and a TypeScript full-surface alias-to-wire
  bijection including the nine Cache and Embedding aliases observed in run
  `31919949691`.
- Platform authority is now rooted in one fixed, active, system/global role
  created only through the direct-Postgres offline bootstrap seam. Served mode
  rejects `--platform-admin`; tenant role CRUD, literal bindings, governance
  documents, and snapshot hydration cannot synthesize the reserved role.
- The central platform predicate is shared by method security and service-grant
  validation. `udb:platform_admin` is forbidden in requested or approved
  service grants, closing grant create/replace, API-key, certificate, and
  request-time resolution paths through the same fail-closed validator.
- The live workflow provisions a distinct platform benchmark identity. Go,
  Python, TypeScript, and PHP authenticate and verify it, use it only for the
  exact Analytics global reads, Backup restore, Tenant administrative purge,
  and Authz governance RPCs, and retain the ordinary principal for tenant Authz
  CRUD and self-purge. Seed actors/reviewers and request attribution are bound
  to the session that actually executes each call.
- Tenant-wide revoke retains the inclusive `iat <= cutoff` denial contract. A
  successful durable/Redis cutoff publication now holds the RPC response until
  a replacement token can be issued in a strictly later second; Redis failure
  remains fail-closed through the durable cutoff and does not report a false
  fresh-session guarantee.

## Verification

No local Cargo, PHP, Python, Node, build, lint, or test command was run, per the
CI-only verification direction. Static inspection and `git diff --check` are the
only local checks. GitHub CI must run:

```text
cd sdk/php && UDB_LIVE_SDK_TESTS=1 vendor/bin/pest tests/Live/GeneratedRpcSurfaceTest.php --filter "retains failed authz prerequisites and emits positive body-failure evidence"
cd sdk/php && UDB_LIVE_SDK_TESTS=1 vendor/bin/pest tests/Live/GeneratedRpcSurfaceTest.php --filter "manifest JSON body hydrates AuthzService create-policy-draft request"
cd sdk/php && UDB_LIVE_SDK_TESTS=1 vendor/bin/pest tests/Live/GeneratedRpcSurfaceTest.php --filter "derives PHP authz attribution from the verified caller without changing target IDs"
cd sdk/typescript && npm run bundle-proto
cd sdk/typescript && npx tsc -p tsconfig.test.json
cd sdk/typescript && node --test --test-name-pattern "benchmark samples retain canonical wire identities" dist-test/live-auth.test.js
cd sdk/typescript && node --test --test-name-pattern "benchmark platform routing is exact" dist-test/live-auth.test.js
cd sdk/php && UDB_LIVE_SDK_TESTS=1 vendor/bin/pest tests/Live/GeneratedRpcSurfaceTest.php --filter "routes only global benchmark RPCs to the platform identity"
```

The release acceptance proof is the complete benchmark workflow: four measured
SDKs each emit the current canonical surface exactly once, PHP never falls back
to a zero-row artifact, TypeScript has no missing or unexpected wire identity,
and the aggregate fatal count is zero before Pages can consume the evidence.
The CI-only security matrix must also exercise the Rust unit/live negative
tests for reserved roles and service-grant scopes plus the four-SDK live suite
with the separately bootstrapped platform identity.
