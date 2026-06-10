# Auth Runbooks

This page is the operator runbook set for UDB authentication and authorization.
Use it with `docs/operations.md`, the generated auth RPC inventory, service
logs, audit events, and the broker metrics endpoint.

## 1. Auth Service Readiness Fails

Symptoms: the broker is healthy enough to answer process checks, but auth
readiness reports unavailable or auth RPCs return `UNAVAILABLE`.

1. Check the auth service readiness response and broker logs for the failing
   dependency name.
2. Verify Postgres connectivity and migrations for authn/authz native tables.
3. Confirm required signing and hash secrets are present in the process
   environment.
4. If readiness fails after a deploy, compare the current descriptor contract
   with the generated inventory in `docs/generated/`.
5. Restore the last known-good config or roll back the deploy if readiness does
   not recover after dependencies are healthy.

## 2. Login Failures Spike

Symptoms: password, OTP, WebAuthn, or token login attempts fail above normal
baseline.

1. Split failures by credential type, tenant, project, and reason code.
2. Confirm upstream IDP and SMTP/SMS/OTP dependencies are reachable.
3. Check for clock skew on broker nodes if OTP, token expiry, or WebAuthn
   assertions fail unexpectedly.
4. Inspect recent auth config changes, especially MFA policy, password policy,
   IDP mapping, and tenant restrictions.
5. If the spike is attack-shaped, enable tenant-level throttling and preserve
   audit exports before mitigation.

## 3. MFA Enrollment Or Challenge Failure

Symptoms: users cannot enroll MFA factors, generate challenges, or complete
MFA verification.

1. Confirm the affected tenant's MFA policy allows the requested factor type.
2. Check whether challenge records are being written and whether expiry windows
   are reasonable.
3. For TOTP, verify device time skew and recovery-code fallback.
4. For WebAuthn, verify relying-party ID, origin, and TLS termination settings.
5. Re-issue enrollment only after confirming the previous factor state is not
   partially persisted.

## 4. Session Revocation Does Not Propagate

Symptoms: revoked sessions or token families continue to authenticate on one or
more nodes.

1. Check revocation audit events and the token-family state for the affected
   subject.
2. Verify all broker nodes receive revocation invalidation events.
3. Compare node clocks and token expiry windows.
4. Inspect metrics for revocation propagation latency and stale cache hits.
5. Restart only the stale auth node if cache invalidation is isolated to that
   process; otherwise pause issuing new sessions until propagation is restored.

## 5. JWT Signing Key Rotation Incident

Symptoms: newly issued tokens fail verification, old tokens are rejected too
early, or JWKS differs across nodes.

1. Read the active and previous signing key IDs from each node.
2. Confirm the JWKS endpoint exposes all keys needed for the overlap window.
3. Verify issuer, audience, algorithm, and key ID in failing tokens.
4. If only one node is stale, force that node to reload signing-key state.
5. If rotation was bad, promote the last valid key, keep old keys published for
   the full token TTL, and rotate again after clients converge.

## 6. API Key Authentication Failure

Symptoms: API-key requests return unauthenticated despite the key being active.

1. Confirm the key prefix routes to the expected tenant and project.
2. Verify the stored key hash was produced with the currently configured hash
   secret.
3. Check key status, expiry, allowed scopes, and deleted-at fields.
4. Inspect audit events for credential type, subject, and denial reason.
5. Reissue the key if the hash secret changed without a migration plan.

## 7. Authorization Denial Spike

Symptoms: `authorize`, `check_access`, or `batch_check_permissions` deny traffic
that previously succeeded.

1. Group denies by tenant, project, action, resource type, and policy version.
2. Use the decision audit fields to identify matched policy IDs and deny reason.
3. Compare the current policy version with the last known-good version.
4. Run the policy lint RPC and inspect broad deny, shadowed allow, and dangling
   role findings.
5. Roll back or supersede the policy version if the denial pattern matches an
   unintended change.

## 8. Policy Snapshot Stale Across Nodes

Symptoms: different nodes return different authorization decisions for the same
principal, resource, action, and tenant.

1. Compare policy version and relationship version returned in decisions from
   each node.
2. Check control-plane reload and invalidation-lag metrics.
3. Verify the policy outbox/subscriber path is advancing on every node.
4. Force a snapshot reload on stale nodes or restart only those nodes if reload
   is stuck.
5. Preserve one example decision input and output per node for incident review.

## 9. Relationship Tuple Drift

Symptoms: ReBAC decisions do not match expected ownership or relationship state.

1. Read the tuple records for the subject, relation, object, tenant, and project.
2. Check for duplicate tuples, expired tuple conditions, and missing project
   scope.
3. Compare relationship version in decision audits before and after tuple edits.
4. Rebuild or replay tuple writes from the source of truth if durable state is
   inconsistent.
5. Run a canary authorization check before reopening affected workflows.

## 10. Policy Bundle Signing Failure

Symptoms: SDK policy bundle RPC returns failed precondition or internal signing
errors.

1. Confirm `UDB_POLICY_BUNDLE_SECRET` or the configured fallback signing secret
   exists on every serving node.
2. Check bundle key ID, issue time, expiry, tenant ID, and project ID.
3. Verify clients reject bundles only after the advertised expiry.
4. Compare policy and relationship versions in the signed bundle with server
   decisions.
5. Rotate the bundle secret only with an overlap plan for SDK cache expiry.

## 11. IDP Mapping Or SCIM Sync Failure

Symptoms: external users authenticate but map to the wrong subject, tenant,
project, roles, or groups.

1. Inspect the IDP mapping for issuer, subject claim, email claim, tenant
   binding, and role/group rules.
2. Validate SAML/OIDC metadata freshness and certificate validity.
3. Check replay protection for SAML and nonce/state handling for OIDC.
4. Compare SCIM directory state with local external-identity records.
5. Disable the affected mapping rule rather than the whole tenant when blast
   radius is limited to one provider.

## 12. Auth Audit Export Or Compliance Gap

Symptoms: audit exports are missing decisions, logins, revocations, policy
changes, native-access grants, or IDP events.

1. Confirm the event sink is configured and reachable.
2. Check topic names, correlation IDs, tenant IDs, and compliance envelope
   fields in recent auth events.
3. Verify sensitive fields are redacted by comparing with
   `docs/generated/authn-authz-sensitive-fields.md`.
4. Replay from the durable audit store or event outbox when available.
5. Record the missing event class, affected time range, tenant/project scope,
   and recovery action in the incident ticket.
