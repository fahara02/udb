# UDB v0.5.7 dynamic database credential issuance is not replay-safe or revocable

Date: 2026-08-14
Status: new issuance disabled and expiry cleanup hardened; public recovery/revoke contract remains open
Affected path: Vault dynamic database credentials and lease reaper

## Summary

GenerateDatabaseCredentials has no idempotency key and there is no public revoke
RPC. Response loss creates an additional live login on retry, while callers
cannot revoke a suspected credential before TTL. Expiry relies on password
`VALID UNTIL` plus a periodic DROP ROLE; the worker never terminates existing
sessions and emits no durable revocation event. Physical-role creation and lease
persistence also remain split.

## Confirmed served path

- Every request generates a fresh lease ID, username, password, and CREATE ROLE;
  the request has no idempotency field or client operation ID.
- The service exposes issuance only; no RevokeDatabaseCredentials RPC exists.
- If lease persistence fails, cleanup DROP ROLE is best effort. Cleanup failure
  returns the write error while leaving an untracked live role.
- The leader worker selects expired ACTIVE leases, executes `DROP ROLE IF EXISTS`,
  then marks the row REVOKED. It does not terminate sessions, fence use, or emit a
  revocation/outbox audit event.
- Worker/drop and lease update are separate operations; startup/readiness does not
  prove all expired roles are gone.

## Consequences

- Client/network response loss can leave a valid credential the legitimate client
  never received or can cause multiple active logins after retry.
- A leaked credential cannot be revoked through the public Vault contract.
- Connections established before expiry can outlive the advertised lease unless
  separately terminated by database policy/operations.
- Lease state and physical role/session reality can diverge without durable
  revocation evidence.

## Required correction

- Add required idempotency to issuance and return the original one-time outcome
  through an appropriately protected recovery protocol, or use a two-phase claim
  that never creates an unrecoverable credential.
- Add tenant/project-scoped revoke plus emergency revoke-all, with durable state,
  session termination, physical-role cleanup, and outbox audit.
- Persist STARTING/ACTIVE/REVOKING/REVOKED/FAILED reconciliation states and let a
  supervised worker repair every split boundary.
- Enforce expiry at connection/session level, not only new password login, and
  verify role absence before final REVOKED state.
- Add response-loss, lease-write cleanup-failure, active-session expiry, and
  worker-restart live tests.

## 2026-08-14 correction

- Unsafe new issuance is disabled, so response loss/retry can no longer create
  an additional untracked direct PostgreSQL login through the served endpoint.
- For existing ACTIVE leases, the reaper now terminates every matching backend
  session before role removal, executes the drop, verifies the role is absent,
  and only then marks the lease REVOKED. Any fencing/drop/verification failure
  leaves the lease ACTIVE for retry.
- The worker re-resolves and sweeps every active project's Vault store on each
  pass, covering project-routed legacy leases rather than only the default pool.

This is a containment and expiry-reconciliation improvement, not completion of
the lifecycle contract. There is still no tenant/project-scoped public revoke or
emergency revoke-all RPC, no STARTING/REVOKING/FAILED state machine, no durable
revocation outbox event, and no recovery protocol for historical one-time
responses. Those items and the listed live failure-injection tests remain open.

## Verification log

- Traced request schema, role creation, cleanup, lease persistence, service RPC
  inventory, and reaper behavior.
- `cargo check --lib --no-default-features --features postgres -j 2` passed
  locally after the correction (warnings only).
- Focused Vault unit execution was terminated after the local linker remained
  CPU-bound for more than ten minutes; no test result is claimed. GitHub CI is
  pending for this wave; no production data was mutated.
