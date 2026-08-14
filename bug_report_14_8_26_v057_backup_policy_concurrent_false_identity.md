# UDB v0.5.7 concurrent backup-policy upserts can return a nonexistent ID

Date: 2026-08-14
Status: confirmed; correction not yet implemented
Affected paths: `PutBackupPolicy`, `DeleteBackupPolicy`

## Summary

Policy upsert discovers the existing ID with an unlocked read and otherwise
generates a new UUID before an update-on-conflict write keyed by tenant/name.
Two concurrent creates can generate different IDs: the winner inserts its ID,
the loser updates that row without updating `policy_id`, then returns and emits
the loser's nonexistent UUID. Delete also returns `deleted=true` without checking
whether any row existed.

## Confirmed served path

- `PutBackupPolicy` reads by tenant/name and chooses `Uuid::new_v4()` on an empty
  result.
- The conflict strategy targets `(tenant_id, policy_name)` but its update field
  list intentionally excludes `policy_id`.
- The handler does not read back the canonical row after write. It returns its
  local `policy_id` and puts that value in the best-effort upsert event.
- `DeleteBackupPolicy` discards the mutation result and always returns
  `deleted: true`, including a missing policy.
- No idempotency key or optimistic revision exists on either mutation.

## Consequences

- A successful response/event can identify a policy UUID that cannot be read or
  correlated to durable state.
- Automation may persist the false ID and mis-audit the configured policy.
- Concurrent responses give mutually incompatible identities for one logical
  tenant/name row.
- Delete acknowledgement cannot distinguish an actual mutation from a no-op.

## Required correction

- Generate canonical ID inside one database statement or transaction and use
  `RETURNING policy_id` from both insert and conflict-update paths.
- Emit and respond only with the returned durable identity.
- Add mutation idempotency/revision semantics and make policy state plus outbox
  atomic.
- Return the actual delete outcome or a typed not-found result, according to the
  declared contract.
- Add a concurrent same-name creation test and response-loss replay test.

## Verification log

- Traced policy read, conflict strategy, response/event construction, and delete
  result handling.
- No production data was mutated and no correction has yet been applied.
