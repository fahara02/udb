# Bug report: v0.5.11 SDK governance benchmark seed authority

Date: 2026-08-16
Status: fixed in source; CI verification required

## Failure evidence

Post-release benchmark run `31941904203` for tag `v0.5.10` and commit
`b98c8be97c745b904d9922e7a4a84f246635a14e` emitted the complete canonical
surface (381 RPCs per measured SDK, 1,524 attempts total), but the strict proof
gate correctly rejected 39 fatal rows.

The PHP governance root seed failed with `PERMISSION_DENIED` because
`CreatePolicyDraft` carried a verified `platform_admin` bearer but omitted the
governance gate's explicit audited break-glass fields. Dependent PHP rows were
reported as `SEED_BLOCKED` or `SKIP_NO_BODY`. TypeScript made the same authority
mistake but silently swallowed the root exception, retained manifest-only
placeholder IDs such as `draft-1` and `canary-1`, and consequently reported 11
misleading server `INTERNAL` UUID parse failures.

## Root cause

The governance server intentionally does not treat `udb:*` as an
`authz:policy:*` standing grant. A verified platform administrator may instead
use the explicit break-glass branch only when the request includes a reason and
a future expiry no more than 900 seconds away. The measured canonical request
bodies already used that contract; the PHP and TypeScript seed lifecycles did
not.

PHP also derived a synthetic attribution UUID for `reviewer`, while governance
requires `GovernanceActor.subject` and `reviewer` to match the verified platform
login's `user_id`.

## Resolution

- Both SDK seed lifecycles use the distinct offline-provisioned platform session
  and construct a reason-bearing, 900-second audited break-glass actor.
- Actor subject and reviewer are bound to the verified platform principal's
  `user_id`; session refresh must preserve that identity.
- TypeScript quarantines manifest-only governance placeholder IDs before a live
  seed. Failed seeds retain source RPC, numeric/name gRPC status, and details;
  every dependent row becomes `SEED_BLOCKED` with positive one-row timing
  evidence instead of calling the server with fabricated IDs.
- PHP governance sub-seeds no longer swallow draft/submit/approve/canary/version
  failures. Existing blocked-seed reporting now retains their original status,
  including successful responses that omit a required identifier.

The server governance scope and break-glass checks are unchanged. Asset,
Backup, Authz CRUD production fixes, generated bodies, and benchmark gate logic
are outside this patch.

## Verification

No local PHP, Node, Cargo, build, test, or formatter command was run, per the
CI-only instruction. Required CI proof:

1. Run the PHP and TypeScript SDK test jobs that cover
   `GeneratedRpcSurfaceTest.php` and `live-auth.test.ts`.
2. Run the post-release benchmark against a successor product tag.
3. Confirm all governance seed calls succeed under the verified platform
   identity, or, on an injected seed denial, dependent rows retain the original
   `SEED_BLOCKED` provenance without placeholder UUID calls.
4. Confirm the central canonical gate still requires 381 rows per measured SDK,
   1,524 total attempts, and zero fatal rows.
