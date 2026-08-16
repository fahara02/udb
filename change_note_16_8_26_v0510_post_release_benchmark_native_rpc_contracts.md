# Change note: v0.5.10 post-release benchmark native RPC contracts

Date: 2026-08-16
Release: 0.5.10

This change closes the non-authority DataBroker/Authn/Vault harness defects
identified by v0.5.9 post-release benchmark run `31919949691` (job
`95098106809`, artifact `9256120524`).

## Changed

- Migration planning now consults the canonical backend lifecycle capability
  before emitting non-Postgres resource verification. Redis key namespaces are
  logical and no longer become impossible native `verify_resource` operations.
- Migration apply preflight reads the compiled plugin's canonical backend kind
  and rejects resource operations only when that runtime authority lacks a
  native resource-lifecycle executor.
- MFA verification resolves the neutral runtime context from the decoded
  request tenant/project and validated bearer claim. Missing scope inherits the
  claim; a conflicting scope is denied.
- `VerifyMfaChallenge` is now a mutation contract, not a retry-safe read. The
  descriptor-diff classifier records operation-kind changes as behavioral drift
  so future retry/probe semantic changes cannot bypass native-contract review.
- WebAuthn credential delete/rename preserve the authorized target user's exact
  tenant/project when dispatching neutral delete/update operations.
- Logout now carries the validated claim/request tenant and project through
  refresh-family revocation for both one-session and all-sessions paths. Native
  dispatch and the raw Postgres fallback share the same tenant/project
  predicates; the fallback also installs the transaction-local tenant RLS
  setting before updating token families. Retried Logout calls replay the
  idempotent family revoke after a partial session-only success.
- The Authn benchmark manifest now uses independent registered WebAuthn users
  and credential ids for delete and rename, an executable recovery-code proof,
  and explicit request scope. Go/Python/TypeScript/PHP seed code captures both
  real registration responses and preserves those semantic fixtures,
  irrespective of RPC ordering.
- All four benchmark SDKs provide the required stable Vault generation
  idempotency key and create a distinct live lease for the destructive revoke
  row. Cleanup replays revoke safely if the measured row already removed that
  lease.
- Focused Rust and cross-SDK assertions pin capability selection, body scope,
  recovery proof/reference hydration, exact WebAuthn fixture usage, and Vault
  idempotency/lease hydration.

Generated benchmark JSON, SDK artifacts, and documentation derived from the
source manifest are deliberately not hand-edited; CI/codegen must regenerate
them. Integration owns the corresponding native-contract version bump. Pages/
collector, Authz/platform authority, and Vault physical authority files are
outside this change.

## Verification

- Static source inspection and `git diff --check` only.
- No local Cargo/build/test/codegen/rustfmt was run.
- CI must run the focused Rust filters (including Logout family-scope and
  token-family predicate coverage), cross-SDK manifest-body tests, and a served
  reusable SDK benchmark proving the affected RPC rows complete without fatal
  status or missing-body evidence in Go, Python, TypeScript, and PHP.
