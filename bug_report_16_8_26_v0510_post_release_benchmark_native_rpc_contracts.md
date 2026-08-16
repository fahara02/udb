# Bug report: post-release benchmark native RPC contract failures

Date: 2026-08-16
Affected evidence: v0.5.9 benchmark run `31919949691`, job `95098106809`
Target correction: 0.5.10
Severity: release-evidence blocker

## Observed

The first post-release v0.5.9 benchmark correctly failed closed, but four of
its fatal RPC families exposed product/fixture defects rather than unavailable
capabilities:

- DataBroker `ApplyMigration` returned `INTERNAL` while verifying the logical
  Redis namespace `redis://udb:session:{session_id}`. Redis intentionally has
  no native resource-lifecycle executor.
- Authn `VerifyMfaChallenge` returned `INVALID_ARGUMENT`;
  `DeleteWebAuthnCredential` and `RenamePasskey` returned `INTERNAL` from
  claim-bound native dispatch.
- Python Authn `Logout` returned `INTERNAL` after the session revoke succeeded,
  because its refresh-family side effect dispatched a tenant-scoped entity
  update with an empty tenant/project context.
- Go and Python reported `SKIP_NO_BODY` for Vault
  `GenerateDatabaseCredentials` because their seed maps omitted the required
  idempotency key. TypeScript retained placeholder idempotency/lease values and
  PHP populated neither required reference, so the same contract would fail
  once those harnesses reached the rows. None issued a dedicated real lease for
  the revoke measurement.
- Delete/rename used the generic DataBroker `record_id` as a WebAuthn
  credential id, and MFA paired an EMAIL_OTP challenge with an unrelated OTP
  user's code.

The retained benchmark artifact is `9256120524`. These failures were present
across Go/Python and, where the same manifest/seed contract was shared, the
other measured SDKs; they were not caused by manifest row randomization.

## Root cause

`plan_migration` emitted `verify_resource` for every recognized non-Postgres
store without consulting the canonical V2 lifecycle capability. Apply then
called Redis `list_resources`, whose deliberate `FAILED_PRECONDITION` was
wrapped as an internal migration failure.

The three Authn handlers lowered serving requests with an empty neutral
`RequestContext`. MFA ignored its decoded request context; WebAuthn delete and
rename discarded the already-authorized target user's tenant/project before
native dispatch. Neutral IR therefore failed the tenant-scope gate.

Logout had the same raw/store split in token-family cleanup. Both the
session-bound and all-sessions principal-bound family revokers constructed
`authn_context("", "")`, and their raw Postgres fallback updated by credential
identity without tenant/project predicates. The served native route therefore
failed the scope gate, while the fallback retained broader authority than the
request contract.

`VerifyMfaChallenge` was also declared `OPERATION_KIND_READ_ONLY` even though it
increments the attempt counter, consumes a successful challenge, and can
consume a recovery code. SDKs therefore treated it as retry-safe and benchmark
harnesses performed an excluded read warm-up that consumed the only successful
proof. Descriptor diff did not compare `operation_kind`, so this behavioral
contract error could be corrected without appearing in the version gate.

The benchmark body/seed contract also conflated unrelated semantic ids. The
WebAuthn bootstrap discarded the credential id returned by registration, the
MFA seed created a factor whose proof the harness could not possess, and the
SDK Vault seeds did not consistently populate the two newly required
dynamic-credential references.

## Correction

- Plan native migration resource operations from
  `BackendKind::capabilities_v2().lifecycle` and preflight against the compiled
  plugin's own `kind()` capability. Logical-only Redis namespaces are not
  emitted as physical verification operations; forged/non-native resource
  operations fail preflight without rejecting a compiled native plugin.
- Resolve MFA native routing from the decoded body scope and validated claim.
  Omitted body scope inherits the claim; conflicts fail closed.
- Declare MFA verification as `OPERATION_KIND_MUTATION`, making generated SDK
  retry/probe/benchmark behavior match its actual state changes. Descriptor
  diff now reports any operation-kind drift as `BehavioralChange`.
- Route WebAuthn delete/rename through the authorized target user's exact
  tenant/project instead of an empty context, while retaining field validation
  before store access.
- Resolve Logout's family-revocation scope from the validated bearer claim and
  optional request context, then carry exact tenant/project predicates through
  both session and all-sessions family revocation. The raw Postgres fallback
  installs the tenant RLS setting transaction-locally and uses the same exact
  tenant/project keys. A single-session retry always replays the idempotent
  family revoke even if an earlier partial attempt already revoked the session.
- Make the benchmark manifest use two independently registered WebAuthn users
  and credentials for delete versus rename, a recovery-code challenge/proof
  pair, and explicit tenant/project context. The two mutations are now
  order-independent and neither removes the main authentication passkey.
- Capture the real WebAuthn id in all four benchmark SDKs, seed stable Vault generation
  idempotency keys, and create a separate real dynamic-credential lease for the
  destructive revoke measurement and cleanup.

## Regression and acceptance

Unit/static coverage pins Redis as non-native migration lifecycle, native
resource backends as eligible, claim-scope inheritance and foreign-scope
rejection, operation-kind behavioral classification, semantic body references,
and real fixture capture. Generated
`docs/generated/bench-bodies.json` and SDK-derived surfaces must be regenerated
in CI from the edited source manifest before tests run.

Required CI filters:

- `cargo test --all-features migration_resource_ops_follow_native_lifecycle_capability`
- `cargo test --all-features request_authn_context_`
- `cargo test --all-features 'logout_family_scope|scoped_token_family_filter'`
- `cargo test --all-features changed_operation_kind_is_behavioral`
- `go test ./sdk/go/udbclient -run 'TestLivePerfExplicitBodyCoverage|TestBuildManifestJSONBodyUsesSharedManifest'`
- `(cd sdk/typescript && npm test)`
- `(cd sdk/php && vendor/bin/pest tests/Live/GeneratedRpcSurfaceTest.php)`
- the reusable live SDK benchmark with Go, Python, TypeScript, and PHP enabled, specifically the
  DataBroker `ApplyMigration`, Authn `VerifyMfaChallenge` /
  `RenamePasskey` / `DeleteWebAuthnCredential`, and Vault
  `GenerateDatabaseCredentials` / `RevokeDatabaseCredentials` rows.

No local build, Cargo, code generation, test, formatter, workflow dispatch, or
benchmark was run. Verification is CI-only plus static `git diff --check`.
