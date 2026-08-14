# UDB v0.5.7 change note — Vault KEK, project routing, and credential authority

Date: 2026-08-14

## Outcome

- Vault now remains sealed unless a real authenticated master-KEK envelope is
  available. Plaintext/base64 DEKs are rejected on both wrap and unwrap paths.
- Raw SQL and typed native Vault operations can share one dynamically resolved,
  active-project Postgres instance; the stale build-time default pool was
  removed from the service.
- Vault event routing and the legacy lease reaper now re-resolve project stores.
- Direct PostgreSQL credential issuance is intentionally disabled until UDB has
  an immutable tenant/project authorization authority. Existing expired leases
  receive session termination, role removal, and absence verification before
  their durable state becomes REVOKED.

## Compatibility and operator impact

- Deployments using native Vault must configure a real KEK. Generic development
  plaintext fallback no longer unseals Vault.
- Existing non-envelope Vault DEK rows now fail closed and require a controlled
  offline inventory/rewrap migration; no automatic plaintext migration is
  attempted.
- `GenerateDatabaseCredentials` now returns a typed capability refusal instead
  of minting a role whose physical privileges are not tenant-bound. This is an
  intentional secure breaking change.

## Verification

- Targeted `rustfmt` completed for the edited Rust modules.
- `cargo check --lib --no-default-features --features postgres -j 2` passed
  locally (warnings only).
- Focused Vault unit execution was terminated after the local linker remained
  CPU-bound for more than ten minutes. No local test result is claimed; GitHub
  CI is the test authority for this wave.
- GitHub CI for the preceding backup/topology commit completed successfully in
  full, including Ubuntu/Windows Rust jobs and library tests. This Vault wave has
  not yet been pushed and therefore has no CI result at this point.

## Explicit residual work

- Startup/readiness inventory, provider/key-version reporting, and an offline
  rewrap workflow for legacy non-envelope DEKs.
- A two-project/two-instance live Vault conformance test and persisted physical
  provenance for historical credential leases.
- A trusted tenant-bound credential broker or database-native authorization
  design, followed by idempotent issuance/recovery, public revoke/revoke-all,
  durable revocation events, reconciliation states, and failure-injection tests.

## CI follow-up

- GitHub CI run `31817914608` reached the quick gate and reported one mechanical
  `rustfmt` line-wrapping difference in `src/runtime/core/native_store.rs`.
- The same focused correction preserves the selected-instance value as the
  borrowed `Option<&str>` required by `backend_executor_for_project`; the first
  commit's isolated staging reconstruction had converted it to an incompatible
  owned `Option<String>`.
- No local build or test was run for this follow-up. The replacement GitHub CI
  run remains the compilation and test authority.
- Replacement CI run `31818197455` compiled the PostgreSQL-slim target, the
  live-tier broker, and the full Ubuntu/all-features library test binaries. Both
  library jobs then reported the same single failed assertion after thousands
  of passing tests: the no-runtime `DestroySecret` guard received the new
  missing-catalog capability before the established missing-runtime capability.
- `resolve_project_store` now checks its primary runtime capability before the
  active-project catalog. Both remain fail-closed before store selection; this
  restores the documented error precedence without weakening project routing.
  No local test was run for this correction; the next GitHub CI run is the
  authority.
