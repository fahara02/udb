# Change note: v0.5.7 post-release benchmark identity and replay correction

Date: 2026-08-15
Release: 0.5.7

## Changed

- The reusable release benchmark now provisions the SDK identity with canonical
  project UUID `00000000-0000-4000-8000-000000000057` and carries it into the
  CDC reset rows instead of mixing it with project code `default`.
- Go, TypeScript, and PHP now measure the single-use `RefreshToken` rotation once
  per fixture token, preserving replay-theft enforcement without invalidating the
  bearer used by unrelated benchmark RPCs.
- All four benchmark harnesses now place tenant-wide session revocation last in
  their authentication teardown phase, then re-authenticate exactly once before
  the final self `PurgeTenant` measurement.
- Manual benchmark dispatch accepts an optional harness checkout ref. The normal
  post-release path remains tag-pinned; the override permits the corrected `main`
  harness to test the immutable v0.5.7 release binary without moving its tag.

## Verification

- No local Cargo/build/test command is run.
- GitHub CI must validate the workflow and all modified SDK harnesses.
- A manual benchmark must use `release_tag=v0.5.7`,
  `release_asset=udb-linux-amd64-full`, and `checkout_ref=main`.
- Completion requires `summary.failed_rpc_count=0`, four SDKs with status `ok`,
  and the downstream Pages deployment consuming that exact benchmark artifact.
