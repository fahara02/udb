# Change note: v0.5.7 post-release docs, examples, benchmark, and Pages refresh

Date: 2026-08-15
Release: 0.5.7

## Source refresh

- Expanded the v0.5.7 changelog with the final CDC authorization/replay/policy,
  Backup snapshot/topology, Vault KEK/project-store/dynamic-credential,
  project-ownership, and storage GC-readiness corrections.
- Added operator-facing CDC and Backup behavior to the security and operations
  guides, including the fail-closed and unsupported-topology boundaries.
- Added the Vault and project-ownership contracts to the native-service guide.
- Updated the checked-in Go arbitrary-project example to use v0.5.7 as its
  concrete release-tag example.

## Benchmark and Pages authority

The committed `docs/site/bench-results.json` is not edited. The exact released
v0.5.7 binary and tag-pinned SDK harness must produce
`sdk-benchmark-results`; `pages.yml` must consume that artifact and deploy it
with the latest `main` documentation. This preserves the repository's single
benchmark producer and single Pages deployer.

## Verification policy

- No local Cargo/build/test command is run, per operator direction.
- Static diff/whitespace and stale-version scans may run locally.
- GitHub CI must pass the documentation/example commit, including Markdown
  links, version consistency, and six-language scaffold compilation.
- The post-release `Benchmark · SDKs` workflow and its downstream
  `Deploy site (GitHub Pages)` workflow must both succeed after this source
  refresh lands on `main`.
