# Change note: v0.5.8 release readiness and Pages evidence gate

Date: 2026-08-15
Release: 0.5.8

## Changed

- The shared product and SDK version source advances from 0.5.7 to 0.5.8; the
  repository version propagator refreshes governed manifests, generated client
  constants, launchers, maintained docs, examples, and static Pages version
  labels.
- The previously manual Go arbitrary-project example release tag is now governed
  by `scripts/check-versions.mjs`, preventing its install guidance from drifting
  again.
- Pages skips validation-only benchmark completions and accepts only successful
  real benchmark runs.
- A benchmark-triggered Pages build now requires the fresh benchmark artifact;
  only direct docs/wasm pushes may retain the committed benchmark JSON.
- Workflow posture and the site-maintainer guide pin those two Pages evidence
  rules so a later CI refactor cannot silently restore the fallback.
- The changelog records the restore self-journal fix and benchmark/auth harness
  corrections included in this patch release.

## Verification

- No local Cargo/build/test command is run, per operator direction.
- GitHub CI must validate version consistency, workflow posture, all Rust/SDK
  builds and tests, scaffold examples, and Pages build behavior.
- The release tag must point at the exact green `main` commit and remain
  immutable.
- Completion requires the v0.5.8 release graph to succeed, followed by four SDK
  benchmark results with zero failed RPCs and a Pages deployment consuming that
  exact artifact.
