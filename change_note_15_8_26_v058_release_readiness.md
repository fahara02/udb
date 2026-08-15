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
- Windows native and release builds now reuse the hosted runner's installed
  Strawberry Perl and install NASM 3.02 from the official project archive. The
  archive has a pinned SHA-256, bounded retries, and a hard checksum failure;
  the Chocolatey community feed is no longer a release-build dependency.
- The workflow-posture positive and ordering-negative self-test fixtures now
  include the successful-real-run and fresh-artifact requirements enforced for
  Pages, so the ordering mutation continues to exercise the intended failure.
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
- Main CI attempts 1 and 2 at commit `c381a9f3` passed all substantive product
  jobs except `Auth binary (windows-amd64)`, which stopped before compilation
  when Chocolatey's NASM feed first timed out and then returned HTTP 499. The
  exact Windows build must pass with the checksum-pinned bootstrap before tag
  creation.
- PR #29 workflow-lint run `31876409470` exposed the remaining stale
  ordering-negative Pages fixture; the fixture is synchronized before the next
  CI run.
- Follow-up workflow-lint run `31876479588` passed that self-test and then found
  the repository scan still required the removed live-dashboard fallback
  wording. The guard and its README fixture now require the fail-closed fresh
  artifact, validation-run exclusion, and committed-JSON-only direct-push rules.
