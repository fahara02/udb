# Bug report: VerifyMfaChallenge was advertised as read-only while consuming state

## Observed

`AuthnService.VerifyMfaChallenge` increments attempts, marks a challenge
consumed, and can consume a recovery code. Its native operation contract was
nevertheless `READ_ONLY`. SDK benchmark harnesses therefore warmed it up and
could repeat it under read semantics; the excluded warm-up consumed the only
successful proof before the measured call.

The descriptor-diff classifier also ignored `operation_kind`, so this semantic
contract change could drift without appearing in the native contract report.

## Impact

Retry, ordering, benchmarking, and generated SDK metadata could treat a
state-consuming authentication proof as an idempotent read. That is unsafe even
when the served handler itself validates the challenge correctly.

## Required correction

- Reclassify VerifyMfaChallenge as `MUTATION` in the canonical proto.
- Classify operation-kind drift as a native `BehavioralChange`.
- Advance the independent native contract from 7.0.0 to 7.1.0.
- Regenerate the descriptor baseline, native manifest/docs, SDK metadata, and
  canonical benchmark body manifest in CI.
- Benchmark one real recovery-code challenge in the claim-bound project.

## Evidence

The v0.5.9 post-release benchmark exposed the destructive read classification.
Verification is CI-only; no local build, test, formatter, or generator was run.
