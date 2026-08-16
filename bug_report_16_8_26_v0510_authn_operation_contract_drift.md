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
PR CI run `31922841419` confirmed that the proto itself builds, then emitted
the deterministic `ci-sdk-codegen-repair-1` artifact (`9256894085`) for the
six language SDK descriptor files and `ci-rustfmt-repair-1`
(`9256891010`) for the Rust source formatting drift. Those CI-generated
patches are the only formatter/code-generator output applied. The same run's
SDK conformance job then identified stale test-only hydration fixtures: the
TypeScript Authn tests lacked the new project context seed, and the Go shared
manifest test lacked the recovery-code seed. The fixtures now supply those
canonical values rather than weakening body hydration. Verification remains
CI-only; no local build, test, formatter, or generator was run.

Successor CI run `31923101583` passed the Linux build and all 2,757 library
tests (153 ignored, zero failures), including
`changed_operation_kind_is_behavioral`. Its only Linux failure was the
intentional 7.0.0-to-7.1.0 native-contract freshness gate, which emitted
`ci-native-docs-repair-1` artifact `9257086678`. That binary-safe artifact is
the sole source of the regenerated native manifest, native docs, descriptor
baseline, canonical codebase map, and bundled map.

CI run `31923594749` then reached the OpenAPI freshness gate and emitted
`ci-sdk-codegen-repair-1` artifact `9257136304`. Its sole change marks
`VerifyMfaChallenge` non-retry-safe and `mutation` in Swagger, matching the
descriptor contract.

CI run `31924007614` passed the Linux build and all 2,757 library tests before
the new SDK drift gate emitted `ci-native-docs-repair-1` artifact
`9257364918`. The artifact contains exactly the six descriptor-driven
robustness clients and the two SDK benchmark documents, all regenerated from
the built v0.5.10 broker.
