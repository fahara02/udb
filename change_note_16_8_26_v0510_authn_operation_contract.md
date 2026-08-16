# Change note: Authn challenge verification is a mutation

The v0.5.10 contract declares `VerifyMfaChallenge` as `MUTATION`, matching its
attempt counter and challenge/recovery-code consumption. Descriptor comparison
now reports operation-kind changes as behavioral drift, and the independent
native contract advances to 7.1.0.

The benchmark seeds a real recovery-code challenge with tenant/project context,
so the measured call is the single state-consuming operation rather than a
post-warm-up replay. CI must regenerate and verify the descriptor baseline,
native docs/manifest, SDK artifacts, and benchmark manifest. No local build,
test, formatter, or generator was run.

The first PR CI run, `31922841419`, produced and supplied the exact repair
artifacts used for the checked-in outputs: `ci-sdk-codegen-repair-1`
(`9256894085`) and `ci-rustfmt-repair-1` (`9256891010`). A subsequent CI
run must prove that both freshness gates are clean.

Its SDK conformance job also proved that the changed challenge/WebAuthn bodies
need explicit project and recovery-code values in their unit fixtures. The Go
and TypeScript tests now hydrate those same canonical seeds and continue to
fail closed when a required seed is absent.

CI run `31923101583` then passed the Linux build and 2,757 library tests with
zero failures. The expected contract freshness failure produced
`ci-native-docs-repair-1` (`9257086678`), whose 7.1.0 manifest, docs, binary
baseline, and synchronized maps are now checked in for a clean follow-up run.

The following run, `31923594749`, supplied the final OpenAPI repair artifact
`9257136304`; Swagger now exposes the same mutation and retry-safety semantics
as the proto and native contract.
