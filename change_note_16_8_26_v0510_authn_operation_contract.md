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
