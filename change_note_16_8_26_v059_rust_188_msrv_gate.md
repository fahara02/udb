# Change note: v0.5.9 Rust 1.88 compatibility gate

UDB now declares and continuously compiles all features and targets with Rust
1.88, the actual minimum required by repository-wide let-chain usage. The
previous 1.85 manifest claim was not exercised by CI and did not match
compilable source.

The new gate immediately found and closed an optional-feature compile island:
the init prompt's `inquire` selection types now have a feature-gated
module-scope import shared by the runner and helper functions. This keeps the
interactive dependency absent from builds without `init-prompts` while making
the complete feature set compile as one package contract.

This corrects the compatibility contract without rewriting security or runtime
control flow solely to preserve a stale compiler claim. Local Cargo/build/test/
rustfmt was intentionally not run; GitHub CI is the required proof.
