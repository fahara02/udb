# Change note: v0.5.9 Rust 1.88 compatibility gate

UDB now declares and continuously compiles all targets with Rust 1.88, the
actual minimum required by repository-wide let-chain usage. The previous 1.85
manifest claim was not exercised by CI and did not match compilable source.

This corrects the compatibility contract without rewriting security or runtime
control flow solely to preserve a stale compiler claim. Local Cargo/build/test/
rustfmt was intentionally not run; GitHub CI is the required proof.
