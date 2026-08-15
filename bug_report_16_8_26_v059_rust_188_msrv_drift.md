# Bug report: declared Rust 1.85 rejected repository source

Date: 2026-08-16
Release: 0.5.9
Severity: release-blocking compatibility drift

## Observed

`Cargo.toml` declared `rust-version = "1.85"`, but repository source uses
let-chain syntax stabilized in Rust 1.88 across generation, IR compilers,
schema, XA, security, native helpers, and service code. Existing CI Rust build
jobs selected the current stable toolchain; the only 1.85 toolchain references
were supply-chain commands and did not compile UDB source.

## Impact

A published crate could pass every stable CI build while failing for consumers
using the explicitly supported minimum compiler. Authentication and native
control-plane source was therefore outside the advertised compatibility gate.

## Correction

- Set the manifest's minimum supported Rust version to the honest repository
  floor, Rust 1.88.
- Add a required GitHub CI job that runs
  `cargo check --locked --all-features --all-targets` with Rust 1.88.0 after
  the quick gate.

## Verification

No local Cargo/build/test/rustfmt command was run. GitHub CI must compile the
complete target set on both the normal stable jobs and the exact declared MSRV.
