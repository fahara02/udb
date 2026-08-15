# Bug report: Windows auth build depended on an unreliable NASM feed

Date: 2026-08-15
Affected release process: 0.5.8 preparation
Severity: release availability and supply-chain integrity

## Observed

The Windows auth-binary job in main CI installed Strawberry Perl and NASM in one
unconditional Chocolatey command. The hosted runner already contained
Strawberry Perl, but the job still queried Chocolatey's community feed for
NASM. Main CI attempt 1 timed out while reading that feed; failed-job rerun
attempt 2 received HTTP 499. Both runs stopped before Rust compilation while all
other substantive jobs passed.

## Impact

A transient third-party package-index failure could block an otherwise green
release indefinitely. The workflow also delegated NASM version selection and
artifact verification to a mutable community package feed rather than pinning
the native tool consumed by release builds.

## Required correction

- Reuse the hosted Windows runner's installed Strawberry Perl and verify that a
  real Perl executable is available.
- Download a fixed NASM version from the official NASM release archive with
  bounded retries.
- If the official host is unavailable, use a direct versioned package endpoint
  rather than a mutable feed lookup, and verify both the package and embedded
  official-installer SHA-256 values.
- Verify the official archive before extraction or the fallback installer
  before execution, then verify `nasm.exe` exists and runs.
- Fail closed on a missing Perl, download exhaustion, checksum mismatch, or
  malformed archive.
- Pin this bootstrap contract in the workflow-posture guard.

## Evidence

- Main CI run `31867114010`, attempt 1: the Chocolatey NASM lookup exceeded its
  100-second HTTP timeout.
- The same run, failed-job rerun attempt 2: the Chocolatey NASM lookup returned
  HTTP 499.
- The official `nasm-3.02-win64.zip` archive hashes to
  `161D0BFAFF53C2F9E9F3E69FD0672323EBABAFD1268976A5CEC11BE92A19AEE7` and
  contains `nasm-3.02/nasm.exe`.
- PR #30 CI run `31877820722` showed that nasm.us can also be unreachable for
  all three attempts. The direct `nasm/3.2.0` package hashes to
  `9A72BA9D6F6F0DC2A5598EC160366B2BDD925A23E229DFB5D854F63C0F2A2160`;
  its embedded x64 installer hashes to
  `0DDB40310861EB29F4D649FEB9466779982A2D251C0DB2B9CF0D21CF591171F3`.
