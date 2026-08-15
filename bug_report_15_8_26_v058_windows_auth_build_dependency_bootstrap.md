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
- Pin and verify the archive SHA-256 before extraction, then verify `nasm.exe`
  exists and runs.
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
