# Bug report: v0.5.7 post-release benchmark identity and replay drift

Date: 2026-08-15
Release: 0.5.7
Severity: release-evidence / security-regression harness

## Observed

The automatic benchmark of the released v0.5.7 binary completed every language
step but failed its aggregate gate. The uploaded artifact reported 774 failed
RPC rows and the PHP harness exited before producing a report.

The failures had two deterministic signatures:

- Storage, Scheduler, and Workflow rejected project code `default` because their
  v0.5.7 ownership stores now require a canonical UUID project identifier.
- Go, TypeScript, and PHP measured `RefreshToken` as a repeatable mutation. The
  second use of the same refresh token correctly triggered v0.5.7 replay-theft
  handling, which revoked the benchmark principal's sessions. Nearly every later
  RPC then returned `UNAUTHENTICATED`. PHP additionally could not create its
  project-owned seed records and stopped on a missing dependent fixture.
- All four SDK harnesses deliberately measured tenant-wide session revocation in
  their terminal authentication phase, but then attempted the final self
  `PurgeTenant` with the bearer that operation had just invalidated.

## Impact

The benchmark was exercising stale harness assumptions rather than the released
security contract. Publishing its failed payload would make the Pages dashboard
misrepresent both SDK health and v0.5.7 project isolation.

## Required correction

1. Bind the release benchmark to one deterministic canonical project UUID and
   use that same value in reset fixtures.
2. Measure refresh-token rotation exactly once per fixture token.
3. Order tenant-wide revocation last in the authentication teardown phase and
   log in through the public credential path before final self-purge.
4. Keep the v0.5.7 tag immutable while allowing an explicitly reviewed `main`
   harness ref to benchmark the already-published v0.5.7 binary.
5. Require the existing zero-failed-RPC aggregate gate before Pages deployment.

## Evidence

Failed benchmark run `31861853604` used release tag `v0.5.7`, release asset
`udb-linux-amd64-full`, and release commit
`d2a8accc7e00f19ac5c93cb863a65afc1d664a9b`. Its artifact recorded three SDKs
as nominally complete, PHP as failed, and `failed_rpc_count=774`; logs contained
`project_id must be a valid UUID` followed by bulk `invalid bearer token`
failures.
