# Change note: v0.5.11 SDK governance benchmark seed authority

Date: 2026-08-16

The PHP and TypeScript live benchmark harnesses now seed Authz governance state
through the same explicit, audited platform break-glass contract used by their
measured governance requests. The actor is bound to the verified platform
principal `user_id`, carries a reason and a maximum 900-second expiry, and is
rechecked after session refresh. PHP reviewers now use that same verified user
identity instead of a derived attribution UUID.

TypeScript live seeds no longer inherit manifest-test placeholder governance
IDs. The fixture store records failed seed source/status/details and emits a
complete `SEED_BLOCKED` benchmark row for each dependent RPC. PHP governance
draft, submit, approve, canary, and rollback sub-seeds likewise preserve failure
provenance instead of silently degrading to missing bodies.

Files changed:

- `sdk/typescript/live-auth.test.ts`
- `sdk/php/tests/Live/GeneratedRpcSurfaceTest.php`
- `docs/reports/bug_report_16_8_26_v0511_sdk_governance_benchmark_seed_authority.md`
- `docs/reports/change_note_16_8_26_v0511_sdk_governance_benchmark_seed_authority.md`

Local builds and tests were intentionally not run. CI must execute the focused
PHP/TypeScript SDK suites and the successor post-release 381-RPC-per-SDK proof.
