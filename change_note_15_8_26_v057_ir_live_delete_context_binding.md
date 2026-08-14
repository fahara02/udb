# Change note: IR live delete context binding

Date: 2026-08-15  
Release: 0.5.7 follow-up

## Observed failure

Post-merge CI run `31846072250` reached the push-only `Native services +
canonical stores (live)` lane after the full PR and focused critical live suites
were green. Its IR compiler golden stage ran 22 tests: 21 passed and
`postgres_data_plane_planner_and_bridged_ir_match_live_rows` failed while binding
the legacy planner delete with `3 columns, 2 values`.

## Root cause

The production PostgreSQL delete fallback was hardened to append verified
tenant/project predicates and their `context_parameter_values` after the
caller-owned filter parameters. The live A/B oracle's `execute_legacy_delete`
helper still bound only raw filter values, so its test-only executor no longer
matched the production planner contract. The bridged neutral-IR executor was not
the failing path.

## Change

- Normalize the test request filter to physical column names, matching the
  production delete fallback.
- Bind normalized filter values first and append the planner's verified context
  parameter values in placeholder order.
- Pin both soft-delete and physical-delete planner tests to the returned trusted
  tenant bind, and correct the stale bridge commentary that claimed only neutral
  IR carried the backstop.
- Expose `UDB_IR_LIVE_GOLDEN_TESTS` and the integration PostgreSQL DSN in the
  lightweight `live-quick` workflow so this exact ignored regression can be run
  on a pull-request branch without waiting for a post-merge full backend stack.

## Verification policy

No local Cargo build or test is run. GitHub CI must pass, and the focused
`postgres_data_plane_planner_and_bridged_ir_match_live_rows` live-quick run must
execute one test with zero failures before the follow-up is merged.
