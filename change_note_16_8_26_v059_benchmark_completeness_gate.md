# Change note: v0.5.9 benchmark evidence completeness gate

Date: 2026-08-16
Release: 0.5.9

## Changed

- The benchmark collector now derives its expected RPC surface from
  `docs/generated/bench-bodies.json` rather than a hardcoded count.
- The generated manifest must contain one unique, internally consistent
  `service/rpc` wire identity per row. Its SHA-256 and dynamically derived
  surface count are recorded in `benchmark_contract`.
- Go, Python, TypeScript, and PHP must each report the entire canonical wire-RPC
  set exactly once with matching header/full-row counts, status `ok`, and zero
  failed RPCs. Every row's API alias and operation id must match the canonical
  manifest, and the gate independently normalizes row error evidence so a
  zero-valued summary cannot conceal a failed RPC.
- C# and Java remain explicit static-only skips. Missing, unexpected, duplicate,
  or incorrectly measured/skipped SDK entries fail the benchmark gate.
- Summary evidence now records the canonical RPC count, measured SDK count, and
  expected attempted aggregate. Every summary count must match the canonical
  contract.
- The reusable benchmark passes the generated manifest explicitly to both
  collection and the final always-run gate, preserving upload-before-fail
  diagnostics.
- A fresh post-release Pages run fetches the canonical manifest from the exact
  benchmarked commit and invokes the same collector gate before deployment.
  The previously committed historical dashboard remains publishable on direct
  documentation builds and is not relabeled as current release proof.
- Pages artifact validation no longer backfills missing wire, alias, or
  operation identities with `setdefault`; it rejects non-object rows and empty
  identity fields without mutating benchmark evidence.
- The Pages invocation uses the runner's guaranteed `python3` command; the
  reusable benchmark retains its toolchain-provided `python` command.
- Selftests and source-posture guards cover RPC/SDK membership drift, duplicate
  identities, count tampering, explicit skips, and both workflow handoffs.
- Required PR CI syntax-checks the collector and runs its completeness selftest,
  so this release gate cannot wait until a post-merge benchmark validation run
  to discover a parser or regression-fixture failure.
- Benchmark JSON schema v2 records `evidence_status` and separates attempted,
  successful measured, capability-skipped, and failed RPC counts per SDK, in
  aggregate, and in the current history point. All count fields are exact
  integers; booleans are rejected.
- The collector Counter now counts manifest keys instead of treating
  `(api_alias, operation_id)` tuples as numeric counts. Each full row must bind
  `service`/`rpc` to canonical `wire_api`, carry a positive iteration count and
  finite nonnegative emitted latency values, and expose a `result_status`
  consistent with its preserved canonical error status.
- `CAPABILITY_SKIPPED` remains explicit nonfatal evidence rather than being
  rewritten to ordinary success. The gate recomputes capability skips and the
  exact fatal-row set, requires `failed_rpcs` to match it, and rejects current
  history counts or SDK statuses that diverge from the payload.
- The TypeScript per-RPC harness now emits its actual iteration count for unary
  and every streaming measurement variant, aligning its full/slowest report
  schema with the other measured SDKs.
- Push and manual Pages runs without a fresh artifact accept only the exact
  pinned SHA-256 of the committed v0.4.28 JSON. This closes the
  changed-predecessor/unchanged-successor bypass left by a relative Git diff.
  Committed schema-v2/canonical-complete payloads fail explicitly; new green
  proof must use Release -> Benchmark. The dashboard marks historical evidence
  legacy/incomplete and shows attempted, successful, capability-skipped, and
  failed counts without rewriting the JSON bytes.
- Follow-up edits cover `.github/workflows/pages.yml`, the collector, benchmark
  and workflow posture guards, the TypeScript harness, the benchmark dashboard
  JavaScript/markup/README, this note/report, and `CHANGELOG.md`.
- Final review aligned the `pages_good` posture fixture with production's
  `if got_fresh ... else ... fi` authority boundary. The checker now validates
  nested branch depth and ordering, and a negative fixture proves that moving
  the schema rejection/digest pin after the no-fresh `else` fails posture.

## Verification

- No local Python, Cargo, build, test, rustfmt, tag, or workflow dispatch is run.
- Static verification is limited to source inspection and `git diff --check`.
- Workflow-lint run `31912397487` passed `actionlint` but exposed a stale
  negative selftest mutation for the old single-line collector command. The
  fixture now removes the new gate target token and expects the current
  diagnostic; verification remains CI-only.
- GitHub CI must run `python scripts/collect_sdk_bench_results.py --selftest`,
  `python scripts/check-bench-harness-posture.py --selftest`,
  `python scripts/check-bench-harness-posture.py`,
  `python scripts/check-workflow-posture.py --selftest`, the TypeScript SDK
  typecheck/build job, workflow lint, and the post-release Benchmark -> Pages
  proof chain.
