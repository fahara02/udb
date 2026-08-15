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
  expected measured aggregate. Every summary count must match the canonical
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

## Verification

- No local Python, Cargo, build, test, rustfmt, tag, or workflow dispatch is run.
- Static verification is limited to source inspection and `git diff --check`.
- GitHub CI must run `python scripts/collect_sdk_bench_results.py --selftest`,
  `python scripts/check-bench-harness-posture.py --selftest`,
  `python scripts/check-bench-harness-posture.py`,
  `python scripts/check-workflow-posture.py --selftest`, workflow lint, and the
  post-release Benchmark -> Pages proof chain.
