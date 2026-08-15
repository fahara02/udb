# Bug report: post-release benchmark accepted incomplete RPC coverage

Date: 2026-08-16
Affected release evidence: 0.5.9 pre-release
Target correction: 0.5.9
Severity: release blocker

## Observed

The SDK benchmark collector recorded each report's `RPCs measured` header and
the parsed `full_rpcs` rows, but its final gate checked only that no measured SDK
had a failed process status and that the aggregate failed-RPC count was zero.
Pages checked that `failed_rpc_count` existed and that release provenance was
valid, but did not require the artifact to contain the complete generated RPC
surface.

Consequently, a report with a stale or reduced header, a missing full-RPC row,
a duplicate row hiding another RPC, a missing measured SDK, or a tampered
aggregate could pass the release-evidence chain when every remaining row was
successful. The intended 0.5.9 evidence is four complete measured SDK reports,
but the workflow did not derive and enforce that completeness contract.

## Root cause

`scripts/collect_sdk_bench_results.py` treated benchmark completeness as an
implicit property of each language harness. It did not load the canonical
generated benchmark body manifest when collecting or gating evidence. The
benchmark workflow and Pages therefore had no shared fail-closed validator for
the expected wire-RPC identity set.

## Required correction

- Load `docs/generated/bench-bodies.json` as the canonical generated RPC
  surface; reject malformed identities and duplicate canonical `service/rpc`
  rows.
- Record the canonical manifest digest, derived RPC count, measured SDK ids,
  explicit skipped SDK ids, and derived aggregate expectation in the benchmark
  artifact.
- Require Go, Python, TypeScript, and PHP exactly once with status `ok`, a
  report header count equal to the canonical surface, zero failed RPCs, and a
  `full_rpcs` multiset containing every canonical identity exactly once. Bind
  each row's public alias and operation id to the same canonical manifest row,
  and recompute row failures from normalized `err_code`/`err` evidence instead
  of trusting zero-valued summaries.
- Require C# and Java exactly once as explicit skips with no measured rows.
- Reject missing, unexpected, or duplicate SDK entries and reject any summary
  count that disagrees with the dynamically derived contract.
- Keep artifact collection and upload ahead of the final failure gate so an
  incomplete run remains diagnosable.
- Make Pages invoke the same collector gate against the benchmark artifact and
  the canonical manifest fetched from the artifact's exact release commit.
  Pages must not implement a second identity validator or validate against a
  potentially newer default-branch manifest.
- Remove Pages' legacy identity backfill. Missing wire/API/operation identity
  fields must fail artifact validation instead of being cosmetically repaired
  immediately before the assertion.
- Run the collector syntax check and completeness selftest in required PR CI,
  in addition to the push-only benchmark workflow validation.

## Regression coverage

The collector selftest covers a complete dynamic fixture plus missing, extra,
and duplicate RPC rows; report-header mismatch; missing, extra, and duplicate
SDK entries; aggregate tampering; an illegally skipped measured SDK; an
illegally measured static-only SDK; contract tampering; and duplicate canonical
manifest identities. It also covers a failed full row under zero summaries,
missing row identity, and canonical alias/operation-id mismatches. Workflow and
benchmark-harness posture guards pin the canonical-manifest handoffs and the
exact-commit Pages invocation.

## Acceptance

GitHub CI must run the collector selftest, benchmark-harness posture selftest,
workflow-posture selftest, and workflow lint. After the v0.5.9 tag is published,
the Release-triggered benchmark must emit four complete measured surfaces and
zero failures, and the resulting Pages workflow must pass the same central gate
before deployment. No local Python, Cargo, build, test, or formatting command is
used for this correction.

The first workflow-lint run (`31912397487`, job `95079351831`) confirmed that
`actionlint` accepted the workflows, then found one stale workflow-posture
selftest mutation: it attempted to replace the former single-line collector
gate command, so the new multiline `--gate` target remained intact and the
negative fixture produced no failure. The selftest now removes the exact
`--gate docs/site/bench-results.json` token and asserts the current posture
diagnostic. A successor CI/workflow-lint run must prove the corrected selftest.

## PR #35 follow-up findings

Static review of the first completeness implementation found that
`Counter(canonical_rpcs)` interpreted the manifest dictionary's
`(api_alias, operation_id)` tuple values as Counter counts. Counter subtraction
therefore raised a tuple/integer `TypeError` before a valid artifact could pass.
The gate also proved row identity but not that a row contained real timing
evidence, did not bind `service`/`rpc` back to `wire_api`, erased
`CAPABILITY_SKIPPED` into ordinary success, trusted the reported `failed_rpcs`
list, and did not bind the current history point to the current SDK evidence.

The four live report schemas were audited directly. Go emits p50/p99/mean,
min/max, and iterations; Python and PHP emit p50/p99/mean and iterations;
TypeScript emitted the three latency fields but omitted iterations even though
the harness owns the actual loop counts. The TypeScript sample/report now emits
the actual iteration count for unary, first-event, first-response, and
stream-open rows, allowing one uniform positive-integer iteration requirement.
All emitted latency values must be finite and nonnegative.

The committed v0.4.28 `docs/site/bench-results.json` contains only 376 rows per
measured SDK versus the current 381-RPC surface and predates the canonical
contract. It is preserved byte-for-byte as historical data, but the dashboard
must render it as legacy/incomplete and non-green. The initial predecessor diff
guard was still bypassable when an earlier failed deploy changed the committed
file and a later successor left it unchanged. Every no-fresh-artifact path now
accepts only the pinned v0.4.28 SHA-256
`52461f66687c1bfbdaa7c49d192ca3a3eb94fdf9ed0c19a9a6c9c34bff1708c6`
and explicitly rejects committed schema-v2/canonical-complete payloads. Only the
Release -> Benchmark workflow-run artifact may publish new green evidence.

Schema v2 distinguishes attempted RPC rows, successful measured rows,
explicit capability skips, and fatal failures. The gate recomputes all four
per SDK and in aggregate using exact integers (booleans are invalid), requires
finite measurement evidence, checks `result_status`, requires the claimed
fatal list to equal the fatal full-row set, and binds the current history
summary/SDK statuses/counts to the current payload. Selftests cover the Counter
regression, missing/invalid measurements, service/RPC mismatch, capability-skip
preservation/count tampering, aggregate tampering, typed counts, exact fatal
sets, and history tampering.

Files covered by this follow-up are `.github/workflows/pages.yml`,
`scripts/collect_sdk_bench_results.py`, `scripts/check-bench-harness-posture.py`,
`scripts/check-workflow-posture.py`, `sdk/typescript/live-auth.test.ts`,
`docs/site/benchmarks.js`, `docs/site/benchmarks.html`, `docs/site/README.md`,
this bug report, the
paired change note, and `CHANGELOG.md`. Verification remains CI-only; no local
Python, TypeScript, Cargo, build, test, or formatting command is run.

PR CI run `31914138188` at combined head `70251567` reached the quick gate and
failed only the Rust formatting check for the consolidated PostgreSQL array
match. CI repair artifact `ci-rustfmt-repair-1` (artifact `9254465048`) supplied
the exact formatting diff, which is applied without running a local formatter.
Workflow-lint run `31914138204` passed `actionlint` and then exposed another
stale negative fixture: it tried to remove an obsolete `api_alias` expression
instead of mutating the current public-identity field loop. The fixture now
removes `api_alias` from that exact loop and asserts the current diagnostic.

Final authority review found that the workflow-posture `pages_good` fixture
listed the schema-v2 rejection and digest pin after the fresh branch's closing
`fi`, even though production correctly placed them in `else`. Token-only posture
could therefore accept a regression that ran the historical pin after a fresh
artifact or moved it outside the no-fresh authority branch. The fixture now
mirrors production. The checker follows nested shell `if`/`fi` depth, requires
exactly one outer `else`, and requires schema-v2 rejection, digest calculation,
and digest comparison in that order before the outer closing `fi`. A negative
fixture closes the `else` before the pin block and proves that this scope drift
fails. Verification remains CI-only.
