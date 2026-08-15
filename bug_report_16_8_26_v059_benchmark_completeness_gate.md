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
