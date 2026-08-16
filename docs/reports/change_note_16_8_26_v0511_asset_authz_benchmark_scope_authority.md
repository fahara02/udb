# Change note: v0.5.11 Asset/Authz benchmark scope authority

Date: 2026-08-16
Release: 0.5.11

This change closes the four Go/Python product failures retained in v0.5.10 SDK
benchmark run `31941904203` (job `95152213168`, artifact `9262324159`).

## Changed

- Asset registration now stores the effective project already validated and
  resolved from the request body or bearer/header metadata. Its event payload
  carries the same canonical project.
- A project-scoped caller can immediately read and list an asset registered
  with an empty body `project_id`; a different project remains unable to see it.
- Authz policy deletion, role deletion, assignment revocation, and the role's
  assignment cascade now compile under the verified request tenant instead of
  an empty context.
- Authz identifier-only mutations fail closed when tenant authority is absent,
  and their tenant-only entity security contract does not inherit an unrelated
  project predicate.
- The human benchmark body now sends the live project explicitly and documents
  the opaque `VARCHAR(120)` project column. The served live regression keeps an
  empty body project to prove metadata fallback. Generated benchmark JSON is
  derived from the canonical Markdown and remains CI freshness-gated.

## Verification

Static source review and `git diff --check` are the only local verification.
CI must run the focused context unit tests, the Asset/Authz ignored live tests,
generated-document freshness checks, and the reusable Go/Python SDK benchmark
for the four affected RPC rows. No local Cargo/build/test/rustfmt/codegen was
run.

Focused CI at code head `0617552c55af0de03facf6102cb0351199acfad5` is green:
Asset read-after-write run `31944653811`, Authz admin/audit run `31944653364`,
and Authz role-policy run `31944653361` each executed their exact ignored live
regression with one pass and zero failures. Full PR CI and successor
post-release benchmark evidence remain required.
