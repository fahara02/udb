# UDB CI Architecture (the pipeline contract)

This is the authoritative contract for what CI runs, on which event, and which
checks gate merges. It is the macro layer of `private/masterplan/todos/15-ci-
workflow-consolidation.md`; every workflow must conform to it. When a workflow
disagrees, fix the workflow.

## Four pipelines, keyed by event

| Pipeline | Event | Purpose | Budget | Gates merge? |
| --- | --- | --- | --- | --- |
| **PR gate** | `pull_request` | fast correctness feedback | ≤ ~8 min | YES (required) |
| **Integration** | `push: main` | full live + feature coverage | ≤ ~30 min | no (post-merge) |
| **Release** | `push: tag v*` | build → publish → verify (orchestrated) | ≤ ~40 min | release-blocking |
| **Scheduled** | `schedule` + dispatch | maintenance: GHCR prune, supply-chain, drift | n/a | no |

Side-channels (not a merge concern): `pages.yml` (site/dashboard), `publish-skill.yml`.

## Dependency graphs

**PR gate** — fail-faster + low cost + subset coverage. Cheap jobs run at `t=0`
and self-fail; only the expensive jobs wait on one quick-gate. CI keeps SDK
coverage offline here: static SDK builds, mock-transport conformance, facade
sequence gates, and generated scaffold compiles. Live all-SDK/all-RPC coverage is
owned by the post-release benchmark, not by PR CI.
```
t=0 parallel: quick-gate(fmt+buf-lint+buf-breaking+version+source-posture)
              clippy-advisory
              rust · buf(generate/drift) · supply-chain · docs-links · versions
              sdk-static[go|ts|py|php|c#|java] · sdk-conformance(mock)
quick-gate ─needs→ build-broker(debug ×1) ─needs→ { smoke(merged) ‖ scaffold-compiles }
quick-gate ─needs→ feature-check[SUBSET]
```

**Integration (main)** — full coverage, parallel, `fail-fast:false`:
```
quick-gate ─needs→ build-broker → { smoke ‖ scaffold-compiles ‖ native-integration }
quick-gate ─needs→ feature-matrix[ALL 18] ‖ platform-build[targets]
```

**Release (tag)** — one guard, parallel publish fan-out after a single build.
Post-release benchmark, Pages, and cleanup are event-chained side effects, not
inline jobs in `release.yml`.
```
version-guard(×1) → build-binaries[5 targets]
   → { crates ‖ docker ‖ ts ‖ py ‖ c# ‖ packagist }   (consume the asset)
```

Post-release chain:
```
Release success -> benchmark-sdks.yml -> _live-sdk-suite.yml
Benchmark completion -> pages.yml (publishes fresh or debug benchmark JSON)
Release success / schedule / dispatch -> cleanup-packages.yml
```

## Building blocks (single source each)

Composite actions (`.github/actions/`, step-level reuse):
- `setup-rust` — toolchain + `Swatinem/rust-cache` + platform build deps.
- `broker-env` — the ONE canonical dev-posture `UDB_*` env (→ `$GITHUB_ENV`).
- `start-backends` — `docker run` minio/kafka/qdrant/redis/clickhouse/neo4j +
  topics/bucket (postgres/mongo stay as `services:` in the caller).
- `launch-broker` — bootstrap + `serve proto` + wait-ready.
- `version-guard` — `check-versions.mjs` + `versions.json` tag/version match.
- `setup-sdk-toolchains` — node/python/go/dotnet/java/php toggles.

Reusable workflows (`workflow_call`, job-level reuse):
- `_live-sdk-suite.yml` — release-binary SDK live benchmark/perf suite. Called
  by `benchmark-sdks.yml` after the top-level Release workflow or manually for
  diagnostics. It is intentionally not a CI conformance leg; CI owns only the
  offline SDK conformance/facade/scaffold gates.

Self-test + lint:
- `_selftest.yml` — `workflow_dispatch`; proves each composite on the runner
  before any gating pipeline adopts it.
- `lint-workflows.yml` — path-scoped actionlint + workflow posture over
  workflows/actions and posture-sensitive helper sources. It is not currently a
  branch-protection required check because it is intentionally `paths:`-filtered.

## Required checks (branch protection)

Only fast, deterministic PR-gate jobs are REQUIRED. Keep the names below stable;
if a required job is renamed/merged/moved-to-reusable, update branch protection in
the SAME change (see Footguns in Chapter 15).

Required (PR gate): `quick-gate`, `buf`, `versions`, `sdk-static (*)`,
`sdk-conformance`, `smoke`, `scaffold-compiles`.

Required reported check names (branch protection): `quick-gate`, `Proto (buf)`,
`Version consistency`, `PHP SDK (pest)`, `Go SDK (vet + build)`,
`TypeScript SDK (typecheck + build)`, `Python SDK (pytest)`, `C# SDK (build)`,
`Java SDK (compile)`, `SDK conformance (all languages)`, `smoke`,
`Scaffold examples compile (six SDKs)`.

Manual audit path: run `.github/workflows/branch-protection-audit.yml` against
the protected branch after any required-check rename. It runs
`scripts/check-branch-protection-lockstep.mjs`, which compares this documented
list with GitHub's required-status-check settings. The audit validates explicit
`--repo`/`GITHUB_REPOSITORY` and `--branch` inputs as canonical tokens before
calling GitHub, so padded, malformed, or non-canonical lookup targets fail as
operator-input errors. A green audit is still runner evidence, not a substitute
for opening a test PR after a rename.
Fixture-mode runner evidence must use explicit `runs`/`jobs` objects, every
expected run lane, every expected job lane array, and JSON-object job entries;
malformed fixture shape fails before parity checks read lane data.
Live GitHub API runner evidence is parsed only after an integer 2xx HTTP status
check, unpadded JSON content type check, and duplicate-key scan, so API bodies
cannot satisfy parity through malformed status metadata, misleading HTML success
pages, padded response metadata, or last-writer JSON object semantics.
Live GitHub API calls also use a 30s request timeout and destroy stalled HTTPS
requests, so the final evidence audit fails closed instead of hanging on a dead
socket.
Runner budget duration uses the canonical run start timestamp and the canonical
completion timestamp (`completed_at` before `updated_at`), matching freshness,
ordering, and job-window proof.

NOT required (advisory/slow/post-merge): `load-smoke`, `Clippy advisory`,
advisory phpstan, `feature-matrix` (integration), `native-integration`, live
SDK benchmark/perf, path-scoped `lint-workflows.yml`/`actionlint`, all release
jobs.

Notes:
- Required checks must NOT be `paths:`-filtered (a filtered required check that
  doesn't run leaves the PR stuck "Expected — waiting").
- A check that runs via `workflow_call` reports as `caller-job / called-job`; use
  that exact name in the required list.

## Migration order (and safety rails)

1. Land the building blocks (composites, reusable, self-test, actionlint) — all
   ADDITIVE, nothing existing touched. (Done in the working tree.)
2. Prove each primitive via `_selftest.yml` (dispatch) on the runner.
3. SHADOW-run new pipelines non-gating alongside the old (15.A.7); compare on real
   PR/main/tag events.
4. Cut over one pipeline at a time; update branch-protection required-check names
   in lockstep with any rename (15.A.6); delete the old only after green.

## Budget Measurement Ledger

Targets stay fixed until runner evidence says otherwise: PR gate <= ~8 min,
Integration <= ~30 min, Release <= ~40 min, post-release benchmark <= 120 min,
and post-benchmark Pages deploy <= 20 min.

Current source evidence:
- Required PR check jobs all declare timeout-minutes.
- Required PR timeout ceilings are enforced by scripts/ci-inventory.mjs.
- Critical PR artifact path: quick-gate -> build-broker -> {smoke, scaffold-compiles}.
- Cheap PR checks stay dependency-free and start at t=0.
- Timeout ceilings are source guardrails, not runner wall-clock evidence.
- `_live-sdk-suite.yml` callers are limited to `benchmark-sdks.yml` plus the
  dispatch-only `_shadow-live-sdk.yml` diagnostic; CI must not call it.
- Pages deploy, `pages: write`, and `concurrency.group: pages` are single-owned
  by `pages.yml`.
- PR broker compile count: 1 debug build in build-broker, consumed by smoke and
  scaffold-compiles via same-run artifacts.
- Release graph: ci-green -> version-guard -> build-binaries -> parallel publishers.
- Post-release benchmark runs only after top-level Release success on a v* tag.
- Release broker binary is produced by `release-binaries.yml` and consumed by
  release publishers / post-release benchmark through the release asset path.
- Manual runner-evidence audit path: run
  `.github/workflows/runner-evidence-audit.yml` after a representative PR CI,
  main CI, lint/actionlint, release run, manual release-binary dry-run,
  post-release benchmark run, post-benchmark Pages deploy, and
  branch-protection lockstep audit complete. The lint/actionlint
  evidence may come from the latest successful `workflow_dispatch`,
  `pull_request`, or `push` run, or from an exact supplied run id; non-PR lint
  evidence must be on `main`. It runs
  `scripts/check-ci-runner-evidence.mjs`, checks success conclusions, enforces
  PR <= 8 min / integration <= 30 min / release publish fanout <= 40 min /
  release-binary dry-run <= 120 min / post-release benchmark <= 120 min /
  post-benchmark Pages deploy <= 20 min / branch-protection audit <= 10 min /
  lint <= 10 min, rejects any CLI budget override above those ceilings, requires
  numeric timing/freshness overrides to be canonical positive decimals, and
  validates explicit `--release-tag` inputs before lookup so padded or malformed
  tags fail as operator-input errors. Exact `--*-run-id` overrides are also
  validated as unpadded positive integers before lookup. Explicit `--branch`
  lookup input must be a canonical branch token with no surrounding or embedded
  whitespace. Explicit `--repo`/`GITHUB_REPOSITORY` input must be a canonical
  `owner/repo` token, and the audit verifies the PR run has `quick-gate`,
  exactly one `build-broker` job, and the
  artifact-path consumers (`smoke`, `Scaffold examples compile (six SDKs)`).
  Those PR artifact-path jobs must each appear exactly once; duplicate
  `quick-gate`/`smoke`/scaffold evidence fails like a duplicate `build-broker`.
  PR evidence also verifies every documented branch-protection check above is
  present exactly once and completed successfully: `Proto (buf)`, `Version
  consistency`, all six SDK static checks, `SDK conformance (all languages)`,
  `smoke`, and scaffold compilation.
  Required/advisory job names are validated as non-empty strings without
  surrounding whitespace before matching, so padded or non-string job-name
  evidence fails explicitly instead of being counted as a missing check.
  Each audited run must expose a canonical positive `run_attempt`, and matched
  jobs must bind to that same attempt; missing run-attempt evidence fails before
  no-check-lost parity is accepted.
  PR evidence also verifies non-branch-protection PR jobs that would otherwise
  be easy to lose during consolidation: `Rust (ubuntu-latest)`,
  `Rust (windows-latest)`, `Slim build (postgres-only)`,
  `Feature check (all-features)`, `Supply chain policy`, and
  `Markdown local links + readiness artifacts`; each must appear exactly once
  and complete successfully.
  Integration
  evidence must be a `ci.yml` push on `main`; fixture evidence enforces the same
  branch identity as live auditing. It also verifies lane-specific required jobs
  are completed successfully: lint includes `actionlint`; integration includes
  the full `ci.yml` push inventory, including Rust OS jobs, release-binary
  matrix jobs, the full plugin feature matrix, static SDK jobs, `Proto (buf)`,
  `SDK conformance (all languages)`, scaffold/docs/version jobs, `smoke`, and
  the displayed live job `Native services + canonical stores (live)`; release
  includes the orchestrator/fanout jobs through `publish-packagist`, and
  `publish-go / tag sdk/go module` — the Go SDK publishes to no registry, so
  that job's `sdk/go/vX.Y.Z` tag is the only evidence Go consumers can resolve
  the release at all. That job entered the graph after v0.5.6, so auditing
  v0.5.6 or earlier reports it missing; those tags were created by hand and are
  verified with `git ls-remote --tags origin 'sdk/go/*'` instead. Release
  dry-run evidence must be a `release-binaries.yml` `workflow_dispatch` run
  with `Version guard`, `Vendored ffmpeg guard`, and build jobs for
  `udb-linux-amd64`, `udb-windows-amd64.exe`, `udb-darwin-arm64`,
  `udb-darwin-amd64`, and `udb-linux-amd64-full`. Benchmark evidence
  must be a `benchmark-sdks.yml` `workflow_run` on the same released `v*.*.*` tag
  as the audited Release run with `Release binary + SDK live benchmarks`; Pages
  evidence must be a `pages.yml` `workflow_run` on that exact tag with `build`
  and `deploy`, proving the benchmark artifact was produced and the site
  publish lane completed after it. The Release, benchmark, and Pages runs must
  also expose the same canonical unpadded 40-hex `head_sha`; missing,
  malformed, padded, or mismatched SHAs fail the audit so moved/reused tags
  cannot splice evidence from different commits. Release `head_branch` tag
  tokens must be canonical unpadded `vMAJOR.MINOR.PATCH` values too. The
  release-binary dry-run evidence must expose that same release tag and
  `head_sha`, so the manual dry-run proves the same tag/commit as the audited
  Release. The same audit requires chronological order as well: benchmark may
  not start before Release completes, and Pages may not start before benchmark
  completes. Automatic live lookup for the manual release-binary dry-run is
  tag-aware: when an exact `--release-dry-run-id` is not supplied, the audit
  searches `release-binaries.yml` `workflow_dispatch` runs on the audited
  release tag branch before applying the tag/SHA equality checks.
  Branch-protection evidence
  must be a `branch-protection-audit.yml` `workflow_dispatch` run on `main`
  with `Branch protection required checks match docs`, and must expose the same
  canonical unpadded 40-hex `head_sha` as the audited integration CI run. Those
  automatic live lookups are branch-aware too: without an exact
  `--branch-protection-run-id`, the audit requests the latest successful
  `branch-protection-audit.yml` `workflow_dispatch` run on the audited
  integration branch before applying branch/SHA equality checks. Those
  lane-specific required jobs must appear exactly once; duplicate required jobs
  fail the audit just like missing or skipped jobs.
  The audit fetches every GitHub Actions jobs page for each run before checking
  that inventory, so large workflow runs cannot hide required-job drift beyond
  the first 100 jobs. The GitHub jobs API response must include a `jobs` array
  of objects plus a stable non-negative integer `total_count`, and returned job
  rows must neither fall short of nor exceed that declared count; workflow-run
  lookup responses must include a `workflow_runs` array of objects; and exact
  run ID lookups must return a JSON object whose `id` is a canonical unpadded
  positive integer token matching the requested run. Every run object used as
  evidence must also expose a canonical unpadded positive integer `id`, and
  a canonical `https://github.com/owner/repo/actions/runs/<id>` `html_url`
  bound to that same run ID; live evidence also binds the URL's owner/repo to
  the validated `--repo`/`GITHUB_REPOSITORY` value. Fixture-mode evidence must
  also use one owner/repo consistently across all audited run URLs, so a local
  replay cannot combine green runs from multiple repositories. Distinct
  evidence lanes are compared only after that canonical token validation.
  `total_count` is capped at 500 jobs per run so evidence collection cannot be
  turned into unbounded pagination. The jobs endpoint is requested with
  `per_page=100`, and any page returning more than 100 job rows is rejected
  before required-job parity can pass.
  Automatic workflow-run discovery requests at most 100 completed run
  candidates, and non-`completed` candidates are ignored even if a malformed
  response carries `conclusion: success`; older evidence must be supplied by
  exact run ID.
  Malformed response or entry shapes fail before timing/no-check-lost evidence
  is accepted. Successful GitHub API responses must also carry an integer 2xx
  HTTP status code plus unpadded `application/json` or
  `application/vnd.github+json` content type before parsing, so malformed
  status metadata, a misleading successful HTML/text response, or padded
  response metadata cannot satisfy evidence. Each GitHub API JSON response is capped at 4 MiB before parsing,
  so live evidence collection fails closed instead of reading unbounded API bodies. Fixture-mode JSON inputs must be regular files capped
  at 1 MiB before parsing, so local evidence replay fails closed on directories
  or oversized fixtures. Fixture JSON is also scanned for duplicate object keys
  before `JSON.parse`, so a duplicate-key fixture cannot rely on last-writer
  parsing semantics to satisfy evidence.
  Every matched evidence job must also carry the same GitHub Actions `run_id` as
  the run it is being used to prove; a fixture or API response that mixes a
  successful run with jobs from another run fails before no-check-lost parity is
  accepted. If the run exposes `run_attempt`, matched evidence jobs must match
  that attempt too, so a rerun cannot be proven with successful jobs from an
  earlier attempt. Matched jobs must also expose their own canonical GitHub
  Actions job `id`, and job `id`/`run_id`/`run_attempt` values must be canonical
  unpadded positive integer tokens before comparison; matched job IDs must be
  unique within each evidence lane. Required/advisory evidence jobs must also expose valid
  `started_at`/`completed_at` timestamps inside the parent run window;
  impossible or out-of-run job timing fails the audit before budget evidence is
  accepted, while unrelated non-required jobs are ignored for this proof. Run
  and job timestamps must be canonical GitHub Actions UTC tokens
  (`YYYY-MM-DDTHH:MM:SS(.mmm)Z`); padded timestamps or offset-form timestamps
  fail before timing evidence is accepted.
  The audit also rejects run evidence older than 14 days by default; the manual
  workflow exposes `max_evidence_age_days` and passes it to
  `--max-evidence-age-days`, but the script caps that override at 14 days so it
  can tighten freshness, not loosen it.
  Timing/no-check-lost proof cannot be satisfied with stale historical green
  runs after the pipeline shape changes.
  The audit rejects reusing the same GitHub Actions run ID across evidence
  lanes, so lint, PR, integration, release, release dry-run, benchmark, Pages,
  and branch-protection proof must each come from their own run.
- PR budget evidence measures the branch-protection-required lane, not the
  entire `ci.yml` workflow duration; optional PR advisory jobs are still audited
  for no-check-lost presence/success but cannot make the merge gate miss its
  timing budget.
- Runner wall-clock evidence is still required before marking 15.A.5 done; the
  audit workflow is the evidence collection path, not a recorded green run.

## Adoption map (existing job → final form)

| Existing | Final |
| --- | --- |
| `ci.yml::grpc-reflection-smoke` + `load-smoke` | merged `smoke` job, `needs: build-broker`, uses launch-broker |
| `ci.yml::sdk-live-conformance` | removed; CI keeps offline SDK gates, post-release benchmark owns live all-SDK/all-RPC coverage |
| `ci.yml::rust` toolchain blocks | `setup-rust` |
| `ci.yml::versions` + 7 release guards | `version-guard` |
| `benchmark-sdks.yml::benchmark` | calls `_live-sdk-suite`; release binary; no own pages deploy |
| `feature-matrix.yml` | folded into ci feature jobs (PR subset / integration full) |
| inline UDB_* env (ci + benchmark) | `broker-env` |
| inline kafka/minio/qdrant setup | `start-backends` |
| `release-*` toolchain/guard blocks | `setup-rust` / `version-guard` / `setup-sdk-toolchains` |
| `release-docker.yml::cleanup` | removed; `cleanup-packages.yml` is sole owner |
| pages build/deploy in benchmark | removed; `pages.yml` is sole owner |

The full atomic plan + risks live in
`private/masterplan/todos/15-ci-workflow-consolidation.md`.
