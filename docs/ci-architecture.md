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
and self-fail; only the expensive jobs wait on one ~90s quick-gate:
```
t=0 parallel: quick-gate(fmt+clippy+buf-lint+buf-breaking+version)
              buf(generate/drift) · supply-chain · docs-links
              sdk-static[go|ts|py|php|c#|java] · sdk-conformance(mock) · actionlint
quick-gate ─needs→ build-broker(debug ×1) ─needs→ { smoke(merged) ‖ live-suite[conformance] }
quick-gate ─needs→ feature-check[SUBSET: slim-postgres, default, all-features]
```

**Integration (main)** — full coverage, parallel, `fail-fast:false`:
```
quick-gate ─needs→ build-broker → { smoke ‖ live-suite[conformance] ‖ native-integration }
quick-gate ─needs→ feature-matrix[ALL 18] ‖ platform-build[targets]
```

**Release (tag)** — one guard, parallel publish fan-out after a single build:
```
version-guard(×1) → build-binaries[5 targets]
   → { crates ‖ docker ‖ ts ‖ py ‖ c# ‖ packagist }   (consume the asset)
   → live-suite[perf] → pages → cleanup
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
- `_live-sdk-suite.yml` — backends + broker + SDK live tests; `mode:
  conformance|perf`. Called by ci (conformance) and the release bench (perf).

Self-test + lint:
- `_selftest.yml` — `workflow_dispatch`; proves each composite on the runner
  before any gating pipeline adopts it.
- `lint-workflows.yml` — actionlint over all workflows + actions.

## Required checks (branch protection)

Only fast, deterministic PR-gate jobs are REQUIRED. Keep the names below stable;
if a required job is renamed/merged/moved-to-reusable, update branch protection in
the SAME change (see Footguns in Chapter 15).

Required (PR gate): `quick-gate`, `buf`, `versions`, `sdk-static (*)`,
`sdk-conformance`, `smoke`, `live-suite (conformance)`, `actionlint`.

NOT required (advisory/slow/post-merge): `load-smoke`, advisory clippy/phpstan,
`feature-matrix` (integration), `native-integration`, perf, all release jobs.

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

## Adoption map (existing job → final form)

| Existing | Final |
| --- | --- |
| `ci.yml::grpc-reflection-smoke` + `load-smoke` | merged `smoke` job, `needs: build-broker`, uses launch-broker |
| `ci.yml::sdk-live-conformance` | calls `_live-sdk-suite[conformance]` |
| `ci.yml::rust` toolchain blocks | `setup-rust` |
| `ci.yml::versions` + 7 release guards | `version-guard` |
| `benchmark-sdks.yml::benchmark` | calls `_live-sdk-suite[perf]`; release binary; no own pages deploy |
| `feature-matrix.yml` | folded into ci feature jobs (PR subset / integration full) |
| inline UDB_* env (ci + benchmark) | `broker-env` |
| inline kafka/minio/qdrant setup | `start-backends` |
| `release-*` toolchain/guard blocks | `setup-rust` / `version-guard` / `setup-sdk-toolchains` |
| `release-docker.yml::cleanup` | removed; `cleanup-packages.yml` is sole owner |
| pages build/deploy in benchmark | removed; `pages.yml` is sole owner |

The full atomic plan + risks live in
`private/masterplan/todos/15-ci-workflow-consolidation.md`.
