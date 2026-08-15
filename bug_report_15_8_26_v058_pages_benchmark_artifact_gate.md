# Bug report: benchmark validation could republish stale Pages evidence

Date: 2026-08-15
Affected release process: 0.5.8 preparation
Severity: release-evidence integrity

## Observed

`Benchmark · SDKs` runs on `main` pushes to validate its workflow definition.
That mode intentionally produces no `sdk-benchmark-results` artifact, but every
completed benchmark workflow triggered `pages.yml`. Pages retried the missing
artifact and then deployed the committed `docs/site/bench-results.json`, whose
historical snapshot was older than the current release.

The same fallback also applied to a real benchmark-triggered deployment if its
artifact was unexpectedly absent, allowing a successful Pages run to present
old benchmark data as the newest release evidence.

## Impact

A documentation or release-preparation push could overwrite the live benchmark
dashboard with stale results even though no benchmark had run. A missing result
artifact was not distinguishable from an intentional docs-only Pages build.

## Required correction

- Skip Pages for benchmark workflow runs whose underlying event is `push`
  validation.
- Require a successful real benchmark workflow conclusion.
- When Pages has a benchmark trigger id, require the fresh
  `sdk-benchmark-results` artifact after bounded retries and fail closed if it is
  absent.
- Preserve the committed-result fallback only for direct Pages pushes that have
  no benchmark trigger.

## Evidence

Main benchmark validation run `31867114183` completed successfully without a
results artifact and triggered Pages run `31867131580`, which deployed the
committed dashboard fallback.
