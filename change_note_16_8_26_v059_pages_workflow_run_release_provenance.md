# Change note: bind Pages proof to the release tag, not workflow head metadata

Pages no longer exports or compares
`github.event.workflow_run.head_sha` when validating the fresh benchmark
artifact. That field identifies the default-branch workflow revision selected
by GitHub and can advance independently after an immutable release tag is
published.

The deploy remains fail closed on all authoritative evidence:

- the artifact must come from the exact triggering Benchmark run id;
- that run must have `event=workflow_run`, which is available only through the
  Benchmark workflow's Release completion trigger;
- the artifact must identify the expected repository release URL, SemVer tag,
  supported Linux asset, 40-hex commit, and 64-hex digest;
- the published tag must resolve to the artifact's commit;
- the downloaded published checksum sidecar must name the same asset and match
  the recorded digest;
- the history tail and central canonical benchmark completeness gate must pass.

The workflow posture guard now rejects `TRIGGER_HEAD_SHA`, direct use of
`github.event.workflow_run.head_sha`, or the old comparison diagnostic in
Pages. Its selftest injects that regression and requires the guard to catch it.
The runner-evidence audit still validates that every run SHA is canonical, but
no longer equates downstream workflow revisions with the release commit. Its
live mode requires explicit Benchmark and Pages run ids and now treats those
ids only as selectors, not proof. It downloads `sdk-benchmark-results` from the
selected Benchmark run, resolves the published tag and checksum, and requires
the payload's release tag, commit, asset digest, run id/attempt, and history tail
to match the audited Release. It then downloads the selected Pages
`github-pages` artifact and requires its `bench-results.json` to equal the
selected Benchmark evidence byte for byte. Cross-release run splicing therefore
fails even when all selected runs are successful, chronological, and on main.

The runner-evidence source selftest and workflow posture guard require these
artifact/run/tag/commit/checksum bindings and cover wrong-run, wrong-commit,
wrong-checksum, and Pages-artifact-splice regressions. The successor audit
workflow makes the Benchmark and Pages artifact-selector run ids required.

Verification is CI-only. Required checks are the PR quick gate, workflow lint
(including `node --check scripts/check-ci-runner-evidence.mjs` and its
`--selftest`), and a runner-evidence audit selecting the exact successor Release,
Benchmark, and Pages run ids. The automatic post-release Benchmark and
downstream Pages build/deploy must also pass for the immutable successor release
containing this correction. The published v0.5.9 tag is not moved or reused. No
local build, test, Python execution, or workflow dispatch was run.
