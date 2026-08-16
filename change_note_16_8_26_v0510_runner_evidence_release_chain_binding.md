# Change note: bind runner evidence to one immutable release chain

The v0.5.10 runner-evidence audit no longer treats successful chronological run
ids as release provenance. It downloads the exact selected Benchmark artifact,
checks its recorded run id/attempt and immutable release tag, commit, asset, and
digest against the audited Release and published checksum, then downloads the
exact selected Pages artifact and requires the deployed benchmark JSON to match
the Benchmark artifact byte for byte.

Benchmark and Pages run ids are now required dispatch inputs because GitHub's
downstream `workflow_run` metadata cannot safely discover an immutable release
chain after the default branch advances. The ids select artifacts; the artifact,
published-tag, and checksum comparisons establish authority.

Source selftests cover a wrong benchmark artifact run, a Pages artifact splice,
a mismatched recorded commit, a moved release tag, and a mismatched checksum.
Workflow posture also rejects making either artifact selector optional.

Verification remains CI-only:

- `node --check scripts/check-ci-runner-evidence.mjs`
- `node scripts/check-ci-runner-evidence.mjs --selftest`
- `python scripts/check-workflow-posture.py`
- dispatch `runner-evidence-audit.yml` with the exact successor Release,
  Benchmark, and Pages run ids.

No local build, test, script execution, workflow dispatch, tag, or release was
performed.
