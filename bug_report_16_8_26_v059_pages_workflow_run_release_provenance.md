# Bug report: Pages compared released benchmark evidence to a mutable workflow revision

## Observed

After the successful v0.5.9 Release run `31916838458`, the automatic Benchmark
run `31919949691` was correctly created as a `workflow_run`. The released tag
and binary resolve to commit `65587fccf40ed53e2e2b4b032808d9eb4b0d9ca0`,
but GitHub reports the Benchmark run's `head_sha` as the newer default-branch
workflow revision `6bf4840278b88820d82c5bb684ff0a3e7bcc078c`.

Pages exported that mutable `github.event.workflow_run.head_sha` as
`TRIGGER_HEAD_SHA` and required the benchmark artifact's immutable release
commit to equal it. A normal main-branch advance between release completion and
benchmark completion therefore made valid release proof undeployable.

## Impact

The check could not admit forged benchmark evidence because Pages separately
binds the artifact to the exact triggering run id, validates that the Benchmark
run was itself a Release `workflow_run`, resolves the published release tag to
the recorded commit, downloads the exact published binary checksum sidecar, and
reruns the canonical RPC completeness gate. It did, however, turn those valid
post-release artifacts into deterministic false failures whenever main moved.

That blocked the required Release -> Benchmark -> Pages proof chain even though
the released asset, tag, commit, manifest, checksum, and benchmark run were all
internally consistent.

## Required correction

- Do not treat a downstream workflow's `head_sha` as the released commit.
- Keep the exact triggering Benchmark run-id and Release-workflow event gates.
- Keep tag-to-commit resolution, release URL, binary checksum, history, and
  canonical benchmark completeness validation fail closed.
- Add workflow-posture coverage that rejects reintroducing the mutable-head
  comparison.
- Correct the runner-evidence audit, which repeated the same equality and could
  not safely auto-discover downstream runs after main advanced. Explicit run
  ids alone are not authority: the audit must download the selected Benchmark
  and Pages artifacts, bind the benchmark payload to the audited release
  tag/commit/published checksum/run id, and prove that Pages contains those
  exact bytes.

## Evidence

- Release run `31916838458`: success at tag commit `65587fcc...`.
- Benchmark run `31919949691`: `event=workflow_run`, reported
  `headSha=6bf48402...` after main advanced.
- The live benchmark resolves and executes the immutable v0.5.9 release asset;
  its artifact is expected to record the tag commit, not the later workflow
  definition commit.
- Runner-evidence live discovery now requires explicit Benchmark and Pages run
  ids because GitHub's downstream run list does not expose the originating
  release tag as authoritative metadata.
- Static review found that the explicit ids were still spliceable: chronology
  and `main` branch metadata allowed successful downstream runs from a different
  release to be paired with the audited Release. The audit now downloads
  `sdk-benchmark-results` from the selected Benchmark run and `github-pages`
  from the selected Pages run, then rejects any run-id, attempt, tag, commit,
  asset, checksum, digest, history-tail, or byte-for-byte evidence mismatch.
- Focused source selftests cover a wrong benchmark artifact run, a Pages
  artifact splice, a wrong recorded release commit, a moved tag, and a
  mismatched published checksum.
- The successor runner-evidence workflow requires both downstream run ids; it
  no longer advertises an optional mode that the safe artifact audit cannot use.
- No local build, test, Python execution, or workflow dispatch was used.
