# Bug report: runner evidence could splice successful downstream release runs

## Observed

The runner-evidence audit accepted explicitly selected Release, Benchmark, and
Pages runs after checking workflow identity, success, chronology, branch, and
run-id shape. GitHub `workflow_run` metadata identifies the mutable default
branch revision, not the immutable release tag that caused the downstream run.
Consequently, explicit Benchmark and Pages ids from a different successful
release could be paired with the audited Release if their timestamps were later.

The workflow also described both downstream run ids as optional even though
safe artifact-bound discovery was not available and the audit failed when they
were omitted.

## Impact

The Pages deployment workflow independently validated its own triggering
artifact, so this did not permit Pages to deploy forged benchmark proof. It did
make the separate closeout attestation spliceable: an operator could produce a
green runner-evidence result whose three selected runs did not describe one
release chain.

## Correction

- Download `sdk-benchmark-results` from the exact selected Benchmark run.
- Bind its schema-v2 canonical payload to the audited release tag and commit,
  selected benchmark run id/attempt, release URL, supported asset, published tag
  resolution, checksum sidecar, digest, and current history point.
- Download `github-pages` from the exact selected Pages run, extract its root
  `bench-results.json`, and require byte-for-byte equality with the selected
  Benchmark artifact.
- Make Benchmark and Pages run ids required workflow-dispatch inputs.
- Keep artifact reads bounded and reject duplicate JSON keys, noncanonical
  identities, moved tags, checksum mismatches, and cross-run evidence splices.

## Verification posture

No local Node, Python, Cargo, SDK, or workflow execution was performed. CI must
run the runner-evidence Node syntax check and selftest, workflow posture, and an
actual audit with exact successor Release, Benchmark, and Pages run ids.
