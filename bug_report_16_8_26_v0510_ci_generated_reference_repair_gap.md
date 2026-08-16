# Bug report: CI could detect generated reference drift but not return its repair

## Observed

The Linux Rust job checks the canonical codebase map before the native contract
gate. A source edit that makes the map stale stops that job, while the existing
failure repair artifact regenerated only the native manifest, native docs, and
descriptor baseline. The failing run therefore had no CI-generated patch for
the codebase map or the bundled `udb-coding` reference that mirrors it.

## Impact

The repository deliberately forbids treating generated documentation as hand
authored. Under CI-only release verification, a correct source change could not
be completed from the repair artifact even though CI had the required Python
runtime and the already-built broker.

## Required correction

- Regenerate the canonical codebase map in the existing Linux failure-repair
  step.
- Synchronize the bundled skill reference from that canonical map.
- Include both files in the binary-safe repair patch alongside the native
  contract artifacts.
- Keep the freshness gates unchanged: generated drift must still fail the run
  that produced the diagnostic patch.

## Evidence

No local generator, build, or test is used. The PR CI Linux job must emit the
repair artifact when either reference is stale; the repaired commit must then
pass the same freshness gates in a new run.

Workflow-lint run `31922841467` also exposed a selftest-only parser defect:
the posture guard recognized only a bare `if: runner.os == 'Linux'` line, not
the stricter failure-repair condition
`if: failure() && runner.os == 'Linux'`. The guard now recognizes the Linux
predicate within a compound condition while still requiring it in every
generated-contract step.

The first correction was applied to a different checker with an identical
condition. Successor workflow-lint run `31923101545` proved that the generated
contract checker itself was still stale; the correction is now scoped to
`check_ci_rust_generated_contract_doc_gates`, and the unrelated public-doc
checker is restored unchanged. CI run `31923101583` also reached the skill
wrapper gate and identified the three provider mirrors that still advertised
0.5.9 after the canonical v0.5.10 version propagation. Those mirrors now match
the canonical using-UDB body exactly for the six release references.

The repaired workflow proved its purpose in that same CI run: after the Linux
build and complete library suite passed, the expected native-contract
freshness failure emitted `ci-native-docs-repair-1` artifact `9257086678`.
The artifact contains all five governed outputs, including both codebase maps,
so no generated reference was hand-authored.

CI run `31923594749` reached the remaining udb-coding sync guard and identified
one stale baseline line in each provider wrapper. The plugin reference, OpenAI
instructions, and Ollama Modelfile now mirror the canonical 0.5.10 baseline;
their curated companion content is unchanged.

CI run `31923743283` then exposed the same repair gap for the descriptor-driven
high-level SDK clients: the raw protobuf stubs and Swagger were current, but the
six robustness clients still advertised `VerifyMfaChallenge` as read-only, so
the generated SDK benchmark listing could not be refreshed. The Linux Rust job
now regenerates all six clients and both SDK benchmark documents, fails on any
`sdk/` drift, and includes that entire deterministic diff in the existing
binary-safe repair artifact.
