# Change note: include generated maps in the CI repair artifact

The Linux CI failure-repair step now regenerates `docs/generated/codebase-map.md`
and synchronizes the bundled `udb-coding` reference before creating
`ci-native-docs.patch`. The patch includes those two files together with the
native contract manifest, generated native-service docs, and binary descriptor
baseline.

The freshness checks still fail first and remain authoritative. This change
only makes their deterministic correction available from CI, which is required
for the v0.5.10 CI-only release workflow. No local build, test, formatter,
Python, Node, or code generator was run.

The workflow-posture selftest now accepts the production repair step's compound
`failure() && runner.os == 'Linux'` expression as Linux-scoped instead of
requiring that predicate to occupy the entire YAML condition. This corrects the
false negative reported by workflow-lint run `31922841467`; the production
workflow and its failure-only behavior are unchanged.

Successor run `31923101545` localized the duplicated-condition mistake, so the
compound-condition recognition now lives only in the generated-contract checker.
CI run `31923101583` then identified stale using-UDB provider wrappers; the
plugin reference, OpenAI instructions, and Ollama Modelfile now carry the same
v0.5.10 baseline and install commands as the canonical shared document. No local
sync generator or test was run.

CI run `31923101583` emitted the first complete repair artifact under this
contract: `ci-native-docs-repair-1` (`9257086678`). It supplied the canonical
and bundled codebase maps together with the 7.1.0 native manifest, generated
service docs, and binary descriptor baseline.
