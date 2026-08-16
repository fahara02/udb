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
