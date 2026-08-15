# Bug report: v0.5.7 post-release documentation and page staleness

Date: 2026-08-15
Release: 0.5.7
Severity: release-readiness / operator-safety documentation

## Observed

The v0.5.7 package release completed successfully, but the source release notes
stopped before the final CDC, Backup, Vault, project-ownership, and storage
GC-readiness waves. The public security and operations guides therefore named
the affected subsystems without documenting their new fail-closed operational
contracts. One checked-in Go example still advertised v0.4.13 as its concrete
release-tag example.

The committed Pages benchmark payload was older still: it identified v0.4.28.
That JSON is intentionally produced by the post-release SDK benchmark and
injected by the downstream Pages workflow, so editing it by hand would create a
second, unaudited benchmark authority.

## Impact

- Operators could miss that open CDC streams are periodically reauthorized and
  that cursor/topic-policy failures no longer fall back.
- Vault deployments could miss the master-KEK envelope requirement and the
  fixed-policy boundary of dynamic database credentials.
- Backup operators could incorrectly expect atomic completion across multiple
  canonical PostgreSQL instances or restore from mutable defaults.
- Users copying the Go example could pin an obsolete broker binary.
- The live benchmark dashboard could continue showing v0.4.28 after v0.5.7 was
  published unless the benchmark-to-Pages chain completed successfully.

## Required correction

1. Extend the v0.5.7 changelog from the authoritative change notes.
2. Document the CDC, Vault, backup/topology, and project-ownership boundaries in
   the maintained security, operations, and native-service guides.
3. Refresh the stale checked-in example tag.
4. Do not edit generated docs or benchmark JSON manually.
5. Require CI for the source edit, then require an exact v0.5.7 post-release
   benchmark artifact and its downstream Pages deployment.

## Root cause

The release-critical code waves and their individual change notes were merged
after the broad v0.5.7 changelog/version refresh. Package publication correctly
waited for code CI, but the separate post-release documentation/example and
benchmark/Pages closure was not included in the final completion statement.
