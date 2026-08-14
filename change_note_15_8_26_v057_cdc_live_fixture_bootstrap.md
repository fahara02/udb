# Change note: CDC live fixture startup parity

Date: 2026-08-15
Release: 0.5.7 follow-up

## Change

- Initialize the fail-closed CDC topic-policy snapshot in the five direct-engine
  live tests covering mismatched-envelope DLQ routing, scoped replay, auth event
  publication, notification event publication, and HA duplicate processing.
- Restore `udb_system` after the general native-service live fixture drops all
  UDB schemas and reapplies native proto DDL, so singleton workers receive the
  same catalog prerequisites as served startup.
- Preserve production safety: `CdcEngine::new` remains unavailable until one
  complete policy generation loads, and startup still refuses CDC service when
  that load fails.

## Verification policy

No local Cargo build or test is run because local hardware is constrained and
the user required CI-only validation. GitHub Actions must pass the focused CDC
and event live filters, the pull-request checks, and the complete post-merge
`main` workflow before `v0.5.7` is published.
