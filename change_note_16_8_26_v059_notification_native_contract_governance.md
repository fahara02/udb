# UDB v0.5.9 Notification native-contract governance

Date: 2026-08-16
Release: 0.5.9
Status: source change complete; CI-generated artifact refresh pending

## Change

The independent native contract advances from `6.0.0` to `7.0.0`. This records
the EventBreaking Notification descriptor correction separately from the
package version:

- sent-event ordering is keyed by `recipient_ref`;
- `SendNotification` exposes conditional sent and suppressed event outcomes;
  terminal opt-out suppression is not a retryable lifecycle state; and
- log/template/preference/delivery-attempt rows now enforce exact project
  ownership with tenant+project RLS and project-aware unique keys.
- sent, suppressed, failed, and delivered event messages now expose the exact
  `project_id` already carried by the served outbox payload.

The v0.5.9 changelog now describes both the customer-visible Notification
behavior and why Backup/Notification database-contract changes plus Notification
event-contract changes require the new native-contract major. Legacy
log/template/preference/delivery-attempt rows without authoritative project
provenance remain blank and inaccessible until an operator migrates or recreates
them.

## Generated artifacts

No generated file was hand-edited. GitHub CI must regenerate these files from
the merged proto and source contract:

- `docs/generated/udb-native-contract.json`;
- `docs/generated/native-services.md`; and
- `docs/generated/contract-baseline.bin`.

The existing Linux Rust repair step is still correctly ordered and scoped. It
uses the same job's compiled `target/debug/udb`, writes all three artifacts,
creates a binary diff at `ci-native-docs.patch`, and uploads artifact
`ci-native-docs-repair-${{ github.run_attempt }}`. The raw SDK/OpenAPI generation
lane remains separate and is not changed by this governance patch.

## Verification

- Source inspection confirms the Notification event changes are
  `EventBreaking` and the persisted ownership changes are database-contract
  breaking under `descriptor_diff`.
- Source inspection confirms the CI repair step runs after the Linux all-targets
  build and includes the binary baseline in its patch.
- No local Cargo/build/test/rustfmt command was run.
