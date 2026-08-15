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

GitHub CI run `31906893924` produced `ci-sdk-codegen-repair-1`; its applied
patch has SHA-256
`2A4B9AB4D16BD801B01A9BCFCEE9B9DD91619D66904F13CB8C02A265285BE055`
and refreshes all maintained SDK event bindings for the additive project
provenance fields. No local Buf or SDK generator was run.

The same run's successful all-target build and test compilation produced
`ci-native-docs-repair-1`; the applied binary patch has SHA-256
`7C31C3A72FF74F9DB14B82E236DC91302FC9916FF99140727D70023BBB0B9496`
and refreshes the 7.0.0 native manifest, native-services Markdown, and binary
descriptor baseline from that exact runner-built broker.
