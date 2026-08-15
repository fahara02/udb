# UDB v0.5.9 Notification contract changes require a native-contract major bump

Date: 2026-08-16
Status: source governance fixed; CI-generated artifacts pending
Affected paths: Notification event/service protos, native contract baseline and
generated contract documentation

## Summary

The v0.5.9 Notification correction changes contract semantics consumed by
downstream event clients:

- `NotificationSentEvent` moves its partition key from `tenant_id` to
  `recipient_ref`;
- `SendNotification` declares conditional sent and suppressed outcomes using
  the keys emitted by the served path; and
- `RetryNotification` no longer declares terminal `SUPPRESSED` rows as a legal
  retry source; and
- `NotificationLog`, `NotificationTemplate`, `NotificationPreference`, and
  `NotificationDeliveryAttempt` enforce first-class project ownership,
  tenant+project RLS, and project-aware uniqueness so physical-store colocation
  cannot collapse two projects into one logical authority.

The committed native-contract source version remained `6.0.0`, which already
identified the Backup tenant/project database-contract break. Its descriptor
baseline and generated manifest therefore still described the older
Notification partition, emission, and lifecycle contract.

## Risk

`descriptor_diff` classifies a changed event partition key or emitted-event
signature as `EventBreaking`; first-class persisted project columns also change
the native database contract. Regenerating the baseline while leaving the
independent contract version at `6.0.0` would hide intentional breaking
contracts behind an already-consumed major version. Generated documentation
would also continue to advertise an authority level that no longer matches the
source descriptor.

## Correction

- Advance `NATIVE_CONTRACT_VERSION` to `7.0.0`.
- Pin the source-only docs/CI posture guard to the new generated native-docs
  header and retain its stale-version negative fixture.
- Record the Notification event/lifecycle correction and independent contract
  major in the v0.5.9 changelog.
- Make log/template/preference/delivery-attempt project ownership part of the
  proto schema. Blank legacy ownership is quarantined because it cannot be
  safely inferred.
- Leave descriptor-derived files untouched locally. The Linux CI Rust job must
  regenerate and publish the native manifest, native-services Markdown, and
  binary descriptor baseline from the broker it compiled from this revision.

## Verification posture

No local Cargo command, build, test, rustfmt, or generated-file edit is part of
this correction. GitHub CI remains authoritative. The existing repair step runs
after `cargo build --all-targets`, verifies `target/debug/udb`, and uploads the
binary-safe `ci-native-docs.patch` as
`ci-native-docs-repair-${{ github.run_attempt }}` when a native drift or breaking
gate fails.
