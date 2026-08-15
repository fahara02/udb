# Bug report: Vault destruction could commit without audit evidence

Date: 2026-08-16
Affected release: unreleased 0.5.9 integration
Target correction: 0.5.9
Severity: critical audit and irreversible-data-lifecycle boundary

## Observed

`DestroySecret` crypto-shredded every matching secret version and committed the
PostgreSQL transaction before it attempted to emit the `secret.destroyed`
event. The event used the best-effort post-commit emitter. A process failure or
outbox error after the commit therefore left an irreversibly destroyed secret
without its durable audit/CDC evidence.

## Impact

- Ciphertext and wrapped DEKs could be destroyed without a corresponding
  project-scoped outbox record.
- An outbox failure could not roll the destructive mutation back.
- Compliance consumers could observe a permanent gap for a successful
  irreversible operation.

## Required correction

Insert the exact tenant/project `secret.destroyed` outbox event through the
existing transaction-aware Vault event helper before commit. Treat enqueue
failure as an RPC failure and roll back the shred and event together. Do not
fall back to a post-commit best-effort emitter.

## Verification required

- CI must compile and run the full library suite.
- The ignored served project-store regression
  `served_vault_pins_typed_raw_and_outbox_paths_to_each_project_instance` must
  prove the destroyed event is written only to the selected project's outbox,
  then inject a missing outbox relation and prove the failed destroy rolls back
  with the secret still readable.
- No local Cargo/build/test command is run, per operator direction.
