# Change note: v0.5.9 Vault destruction uses an atomic outbox

Date: 2026-08-16
Release: 0.5.9

## Changed

- `DestroySecret` now enqueues `udb.vault.secret.destroyed.v1` inside the same
  PostgreSQL transaction that crypto-shreds all secret versions.
- The outbox envelope uses the already resolved and pinned tenant/project store
  authority.
- An enqueue error aborts the transaction, preventing a committed destructive
  mutation without durable audit evidence.
- The former post-commit best-effort event call is removed.

## Verification

- No local Cargo/build/test command is run, per operator direction.
- GitHub CI must run the full library suite and the focused ignored served Vault
  project-store regression, including its missing-outbox rollback proof, before
  release acceptance.
