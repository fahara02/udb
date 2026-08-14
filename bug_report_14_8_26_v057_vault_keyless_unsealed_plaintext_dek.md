# UDB v0.5.7 Vault is unsealed with plaintext DEKs by default when no KEK exists

Date: 2026-08-14
Status: served-path plaintext-DEK defect corrected; startup inventory/migration work remains
Affected paths: Vault KV and transit key material

## Summary

Vault is enabled by default and its Vault-specific real-KEK requirement defaults
off. Outside global fail-closed mode, the master-key helper passes plaintext
through, `check_seal` returns success, and every freshly generated data key is
stored as base64 plaintext in `data_key_wrapped`/`wrapped_key_material`. SealStatus
reports this as an unsealed "dev passthrough" Vault rather than refusing secret
operations.

## Confirmed served path

- `vault_require_master_key()` returns false when
  `UDB_VAULT_REQUIRE_MASTER_KEY` is absent.
- `DataBrokerRuntime::encrypt_secret_at_rest` returns its plaintext argument when
  no encryption runtime exists and global fail-closed mode is false.
- `check_seal` detects that its probe is not an AEAD envelope but rejects only
  when the optional Vault-specific flag is true.
- `wrap_dek` calls that same helper on the base64 DEK, so the database stores the
  usable key material without a KEK envelope.
- Native Vault is descriptor-default enabled; configuration omission is enough
  to expose the behavior.

## Consequences

- Database read access yields both ciphertext and its plaintext DEK, eliminating
  the intended envelope-encryption boundary.
- A deployment can report Vault unsealed and pass functional tests while having
  no effective secret-at-rest protection.
- Operators must know to set one of multiple independent fail-closed flags; the
  secure posture is opt-in for the component named Vault.

## Required correction

- Make a real KEK mandatory whenever native Vault is enabled, independent of the
  generic dev plaintext mode; otherwise mark Vault not-serving/sealed.
- Permit plaintext passthrough only behind an explicit development-only switch
  that cannot be enabled in production posture and is loud in readiness.
- Refuse startup or the Vault service registration when existing usable key rows
  contain non-envelope material, with a controlled migration/rewrap procedure.
- Make SealStatus/readiness expose key provider/version and a non-serving reason,
  without revealing key material.
- Add a default-configuration test proving Put/Create/Encrypt fail sealed without
  a real KEK.

## 2026-08-14 correction

- Removed the optional `UDB_VAULT_REQUIRE_MASTER_KEY` posture. The Vault seal
  gate now requires a non-empty authenticated `udb-aead:` KEK envelope in every
  served posture, even when the generic runtime permits development plaintext.
- `wrap_dek` independently rejects a plaintext/base64 passthrough before it can
  be persisted, and `unwrap_dek` refuses legacy non-envelope DEKs instead of
  silently accepting them.
- `SealStatus` can no longer report the production runtime as unsealed merely
  because generic development passthrough is active.
- Added unit guards for a default runtime with no KEK and for rejection of
  plaintext/empty pseudo-envelopes.

This closes new plaintext-DEK creation and use on the served Vault path. It does
not yet inventory existing rows at startup, provide an offline rewrap tool, or
expose provider/key-version metadata in readiness. Those deployment/migration
items remain open; affected legacy rows now fail closed when accessed.

## Verification log

- Traced Vault enablement, seal probing, runtime plaintext fallback, and DEK wrap
  persistence.
- `cargo check --lib --no-default-features --features postgres -j 2` passed
  locally after the correction (warnings only).
- Focused Vault unit execution was terminated after the local linker remained
  CPU-bound for more than ten minutes; no test result is claimed. GitHub CI is
  pending for this wave; no production data was mutated.
