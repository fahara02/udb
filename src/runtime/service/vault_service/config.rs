//! Static configuration for the native `VaultService`: the entity message names,
//! the versioned outbox topics (including the V-1 audit-coverage topics), the KV
//! and transit-key state tokens, the transit-algorithm allow-list, the envelope
//! tags, the version/list/TTL bounds, the seal probe, and the dynamic
//! DB-credential lease-reaper cadence. Extracted verbatim from the former god
//! file; every value is byte-stable for downstream audit/CDC consumers.

use std::sync::OnceLock;
use std::time::Duration;

pub(crate) const VAULT_SECRET_MSG: &str = "udb.core.vault.entity.v1.VaultSecret";
pub(crate) const VAULT_TRANSIT_KEY_MSG: &str = "udb.core.vault.entity.v1.VaultTransitKey";
pub(crate) const VAULT_DB_CREDENTIAL_LEASE_MSG: &str =
    "udb.core.vault.entity.v1.VaultDbCredentialLease";

pub(crate) const TOPIC_SECRET_PUT: &str = "udb.vault.secret.put.v1";
pub(crate) const TOPIC_SECRET_ACCESSED: &str = "udb.vault.secret.accessed.v1";
pub(crate) const TOPIC_SECRET_DELETED: &str = "udb.vault.secret.deleted.v1";
pub(crate) const TOPIC_SECRET_RESTORED: &str = "udb.vault.secret.restored.v1";
pub(crate) const TOPIC_SECRET_DESTROYED: &str = "udb.vault.secret.destroyed.v1";
pub(crate) const TOPIC_KEY_CREATED: &str = "udb.vault.transit_key.created.v1";
pub(crate) const TOPIC_KEY_ROTATED: &str = "udb.vault.transit_key.rotated.v1";
pub(crate) const TOPIC_TRANSIT_DECRYPTED: &str = "udb.vault.transit.decrypted.v1";
pub(crate) const TOPIC_DB_CREDENTIAL_ISSUED: &str = "udb.vault.db_credential.issued.v1";
// Audit-coverage topics. `secret.listed` fulfils the ListSecrets event contract
// declared in the proto (`method_event_contract`); the transit.* audit topics
// give the previously-unaudited crypto RPCs (Encrypt/Sign/Hmac/Verify) the same
// tenant-bound outbox audit trail the Decrypt read already emits. Every payload
// carries only tenant/key/version metadata — NEVER plaintext, ciphertext, or key
// material (vault's redaction doctrine).
pub(crate) const TOPIC_SECRET_LISTED: &str = "udb.vault.secret.listed.v1";
pub(crate) const TOPIC_TRANSIT_ENCRYPTED: &str = "udb.vault.transit.encrypted.v1";
pub(crate) const TOPIC_TRANSIT_SIGNED: &str = "udb.vault.transit.signed.v1";
pub(crate) const TOPIC_TRANSIT_HMAC: &str = "udb.vault.transit.hmac.v1";
pub(crate) const TOPIC_TRANSIT_VERIFIED: &str = "udb.vault.transit.verified.v1";
// Dedicated audit topics for the two envelope-management crypto RPCs. Previously
// GenerateDataKey and Rewrap both emitted under `TOPIC_TRANSIT_ENCRYPTED`, which
// conflated three distinct operations on one audit stream; a compliance reviewer
// could not tell an Encrypt from a data-key mint or a post-rotation rewrap. Each
// still carries only tenant/key/version metadata — NEVER key material.
pub(crate) const TOPIC_DATA_KEY_GENERATED: &str = "udb.vault.transit.data_key_generated.v1";
pub(crate) const TOPIC_TRANSIT_REWRAPPED: &str = "udb.vault.transit.rewrapped.v1";

// KV secret version states.
pub(crate) const STATE_ACTIVE: &str = "ACTIVE";
pub(crate) const STATE_DELETED: &str = "DELETED";
pub(crate) const STATE_DESTROYED: &str = "DESTROYED";

// Transit key states — mirror the auth-service signing-key registry rotation:
// ACTIVE encrypts/signs (and decrypts/verifies); VERIFYING decrypts/verifies
// only, during the rotation overlap; RETIRED is excluded.
pub(crate) const KEY_STATE_ACTIVE: &str = "ACTIVE";
pub(crate) const KEY_STATE_VERIFYING: &str = "VERIFYING";

pub(crate) const DEFAULT_TRANSIT_ALGORITHM: &str = "aes256-gcm-siv";
/// The asymmetric signing algorithm: a key created with this algorithm signs via
/// real Ed25519 (Sign/Verify) and does nothing else — distinct from both the
/// AES-256-GCM-SIV encryption key and the dedicated HMAC-SHA256 MAC key. The
/// 32-byte transit key material doubles as the Ed25519 seed, so no separate key
/// generation is needed.
pub(crate) const SIGNING_TRANSIT_ALGORITHM: &str = "ed25519";
/// The dedicated HMAC algorithm: a key created with this algorithm MACs via
/// FIPS-198 HMAC-SHA256 (Hmac/Verify) and does nothing else. Isolating HMAC onto
/// its own algorithm keeps the three transit purposes fully separated — an
/// encryption key can no longer double as a MAC key. The 32-byte transit key
/// material is the algorithm-agnostic HMAC key, so no separate key generation is
/// needed.
pub(crate) const HMAC_TRANSIT_ALGORITHM: &str = "hmac-sha256";
/// The transit algorithms the crypto stack actually implements: the
/// AES-256-GCM-SIV envelope primitive (Encrypt/Decrypt), Ed25519 (asymmetric
/// Sign/Verify), and HMAC-SHA256 (symmetric MAC via Hmac/Verify). Each algorithm
/// serves exactly one purpose — the three are fully isolated. An unknown
/// `algorithm` on CreateTransitKey is rejected up front instead of being silently
/// coerced to the default (a latent capability lie). Extend this set only when a
/// new primitive is genuinely wired into the seal/open/sign/mac path.
pub(crate) const SUPPORTED_TRANSIT_ALGORITHMS: &[&str] = &[
    DEFAULT_TRANSIT_ALGORITHM,
    SIGNING_TRANSIT_ALGORITHM,
    HMAC_TRANSIT_ALGORITHM,
];

/// Transit ciphertext envelope tag: `udb-vault:v<keyver>:<b64(nonce||ct)>`.
/// Distinct from the broker's `udb-aead:` master-key envelope so the two layers
/// are never confused.
pub(crate) const VAULT_TRANSIT_ENVELOPE_PREFIX: &str = "udb-vault:v";
/// Transit MAC/signature envelope tag: `udb-vmac:v<keyver>:<b64(mac)>`.
pub(crate) const VAULT_HMAC_PREFIX: &str = "udb-vmac:v";
/// Ed25519 signature envelope tag: `udb-vsig:v<keyver>:<b64(sig)>`. Distinct from
/// the HMAC tag so Verify dispatches to the right primitive by the envelope alone.
pub(crate) const VAULT_ED25519_PREFIX: &str = "udb-vsig:v";

/// Bound on how many versions of a single path/key are scanned in one read.
pub(crate) const MAX_VERSIONS_SCAN: u32 = 10_000;
/// Per-list secret-path cap.
pub(crate) const MAX_LIST_SECRETS: u32 = 1_000;
pub(crate) const DEFAULT_DB_CREDENTIAL_TTL_SECONDS: i32 = 900;
pub(crate) const MIN_DB_CREDENTIAL_TTL_SECONDS: i32 = 60;
pub(crate) const DEFAULT_DB_CREDENTIAL_MAX_TTL_SECONDS: i32 = 3600;
pub const VAULT_DB_LEASE_REAPER_BATCH: i64 = 100;
/// Default cap on `BatchEncrypt` plaintext items per request (env-overridable via
/// `max_batch_encrypt_items`).
pub(crate) const MAX_BATCH_ENCRYPT_ITEMS: usize = 256;
/// Default cap on `BatchDecrypt` ciphertext items per request (env-overridable via
/// `max_batch_decrypt_items`). Same order as the encrypt cap — an unbounded
/// ciphertexts vector otherwise drives an N-element `Vec::with_capacity` plus N
/// master-key unwrap/open ops before any bound is applied.
pub(crate) const MAX_BATCH_DECRYPT_ITEMS: usize = 256;
/// Default per-tenant transit crypto-op volume cap per fixed one-minute window
/// (env-overridable via `vault_transit_quota_per_minute`). Generous by design —
/// it protects the master-key unwrap path from a single tenant monopolizing the
/// vault, not normal traffic. `0` disables the cap.
pub(crate) const VAULT_TRANSIT_QUOTA_PER_MINUTE: i64 = 6_000;
/// Constant the seal gate round-trips through `encrypt_secret_at_rest` to probe
/// master-key availability without exposing real secret material.
pub(crate) const SEAL_PROBE: &str = "udb-vault-seal-probe";
pub(crate) const VAULT_RUNTIME_REQUIRED_MESSAGE: &str =
    "vault service requires runtime native-entity dispatch (no runtime configured)";
pub(crate) const VAULT_MASTER_KEY_UNAVAILABLE_MESSAGE: &str =
    "vault is sealed: master key unavailable";

pub fn vault_db_lease_reaper_interval() -> Duration {
    static INTERVAL: OnceLock<Duration> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        Duration::from_secs(
            std::env::var("UDB_VAULT_DB_LEASE_REAPER_INTERVAL_SECS")
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(60),
        )
    })
}

/// Env-governed cap on how many versions of a single path/key are scanned in one
/// read. Mirrors the `vault_db_lease_reaper_interval` OnceLock pattern; falls back
/// to the byte-stable `MAX_VERSIONS_SCAN` default when unset/invalid.
pub(crate) fn max_versions_scan() -> u32 {
    static MAX: OnceLock<u32> = OnceLock::new();
    *MAX.get_or_init(|| {
        std::env::var("UDB_VAULT_MAX_VERSIONS_SCAN")
            .ok()
            .and_then(|value| value.trim().parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(MAX_VERSIONS_SCAN)
    })
}

/// Env-governed reaper batch size, for parity with the already-env reaper
/// interval. Falls back to the `VAULT_DB_LEASE_REAPER_BATCH` default when
/// unset/invalid.
pub fn vault_db_lease_reaper_batch() -> i64 {
    static BATCH: OnceLock<i64> = OnceLock::new();
    *BATCH.get_or_init(|| {
        std::env::var("UDB_VAULT_DB_LEASE_REAPER_BATCH")
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(VAULT_DB_LEASE_REAPER_BATCH)
    })
}

/// Env-governed cap on the number of plaintexts accepted by `BatchEncrypt` in a
/// single request; over-cap requests are rejected before any allocation/crypto.
pub(crate) fn max_batch_encrypt_items() -> usize {
    static MAX: OnceLock<usize> = OnceLock::new();
    *MAX.get_or_init(|| {
        std::env::var("UDB_VAULT_MAX_BATCH_ENCRYPT")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(MAX_BATCH_ENCRYPT_ITEMS)
    })
}

/// Env-governed cap on the number of ciphertexts accepted by `BatchDecrypt` in a
/// single request; over-cap requests are rejected before any allocation/crypto,
/// mirroring [`max_batch_encrypt_items`]. Falls back to the byte-stable
/// `MAX_BATCH_DECRYPT_ITEMS` default when `UDB_VAULT_MAX_BATCH_DECRYPT_ITEMS` is
/// unset/invalid.
pub(crate) fn max_batch_decrypt_items() -> usize {
    static MAX: OnceLock<usize> = OnceLock::new();
    *MAX.get_or_init(|| {
        std::env::var("UDB_VAULT_MAX_BATCH_DECRYPT_ITEMS")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(MAX_BATCH_DECRYPT_ITEMS)
    })
}

/// Env-governed per-tenant transit crypto-op volume cap per fixed one-minute
/// window (`UDB_VAULT_TRANSIT_QUOTA_PER_MINUTE`). A non-positive value (including the
/// explicit `0`) disables the cap; any positive value overrides the byte-stable
/// `VAULT_TRANSIT_QUOTA_PER_MINUTE` default. Mirrors the other Vault OnceLock
/// knobs.
pub(crate) fn vault_transit_quota_per_minute() -> i64 {
    static MAX: OnceLock<i64> = OnceLock::new();
    *MAX.get_or_init(|| {
        std::env::var("UDB_VAULT_TRANSIT_QUOTA_PER_MINUTE")
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .unwrap_or(VAULT_TRANSIT_QUOTA_PER_MINUTE)
    })
}
