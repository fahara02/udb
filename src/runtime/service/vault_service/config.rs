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
/// The transit algorithms the crypto stack actually implements. Only the
/// AES-256-GCM-SIV envelope primitive exists today, so this is the sole accepted
/// value; an unknown `algorithm` on CreateTransitKey is rejected up front instead
/// of being silently coerced to the default (a latent capability lie). Extend this
/// set only when a new primitive is genuinely wired into the seal/open path.
pub(crate) const SUPPORTED_TRANSIT_ALGORITHMS: &[&str] = &[DEFAULT_TRANSIT_ALGORITHM];

/// Transit ciphertext envelope tag: `udb-vault:v<keyver>:<b64(nonce||ct)>`.
/// Distinct from the broker's `udb-aead:` master-key envelope so the two layers
/// are never confused.
pub(crate) const VAULT_TRANSIT_ENVELOPE_PREFIX: &str = "udb-vault:v";
/// Transit MAC/signature envelope tag: `udb-vmac:v<keyver>:<b64(mac)>`.
pub(crate) const VAULT_HMAC_PREFIX: &str = "udb-vmac:v";

/// Bound on how many versions of a single path/key are scanned in one read.
pub(crate) const MAX_VERSIONS_SCAN: u32 = 10_000;
/// Per-list secret-path cap.
pub(crate) const MAX_LIST_SECRETS: u32 = 1_000;
pub(crate) const DEFAULT_DB_CREDENTIAL_TTL_SECONDS: i32 = 900;
pub(crate) const MIN_DB_CREDENTIAL_TTL_SECONDS: i32 = 60;
pub(crate) const DEFAULT_DB_CREDENTIAL_MAX_TTL_SECONDS: i32 = 3600;
pub const VAULT_DB_LEASE_REAPER_BATCH: i64 = 100;
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
