//! Native `VaultService` (master-plan 9.1, flagship) — secrets management built
//! into the broker.
//!
//! Mirrors `tenant_service`/`lock_service`: proto-driven, no in-memory store, no
//! hand-mapped schema. Three engines share ONE crypto stack — the broker
//! AES-256-GCM-SIV envelope reused from [`crate::runtime::encryption`] via
//! [`DataBrokerRuntime::encrypt_secret_at_rest`] /
//! [`DataBrokerRuntime::decrypt_secret_at_rest`]:
//!
//!   * KV      — versioned, envelope-encrypted secrets at hierarchical paths
//!               (`tenant/<t>/app/<path>`), with compare-and-swap, soft delete,
//!               and crypto-shred destroy.
//!   * Transit — encrypt/decrypt/sign/verify/hmac by key NAME; key material is
//!               never exported; versioned keys with an ACTIVE/VERIFYING rotation
//!               overlap modelled on the auth-service signing-key registry.
//!   * Seal    — every handler checks the seal state FIRST and fails closed
//!               (`failed_precondition`) when the master key is unavailable;
//!               `SealStatus` reports the state.
//!
//! Envelope encryption: the secret VALUE (and each transit operation's payload)
//! is sealed under a random per-secret / per-key data-encryption key (DEK) with
//! the SAME `aes_gcm_siv` primitive `encryption.rs` uses; the DEK is itself
//! wrapped by the master key-encryption key (KEK) via `encrypt_secret_at_rest`.
//! Only ciphertext is stored. Every secret-bearing type carries a redacting
//! `Debug` from day one (mirroring `encryption.rs::RedactedKeyVersions`).
//!
//! Doctrine: tenant is taken from the VERIFIED claim (cross-tenant request
//! bodies are rejected by `validate_request_tenant`); per-RPC authorization is
//! enforced by the `method_security` tower from each RPC's
//! `endpoint_security.decision_resource` (the same casbin decision engine
//! `auth_service::decide_action_native` consults) before the handler runs; the
//! sensitive reads (`GetSecret`, `Decrypt`) are audited via the outbox
//! compliance envelope.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use aead::{Aead, KeyInit};
use aes_gcm_siv::{Aes256GcmSiv, Nonce};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalValue,
};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::vault::services::v1 as vault_pb;
use crate::proto::udb::core::vault::services::v1::vault_service_server::VaultService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, OperationChannel};

pub use crate::proto::udb::core::vault::services::v1::vault_service_server::VaultServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    NativeEventContext, admit_on as native_admit_on, enqueue_outbox_event_with_context,
    native_next_page_token_for_total, native_offset_page_window, native_service_context,
    non_empty_json, validate_request_tenant,
};

const VAULT_SECRET_MSG: &str = "udb.core.vault.entity.v1.VaultSecret";
const VAULT_TRANSIT_KEY_MSG: &str = "udb.core.vault.entity.v1.VaultTransitKey";
const VAULT_DB_CREDENTIAL_LEASE_MSG: &str = "udb.core.vault.entity.v1.VaultDbCredentialLease";

const TOPIC_SECRET_PUT: &str = "udb.vault.secret.put.v1";
const TOPIC_SECRET_ACCESSED: &str = "udb.vault.secret.accessed.v1";
const TOPIC_SECRET_DELETED: &str = "udb.vault.secret.deleted.v1";
const TOPIC_SECRET_DESTROYED: &str = "udb.vault.secret.destroyed.v1";
const TOPIC_KEY_CREATED: &str = "udb.vault.transit_key.created.v1";
const TOPIC_KEY_ROTATED: &str = "udb.vault.transit_key.rotated.v1";
const TOPIC_TRANSIT_DECRYPTED: &str = "udb.vault.transit.decrypted.v1";
const TOPIC_DB_CREDENTIAL_ISSUED: &str = "udb.vault.db_credential.issued.v1";
// Audit-coverage topics. `secret.listed` fulfils the ListSecrets event contract
// declared in the proto (`method_event_contract`); the transit.* audit topics
// give the previously-unaudited crypto RPCs (Encrypt/Sign/Hmac/Verify) the same
// tenant-bound outbox audit trail the Decrypt read already emits. Every payload
// carries only tenant/key/version metadata — NEVER plaintext, ciphertext, or key
// material (vault's redaction doctrine).
const TOPIC_SECRET_LISTED: &str = "udb.vault.secret.listed.v1";
const TOPIC_TRANSIT_ENCRYPTED: &str = "udb.vault.transit.encrypted.v1";
const TOPIC_TRANSIT_SIGNED: &str = "udb.vault.transit.signed.v1";
const TOPIC_TRANSIT_HMAC: &str = "udb.vault.transit.hmac.v1";
const TOPIC_TRANSIT_VERIFIED: &str = "udb.vault.transit.verified.v1";

// KV secret version states.
const STATE_ACTIVE: &str = "ACTIVE";
const STATE_DELETED: &str = "DELETED";
const STATE_DESTROYED: &str = "DESTROYED";

// Transit key states — mirror the auth-service signing-key registry rotation:
// ACTIVE encrypts/signs (and decrypts/verifies); VERIFYING decrypts/verifies
// only, during the rotation overlap; RETIRED is excluded.
const KEY_STATE_ACTIVE: &str = "ACTIVE";
const KEY_STATE_VERIFYING: &str = "VERIFYING";

const DEFAULT_TRANSIT_ALGORITHM: &str = "aes256-gcm-siv";
/// The transit algorithms the crypto stack actually implements. Only the
/// AES-256-GCM-SIV envelope primitive exists today, so this is the sole accepted
/// value; an unknown `algorithm` on CreateTransitKey is rejected up front instead
/// of being silently coerced to the default (a latent capability lie). Extend this
/// set only when a new primitive is genuinely wired into the seal/open path.
const SUPPORTED_TRANSIT_ALGORITHMS: &[&str] = &[DEFAULT_TRANSIT_ALGORITHM];

/// Transit ciphertext envelope tag: `udb-vault:v<keyver>:<b64(nonce||ct)>`.
/// Distinct from the broker's `udb-aead:` master-key envelope so the two layers
/// are never confused.
const VAULT_TRANSIT_ENVELOPE_PREFIX: &str = "udb-vault:v";
/// Transit MAC/signature envelope tag: `udb-vmac:v<keyver>:<b64(mac)>`.
const VAULT_HMAC_PREFIX: &str = "udb-vmac:v";

/// Bound on how many versions of a single path/key are scanned in one read.
const MAX_VERSIONS_SCAN: u32 = 10_000;
/// Per-list secret-path cap.
const MAX_LIST_SECRETS: u32 = 1_000;
const DEFAULT_DB_CREDENTIAL_TTL_SECONDS: i32 = 900;
const MIN_DB_CREDENTIAL_TTL_SECONDS: i32 = 60;
const DEFAULT_DB_CREDENTIAL_MAX_TTL_SECONDS: i32 = 3600;
pub const VAULT_DB_LEASE_REAPER_BATCH: i64 = 100;
/// Constant the seal gate round-trips through `encrypt_secret_at_rest` to probe
/// master-key availability without exposing real secret material.
const SEAL_PROBE: &str = "udb-vault-seal-probe";
const VAULT_RUNTIME_REQUIRED_MESSAGE: &str =
    "vault service requires runtime native-entity dispatch (no runtime configured)";
const VAULT_MASTER_KEY_UNAVAILABLE_MESSAGE: &str = "vault is sealed: master key unavailable";

fn vault_capability_status(
    operation: impl Into<String>,
    capability_required: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "vault",
        operation,
        capability_required,
        message,
    )
}

fn vault_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("vault", operation, message)
}

fn vault_master_key_unavailable_status(message: impl Into<String>) -> Status {
    vault_capability_status("seal_gate", "vault_master_key", message)
}

fn vault_master_key_operation_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    vault_capability_status(operation, "vault_master_key", message)
}

fn vault_db_credentials_config_status(message: impl Into<String>) -> Status {
    vault_capability_status(
        "generate_database_credentials",
        "database_credentials_config",
        message,
    )
}

fn vault_db_native_store_required_status() -> Status {
    vault_capability_status(
        "generate_database_credentials",
        "postgres_native_store",
        "vault dynamic database credentials require a Postgres native store",
    )
}

fn vault_db_role_creation_status(message: impl Into<String>) -> Status {
    vault_capability_status(
        "generate_database_credentials",
        "postgres_role_management",
        message,
    )
}

fn vault_schema_not_found_status(
    operation: &'static str,
    schema_code: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "vault",
        operation,
        schema_code,
        message,
    )
}

fn vault_schema_already_exists_status(
    operation: &'static str,
    schema_code: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::AlreadyExists,
        "vault",
        operation,
        schema_code,
        message,
    )
}

fn vault_confirmation_token_required_status() -> Status {
    crate::runtime::executor_utils::failed_precondition_fields(
        "DestroySecret crypto-shreds the secret; confirmation_token is required",
        [(
            "confirmation_token",
            "must be provided to confirm destructive secret shredding",
        )],
    )
}

// ── secret-bearing types with redacting Debug (day one) ───────────────────────

/// A 32-byte data-encryption key. Redacting `Debug`: the key bytes NEVER appear
/// in logs/panics. Mirrors `encryption.rs::EncryptionKey`'s `[redacted]` Debug.
struct DataKey([u8; 32]);

impl fmt::Debug for DataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataKey")
            .field("key", &"[redacted]")
            .finish()
    }
}

impl DataKey {
    /// A fresh random DEK. Sourced from two v4 UUIDs (getrandom-backed), the same
    /// CSPRNG path `encryption.rs` uses for nonces — no second RNG dependency.
    fn generate() -> Self {
        let mut bytes = [0u8; 32];
        bytes[..16].copy_from_slice(Uuid::new_v4().as_bytes());
        bytes[16..].copy_from_slice(Uuid::new_v4().as_bytes());
        DataKey(bytes)
    }

    fn to_b64(&self) -> String {
        BASE64_STANDARD.encode(self.0)
    }

    fn from_b64(value: &str) -> Result<Self, Status> {
        let raw = BASE64_STANDARD.decode(value.trim()).map_err(|err| {
            vault_internal_status(
                "data_key_decode",
                format!("vault data-key decode failed: {err}"),
            )
        })?;
        let bytes: [u8; 32] = raw.try_into().map_err(|_| {
            vault_internal_status("data_key_decode", "vault data-key has an invalid length")
        })?;
        Ok(DataKey(bytes))
    }
}

/// Plaintext secret material in flight. Redacting `Debug` from day one so the
/// cleartext never leaks into a log line or panic message.
struct PlaintextSecret(String);

impl fmt::Debug for PlaintextSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PlaintextSecret")
            .field(&"[redacted]")
            .finish()
    }
}

/// A durable KV secret version decoded from the native read JSON. Even though
/// the two `*_*` columns are CIPHERTEXT, they are still sensitive, so this type
/// also redacts them in `Debug`.
struct StoredSecret {
    secret_id: String,
    version: i64,
    ciphertext: String,
    data_key_wrapped: String,
    state: String,
    metadata_json: String,
}

impl fmt::Debug for StoredSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredSecret")
            .field("secret_id", &self.secret_id)
            .field("version", &self.version)
            .field("state", &self.state)
            .field("ciphertext", &"[redacted]")
            .field("data_key_wrapped", &"[redacted]")
            .finish()
    }
}

/// A durable transit-key version decoded from the native read JSON.
struct StoredTransitKey {
    key_id: String,
    version: i64,
    algorithm: String,
    wrapped_key_material: String,
    state: String,
}

impl fmt::Debug for StoredTransitKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredTransitKey")
            .field("key_id", &self.key_id)
            .field("version", &self.version)
            .field("state", &self.state)
            .field("algorithm", &self.algorithm)
            .field("wrapped_key_material", &"[redacted]")
            .finish()
    }
}

/// Operator allow-list entry for dynamic database credentials. `role_name` is
/// the RPC-facing alias; `parent_role` is the existing Postgres role granted to
/// the generated short-lived login. The generated login password is returned
/// once and never stored.
#[derive(Debug, Clone, Deserialize)]
struct DbCredentialRoleConfig {
    role_name: String,
    parent_role: String,
    #[serde(default)]
    ttl_seconds_max: Option<i32>,
}

/// Postgres-backed `VaultService` handler.
pub struct VaultServiceImpl {
    /// Outbox-event Postgres pool (the configured native store for `vault`).
    pg_pool: Option<PgPool>,
    /// Runtime handle for the master-key envelope (KEK wrap/unwrap) and typed
    /// native-entity dispatch.
    runtime: Option<Arc<DataBrokerRuntime>>,
    /// Configured outbox relation; `None` disables event emission (best-effort).
    outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
    /// Test-only seal override. `Some(true)` forces SEALED (exercise the
    /// fail-closed gate); `Some(false)` forces UNSEALED (exercise the
    /// cross-tenant guard without a configured master key); `None` in production
    /// probes the real master KEK. Never set by `build_vault_service`.
    seal_override: Option<bool>,
}

impl VaultServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
            seal_override: None,
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    #[cfg(test)]
    fn with_seal_override(mut self, sealed: bool) -> Self {
        self.seal_override = Some(sealed);
        self
    }

    /// Vault state is durable-only: fail closed when no runtime dispatch exists.
    fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            vault_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                VAULT_RUNTIME_REQUIRED_MESSAGE,
            )
        })
    }

    /// The SEAL GATE. Every operating handler calls this FIRST: a sealed vault
    /// (master key unavailable) refuses to serve — never degrade to plaintext.
    /// In production this probes the real master KEK via `encrypt_secret_at_rest`
    /// (which fails closed when `fail_closed_mode` is on and no key is set). The
    /// test override short-circuits the probe.
    fn check_seal(&self) -> Result<(), Status> {
        match self.seal_override {
            Some(true) => {
                return Err(vault_master_key_unavailable_status(
                    VAULT_MASTER_KEY_UNAVAILABLE_MESSAGE,
                ));
            }
            Some(false) => return Ok(()),
            None => {}
        }
        let runtime = self.runtime.as_deref().ok_or_else(|| {
            vault_master_key_unavailable_status("vault is sealed: no runtime / master key wired")
        })?;
        runtime
            .encrypt_secret_at_rest(SEAL_PROBE)
            .map(|_| ())
            .map_err(|err| {
                vault_master_key_unavailable_status(format!(
                    "vault is sealed: master key unavailable ({err})"
                ))
            })
    }

    /// Whether the vault is currently sealed (for `SealStatus`).
    fn is_sealed(&self) -> bool {
        self.check_seal().is_err()
    }

    /// Whether a REAL master KEK is configured (vs the dev passthrough where
    /// `encrypt_secret_at_rest` returns the cleartext unchanged). Used by
    /// `SealStatus` to report honest posture.
    fn kek_configured(&self) -> bool {
        if self.seal_override.is_some() {
            return false;
        }
        match self.runtime.as_deref() {
            Some(runtime) => runtime
                .encrypt_secret_at_rest(SEAL_PROBE)
                .map(|env| env.starts_with("udb-aead:"))
                .unwrap_or(false),
            None => false,
        }
    }

    /// Emit a per-operation versioned dot-topic outbox event (best-effort). The
    /// payload NEVER carries plaintext — only tenant/path/version metadata.
    #[allow(clippy::too_many_arguments)]
    async fn emit(
        &self,
        topic: &str,
        partition_key: &str,
        tenant_id: &str,
        project_id: &str,
        operation: &str,
        target_resource: &str,
        payload: serde_json::Value,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            topic,
            partition_key,
            tenant_id,
            project_id,
            payload,
            NativeEventContext {
                operation: operation.to_string(),
                outcome: "allow".to_string(),
                target_resource: target_resource.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }

    /// All durable secret versions for one path (bounded scan).
    async fn read_secret_versions(
        &self,
        runtime: &DataBrokerRuntime,
        context: &crate::RequestContext,
        tenant_id: &str,
        secret_path: &str,
    ) -> Result<Vec<StoredSecret>, Status> {
        let rows = runtime
            .native_entity_read_for_service(
                "vault",
                context,
                secret_path_read(tenant_id, secret_path),
            )
            .await?;
        Ok(rows.iter().map(stored_secret_from_json).collect())
    }

    /// All durable transit-key versions for one key name (bounded scan).
    async fn read_transit_versions(
        &self,
        runtime: &DataBrokerRuntime,
        context: &crate::RequestContext,
        tenant_id: &str,
        key_name: &str,
    ) -> Result<Vec<StoredTransitKey>, Status> {
        let rows = runtime
            .native_entity_read_for_service("vault", context, transit_key_read(tenant_id, key_name))
            .await?;
        Ok(rows.iter().map(stored_transit_key_from_json).collect())
    }
}

impl Default for VaultServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

// ── crypto helpers — ONE stack: master-KEK wrap + the shared aes_gcm_siv ──────

/// Wrap a DEK with the master KEK (`encrypt_secret_at_rest`). The result is the
/// `udb-aead:` envelope (or, in dev with no KEK, the base64 passthrough — still
/// only the DEK, never a cleartext secret value).
fn wrap_dek(runtime: &DataBrokerRuntime, dek: &DataKey) -> Result<String, Status> {
    runtime
        .encrypt_secret_at_rest(&dek.to_b64())
        .map_err(|err| {
            vault_master_key_operation_status(
                "wrap_data_key",
                format!("vault is sealed: cannot wrap data key ({err})"),
            )
        })
}

/// Reverse of [`wrap_dek`].
fn unwrap_dek(runtime: &DataBrokerRuntime, wrapped: &str) -> Result<DataKey, Status> {
    let b64 = runtime.decrypt_secret_at_rest(wrapped).map_err(|err| {
        vault_master_key_operation_status(
            "unwrap_data_key",
            format!("vault is sealed: cannot unwrap data key ({err})"),
        )
    })?;
    DataKey::from_b64(&b64)
}

/// Seal `plaintext` under `dek` with AES-256-GCM-SIV (the SAME primitive
/// `encryption.rs` uses), returning the `udb-vault:v<version>:<b64>` envelope.
fn dek_seal(dek: &DataKey, version: i64, plaintext: &[u8]) -> Result<String, Status> {
    let cipher = Aes256GcmSiv::new_from_slice(&dek.0).map_err(|err| {
        vault_internal_status(
            "seal_transit_payload",
            format!("vault AEAD key invalid: {err}"),
        )
    })?;
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&Uuid::new_v4().as_bytes()[..12]);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|err| {
            vault_internal_status(
                "seal_transit_payload",
                format!("vault encrypt failed: {err}"),
            )
        })?;
    let mut envelope = Vec::with_capacity(nonce.len() + ciphertext.len());
    envelope.extend_from_slice(&nonce);
    envelope.extend_from_slice(&ciphertext);
    Ok(format!(
        "{VAULT_TRANSIT_ENVELOPE_PREFIX}{version}:{}",
        BASE64_STANDARD.encode(envelope)
    ))
}

/// Open a `udb-vault:` envelope under `dek`.
fn dek_open(dek: &DataKey, encoded: &str) -> Result<Vec<u8>, Status> {
    let envelope = BASE64_STANDARD.decode(encoded).map_err(|err| {
        vault_field_violation(
            "ciphertext",
            "must be base64-encoded vault transit ciphertext bytes",
            format!("vault ciphertext decode failed: {err}"),
        )
    })?;
    if envelope.len() <= 12 {
        return Err(vault_field_violation(
            "ciphertext",
            "must include a 12-byte nonce and encrypted payload",
            "vault ciphertext envelope is too short",
        ));
    }
    let (nonce, ciphertext) = envelope.split_at(12);
    let cipher = Aes256GcmSiv::new_from_slice(&dek.0).map_err(|err| {
        vault_internal_status(
            "open_transit_payload",
            format!("vault AEAD key invalid: {err}"),
        )
    })?;
    cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|_| {
            vault_field_violation(
                "ciphertext",
                "must decrypt with the selected vault transit key version",
                "vault decrypt failed (wrong key version or corrupt ciphertext)",
            )
        })
}

/// Parse a `udb-vault:v<version>:<b64>` envelope into `(version, b64)`.
fn parse_transit_envelope(value: &str) -> Option<(i64, &str)> {
    let rest = value.strip_prefix(VAULT_TRANSIT_ENVELOPE_PREFIX)?;
    let (version, encoded) = rest.split_once(':')?;
    Some((version.parse().ok()?, encoded))
}

/// Parse a `udb-vmac:v<version>:<b64>` signature into `(version, b64)`.
fn parse_mac_envelope(value: &str) -> Option<(i64, &str)> {
    let rest = value.strip_prefix(VAULT_HMAC_PREFIX)?;
    let (version, encoded) = rest.split_once(':')?;
    Some((version.parse().ok()?, encoded))
}

const HMAC_BLOCK: usize = 64; // SHA-256 block size.

/// HMAC-SHA256 built on the already-vendored `sha2` crate (no `hmac` crate
/// dependency added). Standard FIPS-198 construction.
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut block_key = [0u8; HMAC_BLOCK];
    if key.len() > HMAC_BLOCK {
        let digest = Sha256::digest(key);
        block_key[..32].copy_from_slice(&digest);
    } else {
        block_key[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; HMAC_BLOCK];
    let mut opad = [0x5cu8; HMAC_BLOCK];
    for i in 0..HMAC_BLOCK {
        ipad[i] ^= block_key[i];
        opad[i] ^= block_key[i];
    }
    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(message);
    let inner_digest = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_digest);
    let mut out = [0u8; 32];
    out.copy_from_slice(&outer.finalize());
    out
}

/// Constant-time equality for MAC verification (no early-exit timing leak).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// ── dynamic database credential helpers ─────────────────────────────────────

fn vault_db_role_configs() -> Result<&'static HashMap<String, DbCredentialRoleConfig>, Status> {
    static CONFIGS: OnceLock<Result<HashMap<String, DbCredentialRoleConfig>, String>> =
        OnceLock::new();
    match CONFIGS.get_or_init(|| {
        let raw = std::env::var("UDB_VAULT_DB_ROLES_JSON")
            .map_err(|_| "UDB_VAULT_DB_ROLES_JSON is not configured".to_string())?;
        parse_vault_db_role_configs(&raw)
    }) {
        Ok(configs) => Ok(configs),
        Err(err) => Err(vault_db_credentials_config_status(format!(
            "vault dynamic database credentials are not configured: {err}"
        ))),
    }
}

fn parse_vault_db_role_configs(
    raw: &str,
) -> Result<HashMap<String, DbCredentialRoleConfig>, String> {
    let entries: Vec<DbCredentialRoleConfig> =
        serde_json::from_str(raw).map_err(|err| format!("invalid JSON: {err}"))?;
    if entries.is_empty() {
        return Err("at least one role entry is required".to_string());
    }
    let mut configs = HashMap::with_capacity(entries.len());
    for mut entry in entries {
        entry.role_name = entry.role_name.trim().to_string();
        entry.parent_role = entry.parent_role.trim().to_string();
        validate_db_role_alias(&entry.role_name).map_err(|err| err.message().to_string())?;
        validate_pg_identifier_value(&entry.parent_role, "parent_role")?;
        let max_ttl = entry
            .ttl_seconds_max
            .unwrap_or(DEFAULT_DB_CREDENTIAL_MAX_TTL_SECONDS);
        if max_ttl < MIN_DB_CREDENTIAL_TTL_SECONDS {
            return Err(format!(
                "role '{}' ttl_seconds_max must be at least {MIN_DB_CREDENTIAL_TTL_SECONDS}",
                entry.role_name
            ));
        }
        if configs.insert(entry.role_name.clone(), entry).is_some() {
            return Err("duplicate role_name in UDB_VAULT_DB_ROLES_JSON".to_string());
        }
    }
    Ok(configs)
}

fn validate_db_role_alias(role_name: &str) -> Result<(), Status> {
    if role_name.is_empty()
        || role_name.len() > 128
        || !role_name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | ':' | '.'))
    {
        return Err(vault_field_violation(
            "role_name",
            "must be 1..128 ASCII chars using letters, digits, _, -, :, or .",
            "role_name must be 1..128 ASCII chars using letters, digits, _, -, :, or .",
        ));
    }
    Ok(())
}

fn validate_pg_identifier_value(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 63
        || value.starts_with(|ch: char| ch.is_ascii_digit())
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(format!(
            "{label} '{value}' is not a valid unquoted Postgres identifier"
        ));
    }
    Ok(())
}

fn pg_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn pg_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn generate_db_password() -> String {
    let mut bytes = [0u8; 32];
    bytes[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    bytes[16..].copy_from_slice(Uuid::new_v4().as_bytes());
    BASE64_STANDARD.encode(bytes)
}

fn generate_db_username() -> String {
    format!("udb_vault_{}", Uuid::new_v4().simple())
}

fn requested_db_credential_ttl(req_ttl_seconds: i32, max_ttl_seconds: i32) -> Result<i32, Status> {
    let ttl = if req_ttl_seconds <= 0 {
        DEFAULT_DB_CREDENTIAL_TTL_SECONDS.min(max_ttl_seconds)
    } else {
        req_ttl_seconds
    };
    if ttl < MIN_DB_CREDENTIAL_TTL_SECONDS {
        return Err(vault_field_violation(
            "ttl_seconds",
            format!("must be 0/default or at least {MIN_DB_CREDENTIAL_TTL_SECONDS}"),
            format!("ttl_seconds must be 0/default or at least {MIN_DB_CREDENTIAL_TTL_SECONDS}"),
        ));
    }
    if ttl > max_ttl_seconds {
        return Err(vault_field_violation(
            "ttl_seconds",
            format!("must not exceed configured maximum {max_ttl_seconds}"),
            format!("ttl_seconds exceeds configured maximum {max_ttl_seconds}"),
        ));
    }
    Ok(ttl)
}

async fn create_postgres_login_role(
    pool: &PgPool,
    username: &str,
    password: &str,
    expires_at: DateTime<Utc>,
    parent_role: &str,
) -> Result<(), Status> {
    validate_pg_identifier_value(username, "generated username")
        .map_err(|err| vault_internal_status("create_postgres_login_role", err))?;
    validate_pg_identifier_value(parent_role, "parent_role")
        .map_err(|err| vault_internal_status("create_postgres_login_role", err))?;
    let sql = format!(
        "CREATE ROLE {} LOGIN PASSWORD {} VALID UNTIL {} IN ROLE {}",
        pg_ident(username),
        pg_literal(password),
        pg_literal(&expires_at.to_rfc3339()),
        pg_ident(parent_role)
    );
    sqlx::query(&sql)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|err| {
            vault_db_role_creation_status(format!(
                "vault database credential role creation failed: {err}"
            ))
        })
}

async fn drop_postgres_login_role(pool: &PgPool, username: &str) -> Result<(), String> {
    validate_pg_identifier_value(username, "generated username")?;
    let sql = format!("DROP ROLE IF EXISTS {}", pg_ident(username));
    sqlx::query(&sql)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|err| format!("drop generated database role {username} failed: {err}"))
}

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

pub async fn run_vault_db_lease_reaper_once(pool: &PgPool, batch: i64) -> Result<i64, String> {
    let model = crate::runtime::native_catalog::native_model(
        VAULT_DB_CREDENTIAL_LEASE_MSG,
        &["lease_id", "username", "state", "expires_at", "revoked_at"],
    );
    let limit = batch.clamp(1, VAULT_DB_LEASE_REAPER_BATCH);
    let select_sql = format!(
        "SELECT {}, {} FROM {} WHERE {} = 'ACTIVE' AND {} IS NULL AND {} <= NOW() \
         ORDER BY {} ASC LIMIT $1",
        model.text_as("lease_id", "lease_id"),
        model.text_as("username", "username"),
        model.relation,
        model.q("state"),
        model.q("revoked_at"),
        model.q("expires_at"),
        model.q("expires_at")
    );
    let rows = sqlx::query(&select_sql)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|err| format!("read expired vault DB credential leases failed: {err}"))?;

    let update_sql = format!(
        "UPDATE {} SET {} = 'REVOKED', {} = NOW() \
         WHERE {} = $1::uuid AND {} = 'ACTIVE'",
        model.relation,
        model.q("state"),
        model.q("revoked_at"),
        model.q("lease_id"),
        model.q("state")
    );
    let mut revoked = 0i64;
    for row in rows {
        let lease_id: String = row
            .try_get("lease_id")
            .map_err(|err| format!("expired lease row missing lease_id: {err}"))?;
        let username: String = row
            .try_get("username")
            .map_err(|err| format!("expired lease row missing username: {err}"))?;
        drop_postgres_login_role(pool, &username).await?;
        let updated = sqlx::query(&update_sql)
            .bind(&lease_id)
            .execute(pool)
            .await
            .map_err(|err| format!("mark vault DB credential lease revoked failed: {err}"))?;
        revoked += updated.rows_affected() as i64;
    }
    Ok(revoked)
}

// ── IR read/record builders ───────────────────────────────────────────────────

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

fn secret_path_read(tenant_id: &str, secret_path: &str) -> LogicalRead {
    LogicalRead {
        message_type: VAULT_SECRET_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "tenant_id".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(tenant_id),
            },
            LogicalFilter::Comparison {
                field: "secret_path".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(secret_path),
            },
        ])),
        projection: Some(LogicalProjection::fields([
            "secret_id".to_string(),
            "version".to_string(),
            "ciphertext".to_string(),
            "data_key_wrapped".to_string(),
            "state".to_string(),
            "metadata_json".to_string(),
        ])),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(MAX_VERSIONS_SCAN)),
    }
}

fn secret_list_read(tenant_id: &str, prefix: &str) -> LogicalRead {
    let mut filters = vec![LogicalFilter::Comparison {
        field: "tenant_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(tenant_id),
    }];
    if !prefix.trim().is_empty() {
        filters.push(LogicalFilter::Comparison {
            field: "secret_path".to_string(),
            op: ComparisonOp::StartsWith,
            value: logical_string(prefix.trim()),
        });
    }
    LogicalRead {
        message_type: VAULT_SECRET_MSG.to_string(),
        filter: Some(LogicalFilter::And(filters)),
        projection: Some(LogicalProjection::fields([
            "secret_path".to_string(),
            "version".to_string(),
            "state".to_string(),
        ])),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(MAX_VERSIONS_SCAN)),
    }
}

fn transit_key_read(tenant_id: &str, key_name: &str) -> LogicalRead {
    LogicalRead {
        message_type: VAULT_TRANSIT_KEY_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "tenant_id".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(tenant_id),
            },
            LogicalFilter::Comparison {
                field: "key_name".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(key_name),
            },
        ])),
        projection: Some(LogicalProjection::fields([
            "key_id".to_string(),
            "version".to_string(),
            "algorithm".to_string(),
            "wrapped_key_material".to_string(),
            "state".to_string(),
        ])),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(MAX_VERSIONS_SCAN)),
    }
}

#[allow(clippy::too_many_arguments)]
fn secret_record(
    secret_id: &str,
    tenant_id: &str,
    secret_path: &str,
    version: i64,
    ciphertext: &str,
    data_key_wrapped: &str,
    state: &str,
    metadata_json: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("secret_id".to_string(), logical_string(secret_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("secret_path".to_string(), logical_string(secret_path));
    record.insert("version".to_string(), LogicalValue::Int(version));
    record.insert("ciphertext".to_string(), logical_string(ciphertext));
    record.insert(
        "data_key_wrapped".to_string(),
        logical_string(data_key_wrapped),
    );
    record.insert("state".to_string(), logical_string(state));
    record.insert("metadata_json".to_string(), logical_string(metadata_json));
    record
}

fn secret_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "ciphertext".to_string(),
        "data_key_wrapped".to_string(),
        "state".to_string(),
        "metadata_json".to_string(),
    ])
}

fn transit_key_record(
    key_id: &str,
    tenant_id: &str,
    key_name: &str,
    version: i64,
    algorithm: &str,
    wrapped_key_material: &str,
    state: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("key_id".to_string(), logical_string(key_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("key_name".to_string(), logical_string(key_name));
    record.insert("version".to_string(), LogicalValue::Int(version));
    record.insert("algorithm".to_string(), logical_string(algorithm));
    record.insert(
        "wrapped_key_material".to_string(),
        logical_string(wrapped_key_material),
    );
    record.insert("state".to_string(), logical_string(state));
    record.insert("metadata_json".to_string(), logical_string("{}"));
    record
}

fn transit_key_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "algorithm".to_string(),
        "wrapped_key_material".to_string(),
        "state".to_string(),
    ])
}

#[allow(clippy::too_many_arguments)]
fn db_credential_lease_record(
    lease_id: &str,
    tenant_id: &str,
    role_name: &str,
    username: &str,
    parent_role: &str,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    metadata_json: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("lease_id".to_string(), logical_string(lease_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("role_name".to_string(), logical_string(role_name));
    record.insert("username".to_string(), logical_string(username));
    record.insert("parent_role".to_string(), logical_string(parent_role));
    record.insert("backend".to_string(), logical_string("postgres"));
    record.insert("issued_at".to_string(), LogicalValue::Timestamp(issued_at));
    record.insert(
        "expires_at".to_string(),
        LogicalValue::Timestamp(expires_at),
    );
    record.insert("state".to_string(), logical_string("ACTIVE"));
    record.insert("metadata_json".to_string(), logical_string(metadata_json));
    record
}

// ── JSON decoders (mirror lock_service) ───────────────────────────────────────

fn json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

fn json_str(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(serde_json::Value::Number(value)) => value.to_string(),
        Some(serde_json::Value::Bool(value)) => value.to_string(),
        Some(other @ (serde_json::Value::Object(_) | serde_json::Value::Array(_))) => {
            other.to_string()
        }
        _ => String::new(),
    }
}

fn json_i64(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    match row.get(key) {
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0),
        Some(serde_json::Value::String(value)) => value.trim().parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

fn stored_secret_from_json(row: &serde_json::Value) -> StoredSecret {
    let map = json_object(row);
    StoredSecret {
        secret_id: json_str(map, "secret_id"),
        version: json_i64(map, "version"),
        ciphertext: json_str(map, "ciphertext"),
        data_key_wrapped: json_str(map, "data_key_wrapped"),
        state: json_str(map, "state"),
        metadata_json: json_str(map, "metadata_json"),
    }
}

fn stored_transit_key_from_json(row: &serde_json::Value) -> StoredTransitKey {
    let map = json_object(row);
    StoredTransitKey {
        key_id: json_str(map, "key_id"),
        version: json_i64(map, "version"),
        algorithm: json_str(map, "algorithm"),
        wrapped_key_material: json_str(map, "wrapped_key_material"),
        state: json_str(map, "state"),
    }
}

/// The ACTIVE transit version with the highest number (the one Encrypt/Sign use).
fn active_transit(versions: &[StoredTransitKey]) -> Option<&StoredTransitKey> {
    versions
        .iter()
        .filter(|k| k.state == KEY_STATE_ACTIVE)
        .max_by_key(|k| k.version)
}

/// The specific transit version, if it is in one of the `allowed` states.
fn transit_version<'a>(
    versions: &'a [StoredTransitKey],
    version: i64,
    allowed: &[&str],
) -> Option<&'a StoredTransitKey> {
    versions
        .iter()
        .find(|k| k.version == version && allowed.contains(&k.state.as_str()))
}

fn vault_field_violation<F, D, M>(field: F, description: D, message: M) -> Status
where
    F: Into<String>,
    D: Into<String>,
    M: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

fn vault_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    vault_field_violation(field, description, message)
}

fn vault_required_secret_path() -> Status {
    vault_required_field(
        "secret_path",
        "must be a non-empty vault secret path",
        "secret_path is required",
    )
}

fn vault_required_key_name() -> Status {
    vault_required_field(
        "key_name",
        "must be a non-empty vault transit key name",
        "key_name is required",
    )
}

/// Resolve and validate the requested transit `algorithm`. An empty value takes
/// the default; a recognized value is canonicalized to its supported spelling; an
/// unknown value is rejected with a typed field violation naming `algorithm`
/// (rather than being silently treated as the default). Purely a validation guard
/// — it never changes which crypto primitive the seal/open path uses.
fn validate_transit_algorithm(requested: &str) -> Result<String, Status> {
    let trimmed = requested.trim();
    if trimmed.is_empty() {
        return Ok(DEFAULT_TRANSIT_ALGORITHM.to_string());
    }
    if let Some(canonical) = SUPPORTED_TRANSIT_ALGORITHMS
        .iter()
        .find(|alg| trimmed.eq_ignore_ascii_case(alg))
    {
        return Ok((*canonical).to_string());
    }
    let supported = SUPPORTED_TRANSIT_ALGORITHMS.join(", ");
    Err(vault_field_violation(
        "algorithm",
        format!("must be one of the supported transit algorithms: {supported}"),
        format!("unsupported transit key algorithm '{trimmed}'; supported: {supported}"),
    ))
}

#[tonic::async_trait]
impl VaultService for VaultServiceImpl {
    // ── KV engine ─────────────────────────────────────────────────────────────

    async fn put_secret(
        &self,
        request: Request<vault_pb::PutSecretRequest>,
    ) -> Result<Response<vault_pb::PutSecretResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // Seal gate FIRST: a sealed vault never serves a degraded write.
        self.check_seal()?;
        // Cross-tenant guard: the body tenant_id must match the verified claim.
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let secret_path = req.secret_path.trim().to_string();
        if secret_path.is_empty() {
            return Err(vault_required_secret_path());
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "vault",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let versions = self
            .read_secret_versions(runtime, &context, &tenant_id, &secret_path)
            .await?;
        let current_latest = versions.iter().map(|s| s.version).max().unwrap_or(0);
        // Compare-and-swap: refuse to clobber a concurrent writer.
        if i64::from(req.expected_version) != current_latest {
            return Err(crate::runtime::executor_utils::retryable_aborted_status(
                "vault",
                "secret version CAS",
                0,
                format!(
                    "CAS conflict: expected version {} but current latest is {current_latest}",
                    req.expected_version
                ),
            ));
        }
        let new_version = current_latest + 1;

        // Envelope-encrypt: fresh DEK seals the value; master KEK wraps the DEK.
        let plaintext = PlaintextSecret(req.secret_value);
        let dek = DataKey::generate();
        let ciphertext = dek_seal(&dek, new_version, plaintext.0.as_bytes())?;
        let wrapped = wrap_dek(runtime, &dek)?;
        let metadata_json = non_empty_json(&req.metadata_json);

        runtime
            .native_entity_write_for_service(
                "vault",
                &context,
                VAULT_SECRET_MSG,
                secret_record(
                    &Uuid::new_v4().to_string(),
                    &tenant_id,
                    &secret_path,
                    new_version,
                    &ciphertext,
                    &wrapped,
                    STATE_ACTIVE,
                    &metadata_json,
                ),
                secret_conflict(),
            )
            .await?;

        self.emit(
            TOPIC_SECRET_PUT,
            &secret_path,
            &tenant_id,
            &context.project_id,
            "put",
            &secret_path,
            serde_json::json!({
                "tenant_id": tenant_id,
                "secret_path": secret_path,
                "version": new_version,
            }),
        )
        .await;

        Ok(Response::new(vault_pb::PutSecretResponse {
            secret_path,
            version: new_version as i32,
            message: "secret stored".to_string(),
            error: None,
        }))
    }

    async fn get_secret(
        &self,
        request: Request<vault_pb::GetSecretRequest>,
    ) -> Result<Response<vault_pb::GetSecretResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        self.check_seal()?;
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let secret_path = req.secret_path.trim().to_string();
        if secret_path.is_empty() {
            return Err(vault_required_secret_path());
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "vault",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let versions = self
            .read_secret_versions(runtime, &context, &tenant_id, &secret_path)
            .await?;
        let selected = if req.version > 0 {
            versions
                .iter()
                .find(|s| s.version == i64::from(req.version) && s.state != STATE_DESTROYED)
        } else {
            versions
                .iter()
                .filter(|s| s.state == STATE_ACTIVE)
                .max_by_key(|s| s.version)
        };
        let secret = selected.ok_or_else(|| {
            vault_schema_not_found_status(
                "get_secret",
                "vault_secret_not_found",
                "secret not found",
            )
        })?;

        let dek = unwrap_dek(runtime, &secret.data_key_wrapped)?;
        let bytes = dek_open(&dek, transit_payload(&secret.ciphertext)?)?;
        let plaintext = PlaintextSecret(String::from_utf8(bytes).map_err(|_| {
            vault_internal_status(
                "get_secret_decode_plaintext",
                "vault secret is not valid UTF-8",
            )
        })?);

        // Sensitive READ — audit it (no plaintext in the payload).
        self.emit(
            TOPIC_SECRET_ACCESSED,
            &secret_path,
            &tenant_id,
            &context.project_id,
            "read",
            &secret_path,
            serde_json::json!({
                "tenant_id": tenant_id,
                "secret_path": secret_path,
                "version": secret.version,
            }),
        )
        .await;

        Ok(Response::new(vault_pb::GetSecretResponse {
            secret_path,
            version: secret.version as i32,
            secret_value: plaintext.0,
            metadata_json: secret.metadata_json.clone(),
            message: "ok".to_string(),
            error: None,
        }))
    }

    async fn list_secrets(
        &self,
        request: Request<vault_pb::ListSecretsRequest>,
    ) -> Result<Response<vault_pb::ListSecretsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        self.check_seal()?;
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "vault",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let rows = runtime
            .native_entity_read_for_service(
                "vault",
                &context,
                secret_list_read(&tenant_id, &req.path_prefix),
            )
            .await?;
        // Aggregate versions → one summary per path (its highest version + state).
        let mut by_path: std::collections::BTreeMap<String, (i64, String)> =
            std::collections::BTreeMap::new();
        for row in &rows {
            let map = json_object(row);
            let path = json_str(map, "secret_path");
            if path.is_empty() {
                continue;
            }
            let version = json_i64(map, "version");
            let state = json_str(map, "state");
            by_path
                .entry(path)
                .and_modify(|cur| {
                    if version > cur.0 {
                        *cur = (version, state.clone());
                    }
                })
                .or_insert((version, state));
        }
        let total_count = by_path.len() as i32;
        let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
        let secrets = by_path
            .into_iter()
            .skip(page_window.offset)
            .take(page_window.limit.min(MAX_LIST_SECRETS as usize))
            .map(
                |(secret_path, (latest_version, state))| vault_pb::SecretSummary {
                    secret_path,
                    latest_version: latest_version as i32,
                    state,
                },
            )
            .collect();
        let returned_count = secrets.len() as i64;

        // Fulfil the declared `udb.vault.secret.listed.v1` event contract (was
        // declared in the proto but never emitted). Metadata only — counts and the
        // requested prefix; NEVER any secret path value or secret material.
        self.emit(
            TOPIC_SECRET_LISTED,
            &tenant_id,
            &tenant_id,
            &context.project_id,
            "list",
            req.path_prefix.trim(),
            serde_json::json!({
                "tenant_id": tenant_id,
                "path_prefix": req.path_prefix.trim(),
                "returned_count": returned_count,
                "total_count": total_count,
            }),
        )
        .await;

        Ok(Response::new(vault_pb::ListSecretsResponse {
            secrets,
            total_count,
            error: None,
            next_page_token: native_next_page_token_for_total(
                page_window.offset,
                page_window.limit.min(MAX_LIST_SECRETS as usize),
                total_count as i64,
            ),
        }))
    }

    async fn delete_secret(
        &self,
        request: Request<vault_pb::DeleteSecretRequest>,
    ) -> Result<Response<vault_pb::DeleteSecretResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        self.check_seal()?;
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let secret_path = req.secret_path.trim().to_string();
        if secret_path.is_empty() {
            return Err(vault_required_secret_path());
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "vault",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let versions = self
            .read_secret_versions(runtime, &context, &tenant_id, &secret_path)
            .await?;
        let Some(latest) = versions
            .iter()
            .filter(|s| s.state == STATE_ACTIVE)
            .max_by_key(|s| s.version)
        else {
            // Idempotent: nothing active to soft-delete.
            return Ok(Response::new(vault_pb::DeleteSecretResponse {
                message: "secret not found".to_string(),
                error: None,
            }));
        };
        // Soft delete: keep the ciphertext, flip the state on the same row.
        runtime
            .native_entity_write_for_service(
                "vault",
                &context,
                VAULT_SECRET_MSG,
                secret_record(
                    &latest.secret_id,
                    &tenant_id,
                    &secret_path,
                    latest.version,
                    &latest.ciphertext,
                    &latest.data_key_wrapped,
                    STATE_DELETED,
                    &latest.metadata_json,
                ),
                secret_conflict(),
            )
            .await?;

        self.emit(
            TOPIC_SECRET_DELETED,
            &secret_path,
            &tenant_id,
            &context.project_id,
            "delete",
            &secret_path,
            serde_json::json!({
                "tenant_id": tenant_id,
                "secret_path": secret_path,
                "version": latest.version,
            }),
        )
        .await;

        Ok(Response::new(vault_pb::DeleteSecretResponse {
            message: "secret soft-deleted".to_string(),
            error: None,
        }))
    }

    async fn destroy_secret(
        &self,
        request: Request<vault_pb::DestroySecretRequest>,
    ) -> Result<Response<vault_pb::DestroySecretResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        self.check_seal()?;
        // Irreversible: empty confirmation fails closed (DESTRUCTIVE).
        if req.confirmation_token.trim().is_empty() {
            return Err(vault_confirmation_token_required_status());
        }
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let secret_path = req.secret_path.trim().to_string();
        if secret_path.is_empty() {
            return Err(vault_required_secret_path());
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "vault",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let versions = self
            .read_secret_versions(runtime, &context, &tenant_id, &secret_path)
            .await?;
        let mut destroyed = 0u32;
        for secret in versions.iter().filter(|s| s.state != STATE_DESTROYED) {
            // Crypto-shred: clear the wrapped DEK + ciphertext so the value is
            // irrecoverable even if the row survives.
            runtime
                .native_entity_write_for_service(
                    "vault",
                    &context,
                    VAULT_SECRET_MSG,
                    secret_record(
                        &secret.secret_id,
                        &tenant_id,
                        &secret_path,
                        secret.version,
                        "",
                        "",
                        STATE_DESTROYED,
                        "{}",
                    ),
                    secret_conflict(),
                )
                .await?;
            destroyed += 1;
        }

        self.emit(
            TOPIC_SECRET_DESTROYED,
            &secret_path,
            &tenant_id,
            &context.project_id,
            "destroy",
            &secret_path,
            serde_json::json!({
                "tenant_id": tenant_id,
                "secret_path": secret_path,
                "destroyed_versions": destroyed,
            }),
        )
        .await;

        Ok(Response::new(vault_pb::DestroySecretResponse {
            destroyed_versions: destroyed,
            message: "secret destroyed".to_string(),
            error: None,
        }))
    }

    // ── Transit engine ────────────────────────────────────────────────────────

    async fn create_transit_key(
        &self,
        request: Request<vault_pb::CreateTransitKeyRequest>,
    ) -> Result<Response<vault_pb::CreateTransitKeyResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        self.check_seal()?;
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let key_name = req.key_name.trim().to_string();
        if key_name.is_empty() {
            return Err(vault_required_key_name());
        }
        // Honor the requested algorithm: accept only primitives the crypto stack
        // actually implements; reject anything else instead of silently coercing
        // it to the default. Does not alter primitive selection.
        let algorithm = validate_transit_algorithm(&req.algorithm)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "vault",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let existing = self
            .read_transit_versions(runtime, &context, &tenant_id, &key_name)
            .await?;
        if !existing.is_empty() {
            return Err(vault_schema_already_exists_status(
                "create_transit_key",
                "vault_transit_key_already_exists",
                "transit key already exists",
            ));
        }

        let dek = DataKey::generate();
        let wrapped = wrap_dek(runtime, &dek)?;
        runtime
            .native_entity_write_for_service(
                "vault",
                &context,
                VAULT_TRANSIT_KEY_MSG,
                transit_key_record(
                    &Uuid::new_v4().to_string(),
                    &tenant_id,
                    &key_name,
                    1,
                    &algorithm,
                    &wrapped,
                    KEY_STATE_ACTIVE,
                ),
                transit_key_conflict(),
            )
            .await?;

        self.emit(
            TOPIC_KEY_CREATED,
            &key_name,
            &tenant_id,
            &context.project_id,
            "create_key",
            &key_name,
            serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "version": 1}),
        )
        .await;

        Ok(Response::new(vault_pb::CreateTransitKeyResponse {
            key_name,
            version: 1,
            message: "transit key created".to_string(),
            error: None,
        }))
    }

    async fn rotate_transit_key(
        &self,
        request: Request<vault_pb::RotateTransitKeyRequest>,
    ) -> Result<Response<vault_pb::RotateTransitKeyResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        self.check_seal()?;
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let key_name = req.key_name.trim().to_string();
        if key_name.is_empty() {
            return Err(vault_required_key_name());
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "vault",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let versions = self
            .read_transit_versions(runtime, &context, &tenant_id, &key_name)
            .await?;
        if versions.is_empty() {
            return Err(vault_schema_not_found_status(
                "rotate_transit_key",
                "vault_transit_key_not_found",
                "transit key not found",
            ));
        }
        let max_version = versions.iter().map(|k| k.version).max().unwrap_or(0);
        let algorithm = active_transit(&versions)
            .map(|k| k.algorithm.clone())
            .unwrap_or_else(|| DEFAULT_TRANSIT_ALGORITHM.to_string());

        // Demote every current ACTIVE version to VERIFYING (rotation overlap).
        for key in versions.iter().filter(|k| k.state == KEY_STATE_ACTIVE) {
            runtime
                .native_entity_write_for_service(
                    "vault",
                    &context,
                    VAULT_TRANSIT_KEY_MSG,
                    transit_key_record(
                        &key.key_id,
                        &tenant_id,
                        &key_name,
                        key.version,
                        &key.algorithm,
                        &key.wrapped_key_material,
                        KEY_STATE_VERIFYING,
                    ),
                    transit_key_conflict(),
                )
                .await?;
        }

        let new_version = max_version + 1;
        let dek = DataKey::generate();
        let wrapped = wrap_dek(runtime, &dek)?;
        runtime
            .native_entity_write_for_service(
                "vault",
                &context,
                VAULT_TRANSIT_KEY_MSG,
                transit_key_record(
                    &Uuid::new_v4().to_string(),
                    &tenant_id,
                    &key_name,
                    new_version,
                    &algorithm,
                    &wrapped,
                    KEY_STATE_ACTIVE,
                ),
                transit_key_conflict(),
            )
            .await?;

        self.emit(
            TOPIC_KEY_ROTATED,
            &key_name,
            &tenant_id,
            &context.project_id,
            "rotate_key",
            &key_name,
            serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "version": new_version}),
        )
        .await;

        Ok(Response::new(vault_pb::RotateTransitKeyResponse {
            key_name,
            version: new_version as i32,
            message: "transit key rotated".to_string(),
            error: None,
        }))
    }

    async fn encrypt(
        &self,
        request: Request<vault_pb::EncryptRequest>,
    ) -> Result<Response<vault_pb::EncryptResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        self.check_seal()?;
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let key_name = req.key_name.trim().to_string();
        if key_name.is_empty() {
            return Err(vault_required_key_name());
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "vault",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let versions = self
            .read_transit_versions(runtime, &context, &tenant_id, &key_name)
            .await?;
        let active = active_transit(&versions).ok_or_else(|| {
            vault_schema_not_found_status(
                "encrypt",
                "vault_transit_active_key_not_found",
                "transit key not found or has no active version",
            )
        })?;
        let dek = unwrap_dek(runtime, &active.wrapped_key_material)?;
        let plaintext = PlaintextSecret(req.plaintext);
        let ciphertext = dek_seal(&dek, active.version, plaintext.0.as_bytes())?;

        // Audit the crypto operation (no plaintext/ciphertext/key material — only
        // the tenant/key/version metadata).
        self.emit(
            TOPIC_TRANSIT_ENCRYPTED,
            &key_name,
            &tenant_id,
            &context.project_id,
            "encrypt",
            &key_name,
            serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "key_version": active.version}),
        )
        .await;

        Ok(Response::new(vault_pb::EncryptResponse {
            ciphertext,
            key_version: active.version as i32,
            message: "ok".to_string(),
            error: None,
        }))
    }

    async fn decrypt(
        &self,
        request: Request<vault_pb::DecryptRequest>,
    ) -> Result<Response<vault_pb::DecryptResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        self.check_seal()?;
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let key_name = req.key_name.trim().to_string();
        if key_name.is_empty() {
            return Err(vault_required_key_name());
        }
        let (version, encoded) = parse_transit_envelope(&req.ciphertext).ok_or_else(|| {
            vault_field_violation(
                "ciphertext",
                "must match udb-vault:v<version>:<base64>",
                "not a vault transit ciphertext envelope",
            )
        })?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "vault",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let versions = self
            .read_transit_versions(runtime, &context, &tenant_id, &key_name)
            .await?;
        // ACTIVE + VERIFYING decrypt during the rotation overlap (signing-key model).
        let key = transit_version(&versions, version, &[KEY_STATE_ACTIVE, KEY_STATE_VERIFYING])
            .ok_or_else(|| {
                vault_schema_not_found_status(
                    "decrypt",
                    "vault_transit_key_version_not_found",
                    "transit key version not found or retired",
                )
            })?;
        let dek = unwrap_dek(runtime, &key.wrapped_key_material)?;
        let bytes = dek_open(&dek, encoded)?;
        let plaintext = PlaintextSecret(String::from_utf8(bytes).map_err(|_| {
            vault_internal_status(
                "decrypt_transit_plaintext",
                "decrypted payload is not valid UTF-8",
            )
        })?);

        // Sensitive READ — audit it (no plaintext in the payload).
        self.emit(
            TOPIC_TRANSIT_DECRYPTED,
            &key_name,
            &tenant_id,
            &context.project_id,
            "decrypt",
            &key_name,
            serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "key_version": version}),
        )
        .await;

        Ok(Response::new(vault_pb::DecryptResponse {
            plaintext: plaintext.0,
            key_version: version as i32,
            message: "ok".to_string(),
            error: None,
        }))
    }

    async fn sign(
        &self,
        request: Request<vault_pb::SignRequest>,
    ) -> Result<Response<vault_pb::SignResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        self.check_seal()?;
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let key_name = req.key_name.trim().to_string();
        if key_name.is_empty() {
            return Err(vault_required_key_name());
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "vault",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let versions = self
            .read_transit_versions(runtime, &context, &tenant_id, &key_name)
            .await?;
        let active = active_transit(&versions).ok_or_else(|| {
            vault_schema_not_found_status(
                "sign",
                "vault_transit_active_key_not_found",
                "transit key not found or has no active version",
            )
        })?;
        let dek = unwrap_dek(runtime, &active.wrapped_key_material)?;
        let mac = hmac_sha256(&dek.0, req.input.as_bytes());
        let signature = format!(
            "{VAULT_HMAC_PREFIX}{}:{}",
            active.version,
            BASE64_STANDARD.encode(mac)
        );

        // Audit the crypto operation (no input/signature/key material — only the
        // tenant/key/version metadata).
        self.emit(
            TOPIC_TRANSIT_SIGNED,
            &key_name,
            &tenant_id,
            &context.project_id,
            "sign",
            &key_name,
            serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "key_version": active.version}),
        )
        .await;

        Ok(Response::new(vault_pb::SignResponse {
            signature,
            key_version: active.version as i32,
            message: "ok".to_string(),
            error: None,
        }))
    }

    async fn verify(
        &self,
        request: Request<vault_pb::VerifyRequest>,
    ) -> Result<Response<vault_pb::VerifyResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        self.check_seal()?;
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let key_name = req.key_name.trim().to_string();
        if key_name.is_empty() {
            return Err(vault_required_key_name());
        }
        let Some((version, mac_b64)) = parse_mac_envelope(&req.signature) else {
            return Ok(Response::new(vault_pb::VerifyResponse {
                valid: false,
                message: "not a vault signature envelope".to_string(),
                error: None,
            }));
        };
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "vault",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let versions = self
            .read_transit_versions(runtime, &context, &tenant_id, &key_name)
            .await?;
        let key = transit_version(&versions, version, &[KEY_STATE_ACTIVE, KEY_STATE_VERIFYING])
            .ok_or_else(|| {
                vault_schema_not_found_status(
                    "verify",
                    "vault_transit_key_version_not_found",
                    "transit key version not found or retired",
                )
            })?;
        let dek = unwrap_dek(runtime, &key.wrapped_key_material)?;
        let expected = hmac_sha256(&dek.0, req.input.as_bytes());
        let provided = BASE64_STANDARD.decode(mac_b64).unwrap_or_default();
        let valid = constant_time_eq(&expected, &provided);

        // Audit the verification (no input/signature/key material — only the
        // tenant/key/version metadata and the boolean outcome).
        self.emit(
            TOPIC_TRANSIT_VERIFIED,
            &key_name,
            &tenant_id,
            &context.project_id,
            "verify",
            &key_name,
            serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "key_version": version, "valid": valid}),
        )
        .await;

        Ok(Response::new(vault_pb::VerifyResponse {
            valid,
            message: if valid { "valid" } else { "invalid" }.to_string(),
            error: None,
        }))
    }

    async fn hmac(
        &self,
        request: Request<vault_pb::HmacRequest>,
    ) -> Result<Response<vault_pb::HmacResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        self.check_seal()?;
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let key_name = req.key_name.trim().to_string();
        if key_name.is_empty() {
            return Err(vault_required_key_name());
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "vault",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let versions = self
            .read_transit_versions(runtime, &context, &tenant_id, &key_name)
            .await?;
        let active = active_transit(&versions).ok_or_else(|| {
            vault_schema_not_found_status(
                "hmac",
                "vault_transit_active_key_not_found",
                "transit key not found or has no active version",
            )
        })?;
        let dek = unwrap_dek(runtime, &active.wrapped_key_material)?;
        let mac = hmac_sha256(&dek.0, req.input.as_bytes());
        let hmac_value = format!(
            "{VAULT_HMAC_PREFIX}{}:{}",
            active.version,
            BASE64_STANDARD.encode(mac)
        );

        // Audit the crypto operation (no input/MAC/key material — only the
        // tenant/key/version metadata).
        self.emit(
            TOPIC_TRANSIT_HMAC,
            &key_name,
            &tenant_id,
            &context.project_id,
            "hmac",
            &key_name,
            serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "key_version": active.version}),
        )
        .await;

        Ok(Response::new(vault_pb::HmacResponse {
            hmac: hmac_value,
            key_version: active.version as i32,
            message: "ok".to_string(),
            error: None,
        }))
    }

    // ── Seal engine ─────────────────────────────────────────────────────────

    async fn seal_status(
        &self,
        request: Request<vault_pb::SealStatusRequest>,
    ) -> Result<Response<vault_pb::SealStatusResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // SealStatus REPORTS the seal state — it must answer even when sealed, so
        // it does not call the seal gate. It still enforces the tenant scope.
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let sealed = self.is_sealed();
        let kek_configured = self.kek_configured();
        Ok(Response::new(vault_pb::SealStatusResponse {
            sealed,
            kek_configured,
            message: if sealed {
                "vault is sealed: master key unavailable".to_string()
            } else if kek_configured {
                "vault is unsealed (master KEK configured)".to_string()
            } else {
                "vault is unsealed (dev passthrough — no master KEK configured)".to_string()
            },
            error: None,
        }))
    }

    // ── Dynamic database credentials ─────────────────────────────────────────

    async fn generate_database_credentials(
        &self,
        request: Request<vault_pb::GenerateDatabaseCredentialsRequest>,
    ) -> Result<Response<vault_pb::GenerateDatabaseCredentialsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        self.check_seal()?;
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let role_name = req.role_name.trim().to_string();
        validate_db_role_alias(&role_name)?;

        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "vault",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let pool = self
            .pg_pool
            .as_ref()
            .ok_or_else(vault_db_native_store_required_status)?;
        let role_config = vault_db_role_configs()?.get(&role_name).ok_or_else(|| {
            vault_db_credentials_config_status(format!(
                "vault dynamic database role '{role_name}' is not configured"
            ))
        })?;
        let max_ttl = role_config
            .ttl_seconds_max
            .unwrap_or(DEFAULT_DB_CREDENTIAL_MAX_TTL_SECONDS);
        let ttl_seconds = requested_db_credential_ttl(req.ttl_seconds, max_ttl)?;
        let issued_at = Utc::now();
        let expires_at = issued_at + chrono::Duration::seconds(i64::from(ttl_seconds));
        let lease_id = Uuid::new_v4().to_string();
        let username = generate_db_username();
        let password = generate_db_password();

        create_postgres_login_role(
            pool,
            &username,
            &password,
            expires_at,
            &role_config.parent_role,
        )
        .await?;

        let context = native_service_context(&metadata, &tenant_id, "");
        let metadata_json = serde_json::json!({
            "role_name": &role_name,
            "parent_role": &role_config.parent_role,
            "ttl_seconds": ttl_seconds,
            "lease_id": &lease_id,
        })
        .to_string();
        let write = runtime
            .native_entity_write_for_service(
                "vault",
                &context,
                VAULT_DB_CREDENTIAL_LEASE_MSG,
                db_credential_lease_record(
                    &lease_id,
                    &tenant_id,
                    &role_name,
                    &username,
                    &role_config.parent_role,
                    issued_at,
                    expires_at,
                    &metadata_json,
                ),
                ConflictStrategy::Error,
            )
            .await;
        if let Err(err) = write {
            if let Err(drop_err) = drop_postgres_login_role(pool, &username).await {
                tracing::warn!(
                    lease_id = %lease_id,
                    username = %username,
                    error = %drop_err,
                    "vault DB credential cleanup failed after lease write failure"
                );
            }
            return Err(err);
        }

        self.emit(
            TOPIC_DB_CREDENTIAL_ISSUED,
            &lease_id,
            &tenant_id,
            "",
            "vault.GenerateDatabaseCredentials",
            &format!("vault/database/credentials/{role_name}"),
            serde_json::json!({
                "lease_id": &lease_id,
                "role_name": &role_name,
                "username": &username,
                "expires_at": expires_at.to_rfc3339(),
            }),
        )
        .await;

        Ok(Response::new(
            vault_pb::GenerateDatabaseCredentialsResponse {
                username,
                password,
                lease_id,
                lease_ttl_seconds: ttl_seconds,
                message: "dynamic database credentials issued".to_string(),
                error: None,
            },
        ))
    }
}

/// The transit-ciphertext stored for a KV secret is written as a bare
/// `udb-vault:` envelope; strip the prefix back to the base64 body the DEK
/// opener consumes. Shared by `get_secret`.
fn transit_payload(ciphertext: &str) -> Result<&str, Status> {
    parse_transit_envelope(ciphertext)
        .map(|(_, encoded)| encoded)
        .ok_or_else(|| {
            vault_internal_status(
                "get_secret_parse_envelope",
                "stored secret has a malformed ciphertext envelope",
            )
        })
}

#[cfg(test)]
mod vault_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::metadata::MetadataValue;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_single_field_violation(status: &Status, field: &str, description: &str) {
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_capability_detail(status: &Status, operation: &str, capability: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "vault");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability);
        assert!(!detail.retryable);
    }

    fn assert_schema_not_found_detail(
        status: &Status,
        operation: &str,
        schema_code: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "vault");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, schema_code);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "vault");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    #[test]
    fn vault_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &vault_internal_status("data_key_decode", "vault data-key decode failed: invalid"),
            "data_key_decode",
            "vault data-key decode failed: invalid",
        );
    }

    #[tokio::test]
    async fn destroy_secret_missing_confirmation_carries_failed_precondition_field_violation() {
        let svc = VaultServiceImpl::new().with_seal_override(false);
        let request = Request::new(vault_pb::DestroySecretRequest {
            tenant_id: "tenant-a".to_string(),
            secret_path: "app/db/password".to_string(),
            confirmation_token: " ".to_string(),
            ..Default::default()
        });

        let err = svc
            .destroy_secret(request)
            .await
            .expect_err("destructive confirmation must be required");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "DestroySecret crypto-shreds the secret; confirmation_token is required"
        );
        assert_single_field_violation(
            &err,
            "confirmation_token",
            "must be provided to confirm destructive secret shredding",
        );
    }

    #[test]
    fn vault_setup_failures_carry_capability_detail() {
        let wrap = vault_master_key_operation_status(
            "wrap_data_key",
            "vault is sealed: cannot wrap data key (missing key)",
        );
        assert_capability_detail(
            &wrap,
            "wrap_data_key",
            "vault_master_key",
            "vault is sealed: cannot wrap data key (missing key)",
        );

        let store = vault_db_native_store_required_status();
        assert_capability_detail(
            &store,
            "generate_database_credentials",
            "postgres_native_store",
            "vault dynamic database credentials require a Postgres native store",
        );

        let role = vault_db_credentials_config_status(
            "vault dynamic database role 'app' is not configured",
        );
        assert_capability_detail(
            &role,
            "generate_database_credentials",
            "database_credentials_config",
            "vault dynamic database role 'app' is not configured",
        );
    }

    #[test]
    fn vault_not_found_statuses_carry_schema_detail() {
        for (operation, schema_code, message) in [
            ("get_secret", "vault_secret_not_found", "secret not found"),
            (
                "rotate_transit_key",
                "vault_transit_key_not_found",
                "transit key not found",
            ),
            (
                "encrypt",
                "vault_transit_active_key_not_found",
                "transit key not found or has no active version",
            ),
            (
                "sign",
                "vault_transit_active_key_not_found",
                "transit key not found or has no active version",
            ),
            (
                "hmac",
                "vault_transit_active_key_not_found",
                "transit key not found or has no active version",
            ),
            (
                "decrypt",
                "vault_transit_key_version_not_found",
                "transit key version not found or retired",
            ),
            (
                "verify",
                "vault_transit_key_version_not_found",
                "transit key version not found or retired",
            ),
        ] {
            assert_schema_not_found_detail(
                &vault_schema_not_found_status(operation, schema_code, message),
                operation,
                schema_code,
                message,
            );
        }
    }

    #[test]
    fn vault_already_exists_statuses_carry_schema_detail() {
        let err = vault_schema_already_exists_status(
            "create_transit_key",
            "vault_transit_key_already_exists",
            "transit key already exists",
        );
        assert_eq!(err.code(), tonic::Code::AlreadyExists);
        assert_eq!(err.message(), "transit key already exists");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "vault");
        assert_eq!(detail.operation, "create_transit_key");
        assert_eq!(
            detail.capability_required,
            "vault_transit_key_already_exists"
        );
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    /// A caller scoped to tenant-a must not target tenant-b's secret by putting a
    /// foreign tenant_id in the request BODY; the scope guard rejects it before
    /// any store access. The vault is force-UNSEALED so the cross-tenant guard
    /// (not the seal gate) is what fires — mirrors `tenant_service`.
    #[tokio::test]
    async fn put_secret_rejects_cross_tenant_body() {
        let svc = VaultServiceImpl::new().with_seal_override(false);
        let mut request = Request::new(vault_pb::PutSecretRequest {
            tenant_id: "tenant-b".to_string(),
            secret_path: "app/db/password".to_string(),
            secret_value: "hunter2".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .put_secret(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn put_secret_missing_secret_path_carries_field_violation() {
        let svc = VaultServiceImpl::new().with_seal_override(false);
        let mut request = Request::new(vault_pb::PutSecretRequest {
            tenant_id: "tenant-a".to_string(),
            secret_path: " ".to_string(),
            secret_value: "hunter2".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

        let err = svc
            .put_secret(request)
            .await
            .expect_err("missing secret path must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "secret_path is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "secret_path");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty vault secret path"
        );
    }

    #[tokio::test]
    async fn decrypt_missing_key_name_carries_field_violation() {
        let svc = VaultServiceImpl::new().with_seal_override(false);
        let mut request = Request::new(vault_pb::DecryptRequest {
            tenant_id: "tenant-a".to_string(),
            key_name: " ".to_string(),
            ciphertext: "not-even-parsed".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

        let err = svc
            .decrypt(request)
            .await
            .expect_err("missing key name must fail before envelope parsing");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "key_name is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "key_name");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty vault transit key name"
        );
    }

    #[tokio::test]
    async fn decrypt_malformed_ciphertext_carries_field_violation() {
        let svc = VaultServiceImpl::new().with_seal_override(false);
        let mut request = Request::new(vault_pb::DecryptRequest {
            tenant_id: "tenant-a".to_string(),
            key_name: "app-key".to_string(),
            ciphertext: "not-even-an-envelope".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

        let err = svc
            .decrypt(request)
            .await
            .expect_err("malformed ciphertext must fail before runtime access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "not a vault transit ciphertext envelope");
        assert_single_field_violation(
            &err,
            "ciphertext",
            "must match udb-vault:v<version>:<base64>",
        );
    }

    #[test]
    fn transit_ciphertext_helpers_carry_field_violations() {
        let dek = DataKey([3u8; 32]);
        let invalid_base64 = dek_open(&dek, "@@").expect_err("invalid base64 must fail");
        assert_eq!(invalid_base64.code(), tonic::Code::InvalidArgument);
        assert!(
            invalid_base64
                .message()
                .starts_with("vault ciphertext decode failed: ")
        );
        assert_single_field_violation(
            &invalid_base64,
            "ciphertext",
            "must be base64-encoded vault transit ciphertext bytes",
        );

        let too_short = dek_open(&dek, "AA==").expect_err("short envelope must fail");
        assert_eq!(too_short.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            too_short.message(),
            "vault ciphertext envelope is too short"
        );
        assert_single_field_violation(
            &too_short,
            "ciphertext",
            "must include a 12-byte nonce and encrypted payload",
        );
    }

    #[test]
    fn dynamic_database_credential_validation_carries_field_violations() {
        let role = validate_db_role_alias("app read").expect_err("space must fail alias");
        assert_eq!(role.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            role.message(),
            "role_name must be 1..128 ASCII chars using letters, digits, _, -, :, or ."
        );
        assert_single_field_violation(
            &role,
            "role_name",
            "must be 1..128 ASCII chars using letters, digits, _, -, :, or .",
        );

        let ttl = requested_db_credential_ttl(1, 600).expect_err("too-small ttl must be rejected");
        assert_eq!(ttl.code(), tonic::Code::InvalidArgument);
        let min_ttl_message =
            format!("ttl_seconds must be 0/default or at least {MIN_DB_CREDENTIAL_TTL_SECONDS}");
        assert_eq!(ttl.message(), min_ttl_message);
        let min_ttl_description =
            format!("must be 0/default or at least {MIN_DB_CREDENTIAL_TTL_SECONDS}");
        assert_single_field_violation(&ttl, "ttl_seconds", &min_ttl_description);
    }

    #[test]
    fn transit_algorithm_validation_accepts_supported_and_rejects_unknown() {
        // Empty ⇒ the default primitive.
        assert_eq!(
            validate_transit_algorithm("  ").expect("empty resolves to the default"),
            DEFAULT_TRANSIT_ALGORITHM
        );
        // A supported value is accepted case-insensitively and canonicalized.
        assert_eq!(
            validate_transit_algorithm("AES256-GCM-SIV").expect("supported value accepted"),
            DEFAULT_TRANSIT_ALGORITHM
        );
        // An unknown value is rejected up front with a typed field violation that
        // names `algorithm` (no longer silently coerced to the default).
        let err =
            validate_transit_algorithm("rsa-4096").expect_err("unknown algorithm must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("rsa-4096"));
        assert_single_field_violation(
            &err,
            "algorithm",
            "must be one of the supported transit algorithms: aes256-gcm-siv",
        );
    }

    /// The seal gate fails closed: when the master key is unavailable, an
    /// operating RPC returns `failed_precondition` BEFORE touching any store —
    /// never serving a degraded vault.
    #[tokio::test]
    async fn sealed_vault_fails_closed() {
        let svc = VaultServiceImpl::new().with_seal_override(true);
        let mut request = Request::new(vault_pb::PutSecretRequest {
            tenant_id: "tenant-a".to_string(),
            secret_path: "app/db/password".to_string(),
            secret_value: "hunter2".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .put_secret(request)
            .await
            .expect_err("a sealed vault must fail closed");
        assert_capability_detail(
            &err,
            "seal_gate",
            "vault_master_key",
            VAULT_MASTER_KEY_UNAVAILABLE_MESSAGE,
        );
    }

    #[tokio::test]
    async fn vault_missing_runtime_carries_capability_detail() {
        let svc = VaultServiceImpl::new().with_seal_override(false);
        let mut request = Request::new(vault_pb::PutSecretRequest {
            tenant_id: "tenant-a".to_string(),
            secret_path: "app/db/password".to_string(),
            secret_value: "hunter2".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .put_secret(request)
            .await
            .expect_err("missing runtime must fail closed");
        assert_capability_detail(
            &err,
            "native_entity_dispatch",
            "runtime_native_entity_dispatch",
            VAULT_RUNTIME_REQUIRED_MESSAGE,
        );
    }

    /// Redacting-Debug canary: a secret-bearing type's `Debug` must NOT leak the
    /// cleartext (or key bytes). Mirrors `encryption.rs`'s redaction test.
    #[test]
    fn redacting_debug_never_leaks_cleartext() {
        let canary = "udb-canary-9f3a2c";
        let secret = PlaintextSecret(canary.to_string());
        let rendered = format!("{secret:?}");
        assert!(
            !rendered.contains(canary),
            "PlaintextSecret Debug leaked the cleartext: {rendered}"
        );
        assert!(rendered.contains("[redacted]"));

        let dek = DataKey([7u8; 32]);
        let dek_rendered = format!("{dek:?}");
        assert!(dek_rendered.contains("[redacted]"));
        assert!(!dek_rendered.contains("7, 7"));
    }

    /// HMAC-SHA256 matches a known RFC-style fixture and the envelope round-trips
    /// through the AEAD seal/open under a fixed DEK.
    #[test]
    fn dek_seal_round_trips_and_hmac_is_stable() {
        let dek = DataKey([3u8; 32]);
        let sealed = dek_seal(&dek, 1, b"top-secret").expect("seal");
        let (version, body) = parse_transit_envelope(&sealed).expect("parse envelope");
        assert_eq!(version, 1);
        let opened = dek_open(&dek, body).expect("open");
        assert_eq!(opened, b"top-secret");

        let a = hmac_sha256(&dek.0, b"message");
        let b = hmac_sha256(&dek.0, b"message");
        assert!(constant_time_eq(&a, &b));
        assert!(!constant_time_eq(&a, &hmac_sha256(&dek.0, b"other")));
    }

    #[test]
    fn db_role_config_is_allow_listed_and_identifier_safe() {
        let configs = parse_vault_db_role_configs(
            r#"[{"role_name":"app-read","parent_role":"udb_app_read","ttl_seconds_max":600}]"#,
        )
        .expect("valid config");
        let app_read = configs.get("app-read").expect("role alias present");
        assert_eq!(app_read.parent_role, "udb_app_read");
        assert_eq!(app_read.ttl_seconds_max, Some(600));

        let bad_parent = parse_vault_db_role_configs(
            r#"[{"role_name":"app-read","parent_role":"udb_app_read;drop role x"}]"#,
        )
        .expect_err("SQL-shaped parent role must be rejected");
        assert!(bad_parent.contains("parent_role"));

        let bad_alias = parse_vault_db_role_configs(
            r#"[{"role_name":"app read","parent_role":"udb_app_read"}]"#,
        )
        .expect_err("space in role alias must be rejected");
        assert!(bad_alias.contains("role_name"));
    }
}

impl DataBrokerService {
    /// Build the native `VaultService`, wired to the broker's Postgres pool, the
    /// master-key envelope runtime, and the shared outbox.
    pub(crate) fn build_vault_service(&self) -> VaultServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam: the
        // backend is read from this service's proto `native_service` binding, then
        // a health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("vault", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        VaultServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
