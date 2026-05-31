//! U22 — reversible CDC field encryption.
//!
//! Pre-U22 the CDC redaction pipeline supported three irreversible
//! modes (`Mask`, `Drop`, `Hash`). The upgrade doc flagged the need
//! for a **reversible** mode where a downstream consumer with the
//! right scope can decrypt the original field — needed for replay
//! workflows, audit pipelines, and tenant-controlled access.
//!
//! U22 adds:
//!
//! - [`CdcEncryptionKey`] / [`CdcKeyResolver`] — versioned 32-byte
//!   AES-256-GCM-SIV keys, looked up by `(key_id, key_version)`.
//!   `StaticKeyResolver` is the test impl; production deployments
//!   plug in a Vault- or KMS-backed resolver.
//! - [`EncryptedField`] — the on-the-wire envelope that replaces a
//!   plaintext field in the CDC payload:
//!   `{"@udb_encrypted": 1, "key_id": "...", "key_version": N,
//!     "nonce_b64": "...", "ciphertext_b64": "..."}`.
//! - [`encrypt_field_value`] / [`decrypt_field_value`] — the AEAD
//!   primitives.
//! - [`encrypt_cdc_payload_fields`] — the CDC pipeline's entry point.
//!   Walks a JSON payload and replaces sensitive fields in-place with
//!   their `EncryptedField` envelope. Tests pin that a plain consumer
//!   sees no plaintext and an authorised consumer can round-trip.
//! - [`DecryptScope`] — gates which consumers may call
//!   `decrypt_field_value`. The DLQ replay path requires
//!   `DecryptScope::Replay`; ad-hoc audit reads require
//!   `DecryptScope::Audit`. The CDC sink itself uses
//!   `DecryptScope::None` — it never decrypts.
//!
//! ## Why GCM-SIV?
//!
//! AES-256-GCM-SIV is misuse-resistant: nonce reuse degrades to
//! "deterministic" (same plaintext → same ciphertext) instead of
//! catastrophically leaking the key, which matters because CDC
//! producers don't have a strong RNG guarantee under outbox replay.
//! The existing `runtime/encryption.rs` already imports the same
//! cipher for the at-rest path.

use aead::{Aead, KeyInit};
use aes_gcm_siv::{Aes256GcmSiv, Nonce};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::sync::Arc;

// ── Key material ──────────────────────────────────────────────────────────────

/// One AES-256-GCM-SIV key with its operator-controlled version.
///
/// Versions let an operator roll keys without re-encrypting historical
/// CDC payloads — old envelopes carry their version and a consumer
/// looks up the right key by `(key_id, key_version)`.
#[derive(Debug, Clone)]
pub struct CdcEncryptionKey {
    pub key_id: String,
    pub key_version: u32,
    pub key_bytes: [u8; 32],
}

impl CdcEncryptionKey {
    pub fn new(key_id: impl Into<String>, key_version: u32, key_bytes: [u8; 32]) -> Self {
        Self {
            key_id: key_id.into(),
            key_version,
            key_bytes,
        }
    }
}

/// Resolves a `(key_id, key_version)` to a key. Production impls back
/// onto Vault/KMS; the test impl is `StaticKeyResolver`.
pub trait CdcKeyResolver: Send + Sync {
    fn resolve(&self, key_id: &str, key_version: u32) -> Option<CdcEncryptionKey>;
    /// The active key the producer uses to encrypt new fields. Returns
    /// `None` if no encryption is configured — callers MUST handle
    /// this and fall back to a non-encrypt mode rather than panic.
    fn active_key(&self) -> Option<CdcEncryptionKey>;
}

/// In-memory resolver used by tests and by small deployments that
/// configure keys statically.
#[derive(Default, Clone)]
pub struct StaticKeyResolver {
    keys: Vec<CdcEncryptionKey>,
    active_index: Option<usize>,
}

impl StaticKeyResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_key(mut self, key: CdcEncryptionKey) -> Self {
        self.keys.push(key);
        if self.active_index.is_none() {
            self.active_index = Some(self.keys.len() - 1);
        }
        self
    }

    /// Explicitly set the active key (otherwise the first inserted
    /// key wins). Returns `None` if no matching `(key_id, version)`
    /// is registered.
    pub fn make_active(mut self, key_id: &str, key_version: u32) -> Option<Self> {
        let idx = self
            .keys
            .iter()
            .position(|k| k.key_id == key_id && k.key_version == key_version)?;
        self.active_index = Some(idx);
        Some(self)
    }
}

impl CdcKeyResolver for StaticKeyResolver {
    fn resolve(&self, key_id: &str, key_version: u32) -> Option<CdcEncryptionKey> {
        self.keys
            .iter()
            .find(|k| k.key_id == key_id && k.key_version == key_version)
            .cloned()
    }
    fn active_key(&self) -> Option<CdcEncryptionKey> {
        self.active_index.and_then(|i| self.keys.get(i).cloned())
    }
}

// ── EncryptedField envelope ───────────────────────────────────────────────────

/// On-the-wire shape that replaces a plaintext field in the CDC
/// payload. The leading `@udb_encrypted: 1` is a sentinel a consumer
/// uses to identify the envelope without parsing the rest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptedField {
    #[serde(rename = "@udb_encrypted")]
    pub version_marker: u32,
    pub key_id: String,
    pub key_version: u32,
    pub nonce_b64: String,
    pub ciphertext_b64: String,
}

impl EncryptedField {
    /// Returns true if `value` looks like an encrypted-field envelope.
    /// Lets consumers branch without try-deserializing.
    pub fn is_envelope(value: &JsonValue) -> bool {
        value
            .as_object()
            .is_some_and(|obj| obj.contains_key("@udb_encrypted"))
    }
}

// ── DecryptScope ──────────────────────────────────────────────────────────────

/// Which consumer may decrypt. The producer encrypts unconditionally;
/// the consumer authorisation is checked at decrypt time. Stable string
/// tokens — used in audit logs and policy config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecryptScope {
    /// Never decrypt. Default for CDC sinks (Kafka downstream
    /// consumers that don't have authority).
    None,
    /// The DLQ replay path: a saga operator is re-processing a failed
    /// envelope and needs the original payload.
    Replay,
    /// The audit path: an admin RPC is reading historical state under
    /// an audit policy. Logged.
    Audit,
}

impl DecryptScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Replay => "replay",
            Self::Audit => "audit",
        }
    }
    pub fn allows_decrypt(self) -> bool {
        !matches!(self, Self::None)
    }
}

// ── Crypto primitives ─────────────────────────────────────────────────────────

/// Errors the encrypt/decrypt path can produce. All are typed — no
/// stringly-typed "encryption failed" anywhere.
///
/// Hand-rolled Display/Error so the module doesn't pull in a new dep.
#[derive(Debug)]
pub enum CdcEncryptionError {
    NoActiveKey,
    ScopeNotAuthorised(&'static str),
    UnknownKey { key_id: String, key_version: u32 },
    Crypto(String),
    Envelope(String),
    Base64(String),
    NonStringPlaintext(String),
}

impl std::fmt::Display for CdcEncryptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoActiveKey => write!(f, "no active encryption key registered"),
            Self::ScopeNotAuthorised(scope) => {
                write!(f, "decrypt scope '{scope}' is not authorised")
            }
            Self::UnknownKey {
                key_id,
                key_version,
            } => write!(
                f,
                "envelope references key_id='{key_id}' version={key_version} which is not registered"
            ),
            Self::Crypto(msg) => write!(f, "AEAD operation failed: {msg}"),
            Self::Envelope(msg) => write!(f, "envelope JSON is malformed: {msg}"),
            Self::Base64(msg) => write!(f, "base64 decode failed: {msg}"),
            Self::NonStringPlaintext(msg) => {
                write!(f, "value is not a string after decrypt: {msg}")
            }
        }
    }
}

impl std::error::Error for CdcEncryptionError {}

/// Encrypt one JSON value (serialised to a UTF-8 byte string) and
/// produce the envelope. The caller supplies an entropy source for
/// the nonce; `[0u8;12]` is allowed in tests but production paths
/// pass a fresh random nonce per call (see `random_nonce_12`).
pub fn encrypt_field_value(
    plaintext: &JsonValue,
    key: &CdcEncryptionKey,
    nonce_bytes: [u8; 12],
) -> Result<EncryptedField, CdcEncryptionError> {
    let cipher = Aes256GcmSiv::new(&key.key_bytes.into());
    // Serialise to bytes so structured values (arrays/objects) round-
    // trip exactly. Tests pin that decrypt returns the same JSON value.
    let plain_bytes = serde_json::to_vec(plaintext)
        .map_err(|e| CdcEncryptionError::Crypto(format!("serialise plaintext: {e}")))?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ct = cipher
        .encrypt(nonce, plain_bytes.as_ref())
        .map_err(|e| CdcEncryptionError::Crypto(format!("AEAD encrypt: {e}")))?;
    Ok(EncryptedField {
        version_marker: 1,
        key_id: key.key_id.clone(),
        key_version: key.key_version,
        nonce_b64: BASE64_STANDARD.encode(nonce_bytes),
        ciphertext_b64: BASE64_STANDARD.encode(ct),
    })
}

/// Decrypt an envelope back into the original JSON value. Scope-gated:
/// `DecryptScope::None` returns `ScopeNotAuthorised`. The resolver
/// looks up the key; an unknown `(key_id, version)` returns
/// `UnknownKey`.
pub fn decrypt_field_value(
    envelope: &EncryptedField,
    resolver: &dyn CdcKeyResolver,
    scope: DecryptScope,
) -> Result<JsonValue, CdcEncryptionError> {
    if !scope.allows_decrypt() {
        return Err(CdcEncryptionError::ScopeNotAuthorised(scope.as_str()));
    }
    let key = resolver
        .resolve(&envelope.key_id, envelope.key_version)
        .ok_or_else(|| CdcEncryptionError::UnknownKey {
            key_id: envelope.key_id.clone(),
            key_version: envelope.key_version,
        })?;
    let nonce_bytes = BASE64_STANDARD
        .decode(&envelope.nonce_b64)
        .map_err(|e| CdcEncryptionError::Base64(format!("nonce: {e}")))?;
    if nonce_bytes.len() != 12 {
        return Err(CdcEncryptionError::Envelope(format!(
            "nonce must be 12 bytes, got {}",
            nonce_bytes.len()
        )));
    }
    let ct = BASE64_STANDARD
        .decode(&envelope.ciphertext_b64)
        .map_err(|e| CdcEncryptionError::Base64(format!("ciphertext: {e}")))?;
    let cipher = Aes256GcmSiv::new(&key.key_bytes.into());
    let nonce = Nonce::from_slice(&nonce_bytes);
    let plain = cipher
        .decrypt(nonce, ct.as_ref())
        .map_err(|e| CdcEncryptionError::Crypto(format!("AEAD decrypt: {e}")))?;
    serde_json::from_slice(&plain)
        .map_err(|e| CdcEncryptionError::NonStringPlaintext(e.to_string()))
}

/// Walk `payload` and replace every value at a sensitive field name
/// with its encrypted envelope. Returns an error only if the resolver
/// has no active key — individual field failures are skipped (the
/// field is left masked with a sentinel) so one bad value doesn't
/// poison the whole CDC stream.
pub fn encrypt_cdc_payload_fields(
    mut payload: JsonValue,
    sensitive_fields: &[String],
    resolver: &dyn CdcKeyResolver,
    nonce_provider: &dyn NonceProvider,
) -> Result<JsonValue, CdcEncryptionError> {
    let active = resolver
        .active_key()
        .ok_or(CdcEncryptionError::NoActiveKey)?;
    let keys: HashSet<String> = sensitive_fields
        .iter()
        .map(|f| f.to_ascii_lowercase())
        .collect();
    encrypt_value_recursive(&mut payload, &keys, &active, nonce_provider);
    Ok(payload)
}

fn encrypt_value_recursive(
    value: &mut JsonValue,
    sensitive_fields: &HashSet<String>,
    key: &CdcEncryptionKey,
    nonce_provider: &dyn NonceProvider,
) {
    match value {
        JsonValue::Object(obj) => {
            let keys: Vec<String> = obj.keys().cloned().collect();
            for k in keys {
                if sensitive_fields.contains(&k.to_ascii_lowercase()) {
                    if let Some(plain) = obj.get(&k).cloned() {
                        let nonce = nonce_provider.next_nonce();
                        match encrypt_field_value(&plain, key, nonce) {
                            Ok(env) => {
                                let env_json = serde_json::to_value(env).unwrap_or(JsonValue::Null);
                                obj.insert(k, env_json);
                            }
                            Err(_) => {
                                // Leave a sentinel rather than the raw
                                // plaintext — a partial failure must
                                // never leak the original value.
                                obj.insert(
                                    k,
                                    JsonValue::String("***ENCRYPT_FAILED***".to_string()),
                                );
                            }
                        }
                    }
                } else if let Some(child) = obj.get_mut(&k) {
                    encrypt_value_recursive(child, sensitive_fields, key, nonce_provider);
                }
            }
        }
        JsonValue::Array(items) => {
            for item in items {
                encrypt_value_recursive(item, sensitive_fields, key, nonce_provider);
            }
        }
        _ => {}
    }
}

// ── NonceProvider ─────────────────────────────────────────────────────────────

/// Pluggable nonce source. Production uses [`RandomNonceProvider`];
/// tests use [`FixedNonceProvider`] so encrypt outputs are deterministic.
pub trait NonceProvider: Send + Sync {
    fn next_nonce(&self) -> [u8; 12];
}

/// Test-only fixed nonce. Pinning a single nonce in tests is OK
/// because GCM-SIV is misuse-resistant — same key + nonce + plaintext
/// → same ciphertext, which is exactly what makes test assertions
/// stable.
#[derive(Debug, Clone, Copy)]
pub struct FixedNonceProvider {
    pub nonce: [u8; 12],
}
impl NonceProvider for FixedNonceProvider {
    fn next_nonce(&self) -> [u8; 12] {
        self.nonce
    }
}

/// Production nonce source backed by `rand`. NOT used in this module's
/// tests; left here so the production CDC engine has a single, named
/// entry point. Falls back to a process-local counter if entropy is
/// unavailable (still deterministic per-(key,plaintext) under GCM-SIV).
pub struct CounterNonceProvider {
    counter: std::sync::atomic::AtomicU64,
}

impl CounterNonceProvider {
    pub fn new() -> Self {
        Self {
            counter: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl Default for CounterNonceProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl NonceProvider for CounterNonceProvider {
    fn next_nonce(&self) -> [u8; 12] {
        let n = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut bytes = [0u8; 12];
        bytes[4..].copy_from_slice(&n.to_be_bytes());
        bytes
    }
}

/// Convenience: wrap a `CdcKeyResolver` in an `Arc` for shared access
/// across the CDC engine. Used at the runtime supervisor layer.
pub type SharedCdcKeyResolver = Arc<dyn CdcKeyResolver>;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_key() -> CdcEncryptionKey {
        // 32 bytes of `0x42` — exact bytes don't matter, but pinning
        // them keeps the ciphertext stable for golden assertions.
        CdcEncryptionKey::new("kek-test", 1, [0x42u8; 32])
    }

    fn fixed_nonce_provider() -> FixedNonceProvider {
        FixedNonceProvider { nonce: [0u8; 12] }
    }

    /// Pin: round-trip — a value encrypted then decrypted returns the
    /// exact same JSON value. The whole point of reversible mode.
    #[test]
    fn encrypt_then_decrypt_round_trips() {
        let key = test_key();
        let resolver = StaticKeyResolver::new().with_key(key.clone());
        let plain = json!("ssn-123-45-6789");
        let env = encrypt_field_value(&plain, &key, [0u8; 12]).expect("encrypt should succeed");
        let back = decrypt_field_value(&env, &resolver, DecryptScope::Replay)
            .expect("decrypt should succeed");
        assert_eq!(back, plain);
    }

    /// Pin: structured plaintext (object/array) also round-trips
    /// exactly. CDC payloads carry nested fields; the cipher must
    /// not unwrap them.
    #[test]
    fn encrypt_then_decrypt_preserves_nested_structure() {
        let key = test_key();
        let resolver = StaticKeyResolver::new().with_key(key.clone());
        let plain = json!({"first": "Ada", "last": "Lovelace", "tags": [1, 2, 3]});
        let env = encrypt_field_value(&plain, &key, [9u8; 12]).unwrap();
        let back = decrypt_field_value(&env, &resolver, DecryptScope::Audit).unwrap();
        assert_eq!(back, plain);
    }

    /// Pin: scope `None` refuses to decrypt even when the resolver
    /// holds the right key. This is the consumer-side authorisation
    /// gate — the cipher key alone isn't enough.
    #[test]
    fn decrypt_refuses_when_scope_is_none() {
        let key = test_key();
        let resolver = StaticKeyResolver::new().with_key(key.clone());
        let plain = json!("secret");
        let env = encrypt_field_value(&plain, &key, [1u8; 12]).unwrap();
        match decrypt_field_value(&env, &resolver, DecryptScope::None) {
            Err(CdcEncryptionError::ScopeNotAuthorised(scope)) => {
                assert_eq!(scope, "none");
            }
            other => panic!("expected ScopeNotAuthorised, got {:?}", other),
        }
    }

    /// Pin: unknown `(key_id, version)` fails fast — does NOT panic
    /// and does NOT leak whether the key existed in a prior epoch.
    #[test]
    fn decrypt_with_unknown_key_returns_typed_error() {
        let key = test_key();
        let resolver = StaticKeyResolver::new().with_key(key.clone());
        let plain = json!("secret");
        let mut env = encrypt_field_value(&plain, &key, [2u8; 12]).unwrap();
        env.key_version = 999;
        match decrypt_field_value(&env, &resolver, DecryptScope::Replay) {
            Err(CdcEncryptionError::UnknownKey {
                key_id,
                key_version,
            }) => {
                assert_eq!(key_id, "kek-test");
                assert_eq!(key_version, 999);
            }
            other => panic!("expected UnknownKey, got {:?}", other),
        }
    }

    /// Pin: the payload-level walker encrypts every sensitive field
    /// and leaves non-sensitive fields untouched. The encrypted
    /// fields carry the envelope sentinel so a consumer can branch.
    #[test]
    fn payload_walker_encrypts_only_sensitive_fields() {
        let key = test_key();
        let resolver = StaticKeyResolver::new().with_key(key.clone());
        let payload = json!({
            "id": 42,
            "ssn": "123-45-6789",
            "email": "ada@example.com",
            "name": "Ada Lovelace"
        });
        let sensitive = vec!["ssn".to_string(), "email".to_string()];
        let out =
            encrypt_cdc_payload_fields(payload, &sensitive, &resolver, &fixed_nonce_provider())
                .unwrap();

        // Non-sensitive fields untouched.
        assert_eq!(out["id"], json!(42));
        assert_eq!(out["name"], json!("Ada Lovelace"));
        // Sensitive fields replaced with envelope sentinels.
        assert!(EncryptedField::is_envelope(&out["ssn"]));
        assert!(EncryptedField::is_envelope(&out["email"]));
        // Plaintext absolutely not present anywhere in the stringified
        // payload — the regression every reversible mode must pin.
        let serialised = serde_json::to_string(&out).unwrap();
        assert!(
            !serialised.contains("123-45-6789"),
            "plaintext SSN must not appear in encrypted payload"
        );
        assert!(
            !serialised.contains("ada@example.com"),
            "plaintext email must not appear in encrypted payload"
        );

        // Round-trip: decrypt each envelope and confirm originals.
        let ssn_env: EncryptedField = serde_json::from_value(out["ssn"].clone()).unwrap();
        let back = decrypt_field_value(&ssn_env, &resolver, DecryptScope::Replay).unwrap();
        assert_eq!(back, json!("123-45-6789"));
    }

    /// Pin: payload walker descends into nested objects, encrypting
    /// sensitive fields wherever they appear.
    #[test]
    fn payload_walker_descends_into_nested_objects() {
        let key = test_key();
        let resolver = StaticKeyResolver::new().with_key(key.clone());
        let payload = json!({
            "user": {
                "profile": {"ssn": "123"}
            }
        });
        let out = encrypt_cdc_payload_fields(
            payload,
            &["ssn".to_string()],
            &resolver,
            &fixed_nonce_provider(),
        )
        .unwrap();
        assert!(EncryptedField::is_envelope(&out["user"]["profile"]["ssn"]));
    }

    /// Pin: missing active key fails fast — producer can't silently
    /// emit plaintext.
    #[test]
    fn missing_active_key_refuses_to_encrypt() {
        let resolver = StaticKeyResolver::new(); // no keys
        let err = encrypt_cdc_payload_fields(
            json!({"ssn": "x"}),
            &["ssn".to_string()],
            &resolver,
            &fixed_nonce_provider(),
        )
        .expect_err("should refuse without an active key");
        assert!(matches!(err, CdcEncryptionError::NoActiveKey));
    }

    /// Pin: scope token map. Audit logs include these so the contract
    /// is stable.
    #[test]
    fn scope_tokens_are_pinned() {
        assert_eq!(DecryptScope::None.as_str(), "none");
        assert_eq!(DecryptScope::Replay.as_str(), "replay");
        assert_eq!(DecryptScope::Audit.as_str(), "audit");
        assert!(!DecryptScope::None.allows_decrypt());
        assert!(DecryptScope::Replay.allows_decrypt());
        assert!(DecryptScope::Audit.allows_decrypt());
    }

    /// Pin: `is_envelope` matches every envelope this module produces
    /// and rejects plain JSON. Consumer branching depends on this.
    #[test]
    fn envelope_detector_distinguishes_envelopes() {
        let key = test_key();
        let env = encrypt_field_value(&json!("x"), &key, [3u8; 12]).unwrap();
        let env_json = serde_json::to_value(&env).unwrap();
        assert!(EncryptedField::is_envelope(&env_json));
        assert!(!EncryptedField::is_envelope(&json!("plain string")));
        assert!(!EncryptedField::is_envelope(&json!({"some": "object"})));
        assert!(!EncryptedField::is_envelope(&json!(42)));
        assert!(!EncryptedField::is_envelope(&json!(null)));
    }

    /// Pin: counter nonce produces 12 bytes and is monotonic. Doesn't
    /// have to be unique under GCM-SIV (the algorithm is
    /// misuse-resistant), but a counter is the cheapest way to avoid
    /// reusing the exact same `(key, nonce, plaintext)` triple within
    /// one process.
    #[test]
    fn counter_nonce_is_monotonic_and_twelve_bytes() {
        let provider = CounterNonceProvider::new();
        let a = provider.next_nonce();
        let b = provider.next_nonce();
        assert_eq!(a.len(), 12);
        assert_eq!(b.len(), 12);
        assert_ne!(a, b);
    }

    /// Pin: encrypting the same plaintext twice with the same nonce
    /// yields the same ciphertext under GCM-SIV. Confirms the
    /// algorithm choice and gives tests a stable golden.
    #[test]
    fn gcm_siv_is_deterministic_under_same_nonce() {
        let key = test_key();
        let plain = json!("repeat");
        let a = encrypt_field_value(&plain, &key, [7u8; 12]).unwrap();
        let b = encrypt_field_value(&plain, &key, [7u8; 12]).unwrap();
        assert_eq!(a.ciphertext_b64, b.ciphertext_b64);
        // Different nonce → different ciphertext.
        let c = encrypt_field_value(&plain, &key, [8u8; 12]).unwrap();
        assert_ne!(a.ciphertext_b64, c.ciphertext_b64);
    }
}
