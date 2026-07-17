//! The isolated crypto stack for the native `VaultService` — ONE stack: the
//! master-KEK wrap (`encrypt_secret_at_rest`) plus the shared `aes_gcm_siv`
//! primitive `encryption.rs` uses. Extracted verbatim from the former god file:
//! the redacting-`Debug` secret-bearing key/plaintext types, the DEK wrap/unwrap,
//! the AES-256-GCM-SIV seal/open, the `udb-vault:` / `udb-vmac:` envelope parsers,
//! the FIPS-198 HMAC-SHA256 + constant-time compare, the transit-algorithm
//! allow-list guard, and the stored-secret envelope stripper.
//!
//! CRITICAL: this is security-critical crypto code — every byte here is a pure
//! relocation of the tested primitives; no encryption/decryption/signing/HMAC/
//! wrap/envelope/seal logic is altered.

use std::fmt;

use aead::{Aead, KeyInit};
use aes_gcm_siv::{Aes256GcmSiv, Nonce};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ed25519_dalek::{Signature, Signer, SigningKey};
use sha2::{Digest, Sha256};
use tonic::Status;
use uuid::Uuid;

use crate::runtime::DataBrokerRuntime;

use super::config::{
    DEFAULT_TRANSIT_ALGORITHM, SIGNING_TRANSIT_ALGORITHM, SUPPORTED_TRANSIT_ALGORITHMS,
    VAULT_ED25519_PREFIX, VAULT_HMAC_PREFIX, VAULT_TRANSIT_ENVELOPE_PREFIX,
};
use super::errors::{
    vault_field_violation, vault_internal_status, vault_master_key_operation_status,
};

// ── secret-bearing types with redacting Debug (day one) ───────────────────────

/// A 32-byte data-encryption key. Redacting `Debug`: the key bytes NEVER appear
/// in logs/panics. Mirrors `encryption.rs::EncryptionKey`'s `[redacted]` Debug.
pub(crate) struct DataKey(pub(crate) [u8; 32]);

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
    pub(crate) fn generate() -> Self {
        let mut bytes = [0u8; 32];
        bytes[..16].copy_from_slice(Uuid::new_v4().as_bytes());
        bytes[16..].copy_from_slice(Uuid::new_v4().as_bytes());
        DataKey(bytes)
    }

    pub(crate) fn to_b64(&self) -> String {
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
pub(crate) struct PlaintextSecret(pub(crate) String);

impl fmt::Debug for PlaintextSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PlaintextSecret")
            .field(&"[redacted]")
            .finish()
    }
}

// ── crypto helpers — ONE stack: master-KEK wrap + the shared aes_gcm_siv ──────

/// Wrap a DEK with the master KEK (`encrypt_secret_at_rest`). The result is the
/// `udb-aead:` envelope (or, in dev with no KEK, the base64 passthrough — still
/// only the DEK, never a cleartext secret value).
pub(crate) fn wrap_dek(runtime: &DataBrokerRuntime, dek: &DataKey) -> Result<String, Status> {
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
pub(crate) fn unwrap_dek(runtime: &DataBrokerRuntime, wrapped: &str) -> Result<DataKey, Status> {
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
pub(crate) fn dek_seal(dek: &DataKey, version: i64, plaintext: &[u8]) -> Result<String, Status> {
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
pub(crate) fn dek_open(dek: &DataKey, encoded: &str) -> Result<Vec<u8>, Status> {
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
pub(crate) fn parse_transit_envelope(value: &str) -> Option<(i64, &str)> {
    let rest = value.strip_prefix(VAULT_TRANSIT_ENVELOPE_PREFIX)?;
    let (version, encoded) = rest.split_once(':')?;
    Some((version.parse().ok()?, encoded))
}

/// Parse a `udb-vmac:v<version>:<b64>` signature into `(version, b64)`.
pub(crate) fn parse_mac_envelope(value: &str) -> Option<(i64, &str)> {
    let rest = value.strip_prefix(VAULT_HMAC_PREFIX)?;
    let (version, encoded) = rest.split_once(':')?;
    Some((version.parse().ok()?, encoded))
}

/// Parse a `udb-vsig:v<version>:<b64>` Ed25519 signature into `(version, b64)`.
pub(crate) fn parse_ed25519_envelope(value: &str) -> Option<(i64, &str)> {
    let rest = value.strip_prefix(VAULT_ED25519_PREFIX)?;
    let (version, encoded) = rest.split_once(':')?;
    Some((version.parse().ok()?, encoded))
}

/// Ed25519-sign `message` with the 32-byte transit key material as the seed,
/// returning the base64 of the 64-byte signature. The key material never leaves
/// the broker; the signature is a real asymmetric signature verifiable by anyone
/// holding the corresponding public key. Deterministic (RFC 8032).
pub(crate) fn ed25519_sign_b64(seed: &[u8; 32], message: &[u8]) -> String {
    let signing_key = SigningKey::from_bytes(seed);
    BASE64_STANDARD.encode(signing_key.sign(message).to_bytes())
}

/// Verify a base64 Ed25519 signature over `message` under the key derived from
/// `seed`. Any decode/length/verification failure is a plain `false` (no panic,
/// no error leak) — the caller reports only the boolean outcome. Uses
/// `verify_strict` to reject malleable/small-order signatures.
pub(crate) fn ed25519_verify_b64(seed: &[u8; 32], message: &[u8], signature_b64: &str) -> bool {
    let Ok(bytes) = BASE64_STANDARD.decode(signature_b64) else {
        return false;
    };
    let Ok(signature_bytes): Result<[u8; 64], _> = bytes.try_into() else {
        return false;
    };
    let signature = Signature::from_bytes(&signature_bytes);
    SigningKey::from_bytes(seed)
        .verifying_key()
        .verify_strict(message, &signature)
        .is_ok()
}

const HMAC_BLOCK: usize = 64; // SHA-256 block size.

/// HMAC-SHA256 built on the already-vendored `sha2` crate (no `hmac` crate
/// dependency added). Standard FIPS-198 construction.
pub(crate) fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
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
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Resolve and validate the requested transit `algorithm`. An empty value takes
/// the default; a recognized value is canonicalized to its supported spelling; an
/// unknown value is rejected with a typed field violation naming `algorithm`
/// (rather than being silently treated as the default). Purely a validation guard
/// — it never changes which crypto primitive the seal/open path uses.
pub(crate) fn validate_transit_algorithm(requested: &str) -> Result<String, Status> {
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

/// Reject the encryption path (Encrypt/Decrypt/GenerateDataKey/Rewrap) on a key
/// created with the Ed25519 signing algorithm: its material is a signature seed,
/// not an AEAD key, so encrypting with it is a key-purpose confusion. Keeps key
/// purpose unambiguous — a signing key signs (Sign/Verify), an encryption key
/// encrypts. A no-op for every encryption-algorithm key (the common case).
pub(crate) fn require_encryption_algorithm(algorithm: &str, operation: &str) -> Result<(), Status> {
    if algorithm == SIGNING_TRANSIT_ALGORITHM {
        return Err(vault_field_violation(
            "key_name",
            format!(
                "must name an encryption key ({DEFAULT_TRANSIT_ALGORITHM}), not an \
                 {SIGNING_TRANSIT_ALGORITHM} signing key"
            ),
            format!(
                "{operation} requires an encryption key; the named key uses the \
                 {SIGNING_TRANSIT_ALGORITHM} signing algorithm (use Sign/Verify instead)"
            ),
        ));
    }
    Ok(())
}

/// The transit-ciphertext stored for a KV secret is written as a bare
/// `udb-vault:` envelope; strip the prefix back to the base64 body the DEK
/// opener consumes. Shared by `get_secret`.
pub(crate) fn transit_payload(ciphertext: &str) -> Result<&str, Status> {
    parse_transit_envelope(ciphertext)
        .map(|(_, encoded)| encoded)
        .ok_or_else(|| {
            vault_internal_status(
                "get_secret_parse_envelope",
                "stored secret has a malformed ciphertext envelope",
            )
        })
}
