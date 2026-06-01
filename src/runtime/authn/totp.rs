//! RFC 6238 TOTP (time-based one-time passwords) for native MFA.
//!
//! Authenticator apps (Google Authenticator, Authy, 1Password, …) default to
//! HMAC-SHA1 / 30-second steps / 6 digits, so that is what this module
//! implements for interoperability. The shared secret is generated server-side,
//! handed to the user once as a base32 string + `otpauth://` provisioning URI,
//! and stored encrypted at rest (AES-256-GCM-SIV) — TOTP verification needs the
//! plaintext secret, so it is encrypted, not hashed.

use aes_gcm_siv::aead::Aead;
use aes_gcm_siv::{Aes256GcmSiv, KeyInit, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use sha2::{Digest, Sha256};

use crate::runtime::security::hmac_sha1;

const STEP_SECS: u64 = 30;
const DIGITS: u32 = 6;
/// Accept codes within ±1 step (±30s) to tolerate clock skew, matching common
/// authenticator-server practice.
const DEFAULT_WINDOW: i64 = 1;

const BASE32_ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// Generate a fresh 160-bit TOTP secret, base32-encoded (no padding). Entropy
/// comes from two UUIDv4s so no extra RNG dependency is needed.
pub fn generate_secret() -> String {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    base32_encode(&bytes[..20])
}

/// Build the `otpauth://totp/...` provisioning URI an authenticator app scans.
pub fn provisioning_uri(issuer: &str, account: &str, secret_base32: &str) -> String {
    let issuer_enc = urlencoding::encode(issuer);
    let account_enc = urlencoding::encode(account);
    format!(
        "otpauth://totp/{issuer_enc}:{account_enc}?secret={secret_base32}&issuer={issuer_enc}&algorithm=SHA1&digits={DIGITS}&period={STEP_SECS}"
    )
}

/// Verify a submitted TOTP `code` against the base32 `secret` at `unix_time`,
/// allowing ±1 step of clock skew. Returns false for malformed secrets/codes.
pub fn verify(secret_base32: &str, code: &str, unix_time: u64) -> bool {
    verify_with_window(secret_base32, code, unix_time, DEFAULT_WINDOW)
}

pub fn verify_with_window(secret_base32: &str, code: &str, unix_time: u64, window: i64) -> bool {
    let code = code.trim();
    if code.len() != DIGITS as usize || !code.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    let Some(key) = base32_decode(secret_base32) else {
        return false;
    };
    let step = (unix_time / STEP_SECS) as i64;
    let mut matched = false;
    // Iterate the whole window unconditionally (no early break) and OR the
    // per-step byte diff so the comparison time doesn't leak which step matched.
    for offset in -window..=window {
        if step + offset < 0 {
            continue;
        }
        let counter = (step + offset) as u64;
        let candidate = hotp(&key, counter);
        let mut local = 0u8;
        for (x, y) in candidate.as_bytes().iter().zip(code.as_bytes()) {
            local |= x ^ y;
        }
        matched |= local == 0;
    }
    matched
}

/// Compute the current TOTP code for a secret (used by tests / enrollment echo).
pub fn current_code(secret_base32: &str, unix_time: u64) -> Option<String> {
    let key = base32_decode(secret_base32)?;
    Some(hotp(&key, unix_time / STEP_SECS))
}

/// HOTP (RFC 4226) truncation over HMAC-SHA1 of the big-endian counter.
fn hotp(key: &[u8], counter: u64) -> String {
    let mac = hmac_sha1(key, &counter.to_be_bytes());
    let offset = (mac[19] & 0x0f) as usize;
    let bin = ((u32::from(mac[offset]) & 0x7f) << 24)
        | (u32::from(mac[offset + 1]) << 16)
        | (u32::from(mac[offset + 2]) << 8)
        | u32::from(mac[offset + 3]);
    let modulo = 10u32.pow(DIGITS);
    format!("{:0width$}", bin % modulo, width = DIGITS as usize)
}

// ── Secret-at-rest encryption ────────────────────────────────────────────────

/// Encrypt a TOTP secret for storage in `totp_secret_enc`. Output is
/// `base64(nonce ‖ ciphertext)`. The AES-256 key is `SHA256(enc_key)`; the
/// 96-bit nonce is random (UUIDv4 entropy). Returns `None` if encryption fails.
pub fn encrypt_secret(secret_base32: &str, enc_key: &[u8]) -> Option<String> {
    let key = Sha256::digest(enc_key);
    let cipher = Aes256GcmSiv::new_from_slice(&key).ok()?;
    let uuid = uuid::Uuid::new_v4();
    let nonce_bytes = &uuid.as_bytes()[..12];
    let nonce = Nonce::from_slice(nonce_bytes);
    let ct = cipher.encrypt(nonce, secret_base32.as_bytes()).ok()?;
    let mut out = Vec::with_capacity(12 + ct.len());
    out.extend_from_slice(nonce_bytes);
    out.extend_from_slice(&ct);
    Some(B64.encode(out))
}

/// Decrypt a stored TOTP secret produced by [`encrypt_secret`]. Returns `None`
/// for malformed/garbled input or a wrong key.
pub fn decrypt_secret(stored: &str, enc_key: &[u8]) -> Option<String> {
    let raw = B64.decode(stored.trim()).ok()?;
    if raw.len() < 12 {
        return None;
    }
    let (nonce_bytes, ct) = raw.split_at(12);
    let key = Sha256::digest(enc_key);
    let cipher = Aes256GcmSiv::new_from_slice(&key).ok()?;
    let nonce = Nonce::from_slice(nonce_bytes);
    let plain = cipher.decrypt(nonce, ct).ok()?;
    String::from_utf8(plain).ok()
}

// ── base32 (RFC 4648, no padding) ────────────────────────────────────────────

fn base32_encode(data: &[u8]) -> String {
    let mut out = String::new();
    let mut buffer = 0u32;
    let mut bits = 0u32;
    for &byte in data {
        buffer = (buffer << 8) | u32::from(byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            let idx = ((buffer >> bits) & 0x1f) as usize;
            out.push(BASE32_ALPHABET[idx] as char);
        }
    }
    if bits > 0 {
        let idx = ((buffer << (5 - bits)) & 0x1f) as usize;
        out.push(BASE32_ALPHABET[idx] as char);
    }
    out
}

fn base32_decode(s: &str) -> Option<Vec<u8>> {
    let mut buffer = 0u32;
    let mut bits = 0u32;
    let mut out = Vec::new();
    for c in s.trim().chars() {
        if c == '=' || c.is_whitespace() {
            continue;
        }
        let upper = c.to_ascii_uppercase();
        let val = BASE32_ALPHABET.iter().position(|&b| b as char == upper)? as u32;
        buffer = (buffer << 5) | val;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push(((buffer >> bits) & 0xff) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base32_round_trips() {
        let data = b"abcdefghij1234567890";
        let enc = base32_encode(data);
        assert_eq!(base32_decode(&enc).unwrap(), data);
    }

    #[test]
    fn rfc6238_known_vector_sha1() {
        // RFC 6238 Appendix B seed "12345678901234567890" (ASCII) at T=59 → 287082.
        let secret = base32_encode(b"12345678901234567890");
        assert_eq!(current_code(&secret, 59).unwrap(), "287082");
        assert_eq!(current_code(&secret, 1111111109).unwrap(), "081804");
    }

    #[test]
    fn verify_accepts_current_and_skew_window() {
        let secret = generate_secret();
        let t = 1_700_000_000u64;
        let code = current_code(&secret, t).unwrap();
        assert!(verify(&secret, &code, t));
        // within ±1 step
        assert!(verify(&secret, &code, t + 25));
        // far outside the window
        assert!(!verify(&secret, &code, t + 600));
        assert!(!verify(&secret, "000000", t) || code == "000000");
    }

    #[test]
    fn encrypt_secret_round_trips_and_rejects_wrong_key() {
        let secret = generate_secret();
        let enc = encrypt_secret(&secret, b"server-enc-key").unwrap();
        assert_ne!(enc, secret);
        assert_eq!(
            decrypt_secret(&enc, b"server-enc-key").as_deref(),
            Some(secret.as_str())
        );
        assert!(decrypt_secret(&enc, b"wrong-key").is_none());
    }

    #[test]
    fn provisioning_uri_is_well_formed() {
        let uri = provisioning_uri("UDB", "alice@example.com", "JBSWY3DPEHPK3PXP");
        assert!(uri.starts_with("otpauth://totp/UDB:"));
        assert!(uri.contains("secret=JBSWY3DPEHPK3PXP"));
        assert!(uri.contains("algorithm=SHA1"));
        assert!(uri.contains("period=30"));
    }
}
