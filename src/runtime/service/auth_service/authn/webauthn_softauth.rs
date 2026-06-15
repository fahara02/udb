//! Dev-only software WebAuthn authenticator (bug_report.md Q#9-11).
//!
//! REAL cryptography, NOT an accept-any bypass: a per-user P-256 keypair produces
//! a genuine `"none"`-attestation registration credential and an ES256 assertion
//! that `webauthn_rs`'s `finish_passkey_*` verifies legitimately. It exists only so
//! a headless conformance harness (which has no hardware authenticator) can complete
//! the Finish ceremonies. Instantiated ONLY when `UDB_WEBAUTHN_TEST_MODE=1`; a
//! production broker never constructs it (fail-closed default), so it can never
//! weaken real passkey verification.

use std::sync::OnceLock;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey};
use sha2::{Digest, Sha256};

const FLAG_UP: u8 = 0x01; // user present
const FLAG_UV: u8 = 0x04; // user verified (passkeys require UV)
const FLAG_AT: u8 = 0x40; // attested credential data included

/// Sentinel the harness sends as `public_key_credential_json` to ask the broker's
/// dev authenticator to complete the ceremony (test mode only).
pub(super) const TEST_CREDENTIAL_SENTINEL: &str = "__UDB_WEBAUTHN_TEST__";

/// Whether the dev authenticator is enabled (resolved once). Fail-closed default.
pub(super) fn test_mode_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("UDB_WEBAUTHN_TEST_MODE")
            .map(|v| {
                let v = v.trim();
                v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes")
            })
            .unwrap_or(false)
    })
}

/// One software authenticator: a P-256 keypair + credential id. STATELESS —
/// both are derived deterministically from the user id, so nothing is stored.
struct SoftAuthenticator {
    signing_key: SigningKey,
    credential_id: Vec<u8>,
}

impl SoftAuthenticator {
    /// Derive this user's authenticator DETERMINISTICALLY from `user_id`. No state
    /// is kept anywhere: registration and a later assertion — even on a different
    /// broker replica or after a restart — reconstruct the SAME keypair, so the
    /// passkey public key stored at registration always matches the assertion
    /// signature. signCount is fixed at 0 (WebAuthn treats 0/0 as "counter
    /// unsupported", so no per-user counter state is needed either).
    fn for_user(user_id: &str) -> Result<Self, String> {
        // P-256 private scalar from a domain-separated SHA-256 of the user id;
        // re-hash on the astronomically rare invalid-scalar (>= curve order / zero).
        let mut seed = Sha256::digest(format!("udb-webauthn-dev-key:{user_id}").as_bytes());
        let signing_key = loop {
            match SigningKey::from_slice(seed.as_slice()) {
                Ok(key) => break key,
                Err(_) => seed = Sha256::digest(seed),
            }
        };
        let credential_id =
            Sha256::digest(format!("udb-webauthn-dev-cred:{user_id}").as_bytes())[..16].to_vec();
        Ok(Self {
            signing_key,
            credential_id,
        })
    }

    /// COSE_Key (EC2 / P-256 / ES256), hand-encoded CBOR (fixed 77-byte shape).
    fn cose_key(&self) -> Vec<u8> {
        let point = self.signing_key.verifying_key().to_encoded_point(false);
        let x = point.x().expect("uncompressed P-256 point has x");
        let y = point.y().expect("uncompressed P-256 point has y");
        let mut out = Vec::with_capacity(77);
        out.push(0xA5); // map(5)
        out.extend_from_slice(&[0x01, 0x02]); // 1 (kty) : 2 (EC2)
        out.extend_from_slice(&[0x03, 0x26]); // 3 (alg) : -7 (ES256)
        out.extend_from_slice(&[0x20, 0x01]); // -1 (crv) : 1 (P-256)
        out.extend_from_slice(&[0x21, 0x58, 0x20]); // -2 (x) : bytes(32)
        out.extend_from_slice(x);
        out.extend_from_slice(&[0x22, 0x58, 0x20]); // -3 (y) : bytes(32)
        out.extend_from_slice(y);
        out
    }

    fn authenticator_data(&self, rp_id: &str, flags: u8, include_cred: bool) -> Vec<u8> {
        let mut ad = Vec::new();
        ad.extend_from_slice(&Sha256::digest(rp_id.as_bytes())); // rpIdHash (32)
        ad.push(flags);
        ad.extend_from_slice(&0u32.to_be_bytes()); // signCount: 0 (counter-unsupported)
        if include_cred {
            ad.extend_from_slice(&[0u8; 16]); // aaguid (all-zero for a soft authenticator)
            ad.extend_from_slice(&(self.credential_id.len() as u16).to_be_bytes());
            ad.extend_from_slice(&self.credential_id);
            ad.extend_from_slice(&self.cose_key());
        }
        ad
    }

    /// `RegisterPublicKeyCredential` JSON for `challenge_b64` (verbatim from the
    /// creation options) — a `"none"`-attestation registration response.
    fn registration_json(&self, rp_id: &str, origin: &str, challenge_b64: &str) -> String {
        let client_data = client_data_json("webauthn.create", challenge_b64, origin);
        let auth_data = self.authenticator_data(rp_id, FLAG_UP | FLAG_UV | FLAG_AT, true);
        // attestationObject = { "fmt":"none", "attStmt":{}, "authData": <bytes> }
        let mut att = Vec::new();
        att.push(0xA3); // map(3)
        att.extend_from_slice(&[0x63, b'f', b'm', b't']); // text "fmt"
        att.extend_from_slice(&[0x64, b'n', b'o', b'n', b'e']); // text "none"
        att.extend_from_slice(&[0x67, b'a', b't', b't', b'S', b't', b'm', b't']); // text "attStmt"
        att.push(0xA0); // map(0)
        att.extend_from_slice(&[0x68, b'a', b'u', b't', b'h', b'D', b'a', b't', b'a']); // text "authData"
        push_cbor_bytes(&mut att, &auth_data);
        let id = B64URL.encode(&self.credential_id);
        format!(
            "{{\"id\":\"{id}\",\"rawId\":\"{id}\",\"type\":\"public-key\",\"response\":{{\"attestationObject\":\"{}\",\"clientDataJSON\":\"{}\"}},\"clientExtensionResults\":{{}}}}",
            B64URL.encode(&att),
            B64URL.encode(client_data.as_bytes())
        )
    }

    /// `PublicKeyCredential` (assertion) JSON for `challenge_b64`, signed with the
    /// registered keypair (ES256 over `authData || SHA256(clientDataJSON)`).
    fn assertion_json(
        &self,
        rp_id: &str,
        origin: &str,
        challenge_b64: &str,
        user_handle_b64: &str,
    ) -> String {
        let client_data = client_data_json("webauthn.get", challenge_b64, origin);
        let auth_data = self.authenticator_data(rp_id, FLAG_UP | FLAG_UV, false);
        let mut signed = auth_data.clone();
        signed.extend_from_slice(&Sha256::digest(client_data.as_bytes()));
        let sig: Signature = self.signing_key.sign(&signed);
        let sig_der = sig.to_der();
        let id = B64URL.encode(&self.credential_id);
        format!(
            "{{\"id\":\"{id}\",\"rawId\":\"{id}\",\"type\":\"public-key\",\"response\":{{\"authenticatorData\":\"{}\",\"clientDataJSON\":\"{}\",\"signature\":\"{}\",\"userHandle\":\"{}\"}},\"clientExtensionResults\":{{}}}}",
            B64URL.encode(&auth_data),
            B64URL.encode(client_data.as_bytes()),
            B64URL.encode(sig_der.as_bytes()),
            user_handle_b64
        )
    }
}

fn client_data_json(typ: &str, challenge_b64: &str, origin: &str) -> String {
    // challenge is b64url and origin is a URL token — neither needs JSON escaping.
    format!(
        "{{\"type\":\"{typ}\",\"challenge\":\"{challenge_b64}\",\"origin\":\"{origin}\",\"crossOrigin\":false}}"
    )
}

/// CBOR byte-string header + payload (authData is always < 65536 bytes).
fn push_cbor_bytes(out: &mut Vec<u8>, b: &[u8]) {
    if b.len() < 24 {
        out.push(0x40 | b.len() as u8);
    } else if b.len() < 256 {
        out.push(0x58);
        out.push(b.len() as u8);
    } else {
        out.push(0x59);
        out.extend_from_slice(&(b.len() as u16).to_be_bytes());
    }
    out.extend_from_slice(b);
}

/// Pull the verbatim b64url `publicKey.challenge` out of a creation/request
/// options JSON (what `webauthn-rs` serialized to the client at Start).
pub(super) fn challenge_from_options(options_json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(options_json)
        .ok()?
        .get("publicKey")?
        .get("challenge")?
        .as_str()
        .map(str::to_string)
}

/// Registration credential for `user_id` — derives the deterministic keypair and
/// returns a `"none"`-attestation response. No state is stored; the keypair is
/// reconstructed identically at assertion time.
pub(super) fn make_registration(
    user_id: &str,
    rp_id: &str,
    origin: &str,
    challenge_b64: &str,
) -> Result<String, String> {
    Ok(SoftAuthenticator::for_user(user_id)?.registration_json(rp_id, origin, challenge_b64))
}

/// Assertion for `user_id` — reconstructs the SAME deterministic keypair and signs.
/// The userHandle is the user's UUID bytes — exactly what `start_passkey_*` bound
/// as the passkey's user handle, so `webauthn-rs` matches it on Finish.
pub(super) fn make_assertion(
    user_id: &str,
    rp_id: &str,
    origin: &str,
    challenge_b64: &str,
) -> Result<String, String> {
    let user_handle_b64 = uuid::Uuid::parse_str(user_id)
        .map(|u| B64URL.encode(u.as_bytes()))
        .unwrap_or_else(|_| B64URL.encode(user_id.as_bytes()));
    Ok(SoftAuthenticator::for_user(user_id)?.assertion_json(
        rp_id,
        origin,
        challenge_b64,
        &user_handle_b64,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use webauthn_rs::prelude::{Url, WebauthnBuilder};

    // Full in-process ceremony: start → dev soft-authenticator → finish, proving the
    // minted credential/assertion verify against the REAL webauthn-rs verifier
    // (surfaces the actual rejection reason instead of the handler's opaque
    // "invalid credential"). No DB required.
    #[test]
    fn soft_authenticator_registration_then_authentication_verify() {
        let rp_id = "localhost";
        let origin = Url::parse("http://localhost").unwrap();
        let origin_str = origin.origin().ascii_serialization();
        let webauthn = WebauthnBuilder::new(rp_id, &origin)
            .unwrap()
            .build()
            .unwrap();
        let user_uuid = uuid::Uuid::new_v4();
        let user_id = user_uuid.to_string();

        // --- registration ---
        let (creation, reg_state) = webauthn
            .start_passkey_registration(user_uuid, "tester", "Tester", None)
            .unwrap();
        let creation_json = serde_json::to_string(&creation).unwrap();
        let challenge = challenge_from_options(&creation_json).expect("creation challenge");
        let reg_json = make_registration(&user_id, rp_id, &origin_str, &challenge).unwrap();
        let reg_cred: webauthn_rs::prelude::RegisterPublicKeyCredential =
            serde_json::from_str(&reg_json).unwrap();
        let passkey = webauthn
            .finish_passkey_registration(&reg_cred, &reg_state)
            .unwrap_or_else(|e| panic!("registration must verify: {e:?}"));

        // --- authentication ---
        let (request, auth_state) = webauthn
            .start_passkey_authentication(std::slice::from_ref(&passkey))
            .unwrap();
        let request_json = serde_json::to_string(&request).unwrap();
        let auth_challenge = challenge_from_options(&request_json).expect("request challenge");
        let auth_json = make_assertion(&user_id, rp_id, &origin_str, &auth_challenge).unwrap();
        let auth_cred: webauthn_rs::prelude::PublicKeyCredential =
            serde_json::from_str(&auth_json).unwrap();
        let result = webauthn
            .finish_passkey_authentication(&auth_cred, &auth_state)
            .unwrap_or_else(|e| panic!("authentication must verify: {e:?}"));
        assert!(
            result.user_verified(),
            "assertion must report user-verified"
        );
    }
}
