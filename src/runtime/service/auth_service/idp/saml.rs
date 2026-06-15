//! SAML 2.0 web-SSO support: metadata import, AuthnRequest construction, and
//! SAMLResponse/assertion validation (NameID + attributes + conditions).
//!
//! Crypto posture (no capability lie):
//!   * Assertion signature verification uses pure-Rust `rsa` + `sha2`/`sha-1`
//!     (RSA-PKCS1v15) with the IdP certificate's public key extracted via
//!     `x509-parser`.
//!   * Exclusive XML Canonicalization (xml-exc-c14n, <https://www.w3.org/TR/xml-exc-c14n/>)
//!     IS applied (pure-Rust, see the sibling [`crate::runtime::service::auth_service::idp::c14n`]
//!     module). The full XML-DSig verification is performed:
//!       1. The signed element referenced by `<ds:Reference URI="#id">` is
//!          located by its `ID` attribute, canonicalized with exclusive C14N
//!          (honoring any `InclusiveNamespaces/@PrefixList`), digested
//!          (SHA-256/SHA-1 per `<ds:DigestMethod>`), and compared constant-spec
//!          to `<ds:DigestValue>`. This is what binds the signature to the
//!          *content* and is what closes the transform-bypass: a mutated
//!          document yields a different canonical digest and fails.
//!       2. `<ds:SignedInfo>` is canonicalized with exclusive C14N and the
//!          `<ds:SignatureValue>` is RSA-PKCS1v15-verified over those octets.
//!     Both must pass. We never report `signature_verified = true` without both
//!     the reference-digest match AND the RSA verification succeeding.
//!   * When a provider has NO certs configured, signature verification cannot be
//!     performed at all; such an assertion is rejected (fail-closed) rather than
//!     trusted.
//!
//! Everything that does not require crypto (metadata import, request build,
//! NameID/attribute/condition parsing, clock-skew) is fully implemented.

use std::collections::BTreeMap;
use std::io::Write;

use chrono::{DateTime, Utc};
use quick_xml::Reader;
use quick_xml::events::Event;

use super::c14n;

/// Parsed SAML IdP metadata.
#[derive(Debug, Clone, Default)]
pub struct SamlMetadata {
    pub entity_id: String,
    pub sso_url: String,
    /// Base64 DER (no PEM header) signing certificates.
    pub signing_certs_b64: Vec<String>,
}

/// Result of validating a SAMLResponse assertion.
#[derive(Debug, Clone, Default)]
pub struct SamlAssertion {
    pub assertion_id: String,
    pub issuer: String,
    pub name_id: String,
    pub attributes: BTreeMap<String, Vec<String>>,
    pub authn_context: String,
    pub not_before_unix: i64,
    pub not_on_or_after_unix: i64,
    pub audience: String,
}

#[derive(Debug)]
#[allow(dead_code)] // Replay/SignatureBlocked are part of the documented error surface;
// replay is enforced by the durable cache in `store`, and SignatureBlocked
// documents the host-blocked C14N path (see module docs).
pub enum SamlError {
    Parse(String),
    Expired(String),
    Replay(String),
    Signature(String),
    SignatureBlocked(String),
}

impl std::fmt::Display for SamlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SamlError::Parse(m) => write!(f, "saml parse error: {m}"),
            SamlError::Expired(m) => write!(f, "saml assertion expired: {m}"),
            SamlError::Replay(m) => write!(f, "saml assertion replay: {m}"),
            SamlError::Signature(m) => write!(f, "saml signature invalid: {m}"),
            SamlError::SignatureBlocked(m) => write!(f, "saml signature unverifiable: {m}"),
        }
    }
}

/// Parse SAML 2.0 IdP metadata XML, extracting entityID, the HTTP-Redirect/POST
/// SingleSignOnService location, and any signing X509 certificates.
pub fn parse_metadata(xml: &str) -> Result<SamlMetadata, SamlError> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);
    let mut out = SamlMetadata::default();
    let mut in_signing_key = false;
    let mut in_cert = false;
    let mut cert_buf = String::new();
    let mut sso_redirect = String::new();
    let mut sso_post = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                let name = local_name(e.name().as_ref());
                match name.as_str() {
                    "EntityDescriptor" => {
                        if let Some(v) = attr_value(&e, "entityID") {
                            out.entity_id = v;
                        }
                    }
                    "KeyDescriptor" => {
                        // use="signing" or unspecified (then usable for signing).
                        in_signing_key = match attr_value(&e, "use") {
                            Some(u) => u.eq_ignore_ascii_case("signing"),
                            None => true,
                        };
                    }
                    "X509Certificate" => {
                        in_cert = in_signing_key;
                        cert_buf.clear();
                    }
                    "SingleSignOnService" => {
                        let binding = attr_value(&e, "Binding").unwrap_or_default();
                        let location = attr_value(&e, "Location").unwrap_or_default();
                        if binding.contains("HTTP-Redirect") {
                            sso_redirect = location;
                        } else if binding.contains("HTTP-POST") {
                            sso_post = location;
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(t)) => {
                if in_cert {
                    cert_buf.push_str(&t.unescape().unwrap_or_default());
                }
            }
            Ok(Event::End(e)) => {
                let name = local_name(e.name().as_ref());
                match name.as_str() {
                    "X509Certificate" => {
                        if in_cert {
                            let clean: String =
                                cert_buf.chars().filter(|c| !c.is_whitespace()).collect();
                            if !clean.is_empty() {
                                out.signing_certs_b64.push(clean);
                            }
                        }
                        in_cert = false;
                    }
                    "KeyDescriptor" => in_signing_key = false,
                    _ => {}
                }
            }
            Err(e) => return Err(SamlError::Parse(format!("metadata xml: {e}"))),
            _ => {}
        }
    }
    out.sso_url = if !sso_redirect.is_empty() {
        sso_redirect
    } else {
        sso_post
    };
    if out.entity_id.is_empty() {
        return Err(SamlError::Parse("metadata missing entityID".into()));
    }
    Ok(out)
}

/// Build a base64(DEFLATE) SAML AuthnRequest body for the HTTP-Redirect binding,
/// plus its request id. The caller composes the redirect URL with the SSO
/// endpoint and (optionally) a Signature query parameter.
pub fn build_authn_request(
    issuer_entity_id: &str,
    sso_url: &str,
    acs_url: &str,
) -> Result<(String, String), SamlError> {
    let request_id = format!("_{}", uuid::Uuid::new_v4().simple());
    let instant = Utc::now().to_rfc3339();
    let xml = format!(
        "<samlp:AuthnRequest xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" \
xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" ID=\"{id}\" Version=\"2.0\" \
IssueInstant=\"{instant}\" Destination=\"{dest}\" \
ProtocolBinding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\" \
AssertionConsumerServiceURL=\"{acs}\">\
<saml:Issuer>{issuer}</saml:Issuer>\
<samlp:NameIDPolicy Format=\"urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress\" \
AllowCreate=\"true\"/></samlp:AuthnRequest>",
        id = request_id,
        instant = instant,
        dest = xml_escape(sso_url),
        acs = xml_escape(acs_url),
        issuer = xml_escape(issuer_entity_id),
    );
    // HTTP-Redirect binding: raw DEFLATE (no zlib header) then base64.
    let mut encoder =
        flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(xml.as_bytes())
        .map_err(|e| SamlError::Parse(format!("deflate failed: {e}")))?;
    let compressed = encoder
        .finish()
        .map_err(|e| SamlError::Parse(format!("deflate finish failed: {e}")))?;
    use base64::Engine as _;
    let saml_request = base64::engine::general_purpose::STANDARD.encode(compressed);
    Ok((saml_request, request_id))
}

/// Validate a base64-encoded SAMLResponse (HTTP-POST binding) against the
/// provider's signing certs and clock-skew tolerance.
///
/// Returns the parsed assertion on success. The signature is verified with
/// `verify_signature`; on any failure this returns `Err` (fail-closed). Replay
/// detection is the caller's job (it owns the durable replay cache) using the
/// returned `assertion_id` + `not_on_or_after_unix`.
pub fn validate_response(
    saml_response_b64: &str,
    signing_certs_b64: &[String],
    expected_audience: &str,
    clock_skew_secs: i64,
    now_unix: i64,
) -> Result<(SamlAssertion, bool), SamlError> {
    use base64::Engine as _;
    let xml_bytes = base64::engine::general_purpose::STANDARD
        .decode(saml_response_b64.trim())
        .map_err(|e| SamlError::Parse(format!("base64 decode: {e}")))?;
    let xml = String::from_utf8(xml_bytes).map_err(|e| SamlError::Parse(format!("utf8: {e}")))?;

    let assertion = parse_assertion(&xml)?;

    // Clock-skew window checks (J2.2 / J3).
    if assertion.not_before_unix > 0 && now_unix + clock_skew_secs < assertion.not_before_unix {
        return Err(SamlError::Expired(format!(
            "assertion not yet valid (notBefore={}, now={})",
            assertion.not_before_unix, now_unix
        )));
    }
    if assertion.not_on_or_after_unix > 0
        && now_unix - clock_skew_secs >= assertion.not_on_or_after_unix
    {
        return Err(SamlError::Expired(format!(
            "assertion expired (notOnOrAfter={}, now={})",
            assertion.not_on_or_after_unix, now_unix
        )));
    }
    if !expected_audience.is_empty()
        && !assertion.audience.is_empty()
        && assertion.audience != expected_audience
    {
        return Err(SamlError::Parse(format!(
            "audience mismatch: assertion={}, expected={}",
            assertion.audience, expected_audience
        )));
    }

    // Signature verification (fail-closed). No certs → cannot verify → reject.
    if signing_certs_b64.is_empty() {
        return Err(SamlError::Signature(
            "no IdP signing certificate configured; cannot verify assertion signature".into(),
        ));
    }
    let verified = verify_signature(&xml, signing_certs_b64)?;
    if !verified {
        return Err(SamlError::Signature(
            "assertion signature did not match any configured IdP certificate".into(),
        ));
    }
    Ok((assertion, verified))
}

/// Parse a SAMLResponse's first <Assertion>: ID, Issuer, NameID, Conditions
/// (NotBefore/NotOnOrAfter/Audience), AuthnContextClassRef, and AttributeStatement.
fn parse_assertion(xml: &str) -> Result<SamlAssertion, SamlError> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);
    let mut a = SamlAssertion::default();

    let mut path: Vec<String> = Vec::new();
    let mut cur_attr_name = String::new();
    let mut collecting_value = false;
    let mut value_buf = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                let name = local_name(e.name().as_ref());
                path.push(name.clone());
                match name.as_str() {
                    "Assertion" => {
                        if a.assertion_id.is_empty() {
                            if let Some(v) = attr_value(&e, "ID") {
                                a.assertion_id = v;
                            }
                        }
                    }
                    "Conditions" => {
                        if let Some(v) = attr_value(&e, "NotBefore") {
                            a.not_before_unix = parse_instant(&v);
                        }
                        if let Some(v) = attr_value(&e, "NotOnOrAfter") {
                            a.not_on_or_after_unix = parse_instant(&v);
                        }
                    }
                    "Attribute" => {
                        cur_attr_name = attr_value(&e, "Name")
                            .or_else(|| attr_value(&e, "FriendlyName"))
                            .unwrap_or_default();
                    }
                    "AttributeValue" => {
                        collecting_value = true;
                        value_buf.clear();
                    }
                    "AuthnContextClassRef" => {
                        collecting_value = true;
                        value_buf.clear();
                    }
                    "NameID" | "Audience" | "Issuer" => {
                        collecting_value = true;
                        value_buf.clear();
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(t)) => {
                if collecting_value {
                    value_buf.push_str(&t.unescape().unwrap_or_default());
                }
            }
            Ok(Event::End(e)) => {
                let name = local_name(e.name().as_ref());
                match name.as_str() {
                    "NameID" => {
                        if a.name_id.is_empty() {
                            a.name_id = value_buf.trim().to_string();
                        }
                        collecting_value = false;
                    }
                    "Issuer" => {
                        if a.issuer.is_empty() {
                            a.issuer = value_buf.trim().to_string();
                        }
                        collecting_value = false;
                    }
                    "Audience" => {
                        if a.audience.is_empty() {
                            a.audience = value_buf.trim().to_string();
                        }
                        collecting_value = false;
                    }
                    "AuthnContextClassRef" => {
                        if a.authn_context.is_empty() {
                            a.authn_context = value_buf.trim().to_string();
                        }
                        collecting_value = false;
                    }
                    "AttributeValue" => {
                        if !cur_attr_name.is_empty() {
                            a.attributes
                                .entry(cur_attr_name.clone())
                                .or_default()
                                .push(value_buf.trim().to_string());
                        }
                        collecting_value = false;
                    }
                    "Attribute" => cur_attr_name.clear(),
                    _ => {}
                }
                path.pop();
            }
            Err(e) => return Err(SamlError::Parse(format!("assertion xml: {e}"))),
            _ => {}
        }
    }

    if a.name_id.is_empty() {
        return Err(SamlError::Parse("assertion missing NameID".into()));
    }
    if a.assertion_id.is_empty() {
        return Err(SamlError::Parse("assertion missing ID".into()));
    }
    Ok(a)
}

/// Full XML-DSig verification of a SAMLResponse, applying exclusive XML
/// canonicalization (xml-exc-c14n) so the check cannot be bypassed by XML
/// transforms. Two independent properties are enforced (both must hold):
///
///   1. **Reference digest** — the element named by `<ds:Reference URI="#id">`
///      is located by its `ID` attribute, canonicalized with exclusive C14N
///      (honoring `InclusiveNamespaces/@PrefixList`), hashed with the
///      `<ds:DigestMethod>` algorithm, and compared to `<ds:DigestValue>`. This
///      binds the signature to the actual asserted content.
///   2. **Signature** — `<ds:SignedInfo>` is canonicalized with exclusive C14N
///      and `<ds:SignatureValue>` is RSA-PKCS1v15-verified over those octets
///      with each configured cert's public key (SHA-256 or SHA-1 per
///      `<ds:SignatureMethod>`).
///
/// Returns `Ok(true)` only when BOTH pass for some configured cert; otherwise
/// `Ok(false)` / `Err` (fail-closed). It never returns true without a real
/// cryptographic check on the canonical bytes.
fn verify_signature(xml: &str, signing_certs_b64: &[String]) -> Result<bool, SamlError> {
    use base64::Engine as _;

    // ── 1. Reference digest over the canonicalized signed element ────────────
    // Pull the Reference URI ("#id"), the digest algorithm, the expected digest,
    // and any inclusive-namespace PrefixList from inside <ds:SignedInfo>.
    let signed_info = extract_element(xml, "SignedInfo")
        .ok_or_else(|| SamlError::Signature("response has no <ds:SignedInfo> to verify".into()))?;
    let reference_uri = attr_of_element(&signed_info, "Reference", "URI").unwrap_or_default();
    let digest_value_b64 = extract_text(&signed_info, "DigestValue")
        .ok_or_else(|| SamlError::Signature("response has no <ds:DigestValue>".into()))?;
    let expected_digest = base64::engine::general_purpose::STANDARD
        .decode(
            digest_value_b64
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect::<String>(),
        )
        .map_err(|e| SamlError::Signature(format!("digest value base64: {e}")))?;
    let digest_sha256 = {
        // DigestMethod algorithm: ...#sha256 vs ...#sha1.
        let dm = extract_element(&signed_info, "DigestMethod").unwrap_or_default();
        !dm.contains("#sha1")
    };
    let prefix_list = extract_prefix_list(&signed_info);

    // Locate the referenced element. The URI is "#<ID>"; an empty URI means the
    // whole document (we then canonicalize the root Response element).
    let ref_id = reference_uri.trim_start_matches('#').to_string();
    let signed_local = if ref_id.is_empty() {
        // Empty same-document reference → the document element.
        "Response".to_string()
    } else {
        find_local_name_by_id(xml, &ref_id).ok_or_else(|| {
            SamlError::Signature(format!(
                "signed reference URI {reference_uri} does not match any element ID"
            ))
        })?
    };
    let canon_ref = if ref_id.is_empty() {
        c14n::canonicalize_element(xml, &signed_local, &prefix_list)
    } else {
        c14n::canonicalize_element_by_id(xml, &ref_id, &prefix_list)
    }
    .ok_or_else(|| SamlError::Signature("failed to canonicalize signed element".into()))?;
    let actual_digest = if digest_sha256 {
        use sha2::{Digest, Sha256};
        Sha256::digest(&canon_ref).to_vec()
    } else {
        use sha1::{Digest, Sha1};
        Sha1::digest(&canon_ref).to_vec()
    };
    if actual_digest != expected_digest {
        // The signed content's canonical digest does not match the signature's
        // DigestValue → the document was transformed/mutated. Fail closed.
        return Ok(false);
    }

    // ── 2. RSA verify over the canonicalized SignedInfo octets ───────────────
    let canon_signed_info = c14n::canonicalize_element(xml, "SignedInfo", &prefix_list)
        .ok_or_else(|| SamlError::Signature("failed to canonicalize <ds:SignedInfo>".into()))?;
    let sig_value_b64 = extract_text(xml, "SignatureValue")
        .ok_or_else(|| SamlError::Signature("response has no <ds:SignatureValue>".into()))?;
    let signature = base64::engine::general_purpose::STANDARD
        .decode(
            sig_value_b64
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect::<String>(),
        )
        .map_err(|e| SamlError::Signature(format!("signature value base64: {e}")))?;

    // Digest algorithm from SignatureMethod (rsa-sha256 vs rsa-sha1).
    let use_sha256 = !signed_info.contains("rsa-sha1");
    let signed_info = String::from_utf8_lossy(&canon_signed_info).to_string();

    for cert_b64 in signing_certs_b64 {
        let der = match base64::engine::general_purpose::STANDARD.decode(
            cert_b64
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect::<String>(),
        ) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let Ok((_, cert)) = x509_parser::parse_x509_certificate(&der) else {
            continue;
        };
        let spki_der = cert.public_key().raw;
        // Parse the RSA public key from the SubjectPublicKeyInfo. Try the full
        // SPKI DER first, then fall back to a bare PKCS#1 RSAPublicKey carried in
        // the BIT STRING. The two decoders have different error types, so resolve
        // each to Option before combining.
        use rsa::pkcs1::DecodeRsaPublicKey;
        use rsa::pkcs8::DecodePublicKey;
        let public_key = rsa::RsaPublicKey::from_public_key_der(spki_der)
            .ok()
            .or_else(|| {
                rsa::RsaPublicKey::from_pkcs1_der(
                    cert.public_key().subject_public_key.data.as_ref(),
                )
                .ok()
            });
        let Some(public_key) = public_key else {
            continue;
        };

        // PKCS#1 v1.5 verification via `new_unprefixed` + a manually-prepended
        // DigestInfo (RFC 8017 §9.2). We avoid `Pkcs1v15Sign::new::<D>()` because
        // it requires `D: const_oid::AssociatedOid`, which is unsatisfiable here:
        // the tree carries two incompatible `const-oid` majors — `0.9.6` (the
        // `rsa 0.9` / `digest 0.10` stack) and `0.10.2` (the `digest 0.11` stack
        // pulled by aws-sdk-s3 / postgres-protocol) — so `sha2`'s `oid` impl and
        // `rsa`'s bound resolve to different `AssociatedOid` traits. Prepending
        // the fixed DigestInfo prefix ourselves is version-independent.
        let ok = if use_sha256 {
            use sha2::{Digest, Sha256};
            // DigestInfo prefix for SHA-256 (RFC 8017 §9.2 note 1).
            const SHA256_DIGEST_INFO_PREFIX: [u8; 19] = [
                0x30, 0x31, 0x30, 0x0d, 0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02,
                0x01, 0x05, 0x00, 0x04, 0x20,
            ];
            let mut digest_info = SHA256_DIGEST_INFO_PREFIX.to_vec();
            digest_info.extend_from_slice(&Sha256::digest(signed_info.as_bytes()));
            public_key
                .verify(
                    rsa::Pkcs1v15Sign::new_unprefixed(),
                    &digest_info,
                    &signature,
                )
                .is_ok()
        } else {
            use sha1::{Digest, Sha1};
            // DigestInfo prefix for SHA-1 (RFC 8017 §9.2 note 1).
            const SHA1_DIGEST_INFO_PREFIX: [u8; 15] = [
                0x30, 0x21, 0x30, 0x09, 0x06, 0x05, 0x2b, 0x0e, 0x03, 0x02, 0x1a, 0x05, 0x00, 0x04,
                0x14,
            ];
            let mut digest_info = SHA1_DIGEST_INFO_PREFIX.to_vec();
            digest_info.extend_from_slice(&Sha1::digest(signed_info.as_bytes()));
            public_key
                .verify(
                    rsa::Pkcs1v15Sign::new_unprefixed(),
                    &digest_info,
                    &signature,
                )
                .is_ok()
        };
        if ok {
            return Ok(true);
        }
    }
    Ok(false)
}

// ── dev-only self-asserted IdP (bug_report.md #12-13) ────────────────────────
//
// Mirrors the WebAuthn soft-authenticator decision: the production verifier above
// is untouched and stays fail-closed; this path only lets a headless conformance
// harness (which has no external IdP) exercise `SamlAcs`/`ResolveExternalIdentity`
// end-to-end. When `UDB_SAML_TEST_MODE` is on, the caller mints a SAMLResponse here
// with a dev IdP keypair and a REAL enveloped XML-DSig (exclusive-C14N reference
// digest + RSA-PKCS1v15-SHA256 over the canonical SignedInfo) that the same
// `validate_response`/`verify_signature` path checks cryptographically — never an
// accept-any bypass. A production broker never sets the flag, so the dev IdP cannot
// authenticate anyone (and even if it ran, its self-signed cert is not any real
// provider's configured signing cert).

/// Sentinel a test harness sends as the `saml_response` (optionally suffixed with
/// `:<name_id>`) to request a dev self-asserted login.
pub const DEV_SENTINEL: &str = "__UDB_SAML_TEST__";

/// Dev IdP signing keypair + self-signed cert (RSA-2048, generated once with
/// openssl). The key only ever signs assertions consumed by THIS broker in test
/// mode; it is inert unless `dev_self_assert` is called behind the runtime gate.
const DEV_IDP_KEY_PKCS8_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC1ghN2ucF/7n9B\n\
UhQ7Ozcq8jU/ngwXeHp6umvGRjadfqOaA5MOoy6Zc1mFLHTOiJw58pHM+oFB7BRo\n\
OKIM/FcTdGhDg6JX22SxXhRex7MYjjelStd0JRKjzzJs4lV7wrXUhCoDCeFftGSx\n\
jzxLDc/dksxhjBGHXljcbvLDUl1ZezB5oLILVfVI7pRfWl2YwENoxHhdCJurMM5a\n\
2a/a7hROihGlNKgPijSnTadzW9EPgggrCOVb9pEGfWhZnJmHWrV39jP4SdHodM2D\n\
rUb3u7Y+9ztJ6DCRfnjAK/Fap0Rvppm8G0hZU7nhmJ5eK/v6MgS2a0ZIYs8YO+jr\n\
td9jODSpAgMBAAECggEAC1rr5M2SMXK2O1vrMBlwRhuJAUXd88nxv6PSAkF6QTge\n\
/A+lL5E95RO2UgKJ/DHHtEhcnro9Q+aFEFAasz1GJU1vCGo/ycdL8Vy1YYiUx8B9\n\
8rVP7VA0blMUEIPIXUm9HmJ2TmJb2yTp98HCP9/JVU9NwfyFTDa20HOQdG++r05O\n\
rT82pznMYbNjbEJQsI49hcIYOadXo4u9Pn7N0Rbsc303MybHZacWWpfAHl7PTBrn\n\
NINP8UUnYWcmETiaQ0wzJAVLofoYwMkVEiKEwDsFOSuVzCdgU6YHV25MHECotCZd\n\
KK1TCoBzuv6PEBIa5dzm0phLOnytPitRPpN4fNp6AQKBgQDY/gkWu7vaANAPor7O\n\
D7S7mK8ubEnS0f13MlzRTSXR7R+zY2G+gTyyHumAWzN7mSXy+rYCdIxbVy7BieSG\n\
5zG32eS6Npxa3OIETHfnv9wT0UmqVZG3GVruRHX84+Pt5SNa0jVtGSFrB0PMY4gu\n\
xhJEE3gM77SYtjbG7cSdsJ79+QKBgQDWIw9+76rdX5xTTptAbuMwpeX2Cb90ADOK\n\
d+g+t3H/S5UoiWFdKn62IJxtpJZfRUUl9XlSAlHxV3wj6oanPJp6O2Mq4Qn0PijG\n\
BIicPKIajJjmoxgBZcJy6kAZRMOHMWhwrM1l4FJjHh8+dXJqDWyico11nR6emm4R\n\
I9xV6FxYMQKBgBAoWmTm9cX16YhAhhSx9rNBW0oJpjWcjVMi3OZ46CgJkCK7c7vL\n\
w8k/pAN6xwqdDMZbBNKJ+ymSBFlE+09QR9N41h9HkbzyVaIcT5FiJ/ER1HpqhL8t\n\
lCfJ0T9TeNVuCoPowzGsfWCK2fGON8XD2fhXusi70KbOaqXFbq6PSEeBAoGBAM/9\n\
rgd1c1kijQy9xT6IdlPCT+LzBOr/ZxCP9x0zwZ5fI7oD9nYv2HO+qTI2M3jGJ6v/\n\
CqAFcOIiP4oDOlcmHkWreV8kxi5eUexEawyWOD3hYoJi1+ZDmONVdH0WtXSTIQaQ\n\
UdEqWdu8Xkykd0VbVLFU4uHiguM6zL4JPvKSh1+BAoGAYOniCR6oqYAPE+pjupJf\n\
ldU8QuNu1OA0r2vHFm94vegzivfEWZXga6AxNlnQozWVVE8fJL30WDLD6G+UVUPK\n\
KLhovYU9JhQ7ZbrQUOf7zrx243CdlDgFxw3rKZQgilG15Jtdj/zd389aeDNCh0b3\n\
3bfpBH7A3LezGNZXuzs66XA=\n\
-----END PRIVATE KEY-----\n";

/// Self-signed cert (base64 DER) matching [`DEV_IDP_KEY_PKCS8_PEM`].
const DEV_IDP_CERT_DER_B64: &str = "MIIDEzCCAfugAwIBAgIUbx65hlvb7IxFZfBkI6sCI2bVY4swDQYJKoZIhvcNAQELBQAwGDEWMBQGA1UEAwwNc2FtbC10ZXN0LWlkcDAgFw0yNjA2MDkxNzAwMTBaGA8yMTI2MDUxNjE3MDAxMFowGDEWMBQGA1UEAwwNc2FtbC10ZXN0LWlkcDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALWCE3a5wX/uf0FSFDs7NyryNT+eDBd4enq6a8ZGNp1+o5oDkw6jLplzWYUsdM6InDnykcz6gUHsFGg4ogz8VxN0aEODolfbZLFeFF7HsxiON6VK13QlEqPPMmziVXvCtdSEKgMJ4V+0ZLGPPEsNz92SzGGMEYdeWNxu8sNSXVl7MHmgsgtV9UjulF9aXZjAQ2jEeF0Im6swzlrZr9ruFE6KEaU0qA+KNKdNp3Nb0Q+CCCsI5Vv2kQZ9aFmcmYdatXf2M/hJ0eh0zYOtRve7tj73O0noMJF+eMAr8VqnRG+mmbwbSFlTueGYnl4r+/oyBLZrRkhizxg76Ou132M4NKkCAwEAAaNTMFEwHQYDVR0OBBYEFMX5JzOA3JOYu33vV95SFBRGLyD8MB8GA1UdIwQYMBaAFMX5JzOA3JOYu33vV95SFBRGLyD8MA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAGkBjr+d8sYYZtg5NYrcQLTV2vqYn8/FTwXLfr7W+QtjKSfe1oWHssaDowMIb4TV9Lzb8tAOLzqXhVglBflY5LV8SZ7GlM11nMmwCX1PS5gYWENW38TZV7zwOOJTHGgmUm/VL7JIxF5BVp6zQLoT3x7g8yBYVbpVS8LSBuPyQNjLdBHY364B+eIm+UW/ke+7jCxqE46NDGEyoZp2MNBmRrF5BGmyVdjS4+hdPiX9Sc+FtrTeX0PK6QbVAjXLHZlu4lrGQRCoYkYHQxImVtj88dk3EhlsHgJtDOIfBJDtOudkLKcY+wgeaTaKNHU21p954mhk+q5nuZ8kCzqVmzkI4fU=";

/// Whether the dev self-asserted SAML path is enabled (resolved once).
/// Fail-closed default: production never sets `UDB_SAML_TEST_MODE`.
pub fn dev_test_mode_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("UDB_SAML_TEST_MODE")
            .map(|v| {
                let v = v.trim();
                v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes")
            })
            .unwrap_or(false)
    })
}

/// Sign a SAMLResponse that contains a `__SIG__` placeholder immediately after the
/// Assertion's `<saml:Issuer>`, producing an enveloped XML-DSig exactly as
/// [`verify_signature`] expects: an exclusive-C14N reference digest over the
/// (signature-stripped) Assertion and an RSA-PKCS1v15-SHA256 signature over the
/// canonical `<ds:SignedInfo>`. Returns the response with the placeholder replaced.
fn sign_enveloped_response(
    response_with_placeholder: &str,
    assertion_id: &str,
    key_pkcs8_pem: &str,
) -> Result<String, SamlError> {
    use base64::Engine as _;
    use rsa::pkcs8::DecodePrivateKey;
    use sha2::{Digest, Sha256};

    // 1. Canonicalize the Assertion (enveloped signature stripped) and digest it.
    let no_sig = response_with_placeholder.replace("__SIG__", "");
    let canon_assertion = c14n::canonicalize_element_by_id(&no_sig, assertion_id, &[])
        .ok_or_else(|| SamlError::Signature("dev: failed to canonicalize assertion".into()))?;
    let digest_b64 =
        base64::engine::general_purpose::STANDARD.encode(Sha256::digest(&canon_assertion));

    // 2. SignedInfo referencing the Assertion with that digest.
    let signed_info = format!(
        r##"<ds:SignedInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#"><ds:CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"></ds:CanonicalizationMethod><ds:SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"></ds:SignatureMethod><ds:Reference URI="#{aid}"><ds:Transforms><ds:Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"></ds:Transform><ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"></ds:Transform></ds:Transforms><ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"></ds:DigestMethod><ds:DigestValue>{dv}</ds:DigestValue></ds:Reference></ds:SignedInfo>"##,
        aid = assertion_id,
        dv = digest_b64,
    );

    // 3. Canonicalize SignedInfo and RSA-PKCS1v15 sign over it (SHA-256), matching
    //    verify_signature's unprefixed-DigestInfo construction.
    let wrapped =
        format!(r#"<wrap xmlns:ds="http://www.w3.org/2000/09/xmldsig#">{signed_info}</wrap>"#);
    let canon_si = c14n::canonicalize_element(&wrapped, "SignedInfo", &[])
        .ok_or_else(|| SamlError::Signature("dev: failed to canonicalize SignedInfo".into()))?;
    const SHA256_DIGEST_INFO_PREFIX: [u8; 19] = [
        0x30, 0x31, 0x30, 0x0d, 0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x01,
        0x05, 0x00, 0x04, 0x20,
    ];
    let mut digest_info = SHA256_DIGEST_INFO_PREFIX.to_vec();
    digest_info.extend_from_slice(&Sha256::digest(&canon_si));
    let key = rsa::RsaPrivateKey::from_pkcs8_pem(key_pkcs8_pem)
        .map_err(|e| SamlError::Signature(format!("dev: load signing key: {e}")))?;
    let sig = key
        .sign(rsa::Pkcs1v15Sign::new_unprefixed(), &digest_info)
        .map_err(|e| SamlError::Signature(format!("dev: sign: {e}")))?;
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig);

    // 4. Assemble the enveloped <ds:Signature> and splice it in.
    let signature = format!(
        r#"<ds:Signature xmlns:ds="http://www.w3.org/2000/09/xmldsig#">{signed_info}<ds:SignatureValue>{sig}</ds:SignatureValue></ds:Signature>"#,
        sig = sig_b64,
    );
    Ok(response_with_placeholder.replace("__SIG__", &signature))
}

/// Build a dev, self-asserted, validly-signed base64 SAMLResponse for `name_id`.
/// Returns `(saml_response_b64, signing_cert_der_b64)`; the caller feeds both to
/// [`validate_response`] so the real signature path verifies it. Window is ±5 min.
pub fn dev_self_assert(
    issuer: &str,
    name_id: &str,
    audience: &str,
    attributes: &BTreeMap<String, Vec<String>>,
) -> Result<(String, String), SamlError> {
    use base64::Engine as _;
    let assertion_id = format!("_{}", uuid::Uuid::new_v4().simple());
    let now = Utc::now();
    let not_before = (now - chrono::Duration::minutes(5)).to_rfc3339();
    let not_after = (now + chrono::Duration::minutes(5)).to_rfc3339();

    // Always carry an `email` attribute (NameID) so claim mapping has something to
    // resolve even when the caller passes no attributes.
    let mut attrs = attributes.clone();
    attrs
        .entry("email".to_string())
        .or_insert_with(|| vec![name_id.to_string()]);
    let mut attr_xml = String::new();
    if !attrs.is_empty() {
        attr_xml.push_str("<saml:AttributeStatement>");
        for (name, values) in &attrs {
            attr_xml.push_str(&format!(r#"<saml:Attribute Name="{}">"#, xml_escape(name)));
            for v in values {
                attr_xml.push_str(&format!(
                    "<saml:AttributeValue>{}</saml:AttributeValue>",
                    xml_escape(v)
                ));
            }
            attr_xml.push_str("</saml:Attribute>");
        }
        attr_xml.push_str("</saml:AttributeStatement>");
    }

    let response = format!(
        r#"<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"><saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" ID="{aid}"><saml:Issuer>{issuer}</saml:Issuer>__SIG__<saml:Subject><saml:NameID>{nid}</saml:NameID></saml:Subject><saml:Conditions NotBefore="{nb}" NotOnOrAfter="{na}"><saml:AudienceRestriction><saml:Audience>{aud}</saml:Audience></saml:AudienceRestriction></saml:Conditions><saml:AuthnStatement><saml:AuthnContext><saml:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:PasswordProtectedTransport</saml:AuthnContextClassRef></saml:AuthnContext></saml:AuthnStatement>{attrs}</saml:Assertion></samlp:Response>"#,
        aid = assertion_id,
        issuer = xml_escape(issuer),
        nid = xml_escape(name_id),
        nb = not_before,
        na = not_after,
        aud = xml_escape(audience),
        attrs = attr_xml,
    );

    let signed = sign_enveloped_response(&response, &assertion_id, DEV_IDP_KEY_PKCS8_PEM)?;
    let response_b64 = base64::engine::general_purpose::STANDARD.encode(signed.as_bytes());
    Ok((response_b64, DEV_IDP_CERT_DER_B64.to_string()))
}

// ── small XML helpers (namespace-agnostic, local-name based) ─────────────────

fn local_name(qname: &[u8]) -> String {
    let s = String::from_utf8_lossy(qname);
    match s.rsplit_once(':') {
        Some((_, local)) => local.to_string(),
        None => s.to_string(),
    }
}

fn attr_value(e: &quick_xml::events::BytesStart<'_>, key: &str) -> Option<String> {
    for attr in e.attributes().flatten() {
        let k = local_name(attr.key.as_ref());
        if k == key {
            return Some(String::from_utf8_lossy(&attr.value).to_string());
        }
    }
    None
}

/// Read an attribute (by local name) off the opening tag of the first element
/// with the given local name within `xml`. Works for both regular and
/// self-closing elements (e.g. `<ds:Reference URI=…>` and the usually
/// self-closing `<ec:InclusiveNamespaces PrefixList=…/>`).
fn attr_of_element(xml: &str, element_local: &str, attr_local: &str) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    reader.check_end_names(false);
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                if local_name(e.name().as_ref()) == element_local {
                    if let Some(v) = attr_value(&e, attr_local) {
                        return Some(v);
                    }
                }
            }
            Ok(Event::Eof) => return None,
            Err(_) => return None,
            _ => {}
        }
    }
}

/// Extract the `InclusiveNamespaces/@PrefixList` (whitespace-separated prefixes)
/// from inside `<ds:SignedInfo>`, if the exclusive-C14N transform declares one.
/// Returns an empty vec when absent (plain exclusive C14N, no forced prefixes).
fn extract_prefix_list(signed_info: &str) -> Vec<String> {
    match attr_of_element(signed_info, "InclusiveNamespaces", "PrefixList") {
        Some(list) => list
            .split_whitespace()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        None => Vec::new(),
    }
}

/// Find the local name of the element whose `ID`/`Id`/`id` attribute equals
/// `id`. Used to resolve an XML-DSig `<ds:Reference URI="#id">` to the element
/// it signs (e.g. the `<saml:Assertion>`).
fn find_local_name_by_id(xml: &str, id: &str) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    reader.check_end_names(false);
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                for attr in e.attributes().flatten() {
                    let k = local_name(attr.key.as_ref());
                    if (k == "ID" || k == "Id" || k == "id")
                        && String::from_utf8_lossy(&attr.value) == id
                    {
                        return Some(local_name(e.name().as_ref()));
                    }
                }
            }
            Ok(Event::Eof) => return None,
            Err(_) => return None,
            _ => {}
        }
    }
}

/// Extract the raw outer text of the first element with the given local name
/// (used for SignedInfo octets). Returns the substring spanning the opening
/// `<...local...>` through its matching `</...local>`. Assumes no nested element
/// of the same local name (true for SignedInfo/SignatureValue in XML-DSig).
fn extract_element(xml: &str, local: &str) -> Option<String> {
    let mut search_from = 0usize;
    loop {
        let rel = xml.get(search_from..)?.find('<')?;
        let start = search_from + rel;
        let after_lt = xml.get(start + 1..)?;
        // Skip closing tags, declarations, and processing instructions.
        if after_lt.starts_with('/') || after_lt.starts_with('?') || after_lt.starts_with('!') {
            search_from = start + 1;
            continue;
        }
        let name_end = after_lt
            .find(|c: char| c.is_whitespace() || c == '>' || c == '/')
            .unwrap_or(after_lt.len());
        let qname = &after_lt[..name_end];
        let lname = qname.rsplit_once(':').map(|(_, l)| l).unwrap_or(qname);
        if lname == local {
            // Find the matching "</...local>" after the opening tag.
            let mut idx = start;
            loop {
                let crel = xml.get(idx..)?.find("</")?;
                let cpos = idx + crel;
                let cseg = xml.get(cpos + 2..)?;
                let cname_end = cseg
                    .find(|c: char| c.is_whitespace() || c == '>')
                    .unwrap_or(cseg.len());
                let cqname = &cseg[..cname_end];
                let clname = cqname.rsplit_once(':').map(|(_, l)| l).unwrap_or(cqname);
                if clname == local {
                    let end = xml.get(cpos..)?.find('>')? + cpos + 1;
                    return Some(xml.get(start..end)?.to_string());
                }
                idx = cpos + 2;
            }
        }
        search_from = start + 1;
    }
}

/// Extract the text content of the first element with the given local name.
fn extract_text(xml: &str, local: &str) -> Option<String> {
    let el = extract_element(xml, local)?;
    let gt = el.find('>')?;
    let close = el.rfind("</")?;
    if close <= gt {
        return None;
    }
    Some(el.get(gt + 1..close)?.to_string())
}

fn parse_instant(s: &str) -> i64 {
    DateTime::parse_from_rfc3339(s.trim())
        .map(|dt| dt.with_timezone(&Utc).timestamp())
        .unwrap_or(0)
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;

    const METADATA: &str = r#"<?xml version="1.0"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://idp.example.com/meta">
  <md:IDPSSODescriptor>
    <md:KeyDescriptor use="signing">
      <ds:KeyInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
        <ds:X509Data><ds:X509Certificate>MIIBdummycert==</ds:X509Certificate></ds:X509Data>
      </ds:KeyInfo>
    </md:KeyDescriptor>
    <md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.example.com/sso"/>
  </md:IDPSSODescriptor>
</md:EntityDescriptor>"#;

    #[test]
    fn parses_metadata_fields() {
        let m = parse_metadata(METADATA).expect("metadata parse");
        assert_eq!(m.entity_id, "https://idp.example.com/meta");
        assert_eq!(m.sso_url, "https://idp.example.com/sso");
        assert_eq!(m.signing_certs_b64, vec!["MIIBdummycert==".to_string()]);
    }

    #[test]
    fn builds_deflated_authn_request() {
        let (req, id) = build_authn_request(
            "https://sp.example.com",
            "https://idp/sso",
            "https://sp/acs",
        )
        .expect("build");
        assert!(!req.is_empty());
        assert!(id.starts_with('_'));
    }

    fn assertion_xml(id: &str, not_before: &str, not_after: &str) -> String {
        format!(
            r#"<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">
  <saml:Assertion ID="{id}">
    <saml:Issuer>https://idp.example.com/meta</saml:Issuer>
    <saml:Subject><saml:NameID>user@corp.com</saml:NameID></saml:Subject>
    <saml:Conditions NotBefore="{nb}" NotOnOrAfter="{na}">
      <saml:AudienceRestriction><saml:Audience>https://sp.example.com</saml:Audience></saml:AudienceRestriction>
    </saml:Conditions>
    <saml:AuthnStatement><saml:AuthnContext><saml:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:PasswordProtectedTransport</saml:AuthnContextClassRef></saml:AuthnContext></saml:AuthnStatement>
    <saml:AttributeStatement>
      <saml:Attribute Name="groups"><saml:AttributeValue>eng</saml:AttributeValue><saml:AttributeValue>admins</saml:AttributeValue></saml:Attribute>
      <saml:Attribute Name="email"><saml:AttributeValue>user@corp.com</saml:AttributeValue></saml:Attribute>
    </saml:AttributeStatement>
  </saml:Assertion>
</samlp:Response>"#,
            id = id,
            nb = not_before,
            na = not_after
        )
    }

    #[test]
    fn parses_assertion_nameid_attributes_conditions() {
        let xml = assertion_xml("_abc123", "2020-01-01T00:00:00Z", "2099-01-01T00:00:00Z");
        let a = parse_assertion(&xml).expect("assertion parse");
        assert_eq!(a.assertion_id, "_abc123");
        assert_eq!(a.name_id, "user@corp.com");
        assert_eq!(a.issuer, "https://idp.example.com/meta");
        assert_eq!(a.audience, "https://sp.example.com");
        assert_eq!(
            a.attributes.get("groups").unwrap(),
            &vec!["eng".to_string(), "admins".to_string()]
        );
        assert!(a.authn_context.contains("PasswordProtectedTransport"));
        assert!(a.not_on_or_after_unix > a.not_before_unix);
    }

    #[test]
    fn rejects_expired_assertion() {
        let xml = assertion_xml("_old", "2000-01-01T00:00:00Z", "2000-01-02T00:00:00Z");
        let b64 = {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD.encode(xml.as_bytes())
        };
        // now is far in the future → expired.
        let err = validate_response(&b64, &["cert".into()], "", 60, 4102444800).unwrap_err();
        assert!(matches!(err, SamlError::Expired(_)));
    }

    #[test]
    fn fails_closed_without_certs() {
        let xml = assertion_xml("_x", "2020-01-01T00:00:00Z", "2099-01-01T00:00:00Z");
        let b64 = {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD.encode(xml.as_bytes())
        };
        // valid window but NO certs → must fail closed (never trust).
        let err = validate_response(&b64, &[], "", 60, 1577836900).unwrap_err();
        assert!(matches!(err, SamlError::Signature(_)));
    }

    #[test]
    fn extract_element_and_text() {
        let xml = r#"<a><ds:SignedInfo x="1">hello<b/></ds:SignedInfo><ds:SignatureValue>QUJD</ds:SignatureValue></a>"#;
        let si = extract_element(xml, "SignedInfo").unwrap();
        assert!(si.starts_with("<ds:SignedInfo"));
        assert!(si.ends_with("</ds:SignedInfo>"));
        assert_eq!(extract_text(xml, "SignatureValue").unwrap(), "QUJD");
    }

    // ── Real XML-DSig roundtrip with exclusive C14N ──────────────────────────
    // Reuses the module's dev IdP keypair/cert (a fixed RSA-2048 self-signed pair)
    // and the shared `sign_enveloped_response` helper so the test signs exactly the
    // way the runtime dev path does — no second copy of the key or the signer.
    use super::{DEV_IDP_CERT_DER_B64 as TEST_CERT_DER_B64, DEV_IDP_KEY_PKCS8_PEM};

    /// Build a SAMLResponse with an enveloped XML-DSig signature over the Assertion,
    /// signed with the dev IdP key via the shared `sign_enveloped_response` helper.
    fn signed_response(name_id: &str) -> String {
        let assertion_id = "_assert1";
        let response = format!(
            r#"<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"><saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" ID="{aid}"><saml:Issuer>https://idp.example.com/meta</saml:Issuer>__SIG__<saml:Subject><saml:NameID>{nid}</saml:NameID></saml:Subject><saml:Conditions NotBefore="2020-01-01T00:00:00Z" NotOnOrAfter="2099-01-01T00:00:00Z"><saml:AudienceRestriction><saml:Audience>https://sp.example.com</saml:Audience></saml:AudienceRestriction></saml:Conditions></saml:Assertion></samlp:Response>"#,
            aid = assertion_id,
            nid = name_id,
        );
        sign_enveloped_response(&response, assertion_id, DEV_IDP_KEY_PKCS8_PEM)
            .expect("dev sign roundtrip")
    }

    #[test]
    fn verifies_real_signed_assertion_with_c14n() {
        let xml = signed_response("user@corp.com");
        let verified =
            verify_signature(xml.as_str(), &[TEST_CERT_DER_B64.to_string()]).expect("verify ok");
        assert!(verified, "valid canonical signature must verify");
    }

    #[test]
    fn rejects_tampered_assertion_after_signing() {
        // Sign for one NameID, then mutate the document's NameID. The reference
        // digest over the canonical (mutated) Assertion no longer matches the
        // signed DigestValue → must fail. This is the transform-bypass closure.
        let xml = signed_response("user@corp.com");
        let tampered = xml.replace("user@corp.com", "attacker@evil.com");
        let verified = verify_signature(tampered.as_str(), &[TEST_CERT_DER_B64.to_string()])
            .expect("verify runs");
        assert!(!verified, "tampered assertion must NOT verify");
    }

    #[test]
    fn rejects_comment_injection() {
        // Injecting an XML comment into the signed Assertion must not change the
        // canonical digest in a way that lets a different logical value through;
        // here we inject a comment that splits the NameID text. Exclusive C14N
        // drops comments, so "us<!--x-->er" canonicalizes to "user" — the digest
        // still matches and the ORIGINAL value stands (no privilege change). The
        // security property under test: a comment cannot smuggle a *different*
        // effective NameID past the digest. We assert the signed value wins.
        let xml = signed_response("user@corp.com");
        // Inject a comment inside the NameID of the signed document.
        let injected = xml.replace(
            "<saml:NameID>user@corp.com</saml:NameID>",
            "<saml:NameID>user@corp.com<!-- attacker@evil.com --></saml:NameID>",
        );
        // Signature still verifies (comment is canonically irrelevant)...
        let verified = verify_signature(injected.as_str(), &[TEST_CERT_DER_B64.to_string()])
            .expect("verify runs");
        assert!(verified, "comment is c14n-irrelevant, signature holds");
        // ...and the parsed NameID is still the SIGNED value, not the comment.
        let parsed = parse_assertion(&injected).expect("parse");
        assert_eq!(parsed.name_id, "user@corp.com");
    }

    #[test]
    fn dev_self_assert_roundtrips_through_validate() {
        // #12-13: the dev IdP mints a real signed assertion that the production
        // validate_response/verify_signature path accepts cryptographically.
        let (resp_b64, cert) = dev_self_assert(
            "urn:udb:dev-idp",
            "dev-user@udb.local",
            "https://sp.example.com",
            &BTreeMap::new(),
        )
        .expect("dev self-assert");
        let (assertion, verified) = validate_response(
            &resp_b64,
            &[cert],
            "https://sp.example.com",
            300,
            Utc::now().timestamp(),
        )
        .expect("validate dev assertion");
        assert!(verified, "dev assertion must verify cryptographically");
        assert_eq!(assertion.name_id, "dev-user@udb.local");
        assert_eq!(assertion.audience, "https://sp.example.com");
        assert_eq!(
            assertion.attributes.get("email").map(|v| v.as_slice()),
            Some(["dev-user@udb.local".to_string()].as_slice())
        );
    }
}
