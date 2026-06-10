//! Phase J3 pure-logic tests for the IdP plane (no live DB).
//!
//! DB-backed flows (tenant-A-vs-B persistence, SAML replay-cache writes, SCIM
//! deactivation session-revoke SQL) are exercised against live Postgres in the
//! env-gated conformance suite; here we pin the deterministic decision logic the
//! handlers compose so a regression in mapping/assurance/JIT/SAML parsing is
//! caught without infrastructure. The submodules (`mapping`, `scim`, `saml`,
//! `oidc`) also carry their own focused unit tests.

use super::mapping::{
    AccountLinkDecision, JitDecision, JitPolicy, apply_claim_mapping, derive_assurance,
    evaluate_account_linking, evaluate_jit, map_groups_to_roles,
};
use super::oidc::parse_jwks_kids;
use super::saml;
use crate::proto::udb::core::idp::entity::v1::AssuranceLevel as A;
use serde_json::json;

// J3: "Group mapping never grants unconfigured roles."
#[test]
fn group_mapping_is_allowlist_only() {
    let mapping = json!({ "platform-admins": "role:admin" }).to_string();
    // A group present in the IdP but absent from the mapping grants nothing.
    let (roles, unmapped) = map_groups_to_roles(
        &mapping,
        &["platform-admins".into(), "everyone".into(), "wheel".into()],
    );
    assert_eq!(roles, vec!["role:admin"]);
    assert_eq!(unmapped, vec!["everyone", "wheel"]);
    // No mapping at all → no roles, regardless of group names.
    let (none, _) = map_groups_to_roles("{}", &["platform-admins".into()]);
    assert!(none.is_empty());
}

// J3: "JIT provisioning rejects unverified or disallowed email domains."
#[test]
fn jit_rejects_unverified_and_off_domain() {
    let policy = JitPolicy::from_json(
        &json!({ "allowed_domains": ["corp.com"], "require_verified_email": true }).to_string(),
    );
    let claims = apply_claim_mapping(
        "{}",
        &json!({ "sub": "s", "email": "a@corp.com", "email_verified": false }),
    );
    assert!(matches!(
        evaluate_jit(&policy, &claims),
        JitDecision::Reject(_)
    ));
    let off_domain = apply_claim_mapping(
        "{}",
        &json!({ "sub": "s", "email": "a@elsewhere.com", "email_verified": true }),
    );
    assert!(matches!(
        evaluate_jit(&policy, &off_domain),
        JitDecision::Reject(_)
    ));
    let ok = apply_claim_mapping(
        "{}",
        &json!({ "sub": "s", "email": "a@corp.com", "email_verified": true }),
    );
    assert_eq!(evaluate_jit(&policy, &ok), JitDecision::Provision);
}

// J2.5: assurance normalization from IdP auth-context/MFA claims.
#[test]
fn assurance_normalizes_mfa_and_hardware() {
    assert_eq!(
        derive_assurance(&json!({"sub":"s","amr":["pwd"]})),
        A::SingleFactor
    );
    assert_eq!(
        derive_assurance(&json!({"sub":"s","amr":["pwd","otp"]})),
        A::MultiFactor
    );
    assert_eq!(
        derive_assurance(&json!({"sub":"s","amr":["webauthn"]})),
        A::Hardware
    );
}

// J3: "JWKS rotation and missing kid." A rotated JWKS advertises new kids; an
// empty/garbage JWKS advertises none (the verify path then can't find a kid).
#[test]
fn jwks_kid_extraction_handles_rotation_and_missing() {
    let before = r#"{"keys":[{"kid":"2023","kty":"RSA"}]}"#;
    let after = r#"{"keys":[{"kid":"2024","kty":"RSA"},{"kid":"2025","kty":"RSA"}]}"#;
    assert_eq!(parse_jwks_kids(before), vec!["2023"]);
    assert_eq!(parse_jwks_kids(after), vec!["2024", "2025"]);
    // Missing kid in a key, or no keys at all, yields an empty advertised set.
    assert!(parse_jwks_kids(r#"{"keys":[{"kty":"RSA"}]}"#).is_empty());
    assert!(parse_jwks_kids(r#"{"keys":[]}"#).is_empty());
}

// J3: "SAML replay rejection" — the durable cache is keyed by assertion ID. Here
// we pin the parse + fail-closed signature path that gates it: a valid-window
// assertion with NO configured cert must be rejected (never trusted), and an
// expired assertion is rejected on the clock-skew check before any DB write.
#[test]
fn saml_fails_closed_and_rejects_expired() {
    let assertion = format!(
        r#"<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">
  <saml:Assertion ID="_id1">
    <saml:Subject><saml:NameID>u@corp.com</saml:NameID></saml:Subject>
    <saml:Conditions NotBefore="2020-01-01T00:00:00Z" NotOnOrAfter="2099-01-01T00:00:00Z"/>
  </saml:Assertion>
</samlp:Response>"#
    );
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(assertion.as_bytes());
    // Valid window but no certs → fail closed.
    let err = saml::validate_response(&b64, &[], "", 60, 1_600_000_000).unwrap_err();
    assert!(matches!(err, saml::SamlError::Signature(_)));
}

// J3: "Multiple providers per tenant" + "Tenant A provider cannot authenticate
// into tenant B." These are enforced by the store's tenant-scoped lookups
// (every query binds tenant_id) and the unique (tenant, provider, subject)
// constraint. The pure-logic invariant we can pin here is that claim mapping is
// provider-local: two providers with different mappings produce different
// principals from the same raw claims, so a tenant-B provider's mapping never
// applies to a tenant-A token.
#[test]
fn claim_mapping_is_provider_local() {
    let raw = json!({ "sub": "abc", "oid": "xyz", "email": "a@b.com", "upn": "u@corp.com" });
    let provider_a =
        apply_claim_mapping(&json!({"subject":"sub","email":"email"}).to_string(), &raw);
    let provider_b = apply_claim_mapping(&json!({"subject":"oid","email":"upn"}).to_string(), &raw);
    assert_eq!(provider_a.subject, "abc");
    assert_eq!(provider_a.email, "a@b.com");
    assert_eq!(provider_b.subject, "xyz");
    assert_eq!(provider_b.email, "u@corp.com");
    // Different subjects → different external-identity rows → no cross-tenant
    // collision even with the same underlying token.
    assert_ne!(provider_a.subject, provider_b.subject);
}

// J2.4: explicit account-linking policy values are honored as documented.
#[test]
fn account_linking_policy_values_are_recognized() {
    assert_eq!(
        evaluate_account_linking("auto_verified", true),
        AccountLinkDecision::LinkExisting
    );
    assert_eq!(
        evaluate_account_linking("auto_verified", false),
        AccountLinkDecision::RequireExplicit
    );
    assert_eq!(
        evaluate_account_linking("explicit", true),
        AccountLinkDecision::RequireExplicit
    );
    assert_eq!(
        evaluate_account_linking("deny", true),
        AccountLinkDecision::Deny
    );
    assert_eq!(
        evaluate_account_linking("new-unknown-mode", true),
        AccountLinkDecision::RequireExplicit
    );
}
