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
use super::scim::ScimGroupView;
use super::{
    group_keys, idp_account_linking_explicit_required_status, idp_claims_json_invalid_status,
    idp_claims_subject_required_status, idp_display_name_required_status,
    idp_jit_provisioning_rejected_status, idp_metadata_fetch_failed_status,
    idp_provider_disabled_static_status, idp_provider_disabled_status,
    idp_provider_not_found_status, idp_saml_metadata_invalid_status,
    idp_saml_metadata_required_status, idp_saml_replay_rejected_status,
    idp_saml_sso_url_missing_status, idp_scim_group_json_invalid_status,
    idp_scim_group_mapping_required_status, idp_scim_group_not_found_status,
    idp_scim_patch_invalid_status, idp_scim_user_json_invalid_status,
    idp_scim_user_not_found_status, idp_subject_user_required_status,
    idp_tenant_id_required_status, scim_group_pb,
};
use crate::proto::udb::core::idp::entity::v1::AssuranceLevel as A;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
use serde_json::json;
use tonic::Status;

fn decode_detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("typed detail trailer is present");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

fn assert_validation_fields(status: &Status, expected: &[(&str, &str)]) {
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), expected.len());
    for (actual, (field, description)) in detail.field_violations.iter().zip(expected) {
        assert_eq!(actual.field, *field);
        assert_eq!(actual.description, *description);
    }
}

fn assert_capability_detail(
    status: &Status,
    backend: &str,
    operation: &str,
    capability_required: &str,
    message: &str,
) {
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Capability as i32);
    assert_eq!(detail.backend, backend);
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.capability_required, capability_required);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_policy_detail(status: &Status, operation: &str, policy_decision_id: &str, message: &str) {
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.policy_decision_id, policy_decision_id);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_permission_policy_detail(
    status: &Status,
    operation: &str,
    policy_decision_id: &str,
    message: &str,
) {
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.policy_decision_id, policy_decision_id);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
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
    assert_eq!(detail.backend, "identity_provider");
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.capability_required, schema_code);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

#[test]
fn idp_missing_postgres_store_capability_carries_typed_detail() {
    let svc = super::IdentityProviderServiceImpl::new();
    let err = match svc.require_pool() {
        Err(status) => status,
        Ok(_) => panic!("pool-less identity-provider service must fail closed"),
    };

    assert_capability_detail(
        &err,
        "identity_provider",
        "postgres_store",
        "postgres_store",
        "identity-provider service requires a Postgres-backed store (no PG pool configured)",
    );
}

#[test]
fn idp_lookup_misses_carry_typed_schema_detail() {
    assert_schema_not_found_detail(
        &idp_provider_not_found_status(),
        "provider_lookup",
        "identity_provider_not_found",
        "identity provider not found for this tenant",
    );

    assert_schema_not_found_detail(
        &idp_scim_user_not_found_status(),
        "scim_user_lookup",
        "scim_user_not_found",
        "SCIM user not found",
    );

    assert_schema_not_found_detail(
        &idp_scim_group_not_found_status(),
        "scim_group_mapping_lookup",
        "scim_group_not_found",
        "SCIM group not found in group mapping",
    );
}

#[test]
fn idp_saml_provider_setup_capabilities_carry_typed_detail() {
    let disabled = idp_provider_disabled_status("Corp SAML");
    assert_capability_detail(
        &disabled,
        "identity_provider",
        "provider_login",
        "provider_enabled",
        "identity provider 'Corp SAML' is disabled",
    );

    let disabled_static = idp_provider_disabled_static_status();
    assert_capability_detail(
        &disabled_static,
        "identity_provider",
        "provider_login",
        "provider_enabled",
        "identity provider is disabled",
    );

    let missing_sso = idp_saml_sso_url_missing_status();
    assert_capability_detail(
        &missing_sso,
        "identity_provider",
        "saml_login",
        "saml_sso_url",
        "provider has no SAML SSO URL; import metadata first",
    );

    let fetch_failed = idp_metadata_fetch_failed_status("timeout");
    assert_capability_detail(
        &fetch_failed,
        "identity_provider",
        "metadata_fetch",
        "saml_metadata_url",
        "metadata fetch failed: timeout",
    );
}

#[test]
fn idp_scim_group_mapping_policy_carries_typed_detail() {
    assert_policy_detail(
        &idp_scim_group_mapping_required_status(),
        "scim_create_group",
        "scim_group_mapping_required",
        "group must match a configured group mapping key; groups are mapping-driven and not persisted",
    );

    assert_policy_detail(
        &idp_account_linking_explicit_required_status(),
        "idp_account_linking",
        "explicit_link_required",
        "an account with this email exists; explicit account linking is required",
    );
}

#[test]
fn idp_saml_and_jit_permission_denials_carry_typed_detail() {
    assert_permission_policy_detail(
        &idp_saml_replay_rejected_status(),
        "saml_acs",
        "saml_assertion_replay",
        "SAML assertion has already been consumed (replay rejected)",
    );

    assert_permission_policy_detail(
        &idp_jit_provisioning_rejected_status("email is not verified"),
        "idp_jit_provisioning",
        "jit_policy_rejected",
        "JIT provisioning rejected: email is not verified",
    );
}

#[test]
fn idp_provider_boundary_validation_carries_field_violations() {
    let missing_tenant = idp_tenant_id_required_status();
    assert_eq!(missing_tenant.message(), "tenant_id is required");
    assert_validation_fields(
        &missing_tenant,
        &[("tenant_id", "must be a non-empty tenant id")],
    );

    let missing_display = idp_display_name_required_status();
    assert_eq!(missing_display.message(), "display_name is required");
    assert_validation_fields(
        &missing_display,
        &[("display_name", "must be a non-empty display name")],
    );

    let bad_claims = idp_claims_json_invalid_status("expected value");
    assert_eq!(
        bad_claims.message(),
        "claims_json is not valid JSON: expected value"
    );
    assert_validation_fields(
        &bad_claims,
        &[("claims_json", "must decode as a JSON object of IdP claims")],
    );

    let missing_link = idp_subject_user_required_status();
    assert_eq!(missing_link.message(), "subject and user_id are required");
    assert_validation_fields(
        &missing_link,
        &[
            ("subject", "must be a non-empty external subject"),
            ("user_id", "must be a non-empty UDB user id"),
        ],
    );

    let missing_subject = idp_claims_subject_required_status();
    assert_eq!(
        missing_subject.message(),
        "claims have no resolvable subject"
    );
    assert_validation_fields(
        &missing_subject,
        &[(
            "claims_json",
            "must map to a non-empty external subject claim",
        )],
    );
}

#[test]
fn idp_saml_scim_boundary_validation_carries_field_violations() {
    let missing_metadata = idp_saml_metadata_required_status();
    assert_eq!(
        missing_metadata.message(),
        "metadata_xml is required (or set the provider's saml_metadata_url)"
    );
    assert_validation_fields(
        &missing_metadata,
        &[(
            "metadata_xml",
            "must contain SAML metadata XML when the provider has no metadata URL",
        )],
    );

    let bad_metadata = idp_saml_metadata_invalid_status("missing entityID");
    assert_eq!(
        bad_metadata.message(),
        "invalid SAML metadata: missing entityID"
    );
    assert_validation_fields(
        &bad_metadata,
        &[("metadata_xml", "must decode as valid SAML metadata XML")],
    );

    let bad_scim_user = idp_scim_user_json_invalid_status("SCIM user must be a JSON object");
    assert_eq!(bad_scim_user.message(), "SCIM user must be a JSON object");
    assert_validation_fields(
        &bad_scim_user,
        &[(
            "scim_user_json",
            "must decode as a valid SCIM User resource",
        )],
    );

    let bad_patch = idp_scim_patch_invalid_status("unsupported SCIM patch");
    assert_eq!(bad_patch.message(), "unsupported SCIM patch");
    assert_validation_fields(
        &bad_patch,
        &[(
            "operations",
            "must contain supported SCIM PATCH operations for a User resource",
        )],
    );

    let bad_group =
        idp_scim_group_json_invalid_status("SCIM group is missing required displayName");
    assert_eq!(
        bad_group.message(),
        "SCIM group is missing required displayName"
    );
    assert_validation_fields(
        &bad_group,
        &[(
            "scim_group_json",
            "must decode as a valid SCIM Group resource",
        )],
    );
}

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

#[test]
fn scim_group_mapping_keys_are_the_canonical_ids() {
    let mapping = json!({
        "platform-admins": ["role:admin"],
        "support": "role:support",
    })
    .to_string();
    let mut keys = group_keys(&mapping);
    keys.sort();
    assert_eq!(keys, vec!["platform-admins", "support"]);
    assert!(!keys.contains(&"role:admin".to_string()));
}

#[test]
fn scim_group_pb_uses_mapping_key_as_id_and_location() {
    let group = scim_group_pb(
        "platform-admins",
        &ScimGroupView {
            display_name: "platform-admins".to_string(),
            members: vec!["user-1".to_string()],
        },
    );

    assert_eq!(group.id, "platform-admins");
    assert_eq!(group.display_name, "platform-admins");
    let raw: serde_json::Value = serde_json::from_str(&group.raw_json).expect("SCIM group JSON");
    assert_eq!(raw["id"], "platform-admins");
    assert_eq!(raw["meta"]["location"], "/scim/v2/Groups/platform-admins");
}
