//! Live Postgres tests for the native auth services.
//!
//! These tests intentionally do not use in-memory stores or snapshot mutation.
//! They apply native auth DDL generated from embedded UDB protos, exercise the
//! Postgres-backed services, and drop the native auth schemas at the end.

mod analytics_live;
mod apikey_live;
mod authn_atomicity_live;
mod authn_authenticate_live;
mod authn_jwt_live;
mod authn_mfa_live;
mod authn_noleak_live;
mod authn_otp_password_live;
#[cfg(feature = "redis")]
mod authn_redis_live;
mod authn_security_live;
mod authn_session_live;
mod authn_user_live;
mod authn_webauthn_noleak_live;
mod authz_admin_live;
mod authz_rbac_live;
mod authz_rebac_live;
#[cfg(feature = "kafka")]
mod cdc_live;
#[cfg(feature = "kafka")]
mod events_live;
mod fault_injection_live;
mod grants_live;
mod ha_convergence_live;
mod ha_jwks_rotation_live;
mod ha_multinode_live;
#[cfg(feature = "kafka")]
mod notification_events_live;
mod notification_live;
mod support;
mod tenant_live;

#[test]
fn authn_outbound_sources_do_not_reintroduce_banned_echoes() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mappings =
        std::fs::read_to_string(root.join("src/runtime/service/auth_service/mappings.rs"))
            .expect("read mappings.rs");
    let sessions =
        std::fs::read_to_string(root.join("src/runtime/service/auth_service/authn/sessions.rs"))
            .expect("read sessions.rs");
    let login =
        std::fs::read_to_string(root.join("src/runtime/service/auth_service/authn/login.rs"))
            .expect("read login.rs");
    let core = std::fs::read_to_string(root.join("src/runtime/service/auth_service/authn/core.rs"))
        .expect("read core.rs");

    for banned in [
        "session_id: rec.session_id_hash",
        "session_id: req.session_id",
        "token_id: rec.session_id_hash",
        "\"session_id\": session_id",
        "\"session_id\": req.session_id",
    ] {
        assert!(
            !mappings.contains(banned) && !sessions.contains(banned) && !login.contains(banned),
            "banned outbound token/hash echo pattern reappeared: {banned}"
        );
    }

    for required in [
        "password_hash: String::new()",
        "totp_secret_enc: String::new()",
        "session_token_lookup: String::new()",
        "session_token_hash: String::new()",
        "csrf_token_hash: String::new()",
        "key_hash: String::new()",
    ] {
        assert!(
            mappings.contains(required)
                || core.contains(required)
                || std::fs::read_to_string(root.join("src/runtime/service/auth_service/apikey.rs"))
                    .expect("read apikey.rs")
                    .contains(required),
            "required redaction assignment missing: {required}"
        );
    }
}
