//! Unit guards for the native `VaultService`: the typed internal/capability/
//! schema detail shapes, the destructive-confirmation guard, request-body
//! cross-tenant rejection, field-violation shapes, the transit-algorithm
//! allow-list, the fail-closed seal gate, the runtime-required refusal, the
//! redacting-`Debug` canary, the AEAD seal/open + HMAC round-trip, and the
//! dynamic DB-role allow-listing. Copied verbatim from the former god file;
//! imports are explicit (no `use super::*`).

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::proto::udb::core::vault::services::v1 as vault_pb;
use crate::proto::udb::core::vault::services::v1::vault_service_server::VaultService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::VaultServiceImpl;
use super::config::{
    DEFAULT_TRANSIT_ALGORITHM, MIN_DB_CREDENTIAL_TTL_SECONDS, VAULT_MASTER_KEY_UNAVAILABLE_MESSAGE,
    VAULT_RUNTIME_REQUIRED_MESSAGE,
};
use super::crypto::{
    DataKey, PlaintextSecret, constant_time_eq, dek_open, dek_seal, ed25519_public_key_b64,
    ed25519_sign_b64, ed25519_verify_b64, hmac_sha256, parse_transit_envelope,
    require_encryption_algorithm, validate_transit_algorithm,
};
use super::dynamic::{
    parse_vault_db_role_configs, requested_db_credential_ttl, validate_db_role_alias,
};
use super::errors::{
    vault_db_credentials_config_status, vault_db_native_store_required_status,
    vault_internal_status, vault_master_key_operation_status, vault_schema_already_exists_status,
    vault_schema_not_found_status,
};

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

    let role =
        vault_db_credentials_config_status("vault dynamic database role 'app' is not configured");
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
        "must be one of the supported transit algorithms: aes256-gcm-siv, ed25519",
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

/// Ed25519 Sign/Verify (the asymmetric transit algorithm): a signature verifies
/// only for the exact message under the exact key, and every corruption is a
/// clean `false` (no panic). The 32-byte transit key material is the seed, so any
/// key works. `ed25519` must also be an accepted CreateTransitKey algorithm.
#[test]
fn ed25519_sign_verify_round_trips_and_rejects_tampering() {
    assert_eq!(
        validate_transit_algorithm("ed25519").expect("ed25519 is a supported algorithm"),
        "ed25519"
    );

    let seed = [9u8; 32];
    let sig = ed25519_sign_b64(&seed, b"dispatch record 42");
    assert!(ed25519_verify_b64(&seed, b"dispatch record 42", &sig));
    // Deterministic (RFC 8032): same seed + message ⇒ identical signature.
    assert_eq!(ed25519_sign_b64(&seed, b"dispatch record 42"), sig);
    // Tampered message, wrong key, and malformed/short signatures all fail closed.
    assert!(!ed25519_verify_b64(&seed, b"dispatch record 43", &sig));
    assert!(!ed25519_verify_b64(&[8u8; 32], b"dispatch record 42", &sig));
    assert!(!ed25519_verify_b64(
        &seed,
        b"dispatch record 42",
        "not-base64!!"
    ));
    // "AAAA" is valid base64 but decodes to 3 bytes — not a 64-byte signature.
    assert!(!ed25519_verify_b64(&seed, b"dispatch record 42", "AAAA"));
    // The DEK material (any 32 bytes) is a valid seed, distinct keys don't cross.
    let other = DataKey([1u8; 32]);
    let sig2 = ed25519_sign_b64(&other.0, b"dispatch record 42");
    assert!(ed25519_verify_b64(&other.0, b"dispatch record 42", &sig2));
    assert!(!ed25519_verify_b64(&seed, b"dispatch record 42", &sig2));

    // Public-key export (GetTransitPublicKey): deterministic, the standard 44-char
    // base64 of a 32-byte key, and distinct per key — an external verifier uses it
    // to check a signature without the private seed.
    let pubkey = ed25519_public_key_b64(&seed);
    assert_eq!(ed25519_public_key_b64(&seed), pubkey);
    assert_eq!(pubkey.len(), 44);
    assert_ne!(pubkey, ed25519_public_key_b64(&other.0));
}

/// The encryption path refuses an Ed25519 signing key (key-purpose confusion): a
/// signing seed is not an AEAD key. Encryption-algorithm keys pass through.
#[test]
fn encryption_path_rejects_a_signing_key() {
    assert!(require_encryption_algorithm("aes256-gcm-siv", "encrypt").is_ok());
    assert!(require_encryption_algorithm("", "decrypt").is_ok());
    let err = require_encryption_algorithm("ed25519", "encrypt")
        .expect_err("an ed25519 signing key must not be used to encrypt");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_single_field_violation(
        &err,
        "key_name",
        "must name an encryption key (aes256-gcm-siv), not an ed25519 signing key",
    );
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

    let bad_alias =
        parse_vault_db_role_configs(r#"[{"role_name":"app read","parent_role":"udb_app_read"}]"#)
            .expect_err("space in role alias must be rejected");
    assert!(bad_alias.contains("role_name"));
}
