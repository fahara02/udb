//! Unit guards for the native `VaultService`: the typed internal/capability/
//! schema detail shapes, the destructive-confirmation guard, request-body
//! cross-tenant rejection, field-violation shapes, the transit-algorithm
//! allow-list, the fail-closed seal gate, the runtime-required refusal, the
//! redacting-`Debug` canary, the AEAD seal/open + HMAC round-trip, and the
//! dynamic DB-role allow-listing. Copied verbatim from the former god file;
//! imports are explicit (no `use super::*`).

use std::sync::Arc;

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::proto::udb::core::vault::services::v1 as vault_pb;
use crate::proto::udb::core::vault::services::v1::vault_service_server::VaultService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::VaultServiceImpl;
use super::config::{
    DEFAULT_TRANSIT_ALGORITHM, MAX_BATCH_DECRYPT_ITEMS, MAX_BATCH_ENCRYPT_ITEMS, MAX_VERSIONS_SCAN,
    MIN_DB_CREDENTIAL_TTL_SECONDS, VAULT_DB_LEASE_REAPER_BATCH,
    VAULT_MASTER_KEY_UNAVAILABLE_MESSAGE, VAULT_RUNTIME_REQUIRED_MESSAGE, max_batch_decrypt_items,
    max_batch_encrypt_items, max_versions_scan, vault_db_lease_reaper_batch,
};
use super::config::{STATE_ACTIVE, STATE_DELETED};
use super::crypto::{
    DataKey, PlaintextSecret, constant_time_eq, dek_open, dek_seal, ed25519_public_key_b64,
    ed25519_sign_b64, ed25519_verify_b64, hmac_sha256, is_wrapped_dek_envelope,
    parse_transit_envelope, require_encryption_algorithm, require_hmac_algorithm,
    require_signing_algorithm, validate_transit_algorithm,
};
use super::dynamic::{
    parse_vault_db_role_configs, requested_db_credential_ttl, validate_db_credential_binding,
    validate_db_role_alias,
};
use super::errors::{
    is_duplicate_conflict, vault_db_credentials_config_status,
    vault_db_native_store_required_status, vault_internal_status,
    vault_master_key_operation_status, vault_schema_already_exists_status,
    vault_schema_not_found_status, vault_secret_cas_conflict_status,
};
use super::model::{StoredSecret, select_readable_secret};
use super::store::{secret_shred_all_sql, transit_demote_active_sql, transit_insert_rotated_sql};

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
        "must be one of the supported transit algorithms: aes256-gcm-siv, ed25519, hmac-sha256",
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

#[test]
fn default_runtime_without_master_kek_is_sealed() {
    let svc = VaultServiceImpl::new()
        .with_runtime(Some(Arc::new(crate::runtime::DataBrokerRuntime::default())));
    let err = svc
        .check_seal()
        .expect_err("Vault must not inherit the runtime's plaintext development fallback");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    let detail = decode_detail(&err);
    assert_eq!(detail.operation, "seal_gate");
    assert_eq!(detail.capability_required, "vault_master_key");
}

#[test]
fn only_authenticated_master_kek_envelopes_are_accepted_as_wrapped_deks() {
    assert!(is_wrapped_dek_envelope("udb-aead:v1:nonce:ciphertext"));
    assert!(!is_wrapped_dek_envelope("udb-aead:"));
    assert!(!is_wrapped_dek_envelope(
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    ));
    assert!(!is_wrapped_dek_envelope("plaintext"));
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

/// The encryption path refuses a non-encryption key (key-purpose confusion): a
/// signing seed is not an AEAD key, and an hmac-sha256 key is a dedicated MAC key.
/// Encryption-algorithm keys (and the empty/legacy default) pass through.
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
        "must name an encryption key (aes256-gcm-siv), not a 'ed25519' key",
    );
    // A dedicated hmac-sha256 key is likewise refused for the encryption path, so
    // one key can never serve both Encrypt and HMAC.
    let hmac_err = require_encryption_algorithm("hmac-sha256", "encrypt")
        .expect_err("an hmac-sha256 key must not be used to encrypt");
    assert_eq!(hmac_err.code(), tonic::Code::InvalidArgument);
    assert_single_field_violation(
        &hmac_err,
        "key_name",
        "must name an encryption key (aes256-gcm-siv), not a 'hmac-sha256' key",
    );
}

#[test]
fn db_role_config_is_allow_listed_and_identifier_safe() {
    let configs = parse_vault_db_role_configs(
        r#"[{"role_name":"app-read","tenant_id":"tenant-a","project_id":"default","target_instance":"primary","database_name":"udb","policy_revision":"rev-1","relations":[{"schema":"app","table":"records","tenant_column":"tenant_id","project_column":"project_id","privileges":["select"]}],"ttl_seconds_max":600}]"#,
    )
    .expect("valid config");
    let app_read = configs.get("app-read").expect("role alias present");
    assert_eq!(app_read.tenant_id, "tenant-a");
    assert_eq!(app_read.project_id, "default");
    assert_eq!(app_read.target_instance, "primary");
    assert_eq!(app_read.database_name, "udb");
    assert_eq!(app_read.policy_revision, "rev-1");
    assert_eq!(app_read.relations[0].privileges, ["SELECT"]);
    assert_eq!(app_read.ttl_seconds_max, Some(600));

    let legacy_parent = parse_vault_db_role_configs(
        r#"[{"role_name":"app-read","parent_role":"udb_app_read;drop role x"}]"#,
    )
    .expect_err("legacy global parent-role delegation must be rejected");
    assert!(legacy_parent.contains("unknown field") || legacy_parent.contains("tenant_id"));

    let bad_alias = parse_vault_db_role_configs(
        r#"[{"role_name":"app read","tenant_id":"tenant-a","project_id":"default","target_instance":"primary","database_name":"udb","policy_revision":"rev-1","relations":[{"schema":"app","table":"records","tenant_column":"tenant_id","project_column":"project_id","privileges":["SELECT"]}]}]"#,
    )
    .expect_err("space in role alias must be rejected");
    assert!(bad_alias.contains("role_name"));

    let write_grant = parse_vault_db_role_configs(
        r#"[{"role_name":"app-read","tenant_id":"tenant-a","project_id":"default","target_instance":"primary","database_name":"udb","policy_revision":"rev-1","relations":[{"schema":"app","table":"records","tenant_column":"tenant_id","project_column":"project_id","privileges":["UPDATE"]}]}]"#,
    )
    .expect_err("the first authority version is intentionally read-only");
    assert!(write_grant.contains("only SELECT is supported"));
}

#[test]
fn dynamic_database_credential_alias_is_exactly_scope_and_instance_bound() {
    let configs = parse_vault_db_role_configs(
        r#"[{"role_name":"app-read","tenant_id":"tenant-a","project_id":"default","target_instance":"primary","database_name":"udb","policy_revision":"rev-1","relations":[{"schema":"app","table":"records","tenant_column":"tenant_id","project_column":"project_id","privileges":["SELECT"]}]}]"#,
    )
    .expect("valid config");
    let config = &configs["app-read"];
    validate_db_credential_binding(config, "tenant-a", "default", "primary")
        .expect("exact binding must pass");
    for (tenant, project, instance) in [
        ("tenant-b", "default", "primary"),
        ("tenant-a", "project-b", "primary"),
        ("tenant-a", "default", "replica-b"),
    ] {
        let err = validate_db_credential_binding(config, tenant, project, instance)
            .expect_err("any binding drift must fail closed");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with UDB_VAULT_DB_ROLES_JSON authority config"]
async fn vault_db_credentials_live_enforce_fixed_tenant_and_project_after_guc_change() {
    if std::env::var("UDB_LIVE_AUTH_TESTS").ok().as_deref() != Some("1") {
        return;
    }
    use sqlx::Connection as _;
    use crate::runtime::service::live_tests::support::{
        live_native_service_db_lock, live_pg_dsn, live_pg_pool, migrate_native_service_db,
        vault_service,
    };

    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    sqlx::query("CREATE SCHEMA udb_vault_authority_test")
        .execute(&pool)
        .await
        .expect("create authority test schema");
    sqlx::query(
        "CREATE TABLE udb_vault_authority_test.records (\
             record_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, project_id TEXT NOT NULL, payload TEXT NOT NULL\
         )",
    )
    .execute(&pool)
    .await
    .expect("create authority test table");
    sqlx::query("ALTER TABLE udb_vault_authority_test.records ENABLE ROW LEVEL SECURITY")
        .execute(&pool)
        .await
        .expect("enable test RLS");
    sqlx::query(
        "CREATE POLICY caller_scope ON udb_vault_authority_test.records \
         AS PERMISSIVE FOR SELECT TO PUBLIC USING (true)",
    )
    .execute(&pool)
    .await
    .expect("create deliberately broad baseline policy");
    sqlx::query(
        "INSERT INTO udb_vault_authority_test.records (record_id, tenant_id, project_id, payload) \
         VALUES ('own', 'tenant-a', 'default', 'own'), \
                ('foreign-tenant', 'tenant-b', 'default', 'foreign'), \
                ('foreign-project', 'tenant-a', 'project-b', 'foreign')",
    )
    .execute(&pool)
    .await
    .expect("seed cross-scope records");

    let mut svc = vault_service().await;
    let mut request = Request::new(vault_pb::GenerateDatabaseCredentialsRequest {
        tenant_id: "tenant-a".to_string(),
        project_id: "default".to_string(),
        role_name: "authority-test-read".to_string(),
        ttl_seconds: 300,
        idempotency_key: "authority-live-response-loss".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    request.metadata_mut().insert(
        "x-udb-project-id",
        MetadataValue::from_static("default"),
    );
    let issued = svc
        .generate_database_credentials(request)
        .await
        .expect("served Vault RPC must issue the bound credential")
        .into_inner();

    let dsn = live_pg_dsn();
    let options = dsn
        .parse::<sqlx::postgres::PgConnectOptions>()
        .expect("parse live Postgres DSN")
        .username(&issued.username)
        .password(&issued.password);
    let mut credential_conn = sqlx::PgConnection::connect_with(&options)
        .await
        .expect("connect with generated credential");
    let visible: Vec<String> = sqlx::query_scalar(
        "SELECT record_id FROM udb_vault_authority_test.records ORDER BY record_id",
    )
    .fetch_all(&mut credential_conn)
    .await
    .expect("read through generated credential");
    assert_eq!(visible, ["own"]);

    // Ordinary custom GUCs are caller-changeable. The negative proof changes
    // both hints to foreign scope and then reads again: the fixed restrictive
    // policy, not the GUC, remains the authorization boundary.
    sqlx::query("SET app.current_tenant_id = 'tenant-b'")
        .execute(&mut credential_conn)
        .await
        .expect("caller can change the tenant hint");
    sqlx::query("SET app.current_project_id = 'project-b'")
        .execute(&mut credential_conn)
        .await
        .expect("caller can change the project hint");
    let visible_after_change: Vec<String> = sqlx::query_scalar(
        "SELECT record_id FROM udb_vault_authority_test.records ORDER BY record_id",
    )
    .fetch_all(&mut credential_conn)
    .await
    .expect("read after hostile GUC change");
    assert_eq!(visible_after_change, ["own"]);

    // Simulate response loss: the caller repeats the exact request with the same
    // key. The served path must recover the original KEK-wrapped password and
    // lease rather than minting a second login.
    let mut replay_request = Request::new(vault_pb::GenerateDatabaseCredentialsRequest {
        tenant_id: "tenant-a".to_string(),
        project_id: "default".to_string(),
        role_name: "authority-test-read".to_string(),
        ttl_seconds: 300,
        idempotency_key: "authority-live-response-loss".to_string(),
        ..Default::default()
    });
    replay_request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    replay_request.metadata_mut().insert(
        "x-udb-project-id",
        MetadataValue::from_static("default"),
    );
    let replayed = svc
        .generate_database_credentials(replay_request)
        .await
        .expect("response-loss replay must recover the original credential")
        .into_inner();
    assert!(replayed.replayed);
    assert_eq!(replayed.lease_id, issued.lease_id);
    assert_eq!(replayed.username, issued.username);
    assert_eq!(replayed.password, issued.password);

    // Revoke through the actual VaultService handler while a session is live.
    // The response is allowed only after the session is terminated and the role
    // is proven absent; the same transaction writes durable outbox evidence.
    let mut revoke_request = Request::new(vault_pb::RevokeDatabaseCredentialsRequest {
        tenant_id: "tenant-a".to_string(),
        project_id: "default".to_string(),
        lease_id: issued.lease_id.clone(),
        reason: "live response-loss credential cleanup".to_string(),
        ..Default::default()
    });
    revoke_request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    revoke_request.metadata_mut().insert(
        "x-udb-project-id",
        MetadataValue::from_static("default"),
    );
    let revoked = svc
        .revoke_database_credentials(revoke_request)
        .await
        .expect("served revoke must terminate sessions and remove the role")
        .into_inner();
    assert_eq!(revoked.state, "REVOKED");
    assert!(!postgres_role_exists(&pool, &issued.username)
        .await
        .expect("role absence proof"));
    assert!(
        sqlx::query("SELECT 1")
            .execute(&mut credential_conn)
            .await
            .is_err(),
        "the pre-revocation database session must be terminated"
    );
    let revoked_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM udb_system.outbox_events \
         WHERE topic = 'udb.vault.db_credential.revoked.v1' AND partition_key = $1",
    )
    .bind(&issued.lease_id)
    .fetch_one(&pool)
    .await
    .expect("count durable revocation evidence");
    assert_eq!(revoked_events, 1);
    let recovery_envelope: String = sqlx::query_scalar(
        "SELECT credential_ciphertext FROM udb_vault.vault_db_credential_leases \
         WHERE lease_id = $1::uuid",
    )
    .bind(&issued.lease_id)
    .fetch_one(&pool)
    .await
    .expect("load revoked credential recovery envelope");
    assert!(
        recovery_envelope.is_empty(),
        "terminal revocation must crypto-shred replayable password recovery material"
    );

    // Fault injection at the last transactional boundary: a missing outbox
    // relation makes the strict issued-event insert fail after role/policy SQL.
    // Because STARTING + physical authority + ACTIVE + outbox share one PG tx,
    // neither a role nor a lease may survive the failure.
    let roles_before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_roles WHERE rolname LIKE 'udb_vault_%'",
    )
    .fetch_one(&pool)
    .await
    .expect("count generated roles before fault injection");
    svc.outbox_relation = Some("\"udb_system\".\"missing_vault_outbox\"".to_string());
    let mut failed_request = Request::new(vault_pb::GenerateDatabaseCredentialsRequest {
        tenant_id: "tenant-a".to_string(),
        project_id: "default".to_string(),
        role_name: "authority-test-read".to_string(),
        ttl_seconds: 300,
        idempotency_key: "authority-live-outbox-failure".to_string(),
        ..Default::default()
    });
    failed_request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    failed_request.metadata_mut().insert(
        "x-udb-project-id",
        MetadataValue::from_static("default"),
    );
    svc.generate_database_credentials(failed_request)
        .await
        .expect_err("strict outbox failure must roll back role and lease");
    let roles_after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_roles WHERE rolname LIKE 'udb_vault_%'",
    )
    .fetch_one(&pool)
    .await
    .expect("count generated roles after fault injection");
    assert_eq!(roles_after, roles_before);
    let stranded_leases: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM udb_vault.vault_db_credential_leases \
         WHERE tenant_id = 'tenant-a' AND project_id = 'default' AND idempotency_key = $1",
    )
    .bind("authority-live-outbox-failure")
    .fetch_one(&pool)
    .await
    .expect("count failed issuance leases");
    assert_eq!(stranded_leases, 0);
}

/// DestroySecret is an irreversible crypto-shred, so a merely non-empty
/// confirmation token is not enough: it must name the exact `secret_path`. A
/// mismatching token fails closed with a typed field violation before any
/// admission/runtime access.
#[tokio::test]
async fn destroy_secret_confirmation_must_match_secret_path() {
    let svc = VaultServiceImpl::new().with_seal_override(false);
    let mut request = Request::new(vault_pb::DestroySecretRequest {
        tenant_id: "tenant-a".to_string(),
        secret_path: "app/db/password".to_string(),
        confirmation_token: "app/db/passwrd".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

    let err = svc
        .destroy_secret(request)
        .await
        .expect_err("a confirmation token that does not equal the path must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "DestroySecret confirmation_token must match the secret_path to authorize the irreversible crypto-shred"
    );
    assert_single_field_violation(
        &err,
        "confirmation_token",
        "must exactly equal the secret_path being destroyed",
    );
}

/// A confirmation token equal to the `secret_path` satisfies the binding guard;
/// the RPC then proceeds and (with no runtime configured in the unit) fails at the
/// runtime capability gate — proving the guard was passed, not the confirmation.
#[tokio::test]
async fn destroy_secret_matching_confirmation_passes_the_binding_guard() {
    let svc = VaultServiceImpl::new().with_seal_override(false);
    let mut request = Request::new(vault_pb::DestroySecretRequest {
        tenant_id: "tenant-a".to_string(),
        secret_path: "app/db/password".to_string(),
        confirmation_token: "app/db/password".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

    let err = svc
        .destroy_secret(request)
        .await
        .expect_err("no runtime is configured in the unit");
    assert_capability_detail(
        &err,
        "native_entity_dispatch",
        "runtime_native_entity_dispatch",
        VAULT_RUNTIME_REQUIRED_MESSAGE,
    );
}

/// BatchEncrypt caps the number of plaintexts per request (env
/// `UDB_VAULT_MAX_BATCH_ENCRYPT`), rejecting an over-cap batch with a typed field
/// violation before any allocation or crypto work.
#[tokio::test]
async fn batch_encrypt_rejects_over_cap_batches() {
    let svc = VaultServiceImpl::new().with_seal_override(false);
    let over_cap = max_batch_encrypt_items() + 1;
    let mut request = Request::new(vault_pb::BatchEncryptRequest {
        tenant_id: "tenant-a".to_string(),
        key_name: "app-key".to_string(),
        plaintexts: vec![String::new(); over_cap],
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

    let err = svc
        .batch_encrypt(request)
        .await
        .expect_err("an over-cap plaintext batch must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().starts_with("batch_encrypt accepts at most "),
        "unexpected message: {}",
        err.message()
    );
    assert_single_field_violation(
        &err,
        "plaintexts",
        "must not exceed the configured batch-encrypt item cap",
    );
}

/// BatchDecrypt caps the number of ciphertexts per request (env
/// `UDB_VAULT_MAX_BATCH_DECRYPT_ITEMS`), rejecting an over-cap batch with a typed
/// field violation before any allocation, master-key unwrap, or crypto work —
/// closing the parity gap with BatchEncrypt.
#[tokio::test]
async fn batch_decrypt_rejects_over_cap_batches() {
    let svc = VaultServiceImpl::new().with_seal_override(false);
    let over_cap = max_batch_decrypt_items() + 1;
    let mut request = Request::new(vault_pb::BatchDecryptRequest {
        tenant_id: "acme-tenant".to_string(),
        key_name: "acme-key".to_string(),
        ciphertexts: vec![String::new(); over_cap],
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("acme-tenant"));

    let err = svc
        .batch_decrypt(request)
        .await
        .expect_err("an over-cap ciphertext batch must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().starts_with("batch_decrypt accepts at most "),
        "unexpected message: {}",
        err.message()
    );
    assert_single_field_violation(
        &err,
        "ciphertexts",
        "must not exceed the configured batch-decrypt item cap",
    );
}

/// Key separation (M12, full): the three transit purposes are FULLY isolated by
/// algorithm. aes256-gcm-siv (and the empty/legacy default) → Encrypt/Decrypt
/// ONLY; ed25519 → Sign/Verify ONLY; hmac-sha256 → Hmac/Verify ONLY. No single key
/// serves two purposes — in particular an encryption key can no longer double as
/// an HMAC key. This closes the cited exploit with a dedicated HMAC algorithm.
#[test]
fn transit_key_purpose_is_separated_by_algorithm() {
    // HMAC guard: ONLY the dedicated hmac-sha256 key is admitted; the encryption
    // key and the Ed25519 signing key (and the empty/legacy default) are refused.
    assert!(require_hmac_algorithm("hmac-sha256", "hmac").is_ok());
    for wrong in ["aes256-gcm-siv", "ed25519", ""] {
        assert!(
            require_hmac_algorithm(wrong, "hmac").is_err(),
            "a non-HMAC key ('{wrong}') must not be usable for HMAC"
        );
    }
    let hmac_err = require_hmac_algorithm("aes256-gcm-siv", "hmac")
        .expect_err("an encryption key must not be usable for HMAC");
    assert_eq!(hmac_err.code(), tonic::Code::InvalidArgument);
    assert_single_field_violation(&hmac_err, "key_name", "must name an hmac-sha256 HMAC key");

    // Signing guard: only the Ed25519 signing key signs/verifies; the encryption
    // and HMAC keys are refused.
    assert!(require_signing_algorithm("ed25519", "sign").is_ok());
    for wrong in ["aes256-gcm-siv", "hmac-sha256", ""] {
        assert!(
            require_signing_algorithm(wrong, "sign").is_err(),
            "a non-signing key ('{wrong}') must not be usable for Sign/Verify"
        );
    }

    // Encryption guard: only a symmetric encryption key (or the empty/legacy
    // default) encrypts; the Ed25519 signing key and the hmac-sha256 key are
    // refused. The encryption key no longer passes the HMAC guard above — the two
    // purposes are fully separated.
    assert!(require_encryption_algorithm("aes256-gcm-siv", "encrypt").is_ok());
    assert!(require_encryption_algorithm("", "decrypt").is_ok());
    for wrong in ["ed25519", "hmac-sha256"] {
        assert!(
            require_encryption_algorithm(wrong, "encrypt").is_err(),
            "a non-encryption key ('{wrong}') must not be usable to encrypt"
        );
    }
}

/// The env-governed Vault knobs fall back to their byte-stable const defaults when
/// their `UDB_VAULT_*` variables are unset (the case under `cargo test`).
#[test]
fn vault_env_knobs_default_to_the_consts() {
    assert_eq!(max_versions_scan(), MAX_VERSIONS_SCAN);
    assert_eq!(vault_db_lease_reaper_batch(), VAULT_DB_LEASE_REAPER_BATCH);
    assert_eq!(max_batch_encrypt_items(), MAX_BATCH_ENCRYPT_ITEMS);
    assert_eq!(max_batch_decrypt_items(), MAX_BATCH_DECRYPT_ITEMS);
}

fn stored_secret(version: i64, state: &str) -> StoredSecret {
    StoredSecret {
        secret_id: format!("id-{version}"),
        version,
        ciphertext: "udb-vault:v1:AA".to_string(),
        data_key_wrapped: "udb-aead:AA".to_string(),
        state: state.to_string(),
        metadata_json: "{}".to_string(),
    }
}

/// The authoritative PutSecret CAS is the unique `(tenant_id, secret_path,
/// version)` index: the executor surfaces the collision as `AlreadyExists`, which
/// `is_duplicate_conflict` recognises and the handler remaps to a clean, retryable
/// `ABORTED` (never a raw `23505`/`AlreadyExists`).
#[test]
fn put_secret_cas_conflict_maps_to_aborted() {
    let already_exists = vault_schema_already_exists_status(
        "create_transit_key",
        "vault_transit_key_already_exists",
        "transit key already exists",
    );
    assert!(is_duplicate_conflict(&already_exists));
    // A NotFound (or any non-AlreadyExists) is NOT a CAS collision.
    let not_found =
        vault_schema_not_found_status("get_secret", "vault_secret_not_found", "secret not found");
    assert!(!is_duplicate_conflict(&not_found));

    let cas = vault_secret_cas_conflict_status(7);
    assert_eq!(cas.code(), tonic::Code::Aborted);
    let detail = decode_detail(&cas);
    assert_eq!(detail.kind, ErrorKind::Retryable as i32);
    assert_eq!(detail.backend, "vault");
    assert_eq!(detail.operation, "secret_version_CAS");
    assert!(detail.retryable);
    assert!(cas.message().contains("version 7"));
}

/// GetSecret never returns a soft-DELETED value: an explicit `version = N` must
/// require ACTIVE exactly as `version = 0` does. A DELETED version reads as "not
/// found" whether or not the caller names it.
#[test]
fn get_secret_hides_soft_deleted_versions() {
    // version 0 (latest ACTIVE) skips a DELETED higher version.
    let versions = vec![
        stored_secret(1, STATE_ACTIVE),
        stored_secret(2, STATE_DELETED),
    ];
    let latest = select_readable_secret(&versions, 0).expect("an ACTIVE version exists");
    assert_eq!(latest.version, 1);

    // An explicit request for the DELETED version 2 is hidden (None), not returned.
    assert!(select_readable_secret(&versions, 2).is_none());

    // An explicit request for the ACTIVE version 1 is honored.
    assert_eq!(
        select_readable_secret(&versions, 1)
            .expect("version 1 is ACTIVE")
            .version,
        1
    );

    // All-DELETED ⇒ nothing readable at any selector.
    let deleted_only = vec![stored_secret(1, STATE_DELETED)];
    assert!(select_readable_secret(&deleted_only, 0).is_none());
    assert!(select_readable_secret(&deleted_only, 1).is_none());
}

/// Logic-level guard for the atomic rotation: the demote statement flips ONLY the
/// currently-ACTIVE versions to VERIFYING, and the insert always writes exactly
/// one new ACTIVE version (with an atomically-computed `MAX(version)+1`). Run in
/// one transaction, this guarantees a reader always sees ≥1 ACTIVE version.
#[test]
fn rotate_sql_builders_keep_exactly_one_active_and_demote_the_rest() {
    let demote = transit_demote_active_sql();
    assert!(demote.starts_with("UPDATE "));
    assert!(demote.contains("'VERIFYING'"));
    assert!(demote.contains("'ACTIVE'"));

    let insert = transit_insert_rotated_sql();
    assert!(insert.starts_with("INSERT INTO "));
    // The new row is ACTIVE, and its version is MAX+1 computed inside the tx.
    assert!(insert.contains("'ACTIVE'"));
    assert!(insert.contains("MAX("));
    assert!(insert.contains("RETURNING"));
}

/// Logic-level guard for the atomic destroy: one UPDATE crypto-shreds every
/// non-DESTROYED version (clears both ciphertext columns, flips to DESTROYED), so
/// `rows_affected` is the exact destroyed count and no partial shred is possible.
#[test]
fn destroy_sql_shreds_every_non_destroyed_version() {
    let shred = secret_shred_all_sql();
    assert!(shred.starts_with("UPDATE "));
    assert!(shred.contains("'DESTROYED'"));
    // Only versions not already destroyed are touched (idempotent, accurate count).
    assert!(shred.contains("<> 'DESTROYED'"));
}
