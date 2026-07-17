//! The seventeen `VaultService` RPC handlers (KV put/get/list/delete/destroy,
//! transit create/rotate/encrypt/decrypt/sign/verify/hmac + envelope
//! generate-data-key/rewrap + ed25519 get-transit-public-key, seal-status, and
//! dynamic database-credential generation), extracted from the trait impl as free
//! `pub(crate) async fn`s taking `svc` where the trait method took `&self`.
//! `mod.rs` delegates one line to each. Bodies are verbatim — the same seal gate,
//! cross-tenant guard, admission, envelope crypto, versioned CAS, rotation
//! overlap, redaction, and audit-emit (including the V-1 audit calls in
//! encrypt/sign/hmac/verify/list_secrets) as the former god file.
//!
//! CRITICAL: no crypto/encryption/decryption/signing/HMAC/wrap/envelope/seal
//! logic is altered — only the `self` → `svc` receiver rename.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::Utc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::ConflictStrategy;
use crate::proto::udb::core::vault::services::v1 as vault_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, native_next_page_token_for_total, native_offset_page_window,
    native_service_context, non_empty_json, validate_request_tenant,
};
use super::VaultServiceImpl;
use super::config::{
    DEFAULT_DB_CREDENTIAL_MAX_TTL_SECONDS, DEFAULT_TRANSIT_ALGORITHM, KEY_STATE_ACTIVE,
    KEY_STATE_VERIFYING, MAX_LIST_SECRETS, SIGNING_TRANSIT_ALGORITHM, STATE_ACTIVE, STATE_DELETED,
    STATE_DESTROYED, TOPIC_DB_CREDENTIAL_ISSUED, TOPIC_KEY_CREATED, TOPIC_KEY_ROTATED,
    TOPIC_SECRET_ACCESSED, TOPIC_SECRET_DELETED, TOPIC_SECRET_DESTROYED, TOPIC_SECRET_LISTED,
    TOPIC_SECRET_PUT, TOPIC_TRANSIT_DECRYPTED, TOPIC_TRANSIT_ENCRYPTED, TOPIC_TRANSIT_HMAC,
    TOPIC_TRANSIT_SIGNED, TOPIC_TRANSIT_VERIFIED, VAULT_DB_CREDENTIAL_LEASE_MSG,
    VAULT_ED25519_PREFIX, VAULT_HMAC_PREFIX, VAULT_SECRET_MSG, VAULT_TRANSIT_KEY_MSG,
};
use super::crypto::{
    DataKey, PlaintextSecret, constant_time_eq, dek_open, dek_seal, ed25519_public_key_b64,
    ed25519_sign_b64, ed25519_verify_b64, hmac_sha256, parse_ed25519_envelope, parse_mac_envelope,
    parse_transit_envelope, require_encryption_algorithm, require_signing_algorithm,
    transit_payload, unwrap_dek, validate_transit_algorithm, wrap_dek,
};
use super::dynamic::{
    create_postgres_login_role, drop_postgres_login_role, generate_db_password,
    generate_db_username, requested_db_credential_ttl, validate_db_role_alias,
    vault_db_role_configs,
};
use super::errors::{
    vault_confirmation_token_required_status, vault_db_credentials_config_status,
    vault_db_native_store_required_status, vault_field_violation, vault_internal_status,
    vault_required_key_name, vault_required_secret_path, vault_schema_already_exists_status,
    vault_schema_not_found_status,
};
use super::model::{active_transit, json_i64, json_object, json_str, transit_version};
use super::store::{
    db_credential_lease_record, secret_conflict, secret_list_read, secret_record,
    transit_key_conflict, transit_key_record,
};

// ── KV engine ─────────────────────────────────────────────────────────────

pub(crate) async fn put_secret(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::PutSecretRequest>,
) -> Result<Response<vault_pb::PutSecretResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // Seal gate FIRST: a sealed vault never serves a degraded write.
    svc.check_seal()?;
    // Cross-tenant guard: the body tenant_id must match the verified claim.
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let secret_path = req.secret_path.trim().to_string();
    if secret_path.is_empty() {
        return Err(vault_required_secret_path());
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let versions = svc
        .read_secret_versions(runtime, &context, &tenant_id, &secret_path)
        .await?;
    let current_latest = versions.iter().map(|s| s.version).max().unwrap_or(0);
    // Compare-and-swap: refuse to clobber a concurrent writer.
    if i64::from(req.expected_version) != current_latest {
        return Err(crate::runtime::executor_utils::retryable_aborted_status(
            "vault",
            "secret version CAS",
            0,
            format!(
                "CAS conflict: expected version {} but current latest is {current_latest}",
                req.expected_version
            ),
        ));
    }
    let new_version = current_latest + 1;

    // Envelope-encrypt: fresh DEK seals the value; master KEK wraps the DEK.
    let plaintext = PlaintextSecret(req.secret_value);
    let dek = DataKey::generate();
    let ciphertext = dek_seal(&dek, new_version, plaintext.0.as_bytes())?;
    let wrapped = wrap_dek(runtime, &dek)?;
    let metadata_json = non_empty_json(&req.metadata_json);

    runtime
        .native_entity_write_for_service(
            "vault",
            &context,
            VAULT_SECRET_MSG,
            secret_record(
                &Uuid::new_v4().to_string(),
                &tenant_id,
                &secret_path,
                new_version,
                &ciphertext,
                &wrapped,
                STATE_ACTIVE,
                &metadata_json,
            ),
            secret_conflict(),
        )
        .await?;

    svc.emit(
        TOPIC_SECRET_PUT,
        &secret_path,
        &tenant_id,
        &context.project_id,
        "put",
        &secret_path,
        serde_json::json!({
            "tenant_id": tenant_id,
            "secret_path": secret_path,
            "version": new_version,
        }),
    )
    .await;

    Ok(Response::new(vault_pb::PutSecretResponse {
        secret_path,
        version: new_version as i32,
        message: "secret stored".to_string(),
        error: None,
    }))
}

pub(crate) async fn get_secret(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::GetSecretRequest>,
) -> Result<Response<vault_pb::GetSecretResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let secret_path = req.secret_path.trim().to_string();
    if secret_path.is_empty() {
        return Err(vault_required_secret_path());
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let versions = svc
        .read_secret_versions(runtime, &context, &tenant_id, &secret_path)
        .await?;
    let selected = if req.version > 0 {
        versions
            .iter()
            .find(|s| s.version == i64::from(req.version) && s.state != STATE_DESTROYED)
    } else {
        versions
            .iter()
            .filter(|s| s.state == STATE_ACTIVE)
            .max_by_key(|s| s.version)
    };
    let secret = selected.ok_or_else(|| {
        vault_schema_not_found_status("get_secret", "vault_secret_not_found", "secret not found")
    })?;

    let dek = unwrap_dek(runtime, &secret.data_key_wrapped)?;
    let bytes = dek_open(&dek, transit_payload(&secret.ciphertext)?)?;
    let plaintext = PlaintextSecret(String::from_utf8(bytes).map_err(|_| {
        vault_internal_status(
            "get_secret_decode_plaintext",
            "vault secret is not valid UTF-8",
        )
    })?);

    // Sensitive READ — audit it (no plaintext in the payload).
    svc.emit(
        TOPIC_SECRET_ACCESSED,
        &secret_path,
        &tenant_id,
        &context.project_id,
        "read",
        &secret_path,
        serde_json::json!({
            "tenant_id": tenant_id,
            "secret_path": secret_path,
            "version": secret.version,
        }),
    )
    .await;

    Ok(Response::new(vault_pb::GetSecretResponse {
        secret_path,
        version: secret.version as i32,
        secret_value: plaintext.0,
        metadata_json: secret.metadata_json.clone(),
        message: "ok".to_string(),
        error: None,
    }))
}

pub(crate) async fn list_secrets(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::ListSecretsRequest>,
) -> Result<Response<vault_pb::ListSecretsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let rows = runtime
        .native_entity_read_for_service(
            "vault",
            &context,
            secret_list_read(&tenant_id, &req.path_prefix),
        )
        .await?;
    // Aggregate versions → one summary per path (its highest version + state).
    let mut by_path: std::collections::BTreeMap<String, (i64, String)> =
        std::collections::BTreeMap::new();
    for row in &rows {
        let map = json_object(row);
        let path = json_str(map, "secret_path");
        if path.is_empty() {
            continue;
        }
        let version = json_i64(map, "version");
        let state = json_str(map, "state");
        by_path
            .entry(path)
            .and_modify(|cur| {
                if version > cur.0 {
                    *cur = (version, state.clone());
                }
            })
            .or_insert((version, state));
    }
    let total_count = by_path.len() as i32;
    let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
    let secrets = by_path
        .into_iter()
        .skip(page_window.offset)
        .take(page_window.limit.min(MAX_LIST_SECRETS as usize))
        .map(
            |(secret_path, (latest_version, state))| vault_pb::SecretSummary {
                secret_path,
                latest_version: latest_version as i32,
                state,
            },
        )
        .collect::<Vec<_>>();
    let returned_count = secrets.len() as i64;

    // Fulfil the declared `udb.vault.secret.listed.v1` event contract (was
    // declared in the proto but never emitted). Metadata only — counts and the
    // requested prefix; NEVER any secret path value or secret material.
    svc.emit(
        TOPIC_SECRET_LISTED,
        &tenant_id,
        &tenant_id,
        &context.project_id,
        "list",
        req.path_prefix.trim(),
        serde_json::json!({
            "tenant_id": tenant_id,
            "path_prefix": req.path_prefix.trim(),
            "returned_count": returned_count,
            "total_count": total_count,
        }),
    )
    .await;

    Ok(Response::new(vault_pb::ListSecretsResponse {
        secrets,
        total_count,
        error: None,
        next_page_token: native_next_page_token_for_total(
            page_window.offset,
            page_window.limit.min(MAX_LIST_SECRETS as usize),
            total_count as i64,
        ),
    }))
}

pub(crate) async fn delete_secret(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::DeleteSecretRequest>,
) -> Result<Response<vault_pb::DeleteSecretResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let secret_path = req.secret_path.trim().to_string();
    if secret_path.is_empty() {
        return Err(vault_required_secret_path());
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let versions = svc
        .read_secret_versions(runtime, &context, &tenant_id, &secret_path)
        .await?;
    let Some(latest) = versions
        .iter()
        .filter(|s| s.state == STATE_ACTIVE)
        .max_by_key(|s| s.version)
    else {
        // Idempotent: nothing active to soft-delete.
        return Ok(Response::new(vault_pb::DeleteSecretResponse {
            message: "secret not found".to_string(),
            error: None,
        }));
    };
    // Soft delete: keep the ciphertext, flip the state on the same row.
    runtime
        .native_entity_write_for_service(
            "vault",
            &context,
            VAULT_SECRET_MSG,
            secret_record(
                &latest.secret_id,
                &tenant_id,
                &secret_path,
                latest.version,
                &latest.ciphertext,
                &latest.data_key_wrapped,
                STATE_DELETED,
                &latest.metadata_json,
            ),
            secret_conflict(),
        )
        .await?;

    svc.emit(
        TOPIC_SECRET_DELETED,
        &secret_path,
        &tenant_id,
        &context.project_id,
        "delete",
        &secret_path,
        serde_json::json!({
            "tenant_id": tenant_id,
            "secret_path": secret_path,
            "version": latest.version,
        }),
    )
    .await;

    Ok(Response::new(vault_pb::DeleteSecretResponse {
        message: "secret soft-deleted".to_string(),
        error: None,
    }))
}

pub(crate) async fn destroy_secret(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::DestroySecretRequest>,
) -> Result<Response<vault_pb::DestroySecretResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    // Irreversible: empty confirmation fails closed (DESTRUCTIVE).
    if req.confirmation_token.trim().is_empty() {
        return Err(vault_confirmation_token_required_status());
    }
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let secret_path = req.secret_path.trim().to_string();
    if secret_path.is_empty() {
        return Err(vault_required_secret_path());
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let versions = svc
        .read_secret_versions(runtime, &context, &tenant_id, &secret_path)
        .await?;
    let mut destroyed = 0u32;
    for secret in versions.iter().filter(|s| s.state != STATE_DESTROYED) {
        // Crypto-shred: clear the wrapped DEK + ciphertext so the value is
        // irrecoverable even if the row survives.
        runtime
            .native_entity_write_for_service(
                "vault",
                &context,
                VAULT_SECRET_MSG,
                secret_record(
                    &secret.secret_id,
                    &tenant_id,
                    &secret_path,
                    secret.version,
                    "",
                    "",
                    STATE_DESTROYED,
                    "{}",
                ),
                secret_conflict(),
            )
            .await?;
        destroyed += 1;
    }

    svc.emit(
        TOPIC_SECRET_DESTROYED,
        &secret_path,
        &tenant_id,
        &context.project_id,
        "destroy",
        &secret_path,
        serde_json::json!({
            "tenant_id": tenant_id,
            "secret_path": secret_path,
            "destroyed_versions": destroyed,
        }),
    )
    .await;

    Ok(Response::new(vault_pb::DestroySecretResponse {
        destroyed_versions: destroyed,
        message: "secret destroyed".to_string(),
        error: None,
    }))
}

// ── Transit engine ────────────────────────────────────────────────────────

pub(crate) async fn create_transit_key(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::CreateTransitKeyRequest>,
) -> Result<Response<vault_pb::CreateTransitKeyResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let key_name = req.key_name.trim().to_string();
    if key_name.is_empty() {
        return Err(vault_required_key_name());
    }
    // Honor the requested algorithm: accept only primitives the crypto stack
    // actually implements; reject anything else instead of silently coercing
    // it to the default. Does not alter primitive selection.
    let algorithm = validate_transit_algorithm(&req.algorithm)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let existing = svc
        .read_transit_versions(runtime, &context, &tenant_id, &key_name)
        .await?;
    if !existing.is_empty() {
        return Err(vault_schema_already_exists_status(
            "create_transit_key",
            "vault_transit_key_already_exists",
            "transit key already exists",
        ));
    }

    let dek = DataKey::generate();
    let wrapped = wrap_dek(runtime, &dek)?;
    runtime
        .native_entity_write_for_service(
            "vault",
            &context,
            VAULT_TRANSIT_KEY_MSG,
            transit_key_record(
                &Uuid::new_v4().to_string(),
                &tenant_id,
                &key_name,
                1,
                &algorithm,
                &wrapped,
                KEY_STATE_ACTIVE,
            ),
            transit_key_conflict(),
        )
        .await?;

    svc.emit(
        TOPIC_KEY_CREATED,
        &key_name,
        &tenant_id,
        &context.project_id,
        "create_key",
        &key_name,
        serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "version": 1}),
    )
    .await;

    Ok(Response::new(vault_pb::CreateTransitKeyResponse {
        key_name,
        version: 1,
        message: "transit key created".to_string(),
        error: None,
    }))
}

pub(crate) async fn rotate_transit_key(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::RotateTransitKeyRequest>,
) -> Result<Response<vault_pb::RotateTransitKeyResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let key_name = req.key_name.trim().to_string();
    if key_name.is_empty() {
        return Err(vault_required_key_name());
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let versions = svc
        .read_transit_versions(runtime, &context, &tenant_id, &key_name)
        .await?;
    if versions.is_empty() {
        return Err(vault_schema_not_found_status(
            "rotate_transit_key",
            "vault_transit_key_not_found",
            "transit key not found",
        ));
    }
    let max_version = versions.iter().map(|k| k.version).max().unwrap_or(0);
    let algorithm = active_transit(&versions)
        .map(|k| k.algorithm.clone())
        .unwrap_or_else(|| DEFAULT_TRANSIT_ALGORITHM.to_string());

    // Demote every current ACTIVE version to VERIFYING (rotation overlap).
    for key in versions.iter().filter(|k| k.state == KEY_STATE_ACTIVE) {
        runtime
            .native_entity_write_for_service(
                "vault",
                &context,
                VAULT_TRANSIT_KEY_MSG,
                transit_key_record(
                    &key.key_id,
                    &tenant_id,
                    &key_name,
                    key.version,
                    &key.algorithm,
                    &key.wrapped_key_material,
                    KEY_STATE_VERIFYING,
                ),
                transit_key_conflict(),
            )
            .await?;
    }

    let new_version = max_version + 1;
    let dek = DataKey::generate();
    let wrapped = wrap_dek(runtime, &dek)?;
    runtime
        .native_entity_write_for_service(
            "vault",
            &context,
            VAULT_TRANSIT_KEY_MSG,
            transit_key_record(
                &Uuid::new_v4().to_string(),
                &tenant_id,
                &key_name,
                new_version,
                &algorithm,
                &wrapped,
                KEY_STATE_ACTIVE,
            ),
            transit_key_conflict(),
        )
        .await?;

    svc.emit(
        TOPIC_KEY_ROTATED,
        &key_name,
        &tenant_id,
        &context.project_id,
        "rotate_key",
        &key_name,
        serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "version": new_version}),
    )
    .await;

    Ok(Response::new(vault_pb::RotateTransitKeyResponse {
        key_name,
        version: new_version as i32,
        message: "transit key rotated".to_string(),
        error: None,
    }))
}

pub(crate) async fn encrypt(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::EncryptRequest>,
) -> Result<Response<vault_pb::EncryptResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let key_name = req.key_name.trim().to_string();
    if key_name.is_empty() {
        return Err(vault_required_key_name());
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let versions = svc
        .read_transit_versions(runtime, &context, &tenant_id, &key_name)
        .await?;
    let active = active_transit(&versions).ok_or_else(|| {
        vault_schema_not_found_status(
            "encrypt",
            "vault_transit_active_key_not_found",
            "transit key not found or has no active version",
        )
    })?;
    require_encryption_algorithm(&active.algorithm, "encrypt")?;
    let dek = unwrap_dek(runtime, &active.wrapped_key_material)?;
    let plaintext = PlaintextSecret(req.plaintext);
    let ciphertext = dek_seal(&dek, active.version, plaintext.0.as_bytes())?;

    // Audit the crypto operation (no plaintext/ciphertext/key material — only
    // the tenant/key/version metadata).
    svc.emit(
        TOPIC_TRANSIT_ENCRYPTED,
        &key_name,
        &tenant_id,
        &context.project_id,
        "encrypt",
        &key_name,
        serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "key_version": active.version}),
    )
    .await;

    Ok(Response::new(vault_pb::EncryptResponse {
        ciphertext,
        key_version: active.version as i32,
        message: "ok".to_string(),
        error: None,
    }))
}

pub(crate) async fn decrypt(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::DecryptRequest>,
) -> Result<Response<vault_pb::DecryptResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let key_name = req.key_name.trim().to_string();
    if key_name.is_empty() {
        return Err(vault_required_key_name());
    }
    let (version, encoded) = parse_transit_envelope(&req.ciphertext).ok_or_else(|| {
        vault_field_violation(
            "ciphertext",
            "must match udb-vault:v<version>:<base64>",
            "not a vault transit ciphertext envelope",
        )
    })?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let versions = svc
        .read_transit_versions(runtime, &context, &tenant_id, &key_name)
        .await?;
    // ACTIVE + VERIFYING decrypt during the rotation overlap (signing-key model).
    let key = transit_version(&versions, version, &[KEY_STATE_ACTIVE, KEY_STATE_VERIFYING])
        .ok_or_else(|| {
            vault_schema_not_found_status(
                "decrypt",
                "vault_transit_key_version_not_found",
                "transit key version not found or retired",
            )
        })?;
    require_encryption_algorithm(&key.algorithm, "decrypt")?;
    let dek = unwrap_dek(runtime, &key.wrapped_key_material)?;
    let bytes = dek_open(&dek, encoded)?;
    let plaintext = PlaintextSecret(String::from_utf8(bytes).map_err(|_| {
        vault_internal_status(
            "decrypt_transit_plaintext",
            "decrypted payload is not valid UTF-8",
        )
    })?);

    // Sensitive READ — audit it (no plaintext in the payload).
    svc.emit(
        TOPIC_TRANSIT_DECRYPTED,
        &key_name,
        &tenant_id,
        &context.project_id,
        "decrypt",
        &key_name,
        serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "key_version": version}),
    )
    .await;

    Ok(Response::new(vault_pb::DecryptResponse {
        plaintext: plaintext.0,
        key_version: version as i32,
        message: "ok".to_string(),
        error: None,
    }))
}

pub(crate) async fn generate_data_key(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::GenerateDataKeyRequest>,
) -> Result<Response<vault_pb::GenerateDataKeyResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let key_name = req.key_name.trim().to_string();
    if key_name.is_empty() {
        return Err(vault_required_key_name());
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let versions = svc
        .read_transit_versions(runtime, &context, &tenant_id, &key_name)
        .await?;
    let active = active_transit(&versions).ok_or_else(|| {
        vault_schema_not_found_status(
            "generate_data_key",
            "vault_transit_active_key_not_found",
            "transit key not found or has no active version",
        )
    })?;
    // Envelope encryption: mint a fresh random 256-bit DEK, wrap it under the
    // transit key (same seal path as Encrypt). The plaintext DEK is returned ONCE
    // for the caller's local use and is never persisted broker-side.
    require_encryption_algorithm(&active.algorithm, "generate_data_key")?;
    let transit_dek = unwrap_dek(runtime, &active.wrapped_key_material)?;
    let new_dek = DataKey::generate();
    let ciphertext = dek_seal(&transit_dek, active.version, &new_dek.0)?;
    let plaintext = new_dek.to_b64();

    // Audit the key generation (no key material — only tenant/key/version).
    svc.emit(
        TOPIC_TRANSIT_ENCRYPTED,
        &key_name,
        &tenant_id,
        &context.project_id,
        "generate_data_key",
        &key_name,
        serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "key_version": active.version}),
    )
    .await;

    Ok(Response::new(vault_pb::GenerateDataKeyResponse {
        plaintext,
        ciphertext,
        key_version: active.version as i32,
        message: "ok".to_string(),
        error: None,
    }))
}

pub(crate) async fn rewrap(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::RewrapRequest>,
) -> Result<Response<vault_pb::RewrapResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let key_name = req.key_name.trim().to_string();
    if key_name.is_empty() {
        return Err(vault_required_key_name());
    }
    let (version, encoded) = parse_transit_envelope(&req.ciphertext).ok_or_else(|| {
        vault_field_violation(
            "ciphertext",
            "must match udb-vault:v<version>:<base64>",
            "not a vault transit ciphertext envelope",
        )
    })?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let versions = svc
        .read_transit_versions(runtime, &context, &tenant_id, &key_name)
        .await?;
    // Open with the version embedded in the envelope (ACTIVE + VERIFYING during a
    // rotation overlap), then re-seal under the CURRENT active version — the
    // post-rotation migration primitive. Operate on raw bytes: a rewrapped payload
    // may be a binary data key, not UTF-8.
    let old_key = transit_version(&versions, version, &[KEY_STATE_ACTIVE, KEY_STATE_VERIFYING])
        .ok_or_else(|| {
            vault_schema_not_found_status(
                "rewrap",
                "vault_transit_key_version_not_found",
                "transit key version not found or retired",
            )
        })?;
    require_encryption_algorithm(&old_key.algorithm, "rewrap")?;
    let old_dek = unwrap_dek(runtime, &old_key.wrapped_key_material)?;
    let raw = dek_open(&old_dek, encoded)?;

    let active = active_transit(&versions).ok_or_else(|| {
        vault_schema_not_found_status(
            "rewrap",
            "vault_transit_active_key_not_found",
            "transit key not found or has no active version",
        )
    })?;
    let active_dek = unwrap_dek(runtime, &active.wrapped_key_material)?;
    let ciphertext = dek_seal(&active_dek, active.version, &raw)?;

    // Audit the rewrap (no key material — only tenant/key/old+new version).
    svc.emit(
        TOPIC_TRANSIT_ENCRYPTED,
        &key_name,
        &tenant_id,
        &context.project_id,
        "rewrap",
        &key_name,
        serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "from_version": version, "key_version": active.version}),
    )
    .await;

    Ok(Response::new(vault_pb::RewrapResponse {
        ciphertext,
        key_version: active.version as i32,
        message: "ok".to_string(),
        error: None,
    }))
}

pub(crate) async fn get_transit_public_key(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::GetTransitPublicKeyRequest>,
) -> Result<Response<vault_pb::GetTransitPublicKeyResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let key_name = req.key_name.trim().to_string();
    if key_name.is_empty() {
        return Err(vault_required_key_name());
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let versions = svc
        .read_transit_versions(runtime, &context, &tenant_id, &key_name)
        .await?;
    if versions.is_empty() {
        return Err(vault_schema_not_found_status(
            "get_transit_public_key",
            "vault_transit_key_not_found",
            "transit key not found",
        ));
    }
    let algorithm = versions
        .first()
        .map(|key| key.algorithm.clone())
        .unwrap_or_default();
    // Only an Ed25519 signing key has a public half to export.
    require_signing_algorithm(&algorithm, "get_transit_public_key")?;

    // One public key per usable version (ACTIVE + the rotation-overlap VERIFYING),
    // so a verifier can check a signature produced under any live version. The
    // private seed never leaves the broker.
    let mut public_keys = Vec::new();
    for key in versions
        .iter()
        .filter(|key| key.state == KEY_STATE_ACTIVE || key.state == KEY_STATE_VERIFYING)
    {
        let dek = unwrap_dek(runtime, &key.wrapped_key_material)?;
        public_keys.push(vault_pb::TransitPublicKey {
            version: key.version as i32,
            public_key: ed25519_public_key_b64(&dek.0),
            state: key.state.clone(),
        });
    }

    Ok(Response::new(vault_pb::GetTransitPublicKeyResponse {
        key_name,
        algorithm,
        public_keys,
        message: "ok".to_string(),
        error: None,
    }))
}

pub(crate) async fn sign(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::SignRequest>,
) -> Result<Response<vault_pb::SignResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let key_name = req.key_name.trim().to_string();
    if key_name.is_empty() {
        return Err(vault_required_key_name());
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let versions = svc
        .read_transit_versions(runtime, &context, &tenant_id, &key_name)
        .await?;
    let active = active_transit(&versions).ok_or_else(|| {
        vault_schema_not_found_status(
            "sign",
            "vault_transit_active_key_not_found",
            "transit key not found or has no active version",
        )
    })?;
    let dek = unwrap_dek(runtime, &active.wrapped_key_material)?;
    // A key created with the Ed25519 algorithm produces a real asymmetric
    // signature (the 32-byte material is the seed); every other key keeps the
    // symmetric HMAC. The envelope prefix records which, so Verify dispatches
    // correctly. Both preserve the existing behavior for existing keys.
    let signature = if active.algorithm == SIGNING_TRANSIT_ALGORITHM {
        format!(
            "{VAULT_ED25519_PREFIX}{}:{}",
            active.version,
            ed25519_sign_b64(&dek.0, req.input.as_bytes())
        )
    } else {
        format!(
            "{VAULT_HMAC_PREFIX}{}:{}",
            active.version,
            BASE64_STANDARD.encode(hmac_sha256(&dek.0, req.input.as_bytes()))
        )
    };

    // Audit the crypto operation (no input/signature/key material — only the
    // tenant/key/version metadata).
    svc.emit(
        TOPIC_TRANSIT_SIGNED,
        &key_name,
        &tenant_id,
        &context.project_id,
        "sign",
        &key_name,
        serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "key_version": active.version}),
    )
    .await;

    Ok(Response::new(vault_pb::SignResponse {
        signature,
        key_version: active.version as i32,
        message: "ok".to_string(),
        error: None,
    }))
}

pub(crate) async fn verify(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::VerifyRequest>,
) -> Result<Response<vault_pb::VerifyResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let key_name = req.key_name.trim().to_string();
    if key_name.is_empty() {
        return Err(vault_required_key_name());
    }
    // Accept either an Ed25519 signature envelope or the HMAC envelope; the
    // prefix selects the verification primitive below (an old HMAC signature
    // still verifies exactly as before).
    let Some((version, signature_b64, is_ed25519)) = parse_ed25519_envelope(&req.signature)
        .map(|(v, s)| (v, s, true))
        .or_else(|| parse_mac_envelope(&req.signature).map(|(v, s)| (v, s, false)))
    else {
        return Ok(Response::new(vault_pb::VerifyResponse {
            valid: false,
            message: "not a vault signature envelope".to_string(),
            error: None,
        }));
    };
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let versions = svc
        .read_transit_versions(runtime, &context, &tenant_id, &key_name)
        .await?;
    let key = transit_version(&versions, version, &[KEY_STATE_ACTIVE, KEY_STATE_VERIFYING])
        .ok_or_else(|| {
            vault_schema_not_found_status(
                "verify",
                "vault_transit_key_version_not_found",
                "transit key version not found or retired",
            )
        })?;
    let dek = unwrap_dek(runtime, &key.wrapped_key_material)?;
    let valid = if is_ed25519 {
        ed25519_verify_b64(&dek.0, req.input.as_bytes(), signature_b64)
    } else {
        let expected = hmac_sha256(&dek.0, req.input.as_bytes());
        let provided = BASE64_STANDARD.decode(signature_b64).unwrap_or_default();
        constant_time_eq(&expected, &provided)
    };

    // Audit the verification (no input/signature/key material — only the
    // tenant/key/version metadata and the boolean outcome).
    svc.emit(
        TOPIC_TRANSIT_VERIFIED,
        &key_name,
        &tenant_id,
        &context.project_id,
        "verify",
        &key_name,
        serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "key_version": version, "valid": valid}),
    )
    .await;

    Ok(Response::new(vault_pb::VerifyResponse {
        valid,
        message: if valid { "valid" } else { "invalid" }.to_string(),
        error: None,
    }))
}

pub(crate) async fn hmac(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::HmacRequest>,
) -> Result<Response<vault_pb::HmacResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let key_name = req.key_name.trim().to_string();
    if key_name.is_empty() {
        return Err(vault_required_key_name());
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let versions = svc
        .read_transit_versions(runtime, &context, &tenant_id, &key_name)
        .await?;
    let active = active_transit(&versions).ok_or_else(|| {
        vault_schema_not_found_status(
            "hmac",
            "vault_transit_active_key_not_found",
            "transit key not found or has no active version",
        )
    })?;
    let dek = unwrap_dek(runtime, &active.wrapped_key_material)?;
    let mac = hmac_sha256(&dek.0, req.input.as_bytes());
    let hmac_value = format!(
        "{VAULT_HMAC_PREFIX}{}:{}",
        active.version,
        BASE64_STANDARD.encode(mac)
    );

    // Audit the crypto operation (no input/MAC/key material — only the
    // tenant/key/version metadata).
    svc.emit(
        TOPIC_TRANSIT_HMAC,
        &key_name,
        &tenant_id,
        &context.project_id,
        "hmac",
        &key_name,
        serde_json::json!({"tenant_id": tenant_id, "key_name": key_name, "key_version": active.version}),
    )
    .await;

    Ok(Response::new(vault_pb::HmacResponse {
        hmac: hmac_value,
        key_version: active.version as i32,
        message: "ok".to_string(),
        error: None,
    }))
}

// ── Seal engine ─────────────────────────────────────────────────────────

pub(crate) async fn seal_status(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::SealStatusRequest>,
) -> Result<Response<vault_pb::SealStatusResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // SealStatus REPORTS the seal state — it must answer even when sealed, so
    // it does not call the seal gate. It still enforces the tenant scope.
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let sealed = svc.is_sealed();
    let kek_configured = svc.kek_configured();
    Ok(Response::new(vault_pb::SealStatusResponse {
        sealed,
        kek_configured,
        message: if sealed {
            "vault is sealed: master key unavailable".to_string()
        } else if kek_configured {
            "vault is unsealed (master KEK configured)".to_string()
        } else {
            "vault is unsealed (dev passthrough — no master KEK configured)".to_string()
        },
        error: None,
    }))
}

// ── Dynamic database credentials ─────────────────────────────────────────

pub(crate) async fn generate_database_credentials(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::GenerateDatabaseCredentialsRequest>,
) -> Result<Response<vault_pb::GenerateDatabaseCredentialsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let role_name = req.role_name.trim().to_string();
    validate_db_role_alias(&role_name)?;

    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let pool = svc
        .pg_pool
        .as_ref()
        .ok_or_else(vault_db_native_store_required_status)?;
    let role_config = vault_db_role_configs()?.get(&role_name).ok_or_else(|| {
        vault_db_credentials_config_status(format!(
            "vault dynamic database role '{role_name}' is not configured"
        ))
    })?;
    let max_ttl = role_config
        .ttl_seconds_max
        .unwrap_or(DEFAULT_DB_CREDENTIAL_MAX_TTL_SECONDS);
    let ttl_seconds = requested_db_credential_ttl(req.ttl_seconds, max_ttl)?;
    let issued_at = Utc::now();
    let expires_at = issued_at + chrono::Duration::seconds(i64::from(ttl_seconds));
    let lease_id = Uuid::new_v4().to_string();
    let username = generate_db_username();
    let password = generate_db_password();

    create_postgres_login_role(
        pool,
        &username,
        &password,
        expires_at,
        &role_config.parent_role,
    )
    .await?;

    let context = native_service_context(&metadata, &tenant_id, "");
    let metadata_json = serde_json::json!({
        "role_name": &role_name,
        "parent_role": &role_config.parent_role,
        "ttl_seconds": ttl_seconds,
        "lease_id": &lease_id,
    })
    .to_string();
    let write = runtime
        .native_entity_write_for_service(
            "vault",
            &context,
            VAULT_DB_CREDENTIAL_LEASE_MSG,
            db_credential_lease_record(
                &lease_id,
                &tenant_id,
                &role_name,
                &username,
                &role_config.parent_role,
                issued_at,
                expires_at,
                &metadata_json,
            ),
            ConflictStrategy::Error,
        )
        .await;
    if let Err(err) = write {
        if let Err(drop_err) = drop_postgres_login_role(pool, &username).await {
            tracing::warn!(
                lease_id = %lease_id,
                username = %username,
                error = %drop_err,
                "vault DB credential cleanup failed after lease write failure"
            );
        }
        return Err(err);
    }

    svc.emit(
        TOPIC_DB_CREDENTIAL_ISSUED,
        &lease_id,
        &tenant_id,
        "",
        "vault.GenerateDatabaseCredentials",
        &format!("vault/database/credentials/{role_name}"),
        serde_json::json!({
            "lease_id": &lease_id,
            "role_name": &role_name,
            "username": &username,
            "expires_at": expires_at.to_rfc3339(),
        }),
    )
    .await;

    Ok(Response::new(
        vault_pb::GenerateDatabaseCredentialsResponse {
            username,
            password,
            lease_id,
            lease_ttl_seconds: ttl_seconds,
            message: "dynamic database credentials issued".to_string(),
            error: None,
        },
    ))
}
