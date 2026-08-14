//! Durable lifecycle for Vault dynamic database credentials.
//!
//! Issuance is claimed by `(tenant_id, project_id, idempotency_key)` before any
//! physical role exists. The password is retained only as a master-KEK envelope,
//! allowing an authenticated identical replay to recover the original response.
//! Revocation first records durable intent, then terminates sessions, drops the
//! role, proves absence, and atomically records REVOKED plus its outbox evidence.

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::vault::services::v1 as vault_pb;
use crate::runtime::channels::OperationChannel;
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::super::native_helpers::{
    NativeEventContext, admit_on as native_admit_on, enqueue_outbox_event_in_tx,
    project_scoped_native_service_context, validate_request_scope,
};
use super::VaultServiceImpl;
use super::config::{
    DB_LEASE_ACTIVE, DB_LEASE_FAILED, DB_LEASE_REVOKED, DB_LEASE_REVOKING, DB_LEASE_STARTING,
    TOPIC_DB_CREDENTIAL_ISSUED, TOPIC_DB_CREDENTIAL_REVOKED, VAULT_DB_CREDENTIAL_LEASE_MSG,
    vault_db_lease_reaper_batch,
};
use super::dynamic::{
    create_postgres_login_role, drop_postgres_login_role, generate_db_password,
    generate_db_username, postgres_role_exists, requested_db_credential_ttl,
    terminate_postgres_login_sessions, validate_db_role_alias, vault_db_role_configs,
};
use super::errors::{
    vault_db_idempotency_conflict_status, vault_db_lease_not_found_status,
    vault_db_reconciliation_status, vault_field_violation, vault_internal_status,
    vault_required_field,
};

const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;
const MAX_REVOCATION_REASON_BYTES: usize = 2_048;
const STARTING_RECONCILIATION_GRACE_SECONDS: i32 = 30;

#[derive(Clone)]
pub(crate) struct DbCredentialLease {
    pub(crate) lease_id: String,
    pub(crate) tenant_id: String,
    pub(crate) project_id: String,
    pub(crate) role_name: String,
    pub(crate) username: String,
    pub(crate) expires_at: DateTime<Utc>,
    pub(crate) state: String,
    pub(crate) request_hash: String,
    pub(crate) credential_ciphertext: String,
    pub(crate) target_instance: String,
    pub(crate) revocation_operation_id: String,
    pub(crate) revocation_requested: bool,
}

impl std::fmt::Debug for DbCredentialLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbCredentialLease")
            .field("lease_id", &self.lease_id)
            .field("tenant_id", &self.tenant_id)
            .field("project_id", &self.project_id)
            .field("role_name", &self.role_name)
            .field("username", &self.username)
            .field("state", &self.state)
            .field("expires_at", &self.expires_at)
            .field("credential_ciphertext", &"[redacted]")
            .finish()
    }
}

fn lease_model() -> NativeModel {
    native_model(
        VAULT_DB_CREDENTIAL_LEASE_MSG,
        &[
            "lease_id",
            "tenant_id",
            "project_id",
            "role_name",
            "username",
            "parent_role",
            "issued_at",
            "expires_at",
            "revoked_at",
            "state",
            "metadata_json",
            "idempotency_key",
            "request_hash",
            "credential_ciphertext",
            "target_instance",
            "last_error",
            "revoke_reason",
            "revocation_operation_id",
            "revocation_requested_at",
        ],
    )
}

fn lease_projection(model: &NativeModel) -> String {
    [
        model.text_as("lease_id", "lease_id"),
        model.text_as("tenant_id", "tenant_id"),
        model.text_as("project_id", "project_id"),
        model.text_as("role_name", "role_name"),
        model.text_as("username", "username"),
        format!("{} AS expires_at", model.q("expires_at")),
        model.text_as("state", "state"),
        model.text_as("request_hash", "request_hash"),
        model.text_as("credential_ciphertext", "credential_ciphertext"),
        model.text_as("target_instance", "target_instance"),
        format!(
            "COALESCE({}::text, '') AS revocation_operation_id",
            model.q("revocation_operation_id")
        ),
        format!(
            "({} IS NOT NULL) AS revocation_requested",
            model.q("revocation_requested_at")
        ),
    ]
    .join(", ")
}

fn lease_from_row(row: &sqlx::postgres::PgRow) -> Result<DbCredentialLease, Status> {
    let field = |name: &str, err: sqlx::Error| {
        vault_internal_status(
            "database_credential_lease_decode",
            format!("database credential lease field '{name}' is invalid: {err}"),
        )
    };
    Ok(DbCredentialLease {
        lease_id: row
            .try_get("lease_id")
            .map_err(|err| field("lease_id", err))?,
        tenant_id: row
            .try_get("tenant_id")
            .map_err(|err| field("tenant_id", err))?,
        project_id: row
            .try_get("project_id")
            .map_err(|err| field("project_id", err))?,
        role_name: row
            .try_get("role_name")
            .map_err(|err| field("role_name", err))?,
        username: row
            .try_get("username")
            .map_err(|err| field("username", err))?,
        expires_at: row
            .try_get("expires_at")
            .map_err(|err| field("expires_at", err))?,
        state: row.try_get("state").map_err(|err| field("state", err))?,
        request_hash: row
            .try_get("request_hash")
            .map_err(|err| field("request_hash", err))?,
        credential_ciphertext: row
            .try_get("credential_ciphertext")
            .map_err(|err| field("credential_ciphertext", err))?,
        target_instance: row
            .try_get("target_instance")
            .map_err(|err| field("target_instance", err))?,
        revocation_operation_id: row
            .try_get("revocation_operation_id")
            .map_err(|err| field("revocation_operation_id", err))?,
        revocation_requested: row
            .try_get("revocation_requested")
            .map_err(|err| field("revocation_requested", err))?,
    })
}

pub(crate) async fn load_lease_by_idempotency(
    pool: &PgPool,
    tenant_id: &str,
    project_id: &str,
    idempotency_key: &str,
) -> Result<Option<DbCredentialLease>, Status> {
    let model = lease_model();
    let sql = format!(
        "SELECT {} FROM {} WHERE {} = $1 AND {} = $2 AND {} = $3",
        lease_projection(&model),
        model.relation,
        model.q("tenant_id"),
        model.q("project_id"),
        model.q("idempotency_key"),
    );
    sqlx::query(&sql)
        .bind(tenant_id)
        .bind(project_id)
        .bind(idempotency_key)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            vault_internal_status(
                "database_credential_idempotency_lookup",
                format!("database credential replay lookup failed: {err}"),
            )
        })?
        .as_ref()
        .map(lease_from_row)
        .transpose()
}

pub(crate) async fn load_lease_by_id(
    pool: &PgPool,
    tenant_id: &str,
    project_id: &str,
    lease_id: &str,
) -> Result<Option<DbCredentialLease>, Status> {
    let model = lease_model();
    let sql = format!(
        "SELECT {} FROM {} WHERE {} = $1::uuid AND {} = $2 AND {} = $3",
        lease_projection(&model),
        model.relation,
        model.q("lease_id"),
        model.q("tenant_id"),
        model.q("project_id"),
    );
    sqlx::query(&sql)
        .bind(lease_id)
        .bind(tenant_id)
        .bind(project_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            vault_internal_status(
                "database_credential_lease_lookup",
                format!("database credential lease lookup failed: {err}"),
            )
        })?
        .as_ref()
        .map(lease_from_row)
        .transpose()
}

pub(crate) async fn load_reconciliation_candidates(
    pool: &PgPool,
    batch: i64,
) -> Result<Vec<DbCredentialLease>, Status> {
    let model = lease_model();
    let sql = format!(
        "SELECT {} FROM {} WHERE \
         ({} = $2 AND {} <= NOW() - make_interval(secs => $6)) OR \
         ({} = $3 AND {} <= NOW()) OR {} = $4 OR \
         ({} = $5 AND {} IS NOT NULL) \
         ORDER BY {} ASC LIMIT $1",
        lease_projection(&model),
        model.relation,
        model.q("state"),
        model.q("issued_at"),
        model.q("state"),
        model.q("expires_at"),
        model.q("state"),
        model.q("state"),
        model.q("revocation_requested_at"),
        model.q("issued_at"),
    );
    let rows = sqlx::query(&sql)
        .bind(batch.clamp(1, vault_db_lease_reaper_batch()))
        .bind(DB_LEASE_STARTING)
        .bind(DB_LEASE_ACTIVE)
        .bind(DB_LEASE_REVOKING)
        .bind(DB_LEASE_FAILED)
        .bind(STARTING_RECONCILIATION_GRACE_SECONDS)
        .fetch_all(pool)
        .await
        .map_err(|err| {
            vault_internal_status(
                "database_credential_reconciliation_load",
                format!("database credential reconciliation load failed: {err}"),
            )
        })?;
    rows.iter().map(lease_from_row).collect()
}

fn issuance_request_hash(
    tenant_id: &str,
    project_id: &str,
    role_name: &str,
    policy_revision: &str,
    ttl_seconds: i32,
    target_instance: &str,
) -> String {
    crate::runtime::executor_utils::checksum_str(
        &serde_json::json!({
            "v": 1,
            "operation": "vault.generate_database_credentials",
            "tenant_id": tenant_id,
            "project_id": project_id,
            "role_name": role_name,
            "policy_revision": policy_revision,
            "ttl_seconds": ttl_seconds,
            "target_instance": target_instance,
        })
        .to_string(),
    )
}

fn response_from_lease(
    svc: &VaultServiceImpl,
    lease: &DbCredentialLease,
    replayed: bool,
) -> Result<vault_pb::GenerateDatabaseCredentialsResponse, Status> {
    if lease.state != DB_LEASE_ACTIVE {
        return Err(vault_db_reconciliation_status(format!(
            "database credential lease {} is in {} and cannot be recovered until reconciliation completes",
            lease.lease_id, lease.state
        )));
    }
    if lease.expires_at <= Utc::now() {
        return Err(vault_db_reconciliation_status(format!(
            "database credential lease {} has expired and is pending revocation",
            lease.lease_id
        )));
    }
    let runtime = svc.require_runtime()?;
    if !lease.credential_ciphertext.starts_with("udb-aead:") {
        return Err(vault_internal_status(
            "database_credential_recovery",
            "database credential recovery envelope is not protected by the master KEK",
        ));
    }
    let password = runtime
        .decrypt_secret_at_rest(&lease.credential_ciphertext)
        .map_err(|err| {
            vault_internal_status(
                "database_credential_recovery",
                format!("database credential recovery envelope could not be opened: {err}"),
            )
        })?;
    let remaining = (lease.expires_at - Utc::now())
        .num_seconds()
        .clamp(0, i64::from(i32::MAX)) as i32;
    Ok(vault_pb::GenerateDatabaseCredentialsResponse {
        username: lease.username.clone(),
        password,
        lease_id: lease.lease_id.clone(),
        lease_ttl_seconds: remaining,
        message: if replayed {
            "original database credential response recovered from the idempotent lease".to_string()
        } else {
            "database credentials issued".to_string()
        },
        replayed,
        state: lease.state.clone(),
        error: None,
    })
}

fn event_context(operation: &str, lease_id: &str) -> NativeEventContext {
    NativeEventContext {
        actor: crate::runtime::otel::current_actor(),
        auth_method: crate::runtime::otel::current_auth_method(),
        operation: operation.to_string(),
        outcome: "success".to_string(),
        correlation_id: lease_id.to_string(),
        target_resource: format!("vault_db_credential_lease:{lease_id}"),
        ..NativeEventContext::default()
    }
}

pub(crate) async fn activate_lease(
    pool: &PgPool,
    outbox_relation: Option<&str>,
    lease: &DbCredentialLease,
    actor_operation: &str,
) -> Result<bool, Status> {
    let model = lease_model();
    let mut tx = pool.begin().await.map_err(|err| {
        vault_internal_status(
            "database_credential_activate_begin",
            format!("database credential activation transaction failed: {err}"),
        )
    })?;
    let sql = format!(
        "UPDATE {} SET {} = $2, {} = '', {} = NULL WHERE {} = $1::uuid AND {} = $3",
        model.relation,
        model.q("state"),
        model.q("last_error"),
        model.q("revoked_at"),
        model.q("lease_id"),
        model.q("state"),
    );
    let updated = sqlx::query(&sql)
        .bind(&lease.lease_id)
        .bind(DB_LEASE_ACTIVE)
        .bind(DB_LEASE_STARTING)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            vault_internal_status(
                "database_credential_activate",
                format!("database credential activation failed: {err}"),
            )
        })?;
    if updated.rows_affected() == 0 {
        tx.rollback().await.ok();
        return Ok(false);
    }
    enqueue_outbox_event_in_tx(
        &mut *tx,
        outbox_relation,
        TOPIC_DB_CREDENTIAL_ISSUED,
        &lease.lease_id,
        &lease.tenant_id,
        &lease.project_id,
        serde_json::json!({
            "lease_id": lease.lease_id,
            "tenant_id": lease.tenant_id,
            "project_id": lease.project_id,
            "role_name": lease.role_name,
            "username": lease.username,
            "expires_at": lease.expires_at.to_rfc3339(),
            "state": DB_LEASE_ACTIVE,
        }),
        event_context(actor_operation, &lease.lease_id),
    )
    .await
    .map_err(|err| vault_internal_status("database_credential_issued_outbox", err))?;
    tx.commit().await.map_err(|err| {
        vault_internal_status(
            "database_credential_activate_commit",
            format!("database credential activation commit failed: {err}"),
        )
    })?;
    Ok(true)
}

pub(crate) async fn mark_lease_failed(
    pool: &PgPool,
    lease_id: &str,
    error: &str,
) -> Result<(), Status> {
    let model = lease_model();
    let sql = format!(
        "UPDATE {} SET {} = $2, {} = $3 WHERE {} = $1::uuid AND {} <> $4",
        model.relation,
        model.q("state"),
        model.q("last_error"),
        model.q("lease_id"),
        model.q("state"),
    );
    sqlx::query(&sql)
        .bind(lease_id)
        .bind(DB_LEASE_FAILED)
        .bind(error)
        .bind(DB_LEASE_REVOKED)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|err| {
            vault_internal_status(
                "database_credential_mark_failed",
                format!("database credential FAILED transition failed: {err}"),
            )
        })
}

pub(crate) async fn generate_database_credentials(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::GenerateDatabaseCredentialsRequest>,
) -> Result<Response<vault_pb::GenerateDatabaseCredentialsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    svc.check_seal()?;
    validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let role_name = req.role_name.trim().to_string();
    validate_db_role_alias(&role_name)?;
    let idempotency_key = req.idempotency_key.trim().to_string();
    if idempotency_key.is_empty() {
        return Err(vault_required_field(
            "idempotency_key",
            "must be a non-empty caller-supplied replay key",
            "idempotency_key is required",
        ));
    }
    if idempotency_key.len() > MAX_IDEMPOTENCY_KEY_BYTES {
        return Err(vault_field_violation(
            "idempotency_key",
            "must not exceed 128 bytes",
            "idempotency_key exceeds the maximum length",
        ));
    }
    let roles = vault_db_role_configs()?;
    let role = roles.get(&role_name).ok_or_else(|| {
        super::errors::vault_db_credentials_config_status(format!(
            "vault dynamic database role '{role_name}' is not configured"
        ))
    })?;
    let ttl = requested_db_credential_ttl(
        req.ttl_seconds,
        role.ttl_seconds_max
            .unwrap_or(super::config::DEFAULT_DB_CREDENTIAL_MAX_TTL_SECONDS),
    )?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Admin,
        &tenant_id,
        Some(req.project_id.trim()),
    )
    .await?;
    let mut context = project_scoped_native_service_context(&metadata, &tenant_id);
    if context.project_id.trim().is_empty() && !req.project_id.trim().is_empty() {
        context.project_id = req.project_id.trim().to_string();
    }
    let (context, pool) =
        svc.resolve_project_store(context, true, "generate_database_credentials")?;
    let request_hash = issuance_request_hash(
        &tenant_id,
        &context.project_id,
        &role_name,
        &role.policy_revision,
        ttl,
        &context.target_instance,
    );
    if let Some(existing) =
        load_lease_by_idempotency(&pool, &tenant_id, &context.project_id, &idempotency_key).await?
    {
        if existing.request_hash != request_hash {
            return Err(vault_db_idempotency_conflict_status());
        }
        return response_from_lease(svc, &existing, true).map(Response::new);
    }

    let lease_id = Uuid::new_v4().to_string();
    let username = generate_db_username();
    let password = generate_db_password();
    let runtime = svc.require_runtime()?;
    let credential_ciphertext = runtime.encrypt_secret_at_rest(&password).map_err(|err| {
        vault_internal_status(
            "database_credential_recovery_seal",
            format!("database credential recovery envelope could not be sealed: {err}"),
        )
    })?;
    if !credential_ciphertext.starts_with("udb-aead:") {
        return Err(vault_internal_status(
            "database_credential_recovery_seal",
            "database credential recovery envelope was not protected by the master KEK",
        ));
    }
    let issued_at = Utc::now();
    let expires_at = issued_at + chrono::Duration::seconds(i64::from(ttl));
    let model = lease_model();
    let insert_sql = format!(
        "INSERT INTO {} ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}) \
         VALUES ($1::uuid, $2, $3, $4, $5, $6, 'postgres', $7, $8, $9, $10, $11, $12, $13, '{{}}'::jsonb) \
         ON CONFLICT ({}, {}, {}) WHERE {} <> '' DO NOTHING",
        model.relation,
        model.q("lease_id"),
        model.q("tenant_id"),
        model.q("project_id"),
        model.q("role_name"),
        model.q("username"),
        model.q("parent_role"),
        model.q("backend"),
        model.q("issued_at"),
        model.q("expires_at"),
        model.q("state"),
        model.q("idempotency_key"),
        model.q("request_hash"),
        model.q("credential_ciphertext"),
        model.q("target_instance"),
        model.q("metadata_json"),
        model.q("tenant_id"),
        model.q("project_id"),
        model.q("idempotency_key"),
        model.q("idempotency_key"),
    );
    let mut authority_tx = pool.begin().await.map_err(|err| {
        vault_internal_status(
            "database_credential_authority_begin",
            format!("database credential authority transaction failed: {err}"),
        )
    })?;
    let inserted = sqlx::query(&insert_sql)
        .bind(&lease_id)
        .bind(&tenant_id)
        .bind(&context.project_id)
        .bind(&role_name)
        .bind(&username)
        .bind("")
        .bind(issued_at)
        .bind(expires_at)
        .bind(DB_LEASE_STARTING)
        .bind(&idempotency_key)
        .bind(&request_hash)
        .bind(&credential_ciphertext)
        .bind(&context.target_instance)
        .execute(&mut *authority_tx)
        .await
        .map_err(|err| {
            vault_internal_status(
                "database_credential_starting",
                format!("database credential STARTING claim failed: {err}"),
            )
        })?;
    if inserted.rows_affected() == 0 {
        authority_tx.rollback().await.ok();
        let existing = load_lease_by_idempotency(
            &pool,
            &tenant_id,
            &context.project_id,
            &idempotency_key,
        )
        .await?
        .ok_or_else(|| {
            vault_db_reconciliation_status(
                "database credential idempotency claim raced but no durable winner is visible",
            )
        })?;
        if existing.request_hash != request_hash {
            return Err(vault_db_idempotency_conflict_status());
        }
        return response_from_lease(svc, &existing, true).map(Response::new);
    }
    let mut lease = DbCredentialLease {
        lease_id,
        tenant_id,
        project_id: context.project_id,
        role_name,
        username,
        expires_at,
        state: DB_LEASE_STARTING.to_string(),
        request_hash,
        credential_ciphertext,
        target_instance: context.target_instance,
        revocation_operation_id: String::new(),
        revocation_requested: false,
    };
    let provenance = match create_postgres_login_role(
        authority_tx.as_mut(),
        &lease.username,
        &password,
        lease.expires_at,
        &lease.tenant_id,
        &lease.project_id,
        &lease.target_instance,
        role,
    )
    .await
    {
        Ok(provenance) => provenance,
        Err(err) => {
            authority_tx.rollback().await.ok();
            return Err(err);
        }
    };
    let metadata_json = serde_json::json!({
        "authority": "postgres_direct_restrictive_rls_v1",
        "tenant_id": &lease.tenant_id,
        "project_id": &lease.project_id,
        "role_name": &lease.role_name,
        "ttl_seconds": ttl,
        "lease_id": &lease.lease_id,
        "physical_target": &provenance,
    });
    let activate_sql = format!(
        "UPDATE {} SET {} = $2, {} = $3, {} = '', {} = NULL \
         WHERE {} = $1::uuid AND {} = $4",
        model.relation,
        model.q("state"),
        model.q("metadata_json"),
        model.q("last_error"),
        model.q("revoked_at"),
        model.q("lease_id"),
        model.q("state"),
    );
    let activated = sqlx::query(&activate_sql)
        .bind(&lease.lease_id)
        .bind(DB_LEASE_ACTIVE)
        .bind(&metadata_json)
        .bind(DB_LEASE_STARTING)
        .execute(&mut *authority_tx)
        .await
        .map_err(|err| {
            vault_internal_status(
                "database_credential_activate",
                format!("database credential ACTIVE transition failed: {err}"),
            )
        })?;
    if activated.rows_affected() != 1 {
        authority_tx.rollback().await.ok();
        return Err(vault_db_reconciliation_status(
            "database credential STARTING claim was lost before activation",
        ));
    }
    enqueue_outbox_event_in_tx(
        &mut *authority_tx,
        svc.outbox_relation.as_deref(),
        TOPIC_DB_CREDENTIAL_ISSUED,
        &lease.lease_id,
        &lease.tenant_id,
        &lease.project_id,
        serde_json::json!({
            "lease_id": &lease.lease_id,
            "tenant_id": &lease.tenant_id,
            "project_id": &lease.project_id,
            "role_name": &lease.role_name,
            "username": &lease.username,
            "target_instance": &lease.target_instance,
            "database_name": &provenance.database_name,
            "policy_revision": &provenance.policy_revision,
            "policy_sha256": &provenance.policy_sha256,
            "expires_at": lease.expires_at.to_rfc3339(),
            "state": DB_LEASE_ACTIVE,
        }),
        event_context("generate_database_credentials", &lease.lease_id),
    )
    .await
    .map_err(|err| vault_internal_status("database_credential_issued_outbox", err))?;
    authority_tx.commit().await.map_err(|err| {
        vault_internal_status(
            "database_credential_authority_commit",
            format!(
                "database credential authority commit outcome is unknown; replay the same idempotency_key: {err}"
            ),
        )
    })?;
    lease.state = DB_LEASE_ACTIVE.to_string();
    response_from_lease(svc, &lease, false).map(Response::new)
}

pub(crate) async fn transition_to_revoking(
    pool: &PgPool,
    lease_id: &str,
    operation_id: &str,
    reason: &str,
) -> Result<(), Status> {
    let model = lease_model();
    let sql = format!(
        "UPDATE {} SET {} = $2, {} = $3::uuid, {} = NOW(), {} = $4, {} = '' \
         WHERE {} = $1::uuid AND {} <> $5",
        model.relation,
        model.q("state"),
        model.q("revocation_operation_id"),
        model.q("revocation_requested_at"),
        model.q("revoke_reason"),
        model.q("last_error"),
        model.q("lease_id"),
        model.q("state"),
    );
    sqlx::query(&sql)
        .bind(lease_id)
        .bind(DB_LEASE_REVOKING)
        .bind(operation_id)
        .bind(reason)
        .bind(DB_LEASE_REVOKED)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|err| {
            vault_internal_status(
                "database_credential_revoking",
                format!("database credential REVOKING transition failed: {err}"),
            )
        })
}

pub(crate) async fn finalize_revocation(
    pool: &PgPool,
    outbox_relation: Option<&str>,
    lease: &DbCredentialLease,
    operation_id: &str,
    operation: &str,
) -> Result<(), Status> {
    terminate_postgres_login_sessions(pool, &lease.username)
        .await
        .map_err(|err| vault_db_reconciliation_status(err))?;
    drop_postgres_login_role(pool, &lease.username)
        .await
        .map_err(vault_db_reconciliation_status)?;
    if postgres_role_exists(pool, &lease.username)
        .await
        .map_err(vault_db_reconciliation_status)?
    {
        return Err(vault_db_reconciliation_status(format!(
            "generated database role {} still exists after DROP",
            lease.username
        )));
    }
    let model = lease_model();
    let mut tx = pool.begin().await.map_err(|err| {
        vault_internal_status(
            "database_credential_revoke_begin",
            format!("database credential revoke transaction failed: {err}"),
        )
    })?;
    let update = format!(
        "UPDATE {} SET {} = $2, {} = NOW(), {} = '', {} = '', {} = $3::uuid \
         WHERE {} = $1::uuid AND {} <> $2",
        model.relation,
        model.q("state"),
        model.q("revoked_at"),
        model.q("last_error"),
        model.q("credential_ciphertext"),
        model.q("revocation_operation_id"),
        model.q("lease_id"),
        model.q("state"),
    );
    sqlx::query(&update)
        .bind(&lease.lease_id)
        .bind(DB_LEASE_REVOKED)
        .bind(operation_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            vault_internal_status(
                "database_credential_revoke_update",
                format!("database credential REVOKED transition failed: {err}"),
            )
        })?;
    enqueue_outbox_event_in_tx(
        &mut *tx,
        outbox_relation,
        TOPIC_DB_CREDENTIAL_REVOKED,
        &lease.lease_id,
        &lease.tenant_id,
        &lease.project_id,
        serde_json::json!({
            "lease_id": lease.lease_id,
            "tenant_id": lease.tenant_id,
            "project_id": lease.project_id,
            "role_name": lease.role_name,
            "username": lease.username,
            "operation_id": operation_id,
            "state": DB_LEASE_REVOKED,
        }),
        event_context(operation, &lease.lease_id),
    )
    .await
    .map_err(|err| vault_internal_status("database_credential_revoked_outbox", err))?;
    tx.commit().await.map_err(|err| {
        vault_internal_status(
            "database_credential_revoke_commit",
            format!("database credential REVOKED commit failed: {err}"),
        )
    })
}

fn validate_revocation_reason(reason: &str) -> Result<String, Status> {
    let reason = reason.trim().to_string();
    if reason.is_empty() {
        return Err(vault_required_field(
            "reason",
            "must be a non-empty revocation justification",
            "revocation reason is required",
        ));
    }
    if reason.len() > MAX_REVOCATION_REASON_BYTES {
        return Err(vault_field_violation(
            "reason",
            "must not exceed 2048 bytes",
            "revocation reason exceeds the maximum length",
        ));
    }
    Ok(reason)
}

pub(crate) async fn revoke_database_credentials(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::RevokeDatabaseCredentialsRequest>,
) -> Result<Response<vault_pb::RevokeDatabaseCredentialsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let reason = validate_revocation_reason(&req.reason)?;
    let lease_id = Uuid::parse_str(req.lease_id.trim())
        .map_err(|_| {
            vault_field_violation(
                "lease_id",
                "must be a valid UUID",
                "lease_id must be a valid UUID",
            )
        })?
        .to_string();
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Admin,
        &tenant_id,
        Some(req.project_id.trim()),
    )
    .await?;
    let mut context = project_scoped_native_service_context(&metadata, &tenant_id);
    if context.project_id.trim().is_empty() && !req.project_id.trim().is_empty() {
        context.project_id = req.project_id.trim().to_string();
    }
    let (mut context, lease_pool) =
        svc.resolve_project_store(context, true, "revoke_database_credentials")?;
    let mut lease = load_lease_by_id(&lease_pool, &tenant_id, &context.project_id, &lease_id)
        .await?
        .ok_or_else(vault_db_lease_not_found_status)?;
    if lease.state == DB_LEASE_REVOKED
        && !postgres_role_exists(&lease_pool, &lease.username)
            .await
            .map_err(vault_db_reconciliation_status)?
    {
        return Ok(Response::new(vault_pb::RevokeDatabaseCredentialsResponse {
            lease_id,
            state: DB_LEASE_REVOKED.to_string(),
            replayed: true,
            operation_id: lease.revocation_operation_id,
            message: "database credential was already revoked and role absence is verified"
                .to_string(),
            error: None,
        }));
    }
    let operation_id = if lease.revocation_operation_id.is_empty() {
        Uuid::new_v4().to_string()
    } else {
        lease.revocation_operation_id.clone()
    };
    transition_to_revoking(&lease_pool, &lease_id, &operation_id, &reason).await?;
    lease.state = DB_LEASE_REVOKING.to_string();
    lease.revocation_operation_id = operation_id.clone();
    lease.revocation_requested = true;
    context.target_instance = lease.target_instance.clone();
    let runtime = svc.require_runtime()?;
    let (physical_pool, resolved_instance) =
        runtime.native_store_postgres_binding_for_service("vault", true, &context)?;
    if resolved_instance.unwrap_or_default() != lease.target_instance {
        let err = "database credential physical authority no longer resolves to its immutable target instance";
        let _ = mark_lease_failed(&lease_pool, &lease_id, err).await;
        return Err(vault_db_reconciliation_status(err));
    }
    if let Err(err) = finalize_revocation(
        &physical_pool,
        svc.outbox_relation.as_deref(),
        &lease,
        &operation_id,
        "revoke_database_credentials",
    )
    .await
    {
        let _ = mark_lease_failed(&lease_pool, &lease_id, err.message()).await;
        return Err(err);
    }
    Ok(Response::new(vault_pb::RevokeDatabaseCredentialsResponse {
        lease_id,
        state: DB_LEASE_REVOKED.to_string(),
        replayed: false,
        operation_id,
        message: "database credential sessions terminated and role absence verified".to_string(),
        error: None,
    }))
}

pub(crate) async fn emergency_revoke_database_credentials(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::EmergencyRevokeDatabaseCredentialsRequest>,
) -> Result<Response<vault_pb::EmergencyRevokeDatabaseCredentialsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let reason = validate_revocation_reason(&req.reason)?;
    let mut context = project_scoped_native_service_context(&metadata, &tenant_id);
    if context.project_id.trim().is_empty() && !req.project_id.trim().is_empty() {
        context.project_id = req.project_id.trim().to_string();
    }
    let (context, pool) =
        svc.resolve_project_store(context, true, "emergency_revoke_database_credentials")?;
    let expected_confirmation = format!("{}:{}", tenant_id, context.project_id);
    if req.confirmation_token.trim() != expected_confirmation {
        return Err(vault_field_violation(
            "confirmation_token",
            "must exactly equal '<tenant_id>:<resolved-project_id>'",
            "emergency revocation confirmation_token does not match the authenticated tenant/project",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "vault",
        OperationChannel::Admin,
        &tenant_id,
        Some(&context.project_id),
    )
    .await?;
    let operation_id = Uuid::new_v4().to_string();
    let model = lease_model();
    let mark_sql = format!(
        "UPDATE {} SET {} = $3, {} = $4::uuid, {} = NOW(), {} = $5, {} = '' \
         WHERE {} = $1 AND {} = $2 AND {} <> $6",
        model.relation,
        model.q("state"),
        model.q("revocation_operation_id"),
        model.q("revocation_requested_at"),
        model.q("revoke_reason"),
        model.q("last_error"),
        model.q("tenant_id"),
        model.q("project_id"),
        model.q("state"),
    );
    let matched = sqlx::query(&mark_sql)
        .bind(&tenant_id)
        .bind(&context.project_id)
        .bind(DB_LEASE_REVOKING)
        .bind(&operation_id)
        .bind(&reason)
        .bind(DB_LEASE_REVOKED)
        .execute(&pool)
        .await
        .map_err(|err| {
            vault_internal_status(
                "database_credential_emergency_mark",
                format!("emergency database credential revocation mark failed: {err}"),
            )
        })?
        .rows_affected() as i64;
    let select_sql = format!(
        "SELECT {} FROM {} WHERE {} = $1 AND {} = $2 AND {} = $3::uuid AND {} = $4 \
         ORDER BY {} ASC LIMIT $5",
        lease_projection(&model),
        model.relation,
        model.q("tenant_id"),
        model.q("project_id"),
        model.q("revocation_operation_id"),
        model.q("state"),
        model.q("issued_at"),
    );
    let rows = sqlx::query(&select_sql)
        .bind(&tenant_id)
        .bind(&context.project_id)
        .bind(&operation_id)
        .bind(DB_LEASE_REVOKING)
        .bind(vault_db_lease_reaper_batch())
        .fetch_all(&pool)
        .await
        .map_err(|err| {
            vault_internal_status(
                "database_credential_emergency_load",
                format!("emergency database credential revocation load failed: {err}"),
            )
        })?;
    let leases = rows
        .iter()
        .map(lease_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let mut revoked = 0i64;
    for lease in leases {
        let mut exact = context.clone();
        exact.target_instance = lease.target_instance.clone();
        let runtime = svc.require_runtime()?;
        let (physical_pool, resolved) =
            runtime.native_store_postgres_binding_for_service("vault", true, &exact)?;
        if resolved.unwrap_or_default() != lease.target_instance {
            let _ = mark_lease_failed(
                &pool,
                &lease.lease_id,
                "immutable target instance is no longer routable",
            )
            .await;
            continue;
        }
        match finalize_revocation(
            &physical_pool,
            svc.outbox_relation.as_deref(),
            &lease,
            &operation_id,
            "emergency_revoke_database_credentials",
        )
        .await
        {
            Ok(()) => revoked += 1,
            Err(err) => {
                let _ = mark_lease_failed(&pool, &lease.lease_id, err.message()).await;
            }
        }
    }
    Ok(Response::new(
        vault_pb::EmergencyRevokeDatabaseCredentialsResponse {
            operation_id,
            matched_count: matched,
            revoked_count: revoked,
            message: if revoked == matched {
                "all matching database credentials were revoked and verified".to_string()
            } else {
                "revocation intent is durable; the leader reconciler will finish remaining leases"
                    .to_string()
            },
            error: None,
        },
    ))
}
