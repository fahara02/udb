//! Dynamic database-credential engine helpers for the native `VaultService`: the
//! operator allow-list config (`UDB_VAULT_DB_ROLES_JSON`), the Postgres-identifier
//! validators, the identifier/literal quoters, the generated username/password
//! sources, the TTL resolver, and the short-lived login-role create/drop SQL.
//! Extracted verbatim from the former god file — the allow-listing, the
//! identifier-safety checks, and the role DDL are byte-for-byte identical.

use std::collections::HashMap;
use std::sync::OnceLock;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use tonic::Status;
use uuid::Uuid;

use super::config::{
    DEFAULT_DB_CREDENTIAL_MAX_TTL_SECONDS, DEFAULT_DB_CREDENTIAL_TTL_SECONDS,
    MIN_DB_CREDENTIAL_TTL_SECONDS,
};
use super::errors::{
    vault_db_credentials_config_status, vault_db_role_creation_status, vault_field_violation,
    vault_internal_status,
};

/// Operator allow-list entry for dynamic database credentials. `role_name` is
/// the RPC-facing alias; `parent_role` is the existing Postgres role granted to
/// the generated short-lived login. The generated login password is returned
/// once and never stored.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct DbCredentialRoleConfig {
    pub(crate) role_name: String,
    pub(crate) parent_role: String,
    #[serde(default)]
    pub(crate) ttl_seconds_max: Option<i32>,
}

pub(crate) fn vault_db_role_configs()
-> Result<&'static HashMap<String, DbCredentialRoleConfig>, Status> {
    static CONFIGS: OnceLock<Result<HashMap<String, DbCredentialRoleConfig>, String>> =
        OnceLock::new();
    match CONFIGS.get_or_init(|| {
        let raw = std::env::var("UDB_VAULT_DB_ROLES_JSON")
            .map_err(|_| "UDB_VAULT_DB_ROLES_JSON is not configured".to_string())?;
        parse_vault_db_role_configs(&raw)
    }) {
        Ok(configs) => Ok(configs),
        Err(err) => Err(vault_db_credentials_config_status(format!(
            "vault dynamic database credentials are not configured: {err}"
        ))),
    }
}

pub(crate) fn parse_vault_db_role_configs(
    raw: &str,
) -> Result<HashMap<String, DbCredentialRoleConfig>, String> {
    let entries: Vec<DbCredentialRoleConfig> =
        serde_json::from_str(raw).map_err(|err| format!("invalid JSON: {err}"))?;
    if entries.is_empty() {
        return Err("at least one role entry is required".to_string());
    }
    let mut configs = HashMap::with_capacity(entries.len());
    for mut entry in entries {
        entry.role_name = entry.role_name.trim().to_string();
        entry.parent_role = entry.parent_role.trim().to_string();
        validate_db_role_alias(&entry.role_name).map_err(|err| err.message().to_string())?;
        validate_pg_identifier_value(&entry.parent_role, "parent_role")?;
        let max_ttl = entry
            .ttl_seconds_max
            .unwrap_or(DEFAULT_DB_CREDENTIAL_MAX_TTL_SECONDS);
        if max_ttl < MIN_DB_CREDENTIAL_TTL_SECONDS {
            return Err(format!(
                "role '{}' ttl_seconds_max must be at least {MIN_DB_CREDENTIAL_TTL_SECONDS}",
                entry.role_name
            ));
        }
        if configs.insert(entry.role_name.clone(), entry).is_some() {
            return Err("duplicate role_name in UDB_VAULT_DB_ROLES_JSON".to_string());
        }
    }
    Ok(configs)
}

pub(crate) fn validate_db_role_alias(role_name: &str) -> Result<(), Status> {
    if role_name.is_empty()
        || role_name.len() > 128
        || !role_name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | ':' | '.'))
    {
        return Err(vault_field_violation(
            "role_name",
            "must be 1..128 ASCII chars using letters, digits, _, -, :, or .",
            "role_name must be 1..128 ASCII chars using letters, digits, _, -, :, or .",
        ));
    }
    Ok(())
}

pub(crate) fn validate_pg_identifier_value(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 63
        || value.starts_with(|ch: char| ch.is_ascii_digit())
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(format!(
            "{label} '{value}' is not a valid unquoted Postgres identifier"
        ));
    }
    Ok(())
}

pub(crate) fn pg_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn pg_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn generate_db_password() -> String {
    // A clean 256-bit OS-CSPRNG draw (no fixed UUID version/variant nibbles).
    BASE64_STANDARD.encode(crate::runtime::executor_utils::random_32_bytes())
}

pub(crate) fn generate_db_username() -> String {
    format!("udb_vault_{}", Uuid::new_v4().simple())
}

pub(crate) fn requested_db_credential_ttl(
    req_ttl_seconds: i32,
    max_ttl_seconds: i32,
) -> Result<i32, Status> {
    let ttl = if req_ttl_seconds <= 0 {
        DEFAULT_DB_CREDENTIAL_TTL_SECONDS.min(max_ttl_seconds)
    } else {
        req_ttl_seconds
    };
    if ttl < MIN_DB_CREDENTIAL_TTL_SECONDS {
        return Err(vault_field_violation(
            "ttl_seconds",
            format!("must be 0/default or at least {MIN_DB_CREDENTIAL_TTL_SECONDS}"),
            format!("ttl_seconds must be 0/default or at least {MIN_DB_CREDENTIAL_TTL_SECONDS}"),
        ));
    }
    if ttl > max_ttl_seconds {
        return Err(vault_field_violation(
            "ttl_seconds",
            format!("must not exceed configured maximum {max_ttl_seconds}"),
            format!("ttl_seconds exceeds configured maximum {max_ttl_seconds}"),
        ));
    }
    Ok(ttl)
}

pub(crate) async fn create_postgres_login_role(
    pool: &PgPool,
    username: &str,
    password: &str,
    expires_at: DateTime<Utc>,
    parent_role: &str,
) -> Result<(), Status> {
    validate_pg_identifier_value(username, "generated username")
        .map_err(|err| vault_internal_status("create_postgres_login_role", err))?;
    validate_pg_identifier_value(parent_role, "parent_role")
        .map_err(|err| vault_internal_status("create_postgres_login_role", err))?;
    let sql = format!(
        "CREATE ROLE {} LOGIN PASSWORD {} VALID UNTIL {} IN ROLE {}",
        pg_ident(username),
        pg_literal(password),
        pg_literal(&expires_at.to_rfc3339()),
        pg_ident(parent_role)
    );
    sqlx::query(&sql)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|err| {
            vault_db_role_creation_status(format!(
                "vault database credential role creation failed: {err}"
            ))
        })
}

pub(crate) async fn drop_postgres_login_role(pool: &PgPool, username: &str) -> Result<(), String> {
    validate_pg_identifier_value(username, "generated username")?;
    let sql = format!("DROP ROLE IF EXISTS {}", pg_ident(username));
    sqlx::query(&sql)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|err| format!("drop generated database role {username} failed: {err}"))
}

/// Fence an expiring/revoked login before dropping it. Password `VALID UNTIL`
/// prevents new authentication but does not end sessions that connected before
/// expiry, so the lifecycle worker must terminate those backends explicitly.
pub(crate) async fn terminate_postgres_login_sessions(
    pool: &PgPool,
    username: &str,
) -> Result<u64, String> {
    validate_pg_identifier_value(username, "generated username")?;
    let terminated = sqlx::query_scalar::<_, bool>(
        "SELECT pg_terminate_backend(pid) \
         FROM pg_stat_activity \
         WHERE usename = $1 AND pid <> pg_backend_pid()",
    )
    .bind(username)
    .fetch_all(pool)
    .await
    .map_err(|err| {
        format!("terminate sessions for generated database role {username} failed: {err}")
    })?;
    if terminated.iter().any(|did_terminate| !did_terminate) {
        return Err(format!(
            "one or more sessions for generated database role {username} could not be terminated"
        ));
    }
    Ok(terminated.len() as u64)
}

pub(crate) async fn postgres_role_exists(pool: &PgPool, username: &str) -> Result<bool, String> {
    validate_pg_identifier_value(username, "generated username")?;
    sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)")
        .bind(username)
        .fetch_one(pool)
        .await
        .map_err(|err| format!("verify generated database role {username} absence failed: {err}"))
}
