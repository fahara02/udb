//! Postgres persistence for the IdP plane.
//!
//! All table/column identifiers resolve through [`native_model`] so the proto
//! annotations (`pg_table`/`pg_column`) are the single source of truth — no
//! hand-maintained schema copies (mirrors `tenant_service` / `apikey`). No
//! in-memory storage: every method requires a live pool and fails closed
//! otherwise (the handlers gate on `require_pool`).

use sqlx::{PgPool, Row};
use tonic::Status;
use uuid::Uuid;

use crate::backend::BackendKind;
use crate::ir::compile::CompileContext;
use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalAssignment, LogicalFilter, LogicalRecord, LogicalUpdate,
    LogicalValue, LogicalWrite,
};
use crate::runtime::core::DataBrokerRuntime;
use crate::runtime::native_catalog::native_model;

fn idp_store_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("identity_provider", operation, message)
}

/// Phase 5 (encryption-at-rest): seal an IdP secret before it is bound to a SQL
/// parameter. An empty string is the "leave unchanged / store NULL" sentinel for
/// the `NULLIF($n,'')` / `COALESCE(NULLIF(...))` clauses, so it is passed through
/// untouched (encrypting it would defeat that sentinel). Non-empty values are
/// sealed via the runtime's AES-256-GCM-SIV envelope; in dev (no key configured)
/// the plaintext passes through, and in fail-closed mode a missing key errors.
fn seal_idp_secret(runtime: &DataBrokerRuntime, plaintext: &str) -> Result<String, Status> {
    if plaintext.is_empty() {
        return Ok(String::new());
    }
    runtime.encrypt_secret_at_rest(plaintext).map_err(|err| {
        idp_store_internal_status(
            "idp_secret_encrypt",
            format!("idp secret encryption-at-rest failed: {err}"),
        )
    })
}

pub const PROVIDER_MSG: &str = "udb.core.idp.entity.v1.IdentityProvider";
pub const EXTERNAL_IDENTITY_MSG: &str = "udb.core.idp.entity.v1.ExternalIdentity";
pub const SCIM_STATE_MSG: &str = "udb.core.idp.entity.v1.ScimDirectoryState";
pub const SAML_REPLAY_MSG: &str = "udb.core.idp.entity.v1.SamlReplayEntry";
pub const USER_MSG: &str = "udb.core.authn.entity.v1.User";
pub const SESSION_MSG: &str = "udb.core.authn.entity.v1.Session";
pub const TOKEN_FAMILY_MSG: &str = "udb.core.authn.entity.v1.TokenFamily";
pub const DEVICE_MSG: &str = "udb.core.authn.entity.v1.Device";
pub const MFA_CHALLENGE_MSG: &str = "udb.core.authn.entity.v1.MfaChallenge";
pub const OTP_MSG: &str = "udb.core.authn.entity.v1.OTP";
pub const RECOVERY_CODE_MSG: &str = "udb.core.authn.entity.v1.RecoveryCode";
pub const WEBAUTHN_CREDENTIAL_MSG: &str = "udb.core.authn.entity.v1.WebAuthnCredential";
pub const WEBAUTHN_CHALLENGE_MSG: &str = "udb.core.authn.entity.v1.WebAuthnChallenge";
pub const API_KEY_MSG: &str = "udb.core.apikey.entity.v1.ApiKey";

/// Per-principal hard-delete counts for SCIM deprovisioning. The operation is
/// scoped to one `(tenant_id, user_id)` and intentionally does not delete
/// tenant-wide rows such as provider config or tenant policy.
#[derive(Debug, Clone, Default)]
pub struct PrincipalHardDeleteReport {
    pub user_id: String,
    pub tenant_id: String,
    pub deleted_external_identities: u64,
    pub deleted_api_keys: u64,
    pub deleted_token_families: u64,
    pub deleted_sessions: u64,
    pub deleted_devices: u64,
    pub deleted_mfa_challenges: u64,
    pub deleted_otps: u64,
    pub deleted_recovery_codes: u64,
    pub deleted_webauthn_credentials: u64,
    pub deleted_webauthn_challenges: u64,
    pub deleted_users: u64,
    pub principal_denylisted: bool,
}

impl PrincipalHardDeleteReport {
    pub fn total_deleted(&self) -> u64 {
        self.deleted_external_identities
            + self.deleted_api_keys
            + self.deleted_token_families
            + self.deleted_sessions
            + self.deleted_devices
            + self.deleted_mfa_challenges
            + self.deleted_otps
            + self.deleted_recovery_codes
            + self.deleted_webauthn_credentials
            + self.deleted_webauthn_challenges
            + self.deleted_users
    }
}

/// A fully-resolved provider row (runtime view; secrets stay server-side).
#[derive(Debug, Clone, Default)]
pub struct ProviderRow {
    pub provider_id: String,
    pub tenant_id: String,
    pub kind: String,
    pub display_name: String,
    pub issuer: String,
    pub entity_id: String,
    pub jwks_url: String,
    pub saml_metadata_url: String,
    pub client_ids_json: String,
    pub audiences_json: String,
    pub claim_mapping_json: String,
    pub group_mapping_json: String,
    pub jit_policy_json: String,
    pub account_linking_policy: String,
    pub enabled: bool,
    pub saml_idp_certs_json: String,
    pub saml_sso_url: String,
    pub health: String,
    pub last_jwks_refresh_status: String,
    pub created_by: String,
    pub updated_by: String,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
    pub last_jwks_refresh_at_unix: i64,
}

/// Resolved external-identity link row.
#[derive(Debug, Clone, Default)]
pub struct ExternalIdentityRow {
    pub external_identity_id: String,
    pub tenant_id: String,
    pub provider_id: String,
    pub subject: String,
    pub user_id: String,
    pub email: String,
    pub email_verified: bool,
    pub linked_at_unix: i64,
    pub last_login_at_unix: i64,
}

fn map_err(context: &str) -> impl Fn(sqlx::Error) -> Status + '_ {
    // bug_report.md B2/B3: classify recognised SQLSTATEs (unique/not-null/22P02)
    // to typed gRPC codes instead of leaking the raw DB error as Internal.
    move |err| crate::runtime::executor_utils::sqlx_error_to_status(context, &err)
}

fn native_compile_context() -> CompileContext<'static> {
    CompileContext::new(crate::runtime::native_catalog::native_manifest())
}

fn store_invalid_fields<I, F, D>(message: impl Into<String>, fields: I) -> Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, fields)
}

fn uuid_field_status(field: &str) -> Status {
    store_invalid_fields(
        format!("{field} must be a UUID"),
        [(field.to_string(), "must be a valid UUID".to_string())],
    )
}

fn invalid_json_field_value_status(err: impl std::fmt::Display) -> Status {
    store_invalid_fields(
        format!("invalid JSON field value: {err}"),
        [("value", "must decode as valid JSON")],
    )
}

fn named_json_field_status(field: &str, err: impl std::fmt::Display) -> Status {
    store_invalid_fields(
        format!("{field} must be valid JSON: {err}"),
        [(field.to_string(), "must decode as valid JSON".to_string())],
    )
}

fn required_store_field_status(field: &str) -> Status {
    store_invalid_fields(
        format!("{field} is required"),
        [(field.to_string(), "must be non-empty".to_string())],
    )
}

fn not_on_or_after_out_of_range_status() -> Status {
    store_invalid_fields(
        "not_on_or_after_unix is out of range",
        [(
            "not_on_or_after_unix",
            "must be a valid Unix timestamp representable by chrono",
        )],
    )
}

fn uuid_value(value: &str, field: &str) -> Result<LogicalValue, Status> {
    let uuid = Uuid::parse_str(value.trim()).map_err(|_| uuid_field_status(field))?;
    Ok(LogicalValue::String(uuid.to_string()))
}

fn string_or_null(value: &str) -> LogicalValue {
    if value.is_empty() {
        LogicalValue::Null
    } else {
        LogicalValue::String(value.to_string())
    }
}

fn json_or_null(value: &str) -> Result<LogicalValue, Status> {
    if value.is_empty() {
        return Ok(LogicalValue::Null);
    }
    serde_json::from_str(value)
        .map(LogicalValue::Json)
        .map_err(invalid_json_field_value_status)
}

fn json_value(value: &str, field: &str) -> Result<LogicalValue, Status> {
    serde_json::from_str(value)
        .map(LogicalValue::Json)
        .map_err(|err| named_json_field_status(field, err))
}

fn eq(field: &str, value: LogicalValue) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: field.to_string(),
        op: ComparisonOp::Eq,
        value,
    }
}

fn active_provider_filter(provider_id: &str, tenant_id: &str) -> Result<LogicalFilter, Status> {
    Ok(LogicalFilter::And(vec![
        eq("provider_id", uuid_value(provider_id, "provider_id")?),
        eq("tenant_id", LogicalValue::String(tenant_id.to_string())),
        LogicalFilter::IsNull("deleted_at".to_string()),
    ]))
}

async fn execute_typed_write(
    pool: &PgPool,
    op: LogicalWrite,
) -> Result<(u64, Vec<serde_json::Value>), Status> {
    let ctx = native_compile_context();
    let compiled = crate::runtime::service::handlers_data::compile_logical_write_dispatch(
        &BackendKind::Postgres,
        &op,
        &ctx,
    )?;
    execute_compiled_mutation(pool, &compiled.spec_json).await
}

async fn execute_typed_update(
    pool: &PgPool,
    op: LogicalUpdate,
) -> Result<(u64, Vec<serde_json::Value>), Status> {
    let ctx = native_compile_context();
    let compiled = crate::runtime::service::handlers_data::compile_logical_update_dispatch(
        &BackendKind::Postgres,
        &op,
        &ctx,
    )?;
    execute_compiled_mutation(pool, &compiled.spec_json).await
}

async fn execute_compiled_mutation(
    pool: &PgPool,
    spec_json: &str,
) -> Result<(u64, Vec<serde_json::Value>), Status> {
    let spec: serde_json::Value = serde_json::from_str(spec_json).map_err(|err| {
        idp_store_internal_status(
            "typed_native_mutation_json",
            format!("typed native mutation JSON failed: {err}"),
        )
    })?;
    let sql = spec
        .get("sql")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            idp_store_internal_status(
                "typed_native_mutation_compile",
                "typed native mutation did not compile to SQL",
            )
        })?;
    crate::runtime::core::validate_pg_mutation_sql(sql)?;
    let params = crate::runtime::core::dispatch_params(&spec)?;
    let param_types = crate::runtime::core::dispatch_param_types(&spec)?;
    let return_rows = sql.to_ascii_lowercase().contains(" returning ");
    if return_rows {
        let rows = crate::runtime::core::bind_typed_generic_pg_params(
            sqlx::query(sql),
            &params,
            param_types.as_deref(),
        )?
        .fetch_all(pool)
        .await
        .map_err(|err| {
            crate::runtime::executor_utils::sqlx_error_to_status(
                "typed native mutation failed",
                &err,
            )
        })?;
        let rows = crate::runtime::core::pg_rows_to_json(rows)?;
        Ok((rows.len() as u64, rows))
    } else {
        let result = crate::runtime::core::bind_typed_generic_pg_params(
            sqlx::query(sql),
            &params,
            param_types.as_deref(),
        )?
        .execute(pool)
        .await
        .map_err(|err| {
            crate::runtime::executor_utils::sqlx_error_to_status(
                "typed native mutation failed",
                &err,
            )
        })?;
        Ok((result.rows_affected(), Vec::new()))
    }
}

/// Column list selected for a provider (excludes secret columns by design — the
/// admin read surface never returns client_secret / signing key).
fn provider_select_columns() -> Vec<&'static str> {
    vec![
        "provider_id",
        "tenant_id",
        "kind",
        "display_name",
        "issuer",
        "entity_id",
        "jwks_url",
        "saml_metadata_url",
        "client_ids_json",
        "audiences_json",
        "claim_mapping_json",
        "group_mapping_json",
        "jit_policy_json",
        "account_linking_policy",
        "enabled",
        "saml_idp_certs_json",
        "saml_sso_url",
        "health",
        "last_jwks_refresh_status",
        "created_by",
        "updated_by",
        "created_at",
        "updated_at",
        "last_jwks_refresh_at",
        "deleted_at",
    ]
}

fn provider_row_from(row: &sqlx::postgres::PgRow) -> ProviderRow {
    ProviderRow {
        provider_id: row.try_get("provider_id").unwrap_or_default(),
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        kind: row.try_get("kind").unwrap_or_default(),
        display_name: row.try_get("display_name").unwrap_or_default(),
        issuer: row.try_get("issuer").unwrap_or_default(),
        entity_id: row.try_get("entity_id").unwrap_or_default(),
        jwks_url: row.try_get("jwks_url").unwrap_or_default(),
        saml_metadata_url: row.try_get("saml_metadata_url").unwrap_or_default(),
        client_ids_json: row
            .try_get("client_ids_json")
            .unwrap_or_else(|_| "[]".into()),
        audiences_json: row
            .try_get("audiences_json")
            .unwrap_or_else(|_| "[]".into()),
        claim_mapping_json: row
            .try_get("claim_mapping_json")
            .unwrap_or_else(|_| "{}".into()),
        group_mapping_json: row
            .try_get("group_mapping_json")
            .unwrap_or_else(|_| "{}".into()),
        jit_policy_json: row
            .try_get("jit_policy_json")
            .unwrap_or_else(|_| "{}".into()),
        account_linking_policy: row.try_get("account_linking_policy").unwrap_or_default(),
        enabled: row.try_get("enabled").unwrap_or(false),
        saml_idp_certs_json: row
            .try_get("saml_idp_certs_json")
            .unwrap_or_else(|_| "[]".into()),
        saml_sso_url: row.try_get("saml_sso_url").unwrap_or_default(),
        health: row.try_get("health").unwrap_or_default(),
        last_jwks_refresh_status: row.try_get("last_jwks_refresh_status").unwrap_or_default(),
        created_by: row.try_get("created_by").unwrap_or_default(),
        updated_by: row.try_get("updated_by").unwrap_or_default(),
        created_at_unix: row.try_get("created_at_unix").unwrap_or_default(),
        updated_at_unix: row.try_get("updated_at_unix").unwrap_or_default(),
        last_jwks_refresh_at_unix: row.try_get("last_jwks_refresh_at_unix").unwrap_or_default(),
    }
}

/// SELECT clause that aliases every provider column to the runtime field name and
/// renders timestamps as unix-epoch bigints.
fn provider_select_clause() -> String {
    let m = native_model(PROVIDER_MSG, &provider_select_columns());
    let parts = vec![
        m.text_or_empty_as("provider_id", "provider_id"),
        m.text_or_empty_as("tenant_id", "tenant_id"),
        m.text_or_empty_as("kind", "kind"),
        m.text_or_empty_as("display_name", "display_name"),
        m.text_or_empty_as("issuer", "issuer"),
        m.text_or_empty_as("entity_id", "entity_id"),
        m.text_or_empty_as("jwks_url", "jwks_url"),
        m.text_or_empty_as("saml_metadata_url", "saml_metadata_url"),
        m.json_text_as("client_ids_json", "client_ids_json"),
        m.json_text_as("audiences_json", "audiences_json"),
        m.json_text_as("claim_mapping_json", "claim_mapping_json"),
        m.json_text_as("group_mapping_json", "group_mapping_json"),
        m.json_text_as("jit_policy_json", "jit_policy_json"),
        m.text_or_empty_as("account_linking_policy", "account_linking_policy"),
        format!("{} AS enabled", m.q("enabled")),
        m.json_text_as("saml_idp_certs_json", "saml_idp_certs_json"),
        m.text_or_empty_as("saml_sso_url", "saml_sso_url"),
        m.text_or_empty_as("health", "health"),
        m.text_or_empty_as("last_jwks_refresh_status", "last_jwks_refresh_status"),
        m.text_or_empty_as("created_by", "created_by"),
        m.text_or_empty_as("updated_by", "updated_by"),
        m.timestamp_unix_as("created_at", "created_at_unix"),
        m.timestamp_unix_as("updated_at", "updated_at_unix"),
        m.timestamp_unix_as("last_jwks_refresh_at", "last_jwks_refresh_at_unix"),
    ];
    parts.join(", ")
}

/// Insert a provider. `client_secret` / `saml_signing_key_pem` are sealed at rest
/// via [`DataBrokerRuntime::encrypt_secret_at_rest`] before binding (Phase 5), so
/// the columns hold AEAD envelopes rather than cleartext. Empty strings are left
/// as the NULL sentinel. These columns are never returned on any read surface.
#[allow(clippy::too_many_arguments)]
pub async fn insert_provider(
    runtime: &DataBrokerRuntime,
    pool: &PgPool,
    row: &ProviderRow,
    client_secret: &str,
    saml_signing_key_pem: &str,
) -> Result<String, Status> {
    let client_secret = seal_idp_secret(runtime, client_secret)?;
    let saml_signing_key_pem = seal_idp_secret(runtime, saml_signing_key_pem)?;
    let provider_id = Uuid::new_v4();
    let mut record = LogicalRecord::new();
    record.insert(
        "provider_id".to_string(),
        LogicalValue::String(provider_id.to_string()),
    );
    record.insert(
        "tenant_id".to_string(),
        LogicalValue::String(row.tenant_id.clone()),
    );
    record.insert("kind".to_string(), LogicalValue::String(row.kind.clone()));
    record.insert(
        "display_name".to_string(),
        LogicalValue::String(row.display_name.clone()),
    );
    record.insert(
        "issuer".to_string(),
        LogicalValue::String(row.issuer.clone()),
    );
    record.insert(
        "entity_id".to_string(),
        LogicalValue::String(row.entity_id.clone()),
    );
    record.insert(
        "jwks_url".to_string(),
        LogicalValue::String(row.jwks_url.clone()),
    );
    record.insert(
        "saml_metadata_url".to_string(),
        LogicalValue::String(row.saml_metadata_url.clone()),
    );
    record.insert(
        "client_ids_json".to_string(),
        json_value(&row.client_ids_json, "client_ids_json")?,
    );
    record.insert(
        "audiences_json".to_string(),
        json_value(&row.audiences_json, "audiences_json")?,
    );
    record.insert(
        "claim_mapping_json".to_string(),
        json_value(&row.claim_mapping_json, "claim_mapping_json")?,
    );
    record.insert(
        "group_mapping_json".to_string(),
        json_value(&row.group_mapping_json, "group_mapping_json")?,
    );
    record.insert(
        "jit_policy_json".to_string(),
        json_value(&row.jit_policy_json, "jit_policy_json")?,
    );
    record.insert(
        "account_linking_policy".to_string(),
        LogicalValue::String(row.account_linking_policy.clone()),
    );
    record.insert("enabled".to_string(), LogicalValue::Bool(row.enabled));
    record.insert("client_secret".to_string(), string_or_null(&client_secret));
    record.insert(
        "saml_signing_key_pem".to_string(),
        string_or_null(&saml_signing_key_pem),
    );
    record.insert(
        "saml_idp_certs_json".to_string(),
        json_value(&row.saml_idp_certs_json, "saml_idp_certs_json")?,
    );
    record.insert(
        "saml_sso_url".to_string(),
        LogicalValue::String(row.saml_sso_url.clone()),
    );
    record.insert(
        "health".to_string(),
        LogicalValue::String(if row.health.is_empty() {
            // §2 fix: short token (was "PROVIDER_HEALTH_UNSPECIFIED", 27 chars, which
            // overflowed a VARCHAR(24) `health` column).
            "UNSPECIFIED".to_string()
        } else {
            row.health.clone()
        }),
    );
    record.insert(
        "created_by".to_string(),
        LogicalValue::String(row.created_by.clone()),
    );
    record.insert(
        "updated_by".to_string(),
        LogicalValue::String(row.updated_by.clone()),
    );
    let (_, rows) = execute_typed_write(
        pool,
        LogicalWrite {
            message_type: PROVIDER_MSG.to_string(),
            records: vec![record],
            conflict: ConflictStrategy::Error,
            return_fields: vec!["provider_id".to_string()],
        },
    )
    .await?;
    Ok(rows
        .first()
        .and_then(|row| row.get("provider_id"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string())
}

/// Fetch one provider by id, scoped to tenant. Excludes soft-deleted rows.
pub async fn get_provider(
    pool: &PgPool,
    tenant_id: &str,
    provider_id: &str,
) -> Result<Option<ProviderRow>, Status> {
    let m = native_model(PROVIDER_MSG, &["provider_id", "tenant_id", "deleted_at"]);
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {pid} = $1::UUID AND {tenant} = $2 AND {del} IS NULL",
        cols = provider_select_clause(),
        rel = m.relation,
        pid = m.q("provider_id"),
        tenant = m.q("tenant_id"),
        del = m.q("deleted_at"),
    );
    let pid = Uuid::parse_str(provider_id.trim()).map_err(|_| uuid_field_status("provider_id"))?;
    let row = sqlx::query(&sql)
        .bind(pid)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await
        .map_err(map_err("idp provider get failed"))?;
    Ok(row.map(|r| provider_row_from(&r)))
}

/// List providers for a tenant with optional kind/enabled filters and paging.
pub async fn list_providers(
    pool: &PgPool,
    tenant_id: &str,
    kind: &str,
    enabled_only: bool,
    limit: i64,
    offset: i64,
) -> Result<(Vec<ProviderRow>, i64), Status> {
    let m = native_model(
        PROVIDER_MSG,
        &["tenant_id", "kind", "enabled", "deleted_at", "created_at"],
    );
    let base_where = format!(
        "{tenant} = $1 AND {del} IS NULL AND ($2 = '' OR {kind} = $2) \
           AND ($3 = false OR {enabled} = true)",
        tenant = m.q("tenant_id"),
        del = m.q("deleted_at"),
        kind = m.q("kind"),
        enabled = m.q("enabled"),
    );
    let count_sql = format!(
        "SELECT COUNT(*)::bigint AS cnt FROM {rel} WHERE {filter}",
        rel = m.relation,
        filter = base_where,
    );
    let total: i64 = sqlx::query(&count_sql)
        .bind(tenant_id)
        .bind(kind)
        .bind(enabled_only)
        .fetch_one(pool)
        .await
        .map_err(map_err("idp provider count failed"))?
        .try_get("cnt")
        .unwrap_or(0);
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {filter} ORDER BY {created} DESC LIMIT $4 OFFSET $5",
        cols = provider_select_clause(),
        rel = m.relation,
        filter = base_where,
        created = m.q("created_at"),
    );
    let rows = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(kind)
        .bind(enabled_only)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await
        .map_err(map_err("idp provider list failed"))?;
    Ok((rows.iter().map(provider_row_from).collect(), total))
}

/// Patch a provider's mutable fields. Empty string params leave a column
/// unchanged (COALESCE/NULLIF), so callers send only what they want to update.
#[allow(clippy::too_many_arguments)]
pub async fn update_provider(
    runtime: &DataBrokerRuntime,
    pool: &PgPool,
    tenant_id: &str,
    provider_id: &str,
    fields: &ProviderRow,
    client_secret: &str,
    saml_signing_key_pem: &str,
) -> Result<Option<ProviderRow>, Status> {
    // Seal secrets at rest (Phase 5). Empty = "leave column unchanged" sentinel
    // for the COALESCE(NULLIF(...)) clauses, so it is passed through unsealed.
    let client_secret = seal_idp_secret(runtime, client_secret)?;
    let saml_signing_key_pem = seal_idp_secret(runtime, saml_signing_key_pem)?;
    let mut assignments = std::collections::BTreeMap::new();
    for (field, value) in [
        ("display_name", fields.display_name.as_str()),
        ("issuer", fields.issuer.as_str()),
        ("entity_id", fields.entity_id.as_str()),
        ("jwks_url", fields.jwks_url.as_str()),
        ("saml_metadata_url", fields.saml_metadata_url.as_str()),
        (
            "account_linking_policy",
            fields.account_linking_policy.as_str(),
        ),
        ("client_secret", client_secret.as_str()),
        ("saml_signing_key_pem", saml_signing_key_pem.as_str()),
        ("updated_by", fields.updated_by.as_str()),
    ] {
        assignments.insert(
            field.to_string(),
            LogicalAssignment::Coalesce {
                value: string_or_null(value),
            },
        );
    }
    for (field, value) in [
        ("client_ids_json", fields.client_ids_json.as_str()),
        ("audiences_json", fields.audiences_json.as_str()),
        ("claim_mapping_json", fields.claim_mapping_json.as_str()),
        ("group_mapping_json", fields.group_mapping_json.as_str()),
        ("jit_policy_json", fields.jit_policy_json.as_str()),
    ] {
        assignments.insert(
            field.to_string(),
            LogicalAssignment::Coalesce {
                value: json_or_null(value)?,
            },
        );
    }
    let (affected, _) = execute_typed_update(
        pool,
        LogicalUpdate {
            message_type: PROVIDER_MSG.to_string(),
            filter: active_provider_filter(provider_id, tenant_id)?,
            assignments,
            return_fields: Vec::new(),
            require_affected: false,
        },
    )
    .await?;
    if affected == 0 {
        return Ok(None);
    }
    get_provider(pool, tenant_id, provider_id).await
}

/// Set `enabled = false` (disable, not delete). Returns the refreshed row.
pub async fn disable_provider(
    pool: &PgPool,
    tenant_id: &str,
    provider_id: &str,
    updated_by: &str,
) -> Result<Option<ProviderRow>, Status> {
    let mut assignments = std::collections::BTreeMap::new();
    assignments.insert(
        "enabled".to_string(),
        LogicalAssignment::Set {
            value: LogicalValue::Bool(false),
        },
    );
    assignments.insert(
        "updated_by".to_string(),
        LogicalAssignment::Coalesce {
            value: string_or_null(updated_by),
        },
    );
    let (affected, _) = execute_typed_update(
        pool,
        LogicalUpdate {
            message_type: PROVIDER_MSG.to_string(),
            filter: active_provider_filter(provider_id, tenant_id)?,
            assignments,
            return_fields: Vec::new(),
            require_affected: false,
        },
    )
    .await?;
    if affected == 0 {
        return Ok(None);
    }
    get_provider(pool, tenant_id, provider_id).await
}

/// Record the outcome of a JWKS/discovery refresh (J2.1 provider health).
pub async fn record_jwks_refresh(
    pool: &PgPool,
    tenant_id: &str,
    provider_id: &str,
    health: &str,
    status: &str,
) -> Result<(), Status> {
    let mut assignments = std::collections::BTreeMap::new();
    assignments.insert(
        "health".to_string(),
        LogicalAssignment::Set {
            value: LogicalValue::String(health.to_string()),
        },
    );
    assignments.insert(
        "last_jwks_refresh_status".to_string(),
        LogicalAssignment::Set {
            value: LogicalValue::String(status.to_string()),
        },
    );
    assignments.insert(
        "last_jwks_refresh_at".to_string(),
        LogicalAssignment::ServerNow,
    );
    execute_typed_update(
        pool,
        LogicalUpdate {
            message_type: PROVIDER_MSG.to_string(),
            filter: LogicalFilter::And(vec![
                eq("provider_id", uuid_value(provider_id, "provider_id")?),
                eq("tenant_id", LogicalValue::String(tenant_id.to_string())),
            ]),
            assignments,
            return_fields: Vec::new(),
            require_affected: false,
        },
    )
    .await?;
    Ok(())
}

/// Record the outcome of a SCIM directory sync against the per-provider
/// `scim_directory_state` row (J2.3 SCIM health). On success the cursor advances,
/// `last_sync_at` moves to NOW(), `failure_count` resets to 0 and `last_error`
/// clears; on failure the count increments and the error is recorded. Upserts on
/// the unique (tenant, provider) so the first sync creates the row. Best-effort:
/// the provisioning effect is authoritative, this is operational telemetry.
pub async fn record_scim_sync(
    pool: &PgPool,
    tenant_id: &str,
    provider_id: &str,
    success: bool,
    cursor: &str,
    error: &str,
) -> Result<(), Status> {
    let m = native_model(
        SCIM_STATE_MSG,
        &[
            "scim_directory_state_id",
            "tenant_id",
            "provider_id",
            "cursor",
            "last_sync_at",
            "failure_count",
            "last_error",
        ],
    );
    // On success: advance cursor (when provided), stamp last_sync_at, reset the
    // failure counter and clear the error. On failure: keep the cursor, bump the
    // counter and record the error. Both branches upsert the (tenant,provider) row.
    let sql = format!(
        "INSERT INTO {rel} ({tenant}, {pid}, {cursor}, {last}, {fc}, {err}) \
         VALUES ($1, $2::UUID, NULLIF($4,''), \
            CASE WHEN $3 THEN NOW() ELSE NULL END, \
            CASE WHEN $3 THEN 0 ELSE 1 END, \
            NULLIF($5,'')) \
         ON CONFLICT ({tenant}, {pid}) DO UPDATE SET \
            {cursor} = COALESCE(NULLIF($4,''), {rel}.{cursor}), \
            {last} = CASE WHEN $3 THEN NOW() ELSE {rel}.{last} END, \
            {fc} = CASE WHEN $3 THEN 0 ELSE {rel}.{fc} + 1 END, \
            {err} = CASE WHEN $3 THEN NULL ELSE NULLIF($5,'') END",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        pid = m.q("provider_id"),
        cursor = m.q("cursor"),
        last = m.q("last_sync_at"),
        fc = m.q("failure_count"),
        err = m.q("last_error"),
    );
    let pid = Uuid::parse_str(provider_id.trim()).map_err(|_| uuid_field_status("provider_id"))?;
    sqlx::query(&sql)
        .bind(tenant_id)
        .bind(pid)
        .bind(success)
        .bind(cursor)
        .bind(error)
        .execute(pool)
        .await
        .map_err(map_err("scim directory-state update failed"))?;
    Ok(())
}

/// Update SAML metadata-derived fields (SSO URL, entityID, certs) on a provider.
pub async fn update_saml_metadata(
    pool: &PgPool,
    tenant_id: &str,
    provider_id: &str,
    entity_id: &str,
    sso_url: &str,
    certs_json: &str,
    updated_by: &str,
) -> Result<Option<ProviderRow>, Status> {
    let mut assignments = std::collections::BTreeMap::new();
    assignments.insert(
        "entity_id".to_string(),
        LogicalAssignment::Coalesce {
            value: string_or_null(entity_id),
        },
    );
    assignments.insert(
        "saml_sso_url".to_string(),
        LogicalAssignment::Coalesce {
            value: string_or_null(sso_url),
        },
    );
    assignments.insert(
        "saml_idp_certs_json".to_string(),
        LogicalAssignment::Set {
            value: json_value(certs_json, "saml_idp_certs_json")?,
        },
    );
    assignments.insert(
        "updated_by".to_string(),
        LogicalAssignment::Coalesce {
            value: string_or_null(updated_by),
        },
    );
    let (affected, _) = execute_typed_update(
        pool,
        LogicalUpdate {
            message_type: PROVIDER_MSG.to_string(),
            filter: active_provider_filter(provider_id, tenant_id)?,
            assignments,
            return_fields: Vec::new(),
            require_affected: false,
        },
    )
    .await?;
    if affected == 0 {
        return Ok(None);
    }
    get_provider(pool, tenant_id, provider_id).await
}

// ── External identities ─────────────────────────────────────────────────────

fn external_select_clause() -> String {
    let m = native_model(
        EXTERNAL_IDENTITY_MSG,
        &[
            "external_identity_id",
            "tenant_id",
            "provider_id",
            "subject",
            "user_id",
            "email",
            "email_verified",
            "linked_at",
            "last_login_at",
        ],
    );
    [
        m.text_or_empty_as("external_identity_id", "external_identity_id"),
        m.text_or_empty_as("tenant_id", "tenant_id"),
        m.text_or_empty_as("provider_id", "provider_id"),
        m.text_or_empty_as("subject", "subject"),
        m.text_or_empty_as("user_id", "user_id"),
        m.text_or_empty_as("email", "email"),
        format!("{} AS email_verified", m.q("email_verified")),
        m.timestamp_unix_as("linked_at", "linked_at_unix"),
        m.timestamp_unix_as("last_login_at", "last_login_at_unix"),
    ]
    .join(", ")
}

fn external_row_from(row: &sqlx::postgres::PgRow) -> ExternalIdentityRow {
    ExternalIdentityRow {
        external_identity_id: row.try_get("external_identity_id").unwrap_or_default(),
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        provider_id: row.try_get("provider_id").unwrap_or_default(),
        subject: row.try_get("subject").unwrap_or_default(),
        user_id: row.try_get("user_id").unwrap_or_default(),
        email: row.try_get("email").unwrap_or_default(),
        email_verified: row.try_get("email_verified").unwrap_or(false),
        linked_at_unix: row.try_get("linked_at_unix").unwrap_or_default(),
        last_login_at_unix: row.try_get("last_login_at_unix").unwrap_or_default(),
    }
}

/// Look up an external identity by (tenant, provider, subject).
pub async fn get_external_identity(
    pool: &PgPool,
    tenant_id: &str,
    provider_id: &str,
    subject: &str,
) -> Result<Option<ExternalIdentityRow>, Status> {
    let m = native_model(
        EXTERNAL_IDENTITY_MSG,
        &["tenant_id", "provider_id", "subject", "deleted_at"],
    );
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {tenant} = $1 AND {pid} = $2::UUID AND {sub} = $3 \
           AND {del} IS NULL",
        cols = external_select_clause(),
        rel = m.relation,
        tenant = m.q("tenant_id"),
        pid = m.q("provider_id"),
        sub = m.q("subject"),
        del = m.q("deleted_at"),
    );
    let pid = Uuid::parse_str(provider_id.trim()).map_err(|_| uuid_field_status("provider_id"))?;
    let row = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(pid)
        .bind(subject)
        .fetch_optional(pool)
        .await
        .map_err(map_err("external identity lookup failed"))?;
    Ok(row.map(|r| external_row_from(&r)))
}

/// Resolve an external identity by its UUID `external_identity_id` (the id
/// `ScimCreateUser` returns), tenant- and provider-scoped. SCIM-1 (bug_report.md G).
pub async fn get_external_identity_by_id(
    pool: &PgPool,
    tenant_id: &str,
    provider_id: &str,
    external_identity_id: &str,
) -> Result<Option<ExternalIdentityRow>, Status> {
    let m = native_model(
        EXTERNAL_IDENTITY_MSG,
        &[
            "external_identity_id",
            "tenant_id",
            "provider_id",
            "deleted_at",
        ],
    );
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {tenant} = $1 AND {pid} = $2::UUID AND {eid} = $3::UUID \
           AND {del} IS NULL",
        cols = external_select_clause(),
        rel = m.relation,
        tenant = m.q("tenant_id"),
        pid = m.q("provider_id"),
        eid = m.q("external_identity_id"),
        del = m.q("deleted_at"),
    );
    let pid = Uuid::parse_str(provider_id.trim()).map_err(|_| uuid_field_status("provider_id"))?;
    let eid = Uuid::parse_str(external_identity_id.trim())
        .map_err(|_| uuid_field_status("external_identity_id"))?;
    let row = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(pid)
        .bind(eid)
        .fetch_optional(pool)
        .await
        .map_err(map_err("external identity lookup failed"))?;
    Ok(row.map(|r| external_row_from(&r)))
}

/// Insert (or upsert on the unique tenant+provider+subject) an external identity
/// link. Returns the resolved row.
#[allow(clippy::too_many_arguments)]
pub async fn upsert_external_identity(
    pool: &PgPool,
    tenant_id: &str,
    provider_id: &str,
    subject: &str,
    user_id: &str,
    email: &str,
    email_verified: bool,
) -> Result<ExternalIdentityRow, Status> {
    let m = native_model(
        EXTERNAL_IDENTITY_MSG,
        &[
            "external_identity_id",
            "tenant_id",
            "provider_id",
            "subject",
            "user_id",
            "email",
            "email_verified",
            "last_login_at",
        ],
    );
    let sql = format!(
        "INSERT INTO {rel} ({id}, {tenant}, {pid}, {sub}, {uid}, {email}, {ev}, {lla}) \
         VALUES (gen_random_uuid(), $1, $2::UUID, $3, $4::UUID, $5, $6, NOW()) \
         ON CONFLICT ({tenant}, {pid}, {sub}) DO UPDATE SET \
            {uid} = EXCLUDED.{uid}, {email} = EXCLUDED.{email}, \
            {ev} = EXCLUDED.{ev}, {lla} = NOW()",
        rel = m.relation,
        id = m.q("external_identity_id"),
        tenant = m.q("tenant_id"),
        pid = m.q("provider_id"),
        sub = m.q("subject"),
        uid = m.q("user_id"),
        email = m.q("email"),
        ev = m.q("email_verified"),
        lla = m.q("last_login_at"),
    );
    let pid = Uuid::parse_str(provider_id.trim()).map_err(|_| uuid_field_status("provider_id"))?;
    let uid = Uuid::parse_str(user_id.trim()).map_err(|_| uuid_field_status("user_id"))?;
    sqlx::query(&sql)
        .bind(tenant_id)
        .bind(pid)
        .bind(subject)
        .bind(uid)
        .bind(email)
        .bind(email_verified)
        .execute(pool)
        .await
        .map_err(map_err("external identity upsert failed"))?;
    get_external_identity(pool, tenant_id, provider_id, subject)
        .await?
        .ok_or_else(|| {
            idp_store_internal_status(
                "external_identity_upsert_verify",
                "external identity vanished after upsert",
            )
        })
}

/// List external identities for a tenant with optional provider/user filters.
pub async fn list_external_identities(
    pool: &PgPool,
    tenant_id: &str,
    provider_id: &str,
    user_id: &str,
    limit: i64,
    offset: i64,
) -> Result<(Vec<ExternalIdentityRow>, i64), Status> {
    let m = native_model(
        EXTERNAL_IDENTITY_MSG,
        &[
            "tenant_id",
            "provider_id",
            "user_id",
            "deleted_at",
            "linked_at",
        ],
    );
    // provider_id / user_id are UUID columns; cast the empty-string sentinel
    // away by gating on a text param first.
    let base_where = format!(
        "{tenant} = $1 AND {del} IS NULL \
           AND ($2 = '' OR {pid}::text = $2) \
           AND ($3 = '' OR {uid}::text = $3)",
        tenant = m.q("tenant_id"),
        del = m.q("deleted_at"),
        pid = m.q("provider_id"),
        uid = m.q("user_id"),
    );
    let count_sql = format!(
        "SELECT COUNT(*)::bigint AS cnt FROM {rel} WHERE {filter}",
        rel = m.relation,
        filter = base_where,
    );
    let total: i64 = sqlx::query(&count_sql)
        .bind(tenant_id)
        .bind(provider_id.trim())
        .bind(user_id.trim())
        .fetch_one(pool)
        .await
        .map_err(map_err("external identity count failed"))?
        .try_get("cnt")
        .unwrap_or(0);
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {filter} ORDER BY {linked} DESC LIMIT $4 OFFSET $5",
        cols = external_select_clause(),
        rel = m.relation,
        filter = base_where,
        linked = m.q("linked_at"),
    );
    let rows = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(provider_id.trim())
        .bind(user_id.trim())
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await
        .map_err(map_err("external identity list failed"))?;
    Ok((rows.iter().map(external_row_from).collect(), total))
}

/// Soft-unlink an external identity (sets deleted_at). Returns true when a row
/// was affected.
pub async fn unlink_external_identity(
    pool: &PgPool,
    tenant_id: &str,
    external_identity_id: &str,
) -> Result<bool, Status> {
    let mut assignments = std::collections::BTreeMap::new();
    assignments.insert("deleted_at".to_string(), LogicalAssignment::ServerNow);
    let (affected, _) = execute_typed_update(
        pool,
        LogicalUpdate {
            message_type: EXTERNAL_IDENTITY_MSG.to_string(),
            filter: LogicalFilter::And(vec![
                eq(
                    "external_identity_id",
                    uuid_value(external_identity_id, "external_identity_id")?,
                ),
                eq("tenant_id", LogicalValue::String(tenant_id.to_string())),
                LogicalFilter::IsNull("deleted_at".to_string()),
            ]),
            assignments,
            return_fields: Vec::new(),
            require_affected: false,
        },
    )
    .await?;
    Ok(affected > 0)
}

async fn hard_delete_user_rows_tx(
    conn: &mut sqlx::PgConnection,
    message_type: &str,
    required_columns: &[&str],
    tenant_field: &str,
    user_fields: &[&str],
    tenant_id: &str,
    user_id: &str,
) -> Result<u64, Status> {
    if user_fields.is_empty() {
        return Ok(0);
    }
    let m = native_model(message_type, required_columns);
    let user_predicate = user_fields
        .iter()
        .map(|field| format!("{}::text = $2", m.q(field)))
        .collect::<Vec<_>>()
        .join(" OR ");
    let sql = format!(
        "DELETE FROM {rel} WHERE {tenant}::text = $1 AND ({user_predicate})",
        rel = m.relation,
        tenant = m.q(tenant_field),
    );
    sqlx::query(&sql)
        .bind(tenant_id)
        .bind(user_id)
        .execute(&mut *conn)
        .await
        .map(|result| result.rows_affected())
        .map_err(map_err("principal hard delete failed"))
}

/// HARD-delete one SCIM-resolved principal and its credential/session rows.
///
/// This is intentionally principal-scoped, not tenant-wide. It removes rows that
/// are owned by the resolved user id, then deletes the user row last. All
/// identifiers come from proto metadata through `native_model`; no table name or
/// column name is copied by hand.
pub async fn hard_delete_scim_principal(
    pool: &PgPool,
    tenant_id: &str,
    user_id: &str,
    #[cfg(feature = "redis")] denylist: Option<&crate::runtime::authn::revocation::JtiDenylist>,
    now_unix: u64,
) -> Result<PrincipalHardDeleteReport, Status> {
    let tenant_id = tenant_id.trim();
    let user_id = user_id.trim();
    if tenant_id.is_empty() {
        return Err(required_store_field_status("tenant_id"));
    }
    if user_id.is_empty() {
        return Err(required_store_field_status("user_id"));
    }

    let mut tx = pool.begin().await.map_err(|err| {
        idp_store_internal_status(
            "principal_hard_delete_tx_begin",
            format!("principal hard delete tx begin failed: {err}"),
        )
    })?;

    let deleted_external_identities = hard_delete_user_rows_tx(
        &mut *tx,
        EXTERNAL_IDENTITY_MSG,
        &["tenant_id", "user_id"],
        "tenant_id",
        &["user_id"],
        tenant_id,
        user_id,
    )
    .await?;
    let deleted_api_keys = hard_delete_user_rows_tx(
        &mut *tx,
        API_KEY_MSG,
        &["tenant_id", "owner_id"],
        "tenant_id",
        &["owner_id"],
        tenant_id,
        user_id,
    )
    .await?;
    let deleted_token_families = hard_delete_user_rows_tx(
        &mut *tx,
        TOKEN_FAMILY_MSG,
        &["tenant_id", "user_id", "principal_id"],
        "tenant_id",
        &["user_id", "principal_id"],
        tenant_id,
        user_id,
    )
    .await?;
    let deleted_sessions = hard_delete_user_rows_tx(
        &mut *tx,
        SESSION_MSG,
        &["tenant_id", "user_id", "principal_id"],
        "tenant_id",
        &["user_id", "principal_id"],
        tenant_id,
        user_id,
    )
    .await?;
    let deleted_devices = hard_delete_user_rows_tx(
        &mut *tx,
        DEVICE_MSG,
        &["tenant_id", "user_id"],
        "tenant_id",
        &["user_id"],
        tenant_id,
        user_id,
    )
    .await?;
    let deleted_webauthn_challenges = hard_delete_user_rows_tx(
        &mut *tx,
        WEBAUTHN_CHALLENGE_MSG,
        &["tenant_id", "user_id"],
        "tenant_id",
        &["user_id"],
        tenant_id,
        user_id,
    )
    .await?;
    let deleted_webauthn_credentials = hard_delete_user_rows_tx(
        &mut *tx,
        WEBAUTHN_CREDENTIAL_MSG,
        &["tenant_id", "user_id"],
        "tenant_id",
        &["user_id"],
        tenant_id,
        user_id,
    )
    .await?;
    let deleted_mfa_challenges = hard_delete_user_rows_tx(
        &mut *tx,
        MFA_CHALLENGE_MSG,
        &["tenant_id", "user_id"],
        "tenant_id",
        &["user_id"],
        tenant_id,
        user_id,
    )
    .await?;
    let deleted_otps = hard_delete_user_rows_tx(
        &mut *tx,
        OTP_MSG,
        &["tenant_id", "user_id"],
        "tenant_id",
        &["user_id"],
        tenant_id,
        user_id,
    )
    .await?;
    let deleted_recovery_codes = hard_delete_user_rows_tx(
        &mut *tx,
        RECOVERY_CODE_MSG,
        &["tenant_id", "user_id"],
        "tenant_id",
        &["user_id"],
        tenant_id,
        user_id,
    )
    .await?;
    let deleted_users = hard_delete_user_rows_tx(
        &mut *tx,
        USER_MSG,
        &["tenant_id", "user_id"],
        "tenant_id",
        &["user_id"],
        tenant_id,
        user_id,
    )
    .await?;

    tx.commit().await.map_err(|err| {
        idp_store_internal_status(
            "principal_hard_delete_tx_commit",
            format!("principal hard delete tx commit failed: {err}"),
        )
    })?;

    #[cfg(feature = "redis")]
    let principal_denylisted = if let Some(denylist) = denylist {
        match denylist.deny_principal_after(user_id, now_unix).await {
            Ok(()) => true,
            Err(err) => {
                tracing::warn!(
                    tenant_id = %tenant_id,
                    principal_id = %user_id,
                    error = %err,
                    "SCIM hard delete committed but principal denylist cutoff was not recorded"
                );
                false
            }
        }
    } else {
        false
    };
    #[cfg(not(feature = "redis"))]
    let principal_denylisted = {
        let _ = now_unix;
        false
    };

    Ok(PrincipalHardDeleteReport {
        user_id: user_id.to_string(),
        tenant_id: tenant_id.to_string(),
        deleted_external_identities,
        deleted_api_keys,
        deleted_token_families,
        deleted_sessions,
        deleted_devices,
        deleted_mfa_challenges,
        deleted_otps,
        deleted_recovery_codes,
        deleted_webauthn_credentials,
        deleted_webauthn_challenges,
        deleted_users,
        principal_denylisted,
    })
}

// ── SAML replay cache ────────────────────────────────────────────────────────

/// Atomically record a consumed SAML assertion id. Returns `Ok(true)` when this
/// is the first time the assertion was seen (the insert won), or `Ok(false)`
/// when it was already present (a replay — reject, J3). Durable single-use guard.
pub async fn record_saml_assertion(
    pool: &PgPool,
    tenant_id: &str,
    provider_id: &str,
    assertion_id: &str,
    not_on_or_after_unix: i64,
) -> Result<bool, Status> {
    let not_on_or_after = chrono::DateTime::from_timestamp(not_on_or_after_unix, 0)
        .ok_or_else(not_on_or_after_out_of_range_status)?;
    let mut record = LogicalRecord::new();
    record.insert(
        "saml_replay_entry_id".to_string(),
        LogicalValue::String(Uuid::new_v4().to_string()),
    );
    record.insert(
        "tenant_id".to_string(),
        LogicalValue::String(tenant_id.to_string()),
    );
    record.insert(
        "provider_id".to_string(),
        uuid_value(provider_id, "provider_id")?,
    );
    record.insert(
        "assertion_id".to_string(),
        LogicalValue::String(assertion_id.to_string()),
    );
    record.insert(
        "not_on_or_after".to_string(),
        LogicalValue::Timestamp(not_on_or_after),
    );
    let (affected, _) = execute_typed_write(
        pool,
        LogicalWrite {
            message_type: SAML_REPLAY_MSG.to_string(),
            records: vec![record],
            conflict: ConflictStrategy::Ignore,
            return_fields: Vec::new(),
        },
    )
    .await?;
    Ok(affected > 0)
}

// ── User resolution / JIT provisioning ───────────────────────────────────────

/// Find an existing user id by email within a tenant (for account linking).
/// Returns `(user_id, email_verified)` when found.
pub async fn find_user_by_email(
    pool: &PgPool,
    tenant_id: &str,
    email: &str,
) -> Result<Option<(String, bool)>, Status> {
    if email.trim().is_empty() {
        return Ok(None);
    }
    let m = native_model(
        USER_MSG,
        &[
            "user_id",
            "tenant_id",
            "email",
            "email_verified_at",
            "deleted_at",
        ],
    );
    let sql = format!(
        "SELECT {uid}::TEXT AS user_id, ({ev} IS NOT NULL) AS email_verified \
         FROM {rel} WHERE {tenant} = $1 AND lower({email}) = lower($2) AND {del} IS NULL LIMIT 1",
        uid = m.q("user_id"),
        ev = m.q("email_verified_at"),
        rel = m.relation,
        tenant = m.q("tenant_id"),
        email = m.q("email"),
        del = m.q("deleted_at"),
    );
    let row = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(email)
        .fetch_optional(pool)
        .await
        .map_err(map_err("user lookup by email failed"))?;
    Ok(row.map(|r| {
        (
            r.try_get::<String, _>("user_id").unwrap_or_default(),
            r.try_get::<bool, _>("email_verified").unwrap_or(false),
        )
    }))
}

/// JIT-create an external-identity user. Mirrors the authn user table columns the
/// proto manifest exposes; the account is marked external (no password) and its
/// status is ACTIVE. Returns the new user_id.
pub async fn create_external_user(
    pool: &PgPool,
    tenant_id: &str,
    project_id: &str,
    provider_id: &str,
    subject: &str,
    email: &str,
    full_name: &str,
    email_verified: bool,
    created_by: &str,
) -> Result<String, Status> {
    let m = native_model(
        USER_MSG,
        &[
            "user_id",
            "username",
            "email",
            "password_hash",
            "account_kind",
            "status",
            "tenant_id",
            "project_id",
            "full_name",
            "email_verified_at",
            "external_provider_id",
            "external_subject",
            "created_by",
        ],
    );
    // username is unique AND check-constrained to `^[a-z][a-z0-9._-]{2,79}$`, so
    // derive a deterministic, constraint-safe slug from (provider_id, subject):
    // a short SHA-256 hex digest prefixed with `ext-`. Deterministic so re-runs
    // collide on the same row (ON CONFLICT) rather than spawning duplicates.
    let username = external_username(provider_id, subject);
    let sql = format!(
        "INSERT INTO {rel} ({uid}, {uname}, {email}, {pwd}, {ak}, {status}, {tenant}, \
            {project}, {fname}, {eva}, {epid}, {esub}, {cby}) \
         VALUES (gen_random_uuid(), $1, $2, '', 'EXTERNAL_IDENTITY', \
            'ACTIVE', $3, $4, $5, \
            CASE WHEN $6 THEN NOW() ELSE NULL END, $7, $8, $9::UUID) \
         ON CONFLICT ({uname}) DO UPDATE SET {email} = EXCLUDED.{email} \
         RETURNING {uid}::TEXT AS user_id",
        rel = m.relation,
        uid = m.q("user_id"),
        uname = m.q("username"),
        email = m.q("email"),
        pwd = m.q("password_hash"),
        ak = m.q("account_kind"),
        status = m.q("status"),
        tenant = m.q("tenant_id"),
        project = m.q("project_id"),
        fname = m.q("full_name"),
        eva = m.q("email_verified_at"),
        epid = m.q("external_provider_id"),
        esub = m.q("external_subject"),
        cby = m.q("created_by"),
    );
    // `created_by` is a nullable UUID FK to users.user_id. SCIM/JIT provisioning
    // has no creating admin (the caller passes a system label like "scim"), so a
    // non-UUID value must bind as SQL NULL rather than be cast text->uuid (which
    // fails). bug_report.md A3.
    let created_by_uuid = Uuid::parse_str(created_by.trim()).ok();
    let row = sqlx::query(&sql)
        .bind(&username)
        .bind(email)
        .bind(tenant_id)
        .bind(project_id)
        .bind(full_name)
        .bind(email_verified)
        .bind(provider_id)
        .bind(subject)
        .bind(created_by_uuid)
        .fetch_one(pool)
        .await
        .map_err(|err| {
            crate::runtime::executor_utils::sqlx_error_to_status(
                "external user JIT provision failed",
                &err,
            )
        })?;
    Ok(row.try_get::<String, _>("user_id").unwrap_or_default())
}

/// Deactivate (suspend) a user and revoke their active sessions — the SCIM
/// deprovision mapping (J2.3 / J3). Best-effort session revoke; the suspend is
/// the authoritative effect. Returns true when the user row was updated.
pub async fn deactivate_user(
    pool: &PgPool,
    tenant_id: &str,
    user_id: &str,
) -> Result<bool, Status> {
    let mut user_assignments = std::collections::BTreeMap::new();
    user_assignments.insert(
        "status".to_string(),
        LogicalAssignment::Set {
            value: LogicalValue::String("SUSPENDED".to_string()),
        },
    );
    let (affected, _) = execute_typed_update(
        pool,
        LogicalUpdate {
            message_type: USER_MSG.to_string(),
            filter: LogicalFilter::And(vec![
                eq("user_id", uuid_value(user_id, "user_id")?),
                eq("tenant_id", LogicalValue::String(tenant_id.to_string())),
                LogicalFilter::IsNull("deleted_at".to_string()),
            ]),
            assignments: user_assignments,
            return_fields: Vec::new(),
            require_affected: false,
        },
    )
    .await?;

    // Revoke active sessions for the user (tenant-scoped). The plan ties this to
    // tenant policy; we revoke unconditionally on deprovision (fail-safe).
    let mut session_assignments = std::collections::BTreeMap::new();
    session_assignments.insert(
        "is_active".to_string(),
        LogicalAssignment::Set {
            value: LogicalValue::Bool(false),
        },
    );
    session_assignments.insert(
        "revoke_reason".to_string(),
        LogicalAssignment::Set {
            value: LogicalValue::String("scim_deprovision".to_string()),
        },
    );
    let _ = execute_typed_update(
        pool,
        LogicalUpdate {
            message_type: SESSION_MSG.to_string(),
            filter: LogicalFilter::And(vec![
                eq("user_id", uuid_value(user_id, "user_id")?),
                eq("tenant_id", LogicalValue::String(tenant_id.to_string())),
                eq("is_active", LogicalValue::Bool(true)),
            ]),
            assignments: session_assignments,
            return_fields: Vec::new(),
            require_affected: false,
        },
    )
    .await; // best-effort
    Ok(affected > 0)
}

/// Deterministic, check-constraint-safe username for a JIT/SCIM external user.
/// `udb_authn.users.username` enforces `^[a-z][a-z0-9._-]{2,79}$`, so we derive
/// an `ext-<sha256-hex-prefix>` slug from (provider_id, subject): stable across
/// re-logins (so ON CONFLICT collides) and always satisfies the constraint.
fn external_username(provider_id: &str, subject: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"udb.idp.external-username.v1:");
    hasher.update(provider_id.as_bytes());
    hasher.update(b"\x1f");
    hasher.update(subject.as_bytes());
    let digest = hasher.finalize();
    let hex: String = digest.iter().take(20).map(|b| format!("{b:02x}")).collect();
    format!("ext-{hex}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_single_field(status: &Status, field: &str, description: &str) {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "identity_provider");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn idp_store_internal_status_carries_typed_detail() {
        let status = idp_store_internal_status(
            "typed_native_mutation_compile",
            "typed native mutation did not compile to SQL",
        );

        assert_internal_detail(
            &status,
            "typed_native_mutation_compile",
            "typed native mutation did not compile to SQL",
        );
    }

    #[test]
    fn idp_store_uuid_and_required_validation_carries_field_violations() {
        let uuid =
            uuid_value("not-a-uuid", "provider_id").expect_err("malformed provider_id must fail");
        assert_eq!(uuid.message(), "provider_id must be a UUID");
        assert_single_field(&uuid, "provider_id", "must be a valid UUID");

        let missing_tenant = required_store_field_status("tenant_id");
        assert_eq!(missing_tenant.message(), "tenant_id is required");
        assert_single_field(&missing_tenant, "tenant_id", "must be non-empty");

        let out_of_range = not_on_or_after_out_of_range_status();
        assert_eq!(
            out_of_range.message(),
            "not_on_or_after_unix is out of range"
        );
        assert_single_field(
            &out_of_range,
            "not_on_or_after_unix",
            "must be a valid Unix timestamp representable by chrono",
        );
    }

    #[test]
    fn idp_store_json_validation_carries_field_violations() {
        let anonymous = json_or_null("{not-json").expect_err("invalid JSON must fail");
        assert!(anonymous.message().starts_with("invalid JSON field value:"));
        assert_single_field(&anonymous, "value", "must decode as valid JSON");

        let named =
            json_value("{not-json", "saml_idp_certs_json").expect_err("invalid JSON must fail");
        assert!(
            named
                .message()
                .starts_with("saml_idp_certs_json must be valid JSON:")
        );
        assert_single_field(&named, "saml_idp_certs_json", "must decode as valid JSON");
    }
}
