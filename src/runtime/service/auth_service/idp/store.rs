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

use crate::runtime::core::DataBrokerRuntime;
use crate::runtime::native_catalog::native_model;

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
    runtime
        .encrypt_secret_at_rest(plaintext)
        .map_err(|err| Status::internal(format!("idp secret encryption-at-rest failed: {err}")))
}

pub const PROVIDER_MSG: &str = "udb.core.idp.entity.v1.IdentityProvider";
pub const EXTERNAL_IDENTITY_MSG: &str = "udb.core.idp.entity.v1.ExternalIdentity";
pub const SCIM_STATE_MSG: &str = "udb.core.idp.entity.v1.ScimDirectoryState";
pub const SAML_REPLAY_MSG: &str = "udb.core.idp.entity.v1.SamlReplayEntry";
pub const USER_MSG: &str = "udb.core.authn.entity.v1.User";

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
    move |err| Status::internal(format!("{context}: {err}"))
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
    let m = native_model(
        PROVIDER_MSG,
        &[
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
            "client_secret",
            "saml_signing_key_pem",
            "saml_idp_certs_json",
            "saml_sso_url",
            "health",
            "created_by",
            "updated_by",
        ],
    );
    let provider_id = Uuid::new_v4();
    let sql = format!(
        "INSERT INTO {rel} ({pid}, {tenant}, {kind}, {name}, {issuer}, {entity}, {jwks}, \
            {meta}, {clients}, {auds}, {claim}, {group}, {jit}, {alp}, {enabled}, {secret}, \
            {signkey}, {certs}, {sso}, {health}, {cby}, {uby}) \
         VALUES ($1::UUID, $2, $3, $4, $5, $6, $7, $8, $9::JSONB, $10::JSONB, $11::JSONB, \
            $12::JSONB, $13::JSONB, $14, $15, NULLIF($16,''), NULLIF($17,''), $18::JSONB, $19, \
            $20, $21, $22) \
         RETURNING {pid}::TEXT AS provider_id",
        rel = m.relation,
        pid = m.q("provider_id"),
        tenant = m.q("tenant_id"),
        kind = m.q("kind"),
        name = m.q("display_name"),
        issuer = m.q("issuer"),
        entity = m.q("entity_id"),
        jwks = m.q("jwks_url"),
        meta = m.q("saml_metadata_url"),
        clients = m.q("client_ids_json"),
        auds = m.q("audiences_json"),
        claim = m.q("claim_mapping_json"),
        group = m.q("group_mapping_json"),
        jit = m.q("jit_policy_json"),
        alp = m.q("account_linking_policy"),
        enabled = m.q("enabled"),
        secret = m.q("client_secret"),
        signkey = m.q("saml_signing_key_pem"),
        certs = m.q("saml_idp_certs_json"),
        sso = m.q("saml_sso_url"),
        health = m.q("health"),
        cby = m.q("created_by"),
        uby = m.q("updated_by"),
    );
    let created = sqlx::query(&sql)
        .bind(provider_id)
        .bind(&row.tenant_id)
        .bind(&row.kind)
        .bind(&row.display_name)
        .bind(&row.issuer)
        .bind(&row.entity_id)
        .bind(&row.jwks_url)
        .bind(&row.saml_metadata_url)
        .bind(&row.client_ids_json)
        .bind(&row.audiences_json)
        .bind(&row.claim_mapping_json)
        .bind(&row.group_mapping_json)
        .bind(&row.jit_policy_json)
        .bind(&row.account_linking_policy)
        .bind(row.enabled)
        .bind(client_secret)
        .bind(saml_signing_key_pem)
        .bind(&row.saml_idp_certs_json)
        .bind(&row.saml_sso_url)
        .bind(if row.health.is_empty() {
            "PROVIDER_HEALTH_UNSPECIFIED".to_string()
        } else {
            row.health.clone()
        })
        .bind(&row.created_by)
        .bind(&row.updated_by)
        .fetch_one(pool)
        .await
        .map_err(map_err("idp provider insert failed"))?;
    Ok(created
        .try_get::<String, _>("provider_id")
        .unwrap_or_default())
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
    let pid = Uuid::parse_str(provider_id.trim())
        .map_err(|_| Status::invalid_argument("provider_id must be a UUID"))?;
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
    let m = native_model(
        PROVIDER_MSG,
        &[
            "provider_id",
            "tenant_id",
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
            "client_secret",
            "saml_signing_key_pem",
            "updated_by",
            "deleted_at",
        ],
    );
    let sql = format!(
        "UPDATE {rel} SET \
            {name} = COALESCE(NULLIF($3,''), {name}), \
            {issuer} = COALESCE(NULLIF($4,''), {issuer}), \
            {entity} = COALESCE(NULLIF($5,''), {entity}), \
            {jwks} = COALESCE(NULLIF($6,''), {jwks}), \
            {meta} = COALESCE(NULLIF($7,''), {meta}), \
            {clients} = COALESCE(NULLIF($8,'')::JSONB, {clients}), \
            {auds} = COALESCE(NULLIF($9,'')::JSONB, {auds}), \
            {claim} = COALESCE(NULLIF($10,'')::JSONB, {claim}), \
            {group} = COALESCE(NULLIF($11,'')::JSONB, {group}), \
            {jit} = COALESCE(NULLIF($12,'')::JSONB, {jit}), \
            {alp} = COALESCE(NULLIF($13,''), {alp}), \
            {secret} = COALESCE(NULLIF($14,''), {secret}), \
            {signkey} = COALESCE(NULLIF($15,''), {signkey}), \
            {uby} = COALESCE(NULLIF($16,''), {uby}) \
         WHERE {pid} = $1::UUID AND {tenant} = $2 AND {del} IS NULL",
        rel = m.relation,
        name = m.q("display_name"),
        issuer = m.q("issuer"),
        entity = m.q("entity_id"),
        jwks = m.q("jwks_url"),
        meta = m.q("saml_metadata_url"),
        clients = m.q("client_ids_json"),
        auds = m.q("audiences_json"),
        claim = m.q("claim_mapping_json"),
        group = m.q("group_mapping_json"),
        jit = m.q("jit_policy_json"),
        alp = m.q("account_linking_policy"),
        secret = m.q("client_secret"),
        signkey = m.q("saml_signing_key_pem"),
        uby = m.q("updated_by"),
        pid = m.q("provider_id"),
        tenant = m.q("tenant_id"),
        del = m.q("deleted_at"),
    );
    let pid = Uuid::parse_str(provider_id.trim())
        .map_err(|_| Status::invalid_argument("provider_id must be a UUID"))?;
    let affected = sqlx::query(&sql)
        .bind(pid)
        .bind(tenant_id)
        .bind(&fields.display_name)
        .bind(&fields.issuer)
        .bind(&fields.entity_id)
        .bind(&fields.jwks_url)
        .bind(&fields.saml_metadata_url)
        .bind(&fields.client_ids_json)
        .bind(&fields.audiences_json)
        .bind(&fields.claim_mapping_json)
        .bind(&fields.group_mapping_json)
        .bind(&fields.jit_policy_json)
        .bind(&fields.account_linking_policy)
        .bind(client_secret)
        .bind(saml_signing_key_pem)
        .bind(&fields.updated_by)
        .execute(pool)
        .await
        .map_err(map_err("idp provider update failed"))?
        .rows_affected();
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
    let m = native_model(
        PROVIDER_MSG,
        &[
            "provider_id",
            "tenant_id",
            "enabled",
            "updated_by",
            "deleted_at",
        ],
    );
    let sql = format!(
        "UPDATE {rel} SET {enabled} = false, {uby} = COALESCE(NULLIF($3,''), {uby}) \
         WHERE {pid} = $1::UUID AND {tenant} = $2 AND {del} IS NULL",
        rel = m.relation,
        enabled = m.q("enabled"),
        uby = m.q("updated_by"),
        pid = m.q("provider_id"),
        tenant = m.q("tenant_id"),
        del = m.q("deleted_at"),
    );
    let pid = Uuid::parse_str(provider_id.trim())
        .map_err(|_| Status::invalid_argument("provider_id must be a UUID"))?;
    let affected = sqlx::query(&sql)
        .bind(pid)
        .bind(tenant_id)
        .bind(updated_by)
        .execute(pool)
        .await
        .map_err(map_err("idp provider disable failed"))?
        .rows_affected();
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
    let m = native_model(
        PROVIDER_MSG,
        &[
            "provider_id",
            "tenant_id",
            "health",
            "last_jwks_refresh_status",
            "last_jwks_refresh_at",
        ],
    );
    let sql = format!(
        "UPDATE {rel} SET {health} = $3, {status} = $4, {at} = NOW() \
         WHERE {pid} = $1::UUID AND {tenant} = $2",
        rel = m.relation,
        health = m.q("health"),
        status = m.q("last_jwks_refresh_status"),
        at = m.q("last_jwks_refresh_at"),
        pid = m.q("provider_id"),
        tenant = m.q("tenant_id"),
    );
    let pid = Uuid::parse_str(provider_id.trim())
        .map_err(|_| Status::invalid_argument("provider_id must be a UUID"))?;
    sqlx::query(&sql)
        .bind(pid)
        .bind(tenant_id)
        .bind(health)
        .bind(status)
        .execute(pool)
        .await
        .map_err(map_err("idp jwks-refresh status update failed"))?;
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
    let pid = Uuid::parse_str(provider_id.trim())
        .map_err(|_| Status::invalid_argument("provider_id must be a UUID"))?;
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
    let m = native_model(
        PROVIDER_MSG,
        &[
            "provider_id",
            "tenant_id",
            "entity_id",
            "saml_sso_url",
            "saml_idp_certs_json",
            "updated_by",
            "deleted_at",
        ],
    );
    let sql = format!(
        "UPDATE {rel} SET {entity} = COALESCE(NULLIF($3,''), {entity}), \
            {sso} = COALESCE(NULLIF($4,''), {sso}), \
            {certs} = $5::JSONB, \
            {uby} = COALESCE(NULLIF($6,''), {uby}) \
         WHERE {pid} = $1::UUID AND {tenant} = $2 AND {del} IS NULL",
        rel = m.relation,
        entity = m.q("entity_id"),
        sso = m.q("saml_sso_url"),
        certs = m.q("saml_idp_certs_json"),
        uby = m.q("updated_by"),
        pid = m.q("provider_id"),
        tenant = m.q("tenant_id"),
        del = m.q("deleted_at"),
    );
    let pid = Uuid::parse_str(provider_id.trim())
        .map_err(|_| Status::invalid_argument("provider_id must be a UUID"))?;
    let affected = sqlx::query(&sql)
        .bind(pid)
        .bind(tenant_id)
        .bind(entity_id)
        .bind(sso_url)
        .bind(certs_json)
        .bind(updated_by)
        .execute(pool)
        .await
        .map_err(map_err("idp saml metadata update failed"))?
        .rows_affected();
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
    let pid = Uuid::parse_str(provider_id.trim())
        .map_err(|_| Status::invalid_argument("provider_id must be a UUID"))?;
    let row = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(pid)
        .bind(subject)
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
    let pid = Uuid::parse_str(provider_id.trim())
        .map_err(|_| Status::invalid_argument("provider_id must be a UUID"))?;
    let uid = Uuid::parse_str(user_id.trim())
        .map_err(|_| Status::invalid_argument("user_id must be a UUID"))?;
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
        .ok_or_else(|| Status::internal("external identity vanished after upsert"))
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
    let m = native_model(
        EXTERNAL_IDENTITY_MSG,
        &["external_identity_id", "tenant_id", "deleted_at"],
    );
    let sql = format!(
        "UPDATE {rel} SET {del} = NOW() WHERE {id} = $1::UUID AND {tenant} = $2 AND {del} IS NULL",
        rel = m.relation,
        del = m.q("deleted_at"),
        id = m.q("external_identity_id"),
        tenant = m.q("tenant_id"),
    );
    let id = Uuid::parse_str(external_identity_id.trim())
        .map_err(|_| Status::invalid_argument("external_identity_id must be a UUID"))?;
    let affected = sqlx::query(&sql)
        .bind(id)
        .bind(tenant_id)
        .execute(pool)
        .await
        .map_err(map_err("external identity unlink failed"))?
        .rows_affected();
    Ok(affected > 0)
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
    let m = native_model(
        SAML_REPLAY_MSG,
        &[
            "saml_replay_entry_id",
            "tenant_id",
            "provider_id",
            "assertion_id",
            "not_on_or_after",
        ],
    );
    let sql = format!(
        "INSERT INTO {rel} ({id}, {tenant}, {pid}, {aid}, {noa}) \
         VALUES (gen_random_uuid(), $1, $2::UUID, $3, to_timestamp($4)) \
         ON CONFLICT ({tenant}, {pid}, {aid}) DO NOTHING",
        rel = m.relation,
        id = m.q("saml_replay_entry_id"),
        tenant = m.q("tenant_id"),
        pid = m.q("provider_id"),
        aid = m.q("assertion_id"),
        noa = m.q("not_on_or_after"),
    );
    let pid = Uuid::parse_str(provider_id.trim())
        .map_err(|_| Status::invalid_argument("provider_id must be a UUID"))?;
    let affected = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(pid)
        .bind(assertion_id)
        .bind(not_on_or_after_unix)
        .execute(pool)
        .await
        .map_err(map_err("saml replay record failed"))?
        .rows_affected();
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
            CASE WHEN $6 THEN NOW() ELSE NULL END, $7, $8, $9) \
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
    let row = sqlx::query(&sql)
        .bind(&username)
        .bind(email)
        .bind(tenant_id)
        .bind(project_id)
        .bind(full_name)
        .bind(email_verified)
        .bind(provider_id)
        .bind(subject)
        .bind(created_by)
        .fetch_one(pool)
        .await
        .map_err(map_err("external user JIT provision failed"))?;
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
    let um = native_model(USER_MSG, &["user_id", "tenant_id", "status", "deleted_at"]);
    let user_sql = format!(
        "UPDATE {rel} SET {status} = 'SUSPENDED' \
         WHERE {uid} = $1::UUID AND {tenant} = $2 AND {del} IS NULL",
        rel = um.relation,
        status = um.q("status"),
        uid = um.q("user_id"),
        tenant = um.q("tenant_id"),
        del = um.q("deleted_at"),
    );
    let uid = Uuid::parse_str(user_id.trim())
        .map_err(|_| Status::invalid_argument("user_id must be a UUID"))?;
    let affected = sqlx::query(&user_sql)
        .bind(uid)
        .bind(tenant_id)
        .execute(pool)
        .await
        .map_err(map_err("user deactivate failed"))?
        .rows_affected();

    // Revoke active sessions for the user (tenant-scoped). The plan ties this to
    // tenant policy; we revoke unconditionally on deprovision (fail-safe).
    let sm = native_model(
        "udb.core.authn.entity.v1.Session",
        &["user_id", "tenant_id", "is_active", "revoke_reason"],
    );
    let sess_sql = format!(
        "UPDATE {rel} SET {active} = false, {reason} = 'scim_deprovision' \
         WHERE {uid} = $1::UUID AND {tenant} = $2 AND {active} = true",
        rel = sm.relation,
        active = sm.q("is_active"),
        reason = sm.q("revoke_reason"),
        uid = sm.q("user_id"),
        tenant = sm.q("tenant_id"),
    );
    let _ = sqlx::query(&sess_sql)
        .bind(uid)
        .bind(tenant_id)
        .execute(pool)
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
