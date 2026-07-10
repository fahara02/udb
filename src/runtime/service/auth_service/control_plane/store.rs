//! Postgres persistence for the Phase 9 control-plane distribution core.
//!
//! All table/column identifiers resolve through [`native_model`] so the proto
//! `pg_table`/`pg_column` annotations are the single source of truth — no
//! hand-maintained schema copies (mirrors `idp` / `tenant_service`). There is NO
//! in-memory state: the registry of versioned resources and the per-node
//! ACK/NACK ledger both live in Postgres, and every method requires a live pool
//! (the handlers gate on `require_pool`).
//!
//! Version is content-addressed: [`upsert_resource`] computes the content hash
//! and only bumps `version` when the payload content actually changes, so a node
//! is never asked to re-apply identical config.

use sqlx::{PgPool, Row};
use tonic::Status;
use uuid::Uuid;

use crate::backend::BackendKind;
use crate::ir::compile::CompileContext;
use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalValue, LogicalWrite,
};
use crate::proto::udb::core::control::entity::v1::ResourceType;
use crate::runtime::native_catalog::native_model;

use super::resources::{
    self, ResourceModel, aggregate_version, content_version, resource_type_to_db,
};

pub const RESOURCE_MSG: &str = "udb.core.control.entity.v1.ControlPlaneResource";
pub const NODE_STATE_MSG: &str = "udb.core.control.entity.v1.ControlPlaneNodeState";

fn control_store_invalid_fields<I, F, D>(message: impl Into<String>, fields: I) -> Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, fields)
}

fn control_store_required_field(
    message: &'static str,
    field: &'static str,
    description: &'static str,
) -> Status {
    control_store_invalid_fields(message, [(field, description)])
}

fn control_store_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("control_plane", operation, message)
}

fn validate_resource_upsert_input(
    resource_type: ResourceType,
    name: &str,
    payload_json: &str,
) -> Result<(), Status> {
    if name.trim().is_empty() {
        return Err(control_store_required_field(
            "resource name is required",
            "name",
            "must be a non-empty control-plane resource name",
        ));
    }
    if resource_type == ResourceType::Unspecified {
        return Err(control_store_required_field(
            "resource_type is required",
            "resource_type",
            "must specify a control-plane resource type",
        ));
    }
    // Reject non-JSON payloads early so the registry only ever holds valid bodies.
    if serde_json::from_str::<serde_json::Value>(payload_json.trim()).is_err() {
        return Err(control_store_required_field(
            "payload_json must be valid JSON",
            "payload_json",
            "must be valid JSON",
        ));
    }
    Ok(())
}

fn validate_node_id(node_id: &str) -> Result<(), Status> {
    if node_id.trim().is_empty() {
        return Err(control_store_required_field(
            "node_id is required",
            "node_id",
            "must be a non-empty control-plane node id",
        ));
    }
    Ok(())
}

/// Per-node, per-type ACK/NACK ledger row (runtime view).
#[derive(Debug, Clone, Default)]
pub struct NodeStateRow {
    pub node_id: String,
    pub resource_type: String,
    pub subscribed_names_json: String,
    pub accepted_version: String,
    pub last_good_version: String,
    pub last_response_nonce: String,
    pub nack_error_detail: String,
    /// Raw JSON of the bounded served-snapshot ring (newest last). Parsed via
    /// [`parse_served_snapshots`]; empty/absent == `"[]"`.
    pub served_snapshots_json: String,
    pub updated_at_unix: i64,
}

/// How many recently-served snapshots to retain per (node, resource_type) so a
/// `RollbackResources` request always has a durable, known-good target to
/// re-serve. Named so the ring depth is a single source of truth (no magic
/// number); the OLDEST entry is evicted once the ring exceeds this depth.
pub const RETENTION_DEPTH: usize = 10;

/// One resource inside a retained served snapshot — the exact (name, tenant,
/// project, payload) tuple that was served, so a rollback can re-publish it
/// through [`upsert_resource`] (the same registry resolver the serving path uses).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetainedResource {
    pub name: String,
    pub tenant_id: String,
    pub project_id: String,
    pub payload_json: String,
}

/// One retained served snapshot for a (node, resource_type): the aggregate world
/// version served plus the resource set behind it. The rollback target.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetainedSnapshot {
    pub version: String,
    pub served_at_unix: i64,
    pub resources: Vec<RetainedResource>,
}

/// Parse the `served_snapshots` ring JSON into typed snapshots (newest last).
/// Tolerant: a malformed entry/field degrades to defaults rather than erroring,
/// so a hand-edited ring can never break the serving path.
pub fn parse_served_snapshots(raw: &str) -> Vec<RetainedSnapshot> {
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default()
        .iter()
        .map(|entry| RetainedSnapshot {
            version: entry
                .get("version")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
            served_at_unix: entry
                .get("served_at_unix")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0),
            resources: entry
                .get("resources")
                .and_then(serde_json::Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .map(|r| RetainedResource {
                            name: r
                                .get("name")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            tenant_id: r
                                .get("tenant_id")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            project_id: r
                                .get("project_id")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            payload_json: r
                                .get("payload_json")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                        })
                        .collect()
                })
                .unwrap_or_default(),
        })
        .collect()
}

/// Serialize a snapshot ring back to its JSON array form for the `served_snapshots`
/// column. Inverse of [`parse_served_snapshots`].
fn served_snapshots_to_json(snaps: &[RetainedSnapshot]) -> serde_json::Value {
    serde_json::Value::Array(
        snaps
            .iter()
            .map(|s| {
                serde_json::json!({
                    "version": s.version,
                    "served_at_unix": s.served_at_unix,
                    "resources": s
                        .resources
                        .iter()
                        .map(|r| serde_json::json!({
                            "name": r.name,
                            "tenant_id": r.tenant_id,
                            "project_id": r.project_id,
                            "payload_json": r.payload_json,
                        }))
                        .collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

fn map_err(context: &'static str) -> impl Fn(sqlx::Error) -> Status {
    // bug_report.md B2/B3: typed gRPC codes for recognised SQLSTATEs.
    move |err| crate::runtime::executor_utils::sqlx_error_to_status(context, &err)
}

fn native_compile_context() -> CompileContext<'static> {
    CompileContext::new(crate::runtime::native_catalog::native_manifest())
}

fn eq(field: &str, value: LogicalValue) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: field.to_string(),
        op: ComparisonOp::Eq,
        value,
    }
}

async fn execute_typed_write(pool: &PgPool, op: LogicalWrite) -> Result<u64, Status> {
    let ctx = native_compile_context();
    let compiled = crate::runtime::service::handlers_data::compile_logical_write_dispatch(
        &BackendKind::Postgres,
        &op,
        &ctx,
    )?;
    let spec: serde_json::Value = serde_json::from_str(&compiled.spec_json).map_err(|err| {
        control_store_internal_status(
            "typed_native_write_json",
            format!("typed native write JSON failed: {err}"),
        )
    })?;
    let sql = spec
        .get("sql")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            control_store_internal_status(
                "typed_native_write_compile",
                "typed native write did not compile to SQL",
            )
        })?;
    crate::runtime::core::validate_pg_mutation_sql(sql)?;
    let params = crate::runtime::core::dispatch_params(&spec)?;
    let param_types = crate::runtime::core::dispatch_param_types(&spec)?;
    let result = crate::runtime::core::bind_typed_generic_pg_params(
        sqlx::query(sql),
        &params,
        param_types.as_deref(),
    )?
    .execute(pool)
    .await
    .map_err(map_err("typed control-plane write failed"))?;
    Ok(result.rows_affected())
}

async fn execute_typed_read(
    pool: &PgPool,
    op: LogicalRead,
) -> Result<Vec<serde_json::Value>, Status> {
    let ctx = native_compile_context();
    let compiled = crate::runtime::service::handlers_data::compile_logical_read_dispatch(
        &BackendKind::Postgres,
        &op,
        &ctx,
    )?;
    let spec: serde_json::Value = serde_json::from_str(&compiled.spec_json).map_err(|err| {
        control_store_internal_status(
            "typed_native_read_json",
            format!("typed native read JSON failed: {err}"),
        )
    })?;
    let sql = spec
        .get("sql")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            control_store_internal_status(
                "typed_native_read_compile",
                "typed native read did not compile to SQL",
            )
        })?;
    crate::runtime::core::validate_pg_read_sql(sql)?;
    let params = crate::runtime::core::dispatch_params(&spec)?;
    let param_types = crate::runtime::core::dispatch_param_types(&spec)?;
    let rows = crate::runtime::core::bind_typed_generic_pg_params(
        sqlx::query(sql),
        &params,
        param_types.as_deref(),
    )?
    .fetch_all(pool)
    .await
    .map_err(map_err("typed control-plane read failed"))?;
    crate::runtime::core::pg_rows_to_json(rows)
}

// ── ControlPlaneResource (the versioned registry) ──────────────────────────────

fn resource_select_columns() -> Vec<&'static str> {
    vec![
        "resource_id",
        "resource_type",
        "name",
        "tenant_id",
        "project_id",
        "version",
        "content_hash",
        "payload_json",
        "updated_by",
        "updated_at",
    ]
}

fn resource_select_clause() -> String {
    let m = native_model(RESOURCE_MSG, &resource_select_columns());
    let parts = vec![
        m.text_or_empty_as("resource_id", "resource_id"),
        m.text_or_empty_as("resource_type", "resource_type"),
        m.text_or_empty_as("name", "name"),
        m.text_or_empty_as("tenant_id", "tenant_id"),
        m.text_or_empty_as("project_id", "project_id"),
        m.text_or_empty_as("version", "version"),
        m.text_or_empty_as("content_hash", "content_hash"),
        m.json_text_as("payload_json", "payload_json"),
        m.text_or_empty_as("updated_by", "updated_by"),
        m.timestamp_unix_as("updated_at", "updated_at_unix"),
    ];
    parts.join(", ")
}

fn resource_row_from(row: &sqlx::postgres::PgRow) -> ResourceModel {
    ResourceModel {
        resource_id: row.try_get("resource_id").unwrap_or_default(),
        resource_type: row.try_get("resource_type").unwrap_or_default(),
        name: row.try_get("name").unwrap_or_default(),
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        project_id: row.try_get("project_id").unwrap_or_default(),
        version: row.try_get("version").unwrap_or_default(),
        content_hash: row.try_get("content_hash").unwrap_or_default(),
        payload_json: row
            .try_get("payload_json")
            .unwrap_or_else(|_| "{}".to_string()),
        updated_by: row.try_get("updated_by").unwrap_or_default(),
        updated_at_unix: row.try_get("updated_at_unix").unwrap_or(0),
    }
}

/// Upsert a resource keyed by the unique (resource_type, name, tenant_id).
///
/// The version is content-addressed: when an existing row already holds an
/// identical `content_hash` the row is left untouched (version + updated_at do
/// NOT change), so nodes never re-apply unchanged config. A content change bumps
/// `version`/`content_hash`/`updated_at`. `tenant_id` empty == fleet-wide (NULL).
///
/// NULL-safe upsert: an UPDATE guarded by `IS NOT DISTINCT FROM` on the nullable
/// tenant matches the fleet-wide row; if it touches no rows we INSERT. This
/// avoids relying on `ON CONFLICT` over a nullable unique column.
pub async fn upsert_resource(
    pool: &PgPool,
    resource_type: ResourceType,
    name: &str,
    tenant_id: &str,
    project_id: &str,
    payload_json: &str,
    updated_by: &str,
) -> Result<ResourceModel, Status> {
    validate_resource_upsert_input(resource_type, name, payload_json)?;
    let rt_db = resource_type_to_db(resource_type);
    let content_hash = content_version(payload_json);
    let tenant_opt = empty_to_none(tenant_id);
    let project_opt = empty_to_none(project_id);

    let m = native_model(
        RESOURCE_MSG,
        &[
            "resource_id",
            "resource_type",
            "name",
            "tenant_id",
            "project_id",
            "version",
            "content_hash",
            "payload_json",
            "updated_by",
            "updated_at",
        ],
    );
    // 1. Content-addressed UPDATE: only mutate when the content actually changed.
    let update_sql = format!(
        "UPDATE {rel} SET \
            {version} = $4, {chash} = $4, {payload} = $5::JSONB, {project} = $6, \
            {uby} = $7, {updated} = NOW() \
         WHERE {rtype} = $1 AND {name} = $2 AND {tenant} IS NOT DISTINCT FROM $3 \
           AND {chash} <> $4 \
         RETURNING {cols}",
        rel = m.relation,
        version = m.q("version"),
        chash = m.q("content_hash"),
        payload = m.q("payload_json"),
        project = m.q("project_id"),
        uby = m.q("updated_by"),
        updated = m.q("updated_at"),
        rtype = m.q("resource_type"),
        name = m.q("name"),
        tenant = m.q("tenant_id"),
        cols = resource_select_clause(),
    );
    if let Some(row) = sqlx::query(&update_sql)
        .bind(rt_db)
        .bind(name)
        .bind(tenant_opt.as_deref())
        .bind(&content_hash)
        .bind(payload_json)
        .bind(project_opt.as_deref())
        .bind(updated_by)
        .fetch_optional(pool)
        .await
        .map_err(map_err("control resource update failed"))?
    {
        return Ok(resource_row_from(&row));
    }

    // 2. No content change for an existing row → return it unchanged. (Distinct
    //    from "row absent": fetch decides which branch we are in.)
    if let Some(existing) =
        get_resource_by_key(pool, resource_type, name, tenant_opt.as_deref()).await?
    {
        return Ok(existing);
    }

    // 3. Brand-new resource → INSERT.
    let insert_sql = format!(
        "INSERT INTO {rel} \
            ({rid}, {rtype}, {name}, {tenant}, {project}, {version}, {chash}, {payload}, {uby}) \
         VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $5, $6::JSONB, $7) \
         RETURNING {cols}",
        rel = m.relation,
        rid = m.q("resource_id"),
        rtype = m.q("resource_type"),
        name = m.q("name"),
        tenant = m.q("tenant_id"),
        project = m.q("project_id"),
        version = m.q("version"),
        chash = m.q("content_hash"),
        payload = m.q("payload_json"),
        uby = m.q("updated_by"),
        cols = resource_select_clause(),
    );
    let row = sqlx::query(&insert_sql)
        .bind(rt_db)
        .bind(name)
        .bind(tenant_opt.as_deref())
        .bind(project_opt.as_deref())
        .bind(&content_hash)
        .bind(payload_json)
        .bind(updated_by)
        .fetch_one(pool)
        .await
        .map_err(map_err("control resource insert failed"))?;
    Ok(resource_row_from(&row))
}

/// Fetch one resource by its unique (type, name, tenant) key. `tenant=None` is
/// the fleet-wide (NULL) row.
pub async fn get_resource_by_key(
    pool: &PgPool,
    resource_type: ResourceType,
    name: &str,
    tenant_id: Option<&str>,
) -> Result<Option<ResourceModel>, Status> {
    let m = native_model(RESOURCE_MSG, &["resource_type", "name", "tenant_id"]);
    let sql = format!(
        "SELECT {cols} FROM {rel} \
         WHERE {rtype} = $1 AND {name} = $2 AND {tenant} IS NOT DISTINCT FROM $3",
        cols = resource_select_clause(),
        rel = m.relation,
        rtype = m.q("resource_type"),
        name = m.q("name"),
        tenant = m.q("tenant_id"),
    );
    let row = sqlx::query(&sql)
        .bind(resource_type_to_db(resource_type))
        .bind(name)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await
        .map_err(map_err("control resource get failed"))?;
    Ok(row.map(|r| resource_row_from(&r)))
}

/// List resources for one type, optionally filtered by tenant and by an explicit
/// name set (on-demand subscription).
///
/// Tenant semantics: `tenant=None` (or empty) returns ONLY fleet-wide (NULL
/// tenant) rows; `tenant=Some(t)` returns fleet-wide rows PLUS that tenant's rows
/// (so a node always sees the global config, and a tenant fetch layers its own on
/// top). `names` empty == the full state-of-the-world for the type.
pub async fn list_resources(
    pool: &PgPool,
    resource_type: ResourceType,
    tenant_id: Option<&str>,
    names: &[String],
) -> Result<Vec<ResourceModel>, Status> {
    let m = native_model(RESOURCE_MSG, &["resource_type", "name", "tenant_id"]);
    let tenant = tenant_id.filter(|t| !t.trim().is_empty());
    let names: Vec<String> = names
        .iter()
        .map(|n| n.trim().to_string())
        .filter(|n| !n.is_empty())
        .collect();
    let sql = format!(
        "SELECT {cols} FROM {rel} \
         WHERE {rtype} = $1 \
           AND ({tenant} IS NULL OR ($2::TEXT IS NOT NULL AND {tenant} = $2)) \
           AND ($3::TEXT[] IS NULL OR {name} = ANY($3)) \
         ORDER BY {name} ASC",
        cols = resource_select_clause(),
        rel = m.relation,
        rtype = m.q("resource_type"),
        tenant = m.q("tenant_id"),
        name = m.q("name"),
    );
    let names_param: Option<Vec<String>> = if names.is_empty() { None } else { Some(names) };
    let rows = sqlx::query(&sql)
        .bind(resource_type_to_db(resource_type))
        .bind(tenant)
        .bind(names_param.as_deref())
        .fetch_all(pool)
        .await
        .map_err(map_err("control resource list failed"))?;
    Ok(rows.iter().map(resource_row_from).collect())
}

/// The aggregate version-of-the-world for a (type, tenant, names) view — the
/// value a node ACKs against. Computed from the listed resources' content hashes
/// (order-independent), so it equals the version a [`DiscoveryResponse`] carries.
pub async fn world_version(
    pool: &PgPool,
    resource_type: ResourceType,
    tenant_id: Option<&str>,
    names: &[String],
) -> Result<String, Status> {
    let resources = list_resources(pool, resource_type, tenant_id, names).await?;
    Ok(aggregate_version(&resources))
}

/// The most recent `updated_at` (unix seconds) across ALL registry resources,
/// i.e. the emit time of the latest content change applied to the registry.
/// Returns `None` when the registry is empty. Used by the reload subscriber to
/// measure the policy-invalidation propagation lag (emit → apply).
pub async fn latest_updated_at_unix(pool: &PgPool) -> Result<Option<i64>, Status> {
    let m = native_model(RESOURCE_MSG, &["updated_at"]);
    let sql = format!(
        "SELECT {sel} FROM {rel} ORDER BY {ord} DESC LIMIT 1",
        sel = m.timestamp_unix_as("updated_at", "updated_at_unix"),
        rel = m.relation,
        ord = m.q("updated_at"),
    );
    let row = sqlx::query(&sql)
        .fetch_optional(pool)
        .await
        .map_err(map_err("latest updated_at query failed"))?;
    Ok(row.map(|r| r.try_get::<i64, _>("updated_at_unix").unwrap_or(0)))
}

// ── ControlPlaneNodeState (the ACK/NACK ledger) ───────────────────────────────

fn node_state_select_columns() -> Vec<&'static str> {
    vec![
        "node_id",
        "resource_type",
        "subscribed_names",
        "accepted_version",
        "last_good_version",
        "last_response_nonce",
        "nack_error_detail",
        "served_snapshots",
        "updated_at",
    ]
}

fn node_state_select_clause() -> String {
    let m = native_model(NODE_STATE_MSG, &node_state_select_columns());
    let parts = vec![
        m.text_or_empty_as("node_id", "node_id"),
        m.text_or_empty_as("resource_type", "resource_type"),
        m.json_text_as("subscribed_names", "subscribed_names_json"),
        m.text_or_empty_as("accepted_version", "accepted_version"),
        m.text_or_empty_as("last_good_version", "last_good_version"),
        m.text_or_empty_as("last_response_nonce", "last_response_nonce"),
        m.text_or_empty_as("nack_error_detail", "nack_error_detail"),
        m.json_text_as("served_snapshots", "served_snapshots_json"),
        m.timestamp_unix_as("updated_at", "updated_at_unix"),
    ];
    parts.join(", ")
}

fn node_state_row_from(row: &sqlx::postgres::PgRow) -> NodeStateRow {
    NodeStateRow {
        node_id: row.try_get("node_id").unwrap_or_default(),
        resource_type: row.try_get("resource_type").unwrap_or_default(),
        subscribed_names_json: row
            .try_get("subscribed_names_json")
            .unwrap_or_else(|_| "[]".to_string()),
        accepted_version: row.try_get("accepted_version").unwrap_or_default(),
        last_good_version: row.try_get("last_good_version").unwrap_or_default(),
        last_response_nonce: row.try_get("last_response_nonce").unwrap_or_default(),
        nack_error_detail: row.try_get("nack_error_detail").unwrap_or_default(),
        served_snapshots_json: row
            .try_get("served_snapshots_json")
            .unwrap_or_else(|_| "[]".to_string()),
        updated_at_unix: row.try_get("updated_at_unix").unwrap_or(0),
    }
}

fn node_state_row_from_json(row: &serde_json::Value) -> NodeStateRow {
    let updated_at_unix = row
        .get("updated_at")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.timestamp())
        .unwrap_or(0);
    NodeStateRow {
        node_id: row
            .get("node_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        resource_type: row
            .get("resource_type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        subscribed_names_json: row
            .get("subscribed_names")
            .map(|value| {
                if value.is_string() {
                    value.as_str().unwrap_or_default().to_string()
                } else {
                    value.to_string()
                }
            })
            .unwrap_or_else(|| "[]".to_string()),
        accepted_version: row
            .get("accepted_version")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        last_good_version: row
            .get("last_good_version")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        last_response_nonce: row
            .get("last_response_nonce")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        nack_error_detail: row
            .get("nack_error_detail")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        served_snapshots_json: row
            .get("served_snapshots")
            .map(|value| {
                if value.is_string() {
                    value.as_str().unwrap_or_default().to_string()
                } else {
                    value.to_string()
                }
            })
            .unwrap_or_else(|| "[]".to_string()),
        updated_at_unix,
    }
}

/// Ensure a ledger row exists for (node, type) and return it. Idempotent.
pub async fn ensure_node_state(
    pool: &PgPool,
    node_id: &str,
    resource_type: ResourceType,
    subscribed_names: &[String],
) -> Result<NodeStateRow, Status> {
    validate_node_id(node_id)?;
    let names_json = serde_json::to_string(subscribed_names).unwrap_or_else(|_| "[]".to_string());
    // Only overwrite the subscription set when the caller actually provided one,
    // so a bare ACK (no names) does not wipe an established subscription.
    let effective_names = if subscribed_names.is_empty() {
        // Don't clobber: re-read after a no-op upsert below.
        None
    } else {
        Some(names_json)
    };
    match effective_names {
        Some(json) => {
            let mut record = LogicalRecord::new();
            record.insert(
                "node_state_id".to_string(),
                LogicalValue::String(Uuid::new_v4().to_string()),
            );
            record.insert(
                "node_id".to_string(),
                LogicalValue::String(node_id.to_string()),
            );
            record.insert(
                "resource_type".to_string(),
                LogicalValue::String(resource_type_to_db(resource_type).to_string()),
            );
            record.insert(
                "subscribed_names".to_string(),
                serde_json::from_str(&json)
                    .map(LogicalValue::Json)
                    .map_err(|err| {
                        control_store_invalid_fields(
                            format!("subscribed_names JSON failed: {err}"),
                            [("subscribed_names", "must be valid subscribed_names JSON")],
                        )
                    })?,
            );
            record.insert(
                "updated_at".to_string(),
                LogicalValue::Timestamp(chrono::Utc::now()),
            );
            execute_typed_write(
                pool,
                LogicalWrite {
                    message_type: NODE_STATE_MSG.to_string(),
                    records: vec![record],
                    conflict: ConflictStrategy::update_on(
                        vec!["subscribed_names".to_string(), "updated_at".to_string()],
                        vec!["node_id".to_string(), "resource_type".to_string()],
                    ),
                    return_fields: Vec::new(),
                },
            )
            .await?;
        }
        None => {
            let mut record = LogicalRecord::new();
            record.insert(
                "node_state_id".to_string(),
                LogicalValue::String(Uuid::new_v4().to_string()),
            );
            record.insert(
                "node_id".to_string(),
                LogicalValue::String(node_id.to_string()),
            );
            record.insert(
                "resource_type".to_string(),
                LogicalValue::String(resource_type_to_db(resource_type).to_string()),
            );
            record.insert(
                "subscribed_names".to_string(),
                LogicalValue::Json(serde_json::json!([])),
            );
            execute_typed_write(
                pool,
                LogicalWrite {
                    message_type: NODE_STATE_MSG.to_string(),
                    records: vec![record],
                    conflict: ConflictStrategy::Ignore,
                    return_fields: Vec::new(),
                },
            )
            .await?;
        }
    }
    get_node_state(pool, node_id, resource_type)
        .await?
        .ok_or_else(|| {
            control_store_internal_status("ensure_node_state", "node state vanished after ensure")
        })
}

/// Fetch the ledger row for (node, type), if present.
pub async fn get_node_state(
    pool: &PgPool,
    node_id: &str,
    resource_type: ResourceType,
) -> Result<Option<NodeStateRow>, Status> {
    let rows = execute_typed_read(
        pool,
        LogicalRead {
            message_type: NODE_STATE_MSG.to_string(),
            filter: Some(LogicalFilter::And(vec![
                eq("node_id", LogicalValue::String(node_id.to_string())),
                eq(
                    "resource_type",
                    LogicalValue::String(resource_type_to_db(resource_type).to_string()),
                ),
            ])),
            projection: Some(LogicalProjection::fields(
                node_state_select_columns()
                    .into_iter()
                    .map(str::to_string)
                    .collect::<Vec<_>>(),
            )),
            sort: Vec::new(),
            include: Vec::new(),
            pagination: Some(LogicalPagination::limit(1)),
        },
    )
    .await?;
    Ok(rows.first().map(node_state_row_from_json))
}

/// Allocate the next monotonic nonce for (node, type) and stamp it as the
/// `last_response_nonce` (a response is being sent). Returns the new nonce string
/// `"<node-truncated>:<type-ordinal>:<counter>"` — globally unique per node+type.
pub async fn next_response_nonce(
    pool: &PgPool,
    node_id: &str,
    resource_type: ResourceType,
) -> Result<String, Status> {
    ensure_node_state(pool, node_id, resource_type, &[]).await?;
    let m = native_model(
        NODE_STATE_MSG,
        &[
            "node_id",
            "resource_type",
            "nonce_counter",
            "last_response_nonce",
        ],
    );
    let rt_db = resource_type_to_db(resource_type);
    // Bump the counter atomically and build the nonce from the new value, so the
    // nonce is monotonic per (node, resource_type) and uniquely identifies the
    // response the node must echo to ACK/NACK.
    let sql = format!(
        "UPDATE {rel} SET {counter} = {counter} + 1, \
            {nonce} = $2 || ':' || ({counter} + 1)::TEXT, {updated} = NOW() \
         WHERE {node} = $1 AND {rtype} = $3 \
         RETURNING {nonce} AS new_nonce",
        rel = m.relation,
        counter = m.q("nonce_counter"),
        nonce = m.q("last_response_nonce"),
        updated = m.q("updated_at"),
        node = m.q("node_id"),
        rtype = m.q("resource_type"),
    );
    let nonce_prefix = format!("{rt_db}:{node_id}");
    let row = sqlx::query(&sql)
        .bind(node_id)
        .bind(&nonce_prefix)
        .bind(rt_db)
        .fetch_one(pool)
        .await
        .map_err(map_err("control nonce allocation failed"))?;
    Ok(row.try_get::<String, _>("new_nonce").unwrap_or_default())
}

/// Record an ACK: the node applied `accepted_version`. Only honored when
/// `nonce` matches the ledger's `last_response_nonce` (stale acks are ignored).
/// Advances `accepted_version` + `last_good_version`, clears `nack_error_detail`.
/// Returns true when the ack was applied (matching nonce), false when ignored.
pub async fn record_ack(
    pool: &PgPool,
    node_id: &str,
    resource_type: ResourceType,
    accepted_version: &str,
    nonce: &str,
) -> Result<bool, Status> {
    ensure_node_state(pool, node_id, resource_type, &[]).await?;
    let m = native_model(
        NODE_STATE_MSG,
        &[
            "node_id",
            "resource_type",
            "accepted_version",
            "last_good_version",
            "last_response_nonce",
            "nack_error_detail",
        ],
    );
    let sql = format!(
        "UPDATE {rel} SET \
            {accepted} = $3, {good} = $3, {nack} = NULL, {updated} = NOW() \
         WHERE {node} = $1 AND {rtype} = $2 \
           AND {nonce} IS NOT DISTINCT FROM $4",
        rel = m.relation,
        accepted = m.q("accepted_version"),
        good = m.q("last_good_version"),
        nack = m.q("nack_error_detail"),
        updated = m.q("updated_at"),
        node = m.q("node_id"),
        rtype = m.q("resource_type"),
        nonce = m.q("last_response_nonce"),
    );
    let result = sqlx::query(&sql)
        .bind(node_id)
        .bind(resource_type_to_db(resource_type))
        .bind(accepted_version)
        .bind(nonce)
        .execute(pool)
        .await
        .map_err(map_err("control ack record failed"))?;
    Ok(result.rows_affected() > 0)
}

/// Record a NACK: the node REJECTED the pushed version. The node keeps its
/// `last_good_version` and `accepted_version` is NOT advanced — so a bad policy
/// is rejected without the node silently diverging. `nack_error_detail` is set.
/// Only honored when `nonce` matches `last_response_nonce`. Returns true when
/// applied.
pub async fn record_nack(
    pool: &PgPool,
    node_id: &str,
    resource_type: ResourceType,
    nonce: &str,
    error_detail: &str,
) -> Result<bool, Status> {
    ensure_node_state(pool, node_id, resource_type, &[]).await?;
    let m = native_model(
        NODE_STATE_MSG,
        &[
            "node_id",
            "resource_type",
            "nack_error_detail",
            "last_response_nonce",
        ],
    );
    // Deliberately does NOT touch accepted_version or last_good_version.
    let sql = format!(
        "UPDATE {rel} SET {nack} = $3, {updated} = NOW() \
         WHERE {node} = $1 AND {rtype} = $2 \
           AND {nonce} IS NOT DISTINCT FROM $4",
        rel = m.relation,
        nack = m.q("nack_error_detail"),
        updated = m.q("updated_at"),
        node = m.q("node_id"),
        rtype = m.q("resource_type"),
        nonce = m.q("last_response_nonce"),
    );
    let result = sqlx::query(&sql)
        .bind(node_id)
        .bind(resource_type_to_db(resource_type))
        .bind(error_detail)
        .bind(nonce)
        .execute(pool)
        .await
        .map_err(map_err("control nack record failed"))?;
    Ok(result.rows_affected() > 0)
}

/// Retain the snapshot just SERVED to (node, resource_type): append it to the
/// node's bounded `served_snapshots` ring (newest last) and evict the oldest past
/// [`RETENTION_DEPTH`], so a later [`RollbackResources`] has a durable target.
///
/// Re-serving the SAME newest version is a no-op (the serving loop only emits on
/// a real version change, but this guard keeps the ring free of duplicates and
/// stops a churn from rotating good snapshots out). The full `resources` set
/// behind `version` is captured so a rollback re-publishes the exact world the
/// node applied (make-before-break preserved on the way back).
pub async fn retain_served_snapshot(
    pool: &PgPool,
    node_id: &str,
    resource_type: ResourceType,
    version: &str,
    resources: &[ResourceModel],
) -> Result<(), Status> {
    let row = ensure_node_state(pool, node_id, resource_type, &[]).await?;
    let mut snaps = parse_served_snapshots(&row.served_snapshots_json);
    if snaps.last().map(|s| s.version.as_str()) == Some(version) {
        return Ok(());
    }
    snaps.push(RetainedSnapshot {
        version: version.to_string(),
        served_at_unix: chrono::Utc::now().timestamp(),
        resources: resources
            .iter()
            .map(|r| RetainedResource {
                name: r.name.clone(),
                tenant_id: r.tenant_id.clone(),
                project_id: r.project_id.clone(),
                payload_json: r.payload_json.clone(),
            })
            .collect(),
    });
    // Ring eviction: keep only the newest RETENTION_DEPTH snapshots.
    let overflow = snaps.len().saturating_sub(RETENTION_DEPTH);
    if overflow > 0 {
        snaps.drain(0..overflow);
    }
    let json = served_snapshots_to_json(&snaps).to_string();
    let m = native_model(
        NODE_STATE_MSG,
        &["node_id", "resource_type", "served_snapshots", "updated_at"],
    );
    // Touch ONLY the ring column for this (node, type) — never the ACK/NACK
    // version columns, so retention can't perturb the state machine.
    let sql = format!(
        "UPDATE {rel} SET {snaps} = $3::JSONB, {updated} = NOW() \
         WHERE {node} = $1 AND {rtype} = $2",
        rel = m.relation,
        snaps = m.q("served_snapshots"),
        updated = m.q("updated_at"),
        node = m.q("node_id"),
        rtype = m.q("resource_type"),
    );
    sqlx::query(&sql)
        .bind(node_id)
        .bind(resource_type_to_db(resource_type))
        .bind(&json)
        .execute(pool)
        .await
        .map_err(map_err("control snapshot retention failed"))?;
    Ok(())
}

/// Resolve the retained snapshot a [`RollbackResources`] should re-serve for
/// (node, resource_type). When `target_version` is given, the retained snapshot
/// whose version matches (newest match wins); otherwise the most-recently
/// retained snapshot whose version DIFFERS from the node's current world (the
/// "prior good"). Returns `None` when nothing suitable is retained so the caller
/// fails closed instead of silently no-opping. Reuses [`world_version`] — the
/// single existing version resolver — for the current-world comparison.
pub async fn find_rollback_target(
    pool: &PgPool,
    node_id: &str,
    resource_type: ResourceType,
    target_version: Option<&str>,
) -> Result<Option<RetainedSnapshot>, Status> {
    let row = match get_node_state(pool, node_id, resource_type).await? {
        Some(row) => row,
        None => return Ok(None),
    };
    let snaps = parse_served_snapshots(&row.served_snapshots_json);
    let found = match target_version.map(str::trim).filter(|v| !v.is_empty()) {
        Some(target) => snaps.into_iter().rev().find(|s| s.version == target),
        None => {
            // The node's current world for its subscription set (reuses the
            // existing resolver — no second tenant/version path).
            let names = parse_subscribed_names(&row.subscribed_names_json);
            let current = world_version(pool, resource_type, None, &names).await?;
            snaps.into_iter().rev().find(|s| s.version != current)
        }
    };
    Ok(found)
}

/// List ledger rows for admin visibility, optionally filtered by node and/or
/// type. `resource_type == Unspecified` means "all types".
pub async fn list_node_states(
    pool: &PgPool,
    node_id: Option<&str>,
    resource_type: ResourceType,
    limit: i64,
    offset: i64,
) -> Result<(Vec<NodeStateRow>, i64), Status> {
    let m = native_model(NODE_STATE_MSG, &["node_id", "resource_type"]);
    let node = node_id.filter(|n| !n.trim().is_empty());
    let rtype = if resource_type == ResourceType::Unspecified {
        None
    } else {
        Some(resource_type_to_db(resource_type))
    };
    let where_clause = format!(
        "($1::TEXT IS NULL OR {node} = $1) AND ($2::TEXT IS NULL OR {rtype} = $2)",
        node = m.q("node_id"),
        rtype = m.q("resource_type"),
    );
    let count_sql = format!(
        "SELECT COUNT(*) AS total FROM {rel} WHERE {where_clause}",
        rel = m.relation,
    );
    let total: i64 = sqlx::query(&count_sql)
        .bind(node)
        .bind(rtype)
        .fetch_one(pool)
        .await
        .map_err(map_err("control node-state count failed"))?
        .try_get("total")
        .unwrap_or(0);
    let list_sql = format!(
        "SELECT {cols} FROM {rel} WHERE {where_clause} \
         ORDER BY {node} ASC, {rtype} ASC LIMIT $3 OFFSET $4",
        cols = node_state_select_clause(),
        rel = m.relation,
        node = m.q("node_id"),
        rtype = m.q("resource_type"),
    );
    let rows = sqlx::query(&list_sql)
        .bind(node)
        .bind(rtype)
        .bind(limit.max(1))
        .bind(offset.max(0))
        .fetch_all(pool)
        .await
        .map_err(map_err("control node-state list failed"))?;
    Ok((rows.iter().map(node_state_row_from).collect(), total))
}

/// Combined fleet-wide "version-of-the-world" across EVERY distributed resource
/// type (definitions + policies), in dependency order. Used by the in-process
/// reload subscriber to cheaply detect "did anything in the registry change?"
/// without re-reading payloads on the hot path: the per-type world versions are
/// concatenated in [`resources::ordered_resource_types`] order and hashed, so any
/// add/remove/edit of any resource flips the fingerprint. Fleet-wide only
/// (tenant=None); per-tenant overlays are pulled on demand by the stream.
pub async fn fleet_world_fingerprint(pool: &PgPool) -> Result<String, Status> {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for rt in resources::ordered_resource_types() {
        let version = world_version(pool, *rt, None, &[]).await?;
        resource_type_to_db(*rt).hash(&mut hasher);
        version.hash(&mut hasher);
    }
    Ok(format!("cp-fleet-{:016x}", hasher.finish()))
}

fn empty_to_none(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Parse the stored `subscribed_names` JSON array into a Vec.
pub fn parse_subscribed_names(json: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(json.trim()).unwrap_or_default()
}

/// Decode the stored `resource_type` string into the proto enum (re-exported for
/// handler convenience).
pub fn resource_type_of(row_type: &str) -> ResourceType {
    resources::resource_type_from_db(row_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::{Code, Status};

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_validation_field(status: &Status, field: &str, description: &str) {
        assert_eq!(status.code(), Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "control_plane");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn control_store_internal_status_carries_typed_detail() {
        let err = control_store_internal_status(
            "typed_native_read_compile",
            "typed native read did not compile to SQL",
        );

        assert_internal_detail(
            &err,
            "typed_native_read_compile",
            "typed native read did not compile to SQL",
        );
    }

    #[test]
    fn upsert_resource_missing_name_carries_field_violation() {
        let err = validate_resource_upsert_input(ResourceType::BackendTargetDefinition, " ", "{}")
            .expect_err("missing resource name must fail before Postgres availability");

        assert_eq!(err.message(), "resource name is required");
        assert_validation_field(
            &err,
            "name",
            "must be a non-empty control-plane resource name",
        );
    }

    #[test]
    fn upsert_resource_missing_type_carries_field_violation() {
        let err = validate_resource_upsert_input(ResourceType::Unspecified, "primary", "{}")
            .expect_err("missing resource_type must fail before Postgres availability");

        assert_eq!(err.message(), "resource_type is required");
        assert_validation_field(
            &err,
            "resource_type",
            "must specify a control-plane resource type",
        );
    }

    #[test]
    fn upsert_resource_invalid_payload_json_carries_field_violation() {
        let err =
            validate_resource_upsert_input(ResourceType::BackendTargetDefinition, "primary", "{")
                .expect_err("invalid payload_json must fail before Postgres availability");

        assert_eq!(err.message(), "payload_json must be valid JSON");
        assert_validation_field(&err, "payload_json", "must be valid JSON");
    }

    #[test]
    fn ensure_node_state_missing_node_id_carries_field_violation() {
        let err = validate_node_id(" ")
            .expect_err("missing node_id must fail before Postgres availability");

        assert_eq!(err.message(), "node_id is required");
        assert_validation_field(&err, "node_id", "must be a non-empty control-plane node id");
    }

    #[test]
    fn subscribed_names_json_failure_carries_field_violation() {
        let err = control_store_invalid_fields(
            "subscribed_names JSON failed: forced",
            [("subscribed_names", "must be valid subscribed_names JSON")],
        );

        assert_eq!(err.message(), "subscribed_names JSON failed: forced");
        assert_validation_field(
            &err,
            "subscribed_names",
            "must be valid subscribed_names JSON",
        );
    }
}
