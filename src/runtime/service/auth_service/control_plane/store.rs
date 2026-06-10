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

use crate::proto::udb::core::control::entity::v1::ResourceType;
use crate::runtime::native_catalog::native_model;

use super::resources::{
    self, ResourceModel, aggregate_version, content_version, resource_type_to_db,
};

pub const RESOURCE_MSG: &str = "udb.core.control.entity.v1.ControlPlaneResource";
pub const NODE_STATE_MSG: &str = "udb.core.control.entity.v1.ControlPlaneNodeState";

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
    pub updated_at_unix: i64,
}

fn map_err(context: &'static str) -> impl Fn(sqlx::Error) -> Status {
    move |err| Status::internal(format!("{context}: {err}"))
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
    if name.trim().is_empty() {
        return Err(Status::invalid_argument("resource name is required"));
    }
    if resource_type == ResourceType::Unspecified {
        return Err(Status::invalid_argument("resource_type is required"));
    }
    // Reject non-JSON payloads early so the registry only ever holds valid bodies.
    if serde_json::from_str::<serde_json::Value>(payload_json.trim()).is_err() {
        return Err(Status::invalid_argument("payload_json must be valid JSON"));
    }
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
            {version} = $5, {chash} = $5, {payload} = $6::JSONB, {project} = $7, \
            {uby} = $8, {updated} = NOW() \
         WHERE {rtype} = $1 AND {name} = $2 AND {tenant} IS NOT DISTINCT FROM $3 \
           AND {chash} <> $5 \
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
        updated_at_unix: row.try_get("updated_at_unix").unwrap_or(0),
    }
}

/// Ensure a ledger row exists for (node, type) and return it. Idempotent.
pub async fn ensure_node_state(
    pool: &PgPool,
    node_id: &str,
    resource_type: ResourceType,
    subscribed_names: &[String],
) -> Result<NodeStateRow, Status> {
    if node_id.trim().is_empty() {
        return Err(Status::invalid_argument("node_id is required"));
    }
    let m = native_model(
        NODE_STATE_MSG,
        &[
            "node_state_id",
            "node_id",
            "resource_type",
            "subscribed_names",
        ],
    );
    let names_json = serde_json::to_string(subscribed_names).unwrap_or_else(|_| "[]".to_string());
    let insert_sql = format!(
        "INSERT INTO {rel} ({id}, {node}, {rtype}, {subs}) \
         VALUES (gen_random_uuid(), $1, $2, $3::JSONB) \
         ON CONFLICT ({node}, {rtype}) DO UPDATE SET {subs} = EXCLUDED.{subs}, {updated} = NOW()",
        rel = m.relation,
        id = m.q("node_state_id"),
        node = m.q("node_id"),
        rtype = m.q("resource_type"),
        subs = m.q("subscribed_names"),
        updated = m.q("updated_at"),
    );
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
            sqlx::query(&insert_sql)
                .bind(node_id)
                .bind(resource_type_to_db(resource_type))
                .bind(json)
                .execute(pool)
                .await
                .map_err(map_err("control node-state ensure failed"))?;
        }
        None => {
            let bare_sql = format!(
                "INSERT INTO {rel} ({id}, {node}, {rtype}, {subs}) \
                 VALUES (gen_random_uuid(), $1, $2, '[]'::JSONB) \
                 ON CONFLICT ({node}, {rtype}) DO NOTHING",
                rel = m.relation,
                id = m.q("node_state_id"),
                node = m.q("node_id"),
                rtype = m.q("resource_type"),
                subs = m.q("subscribed_names"),
            );
            sqlx::query(&bare_sql)
                .bind(node_id)
                .bind(resource_type_to_db(resource_type))
                .execute(pool)
                .await
                .map_err(map_err("control node-state ensure failed"))?;
        }
    }
    get_node_state(pool, node_id, resource_type)
        .await?
        .ok_or_else(|| Status::internal("node state vanished after ensure"))
}

/// Fetch the ledger row for (node, type), if present.
pub async fn get_node_state(
    pool: &PgPool,
    node_id: &str,
    resource_type: ResourceType,
) -> Result<Option<NodeStateRow>, Status> {
    let m = native_model(NODE_STATE_MSG, &["node_id", "resource_type"]);
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {node} = $1 AND {rtype} = $2",
        cols = node_state_select_clause(),
        rel = m.relation,
        node = m.q("node_id"),
        rtype = m.q("resource_type"),
    );
    let row = sqlx::query(&sql)
        .bind(node_id)
        .bind(resource_type_to_db(resource_type))
        .fetch_optional(pool)
        .await
        .map_err(map_err("control node-state get failed"))?;
    Ok(row.map(|r| node_state_row_from(&r)))
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
