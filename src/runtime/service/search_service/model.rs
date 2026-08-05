//! The `StoredIndex` DTO, the engine-collection resolver, and the JSON / PgRow
//! decoders for the native `SearchService`. Extracted verbatim from the former
//! god file — the mediated-read row decode and the worker-side `PgRow` decode
//! are byte-for-byte identical.

use sqlx::Row;

/// Engine collection an index queries/writes: the explicit resource name, or
/// the index name when unset. ONE resolution shared by query, freshness,
/// reindex, teardown, and provisioning.
pub(crate) fn collection_name(resource_name: &str, index_name: &str) -> String {
    if resource_name.trim().is_empty() {
        index_name.to_string()
    } else {
        resource_name.to_string()
    }
}

/// SRCH1: namespace an engine point-id by the verified tenant so two tenants that
/// share a collection and reuse the same source primary key cannot clobber or
/// delete each other's vectors on the ingest / reindex / teardown write paths.
/// The search READ path strips the prefix ([`strip_tenant_point_id`]) so
/// consumers still see the raw PK they indexed. A blank tenant (no isolation
/// boundary to enforce) is left unscoped so pre-existing points stay reachable.
pub(crate) fn tenant_scoped_point_id(tenant_id: &str, pk: &str) -> String {
    let tenant = tenant_id.trim();
    if tenant.is_empty() {
        pk.to_string()
    } else {
        format!("{tenant}:{pk}")
    }
}

/// Inverse of [`tenant_scoped_point_id`]: strip the verified-tenant prefix from a
/// returned hit id so the consumer sees the raw source PK. Only the exact
/// `"{tenant}:"` prefix is removed (the tenant is the verified claim), so a PK
/// that itself contains ':' is preserved intact.
pub(crate) fn strip_tenant_point_id(tenant_id: &str, id: &str) -> String {
    let tenant = tenant_id.trim();
    if tenant.is_empty() {
        return id.to_string();
    }
    let prefix = format!("{tenant}:");
    id.strip_prefix(&prefix)
        .map(str::to_string)
        .unwrap_or_else(|| id.to_string())
}

/// A registered index decoded from the native read JSON.
pub(crate) struct StoredIndex {
    pub(crate) index_id: String,
    pub(crate) index_name: String,
    pub(crate) source_message_type: String,
    pub(crate) backend: String,
    pub(crate) resource_name: String,
    pub(crate) vector_dims: i32,
    pub(crate) tenant_column: String,
    pub(crate) source_cdc_topic: String,
    pub(crate) status: String,
}

impl StoredIndex {
    pub(crate) fn collection(&self) -> String {
        collection_name(&self.resource_name, &self.index_name)
    }
}

fn index_json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

pub(crate) fn json_str(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(serde_json::Value::Number(value)) => value.to_string(),
        Some(serde_json::Value::Bool(value)) => value.to_string(),
        _ => String::new(),
    }
}

fn json_i32(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i32 {
    match row.get(key) {
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0) as i32,
        Some(serde_json::Value::String(value)) => value.trim().parse::<i32>().unwrap_or(0),
        _ => 0,
    }
}

pub(crate) fn stored_index_from_json(row: &serde_json::Value) -> StoredIndex {
    let map = index_json_object(row);
    StoredIndex {
        index_id: json_str(map, "index_id"),
        index_name: json_str(map, "index_name"),
        source_message_type: json_str(map, "source_message_type"),
        backend: json_str(map, "backend"),
        resource_name: json_str(map, "resource_name"),
        vector_dims: json_i32(map, "vector_dims"),
        tenant_column: json_str(map, "tenant_column"),
        source_cdc_topic: json_str(map, "source_cdc_topic"),
        status: json_str(map, "status"),
    }
}

pub(crate) fn stored_index_from_pg_row(row: &sqlx::postgres::PgRow) -> Result<StoredIndex, String> {
    Ok(StoredIndex {
        index_id: row
            .try_get("index_id")
            .map_err(|err| format!("decode search index id failed: {err}"))?,
        index_name: row
            .try_get("index_name")
            .map_err(|err| format!("decode search index name failed: {err}"))?,
        source_message_type: row
            .try_get("source_message_type")
            .map_err(|err| format!("decode search source message type failed: {err}"))?,
        backend: row
            .try_get("backend")
            .map_err(|err| format!("decode search index backend failed: {err}"))?,
        resource_name: row
            .try_get("resource_name")
            .map_err(|err| format!("decode search resource name failed: {err}"))?,
        vector_dims: row
            .try_get("vector_dims")
            .map_err(|err| format!("decode search vector dims failed: {err}"))?,
        tenant_column: row
            .try_get("tenant_column")
            .map_err(|err| format!("decode search tenant column failed: {err}"))?,
        source_cdc_topic: row
            .try_get("source_cdc_topic")
            .map_err(|err| format!("decode search source cdc topic failed: {err}"))?,
        status: row
            .try_get("status")
            .map_err(|err| format!("decode search index status failed: {err}"))?,
    })
}
