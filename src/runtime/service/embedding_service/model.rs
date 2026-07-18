//! The `StoredSource` DTO, its effective-collection resolver, the JSON decoders
//! for the native read path, and the tenant-tagged embedding-point builder for
//! the native `EmbeddingService`. Extracted verbatim from the former god file —
//! the mediated-read row decode and the fail-closed point builder are
//! byte-for-byte identical.

use tonic::Status;

use crate::proto::VectorPointMutation;

use super::config::DEFAULT_VECTOR_COLLECTION;
use super::errors::{
    embedding_field_violation, embedding_policy_status_with_code, embedding_required_field,
};

/// A registered source decoded from the native read JSON.
pub(crate) struct StoredSource {
    pub(crate) source_id: String,
    pub(crate) tenant_id: String,
    pub(crate) source_name: String,
    pub(crate) source_message_type: String,
    pub(crate) text_fields_json: String,
    pub(crate) target_collection: String,
    pub(crate) model_id: String,
    pub(crate) tenant_column: String,
    pub(crate) source_cdc_topic: String,
    pub(crate) status: String,
}

impl StoredSource {
    /// The effective vector collection (falls back to the asset default only if a
    /// row was persisted without one, which register-time validation prevents).
    pub(crate) fn collection(&self) -> String {
        if self.target_collection.trim().is_empty() {
            DEFAULT_VECTOR_COLLECTION.to_string()
        } else {
            self.target_collection.clone()
        }
    }
}

fn source_json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

fn json_str(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(serde_json::Value::Number(value)) => value.to_string(),
        Some(serde_json::Value::Bool(value)) => value.to_string(),
        Some(value @ serde_json::Value::Array(_)) => value.to_string(),
        Some(value @ serde_json::Value::Object(_)) => value.to_string(),
        _ => String::new(),
    }
}

pub(crate) fn stored_source_from_json(row: &serde_json::Value) -> StoredSource {
    let map = source_json_object(row);
    StoredSource {
        source_id: json_str(map, "source_id"),
        tenant_id: json_str(map, "tenant_id"),
        source_name: json_str(map, "source_name"),
        source_message_type: json_str(map, "source_message_type"),
        text_fields_json: json_str(map, "text_fields_json"),
        target_collection: json_str(map, "target_collection"),
        model_id: json_str(map, "model_id"),
        tenant_column: json_str(map, "tenant_column"),
        source_cdc_topic: json_str(map, "source_cdc_topic"),
        status: json_str(map, "status"),
    }
}

/// Build the tenant-tagged vector point for a reported embedding. Fail CLOSED: an
/// empty (unverified) tenant is rejected so an unscoped vector can never be stored
/// (no fail-open); the stored point carries the `_tenant_id` payload key — the same
/// key the Qdrant IR compiler stamps at write time and the search seam ANDs into
/// its `must` clause — so a later Retrieve's server-side tenant filter matches.
/// Pure — unit-tested without an engine.
pub(crate) fn build_embedding_point(
    row_pk: &str,
    vector: Vec<f32>,
    tenant_id: &str,
    source_name: &str,
) -> Result<VectorPointMutation, Status> {
    let tenant = tenant_id.trim();
    if tenant.is_empty() {
        return Err(embedding_policy_status_with_code(
            tonic::Code::PermissionDenied,
            "embedding_vector_upsert",
            "verified_tenant_required",
            "embedding upsert requires a verified tenant; refusing to store an unscoped vector \
             (no fail-open)",
        ));
    }
    if row_pk.trim().is_empty() {
        return Err(embedding_required_field(
            "row_pk",
            "must be a non-empty source row primary key",
            "row_pk is required",
        ));
    }
    if vector.is_empty() {
        return Err(embedding_required_field(
            "vector",
            "must contain at least one embedding dimension",
            "vector is required",
        ));
    }
    // Tag the isolation key, the owning source, AND the chunk's parent row.
    // `_source` lets the source-teardown pass erase every point of a deleted
    // source by payload filter — retention-independent — instead of re-deriving
    // point ids from `udb.embedding.work.v1` journal events that log retention
    // eventually purges (the GDPR erasure hole). `_parent_pk` (+ `_chunk_seq`)
    // carries chunk provenance so a single source ROW's chunks are erased/
    // re-embedded together by filter (the id itself may be composite
    // `row_pk#chunk:seq`). All tags are written from verified/server-derived
    // values, never raw body, and are normalized/stripped from every RetrieveHit.
    let (parent_pk, chunk_seq) = super::chunking::parse_chunk_point_id(row_pk);
    let mut payload_json = serde_json::json!({
        "_tenant_id": tenant,
        "_parent_pk": parent_pk,
        "_chunk_seq": chunk_seq,
    });
    let source = source_name.trim();
    if !source.is_empty() {
        payload_json["_source"] = serde_json::Value::String(source.to_string());
    }
    let payload = crate::runtime::executor_utils::json_to_struct(&payload_json);
    Ok(VectorPointMutation {
        id: row_pk.to_string(),
        vector,
        payload,
    })
}

/// Build the Qdrant-style payload filter that scopes a source-teardown delete to
/// exactly one source's points: `_tenant_id` AND `_source`, both authoritative
/// `must` clauses. Returns `None` when either scope key is empty — a filter with
/// only one clause could delete another tenant's or another source's vectors, so
/// the caller must fall back to the point-id enumeration rather than issue an
/// under-scoped delete. Pure — unit-tested without an engine.
pub(crate) fn source_teardown_filter(
    tenant_id: &str,
    source_name: &str,
) -> Option<serde_json::Value> {
    let tenant = tenant_id.trim();
    let source = source_name.trim();
    if tenant.is_empty() || source.is_empty() {
        return None;
    }
    Some(serde_json::json!({
        "must": [
            { "key": "_tenant_id", "match": { "value": tenant } },
            { "key": "_source", "match": { "value": source } },
        ]
    }))
}

/// Build the payload filter that scopes a delete to exactly one source ROW's
/// chunks: `_tenant_id` AND `_source` AND `_parent_pk`, all authoritative `must`
/// clauses. Used to erase every chunk of a row on a CDC delete/re-embed without
/// enumerating the (variable, possibly shrunk) set of chunk ids. Returns `None`
/// when any scope key is empty — an under-scoped filter could delete another
/// tenant's/source's/row's vectors, so the caller must fall back. Pure.
pub(crate) fn row_teardown_filter(
    tenant_id: &str,
    source_name: &str,
    parent_pk: &str,
) -> Option<serde_json::Value> {
    let tenant = tenant_id.trim();
    let source = source_name.trim();
    let parent = parent_pk.trim();
    if tenant.is_empty() || source.is_empty() || parent.is_empty() {
        return None;
    }
    Some(serde_json::json!({
        "must": [
            { "key": "_tenant_id", "match": { "value": tenant } },
            { "key": "_source", "match": { "value": source } },
            { "key": "_parent_pk", "match": { "value": parent } },
        ]
    }))
}

/// Merge a caller-supplied Qdrant-style filter under the mandatory server-side
/// tenant clause and return the combined engine filter as a JSON value (the
/// caller wraps it with `json_to_struct`).
///
/// The `_tenant_id` `must` clause is ALWAYS first and authoritative: because it
/// is ANDed in `must`, no caller filter can broaden a query beyond the verified
/// tenant. Callers may not reference any internal `_`-prefixed payload key (the
/// write-stamp namespace, e.g. `_tenant_id`) — such a filter is a tenant-escape
/// attempt and is rejected fail-closed, never silently dropped. Only the
/// `must` / `should` / `must_not` groups are accepted.
pub(crate) fn merge_retrieve_filter(
    tenant_id: &str,
    filter_json: &str,
) -> Result<serde_json::Value, Status> {
    let tenant_clause = serde_json::json!({ "key": "_tenant_id", "match": { "value": tenant_id } });
    let trimmed = filter_json.trim();
    if trimmed.is_empty() {
        return Ok(serde_json::json!({ "must": [tenant_clause] }));
    }
    let user: serde_json::Value = serde_json::from_str(trimmed).map_err(|err| {
        embedding_field_violation(
            "filter_json",
            "must be a JSON object with must/should/must_not condition arrays",
            format!("filter_json is not valid JSON: {err}"),
        )
    })?;
    let obj = user.as_object().ok_or_else(|| {
        embedding_field_violation(
            "filter_json",
            "must be a JSON object with must/should/must_not condition arrays",
            "filter_json must be a JSON object",
        )
    })?;

    let mut must = vec![tenant_clause];
    let mut out = serde_json::Map::new();
    for (group, value) in obj {
        if !matches!(group.as_str(), "must" | "should" | "must_not") {
            return Err(embedding_field_violation(
                "filter_json",
                "only the must/should/must_not condition groups are supported",
                format!("unsupported filter group {group:?}"),
            ));
        }
        let conditions = value.as_array().ok_or_else(|| {
            embedding_field_violation(
                "filter_json",
                "each filter group must be an array of conditions",
                format!("filter group {group:?} must be an array"),
            )
        })?;
        for condition in conditions {
            reject_internal_filter_key(condition)?;
        }
        if group == "must" {
            must.extend(conditions.iter().cloned());
        } else {
            out.insert(group.clone(), value.clone());
        }
    }
    out.insert("must".to_string(), serde_json::Value::Array(must));
    Ok(serde_json::Value::Object(out))
}

/// Reject any filter condition (recursively, through nested must/should/must_not)
/// that targets an internal `_`-prefixed payload key — the server-owned isolation
/// namespace a caller must never address.
fn reject_internal_filter_key(condition: &serde_json::Value) -> Result<(), Status> {
    if let Some(key) = condition.get("key").and_then(|key| key.as_str()) {
        if key.starts_with('_') {
            return Err(embedding_field_violation(
                "filter_json",
                "filter conditions may not reference internal payload keys",
                format!(
                    "filter key {key:?} is reserved (tenant/project isolation is server-enforced)"
                ),
            ));
        }
    }
    for nested in ["must", "should", "must_not"] {
        if let Some(inner) = condition.get(nested).and_then(|value| value.as_array()) {
            for child in inner {
                reject_internal_filter_key(child)?;
            }
        }
    }
    Ok(())
}
