//! The `StoredSource` DTO, its effective-collection resolver, the JSON decoders
//! for the native read path, and the tenant-tagged embedding-point builder for
//! the native `EmbeddingService`. Extracted verbatim from the former god file —
//! the mediated-read row decode and the fail-closed point builder are
//! byte-for-byte identical.

use chrono::DateTime;
use tonic::Status;

use crate::proto::VectorPointMutation;

use super::config::DEFAULT_VECTOR_COLLECTION;
use super::errors::{
    embedding_field_violation, embedding_policy_status_with_code, embedding_required_field,
};

/// A registered source decoded from the native read JSON.
#[derive(Clone)]
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

pub(crate) fn json_str(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(serde_json::Value::Number(value)) => value.to_string(),
        Some(serde_json::Value::Bool(value)) => value.to_string(),
        Some(value @ serde_json::Value::Array(_)) => value.to_string(),
        Some(value @ serde_json::Value::Object(_)) => value.to_string(),
        _ => String::new(),
    }
}

pub(crate) fn json_i32(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i32 {
    row.get(key)
        .and_then(|value| match value {
            serde_json::Value::Number(number) => number.as_i64(),
            serde_json::Value::String(value) => value.parse().ok(),
            _ => None,
        })
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default()
}

pub(crate) fn json_i64(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    row.get(key)
        .and_then(|value| match value {
            serde_json::Value::Number(number) => number.as_i64(),
            serde_json::Value::String(value) => value.parse().ok(),
            _ => None,
        })
        .unwrap_or_default()
}

pub(crate) fn json_bool(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> bool {
    row.get(key)
        .and_then(|value| match value {
            serde_json::Value::Bool(value) => Some(*value),
            serde_json::Value::String(value) => value.parse().ok(),
            _ => None,
        })
        .unwrap_or_default()
}

pub(crate) fn json_timestamp_ms(
    row: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> i64 {
    row.get(key)
        .and_then(|value| match value {
            serde_json::Value::Number(number) => number.as_i64(),
            serde_json::Value::String(value) => value.parse::<i64>().ok().or_else(|| {
                DateTime::parse_from_rfc3339(value)
                    .ok()
                    .map(|ts| ts.timestamp_millis())
            }),
            _ => None,
        })
        .unwrap_or_default()
}

pub(crate) fn native_json_object(
    row: &serde_json::Value,
) -> &serde_json::Map<String, serde_json::Value> {
    source_json_object(row)
}

#[derive(Clone, Debug)]
pub(crate) struct StoredModel {
    pub(crate) model_id: String,
    pub(crate) provider: String,
    pub(crate) model_name: String,
    pub(crate) version: String,
    pub(crate) dimensions: i32,
    pub(crate) matryoshka_dims_json: String,
    pub(crate) distance_metric: String,
    pub(crate) normalize: bool,
    pub(crate) output_dtype: String,
    pub(crate) rescore: bool,
    pub(crate) max_input_tokens: i32,
    pub(crate) tokenizer: String,
    pub(crate) task_type: String,
    pub(crate) asymmetric: bool,
    pub(crate) provider_endpoint_ref: String,
    pub(crate) status: String,
    pub(crate) replacement_model_id: String,
    pub(crate) retire_after_unix_ms: i64,
    pub(crate) vector_backend: String,
    pub(crate) vector_instance: String,
    pub(crate) collection_alias: String,
    pub(crate) active_collection: String,
    pub(crate) chunking_strategy: String,
    pub(crate) chunk_tokens: i32,
    pub(crate) chunk_overlap_tokens: i32,
    pub(crate) contextual_retrieval: bool,
    pub(crate) late_chunking: bool,
    pub(crate) tenant_state: String,
    pub(crate) metadata_json: String,
}

impl StoredModel {
    pub(crate) fn collection(&self) -> &str {
        if self.collection_alias.trim().is_empty() {
            self.active_collection.as_str()
        } else {
            self.collection_alias.as_str()
        }
    }

    /// The dimensionality this model actually serves at (Part B.2.3 Matryoshka).
    /// Equals `dimensions` unless `matryoshka_dims` selects a smaller cut — see
    /// [`super::config::select_matryoshka_dim`]. Used for the collection geometry,
    /// the sidecar `truncate_dim` hint, the stored/fresh vector truncation, and
    /// the Retrieve query-vector cut so all four agree.
    pub(crate) fn serving_dim(&self) -> i32 {
        super::config::select_matryoshka_dim(
            self.dimensions,
            &self.matryoshka_dims_json,
            super::config::embedding_matryoshka_strategy(),
        )
    }
}

/// Truncate an embedding to its first `dim` components (Matryoshka prefix). A
/// non-positive `dim`, or a vector already at/under `dim`, is returned unchanged,
/// so a non-Matryoshka model (whose `serving_dim == dimensions`) is a no-op.
/// Cosine similarity is magnitude-invariant and the stored vectors are truncated
/// the same way, so no re-normalization is required. Pure — unit-tested.
pub(crate) fn truncate_embedding_vector(vector: Vec<f32>, dim: i32) -> Vec<f32> {
    if dim <= 0 {
        return vector;
    }
    let dim = dim as usize;
    if vector.len() <= dim {
        vector
    } else {
        vector[..dim].to_vec()
    }
}

pub(crate) fn stored_model_from_json(row: &serde_json::Value) -> StoredModel {
    let map = source_json_object(row);
    StoredModel {
        model_id: json_str(map, "model_id"),
        provider: json_str(map, "provider"),
        model_name: json_str(map, "model_name"),
        version: json_str(map, "version"),
        dimensions: json_i32(map, "dimensions"),
        matryoshka_dims_json: json_str(map, "matryoshka_dims_json"),
        distance_metric: json_str(map, "distance_metric"),
        normalize: json_bool(map, "normalize"),
        output_dtype: json_str(map, "output_dtype"),
        rescore: json_bool(map, "rescore"),
        max_input_tokens: json_i32(map, "max_input_tokens"),
        tokenizer: json_str(map, "tokenizer"),
        task_type: json_str(map, "task_type"),
        asymmetric: json_bool(map, "asymmetric"),
        provider_endpoint_ref: json_str(map, "provider_endpoint_ref"),
        status: json_str(map, "status"),
        replacement_model_id: json_str(map, "replacement_model_id"),
        retire_after_unix_ms: json_timestamp_ms(map, "retire_after"),
        vector_backend: json_str(map, "vector_backend"),
        vector_instance: json_str(map, "vector_instance"),
        collection_alias: json_str(map, "collection_alias"),
        active_collection: json_str(map, "active_collection"),
        chunking_strategy: json_str(map, "chunking_strategy"),
        chunk_tokens: json_i32(map, "chunk_tokens"),
        chunk_overlap_tokens: json_i32(map, "chunk_overlap_tokens"),
        contextual_retrieval: json_bool(map, "contextual_retrieval"),
        late_chunking: json_bool(map, "late_chunking"),
        tenant_state: json_str(map, "tenant_state"),
        metadata_json: json_str(map, "metadata_json"),
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
        vector_name: String::new(),
    })
}

pub(crate) struct EmbeddingPointMetadata<'a> {
    pub(crate) chunk_hash: &'a str,
    pub(crate) chunk_text: &'a str,
    pub(crate) document_id: &'a str,
    pub(crate) doc_version: &'a str,
    pub(crate) model_id: &'a str,
    pub(crate) vector_name: &'a str,
}

pub(crate) fn build_embedding_point_with_metadata(
    row_pk: &str,
    vector: Vec<f32>,
    tenant_id: &str,
    source_name: &str,
    metadata: EmbeddingPointMetadata<'_>,
) -> Result<VectorPointMutation, Status> {
    let mut point = build_embedding_point(row_pk, vector, tenant_id, source_name)?;
    let Some(payload) = point.payload.as_mut() else {
        return Err(embedding_policy_status_with_code(
            tonic::Code::Internal,
            "embedding_vector_upsert",
            "embedding_payload_required",
            "embedding point lost its mandatory tenant payload",
        ));
    };
    let fields = &mut payload.fields;
    let string_value = |value: &str| prost_types::Value {
        kind: Some(prost_types::value::Kind::StringValue(value.to_string())),
    };
    if !metadata.chunk_hash.trim().is_empty() {
        fields.insert("_chunk_hash".to_string(), string_value(metadata.chunk_hash));
    }
    if !metadata.chunk_text.trim().is_empty() {
        fields.insert("_chunk_text".to_string(), string_value(metadata.chunk_text));
    }
    if !metadata.document_id.trim().is_empty() {
        fields.insert(
            "_document_id".to_string(),
            string_value(metadata.document_id),
        );
    }
    if !metadata.doc_version.trim().is_empty() {
        fields.insert(
            "_doc_version".to_string(),
            string_value(metadata.doc_version),
        );
    }
    fields.insert("_model_id".to_string(), string_value(metadata.model_id));
    fields.insert(
        "_indexed_at_unix_ms".to_string(),
        prost_types::Value {
            kind: Some(prost_types::value::Kind::NumberValue(
                chrono::Utc::now().timestamp_millis() as f64,
            )),
        },
    );
    point.vector_name = metadata.vector_name.trim().to_string();
    Ok(point)
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

pub(crate) fn merge_retrieve_scope_filter(
    tenant_id: &str,
    source_name: &str,
    filter_json: &str,
    parent_pk: Option<&str>,
) -> Result<serde_json::Value, Status> {
    let mut filter = merge_retrieve_filter(tenant_id, filter_json)?;
    let must = filter
        .as_object_mut()
        .and_then(|object| object.get_mut("must"))
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| {
            embedding_policy_status_with_code(
                tonic::Code::Internal,
                "embedding_retrieve_filter",
                "mandatory_tenant_filter_missing",
                "embedding retrieve lost its mandatory tenant filter",
            )
        })?;
    must.push(serde_json::json!({
        "key": "_source", "match": { "value": source_name }
    }));
    if let Some(parent_pk) = parent_pk.filter(|value| !value.trim().is_empty()) {
        must.push(serde_json::json!({
            "key": "_parent_pk", "match": { "value": parent_pk }
        }));
    }
    Ok(filter)
}

/// Build the tenant+source scope filter for the `parent_window` neighbor gather,
/// matching ANY of the selected `parent_pks` in a SINGLE clause (Qdrant
/// `match: { any: [...] }`). This lets the retriever fetch every neighbor chunk
/// for all selected parents in ONE query instead of one ANN search per parent.
/// Empty parents ⇒ `None` (nothing to gather). Pure — unit-tested.
pub(crate) fn merge_retrieve_parent_window_filter(
    tenant_id: &str,
    source_name: &str,
    parent_pks: &[String],
) -> Result<Option<serde_json::Value>, Status> {
    let parents: Vec<&str> = parent_pks
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .collect();
    if parents.is_empty() {
        return Ok(None);
    }
    let mut filter = merge_retrieve_filter(tenant_id, "")?;
    let must = filter
        .as_object_mut()
        .and_then(|object| object.get_mut("must"))
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| {
            embedding_policy_status_with_code(
                tonic::Code::Internal,
                "embedding_retrieve_filter",
                "mandatory_tenant_filter_missing",
                "embedding parent-window retrieve lost its mandatory tenant filter",
            )
        })?;
    must.push(serde_json::json!({
        "key": "_source", "match": { "value": source_name }
    }));
    must.push(serde_json::json!({
        "key": "_parent_pk", "match": { "any": parents }
    }));
    Ok(Some(filter))
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
