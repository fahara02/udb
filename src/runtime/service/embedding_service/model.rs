//! The `StoredSource` DTO, its effective-collection resolver, the JSON decoders
//! for the native read path, and the tenant-tagged embedding-point builder for
//! the native `EmbeddingService`. Extracted verbatim from the former god file —
//! the mediated-read row decode and the fail-closed point builder are
//! byte-for-byte identical.

use tonic::Status;

use crate::proto::VectorPointMutation;

use super::config::DEFAULT_VECTOR_COLLECTION;
use super::errors::{embedding_policy_status_with_code, embedding_required_field};

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
    let payload = crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
        "_tenant_id": tenant,
    }));
    Ok(VectorPointMutation {
        id: row_pk.to_string(),
        vector,
        payload,
    })
}
