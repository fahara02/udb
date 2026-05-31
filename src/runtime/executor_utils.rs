#![allow(clippy::result_large_err)]

//! Pure utility helpers shared across executor modules and the core runtime.
//! Nothing in this module depends on `DataBrokerRuntime` directly.

use std::env;
#[cfg(feature = "s3")]
use std::time::{SystemTime, UNIX_EPOCH};

use prost_types::{ListValue, Struct, Value as ProstValue, value::Kind};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::broker::RequestContext;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestMaterializedView, ManifestStore};
use crate::proto::{RecordSet, RequestContext as ProtoRequestContext, Row as ProtoRow};

// ── Struct / prost ────────────────────────────────────────────────────────────

pub(crate) fn struct_to_json(value: &Struct) -> JsonValue {
    JsonValue::Object(
        value
            .fields
            .iter()
            .map(|(key, value)| (key.clone(), prost_value_to_json(value)))
            .collect(),
    )
}

pub(crate) fn prost_value_to_json(value: &ProstValue) -> JsonValue {
    match &value.kind {
        Some(Kind::NullValue(_)) | None => JsonValue::Null,
        Some(Kind::NumberValue(value)) => JsonValue::from(*value),
        Some(Kind::StringValue(value)) => JsonValue::String(value.clone()),
        Some(Kind::BoolValue(value)) => JsonValue::Bool(*value),
        Some(Kind::StructValue(value)) => struct_to_json(value),
        Some(Kind::ListValue(value)) => JsonValue::Array(
            value
                .values
                .iter()
                .map(prost_value_to_json)
                .collect::<Vec<_>>(),
        ),
    }
}

pub(crate) fn json_to_struct(value: &JsonValue) -> Option<Struct> {
    let JsonValue::Object(map) = value else {
        return None;
    };
    Some(Struct {
        fields: map
            .iter()
            .filter_map(|(key, value)| json_to_prost_value(value).map(|v| (key.clone(), v)))
            .collect(),
    })
}

pub(crate) fn json_to_prost_value(value: &JsonValue) -> Option<ProstValue> {
    Some(ProstValue {
        kind: Some(match value {
            JsonValue::Null => Kind::NullValue(0),
            JsonValue::Bool(value) => Kind::BoolValue(*value),
            JsonValue::Number(value) => Kind::NumberValue(value.as_f64()?),
            JsonValue::String(value) => Kind::StringValue(value.clone()),
            JsonValue::Array(items) => Kind::ListValue(ListValue {
                values: items.iter().filter_map(json_to_prost_value).collect(),
            }),
            JsonValue::Object(_) => Kind::StructValue(json_to_struct(value)?),
        }),
    })
}

// ── RequestContext merging ────────────────────────────────────────────────────

pub(crate) fn merge_context(
    proto_context: Option<&ProtoRequestContext>,
    metadata_context: RequestContext,
) -> RequestContext {
    let Some(proto) = proto_context else {
        return metadata_context;
    };
    RequestContext {
        tenant_id: first_non_empty(&proto.tenant_id, &metadata_context.tenant_id),
        user_id: first_non_empty(&proto.user_id, &metadata_context.user_id),
        correlation_id: first_non_empty(&proto.correlation_id, &metadata_context.correlation_id),
        purpose: first_non_empty(&proto.purpose, &metadata_context.purpose),
        project_id: metadata_context.project_id,
        consistency: metadata_context.consistency,
        client_catalog_version: metadata_context.client_catalog_version,
        target_backend: first_non_empty(&proto.target_backend, &metadata_context.target_backend),
        target_instance: first_non_empty(&proto.target_instance, &metadata_context.target_instance),
        routing_policy: first_non_empty(&proto.routing_policy, &metadata_context.routing_policy),
        primary_read: proto.primary_read || metadata_context.primary_read,
        max_replica_lag_ms: if proto.max_replica_lag_ms > 0 {
            proto.max_replica_lag_ms
        } else {
            metadata_context.max_replica_lag_ms
        },
        eventual_consistency_allowed: proto.eventual_consistency_allowed
            || metadata_context.eventual_consistency_allowed,
        read_fence_json: first_non_empty(&proto.read_fence_json, &metadata_context.read_fence_json),
        scopes: if proto.scopes.is_empty() {
            metadata_context.scopes
        } else {
            proto.scopes.clone()
        },
    }
}

pub(crate) fn first_non_empty(left: &str, right: &str) -> String {
    if left.trim().is_empty() {
        right.to_string()
    } else {
        left.to_string()
    }
}

// ── RecordSet helpers ─────────────────────────────────────────────────────────

pub(crate) fn reject_plan(errors: &[String]) -> Result<(), tonic::Status> {
    if errors.is_empty() {
        Ok(())
    } else {
        Err(tonic::Status::invalid_argument(errors.join("; ")))
    }
}

pub(crate) fn cached_record_set(records_json: Vec<Vec<u8>>) -> RecordSet {
    // Deserialise the cached JSON blobs back into proto rows (best-effort).
    let rows: Vec<ProtoRow> = records_json
        .iter()
        .filter_map(|blob| {
            let value: JsonValue = serde_json::from_slice(blob).ok()?;
            let fields = value
                .as_object()?
                .iter()
                .filter_map(|(key, val)| json_to_prost_value(val).map(|v| (key.clone(), v)))
                .collect();
            Some(ProtoRow { fields })
        })
        .collect();
    RecordSet {
        total_count: rows.len() as i32,
        rows,
        records_json,
        ..RecordSet::default()
    }
}

// ── JSON helpers ──────────────────────────────────────────────────────────────

pub(crate) fn cache_key(
    kind: &str,
    message_type: &str,
    context: &RequestContext,
    manifest_checksum: &str,
    filter: &JsonValue,
    fields: &[String],
) -> String {
    let mut scopes = context.scopes.clone();
    scopes.sort();
    let mut fields = fields.to_vec();
    fields.sort();
    format!(
        "udb:{}:{}:{}:{}:{}:{}:{}:{}",
        kind,
        sanitize_cache_part(&context.tenant_id),
        sanitize_cache_part(&context.purpose),
        checksum_str(&scopes.join(",")),
        message_type,
        sanitize_cache_part(manifest_checksum),
        checksum_json(filter),
        checksum_str(&fields.join(","))
    )
}

pub(crate) fn cache_invalidation_pattern(kind: &str, message_type: &str) -> String {
    format!("udb:{}:*:*:*:{}:*", kind, message_type)
}

pub(crate) fn checksum_json(value: &JsonValue) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.to_string().as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

pub(crate) fn checksum_str(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

fn sanitize_cache_part(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub(crate) fn json_scalar_to_string(value: &JsonValue) -> String {
    match value {
        JsonValue::String(value) => value.clone(),
        JsonValue::Number(value) => value.to_string(),
        JsonValue::Bool(value) => value.to_string(),
        JsonValue::Null => String::new(),
        _ => value.to_string(),
    }
}

pub(crate) fn json_i64(value: &JsonValue) -> Result<i64, tonic::Status> {
    value
        .as_i64()
        .or_else(|| value.as_str()?.parse().ok())
        .ok_or_else(|| tonic::Status::invalid_argument(format!("expected integer, got {value}")))
}

pub(crate) fn json_f64(value: &JsonValue) -> Result<f64, tonic::Status> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse().ok())
        .ok_or_else(|| tonic::Status::invalid_argument(format!("expected number, got {value}")))
}

pub(crate) fn json_is_ciphertext(value: &JsonValue) -> bool {
    value.as_str().is_some_and(is_ciphertext)
}

pub(crate) fn is_ciphertext(value: &str) -> bool {
    value.starts_with("udb-aead:v")
}

// ── Time helpers ──────────────────────────────────────────────────────────────

#[cfg(feature = "s3")]
pub(crate) fn bounded_ttl(ttl_seconds: i32) -> u64 {
    if ttl_seconds <= 0 {
        900
    } else {
        (ttl_seconds as u64).min(3600)
    }
}

#[cfg(feature = "s3")]
pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

// ── Manifest helpers ──────────────────────────────────────────────────────────

pub(crate) fn is_encrypted_column(column: &ManifestColumn) -> bool {
    column.encrypted || column.security.is_encrypted
}

pub(crate) fn declared_materialized_view<'a>(
    manifest: &'a CatalogManifest,
    schema: &str,
    name: &str,
) -> Option<&'a ManifestMaterializedView> {
    manifest
        .tables
        .iter()
        .flat_map(|table| table.materialized_views.iter())
        .find(|view| view.schema == schema && view.name == name)
}

pub(crate) fn store_option(store: &ManifestStore, key: &str) -> String {
    store
        .options
        .iter()
        .find(|option| option.key == key)
        .map(|option| option.value.clone())
        .unwrap_or_default()
}

pub(crate) fn store_option_i32(store: &ManifestStore, key: &str) -> i32 {
    store_option(store, key).parse().unwrap_or_default()
}

// ── SQL / identifier helpers ──────────────────────────────────────────────────

pub(crate) fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Quote an identifier for use in SQL as a double-quoted identifier.
/// Delegates to the canonical implementation in `generation::sql::qi`.
pub(crate) fn qi_runtime(value: &str) -> String {
    crate::generation::sql::qi(value)
}

// ── Generic-dispatch JSON request helpers ───────────────────────────────────────
// Shared by the runtime God-impl and per-backend executors (e.g. Qdrant).

pub(crate) fn json_required_str<'a>(
    value: &'a JsonValue,
    key: &str,
) -> Result<&'a str, tonic::Status> {
    value
        .get(key)
        .and_then(JsonValue::as_str)
        .filter(|raw| !raw.trim().is_empty())
        .ok_or_else(|| tonic::Status::invalid_argument(format!("{key} is required")))
}

pub(crate) fn json_required_f32_vec(
    value: &JsonValue,
    key: &str,
) -> Result<Vec<f32>, tonic::Status> {
    let values = value
        .get(key)
        .and_then(JsonValue::as_array)
        .ok_or_else(|| tonic::Status::invalid_argument(format!("{key} must be an array")))?;
    if values.is_empty() {
        return Err(tonic::Status::invalid_argument(format!(
            "{key} must not be empty"
        )));
    }
    values
        .iter()
        .map(|value| {
            value.as_f64().map(|number| number as f32).ok_or_else(|| {
                tonic::Status::invalid_argument(format!("{key} must contain only numbers"))
            })
        })
        .collect()
}

pub(crate) fn json_i32(value: &JsonValue, key: &str) -> Option<i32> {
    value
        .get(key)
        .and_then(JsonValue::as_i64)
        .and_then(|number| i32::try_from(number).ok())
}

pub(crate) fn json_bool(value: &JsonValue, key: &str) -> Option<bool> {
    value.get(key).and_then(JsonValue::as_bool)
}

/// Decode inline object bytes from a generic-dispatch request body
/// (`data_base64`/`content_base64` or `data_text`/`content_text`).
#[cfg(any(feature = "s3", test))]
pub(crate) fn object_bytes_from_json(value: &JsonValue) -> Result<Vec<u8>, tonic::Status> {
    if let Some(base64_value) = value
        .get("data_base64")
        .or_else(|| value.get("content_base64"))
        .and_then(JsonValue::as_str)
    {
        use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
        return B64.decode(base64_value).map_err(|err| {
            tonic::Status::invalid_argument(format!("invalid object base64: {err}"))
        });
    }
    if let Some(text) = value
        .get("data_text")
        .or_else(|| value.get("content_text"))
        .and_then(JsonValue::as_str)
    {
        return Ok(text.as_bytes().to_vec());
    }
    Err(tonic::Status::invalid_argument(
        "object bytes are required as data_base64, content_base64, data_text, or content_text",
    ))
}

pub(crate) fn validate_identifier(value: &str, label: &str) -> Result<(), tonic::Status> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        || value.starts_with(|ch: char| ch.is_ascii_digit())
    {
        return Err(tonic::Status::invalid_argument(format!(
            "{label} '{value}' is not a valid SQL identifier"
        )));
    }
    Ok(())
}

// ── Environment helpers ───────────────────────────────────────────────────────

pub(crate) fn env_first(keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| env::var(key).ok().filter(|value| !value.trim().is_empty()))
}

pub(crate) fn env_identifier(key: &str, fallback: &str) -> String {
    env::var(key)
        .ok()
        .filter(|value| is_identifier(value))
        .unwrap_or_else(|| fallback.to_string())
}

pub(crate) fn is_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

pub(crate) fn env_u32(key: &str) -> Option<u32> {
    env::var(key).ok()?.parse().ok()
}

pub(crate) fn env_i32(key: &str) -> Option<i32> {
    env::var(key).ok()?.parse().ok()
}

// ── HTTP / Qdrant helpers ─────────────────────────────────────────────────────

#[cfg(feature = "qdrant")]
pub(crate) fn qdrant_status(status: reqwest::StatusCode) -> Result<(), tonic::Status> {
    if status.is_success() {
        Ok(())
    } else {
        Err(tonic::Status::unavailable(format!(
            "Qdrant returned HTTP {status}"
        )))
    }
}
