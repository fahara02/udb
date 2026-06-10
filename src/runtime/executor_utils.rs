#![allow(clippy::result_large_err)]

//! Pure utility helpers shared across executor modules and the core runtime.
//! Nothing in this module depends on `DataBrokerRuntime` directly.

#[cfg(feature = "s3")]
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, future::Future, time::Duration};

use prost_types::{ListValue, Struct, Value as ProstValue, value::Kind};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::broker::RequestContext;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestMaterializedView, ManifestStore};
use crate::proto::{RecordSet, RequestContext as ProtoRequestContext, Row as ProtoRow};

// ── Object-store limits ──────────────────────────────────────────────────────

/// Generic inline object writes are capped; larger uploads should use
/// presigned/resumable upload flows rather than the generic RPC body.
pub(crate) const INLINE_OBJECT_LIMIT_BYTES: usize = 1_048_576;

/// Suggested client backoff (ms) advertised on transient backend failures
/// (HTTP 5xx / server errors) via [`retryable_status`]. SDKs read it from the
/// typed `ErrorDetail.retry_after_ms` to pace automatic retries.
pub(crate) const HTTP_RETRYABLE_BACKOFF_MS: i64 = 250;

pub(crate) fn executor_timeout_duration() -> Duration {
    let millis = env::var("UDB_EXECUTOR_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30_000)
        .max(1);
    Duration::from_millis(millis)
}

pub(crate) async fn with_executor_timeout<T, F>(
    backend: &str,
    operation: &str,
    future: F,
) -> Result<T, tonic::Status>
where
    F: Future<Output = Result<T, tonic::Status>>,
{
    tokio::time::timeout(executor_timeout_duration(), future)
        .await
        .map_err(|_| {
            tonic::Status::deadline_exceeded(format!(
                "{backend} generic {operation} exceeded UDB_EXECUTOR_TIMEOUT_MS"
            ))
        })?
}

pub(crate) fn reject_oversized_object(len: usize) -> Result<(), tonic::Status> {
    if len > INLINE_OBJECT_LIMIT_BYTES {
        Err(tonic::Status::resource_exhausted(
            "generic object writes are limited to 1MB; use presigned upload for larger files",
        ))
    } else {
        Ok(())
    }
}

// ── Typed error details (A.3) ────────────────────────────────────────────────
//
// SDKs should branch on a machine-readable `ErrorDetail` instead of parsing the
// human-readable status message. We prost-encode an `ErrorDetail` and attach it
// to the `tonic::Status` as a binary trailer metadata value under
// `udb-error-detail-bin` (the `-bin` suffix tells gRPC the value is raw bytes,
// base64-transcoded on the wire). The status message stays the same human text,
// so existing string-based behavior is unaffected — the typed detail is purely
// additive.

/// Binary trailer metadata key carrying the prost-encoded `ErrorDetail`.
/// The `-bin` suffix is required by gRPC for binary metadata values.
pub(crate) const ERROR_DETAIL_METADATA_KEY: &str = "udb-error-detail-bin";

/// Build a `tonic::Status` with the given code + human message and attach a
/// prost-encoded [`ErrorDetail`] under [`ERROR_DETAIL_METADATA_KEY`]. The
/// message is preserved verbatim so callers that still read the string keep
/// working; SDKs that understand the typed detail decode the metadata instead.
pub(crate) fn status_with_error_detail(
    code: tonic::Code,
    message: impl Into<String>,
    detail: crate::proto::ErrorDetail,
) -> tonic::Status {
    use prost::Message as _;
    let mut metadata = tonic::metadata::MetadataMap::new();
    let encoded = detail.encode_to_vec();
    let value = tonic::metadata::MetadataValue::from_bytes(&encoded);
    metadata.insert_bin(ERROR_DETAIL_METADATA_KEY, value);
    tonic::Status::with_metadata(code, message.into(), metadata)
}

/// Backend-capability refusal: the target backend does not support the
/// requested operation. `FailedPrecondition`, `kind = CAPABILITY`, not
/// retryable. The caller supplies the same human text it would otherwise pass
/// to `Status::failed_precondition` so messages are unchanged.
pub(crate) fn capability_status(
    backend: impl Into<String>,
    operation: impl Into<String>,
    capability_required: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: backend.into(),
        operation: operation.into(),
        capability_required: capability_required.into(),
        retryable: false,
        retry_after_ms: 0,
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Capability as i32,
    };
    status_with_error_detail(tonic::Code::FailedPrecondition, message, detail)
}

/// Transient-failure refusal the caller may retry. `Unavailable`,
/// `kind = RETRYABLE`, `retryable = true`, with an optional suggested backoff.
pub(crate) fn retryable_status(
    backend: impl Into<String>,
    operation: impl Into<String>,
    retry_after_ms: i64,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: backend.into(),
        operation: operation.into(),
        capability_required: String::new(),
        retryable: true,
        retry_after_ms,
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Retryable as i32,
    };
    status_with_error_detail(tonic::Code::Unavailable, message, detail)
}

/// Compiler refusal: map a `CompileError::code()` token onto a typed
/// `ErrorDetail` (`kind = SCHEMA`, code carried in `capability_required`).
/// `InvalidArgument` matches the existing compile-error mapping. Additive — the
/// human message is preserved.
///
/// Currently exercised only by unit tests: the IR compiler's `CompileError` is
/// not yet surfaced through a request handler (the data plane compiles IR deeper
/// in the stack, where its error is propagated directly), so there is no
/// production call site to wire into. Scoped to `#[cfg(test)]` so it is honestly
/// not-dead in the shipped binary; un-gate it when an IR-compile step lands in a
/// handler that must map `CompileError` -> `tonic::Status` (mirror of the
/// already-wired `retryable_status`).
#[cfg(test)]
pub(crate) fn compile_error_status(
    backend: impl Into<String>,
    operation: impl Into<String>,
    compile_code: &str,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: backend.into(),
        operation: operation.into(),
        capability_required: compile_code.to_string(),
        retryable: false,
        retry_after_ms: 0,
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Schema as i32,
    };
    status_with_error_detail(tonic::Code::InvalidArgument, message, detail)
}

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

// D.3: consuming (`_into_`) variants for callers that OWN the `JsonValue` and
// drop it afterwards. They MOVE keys/strings/arrays into the proto value instead
// of cloning every field — byte-for-byte equivalent output (proven by
// `conversion_tests`), fewer allocations (quantified by `hotpath_bench`'s
// move-vs-clone case).
//
// Wired into the qdrant response/upsert paths, which own each point JSON and
// discard it after building the proto point — so the payload Struct is MOVED out
// (`Value::take` + `json_into_struct`) instead of cloned. Keep the borrowing
// variants for inspect-only / reused sub-value paths (filters, the dual
// JSON+proto row build in `core::mod`).
pub(crate) fn json_into_struct(value: JsonValue) -> Option<Struct> {
    let JsonValue::Object(map) = value else {
        return None;
    };
    Some(Struct {
        fields: map
            .into_iter()
            .filter_map(|(key, value)| json_into_prost_value(value).map(|v| (key, v)))
            .collect(),
    })
}

pub(crate) fn json_into_prost_value(value: JsonValue) -> Option<ProstValue> {
    let kind = match value {
        JsonValue::Null => Kind::NullValue(0),
        JsonValue::Bool(value) => Kind::BoolValue(value),
        JsonValue::Number(value) => Kind::NumberValue(value.as_f64()?),
        JsonValue::String(value) => Kind::StringValue(value), // moved, not cloned
        JsonValue::Array(items) => Kind::ListValue(ListValue {
            values: items
                .into_iter()
                .filter_map(json_into_prost_value)
                .collect(),
        }),
        JsonValue::Object(map) => Kind::StructValue(Struct {
            fields: map
                .into_iter()
                .filter_map(|(key, value)| json_into_prost_value(value).map(|v| (key, v)))
                .collect(),
        }),
    };
    Some(ProstValue { kind: Some(kind) })
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
        // SECURITY: tenant_id is the authenticated isolation boundary — take it
        // from the request metadata context, NOT a caller-supplied per-op proto
        // context (which could target a foreign tenant). Mirrors project_id below.
        tenant_id: metadata_context.tenant_id,
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
        // The broker stamps these from the verified SecurityContext / decision;
        // the wire `RequestContext` cannot assert them, so the metadata side wins.
        service_identity: metadata_context.service_identity,
        decision_id: metadata_context.decision_id,
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

// ── RecordBatchV2 (A.4) ────────────────────────────────────────────────────────

fn proto_row_to_json(row: &ProtoRow) -> JsonValue {
    JsonValue::Object(
        row.fields
            .iter()
            .map(|(key, value)| (key.clone(), prost_value_to_json(value)))
            .collect(),
    )
}

fn decode_base64_cell(value: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    let raw = value.strip_prefix("base64:")?;
    base64::engine::general_purpose::STANDARD.decode(raw).ok()
}

/// Infer a column's [`ColumnType`] and pack the per-row values into the matching
/// typed array, recording NULLs in the bitmap. Bytes columns are recognised via
/// the `base64:` cell prefix SQL executors emit (see [`base64_cell`]); columns
/// whose values mix scalar kinds — or contain arrays/objects — fall back to a
/// JSON-encoded string column so nothing is lost.
fn build_column_batch(name: &str, rows: &[JsonValue]) -> crate::proto::ColumnBatch {
    use crate::proto::{ColumnBatch, ColumnType};

    let cells: Vec<Option<&JsonValue>> = rows
        .iter()
        .map(|row| row.as_object().and_then(|obj| obj.get(name)))
        .collect();

    let (mut saw_bool, mut saw_int, mut saw_float) = (false, false, false);
    let (mut saw_str_plain, mut saw_str_b64, mut saw_nested, mut any_value) =
        (false, false, false, false);
    for cell in &cells {
        match cell {
            None | Some(JsonValue::Null) => {}
            Some(JsonValue::Bool(_)) => {
                saw_bool = true;
                any_value = true;
            }
            Some(JsonValue::Number(n)) => {
                any_value = true;
                if n.is_i64() || n.is_u64() {
                    saw_int = true;
                } else {
                    saw_float = true;
                }
            }
            Some(JsonValue::String(s)) => {
                any_value = true;
                if s.starts_with("base64:") {
                    saw_str_b64 = true;
                } else {
                    saw_str_plain = true;
                }
            }
            Some(JsonValue::Array(_)) | Some(JsonValue::Object(_)) => {
                saw_nested = true;
                any_value = true;
            }
        }
    }

    let categories = [
        saw_bool,
        saw_int || saw_float,
        saw_str_plain || saw_str_b64,
        saw_nested,
    ]
    .iter()
    .filter(|present| **present)
    .count();

    let col_type = if !any_value {
        ColumnType::Null
    } else if categories > 1 {
        ColumnType::Json
    } else if saw_bool {
        ColumnType::Bool
    } else if saw_int && !saw_float {
        ColumnType::Int64
    } else if saw_int || saw_float {
        ColumnType::Double
    } else if saw_str_b64 && !saw_str_plain {
        ColumnType::Bytes
    } else if saw_str_plain || saw_str_b64 {
        ColumnType::String
    } else {
        ColumnType::Json
    };

    let mut column = ColumnBatch {
        name: name.to_string(),
        r#type: col_type as i32,
        nulls: Vec::with_capacity(cells.len()),
        ..ColumnBatch::default()
    };
    for cell in &cells {
        let is_null = matches!(cell, None | Some(JsonValue::Null));
        column.nulls.push(is_null);
        match col_type {
            ColumnType::Bool => column
                .bool_values
                .push(cell.and_then(|v| v.as_bool()).unwrap_or(false)),
            ColumnType::Int64 => column
                .int64_values
                .push(cell.and_then(|v| v.as_i64()).unwrap_or(0)),
            ColumnType::Double => column
                .double_values
                .push(cell.and_then(|v| v.as_f64()).unwrap_or(0.0)),
            ColumnType::String => column.string_values.push(match cell {
                Some(JsonValue::String(s)) => s.clone(),
                Some(other) if !other.is_null() => other.to_string(),
                _ => String::new(),
            }),
            ColumnType::Bytes => column.bytes_values.push(
                cell.and_then(|v| v.as_str())
                    .and_then(decode_base64_cell)
                    .unwrap_or_default(),
            ),
            ColumnType::Json => column.json_values.push(match cell {
                Some(v) if !v.is_null() => v.to_string(),
                _ => String::new(),
            }),
            ColumnType::Null | ColumnType::Unspecified => {}
        }
    }
    column
}

/// Build a columnar [`crate::proto::RecordBatchV2`] from JSON row objects. The
/// rows are the same masked/encrypted JSON the V1 `RecordSet` carries, so the
/// V2 encoding inherits the V1 serializer's PII masking by construction. Column
/// order is the first-seen key order across rows.
pub(crate) fn record_batch_v2_from_json_rows(
    rows: &[JsonValue],
    schema_version: &str,
    next_page_token: String,
    total_count: i32,
) -> crate::proto::RecordBatchV2 {
    let mut field_order: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            for key in obj.keys() {
                if seen.insert(key.clone()) {
                    field_order.push(key.clone());
                }
            }
        }
    }
    let columns = field_order
        .iter()
        .map(|name| build_column_batch(name, rows))
        .collect::<Vec<_>>();
    crate::proto::RecordBatchV2 {
        columns,
        row_count: rows.len() as i32,
        schema_version: schema_version.to_string(),
        field_order,
        next_page_token,
        total_count,
    }
}

/// Convert a V1 [`RecordSet`] into the additive [`crate::proto::RecordBatchV2`]
/// columnar encoding, preferring the serialized `records_json` blobs (the masked
/// canonical form) and falling back to the proto `rows`.
pub(crate) fn record_batch_v2_from_record_set(
    set: &RecordSet,
    schema_version: &str,
) -> crate::proto::RecordBatchV2 {
    let rows: Vec<JsonValue> = if !set.records_json.is_empty() {
        set.records_json
            .iter()
            .filter_map(|blob| serde_json::from_slice(blob).ok())
            .collect()
    } else {
        set.rows.iter().map(proto_row_to_json).collect()
    };
    record_batch_v2_from_json_rows(
        &rows,
        schema_version,
        set.next_page_token.clone(),
        set.total_count,
    )
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
pub(crate) fn object_bytes_from_json(value: &JsonValue) -> Result<Vec<u8>, tonic::Status> {
    if let Some(base64_value) = value
        .get("data_base64")
        .or_else(|| value.get("content_base64"))
        .and_then(JsonValue::as_str)
    {
        // D.5: through the accel layer (scalar today; SIMD swap-in point for D.6).
        return crate::runtime::accel::base64_decode(base64_value).map_err(|err| {
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

// ── Generic-dispatch SQL request helpers ────────────────────────────────────────
// Shared by the SQL/CQL generic-dispatch executors (mysql, sqlite, mssql,
// cassandra). The dispatch JSON shape is `{"sql":"…","params":[…]}` (the
// `parameters` key is accepted as an alias). Pulled out of the per-backend
// modules so the byte-identical parser lives in exactly one place.

/// Parse `{"sql":"…","params":[…]}` (or `"parameters"`) out of a generic
/// dispatch request. Returns the SQL string plus the positional parameter
/// array (defaulting to empty when absent).
pub(crate) fn parse_sql_dispatch(
    request_json: &str,
) -> Result<(String, Vec<JsonValue>), tonic::Status> {
    let value: JsonValue = serde_json::from_str(request_json)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid dispatch JSON: {e}")))?;
    let sql = value
        .get("sql")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| tonic::Status::invalid_argument("missing `sql` in dispatch request"))?
        .to_string();
    let params = value
        .get("params")
        .or_else(|| value.get("parameters"))
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    Ok((sql, params))
}

/// Encode a binary cell as the `base64:<STANDARD>` JSON string the SQL
/// executors (mysql / sqlite / mssql) emit for `BLOB`/`VARBINARY` columns.
#[cfg(any(feature = "mysql", feature = "sqlite", feature = "mssql"))]
pub(crate) fn base64_cell(bytes: &[u8]) -> JsonValue {
    // D.5: through the accel layer (scalar today; SIMD swap-in point for D.6).
    JsonValue::String(format!(
        "base64:{}",
        crate::runtime::accel::base64_encode(bytes)
    ))
}

/// Render a generic `sqlx::Row` into a JSON object. Each column becomes a
/// key; values are coerced by probing `i64 → f64 → bool → String → Vec<u8>`
/// (bytes → `base64:` string) and falling back to JSON null. Shared by the
/// mysql + sqlite executors, whose row converters were byte-identical save
/// for the concrete `Row` type.
#[cfg(any(feature = "mysql", feature = "sqlite"))]
pub(crate) fn sqlx_row_to_json<R>(row: &R) -> JsonValue
where
    R: sqlx::Row,
    usize: sqlx::ColumnIndex<R>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> f64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> bool: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Vec<u8>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
{
    // `Column` is needed for `col.name()`; `Row`'s methods (`columns`,
    // `try_get`) are callable directly on the `R: sqlx::Row`-bounded param
    // without importing the trait into scope.
    use sqlx::Column as _;
    let mut obj = serde_json::Map::new();
    for (i, col) in row.columns().iter().enumerate() {
        let name = col.name().to_string();
        // Try common types in order; fall back to NULL.
        let value: JsonValue = if let Ok(v) = row.try_get::<Option<i64>, _>(i) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<f64>, _>(i) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<bool>, _>(i) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<String>, _>(i) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(i) {
            v.map(|bytes| base64_cell(&bytes))
                .unwrap_or(JsonValue::Null)
        } else {
            JsonValue::Null
        };
        obj.insert(name, value);
    }
    JsonValue::Object(obj)
}

/// Bind a slice of JSON values as positional parameters onto an `sqlx`
/// query. Numbers go to i64/f64, strings/bool/null map directly, and
/// objects/arrays serialise to JSON-text so the driver stores them in a
/// JSON column. Shared by the mysql + sqlite executors.
#[cfg(any(feature = "mysql", feature = "sqlite"))]
pub(crate) fn bind_json_params<'q, DB>(
    mut q: sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>,
    params: &'q [JsonValue],
) -> sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>
where
    DB: sqlx::Database,
    for<'a> Option<i64>: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    for<'a> i64: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    for<'a> f64: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    for<'a> bool: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    for<'a> String: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
{
    for p in params {
        q = match p {
            JsonValue::Null => q.bind(Option::<i64>::None),
            JsonValue::Bool(b) => q.bind(*b),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    q.bind(i)
                } else if let Some(f) = n.as_f64() {
                    q.bind(f)
                } else {
                    q.bind(n.to_string())
                }
            }
            JsonValue::String(s) => q.bind(s.clone()),
            other => q.bind(other.to_string()),
        };
    }
    q
}

/// Apply a batch of context SET / INSERT statements (rendered by
/// `render_sql_session_settings`) on an open `sqlx` transaction. Shared by
/// the mysql + sqlite session-context application paths, which differed only
/// in the connection type and the error-message prefix.
#[cfg(any(feature = "mysql", feature = "sqlite"))]
pub(crate) async fn apply_context_statements<'c, DB>(
    tx: &mut sqlx::Transaction<'c, DB>,
    statements: &[String],
    error_prefix: &str,
) -> Result<(), tonic::Status>
where
    DB: sqlx::Database,
    for<'e> &'e mut <DB as sqlx::Database>::Connection: sqlx::Executor<'e, Database = DB>,
    for<'q> <DB as sqlx::Database>::Arguments<'q>: sqlx::IntoArguments<'q, DB>,
{
    for stmt in statements {
        sqlx::query(stmt)
            .execute(&mut **tx)
            .await
            .map_err(|err| tonic::Status::internal(format!("{error_prefix}: {err}")))?;
    }
    Ok(())
}

// ── Object-store dispatch helper ─────────────────────────────────────────────
// Shared by the GCS + Azure-Blob executors, whose `parse_dispatch` differed
// only in which field name is primary and the missing-field error label.

/// Parse an object-store dispatch request of the shape
/// `{"op":"…","<bucket>":"…","<key>":"…","content_type":"…"}`. The
/// `bucket_keys` / `object_keys` slices list the accepted field aliases in
/// priority order (e.g. `["bucket","container"]`). `bucket_label` names the
/// bucket field in the missing-field error. Returns
/// `(op, bucket, object, content_type)`.
#[cfg(any(feature = "gcs", feature = "azureblob"))]
pub(crate) fn parse_object_dispatch(
    req: &str,
    bucket_keys: &[&str],
    object_keys: &[&str],
    bucket_label: &str,
) -> Result<(String, String, String, Option<String>), tonic::Status> {
    let v: JsonValue = serde_json::from_str(req)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid dispatch JSON: {e}")))?;
    let op = v
        .get("op")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| tonic::Status::invalid_argument("missing `op`"))?
        .to_string();
    let bucket = bucket_keys
        .iter()
        .find_map(|key| v.get(*key).and_then(JsonValue::as_str))
        .ok_or_else(|| tonic::Status::invalid_argument(format!("missing `{bucket_label}`")))?
        .to_string();
    let object = object_keys
        .iter()
        .find_map(|key| v.get(*key).and_then(JsonValue::as_str))
        .unwrap_or("")
        .to_string();
    let content_type = v
        .get("content_type")
        .and_then(JsonValue::as_str)
        .map(str::to_string);
    Ok((op, bucket, object, content_type))
}

// ── REST dispatch + HTTP-status helpers ──────────────────────────────────────
// Shared by the REST-backed executors (pinecone, weaviate, elasticsearch).

/// Parse a REST dispatch request of the shape
/// `{"path":"…","method":"…","body":{…}}`. `method` defaults to `POST` and
/// `body` defaults to JSON null. Byte-identical across pinecone + weaviate +
/// elasticsearch.
#[cfg(any(feature = "pinecone", feature = "weaviate", feature = "elasticsearch"))]
pub(crate) fn parse_rest_dispatch(
    req: &str,
) -> Result<(reqwest::Method, String, JsonValue), tonic::Status> {
    let mut v: JsonValue = serde_json::from_str(req)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid dispatch JSON: {e}")))?;
    let path = v
        .get("path")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| tonic::Status::invalid_argument("missing `path`"))?
        .to_string();
    let method = v
        .get("method")
        .and_then(JsonValue::as_str)
        .unwrap_or("POST")
        .parse::<reqwest::Method>()
        .map_err(|e| tonic::Status::invalid_argument(format!("bad method: {e}")))?;
    // D.3: move the (possibly large, nested) body out via `Value::take` instead of
    // deep-cloning it. `path`/`method` are already extracted, so replacing `body`
    // with Null is harmless; the returned body is byte-for-byte the same value.
    let body = v
        .get_mut("body")
        .map(JsonValue::take)
        .unwrap_or(JsonValue::Null);
    Ok((method, path, body))
}

/// Map an unsuccessful HTTP status + (already-extracted) detail message to a
/// typed `tonic::Status`, tagging the message with `backend`. This is the
/// shared mapping for the REST backends (pinecone, weaviate, elasticsearch).
/// The caller supplies the human-readable `detail` (truncated body, or a parsed
/// `error.reason`) so each backend keeps control of how it summarises bodies.
#[cfg(any(feature = "pinecone", feature = "weaviate", feature = "elasticsearch"))]
pub(crate) fn http_status_to_tonic(
    status: reqwest::StatusCode,
    detail: &str,
    backend: &str,
) -> tonic::Status {
    let code = status.as_u16();
    match code {
        400 | 422 => tonic::Status::invalid_argument(format!("{backend} {code}: {detail}")),
        401 | 403 => tonic::Status::permission_denied(format!("{backend} {code}: {detail}")),
        404 => tonic::Status::not_found(format!("{backend} 404: {detail}")),
        409 => tonic::Status::already_exists(format!("{backend} 409: {detail}")),
        429 => tonic::Status::resource_exhausted(format!("{backend} 429: {detail}")),
        // 5xx is a transient server-side failure: emit the typed retryable detail
        // (Unavailable, retryable=true, suggested backoff) so SDK clients retry.
        500..=599 => retryable_status(
            backend,
            "request",
            HTTP_RETRYABLE_BACKOFF_MS,
            format!("{backend} {code}: {detail}"),
        ),
        _ => tonic::Status::internal(format!("{backend} {code}: {detail}")),
    }
}

// ── Probe builder ─────────────────────────────────────────────────────────────

/// Build a `BackendProbe` from a backend name and a health-check result.
/// Every backend (except S3, which is special-cased to always report ok)
/// constructed its probe with this exact `match` — pulled into one place.
pub(crate) fn build_probe(
    name: &str,
    health: Result<(), String>,
) -> crate::runtime::executors::BackendProbe {
    let (ok, error) = match health {
        Ok(()) => (true, None),
        Err(err) => (false, Some(err)),
    };
    crate::runtime::executors::BackendProbe {
        backend: name.to_string(),
        instance: None,
        ok,
        error,
    }
}

// ── HTTP / Qdrant helpers ─────────────────────────────────────────────────────

#[cfg(feature = "qdrant")]
pub(crate) fn qdrant_status(status: reqwest::StatusCode) -> Result<(), tonic::Status> {
    if status.is_success() {
        Ok(())
    } else if status.is_server_error() {
        // 5xx from Qdrant is transient: typed retryable detail + backoff.
        Err(retryable_status(
            "qdrant",
            "request",
            HTTP_RETRYABLE_BACKOFF_MS,
            format!("Qdrant returned HTTP {status}"),
        ))
    } else {
        Err(tonic::Status::unavailable(format!(
            "Qdrant returned HTTP {status}"
        )))
    }
}

// ── Typed error detail tests (A.3) ────────────────────────────────────────────

#[cfg(test)]
mod conversion_tests {
    use super::{json_into_prost_value, json_into_struct, json_to_prost_value, json_to_struct};
    use serde_json::json;

    fn sample() -> serde_json::Value {
        json!({
            "id": "abc", "n": 42, "f": 3.5, "b": true, "nil": null,
            "tags": ["x", "y", 1, false, null],
            "nested": {"k": "v", "deep": {"arr": [1, 2.5, "z"], "flag": true}}
        })
    }

    #[test]
    fn json_into_struct_matches_borrowing_variant() {
        let v = sample();
        let borrowed = json_to_struct(&v).expect("object");
        let moved = json_into_struct(v).expect("object");
        assert_eq!(
            borrowed, moved,
            "consuming variant must yield an identical Struct"
        );
    }

    #[test]
    fn json_into_prost_value_matches_borrowing_variant() {
        let v = sample();
        let borrowed = json_to_prost_value(&v).expect("value");
        let moved = json_into_prost_value(v).expect("value");
        assert_eq!(borrowed, moved);
    }

    #[test]
    fn json_into_struct_rejects_non_object() {
        assert!(json_into_struct(json!([1, 2, 3])).is_none());
        assert!(json_into_struct(json!("scalar")).is_none());
    }
}

#[cfg(test)]
mod error_detail_tests {
    use super::{
        ERROR_DETAIL_METADATA_KEY, capability_status, compile_error_status, retryable_status,
    };
    use crate::proto::{ErrorDetail, ErrorKind};
    use prost::Message as _;

    /// Decode the prost `ErrorDetail` from the binary trailer the way an SDK
    /// would, so the test proves the typed detail is recoverable end-to-end.
    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        ErrorDetail::decode(raw.as_ref()).expect("trailer decodes as ErrorDetail")
    }

    #[test]
    fn capability_status_carries_typed_detail_and_preserves_message() {
        let status = capability_status(
            "cassandra",
            "ensure_resource",
            "supports_resource_lifecycle",
            "backend 'cassandra' does not support operation 'ensure_resource'",
        );
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        // Human-readable message is preserved verbatim (additive detail only).
        assert!(status.message().contains("does not support operation"));
        let detail = decode_detail(&status);
        assert_eq!(detail.backend, "cassandra");
        assert_eq!(detail.operation, "ensure_resource");
        assert_eq!(detail.capability_required, "supports_resource_lifecycle");
        assert!(!detail.retryable);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
    }

    #[test]
    fn retryable_status_is_unavailable_with_backoff() {
        let status = retryable_status("postgres", "query", 250, "temporarily unavailable");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        let detail = decode_detail(&status);
        assert!(detail.retryable);
        assert_eq!(detail.retry_after_ms, 250);
        assert_eq!(detail.kind, ErrorKind::Retryable as i32);
    }

    #[test]
    fn compile_error_status_carries_code_as_schema_kind() {
        let status = compile_error_status(
            "mssql",
            "mutate",
            "operation_not_supported",
            "compile failed",
        );
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let detail = decode_detail(&status);
        assert_eq!(detail.capability_required, "operation_not_supported");
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert!(!detail.retryable);
    }
}

// ── RecordBatchV2 conversion tests (A.4) ──────────────────────────────────────

#[cfg(test)]
mod record_batch_tests {
    use super::{record_batch_v2_from_json_rows, record_batch_v2_from_record_set};
    use crate::proto::{ColumnType, RecordBatchV2, RecordSet};
    use serde_json::json;

    fn col<'a>(batch: &'a RecordBatchV2, name: &str) -> &'a crate::proto::ColumnBatch {
        batch
            .columns
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("column {name} present"))
    }

    #[test]
    fn infers_typed_columns_with_nulls_bytes_and_json_fallback() {
        let rows = vec![
            json!({"id": 1, "name": "alice", "active": true, "score": 9.5,
                   "blob": "base64:aGk=", "tags": ["a","b"], "maybe": null}),
            json!({"id": 2, "name": "bob", "active": false, "score": 3.0,
                   "blob": "base64:Ynll", "tags": ["c"], "maybe": 7}),
        ];
        let batch = record_batch_v2_from_json_rows(&rows, "v3", "tok".into(), 2);

        assert_eq!(batch.row_count, 2);
        assert_eq!(batch.schema_version, "v3");
        assert_eq!(batch.next_page_token, "tok");
        assert_eq!(batch.total_count, 2);
        // field_order carries every key (order-independent: serde_json Map is sorted).
        let mut got = batch.field_order.clone();
        got.sort();
        assert_eq!(
            got,
            vec!["active", "blob", "id", "maybe", "name", "score", "tags"]
        );

        let id = col(&batch, "id");
        assert_eq!(id.r#type, ColumnType::Int64 as i32);
        assert_eq!(id.int64_values, vec![1, 2]);
        assert_eq!(id.nulls, vec![false, false]);

        assert_eq!(col(&batch, "name").r#type, ColumnType::String as i32);
        assert_eq!(col(&batch, "name").string_values, vec!["alice", "bob"]);

        assert_eq!(col(&batch, "active").r#type, ColumnType::Bool as i32);
        assert_eq!(col(&batch, "active").bool_values, vec![true, false]);

        assert_eq!(col(&batch, "score").r#type, ColumnType::Double as i32);
        assert_eq!(col(&batch, "score").double_values, vec![9.5, 3.0]);

        // bytes detected via the `base64:` cell prefix and decoded.
        let blob = col(&batch, "blob");
        assert_eq!(blob.r#type, ColumnType::Bytes as i32);
        assert_eq!(blob.bytes_values, vec![b"hi".to_vec(), b"bye".to_vec()]);

        // nested arrays fall back to a JSON-encoded string column.
        let tags = col(&batch, "tags");
        assert_eq!(tags.r#type, ColumnType::Json as i32);
        assert_eq!(tags.json_values[0], "[\"a\",\"b\"]");

        // a single scalar category (int) with a NULL → Int64 + null bitmap.
        let maybe = col(&batch, "maybe");
        assert_eq!(maybe.r#type, ColumnType::Int64 as i32);
        assert_eq!(maybe.nulls, vec![true, false]);
        assert_eq!(maybe.int64_values, vec![0, 7]);
    }

    #[test]
    fn all_null_column_is_null_typed() {
        let rows = vec![json!({"x": null}), json!({"x": null})];
        let batch = record_batch_v2_from_json_rows(&rows, "", String::new(), 0);
        let x = col(&batch, "x");
        assert_eq!(x.r#type, ColumnType::Null as i32);
        assert_eq!(x.nulls, vec![true, true]);
        assert!(x.int64_values.is_empty());
    }

    #[test]
    fn mixed_scalar_kinds_fall_back_to_json() {
        let rows = vec![json!({"v": true}), json!({"v": "hello"})];
        let batch = record_batch_v2_from_json_rows(&rows, "", String::new(), 0);
        let v = col(&batch, "v");
        assert_eq!(v.r#type, ColumnType::Json as i32);
        assert_eq!(v.json_values, vec!["true", "\"hello\""]);
    }

    #[test]
    fn from_record_set_uses_records_json() {
        let blob = serde_json::to_vec(&json!({"a": 1})).unwrap();
        let set = RecordSet {
            records_json: vec![blob],
            total_count: 1,
            next_page_token: "n".into(),
            ..RecordSet::default()
        };
        let batch = record_batch_v2_from_record_set(&set, "v1");
        assert_eq!(batch.row_count, 1);
        assert_eq!(batch.total_count, 1);
        assert_eq!(batch.next_page_token, "n");
        assert_eq!(col(&batch, "a").int64_values, vec![1]);
    }

    /// A.5 — compile-time proof that the selected bytes fields are generated as
    /// `bytes::Bytes` (not `Vec<u8>`). Fails to compile if the build.rs
    /// `.bytes([...])` config regresses.
    #[test]
    fn selected_proto_bytes_fields_are_bytes_type() {
        let chunk = crate::proto::Chunk::default();
        let _: bytes::Bytes = chunk.data;
        let row = crate::proto::ProjectionDriftDivergentRow::default();
        let _: bytes::Bytes = row.row_key_json;
    }

    /// A.7 — the V2 columnar frames must carry exactly the same logical values
    /// as the V1 `RecordSet`, and must NOT resurrect any field the V1 serializer
    /// masked out. Because the converter reads the already-masked `records_json`,
    /// a field masked away (absent from the blob) can never appear in V2.
    #[test]
    fn v2_batch_matches_v1_rows_and_inherits_masking() {
        // The V1 serializer masked "ssn" out, so it is absent from the blob.
        let blob = serde_json::to_vec(&json!({"id": 1, "email": "a@b.com"})).unwrap();
        let set = RecordSet {
            records_json: vec![blob],
            total_count: 1,
            ..RecordSet::default()
        };
        let batch = record_batch_v2_from_record_set(&set, "v9");

        let mut names: Vec<String> = batch.columns.iter().map(|c| c.name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["email", "id"], "V2 columns mirror the V1 row");
        assert!(
            !batch.field_order.contains(&"ssn".to_string()),
            "a field masked out of records_json must not leak into the V2 batch"
        );
        assert_eq!(col(&batch, "id").int64_values, vec![1]);
        assert_eq!(col(&batch, "email").string_values, vec!["a@b.com"]);
        assert_eq!(batch.schema_version, "v9");
    }
}
