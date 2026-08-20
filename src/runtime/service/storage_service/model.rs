//! Enum<->db converters, the ETag/`is_public` normalizers, and the mediated-read
//! JSON → `File` decoders for the native `StorageService`. Extracted verbatim
//! from the former god file — the stored-token round-trip and the row decode are
//! byte-for-byte identical.

use tonic::Status;

use crate::proto::udb::core::storage::entity::v1 as storage_entity_pb;

use super::errors::storage_field_violation;

/// Normalize an S3/HTTP ETag for comparison: strip a weak-validator `W/` prefix
/// and the surrounding quotes S3 returns, then lowercase.
pub(crate) fn normalize_etag(etag: &str) -> String {
    etag.trim()
        .trim_start_matches("W/")
        .trim_matches('"')
        .to_ascii_lowercase()
}

// ── enum<->db (stored as VARCHAR via the proto_enum serializer) ───────────────

fn file_type_from_db(value: &str) -> i32 {
    use storage_entity_pb::FileType as T;
    match value {
        "IMAGE" | "FILE_TYPE_IMAGE" => T::Image as i32,
        "VIDEO" | "FILE_TYPE_VIDEO" => T::Video as i32,
        "AUDIO" | "FILE_TYPE_AUDIO" => T::Audio as i32,
        "PDF" | "FILE_TYPE_PDF" => T::Pdf as i32,
        "DOCUMENT" | "FILE_TYPE_DOCUMENT" => T::Document as i32,
        "ARCHIVE" | "FILE_TYPE_ARCHIVE" => T::Archive as i32,
        "OTHER" | "FILE_TYPE_OTHER" => T::Other as i32,
        _ => T::Unspecified as i32,
    }
}

fn file_status_from_db(value: &str) -> i32 {
    use storage_entity_pb::FileStatus as S;
    match value {
        "PENDING" | "FILE_STATUS_PENDING" => S::Pending as i32,
        "ACTIVE" | "FILE_STATUS_ACTIVE" => S::Active as i32,
        "DELETED" | "FILE_STATUS_DELETED" => S::Deleted as i32,
        _ => S::Unspecified as i32,
    }
}

/// DB token -> `ScanVerdict`. Accepts the short and fully-qualified spellings,
/// matching `file_status_from_db`. An unknown token decodes to UNSPECIFIED,
/// which the download gate treats as NOT scanned — never as clean.
pub(crate) fn scan_verdict_from_db(value: &str) -> i32 {
    use storage_entity_pb::ScanVerdict as V;
    match value.trim() {
        "PENDING" | "SCAN_VERDICT_PENDING" => V::Pending as i32,
        "CLEAN" | "SCAN_VERDICT_CLEAN" => V::Clean as i32,
        "INFECTED" | "SCAN_VERDICT_INFECTED" => V::Infected as i32,
        "FAILED" | "SCAN_VERDICT_FAILED" => V::Failed as i32,
        _ => V::Unspecified as i32,
    }
}

/// `ScanVerdict` -> the canonical DB token. Fully-qualified so the stored value
/// matches the column default the manifest declares.
pub(crate) fn scan_verdict_to_db(value: i32) -> &'static str {
    use storage_entity_pb::ScanVerdict as V;
    match V::try_from(value) {
        Ok(V::Pending) => "SCAN_VERDICT_PENDING",
        Ok(V::Clean) => "SCAN_VERDICT_CLEAN",
        Ok(V::Infected) => "SCAN_VERDICT_INFECTED",
        Ok(V::Failed) => "SCAN_VERDICT_FAILED",
        _ => "SCAN_VERDICT_UNSPECIFIED",
    }
}

pub(crate) fn file_type_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "IMAGE" | "FILE_TYPE_IMAGE" => "IMAGE",
        "VIDEO" | "FILE_TYPE_VIDEO" => "VIDEO",
        "AUDIO" | "FILE_TYPE_AUDIO" => "AUDIO",
        "PDF" | "FILE_TYPE_PDF" => "PDF",
        "DOCUMENT" | "FILE_TYPE_DOCUMENT" => "DOCUMENT",
        "ARCHIVE" | "FILE_TYPE_ARCHIVE" => "ARCHIVE",
        "OTHER" | "FILE_TYPE_OTHER" => "OTHER",
        other => {
            return Err(storage_field_violation(
                "file_type",
                "must be a supported FileType enum value",
                format!("unknown file type: {other}"),
            ));
        }
    };
    Ok(short.to_string())
}

/// Normalize a file-status string to the canonical SHORT stored token. Same
/// accept-both-forms / reject-unknown / empty→default contract as
/// [`file_type_to_db`].
#[allow(dead_code)]
pub(crate) fn file_status_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "PENDING" | "FILE_STATUS_PENDING" => "PENDING",
        "ACTIVE" | "FILE_STATUS_ACTIVE" => "ACTIVE",
        "DELETED" | "FILE_STATUS_DELETED" => "DELETED",
        other => {
            return Err(storage_field_violation(
                "status",
                "must be a supported FileStatus enum value",
                format!("unknown file status: {other}"),
            ));
        }
    };
    Ok(short.to_string())
}

// ── is_public presence handling (proto3 optional) ─────────────────────────────

/// Bind value for `is_public` on the register INSERT. The column is NOT NULL,
/// so an absent proto3-optional field defaults to private (`false`) — never
/// binds SQL NULL.
pub(crate) fn register_is_public_bind(requested: Option<bool>) -> bool {
    requested.unwrap_or(false)
}

/// Map the `File.file_type` enum back to its canonical stored token ("" =
/// unspecified → SQL NULL). Inverse of [`file_type_from_db`].
pub(crate) fn file_type_to_short(value: i32) -> &'static str {
    use storage_entity_pb::FileType as T;
    match T::try_from(value).unwrap_or(T::Unspecified) {
        T::Image => "IMAGE",
        T::Video => "VIDEO",
        T::Audio => "AUDIO",
        T::Pdf => "PDF",
        T::Document => "DOCUMENT",
        T::Archive => "ARCHIVE",
        T::Other => "OTHER",
        T::Unspecified => "",
    }
}

/// Map the `File.status` enum back to its canonical stored token. Inverse of
/// [`file_status_from_db`].
pub(crate) fn file_status_to_short(value: i32) -> &'static str {
    use storage_entity_pb::FileStatus as S;
    match S::try_from(value).unwrap_or(S::Unspecified) {
        S::Pending => "PENDING",
        S::Active => "ACTIVE",
        S::Deleted => "DELETED",
        S::Unspecified => "",
    }
}

fn file_json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

fn json_string(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    row.get(key)
        .and_then(|value| match value {
            serde_json::Value::String(value) => Some(value.clone()),
            serde_json::Value::Number(value) => Some(value.to_string()),
            serde_json::Value::Bool(value) => Some(value.to_string()),
            serde_json::Value::Null => None,
            other => Some(other.to_string()),
        })
        .unwrap_or_default()
}

fn json_i64(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    row.get(key)
        .and_then(|value| match value {
            serde_json::Value::Number(value) => value.as_i64(),
            serde_json::Value::String(value) => value.trim().parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or(0)
}

fn json_bool(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> bool {
    row.get(key)
        .and_then(|value| match value {
            serde_json::Value::Bool(value) => Some(*value),
            serde_json::Value::Number(value) => value.as_i64().map(|n| n != 0),
            serde_json::Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
                "true" | "t" | "1" | "yes" => Some(true),
                "false" | "f" | "0" | "no" => Some(false),
                _ => None,
            },
            _ => None,
        })
        .unwrap_or(false)
}

pub(crate) fn file_from_json(row: &serde_json::Value) -> storage_entity_pb::File {
    let row = file_json_object(row);
    storage_entity_pb::File {
        file_id: json_string(row, "file_id"),
        tenant_id: json_string(row, "tenant_id"),
        project_id: json_string(row, "project_id"),
        filename: json_string(row, "filename"),
        content_type: json_string(row, "content_type"),
        size_bytes: json_i64(row, "size_bytes"),
        backend: json_string(row, "backend"),
        bucket: json_string(row, "bucket"),
        object_key: json_string(row, "object_key"),
        url: json_string(row, "url"),
        cdn_url: json_string(row, "cdn_url"),
        file_type: file_type_from_db(&json_string(row, "file_type")),
        reference_id: json_string(row, "reference_id"),
        reference_type: json_string(row, "reference_type"),
        is_public: json_bool(row, "is_public"),
        status: file_status_from_db(&json_string(row, "status")),
        checksum: json_string(row, "checksum"),
        uploaded_by: json_string(row, "uploaded_by"),
        deleted_by: json_string(row, "deleted_by"),
        scan_verdict: scan_verdict_from_db(&json_string(row, "scan_verdict")),
        scanned_by: json_string(row, "scanned_by"),
        scan_detail: json_string(row, "scan_detail"),
        ..Default::default()
    }
}
