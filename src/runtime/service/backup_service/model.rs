//! Pure helpers and JSON row decoders for the native `BackupService`: the
//! identifier-quoted relation name, the ciphertext checksum, and the mediated
//! read row (`serde_json::Value`) → `BackupRunSummary` / `BackupPolicyView`
//! mappings. Extracted verbatim from the former god file.

use chrono::DateTime;
use sha2::{Digest, Sha256};

use crate::proto::udb::core::backup::services::v1 as backup_pb;
use crate::runtime::executor_utils::qi_runtime;

/// `"schema"."table"`, quoted via the SAME runtime identifier-quoter the purge
/// path uses. This is identifier quoting, not tenant-column resolution — the
/// resolver stays single-sourced in `plan_tenant_purge`.
pub(crate) fn qualified_relation(schema: &str, table: &str) -> String {
    format!("{}.{}", qi_runtime(schema), qi_runtime(table))
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

// ── JSON row decode helpers (native read rows arrive as serde_json::Value) ─────

pub(crate) fn row_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
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

fn json_i64(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    match row.get(key) {
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0),
        Some(serde_json::Value::String(value)) => value.trim().parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

fn json_bool(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> bool {
    match row.get(key) {
        Some(serde_json::Value::Bool(value)) => *value,
        Some(serde_json::Value::String(value)) => {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "t"
            )
        }
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0) != 0,
        _ => false,
    }
}

/// Best-effort RFC3339/string → unix seconds (timestamps round-trip as strings).
fn json_unix_ts(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => DateTime::parse_from_rfc3339(value.trim())
            .map(|dt| dt.timestamp())
            .unwrap_or(0),
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0),
        _ => 0,
    }
}

pub(crate) fn run_summary_from_json(row: &serde_json::Value) -> backup_pb::BackupRunSummary {
    let m = row_object(row);
    backup_pb::BackupRunSummary {
        backup_id: json_str(m, "backup_id"),
        tenant_id: json_str(m, "tenant_id"),
        kind: json_str(m, "kind"),
        status: json_str(m, "status"),
        object_prefix: json_str(m, "object_prefix"),
        manifest_checksum: json_str(m, "manifest_checksum"),
        table_count: json_i64(m, "table_count") as i32,
        total_rows: json_i64(m, "total_rows"),
        excluded_count: json_i64(m, "excluded_count") as i32,
        source_tenant_id: json_str(m, "source_tenant_id"),
        target_tenant_id: json_str(m, "target_tenant_id"),
        created_at_unix: json_unix_ts(m, "created_at"),
        completed_at_unix: json_unix_ts(m, "completed_at"),
    }
}

pub(crate) fn policy_view_from_json(row: &serde_json::Value) -> backup_pb::BackupPolicyView {
    let m = row_object(row);
    backup_pb::BackupPolicyView {
        policy_id: json_str(m, "policy_id"),
        tenant_id: json_str(m, "tenant_id"),
        policy_name: json_str(m, "policy_name"),
        schedule_cron: json_str(m, "schedule_cron"),
        retention_days: json_i64(m, "retention_days") as i32,
        max_retained_backups: json_i64(m, "max_retained_backups") as i32,
        enabled: json_bool(m, "enabled"),
        object_backend: json_str(m, "object_backend"),
        object_bucket: json_str(m, "object_bucket"),
        created_at_unix: json_unix_ts(m, "created_at"),
        updated_at_unix: json_unix_ts(m, "updated_at"),
    }
}
