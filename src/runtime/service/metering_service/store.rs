//! Durable-store access for `MeteringService`: the usage-event model + windowed
//! aggregate SQL, JSON row decoding, and the tenant-scoped quota IR builders (via
//! the neutral dispatch, never raw SQL for quota rows).

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalValue,
};
use crate::proto::udb::core::metering::services::v1 as metering_pb;
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::config::{QUOTA_RULE_MSG, USAGE_EVENT_MSG};

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

pub(crate) fn usage_event_model() -> NativeModel {
    native_model(
        USAGE_EVENT_MSG,
        &[
            "tenant_id",
            "method",
            "unit",
            "quantity",
            "occurred_at_unix",
        ],
    )
}

pub(crate) fn install_metering_tenant_scope_sql() -> &'static str {
    "SELECT set_config('app.current_tenant_id', $1, true)"
}

pub(crate) fn windowed_usage_sum_sql() -> &'static str {
    "SELECT COALESCE(SUM(quantity), 0)::bigint \
     FROM udb_metering.usage_events \
     WHERE tenant_id = $1 AND method = $2 AND occurred_at_unix >= $3"
}

// ── JSON row decoders (mirror lock_service/config_service) ─────────────────────

fn row_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
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
        Some(serde_json::Value::String(value)) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "t"
        ),
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0) != 0,
        _ => false,
    }
}

/// A durable quota rule decoded from a native read.
pub(crate) struct StoredQuota {
    pub(crate) quota_id: String,
    pub(crate) limit_value: i64,
    pub(crate) window_seconds: i64,
    pub(crate) enabled: bool,
    pub(crate) revision: i64,
}

pub(crate) fn stored_quota_from_json(row: &serde_json::Value) -> StoredQuota {
    let map = row_object(row);
    StoredQuota {
        quota_id: json_str(map, "quota_id"),
        limit_value: json_i64(map, "limit_value"),
        window_seconds: json_i64(map, "window_seconds"),
        enabled: json_bool(map, "enabled"),
        revision: json_i64(map, "revision"),
    }
}

pub(crate) fn quota_state_from_json(
    row: &serde_json::Value,
    tenant_id: &str,
) -> metering_pb::QuotaState {
    let map = row_object(row);
    metering_pb::QuotaState {
        tenant_id: {
            let stored = json_str(map, "tenant_id");
            if stored.is_empty() {
                tenant_id.to_string()
            } else {
                stored
            }
        },
        project_id: json_str(map, "project_id"),
        metric: json_str(map, "metric"),
        limit_value: json_i64(map, "limit_value"),
        window_seconds: json_i64(map, "window_seconds"),
        enabled: json_bool(map, "enabled"),
        revision: json_i64(map, "revision"),
        metadata_json: {
            let m = json_str(map, "metadata_json");
            if m.is_empty() { "{}".to_string() } else { m }
        },
    }
}

// ── IR builders (tenant-scoped reads/writes via the neutral dispatch) ──────────

fn quota_filter(tenant_id: &str, project_id: &str, metric: Option<&str>) -> LogicalFilter {
    let mut filters = vec![
        LogicalFilter::Comparison {
            field: "tenant_id".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(tenant_id),
        },
        LogicalFilter::Comparison {
            field: "project_id".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(project_id),
        },
    ];
    if let Some(metric) = metric {
        filters.push(LogicalFilter::Comparison {
            field: "metric".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(metric),
        });
    }
    LogicalFilter::And(filters)
}

pub(crate) fn quota_read_exact(tenant_id: &str, project_id: &str, metric: &str) -> LogicalRead {
    LogicalRead {
        message_type: QUOTA_RULE_MSG.to_string(),
        filter: Some(quota_filter(tenant_id, project_id, Some(metric))),
        projection: Some(LogicalProjection::fields([
            "quota_id".to_string(),
            "tenant_id".to_string(),
            "project_id".to_string(),
            "metric".to_string(),
            "limit_value".to_string(),
            "window_seconds".to_string(),
            "enabled".to_string(),
            "revision".to_string(),
            "metadata_json".to_string(),
        ])),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

pub(crate) fn quota_list_read(
    tenant_id: &str,
    project_id: &str,
    offset: u64,
    limit: u32,
) -> LogicalRead {
    // project_id empty narrows to tenant-wide rules of that exact (empty) scope;
    // a non-empty project narrows to that project. Either way RLS scopes to tenant.
    LogicalRead {
        message_type: QUOTA_RULE_MSG.to_string(),
        filter: Some(quota_filter(tenant_id, project_id, None)),
        projection: Some(LogicalProjection::fields([
            "tenant_id".to_string(),
            "project_id".to_string(),
            "metric".to_string(),
            "limit_value".to_string(),
            "window_seconds".to_string(),
            "enabled".to_string(),
            "revision".to_string(),
            "metadata_json".to_string(),
        ])),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn quota_record(
    quota_id: &str,
    tenant_id: &str,
    project_id: &str,
    metric: &str,
    limit_value: i64,
    window_seconds: i64,
    enabled: bool,
    revision: i64,
    metadata_json: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("quota_id".to_string(), logical_string(quota_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("project_id".to_string(), logical_string(project_id));
    record.insert("metric".to_string(), logical_string(metric));
    record.insert("limit_value".to_string(), LogicalValue::Int(limit_value));
    record.insert(
        "window_seconds".to_string(),
        LogicalValue::Int(window_seconds),
    );
    record.insert("enabled".to_string(), LogicalValue::Bool(enabled));
    record.insert("revision".to_string(), LogicalValue::Int(revision));
    record.insert("metadata_json".to_string(), logical_string(metadata_json));
    record
}

pub(crate) fn quota_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "limit_value".to_string(),
        "window_seconds".to_string(),
        "enabled".to_string(),
        "revision".to_string(),
        "metadata_json".to_string(),
    ])
}
