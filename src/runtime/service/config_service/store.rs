//! Durable-store access for `ConfigService`: tenant-scoped IR reads/writes via the
//! neutral compiler (never raw SQL) and JSON row decoding back to typed values.

use std::sync::OnceLock;

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalValue,
};
use crate::proto::udb::core::config::services::v1 as config_pb;

use super::codec::{flag_val_to_proto, stored_to_flag_val};
use super::config::{CONFIG_MSG, MAX_FLAGS_PER_KEY_SCAN};
use super::eval::EvalFlag;

// ---------------------------------------------------------------------------
// IR builders (mirror lock_service): tenant-scoped reads/writes via the neutral
// compiler, never raw SQL.
// ---------------------------------------------------------------------------

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

fn eq(field: &str, value: &str) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: field.to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(value),
    }
}

pub(crate) fn flag_filter(
    tenant_id: &str,
    project_id: Option<&str>,
    environment: Option<&str>,
    flag_key: Option<&str>,
) -> LogicalFilter {
    let mut filters = vec![eq("tenant_id", tenant_id)];
    if let Some(project_id) = project_id {
        filters.push(eq("project_id", project_id));
    }
    if let Some(environment) = environment {
        filters.push(eq("environment", environment));
    }
    if let Some(flag_key) = flag_key {
        filters.push(eq("flag_key", flag_key));
    }
    LogicalFilter::And(filters)
}

fn flag_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "flag_id".to_string(),
        "flag_key".to_string(),
        "project_id".to_string(),
        "environment".to_string(),
        "value_type".to_string(),
        "value_json".to_string(),
        "enabled".to_string(),
        "rollout_percentage".to_string(),
        "rollout_context_key".to_string(),
        "revision".to_string(),
        "metadata_json".to_string(),
    ])
}

pub(crate) fn flag_read_exact(
    tenant_id: &str,
    project_id: &str,
    environment: &str,
    flag_key: &str,
) -> LogicalRead {
    LogicalRead {
        message_type: CONFIG_MSG.to_string(),
        filter: Some(flag_filter(
            tenant_id,
            Some(project_id),
            Some(environment),
            Some(flag_key),
        )),
        projection: Some(flag_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

/// ONE tenant-scoped candidate read for ALL evaluated keys (the batched
/// `EvaluateFlags` path — replaces the previous read-per-key loop):
/// `tenant_id = ? AND flag_key IN (keys)`. The row cap is
/// `keys.len() × MAX_FLAGS_PER_KEY_SCAN`: each key still has at most
/// [`MAX_FLAGS_PER_KEY_SCAN`] scope rows (env/project/tenant arms, one row per
/// scope under the unique scope index), so the batched read carries the same
/// overall bound the per-key loop enforced — collapsed into a single mediated
/// read. Combined with the evaluate-key cap the absolute ceiling is
/// 256 × 64 = 16 384 rows.
pub(crate) fn flag_candidates_batch_read(tenant_id: &str, flag_keys: &[String]) -> LogicalRead {
    LogicalRead {
        message_type: CONFIG_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            eq("tenant_id", tenant_id),
            LogicalFilter::InList {
                field: "flag_key".to_string(),
                values: flag_keys
                    .iter()
                    .map(|key| logical_string(key.as_str()))
                    .collect(),
            },
        ])),
        projection: Some(flag_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(
            (flag_keys.len() as u32).saturating_mul(MAX_FLAGS_PER_KEY_SCAN),
        )),
    }
}

pub(crate) fn flag_list_read(
    tenant_id: &str,
    project_id: Option<&str>,
    environment: Option<&str>,
    offset: u64,
    limit: u32,
) -> LogicalRead {
    LogicalRead {
        message_type: CONFIG_MSG.to_string(),
        filter: Some(flag_filter(tenant_id, project_id, environment, None)),
        projection: Some(flag_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn flag_record(
    flag_id: &str,
    tenant_id: &str,
    project_id: &str,
    environment: &str,
    flag_key: &str,
    value_type: &str,
    value_json: &str,
    enabled: bool,
    rollout_percentage: i32,
    rollout_context_key: &str,
    revision: i64,
    metadata_json: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("flag_id".to_string(), logical_string(flag_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("project_id".to_string(), logical_string(project_id));
    record.insert("environment".to_string(), logical_string(environment));
    record.insert("flag_key".to_string(), logical_string(flag_key));
    record.insert("value_type".to_string(), logical_string(value_type));
    record.insert("value_json".to_string(), logical_string(value_json));
    record.insert("enabled".to_string(), LogicalValue::Bool(enabled));
    record.insert(
        "rollout_percentage".to_string(),
        LogicalValue::Int(i64::from(rollout_percentage)),
    );
    record.insert(
        "rollout_context_key".to_string(),
        logical_string(rollout_context_key),
    );
    record.insert("revision".to_string(), LogicalValue::Int(revision));
    record.insert("metadata_json".to_string(), logical_string(metadata_json));
    record
}

/// Mutable columns an upsert may overwrite for an existing (tenant, project, env,
/// key) row. The conflict target is the `flag_id` PK (reused for the existing
/// scope), so a re-put never violates the unique scope index.
pub(crate) fn flag_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "value_type".to_string(),
        "value_json".to_string(),
        "enabled".to_string(),
        "rollout_percentage".to_string(),
        "rollout_context_key".to_string(),
        "revision".to_string(),
        "metadata_json".to_string(),
    ])
}

// ---------------------------------------------------------------------------
// JSON row decoding.
// ---------------------------------------------------------------------------

fn flag_json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: OnceLock<serde_json::Map<String, serde_json::Value>> = OnceLock::new();
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

/// Read a JSONB column either as the text the driver returned, or as a re-encoded
/// nested JSON value (some drivers hand back the parsed value, not a string).
fn json_value_text(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(value) => value.to_string(),
        None => String::new(),
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
            value.eq_ignore_ascii_case("true") || value.trim() == "1"
        }
        Some(serde_json::Value::Number(value)) => value.as_i64().map(|v| v != 0).unwrap_or(false),
        _ => false,
    }
}

pub(crate) fn eval_flag_from_json(row: &serde_json::Value) -> EvalFlag {
    let map = flag_json_object(row);
    let value_type = json_str(map, "value_type");
    let value_json = json_value_text(map, "value_json");
    EvalFlag {
        flag_id: json_str(map, "flag_id"),
        flag_key: json_str(map, "flag_key"),
        project_id: json_str(map, "project_id"),
        environment: json_str(map, "environment"),
        value: stored_to_flag_val(&value_type, &value_json),
        enabled: json_bool(map, "enabled"),
        rollout_percentage: json_i64(map, "rollout_percentage") as i32,
        rollout_context_key: json_str(map, "rollout_context_key"),
        revision: json_i64(map, "revision"),
    }
}

pub(crate) fn flag_state_from_json(
    row: &serde_json::Value,
    tenant_id: &str,
) -> config_pb::FlagState {
    let ef = eval_flag_from_json(row);
    let map = flag_json_object(row);
    config_pb::FlagState {
        tenant_id: tenant_id.to_string(),
        project_id: ef.project_id.clone(),
        environment: ef.environment.clone(),
        flag_key: ef.flag_key.clone(),
        value: Some(flag_val_to_proto(&ef.value)),
        enabled: ef.enabled,
        rollout_percentage: ef.rollout_percentage,
        rollout_context_key: ef.rollout_context_key.clone(),
        revision: ef.revision,
        metadata_json: json_value_text(map, "metadata_json"),
    }
}
