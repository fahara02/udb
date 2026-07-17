//! Manifest models + row/JSON decoders for the native `AnalyticsService`: the
//! three `NativeModel` definitions, the timestamp helper, the JSON accessors, and
//! the proto-entity decoders from both native-dispatch JSON rows and raw
//! Postgres rows. Extracted verbatim from the former god file.

use chrono::{DateTime, NaiveDate, Utc};
use sqlx::Row;

use crate::proto::udb::core::analytics::entity::v1 as ana_entity_pb;
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::config::{EPS_MSG, PMS_MSG, RAS_MSG};

pub(crate) fn pms_model() -> NativeModel {
    native_model(
        PMS_MSG,
        &[
            "snapshot_id",
            "snapshot_hour",
            "stage_name",
            "tenant_id",
            "total_requests",
            "successful",
            "failed",
            "p50_latency_ms",
            "p95_latency_ms",
            "p99_latency_ms",
            "avg_latency_ms",
            "error_rate",
            "throughput_rps",
            "recorded_at",
        ],
    )
}

pub(crate) fn eps_model() -> NativeModel {
    native_model(
        EPS_MSG,
        &[
            "summary_id",
            "summary_date",
            "executor_identity",
            "workload_kind",
            "total_dispatches",
            "successful_results",
            "timeout_count",
            "error_count",
            "avg_execution_ms",
            "p99_execution_ms",
            "avg_confidence",
            "success_rate",
            "avg_capacity_utilisation",
            "recorded_at",
        ],
    )
}

pub(crate) fn ras_model() -> NativeModel {
    native_model(
        RAS_MSG,
        &[
            "summary_id",
            "summary_date",
            "total_reconciliations",
            "exact_matches",
            "partial_conflicts",
            "hard_conflicts",
            "low_confidence_flagged",
            "avg_reconciliation_ms",
            "resolution_rate",
            "avg_record_confidence",
            "recorded_at",
        ],
    )
}

fn ts(seconds: i64) -> Option<prost_types::Timestamp> {
    if seconds <= 0 {
        None
    } else {
        Some(prost_types::Timestamp { seconds, nanos: 0 })
    }
}

pub(crate) fn timestamp_hour_period(ts: Option<&prost_types::Timestamp>) -> String {
    ts.and_then(|ts| DateTime::<Utc>::from_timestamp(ts.seconds, ts.nanos.max(0) as u32))
        .map(|dt| dt.format("%Y-%m-%dT%H:00:00Z").to_string())
        .unwrap_or_default()
}

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

fn json_string(row: &serde_json::Map<String, serde_json::Value>, field: &str) -> String {
    row.get(field)
        .and_then(|value| match value {
            serde_json::Value::String(value) => Some(value.clone()),
            serde_json::Value::Number(value) => Some(value.to_string()),
            serde_json::Value::Bool(value) => Some(value.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

fn json_i64(row: &serde_json::Map<String, serde_json::Value>, field: &str) -> i64 {
    row.get(field)
        .and_then(|value| match value {
            serde_json::Value::Number(value) => value.as_i64(),
            serde_json::Value::String(value) => value.parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or_default()
}

fn json_f64(row: &serde_json::Map<String, serde_json::Value>, field: &str) -> f64 {
    row.get(field)
        .and_then(|value| match value {
            serde_json::Value::Number(value) => value.as_f64(),
            serde_json::Value::String(value) => value.parse::<f64>().ok(),
            _ => None,
        })
        .unwrap_or_default()
}

fn json_ts(
    row: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Option<prost_types::Timestamp> {
    let value = row.get(field)?;
    if let Some(seconds) = value.as_i64() {
        return ts(seconds);
    }
    let raw = value.as_str()?.trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(seconds) = raw.parse::<i64>() {
        return ts(seconds);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Some(prost_types::Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        });
    }
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        if let Some(dt) = date.and_hms_opt(0, 0, 0) {
            return Some(prost_types::Timestamp {
                seconds: dt.and_utc().timestamp(),
                nanos: 0,
            });
        }
    }
    None
}

pub(crate) fn eps_from_json(row: &serde_json::Value) -> ana_entity_pb::ExecutorPerformanceSummary {
    let row = row_object(row);
    ana_entity_pb::ExecutorPerformanceSummary {
        summary_id: json_string(row, "summary_id"),
        summary_date: json_ts(row, "summary_date"),
        executor_identity: json_string(row, "executor_identity"),
        workload_kind: json_string(row, "workload_kind"),
        total_dispatches: json_i64(row, "total_dispatches"),
        successful_results: json_i64(row, "successful_results"),
        timeout_count: json_i64(row, "timeout_count"),
        error_count: json_i64(row, "error_count"),
        avg_execution_ms: json_f64(row, "avg_execution_ms"),
        p99_execution_ms: json_f64(row, "p99_execution_ms"),
        avg_confidence: json_f64(row, "avg_confidence"),
        success_rate: json_f64(row, "success_rate"),
        avg_capacity_utilisation: json_f64(row, "avg_capacity_utilisation"),
        recorded_at: json_ts(row, "recorded_at"),
    }
}

pub(crate) fn ras_from_json(
    row: &serde_json::Value,
) -> ana_entity_pb::ReconciliationAnalyticsSummary {
    let row = row_object(row);
    ana_entity_pb::ReconciliationAnalyticsSummary {
        summary_id: json_string(row, "summary_id"),
        summary_date: json_ts(row, "summary_date"),
        total_reconciliations: json_i64(row, "total_reconciliations"),
        exact_matches: json_i64(row, "exact_matches"),
        partial_conflicts: json_i64(row, "partial_conflicts"),
        hard_conflicts: json_i64(row, "hard_conflicts"),
        low_confidence_flagged: json_i64(row, "low_confidence_flagged"),
        avg_reconciliation_ms: json_f64(row, "avg_reconciliation_ms"),
        resolution_rate: json_f64(row, "resolution_rate"),
        avg_record_confidence: json_f64(row, "avg_record_confidence"),
        recorded_at: json_ts(row, "recorded_at"),
    }
}

pub(crate) fn pms_from_json(row: &serde_json::Value) -> ana_entity_pb::PipelineMetricSnapshot {
    let row = row_object(row);
    ana_entity_pb::PipelineMetricSnapshot {
        snapshot_id: json_string(row, "snapshot_id"),
        snapshot_hour: json_ts(row, "snapshot_hour"),
        stage_name: json_string(row, "stage_name"),
        tenant_id: json_string(row, "tenant_id"),
        total_requests: json_i64(row, "total_requests"),
        successful: json_i64(row, "successful"),
        failed: json_i64(row, "failed"),
        p50_latency_ms: json_f64(row, "p50_latency_ms"),
        p95_latency_ms: json_f64(row, "p95_latency_ms"),
        p99_latency_ms: json_f64(row, "p99_latency_ms"),
        avg_latency_ms: json_f64(row, "avg_latency_ms"),
        error_rate: json_f64(row, "error_rate"),
        throughput_rps: json_f64(row, "throughput_rps"),
        recorded_at: json_ts(row, "recorded_at"),
    }
}

pub(crate) fn pms_from_row(row: &sqlx::postgres::PgRow) -> ana_entity_pb::PipelineMetricSnapshot {
    ana_entity_pb::PipelineMetricSnapshot {
        snapshot_id: row.try_get("snapshot_id").unwrap_or_default(),
        snapshot_hour: ts(row.try_get("snapshot_hour").unwrap_or(0)),
        stage_name: row.try_get("stage_name").unwrap_or_default(),
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        total_requests: row.try_get("total_requests").unwrap_or(0),
        successful: row.try_get("successful").unwrap_or(0),
        failed: row.try_get("failed").unwrap_or(0),
        p50_latency_ms: row.try_get("p50_latency_ms").unwrap_or(0.0),
        p95_latency_ms: row.try_get("p95_latency_ms").unwrap_or(0.0),
        p99_latency_ms: row.try_get("p99_latency_ms").unwrap_or(0.0),
        avg_latency_ms: row.try_get("avg_latency_ms").unwrap_or(0.0),
        error_rate: row.try_get("error_rate").unwrap_or(0.0),
        throughput_rps: row.try_get("throughput_rps").unwrap_or(0.0),
        recorded_at: ts(row.try_get("recorded_at").unwrap_or(0)),
    }
}

pub(crate) fn eps_from_row(
    row: &sqlx::postgres::PgRow,
) -> ana_entity_pb::ExecutorPerformanceSummary {
    ana_entity_pb::ExecutorPerformanceSummary {
        summary_id: row.try_get("summary_id").unwrap_or_default(),
        summary_date: ts(row.try_get("summary_date").unwrap_or(0)),
        executor_identity: row.try_get("executor_identity").unwrap_or_default(),
        workload_kind: row.try_get("workload_kind").unwrap_or_default(),
        total_dispatches: row.try_get("total_dispatches").unwrap_or(0),
        successful_results: row.try_get("successful_results").unwrap_or(0),
        timeout_count: row.try_get("timeout_count").unwrap_or(0),
        error_count: row.try_get("error_count").unwrap_or(0),
        avg_execution_ms: row.try_get("avg_execution_ms").unwrap_or(0.0),
        p99_execution_ms: row.try_get("p99_execution_ms").unwrap_or(0.0),
        avg_confidence: row.try_get("avg_confidence").unwrap_or(0.0),
        success_rate: row.try_get("success_rate").unwrap_or(0.0),
        avg_capacity_utilisation: row.try_get("avg_capacity_utilisation").unwrap_or(0.0),
        recorded_at: ts(row.try_get("recorded_at").unwrap_or(0)),
    }
}

pub(crate) fn ras_from_row(
    row: &sqlx::postgres::PgRow,
) -> ana_entity_pb::ReconciliationAnalyticsSummary {
    ana_entity_pb::ReconciliationAnalyticsSummary {
        summary_id: row.try_get("summary_id").unwrap_or_default(),
        summary_date: ts(row.try_get("summary_date").unwrap_or(0)),
        total_reconciliations: row.try_get("total_reconciliations").unwrap_or(0),
        exact_matches: row.try_get("exact_matches").unwrap_or(0),
        partial_conflicts: row.try_get("partial_conflicts").unwrap_or(0),
        hard_conflicts: row.try_get("hard_conflicts").unwrap_or(0),
        low_confidence_flagged: row.try_get("low_confidence_flagged").unwrap_or(0),
        avg_reconciliation_ms: row.try_get("avg_reconciliation_ms").unwrap_or(0.0),
        resolution_rate: row.try_get("resolution_rate").unwrap_or(0.0),
        avg_record_confidence: row.try_get("avg_record_confidence").unwrap_or(0.0),
        recorded_at: ts(row.try_get("recorded_at").unwrap_or(0)),
    }
}
