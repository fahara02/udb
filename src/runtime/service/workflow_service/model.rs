//! The manifest model, enum<->db mapping, small pure request helpers, and the
//! `WorkflowInstance` row decoder for the native `WorkflowService`. Table and
//! column identifiers are resolved from the embedded proto manifest via
//! [`NativeModel`], so the SQL follows the same single-source-of-truth rule as
//! the rest of the native services.

use sqlx::Row;
use tonic::Status;

use crate::proto::udb::core::workflow::entity::v1 as wf_pb;
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::config::MAX_WORKFLOW_STEPS;
use super::errors::workflow_internal_status;

pub(crate) fn workflow_model() -> NativeModel {
    native_model(
        super::config::WORKFLOW_MSG,
        &[
            "workflow_id",
            "tenant_id",
            "project_id",
            "workflow_type",
            "status",
            "current_step",
            "total_steps",
            "payload",
            "compensations",
            "correlation_id",
            "saga_id",
            "pending_signal",
            "last_error",
            "next_run_at",
            "last_transition_at",
            "deleted_at",
            "deleted_by",
        ],
    )
}

// ── enum <-> db ────────────────────────────────────────────────────────────────

pub(crate) fn workflow_status_from_db(value: &str) -> i32 {
    use wf_pb::WorkflowStatus as S;
    let v = match value {
        "PENDING" | "WORKFLOW_STATUS_PENDING" => S::Pending,
        "RUNNING" | "WORKFLOW_STATUS_RUNNING" => S::Running,
        "WAITING_SIGNAL" | "WORKFLOW_STATUS_WAITING_SIGNAL" => S::WaitingSignal,
        "COMPLETED" | "WORKFLOW_STATUS_COMPLETED" => S::Completed,
        "COMPENSATING" | "WORKFLOW_STATUS_COMPENSATING" => S::Compensating,
        "COMPENSATED" | "WORKFLOW_STATUS_COMPENSATED" => S::Compensated,
        "CANCELLED" | "WORKFLOW_STATUS_CANCELLED" => S::Cancelled,
        "FAILED" | "WORKFLOW_STATUS_FAILED" => S::Failed,
        _ => S::Unspecified,
    };
    v as i32
}

/// Normalize a status filter string to the canonical SHORT stored token. Empty →
/// empty (no filter). Unknown non-empty input is rejected (fail closed).
pub(crate) fn workflow_status_filter_to_db(value: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(String::new());
    }
    match v.to_ascii_uppercase().as_str() {
        "PENDING" | "WORKFLOW_STATUS_PENDING" => Ok("PENDING".to_string()),
        "RUNNING" | "WORKFLOW_STATUS_RUNNING" => Ok("RUNNING".to_string()),
        "WAITING_SIGNAL" | "WORKFLOW_STATUS_WAITING_SIGNAL" => Ok("WAITING_SIGNAL".to_string()),
        "COMPLETED" | "WORKFLOW_STATUS_COMPLETED" => Ok("COMPLETED".to_string()),
        "COMPENSATING" | "WORKFLOW_STATUS_COMPENSATING" => Ok("COMPENSATING".to_string()),
        "COMPENSATED" | "WORKFLOW_STATUS_COMPENSATED" => Ok("COMPENSATED".to_string()),
        "CANCELLED" | "WORKFLOW_STATUS_CANCELLED" => Ok("CANCELLED".to_string()),
        "FAILED" | "WORKFLOW_STATUS_FAILED" => Ok("FAILED".to_string()),
        other => Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!("unknown workflow status filter: {other}"),
            [("status_filter", "must be a known workflow status")],
        )),
    }
}

/// A terminal status is never re-advanced and cannot be cancelled again.
pub(crate) fn is_terminal_status(status: &str) -> bool {
    matches!(status, "COMPLETED" | "COMPENSATED" | "CANCELLED" | "FAILED")
}

pub(crate) fn clamp_total_steps(requested: i32) -> i32 {
    requested.clamp(1, MAX_WORKFLOW_STEPS)
}

/// Default an empty compensation field to a JSON array (the saga engine expects a
/// JSON array of compensation payloads), unlike [`non_empty_json`] which defaults
/// to an object.
pub(crate) fn non_empty_json_array(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        "[]".to_string()
    } else {
        trimmed.to_string()
    }
}

pub(crate) fn epoch_to_ts(epoch: Option<i64>) -> Option<prost_types::Timestamp> {
    epoch.map(|seconds| prost_types::Timestamp { seconds, nanos: 0 })
}

// ── projection + row mapping ────────────────────────────────────────────────────

pub(crate) fn workflow_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<wf_pb::WorkflowInstance, Status> {
    let map = |e: sqlx::Error| {
        workflow_internal_status(
            "decode_workflow_instance",
            format!("decode workflow instance failed: {e}"),
        )
    };
    Ok(wf_pb::WorkflowInstance {
        workflow_id: row.try_get("workflow_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        project_id: row.try_get("project_id").map_err(map)?,
        workflow_type: row.try_get("workflow_type").map_err(map)?,
        status: workflow_status_from_db(&row.try_get::<String, _>("status").map_err(map)?),
        current_step: row.try_get("current_step").map_err(map)?,
        total_steps: row.try_get("total_steps").map_err(map)?,
        payload: row.try_get("payload").map_err(map)?,
        compensations: row.try_get("compensations").map_err(map)?,
        correlation_id: row.try_get("correlation_id").map_err(map)?,
        saga_id: row.try_get("saga_id").map_err(map)?,
        pending_signal: row.try_get("pending_signal").map_err(map)?,
        last_error: row.try_get("last_error").map_err(map)?,
        next_run_at: epoch_to_ts(
            row.try_get::<Option<i64>, _>("next_run_at_epoch")
                .map_err(map)?,
        ),
        last_transition_at: epoch_to_ts(
            row.try_get::<Option<i64>, _>("last_transition_at_epoch")
                .map_err(map)?,
        ),
        ..Default::default()
    })
}
