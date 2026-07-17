//! Manifest-resolved model for `scheduled_jobs`: the `NativeModel`, enum <-> db
//! token mapping, the SELECT projection, and proto row decoding.

use sqlx::Row;
use tonic::Status;

use crate::proto::udb::core::scheduler::entity::v1 as job_pb;
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::config::SCHEDULED_JOB_MSG;
use super::errors::scheduler_internal_status;

pub(crate) fn scheduled_job_model() -> NativeModel {
    native_model(
        SCHEDULED_JOB_MSG,
        &[
            "job_id",
            "tenant_id",
            "project_id",
            "name",
            "schedule_type",
            "cron_expression",
            "payload",
            "target_topic",
            "status",
            "next_fire_at",
            "last_fired_at",
            "max_attempts",
            "attempt_count",
            "backoff_seconds",
            "deleted_at",
            "deleted_by",
        ],
    )
}

// ── enum <-> db (stored as VARCHAR via the proto_enum serializer) ─────────────

fn schedule_type_from_db(value: &str) -> i32 {
    use job_pb::ScheduleType as T;
    match value {
        "CRON" | "SCHEDULE_TYPE_CRON" => T::Cron as i32,
        "ONE_SHOT" | "SCHEDULE_TYPE_ONE_SHOT" => T::OneShot as i32,
        _ => T::Unspecified as i32,
    }
}

fn job_status_from_db(value: &str) -> i32 {
    use job_pb::JobStatus as S;
    match value {
        "ACTIVE" | "JOB_STATUS_ACTIVE" => S::Active as i32,
        "PAUSED" | "JOB_STATUS_PAUSED" => S::Paused as i32,
        "COMPLETED" | "JOB_STATUS_COMPLETED" => S::Completed as i32,
        "DEAD" | "JOB_STATUS_DEAD" => S::Dead as i32,
        _ => S::Unspecified as i32,
    }
}

/// Normalize a schedule-type string to the canonical SHORT stored token,
/// accepting either the short or proto-prefixed form. Empty input is rejected so
/// a job is never created with an unknown recurrence kind.
pub(crate) fn schedule_type_to_db(value: &str) -> Result<String, Status> {
    match value.trim().to_ascii_uppercase().as_str() {
        "CRON" | "SCHEDULE_TYPE_CRON" => Ok("CRON".to_string()),
        "ONE_SHOT" | "ONESHOT" | "SCHEDULE_TYPE_ONE_SHOT" => Ok("ONE_SHOT".to_string()),
        other => Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!("unknown schedule_type: {other} (expected CRON or ONE_SHOT)"),
            [("schedule_type", "must be CRON or ONE_SHOT")],
        )),
    }
}

/// Normalize a job-status filter string to the canonical SHORT stored token.
/// Empty → empty (no filter). Unknown non-empty input is rejected.
pub(crate) fn job_status_filter_to_db(value: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(String::new());
    }
    match v.to_ascii_uppercase().as_str() {
        "ACTIVE" | "JOB_STATUS_ACTIVE" => Ok("ACTIVE".to_string()),
        "PAUSED" | "JOB_STATUS_PAUSED" => Ok("PAUSED".to_string()),
        "COMPLETED" | "JOB_STATUS_COMPLETED" => Ok("COMPLETED".to_string()),
        "DEAD" | "JOB_STATUS_DEAD" => Ok("DEAD".to_string()),
        other => Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!("unknown job status filter: {other}"),
            [("status_filter", "must be a known job status")],
        )),
    }
}

fn epoch_to_ts(epoch: Option<i64>) -> Option<prost_types::Timestamp> {
    epoch.map(|seconds| prost_types::Timestamp { seconds, nanos: 0 })
}

// ── projection + row mapping ──────────────────────────────────────────────────

pub(crate) fn job_select_projection(m: &NativeModel) -> String {
    [
        m.text("job_id"),
        m.text("tenant_id"),
        m.text_or_empty("project_id"),
        m.select("name"),
        m.select("schedule_type"),
        m.text_or_empty("cron_expression"),
        m.text_or_empty("payload"),
        m.text_or_empty("target_topic"),
        m.select("status"),
        format!(
            "EXTRACT(EPOCH FROM {})::BIGINT AS next_fire_at_epoch",
            m.q("next_fire_at")
        ),
        format!(
            "EXTRACT(EPOCH FROM {})::BIGINT AS last_fired_at_epoch",
            m.q("last_fired_at")
        ),
        m.select("max_attempts"),
        m.select("attempt_count"),
        m.select("backoff_seconds"),
        m.text_or_empty("deleted_by"),
    ]
    .join(", ")
}

pub(crate) fn job_from_row(row: &sqlx::postgres::PgRow) -> Result<job_pb::ScheduledJob, Status> {
    let map = |e: sqlx::Error| {
        scheduler_internal_status(
            "decode_scheduled_job",
            format!("decode scheduled job failed: {e}"),
        )
    };
    Ok(job_pb::ScheduledJob {
        job_id: row.try_get("job_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        project_id: row.try_get("project_id").map_err(map)?,
        name: row.try_get("name").map_err(map)?,
        schedule_type: schedule_type_from_db(
            &row.try_get::<String, _>("schedule_type").map_err(map)?,
        ),
        cron_expression: row.try_get("cron_expression").map_err(map)?,
        payload: row.try_get("payload").map_err(map)?,
        target_topic: row.try_get("target_topic").map_err(map)?,
        status: job_status_from_db(&row.try_get::<String, _>("status").map_err(map)?),
        next_fire_at: epoch_to_ts(
            row.try_get::<Option<i64>, _>("next_fire_at_epoch")
                .map_err(map)?,
        ),
        last_fired_at: epoch_to_ts(
            row.try_get::<Option<i64>, _>("last_fired_at_epoch")
                .map_err(map)?,
        ),
        max_attempts: row.try_get("max_attempts").map_err(map)?,
        attempt_count: row.try_get("attempt_count").map_err(map)?,
        backoff_seconds: row.try_get("backoff_seconds").map_err(map)?,
        ..Default::default()
    })
}
