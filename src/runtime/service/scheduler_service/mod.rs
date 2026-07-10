//! Native `SchedulerService` — proto-driven Postgres CRUD over the UDB-owned
//! `udb_scheduler.scheduled_jobs` table, plus the leader-elected scheduler tick
//! (master-plan 9.3).
//!
//! Mirrors `tenant_service`: no in-memory store, no hand-mapped schema. Table and
//! column identifiers are resolved from the embedded proto manifest via
//! [`NativeModel`] (see `runtime::native_catalog`), so the SQL here follows the
//! same single-source-of-truth rule as the rest of the native services.
//!
//! Doctrine (9.3): mutations persist durably to the canonical store, are
//! tenant-scoped by the verified claim, ride per-tenant fair admission, and emit
//! one outbox event each. The tick is leader-elected (one firer cluster-wide),
//! claims DUE rows with `FOR UPDATE SKIP LOCKED` so two leaders can never
//! double-fire, and FIRES EVENTS ONLY (`udb.scheduler.job.fired.v1`) — it never
//! executes a job payload in-process. A job that cannot make forward progress
//! after `max_attempts` is routed to the dead-letter topic
//! (`udb.scheduler.job.dead.v1`) and marked DEAD. Fail closed on a missing pool
//! or outbox relation.

use std::sync::Arc;

use chrono::{DateTime, Datelike, Duration as ChronoDuration, Timelike, Utc};
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::scheduler::entity::v1 as job_pb;
use crate::proto::udb::core::scheduler::services::v1 as scheduler_pb;
use crate::proto::udb::core::scheduler::services::v1::scheduler_service_server::SchedulerService;
use crate::runtime::channels::{ChannelManager, OperationChannel};
use crate::runtime::native_catalog::{NativeModel, native_model};

pub use crate::proto::udb::core::scheduler::services::v1::scheduler_service_server::SchedulerServiceServer;

use super::DataBrokerService;
use super::auth_service::events::{ComplianceEnvelope, build_native_compliance_envelope};
use super::native_helpers::{
    MAX_LIST_ROWS, NativeEventContext, admit_on as native_admit_on,
    enqueue_outbox_event_with_context, native_next_page_token_for_total, native_offset_page_window,
    non_empty_json, parse_uuid, validate_request_scope,
};

const SCHEDULED_JOB_MSG: &str = "udb.core.scheduler.entity.v1.ScheduledJob";

// ── outbox topics ─────────────────────────────────────────────────────────────
const TOPIC_JOB_CREATED: &str = "udb.scheduler.job.created.v1";
const TOPIC_JOB_DELETED: &str = "udb.scheduler.job.deleted.v1";
const TOPIC_JOB_PAUSED: &str = "udb.scheduler.job.paused.v1";
const TOPIC_JOB_RESUMED: &str = "udb.scheduler.job.resumed.v1";
/// FIRE event: emitted once per due job; consumers do the actual work.
const TOPIC_JOB_FIRED: &str = "udb.scheduler.job.fired.v1";
/// Dead-letter event: emitted when a job exhausts `max_attempts`.
const TOPIC_JOB_DEAD: &str = "udb.scheduler.job.dead.v1";

/// Default batch the tick claims per pass — a named constant (no per-request env
/// reads). Bounds how many DUE jobs one tick fires so a backlog can't starve the
/// transaction.
pub(crate) const SCHEDULER_TICK_BATCH: i64 = 200;

/// Postgres-backed `SchedulerService` handler.
pub struct SchedulerServiceImpl {
    pg_pool: Option<PgPool>,
    /// Transactional-outbox relation; mutations and tick fires enqueue here.
    outbox_relation: Option<String>,
    /// Per-tenant fair-admission manager (same one the data plane uses). `None`
    /// only in bare unit-test construction (admit becomes a no-op).
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
}

fn scheduler_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "scheduler",
        operation,
        capability_required,
        message,
    )
}

fn scheduler_not_found_status(
    operation: &'static str,
    schema_code: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "scheduler",
        operation,
        schema_code,
        message,
    )
}

fn scheduler_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("scheduler", operation, message)
}

impl SchedulerServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Scheduler CRUD is durable-only: fail closed when no Postgres pool exists.
    fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            scheduler_capability_status(
                "postgres_store",
                "postgres_store",
                "scheduler service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }
}

impl Default for SchedulerServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

fn scheduled_job_model() -> NativeModel {
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
fn schedule_type_to_db(value: &str) -> Result<String, Status> {
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
fn job_status_filter_to_db(value: &str) -> Result<String, Status> {
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

fn scheduler_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

// ── projection + row mapping ──────────────────────────────────────────────────

fn job_select_projection(m: &NativeModel) -> String {
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

fn job_from_row(row: &sqlx::postgres::PgRow) -> Result<job_pb::ScheduledJob, Status> {
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

#[tonic::async_trait]
impl SchedulerService for SchedulerServiceImpl {
    async fn create_job(
        &self,
        request: Request<scheduler_pb::CreateJobRequest>,
    ) -> Result<Response<scheduler_pb::CreateJobResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
        if req.name.trim().is_empty() {
            return Err(scheduler_required_field(
                "name",
                "must be a non-empty job name",
                "name is required",
            ));
        }
        let kind = schedule_type_to_db(&req.schedule_type)?;
        if kind == "CRON" {
            if req.cron_expression.trim().is_empty() {
                return Err(scheduler_required_field(
                    "cron_expression",
                    "must be a non-empty cron expression for CRON jobs",
                    "cron_expression is required for CRON jobs",
                ));
            }
            // Reject an unparseable cron up front so a job never lands DEAD on the
            // first tick for a typo (fail closed at the door, not at fire time).
            if next_cron_after(req.cron_expression.trim(), Utc::now()).is_none() {
                return Err(scheduler_required_field(
                    "cron_expression",
                    "must be a valid 5-field cron expression or @macro",
                    "cron_expression is not a valid 5-field cron or @macro",
                ));
            }
        } else if req.next_fire_at.trim().is_empty() {
            return Err(scheduler_required_field(
                "next_fire_at",
                "must be a non-empty RFC3339 timestamp for ONE_SHOT jobs",
                "next_fire_at (RFC3339) is required for ONE_SHOT jobs",
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "scheduler",
            OperationChannel::Admin,
            &req.tenant_id,
            Some(&req.project_id),
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let pool = self.require_pool()?;
        let m = scheduled_job_model();
        let rel = m.relation.clone();
        let job_id = Uuid::new_v4().to_string();
        let payload = non_empty_json(&req.payload);
        let max_attempts = if req.max_attempts > 0 {
            req.max_attempts
        } else {
            5
        };
        let backoff_seconds = if req.backoff_seconds > 0 {
            req.backoff_seconds
        } else {
            60
        };
        // ONE_SHOT with an empty seed is rejected above; a CRON with an empty seed
        // fires immediately (NOW()) and then advances from the cron expression.
        sqlx::query(&format!(
            "INSERT INTO {rel} \
               ({job_id}, {tenant_id}, {project_id}, {name}, {schedule_type}, {cron}, {payload}, \
                {target_topic}, {status}, {next_fire_at}, {max_attempts}, {attempt_count}, {backoff}) \
             VALUES ($1::UUID, $2::UUID, NULLIF($3, '')::UUID, $4, $5, NULLIF($6, ''), $7::JSONB, \
                NULLIF($8, ''), 'ACTIVE', \
                CASE WHEN $9 = '' THEN (CASE WHEN $5 = 'CRON' THEN NOW() ELSE NULL END) \
                     ELSE $9::TIMESTAMPTZ END, \
                $10, 0, $11)",
            job_id = m.q("job_id"),
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
            name = m.q("name"),
            schedule_type = m.q("schedule_type"),
            cron = m.q("cron_expression"),
            payload = m.q("payload"),
            target_topic = m.q("target_topic"),
            status = m.q("status"),
            next_fire_at = m.q("next_fire_at"),
            max_attempts = m.q("max_attempts"),
            attempt_count = m.q("attempt_count"),
            backoff = m.q("backoff_seconds"),
        ))
        .bind(&job_id)
        .bind(&tenant_id)
        .bind(&req.project_id)
        .bind(req.name.trim())
        .bind(&kind)
        .bind(req.cron_expression.trim())
        .bind(&payload)
        .bind(req.target_topic.trim())
        .bind(req.next_fire_at.trim())
        .bind(max_attempts)
        .bind(backoff_seconds)
        .execute(pool)
        .await
        .map_err(|err| {
            scheduler_internal_status(
                "create_scheduled_job",
                format!("create scheduled job failed: {err}"),
            )
        })?;

        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_JOB_CREATED,
            &tenant_id,
            &tenant_id,
            &req.project_id,
            serde_json::json!({
                "job_id": job_id.clone(),
                "tenant_id": tenant_id.clone(),
                "project_id": req.project_id.clone(),
                "name": req.name.clone(),
                "schedule_type": kind.clone(),
            }),
            NativeEventContext::default(),
            Some(&self.metrics),
        )
        .await;

        Ok(Response::new(scheduler_pb::CreateJobResponse {
            job_id,
            message: "scheduled job created".to_string(),
            error: None,
        }))
    }

    async fn get_job(
        &self,
        request: Request<scheduler_pb::GetJobRequest>,
    ) -> Result<Response<scheduler_pb::GetJobResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_scope(&metadata, &req.tenant_id, "")?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "scheduler",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let job_id = parse_uuid("job_id", &req.job_id)?.to_string();
        let pool = self.require_pool()?;
        let m = scheduled_job_model();
        let rel = m.relation.clone();
        let projection = job_select_projection(&m);
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} \
             WHERE {job_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL",
            job_id = m.q("job_id"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(&job_id)
        .bind(&tenant_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            scheduler_internal_status(
                "get_scheduled_job",
                format!("get scheduled job failed: {err}"),
            )
        })?;
        let job = row.as_ref().map(job_from_row).transpose()?.ok_or_else(|| {
            scheduler_not_found_status(
                "get_job",
                "scheduled_job_not_found",
                "scheduled job not found",
            )
        })?;
        Ok(Response::new(scheduler_pb::GetJobResponse {
            job: Some(job),
            error: None,
        }))
    }

    async fn list_jobs(
        &self,
        request: Request<scheduler_pb::ListJobsRequest>,
    ) -> Result<Response<scheduler_pb::ListJobsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_scope(&metadata, &req.tenant_id, "")?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "scheduler",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let status_filter = job_status_filter_to_db(&req.status)?;
        let pool = self.require_pool()?;
        let m = scheduled_job_model();
        let rel = m.relation.clone();
        let projection = job_select_projection(&m);
        let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
        let where_clause = format!(
            "WHERE {tenant_id} = $1::UUID AND {deleted} IS NULL AND ($2 = '' OR {status} = $2)",
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
            status = m.q("status"),
        );
        let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {rel} {where_clause}"))
            .bind(&tenant_id)
            .bind(&status_filter)
            .fetch_one(pool)
            .await
            .map_err(|err| {
                scheduler_internal_status(
                    "list_scheduled_jobs_count",
                    format!("count scheduled jobs failed: {err}"),
                )
            })?;
        let rows = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} {where_clause} \
             ORDER BY {name} LIMIT $3 OFFSET $4",
            name = m.q("name"),
        ))
        .bind(&tenant_id)
        .bind(&status_filter)
        .bind(page_window.limit_i64())
        .bind(page_window.offset_i64())
        .fetch_all(pool)
        .await
        .map_err(|err| {
            scheduler_internal_status(
                "list_scheduled_jobs",
                format!("list scheduled jobs failed: {err}"),
            )
        })?;
        let mut jobs = Vec::with_capacity(rows.len());
        for row in &rows {
            jobs.push(job_from_row(row)?);
        }
        Ok(Response::new(scheduler_pb::ListJobsResponse {
            jobs,
            total_count: total as i32,
            error: None,
            next_page_token: native_next_page_token_for_total(
                page_window.offset,
                page_window.limit,
                total,
            ),
        }))
    }

    async fn delete_job(
        &self,
        request: Request<scheduler_pb::DeleteJobRequest>,
    ) -> Result<Response<scheduler_pb::DeleteJobResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_scope(&metadata, &req.tenant_id, "")?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "scheduler",
            OperationChannel::Admin,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let job_id = parse_uuid("job_id", &req.job_id)?.to_string();
        let pool = self.require_pool()?;
        let m = scheduled_job_model();
        let rel = m.relation.clone();
        // Soft delete: set the manifest soft-delete column; the tick never claims a
        // deleted row (its claim filters `deleted_at IS NULL`).
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET {deleted} = NOW() \
             WHERE {job_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL",
            deleted = m.q("deleted_at"),
            job_id = m.q("job_id"),
            tenant_id = m.q("tenant_id"),
        ))
        .bind(&job_id)
        .bind(&tenant_id)
        .execute(pool)
        .await
        .map_err(|err| {
            scheduler_internal_status(
                "delete_scheduled_job",
                format!("delete scheduled job failed: {err}"),
            )
        })?;
        if result.rows_affected() == 0 {
            return Err(scheduler_not_found_status(
                "delete_job",
                "scheduled_job_not_found",
                "scheduled job not found",
            ));
        }
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_JOB_DELETED,
            &tenant_id,
            &tenant_id,
            "",
            serde_json::json!({ "job_id": job_id.clone(), "tenant_id": tenant_id.clone() }),
            NativeEventContext::default(),
            Some(&self.metrics),
        )
        .await;
        Ok(Response::new(scheduler_pb::DeleteJobResponse {
            message: "scheduled job deleted".to_string(),
            error: None,
        }))
    }

    async fn pause_job(
        &self,
        request: Request<scheduler_pb::PauseJobRequest>,
    ) -> Result<Response<scheduler_pb::PauseJobResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_scope(&metadata, &req.tenant_id, "")?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "scheduler",
            OperationChannel::Admin,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let job_id = parse_uuid("job_id", &req.job_id)?.to_string();
        let pool = self.require_pool()?;
        let m = scheduled_job_model();
        let rel = m.relation.clone();
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET {status} = 'PAUSED' \
             WHERE {job_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL \
               AND {status} = 'ACTIVE'",
            status = m.q("status"),
            job_id = m.q("job_id"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(&job_id)
        .bind(&tenant_id)
        .execute(pool)
        .await
        .map_err(|err| {
            scheduler_internal_status(
                "pause_scheduled_job",
                format!("pause scheduled job failed: {err}"),
            )
        })?;
        if result.rows_affected() == 0 {
            return Err(scheduler_not_found_status(
                "pause_job",
                "active_scheduled_job_not_found",
                "active scheduled job not found",
            ));
        }
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_JOB_PAUSED,
            &tenant_id,
            &tenant_id,
            "",
            serde_json::json!({ "job_id": job_id.clone(), "tenant_id": tenant_id.clone() }),
            NativeEventContext::default(),
            Some(&self.metrics),
        )
        .await;
        Ok(Response::new(scheduler_pb::PauseJobResponse {
            message: "scheduled job paused".to_string(),
            error: None,
        }))
    }

    async fn resume_job(
        &self,
        request: Request<scheduler_pb::ResumeJobRequest>,
    ) -> Result<Response<scheduler_pb::ResumeJobResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_scope(&metadata, &req.tenant_id, "")?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "scheduler",
            OperationChannel::Admin,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let job_id = parse_uuid("job_id", &req.job_id)?.to_string();
        let pool = self.require_pool()?;
        let m = scheduled_job_model();
        let rel = m.relation.clone();
        // Resume re-activates a paused job and re-arms attempt accounting. A job
        // with no future fire time (e.g. a completed one-shot) cannot be resumed.
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET {status} = 'ACTIVE', {attempt_count} = 0 \
             WHERE {job_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL \
               AND {status} = 'PAUSED'",
            status = m.q("status"),
            attempt_count = m.q("attempt_count"),
            job_id = m.q("job_id"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(&job_id)
        .bind(&tenant_id)
        .execute(pool)
        .await
        .map_err(|err| {
            scheduler_internal_status(
                "resume_scheduled_job",
                format!("resume scheduled job failed: {err}"),
            )
        })?;
        if result.rows_affected() == 0 {
            return Err(scheduler_not_found_status(
                "resume_job",
                "paused_scheduled_job_not_found",
                "paused scheduled job not found",
            ));
        }
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_JOB_RESUMED,
            &tenant_id,
            &tenant_id,
            "",
            serde_json::json!({ "job_id": job_id.clone(), "tenant_id": tenant_id.clone() }),
            NativeEventContext::default(),
            Some(&self.metrics),
        )
        .await;
        Ok(Response::new(scheduler_pb::ResumeJobResponse {
            message: "scheduled job resumed".to_string(),
            error: None,
        }))
    }
}

// ── leader-elected tick ───────────────────────────────────────────────────────

/// The `SELECT ... FOR UPDATE SKIP LOCKED` statement the tick uses to claim DUE
/// jobs. Built from the manifest model so column identifiers stay
/// single-sourced. Exposed (and unit-tested) so the no-double-fire contract is
/// asserted on the rendered SQL.
pub(crate) fn due_jobs_claim_sql(m: &NativeModel) -> String {
    let rel = m.relation.clone();
    format!(
        "SELECT {job_id}::text AS job_id, {tenant_id}::text AS tenant_id, \
            COALESCE({project_id}::text, '') AS project_id, {name} AS name, \
            {schedule_type} AS schedule_type, COALESCE({cron}, '') AS cron_expression, \
            COALESCE({payload}::text, '') AS payload, COALESCE({target_topic}, '') AS target_topic, \
            {attempt_count} AS attempt_count, {max_attempts} AS max_attempts, \
            {backoff} AS backoff_seconds \
         FROM {rel} \
         WHERE {status} = 'ACTIVE' AND {deleted} IS NULL \
           AND {next_fire_at} IS NOT NULL AND {next_fire_at} <= NOW() \
         ORDER BY {next_fire_at} \
         LIMIT $1 \
         FOR UPDATE SKIP LOCKED",
        job_id = m.q("job_id"),
        tenant_id = m.q("tenant_id"),
        project_id = m.q("project_id"),
        name = m.q("name"),
        schedule_type = m.q("schedule_type"),
        cron = m.q("cron_expression"),
        payload = m.q("payload"),
        target_topic = m.q("target_topic"),
        attempt_count = m.q("attempt_count"),
        max_attempts = m.q("max_attempts"),
        backoff = m.q("backoff_seconds"),
        status = m.q("status"),
        deleted = m.q("deleted_at"),
        next_fire_at = m.q("next_fire_at"),
    )
}

/// One scheduler-tick pass (leader-elected by the caller). Claims up to
/// `batch_size` DUE jobs with `FOR UPDATE SKIP LOCKED`, then for each job — within
/// the SAME transaction — durably enqueues a fire (or dead-letter) outbox row and
/// advances the job's schedule. Because the advance and the outbox insert commit
/// atomically, a job is never double-fired and every fire is at-least-once via the
/// outbox→CDC pipeline. The tick FIRES EVENTS ONLY; it never runs a payload.
///
/// Returns the number of jobs acted on (fired + dead-lettered). Fail closed: a
/// missing outbox relation yields `Ok(0)` (nothing fired) rather than firing
/// without a durable event.
pub(crate) async fn run_scheduler_tick_once(
    pool: &PgPool,
    outbox_relation: Option<&str>,
    batch_size: i64,
) -> Result<i64, Status> {
    let Some(outbox_rel) = outbox_relation else {
        tracing::warn!("scheduler tick: no outbox relation configured; cannot fire jobs");
        return Ok(0);
    };
    let m = scheduled_job_model();
    let jobs_rel = m.relation.clone();
    let claim_sql = due_jobs_claim_sql(&m);
    let batch = batch_size.clamp(1, MAX_LIST_ROWS);

    let mut tx = pool.begin().await.map_err(|err| {
        scheduler_internal_status(
            "scheduler_tick_begin",
            format!("scheduler tick begin failed: {err}"),
        )
    })?;
    let rows = sqlx::query(&claim_sql)
        .bind(batch)
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| {
            scheduler_internal_status(
                "scheduler_tick_claim",
                format!("scheduler tick claim failed: {err}"),
            )
        })?;

    let now = Utc::now();
    let mut acted = 0i64;
    for row in &rows {
        let get = |c: &str| -> Result<String, Status> {
            row.try_get::<String, _>(c).map_err(|e| {
                scheduler_internal_status(
                    "scheduler_tick_decode",
                    format!("scheduler tick decode {c} failed: {e}"),
                )
            })
        };
        let job_id = get("job_id")?;
        let tenant_id = get("tenant_id")?;
        let project_id = get("project_id")?;
        let name = get("name")?;
        let schedule_type = get("schedule_type")?;
        let cron = get("cron_expression")?;
        let payload = get("payload")?;
        let target_topic = get("target_topic")?;
        let attempt_count: i32 = row.try_get("attempt_count").map_err(|e| {
            scheduler_internal_status(
                "scheduler_tick_decode",
                format!("scheduler tick decode attempt_count: {e}"),
            )
        })?;
        let max_attempts: i32 = row.try_get("max_attempts").map_err(|e| {
            scheduler_internal_status(
                "scheduler_tick_decode",
                format!("scheduler tick decode max_attempts: {e}"),
            )
        })?;
        let backoff_seconds: i32 = row.try_get("backoff_seconds").map_err(|e| {
            scheduler_internal_status(
                "scheduler_tick_decode",
                format!("scheduler tick decode backoff_seconds: {e}"),
            )
        })?;

        // CRON jobs need a parseable expression to advance. A one-shot always fires
        // once. A CRON whose expression no longer yields a future time is a stuck
        // job: back it off and, after max_attempts, dead-letter it.
        let next_fire = if schedule_type == "CRON" {
            next_cron_after(&cron, now)
        } else {
            None // one-shot: no recurrence
        };
        let is_cron = schedule_type == "CRON";
        let stuck_cron = is_cron && next_fire.is_none();

        if stuck_cron {
            let new_attempts = attempt_count.saturating_add(1);
            if new_attempts >= max_attempts {
                // DLQ: exhausted attempts to advance this job.
                insert_tick_outbox(
                    &mut tx,
                    outbox_rel,
                    TOPIC_JOB_DEAD,
                    &tenant_id,
                    &project_id,
                    &job_id,
                    dead_payload(
                        &job_id,
                        &tenant_id,
                        &name,
                        new_attempts,
                        "cron_unresolvable",
                    ),
                    "dead",
                )
                .await?;
                sqlx::query(&format!(
                    "UPDATE {jobs_rel} SET {status} = 'DEAD', {next_fire_at} = NULL, \
                        {attempt_count} = $2 WHERE {job_id} = $1::UUID",
                    status = m.q("status"),
                    next_fire_at = m.q("next_fire_at"),
                    attempt_count = m.q("attempt_count"),
                    job_id = m.q("job_id"),
                ))
                .bind(&job_id)
                .bind(new_attempts)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    scheduler_internal_status(
                        "scheduler_tick_dead_update",
                        format!("scheduler tick dead update failed: {e}"),
                    )
                })?;
            } else {
                // Exponential backoff: defer the next attempt, keep the job ACTIVE.
                let delay = backoff_delay_secs(backoff_seconds, new_attempts);
                sqlx::query(&format!(
                    "UPDATE {jobs_rel} SET {attempt_count} = $2, \
                        {next_fire_at} = NOW() + make_interval(secs => $3::DOUBLE PRECISION) \
                     WHERE {job_id} = $1::UUID",
                    attempt_count = m.q("attempt_count"),
                    next_fire_at = m.q("next_fire_at"),
                    job_id = m.q("job_id"),
                ))
                .bind(&job_id)
                .bind(new_attempts)
                .bind(delay as f64)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    scheduler_internal_status(
                        "scheduler_tick_backoff_update",
                        format!("scheduler tick backoff update failed: {e}"),
                    )
                })?;
            }
            acted += 1;
            continue;
        }

        // Normal path: FIRE the job, then advance its schedule (reset attempts).
        let payload_json: serde_json::Value =
            serde_json::from_str(&payload).unwrap_or(serde_json::Value::Null);
        // Build the payload first (cloning the owned fields) so the borrows passed
        // to `insert_tick_outbox` below stay valid.
        let fired_payload = serde_json::json!({
            "job_id": job_id.clone(),
            "tenant_id": tenant_id.clone(),
            "project_id": project_id.clone(),
            "name": name.clone(),
            "schedule_type": schedule_type.clone(),
            "target_topic": target_topic.clone(),
            "payload": payload_json,
            "fired_at": now.to_rfc3339(),
        });
        insert_tick_outbox(
            &mut tx,
            outbox_rel,
            TOPIC_JOB_FIRED,
            &tenant_id,
            &project_id,
            &job_id,
            fired_payload,
            "fired",
        )
        .await?;

        if let Some(next) = next_fire {
            // Recurring: advance to the next cron occurrence, stay ACTIVE.
            sqlx::query(&format!(
                "UPDATE {jobs_rel} SET {next_fire_at} = to_timestamp($2), \
                    {last_fired_at} = NOW(), {attempt_count} = 0, {status} = 'ACTIVE' \
                 WHERE {job_id} = $1::UUID",
                next_fire_at = m.q("next_fire_at"),
                last_fired_at = m.q("last_fired_at"),
                attempt_count = m.q("attempt_count"),
                status = m.q("status"),
                job_id = m.q("job_id"),
            ))
            .bind(&job_id)
            .bind(next.timestamp() as f64)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                scheduler_internal_status(
                    "scheduler_tick_advance",
                    format!("scheduler tick advance failed: {e}"),
                )
            })?;
        } else {
            // One-shot: fired once, now terminal.
            sqlx::query(&format!(
                "UPDATE {jobs_rel} SET {status} = 'COMPLETED', {next_fire_at} = NULL, \
                    {last_fired_at} = NOW(), {attempt_count} = 0 WHERE {job_id} = $1::UUID",
                status = m.q("status"),
                next_fire_at = m.q("next_fire_at"),
                last_fired_at = m.q("last_fired_at"),
                attempt_count = m.q("attempt_count"),
                job_id = m.q("job_id"),
            ))
            .bind(&job_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                scheduler_internal_status(
                    "scheduler_tick_complete",
                    format!("scheduler tick complete failed: {e}"),
                )
            })?;
        }
        acted += 1;
    }

    tx.commit().await.map_err(|err| {
        scheduler_internal_status(
            "scheduler_tick_commit",
            format!("scheduler tick commit failed: {err}"),
        )
    })?;
    Ok(acted)
}

fn dead_payload(
    job_id: &str,
    tenant_id: &str,
    name: &str,
    attempts: i32,
    reason: &str,
) -> serde_json::Value {
    serde_json::json!({
        "job_id": job_id,
        "tenant_id": tenant_id,
        "name": name,
        "attempts": attempts,
        "reason": reason,
    })
}

/// Exponential backoff (seconds), capped at one hour, used to defer a stuck job.
fn backoff_delay_secs(base: i32, attempt: i32) -> i64 {
    let base = base.max(1) as i64;
    let shift = attempt.clamp(1, 16) as u32 - 1;
    base.saturating_mul(1i64 << shift).min(3600)
}

/// Insert ONE tick outbox row inside the tick transaction (transactional outbox),
/// using the SAME shared compliance envelope the auth/native lanes emit so the CDC
/// tailer decodes it identically. The actor is the scheduler worker (a system
/// principal), not an end user.
async fn insert_tick_outbox(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    relation: &str,
    topic: &str,
    tenant_id: &str,
    project_id: &str,
    job_id: &str,
    payload: serde_json::Value,
    operation: &str,
) -> Result<(), Status> {
    let env = ComplianceEnvelope {
        actor: "udb:scheduler".to_string(),
        operation: operation.to_string(),
        outcome: "success".to_string(),
        auth_method: "system".to_string(),
        ..ComplianceEnvelope::default()
    };
    let event_id = Uuid::new_v4();
    let envelope = build_native_compliance_envelope(
        &event_id.to_string(),
        topic,
        tenant_id, // partition key = tenant_id (matches method_event_contract)
        tenant_id,
        project_id,
        &env,
        job_id, // correlation id
        "none",
        1,
        &[],
        payload,
    );
    crate::runtime::cdc::insert_outbox_row(
        &mut **tx, relation, event_id, topic, tenant_id, &envelope,
    )
    .await
    .map_err(|e| {
        scheduler_internal_status(
            "scheduler_tick_outbox_insert",
            format!("scheduler tick outbox insert failed: {e}"),
        )
    })
}

// ── minimal self-contained cron evaluator (no external crate) ─────────────────
//
// Supports standard 5-field cron (minute hour day-of-month month day-of-week)
// with `*`, `*/step`, `a-b`, `a-b/step`, and comma lists, plus the common
// `@hourly`/`@daily`/`@weekly`/`@monthly`/`@yearly` macros. Day-of-week is 0-6
// (0 = Sunday). When BOTH day-of-month and day-of-week are restricted, a match on
// EITHER fires (Vixie-cron semantics). Returns the first matching minute strictly
// after `after`, searching up to ~366 days; `None` if the expression is invalid
// or unsatisfiable within that horizon.

fn expand_cron_macro(expr: &str) -> String {
    match expr.trim() {
        "@hourly" => "0 * * * *".to_string(),
        "@daily" | "@midnight" => "0 0 * * *".to_string(),
        "@weekly" => "0 0 * * 0".to_string(),
        "@monthly" => "0 0 1 * *".to_string(),
        "@yearly" | "@annually" => "0 0 1 1 *".to_string(),
        other => other.to_string(),
    }
}

fn parse_cron_field(field: &str, min: u8, max: u8) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    for part in field.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return None;
        }
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => (r, s.trim().parse::<u8>().ok().filter(|v| *v > 0)?),
            None => (part, 1u8),
        };
        let (lo, hi) = if range == "*" {
            (min, max)
        } else if let Some((a, b)) = range.split_once('-') {
            (a.trim().parse::<u8>().ok()?, b.trim().parse::<u8>().ok()?)
        } else {
            let v = range.trim().parse::<u8>().ok()?;
            (v, v)
        };
        if lo < min || hi > max || lo > hi {
            return None;
        }
        let mut v = lo;
        while v <= hi {
            out.push(v);
            v = v.saturating_add(step);
            if v == 0 {
                break;
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    if out.is_empty() { None } else { Some(out) }
}

fn next_cron_after(expr: &str, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let normalized = expand_cron_macro(expr);
    let fields: Vec<&str> = normalized.split_whitespace().collect();
    if fields.len() != 5 {
        return None;
    }
    let minutes = parse_cron_field(fields[0], 0, 59)?;
    let hours = parse_cron_field(fields[1], 0, 23)?;
    let doms = parse_cron_field(fields[2], 1, 31)?;
    let months = parse_cron_field(fields[3], 1, 12)?;
    let dows = parse_cron_field(fields[4], 0, 6)?;
    let dom_restricted = fields[2].trim() != "*";
    let dow_restricted = fields[4].trim() != "*";

    let mut t = (after + ChronoDuration::minutes(1))
        .with_second(0)?
        .with_nanosecond(0)?;
    let horizon = after + ChronoDuration::days(366);
    while t <= horizon {
        let minute = t.minute() as u8;
        let hour = t.hour() as u8;
        let dom = t.day() as u8;
        let month = t.month() as u8;
        let dow = t.weekday().num_days_from_sunday() as u8; // 0 = Sunday
        let day_ok = match (dom_restricted, dow_restricted) {
            (false, false) => true,
            (true, false) => doms.contains(&dom),
            (false, true) => dows.contains(&dow),
            (true, true) => doms.contains(&dom) || dows.contains(&dow),
        };
        if minutes.contains(&minute) && hours.contains(&hour) && months.contains(&month) && day_ok {
            return Some(t);
        }
        t = t + ChronoDuration::minutes(1);
    }
    None
}

impl DataBrokerService {
    /// Build the native `SchedulerService`, wired to the broker's Postgres pool
    /// and the transactional outbox. The leader-elected tick worker is spawned by
    /// `serve()` (not here), so a non-leader replica never fires.
    pub(crate) fn build_scheduler_service(&self) -> SchedulerServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam: the
        // backend is read from this service's proto `native_service` binding, then
        // a health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("scheduler", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        SchedulerServiceImpl::new()
            .with_postgres(pg_pool)
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}

#[cfg(test)]
mod scheduler_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::metadata::MetadataValue;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_schema_not_found_detail(
        status: &Status,
        operation: &str,
        schema_code: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "scheduler");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, schema_code);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "scheduler");
        assert_eq!(detail.operation, operation);
        assert!(detail.capability_required.is_empty());
        assert!(detail.policy_decision_id.is_empty());
        assert!(detail.field_violations.is_empty());
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    /// A caller scoped to tenant-a must not read/operate on tenant-b's jobs by
    /// putting a foreign tenant_id in the request BODY; the scope guard rejects
    /// this before any pool/DB access (no Postgres needed).
    #[tokio::test]
    async fn get_job_rejects_cross_tenant_body() {
        let svc = SchedulerServiceImpl::new(); // no pool, no channels (admit no-op)
        let mut request = Request::new(scheduler_pb::GetJobRequest {
            tenant_id: "tenant-b".to_string(),
            job_id: "00000000-0000-0000-0000-000000000001".to_string(),
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .get_job(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn create_job_missing_name_carries_field_violation() {
        let svc = SchedulerServiceImpl::new(); // no pool, no channels (admit no-op)
        let mut request = Request::new(scheduler_pb::CreateJobRequest {
            tenant_id: "tenant-a".to_string(),
            name: "  ".to_string(),
            schedule_type: "CRON".to_string(),
            cron_expression: "@daily".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .create_job(request)
            .await
            .expect_err("missing name must be rejected before pool access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "name is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "name");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty job name"
        );
    }

    #[tokio::test]
    async fn create_one_shot_job_missing_next_fire_at_carries_field_violation() {
        let svc = SchedulerServiceImpl::new(); // no pool, no channels (admit no-op)
        let mut request = Request::new(scheduler_pb::CreateJobRequest {
            tenant_id: "tenant-a".to_string(),
            name: "nightly".to_string(),
            schedule_type: "ONE_SHOT".to_string(),
            next_fire_at: String::new(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .create_job(request)
            .await
            .expect_err("missing next_fire_at must be rejected before pool access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "next_fire_at (RFC3339) is required for ONE_SHOT jobs"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "next_fire_at");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty RFC3339 timestamp for ONE_SHOT jobs"
        );
    }

    #[test]
    fn schedule_type_unknown_value_carries_field_violation() {
        let err = schedule_type_to_db("interval")
            .expect_err("unknown schedule_type must fail before persistence");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "unknown schedule_type: INTERVAL (expected CRON or ONE_SHOT)"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "schedule_type");
        assert_eq!(
            detail.field_violations[0].description,
            "must be CRON or ONE_SHOT"
        );
    }

    #[test]
    fn job_status_filter_unknown_value_carries_field_violation() {
        let err = job_status_filter_to_db("zombie")
            .expect_err("unknown status filter must fail before persistence");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "unknown job status filter: ZOMBIE");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "status_filter");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a known job status"
        );
    }

    #[test]
    fn scheduler_missing_postgres_capability_carries_typed_detail() {
        let err = scheduler_capability_status(
            "postgres_store",
            "postgres_store",
            "scheduler service requires a Postgres-backed store (no PG pool configured)",
        );
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "scheduler service requires a Postgres-backed store (no PG pool configured)"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "scheduler");
        assert_eq!(detail.operation, "postgres_store");
        assert_eq!(detail.capability_required, "postgres_store");
        assert!(!detail.retryable);
    }

    #[test]
    fn scheduler_not_found_statuses_carry_schema_detail() {
        for (operation, schema_code, message) in [
            (
                "get_job",
                "scheduled_job_not_found",
                "scheduled job not found",
            ),
            (
                "delete_job",
                "scheduled_job_not_found",
                "scheduled job not found",
            ),
            (
                "pause_job",
                "active_scheduled_job_not_found",
                "active scheduled job not found",
            ),
            (
                "resume_job",
                "paused_scheduled_job_not_found",
                "paused scheduled job not found",
            ),
        ] {
            assert_schema_not_found_detail(
                &scheduler_not_found_status(operation, schema_code, message),
                operation,
                schema_code,
                message,
            );
        }
    }

    #[test]
    fn scheduler_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &scheduler_internal_status(
                "scheduler_tick_claim",
                "scheduler tick claim failed: database is unavailable",
            ),
            "scheduler_tick_claim",
            "scheduler tick claim failed: database is unavailable",
        );
    }

    /// The due-claim SQL MUST use `FOR UPDATE SKIP LOCKED` so two leaders can never
    /// double-fire the same job, and must filter to ACTIVE, non-deleted, due rows.
    #[test]
    fn due_claim_sql_uses_skip_locked() {
        let sql = due_jobs_claim_sql(&scheduled_job_model());
        assert!(
            sql.contains("FOR UPDATE SKIP LOCKED"),
            "claim must skip locked rows to avoid double-fire: {sql}"
        );
        assert!(sql.contains("'ACTIVE'"), "claim must only take ACTIVE jobs");
        assert!(
            sql.contains("IS NULL"),
            "claim must exclude soft-deleted jobs"
        );
        assert!(
            sql.contains("<= NOW()"),
            "claim must only take jobs whose next_fire_at is due"
        );
    }

    #[test]
    fn cron_evaluator_advances_standard_expressions() {
        // 2026-06-26 12:00:00 UTC (a Friday).
        let base = chrono::DateTime::parse_from_rfc3339("2026-06-26T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        // Every minute → next whole minute.
        let next = next_cron_after("* * * * *", base).expect("every-minute resolves");
        assert_eq!(next.to_rfc3339(), "2026-06-26T12:01:00+00:00");
        // Daily at midnight → next day 00:00.
        let next = next_cron_after("0 0 * * *", base).expect("@daily resolves");
        assert_eq!(next.to_rfc3339(), "2026-06-27T00:00:00+00:00");
        // Macro form resolves identically.
        assert_eq!(next_cron_after("@daily", base), Some(next));
        // Step + list fields parse.
        assert!(next_cron_after("*/15 9-17 * * 1-5", base).is_some());
        // Invalid expressions fail closed.
        assert!(next_cron_after("not a cron", base).is_none());
        assert!(next_cron_after("99 * * * *", base).is_none());
    }
}
