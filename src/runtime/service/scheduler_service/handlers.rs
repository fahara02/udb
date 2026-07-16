//! The six `SchedulerService` RPC handlers (create/get/list/delete/pause/resume).
//! Extracted from the trait impl; `mod.rs` delegates one line to each. Mutations
//! persist durably, are tenant-scoped by the verified claim, ride per-tenant fair
//! admission, and emit one outbox event each.

use chrono::Utc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::scheduler::services::v1 as scheduler_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    NativeEventContext, admit_on as native_admit_on, enqueue_outbox_event_with_context,
    native_next_page_token_for_total, native_offset_page_window, non_empty_json, parse_uuid,
    validate_request_scope,
};
use super::SchedulerServiceImpl;
use super::config::{TOPIC_JOB_CREATED, TOPIC_JOB_DELETED, TOPIC_JOB_PAUSED, TOPIC_JOB_RESUMED};
use super::cron::next_cron_after;
use super::errors::{
    scheduler_internal_status, scheduler_not_found_status, scheduler_required_field,
};
use super::model::{
    job_from_row, job_select_projection, job_status_filter_to_db, schedule_type_to_db,
    scheduled_job_model,
};
use super::quota::{enforce_job_quota, max_jobs_per_tenant};

/// Create a scheduled job. FIRE-ONLY SEMANTICS: when the job is due, the
/// scheduler durably emits one `udb.scheduler.job.fired.v1` event
/// (at-least-once via the outbox→CDC pipeline) — it never executes the
/// payload and never observes whether a consumer succeeded. Consequently
/// `max_attempts`/`backoff_seconds` do NOT govern delivery/execution
/// retries: they only bound the scheduling-side retry of a job whose cron
/// expression can no longer be advanced (a stuck job is backed off and
/// eventually dead-lettered). Consumer execution feedback (ack/nack
/// re-arming) is a separate contract — follow-up 16.12.5.
pub(crate) async fn create_job(
    svc: &SchedulerServiceImpl,
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
        svc.channels.as_ref(),
        &svc.metrics,
        "scheduler",
        OperationChannel::Admin,
        &req.tenant_id,
        Some(&req.project_id),
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let pool = svc.require_pool()?;
    let m = scheduled_job_model();
    let rel = m.relation.clone();
    // Per-tenant job quota: COUNT the tenant's non-deleted jobs and refuse
    // over budget with the typed quota detail (fail closed; mirrors the
    // search-index gate).
    let active_jobs: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {rel} WHERE {tenant_id} = $1::UUID AND {deleted} IS NULL",
        tenant_id = m.q("tenant_id"),
        deleted = m.q("deleted_at"),
    ))
    .bind(&tenant_id)
    .fetch_one(pool)
    .await
    .map_err(|err| {
        scheduler_internal_status(
            "count_scheduled_jobs_quota",
            format!("count scheduled jobs for quota failed: {err}"),
        )
    })?;
    enforce_job_quota(active_jobs, max_jobs_per_tenant())?;
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
        svc.outbox_relation.as_deref(),
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
        Some(&svc.metrics),
    )
    .await;

    Ok(Response::new(scheduler_pb::CreateJobResponse {
        job_id,
        message: "scheduled job created".to_string(),
        error: None,
    }))
}

pub(crate) async fn get_job(
    svc: &SchedulerServiceImpl,
    request: Request<scheduler_pb::GetJobRequest>,
) -> Result<Response<scheduler_pb::GetJobResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, "")?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "scheduler",
        OperationChannel::Read,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let job_id = parse_uuid("job_id", &req.job_id)?.to_string();
    let pool = svc.require_pool()?;
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

pub(crate) async fn list_jobs(
    svc: &SchedulerServiceImpl,
    request: Request<scheduler_pb::ListJobsRequest>,
) -> Result<Response<scheduler_pb::ListJobsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, "")?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "scheduler",
        OperationChannel::Read,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let status_filter = job_status_filter_to_db(&req.status)?;
    let pool = svc.require_pool()?;
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

pub(crate) async fn delete_job(
    svc: &SchedulerServiceImpl,
    request: Request<scheduler_pb::DeleteJobRequest>,
) -> Result<Response<scheduler_pb::DeleteJobResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, "")?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "scheduler",
        OperationChannel::Admin,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let job_id = parse_uuid("job_id", &req.job_id)?.to_string();
    let pool = svc.require_pool()?;
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
        svc.outbox_relation.as_deref(),
        TOPIC_JOB_DELETED,
        &tenant_id,
        &tenant_id,
        "",
        serde_json::json!({ "job_id": job_id.clone(), "tenant_id": tenant_id.clone() }),
        NativeEventContext::default(),
        Some(&svc.metrics),
    )
    .await;
    Ok(Response::new(scheduler_pb::DeleteJobResponse {
        message: "scheduled job deleted".to_string(),
        error: None,
    }))
}

pub(crate) async fn pause_job(
    svc: &SchedulerServiceImpl,
    request: Request<scheduler_pb::PauseJobRequest>,
) -> Result<Response<scheduler_pb::PauseJobResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, "")?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "scheduler",
        OperationChannel::Admin,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let job_id = parse_uuid("job_id", &req.job_id)?.to_string();
    let pool = svc.require_pool()?;
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
        svc.outbox_relation.as_deref(),
        TOPIC_JOB_PAUSED,
        &tenant_id,
        &tenant_id,
        "",
        serde_json::json!({ "job_id": job_id.clone(), "tenant_id": tenant_id.clone() }),
        NativeEventContext::default(),
        Some(&svc.metrics),
    )
    .await;
    Ok(Response::new(scheduler_pb::PauseJobResponse {
        message: "scheduled job paused".to_string(),
        error: None,
    }))
}

pub(crate) async fn resume_job(
    svc: &SchedulerServiceImpl,
    request: Request<scheduler_pb::ResumeJobRequest>,
) -> Result<Response<scheduler_pb::ResumeJobResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, "")?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "scheduler",
        OperationChannel::Admin,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let job_id = parse_uuid("job_id", &req.job_id)?.to_string();
    let pool = svc.require_pool()?;
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
        svc.outbox_relation.as_deref(),
        TOPIC_JOB_RESUMED,
        &tenant_id,
        &tenant_id,
        "",
        serde_json::json!({ "job_id": job_id.clone(), "tenant_id": tenant_id.clone() }),
        NativeEventContext::default(),
        Some(&svc.metrics),
    )
    .await;
    Ok(Response::new(scheduler_pb::ResumeJobResponse {
        message: "scheduled job resumed".to_string(),
        error: None,
    }))
}
