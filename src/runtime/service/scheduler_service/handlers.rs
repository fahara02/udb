//! The six `SchedulerService` RPC handlers (create/get/list/delete/pause/resume).
//! Extracted from the trait impl; `mod.rs` delegates one line to each. Mutations
//! persist durably, are tenant-scoped by the verified claim, ride per-tenant fair
//! admission, and emit one outbox event each.

use chrono::Utc;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::scheduler::services::v1 as scheduler_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    NativeEventContext, admit_on as native_admit_on, enqueue_outbox_event_in_tx,
    metadata_project_id, native_next_page_token_for_total, native_offset_page_window,
    non_empty_json, parse_uuid, validate_request_scope,
};
use super::SchedulerServiceImpl;
use super::config::{
    TOPIC_JOB_CREATED, TOPIC_JOB_DELETED, TOPIC_JOB_PAUSED, TOPIC_JOB_RESUMED, scheduler_default_tz,
};
use super::cron::{next_cron_after_tz, timezone_from_payload};
use super::errors::{
    scheduler_internal_status, scheduler_not_found_status, scheduler_required_field,
};
use super::model::{
    job_from_row, job_select_projection, job_status_filter_to_db, schedule_type_to_db,
    scheduled_job_model,
};
use super::quota::{job_quota_exhausted_status, max_jobs_per_tenant};

use crate::runtime::native_catalog::NativeModel;

/// Resolve Scheduler's authorization project claim-first. ScheduledJob stores
/// UUID project identifiers, so every non-empty authority is validated before
/// reaching SQL; an empty value deliberately preserves tenant-wide operators.
fn resolved_scheduler_project_scope(
    metadata: &MetadataMap,
    request_project_id: &str,
) -> Result<String, Status> {
    let project_id = metadata_project_id(metadata)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| request_project_id.trim().to_string());
    if project_id.is_empty() {
        Ok(String::new())
    } else {
        Ok(parse_uuid("project_id", &project_id)?.to_string())
    }
}

/// Optional project-ownership predicate shared by every post-create Scheduler
/// query. `$n = ''` means an intentionally tenant-wide caller; a project-scoped
/// caller must match the durable ScheduledJob owner exactly.
pub(crate) fn project_scope_predicate(m: &NativeModel, bind: &str) -> String {
    let project_id = m.q("project_id");
    format!("(NULLIF({bind}, '')::UUID IS NULL OR {project_id} = NULLIF({bind}, '')::UUID)")
}

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
    let mut req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
    req.project_id = resolved_scheduler_project_scope(&metadata, &req.project_id)?;
    if req.name.trim().is_empty() {
        return Err(scheduler_required_field(
            "name",
            "must be a non-empty job name",
            "name is required",
        ));
    }
    let kind = schedule_type_to_db(&req.schedule_type)?;
    // Resolve the per-job timezone from the opaque payload up front so an invalid
    // explicit IANA name fails closed at the door (never persisted), independent of
    // schedule kind. Absent/empty falls through to the process default below.
    let job_tz = timezone_from_payload(&req.payload)?;
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
        // Validate in the SAME zone the tick will fire in (per-job override, else
        // the process default, else UTC) so validity and firing never diverge.
        let effective_tz = job_tz.or_else(scheduler_default_tz);
        if next_cron_after_tz(req.cron_expression.trim(), Utc::now(), effective_tz).is_none() {
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
    let limit = max_jobs_per_tenant();
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

    // ONE transaction so the quota gate is ATOMIC and the CREATED outbox event
    // co-commits with the row (transactional outbox). A non-transactional
    // COUNT-then-INSERT is a TOCTOU race: concurrent creates at budget-1 each
    // read `count < budget` (their peers' inserts are invisible under READ
    // COMMITTED) and all land, blowing past `UDB_MAX_JOBS_PER_TENANT`.
    let mut tx = pool.begin().await.map_err(|err| {
        scheduler_internal_status(
            "create_scheduled_job_begin",
            format!("create scheduled job begin failed: {err}"),
        )
    })?;
    // Serialize THIS tenant's creates with a per-tenant advisory xact lock so
    // the count-gated INSERT below cannot race a sibling create; the lock
    // auto-releases on tx end. `hashtext()` folds the tenant UUID text into the
    // bigint the advisory-lock builtin takes (same serialization the audit
    // chain uses).
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::BIGINT)")
        .bind(&tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            scheduler_internal_status(
                "create_scheduled_job_lock",
                format!("create scheduled job tenant lock failed: {err}"),
            )
        })?;
    // Atomic quota gate: the INSERT persists ONLY while the tenant is under
    // budget, counted in the SAME statement under the lock. 0 rows inserted ⇒
    // at/over budget ⇒ fail closed with the typed quota detail (tx rolls back on
    // drop). ONE_SHOT with an empty seed is rejected above; a CRON with an empty
    // seed fires immediately (NOW()) then advances from the cron expression.
    let inserted = sqlx::query(&guarded_insert_job_sql(&m))
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
        .bind(limit)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            scheduler_internal_status(
                "create_scheduled_job",
                format!("create scheduled job failed: {err}"),
            )
        })?;
    if inserted.rows_affected() == 0 {
        return Err(job_quota_exhausted_status(limit));
    }

    // CREATED outbox INSIDE the tx (co-commits with the row). Strict: a failed
    // enqueue rolls the create back rather than silently dropping the event.
    enqueue_outbox_event_in_tx(
        &mut *tx,
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
    )
    .await
    .map_err(|err| {
        scheduler_internal_status(
            "create_scheduled_job_outbox",
            format!("create scheduled job outbox enqueue failed: {err}"),
        )
    })?;

    tx.commit().await.map_err(|err| {
        scheduler_internal_status(
            "create_scheduled_job_commit",
            format!("create scheduled job commit failed: {err}"),
        )
    })?;

    Ok(Response::new(scheduler_pb::CreateJobResponse {
        job_id,
        message: "scheduled job created".to_string(),
        error: None,
    }))
}

/// The atomic, quota-gated INSERT for a scheduled job, built from the manifest
/// model so column identifiers stay single-sourced. The row persists ONLY while
/// the tenant's non-deleted job count is under `$12` (the budget), counted in
/// the SAME statement — so, run under the per-tenant advisory lock, the create
/// path can never exceed the cap (0 rows inserted signals over-budget). Exposed
/// (and unit-tested) so the atomicity contract is asserted on the rendered SQL.
pub(crate) fn guarded_insert_job_sql(m: &NativeModel) -> String {
    let rel = m.relation.clone();
    format!(
        "INSERT INTO {rel} \
           ({job_id}, {tenant_id}, {project_id}, {name}, {schedule_type}, {cron}, {payload}, \
            {target_topic}, {status}, {next_fire_at}, {max_attempts}, {attempt_count}, {backoff}) \
         SELECT $1::UUID, $2::UUID, NULLIF($3, '')::UUID, $4, $5, NULLIF($6, ''), $7::JSONB, \
            NULLIF($8, ''), 'ACTIVE', \
            CASE WHEN $9 = '' THEN (CASE WHEN $5 = 'CRON' THEN NOW() ELSE NULL END) \
                 ELSE $9::TIMESTAMPTZ END, \
            $10, 0, $11 \
         WHERE (SELECT COUNT(*) FROM {rel} \
                WHERE {tenant_id} = $2::UUID AND {deleted} IS NULL) < $12",
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
        deleted = m.q("deleted_at"),
    )
}

pub(crate) async fn get_job(
    svc: &SchedulerServiceImpl,
    request: Request<scheduler_pb::GetJobRequest>,
) -> Result<Response<scheduler_pb::GetJobResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, "")?;
    let project_id = resolved_scheduler_project_scope(&metadata, "")?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "scheduler",
        OperationChannel::Read,
        &req.tenant_id,
        (!project_id.is_empty()).then_some(project_id.as_str()),
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let job_id = parse_uuid("job_id", &req.job_id)?.to_string();
    let pool = svc.require_pool()?;
    let m = scheduled_job_model();
    let rel = m.relation.clone();
    let projection = job_select_projection(&m);
    let project_scope = project_scope_predicate(&m, "$3");
    let row = sqlx::query(&format!(
        "SELECT {projection} FROM {rel} \
         WHERE {job_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL \
           AND {project_scope}",
        job_id = m.q("job_id"),
        tenant_id = m.q("tenant_id"),
        deleted = m.q("deleted_at"),
    ))
    .bind(&job_id)
    .bind(&tenant_id)
    .bind(&project_id)
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
    let project_id = resolved_scheduler_project_scope(&metadata, "")?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "scheduler",
        OperationChannel::Read,
        &req.tenant_id,
        (!project_id.is_empty()).then_some(project_id.as_str()),
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let status_filter = job_status_filter_to_db(&req.status)?;
    let pool = svc.require_pool()?;
    let m = scheduled_job_model();
    let rel = m.relation.clone();
    let projection = job_select_projection(&m);
    let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
    let project_scope = project_scope_predicate(&m, "$3");
    let where_clause = format!(
        "WHERE {tenant_id} = $1::UUID AND {deleted} IS NULL AND ($2 = '' OR {status} = $2) \
           AND {project_scope}",
        tenant_id = m.q("tenant_id"),
        deleted = m.q("deleted_at"),
        status = m.q("status"),
    );
    let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {rel} {where_clause}"))
        .bind(&tenant_id)
        .bind(&status_filter)
        .bind(&project_id)
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
         ORDER BY {name} LIMIT $4 OFFSET $5",
        name = m.q("name"),
    ))
    .bind(&tenant_id)
    .bind(&status_filter)
    .bind(&project_id)
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
    let project_id = resolved_scheduler_project_scope(&metadata, "")?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "scheduler",
        OperationChannel::Admin,
        &req.tenant_id,
        (!project_id.is_empty()).then_some(project_id.as_str()),
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let job_id = parse_uuid("job_id", &req.job_id)?.to_string();
    let pool = svc.require_pool()?;
    let m = scheduled_job_model();
    let rel = m.relation.clone();
    // Soft delete + DELETED outbox event in ONE tx so the event co-commits with
    // the mutation (transactional outbox — no delete without its durable event,
    // no orphan event without the delete). The tick never claims a deleted row
    // (its claim filters `deleted_at IS NULL`).
    let mut tx = pool.begin().await.map_err(|err| {
        scheduler_internal_status(
            "delete_scheduled_job_begin",
            format!("delete scheduled job begin failed: {err}"),
        )
    })?;
    let project_scope = project_scope_predicate(&m, "$3");
    let event_project_id = sqlx::query_scalar::<_, String>(&format!(
        "UPDATE {rel} SET {deleted} = NOW() \
         WHERE {job_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL \
           AND {project_scope} \
         RETURNING COALESCE({project_id_column}::TEXT, '')",
        deleted = m.q("deleted_at"),
        job_id = m.q("job_id"),
        tenant_id = m.q("tenant_id"),
        project_id_column = m.q("project_id"),
    ))
    .bind(&job_id)
    .bind(&tenant_id)
    .bind(&project_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|err| {
        scheduler_internal_status(
            "delete_scheduled_job",
            format!("delete scheduled job failed: {err}"),
        )
    })?;
    let Some(event_project_id) = event_project_id else {
        // Nothing matched: fail closed; the tx rolls back on drop (no event).
        return Err(scheduler_not_found_status(
            "delete_job",
            "scheduled_job_not_found",
            "scheduled job not found",
        ));
    };
    enqueue_outbox_event_in_tx(
        &mut *tx,
        svc.outbox_relation.as_deref(),
        TOPIC_JOB_DELETED,
        &tenant_id,
        &tenant_id,
        &event_project_id,
        serde_json::json!({
            "job_id": job_id.clone(),
            "tenant_id": tenant_id.clone(),
            "project_id": event_project_id.clone(),
        }),
        NativeEventContext::default(),
    )
    .await
    .map_err(|err| {
        scheduler_internal_status(
            "delete_scheduled_job_outbox",
            format!("delete scheduled job outbox enqueue failed: {err}"),
        )
    })?;
    tx.commit().await.map_err(|err| {
        scheduler_internal_status(
            "delete_scheduled_job_commit",
            format!("delete scheduled job commit failed: {err}"),
        )
    })?;
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
    let project_id = resolved_scheduler_project_scope(&metadata, "")?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "scheduler",
        OperationChannel::Admin,
        &req.tenant_id,
        (!project_id.is_empty()).then_some(project_id.as_str()),
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let job_id = parse_uuid("job_id", &req.job_id)?.to_string();
    let pool = svc.require_pool()?;
    let m = scheduled_job_model();
    let rel = m.relation.clone();
    // Pause + PAUSED outbox event in ONE tx (transactional outbox).
    let mut tx = pool.begin().await.map_err(|err| {
        scheduler_internal_status(
            "pause_scheduled_job_begin",
            format!("pause scheduled job begin failed: {err}"),
        )
    })?;
    let project_scope = project_scope_predicate(&m, "$3");
    let event_project_id = sqlx::query_scalar::<_, String>(&format!(
        "UPDATE {rel} SET {status} = 'PAUSED' \
         WHERE {job_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL \
           AND {status} = 'ACTIVE' AND {project_scope} \
         RETURNING COALESCE({project_id_column}::TEXT, '')",
        status = m.q("status"),
        job_id = m.q("job_id"),
        tenant_id = m.q("tenant_id"),
        deleted = m.q("deleted_at"),
        project_id_column = m.q("project_id"),
    ))
    .bind(&job_id)
    .bind(&tenant_id)
    .bind(&project_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|err| {
        scheduler_internal_status(
            "pause_scheduled_job",
            format!("pause scheduled job failed: {err}"),
        )
    })?;
    let Some(event_project_id) = event_project_id else {
        return Err(scheduler_not_found_status(
            "pause_job",
            "active_scheduled_job_not_found",
            "active scheduled job not found",
        ));
    };
    enqueue_outbox_event_in_tx(
        &mut *tx,
        svc.outbox_relation.as_deref(),
        TOPIC_JOB_PAUSED,
        &tenant_id,
        &tenant_id,
        &event_project_id,
        serde_json::json!({
            "job_id": job_id.clone(),
            "tenant_id": tenant_id.clone(),
            "project_id": event_project_id.clone(),
        }),
        NativeEventContext::default(),
    )
    .await
    .map_err(|err| {
        scheduler_internal_status(
            "pause_scheduled_job_outbox",
            format!("pause scheduled job outbox enqueue failed: {err}"),
        )
    })?;
    tx.commit().await.map_err(|err| {
        scheduler_internal_status(
            "pause_scheduled_job_commit",
            format!("pause scheduled job commit failed: {err}"),
        )
    })?;
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
    let project_id = resolved_scheduler_project_scope(&metadata, "")?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "scheduler",
        OperationChannel::Admin,
        &req.tenant_id,
        (!project_id.is_empty()).then_some(project_id.as_str()),
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let job_id = parse_uuid("job_id", &req.job_id)?.to_string();
    let pool = svc.require_pool()?;
    let m = scheduled_job_model();
    let rel = m.relation.clone();
    // Resume re-activates a paused job and re-arms attempt accounting. A job
    // with no future fire time (e.g. a completed one-shot) cannot be resumed.
    // The mutation and its RESUMED outbox event commit in ONE tx.
    let mut tx = pool.begin().await.map_err(|err| {
        scheduler_internal_status(
            "resume_scheduled_job_begin",
            format!("resume scheduled job begin failed: {err}"),
        )
    })?;
    let project_scope = project_scope_predicate(&m, "$3");
    let event_project_id = sqlx::query_scalar::<_, String>(&format!(
        "UPDATE {rel} SET {status} = 'ACTIVE', {attempt_count} = 0 \
         WHERE {job_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL \
           AND {status} = 'PAUSED' AND {project_scope} \
         RETURNING COALESCE({project_id_column}::TEXT, '')",
        status = m.q("status"),
        attempt_count = m.q("attempt_count"),
        job_id = m.q("job_id"),
        tenant_id = m.q("tenant_id"),
        deleted = m.q("deleted_at"),
        project_id_column = m.q("project_id"),
    ))
    .bind(&job_id)
    .bind(&tenant_id)
    .bind(&project_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|err| {
        scheduler_internal_status(
            "resume_scheduled_job",
            format!("resume scheduled job failed: {err}"),
        )
    })?;
    let Some(event_project_id) = event_project_id else {
        return Err(scheduler_not_found_status(
            "resume_job",
            "paused_scheduled_job_not_found",
            "paused scheduled job not found",
        ));
    };
    enqueue_outbox_event_in_tx(
        &mut *tx,
        svc.outbox_relation.as_deref(),
        TOPIC_JOB_RESUMED,
        &tenant_id,
        &tenant_id,
        &event_project_id,
        serde_json::json!({
            "job_id": job_id.clone(),
            "tenant_id": tenant_id.clone(),
            "project_id": event_project_id.clone(),
        }),
        NativeEventContext::default(),
    )
    .await
    .map_err(|err| {
        scheduler_internal_status(
            "resume_scheduled_job_outbox",
            format!("resume scheduled job outbox enqueue failed: {err}"),
        )
    })?;
    tx.commit().await.map_err(|err| {
        scheduler_internal_status(
            "resume_scheduled_job_commit",
            format!("resume scheduled job commit failed: {err}"),
        )
    })?;
    Ok(Response::new(scheduler_pb::ResumeJobResponse {
        message: "scheduled job resumed".to_string(),
        error: None,
    }))
}
