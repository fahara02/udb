//! Unit fixtures for `SchedulerService`: tenant-isolation and typed-detail guards
//! that fire before any pool access, the no-double-fire claim SQL, the per-tenant
//! quota gate, and the self-contained cron evaluator + missed-run accounting.

use chrono::{Duration as ChronoDuration, Utc};

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::proto::udb::core::scheduler::services::v1 as scheduler_pb;
use crate::proto::udb::core::scheduler::services::v1::scheduler_service_server::SchedulerService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::SchedulerServiceImpl;
use super::cron::{MAX_MISSED_RUNS_COUNTED, missed_cron_occurrences, next_cron_after};
use super::errors::{
    scheduler_capability_status, scheduler_internal_status, scheduler_not_found_status,
};
use super::model::{job_status_filter_to_db, schedule_type_to_db, scheduled_job_model};
use super::quota::{DEFAULT_MAX_JOBS_PER_TENANT, enforce_job_quota};
use super::tick::due_jobs_claim_sql;

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
    assert!(
        sql.contains("AS next_fire_at_epoch"),
        "claim must expose the stored due time for missed-run accounting: {sql}"
    );
}

/// The per-tenant job budget refuses at/over budget with the shared typed
/// quota detail (ResourceExhausted + kind QUOTA, not retryable) and admits
/// under-budget creates — same shape as the search-index gate.
#[test]
fn job_quota_gate_refuses_over_budget_with_typed_detail() {
    enforce_job_quota(0, DEFAULT_MAX_JOBS_PER_TENANT).expect("empty tenant admitted");
    enforce_job_quota(DEFAULT_MAX_JOBS_PER_TENANT - 1, DEFAULT_MAX_JOBS_PER_TENANT)
        .expect("under-budget create admitted");
    for count in [DEFAULT_MAX_JOBS_PER_TENANT, DEFAULT_MAX_JOBS_PER_TENANT + 7] {
        let err = enforce_job_quota(count, DEFAULT_MAX_JOBS_PER_TENANT)
            .expect_err("at/over-budget create must be refused");
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            err.message(),
            format!("tenant scheduled-job quota exhausted ({DEFAULT_MAX_JOBS_PER_TENANT})")
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Quota as i32);
        assert_eq!(detail.backend, "scheduler");
        assert_eq!(detail.operation, "tenant_scheduled-job_quota");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }
}

/// Missed-run accounting: an on-time fire stamps 0; a late fire counts the
/// cron occurrences that elapsed between the stored due time and now; the
/// counting loop is bounded by the safety cap; an unparseable expression
/// fails closed to 0 (no phantom missed windows).
#[test]
fn missed_count_counts_elapsed_cron_windows() {
    // 2026-06-26 12:00:00 UTC — the stored (due) fire time of an hourly job.
    let due = chrono::DateTime::parse_from_rfc3339("2026-06-26T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    // On time (fired within the due minute): nothing collapsed.
    let now = due + ChronoDuration::seconds(30);
    assert_eq!(missed_cron_occurrences("0 * * * *", due, now), 0);
    // Fired 3h late: the 13:00, 14:00 and 15:00 windows collapse into one event.
    let now = due + ChronoDuration::hours(3);
    assert_eq!(missed_cron_occurrences("0 * * * *", due, now), 3);
    // Macro form agrees with its 5-field expansion.
    let now = due + ChronoDuration::days(3);
    assert_eq!(missed_cron_occurrences("@daily", due, now), 3);
    // An every-minute job asleep for two days hits the safety cap — the loop
    // is bounded, and the cap reads as "at least this many".
    let now = due + ChronoDuration::days(2);
    assert_eq!(
        missed_cron_occurrences("* * * * *", due, now),
        MAX_MISSED_RUNS_COUNTED
    );
    // Invalid expression: fail closed to zero.
    assert_eq!(missed_cron_occurrences("not a cron", due, now), 0);
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
