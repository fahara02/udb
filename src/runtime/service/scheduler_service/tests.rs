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
use super::cron::{
    MAX_MISSED_RUNS_COUNTED, effective_tz, missed_cron_occurrences, next_cron_after,
    next_cron_after_in_zone, next_cron_after_tz, timezone_from_payload,
};
use super::errors::{
    scheduler_capability_status, scheduler_internal_status, scheduler_not_found_status,
};
use super::handlers::{guarded_insert_job_sql, project_scope_predicate};
use super::model::{job_status_filter_to_db, schedule_type_to_db, scheduled_job_model};
use super::quota::{DEFAULT_MAX_JOBS_PER_TENANT, job_quota_exhausted_status};
use super::tick::{due_jobs_claim_sql, fired_idempotency_key};

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
async fn get_job_accepts_opaque_project_authority_before_pool_access() {
    let tenant_id = "11111111-1111-4111-8111-111111111111";
    let svc = SchedulerServiceImpl::new();
    let mut request = Request::new(scheduler_pb::GetJobRequest {
        tenant_id: tenant_id.to_string(),
        job_id: "22222222-2222-4222-8222-222222222222".to_string(),
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    request
        .metadata_mut()
        .insert("x-udb-project-id", MetadataValue::from_static("not-a-uuid"));

    // An opaque project code is a VALID authority now, so scope resolution must
    // let it through: the call still fails (this service has no pool), but NOT
    // with an InvalidArgument about project_id. Asserting the negative keeps the
    // test independent of whichever capability error the pool-less path returns.
    let err = svc
        .get_job(request)
        .await
        .expect_err("no pool is configured, so the call cannot succeed");
    assert_ne!(
        err.code(),
        tonic::Code::InvalidArgument,
        "an opaque project code must pass scope resolution, got: {err:?}"
    );
}

#[test]
fn scheduler_project_scope_predicate_is_optional_but_exact() {
    let predicate = project_scope_predicate(&scheduled_job_model(), "$3");
    assert!(
        predicate.contains("$3 = ''"),
        "empty bind stays tenant-wide"
    );
    assert!(predicate.contains("project_id"));
    assert!(predicate.contains("= $3"), "a project bind is exact");
    // A project id is opaque, never a UUID: casting the bind would both fail to
    // type-check against the VARCHAR(120) column and reject every human code.
    assert!(
        !predicate.contains("::UUID"),
        "the project bind must not be cast to UUID: {predicate}"
    );
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

/// The per-tenant job budget refuses with the shared typed quota detail
/// (ResourceExhausted + kind QUOTA, not retryable).
///
/// The refusal is asserted directly rather than through a count-then-check
/// helper: that helper was removed because COUNT-then-INSERT is a TOCTOU race,
/// and the budget is enforced inside `guarded_insert_job_sql` — see
/// `insert_job_sql_is_quota_guarded_and_atomic` for the SQL-shape assertion.
#[test]
fn job_quota_gate_refuses_over_budget_with_typed_detail() {
    {
        let err = job_quota_exhausted_status(DEFAULT_MAX_JOBS_PER_TENANT);
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
    assert_eq!(missed_cron_occurrences("0 * * * *", due, now, None), 0);
    // Fired 3h late: the 13:00, 14:00 and 15:00 windows collapse into one event.
    let now = due + ChronoDuration::hours(3);
    assert_eq!(missed_cron_occurrences("0 * * * *", due, now, None), 3);
    // Macro form agrees with its 5-field expansion.
    let now = due + ChronoDuration::days(3);
    assert_eq!(missed_cron_occurrences("@daily", due, now, None), 3);
    // An every-minute job asleep for two days hits the safety cap — the loop
    // is bounded, and the cap reads as "at least this many".
    let now = due + ChronoDuration::days(2);
    assert_eq!(
        missed_cron_occurrences("* * * * *", due, now, None),
        MAX_MISSED_RUNS_COUNTED
    );
    // Invalid expression: fail closed to zero.
    assert_eq!(missed_cron_occurrences("not a cron", due, now, None), 0);
}

/// The create-time job quota MUST be atomic: the rendered INSERT folds the
/// tenant's live count into the SAME statement and persists only while under
/// budget, so — run under the per-tenant advisory lock — concurrent creates at
/// budget-1 can never all land and exceed `UDB_MAX_JOBS_PER_TENANT` (the
/// TOCTOU a separate COUNT-then-INSERT allowed).
#[test]
fn guarded_insert_sql_gates_quota_atomically() {
    let sql = guarded_insert_job_sql(&scheduled_job_model());
    assert!(sql.contains("INSERT INTO"), "must be an INSERT: {sql}");
    assert!(
        sql.contains("SELECT") && !sql.contains("VALUES"),
        "quota gate must fold the count into an INSERT ... SELECT (no plain VALUES): {sql}"
    );
    assert!(
        sql.contains("WHERE (SELECT COUNT(*)"),
        "INSERT must be guarded by a live count subquery: {sql}"
    );
    assert!(
        sql.contains("< $12"),
        "the count must be gated against the budget bind: {sql}"
    );
    assert!(
        sql.contains("IS NULL"),
        "the quota count must exclude soft-deleted jobs: {sql}"
    );
}

/// The atomic (0-rows-inserted) create gate fails with the SAME typed quota
/// detail as the pure `enforce_job_quota` gate — both go through the shared
/// `job_quota_exhausted_status` so the wire contract can't drift between paths.
#[test]
fn job_quota_exhausted_status_matches_pure_gate() {
    let atomic = job_quota_exhausted_status(DEFAULT_MAX_JOBS_PER_TENANT);
    let pure = enforce_job_quota(DEFAULT_MAX_JOBS_PER_TENANT, DEFAULT_MAX_JOBS_PER_TENANT)
        .expect_err("at-budget must refuse");
    assert_eq!(atomic.code(), tonic::Code::ResourceExhausted);
    assert_eq!(atomic.message(), pure.message());
    let detail = decode_detail(&atomic);
    assert_eq!(detail.kind, ErrorKind::Quota as i32);
    assert_eq!(detail.backend, "scheduler");
    assert_eq!(detail.operation, "tenant_scheduled-job_quota");
    assert!(!detail.retryable);
}

/// A valid but sparse quadrennial cron (`0 0 29 2 *`, leap-day) resolves within
/// the extended search horizon instead of returning `None` — which would have
/// rejected it at create or dead-lettered it at fire time. The next occurrence
/// after mid-2026 is 2028-02-29 (a leap year) at 00:00 UTC.
#[test]
fn cron_resolves_quadrennial_leap_day() {
    use chrono::Datelike;
    let base = chrono::DateTime::parse_from_rfc3339("2026-06-26T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let next = next_cron_after("0 0 29 2 *", base).expect("leap-day cron must resolve, not None");
    assert_eq!(next.month(), 2);
    assert_eq!(next.day(), 29);
    assert_eq!(next.to_rfc3339(), "2028-02-29T00:00:00+00:00");
}

/// The fired-event idempotency key is stable per occurrence `(job_id,
/// scheduled_slot)` and independent of wall time, so a redelivered fire dedups;
/// a different occurrence or a different job yields a different key.
#[test]
fn fired_idempotency_key_is_stable_per_occurrence() {
    let slot = chrono::DateTime::parse_from_rfc3339("2026-06-26T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let job = "00000000-0000-0000-0000-000000000009";
    let key = fired_idempotency_key(job, slot);
    assert_eq!(key, format!("{job}:{}", slot.timestamp()));
    // Recomputing for the SAME occurrence is stable (dedup across redelivery).
    assert_eq!(fired_idempotency_key(job, slot), key);
    // A later occurrence of the same job is a different key.
    assert_ne!(
        fired_idempotency_key(job, slot + ChronoDuration::hours(1)),
        key
    );
    // A different job at the same slot is a different key.
    assert_ne!(
        fired_idempotency_key("00000000-0000-0000-0000-00000000000a", slot),
        key
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

/// No timezone ⇒ the historical UTC evaluation, unchanged: `next_cron_after_tz`
/// with `None` is byte-for-byte `next_cron_after`, and missed-run accounting in
/// UTC matches the plain path.
#[test]
fn no_timezone_job_stays_utc() {
    let base = chrono::DateTime::parse_from_rfc3339("2026-06-26T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    for expr in [
        "* * * * *",
        "0 0 * * *",
        "@daily",
        "*/15 9-17 * * 1-5",
        "0 0 29 2 *",
    ] {
        assert_eq!(
            next_cron_after_tz(expr, base, None),
            next_cron_after(expr, base),
            "None-zone must equal the UTC path for {expr}"
        );
    }
    // A payload with no `"timezone"` carries no explicit zone; UTC accounting is
    // the plain-path accounting.
    assert_eq!(timezone_from_payload("{}").unwrap(), None);
    let due = base;
    let now = due + ChronoDuration::hours(3);
    assert_eq!(missed_cron_occurrences("0 * * * *", due, now, None), 3);
}

/// DST spring-forward in `America/New_York`: a daily 09:00-local job advances
/// across the March boundary to the UTC instant dictated by the NEW offset (EDT,
/// UTC-4 → 13:00Z), NOT a fixed pre-DST offset (EST, UTC-5 → would be 14:00Z).
/// Second Sunday of March 2026 is the 8th; clocks spring forward at 02:00 that day.
#[test]
fn dst_spring_forward_advances_by_rule_not_fixed_offset() {
    let ny: chrono_tz::Tz = "America/New_York".parse().unwrap();
    // 2026-03-07 10:00 EST (offset -5) — just after that day's 09:00 fire.
    let after = chrono::DateTime::parse_from_rfc3339("2026-03-07T15:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let next = next_cron_after_in_zone("0 9 * * *", after, ny)
        .expect("daily 09:00 local must resolve across the DST boundary");
    // 09:00 on 2026-03-08 is EDT (UTC-4) ⇒ 13:00Z, proving the offset tracked DST.
    assert_eq!(next.to_rfc3339(), "2026-03-08T13:00:00+00:00");
    assert_ne!(
        next.to_rfc3339(),
        "2026-03-08T14:00:00+00:00",
        "must not apply the stale pre-DST (EST) offset"
    );
    // The same expression with an explicit payload timezone resolves identically.
    let via_payload = effective_tz(r#"{"timezone":"America/New_York"}"#).unwrap();
    assert_eq!(
        next_cron_after_tz("0 9 * * *", after, via_payload),
        Some(next)
    );
}

/// A nonexistent local time (spring-forward gap) resolves to the post-transition
/// instant. `30 2 * * *` on 2026-03-08 falls in the [02:00,03:00) gap; the clock
/// jumps to 03:00 EDT, so the fire lands at the transition instant 07:00Z.
#[test]
fn dst_spring_forward_gap_picks_post_transition_instant() {
    let ny: chrono_tz::Tz = "America/New_York".parse().unwrap();
    let after = chrono::DateTime::parse_from_rfc3339("2026-03-07T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let next = next_cron_after_in_zone("30 2 * * *", after, ny)
        .expect("a gap-time cron must resolve to the post-transition instant");
    // 03:00 EDT == 02:00 EST == 07:00Z, the instant the wall clock jumps to.
    assert_eq!(next.to_rfc3339(), "2026-03-08T07:00:00+00:00");
}

/// An ambiguous local time (fall-back overlap) resolves to the EARLIER instant.
/// DST ends on the first Sunday of November 2026 (the 1st); 01:30 local occurs
/// twice — first as EDT (UTC-4 ⇒ 05:30Z), then as EST (UTC-5 ⇒ 06:30Z). The
/// scheduler picks the earlier one.
#[test]
fn dst_fall_back_overlap_picks_earlier_instant() {
    let ny: chrono_tz::Tz = "America/New_York".parse().unwrap();
    let after = chrono::DateTime::parse_from_rfc3339("2026-11-01T04:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let next = next_cron_after_in_zone("30 1 * * *", after, ny)
        .expect("an overlap-time cron must resolve to the earlier instant");
    assert_eq!(next.to_rfc3339(), "2026-11-01T05:30:00+00:00");
    assert_ne!(
        next.to_rfc3339(),
        "2026-11-01T06:30:00+00:00",
        "must pick the earlier (EDT) instant, not the later (EST) one"
    );
}

/// `timezone_from_payload` reads the opaque `"timezone"` key case-insensitively,
/// treats absent/empty/non-object payloads as "no zone", and rejects a non-empty
/// but invalid IANA name (fail closed).
#[test]
fn timezone_from_payload_parses_absent_valid_and_invalid() {
    // Absent / empty / non-object ⇒ no explicit zone.
    assert_eq!(timezone_from_payload("").unwrap(), None);
    assert_eq!(timezone_from_payload("{}").unwrap(), None);
    assert_eq!(timezone_from_payload(r#"{"other":"x"}"#).unwrap(), None);
    assert_eq!(timezone_from_payload(r#"{"timezone":""}"#).unwrap(), None);
    // Valid name, including a case-insensitive spelling.
    let ny: chrono_tz::Tz = "America/New_York".parse().unwrap();
    assert_eq!(
        timezone_from_payload(r#"{"timezone":"America/New_York"}"#).unwrap(),
        Some(ny)
    );
    assert_eq!(
        timezone_from_payload(r#"{"timezone":"america/new_york"}"#).unwrap(),
        Some(ny)
    );
    // Non-empty invalid name ⇒ fail closed with a typed `timezone` field violation.
    let err = timezone_from_payload(r#"{"timezone":"Mars/Phobos"}"#)
        .expect_err("an unresolvable timezone must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "timezone is not a valid IANA time zone: Mars/Phobos"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "timezone");
}

/// An invalid explicit timezone in the payload is rejected at CREATE, before any
/// pool access — a job is never persisted with an unresolvable zone.
#[tokio::test]
async fn create_job_rejects_invalid_timezone() {
    let svc = SchedulerServiceImpl::new(); // no pool, no channels (admit no-op)
    let mut request = Request::new(scheduler_pb::CreateJobRequest {
        tenant_id: "tenant-a".to_string(),
        name: "nightly".to_string(),
        schedule_type: "CRON".to_string(),
        cron_expression: "@daily".to_string(),
        payload: r#"{"timezone":"Not/AZone"}"#.to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .create_job(request)
        .await
        .expect_err("invalid timezone must be rejected before pool access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "timezone is not a valid IANA time zone: Not/AZone"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations[0].field, "timezone");
}
