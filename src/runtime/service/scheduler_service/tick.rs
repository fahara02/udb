//! The leader-elected scheduler tick: claim DUE jobs with `FOR UPDATE SKIP
//! LOCKED`, then within the SAME transaction durably enqueue a fire (or
//! dead-letter) outbox row and advance the job's schedule. FIRES EVENTS ONLY —
//! it never runs a payload.

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use tonic::Status;
use uuid::Uuid;

use crate::runtime::native_catalog::NativeModel;

use super::super::auth_service::events::{ComplianceEnvelope, build_native_compliance_envelope};
use super::super::native_helpers::MAX_LIST_ROWS;
use super::config::{TOPIC_JOB_DEAD, TOPIC_JOB_FIRED};
use super::cron::{missed_cron_occurrences, next_cron_after};
use super::errors::scheduler_internal_status;
use super::model::scheduled_job_model;

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
            {backoff} AS backoff_seconds, \
            EXTRACT(EPOCH FROM {next_fire_at})::BIGINT AS next_fire_at_epoch \
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
/// HONEST RETRY SEMANTICS: there is NO delivery/execution retry here. The
/// proto's `max_attempts`/`backoff_seconds` are scheduling-side only — they
/// bound the retry of a CRON job whose expression can no longer be advanced
/// (backoff, then dead-letter). Whether a consumer ever executed a fired event
/// is invisible to this tick; ack/nack execution feedback that re-arms
/// `next_fire_at` is follow-up 16.12.5. Missed windows are not replayed
/// one-by-one: a late fire collapses them into ONE event carrying
/// `missed_count` (see [`missed_cron_occurrences`]).
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
        // Stored due time of THIS fire (the claim filters `next_fire_at <= NOW()`,
        // so it is non-null for claimed rows); used for missed-run accounting.
        let due_at_epoch: Option<i64> = row.try_get("next_fire_at_epoch").map_err(|e| {
            scheduler_internal_status(
                "scheduler_tick_decode",
                format!("scheduler tick decode next_fire_at_epoch: {e}"),
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
        // Missed-run accounting: a late fire collapses the elapsed cron windows
        // into this ONE event; stamp how many were collapsed instead of hiding
        // them. Zero for an on-time fire and for one-shots (which fire once by
        // contract, however late).
        let missed_count = if is_cron {
            due_at_epoch
                .and_then(|epoch| DateTime::<Utc>::from_timestamp(epoch, 0))
                .map(|due_at| missed_cron_occurrences(&cron, due_at, now))
                .unwrap_or(0)
        } else {
            0
        };
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
            "missed_count": missed_count,
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
