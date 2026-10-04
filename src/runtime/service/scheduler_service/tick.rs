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
use super::config::{TOPIC_JOB_DEAD, TOPIC_JOB_FIRED, scheduler_default_tz};
use super::cron::{effective_tz, missed_cron_occurrences, next_cron_after_tz};
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
    let mut failed = 0i64;
    // Per-job isolation: every claimed job runs inside its own SAVEPOINT, so a
    // job whose fire/advance errors (bad row, constraint violation, outbox
    // insert failure) is rolled back ALONE and the rest of the batch still
    // commits. The failing job is then recorded on its own row (attempts +
    // backoff, dead-lettered once `max_attempts` is exhausted) so a poison job
    // cannot wedge the scheduler by failing — and rolling back — every tick.
    for row in &rows {
        tick_savepoint(&mut tx, "SAVEPOINT udb_scheduler_job").await?;
        match fire_claimed_job(&mut tx, &m, &jobs_rel, outbox_rel, row, now).await {
            Ok(()) => {
                tick_savepoint(&mut tx, "RELEASE SAVEPOINT udb_scheduler_job").await?;
                acted += 1;
            }
            Err(err) => {
                tick_savepoint(&mut tx, "ROLLBACK TO SAVEPOINT udb_scheduler_job").await?;
                failed += 1;
                let job_id = row.try_get::<String, _>("job_id").unwrap_or_default();
                tracing::warn!(
                    job_id = %job_id,
                    error = %err.message(),
                    "scheduler tick: job failed; isolated from the batch and recorded"
                );
                record_job_tick_failure(&mut tx, &m, &jobs_rel, outbox_rel, row).await?;
            }
        }
    }

    tx.commit().await.map_err(|err| {
        scheduler_internal_status(
            "scheduler_tick_commit",
            format!("scheduler tick commit failed: {err}"),
        )
    })?;
    if failed > 0 {
        tracing::warn!(
            fired = acted,
            failed,
            "scheduler tick: some jobs failed this pass"
        );
    }
    Ok(acted)
}

/// Run one savepoint-control statement on the tick transaction. A failure here
/// means the transaction itself is unusable, so it aborts the whole pass.
async fn tick_savepoint(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    statement: &'static str,
) -> Result<(), Status> {
    sqlx::query(statement)
        .execute(&mut **tx)
        .await
        .map(|_| ())
        .map_err(|e| {
            scheduler_internal_status(
                "scheduler_tick_savepoint",
                format!("scheduler tick savepoint failed: {e}"),
            )
        })
}

/// How a job that FAILED inside the tick is recorded: `Some(delay_secs)` backs
/// it off (attempts bumped, still ACTIVE), `None` dead-letters it because
/// `max_attempts` is exhausted. Pure — the "a poison job ends DEAD instead of
/// failing every tick forever" contract is unit-tested on it.
pub(crate) fn tick_failure_outcome(
    attempt_count: i32,
    max_attempts: i32,
    backoff_seconds: i32,
) -> (i32, Option<i64>) {
    let new_attempts = attempt_count.saturating_add(1);
    if new_attempts >= max_attempts.max(1) {
        (new_attempts, None)
    } else {
        (
            new_attempts,
            Some(backoff_delay_secs(backoff_seconds, new_attempts)),
        )
    }
}

/// Record a job whose fire failed (its own work was already rolled back to the
/// per-job savepoint): bump `attempt_count` and back `next_fire_at` off, or
/// dead-letter it (`DEAD` + a `job.dead` event, reason `tick_error`) once the
/// attempts are exhausted. Runs in its OWN savepoint; if even this bookkeeping
/// fails it is rolled back and logged, so one job can never abort the batch.
async fn record_job_tick_failure(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    m: &NativeModel,
    jobs_rel: &str,
    outbox_rel: &str,
    row: &sqlx::postgres::PgRow,
) -> Result<(), Status> {
    let text = |c: &str| row.try_get::<String, _>(c).unwrap_or_default();
    let job_id = text("job_id");
    if job_id.is_empty() {
        // Undecodable row: nothing addressable to record against.
        return Ok(());
    }
    let attempt_count: i32 = row.try_get("attempt_count").unwrap_or(0);
    let max_attempts: i32 = row.try_get("max_attempts").unwrap_or(1);
    let backoff_seconds: i32 = row.try_get("backoff_seconds").unwrap_or(1);
    let (new_attempts, delay) = tick_failure_outcome(attempt_count, max_attempts, backoff_seconds);

    tick_savepoint(tx, "SAVEPOINT udb_scheduler_job_failure").await?;
    let recorded: Result<(), Status> = async {
        match delay {
            Some(delay) => {
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
                .execute(&mut **tx)
                .await
                .map_err(|e| {
                    scheduler_internal_status(
                        "scheduler_tick_failure_backoff",
                        format!("scheduler tick failure backoff failed: {e}"),
                    )
                })?;
            }
            None => {
                let tenant_id = text("tenant_id");
                insert_tick_outbox(
                    &mut *tx,
                    outbox_rel,
                    TOPIC_JOB_DEAD,
                    &tenant_id,
                    &text("project_id"),
                    &job_id,
                    dead_payload(
                        &job_id,
                        &tenant_id,
                        &text("name"),
                        new_attempts,
                        "tick_error",
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
                .execute(&mut **tx)
                .await
                .map_err(|e| {
                    scheduler_internal_status(
                        "scheduler_tick_failure_dead",
                        format!("scheduler tick failure dead-letter failed: {e}"),
                    )
                })?;
            }
        }
        Ok(())
    }
    .await;
    match recorded {
        Ok(()) => tick_savepoint(tx, "RELEASE SAVEPOINT udb_scheduler_job_failure").await,
        Err(err) => {
            tick_savepoint(tx, "ROLLBACK TO SAVEPOINT udb_scheduler_job_failure").await?;
            tracing::warn!(
                job_id = %job_id,
                error = %err.message(),
                "scheduler tick: could not record job failure; it will be retried next pass"
            );
            Ok(())
        }
    }
}

/// Fire (or back off / dead-letter) ONE claimed job inside the tick
/// transaction: durably enqueue its fire outbox row and advance its schedule.
/// Called under a per-job savepoint by [`run_scheduler_tick_once`].
async fn fire_claimed_job(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    m: &NativeModel,
    jobs_rel: &str,
    outbox_rel: &str,
    row: &sqlx::postgres::PgRow,
    now: DateTime<Utc>,
) -> Result<(), Status> {
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

    // Per-job timezone from the opaque payload (validated at create). On the
    // unreachable parse-error path fall back to the process default so a single
    // tampered row can never abort the whole tick. `None` ⇒ UTC.
    let tz = effective_tz(&payload).unwrap_or_else(|_| scheduler_default_tz());

    // CRON jobs need a parseable expression to advance. A one-shot always fires
    // once. A CRON whose expression no longer yields a future time is a stuck
    // job: back it off and, after max_attempts, dead-letter it. The advance is
    // computed in the job's zone so a wall-clock cron tracks DST.
    let next_fire = if schedule_type == "CRON" {
        next_cron_after_tz(&cron, now, tz)
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
                tx,
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
            .execute(&mut **tx)
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
            .execute(&mut **tx)
            .await
            .map_err(|e| {
                scheduler_internal_status(
                    "scheduler_tick_backoff_update",
                    format!("scheduler tick backoff update failed: {e}"),
                )
            })?;
        }
        return Ok(());
    }

    // Normal path: FIRE the job, then advance its schedule (reset attempts).
    let payload_json: serde_json::Value =
        serde_json::from_str(&payload).unwrap_or(serde_json::Value::Null);
    // The occurrence THIS fire represents: the STORED due time (next_fire_at),
    // which is stable across at-least-once CDC redeliveries — unlike the
    // wall-clock `fired_at`. Claimed rows always carry it (the claim filters
    // `next_fire_at <= NOW()`); fall back to `now` only defensively.
    let scheduled_slot = due_at_epoch
        .and_then(|epoch| DateTime::<Utc>::from_timestamp(epoch, 0))
        .unwrap_or(now);
    // Missed-run accounting: a late fire collapses the elapsed cron windows
    // into this ONE event; stamp how many were collapsed instead of hiding
    // them. Zero for an on-time fire and for one-shots (which fire once by
    // contract, however late).
    let missed_count = if is_cron {
        missed_cron_occurrences(&cron, scheduled_slot, now, tz)
    } else {
        0
    };
    // Stable per-occurrence idempotency key `(job_id, scheduled_slot)`: a
    // redelivered fire of the SAME occurrence carries the SAME key, giving
    // at-least-once consumers a durable dedup key that wall-clock `fired_at`
    // cannot.
    let idempotency_key = fired_idempotency_key(&job_id, scheduled_slot);
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
        "scheduled_slot": scheduled_slot.to_rfc3339(),
        "idempotency_key": idempotency_key,
        "missed_count": missed_count,
    });
    insert_tick_outbox(
        tx,
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
        .execute(&mut **tx)
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
        .execute(&mut **tx)
        .await
        .map_err(|e| {
            scheduler_internal_status(
                "scheduler_tick_complete",
                format!("scheduler tick complete failed: {e}"),
            )
        })?;
    }
    Ok(())
}

/// Stable per-occurrence idempotency key for a fired event: `job_id` joined to
/// the occurrence's STORED due time (`scheduled_slot`, seconds precision). A
/// redelivered fire of the same occurrence yields the same key, so consumers get
/// a durable dedup key across at-least-once CDC redelivery — which the wall-clock
/// `fired_at` (different on each redelivery attempt) cannot provide.
pub(crate) fn fired_idempotency_key(job_id: &str, scheduled_slot: DateTime<Utc>) -> String {
    format!("{job_id}:{}", scheduled_slot.timestamp())
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
