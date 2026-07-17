//! The leader-elected workflow tick: three sub-passes (forward advance,
//! compensation driver, timeout sweep) in ONE transaction so every durable state
//! change and its outbox rows commit atomically. FIRES EVENTS ONLY — it never
//! runs a payload in-process, exactly like the scheduler tick.

use std::sync::Arc;

use chrono::Utc;
use sqlx::{PgPool, Row};
use tonic::Status;
use uuid::Uuid;

use crate::runtime::canonical_store::SystemStores;
use crate::runtime::canonical_store::system_store::{CompensationStatus, SagaStatus, SagaStore};
use crate::runtime::native_catalog::NativeModel;

use super::super::native_helpers::MAX_LIST_ROWS;
use super::config::{
    COMPENSATE_EMITTED_KEY, STATUS_COMPENSATED, STATUS_COMPENSATING, STATUS_FAILED,
    TOPIC_COMPENSATE_STEP, TOPIC_COMPENSATED, TOPIC_COMPLETED, TOPIC_FAILED, TOPIC_STEP_ADVANCED,
    workflow_step_timeout_secs,
};
use super::errors::workflow_internal_status;
use super::events::insert_tick_outbox;
use super::model::workflow_model;

/// The outbox topic a forward advance emits: the terminal step fires
/// `completed`, every intermediate step fires `step.advanced`. Pure — the tick
/// uses it so the "right event per transition" contract is unit-testable.
pub(crate) fn advance_event_topic(to_completed: bool) -> &'static str {
    if to_completed {
        TOPIC_COMPLETED
    } else {
        TOPIC_STEP_ADVANCED
    }
}

/// Read the exactly-once emission marker from an instance payload. Absent or
/// malformed ⇒ 0 (nothing emitted yet). Pure.
pub(crate) fn compensate_emitted_from_payload(payload: &serde_json::Value) -> i32 {
    payload
        .get(COMPENSATE_EMITTED_KEY)
        .and_then(serde_json::Value::as_i64)
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(0)
        .max(0)
}

/// Best-effort human step name for a `compensate.step` event: the recorded
/// compensation entry's `name`/`step`/`type` string when the compensations array
/// carries one at that index, else the positional `step_<index>`. Names only —
/// the compensation payload CONTENTS (application data, possibly sensitive) are
/// never copied into the event. Pure.
pub(crate) fn compensation_step_name(compensations: &serde_json::Value, index: i32) -> String {
    let named = usize::try_from(index)
        .ok()
        .and_then(|i| compensations.as_array().and_then(|arr| arr.get(i)))
        .and_then(|entry| {
            entry
                .get("name")
                .or_else(|| entry.get("step"))
                .or_else(|| entry.get("type"))
                .and_then(serde_json::Value::as_str)
        })
        .map(str::trim)
        .filter(|s| !s.is_empty());
    match named {
        Some(name) => name.to_string(),
        None => format!("step_{index}"),
    }
}

/// The reverse-order `compensate.step` emission plan for a COMPENSATING instance:
/// one `(step_index, step_name)` per completed forward step (indices
/// `0..completed_steps`), LAST completed step first, skipping the
/// `already_emitted` newest entries a previous pass already enqueued — so a
/// re-tick never re-emits. Pure — unit-tested without Postgres.
pub(crate) fn compensation_steps_to_emit(
    compensations: &serde_json::Value,
    completed_steps: i32,
    already_emitted: i32,
) -> Vec<(i32, String)> {
    let completed = completed_steps.max(0);
    let emitted = already_emitted.clamp(0, completed);
    (0..(completed - emitted))
        .map(|offset| {
            let index = completed - emitted - 1 - offset;
            (index, compensation_step_name(compensations, index))
        })
        .collect()
}

/// The `SELECT ... FOR UPDATE SKIP LOCKED` statement the tick uses to claim DUE
/// RUNNING workflow instances. Built from the manifest model so column identifiers
/// stay single-sourced. Exposed (and unit-tested) so the no-double-advance contract
/// is asserted on the rendered SQL.
pub(crate) fn due_workflows_claim_sql(m: &NativeModel) -> String {
    let rel = m.relation.clone();
    format!(
        "SELECT {workflow_id}::text AS workflow_id, {tenant_id}::text AS tenant_id, \
            COALESCE({project_id}::text, '') AS project_id, {workflow_type} AS workflow_type, \
            COALESCE({payload}::text, '') AS payload, COALESCE({saga_id}::text, '') AS saga_id, \
            {current_step} AS current_step, {total_steps} AS total_steps \
         FROM {rel} \
         WHERE {status} = 'RUNNING' AND {deleted} IS NULL \
           AND {next_run_at} IS NOT NULL AND {next_run_at} <= NOW() \
         ORDER BY {next_run_at} \
         LIMIT $1 \
         FOR UPDATE SKIP LOCKED",
        workflow_id = m.q("workflow_id"),
        tenant_id = m.q("tenant_id"),
        project_id = m.q("project_id"),
        workflow_type = m.q("workflow_type"),
        payload = m.q("payload"),
        saga_id = m.q("saga_id"),
        current_step = m.q("current_step"),
        total_steps = m.q("total_steps"),
        status = m.q("status"),
        deleted = m.q("deleted_at"),
        next_run_at = m.q("next_run_at"),
    )
}

/// The claim statement for the tick's compensation driver (16.3.2): COMPENSATING
/// instances, same `FOR UPDATE SKIP LOCKED` discipline as the forward claim so
/// two leaders can never double-emit compensate events. The cancel path clears
/// `next_run_at`, so the claim is keyed on status alone. Exposed for the SQL-shape
/// unit test.
pub(crate) fn compensating_workflows_claim_sql(m: &NativeModel) -> String {
    let rel = m.relation.clone();
    format!(
        "SELECT {workflow_id}::text AS workflow_id, {tenant_id}::text AS tenant_id, \
            COALESCE({project_id}::text, '') AS project_id, {workflow_type} AS workflow_type, \
            COALESCE({payload}::text, '') AS payload, \
            COALESCE({compensations}::text, '') AS compensations, \
            COALESCE({saga_id}::text, '') AS saga_id, \
            {current_step} AS current_step, {total_steps} AS total_steps \
         FROM {rel} \
         WHERE {status} = '{compensating}' AND {deleted} IS NULL \
         ORDER BY {last_transition_at} \
         LIMIT $1 \
         FOR UPDATE SKIP LOCKED",
        workflow_id = m.q("workflow_id"),
        tenant_id = m.q("tenant_id"),
        project_id = m.q("project_id"),
        workflow_type = m.q("workflow_type"),
        payload = m.q("payload"),
        compensations = m.q("compensations"),
        saga_id = m.q("saga_id"),
        current_step = m.q("current_step"),
        total_steps = m.q("total_steps"),
        status = m.q("status"),
        compensating = STATUS_COMPENSATING,
        deleted = m.q("deleted_at"),
        last_transition_at = m.q("last_transition_at"),
    )
}

/// The claim statement for the tick's timeout sweep (16.3.4): RUNNING instances
/// whose last state transition is older than the step timeout (`$2`, seconds —
/// env-resolved once via [`workflow_step_timeout_secs`]). The table's transition
/// stamp is `last_transition_at` (there is no `updated_at` column). Same
/// `FOR UPDATE SKIP LOCKED` shape as the forward claim; rows the SAME tick
/// transaction just advanced carry a fresh `last_transition_at` and are naturally
/// excluded. Exposed for the SQL-shape unit test.
pub(crate) fn timed_out_workflows_claim_sql(m: &NativeModel) -> String {
    let rel = m.relation.clone();
    format!(
        "SELECT {workflow_id}::text AS workflow_id, {tenant_id}::text AS tenant_id, \
            COALESCE({project_id}::text, '') AS project_id, {workflow_type} AS workflow_type, \
            COALESCE({saga_id}::text, '') AS saga_id, \
            {current_step} AS current_step, {total_steps} AS total_steps \
         FROM {rel} \
         WHERE {status} = 'RUNNING' AND {deleted} IS NULL \
           AND {last_transition_at} < NOW() - make_interval(secs => $2::DOUBLE PRECISION) \
         ORDER BY {last_transition_at} \
         LIMIT $1 \
         FOR UPDATE SKIP LOCKED",
        workflow_id = m.q("workflow_id"),
        tenant_id = m.q("tenant_id"),
        project_id = m.q("project_id"),
        workflow_type = m.q("workflow_type"),
        saga_id = m.q("saga_id"),
        current_step = m.q("current_step"),
        total_steps = m.q("total_steps"),
        status = m.q("status"),
        deleted = m.q("deleted_at"),
        last_transition_at = m.q("last_transition_at"),
    )
}

/// Decode one text column of a claimed tick row (shared by the forward,
/// compensation, and timeout passes).
fn tick_row_text(row: &sqlx::postgres::PgRow, column: &str) -> Result<String, Status> {
    row.try_get::<String, _>(column).map_err(|e| {
        workflow_internal_status(
            "workflow_tick_decode",
            format!("workflow tick decode {column} failed: {e}"),
        )
    })
}

/// Decode one i32 column of a claimed tick row.
fn tick_row_i32(row: &sqlx::postgres::PgRow, column: &str) -> Result<i32, Status> {
    row.try_get::<i32, _>(column).map_err(|e| {
        workflow_internal_status(
            "workflow_tick_decode",
            format!("workflow tick decode {column} failed: {e}"),
        )
    })
}

/// One workflow-tick pass (leader-elected by the caller), three sub-passes in ONE
/// transaction so state changes and their outbox rows always commit atomically:
///
/// 1. **Forward advance** — claims up to `batch_size` DUE RUNNING instances with
///    `FOR UPDATE SKIP LOCKED` and advances `current_step`, enqueuing
///    `step.advanced` (or `completed` on the terminal step). Never double-advances;
///    every transition is at-least-once via the outbox→CDC pipeline.
/// 2. **Compensation driver (16.3.2)** — for COMPENSATING instances, emits one
///    `udb.workflow.compensate.step.v1` event per completed step in REVERSE order
///    (application-driven undo; the data-plane `CompensatorRegistry` cannot undo
///    application workflow steps, so they are never routed through it), stamps the
///    emission marker into the instance payload, then settles
///    COMPENSATING → COMPENSATED and emits `udb.workflow.compensated.v1`.
///    Exactly-once on re-tick: the terminal transition commits with the events,
///    and the payload marker skips any already-emitted steps.
/// 3. **Timeout sweep (16.3.4)** — RUNNING instances whose `last_transition_at`
///    exceeds the step timeout move to FAILED and emit `udb.workflow.failed.v1`.
///    Step ADVANCE itself stays timer-driven (`next_run_at`) this wave — a full
///    step-ack contract needs proto surface (follow-up 16.12.3).
///
/// The tick FIRES EVENTS ONLY; it never runs a payload in-process — it is the
/// workflow counterpart of the scheduler tick and adds no second orchestration
/// loop.
///
/// After commit, the linked saga rows are settled on the saga engine (completed →
/// `Committed`, compensated → `Compensated`, timed-out → `Failed`) — best-effort,
/// cross-store, so it cannot fail the durable transition. Fail closed: a missing
/// outbox relation yields `Ok(0)`.
pub(crate) async fn run_workflow_tick_once(
    pool: &PgPool,
    outbox_relation: Option<&str>,
    stores: Option<Arc<dyn SystemStores>>,
    batch_size: i64,
) -> Result<i64, Status> {
    let Some(outbox_rel) = outbox_relation else {
        tracing::warn!("workflow tick: no outbox relation configured; cannot advance workflows");
        return Ok(0);
    };
    let m = workflow_model();
    let wf_rel = m.relation.clone();
    let claim_sql = due_workflows_claim_sql(&m);
    let batch = batch_size.clamp(1, MAX_LIST_ROWS);

    let mut tx = pool.begin().await.map_err(|err| {
        workflow_internal_status(
            "workflow_tick_begin",
            format!("workflow tick begin failed: {err}"),
        )
    })?;
    let rows = sqlx::query(&claim_sql)
        .bind(batch)
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| {
            workflow_internal_status(
                "workflow_tick_claim",
                format!("workflow tick claim failed: {err}"),
            )
        })?;

    let now = Utc::now();
    let mut acted = 0i64;
    let mut completed_sagas: Vec<Uuid> = Vec::new();
    for row in &rows {
        let workflow_id = tick_row_text(row, "workflow_id")?;
        let tenant_id = tick_row_text(row, "tenant_id")?;
        let project_id = tick_row_text(row, "project_id")?;
        let workflow_type = tick_row_text(row, "workflow_type")?;
        let payload = tick_row_text(row, "payload")?;
        let saga_id = tick_row_text(row, "saga_id")?;
        let current_step = tick_row_i32(row, "current_step")?;
        let total_steps = tick_row_i32(row, "total_steps")?;

        let new_step = current_step.saturating_add(1);
        let completed = new_step >= total_steps;
        let topic = advance_event_topic(completed);
        let payload_json: serde_json::Value =
            serde_json::from_str(&payload).unwrap_or(serde_json::Value::Null);
        let event_payload = serde_json::json!({
            "workflow_id": workflow_id.clone(),
            "tenant_id": tenant_id.clone(),
            "project_id": project_id.clone(),
            "workflow_type": workflow_type.clone(),
            "current_step": new_step,
            "total_steps": total_steps,
            "completed": completed,
            "payload": payload_json,
            "advanced_at": now.to_rfc3339(),
        });
        insert_tick_outbox(
            &mut tx,
            outbox_rel,
            topic,
            &tenant_id,
            &project_id,
            &workflow_id,
            event_payload,
            if completed { "completed" } else { "advanced" },
        )
        .await?;

        if completed {
            sqlx::query(&format!(
                "UPDATE {wf_rel} SET {status} = 'COMPLETED', {current_step} = $2, \
                    {next_run_at} = NULL, {last_transition_at} = NOW() WHERE {workflow_id} = $1::UUID",
                status = m.q("status"),
                current_step = m.q("current_step"),
                next_run_at = m.q("next_run_at"),
                last_transition_at = m.q("last_transition_at"),
                workflow_id = m.q("workflow_id"),
            ))
            .bind(&workflow_id)
            .bind(new_step)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                workflow_internal_status(
                    "workflow_tick_complete_update",
                    format!("workflow tick complete update failed: {e}"),
                )
            })?;
            if let Ok(saga_uuid) = saga_id.parse::<Uuid>() {
                completed_sagas.push(saga_uuid);
            }
        } else {
            sqlx::query(&format!(
                "UPDATE {wf_rel} SET {current_step} = $2, {next_run_at} = NOW(), \
                    {last_transition_at} = NOW() WHERE {workflow_id} = $1::UUID",
                current_step = m.q("current_step"),
                next_run_at = m.q("next_run_at"),
                last_transition_at = m.q("last_transition_at"),
                workflow_id = m.q("workflow_id"),
            ))
            .bind(&workflow_id)
            .bind(new_step)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                workflow_internal_status(
                    "workflow_tick_advance_update",
                    format!("workflow tick advance update failed: {e}"),
                )
            })?;
        }
        acted += 1;
    }

    // ── 16.3.2 — compensation driver: COMPENSATING → COMPENSATED ────────────────
    // Application steps cannot be undone by the data-plane CompensatorRegistry, so
    // undo is application-driven: one `compensate.step` event per completed forward
    // step, in REVERSE order, then the terminal `compensated` transition — all in
    // this same transaction, so the events and the terminal state commit
    // atomically. Event payloads carry ids + step name/index only, never the
    // instance payload or the compensation payload contents (no credentials).
    let mut compensated_sagas: Vec<Uuid> = Vec::new();
    let comp_rows = sqlx::query(&compensating_workflows_claim_sql(&m))
        .bind(batch)
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| {
            workflow_internal_status(
                "workflow_tick_compensate_claim",
                format!("workflow tick compensate claim failed: {err}"),
            )
        })?;
    for row in &comp_rows {
        let workflow_id = tick_row_text(row, "workflow_id")?;
        let tenant_id = tick_row_text(row, "tenant_id")?;
        let project_id = tick_row_text(row, "project_id")?;
        let workflow_type = tick_row_text(row, "workflow_type")?;
        let payload = tick_row_text(row, "payload")?;
        let compensations_text = tick_row_text(row, "compensations")?;
        let saga_id = tick_row_text(row, "saga_id")?;
        let current_step = tick_row_i32(row, "current_step")?;
        let total_steps = tick_row_i32(row, "total_steps")?;

        let mut payload_json: serde_json::Value =
            serde_json::from_str(&payload).unwrap_or_else(|_| serde_json::json!({}));
        if !payload_json.is_object() {
            payload_json = serde_json::json!({});
        }
        let already_emitted = compensate_emitted_from_payload(&payload_json);
        let compensations: serde_json::Value =
            serde_json::from_str(&compensations_text).unwrap_or_else(|_| serde_json::json!([]));
        for (step_index, step_name) in
            compensation_steps_to_emit(&compensations, current_step, already_emitted)
        {
            insert_tick_outbox(
                &mut tx,
                outbox_rel,
                TOPIC_COMPENSATE_STEP,
                &tenant_id,
                &project_id,
                &workflow_id,
                serde_json::json!({
                    "workflow_id": workflow_id.clone(),
                    "tenant_id": tenant_id.clone(),
                    "project_id": project_id.clone(),
                    "workflow_type": workflow_type.clone(),
                    "step_index": step_index,
                    "step_name": step_name,
                    "total_steps": total_steps,
                    "requested_at": now.to_rfc3339(),
                }),
                "compensate_step",
            )
            .await?;
        }
        // Stamp the exactly-once marker and settle the terminal state in the SAME
        // transaction as the events above.
        if let Some(obj) = payload_json.as_object_mut() {
            obj.insert(
                COMPENSATE_EMITTED_KEY.to_string(),
                serde_json::json!(current_step.max(0)),
            );
        }
        sqlx::query(&format!(
            "UPDATE {wf_rel} SET {status} = $2, {payload} = $3::JSONB, {next_run_at} = NULL, \
                {last_transition_at} = NOW() WHERE {workflow_id} = $1::UUID",
            status = m.q("status"),
            payload = m.q("payload"),
            next_run_at = m.q("next_run_at"),
            last_transition_at = m.q("last_transition_at"),
            workflow_id = m.q("workflow_id"),
        ))
        .bind(&workflow_id)
        .bind(STATUS_COMPENSATED)
        .bind(payload_json.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            workflow_internal_status(
                "workflow_tick_compensated_update",
                format!("workflow tick compensated update failed: {e}"),
            )
        })?;
        insert_tick_outbox(
            &mut tx,
            outbox_rel,
            TOPIC_COMPENSATED,
            &tenant_id,
            &project_id,
            &workflow_id,
            serde_json::json!({
                "workflow_id": workflow_id.clone(),
                "tenant_id": tenant_id.clone(),
                "project_id": project_id.clone(),
                "workflow_type": workflow_type.clone(),
                "compensated_steps": current_step.max(0),
                "compensated_at": now.to_rfc3339(),
            }),
            "compensated",
        )
        .await?;
        if let Ok(saga_uuid) = saga_id.parse::<Uuid>() {
            compensated_sagas.push(saga_uuid);
        }
        acted += 1;
    }

    // ── 16.3.4 — step-timeout sweep: RUNNING → FAILED ───────────────────────────
    // Guards against instances that stopped transitioning entirely. Step ADVANCE
    // itself remains event/timer-driven (`next_run_at`) this wave — a full
    // step-ack contract needs proto surface (follow-up 16.12.3).
    let timeout_secs = workflow_step_timeout_secs();
    let mut failed_sagas: Vec<Uuid> = Vec::new();
    let stale_rows = sqlx::query(&timed_out_workflows_claim_sql(&m))
        .bind(batch)
        .bind(timeout_secs)
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| {
            workflow_internal_status(
                "workflow_tick_timeout_claim",
                format!("workflow tick timeout claim failed: {err}"),
            )
        })?;
    for row in &stale_rows {
        let workflow_id = tick_row_text(row, "workflow_id")?;
        let tenant_id = tick_row_text(row, "tenant_id")?;
        let project_id = tick_row_text(row, "project_id")?;
        let workflow_type = tick_row_text(row, "workflow_type")?;
        let saga_id = tick_row_text(row, "saga_id")?;
        let current_step = tick_row_i32(row, "current_step")?;
        let total_steps = tick_row_i32(row, "total_steps")?;

        sqlx::query(&format!(
            "UPDATE {wf_rel} SET {status} = $2, {next_run_at} = NULL, {last_error} = $3, \
                {last_transition_at} = NOW() WHERE {workflow_id} = $1::UUID",
            status = m.q("status"),
            next_run_at = m.q("next_run_at"),
            last_error = m.q("last_error"),
            last_transition_at = m.q("last_transition_at"),
            workflow_id = m.q("workflow_id"),
        ))
        .bind(&workflow_id)
        .bind(STATUS_FAILED)
        .bind(format!(
            "workflow step timed out after {timeout_secs}s without a transition"
        ))
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            workflow_internal_status(
                "workflow_tick_timeout_update",
                format!("workflow tick timeout update failed: {e}"),
            )
        })?;
        insert_tick_outbox(
            &mut tx,
            outbox_rel,
            TOPIC_FAILED,
            &tenant_id,
            &project_id,
            &workflow_id,
            serde_json::json!({
                "workflow_id": workflow_id.clone(),
                "tenant_id": tenant_id.clone(),
                "project_id": project_id.clone(),
                "workflow_type": workflow_type.clone(),
                "current_step": current_step,
                "total_steps": total_steps,
                "reason": "step_timeout",
                "timeout_secs": timeout_secs,
                "failed_at": now.to_rfc3339(),
            }),
            "failed",
        )
        .await?;
        if let Ok(saga_uuid) = saga_id.parse::<Uuid>() {
            failed_sagas.push(saga_uuid);
        }
        acted += 1;
    }

    tx.commit().await.map_err(|err| {
        workflow_internal_status(
            "workflow_tick_commit",
            format!("workflow tick commit failed: {err}"),
        )
    })?;

    // Best-effort, cross-store: settle the linked saga rows on the EXISTING saga
    // engine so they are not left in the `Pending`/`Indeterminate` queues
    // (completed → Committed, compensated → Compensated, timed-out → Failed;
    // Failed is NOT recoverable, so no spurious data-plane compensation fires for
    // steps that never ran). A failure here never undoes the durable transitions
    // committed above.
    if let Some(store) = stores {
        for (saga_ids, status, comp_status) in [
            (
                completed_sagas,
                SagaStatus::Committed,
                CompensationStatus::None,
            ),
            (
                compensated_sagas,
                SagaStatus::Compensated,
                CompensationStatus::Completed,
            ),
            (failed_sagas, SagaStatus::Failed, CompensationStatus::None),
        ] {
            for saga_uuid in saga_ids {
                if let Err(err) =
                    SagaStore::update_saga_status(store.as_ref(), saga_uuid, status, comp_status)
                        .await
                {
                    tracing::debug!(error = %err, saga_id = %saga_uuid, "workflow tick: saga settle failed");
                }
            }
        }
    }
    Ok(acted)
}
