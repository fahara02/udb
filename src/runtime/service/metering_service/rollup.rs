//! The leader-owned usage rollup: aggregate durable UsageEvent rows into closed
//! tenant/method/unit/window buckets and export exactly one
//! `udb.metering.rollup.v1` outbox event per bucket, deduped by deterministic
//! `rollup_id` against both the outbox and the CDC journal. Never deletes raw
//! usage events.

use std::sync::Arc;

use sqlx::{PgPool, Row};

use crate::metrics::MetricsRecorder;

use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_with_context};
use super::calc::{closed_rollup_upper_bound, now_unix};
use super::config::{
    DEFAULT_UNIT, TOPIC_USAGE_ROLLUP, metering_rollup_lookback_seconds,
    metering_rollup_window_seconds,
};
use super::store::usage_event_model;

#[derive(Debug, Clone)]
struct UsageRollup {
    rollup_id: String,
    tenant_id: String,
    method: String,
    unit: String,
    window_start_unix: i64,
    window_end_unix: i64,
    quantity: i64,
    event_count: i64,
}

#[cfg(test)]
pub(crate) fn rollup_id(
    tenant_id: &str,
    method: &str,
    unit: &str,
    window_start_unix: i64,
    window_end_unix: i64,
) -> String {
    format!("{tenant_id}:{method}:{unit}:{window_start_unix}:{window_end_unix}")
}

async fn load_usage_rollups(
    pool: &PgPool,
    outbox_relation: &str,
    journal_relation: &str,
    batch: i64,
    window_seconds: i64,
    lookback_seconds: i64,
) -> Result<Vec<UsageRollup>, String> {
    let usage = usage_event_model();
    let rel = usage.relation.clone();
    let tenant = usage.q("tenant_id");
    let method = usage.q("method");
    let unit = usage.q("unit");
    let quantity = usage.q("quantity");
    let ts = usage.q("occurred_at_unix");
    let window = window_seconds.max(1);
    let upper = closed_rollup_upper_bound(now_unix(), window);
    if upper <= 0 {
        return Ok(Vec::new());
    }
    let lower = upper.saturating_sub(lookback_seconds.max(window));
    let limit = batch.max(1);
    let window_expr = format!("(({ts} / $1::BIGINT) * $1::BIGINT)");
    let rollup_id_expr = "CONCAT(r.tenant_id, ':', r.method, ':', r.unit, ':', r.window_start_unix::TEXT, ':', r.window_end_unix::TEXT)";
    let rows = sqlx::query(&format!(
        "WITH rollups AS ( \
             SELECT \
               {tenant}::TEXT AS tenant_id, \
               {method}::TEXT AS method, \
               COALESCE(NULLIF({unit}::TEXT, ''), $6) AS unit, \
               {window_expr}::BIGINT AS window_start_unix, \
               ({window_expr} + $1::BIGINT)::BIGINT AS window_end_unix, \
               COALESCE(SUM(GREATEST({quantity}, 0)), 0)::BIGINT AS quantity, \
               COUNT(*)::BIGINT AS event_count \
             FROM {rel} \
             WHERE {ts} >= $2 \
               AND {ts} < $3 \
               AND COALESCE({tenant}::TEXT, '') <> '' \
               AND COALESCE({method}::TEXT, '') <> '' \
             GROUP BY {tenant}, {method}, COALESCE(NULLIF({unit}::TEXT, ''), $6), {window_expr} \
         ) \
         SELECT r.*, {rollup_id_expr} AS rollup_id \
         FROM rollups r \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM {outbox_relation} o \
             WHERE o.topic = $4 \
               AND COALESCE(o.payload->>'rollup_id', o.payload->'payload'->>'rollup_id') = {rollup_id_expr} \
         ) \
           AND NOT EXISTS ( \
             SELECT 1 FROM {journal_relation} j \
             WHERE j.topic = $4 \
               AND COALESCE(j.payload->>'rollup_id', j.payload->'payload'->>'rollup_id') = {rollup_id_expr} \
         ) \
         ORDER BY r.window_start_unix ASC, r.tenant_id ASC, r.method ASC, r.unit ASC \
         LIMIT $5"
    ))
    .bind(window)
    .bind(lower)
    .bind(upper)
    .bind(TOPIC_USAGE_ROLLUP)
    .bind(limit)
    .bind(DEFAULT_UNIT)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load metering usage rollups failed: {err}"))?;

    rows.into_iter()
        .map(|row| {
            Ok(UsageRollup {
                rollup_id: row
                    .try_get::<String, _>("rollup_id")
                    .map_err(|err| format!("metering rollup_id decode failed: {err}"))?,
                tenant_id: row
                    .try_get::<String, _>("tenant_id")
                    .map_err(|err| format!("metering rollup tenant_id decode failed: {err}"))?,
                method: row
                    .try_get::<String, _>("method")
                    .map_err(|err| format!("metering rollup method decode failed: {err}"))?,
                unit: row
                    .try_get::<String, _>("unit")
                    .map_err(|err| format!("metering rollup unit decode failed: {err}"))?,
                window_start_unix: row.try_get::<i64, _>("window_start_unix").map_err(|err| {
                    format!("metering rollup window_start_unix decode failed: {err}")
                })?,
                window_end_unix: row.try_get::<i64, _>("window_end_unix").map_err(|err| {
                    format!("metering rollup window_end_unix decode failed: {err}")
                })?,
                quantity: row
                    .try_get::<i64, _>("quantity")
                    .map_err(|err| format!("metering rollup quantity decode failed: {err}"))?,
                event_count: row
                    .try_get::<i64, _>("event_count")
                    .map_err(|err| format!("metering rollup event_count decode failed: {err}"))?,
            })
        })
        .collect()
}

/// Run one leader-owned rollup pass over durable UsageEvent rows. The pass emits
/// one `udb.metering.rollup.v1` outbox record per closed
/// tenant/method/unit/window bucket, deduped by deterministic `rollup_id` against
/// both outbox and CDC journal. It never deletes raw usage events.
pub(crate) async fn run_metering_rollup_once(
    pool: &PgPool,
    outbox_relation: &str,
    journal_relation: &str,
    batch: i64,
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) -> Result<i64, String> {
    let rollups = load_usage_rollups(
        pool,
        outbox_relation,
        journal_relation,
        batch,
        metering_rollup_window_seconds(),
        metering_rollup_lookback_seconds(),
    )
    .await?;
    let mut emitted = 0i64;
    for rollup in rollups {
        let payload = serde_json::json!({
            "rollup_id": &rollup.rollup_id,
            "tenant_id": &rollup.tenant_id,
            "method": &rollup.method,
            "unit": &rollup.unit,
            "window_start_unix": rollup.window_start_unix,
            "window_end_unix": rollup.window_end_unix,
            "quantity": rollup.quantity,
            "event_count": rollup.event_count,
            "window_seconds": rollup.window_end_unix.saturating_sub(rollup.window_start_unix),
        });
        enqueue_outbox_event_with_context(
            pool,
            Some(outbox_relation),
            TOPIC_USAGE_ROLLUP,
            &rollup.rollup_id,
            &rollup.tenant_id,
            "",
            payload,
            NativeEventContext {
                operation: "metering.rollup".to_string(),
                target_resource: rollup.method,
                ..NativeEventContext::default()
            },
            metrics,
        )
        .await;
        emitted = emitted.saturating_add(1);
    }
    Ok(emitted)
}
