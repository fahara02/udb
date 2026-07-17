//! The percentile-rollup pass for the native `AnalyticsService`: the
//! `percentile_cont` UPDATE over the trailing window of hourly-mean latencies, the
//! scoped runner (`TriggerSnapshot` inline + tenant-scoped), and the unscoped
//! leader export ([`run_analytics_rollup_once`], driven by `run_while_leader` in
//! `service/mod.rs`/`singleton.rs`). Extracted verbatim from the former god file.

use sqlx::PgPool;
use tonic::Status;

use crate::runtime::native_catalog::NativeModel;

use super::config::rollup_window_hours;
use super::errors::analytics_internal_status;
use super::model::pms_model;
use super::store::install_analytics_tenant_scope_sql;

/// The percentile-rollup UPDATE. The online accumulator keeps only per-hour
/// counts and a running mean (`avg_latency_ms`) — raw per-request latencies are
/// never stored — so true request-latency percentiles are IMPOSSIBLE from the
/// stored data. The honest derivable statistic is `percentile_cont` over the
/// trailing window's HOURLY MEAN latencies per (tenant, stage); every windowed
/// row of the group receives the group's p50/p95/p99 of hourly means. All
/// values are bound ($1 tenant, $2 stage, $3 window hours, $4 target hour);
/// `IS NOT DISTINCT FROM` keeps NULL-tenant (system-wide) groups joinable for
/// the unscoped worker pass while `$1` scoping excludes them for tenant callers.
pub(crate) fn analytics_rollup_sql(m: &NativeModel) -> String {
    format!(
        "WITH pct AS ( \
           SELECT {tenant} AS tenant_key, {stage} AS stage_key, \
                  percentile_cont(0.5)  WITHIN GROUP (ORDER BY {avg}) AS p50, \
                  percentile_cont(0.95) WITHIN GROUP (ORDER BY {avg}) AS p95, \
                  percentile_cont(0.99) WITHIN GROUP (ORDER BY {avg}) AS p99 \
           FROM {rel} \
           WHERE {hour} >= date_trunc('hour', now()) - make_interval(hours => $3::int) \
             AND ($1 = '' OR {tenant} = $1) \
             AND ($2 = '' OR {stage} = $2) \
           GROUP BY {tenant}, {stage} \
         ) \
         UPDATE {rel} AS snap SET \
           {p50} = pct.p50, {p95} = pct.p95, {p99} = pct.p99 \
         FROM pct \
         WHERE snap.{tenant} IS NOT DISTINCT FROM pct.tenant_key \
           AND snap.{stage} = pct.stage_key \
           AND snap.{hour} >= date_trunc('hour', now()) - make_interval(hours => $3::int) \
           AND ($4 = '' OR snap.{hour} = $4::timestamptz)",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        stage = m.q("stage_name"),
        hour = m.q("snapshot_hour"),
        avg = m.q("avg_latency_ms"),
        p50 = m.q("p50_latency_ms"),
        p95 = m.q("p95_latency_ms"),
        p99 = m.q("p99_latency_ms"),
    )
}

/// One scoped percentile-rollup pass (see [`analytics_rollup_sql`] for the
/// statistic and its documented limitation). Empty `tenant_id`/`stage_name`/
/// `hour` mean "unscoped" on that axis. Runs in a transaction that installs the
/// tenant RLS GUC first (metering's pattern) when tenant-scoped. Returns the
/// number of snapshot rows genuinely written.
pub(crate) async fn run_analytics_rollup_scoped(
    pool: &PgPool,
    tenant_id: &str,
    stage_name: &str,
    hour: &str,
) -> Result<u64, Status> {
    let m = pms_model();
    let mut tx = pool.begin().await.map_err(|err| {
        analytics_internal_status(
            "analytics_rollup",
            format!("analytics rollup transaction failed: {err}"),
        )
    })?;
    if !tenant_id.trim().is_empty() {
        sqlx::query(install_analytics_tenant_scope_sql())
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                analytics_internal_status(
                    "analytics_rollup",
                    format!("analytics rollup tenant scope failed: {err}"),
                )
            })?;
    }
    let written = sqlx::query(&analytics_rollup_sql(&m))
        .bind(tenant_id)
        .bind(stage_name)
        .bind(rollup_window_hours())
        .bind(hour)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            analytics_internal_status(
                "analytics_rollup",
                format!("analytics rollup failed: {err}"),
            )
        })?
        .rows_affected();
    tx.commit().await.map_err(|err| {
        analytics_internal_status(
            "analytics_rollup",
            format!("analytics rollup transaction commit failed: {err}"),
        )
    })?;
    Ok(written)
}

/// One unscoped rollup pass — the seam the leader wires under
/// `WORKER_ANALYTICS_ROLLUP` via `run_while_leader` (leader-only wiring in
/// `service/mod.rs`/`singleton.rs`, mirroring `run_scheduler_tick_once`).
/// Returns the number of snapshot rows written this pass.
pub(crate) async fn run_analytics_rollup_once(pool: &PgPool) -> Result<u64, Status> {
    run_analytics_rollup_scoped(pool, "", "", "").await
}
