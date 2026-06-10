//! Native `AnalyticsService` — proto-driven Postgres over the UDB-owned
//! `udb_analytics.{pipeline_metric_snapshots,executor_performance_summaries,
//! reconciliation_analytics_summaries}` tables. Same contract as the other
//! native services: no in-memory store, every identifier resolved from the
//! embedded proto manifest via [`NativeModel`], fail-closed when no PG pool is
//! configured.
//!
//! There is no separate raw-observation table in the proto contract, so
//! `RecordPipelineMetric` performs **online aggregation**: it upserts directly
//! into the hourly snapshot keyed by the table's real unique key
//! `(snapshot_hour, stage_name)`, maintaining running counts, a running mean
//! latency, error rate, and throughput. Latency percentiles (p50/p95/p99)
//! cannot be derived online from a single observation, so they are populated by
//! a separate batch/percentile job (left untouched here). Because aggregation is
//! online, `TriggerSnapshot` reports the current hour's snapshot rows rather than
//! re-aggregating a raw stream.

use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};

use crate::proto::udb::core::analytics::entity::v1 as ana_entity_pb;
use crate::proto::udb::core::analytics::services::v1 as ana_pb;
use crate::proto::udb::core::analytics::services::v1::analytics_service_server::AnalyticsService;
use crate::runtime::native_catalog::{NativeModel, native_model};

pub use crate::proto::udb::core::analytics::services::v1::analytics_service_server::AnalyticsServiceServer;

use super::DataBrokerService;
use super::native_helpers::{native_page_response, native_page_window};

const PMS_MSG: &str = "udb.core.analytics.entity.v1.PipelineMetricSnapshot";
const EPS_MSG: &str = "udb.core.analytics.entity.v1.ExecutorPerformanceSummary";
const RAS_MSG: &str = "udb.core.analytics.entity.v1.ReconciliationAnalyticsSummary";

pub struct AnalyticsServiceImpl {
    pg_pool: Option<PgPool>,
}

impl AnalyticsServiceImpl {
    pub fn new() -> Self {
        Self { pg_pool: None }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "analytics service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }
}

impl Default for AnalyticsServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

fn pms_model() -> NativeModel {
    native_model(
        PMS_MSG,
        &[
            "snapshot_id",
            "snapshot_hour",
            "stage_name",
            "tenant_id",
            "total_requests",
            "successful",
            "failed",
            "p50_latency_ms",
            "p95_latency_ms",
            "p99_latency_ms",
            "avg_latency_ms",
            "error_rate",
            "throughput_rps",
            "recorded_at",
        ],
    )
}

fn eps_model() -> NativeModel {
    native_model(
        EPS_MSG,
        &[
            "summary_id",
            "summary_date",
            "executor_identity",
            "workload_kind",
            "total_dispatches",
            "successful_results",
            "timeout_count",
            "error_count",
            "avg_execution_ms",
            "p99_execution_ms",
            "avg_confidence",
            "success_rate",
            "avg_capacity_utilisation",
            "recorded_at",
        ],
    )
}

fn ras_model() -> NativeModel {
    native_model(
        RAS_MSG,
        &[
            "summary_id",
            "summary_date",
            "total_reconciliations",
            "exact_matches",
            "partial_conflicts",
            "hard_conflicts",
            "low_confidence_flagged",
            "avg_reconciliation_ms",
            "resolution_rate",
            "avg_record_confidence",
            "recorded_at",
        ],
    )
}

fn ts(seconds: i64) -> Option<prost_types::Timestamp> {
    if seconds <= 0 {
        None
    } else {
        Some(prost_types::Timestamp { seconds, nanos: 0 })
    }
}

/// Shared projection for a pipeline snapshot row (aliased to proto field names).
fn pms_projection(m: &NativeModel) -> String {
    format!(
        "{id}, {hour}, {stage}, {tenant}, {total}, {succ}, {failed}, \
         COALESCE({p50},0) AS p50_latency_ms, COALESCE({p95},0) AS p95_latency_ms, \
         COALESCE({p99},0) AS p99_latency_ms, COALESCE({avg},0) AS avg_latency_ms, \
         COALESCE({err},0) AS error_rate, COALESCE({rps},0) AS throughput_rps, {recorded}",
        id = m.text_as("snapshot_id", "snapshot_id"),
        hour = m.timestamp_unix_as("snapshot_hour", "snapshot_hour"),
        stage = m.text_or_empty_as("stage_name", "stage_name"),
        tenant = m.text_or_empty_as("tenant_id", "tenant_id"),
        total = m.select_as("total_requests", "total_requests"),
        succ = m.select_as("successful", "successful"),
        failed = m.select_as("failed", "failed"),
        p50 = m.q("p50_latency_ms"),
        p95 = m.q("p95_latency_ms"),
        p99 = m.q("p99_latency_ms"),
        avg = m.q("avg_latency_ms"),
        err = m.q("error_rate"),
        rps = m.q("throughput_rps"),
        recorded = m.timestamp_unix_as("recorded_at", "recorded_at"),
    )
}

fn pms_from_row(row: &sqlx::postgres::PgRow) -> ana_entity_pb::PipelineMetricSnapshot {
    ana_entity_pb::PipelineMetricSnapshot {
        snapshot_id: row.try_get("snapshot_id").unwrap_or_default(),
        snapshot_hour: ts(row.try_get("snapshot_hour").unwrap_or(0)),
        stage_name: row.try_get("stage_name").unwrap_or_default(),
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        total_requests: row.try_get("total_requests").unwrap_or(0),
        successful: row.try_get("successful").unwrap_or(0),
        failed: row.try_get("failed").unwrap_or(0),
        p50_latency_ms: row.try_get("p50_latency_ms").unwrap_or(0.0),
        p95_latency_ms: row.try_get("p95_latency_ms").unwrap_or(0.0),
        p99_latency_ms: row.try_get("p99_latency_ms").unwrap_or(0.0),
        avg_latency_ms: row.try_get("avg_latency_ms").unwrap_or(0.0),
        error_rate: row.try_get("error_rate").unwrap_or(0.0),
        throughput_rps: row.try_get("throughput_rps").unwrap_or(0.0),
        recorded_at: ts(row.try_get("recorded_at").unwrap_or(0)),
    }
}

fn eps_from_row(row: &sqlx::postgres::PgRow) -> ana_entity_pb::ExecutorPerformanceSummary {
    ana_entity_pb::ExecutorPerformanceSummary {
        summary_id: row.try_get("summary_id").unwrap_or_default(),
        summary_date: ts(row.try_get("summary_date").unwrap_or(0)),
        executor_identity: row.try_get("executor_identity").unwrap_or_default(),
        workload_kind: row.try_get("workload_kind").unwrap_or_default(),
        total_dispatches: row.try_get("total_dispatches").unwrap_or(0),
        successful_results: row.try_get("successful_results").unwrap_or(0),
        timeout_count: row.try_get("timeout_count").unwrap_or(0),
        error_count: row.try_get("error_count").unwrap_or(0),
        avg_execution_ms: row.try_get("avg_execution_ms").unwrap_or(0.0),
        p99_execution_ms: row.try_get("p99_execution_ms").unwrap_or(0.0),
        avg_confidence: row.try_get("avg_confidence").unwrap_or(0.0),
        success_rate: row.try_get("success_rate").unwrap_or(0.0),
        avg_capacity_utilisation: row.try_get("avg_capacity_utilisation").unwrap_or(0.0),
        recorded_at: ts(row.try_get("recorded_at").unwrap_or(0)),
    }
}

fn ras_from_row(row: &sqlx::postgres::PgRow) -> ana_entity_pb::ReconciliationAnalyticsSummary {
    ana_entity_pb::ReconciliationAnalyticsSummary {
        summary_id: row.try_get("summary_id").unwrap_or_default(),
        summary_date: ts(row.try_get("summary_date").unwrap_or(0)),
        total_reconciliations: row.try_get("total_reconciliations").unwrap_or(0),
        exact_matches: row.try_get("exact_matches").unwrap_or(0),
        partial_conflicts: row.try_get("partial_conflicts").unwrap_or(0),
        hard_conflicts: row.try_get("hard_conflicts").unwrap_or(0),
        low_confidence_flagged: row.try_get("low_confidence_flagged").unwrap_or(0),
        avg_reconciliation_ms: row.try_get("avg_reconciliation_ms").unwrap_or(0.0),
        resolution_rate: row.try_get("resolution_rate").unwrap_or(0.0),
        avg_record_confidence: row.try_get("avg_record_confidence").unwrap_or(0.0),
        recorded_at: ts(row.try_get("recorded_at").unwrap_or(0)),
    }
}

#[tonic::async_trait]
impl AnalyticsService for AnalyticsServiceImpl {
    async fn record_pipeline_metric(
        &self,
        request: Request<ana_pb::RecordPipelineMetricRequest>,
    ) -> Result<Response<ana_pb::RecordPipelineMetricResponse>, Status> {
        let req = request.into_inner();
        if req.stage_name.trim().is_empty() {
            return Err(Status::invalid_argument("stage_name is required"));
        }
        let pool = self.require_pool()?;
        let m = pms_model();
        let rel = m.relation.clone();
        let existing_total = format!("existing.{}", m.q("total_requests"));
        let existing_successful = format!("existing.{}", m.q("successful"));
        let existing_failed = format!("existing.{}", m.q("failed"));
        let existing_avg = format!("existing.{}", m.q("avg_latency_ms"));
        let (succ, fail) = if req.is_success {
            (1i64, 0i64)
        } else {
            (0i64, 1i64)
        };
        // Online aggregation into the hourly bucket. ON CONFLICT targets the real
        // unique key (snapshot_hour, stage_name); the running mean latency and
        // derived error_rate/throughput are recomputed from the post-increment
        // total. Percentiles are out of scope for online aggregation.
        // In ON CONFLICT DO UPDATE, bare column refs on the RHS are the EXISTING
        // row's pre-update values (all SET RHS see the old row), and EXCLUDED.* is
        // the would-be-inserted row — so the running mean / rate / rps below are
        // computed consistently against the old totals.
        sqlx::query(&format!(
            "INSERT INTO {rel} AS existing \
               ({hour}, {stage}, {tenant}, {total}, {succ_c}, {fail_c}, {avg}, {err}, {rps}) \
             VALUES (date_trunc('hour', now()), $1, $2, 1, $3, $4, $5, \
                     $4::float8 / 1, 1::float8 / 3600) \
             ON CONFLICT ({hour}, {stage}, {tenant}) DO UPDATE SET \
               {total} = {existing_total} + 1, \
               {succ_c} = {existing_successful} + EXCLUDED.{succ_c}, \
               {fail_c} = {existing_failed} + EXCLUDED.{fail_c}, \
               {avg} = (COALESCE({existing_avg},0) * {existing_total} + $5) / ({existing_total} + 1), \
               {err} = ({existing_failed} + EXCLUDED.{fail_c})::float8 / ({existing_total} + 1), \
               {rps} = ({existing_total} + 1)::float8 / 3600",
            rel = rel,
            hour = m.q("snapshot_hour"),
            stage = m.q("stage_name"),
            tenant = m.q("tenant_id"),
            total = m.q("total_requests"),
            succ_c = m.q("successful"),
            fail_c = m.q("failed"),
            avg = m.q("avg_latency_ms"),
            err = m.q("error_rate"),
            rps = m.q("throughput_rps"),
            existing_total = existing_total,
            existing_successful = existing_successful,
            existing_failed = existing_failed,
            existing_avg = existing_avg,
        ))
        .bind(&req.stage_name)
        .bind(&req.tenant_id)
        .bind(succ)
        .bind(fail)
        .bind(req.latency_ms)
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("record pipeline metric failed: {err}")))?;
        Ok(Response::new(ana_pb::RecordPipelineMetricResponse {
            accepted: true,
        }))
    }

    async fn get_pipeline_summary(
        &self,
        request: Request<ana_pb::GetPipelineSummaryRequest>,
    ) -> Result<Response<ana_pb::GetPipelineSummaryResponse>, Status> {
        let req = request.into_inner();
        let pool = self.require_pool()?;
        let m = pms_model();
        let rel = m.relation.clone();
        let projection = pms_projection(&m);
        let page = native_page_window(req.page.as_ref(), 50);
        let rows = sqlx::query(&format!(
            "SELECT {projection}, COUNT(*) OVER() AS total_count FROM {rel} \
             WHERE ($1 = '' OR {stage} = $1) \
               AND ($2 = '' OR {tenant} = $2) \
               AND ($3 = '' OR {hour} >= $3::timestamptz) \
               AND ($4 = '' OR {hour} <= $4::timestamptz) \
             ORDER BY {hour} DESC LIMIT $5 OFFSET $6",
            stage = m.q("stage_name"),
            tenant = m.q("tenant_id"),
            hour = m.q("snapshot_hour"),
        ))
        .bind(&req.stage_name)
        .bind(&req.tenant_id)
        .bind(&req.hour_from)
        .bind(&req.hour_to)
        .bind(page.limit_i64())
        .bind(page.offset_i64())
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("get pipeline summary failed: {err}")))?;
        let total: i64 = rows
            .first()
            .and_then(|r| r.try_get("total_count").ok())
            .unwrap_or(0);
        let snapshots = rows.iter().map(pms_from_row).collect();
        Ok(Response::new(ana_pb::GetPipelineSummaryResponse {
            snapshots,
            page: Some(native_page_response(req.page.as_ref(), total, 50)),
        }))
    }

    async fn get_executor_performance(
        &self,
        request: Request<ana_pb::GetExecutorPerformanceRequest>,
    ) -> Result<Response<ana_pb::GetExecutorPerformanceResponse>, Status> {
        let req = request.into_inner();
        let pool = self.require_pool()?;
        let m = eps_model();
        let rel = m.relation.clone();
        let projection = format!(
            "{id}, {date}, {exec}, {workload}, {dispatches}, {succ}, {timeouts}, {errors}, \
             COALESCE({avg_exec},0) AS avg_execution_ms, COALESCE({p99},0) AS p99_execution_ms, \
             COALESCE({avg_conf},0) AS avg_confidence, COALESCE({rate},0) AS success_rate, \
             COALESCE({cap},0) AS avg_capacity_utilisation, {recorded}",
            id = m.text_as("summary_id", "summary_id"),
            date = m.timestamp_unix_as("summary_date", "summary_date"),
            exec = m.text_or_empty_as("executor_identity", "executor_identity"),
            workload = m.text_or_empty_as("workload_kind", "workload_kind"),
            dispatches = m.select_as("total_dispatches", "total_dispatches"),
            succ = m.select_as("successful_results", "successful_results"),
            timeouts = m.select_as("timeout_count", "timeout_count"),
            errors = m.select_as("error_count", "error_count"),
            avg_exec = m.q("avg_execution_ms"),
            p99 = m.q("p99_execution_ms"),
            avg_conf = m.q("avg_confidence"),
            rate = m.q("success_rate"),
            cap = m.q("avg_capacity_utilisation"),
            recorded = m.timestamp_unix_as("recorded_at", "recorded_at"),
        );
        let rows = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} \
             WHERE ($1 = '' OR {exec} = $1) \
               AND ($2 = '' OR {workload} = $2) \
               AND ($3 = '' OR {date} >= $3::date) \
               AND ($4 = '' OR {date} <= $4::date) \
             ORDER BY {date} DESC, {exec}",
            exec = m.q("executor_identity"),
            workload = m.q("workload_kind"),
            date = m.q("summary_date"),
        ))
        .bind(&req.executor_identity)
        .bind(&req.workload_kind)
        .bind(&req.date_from)
        .bind(&req.date_to)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("get executor performance failed: {err}")))?;
        let summaries = rows.iter().map(eps_from_row).collect();
        Ok(Response::new(ana_pb::GetExecutorPerformanceResponse {
            summaries,
        }))
    }

    async fn get_reconciliation_analytics(
        &self,
        request: Request<ana_pb::GetReconciliationAnalyticsRequest>,
    ) -> Result<Response<ana_pb::GetReconciliationAnalyticsResponse>, Status> {
        let req = request.into_inner();
        let pool = self.require_pool()?;
        let m = ras_model();
        let rel = m.relation.clone();
        let projection = format!(
            "{id}, {date}, {total}, {exact}, {partial}, {hard}, {low}, \
             COALESCE({avg_ms},0) AS avg_reconciliation_ms, \
             COALESCE({rate},0) AS resolution_rate, \
             COALESCE({conf},0) AS avg_record_confidence, {recorded}",
            id = m.text_as("summary_id", "summary_id"),
            date = m.timestamp_unix_as("summary_date", "summary_date"),
            total = m.select_as("total_reconciliations", "total_reconciliations"),
            exact = m.select_as("exact_matches", "exact_matches"),
            partial = m.select_as("partial_conflicts", "partial_conflicts"),
            hard = m.select_as("hard_conflicts", "hard_conflicts"),
            low = m.select_as("low_confidence_flagged", "low_confidence_flagged"),
            avg_ms = m.q("avg_reconciliation_ms"),
            rate = m.q("resolution_rate"),
            conf = m.q("avg_record_confidence"),
            recorded = m.timestamp_unix_as("recorded_at", "recorded_at"),
        );
        let rows = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} \
             WHERE ($1 = '' OR {date} >= $1::date) AND ($2 = '' OR {date} <= $2::date) \
             ORDER BY {date} DESC",
            date = m.q("summary_date"),
        ))
        .bind(&req.date_from)
        .bind(&req.date_to)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("get reconciliation analytics failed: {err}")))?;
        let summaries: Vec<_> = rows.iter().map(ras_from_row).collect();
        // Overall resolution rate = Σ exact_matches / Σ total_reconciliations;
        // avg_reconciliation_ms = mean of the per-day averages.
        let total_recon: i64 = summaries.iter().map(|s| s.total_reconciliations).sum();
        let total_exact: i64 = summaries.iter().map(|s| s.exact_matches).sum();
        let overall_resolution_rate = if total_recon > 0 {
            total_exact as f64 / total_recon as f64
        } else {
            0.0
        };
        let avg_reconciliation_ms = if summaries.is_empty() {
            0.0
        } else {
            summaries
                .iter()
                .map(|s| s.avg_reconciliation_ms)
                .sum::<f64>()
                / summaries.len() as f64
        };
        Ok(Response::new(ana_pb::GetReconciliationAnalyticsResponse {
            summaries,
            overall_resolution_rate,
            avg_reconciliation_ms,
        }))
    }

    async fn get_throughput(
        &self,
        request: Request<ana_pb::GetThroughputRequest>,
    ) -> Result<Response<ana_pb::GetThroughputResponse>, Status> {
        let req = request.into_inner();
        let pool = self.require_pool()?;
        let m = pms_model();
        let rel = m.relation.clone();
        let row = sqlx::query(&format!(
            "SELECT COALESCE(AVG({rps}),0) AS avg_rps, \
                    COALESCE(MAX({rps}),0) AS peak_rps, \
                    COALESCE(SUM({total}),0)::bigint AS total_requests, \
                    COALESCE(SUM({succ}),0)::bigint AS total_successful \
             FROM {rel} \
             WHERE ($1 = '' OR {tenant} = $1) \
               AND ($2 = '' OR {hour} >= $2::timestamptz) \
               AND ($3 = '' OR {hour} <= $3::timestamptz)",
            rps = m.q("throughput_rps"),
            total = m.q("total_requests"),
            succ = m.q("successful"),
            tenant = m.q("tenant_id"),
            hour = m.q("snapshot_hour"),
        ))
        .bind(&req.tenant_id)
        .bind(&req.hour_from)
        .bind(&req.hour_to)
        .fetch_one(pool)
        .await
        .map_err(|err| Status::internal(format!("get throughput failed: {err}")))?;
        let total_requests: i64 = row.try_get("total_requests").unwrap_or(0);
        let total_successful: i64 = row.try_get("total_successful").unwrap_or(0);
        let overall_success_rate = if total_requests > 0 {
            total_successful as f64 / total_requests as f64
        } else {
            0.0
        };
        Ok(Response::new(ana_pb::GetThroughputResponse {
            avg_rps: row.try_get("avg_rps").unwrap_or(0.0),
            peak_rps: row.try_get("peak_rps").unwrap_or(0.0),
            total_requests,
            overall_success_rate,
        }))
    }

    async fn get_sla_compliance(
        &self,
        request: Request<ana_pb::GetSlaComplianceRequest>,
    ) -> Result<Response<ana_pb::GetSlaComplianceResponse>, Status> {
        let req = request.into_inner();
        let pool = self.require_pool()?;
        let m = pms_model();
        let rel = m.relation.clone();
        let rows = sqlx::query(&format!(
            "SELECT {stage}::TEXT AS stage_name, \
                    to_char({hour} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:00:00\"Z\"') AS period, \
                    COALESCE({p99},0) AS p99_latency_ms, \
                    COALESCE({err},0) AS error_rate \
             FROM {rel} \
             WHERE ($1 = '' OR {stage} = $1) \
               AND ($2 = '' OR {hour} >= $2::date) \
               AND ($3 = '' OR {hour} < ($3::date + 1)) \
             ORDER BY {hour} DESC",
            stage = m.q("stage_name"),
            hour = m.q("snapshot_hour"),
            p99 = m.q("p99_latency_ms"),
            err = m.q("error_rate"),
        ))
        .bind(&req.stage_name)
        .bind(&req.date_from)
        .bind(&req.date_to)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("get sla compliance failed: {err}")))?;

        // A zero threshold means "no threshold configured" → treat as always met
        // so an unconfigured SLA doesn't report spurious violations.
        let p99_threshold = req.p99_threshold_ms;
        let err_threshold = req.error_rate_threshold;
        let mut entries = Vec::with_capacity(rows.len());
        let (mut p99_met, mut err_met) = (0i64, 0i64);
        for row in &rows {
            let p99: f64 = row.try_get("p99_latency_ms").unwrap_or(0.0);
            let error_rate: f64 = row.try_get("error_rate").unwrap_or(0.0);
            let p99_sla_met = p99_threshold <= 0.0 || p99 <= p99_threshold;
            let error_rate_sla_met = err_threshold <= 0.0 || error_rate <= err_threshold;
            if p99_sla_met {
                p99_met += 1;
            }
            if error_rate_sla_met {
                err_met += 1;
            }
            entries.push(ana_pb::SlaComplianceEntry {
                stage_name: row.try_get("stage_name").unwrap_or_default(),
                period: row.try_get("period").unwrap_or_default(),
                p99_latency_ms: p99,
                error_rate,
                p99_sla_met,
                error_rate_sla_met,
            });
        }
        let n = entries.len() as f64;
        Ok(Response::new(ana_pb::GetSlaComplianceResponse {
            overall_p99_compliance_rate: if n > 0.0 { p99_met as f64 / n } else { 0.0 },
            overall_error_rate_compliance_rate: if n > 0.0 { err_met as f64 / n } else { 0.0 },
            entries,
        }))
    }

    async fn trigger_snapshot(
        &self,
        request: Request<ana_pb::TriggerSnapshotRequest>,
    ) -> Result<Response<ana_pb::TriggerSnapshotResponse>, Status> {
        let req = request.into_inner();
        let pool = self.require_pool()?;
        let m = pms_model();
        let rel = m.relation.clone();
        // Aggregation is online (see module docs), so snapshots are already
        // current. Report how many snapshot rows exist for the targeted hour and
        // stage filter. `hour` empty defaults to the CURRENT hour — that is where
        // `record_pipeline_metric` writes, so a trigger right after recording sees
        // the rows it just produced.
        let row = sqlx::query(&format!(
            "SELECT COUNT(*)::bigint AS n FROM {rel} \
             WHERE ($1 = '' OR {stage} = $1) \
               AND {hour} = COALESCE(NULLIF($2,'')::timestamptz, date_trunc('hour', now()))",
            stage = m.q("stage_name"),
            hour = m.q("snapshot_hour"),
        ))
        .bind(&req.stage_name)
        .bind(&req.hour)
        .fetch_one(pool)
        .await
        .map_err(|err| Status::internal(format!("trigger snapshot failed: {err}")))?;
        let n: i64 = row.try_get("n").unwrap_or(0);
        Ok(Response::new(ana_pb::TriggerSnapshotResponse {
            snapshots_written: n as i32,
        }))
    }
}

impl DataBrokerService {
    /// Build the native `AnalyticsService`, wired to the broker's Postgres pool.
    pub(crate) fn build_analytics_service(&self) -> AnalyticsServiceImpl {
        let runtime = self.runtime.load_full();
        let pg_pool = runtime.pg_pool().ok().cloned();
        AnalyticsServiceImpl::new().with_postgres(pg_pool)
    }
}
