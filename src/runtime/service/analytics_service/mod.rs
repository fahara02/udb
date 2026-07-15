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
//! `(snapshot_hour, stage_name, tenant_id)`, maintaining running counts, a
//! running mean latency, error rate, and throughput. Latency percentiles
//! (p50/p95/p99) cannot be derived online from a single observation; they are
//! filled in by the rollup pass ([`run_analytics_rollup_once`], the seam the
//! leader-elected worker drives), which `TriggerSnapshot` also runs inline
//! (tenant-scoped) so its `snapshots_written` reports rows genuinely written.
//! The accumulator keeps only per-hour counts and a running mean — raw
//! per-request latencies are never stored — so the pass computes
//! `percentile_cont` over the trailing window of HOURLY MEAN latencies per
//! (tenant, stage). That is an honest, documented approximation (percentiles of
//! hourly means), NOT per-request latency percentiles; deriving true request
//! percentiles would require a raw-observation store the contract doesn't have.

use std::sync::{Arc, OnceLock};

use chrono::{DateTime, NaiveDate, Utc};
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};

use crate::ir::{
    AggregateExpr, AggregateFunc, ComparisonOp, LogicalAggregate, LogicalFilter, LogicalPagination,
    LogicalProjection, LogicalRead, LogicalSort, LogicalValue, NullOrder, SortDirection,
};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::analytics::entity::v1 as ana_entity_pb;
use crate::proto::udb::core::analytics::services::v1 as ana_pb;
use crate::proto::udb::core::analytics::services::v1::analytics_service_server::AnalyticsService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, OperationChannel};
use crate::runtime::native_catalog::{NativeModel, native_model};

pub use crate::proto::udb::core::analytics::services::v1::analytics_service_server::AnalyticsServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    NativeEventContext, admit_on as native_admit_on, enqueue_outbox_event_with_context,
    metadata_project_id, metadata_tenant_id, native_page_response, native_page_window,
    native_service_context,
};

const PMS_MSG: &str = "udb.core.analytics.entity.v1.PipelineMetricSnapshot";
const EPS_MSG: &str = "udb.core.analytics.entity.v1.ExecutorPerformanceSummary";
const RAS_MSG: &str = "udb.core.analytics.entity.v1.ReconciliationAnalyticsSummary";

/// Outbox topic + event types exactly as the proto `method_event_contract`
/// declares them on the two mutating RPCs (`analytics_service.proto`); the
/// contract this service must actually fulfil.
const TOPIC_ANALYTICS_EVENTS: &str = "analytics.events";
const EVENT_TYPE_PIPELINE_METRIC_RECORDED: &str = "analytics.RecordPipelineMetric";
const EVENT_TYPE_SNAPSHOT_TRIGGERED: &str = "analytics.TriggerSnapshot";

pub struct AnalyticsServiceImpl {
    pg_pool: Option<PgPool>,
    runtime: Option<Arc<DataBrokerRuntime>>,
    outbox_relation: Option<String>,
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
}

fn analytics_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "analytics",
        operation,
        capability_required,
        message,
    )
}

fn analytics_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("analytics", operation, message)
}

impl AnalyticsServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            analytics_capability_status(
                "postgres_store",
                "postgres_store",
                "analytics service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }

    fn runtime(&self) -> Option<&DataBrokerRuntime> {
        self.runtime.as_deref()
    }

    /// Emit one proto-declared `analytics.events` outbox event (best-effort; the
    /// durable write already succeeded). Mirrors `config_service::emit_flag_changed`:
    /// partition key = tenant (the contract's `partition_key_field`), actor = the
    /// verified claim subject (auto-filled from the method-security principal when
    /// no claim context is installed).
    async fn emit_analytics_event(
        &self,
        event_type: &'static str,
        tenant_id: &str,
        project_id: &str,
        stage_name: &str,
        payload: serde_json::Value,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_ANALYTICS_EVENTS,
            tenant_id,
            tenant_id,
            project_id,
            payload,
            NativeEventContext {
                operation: event_type.to_string(),
                target_resource: stage_name.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
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

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

fn analytics_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

/// Pure admin gate for the two system-global reads. `executor_performance_summaries`
/// and `reconciliation_analytics_summaries` have NO tenant column (system-global
/// operational tables), so a WHERE predicate cannot scope them — only a genuine
/// cross-tenant / platform-admin identity may read them. Reuses the ONE existing
/// admin classifier ([`VerifiedClaimContext::is_cross_tenant_admin`], the same set
/// the apikey lane and the native RLS `app.platform_admin` escape hatch honor).
fn platform_admin_guard(
    claim: &crate::runtime::service::method_security::VerifiedClaimContext,
    operation: &'static str,
) -> Result<(), Status> {
    if claim.is_cross_tenant_admin() {
        Ok(())
    } else {
        Err(crate::runtime::executor_utils::policy_status_with_code(
            tonic::Code::PermissionDenied,
            operation,
            "analytics_platform_admin_required",
            "system-global analytics require a platform/cross-tenant admin identity",
        ))
    }
}

/// Enforce [`platform_admin_guard`] against the request's verified claim. Mirrors
/// the D1/D2 handler doctrine in `method_security`: enforcement is skipped ONLY
/// when no claim context is installed at all (in-process / trusted internal
/// caller — the tower gate ALWAYS installs one for transport requests), so an
/// over-the-wire caller can never dodge the gate.
fn require_platform_admin(operation: &'static str) -> Result<(), Status> {
    if !crate::runtime::service::method_security::claim_context_present() {
        return Ok(());
    }
    let claim = crate::runtime::service::method_security::current_claim_context();
    platform_admin_guard(&claim, operation)
}

/// Pure payload builder for the `analytics.events` outbox events (unit-tested
/// SDK-visible shape). `snapshots_written` is present only on TriggerSnapshot.
fn analytics_event_payload(
    event_type: &str,
    stage_name: &str,
    tenant_id: &str,
    snapshots_written: Option<i64>,
) -> serde_json::Value {
    let mut payload = serde_json::json!({
        "event_type": event_type,
        "stage_name": stage_name,
        "tenant_id": tenant_id,
    });
    if let (Some(written), Some(map)) = (snapshots_written, payload.as_object_mut()) {
        map.insert("snapshots_written".to_string(), serde_json::json!(written));
    }
    payload
}

fn maybe_string_filter(field: &str, value: &str) -> Option<LogicalFilter> {
    if value.trim().is_empty() {
        None
    } else {
        Some(LogicalFilter::Comparison {
            field: field.to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(value.trim().to_string()),
        })
    }
}

fn projection(fields: &[&str]) -> LogicalProjection {
    LogicalProjection::fields(fields.iter().map(|field| (*field).to_string()))
}

fn read_filter(filters: Vec<Option<LogicalFilter>>) -> Option<LogicalFilter> {
    let filters = filters.into_iter().flatten().collect::<Vec<_>>();
    if filters.is_empty() {
        None
    } else {
        Some(LogicalFilter::And(filters))
    }
}

fn pipeline_summary_filter(req: &ana_pb::GetPipelineSummaryRequest) -> Option<LogicalFilter> {
    read_filter(vec![
        maybe_string_filter("stage_name", &req.stage_name),
        maybe_string_filter("tenant_id", &req.tenant_id),
    ])
}

fn pipeline_summary_read(filter: Option<LogicalFilter>, offset: u64, limit: u32) -> LogicalRead {
    LogicalRead {
        message_type: PMS_MSG.to_string(),
        filter,
        projection: Some(projection(&[
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
        ])),
        sort: vec![LogicalSort {
            field: "snapshot_hour".to_string(),
            direction: SortDirection::Desc,
            nulls: NullOrder::Default,
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

fn executor_performance_read(req: &ana_pb::GetExecutorPerformanceRequest) -> LogicalRead {
    LogicalRead {
        message_type: EPS_MSG.to_string(),
        filter: read_filter(vec![
            maybe_string_filter("executor_identity", &req.executor_identity),
            maybe_string_filter("workload_kind", &req.workload_kind),
        ]),
        projection: Some(projection(&[
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
        ])),
        sort: vec![LogicalSort {
            field: "summary_date".to_string(),
            direction: SortDirection::Desc,
            nulls: NullOrder::Default,
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(MAX_ANALYTICS_READ_ROWS)),
    }
}

fn reconciliation_analytics_read() -> LogicalRead {
    LogicalRead {
        message_type: RAS_MSG.to_string(),
        filter: None,
        projection: Some(projection(&[
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
        ])),
        sort: vec![LogicalSort {
            field: "summary_date".to_string(),
            direction: SortDirection::Desc,
            nulls: NullOrder::Default,
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(MAX_ANALYTICS_READ_ROWS)),
    }
}

// B12: retained for the typed-aggregate throughput path. `get_throughput` currently
// serves via raw SQL because this typed `LogicalAggregate` returns 0 rows at runtime
// (see bug_report.md B12); restore the call site once that defect is root-caused.
#[allow(dead_code)]
fn throughput_aggregate(req: &ana_pb::GetThroughputRequest) -> LogicalAggregate {
    LogicalAggregate {
        message_type: PMS_MSG.to_string(),
        filter: read_filter(vec![maybe_string_filter("tenant_id", &req.tenant_id)]),
        group_by: Vec::new(),
        aggregates: vec![
            AggregateExpr {
                func: AggregateFunc::Avg,
                field: "throughput_rps".to_string(),
                alias: "avg_rps".to_string(),
            },
            AggregateExpr {
                func: AggregateFunc::Max,
                field: "throughput_rps".to_string(),
                alias: "peak_rps".to_string(),
            },
            AggregateExpr {
                func: AggregateFunc::Sum,
                field: "total_requests".to_string(),
                alias: "total_requests".to_string(),
            },
            AggregateExpr {
                func: AggregateFunc::Sum,
                field: "successful".to_string(),
                alias: "total_successful".to_string(),
            },
        ],
        having: None,
        sort: Vec::new(),
        pagination: None,
    }
}

/// SLA read over the PMS table. `tenant_id` is the VERIFIED claim tenant: the
/// PMS `tenant_id` column is nullable (`NULL` = system-wide aggregate row), so a
/// tenant caller gets an explicit equality predicate — which also excludes the
/// NULL system-wide rows — and never sees another tenant's snapshots.
fn sla_compliance_read(req: &ana_pb::GetSlaComplianceRequest, tenant_id: &str) -> LogicalRead {
    LogicalRead {
        message_type: PMS_MSG.to_string(),
        filter: read_filter(vec![
            maybe_string_filter("stage_name", &req.stage_name),
            maybe_string_filter("tenant_id", tenant_id),
        ]),
        projection: Some(projection(&[
            "snapshot_hour",
            "stage_name",
            "p99_latency_ms",
            "error_rate",
        ])),
        sort: vec![LogicalSort {
            field: "snapshot_hour".to_string(),
            direction: SortDirection::Desc,
            nulls: NullOrder::Default,
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(MAX_ANALYTICS_READ_ROWS)),
    }
}

/// Raw-SQL twin of [`sla_compliance_read`] for date-windowed reads. All values
/// are bound ($1 stage, $2/$3 date window, $4 verified claim tenant, $5 row
/// cap); `{tenant} = $4` both isolates tenants and excludes the NULL-tenant
/// system-wide aggregate rows for tenant callers.
fn sla_compliance_sql(m: &NativeModel) -> String {
    format!(
        "SELECT {stage}::TEXT AS stage_name, \
                to_char({hour} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:00:00\"Z\"') AS period, \
                COALESCE({p99},0) AS p99_latency_ms, \
                COALESCE({err},0) AS error_rate \
         FROM {rel} \
         WHERE ($1 = '' OR {stage} = $1) \
           AND ($2 = '' OR {hour} >= $2::date) \
           AND ($3 = '' OR {hour} < ($3::date + 1)) \
           AND ($4 = '' OR {tenant} = $4) \
         ORDER BY {hour} DESC LIMIT $5",
        rel = m.relation,
        stage = m.q("stage_name"),
        hour = m.q("snapshot_hour"),
        p99 = m.q("p99_latency_ms"),
        err = m.q("error_rate"),
        tenant = m.q("tenant_id"),
    )
}

fn timestamp_hour_period(ts: Option<&prost_types::Timestamp>) -> String {
    ts.and_then(|ts| DateTime::<Utc>::from_timestamp(ts.seconds, ts.nanos.max(0) as u32))
        .map(|dt| dt.format("%Y-%m-%dT%H:00:00Z").to_string())
        .unwrap_or_default()
}

const MAX_ANALYTICS_READ_ROWS: u32 = 10_000;

/// Denominator for the online `throughput_rps` derivation (requests per hourly
/// bucket).
const SECONDS_PER_HOUR: i64 = 3_600;

/// Default page size for pipeline-summary listings.
const PIPELINE_SUMMARY_PAGE_SIZE: i32 = 50;

/// Trailing window (hours) the percentile rollup pass scans. Overridable once
/// via `UDB_ANALYTICS_ROLLUP_WINDOW_HOURS` (OnceLock — never a per-request env
/// read).
const DEFAULT_ROLLUP_WINDOW_HOURS: i32 = 24;

fn rollup_window_hours() -> i32 {
    static HOURS: OnceLock<i32> = OnceLock::new();
    *HOURS.get_or_init(|| {
        std::env::var("UDB_ANALYTICS_ROLLUP_WINDOW_HOURS")
            .ok()
            .and_then(|v| v.trim().parse::<i32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_ROLLUP_WINDOW_HOURS)
    })
}

/// Install the per-transaction tenant GUC the native RLS policies key on, so
/// raw-SQL reads over the RLS-enabled PMS table are row-scoped even on a raw
/// pooled connection (mirrors `metering_service`'s read-path pattern:
/// `set_config(..., is_local = true)` inside the read transaction, in addition
/// to the explicit bound WHERE predicate).
fn install_analytics_tenant_scope_sql() -> &'static str {
    "SELECT set_config('app.current_tenant_id', $1, true)"
}

fn row_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

fn json_string(row: &serde_json::Map<String, serde_json::Value>, field: &str) -> String {
    row.get(field)
        .and_then(|value| match value {
            serde_json::Value::String(value) => Some(value.clone()),
            serde_json::Value::Number(value) => Some(value.to_string()),
            serde_json::Value::Bool(value) => Some(value.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

fn json_i64(row: &serde_json::Map<String, serde_json::Value>, field: &str) -> i64 {
    row.get(field)
        .and_then(|value| match value {
            serde_json::Value::Number(value) => value.as_i64(),
            serde_json::Value::String(value) => value.parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or_default()
}

fn json_f64(row: &serde_json::Map<String, serde_json::Value>, field: &str) -> f64 {
    row.get(field)
        .and_then(|value| match value {
            serde_json::Value::Number(value) => value.as_f64(),
            serde_json::Value::String(value) => value.parse::<f64>().ok(),
            _ => None,
        })
        .unwrap_or_default()
}

fn json_ts(
    row: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Option<prost_types::Timestamp> {
    let value = row.get(field)?;
    if let Some(seconds) = value.as_i64() {
        return ts(seconds);
    }
    let raw = value.as_str()?.trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(seconds) = raw.parse::<i64>() {
        return ts(seconds);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Some(prost_types::Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        });
    }
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        if let Some(dt) = date.and_hms_opt(0, 0, 0) {
            return Some(prost_types::Timestamp {
                seconds: dt.and_utc().timestamp(),
                nanos: 0,
            });
        }
    }
    None
}

fn eps_from_json(row: &serde_json::Value) -> ana_entity_pb::ExecutorPerformanceSummary {
    let row = row_object(row);
    ana_entity_pb::ExecutorPerformanceSummary {
        summary_id: json_string(row, "summary_id"),
        summary_date: json_ts(row, "summary_date"),
        executor_identity: json_string(row, "executor_identity"),
        workload_kind: json_string(row, "workload_kind"),
        total_dispatches: json_i64(row, "total_dispatches"),
        successful_results: json_i64(row, "successful_results"),
        timeout_count: json_i64(row, "timeout_count"),
        error_count: json_i64(row, "error_count"),
        avg_execution_ms: json_f64(row, "avg_execution_ms"),
        p99_execution_ms: json_f64(row, "p99_execution_ms"),
        avg_confidence: json_f64(row, "avg_confidence"),
        success_rate: json_f64(row, "success_rate"),
        avg_capacity_utilisation: json_f64(row, "avg_capacity_utilisation"),
        recorded_at: json_ts(row, "recorded_at"),
    }
}

fn ras_from_json(row: &serde_json::Value) -> ana_entity_pb::ReconciliationAnalyticsSummary {
    let row = row_object(row);
    ana_entity_pb::ReconciliationAnalyticsSummary {
        summary_id: json_string(row, "summary_id"),
        summary_date: json_ts(row, "summary_date"),
        total_reconciliations: json_i64(row, "total_reconciliations"),
        exact_matches: json_i64(row, "exact_matches"),
        partial_conflicts: json_i64(row, "partial_conflicts"),
        hard_conflicts: json_i64(row, "hard_conflicts"),
        low_confidence_flagged: json_i64(row, "low_confidence_flagged"),
        avg_reconciliation_ms: json_f64(row, "avg_reconciliation_ms"),
        resolution_rate: json_f64(row, "resolution_rate"),
        avg_record_confidence: json_f64(row, "avg_record_confidence"),
        recorded_at: json_ts(row, "recorded_at"),
    }
}

fn pms_from_json(row: &serde_json::Value) -> ana_entity_pb::PipelineMetricSnapshot {
    let row = row_object(row);
    ana_entity_pb::PipelineMetricSnapshot {
        snapshot_id: json_string(row, "snapshot_id"),
        snapshot_hour: json_ts(row, "snapshot_hour"),
        stage_name: json_string(row, "stage_name"),
        tenant_id: json_string(row, "tenant_id"),
        total_requests: json_i64(row, "total_requests"),
        successful: json_i64(row, "successful"),
        failed: json_i64(row, "failed"),
        p50_latency_ms: json_f64(row, "p50_latency_ms"),
        p95_latency_ms: json_f64(row, "p95_latency_ms"),
        p99_latency_ms: json_f64(row, "p99_latency_ms"),
        avg_latency_ms: json_f64(row, "avg_latency_ms"),
        error_rate: json_f64(row, "error_rate"),
        throughput_rps: json_f64(row, "throughput_rps"),
        recorded_at: json_ts(row, "recorded_at"),
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
        let metadata = request.metadata().clone();
        let mut req = request.into_inner();
        // B12: persist under the canonical tenant — the VALIDATED bearer/header
        // tenant (a UUID), not the raw request-body string (e.g. a human "code").
        // GetThroughput/GetPipelineSummary filter by this same canonical tenant, so
        // a divergent body value here would store rows the reads can never sum.
        if let Some(canonical) = metadata_tenant_id(&metadata) {
            req.tenant_id = canonical;
        }
        if req.stage_name.trim().is_empty() {
            return Err(analytics_required_field(
                "stage_name",
                "must be a non-empty pipeline stage name",
                "stage_name is required",
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "analytics",
            OperationChannel::Write,
            &req.tenant_id,
            None,
        )
        .await?;
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
        // unique key (snapshot_hour, stage_name, tenant_id); the running mean
        // latency and derived error_rate/throughput are recomputed from the
        // post-increment total. Percentiles are out of scope for online
        // aggregation — the rollup pass fills them (see module docs).
        // In ON CONFLICT DO UPDATE, bare column refs on the RHS are the EXISTING
        // row's pre-update values (all SET RHS see the old row), and EXCLUDED.* is
        // the would-be-inserted row — so the running mean / rate / rps below are
        // computed consistently against the old totals.
        sqlx::query(&format!(
            "INSERT INTO {rel} AS existing \
               ({hour}, {stage}, {tenant}, {total}, {succ_c}, {fail_c}, {avg}, {err}, {rps}) \
             VALUES (date_trunc('hour', now()), $1, $2, 1, $3, $4, $5, \
                     $4::float8 / 1, 1::float8 / {secs_per_hour}) \
             ON CONFLICT ({hour}, {stage}, {tenant}) DO UPDATE SET \
               {total} = {existing_total} + 1, \
               {succ_c} = {existing_successful} + EXCLUDED.{succ_c}, \
               {fail_c} = {existing_failed} + EXCLUDED.{fail_c}, \
               {avg} = (COALESCE({existing_avg},0) * {existing_total} + $5) / ({existing_total} + 1), \
               {err} = ({existing_failed} + EXCLUDED.{fail_c})::float8 / ({existing_total} + 1), \
               {rps} = ({existing_total} + 1)::float8 / {secs_per_hour}",
            rel = rel,
            secs_per_hour = SECONDS_PER_HOUR,
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
        .map_err(|err| {
            crate::runtime::executor_utils::sqlx_error_to_status(
                "record pipeline metric failed",
                &err,
            )
        })?;
        // Fulfil the proto-declared `method_event_contract`: one outbox event per
        // durable mutation (topic `analytics.events`, at-least-once via CDC).
        self.emit_analytics_event(
            EVENT_TYPE_PIPELINE_METRIC_RECORDED,
            &req.tenant_id,
            &metadata_project_id(&metadata).unwrap_or_default(),
            &req.stage_name,
            analytics_event_payload(
                EVENT_TYPE_PIPELINE_METRIC_RECORDED,
                &req.stage_name,
                &req.tenant_id,
                None,
            ),
        )
        .await;
        Ok(Response::new(ana_pb::RecordPipelineMetricResponse {
            accepted: true,
        }))
    }

    async fn get_pipeline_summary(
        &self,
        request: Request<ana_pb::GetPipelineSummaryRequest>,
    ) -> Result<Response<ana_pb::GetPipelineSummaryResponse>, Status> {
        let metadata = request.metadata().clone();
        let mut req = request.into_inner();
        // B12 sibling: read under the SAME canonical (validated bearer/header)
        // tenant RecordPipelineMetric persists under, so a tenant caller can
        // neither read foreign rows nor the NULL-tenant system-wide aggregates
        // by leaving the body tenant empty.
        if let Some(canonical) = metadata_tenant_id(&metadata) {
            req.tenant_id = canonical;
        }
        let page = native_page_window(req.page.as_ref(), PIPELINE_SUMMARY_PAGE_SIZE);
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "analytics",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        if req.hour_from.trim().is_empty()
            && req.hour_to.trim().is_empty()
            && let Some(runtime) = self.runtime()
        {
            let context = native_service_context(&metadata, &req.tenant_id, "");
            let filter = pipeline_summary_filter(&req);
            let total = runtime
                .native_entity_count_for_service("analytics", &context, PMS_MSG, filter.clone())
                .await?;
            let rows = runtime
                .native_entity_read_for_service(
                    "analytics",
                    &context,
                    pipeline_summary_read(filter, page.offset as u64, page.limit as u32),
                )
                .await?;
            let snapshots = rows.iter().map(pms_from_json).collect();
            return Ok(Response::new(ana_pb::GetPipelineSummaryResponse {
                snapshots,
                page: Some(native_page_response(
                    req.page.as_ref(),
                    total,
                    PIPELINE_SUMMARY_PAGE_SIZE,
                )),
            }));
        }
        let pool = self.require_pool()?;
        let m = pms_model();
        let rel = m.relation.clone();
        let projection = pms_projection(&m);
        // Transitional: this response includes COUNT(*) OVER() pagination metadata
        // plus timestamp casts. Native read/count handles the no-window path
        // above; timestamp windows remain on the capability-gated Postgres path
        // until dispatch preserves typed timestamp params across every backend.
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
        .map_err(|err| {
            analytics_internal_status(
                "get_pipeline_summary",
                format!("get pipeline summary failed: {err}"),
            )
        })?;
        let total: i64 = rows
            .first()
            .and_then(|r| r.try_get("total_count").ok())
            .unwrap_or(0);
        let snapshots = rows.iter().map(pms_from_row).collect();
        Ok(Response::new(ana_pb::GetPipelineSummaryResponse {
            snapshots,
            page: Some(native_page_response(
                req.page.as_ref(),
                total,
                PIPELINE_SUMMARY_PAGE_SIZE,
            )),
        }))
    }

    async fn get_executor_performance(
        &self,
        request: Request<ana_pb::GetExecutorPerformanceRequest>,
    ) -> Result<Response<ana_pb::GetExecutorPerformanceResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // `executor_performance_summaries` is a system-global operational table
        // (no tenant column) — gate it behind the platform-admin identity.
        require_platform_admin("get_executor_performance")?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "analytics",
            OperationChannel::Read,
            &metadata_tenant_id(&metadata).unwrap_or_default(),
            None,
        )
        .await?;
        if req.date_from.trim().is_empty()
            && req.date_to.trim().is_empty()
            && let Some(runtime) = self.runtime()
        {
            let context = native_service_context(&metadata, "", "");
            let rows = runtime
                .native_entity_read_for_service(
                    "analytics",
                    &context,
                    executor_performance_read(&req),
                )
                .await?;
            let summaries = rows.iter().map(eps_from_json).collect();
            return Ok(Response::new(ana_pb::GetExecutorPerformanceResponse {
                summaries,
            }));
        }
        let pool = self.require_pool()?;
        let m = eps_model();
        let rel = m.relation.clone();
        // Transitional: date-range filters currently rely on Postgres date casts;
        // typed reads handle the simple entity list above.
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
             ORDER BY {date} DESC, {exec} LIMIT $5",
            exec = m.q("executor_identity"),
            workload = m.q("workload_kind"),
            date = m.q("summary_date"),
        ))
        .bind(&req.executor_identity)
        .bind(&req.workload_kind)
        .bind(&req.date_from)
        .bind(&req.date_to)
        .bind(i64::from(MAX_ANALYTICS_READ_ROWS))
        .fetch_all(pool)
        .await
        .map_err(|err| {
            analytics_internal_status(
                "get_executor_performance",
                format!("get executor performance failed: {err}"),
            )
        })?;
        let summaries = rows.iter().map(eps_from_row).collect();
        Ok(Response::new(ana_pb::GetExecutorPerformanceResponse {
            summaries,
        }))
    }

    async fn get_reconciliation_analytics(
        &self,
        request: Request<ana_pb::GetReconciliationAnalyticsRequest>,
    ) -> Result<Response<ana_pb::GetReconciliationAnalyticsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // `reconciliation_analytics_summaries` is a system-global operational
        // table (no tenant column) — gate it behind the platform-admin identity.
        require_platform_admin("get_reconciliation_analytics")?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "analytics",
            OperationChannel::Read,
            &metadata_tenant_id(&metadata).unwrap_or_default(),
            None,
        )
        .await?;
        if req.date_from.trim().is_empty()
            && req.date_to.trim().is_empty()
            && let Some(runtime) = self.runtime()
        {
            let context = native_service_context(&metadata, "", "");
            let rows = runtime
                .native_entity_read_for_service(
                    "analytics",
                    &context,
                    reconciliation_analytics_read(),
                )
                .await?;
            let summaries: Vec<_> = rows.iter().map(ras_from_json).collect();
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
            return Ok(Response::new(ana_pb::GetReconciliationAnalyticsResponse {
                summaries,
                overall_resolution_rate,
                avg_reconciliation_ms,
            }));
        }
        let pool = self.require_pool()?;
        let m = ras_model();
        let rel = m.relation.clone();
        // Transitional: date-range filters currently rely on Postgres date casts;
        // typed reads handle the simple entity list above.
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
             ORDER BY {date} DESC LIMIT $3",
            date = m.q("summary_date"),
        ))
        .bind(&req.date_from)
        .bind(&req.date_to)
        .bind(i64::from(MAX_ANALYTICS_READ_ROWS))
        .fetch_all(pool)
        .await
        .map_err(|err| {
            analytics_internal_status(
                "get_reconciliation_analytics",
                format!("get reconciliation analytics failed: {err}"),
            )
        })?;
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
        let metadata = request.metadata().clone();
        let mut req = request.into_inner();
        // B12: filter by the canonical tenant — the VALIDATED bearer/header tenant
        // (a UUID), the SAME value RecordPipelineMetric persists under. The raw
        // request-body `tenant_id` may be a human code (e.g. "sdk-live") that matches
        // zero stored rows, so SUM(total_requests) came back 0. Binding to the claim
        // tenant here makes the read scope match the write.
        if let Some(canonical) = metadata_tenant_id(&metadata) {
            req.tenant_id = canonical;
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "analytics",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        // B12: the typed `LogicalAggregate` dispatch returns 0 here at runtime even
        // though the identical raw SQL (and the typed READ over the same table+tenant
        // via GetPipelineSummary) returns the real sum — a defect in the typed
        // aggregate execution path that is NOT a tenant mismatch (RecordPipelineMetric,
        // GetPipelineSummary and this handler all key on the same canonical tenant).
        // Until that typed-aggregate bug is root-caused, serve the throughput SUM via
        // the SAME proven raw-SQL aggregate the windowed branch already uses below
        // (`COALESCE(SUM(..),0)::bigint`), so throughput reports real numbers. See
        // bug_report.md B12.
        let pool = self.require_pool()?;
        let m = pms_model();
        let rel = m.relation.clone();
        // Transitional: timestamp-window casts stay on the configured Postgres
        // native store; the no-window aggregate path uses LogicalAggregate above.
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
        .map_err(|err| {
            analytics_internal_status("get_throughput", format!("get throughput failed: {err}"))
        })?;
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
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let p99_threshold = req.p99_threshold_ms;
        let err_threshold = req.error_rate_threshold;
        // The request body carries no tenant field; scope to the VERIFIED
        // claim/header tenant so a tenant caller never sees foreign-tenant or
        // NULL-tenant (system-wide aggregate) PMS rows.
        let tenant_id = metadata_tenant_id(&metadata).unwrap_or_default();
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "analytics",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        if req.date_from.trim().is_empty()
            && req.date_to.trim().is_empty()
            && let Some(runtime) = self.runtime()
        {
            let context = native_service_context(&metadata, &tenant_id, "");
            let rows = runtime
                .native_entity_read_for_service(
                    "analytics",
                    &context,
                    sla_compliance_read(&req, &tenant_id),
                )
                .await?;
            let mut entries = Vec::with_capacity(rows.len());
            let (mut p99_met, mut err_met) = (0i64, 0i64);
            for row in &rows {
                let snapshot = pms_from_json(row);
                let p99_sla_met = p99_threshold <= 0.0 || snapshot.p99_latency_ms <= p99_threshold;
                let error_rate_sla_met =
                    err_threshold <= 0.0 || snapshot.error_rate <= err_threshold;
                if p99_sla_met {
                    p99_met += 1;
                }
                if error_rate_sla_met {
                    err_met += 1;
                }
                entries.push(ana_pb::SlaComplianceEntry {
                    stage_name: snapshot.stage_name,
                    period: timestamp_hour_period(snapshot.snapshot_hour.as_ref()),
                    p99_latency_ms: snapshot.p99_latency_ms,
                    error_rate: snapshot.error_rate,
                    p99_sla_met,
                    error_rate_sla_met,
                });
            }
            let n = entries.len() as f64;
            return Ok(Response::new(ana_pb::GetSlaComplianceResponse {
                overall_p99_compliance_rate: if n > 0.0 { p99_met as f64 / n } else { 0.0 },
                overall_error_rate_compliance_rate: if n > 0.0 { err_met as f64 / n } else { 0.0 },
                entries,
            }));
        }
        let pool = self.require_pool()?;
        let m = pms_model();
        // Transitional: SLA periods need backend date formatting plus service-side
        // compliance rollup for date windows. The no-window row read uses native
        // entity dispatch above. The read runs in a transaction that first installs
        // the tenant RLS GUC (metering's read-path pattern) so the raw pooled
        // connection is row-scoped in addition to the explicit bound predicate.
        let mut tx = pool.begin().await.map_err(|err| {
            analytics_internal_status(
                "get_sla_compliance",
                format!("get sla compliance transaction failed: {err}"),
            )
        })?;
        if !tenant_id.trim().is_empty() {
            sqlx::query(install_analytics_tenant_scope_sql())
                .bind(&tenant_id)
                .execute(&mut *tx)
                .await
                .map_err(|err| {
                    analytics_internal_status(
                        "get_sla_compliance",
                        format!("get sla compliance tenant scope failed: {err}"),
                    )
                })?;
        }
        let rows = sqlx::query(&sla_compliance_sql(&m))
            .bind(&req.stage_name)
            .bind(&req.date_from)
            .bind(&req.date_to)
            .bind(&tenant_id)
            .bind(i64::from(MAX_ANALYTICS_READ_ROWS))
            .fetch_all(&mut *tx)
            .await
            .map_err(|err| {
                analytics_internal_status(
                    "get_sla_compliance",
                    format!("get sla compliance failed: {err}"),
                )
            })?;
        tx.commit().await.map_err(|err| {
            analytics_internal_status(
                "get_sla_compliance",
                format!("get sla compliance transaction commit failed: {err}"),
            )
        })?;

        // A zero threshold means "no threshold configured" → treat as always met
        // so an unconfigured SLA doesn't report spurious violations.
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
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // Scope the pass to the VERIFIED claim/header tenant: a tenant caller
        // rolls up only its own PMS rows (never the NULL-tenant system-wide
        // aggregates or foreign tenants); the leader-elected worker runs the
        // same pass unscoped.
        let tenant_id = metadata_tenant_id(&metadata).unwrap_or_default();
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "analytics",
            OperationChannel::Write,
            &tenant_id,
            None,
        )
        .await?;
        let pool = self.require_pool()?;
        // Counts/means are aggregated online (see module docs); the percentile
        // columns are what a snapshot pass genuinely has to write. Run the SAME
        // rollup the worker seam runs and report rows actually updated — not a
        // COUNT(*) of pre-existing rows.
        let written = run_analytics_rollup_scoped(pool, &tenant_id, &req.stage_name, &req.hour)
            .await
            .map_err(|status| {
                // Re-tag the rollup failure under this RPC's operation so the
                // served TriggerSnapshot path carries a `trigger_snapshot` typed
                // internal detail (the worker seam keeps `analytics_rollup`).
                analytics_internal_status(
                    "trigger_snapshot",
                    format!("trigger snapshot rollup failed: {}", status.message()),
                )
            })?;
        // Fulfil the proto-declared `method_event_contract` for this mutation.
        self.emit_analytics_event(
            EVENT_TYPE_SNAPSHOT_TRIGGERED,
            &tenant_id,
            &metadata_project_id(&metadata).unwrap_or_default(),
            &req.stage_name,
            analytics_event_payload(
                EVENT_TYPE_SNAPSHOT_TRIGGERED,
                &req.stage_name,
                &tenant_id,
                Some(written as i64),
            ),
        )
        .await;
        Ok(Response::new(ana_pb::TriggerSnapshotResponse {
            snapshots_written: written as i32,
        }))
    }
}

/// The percentile-rollup UPDATE. The online accumulator keeps only per-hour
/// counts and a running mean (`avg_latency_ms`) — raw per-request latencies are
/// never stored — so true request-latency percentiles are IMPOSSIBLE from the
/// stored data. The honest derivable statistic is `percentile_cont` over the
/// trailing window's HOURLY MEAN latencies per (tenant, stage); every windowed
/// row of the group receives the group's p50/p95/p99 of hourly means. All
/// values are bound ($1 tenant, $2 stage, $3 window hours, $4 target hour);
/// `IS NOT DISTINCT FROM` keeps NULL-tenant (system-wide) groups joinable for
/// the unscoped worker pass while `$1` scoping excludes them for tenant callers.
fn analytics_rollup_sql(m: &NativeModel) -> String {
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
async fn run_analytics_rollup_scoped(
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

#[cfg(test)]
mod analytics_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "analytics");
        assert_eq!(detail.operation, operation);
        assert!(detail.capability_required.is_empty());
        assert!(detail.policy_decision_id.is_empty());
        assert!(detail.field_violations.is_empty());
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    #[tokio::test]
    async fn record_pipeline_metric_missing_stage_name_carries_field_violation() {
        let svc = AnalyticsServiceImpl::new(); // no pool; validation must fire first
        let request = Request::new(ana_pb::RecordPipelineMetricRequest {
            stage_name: "  ".to_string(),
            ..Default::default()
        });
        let err = svc
            .record_pipeline_metric(request)
            .await
            .expect_err("missing stage_name must be rejected before pool access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "stage_name is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "stage_name");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty pipeline stage name"
        );
    }

    #[test]
    fn analytics_missing_postgres_capability_carries_typed_detail() {
        let err = analytics_capability_status(
            "postgres_store",
            "postgres_store",
            "analytics service requires a Postgres-backed store (no PG pool configured)",
        );
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "analytics service requires a Postgres-backed store (no PG pool configured)"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "analytics");
        assert_eq!(detail.operation, "postgres_store");
        assert_eq!(detail.capability_required, "postgres_store");
        assert!(!detail.retryable);
    }

    #[test]
    fn analytics_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &analytics_internal_status(
                "get_pipeline_summary",
                "get pipeline summary failed: database is unavailable",
            ),
            "get_pipeline_summary",
            "get pipeline summary failed: database is unavailable",
        );
    }

    fn filter_has_eq(filter: &LogicalFilter, field: &str, value: &str) -> bool {
        match filter {
            LogicalFilter::Comparison {
                field: f,
                op: ComparisonOp::Eq,
                value: LogicalValue::String(v),
            } => f == field && v == value,
            LogicalFilter::And(filters) => filters.iter().any(|f| filter_has_eq(f, field, value)),
            _ => false,
        }
    }

    #[test]
    fn sla_compliance_read_scopes_to_verified_tenant() {
        let req = ana_pb::GetSlaComplianceRequest {
            stage_name: "parse".to_string(),
            ..Default::default()
        };
        let read = sla_compliance_read(&req, "tenant-a");
        let filter = read.filter.expect("tenant-scoped read must carry a filter");
        assert!(
            filter_has_eq(&filter, "tenant_id", "tenant-a"),
            "sla_compliance_read must bind the verified claim tenant"
        );
        assert!(filter_has_eq(&filter, "stage_name", "parse"));
    }

    #[test]
    fn sla_raw_sql_scopes_tenant_and_caps_rows() {
        let sql = sla_compliance_sql(&pms_model());
        assert!(
            sql.contains("= $4"),
            "raw SLA SQL must carry the bound tenant predicate"
        );
        assert!(sql.contains("LIMIT $5"), "raw SLA SQL must cap rows");
        assert!(
            !sql.contains("set_config("),
            "the RLS GUC install runs as its own statement in the read tx, not inline"
        );
        assert!(
            install_analytics_tenant_scope_sql()
                .contains("set_config('app.current_tenant_id', $1, true)"),
            "tenant RLS scope must be installed before the raw scan"
        );
    }

    #[test]
    fn platform_admin_guard_denies_tenant_caller_with_policy_detail() {
        let claim = crate::runtime::service::method_security::VerifiedClaimContext {
            subject: "user-1".to_string(),
            tenant_id: "tenant-a".to_string(),
            scopes: vec!["udb:analytics:get-executor-performance".to_string()],
            authenticated: true,
            ..Default::default()
        };
        let err = platform_admin_guard(&claim, "get_executor_performance")
            .expect_err("non-admin tenant claim must be refused");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, "get_executor_performance");
        assert_eq!(
            detail.policy_decision_id,
            "analytics_platform_admin_required"
        );
        assert!(!detail.retryable);
    }

    #[test]
    fn platform_admin_guard_accepts_admin_scope_and_role() {
        let scope_admin = crate::runtime::service::method_security::VerifiedClaimContext {
            subject: "op-1".to_string(),
            scopes: vec!["udb:admin".to_string()],
            authenticated: true,
            ..Default::default()
        };
        platform_admin_guard(&scope_admin, "get_executor_performance")
            .expect("udb:admin scope must pass");
        let role_admin = crate::runtime::service::method_security::VerifiedClaimContext {
            subject: "op-2".to_string(),
            roles: vec!["platform_admin".to_string()],
            authenticated: true,
            ..Default::default()
        };
        platform_admin_guard(&role_admin, "get_reconciliation_analytics")
            .expect("platform_admin role must pass");
        // Fail closed: an unauthenticated (public-bootstrap) context is never admin.
        let unauthenticated = crate::runtime::service::method_security::VerifiedClaimContext {
            scopes: vec!["udb:admin".to_string()],
            ..Default::default()
        };
        platform_admin_guard(&unauthenticated, "get_executor_performance")
            .expect_err("unauthenticated context must be refused");
    }

    #[test]
    fn analytics_events_match_proto_method_event_contract() {
        // Exact strings from `analytics_service.proto` `method_event_contract`.
        assert_eq!(TOPIC_ANALYTICS_EVENTS, "analytics.events");
        assert_eq!(
            EVENT_TYPE_PIPELINE_METRIC_RECORDED,
            "analytics.RecordPipelineMetric"
        );
        assert_eq!(EVENT_TYPE_SNAPSHOT_TRIGGERED, "analytics.TriggerSnapshot");
        let payload =
            analytics_event_payload(EVENT_TYPE_SNAPSHOT_TRIGGERED, "parse", "tenant-a", Some(7));
        assert_eq!(
            payload.get("event_type").and_then(|v| v.as_str()),
            Some(EVENT_TYPE_SNAPSHOT_TRIGGERED)
        );
        assert_eq!(
            payload.get("stage_name").and_then(|v| v.as_str()),
            Some("parse")
        );
        assert_eq!(
            payload.get("tenant_id").and_then(|v| v.as_str()),
            Some("tenant-a")
        );
        assert_eq!(
            payload.get("snapshots_written").and_then(|v| v.as_i64()),
            Some(7)
        );
        let record = analytics_event_payload(
            EVENT_TYPE_PIPELINE_METRIC_RECORDED,
            "parse",
            "tenant-a",
            None,
        );
        assert!(record.get("snapshots_written").is_none());
    }

    #[test]
    fn rollup_sql_computes_percentiles_over_bound_scope() {
        let sql = analytics_rollup_sql(&pms_model());
        for pct in ["0.5", "0.95", "0.99"] {
            assert!(
                sql.contains(&format!("percentile_cont({pct})")),
                "rollup must compute p{pct} via percentile_cont"
            );
        }
        // Tenant/stage/window/hour all arrive as binds — never formatted in.
        for bind in ["$1", "$2", "$3", "$4"] {
            assert!(sql.contains(bind), "rollup must bind {bind}");
        }
        assert!(
            sql.contains("IS NOT DISTINCT FROM"),
            "NULL-tenant (system-wide) groups must stay joinable for the worker pass"
        );
        assert!(
            !sql.contains("COUNT(*)"),
            "snapshots_written must report rows written, not a row count"
        );
    }
}

impl DataBrokerService {
    /// Build the native `AnalyticsService`, wired to the broker's Postgres pool.
    pub(crate) fn build_analytics_service(&self) -> AnalyticsServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam (extend_udb.md):
        // the backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("analytics", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        AnalyticsServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
