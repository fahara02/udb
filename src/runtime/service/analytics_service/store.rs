//! Neutral-IR query builders and bespoke Postgres SQL for the native
//! `AnalyticsService`: the pipeline-summary / executor-performance /
//! reconciliation / SLA reads, the (retained) typed throughput aggregate, the
//! shared PMS projection, and the tenant-scope RLS GUC install. Extracted
//! verbatim from the former god file.

use crate::ir::{
    AggregateExpr, AggregateFunc, ComparisonOp, LogicalAggregate, LogicalFilter, LogicalPagination,
    LogicalProjection, LogicalRead, LogicalSort, LogicalValue, NullOrder, SortDirection,
};
use crate::proto::udb::core::analytics::services::v1 as ana_pb;
use crate::runtime::native_catalog::NativeModel;

use super::config::{EPS_MSG, MAX_ANALYTICS_READ_ROWS, PMS_MSG, RAS_MSG};

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
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

pub(crate) fn pipeline_summary_filter(
    req: &ana_pb::GetPipelineSummaryRequest,
) -> Option<LogicalFilter> {
    read_filter(vec![
        maybe_string_filter("stage_name", &req.stage_name),
        maybe_string_filter("tenant_id", &req.tenant_id),
    ])
}

pub(crate) fn pipeline_summary_read(
    filter: Option<LogicalFilter>,
    offset: u64,
    limit: u32,
) -> LogicalRead {
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

pub(crate) fn executor_performance_read(
    req: &ana_pb::GetExecutorPerformanceRequest,
) -> LogicalRead {
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

pub(crate) fn reconciliation_analytics_read() -> LogicalRead {
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
pub(crate) fn sla_compliance_read(
    req: &ana_pb::GetSlaComplianceRequest,
    tenant_id: &str,
) -> LogicalRead {
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
pub(crate) fn sla_compliance_sql(m: &NativeModel) -> String {
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

/// Install the per-transaction tenant GUC the native RLS policies key on, so
/// raw-SQL reads over the RLS-enabled PMS table are row-scoped even on a raw
/// pooled connection (mirrors `metering_service`'s read-path pattern:
/// `set_config(..., is_local = true)` inside the read transaction, in addition
/// to the explicit bound WHERE predicate).
pub(crate) fn install_analytics_tenant_scope_sql() -> &'static str {
    "SELECT set_config('app.current_tenant_id', $1, true)"
}

/// Shared projection for a pipeline snapshot row (aliased to proto field names).
pub(crate) fn pms_projection(m: &NativeModel) -> String {
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
