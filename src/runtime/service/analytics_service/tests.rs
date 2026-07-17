//! Unit guards for the native `AnalyticsService`: the field-violation shape on a
//! missing `stage_name`, the typed capability/internal details, the tenant-scoped
//! SLA reads (typed + raw), the platform-admin guard's deny/accept decisions, the
//! contract-declared event topic/type pairing and payload shape, and the
//! percentile rollup SQL. Copied verbatim from the former god file; imports are
//! explicit (no `use super::*`).

use tonic::{Request, Status};

use crate::ir::{ComparisonOp, LogicalFilter, LogicalValue};
use crate::proto::udb::core::analytics::services::v1 as ana_pb;
use crate::proto::udb::core::analytics::services::v1::analytics_service_server::AnalyticsService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::AnalyticsServiceImpl;
use super::config::{
    EVENT_TYPE_PIPELINE_METRIC_RECORDED, EVENT_TYPE_SNAPSHOT_TRIGGERED, TOPIC_ANALYTICS_EVENTS,
};
use super::errors::{analytics_capability_status, analytics_internal_status, platform_admin_guard};
use super::events::analytics_event_payload;
use super::model::pms_model;
use super::rollup::analytics_rollup_sql;
use super::store::{install_analytics_tenant_scope_sql, sla_compliance_read, sla_compliance_sql};

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
