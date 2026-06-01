use super::support::*;
use crate::proto::udb::core::analytics::services::v1 as analytics_pb;
use crate::proto::udb::core::analytics::services::v1::analytics_service_server::AnalyticsService;
use crate::proto::udb::core::common::v1 as common_pb;
use tonic::Request;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_analytics_native_schema_from_proto -- --ignored --nocapture"]
async fn live_postgres_analytics_native_schema_from_proto() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    assert_native_table_columns(
        &pool,
        "udb.core.analytics.entity.v1.PipelineMetricSnapshot",
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
    .await;
    assert_native_table_columns(
        &pool,
        "udb.core.analytics.entity.v1.ExecutorPerformanceSummary",
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
    .await;
    assert_native_table_columns(
        &pool,
        "udb.core.analytics.entity.v1.ReconciliationAnalyticsSummary",
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
    .await;

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_analytics_service_rollups_roundtrip -- --ignored --nocapture"]
async fn live_postgres_analytics_service_rollups_roundtrip() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = analytics_service(pool.clone());
    let tenant_id = seed_default_tenant(&pool).await;
    let stage = format!("execute_{}", uuid::Uuid::new_v4().simple());

    for (latency_ms, is_success) in [(100.0, true), (200.0, true), (400.0, false)] {
        let accepted = svc
            .record_pipeline_metric(Request::new(analytics_pb::RecordPipelineMetricRequest {
                stage_name: stage.clone(),
                tenant_id: tenant_id.clone(),
                latency_ms,
                is_success,
                ..Default::default()
            }))
            .await
            .expect("record pipeline metric")
            .into_inner();
        assert!(accepted.accepted);
    }

    let summary = svc
        .get_pipeline_summary(Request::new(analytics_pb::GetPipelineSummaryRequest {
            stage_name: stage.clone(),
            tenant_id: tenant_id.clone(),
            page: Some(common_pb::PageRequest {
                page: 1,
                page_size: 10,
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .expect("get pipeline summary")
        .into_inner();
    assert_eq!(summary.snapshots.len(), 1);
    let snapshot = &summary.snapshots[0];
    assert_eq!(snapshot.total_requests, 3);
    assert_eq!(snapshot.successful, 2);
    assert_eq!(snapshot.failed, 1);
    assert!((snapshot.avg_latency_ms - (700.0 / 3.0)).abs() < 0.01);
    assert!((snapshot.error_rate - (1.0 / 3.0)).abs() < 0.01);
    assert_eq!(summary.page.as_ref().expect("page").total_items, 1);

    let throughput = svc
        .get_throughput(Request::new(analytics_pb::GetThroughputRequest {
            tenant_id: tenant_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("get throughput")
        .into_inner();
    assert_eq!(throughput.total_requests, 3);
    assert!((throughput.overall_success_rate - (2.0 / 3.0)).abs() < 0.01);

    let snapshot_trigger = svc
        .trigger_snapshot(Request::new(analytics_pb::TriggerSnapshotRequest {
            stage_name: stage.clone(),
            ..Default::default()
        }))
        .await
        .expect("trigger snapshot")
        .into_inner();
    assert_eq!(snapshot_trigger.snapshots_written, 1);

    let model = crate::runtime::native_catalog::native_model(
        "udb.core.analytics.entity.v1.PipelineMetricSnapshot",
        &[
            "stage_name",
            "p99_latency_ms",
            "error_rate",
            "snapshot_hour",
        ],
    );
    sqlx::query(&format!(
        "UPDATE {rel} SET {p99} = $2 WHERE {stage_name} = $1",
        rel = model.relation,
        p99 = model.q("p99_latency_ms"),
        stage_name = model.q("stage_name"),
    ))
    .bind(&stage)
    .bind(450.0)
    .execute(&pool)
    .await
    .expect("seed p99 through native analytics model");

    let sla = svc
        .get_sla_compliance(Request::new(analytics_pb::GetSlaComplianceRequest {
            stage_name: stage,
            p99_threshold_ms: 500.0,
            error_rate_threshold: 0.5,
            ..Default::default()
        }))
        .await
        .expect("get SLA compliance")
        .into_inner();
    assert_eq!(sla.entries.len(), 1);
    assert!(sla.entries[0].p99_sla_met);
    assert!(sla.entries[0].error_rate_sla_met);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_analytics_executor_and_reconciliation_queries -- --ignored --nocapture"]
async fn live_postgres_analytics_executor_and_reconciliation_queries() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = analytics_service(pool.clone());

    let eps = crate::runtime::native_catalog::native_model(
        "udb.core.analytics.entity.v1.ExecutorPerformanceSummary",
        &[
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
        ],
    );
    sqlx::query(&format!(
        "INSERT INTO {rel} ({summary_date}, {executor}, {workload}, {dispatches}, {success}, {timeouts}, {errors}, {avg_ms}, {p99}, {confidence}, {rate}, {capacity}) \
         VALUES ($1::date, $2, $3, 10, 9, 0, 1, 42.5, 90.0, 0.97, 0.9, 0.66)",
        rel = eps.relation,
        summary_date = eps.q("summary_date"),
        executor = eps.q("executor_identity"),
        workload = eps.q("workload_kind"),
        dispatches = eps.q("total_dispatches"),
        success = eps.q("successful_results"),
        timeouts = eps.q("timeout_count"),
        errors = eps.q("error_count"),
        avg_ms = eps.q("avg_execution_ms"),
        p99 = eps.q("p99_execution_ms"),
        confidence = eps.q("avg_confidence"),
        rate = eps.q("success_rate"),
        capacity = eps.q("avg_capacity_utilisation"),
    ))
    .bind("2026-06-01")
    .bind("executor-live")
    .bind("projection")
    .execute(&pool)
    .await
    .expect("seed executor analytics through native model");

    let perf = svc
        .get_executor_performance(Request::new(analytics_pb::GetExecutorPerformanceRequest {
            executor_identity: "executor-live".to_string(),
            workload_kind: "projection".to_string(),
            date_from: "2026-06-01".to_string(),
            date_to: "2026-06-01".to_string(),
        }))
        .await
        .expect("get executor performance")
        .into_inner();
    assert_eq!(perf.summaries.len(), 1);
    assert_eq!(perf.summaries[0].successful_results, 9);
    assert!((perf.summaries[0].success_rate - 0.9).abs() < 0.01);

    let ras = crate::runtime::native_catalog::native_model(
        "udb.core.analytics.entity.v1.ReconciliationAnalyticsSummary",
        &[
            "summary_date",
            "total_reconciliations",
            "exact_matches",
            "partial_conflicts",
            "hard_conflicts",
            "low_confidence_flagged",
            "avg_reconciliation_ms",
            "resolution_rate",
            "avg_record_confidence",
        ],
    );
    for (date, total, exact, avg_ms) in [
        ("2026-06-01", 10i64, 8i64, 30.0),
        ("2026-06-02", 20i64, 10i64, 50.0),
    ] {
        sqlx::query(&format!(
            "INSERT INTO {rel} ({summary_date}, {total}, {exact}, {partial}, {hard}, {low_conf}, {avg_ms}, {rate}, {confidence}) \
             VALUES ($1::date, $2, $3, 1, 1, 0, $4, $3::float8 / $2::float8, 0.95)",
            rel = ras.relation,
            summary_date = ras.q("summary_date"),
            total = ras.q("total_reconciliations"),
            exact = ras.q("exact_matches"),
            partial = ras.q("partial_conflicts"),
            hard = ras.q("hard_conflicts"),
            low_conf = ras.q("low_confidence_flagged"),
            avg_ms = ras.q("avg_reconciliation_ms"),
            rate = ras.q("resolution_rate"),
            confidence = ras.q("avg_record_confidence"),
        ))
        .bind(date)
        .bind(total)
        .bind(exact)
        .bind(avg_ms)
        .execute(&pool)
        .await
        .expect("seed reconciliation analytics through native model");
    }

    let recon = svc
        .get_reconciliation_analytics(Request::new(
            analytics_pb::GetReconciliationAnalyticsRequest {
                date_from: "2026-06-01".to_string(),
                date_to: "2026-06-02".to_string(),
            },
        ))
        .await
        .expect("get reconciliation analytics")
        .into_inner();
    assert_eq!(recon.summaries.len(), 2);
    assert!((recon.overall_resolution_rate - 0.6).abs() < 0.01);
    assert!((recon.avg_reconciliation_ms - 40.0).abs() < 0.01);

    cleanup_native_auth_db(&pool).await;
}
