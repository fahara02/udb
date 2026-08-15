//! Unit + live fixtures for `MeteringService`: the pure quota/window math, the
//! swallow-on-error ingest contract, tenant-isolation and typed-detail guards,
//! and the live rollup/export oracle (master-plan 9.9).

use std::sync::Arc;
use std::time::Duration;

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};
use uuid::Uuid;

use crate::proto::udb::core::metering::services::v1 as metering_pb;
use crate::proto::udb::core::metering::services::v1::metering_service_server::MeteringService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::MeteringServiceImpl;
use super::admission::{admission_metering_method, record_usage, record_usage_strict};
use super::calc::{
    closed_rollup_upper_bound, event_in_window, now_unix, quota_decision, window_start_unix,
};
use super::config::{DEFAULT_ROLLUP_WINDOW_SECONDS, DEFAULT_WINDOW_SECONDS, TOPIC_USAGE_ROLLUP};
use super::errors::{metering_capability_status, metering_internal_status};
use super::rollup::{rollup_id, run_metering_rollup_once};
use super::store::{install_metering_tenant_scope_sql, windowed_usage_sum_sql};

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
    assert_eq!(detail.backend, "metering");
    assert_eq!(detail.operation, operation);
    assert!(detail.capability_required.is_empty());
    assert!(detail.policy_decision_id.is_empty());
    assert!(detail.field_violations.is_empty());
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

async fn live_metering_fixture() -> (sqlx::PgPool, MeteringServiceImpl, String, String) {
    let dsn = std::env::var("UDB_LIVE_NATIVE_PG_DSN")
        .or_else(|_| std::env::var("UDB_LIVE_AUTH_PG_DSN"))
        .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
        .unwrap_or_else(|_| "postgres://udb:udb@127.0.0.1:55432/udb".to_string());
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&dsn)
        .await
        .unwrap_or_else(|err| panic!("connect live metering postgres at {dsn}: {err}"));
    let schemas: Vec<String> = sqlx::query_scalar(
        "SELECT nspname FROM pg_namespace WHERE nspname LIKE 'udb\\_%' ESCAPE '\\'",
    )
    .fetch_all(&pool)
    .await
    .expect("list live metering schemas");
    for schema in schemas {
        let stmt = format!(
            "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
            schema.replace('"', "\"\"")
        );
        sqlx::query(&stmt)
            .execute(&pool)
            .await
            .unwrap_or_else(|err| panic!("drop live metering schema {schema}: {err}"));
    }
    for stmt in crate::runtime::native_catalog::native_service_catalog_ddl() {
        sqlx::raw_sql(&stmt)
            .execute(&pool)
            .await
            .unwrap_or_else(|err| panic!("native service DDL failed: {err}\nSQL:\n{stmt}"));
    }
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live metering system catalog");

    let mut config = crate::runtime::config::UdbConfig::from_env();
    config.primary.direct_dsn = dsn;
    let runtime = Arc::new(DataBrokerRuntime::from_config(config).await);
    let outbox = runtime.config().cdc.outbox_relation();
    let journal = crate::runtime::system::SystemCatalogConfig::current().cdc_journal_relation();
    let svc = MeteringServiceImpl::new()
        .with_postgres(Some(pool.clone()))
        .with_runtime(Some(runtime))
        .with_outbox(Some(outbox.clone()));
    (pool, svc, outbox, journal)
}

/// The pure quota decision: under-limit is allowed, at/over-limit is denied,
/// and remaining is always `max(0, limit - used)`.
#[test]
fn quota_decision_math() {
    // used < limit -> allowed, remaining = limit - used.
    let (allowed, remaining) = quota_decision(3, 10);
    assert!(allowed);
    assert_eq!(remaining, 7);
    // used == limit -> denied (boundary), remaining 0.
    let (allowed, remaining) = quota_decision(10, 10);
    assert!(!allowed);
    assert_eq!(remaining, 0);
    // used > limit -> denied, remaining clamped to 0 (never negative).
    let (allowed, remaining) = quota_decision(15, 10);
    assert!(!allowed);
    assert_eq!(remaining, 0);
    // limit 0 -> deny all.
    let (allowed, remaining) = quota_decision(0, 0);
    assert!(!allowed);
    assert_eq!(remaining, 0);
}

/// The window lower bound is inclusive: an event at exactly `window_start` is
/// counted; one second earlier is not. The SQL aggregate uses `Ge` on the same
/// boundary, so this predicate is the single source of the boundary semantics.
#[test]
fn window_aggregation_boundary() {
    let now = 1_000_000;
    let start = window_start_unix(now, 3_600); // last hour
    assert_eq!(start, now - 3_600);
    assert!(event_in_window(start, start)); // exactly on the boundary: included
    assert!(event_in_window(now, start)); // inside: included
    assert!(!event_in_window(start - 1, start)); // just before: excluded
    // A window larger than `now` floors at 0 (no underflow).
    assert_eq!(window_start_unix(100, 10_000), 0);
}

#[test]
fn windowed_usage_installs_rls_scope_before_aggregate_scan() {
    let install_scope = install_metering_tenant_scope_sql();
    assert!(
        install_scope.contains("set_config('app.current_tenant_id', $1, true)"),
        "tenant scope must be installed before scanning usage_events"
    );

    let aggregate = windowed_usage_sum_sql();
    assert!(
        !aggregate.contains("set_config("),
        "aggregate scan must not install tenant scope inside its WHERE clause"
    );
    assert!(
        aggregate.contains("FROM udb_metering.usage_events")
            && aggregate.contains("tenant_id = $1")
            && aggregate.contains("method = $2")
            && aggregate.contains("occurred_at_unix >= $3"),
        "aggregate must keep the tenant/method/window filters"
    );
}

#[test]
fn admission_metering_method_is_canonical() {
    assert_eq!(admission_metering_method("cache", "read"), "cache.read");
    assert_eq!(admission_metering_method("", "write"), "native.write",);
    assert_eq!(admission_metering_method("  ", "  "), "native.unknown",);
}

#[test]
fn rollup_window_uses_only_closed_buckets() {
    assert_eq!(closed_rollup_upper_bound(3_599, 3_600), 0);
    assert_eq!(closed_rollup_upper_bound(3_600, 3_600), 3_600);
    assert_eq!(closed_rollup_upper_bound(3_601, 3_600), 3_600);
}

#[test]
fn rollup_id_is_stable_for_deduplication() {
    assert_eq!(
        rollup_id("tenant-a", "storage.RegisterUpload", "request", 0, 3_600),
        "tenant-a:storage.RegisterUpload:request:0:3600"
    );
}

/// `record_usage` must NEVER propagate a store error: against a pool pointed at
/// a closed port the INSERT fails, but the function swallows it and returns Ok.
#[tokio::test]
async fn record_usage_swallows_store_error() {
    // Lazy pool: constructed without I/O; the connection (refused) only fails on
    // first use, exactly the "metering store outage" we must not propagate.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(250))
        .connect_lazy("postgres://127.0.0.1:1/udb_metering_test")
        .expect("lazy pool builds without connecting");
    let result = record_usage(
        &pool,
        "tenant-a",
        "principal-1",
        "data.Select",
        "request",
        1,
        0,
    )
    .await;
    assert!(
        result.is_ok(),
        "metering must never fail the metered request",
    );
    // An empty tenant/method is a no-op (still Ok, no panic, no insert attempt).
    assert!(
        record_usage(&pool, "", "p", "m", "request", 1, 0)
            .await
            .is_ok()
    );
    assert!(
        record_usage(&pool, "t", "p", "  ", "request", 1, 0)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn explicit_record_usage_rejects_store_error_instead_of_acknowledging_it() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(250))
        .connect_lazy("postgres://127.0.0.1:1/udb_metering_test")
        .expect("lazy pool builds without connecting");
    let svc = MeteringServiceImpl::new().with_postgres(Some(pool));
    let mut request = Request::new(metering_pb::RecordUsageRequest {
        tenant_id: "tenant-a".to_string(),
        principal_id: "principal-1".to_string(),
        method: "data.Select".to_string(),
        unit: "request".to_string(),
        quantity: 1,
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .record_usage(request)
        .await
        .expect_err("explicit durable ingest must surface a store outage");
    assert_eq!(err.code(), tonic::Code::Internal);
    assert_eq!(decode_detail(&err).operation, "record_usage_insert");

    let strict = record_usage_strict(
        svc.pg_pool.as_ref().expect("test pool"),
        "tenant-a",
        "principal-1",
        "data.Select",
        "request",
        1,
        0,
    )
    .await
    .expect_err("strict seam must propagate the same outage");
    assert_eq!(decode_detail(&strict).operation, "record_usage_insert");
}

/// Live rollup/export oracle for master-plan 9.9: served RecordUsage writes
/// durable rows, QueryUsage sums the same rows, and the leader rollup worker
/// exports exactly one closed-window outbox event with deterministic dedupe.
#[tokio::test]
#[ignore = "requires live Postgres; run with cargo test --lib live_postgres_metering_rollup_exports_closed_window_once -- --ignored --nocapture"]
async fn live_postgres_metering_rollup_exports_closed_window_once() {
    let (pool, svc, outbox, journal) = live_metering_fixture().await;
    let tenant_id = Uuid::new_v4().to_string();
    let method = "storage.RegisterUpload";
    let unit = "bytes";
    let window = DEFAULT_ROLLUP_WINDOW_SECONDS;
    let upper = closed_rollup_upper_bound(now_unix(), window);
    assert!(
        upper >= window,
        "live clock must have at least one closed rollup window"
    );
    let occurred = upper - 60;

    for quantity in [7_i64, 11_i64] {
        MeteringService::record_usage(
            &svc,
            Request::new(metering_pb::RecordUsageRequest {
                tenant_id: tenant_id.clone(),
                principal_id: "principal-live".to_string(),
                method: method.to_string(),
                unit: unit.to_string(),
                quantity,
                occurred_at_unix: occurred,
                metadata_json: "{}".to_string(),
            }),
        )
        .await
        .expect("record_usage")
        .into_inner();
    }

    let usage = MeteringService::query_usage(
        &svc,
        Request::new(metering_pb::QueryUsageRequest {
            tenant_id: tenant_id.clone(),
            metric: method.to_string(),
            window_seconds: DEFAULT_WINDOW_SECONDS,
        }),
    )
    .await
    .expect("query_usage")
    .into_inner();
    assert_eq!(
        usage.used, 18,
        "QueryUsage must sum durable UsageEvent rows"
    );

    let emitted = run_metering_rollup_once(&pool, &outbox, &journal, 10, None)
        .await
        .expect("rollup pass");
    assert_eq!(emitted, 1, "first pass must emit the closed usage bucket");
    let emitted_again = run_metering_rollup_once(&pool, &outbox, &journal, 10, None)
        .await
        .expect("dedupe rollup pass");
    assert_eq!(
        emitted_again, 0,
        "second pass must dedupe against the outbox rollup id"
    );

    let payload: serde_json::Value = sqlx::query_scalar(&format!(
        "SELECT payload FROM {outbox} WHERE topic = $1 ORDER BY event_seq DESC LIMIT 1"
    ))
    .bind(TOPIC_USAGE_ROLLUP)
    .fetch_one(&pool)
    .await
    .expect("read rollup outbox payload");
    assert_eq!(payload["event_type"], TOPIC_USAGE_ROLLUP);
    let rollup_payload = &payload["payload"];
    assert_eq!(rollup_payload["tenant_id"], tenant_id);
    assert_eq!(rollup_payload["method"], method);
    assert_eq!(rollup_payload["unit"], unit);
    assert_eq!(rollup_payload["quantity"], 18);
    assert_eq!(rollup_payload["event_count"], 2);
    assert_eq!(
        rollup_payload["rollup_id"],
        rollup_id(&tenant_id, method, unit, upper - window, upper)
    );
}

/// A caller scoped to tenant-a must not write tenant-b's quota by putting a
/// foreign tenant_id in the request BODY; the guard rejects before any store
/// access (no Postgres needed) — mirrors `lock_service`/`config_service`.
#[tokio::test]
async fn put_quota_rejects_cross_tenant_body() {
    let svc = MeteringServiceImpl::new(); // no runtime, no channels (admit no-op)
    let mut request = Request::new(metering_pb::PutQuotaRequest {
        tenant_id: "tenant-b".to_string(),
        metric: "data.Select".to_string(),
        limit_value: 100,
        window_seconds: 3_600,
        enabled: true,
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .put_quota(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn put_quota_rejects_cross_project_body() {
    let svc = MeteringServiceImpl::new();
    let mut request = Request::new(metering_pb::PutQuotaRequest {
        tenant_id: "tenant-a".to_string(),
        project_id: "project-b".to_string(),
        metric: "data.Select".to_string(),
        limit_value: 100,
        window_seconds: 3_600,
        enabled: true,
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    request
        .metadata_mut()
        .insert("x-udb-project-id", MetadataValue::from_static("project-a"));
    let err = svc
        .put_quota(request)
        .await
        .expect_err("cross-project body must be rejected before runtime access");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        decode_detail(&err).policy_decision_id,
        "project_metadata_mismatch"
    );
}

#[tokio::test]
async fn record_usage_missing_method_carries_field_violation() {
    let svc = MeteringServiceImpl::new(); // no pool/runtime; validation must fire first
    let mut request = Request::new(metering_pb::RecordUsageRequest {
        tenant_id: "tenant-a".to_string(),
        method: "  ".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .record_usage(request)
        .await
        .expect_err("missing method must be rejected before admission/store access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "method is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "method");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty usage method"
    );
}

#[tokio::test]
async fn query_usage_missing_metric_carries_field_violation() {
    let svc = MeteringServiceImpl::new(); // no runtime, no channels (admit no-op)
    let mut request = Request::new(metering_pb::QueryUsageRequest {
        tenant_id: "tenant-a".to_string(),
        metric: String::new(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .query_usage(request)
        .await
        .expect_err("missing metric must be rejected before runtime access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "metric is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "metric");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty metric name"
    );
}

#[tokio::test]
async fn put_quota_negative_limit_carries_field_violation() {
    let svc = MeteringServiceImpl::new(); // no runtime, no channels (admit no-op)
    let mut request = Request::new(metering_pb::PutQuotaRequest {
        tenant_id: "tenant-a".to_string(),
        metric: "data.Select".to_string(),
        limit_value: -1,
        window_seconds: 3_600,
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .put_quota(request)
        .await
        .expect_err("negative limit must be rejected before runtime access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "limit_value must be >= 0");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "limit_value");
    assert_eq!(
        detail.field_violations[0].description,
        "must be greater than or equal to 0"
    );
}

/// CheckQuota likewise rejects a cross-tenant body before any store access.
#[tokio::test]
async fn check_quota_rejects_cross_tenant_body() {
    let svc = MeteringServiceImpl::new();
    let mut request = Request::new(metering_pb::CheckQuotaRequest {
        tenant_id: "tenant-b".to_string(),
        metric: "data.Select".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .check_quota(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[test]
fn metering_missing_runtime_capability_carries_typed_detail() {
    let err = metering_capability_status(
        "native_entity_dispatch",
        "runtime_native_entity_dispatch",
        "metering service requires runtime native-entity dispatch (no runtime configured)",
    );
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "metering service requires runtime native-entity dispatch (no runtime configured)"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Capability as i32);
    assert_eq!(detail.backend, "metering");
    assert_eq!(detail.operation, "native_entity_dispatch");
    assert_eq!(detail.capability_required, "runtime_native_entity_dispatch");
    assert!(!detail.retryable);
}

#[test]
fn metering_internal_status_carries_typed_detail() {
    assert_internal_detail(
        &metering_internal_status(
            "windowed_usage_aggregate",
            "windowed usage aggregate failed: database is unavailable",
        ),
        "windowed_usage_aggregate",
        "windowed usage aggregate failed: database is unavailable",
    );
}
