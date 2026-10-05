//! D11: the served `ScanProjectionDrift` RPC must say when a projection target
//! was NOT actually probed. The drift scanner has checksum probes only for the
//! relational/document/search targets; for Qdrant, Neo4j, Redis and S3 it
//! cannot read the target back, and a report of "0 divergent rows" for those
//! targets used to read as "in sync". The served response now carries a
//! `NOT PROBED` warning per such target (and on the response itself).
//!
//! Drives the real admin handler (`scan_projection_drift_inner`, the method the
//! tonic server dispatches) against a live Postgres source table, with the
//! project's active catalog declaring Qdrant and Redis projection targets.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use tonic::Request;
use uuid::Uuid;

use super::support::{live_native_service_db_lock, require_live_dsn_any};
use crate::engine::FsmState;
use crate::generation::manifest::{ManifestProjection, ManifestStoreOption};
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable, ManifestTableSecurity};
use crate::metrics::MetricsRecorder;
use crate::proto::ProjectionDriftScanRequest;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::config::UdbConfig;
use crate::runtime::projection::ProjectionEngine;
use crate::runtime::security::SecurityConfig;
use crate::runtime::service::DataBrokerService;
use crate::runtime::system::{SystemCatalogConfig, ensure_system_catalog};

fn drift_security() -> SecurityConfig {
    SecurityConfig {
        tls_required: false,
        service_identity_required: false,
        mtls_required: false,
        allow_header_scopes: true,
        ..SecurityConfig::default()
    }
}

fn column(name: &str, is_primary: bool) -> ManifestColumn {
    ManifestColumn {
        field_name: name.to_string(),
        column_name: name.to_string(),
        proto_type: "string".to_string(),
        sql_type: "TEXT".to_string(),
        is_primary,
        not_null: is_primary || name == "tenant_id",
        ..ManifestColumn::default()
    }
}

fn projection(backend: &str, kind: &str, resource: &str) -> ManifestProjection {
    ManifestProjection {
        message_type: "DriftDoc".to_string(),
        projection_kind: kind.to_string(),
        backend: backend.to_string(),
        resource_name: resource.to_string(),
        write_policy: "projection".to_string(),
        fanout_policy: "async_projection".to_string(),
        options: vec![ManifestStoreOption {
            key: "vector_field".to_string(),
            value: "vector".to_string(),
        }],
        ..ManifestProjection::default()
    }
}

#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live lane"]
async fn served_drift_scan_reports_unprobed_targets_as_not_probed_live() {
    let Some(dsn) = require_live_dsn_any(&[
        "UDB_LIVE_NATIVE_PG_DSN",
        "UDB_LIVE_AUTH_PG_DSN",
        "UDB_INTEGRATION_PG_DSN",
    ]) else {
        eprintln!("live Postgres DSN unset — skipping served drift NOT PROBED");
        return;
    };
    let _guard = live_native_service_db_lock().lock().await;
    SecurityConfig::install_global(drift_security());
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&dsn)
        .await
        .unwrap_or_else(|err| panic!("connect live drift postgres at {dsn}: {err}"));
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_drift_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&pool)
        .await
        .expect("create drift schema");
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".drift_docs (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL)"
    ))
    .execute(&pool)
    .await
    .expect("create drift source table");
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".drift_docs (id, tenant_id) VALUES ('d1', 'tenant-a')"
    ))
    .execute(&pool)
    .await
    .expect("seed drift source row");

    let manifest = CatalogManifest {
        checksum_sha256: format!("drift-not-probed-{schema}"),
        tables: vec![ManifestTable {
            message_name: "DriftDoc".to_string(),
            schema: schema.clone(),
            table: "drift_docs".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec![column("id", true), column("tenant_id", false)],
            table_security: ManifestTableSecurity {
                tenant_column: "tenant_id".to_string(),
                ..ManifestTableSecurity::default()
            },
            ..ManifestTable::default()
        }],
        projections: vec![
            projection("qdrant", "vector", "drift_vectors"),
            projection("redis", "cache", "drift:{id}"),
        ],
        ..CatalogManifest::default()
    };

    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = dsn.clone();
    config.security = drift_security();
    let runtime = DataBrokerRuntime::from_config(config).await;
    let metrics: Arc<dyn MetricsRecorder> = Arc::new(crate::metrics::NoopMetrics);
    let mut svc = DataBrokerService::with_runtime_and_state(
        manifest,
        runtime,
        Arc::new(RwLock::new(FsmState::Completed)),
        metrics,
        None,
        true,
    );
    svc.projection_engine = Some(Arc::new(ProjectionEngine::new(
        pool.clone(),
        SystemCatalogConfig::current(),
    )));

    let mut request = Request::new(ProjectionDriftScanRequest {
        message_type: "DriftDoc".to_string(),
        scan_mode: "sample".to_string(),
        rows_per_target: 10,
        ..ProjectionDriftScanRequest::default()
    });
    let md = request.metadata_mut();
    md.insert("x-tenant-id", "tenant-a".parse().unwrap());
    md.insert("x-purpose", "admin".parse().unwrap());
    md.insert("x-scopes", "udb:admin,udb:read,udb:write".parse().unwrap());
    let response = svc
        .scan_projection_drift_inner(request)
        .await
        .expect("served ScanProjectionDrift")
        .into_inner();

    assert_eq!(response.reports.len(), 2, "{response:?}");
    for report in &response.reports {
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("NOT PROBED")),
            "target {}:{} must be reported NOT PROBED, got {:?}",
            report.target_backend,
            report.target_resource,
            report.warnings
        );
        assert!(
            report.divergent_rows.is_empty(),
            "an unprobed target reports no rows — its drift is unknown, not zero"
        );
    }
    assert!(
        response
            .warnings
            .iter()
            .filter(|warning| warning.contains("NOT PROBED"))
            .count()
            >= 2,
        "response-level warnings carry every unprobed target: {:?}",
        response.warnings
    );

    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(&pool)
        .await;
}
