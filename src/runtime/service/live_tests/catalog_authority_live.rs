//! Ignored live regression for durable project-catalog authority.
//!
//! This deliberately exercises one shared Postgres authority through two
//! independent broker instances. It is kept as one serialized test because the
//! individual assertions are one state-machine sequence: idempotent stage,
//! concurrent activation, stale replay, restart hydration, and project denial.

use std::sync::{Arc, RwLock};

use sqlx::PgPool;
use tonic::{Code, Request};
use uuid::Uuid;

use crate::engine::FsmState;
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::StageCatalogRequest;
use crate::runtime::config::UdbConfig;
use crate::runtime::security::SecurityConfig;
use crate::runtime::service::DataBrokerService;
use crate::runtime::service::method_security::{scope_claim_context_for_test, test_claim_context};
use crate::runtime::system::SystemCatalogConfig;
use crate::runtime::{DataBrokerRuntime, native_catalog};

fn catalog_live_dsn() -> Option<String> {
    [
        "UDB_LIVE_NATIVE_PG_DSN",
        "UDB_LIVE_AUTH_PG_DSN",
        "UDB_INTEGRATION_PG_DSN",
        "UDB_PG_DSN",
    ]
    .into_iter()
    .find_map(|key| {
        std::env::var(key)
            .ok()
            .filter(|value| !value.trim().is_empty())
    })
}

fn catalog_live_config(dsn: &str) -> UdbConfig {
    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = dsn.to_string();
    config.project_routing_mode = "permissive".to_string();
    config.security = SecurityConfig {
        tls_required: false,
        service_identity_required: false,
        mtls_required: false,
        allow_header_scopes: true,
        ..SecurityConfig::default()
    };
    config
}

async fn catalog_live_service(dsn: &str) -> DataBrokerService {
    let runtime = DataBrokerRuntime::from_config(catalog_live_config(dsn)).await;
    let lifecycle = Arc::new(RwLock::new(FsmState::Completed));
    let metrics: Arc<dyn MetricsRecorder> = Arc::new(NoopMetrics);
    DataBrokerService::with_runtime_and_state(
        native_catalog::native_manifest().clone(),
        runtime,
        lifecycle,
        metrics,
        None,
        true,
    )
}

async fn durable_active(pool: &PgPool, project_id: &str) -> (String, String, i64) {
    let config = SystemCatalogConfig::default();
    let catalog_relation = config.catalog_versions_relation();
    let binding_relation = config.project_catalog_bindings_relation();
    sqlx::query_as(&format!(
        "SELECT catalog.catalog_id::TEXT, binding.active_catalog_id::TEXT, COUNT(*) OVER ()::BIGINT
           FROM {catalog_relation} AS catalog
           JOIN {binding_relation} AS binding
             ON binding.project_id = catalog.project_id
            AND binding.active_catalog_id = catalog.catalog_id
          WHERE catalog.project_id = $1 AND catalog.status = 'ACTIVE'"
    ))
    .bind(project_id)
    .fetch_one(pool)
    .await
    .expect("load one exact durable ACTIVE catalog")
}

async fn cleanup_catalog_project(pool: &PgPool, project_id: &str) {
    let config = SystemCatalogConfig::default();
    sqlx::query(&format!(
        "DELETE FROM {} WHERE project_id = $1",
        config.migration_runs_relation()
    ))
    .bind(project_id)
    .execute(pool)
    .await
    .unwrap_or_else(|err| panic!("clean project migration runs: {err}"));
    for relation in [
        config.project_catalog_bindings_relation(),
        config.catalog_reload_log_relation(),
        config.catalog_activation_log_relation(),
        config.catalog_versions_relation(),
    ] {
        sqlx::query(&format!("DELETE FROM {relation} WHERE project_id = $1"))
            .bind(project_id)
            .execute(pool)
            .await
            .unwrap_or_else(|err| panic!("clean catalog project from {relation}: {err}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Postgres; covered by CI Native service live tests"]
async fn live_postgres_catalog_authority_end_to_end() {
    let Some(dsn) = catalog_live_dsn() else {
        eprintln!("catalog authority live test skipped: no live Postgres DSN");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&dsn)
        .await
        .unwrap_or_else(|err| panic!("connect catalog-authority live Postgres at {dsn}: {err}"));

    // Use the standard destructive live fixture once, then prove the system
    // migration itself is safe on two further startup passes.
    super::support::migrate_native_service_db(&pool).await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("second system-catalog startup is idempotent");
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("third system-catalog startup is idempotent");

    let project_id = format!("catalog-authority-{}", Uuid::new_v4());
    let denied_project_id = format!("catalog-denied-{}", Uuid::new_v4());
    let tenant_id = format!("tenant-{}", Uuid::new_v4());
    let manifest = serde_json::to_vec(native_catalog::native_manifest())
        .expect("serialize canonical native catalog manifest");
    let pretty_manifest = serde_json::to_vec_pretty(native_catalog::native_manifest())
        .expect("pretty-serialize canonical native catalog manifest");
    assert_ne!(
        manifest, pretty_manifest,
        "raw request evidence must differ"
    );

    let runtime = Arc::new(DataBrokerRuntime::from_config(catalog_live_config(&dsn)).await);

    let first = runtime
        .stage_catalog(
            &project_id,
            "1.0.0",
            &manifest,
            "initial",
            "live-test",
            "none",
            "stage-a",
        )
        .await
        .expect("stage initial catalog");
    let replay = runtime
        .stage_catalog(
            &project_id,
            "1.0.0",
            &manifest,
            "initial",
            "live-test",
            "none",
            "stage-a",
        )
        .await
        .expect("replay identical stage");
    assert!(replay.replayed);
    assert_eq!(first.catalog.catalog_id, replay.catalog.catalog_id);

    let conflict = runtime
        .stage_catalog(
            &project_id,
            "1.0.0",
            &pretty_manifest,
            "initial",
            "live-test",
            "none",
            "stage-a",
        )
        .await
        .expect_err("same idempotency key with different raw evidence must fail");
    assert_eq!(conflict.code(), Code::FailedPrecondition);

    runtime
        .activate_catalog(
            &project_id,
            &first.catalog.catalog_id,
            "initial activation",
            "live-test",
            "activate-a",
        )
        .await
        .expect("activate initial catalog");

    let staged_b = runtime
        .stage_catalog(
            &project_id,
            "1.1.0",
            &manifest,
            "candidate b",
            "live-test",
            "none",
            "stage-b",
        )
        .await
        .expect("stage candidate b");
    let staged_c = runtime
        .stage_catalog(
            &project_id,
            "1.2.0",
            &manifest,
            "candidate c",
            "live-test",
            "none",
            "stage-c",
        )
        .await
        .expect("stage candidate c");

    let runtime_b = Arc::clone(&runtime);
    let runtime_c = Arc::clone(&runtime);
    let project_b = project_id.clone();
    let project_c = project_id.clone();
    let catalog_b = staged_b.catalog.catalog_id.clone();
    let catalog_c = staged_c.catalog.catalog_id.clone();
    let (activate_b, activate_c) = tokio::join!(
        async move {
            runtime_b
                .activate_catalog(
                    &project_b,
                    &catalog_b,
                    "concurrent b",
                    "live-test-instance-b",
                    "activate-b",
                )
                .await
        },
        async move {
            runtime_c
                .activate_catalog(
                    &project_c,
                    &catalog_c,
                    "concurrent c",
                    "live-test-instance-c",
                    "activate-c",
                )
                .await
        }
    );
    let activation_successes = [&activate_b, &activate_c]
        .into_iter()
        .filter(|result| result.is_ok())
        .count();
    assert_eq!(
        activation_successes, 1,
        "two service instances racing from one baseline must commit exactly one ACTIVE"
    );
    let (winner_id, binding_id, active_count) = durable_active(&pool, &project_id).await;
    assert_eq!(winner_id, binding_id);
    assert_eq!(active_count, 1);

    runtime
        .rollback_catalog(
            &project_id,
            &first.catalog.catalog_id,
            "return to a",
            "live-test",
            "rollback-a",
        )
        .await
        .expect("rollback to initial catalog");
    let staged_d = runtime
        .stage_catalog(
            &project_id,
            "2.0.0",
            &manifest,
            "candidate d",
            "live-test",
            "none",
            "stage-d",
        )
        .await
        .expect("stage catalog d");
    runtime
        .activate_catalog(
            &project_id,
            &staged_d.catalog.catalog_id,
            "activate d",
            "live-test",
            "activate-d",
        )
        .await
        .expect("activate catalog d");

    let stale_replay = runtime
        .rollback_catalog(
            &project_id,
            &first.catalog.catalog_id,
            "return to a",
            "live-test",
            "rollback-a",
        )
        .await
        .expect("stale rollback replay returns recorded result");
    assert!(stale_replay.replayed);
    let (after_replay, binding_after_replay, active_count) =
        durable_active(&pool, &project_id).await;
    assert_eq!(after_replay, staged_d.catalog.catalog_id);
    assert_eq!(after_replay, binding_after_replay);
    assert_eq!(
        active_count, 1,
        "stale replay must not toggle durable state"
    );

    let service_one = catalog_live_service(&dsn).await;
    let service_two = catalog_live_service(&dsn).await;
    service_one
        .runtime_snapshot()
        .upgrade_and_validate_catalog_provenance()
        .await
        .expect("first instance validates durable provenance on restart");
    service_two
        .runtime_snapshot()
        .upgrade_and_validate_catalog_provenance()
        .await
        .expect("second instance validates durable provenance on restart");
    service_one
        .reconcile_durable_active_project_catalogs()
        .await
        .expect("first instance restart hydration");
    service_two
        .reconcile_durable_active_project_catalogs()
        .await
        .expect("second instance restart hydration");
    for service in [&service_one, &service_two] {
        let active = service
            .catalog
            .active_exact_for(&project_id)
            .expect("each instance publishes exact project authority");
        assert_eq!(active.metadata.checksum, staged_d.catalog.checksum_sha256);
        assert_eq!(active.metadata.version, staged_d.catalog.version);
    }

    let mut denied_request = Request::new(StageCatalogRequest {
        manifest_json: manifest.clone(),
        project_id: denied_project_id.clone(),
        reason: "cross-project denial".to_string(),
        idempotency_key: "denied-stage".to_string(),
        ..Default::default()
    });
    denied_request
        .metadata_mut()
        .insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    denied_request.metadata_mut().insert(
        "x-project-id",
        project_id.parse().expect("project metadata"),
    );
    denied_request.metadata_mut().insert(
        "x-purpose",
        "catalog.live".parse().expect("purpose metadata"),
    );
    denied_request
        .metadata_mut()
        .insert("x-scopes", "udb:admin".parse().expect("scope metadata"));
    let claim = test_claim_context(
        "catalog-live-user",
        &tenant_id,
        &project_id,
        &["udb:admin"],
        &[],
    );
    let denied =
        scope_claim_context_for_test(claim, service_one.stage_catalog_inner(denied_request))
            .await
            .expect_err("project-scoped admin must not stage another project's catalog");
    assert_eq!(denied.code(), Code::PermissionDenied);

    let config = SystemCatalogConfig::default();
    let denied_rows: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*)::BIGINT FROM {} WHERE project_id = $1",
        config.catalog_versions_relation()
    ))
    .bind(&denied_project_id)
    .fetch_one(&pool)
    .await
    .expect("count denied project rows");
    assert_eq!(denied_rows, 0, "denied project must leave no durable row");

    // A migration plan is bound to both the exact catalog transition and the
    // physical project PostgreSQL target. Superseding the catalog after
    // approval must refuse apply before any ledger operation executes.
    let migration_run = runtime
        .plan_migration(&project_id, false)
        .await
        .expect("plan against exact project catalog and routed PostgreSQL target");
    let migration_run_id: Uuid = migration_run.parse().expect("migration run UUID");
    let runs_relation = config.migration_runs_relation();
    let (planned_catalog_id, planned_checksum, target_backend, target_instance, target_provenance): (
        Option<Uuid>,
        String,
        String,
        String,
        String,
    ) = sqlx::query_as(&format!(
        "SELECT catalog_id, catalog_checksum_sha256, target_backend,
                target_instance, target_provenance_sha256
           FROM {runs_relation} WHERE run_id = $1 AND project_id = $2"
    ))
    .bind(migration_run_id)
    .bind(&project_id)
    .fetch_one(&pool)
    .await
    .expect("load persisted migration authority provenance");
    assert_eq!(
        planned_catalog_id.map(|id| id.to_string()).as_deref(),
        Some(staged_d.catalog.catalog_id.as_str())
    );
    assert_eq!(planned_checksum, staged_d.catalog.checksum_sha256);
    assert_eq!(target_backend, "postgres");
    assert!(!target_instance.is_empty());
    assert_eq!(target_provenance.len(), 64);
    runtime
        .approve_migration_plan(&project_id, &migration_run, "migration-live-approval")
        .await
        .expect("approve provenance-bound migration plan");

    let superseding = runtime
        .stage_catalog(
            &project_id,
            "2.1.0",
            &manifest,
            "supersede migration authority",
            "live-test",
            "none",
            "stage-migration-supersede",
        )
        .await
        .expect("stage superseding catalog");
    runtime
        .activate_catalog(
            &project_id,
            &superseding.catalog.catalog_id,
            "supersede migration authority",
            "live-test",
            "activate-migration-supersede",
        )
        .await
        .expect("activate superseding catalog");
    let stale_apply = runtime
        .apply_migration(&project_id, &migration_run, "migration-live-approval")
        .await
        .expect_err("migration planned against a superseded catalog must fail closed");
    assert_eq!(stale_apply.code(), Code::FailedPrecondition);
    let changed_ops: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*)::BIGINT FROM {} WHERE run_id = $1 AND status <> 'PENDING'",
        config.migration_op_ledger_relation()
    ))
    .bind(migration_run_id)
    .fetch_one(&pool)
    .await
    .expect("count migration operations changed by refused stale apply");
    assert_eq!(
        changed_ops, 0,
        "authority refusal must precede physical apply"
    );

    cleanup_catalog_project(&pool, &project_id).await;
    cleanup_catalog_project(&pool, &denied_project_id).await;
}
