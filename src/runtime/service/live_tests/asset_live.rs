use super::support::*;
use crate::proto::udb::core::asset::services::v1 as asset_pb;
use crate::proto::udb::core::asset::services::v1::asset_service_server::AssetService;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_asset_native_schema_from_proto -- --ignored --nocapture"]
async fn live_postgres_asset_native_schema_from_proto() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;

    assert_native_table_columns(
        &pool,
        "udb.core.asset.entity.v1.PipelineInstance",
        &[
            "instance_id",
            "definition_id",
            "asset_id",
            "tenant_id",
            "status",
            "correlation_id",
            "audit_info",
        ],
    )
    .await;
    assert_native_table_columns(
        &pool,
        "udb.core.asset.entity.v1.PipelineStep",
        &[
            "step_id",
            "instance_id",
            "tenant_id",
            "step_name",
            "step_type",
            "status",
        ],
    )
    .await;

    cleanup_native_service_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_asset_pipeline_roundtrip -- --ignored --nocapture"]
async fn live_postgres_asset_pipeline_roundtrip() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let svc = asset_service(pool.clone()).await;
    let tenant_id = Uuid::new_v4().to_string();

    // a definition with two steps that auto-complete in-process (EXTRACT + EMBED
    // are pure-Rust; no object bytes / external worker required)
    let def = svc
        .create_pipeline_definition(Request::new(asset_pb::CreatePipelineDefinitionRequest {
            tenant_id: tenant_id.clone(),
            name: "text-default".to_string(),
            media_type: "text".to_string(),
            steps: r#"[{"name":"extract","type":"EXTRACT"},{"name":"embed","type":"EMBED"}]"#
                .to_string(),
            ..Default::default()
        }))
        .await
        .expect("create_pipeline_definition")
        .into_inner();
    assert!(!def.definition_id.is_empty());

    // an asset (wrapping a real tenant-owned storage file)
    let file_id = seed_storage_file(&pool, &tenant_id).await;
    let asset = svc
        .register_asset(Request::new(asset_pb::RegisterAssetRequest {
            tenant_id: tenant_id.clone(),
            file_id,
            name: "notes.txt".to_string(),
            media_type: "text".to_string(),
            ..Default::default()
        }))
        .await
        .expect("register_asset")
        .into_inner();

    // start the pipeline (idempotent on correlation_id)
    let corr = format!("asset-{}", asset.asset_id);
    let start = svc
        .start_pipeline(Request::new(asset_pb::StartPipelineRequest {
            tenant_id: tenant_id.clone(),
            definition_id: def.definition_id.clone(),
            asset_id: asset.asset_id.clone(),
            correlation_id: corr.clone(),
            ..Default::default()
        }))
        .await
        .expect("start_pipeline")
        .into_inner();
    let instance_id = start.instance_id.clone();

    // re-start with the same correlation_id → same instance (idempotent)
    let restart = svc
        .start_pipeline(Request::new(asset_pb::StartPipelineRequest {
            tenant_id: tenant_id.clone(),
            definition_id: def.definition_id.clone(),
            asset_id: asset.asset_id.clone(),
            correlation_id: corr,
            ..Default::default()
        }))
        .await
        .expect("start_pipeline (idempotent)")
        .into_inner();
    assert_eq!(
        restart.instance_id, instance_id,
        "correlation_id must dedup"
    );

    // start_pipeline RAN both steps in-process — no manual complete_step.
    let pipe = svc
        .get_pipeline(Request::new(asset_pb::GetPipelineRequest {
            tenant_id,
            instance_id,
        }))
        .await
        .expect("get_pipeline")
        .into_inner();
    assert_eq!(pipe.steps.len(), 2);
    // every step auto-completed: StepStatus::Completed == 3
    for step in &pipe.steps {
        assert_eq!(
            step.status, 3,
            "step {} should be COMPLETED, got {}",
            step.step_name, step.status
        );
    }
    // instance rolled to terminal COMPLETED: PipelineStatus::Completed == 3
    assert_eq!(pipe.instance.expect("instance").status, 3);

    cleanup_native_service_db(&pool).await;
}

/// §1 read-after-write (13.7.1.2): the ids `RegisterAsset` and `StartPipeline`
/// return are IMMEDIATELY gettable by `GetAsset`/`GetPipeline` on the SAME served
/// path with the SAME tenant metadata. Reverting either get guarantee fails this.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_asset_read_after_write -- --ignored --nocapture"]
async fn live_postgres_asset_read_after_write() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let svc = asset_service(pool.clone()).await;
    let tenant_id = Uuid::new_v4().to_string();

    let def = svc
        .create_pipeline_definition(Request::new(asset_pb::CreatePipelineDefinitionRequest {
            tenant_id: tenant_id.clone(),
            name: "ryw-default".to_string(),
            media_type: "text".to_string(),
            steps: r#"[{"name":"extract","type":"EXTRACT"},{"name":"embed","type":"EMBED"}]"#
                .to_string(),
            ..Default::default()
        }))
        .await
        .expect("create_pipeline_definition")
        .into_inner();

    let file_id = seed_storage_file(&pool, &tenant_id).await;
    let asset = svc
        .register_asset(Request::new(asset_pb::RegisterAssetRequest {
            tenant_id: tenant_id.clone(),
            file_id,
            name: "ryw.txt".to_string(),
            media_type: "text".to_string(),
            ..Default::default()
        }))
        .await
        .expect("register_asset")
        .into_inner();

    // RegisterAsset → GetAsset
    assert_create_then_get("RegisterAsset→GetAsset", &asset.asset_id, |id| {
        let svc = &svc;
        let tenant_id = tenant_id.clone();
        async move {
            let got = svc
                .get_asset(Request::new(asset_pb::GetAssetRequest {
                    tenant_id,
                    asset_id: id.clone(),
                }))
                .await?
                .into_inner()
                .asset;
            Ok(got.is_some_and(|a| a.asset_id == id))
        }
    })
    .await;

    let start = svc
        .start_pipeline(Request::new(asset_pb::StartPipelineRequest {
            tenant_id: tenant_id.clone(),
            definition_id: def.definition_id.clone(),
            asset_id: asset.asset_id.clone(),
            correlation_id: format!("ryw-{}", asset.asset_id),
            ..Default::default()
        }))
        .await
        .expect("start_pipeline")
        .into_inner();

    // StartPipeline → GetPipeline (also exercises the 04.2.1 inline-steps path:
    // start returns the steps without a proof GetPipeline round-trip).
    assert!(
        !start.steps.is_empty(),
        "StartPipeline must return inline steps (04.2.1) so startAndWait needs no proof GetPipeline"
    );
    assert_create_then_get("StartPipeline→GetPipeline", &start.instance_id, |id| {
        let svc = &svc;
        let tenant_id = tenant_id.clone();
        async move {
            let pipe = svc
                .get_pipeline(Request::new(asset_pb::GetPipelineRequest {
                    tenant_id,
                    instance_id: id.clone(),
                }))
                .await?
                .into_inner();
            Ok(pipe.instance.is_some_and(|i| i.instance_id == id))
        }
    })
    .await;

    cleanup_native_service_db(&pool).await;
}
