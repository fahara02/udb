//! Live Postgres verification of the storage→asset auto-trigger handler
//! (`AssetServiceImpl::handle_storage_finalized`): a finalized storage file
//! registers an asset and starts the matching pipeline, idempotently. Needs only
//! Postgres (no Kafka/MinIO/Qdrant): the Kafka consumer is thin plumbing over this
//! handler.
//!
//!   UDB_LIVE_AUTH_TESTS=1 cargo test --lib \
//!     live_pg_storage_to_asset_trigger -- --ignored --nocapture

use super::support::*;
use crate::proto::udb::core::asset::services::v1 as asset_pb;
use crate::proto::udb::core::asset::services::v1::asset_service_server::AssetService;
use crate::proto::udb::core::storage::services::v1 as storage_pb;
use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageService;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 ... -- --ignored"]
async fn live_pg_storage_to_asset_trigger() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;

    // Storage + asset services, Postgres-only (no runtime → no presign/object/vector).
    let storage = crate::runtime::service::storage_service::StorageServiceImpl::new()
        .with_postgres(Some(pool.clone()));
    let asset = crate::runtime::service::asset_service::AssetServiceImpl::new()
        .with_postgres(Some(pool.clone()));
    let tenant_id = Uuid::new_v4().to_string();

    // A finalized-ish storage file with a text content type → media_type "text".
    let reg = storage
        .register_upload(Request::new(storage_pb::RegisterUploadRequest {
            tenant_id: tenant_id.clone(),
            filename: "notes.txt".to_string(),
            content_type: "text/plain".to_string(),
            file_type: "DOCUMENT".to_string(),
            ..Default::default()
        }))
        .await
        .expect("register_upload")
        .into_inner();

    // A matching active pipeline definition (EXTRACT only → no object/vector deps).
    asset
        .create_pipeline_definition(Request::new(asset_pb::CreatePipelineDefinitionRequest {
            tenant_id: tenant_id.clone(),
            name: "text-default".to_string(),
            media_type: "text".to_string(),
            steps: r#"[{"name":"extract","type":"EXTRACT"}]"#.to_string(),
            ..Default::default()
        }))
        .await
        .expect("create_pipeline_definition");

    // Trigger: registers the asset + starts the pipeline.
    let first = asset
        .handle_storage_finalized(&reg.file_id, &tenant_id)
        .await
        .expect("trigger")
        .expect("a pipeline should start for a matching definition");

    // Idempotent: a redelivered finalize reuses the asset + dedups the pipeline.
    let second = asset
        .handle_storage_finalized(&reg.file_id, &tenant_id)
        .await
        .expect("trigger (redelivery)")
        .expect("idempotent re-trigger still returns the instance");
    assert_eq!(first, second, "trigger must be idempotent on file_id");

    // The pipeline ran the EXTRACT step to completion.
    let pipe = asset
        .get_pipeline(Request::new(asset_pb::GetPipelineRequest {
            tenant_id: tenant_id.clone(),
            instance_id: first,
        }))
        .await
        .expect("get_pipeline")
        .into_inner();
    assert_eq!(pipe.steps.len(), 1, "one EXTRACT step");
    assert_eq!(pipe.steps[0].status, 3, "EXTRACT step COMPLETED");

    // Exactly one asset exists for the file (no duplicate on re-trigger).
    let assets = asset
        .list_assets(Request::new(asset_pb::ListAssetsRequest {
            tenant_id: tenant_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("list_assets")
        .into_inner();
    assert_eq!(
        assets.total_count, 1,
        "re-trigger must not duplicate the asset"
    );

    // A finalize for a file with no matching pipeline definition is a no-op.
    let other = storage
        .register_upload(Request::new(storage_pb::RegisterUploadRequest {
            tenant_id: tenant_id.clone(),
            filename: "clip.mp4".to_string(),
            content_type: "video/mp4".to_string(),
            file_type: "VIDEO".to_string(),
            ..Default::default()
        }))
        .await
        .expect("register_upload (video)")
        .into_inner();
    let none = asset
        .handle_storage_finalized(&other.file_id, &tenant_id)
        .await
        .expect("trigger (no match)");
    assert!(none.is_none(), "no matching pipeline definition → no-op");

    cleanup_native_service_db(&pool).await;
}
