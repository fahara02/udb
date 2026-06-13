//! Live verification of the asset THUMBNAIL byte-step (feature `asset-image`):
//! a source image is stored in the object store, an asset wraps it, and a
//! THUMBNAIL pipeline fetches → resizes → stores a derived object. Requires live
//! Postgres + MinIO and the `asset-image` feature.
//!
//!   UDB_LIVE_OBJECT_TESTS=1 cargo test --lib --features asset-image \
//!     live_minio_asset_thumbnail_pipeline -- --ignored --nocapture
#![cfg(feature = "asset-image")]

use super::support::*;
use crate::proto::udb::core::asset::services::v1 as asset_pb;
use crate::proto::udb::core::asset::services::v1::asset_service_server::AssetService;
use crate::proto::udb::core::storage::services::v1 as storage_pb;
use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageService;
use crate::runtime::core::setup_data::object_request_json;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres+MinIO + --features asset-image"]
async fn live_minio_asset_thumbnail_pipeline() {
    let _guard = live_native_service_db_lock().lock().await;
    unsafe {
        std::env::set_var(
            "UDB_MINIO_ENDPOINT",
            std::env::var("UDB_MINIO_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19000".to_string()),
        );
        std::env::set_var(
            "UDB_MINIO_ACCESS_KEY",
            std::env::var("UDB_MINIO_ACCESS_KEY").unwrap_or_else(|_| "udbminio".to_string()),
        );
        std::env::set_var(
            "UDB_MINIO_SECRET_KEY",
            std::env::var("UDB_MINIO_SECRET_KEY").unwrap_or_else(|_| "udbminio123".to_string()),
        );
        std::env::set_var(
            "UDB_MINIO_REGION",
            std::env::var("UDB_MINIO_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        );
        std::env::set_var("UDB_STORAGE_BUCKET", "udb-storage");
        std::env::set_var("UDB_STORAGE_OBJECT_BACKEND", "minio");
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
    }
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let runtime = live_runtime().await;

    let storage = crate::runtime::service::storage_service::StorageServiceImpl::new()
        .with_postgres(Some(pool.clone()))
        .with_object(
            Some(runtime.clone()),
            "minio".to_string(),
            "udb-storage".to_string(),
        );
    let asset = crate::runtime::service::asset_service::AssetServiceImpl::new()
        .with_postgres(Some(pool.clone()))
        .with_vector(Some(runtime.clone()), "udb_asset_embeddings_it".to_string());

    let tenant_id = Uuid::new_v4().to_string();

    // 1. register a storage file (mints object_key) and upload a real PNG to it.
    let reg = storage
        .register_upload(Request::new(storage_pb::RegisterUploadRequest {
            tenant_id: tenant_id.clone(),
            filename: "src.png".to_string(),
            content_type: "image/png".to_string(),
            file_type: "IMAGE".to_string(),
            ..Default::default()
        }))
        .await
        .expect("register_upload")
        .into_inner();

    // a 512x512 red PNG
    let mut src = image::RgbImage::new(512, 512);
    for p in src.pixels_mut() {
        *p = image::Rgb([200, 30, 30]);
    }
    let mut png = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgb8(src)
        .write_to(&mut png, image::ImageFormat::Png)
        .expect("encode src png");
    runtime
        .put_object_backend_target(
            "minio",
            None,
            &object_request_json("put", "udb-storage", &reg.object_key, "image/png"),
            png.into_inner(),
        )
        .await
        .expect("upload source png");

    // 2. wrap it as an asset
    let asset_rec = asset
        .register_asset(Request::new(asset_pb::RegisterAssetRequest {
            tenant_id: tenant_id.clone(),
            file_id: reg.file_id.clone(),
            name: "picture".to_string(),
            media_type: "image".to_string(),
            ..Default::default()
        }))
        .await
        .expect("register_asset")
        .into_inner();

    // 3. THUMBNAIL pipeline
    let def = asset
        .create_pipeline_definition(Request::new(asset_pb::CreatePipelineDefinitionRequest {
            tenant_id: tenant_id.clone(),
            name: "thumb".to_string(),
            media_type: "image".to_string(),
            steps: r#"[{"name":"thumb","type":"THUMBNAIL"}]"#.to_string(),
            ..Default::default()
        }))
        .await
        .expect("create_pipeline_definition")
        .into_inner();
    let start = asset
        .start_pipeline(Request::new(asset_pb::StartPipelineRequest {
            tenant_id: tenant_id.clone(),
            definition_id: def.definition_id,
            asset_id: asset_rec.asset_id.clone(),
            correlation_id: format!("it-{}", asset_rec.asset_id),
            ..Default::default()
        }))
        .await
        .expect("start_pipeline")
        .into_inner();

    // 4. the THUMBNAIL step completed and produced a derived object
    let pipe = asset
        .get_pipeline(Request::new(asset_pb::GetPipelineRequest {
            tenant_id: tenant_id.clone(),
            instance_id: start.instance_id,
        }))
        .await
        .expect("get_pipeline")
        .into_inner();
    let step = pipe.steps.first().expect("one step");
    assert_eq!(
        step.status, 3,
        "THUMBNAIL step must be COMPLETED; result={}",
        step.result
    );
    let result: serde_json::Value = serde_json::from_str(&step.result).unwrap_or_default();
    let derived_key = result["derived_object_key"]
        .as_str()
        .expect("derived_object_key in step result");

    // 5. the derived object is a valid, smaller PNG
    let derived = runtime
        .get_object_backend_target(
            "minio",
            None,
            &object_request_json("get", "udb-storage", derived_key, ""),
        )
        .await
        .expect("fetch derived object");
    let thumb = image::load_from_memory(&derived).expect("derived object is a valid image");
    assert!(
        thumb.width() <= 256 && thumb.height() <= 256,
        "thumbnail must be downscaled"
    );

    // cleanup
    let _ = runtime
        .delete_object_backend_target(
            "minio",
            None,
            "default",
            &object_request_json("delete", "udb-storage", &reg.object_key, ""),
        )
        .await;
    let _ = runtime
        .delete_object_backend_target(
            "minio",
            None,
            "default",
            &object_request_json("delete", "udb-storage", derived_key, ""),
        )
        .await;
    cleanup_native_service_db(&pool).await;
}
