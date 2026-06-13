//! Live verification that a completed `EMBED` pipeline step upserts its vector
//! into the vector backend (Qdrant). Requires live Postgres + Qdrant.
//!
//!   UDB_LIVE_OBJECT_TESTS=1 cargo test --lib \
//!     live_qdrant_embed_pipeline_upserts_vector -- --ignored --nocapture

use super::support::*;
use crate::proto::udb::core::asset::services::v1 as asset_pb;
use crate::proto::udb::core::asset::services::v1::asset_service_server::AssetService;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres+Qdrant; run with UDB_LIVE_OBJECT_TESTS=1 ... -- --ignored"]
async fn live_qdrant_embed_pipeline_upserts_vector() {
    let _guard = live_native_service_db_lock().lock().await;
    let qdrant_url =
        std::env::var("UDB_QDRANT_URL").unwrap_or_else(|_| "http://127.0.0.1:56333".to_string());
    unsafe {
        std::env::set_var("UDB_QDRANT_URL", &qdrant_url);
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
    }
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;

    let collection = "udb_asset_embeddings_it";
    unsafe {
        std::env::set_var("UDB_ASSET_VECTOR_COLLECTION", collection);
    }
    let svc = asset_service(pool.clone()).await;

    let tenant_id = Uuid::new_v4().to_string();

    // definition with a single EMBED step
    let def = svc
        .create_pipeline_definition(Request::new(asset_pb::CreatePipelineDefinitionRequest {
            tenant_id: tenant_id.clone(),
            name: "embed-only".to_string(),
            media_type: "text".to_string(),
            steps: r#"[{"name":"embed","type":"EMBED"}]"#.to_string(),
            ..Default::default()
        }))
        .await
        .expect("create_pipeline_definition")
        .into_inner();

    // asset (its asset_id is the Qdrant point id), wrapping a real storage file
    let file_id = seed_storage_file(&pool, &tenant_id).await;
    let asset = svc
        .register_asset(Request::new(asset_pb::RegisterAssetRequest {
            tenant_id: tenant_id.clone(),
            file_id,
            name: "doc-to-embed".to_string(),
            media_type: "text".to_string(),
            ..Default::default()
        }))
        .await
        .expect("register_asset")
        .into_inner();

    // start → runs EMBED inline → upserts the vector to Qdrant
    svc.start_pipeline(Request::new(asset_pb::StartPipelineRequest {
        tenant_id: tenant_id.clone(),
        definition_id: def.definition_id,
        asset_id: asset.asset_id.clone(),
        correlation_id: format!("it-{}", asset.asset_id),
        ..Default::default()
    }))
    .await
    .expect("start_pipeline");

    // assert the point landed in Qdrant (GET the point by asset_id)
    let http = reqwest::Client::new();
    let url = format!(
        "{}/collections/{collection}/points/{}",
        qdrant_url.trim_end_matches('/'),
        asset.asset_id,
    );
    let resp = http.get(&url).send().await.expect("qdrant get point");
    assert!(
        resp.status().is_success(),
        "qdrant point fetch: {}",
        resp.status()
    );
    let body: serde_json::Value = resp.json().await.expect("qdrant json");
    let vector = body["result"]["vector"].as_array();
    assert!(
        vector.map(|v| !v.is_empty()).unwrap_or(false),
        "EMBED step must have upserted a non-empty vector for the asset; got {body}"
    );

    // cleanup the test collection
    let _ = http
        .delete(format!(
            "{}/collections/{collection}",
            qdrant_url.trim_end_matches('/')
        ))
        .send()
        .await;
    cleanup_native_service_db(&pool).await;
}
