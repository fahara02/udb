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

    // 1.25: the engine point id is tenant-scoped (`{tenant}:{asset_id}`, hashed to
    // a UUID by the Qdrant seam), so the bare asset id must NOT address a point.
    let http = reqwest::Client::new();
    let base = qdrant_url.trim_end_matches('/');
    let raw = http
        .get(format!(
            "{base}/collections/{collection}/points/{}",
            asset.asset_id
        ))
        .send()
        .await
        .expect("qdrant get raw point");
    assert_eq!(
        raw.status().as_u16(),
        404,
        "the EMBED point must not be addressable by the bare (unscoped) asset id"
    );
    // The tenant's point is found through its `_tenant_id` stamp and carries a
    // non-empty vector.
    let resp = http
        .post(format!("{base}/collections/{collection}/points/scroll"))
        .json(&serde_json::json!({
            "filter": {"must": [{"key": "_tenant_id", "match": {"value": tenant_id}}]},
            "limit": 10,
            "with_payload": true,
            "with_vector": true,
        }))
        .send()
        .await
        .expect("qdrant scroll");
    assert!(
        resp.status().is_success(),
        "qdrant scroll: {}",
        resp.status()
    );
    let body: serde_json::Value = resp.json().await.expect("qdrant json");
    let points = body["result"]["points"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        points.len(),
        1,
        "exactly one EMBED point must carry this tenant's stamp; got {body}"
    );
    let vector = points[0]["vector"].as_array();
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
