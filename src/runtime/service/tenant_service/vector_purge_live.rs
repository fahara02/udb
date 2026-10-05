//! LIVE hard-purge vector erasure on a real Qdrant + Postgres (C9).
//!
//! Tenants A and B both hold points in (1) an `EnsureResource` collection whose
//! route lives ONLY in the durable route table (recorded by another runtime —
//! the purging replica never saw it in memory) and (2) the asset `EMBED`
//! collection. The hard purge's vector leg for A, run on a fresh replica, must
//! erase A's points from both while leaving B's intact, and A's scoped search
//! afterwards is empty while B's still resolves (the route loads on miss).
//!
//! Run with a live Qdrant + Postgres:
//!   UDB_QDRANT_URL=http://127.0.0.1:56333 cargo test --lib tenant_vector_purge_ \
//!     -- --ignored --nocapture

use serde_json::json;
use uuid::Uuid;

use crate::generation::CatalogManifest;
use crate::proto::{VectorPointMutation, VectorSearchRequest};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::config::UdbConfig;
use crate::runtime::executor_utils::json_to_struct;
use crate::runtime::service::DataBrokerService;
use crate::runtime::service::live_tests::support::{
    live_native_service_db_lock, live_pg_dsn, live_pg_pool, migrate_native_service_db,
};

const PROJECT: &str = "default";

async fn fresh_runtime() -> DataBrokerRuntime {
    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = live_pg_dsn();
    DataBrokerRuntime::from_config(config).await
}

fn stamped_point(tenant: &str, vector: [f32; 4]) -> VectorPointMutation {
    VectorPointMutation {
        id: Uuid::new_v4().to_string(),
        vector: vector.to_vec(),
        payload: json_to_struct(&json!({ "_tenant_id": tenant, "_project_id": PROJECT })),
        vector_name: String::new(),
    }
}

/// Exact count of `tenant`'s points in `collection`, read straight from Qdrant.
async fn qdrant_count(qdrant: &str, collection: &str, tenant: &str) -> u64 {
    let resp = reqwest::Client::new()
        .post(format!(
            "{}/collections/{collection}/points/count",
            qdrant.trim_end_matches('/')
        ))
        .json(&json!({
            "filter": { "must": [ { "key": "_tenant_id", "match": { "value": tenant } } ] },
            "exact": true,
        }))
        .send()
        .await
        .expect("qdrant count");
    assert!(
        resp.status().is_success(),
        "qdrant count: {}",
        resp.status()
    );
    let body: serde_json::Value = resp.json().await.expect("qdrant count json");
    body["result"]["count"].as_u64().expect("count")
}

async fn scoped_hits(runtime: &DataBrokerRuntime, tenant: &str, collection: &str) -> usize {
    let context = crate::RequestContext {
        tenant_id: tenant.to_string(),
        project_id: PROJECT.to_string(),
        scopes: vec!["udb:vector:read".to_string()],
        ..crate::RequestContext::default()
    };
    runtime
        .vector_search_routed(
            &CatalogManifest::default(),
            VectorSearchRequest {
                collection: collection.to_string(),
                vector: vec![0.1, 0.2, 0.3, 0.4],
                limit: 10,
                ..Default::default()
            },
            context,
            // No route override: the collection resolves through the durable
            // EnsureResource route table on this fresh replica.
            None,
        )
        .await
        .expect("scoped search on the persisted route")
        .points
        .len()
}

#[tokio::test]
#[ignore = "requires live Qdrant+Postgres; runs in the CI live lane (-- --ignored)"]
async fn tenant_vector_purge_erases_routed_and_asset_vectors_live() {
    let _guard = live_native_service_db_lock().lock().await;
    let qdrant =
        std::env::var("UDB_QDRANT_URL").unwrap_or_else(|_| "http://127.0.0.1:56333".to_string());
    let asset_collection = format!("udb_asset_purge_it_{}", Uuid::new_v4().simple());
    // SAFETY: test-only env mutation, serialized by the native-service DB lock.
    unsafe {
        std::env::set_var("UDB_QDRANT_URL", &qdrant);
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
        std::env::set_var("UDB_ASSET_VECTOR_COLLECTION", &asset_collection);
    }
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;

    let routed = format!("udb_routed_purge_it_{}", Uuid::new_v4().simple());
    let tenant_a = Uuid::new_v4().to_string();
    let tenant_b = Uuid::new_v4().to_string();

    // Replica 1 records the EnsureResource route durably and writes both tenants'
    // points into both collections.
    let writer = fresh_runtime().await;
    writer
        .persist_vector_resource_route(&tenant_a, PROJECT, &routed, "qdrant", None)
        .await
        .expect("persist routed collection");
    for collection in [&routed, &asset_collection] {
        for (tenant, vector) in [
            (&tenant_a, [0.1f32, 0.2, 0.3, 0.4]),
            (&tenant_b, [0.4f32, 0.3, 0.2, 0.1]),
        ] {
            writer
                .vector_upsert_backend_target(
                    None,
                    PROJECT,
                    collection,
                    4,
                    vec![stamped_point(tenant, vector)],
                )
                .await
                .unwrap_or_else(|err| panic!("seed {collection}: {err:?}"));
        }
        assert_eq!(qdrant_count(&qdrant, collection, &tenant_a).await, 1);
        assert_eq!(qdrant_count(&qdrant, collection, &tenant_b).await, 1);
    }

    // Replica 2 (empty in-process route map) runs the purge's vector leg.
    let broker = DataBrokerService::with_runtime(CatalogManifest::default(), fresh_runtime().await);
    let tenant_svc = broker.build_tenant_service();
    let report = super::tenant_purge::purge_tenant_vector_stores(
        &tenant_svc,
        &CatalogManifest::default(),
        &tenant_a,
    )
    .await;
    for collection in [&routed, &asset_collection] {
        assert!(
            report
                .purged
                .iter()
                .any(|entry| entry["table"] == serde_json::json!(collection)),
            "purge report must list {collection}: purged={:?} excluded={:?}",
            report.purged,
            report.excluded
        );
        assert_eq!(
            qdrant_count(&qdrant, collection, &tenant_a).await,
            0,
            "tenant A's vectors must be erased from {collection}"
        );
        assert_eq!(
            qdrant_count(&qdrant, collection, &tenant_b).await,
            1,
            "tenant B's vectors in {collection} must survive A's purge"
        );
    }

    // A's scoped search is empty; B's still resolves and finds its point.
    let reader = fresh_runtime().await;
    assert_eq!(scoped_hits(&reader, &tenant_a, &routed).await, 0);
    assert_eq!(scoped_hits(&reader, &tenant_b, &routed).await, 1);

    let http = reqwest::Client::new();
    for collection in [&routed, &asset_collection] {
        let _ = http
            .delete(format!(
                "{}/collections/{collection}",
                qdrant.trim_end_matches('/')
            ))
            .send()
            .await;
    }
    // SAFETY: as above.
    unsafe {
        std::env::remove_var("UDB_ASSET_VECTOR_COLLECTION");
    }
}
