//! LIVE worker-seam tenant isolation of embedding vectors on a real Qdrant (C4).
//!
//! Two tenants embed a row with the SAME source primary key into ONE shared
//! collection through the exact write path the embedding worker uses
//! (`model::build_embedding_point` → `RuntimeVectorStore::upsert`), then:
//! * each tenant's scoped search (the `Retrieve` scope filter on the same vector
//!   store) returns only its own vector — B's same-pk upsert did not replace A's;
//! * A's delete by LOGICAL point id (`RuntimeVectorStore::delete_points`, the
//!   worker's delete) removes A's point only — B's same-pk point survives.
//!
//! Reverting `tenant_scoped_point_id` breaks both: the second upsert overwrites
//! the single shared point, and A's delete removes B's vector.
//!
//! Run with a live Qdrant + Postgres:
//!   UDB_QDRANT_URL=http://127.0.0.1:56333 cargo test --lib embedding_tenant_iso_ \
//!     -- --ignored --nocapture

use std::sync::Arc;

use uuid::Uuid;

use super::model::{build_embedding_point, merge_retrieve_scope_filter};
use super::vector_store::{RuntimeVectorStore, VectorStore};
use crate::proto::VectorSearchRequest;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::config::UdbConfig;
use crate::runtime::service::live_tests::support::{
    live_native_service_db_lock, live_pg_dsn, live_pg_pool, migrate_native_service_db,
};

const PROJECT: &str = "default";
const SOURCE: &str = "acme.docs.v1.Doc";

fn normalized(v: [f32; 4]) -> Vec<f32> {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    v.iter().map(|x| x / norm).collect()
}

fn close(a: &[f32], b: &[f32]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| (x - y).abs() < 1e-3)
}

/// The tenant's scoped search exactly as `Retrieve` issues it: the scope filter
/// from `merge_retrieve_scope_filter` (tenant + source) on the worker's vector
/// store. Returns each hit's stored vector.
async fn tenant_vectors(
    store: &RuntimeVectorStore,
    tenant: &str,
    collection: &str,
    query: [f32; 4],
) -> Vec<Vec<f32>> {
    let filter = merge_retrieve_scope_filter(tenant, SOURCE, "", None).expect("scope filter");
    store
        .search(&VectorSearchRequest {
            collection: collection.to_string(),
            vector: query.to_vec(),
            filter: crate::runtime::executor_utils::json_to_struct(&filter),
            limit: 10,
            with_vector: true,
            ..Default::default()
        })
        .await
        .expect("tenant-scoped retrieve search")
        .points
        .into_iter()
        .map(|point| point.vector)
        .collect()
}

#[tokio::test]
#[ignore = "requires live Qdrant+Postgres; runs in the CI live lane (-- --ignored)"]
async fn embedding_tenant_iso_same_pk_retrieve_and_delete_live() {
    let _guard = live_native_service_db_lock().lock().await;
    let qdrant_url =
        std::env::var("UDB_QDRANT_URL").unwrap_or_else(|_| "http://127.0.0.1:56333".to_string());
    // SAFETY: test-only env mutation, serialized by the native-service DB lock.
    unsafe {
        std::env::set_var("UDB_QDRANT_URL", &qdrant_url);
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
    }
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = live_pg_dsn();
    let runtime = Arc::new(DataBrokerRuntime::from_config(config).await);

    let collection = format!("udb_embed_iso_{}", Uuid::new_v4().simple());
    let tenant_a = Uuid::new_v4().to_string();
    let tenant_b = Uuid::new_v4().to_string();
    let pk = Uuid::new_v4().to_string(); // SAME source pk for both tenants
    let vec_a = [0.10f32, 0.20, 0.30, 0.40];
    let vec_b = [0.90f32, 0.10, 0.05, 0.02];

    let store = RuntimeVectorStore::for_routing(runtime.clone(), PROJECT, "qdrant", "");
    store
        .ensure_collection(&collection, 4, "cosine", "", &[])
        .await
        .expect("ensure embedding collection");
    for (tenant, vector) in [(&tenant_a, vec_a), (&tenant_b, vec_b)] {
        let point =
            build_embedding_point(&pk, vector.to_vec(), tenant, SOURCE).expect("embedding point");
        store
            .upsert(&collection, 4, "cosine", "", vec![point])
            .await
            .expect("worker-path embedding upsert");
    }

    // Retrieve isolation: each tenant sees exactly its own vector at the shared pk.
    let a = tenant_vectors(&store, &tenant_a, &collection, vec_b).await;
    assert_eq!(a.len(), 1, "tenant A must see exactly one point: {a:?}");
    assert!(
        close(&a[0], &normalized(vec_a)),
        "tenant A's point must hold A's vector (revert => B overwrote it): {a:?}"
    );
    let b = tenant_vectors(&store, &tenant_b, &collection, vec_a).await;
    assert_eq!(b.len(), 1, "tenant B must see exactly one point: {b:?}");
    assert!(close(&b[0], &normalized(vec_b)), "{b:?}");

    // A's worker delete by logical id removes only A's point.
    store
        .delete_points(&tenant_a, &collection, vec![pk.clone()])
        .await
        .expect("worker-path delete for tenant A");
    assert!(
        tenant_vectors(&store, &tenant_a, &collection, vec_a)
            .await
            .is_empty(),
        "tenant A's point must be gone"
    );
    let b = tenant_vectors(&store, &tenant_b, &collection, vec_b).await;
    assert_eq!(
        b.len(),
        1,
        "tenant B's same-pk point must survive A's delete (revert => deleted): {b:?}"
    );

    let _ = reqwest::Client::new()
        .delete(format!(
            "{}/collections/{collection}",
            qdrant_url.trim_end_matches('/')
        ))
        .send()
        .await;
}

/// C10: `Retrieve` on a non-Qdrant backend refuses a caller filter clause the
/// backend cannot express (`match.text` on Weaviate) with `InvalidArgument`
/// instead of silently dropping it (which would widen the query).
#[tokio::test]
#[ignore = "requires live Weaviate+Postgres; runs in the CI live lane (-- --ignored)"]
async fn embedding_retrieve_weaviate_rejects_unsupported_clause_live() {
    let Some(weaviate) =
        crate::runtime::service::live_tests::support::require_live_dsn("UDB_WEAVIATE_DSN")
    else {
        eprintln!("UDB_WEAVIATE_DSN unset — skipping");
        return;
    };
    let _guard = live_native_service_db_lock().lock().await;
    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = live_pg_dsn();
    let runtime = Arc::new(DataBrokerRuntime::from_config(config).await);
    let class = format!("UdbC10{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    let store = RuntimeVectorStore::for_routing(runtime, PROJECT, "weaviate", "");
    store
        .ensure_collection(&class, 4, "cosine", "", &[])
        .await
        .expect("ensure Weaviate class");

    let filter = merge_retrieve_scope_filter(
        &tenant,
        SOURCE,
        r#"{"must":[{"key":"title","match":{"text":"hello"}}]}"#,
        None,
    )
    .expect("the retrieve scope filter accepts a non-reserved key");
    let err = store
        .search(&VectorSearchRequest {
            collection: class.clone(),
            vector: vec![0.1, 0.2, 0.3, 0.4],
            filter: crate::runtime::executor_utils::json_to_struct(&filter),
            limit: 5,
            with_payload: true,
            ..Default::default()
        })
        .await
        .expect_err("match.text is not expressible on Weaviate and must be refused");
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err:?}");

    let _ = reqwest::Client::new()
        .delete(format!(
            "{}/v1/schema/{class}",
            weaviate.trim_end_matches('/')
        ))
        .send()
        .await;
}
