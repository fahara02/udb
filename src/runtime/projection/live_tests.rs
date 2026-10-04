//! Seam test for the projection pipeline against a live Postgres + Qdrant:
//! the REAL write-path enqueue (`ProjectionEngine::enqueue_write_tasks_tx`, the
//! call the Upsert/Delete handlers make inside their transaction) → the REAL
//! `ProjectionWorker::run_once` → Qdrant → a tenant-scoped vector search with the
//! same `_tenant_id` / `_project_id` filter `VectorSearch` applies.
//!
//! Until this test nothing ran the worker: projected points carried no tenant
//! stamp (invisible to every scoped search) and two tenants' rows with one
//! primary key collided on one point, and no test could have noticed.
//!
//! Run with a live stack (CI's `--ignored` lib step provides both):
//!   UDB_INTEGRATION_PG_DSN=postgres://udb:udb@localhost:55432/udb \
//!   UDB_QDRANT_URL=http://localhost:56333 \
//!     cargo test --lib projection::live_tests -- --ignored --nocapture

use std::sync::Arc;

use serde_json::json;
use uuid::Uuid;

use super::*;
use crate::generation::manifest::{
    ManifestColumn, ManifestProjection, ManifestStoreOption, ManifestTable,
};

fn live_pg_dsn() -> String {
    std::env::var("UDB_LIVE_NATIVE_PG_DSN")
        .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
        .unwrap_or_else(|_| "postgres://udb:udb@127.0.0.1:55432/udb".to_string())
}

fn seam_manifest(collection: &str) -> CatalogManifest {
    CatalogManifest {
        checksum_sha256: format!("projection-seam-{collection}"),
        tables: vec![ManifestTable {
            message_name: "SeamDocument".to_string(),
            schema: "app".to_string(),
            table: "seam_documents".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec![
                ManifestColumn {
                    column_name: "id".to_string(),
                    ..Default::default()
                },
                ManifestColumn {
                    column_name: "tenant_id".to_string(),
                    is_tenant_column: true,
                    ..Default::default()
                },
            ],
            ..ManifestTable::default()
        }],
        projections: vec![ManifestProjection {
            message_type: "SeamDocument".to_string(),
            projection_kind: "vector".to_string(),
            backend: "qdrant".to_string(),
            resource_name: collection.to_string(),
            write_policy: "projection".to_string(),
            fanout_policy: "async_projection".to_string(),
            options: vec![ManifestStoreOption {
                key: "vector_field".to_string(),
                value: "vector".to_string(),
            }],
            ..ManifestProjection::default()
        }],
        ..CatalogManifest::default()
    }
}

async fn enqueue(
    pool: &PgPool,
    plans: &[ProjectionPlan],
    operation: &str,
    payload: &serde_json::Value,
) -> Vec<String> {
    let mut tx = pool.begin().await.expect("begin enqueue tx");
    let keys = ProjectionEngine::enqueue_write_tasks_tx(
        &mut tx,
        &SystemCatalogConfig::current(),
        crate::runtime::catalog::DEFAULT_PROJECT_ID,
        "SeamDocument",
        operation,
        payload,
        plans,
    )
    .await
    .expect("enqueue projection task");
    tx.commit().await.expect("commit enqueue tx");
    keys
}

async fn task_statuses(pool: &PgPool, keys: &[String]) -> Vec<String> {
    let rel = SystemCatalogConfig::current().projection_tasks_relation();
    sqlx::query_scalar(&format!(
        "SELECT status FROM {rel} WHERE idempotency_key = ANY($1) ORDER BY status"
    ))
    .bind(keys)
    .fetch_all(pool)
    .await
    .expect("read projection task statuses")
}

async fn scoped_search(
    runtime: &crate::runtime::DataBrokerRuntime,
    collection: &str,
    tenant: &str,
) -> Vec<serde_json::Value> {
    let request = json!({
        "collection": collection,
        "vector": [0.1, 0.2, 0.3, 0.4],
        "limit": 10,
        "with_payload": true,
        "filter": {"must": [
            {"key": "_tenant_id", "match": {"value": tenant}},
            {"key": "_project_id", "match": {"value": crate::runtime::catalog::DEFAULT_PROJECT_ID}}
        ]}
    });
    let response = runtime
        .search_backend_target("qdrant", None, &request.to_string())
        .await
        .expect("tenant-scoped qdrant search");
    serde_json::from_str::<Vec<serde_json::Value>>(&response).expect("search hits JSON")
}

#[tokio::test]
#[ignore = "requires live Postgres+Qdrant: UDB_INTEGRATION_PG_DSN + UDB_QDRANT_URL ... -- --ignored"]
async fn live_projection_worker_projects_tenant_scoped_points_into_qdrant() {
    let qdrant_url =
        std::env::var("UDB_QDRANT_URL").unwrap_or_else(|_| "http://127.0.0.1:56333".to_string());
    unsafe {
        std::env::set_var("UDB_QDRANT_URL", &qdrant_url);
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
    }
    let dsn = live_pg_dsn();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&dsn)
        .await
        .unwrap_or_else(|err| panic!("connect live projection postgres at {dsn}: {err}"));
    sqlx::query("CREATE SCHEMA IF NOT EXISTS udb_system")
        .execute(&pool)
        .await
        .expect("create udb_system schema");
    let store = crate::runtime::canonical_store::postgres::PostgresCanonicalStore::new(
        pool.clone(),
        "primary",
        "udb_system.outbox_events",
    );
    crate::runtime::canonical_store::system_store::ProjectionTaskStore::ensure_projection_tables(
        &store,
    )
    .await
    .expect("ensure projection task ledger");

    let mut config = crate::runtime::config::UdbConfig::from_env();
    config.primary.direct_dsn = dsn.clone();
    let runtime = Arc::new(crate::runtime::DataBrokerRuntime::from_config(config).await);

    let collection = format!("udb_projection_seam_{}", Uuid::new_v4().simple());
    runtime
        .ensure_resource_backend(
            "qdrant",
            &collection,
            r#"{"dimension":4,"distance":"cosine"}"#,
        )
        .await
        .expect("create seam qdrant collection");

    let manifest = seam_manifest(&collection);
    let plans = ProjectionPlan::from_manifest(&manifest);
    let catalog = Arc::new(CatalogManager::new(manifest.clone()));
    let worker = ProjectionWorker {
        store: Arc::new(store),
        runtime: runtime.clone(),
        config: SystemCatalogConfig::current(),
        settings: ProjectionWorkerSettings {
            project_id: Some(crate::runtime::catalog::DEFAULT_PROJECT_ID.to_string()),
            ..ProjectionWorkerSettings::default()
        },
        metrics: Arc::new(crate::metrics::NoopMetrics),
        catalog,
    };

    // Two tenants write a row with the SAME primary key.
    let mut keys = enqueue(
        &pool,
        &plans,
        "upsert",
        &json!({"id": "doc-1", "tenant_id": "tenant-a", "vector": [0.1, 0.2, 0.3, 0.4]}),
    )
    .await;
    keys.extend(
        enqueue(
            &pool,
            &plans,
            "upsert",
            &json!({"id": "doc-1", "tenant_id": "tenant-b", "vector": [0.4, 0.3, 0.2, 0.1]}),
        )
        .await,
    );
    worker.run_once().await;
    assert_eq!(
        task_statuses(&pool, &keys).await,
        vec!["COMPLETED".to_string(), "COMPLETED".to_string()],
        "both projection tasks must apply"
    );

    // Each tenant's scoped search finds exactly its own point: the point is
    // stamped with `_tenant_id`, and the tenant-scoped id kept B from
    // overwriting A's vector.
    for tenant in ["tenant-a", "tenant-b"] {
        let hits = scoped_search(&runtime, &collection, tenant).await;
        assert_eq!(hits.len(), 1, "{tenant}: {hits:?}");
        assert_eq!(hits[0]["payload"]["_tenant_id"], tenant, "{hits:?}");
        assert_eq!(hits[0]["payload"]["tenant_id"], tenant, "{hits:?}");
        assert_eq!(hits[0]["payload"]["id"], "doc-1", "{hits:?}");
    }

    // A delete issued by tenant A (filter names no tenant; the write path adds
    // the verified one) removes only A's point.
    let delete_payload = scoped_delete_payload(
        &manifest,
        "SeamDocument",
        &json!({"id": "doc-1"}),
        "tenant-a",
    );
    let delete_keys = enqueue(&pool, &plans, "delete", &delete_payload).await;
    worker.run_once().await;
    assert_eq!(
        task_statuses(&pool, &delete_keys).await,
        vec!["COMPLETED".to_string()]
    );
    assert!(
        scoped_search(&runtime, &collection, "tenant-a")
            .await
            .is_empty()
    );
    assert_eq!(
        scoped_search(&runtime, &collection, "tenant-b").await.len(),
        1
    );

    // Cleanup.
    let rel = SystemCatalogConfig::current().projection_tasks_relation();
    let mut all_keys = keys;
    all_keys.extend(delete_keys);
    let _ = sqlx::query(&format!(
        "DELETE FROM {rel} WHERE idempotency_key = ANY($1)"
    ))
    .bind(&all_keys)
    .execute(&pool)
    .await;
    let _ = reqwest::Client::new()
        .delete(format!(
            "{}/collections/{collection}",
            qdrant_url.trim_end_matches('/')
        ))
        .send()
        .await;
}
