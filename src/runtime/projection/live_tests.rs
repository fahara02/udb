//! Seam tests for the projection pipeline against live backends. Every test
//! drives the REAL `ProjectionWorker::run_once` (claim → supersede → render →
//! target write → mark) and reads the outcome back from the target store,
//! tenant-scoped:
//!
//! * the SERVED write path — `DataBroker::{upsert, update, delete}` on a
//!   `DataBrokerService`, which enqueue projection tasks inside the write
//!   transaction — feeding Qdrant (read back through the served `VectorSearch`)
//!   and MinIO (D1, D3, D7, D13);
//! * the task ledger's ordering and lease rules on Postgres (D8, D9, D10);
//! * the document / cache / analytical targets once their DSNs reach the live
//!   lane (D2 MongoDB, D4 Redis, D5 ClickHouse).
//!
//! Run with a live stack (CI's native `--ignored` lib step provides it):
//!   UDB_INTEGRATION_PG_DSN=postgres://udb:udb@localhost:55432/udb \
//!   UDB_QDRANT_URL=http://localhost:56333 \
//!     cargo test --lib projection::live_tests -- --ignored --nocapture --test-threads=1

use std::sync::{Arc, RwLock};

use serde_json::json;
use uuid::Uuid;

use super::*;
use crate::generation::manifest::{
    ManifestColumn, ManifestProjection, ManifestStore, ManifestStoreOption, ManifestTable,
    ManifestTableSecurity,
};
use crate::proto::data_broker_server::DataBroker;
use crate::runtime::canonical_store::postgres::PostgresCanonicalStore;
use crate::runtime::canonical_store::system_store::ProjectionTaskStore;
use crate::runtime::executor_utils::{json_to_struct, struct_to_json};
use crate::runtime::service::DataBrokerService;

const PROJECT: &str = crate::runtime::catalog::DEFAULT_PROJECT_ID;
const FULL_SCOPES: &str = "udb:admin,udb:read,udb:write,udb:vector:read,udb:vector:write";

fn live_pg_dsn() -> String {
    std::env::var("UDB_LIVE_NATIVE_PG_DSN")
        .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
        .unwrap_or_else(|_| "postgres://udb:udb@127.0.0.1:55432/udb".to_string())
}

/// A backend DSN for the live lane: `None` outside it (a local `--ignored` run
/// without that backend skips), a panic inside it (a missing DSN must fail the
/// lane, not report a green test that verified nothing).
fn require_backend_dsn(names: &[&str]) -> Option<String> {
    crate::runtime::service::live_tests::support::require_live_dsn_any(names)
}

fn qdrant_url() -> String {
    let url =
        std::env::var("UDB_QDRANT_URL").unwrap_or_else(|_| "http://127.0.0.1:56333".to_string());
    // SAFETY: test-only env mutation; the live lane runs single-threaded and
    // these tests also hold the native-service DB lock.
    unsafe {
        std::env::set_var("UDB_QDRANT_URL", &url);
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
    }
    url
}

async fn ledger_pool() -> PgPool {
    let dsn = live_pg_dsn();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(6)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&dsn)
        .await
        .unwrap_or_else(|err| panic!("connect live projection postgres at {dsn}: {err}"));
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("bootstrap the UDB system catalog (projection ledger incl. row_revision)");
    pool
}

async fn ledger_store(pool: &PgPool) -> PostgresCanonicalStore {
    let store = PostgresCanonicalStore::new(pool.clone(), "primary", "udb_system.outbox_events");
    ProjectionTaskStore::ensure_projection_tables(&store)
        .await
        .expect("ensure projection task ledger");
    store
}

fn worker(
    store: PostgresCanonicalStore,
    runtime: Arc<crate::runtime::DataBrokerRuntime>,
    catalog: Arc<CatalogManager>,
) -> ProjectionWorker {
    ProjectionWorker {
        store: Arc::new(store),
        runtime,
        config: SystemCatalogConfig::current(),
        settings: ProjectionWorkerSettings {
            project_id: Some(PROJECT.to_string()),
            batch_size: 500,
            task_lease_secs: MIN_TASK_LEASE_SECS,
            ..ProjectionWorkerSettings::default()
        },
        metrics: Arc::new(crate::metrics::NoopMetrics),
        catalog,
    }
}

async fn runtime_from_env() -> Arc<crate::runtime::DataBrokerRuntime> {
    let mut config = crate::runtime::config::UdbConfig::from_env();
    config.primary.direct_dsn = live_pg_dsn();
    Arc::new(crate::runtime::DataBrokerRuntime::from_config(config).await)
}

fn opt(key: &str, value: &str) -> ManifestStoreOption {
    ManifestStoreOption {
        key: key.to_string(),
        value: value.to_string(),
    }
}

fn text_col(name: &str, is_primary: bool) -> ManifestColumn {
    ManifestColumn {
        field_name: name.to_string(),
        column_name: name.to_string(),
        proto_type: "string".to_string(),
        sql_type: "TEXT".to_string(),
        is_primary,
        not_null: is_primary || name == "tenant_id",
        is_tenant_column: name == "tenant_id",
        ..ManifestColumn::default()
    }
}

fn projection(
    message_type: &str,
    kind: &str,
    backend: &str,
    resource: &str,
    options: Vec<ManifestStoreOption>,
) -> ManifestProjection {
    ManifestProjection {
        message_type: message_type.to_string(),
        projection_kind: kind.to_string(),
        backend: backend.to_string(),
        resource_name: resource.to_string(),
        write_policy: "projection".to_string(),
        fanout_policy: "async_projection".to_string(),
        options,
        ..ManifestProjection::default()
    }
}

/// A ledger-only manifest (no physical source table): tasks are enqueued
/// through the write path's own `enqueue_write_tasks_tx`.
fn ledger_manifest(tag: &str, projections: Vec<ManifestProjection>) -> CatalogManifest {
    CatalogManifest {
        checksum_sha256: format!("projection-seam-{tag}"),
        tables: vec![ManifestTable {
            message_name: "SeamDocument".to_string(),
            schema: "app".to_string(),
            table: "seam_documents".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec![text_col("id", true), text_col("tenant_id", false)],
            ..ManifestTable::default()
        }],
        projections,
        ..CatalogManifest::default()
    }
}

fn vector_projection(collection: &str) -> ManifestProjection {
    projection(
        "SeamDocument",
        "vector",
        "qdrant",
        collection,
        vec![opt("vector_field", "vector")],
    )
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
        PROJECT,
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

async fn task_field(pool: &PgPool, key: &str, column: &str) -> String {
    let rel = SystemCatalogConfig::current().projection_tasks_relation();
    sqlx::query_scalar(&format!(
        "SELECT {column}::TEXT FROM {rel} WHERE idempotency_key = $1"
    ))
    .bind(key)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|err| panic!("read projection task {column} for {key}: {err}"))
}

async fn set_task_state(pool: &PgPool, key: &str, assignment: &str) {
    let rel = SystemCatalogConfig::current().projection_tasks_relation();
    sqlx::query(&format!(
        "UPDATE {rel} SET {assignment} WHERE idempotency_key = $1"
    ))
    .bind(key)
    .execute(pool)
    .await
    .unwrap_or_else(|err| panic!("set projection task state ({assignment}) for {key}: {err}"));
}

async fn delete_tasks_for_resource(pool: &PgPool, resource: &str) {
    let rel = SystemCatalogConfig::current().projection_tasks_relation();
    let _ = sqlx::query(&format!("DELETE FROM {rel} WHERE resource_name = $1"))
        .bind(resource)
        .execute(pool)
        .await;
}

async fn create_qdrant_collection(qdrant: &str, collection: &str) {
    let response = reqwest::Client::new()
        .put(format!(
            "{}/collections/{collection}",
            qdrant.trim_end_matches('/')
        ))
        .json(&json!({"vectors": {"size": 4, "distance": "Cosine"}}))
        .send()
        .await
        .unwrap_or_else(|err| panic!("create qdrant collection {collection}: {err}"));
    assert!(
        response.status().is_success(),
        "create qdrant collection {collection}: {}",
        response.status()
    );
}

async fn drop_qdrant_collection(qdrant: &str, collection: &str) {
    let _ = reqwest::Client::new()
        .delete(format!(
            "{}/collections/{collection}",
            qdrant.trim_end_matches('/')
        ))
        .send()
        .await;
}

/// Tenant-scoped point payloads straight from Qdrant (the same `_tenant_id` /
/// `_project_id` filter `VectorSearch` ANDs in).
async fn scoped_points(
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
            {"key": "_project_id", "match": {"value": PROJECT}}
        ]}
    });
    let response = runtime
        .search_backend_target("qdrant", None, &request.to_string())
        .await
        .expect("tenant-scoped qdrant search");
    serde_json::from_str::<Vec<serde_json::Value>>(&response)
        .expect("search hits JSON")
        .into_iter()
        .map(|hit| hit["payload"].clone())
        .collect()
}

// ── Served harness ────────────────────────────────────────────────────────────

fn header_security() -> crate::runtime::security::SecurityConfig {
    crate::runtime::security::SecurityConfig {
        tls_required: false,
        service_identity_required: false,
        mtls_required: false,
        allow_header_scopes: true,
        ..crate::runtime::security::SecurityConfig::default()
    }
}

/// A served `DataBrokerService` on a live PG runtime, default-allow authz (the
/// projection seam is under test here, not policy), header credentials.
async fn served_service(mut manifest: CatalogManifest) -> DataBrokerService {
    if manifest.checksum_sha256.trim().is_empty() {
        manifest.checksum_sha256 = format!("sha256:{}", Uuid::new_v4().simple());
    }
    crate::runtime::security::SecurityConfig::install_global(header_security());
    let mut config = crate::runtime::config::UdbConfig::from_env();
    config.primary.direct_dsn = live_pg_dsn();
    config.security = header_security();
    let runtime = crate::runtime::DataBrokerRuntime::from_config(config).await;
    let lifecycle = Arc::new(RwLock::new(crate::engine::FsmState::Completed));
    let metrics: Arc<dyn MetricsRecorder> = Arc::new(crate::metrics::NoopMetrics);
    DataBrokerService::with_runtime_and_state(manifest, runtime, lifecycle, metrics, None, true)
}

fn with_ctx<T>(message: T, tenant: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(message);
    let md = req.metadata_mut();
    md.insert("x-tenant-id", tenant.parse().unwrap());
    md.insert("x-purpose", "admin".parse().unwrap());
    md.insert("x-scopes", FULL_SCOPES.parse().unwrap());
    req
}

async fn served_upsert(
    svc: &DataBrokerService,
    tenant: &str,
    message: &str,
    record: serde_json::Value,
) {
    svc.upsert(with_ctx(
        crate::proto::UpsertRequest {
            message_type: message.to_string(),
            record_json: serde_json::to_vec(&record).unwrap(),
            ..crate::proto::UpsertRequest::default()
        },
        tenant,
    ))
    .await
    .unwrap_or_else(|err| panic!("served Upsert {record}: {err:?}"));
}

async fn served_delete(
    svc: &DataBrokerService,
    tenant: &str,
    message: &str,
    filter: serde_json::Value,
) {
    svc.delete(with_ctx(
        crate::proto::DeleteRequest {
            message_type: message.to_string(),
            filter: json_to_struct(&filter),
            ..crate::proto::DeleteRequest::default()
        },
        tenant,
    ))
    .await
    .unwrap_or_else(|err| panic!("served Delete {filter}: {err:?}"));
}

async fn served_vector_payloads(
    svc: &DataBrokerService,
    tenant: &str,
    collection: &str,
) -> Vec<serde_json::Value> {
    let response = svc
        .vector_search(with_ctx(
            crate::proto::VectorSearchRequest {
                collection: collection.to_string(),
                vector: vec![0.1, 0.2, 0.3, 0.4],
                limit: 10,
                with_payload: true,
                ..crate::proto::VectorSearchRequest::default()
            },
            tenant,
        ))
        .await
        .unwrap_or_else(|err| panic!("served VectorSearch for {tenant}: {err:?}"))
        .into_inner();
    response
        .points
        .iter()
        .map(|point| {
            point
                .payload
                .as_ref()
                .map(struct_to_json)
                .unwrap_or(serde_json::Value::Null)
        })
        .collect()
}

/// A served table `<schema>.<table>` (id TEXT PK, tenant_id TEXT) plus
/// `extra` columns, tenant-scoped on `tenant_id`, with `projections`.
fn served_manifest(
    schema: &str,
    table: &str,
    message: &str,
    extra: Vec<ManifestColumn>,
    projections: Vec<ManifestProjection>,
    stores: Vec<ManifestStore>,
) -> CatalogManifest {
    // Tenant-composite key: the seams below give two tenants the SAME id. On a
    // bare-id key the second write is a cross-tenant takeover Upsert refuses.
    let mut tenant = text_col("tenant_id", false);
    tenant.is_primary = true;
    let mut columns = vec![text_col("id", true), tenant];
    columns.extend(extra);
    CatalogManifest {
        tables: vec![ManifestTable {
            proto_package: "acme.proj.v1".to_string(),
            message_name: message.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            primary_key: vec!["tenant_id".to_string(), "id".to_string()],
            table_security: ManifestTableSecurity {
                tenant_column: "tenant_id".to_string(),
                ..ManifestTableSecurity::default()
            },
            columns,
            ..ManifestTable::default()
        }],
        projections,
        stores,
        ..CatalogManifest::default()
    }
}

async fn create_source_table(pool: &PgPool, schema: &str, table: &str, extra_ddl: &str) {
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(pool)
        .await
        .expect("create throwaway projection source schema");
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".\"{table}\" \
         (id TEXT NOT NULL, tenant_id TEXT NOT NULL{extra_ddl},          PRIMARY KEY (tenant_id, id))"
    ))
    .execute(pool)
    .await
    .expect("create projection source table");
}

async fn drop_schema(pool: &PgPool, schema: &str) {
    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(pool)
        .await;
}

// ── D13 + D7: served writes → worker → served tenant-scoped VectorSearch ─────

/// D13: two tenants upsert a row with the SAME primary key through the served
/// `Upsert`; one `ProjectionWorker::run_once` projects both into Qdrant; each
/// tenant's served `VectorSearch` sees exactly its own point.
/// D7: a served `Delete` by tenant A (the write path hands the worker A's
/// VERIFIED tenant) removes A's point and leaves B's.
#[tokio::test]
#[ignore = "requires live Postgres+Qdrant (UDB_INTEGRATION_PG_DSN + UDB_QDRANT_URL); runs in the CI --ignored live lane"]
async fn live_served_writes_project_tenant_scoped_points_into_qdrant() {
    let _guard = crate::runtime::service::live_tests::support::live_native_service_db_lock()
        .lock()
        .await;
    let qdrant = qdrant_url();
    let pool = ledger_pool().await;
    let store = ledger_store(&pool).await;

    let schema = format!("udb_proj_{}", Uuid::new_v4().simple());
    let collection = format!("udb_projection_seam_{}", Uuid::new_v4().simple());
    const MSG: &str = "acme.proj.v1.SeamVector";
    create_source_table(&pool, &schema, "seam_vectors", ", vector JSONB").await;
    create_qdrant_collection(&qdrant, &collection).await;

    let mut vector = text_col("vector", false);
    vector.sql_type = "JSONB".to_string();
    vector.is_jsonb = true;
    let svc = served_service(served_manifest(
        &schema,
        "seam_vectors",
        "SeamVector",
        vec![vector],
        vec![projection(
            "SeamVector",
            "vector",
            "qdrant",
            &collection,
            vec![opt("vector_field", "vector")],
        )],
        vec![ManifestStore {
            store_kind: "vector".to_string(),
            backend: "qdrant".to_string(),
            resource_name: collection.clone(),
            options: vec![opt("dimension", "4")],
            ..ManifestStore::default()
        }],
    ))
    .await;
    let worker = worker(store, svc.runtime_snapshot(), svc.catalog.clone());

    let tenant_a = Uuid::new_v4().to_string();
    let tenant_b = Uuid::new_v4().to_string();
    served_upsert(
        &svc,
        &tenant_a,
        MSG,
        json!({"id": "doc-1", "tenant_id": tenant_a, "vector": [0.1, 0.2, 0.3, 0.4]}),
    )
    .await;
    served_upsert(
        &svc,
        &tenant_b,
        MSG,
        json!({"id": "doc-1", "tenant_id": tenant_b, "vector": [0.4, 0.3, 0.2, 0.1]}),
    )
    .await;
    worker.run_once().await;

    for tenant in [&tenant_a, &tenant_b] {
        let hits = served_vector_payloads(&svc, tenant, &collection).await;
        assert_eq!(hits.len(), 1, "{tenant}: {hits:?}");
        assert_eq!(hits[0]["_tenant_id"], tenant.as_str(), "{hits:?}");
        assert_eq!(hits[0]["tenant_id"], tenant.as_str(), "{hits:?}");
        assert_eq!(hits[0]["id"], "doc-1", "{hits:?}");
    }

    // D7: tenant A deletes ITS doc-1; B's point with the same id survives.
    served_delete(
        &svc,
        &tenant_a,
        MSG,
        json!({"id": "doc-1", "tenant_id": tenant_a}),
    )
    .await;
    worker.run_once().await;
    assert!(
        served_vector_payloads(&svc, &tenant_a, &collection)
            .await
            .is_empty(),
        "tenant A's projected point must be gone after A's served Delete"
    );
    let b_hits = served_vector_payloads(&svc, &tenant_b, &collection).await;
    assert_eq!(
        b_hits.len(),
        1,
        "tenant B's point must survive A's delete: {b_hits:?}"
    );
    assert_eq!(b_hits[0]["_tenant_id"], tenant_b.as_str());

    delete_tasks_for_resource(&pool, &collection).await;
    drop_qdrant_collection(&qdrant, &collection).await;
    drop_schema(&pool, &schema).await;
}

// ── Postgres full-text + Qdrant dense hybrid over a projected collection ──────

/// Payload ids of a served hybrid/vector result, in result order.
fn payload_ids(set: &crate::proto::VectorSet) -> Vec<String> {
    set.points
        .iter()
        .map(|point| {
            point
                .payload
                .as_ref()
                .map(struct_to_json)
                .and_then(|payload| {
                    payload
                        .get("id")
                        .and_then(|id| id.as_str())
                        .map(str::to_string)
                })
                .unwrap_or_default()
        })
        .collect()
}

/// A projection declaring `fts_columns` makes served `VectorHybridSearch` fuse
/// Postgres full-text search over the SOURCE table with the Qdrant kNN leg:
/// each tenant gets only its own documents, and a document that matches the
/// text but is not among the dense candidates (prefetch 1) still comes back,
/// with its Qdrant payload. `payload_fields` keeps the vector out of the point
/// payload and an array column stays a JSON array a match filter can hit.
#[tokio::test]
#[ignore = "requires live Postgres+Qdrant (UDB_INTEGRATION_PG_DSN + UDB_QDRANT_URL); runs in the CI --ignored live lane"]
async fn live_pg_fts_hybrid_search_fuses_own_tenant_text_and_dense_hits() {
    let _guard = crate::runtime::service::live_tests::support::live_native_service_db_lock()
        .lock()
        .await;
    let qdrant = qdrant_url();
    let pool = ledger_pool().await;
    let store = ledger_store(&pool).await;

    let schema = format!("udb_proj_{}", Uuid::new_v4().simple());
    let collection = format!("udb_fts_hybrid_{}", Uuid::new_v4().simple());
    const MSG: &str = "acme.proj.v1.FtsDoc";
    create_source_table(
        &pool,
        &schema,
        "fts_docs",
        ", vector JSONB, body TEXT, topic_ids JSONB",
    )
    .await;
    create_qdrant_collection(&qdrant, &collection).await;

    let mut vector = text_col("vector", false);
    vector.sql_type = "JSONB".to_string();
    vector.is_jsonb = true;
    let mut topic_ids = text_col("topic_ids", false);
    topic_ids.sql_type = "JSONB".to_string();
    topic_ids.is_jsonb = true;
    let svc = served_service(served_manifest(
        &schema,
        "fts_docs",
        "FtsDoc",
        vec![vector, text_col("body", false), topic_ids],
        vec![projection(
            "FtsDoc",
            "vector",
            "qdrant",
            &collection,
            vec![
                opt("vector_field", "vector"),
                opt("fts_columns", "body"),
                opt("fts_config", "english"),
                opt("payload_fields", "body,topic_ids"),
            ],
        )],
        vec![ManifestStore {
            store_kind: "vector".to_string(),
            backend: "qdrant".to_string(),
            resource_name: collection.clone(),
            options: vec![opt("dimension", "4")],
            ..ManifestStore::default()
        }],
    ))
    .await;
    let worker = worker(store, svc.runtime_snapshot(), svc.catalog.clone());

    let tenant_a = Uuid::new_v4().to_string();
    let tenant_b = Uuid::new_v4().to_string();
    // a-lex matches the text but is the dense FAR neighbour; a-dense is the
    // dense nearest neighbour with no text match; b-lex is tenant B's document
    // that matches both (it must never reach tenant A).
    for (tenant, id, body, vector, topics) in [
        (
            &tenant_a,
            "a-lex",
            "Zebra crossings explained",
            json!([0.0, 0.0, 0.0, 1.0]),
            json!(["t-zebra", "t-roads"]),
        ),
        (
            &tenant_a,
            "a-dense",
            "Quarterly revenue report",
            json!([1.0, 0.0, 0.0, 0.0]),
            json!(["t-finance"]),
        ),
        (
            &tenant_b,
            "b-lex",
            "Zebra herds on the move",
            json!([1.0, 0.0, 0.0, 0.0]),
            json!(["t-zebra"]),
        ),
    ] {
        served_upsert(
            &svc,
            tenant,
            MSG,
            json!({"id": id, "tenant_id": tenant, "body": body, "vector": vector, "topic_ids": topics}),
        )
        .await;
    }
    worker.run_once().await;

    let hybrid = |tenant: &str| {
        with_ctx(
            crate::proto::VectorHybridSearchRequest {
                collection: collection.clone(),
                vector: vec![1.0, 0.0, 0.0, 0.0],
                text_query: "zebra".to_string(),
                limit: 10,
                prefetch_limit: 1,
                with_payload: true,
                ..crate::proto::VectorHybridSearchRequest::default()
            },
            tenant,
        )
    };
    let a_hits = svc
        .vector_hybrid_search(hybrid(&tenant_a))
        .await
        .unwrap_or_else(|err| panic!("served full-text hybrid for tenant A: {err:?}"))
        .into_inner();
    let mut a_ids = payload_ids(&a_hits);
    a_ids.sort();
    assert_eq!(
        a_ids,
        vec!["a-dense", "a-lex"],
        "tenant A gets its dense neighbour AND its text-only match, never tenant B's: {a_hits:?}"
    );
    let a_lex = a_hits
        .points
        .iter()
        .filter_map(|point| point.payload.as_ref().map(struct_to_json))
        .find(|payload| payload["id"] == "a-lex")
        .expect("the text-only hit carries its Qdrant payload");
    assert_eq!(a_lex["_tenant_id"], tenant_a.as_str(), "{a_lex}");
    assert_eq!(a_lex["body"], "Zebra crossings explained", "{a_lex}");
    assert!(
        a_lex.get("vector").is_none(),
        "payload_fields keeps the vector out: {a_lex}"
    );
    assert_eq!(a_lex["topic_ids"], json!(["t-zebra", "t-roads"]), "{a_lex}");

    let b_hits = svc
        .vector_hybrid_search(hybrid(&tenant_b))
        .await
        .unwrap_or_else(|err| panic!("served full-text hybrid for tenant B: {err:?}"))
        .into_inner();
    assert_eq!(payload_ids(&b_hits), vec!["b-lex"], "{b_hits:?}");

    // An array payload field is a JSON array: a match on ONE element hits.
    let by_topic = svc
        .vector_search(with_ctx(
            crate::proto::VectorSearchRequest {
                collection: collection.clone(),
                vector: vec![1.0, 0.0, 0.0, 0.0],
                filter: json_to_struct(
                    &json!({"must": [{"key": "topic_ids", "match": {"value": "t-roads"}}]}),
                ),
                limit: 10,
                with_payload: true,
                ..crate::proto::VectorSearchRequest::default()
            },
            &tenant_a,
        ))
        .await
        .unwrap_or_else(|err| panic!("served VectorSearch on an array element: {err:?}"))
        .into_inner();
    assert_eq!(payload_ids(&by_topic), vec!["a-lex"], "{by_topic:?}");

    delete_tasks_for_resource(&pool, &collection).await;
    drop_qdrant_collection(&qdrant, &collection).await;
    drop_schema(&pool, &schema).await;
}

// ── D1: projection + CDC carry the bytes the database holds ───────────────────

/// D1: an encrypted column reaches the projection task and the CDC outbox as
/// the EXACT ciphertext the row holds — after the insert AND after an update
/// (whose RETURNING rows used to be decrypted and RE-encrypted with a fresh
/// nonce, so the projection held different bytes than the source and replay).
/// No plaintext reaches either.
#[tokio::test]
#[ignore = "requires live Postgres + UDB_ENCRYPTION_KEY; runs in the CI --ignored live lane"]
async fn live_served_writes_project_and_emit_the_stored_ciphertext() {
    let _guard = crate::runtime::service::live_tests::support::live_native_service_db_lock()
        .lock()
        .await;
    if require_backend_dsn(&["UDB_ENCRYPTION_KEY"]).is_none() {
        eprintln!("UDB_ENCRYPTION_KEY unset — skipping D1 ciphertext seam");
        return;
    }
    let pool = ledger_pool().await;
    let schema = format!("udb_proj_{}", Uuid::new_v4().simple());
    let cache_resource = format!("d1_cache_{}", Uuid::new_v4().simple());
    let topic = format!("udb.live.proj.d1.{}.v1", Uuid::new_v4().simple());
    const MSG: &str = "acme.proj.v1.Vaulted";
    create_source_table(&pool, &schema, "vaulted", ", secret TEXT").await;

    let mut secret = text_col("secret", false);
    secret.encrypted = true;
    let mut manifest = served_manifest(
        &schema,
        "vaulted",
        "Vaulted",
        vec![secret],
        vec![projection(
            "Vaulted",
            "cache",
            "redis",
            &cache_resource,
            vec![opt("key_pattern", "vaulted:{id}")],
        )],
        Vec::new(),
    );
    manifest.tables[0].cdc_topic = topic.clone();
    let svc = served_service(manifest).await;

    let tenant = Uuid::new_v4().to_string();
    let id = format!("vaulted-{}", Uuid::new_v4().simple());
    served_upsert(
        &svc,
        &tenant,
        MSG,
        json!({"id": id, "tenant_id": tenant, "secret": "before-plain"}),
    )
    .await;

    async fn stored_secret(pool: &PgPool, schema: &str, id: &str) -> String {
        sqlx::query_scalar::<_, Option<String>>(&format!(
            "SELECT secret FROM \"{schema}\".vaulted WHERE id = $1"
        ))
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read the stored encrypted column")
        .expect("encrypted column must not be NULL")
    }
    async fn newest_task_secret(pool: &PgPool, resource: &str, id: &str) -> String {
        let rel = SystemCatalogConfig::current().projection_tasks_relation();
        sqlx::query_scalar::<_, Option<String>>(&format!(
            "SELECT source_payload ->> 'secret' FROM {rel} \
             WHERE resource_name = $1 AND source_payload ->> 'id' = $2 \
             ORDER BY row_revision DESC LIMIT 1"
        ))
        .bind(resource)
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read the newest projection task for the row")
        .expect("the projection task must carry the encrypted column")
    }

    let stored = stored_secret(&pool, &schema, &id).await;
    assert!(
        !stored.contains("before-plain"),
        "the column must be stored encrypted: {stored}"
    );
    assert_eq!(
        newest_task_secret(&pool, &cache_resource, &id).await,
        stored,
        "the upsert's projection task must carry the stored ciphertext byte for byte"
    );

    svc.update(with_ctx(
        crate::proto::UpdateRequest {
            message_type: MSG.to_string(),
            filter: json_to_struct(&json!({"id": id, "tenant_id": tenant})),
            changes: json_to_struct(&json!({"secret": "after-plain"})),
            ..crate::proto::UpdateRequest::default()
        },
        &tenant,
    ))
    .await
    .expect("served Update of the encrypted column");
    let stored_after = stored_secret(&pool, &schema, &id).await;
    assert_ne!(
        stored_after, stored,
        "the update must change the ciphertext"
    );
    assert!(!stored_after.contains("after-plain"), "{stored_after}");
    assert_eq!(
        newest_task_secret(&pool, &cache_resource, &id).await,
        stored_after,
        "the update's projection task must carry the stored ciphertext, not a re-encryption"
    );

    // The CDC outbox for this topic holds the change events, never plaintext.
    let outbox = SystemCatalogConfig::current().cdc.outbox_relation();
    let leaked: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {outbox} WHERE topic = $1 \
         AND (payload::text LIKE '%before-plain%' OR payload::text LIKE '%after-plain%')"
    ))
    .bind(&topic)
    .fetch_one(&pool)
    .await
    .expect("scan the outbox for plaintext");
    assert_eq!(leaked, 0, "a CDC change event carried the plaintext");
    if crate::runtime::cdc::cdc_delivery_enabled() {
        let emitted: i64 =
            sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {outbox} WHERE topic = $1"))
                .bind(&topic)
                .fetch_one(&pool)
                .await
                .expect("count outbox events");
        assert!(
            emitted >= 2,
            "upsert + update must each emit a change event, got {emitted}"
        );
    }

    let _ = sqlx::query(&format!("DELETE FROM {outbox} WHERE topic = $1"))
        .bind(&topic)
        .execute(&pool)
        .await;
    delete_tasks_for_resource(&pool, &cache_resource).await;
    drop_schema(&pool, &schema).await;
}

// ── D3: object projection keys and scoped delete on MinIO ─────────────────────

#[cfg(feature = "s3")]
fn live_s3_client() -> aws_sdk_s3::Client {
    use aws_sdk_s3::config::{Credentials, Region};
    let var = |keys: &[&str], default: &str| {
        keys.iter()
            .find_map(|key| crate::runtime::service::live_tests::support::live_env(key))
            .unwrap_or_else(|| default.to_string())
    };
    let endpoint = var(
        &["UDB_MINIO_ENDPOINT", "UDB_INTEGRATION_MINIO_ENDPOINT"],
        "http://127.0.0.1:59000",
    );
    let access = var(
        &["UDB_MINIO_ACCESS_KEY", "UDB_INTEGRATION_MINIO_ACCESS_KEY"],
        "minio",
    );
    let secret = var(
        &["UDB_MINIO_SECRET_KEY", "UDB_INTEGRATION_MINIO_SECRET_KEY"],
        "minio123",
    );
    let region = var(&["UDB_MINIO_REGION"], "us-east-1");
    // SAFETY: test-only env mutation (single-threaded live lane, DB lock held):
    // the runtime under test resolves its MinIO instance from the same values.
    unsafe {
        std::env::set_var("UDB_MINIO_ENDPOINT", &endpoint);
        std::env::set_var("UDB_MINIO_ACCESS_KEY", &access);
        std::env::set_var("UDB_MINIO_SECRET_KEY", &secret);
        std::env::set_var("UDB_MINIO_REGION", &region);
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
    }
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .credentials_provider(Credentials::new(access, secret, None, None, "udb-d3-test"))
        .region(Region::new(region))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}

/// D3: an object projection writes `t:{tenant}/p:{project}/{prefix}/{id}.json`,
/// so two tenants' rows with one id are two objects, and tenant A's served
/// Delete removes only A's object.
#[cfg(feature = "s3")]
#[tokio::test]
#[ignore = "requires live Postgres+MinIO (UDB_INTEGRATION_PG_DSN + UDB_MINIO_ENDPOINT); runs in the CI --ignored live lane"]
async fn live_object_projection_keys_by_scope_and_delete_spares_other_tenant() {
    let _guard = crate::runtime::service::live_tests::support::live_native_service_db_lock()
        .lock()
        .await;
    let s3 = live_s3_client();
    let pool = ledger_pool().await;
    let store = ledger_store(&pool).await;

    let bucket = format!("udb-proj-{}", &Uuid::new_v4().simple().to_string()[..16]);
    s3.create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .unwrap_or_else(|err| panic!("create MinIO bucket {bucket}: {err:?}"));
    let schema = format!("udb_proj_{}", Uuid::new_v4().simple());
    const MSG: &str = "acme.proj.v1.SeamObject";
    create_source_table(&pool, &schema, "seam_objects", ", status TEXT").await;
    let svc = served_service(served_manifest(
        &schema,
        "seam_objects",
        "SeamObject",
        vec![text_col("status", false)],
        vec![projection(
            "SeamObject",
            "object",
            "minio",
            &bucket,
            // Composite (tenant_id, id) key: name objects by `id`, not by
            // the JSON-encoded row key.
            vec![opt("key_prefix", "docs"), opt("id_field", "id")],
        )],
        Vec::new(),
    ))
    .await;
    let worker = worker(store, svc.runtime_snapshot(), svc.catalog.clone());

    let tenant_a = Uuid::new_v4().to_string();
    let tenant_b = Uuid::new_v4().to_string();
    for (tenant, status) in [(&tenant_a, "a"), (&tenant_b, "b")] {
        served_upsert(
            &svc,
            tenant,
            MSG,
            json!({"id": "doc-1", "tenant_id": tenant, "status": status}),
        )
        .await;
    }
    worker.run_once().await;

    let key_a = format!("t:{tenant_a}/p:{PROJECT}/docs/doc-1.json");
    let key_b = format!("t:{tenant_b}/p:{PROJECT}/docs/doc-1.json");
    let exists = |key: String| {
        let s3 = s3.clone();
        let bucket = bucket.clone();
        async move {
            s3.head_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .is_ok()
        }
    };
    if !exists(key_a.clone()).await {
        // Name the cause: the ledger row says whether the task was applied,
        // dead-lettered, or never claimed.
        let rel =
            crate::runtime::system::SystemCatalogConfig::current().projection_tasks_relation();
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(&format!(
            "SELECT target_backend, resource_name, status, last_error FROM {rel} \
             WHERE resource_name = $1 ORDER BY updated_at DESC LIMIT 6"
        ))
        .bind(&bucket)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();
        panic!("missing tenant A object {key_a}; projection tasks for {bucket}: {rows:?}");
    }
    assert!(
        exists(key_b.clone()).await,
        "missing tenant B object {key_b}"
    );
    let body = s3
        .get_object()
        .bucket(&bucket)
        .key(&key_a)
        .send()
        .await
        .expect("read tenant A's projected object")
        .body
        .collect()
        .await
        .expect("read object body")
        .into_bytes();
    let object: serde_json::Value = serde_json::from_slice(&body).expect("object JSON");
    assert_eq!(
        object["payload"]["_tenant_id"],
        tenant_a.as_str(),
        "{object}"
    );
    assert_eq!(object["payload"]["status"], "a", "{object}");

    served_delete(
        &svc,
        &tenant_a,
        MSG,
        json!({"id": "doc-1", "tenant_id": tenant_a}),
    )
    .await;
    worker.run_once().await;
    assert!(
        !exists(key_a.clone()).await,
        "tenant A's object must be deleted by A's served Delete"
    );
    assert!(
        exists(key_b.clone()).await,
        "tenant B's object must survive A's delete"
    );

    for key in [key_a, key_b] {
        let _ = s3.delete_object().bucket(&bucket).key(key).send().await;
    }
    let _ = s3.delete_bucket().bucket(&bucket).send().await;
    delete_tasks_for_resource(&pool, &bucket).await;
    drop_schema(&pool, &schema).await;
}

// ── D8 / D9 / D10: ledger lease, ordering and fence on Postgres ───────────────

/// D8: a task left IN_PROGRESS by a crashed worker is reclaimed by the worker
/// ITSELF once its claim lease expires (not only by the off-by-default
/// reconciler), applied, and marked COMPLETED.
#[tokio::test]
#[ignore = "requires live Postgres+Qdrant (UDB_INTEGRATION_PG_DSN + UDB_QDRANT_URL); runs in the CI --ignored live lane"]
async fn live_projection_worker_reclaims_a_stale_in_progress_task() {
    let qdrant = qdrant_url();
    let pool = ledger_pool().await;
    let store = ledger_store(&pool).await;
    let runtime = runtime_from_env().await;
    let collection = format!("udb_projection_d8_{}", Uuid::new_v4().simple());
    create_qdrant_collection(&qdrant, &collection).await;
    let manifest = ledger_manifest(&collection, vec![vector_projection(&collection)]);
    let plans = ProjectionPlan::from_manifest(&manifest);
    let worker = worker(
        store,
        runtime.clone(),
        Arc::new(CatalogManager::new(manifest.clone())),
    );

    let tenant = Uuid::new_v4().to_string();
    let keys = enqueue(
        &pool,
        &plans,
        "upsert",
        &json!({"id": "stale-1", "tenant_id": tenant, "status": "v1", "vector": [0.1, 0.2, 0.3, 0.4]}),
    )
    .await;
    // A worker claimed it and died: IN_PROGRESS, last touched long ago.
    set_task_state(
        &pool,
        &keys[0],
        "status = 'IN_PROGRESS', updated_at = NOW() - INTERVAL '1 hour'",
    )
    .await;

    worker.run_once().await;
    assert_eq!(
        task_statuses(&pool, &keys).await,
        vec!["COMPLETED".to_string()],
        "the expired claim must be reclaimed, applied and completed"
    );
    let points = scoped_points(&runtime, &collection, &tenant).await;
    assert_eq!(points.len(), 1, "{points:?}");
    assert_eq!(points[0]["status"], "v1", "{points:?}");

    delete_tasks_for_resource(&pool, &collection).await;
    drop_qdrant_collection(&qdrant, &collection).await;
}

/// D9: tasks for one row apply in row-revision order.
/// 1. v1 then v2 enqueued, v1 fails → the next pass retires v1 (superseded)
///    and the target holds v2.
/// 2. v3 is IN_PROGRESS (a slow worker) while v4 is queued → v4 is NOT claimed
///    alongside it; once v3 fails, v3 is retired and v4 lands.
/// 3. The row returns to v2's exact content (ABA) → v2's COMPLETED task is
///    re-armed with a FRESH, higher revision and the target holds v2 again.
#[tokio::test]
#[ignore = "requires live Postgres+Qdrant (UDB_INTEGRATION_PG_DSN + UDB_QDRANT_URL); runs in the CI --ignored live lane"]
async fn live_projection_applies_one_rows_tasks_in_revision_order() {
    let qdrant = qdrant_url();
    let pool = ledger_pool().await;
    let store = ledger_store(&pool).await;
    let runtime = runtime_from_env().await;
    let collection = format!("udb_projection_d9_{}", Uuid::new_v4().simple());
    create_qdrant_collection(&qdrant, &collection).await;
    let manifest = ledger_manifest(&collection, vec![vector_projection(&collection)]);
    let plans = ProjectionPlan::from_manifest(&manifest);
    let worker = worker(
        store,
        runtime.clone(),
        Arc::new(CatalogManager::new(manifest.clone())),
    );
    let tenant = Uuid::new_v4().to_string();
    let version = |status: &str| json!({"id": "row-1", "tenant_id": tenant, "status": status, "vector": [0.1, 0.2, 0.3, 0.4]});
    async fn single_status(
        runtime: &crate::runtime::DataBrokerRuntime,
        collection: &str,
        tenant: &str,
    ) -> String {
        let points = scoped_points(runtime, collection, tenant).await;
        assert_eq!(points.len(), 1, "one point per row: {points:?}");
        points[0]["status"].as_str().unwrap_or_default().to_string()
    }

    // 1. v1 fails after v2 was written: v2 wins, v1 is retired, never applied.
    let k1 = enqueue(&pool, &plans, "upsert", &version("v1")).await;
    let k2 = enqueue(&pool, &plans, "upsert", &version("v2")).await;
    let rev1: i64 = task_field(&pool, &k1[0], "row_revision")
        .await
        .parse()
        .unwrap();
    let rev2: i64 = task_field(&pool, &k2[0], "row_revision")
        .await
        .parse()
        .unwrap();
    assert!(
        rev2 > rev1,
        "the later write must carry the higher revision"
    );
    set_task_state(
        &pool,
        &k1[0],
        "status = 'FAILED', retry_count = 1, next_retry_at = NULL",
    )
    .await;
    worker.run_once().await;
    assert_eq!(single_status(&runtime, &collection, &tenant).await, "v2");
    assert!(
        task_field(&pool, &k1[0], "last_error")
            .await
            .contains("superseded"),
        "the failed older task must be retired as superseded"
    );

    // 2. One task per row in flight: v4 waits while v3 is IN_PROGRESS.
    let k3 = enqueue(&pool, &plans, "upsert", &version("v3")).await;
    set_task_state(&pool, &k3[0], "status = 'IN_PROGRESS', updated_at = NOW()").await;
    let k4 = enqueue(&pool, &plans, "upsert", &version("v4")).await;
    worker.run_once().await;
    assert_eq!(
        task_statuses(&pool, &k4).await,
        vec!["PENDING".to_string()],
        "a task must not be claimed while its row has one in flight"
    );
    assert_eq!(single_status(&runtime, &collection, &tenant).await, "v2");
    // v3's worker fails; the next pass retires v3 and applies v4.
    set_task_state(
        &pool,
        &k3[0],
        "status = 'FAILED', retry_count = 1, next_retry_at = NULL",
    )
    .await;
    worker.run_once().await;
    assert_eq!(single_status(&runtime, &collection, &tenant).await, "v4");
    assert!(
        task_field(&pool, &k3[0], "last_error")
            .await
            .contains("superseded")
    );

    // 3. ABA: the row returns to v2's exact content.
    let rearmed = enqueue(&pool, &plans, "upsert", &version("v2")).await;
    assert_eq!(rearmed, k2, "same content maps onto v2's task");
    let rev2_rearmed: i64 = task_field(&pool, &k2[0], "row_revision")
        .await
        .parse()
        .unwrap();
    let rev4: i64 = task_field(&pool, &k4[0], "row_revision")
        .await
        .parse()
        .unwrap();
    assert!(
        rev2_rearmed > rev4,
        "a re-armed task must take a fresh revision above the newer write ({rev2_rearmed} <= {rev4})"
    );
    worker.run_once().await;
    assert_eq!(
        single_status(&runtime, &collection, &tenant).await,
        "v2",
        "the row's current state (v2 again) must win"
    );

    delete_tasks_for_resource(&pool, &collection).await;
    drop_qdrant_collection(&qdrant, &collection).await;
}

/// D10 (PostgreSQL ledger): a read fence on a projection key the ledger has
/// never seen does NOT clear — it times out into `ProjectionMissing` — while a
/// key whose task COMPLETED clears.
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live lane"]
async fn live_pg_fence_on_an_unknown_projection_key_is_stale() {
    use crate::runtime::consistency::{ReadFence, StaleReadWarning};
    use crate::runtime::consistency_fence::{FenceOutcome, wait_for_fence};

    let pool = ledger_pool().await;
    let store: Arc<dyn crate::runtime::canonical_store::SystemStores> =
        Arc::new(ledger_store(&pool).await);
    let ghost = format!("ghost-{}", Uuid::new_v4().simple());
    let outcome = wait_for_fence(
        store.as_ref(),
        &ReadFence {
            min_outbox_lsn: String::new(),
            projection_task_ids: vec![ghost.clone()],
            max_wait_ms: 150,
        },
        "qdrant",
        "test",
    )
    .await;
    match outcome {
        FenceOutcome::Stale(StaleReadWarning::ProjectionMissing { backend, resource }) => {
            assert_eq!(backend, "qdrant");
            assert!(resource.contains(&ghost), "{resource}");
        }
        other => panic!("an unknown fence key must not clear on PostgreSQL, got {other:?}"),
    }

    // A completed task's key clears the fence.
    let resource = format!("udb_projection_d10_{}", Uuid::new_v4().simple());
    let manifest = ledger_manifest(&resource, vec![vector_projection(&resource)]);
    let plans = ProjectionPlan::from_manifest(&manifest);
    let keys = enqueue(
        &pool,
        &plans,
        "upsert",
        &json!({"id": "fence-1", "tenant_id": "tenant-a", "vector": [0.1, 0.2, 0.3, 0.4]}),
    )
    .await;
    set_task_state(
        &pool,
        &keys[0],
        "status = 'COMPLETED', completed_at = NOW()",
    )
    .await;
    let cleared = wait_for_fence(
        store.as_ref(),
        &ReadFence {
            min_outbox_lsn: String::new(),
            projection_task_ids: keys.clone(),
            max_wait_ms: 2_000,
        },
        "qdrant",
        "test",
    )
    .await;
    assert_eq!(cleared, FenceOutcome::Cleared);
    delete_tasks_for_resource(&pool, &resource).await;
}

// ── Phase 3: document / cache / analytical targets ────────────────────────────

/// D2: MongoDB projection — two tenants' rows with one id are two documents
/// (stamped and keyed by `_tenant_id`/`_project_id`), and a scoped delete
/// removes only the deleting tenant's document.
#[cfg(feature = "mongodb-native")]
#[tokio::test]
#[ignore = "requires live Postgres+MongoDB (UDB_MONGODB_DSN); runs in the CI --ignored live lane"]
async fn live_mongodb_projection_keeps_two_tenants_same_id_apart() {
    use mongodb_driver::bson::{Document, doc};

    let Some(dsn) = require_backend_dsn(&["UDB_MONGODB_DSN", "UDB_NOSQL_DSN"]) else {
        eprintln!("UDB_MONGODB_DSN unset — skipping D2");
        return;
    };
    // SAFETY: test-only env mutation (single-threaded live lane): the runtime
    // resolves its MongoDB instance from UDB_NOSQL_DSN.
    unsafe {
        std::env::set_var("UDB_NOSQL_DSN", &dsn);
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
    }
    let pool = ledger_pool().await;
    let store = ledger_store(&pool).await;
    let runtime = runtime_from_env().await;
    let collection = format!("udb_projection_d2_{}", Uuid::new_v4().simple());
    let manifest = ledger_manifest(
        &collection,
        vec![projection(
            "SeamDocument",
            "document",
            "mongodb",
            &collection,
            Vec::new(),
        )],
    );
    let plans = ProjectionPlan::from_manifest(&manifest);
    let worker = worker(
        store,
        runtime.clone(),
        Arc::new(CatalogManager::new(manifest.clone())),
    );

    let tenant_a = Uuid::new_v4().to_string();
    let tenant_b = Uuid::new_v4().to_string();
    let mut keys = Vec::new();
    for (tenant, status) in [(&tenant_a, "a"), (&tenant_b, "b")] {
        keys.extend(
            enqueue(
                &pool,
                &plans,
                "upsert",
                &json!({"id": "doc-1", "tenant_id": tenant, "status": status}),
            )
            .await,
        );
    }
    worker.run_once().await;
    assert_eq!(
        task_statuses(&pool, &keys).await,
        vec!["COMPLETED".to_string(), "COMPLETED".to_string()]
    );

    let client = mongodb_driver::Client::with_uri_str(&dsn)
        .await
        .expect("connect live MongoDB");
    let database = crate::runtime::executors::mongodb::MongoDbConfig::db_from_dsn(&dsn)
        .unwrap_or_else(|| "udb".to_string());
    let docs = client
        .database(&database)
        .collection::<Document>(&collection);
    assert_eq!(
        docs.count_documents(doc! {"id": "doc-1"})
            .await
            .expect("count projected documents"),
        2,
        "two tenants' rows with one id must be two documents"
    );
    for (tenant, status) in [(&tenant_a, "a"), (&tenant_b, "b")] {
        let found = docs
            .find_one(doc! {"id": "doc-1", "_tenant_id": tenant.as_str()})
            .await
            .expect("find the tenant's document")
            .unwrap_or_else(|| panic!("tenant {tenant}'s document is missing"));
        assert_eq!(found.get_str("status").ok(), Some(status));
        assert_eq!(found.get_str("_project_id").ok(), Some(PROJECT));
    }

    let delete_payload = scoped_delete_payload(
        &manifest,
        "SeamDocument",
        &json!({"id": "doc-1"}),
        &tenant_a,
    );
    let delete_keys = enqueue(&pool, &plans, "delete", &delete_payload).await;
    worker.run_once().await;
    assert_eq!(
        task_statuses(&pool, &delete_keys).await,
        vec!["COMPLETED".to_string()]
    );
    assert_eq!(
        docs.count_documents(doc! {"id": "doc-1", "_tenant_id": tenant_a.as_str()})
            .await
            .expect("count A's documents"),
        0
    );
    assert_eq!(
        docs.count_documents(doc! {"id": "doc-1", "_tenant_id": tenant_b.as_str()})
            .await
            .expect("count B's documents"),
        1,
        "tenant B's document must survive A's delete"
    );

    let _ = docs.drop().await;
    delete_tasks_for_resource(&pool, &collection).await;
}

/// D4: Redis projection keys are `t:{tenant}/p:{project}/{pattern}`, and a
/// delete removes EXACTLY its key: deleting A's `p*` leaves A's `p1` (which a
/// glob `p*` would match) and B's `p*` in place.
#[cfg(feature = "redis")]
#[tokio::test]
#[ignore = "requires live Postgres+Redis (UDB_REDIS_DSN); runs in the CI --ignored live lane"]
async fn live_redis_projection_delete_removes_exactly_its_key() {
    use redis::AsyncCommands as _;

    let Some(dsn) = require_backend_dsn(&["UDB_REDIS_DSN", "UDB_INTEGRATION_REDIS_URL"]) else {
        eprintln!("UDB_REDIS_DSN unset — skipping D4");
        return;
    };
    // SAFETY: test-only env mutation (single-threaded live lane): the runtime
    // resolves its Redis instance from UDB_REDIS_DSN.
    unsafe {
        std::env::set_var("UDB_REDIS_DSN", &dsn);
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
    }
    let pool = ledger_pool().await;
    let store = ledger_store(&pool).await;
    let runtime = runtime_from_env().await;
    let resource = format!("udb_projection_d4_{}", Uuid::new_v4().simple());
    let pattern = format!("{resource}:{{id}}");
    let manifest = ledger_manifest(
        &resource,
        vec![projection(
            "SeamDocument",
            "cache",
            "redis",
            &resource,
            vec![opt("key_pattern", &pattern), opt("ttl_seconds", "600")],
        )],
    );
    let plans = ProjectionPlan::from_manifest(&manifest);
    let worker = worker(
        store,
        runtime.clone(),
        Arc::new(CatalogManager::new(manifest.clone())),
    );

    let tenant_a = Uuid::new_v4().to_string();
    let tenant_b = Uuid::new_v4().to_string();
    for (tenant, id) in [(&tenant_a, "p*"), (&tenant_a, "p1"), (&tenant_b, "p*")] {
        enqueue(
            &pool,
            &plans,
            "upsert",
            &json!({"id": id, "tenant_id": tenant}),
        )
        .await;
    }
    worker.run_once().await;

    let key = |tenant: &str, id: &str| format!("t:{tenant}/p:{PROJECT}/{resource}:{id}");
    let client = redis::Client::open(dsn.as_str()).expect("open live Redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect live Redis");
    for (tenant, id) in [(&tenant_a, "p*"), (&tenant_a, "p1"), (&tenant_b, "p*")] {
        let exists: bool = conn
            .exists(key(tenant, id))
            .await
            .expect("EXISTS projected key");
        assert!(exists, "missing projected key {}", key(tenant, id));
    }

    let delete_payload =
        scoped_delete_payload(&manifest, "SeamDocument", &json!({"id": "p*"}), &tenant_a);
    let delete_keys = enqueue(&pool, &plans, "delete", &delete_payload).await;
    worker.run_once().await;
    assert_eq!(
        task_statuses(&pool, &delete_keys).await,
        vec!["COMPLETED".to_string()]
    );
    let a_star: bool = conn.exists(key(&tenant_a, "p*")).await.unwrap();
    let a_one: bool = conn.exists(key(&tenant_a, "p1")).await.unwrap();
    let b_star: bool = conn.exists(key(&tenant_b, "p*")).await.unwrap();
    assert!(!a_star, "A's `p*` must be deleted");
    assert!(
        a_one,
        "A's `p1` must survive: the delete is exact, not a glob"
    );
    assert!(b_star, "B's `p*` must survive A's delete");

    let _: Result<i64, _> = conn
        .del(vec![key(&tenant_a, "p1"), key(&tenant_b, "p*")])
        .await;
    delete_tasks_for_resource(&pool, &resource).await;
}

/// D5: an append-only ClickHouse projection inserts every change as a row and
/// treats a source delete as a no-op (the task completes, the rows stay) —
/// it never claims a delete it cannot perform.
#[cfg(feature = "clickhouse")]
#[tokio::test]
#[ignore = "requires live Postgres+ClickHouse (UDB_CLICKHOUSE_DSN); runs in the CI --ignored live lane"]
async fn live_clickhouse_append_only_projection_inserts_and_skips_deletes() {
    use crate::runtime::executors::clickhouse::ClickHouseConfig;

    let Some(dsn) = require_backend_dsn(&["UDB_CLICKHOUSE_DSN", "UDB_COLUMN_DSN"]) else {
        eprintln!("UDB_CLICKHOUSE_DSN unset — skipping D5");
        return;
    };
    let env_or = |keys: &[&str], default: &str| {
        keys.iter()
            .find_map(|key| crate::runtime::service::live_tests::support::live_env(key))
            .unwrap_or_else(|| default.to_string())
    };
    let user = env_or(&["UDB_CLICKHOUSE_USER", "UDB_COLUMN_USER"], "default");
    let password = env_or(&["UDB_CLICKHOUSE_PASSWORD", "UDB_COLUMN_PASSWORD"], "");
    let database = env_or(
        &["UDB_CLICKHOUSE_DATABASE", "UDB_COLUMN_DATABASE"],
        &ClickHouseConfig::db_from_dsn(&dsn).unwrap_or_else(|| "default".to_string()),
    );
    // SAFETY: test-only env mutation (single-threaded live lane): the runtime
    // resolves its ClickHouse instance from the UDB_COLUMN_* variables.
    unsafe {
        std::env::set_var("UDB_COLUMN_DSN", &dsn);
        std::env::set_var("UDB_COLUMN_USER", &user);
        std::env::set_var("UDB_COLUMN_PASSWORD", &password);
        std::env::set_var("UDB_COLUMN_DATABASE", &database);
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
    }
    let http_base = ClickHouseConfig::http_base_from_dsn(&dsn);
    let http = reqwest::Client::new();
    let clickhouse = |sql: String| {
        let request = http
            .post(format!("{}/", http_base.trim_end_matches('/')))
            .basic_auth(user.clone(), Some(password.clone()))
            .body(sql);
        async move {
            let response = request.send().await.expect("ClickHouse HTTP request");
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            assert!(status.is_success(), "ClickHouse {status}: {body}");
            body
        }
    };

    let pool = ledger_pool().await;
    let store = ledger_store(&pool).await;
    let table = format!("udb_projection_d5_{}", Uuid::new_v4().simple());
    clickhouse(format!(
        "CREATE TABLE `{database}`.`{table}` (id String, tenant_id String, status String) \
         ENGINE = MergeTree ORDER BY id"
    ))
    .await;
    let runtime = runtime_from_env().await;
    let manifest = ledger_manifest(
        &table,
        vec![projection(
            "SeamDocument",
            "analytical",
            "clickhouse",
            &table,
            vec![opt("append_only", "true")],
        )],
    );
    let plans = ProjectionPlan::from_manifest(&manifest);
    let worker = worker(
        store,
        runtime.clone(),
        Arc::new(CatalogManager::new(manifest.clone())),
    );
    let tenant = Uuid::new_v4().to_string();
    let mut keys = Vec::new();
    for status in ["v1", "v2"] {
        keys.extend(
            enqueue(
                &pool,
                &plans,
                "upsert",
                &json!({"id": "doc-1", "tenant_id": tenant, "status": status}),
            )
            .await,
        );
    }
    // Append-only is order-independent: both versions are inserted (no
    // supersede is needed for correctness, but the ledger may retire v1 —
    // what matters is that every applied change is a NEW row).
    worker.run_once().await;
    let count = || {
        clickhouse(format!(
            "SELECT count() FROM `{database}`.`{table}` WHERE id = 'doc-1' FORMAT TabSeparated"
        ))
    };
    let applied: i64 = count().await.trim().parse().expect("count rows");
    assert!(applied >= 1, "the newest change must be inserted");
    let newest = clickhouse(format!(
        "SELECT status FROM `{database}`.`{table}` WHERE id = 'doc-1' \
         ORDER BY status DESC LIMIT 1 FORMAT TabSeparated"
    ))
    .await;
    assert_eq!(newest.trim(), "v2");

    let delete_payload =
        scoped_delete_payload(&manifest, "SeamDocument", &json!({"id": "doc-1"}), &tenant);
    let delete_keys = enqueue(&pool, &plans, "delete", &delete_payload).await;
    worker.run_once().await;
    assert_eq!(
        task_statuses(&pool, &delete_keys).await,
        vec!["COMPLETED".to_string()],
        "an append-only target completes a delete without applying it"
    );
    let after: i64 = count().await.trim().parse().expect("count rows");
    assert_eq!(
        after, applied,
        "an append-only target keeps its rows on delete"
    );

    clickhouse(format!("DROP TABLE IF EXISTS `{database}`.`{table}`")).await;
    delete_tasks_for_resource(&pool, &table).await;
}

/// A permanently invalid immutable task stays parked across worker/reconciler
/// reconstruction and real source replay. Corrected source/catalog content
/// produces a new authorized key; real backend unavailability still repairs.
#[tokio::test]
#[ignore = "requires live Postgres+Qdrant; runs in the CI --ignored live lane"]
async fn live_projection_permanent_payload_survives_restart_replay_and_corrected_recovery() {
    use crate::runtime::canonical_store::system_store::{
        ProjectionFailureDisposition, projection_failure_disposition,
        projection_failure_is_automatically_repairable,
    };
    use crate::runtime::consistency::{ReadFence, StaleReadWarning, WriteReceipt};
    use crate::runtime::consistency_fence::{FenceOutcome, wait_for_fence};
    use futures::FutureExt;

    let _guard = crate::runtime::service::live_tests::support::live_native_service_db_lock()
        .lock()
        .await;
    let Some(_) = require_backend_dsn(&["UDB_LIVE_NATIVE_PG_DSN", "UDB_INTEGRATION_PG_DSN"]) else {
        return;
    };
    let qdrant = qdrant_url();
    let pool = ledger_pool().await;
    let schema = format!("udb_projection_permanent_{}", Uuid::new_v4().simple());
    let collection = format!("udb_projection_permanent_{}", Uuid::new_v4().simple());
    const MSG: &str = "acme.proj.v1.PermanentVector";
    create_source_table(
        &pool,
        &schema,
        "owned_vectors",
        ", values_json JSONB, label TEXT",
    )
    .await;
    // Do not create the collection yet. Missing payload is rejected before a
    // backend request; later valid content observes a real missing collection.
    let mut values = text_col("values_json", false);
    values.sql_type = "JSONB".to_string();
    values.is_jsonb = true;
    let mut manifest = served_manifest(
        &schema,
        "owned_vectors",
        "PermanentVector",
        vec![values, text_col("label", false)],
        vec![projection(
            "PermanentVector",
            "vector",
            "qdrant",
            &collection,
            vec![opt("vector_field", "absent_vector")],
        )],
        vec![ManifestStore {
            store_kind: "vector".to_string(),
            backend: "qdrant".to_string(),
            resource_name: collection.clone(),
            options: vec![opt("dimension", "4")],
            ..ManifestStore::default()
        }],
    );
    manifest.checksum_sha256 = format!("permanent-source-{}", Uuid::new_v4().simple());
    let svc = served_service(manifest.clone()).await;
    let tenant = Uuid::new_v4().to_string();
    let outcome = std::panic::AssertUnwindSafe(async {
        let payload = |label: &str| {
            json!({
                "id":"owned-row", "tenant_id":tenant,
                "values_json":[0.1,0.2,0.3,0.4], "label":label,
            })
        };
        // PROJECTION_PERMANENT_REPLAY_DIAGNOSTIC_HELPER_BEGIN
        async fn owned_replay_diagnostic(
            pool: &PgPool,
            schema: &str,
            resource: &str,
            tenant: &str,
            original_payload: &serde_json::Value,
            original_key: &str,
            phase: &str,
        ) {
            use sqlx::Row;
            assert!(schema.starts_with("udb_projection_permanent_"));
            assert!(resource.starts_with("udb_projection_permanent_"));
            let rel = SystemCatalogConfig::current().projection_tasks_relation();
            let rows = sqlx::query(&format!(
                "SELECT idempotency_key, source_checksum, manifest_checksum, target_backend, \
                 target_instance, status, retry_count, row_revision::TEXT AS row_revision, \
                 last_error, source_payload, source_payload = $5::JSONB AS matches_original \
                 FROM {rel} WHERE resource_name = $1 AND source_schema = $2 \
                 AND source_table = 'owned_vectors' AND project_id = $4 \
                 AND source_row_key ->> 'id' = 'owned-row' \
                 AND source_row_key ->> 'tenant_id' = $3 \
                 AND source_payload ->> 'tenant_id' = $3 \
                 ORDER BY row_revision LIMIT 4"
            ))
            .bind(resource)
            .bind(schema)
            .bind(tenant)
            .bind(PROJECT)
            .bind(original_payload.to_string())
            .fetch_all(pool)
            .await
            .expect("read only the bounded owned synthetic projection tasks");
            assert!(
                (2..=3).contains(&rows.len()),
                "owned diagnostic must contain the two original tasks and at most one replay task"
            );
            let source_rows = sqlx::query_scalar::<_, serde_json::Value>(&format!(
                "SELECT to_jsonb(t) FROM {}.{} AS t \
                 WHERE id = $1 AND tenant_id = $2 LIMIT 2",
                qi(schema),
                qi("owned_vectors")
            ))
            .bind("owned-row")
            .bind(tenant)
            .fetch_all(pool)
            .await
            .expect("read only the owned synthetic canonical source row");
            assert_eq!(source_rows.len(), 1, "one owned source row is required");
            let source_payload = &source_rows[0];
            let key_order = |value: &serde_json::Value| {
                value
                    .as_object()
                    .expect("synthetic source is an object")
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
            };
            let tasks = rows
                .into_iter()
                .map(|row| {
                    let payload: serde_json::Value = row.try_get("source_payload").unwrap();
                    let error: String = row.try_get("last_error").unwrap();
                    json!({
                        "natural_key": row.try_get::<String, _>("idempotency_key").unwrap(),
                        "source_checksum": row.try_get::<String, _>("source_checksum").unwrap(),
                        "manifest_checksum": row.try_get::<String, _>("manifest_checksum").unwrap(),
                        "target_backend": row.try_get::<String, _>("target_backend").unwrap(),
                        "target_instance": row.try_get::<String, _>("target_instance").unwrap(),
                        "status": row.try_get::<String, _>("status").unwrap(),
                        "retry_count": row.try_get::<i32, _>("retry_count").unwrap(),
                        "row_revision": row.try_get::<String, _>("row_revision").unwrap(),
                        "failure_disposition": format!("{:?}", projection_failure_disposition(&error)),
                        "semantic_payload_matches_original": row.try_get::<bool, _>("matches_original").unwrap(),
                        "semantic_payload_matches_sql_row": payload == *source_payload,
                        "stored_json_key_order": key_order(&payload),
                        "recomputed_stored_payload_checksum": ProjectionEngine::source_checksum(&payload),
                    })
                })
                .collect::<Vec<_>>();
            println!(
                "PROJECTION_PERMANENT_REPLAY_DIAGNOSTIC {}",
                json!({
                    "phase": phase,
                    "scope": "owned synthetic resource, source row and tenant only",
                    "task_count": tasks.len(),
                    "original_key": original_key,
                    "original_request_key_order": key_order(original_payload),
                    "original_request_source_checksum": ProjectionEngine::source_checksum(original_payload),
                    "sql_replay_key_order": key_order(source_payload),
                    "sql_replay_source_checksum": ProjectionEngine::source_checksum(source_payload),
                    "semantic_sql_payload_matches_original": source_payload == original_payload,
                    "tasks": tasks,
                })
            );
        }
        // PROJECTION_PERMANENT_REPLAY_DIAGNOSTIC_HELPER_END
        async fn write_receipt(
            svc: &DataBrokerService,
            tenant: &str,
            record: serde_json::Value,
        ) -> WriteReceipt {
            let response = svc
                .upsert(with_ctx(
                    crate::proto::UpsertRequest {
                        message_type: MSG.to_string(),
                        record_json: serde_json::to_vec(&record).unwrap(),
                        ..crate::proto::UpsertRequest::default()
                    },
                    tenant,
                ))
                .await
                .expect("actual served source write")
                .into_inner();
            serde_json::from_str(&response.write_receipt_json).expect("actual producer receipt")
        }
        let first = write_receipt(&svc, &tenant, payload("poison-v1")).await;
        assert_eq!(first.projection_task_ids.len(), 1);
        let first_key = &first.projection_task_ids[0];
        let initial_worker = worker(
            ledger_store(&pool).await,
            svc.runtime_snapshot(),
            svc.catalog.clone(),
        );
        assert_eq!(initial_worker.run_once().await, (0, 1));
        drop(initial_worker);
        assert_eq!(
            task_statuses(&pool, &first.projection_task_ids).await,
            ["DEAD_LETTER"]
        );
        assert_eq!(task_field(&pool, first_key, "retry_count").await, "1");
        let stored_error = task_field(&pool, first_key, "last_error").await;
        assert_eq!(
            projection_failure_disposition(&stored_error),
            ProjectionFailureDisposition::PermanentPayload(
                ProjectionPermanentPayloadReason::MissingVectorPayload
            )
        );
        let first_revision = task_field(&pool, first_key, "row_revision").await;
        // Create a genuinely different source-content key, then return to the
        // exact earlier poison. Replay must not rearm that immutable key even
        // though a newer row revision exists (valid ABA behavior stays intact).
        let second = write_receipt(&svc, &tenant, payload("poison-v2")).await;
        assert_ne!(second.projection_task_ids, first.projection_task_ids);
        let restarted = worker(
            ledger_store(&pool).await,
            svc.runtime_snapshot(),
            svc.catalog.clone(),
        );
        assert_eq!(restarted.run_once().await, (0, 1));
        let returned = write_receipt(&svc, &tenant, payload("poison-v1")).await;
        assert_eq!(returned.projection_task_ids, first.projection_task_ids);
        let reconciler = ReconciliationWorker {
            pool: pool.clone(),
            store: Arc::new(ledger_store(&pool).await),
            config: SystemCatalogConfig::current(),
            settings: ReconciliationSettings {
                enabled: true,
                max_source_scan_rows: 10,
                ..ReconciliationSettings::default()
            },
            metrics: Arc::new(crate::metrics::NoopMetrics),
            catalog: svc.catalog.clone(),
            runtime: svc.runtime_snapshot(),
        };
        // PROJECTION_PERMANENT_REPLAY_DIAGNOSTIC_BEFORE_BEGIN
        let diagnostic_payload = payload("poison-v1");
        owned_replay_diagnostic(
            &pool,
            &schema,
            &collection,
            &tenant,
            &diagnostic_payload,
            first_key,
            "before_first_reconciler",
        )
        .await;
        // PROJECTION_PERMANENT_REPLAY_DIAGNOSTIC_BEFORE_END
        for pass in 0..3 {
            let reports = reconciler.run_once().await;
            // PROJECTION_PERMANENT_REPLAY_DIAGNOSTIC_RECONCILER_BEGIN
            if pass == 0 {
                owned_replay_diagnostic(
                    &pool,
                    &schema,
                    &collection,
                    &tenant,
                    &diagnostic_payload,
                    first_key,
                    "after_first_reconciler_before_claim",
                )
                .await;
            }
            // PROJECTION_PERMANENT_REPLAY_DIAGNOSTIC_RECONCILER_END
            assert!(
                reports
                    .iter()
                    .all(|report| report.repair_tasks_enqueued == 0)
            );
            // PROJECTION_PERMANENT_REPLAY_DIAGNOSTIC_CLAIM_BEGIN
            let worker_outcome = restarted.run_once().await;
            if pass == 0 {
                println!(
                    "PROJECTION_PERMANENT_REPLAY_DIAGNOSTIC {}",
                    json!({
                        "phase": "first_global_worker_outcome",
                        "worker_outcome": worker_outcome,
                        "scope": "outcome only; owned snapshots determine owned-key changes",
                    })
                );
                owned_replay_diagnostic(
                    &pool,
                    &schema,
                    &collection,
                    &tenant,
                    &diagnostic_payload,
                    first_key,
                    "after_first_worker_claim",
                )
                .await;
            }
            // PROJECTION_PERMANENT_REPLAY_DIAGNOSTIC_CLAIM_END
            assert_eq!(worker_outcome, (0, 0));
            assert_eq!(task_field(&pool, first_key, "retry_count").await, "1");
            assert_eq!(
                task_field(&pool, first_key, "row_revision").await,
                first_revision
            );
            assert_eq!(
                task_field(&pool, first_key, "last_error").await,
                stored_error
            );
        }
        let store = ledger_store(&pool).await;
        let old_fence = ReadFence {
            min_outbox_lsn: String::new(),
            projection_task_ids: first.projection_task_ids.clone(),
            max_wait_ms: 100,
        };
        assert!(matches!(
            wait_for_fence(&store, &old_fence, "qdrant", &collection).await,
            FenceOutcome::Stale(StaleReadWarning::ProjectionMissing { .. })
        ));

        // Real catalog correction maps the existing JSONB vector source.
        // Both manifest and source content change, not merely ledger revision.
        let mut corrected = manifest.clone();
        corrected.checksum_sha256 = format!("corrected-source-{}", Uuid::new_v4().simple());
        corrected.projections[0].options = vec![opt("vector_field", "values_json")];
        svc.catalog
            .stage_catalog(
                corrected,
                PROJECT.to_string(),
                "2.0.0".to_string(),
                "any".to_string(),
            )
            .await
            .expect("stage corrected test catalog");
        svc.catalog
            .activate_catalog_for(PROJECT, "2.0.0")
            .await
            .expect("activate corrected test catalog");
        let fresh = write_receipt(&svc, &tenant, payload("corrected-v3")).await;
        assert_ne!(fresh.projection_task_ids, first.projection_task_ids);
        assert_ne!(fresh.manifest_checksum, first.manifest_checksum);
        let mut recovering = worker(
            ledger_store(&pool).await,
            svc.runtime_snapshot(),
            svc.catalog.clone(),
        );
        recovering.settings.max_retries = 1;
        // Valid vector, missing real collection: retryable backend failure.
        assert_eq!(recovering.run_once().await, (0, 1));
        assert_eq!(
            task_statuses(&pool, &fresh.projection_task_ids).await,
            ["DEAD_LETTER"]
        );
        assert!(projection_failure_is_automatically_repairable(
            &task_field(&pool, &fresh.projection_task_ids[0], "last_error").await
        ));
        create_qdrant_collection(&qdrant, &collection).await;
        let repaired = reconciler.run_once().await;
        assert_eq!(
            repaired
                .iter()
                .map(|report| report.repair_tasks_enqueued)
                .sum::<i64>(),
            1
        );
        assert_eq!(recovering.run_once().await, (1, 0));
        let points = served_vector_payloads(&svc, &tenant, &collection).await;
        assert_eq!(points.len(), 1);
        assert_eq!(points[0]["label"], "corrected-v3");
        assert_eq!(
            task_statuses(&pool, &first.projection_task_ids).await,
            ["DEAD_LETTER"]
        );
        assert!(matches!(
            wait_for_fence(&store, &old_fence, "qdrant", &collection).await,
            FenceOutcome::Stale(StaleReadWarning::ProjectionMissing { .. })
        ));
        let fresh_fence = ReadFence {
            min_outbox_lsn: String::new(),
            projection_task_ids: fresh.projection_task_ids.clone(),
            max_wait_ms: 100,
        };
        assert_eq!(
            wait_for_fence(&store, &fresh_fence, "qdrant", &collection).await,
            FenceOutcome::Cleared
        );
    })
    .catch_unwind()
    .await;
    delete_tasks_for_resource(&pool, &collection).await;
    drop_qdrant_collection(&qdrant, &collection).await;
    drop_schema(&pool, &schema).await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
