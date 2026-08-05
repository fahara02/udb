//! SRCH1 — LIVE tenant-isolation of the native SearchService vector path.
//!
//! Reproduces the customer-reported cross-tenant CLOBBER against a REAL Qdrant
//! (+ Postgres): two tenants ingest a document with the SAME source primary key
//! into the SAME shared collection. The fix namespaces the engine point-id by the
//! VERIFIED tenant (`tenant_scoped_point_id`, src/runtime/service/search_service/
//! model.rs), so:
//!   * tenant A's SERVED `Search` returns ONLY A's vector (B never overwrote it),
//!     and the returned hit id is the RAW source pk (the tenant prefix is stripped
//!     by `strip_tenant_point_id` on the read path), and
//!   * a teardown/delete by A removes ONLY A's point — tenant B's same-pk point
//!     survives.
//!
//! The write-side namespacing lives ONLY in the leader-owned worker passes, so the
//! ingest and teardown are driven through their real `pub(crate)` entry points
//! (`run_index_freshness_consumer` / `run_search_reindex_once`) after the SERVED
//! `CreateIndex` / `DeleteIndex` RPCs register the indexes. Every point id is still
//! computed by the real worker code, so reverting `tenant_scoped_point_id` breaks
//! the assertions (A's search returns 0 — B clobbered A; teardown deletes the lone
//! shared point — B vanishes).
//!
//! NOT purely served (documented): the harness runs no CDC engine, so the durable
//! CDC-journal rows the freshness/teardown workers consume are inserted directly
//! (the same technique `native_events_live.rs` uses for the outbox), and the
//! vector route `CreateIndex` does not register is registered by the test
//! (`record_vector_resource_backend`, as the EnsureResource RPC would). The
//! tenant-namespacing under test is unaffected — it is computed by the worker.
//!
//! Run with a live Qdrant + Postgres (see the session runbook):
//!   UDB_LIVE_OBJECT_TESTS=1 UDB_QDRANT_URL=http://127.0.0.1:56333 \
//!     cargo test --lib search_tenant_ -- --ignored --nocapture

use super::support::*;
use crate::proto::udb::core::search::services::v1 as search_pb;
use crate::proto::udb::core::search::services::v1::search_service_server::SearchService;
use crate::runtime::service::search_service::{
    run_index_freshness_consumer, run_search_reindex_once,
};
use std::sync::Arc;
use tonic::Request;
use uuid::Uuid;

const SOURCE_MESSAGE: &str = "udb.core.embedding.entity.v1.EmbeddingDocument";
const SOURCE_TOPIC: &str = "udb.embedding.embedding_documents.cdc";
const TOPIC_DELETED: &str = "udb.search.index.deleted.v1";
const PROJECT: &str = "default";
const OUTBOX: &str = "udb_system.outbox_events";
const JOURNAL: &str = "udb_system.udb_cdc_event_journal";

/// A request carrying the project header so the served handler resolves the same
/// `default` project the vector route / journal payloads / source rows use.
fn project_request<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("x-udb-project-id", PROJECT.parse().unwrap());
    request
}

async fn ensure_outbox_and_journal(pool: &sqlx::PgPool) {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS udb_system")
        .execute(pool)
        .await
        .expect("create udb_system schema");
    // Outbox the search service emits its index/freshness markers into.
    sqlx::query("DROP TABLE IF EXISTS udb_system.outbox_events CASCADE")
        .execute(pool)
        .await
        .expect("drop outbox");
    sqlx::query(
        "CREATE TABLE udb_system.outbox_events ( \
            event_seq     BIGSERIAL PRIMARY KEY, \
            event_id      UUID NOT NULL UNIQUE, \
            topic         TEXT NOT NULL, \
            partition_key TEXT NOT NULL DEFAULT '', \
            payload       JSONB NOT NULL, \
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW() )",
    )
    .execute(pool)
    .await
    .expect("create outbox table");
    // The durable CDC journal the freshness/teardown workers consume (columns the
    // worker join SQL reads: event_id / topic / payload / delivery_state /
    // published_at).
    sqlx::query("DROP TABLE IF EXISTS udb_system.udb_cdc_event_journal CASCADE")
        .execute(pool)
        .await
        .expect("drop journal");
    sqlx::query(
        "CREATE TABLE udb_system.udb_cdc_event_journal ( \
            event_id       UUID PRIMARY KEY, \
            topic          TEXT NOT NULL, \
            partition_key  TEXT NOT NULL DEFAULT '', \
            payload        JSONB NOT NULL, \
            delivery_state TEXT NOT NULL DEFAULT 'published', \
            published_at   TIMESTAMPTZ NOT NULL DEFAULT NOW() )",
    )
    .execute(pool)
    .await
    .expect("create journal table");
}

/// Insert one source-topic CDC journal row carrying a dense embedding for
/// `tenant`, so the freshness worker upserts `tenant:pk` for it. `age_secs`
/// orders application (older applied first — under a revert the newer clobbers).
async fn seed_freshness_event(
    pool: &sqlx::PgPool,
    tenant: &str,
    pk: &str,
    vector: [f32; 4],
    age_secs: i64,
) {
    let payload = serde_json::json!({
        "tenant_id": tenant,
        "project_id": PROJECT,
        "id": pk,
        "vector": vector.to_vec(),
        "payload": { "kind": "doc" },
    });
    sqlx::query(&format!(
        "INSERT INTO {JOURNAL} (event_id, topic, payload, delivery_state, published_at) \
         VALUES ($1::uuid, $2, $3::jsonb, 'published', NOW() - ($4 || ' seconds')::interval)"
    ))
    .bind(Uuid::new_v4().to_string())
    .bind(SOURCE_TOPIC)
    .bind(payload.to_string())
    .bind(age_secs.to_string())
    .execute(pool)
    .await
    .expect("seed freshness journal event");
}

/// Register an ACTIVE search index for `tenant` over the shared collection via the
/// SERVED `CreateIndex`. `provision_dims` > 0 provisions the Qdrant collection;
/// 0 reuses an already-provisioned collection (avoids a re-ensure conflict).
async fn register_index(
    svc: &crate::runtime::service::search_service::SearchServiceImpl,
    tenant: &str,
    index_name: &str,
    collection: &str,
    provision_dims: i32,
) {
    svc.create_index(project_request(search_pb::CreateIndexRequest {
        tenant_id: tenant.to_string(),
        index_name: index_name.to_string(),
        source_message_type: SOURCE_MESSAGE.to_string(),
        backend: "qdrant".to_string(),
        resource_name: collection.to_string(),
        vector_dims: provision_dims,
        metadata_json: String::new(),
    }))
    .await
    .expect("served create_index");
}

/// Served vector `Search` for `tenant` against `index_name`; returns the hit ids
/// (already tenant-prefix-stripped by the handler).
async fn search_ids(
    svc: &crate::runtime::service::search_service::SearchServiceImpl,
    tenant: &str,
    index_name: &str,
    query: [f32; 4],
) -> Vec<String> {
    svc.search(project_request(search_pb::SearchRequest {
        tenant_id: tenant.to_string(),
        index_name: index_name.to_string(),
        query_text: String::new(),
        query_vector: query.to_vec(),
        top_k: 10,
        mode: search_pb::SearchMode::Vector as i32,
        page_size: 0,
        page_token: String::new(),
    }))
    .await
    .expect("served search")
    .into_inner()
    .hits
    .into_iter()
    .map(|hit| hit.id)
    .collect()
}

#[tokio::test]
#[ignore = "requires live Qdrant+Postgres; run with UDB_LIVE_OBJECT_TESTS=1 UDB_QDRANT_URL=... -- --ignored"]
async fn search_tenant_isolation_no_clobber_and_teardown_survives_live() {
    let _guard = live_native_service_db_lock().lock().await;
    let qdrant_url =
        std::env::var("UDB_QDRANT_URL").unwrap_or_else(|_| "http://127.0.0.1:56333".to_string());
    unsafe {
        std::env::set_var("UDB_QDRANT_URL", &qdrant_url);
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
    }
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    ensure_outbox_and_journal(&pool).await;
    // The teardown worker enumerates the source PK for tenant A via the source
    // table; drop every FK on it so a bare source row can stand alone (the exact
    // constraint name is proto-declared, but drop them all to be robust).
    sqlx::raw_sql(
        "DO $$ DECLARE r record; BEGIN \
           FOR r IN SELECT c.conname FROM pg_constraint c \
             JOIN pg_class t ON c.conrelid = t.oid \
             JOIN pg_namespace n ON t.relnamespace = n.oid \
             WHERE n.nspname = 'udb_embedding' AND t.relname = 'embedding_documents' \
               AND c.contype = 'f' LOOP \
             EXECUTE format('ALTER TABLE udb_embedding.embedding_documents DROP CONSTRAINT %I', r.conname); \
           END LOOP; END $$;",
    )
    .execute(&pool)
    .await
    .expect("drop embedding_documents FKs");

    let collection = format!("udb_search_clobber_it_{}", Uuid::new_v4().simple());
    let tenant_a = Uuid::new_v4().to_string();
    let tenant_b = Uuid::new_v4().to_string();
    let index_a = "idx-a";
    let index_b = "idx-b";
    let pk = Uuid::new_v4().to_string(); // SAME source primary key for both tenants
    let vec_a = [0.10f32, 0.20, 0.30, 0.40];
    let vec_b = [0.90f32, 0.80, 0.70, 0.60];

    let svc = search_service(pool.clone())
        .await
        .with_outbox(Some(OUTBOX.to_string()));
    let runtime = svc.runtime.clone().expect("search service runtime");

    // (a) Register A's + B's index over the SHARED collection (served). A
    // provisions the Qdrant collection; B reuses it.
    register_index(&svc, &tenant_a, index_a, &collection, 4).await;
    register_index(&svc, &tenant_b, index_b, &collection, 0).await;

    // The route CreateIndex does not register (only the EnsureResource RPC does).
    runtime.record_vector_resource_backend(PROJECT, &collection, "qdrant", None);

    // Two CDC events reusing the SAME pk; A applied first, B second.
    seed_freshness_event(&pool, &tenant_a, &pk, vec_a, 10).await;
    seed_freshness_event(&pool, &tenant_b, &pk, vec_b, 0).await;

    let svc = Arc::new(svc);
    // Drive the real freshness worker → upserts tenant_scoped_point_id for each.
    run_index_freshness_consumer(svc.clone(), JOURNAL, 128)
        .await
        .expect("freshness consumer pass");

    // (b) CLOBBER assertion: A's served search returns ONLY A's point, and the hit
    // id is the RAW pk (prefix stripped). Under a revert, B's upsert overwrote the
    // single shared point (stamped tenant B), so A's search returns 0 hits.
    let a_hits = search_ids(&svc, &tenant_a, index_a, vec_a).await;
    assert_eq!(
        a_hits,
        vec![pk.clone()],
        "tenant A must see exactly its own vector at the RAW pk (revert ⇒ B clobbered A ⇒ 0 hits)"
    );

    // (c) TEARDOWN assertion: A deletes its index; only A's point is purged, B's
    // same-pk point survives. Seed A's source row so teardown can enumerate the pk.
    sqlx::query(
        "INSERT INTO udb_embedding.embedding_documents \
            (document_id, tenant_id, project_id, external_id, model_id, target_collection) \
         VALUES ($1::uuid, $2, $3, $4, $5, $6)",
    )
    .bind(&pk)
    .bind(&tenant_a)
    .bind(PROJECT)
    .bind(format!("ext-{}", Uuid::new_v4().simple()))
    .bind("model-x")
    .bind(&collection)
    .execute(&pool)
    .await
    .expect("seed tenant-A source row");

    svc.delete_index(project_request(search_pb::DeleteIndexRequest {
        tenant_id: tenant_a.clone(),
        index_name: index_a.to_string(),
    }))
    .await
    .expect("served delete_index");

    // Seed the index.deleted journal row the teardown pass consumes.
    let deleted_payload = serde_json::json!({
        "tenant_id": tenant_a,
        "project_id": PROJECT,
        "index_name": index_a,
    });
    sqlx::query(&format!(
        "INSERT INTO {JOURNAL} (event_id, topic, payload, delivery_state) \
         VALUES ($1::uuid, $2, $3::jsonb, 'published')"
    ))
    .bind(Uuid::new_v4().to_string())
    .bind(TOPIC_DELETED)
    .bind(deleted_payload.to_string())
    .execute(&pool)
    .await
    .expect("seed index.deleted journal event");

    run_search_reindex_once(svc.clone(), JOURNAL, 128)
        .await
        .expect("reindex/teardown pass");

    // Tenant B's index is still ACTIVE and its same-pk point must survive A's
    // teardown. Under a revert, the lone shared point (B's) was deleted by A's
    // teardown, so B's search returns 0 hits.
    let b_hits = search_ids(&svc, &tenant_b, index_b, vec_b).await;
    assert_eq!(
        b_hits,
        vec![pk.clone()],
        "tenant B's same-pk point must survive tenant A's teardown (revert ⇒ B's point deleted ⇒ 0 hits)"
    );

    // Cleanup: drop the throwaway Qdrant collection + native schemas.
    let http = reqwest::Client::new();
    let _ = http
        .delete(format!(
            "{}/collections/{collection}",
            qdrant_url.trim_end_matches('/')
        ))
        .send()
        .await;
    cleanup_native_service_db(&pool).await;
}
