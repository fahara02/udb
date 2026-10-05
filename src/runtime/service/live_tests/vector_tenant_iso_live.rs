//! LIVE served-path tenant isolation of the DataBroker vector RPCs on a real
//! Qdrant + Postgres (C2, C3, C8 of the 0.5.24 ledger).
//!
//! Every request enters through the real DataBroker handlers
//! (`ensure_resource`, `vector_upsert`, `vector_search`, `vector_hybrid_search`)
//! with header credentials, exactly as an SDK call does:
//!
//! * C3 — the collection is created through served `EnsureResource` on one
//!   service, then written and searched through a SECOND service with a fresh
//!   runtime (an empty in-process route map). It resolves only because the route
//!   was persisted in `udb_system.udb_vector_resource_routes`; no test code
//!   registers a route.
//! * C2 — a caller filter that names the reserved `_tenant_id` key (here inside a
//!   `should`, the shape that could widen past the server-ANDed tenant clause) is
//!   refused with `InvalidArgument`; a caller without `udb:vector:read` is refused.
//! * C8 — a hybrid (vector + text) search returns only the calling tenant's
//!   points, served by Qdrant's native fusion query: the process-wide fallback
//!   counter `udb_vector_hybrid_fallback_total` does not move.
//!
//! Run with a live Qdrant + Postgres:
//!   UDB_QDRANT_URL=http://127.0.0.1:56333 cargo test --lib vector_tenant_iso_ \
//!     -- --ignored --nocapture

use std::sync::{Arc, RwLock};

use serde_json::json;
use tonic::{Code, Request};
use uuid::Uuid;

use super::support::{live_native_service_db_lock, live_pg_dsn, live_pg_pool};
use crate::engine::FsmState;
use crate::generation::CatalogManifest;
use crate::metrics::{MetricsRecorder, PrometheusMetrics};
use crate::proto::data_broker_server::DataBroker;
use crate::proto::{
    ResourceAdminRequest, VectorHybridSearchRequest, VectorPointMutation, VectorSearchRequest,
    VectorUpsertRequest,
};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::config::UdbConfig;
use crate::runtime::executor_utils::json_to_struct;
use crate::runtime::security::SecurityConfig;
use crate::runtime::service::DataBrokerService;

const FULL_SCOPES: &str = "udb:admin,udb:read,udb:write,udb:vector:read,udb:vector:write";

fn qdrant_url() -> String {
    let url =
        std::env::var("UDB_QDRANT_URL").unwrap_or_else(|_| "http://127.0.0.1:56333".to_string());
    // SAFETY: test-only env mutation, serialized by the native-service DB lock.
    unsafe {
        std::env::set_var("UDB_QDRANT_URL", &url);
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
    }
    url
}

fn header_security() -> SecurityConfig {
    SecurityConfig {
        tls_required: false,
        service_identity_required: false,
        mtls_required: false,
        allow_header_scopes: true,
        ..SecurityConfig::default()
    }
}

/// A served DataBrokerService over a FRESH runtime (its own empty in-process
/// vector route map) with an EMPTY manifest, so no vector collection is
/// declared: every route must come from the durable route table.
async fn fresh_service() -> DataBrokerService {
    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = live_pg_dsn();
    config.security = header_security();
    let runtime = DataBrokerRuntime::from_config(config).await;
    let lifecycle = Arc::new(RwLock::new(FsmState::Completed));
    let metrics: Arc<dyn MetricsRecorder> = Arc::new(PrometheusMetrics::new().expect("metrics"));
    let mut manifest = CatalogManifest::default();
    manifest.checksum_sha256 = format!("sha256:{}", Uuid::new_v4().simple());
    DataBrokerService::with_runtime_and_state(manifest, runtime, lifecycle, metrics, None, true)
}

fn with_ctx<T>(message: T, tenant: &str, scopes: &str) -> Request<T> {
    let mut req = Request::new(message);
    let md = req.metadata_mut();
    md.insert("x-tenant-id", tenant.parse().unwrap());
    md.insert("x-purpose", "admin".parse().unwrap());
    md.insert("x-scopes", scopes.parse().unwrap());
    req
}

fn point(id: &str, vector: [f32; 4], owner: &str) -> VectorPointMutation {
    VectorPointMutation {
        id: id.to_string(),
        vector: vector.to_vec(),
        payload: json_to_struct(&json!({ "owner": owner, "body": "shared document text" })),
        vector_name: String::new(),
    }
}

fn owners(set: &crate::proto::VectorSet) -> Vec<String> {
    set.points
        .iter()
        .map(|p| {
            p.payload
                .as_ref()
                .map(crate::runtime::executor_utils::struct_to_json)
                .and_then(|v| v.get("owner").and_then(|o| o.as_str()).map(str::to_string))
                .unwrap_or_default()
        })
        .collect()
}

async fn drop_collection(qdrant: &str, collection: &str) {
    let _ = reqwest::Client::new()
        .delete(format!(
            "{}/collections/{collection}",
            qdrant.trim_end_matches('/')
        ))
        .send()
        .await;
}

#[tokio::test]
#[ignore = "requires live Qdrant+Postgres; runs in the CI live lane (-- --ignored)"]
async fn vector_tenant_iso_served_routes_filters_and_hybrid_live() {
    let _guard = live_native_service_db_lock().lock().await;
    let qdrant = qdrant_url();
    SecurityConfig::install_global(header_security());
    let pool = live_pg_pool().await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog (incl. udb_vector_resource_routes)");

    let collection = format!("udb_vec_iso_{}", Uuid::new_v4().simple());
    let tenant_a = Uuid::new_v4().to_string();
    let tenant_b = Uuid::new_v4().to_string();

    // ── C3: create through EnsureResource on replica 1 ────────────────────────
    let replica1 = fresh_service().await;
    replica1
        .ensure_resource(with_ctx(
            ResourceAdminRequest {
                backend: "qdrant".to_string(),
                resource_name: collection.clone(),
                spec_json: json!({ "dimension": 4, "distance": "cosine" }).to_string(),
                ..Default::default()
            },
            &tenant_a,
            FULL_SCOPES,
        ))
        .await
        .expect("served EnsureResource");
    let persisted: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM udb_system.udb_vector_resource_routes \
         WHERE tenant_id = $1 AND collection = $2 AND backend = 'qdrant'",
    )
    .bind(&tenant_a)
    .bind(&collection)
    .fetch_one(&pool)
    .await
    .expect("read persisted route");
    assert_eq!(persisted, 1, "EnsureResource must persist the vector route");

    // Replica 2: a fresh runtime that never saw the EnsureResource call.
    let replica2 = fresh_service().await;
    let vec_a = [0.10f32, 0.20, 0.30, 0.40];
    let vec_b = [0.12f32, 0.21, 0.29, 0.41];
    for (tenant, id, vector, owner) in [
        (&tenant_a, Uuid::new_v4().to_string(), vec_a, "A"),
        (&tenant_b, Uuid::new_v4().to_string(), vec_b, "B"),
    ] {
        replica2
            .vector_upsert(with_ctx(
                VectorUpsertRequest {
                    collection: collection.clone(),
                    points: vec![point(&id, vector, owner)],
                    ..Default::default()
                },
                tenant,
                FULL_SCOPES,
            ))
            .await
            .unwrap_or_else(|err| {
                panic!("served VectorUpsert on the persisted route (tenant {owner}): {err:?}")
            });
    }

    let search = |filter: Option<serde_json::Value>| VectorSearchRequest {
        collection: collection.clone(),
        vector: vec_b.to_vec(),
        filter: filter.as_ref().and_then(json_to_struct),
        limit: 10,
        with_payload: true,
        ..Default::default()
    };
    let a_hits = replica2
        .vector_search(with_ctx(search(None), &tenant_a, FULL_SCOPES))
        .await
        .expect("served VectorSearch for tenant A")
        .into_inner();
    assert_eq!(
        owners(&a_hits),
        vec!["A"],
        "tenant A must see only its point"
    );

    // ── C2: a `should` on the reserved `_tenant_id` key naming tenant B ───────
    let widened = json!({ "should": [ { "key": "_tenant_id", "match": { "value": tenant_b } } ] });
    let err = replica2
        .vector_search(with_ctx(search(Some(widened)), &tenant_a, FULL_SCOPES))
        .await
        .expect_err("a reserved-key filter must be refused");
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
    assert!(err.message().contains("_tenant_id"), "{err:?}");

    // A caller without `udb:vector:read` is refused (served deny).
    let err = replica2
        .vector_search(with_ctx(
            search(None),
            &tenant_a,
            "udb:admin,udb:read,udb:write",
        ))
        .await
        .expect_err("a search without udb:vector:read must be refused");
    assert!(err.message().contains("udb:vector:read"), "{err:?}");

    // ── C8: hybrid search is tenant-only and served by native fusion ──────────
    let fallbacks_before = crate::runtime::metrics::vector_hybrid_fallbacks().get();
    let hybrid = replica2
        .vector_hybrid_search(with_ctx(
            VectorHybridSearchRequest {
                collection: collection.clone(),
                vector: vec_b.to_vec(),
                text_query: "shared document".to_string(),
                limit: 10,
                with_payload: true,
                ..Default::default()
            },
            &tenant_a,
            FULL_SCOPES,
        ))
        .await
        .expect("served VectorHybridSearch for tenant A")
        .into_inner();
    assert_eq!(
        owners(&hybrid),
        vec!["A"],
        "hybrid must return tenant A's point only"
    );
    assert_eq!(
        crate::runtime::metrics::vector_hybrid_fallbacks().get(),
        fallbacks_before,
        "hybrid search must be served by the native fusion query, not the dense fallback"
    );

    drop_collection(&qdrant, &collection).await;
    let _ = sqlx::query("DELETE FROM udb_system.udb_vector_resource_routes WHERE collection = $1")
        .bind(&collection)
        .execute(&pool)
        .await;
}
