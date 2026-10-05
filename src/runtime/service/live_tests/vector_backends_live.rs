//! LIVE tenant isolation of the non-Qdrant vector backends through the runtime's
//! routed vector seam (`vector_upsert_routed` / `vector_search_routed`, the path
//! the DataBroker vector RPCs and the search service take for an
//! Elasticsearch / Weaviate / Pinecone collection):
//!
//! * C1 (Elasticsearch) — a caller filter with ONLY a `should` group still gets
//!   the tenant scope as an unconditional conjunct: tenant A never sees B's
//!   matching point.
//! * C5 (Weaviate) — tenants `acme` and `acme-eu` share a class created by
//!   `EnsureResource` (`tokenization: field`): `acme` never matches `acme-eu`.
//!   A class whose scope properties use Weaviate's default `word` tokenization
//!   is refused with `FailedPrecondition` instead of being searched.
//! * C6 (Weaviate) — re-upserting the same caller id replaces (deterministic
//!   object id), and the hit carries the caller id and the stored payload.
//! * C7 (Pinecone, local HTTP stub — no Pinecone in CI) — the project rides as
//!   the namespace and the logical collection + tenant are both in the
//!   metadata filter.
//!
//! The Elasticsearch / Weaviate tests need `UDB_ELASTIC_DSN` / `UDB_WEAVIATE_DSN`;
//! in the CI live lane (`UDB_LIVE_AUTH_TESTS=1`) a missing DSN FAILS the test.

use serde_json::json;
use tonic::Code;
use uuid::Uuid;

use super::support::{live_native_service_db_lock, live_pg_dsn};
use crate::generation::CatalogManifest;
use crate::proto::{VectorPointMutation, VectorSearchRequest, VectorSet, VectorUpsertRequest};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::config::UdbConfig;
use crate::runtime::core::ResolvedBackendSelector;
use crate::runtime::executor_utils::{json_to_struct, struct_to_json};

const PROJECT: &str = "default";

/// The backend DSN for a Phase 3 test. In the CI live lane a missing DSN is a
/// FAILURE (a skipped isolation test proves nothing); locally the test skips.
fn backend_dsn(name: &str) -> Option<String> {
    super::support::require_live_dsn(name)
}

async fn runtime() -> DataBrokerRuntime {
    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = live_pg_dsn();
    DataBrokerRuntime::from_config(config).await
}

fn ctx(tenant: &str) -> crate::RequestContext {
    crate::RequestContext {
        tenant_id: tenant.to_string(),
        project_id: PROJECT.to_string(),
        scopes: vec![
            "udb:vector:read".to_string(),
            "udb:vector:write".to_string(),
        ],
        ..crate::RequestContext::default()
    }
}

fn route(backend: &str) -> Option<ResolvedBackendSelector> {
    Some(ResolvedBackendSelector {
        backend: backend.to_string(),
        instance: None,
    })
}

fn point(id: &str, vector: [f32; 4], owner: &str) -> VectorPointMutation {
    VectorPointMutation {
        id: id.to_string(),
        vector: vector.to_vec(),
        payload: json_to_struct(&json!({ "owner": owner, "kind": "doc" })),
        vector_name: String::new(),
    }
}

async fn upsert(
    runtime: &DataBrokerRuntime,
    backend: &str,
    collection: &str,
    tenant: &str,
    points: Vec<VectorPointMutation>,
) {
    runtime
        .vector_upsert_routed(
            &CatalogManifest::default(),
            VectorUpsertRequest {
                collection: collection.to_string(),
                points,
                ..Default::default()
            },
            ctx(tenant),
            route(backend),
        )
        .await
        .unwrap_or_else(|err| panic!("{backend} upsert for {tenant}: {err:?}"));
}

async fn search(
    runtime: &DataBrokerRuntime,
    backend: &str,
    collection: &str,
    tenant: &str,
    filter: Option<serde_json::Value>,
) -> Result<VectorSet, tonic::Status> {
    runtime
        .vector_search_routed(
            &CatalogManifest::default(),
            VectorSearchRequest {
                collection: collection.to_string(),
                vector: vec![0.1, 0.2, 0.3, 0.4],
                filter: filter.as_ref().and_then(json_to_struct),
                limit: 10,
                with_payload: true,
                ..Default::default()
            },
            ctx(tenant),
            route(backend),
        )
        .await
}

fn owners(set: &VectorSet) -> Vec<String> {
    set.points
        .iter()
        .map(|p| {
            p.payload
                .as_ref()
                .map(struct_to_json)
                .and_then(|v| v.get("owner").and_then(|o| o.as_str()).map(str::to_string))
                .unwrap_or_default()
        })
        .collect()
}

#[tokio::test]
#[ignore = "requires live Elasticsearch+Postgres; runs in the CI live lane (-- --ignored)"]
async fn vector_es_should_only_filter_is_tenant_scoped_live() {
    let Some(es) = backend_dsn("UDB_ELASTIC_DSN") else {
        return;
    };
    let _guard = live_native_service_db_lock().lock().await;
    let runtime = runtime().await;
    let collection = format!("udb_c1_{}", Uuid::new_v4().simple());
    // Hyphen-free tenant ids keep this test about the filter SHAPE, independent
    // of the index's keyword mapping.
    let tenant_a = format!("ta{}", Uuid::new_v4().simple());
    let tenant_b = format!("tb{}", Uuid::new_v4().simple());
    runtime
        .vector_ensure_backend_kind_target(
            "elasticsearch",
            None,
            PROJECT,
            &collection,
            4,
            "cosine",
            "",
            &[],
        )
        .await
        .expect("ensure ES vector index");
    upsert(
        &runtime,
        "elasticsearch",
        &collection,
        &tenant_a,
        vec![point("a-1", [0.1, 0.2, 0.3, 0.4], "A")],
    )
    .await;
    upsert(
        &runtime,
        "elasticsearch",
        &collection,
        &tenant_b,
        vec![point("b-1", [0.1, 0.2, 0.3, 0.41], "B")],
    )
    .await;

    // A should-ONLY caller filter that B's point also satisfies.
    let should_only = json!({ "should": [ { "key": "kind", "match": { "value": "doc" } } ] });
    let hits = search(
        &runtime,
        "elasticsearch",
        &collection,
        &tenant_a,
        Some(should_only),
    )
    .await
    .expect("ES search with a should-only filter");
    assert_eq!(
        owners(&hits),
        vec!["A"],
        "a should-only filter must not widen past tenant A"
    );

    let _ = reqwest::Client::new()
        .delete(format!("{}/{collection}", es.trim_end_matches('/')))
        .send()
        .await;
}

#[tokio::test]
#[ignore = "requires live Weaviate+Postgres; runs in the CI live lane (-- --ignored)"]
async fn vector_weaviate_acme_never_matches_acme_eu_live() {
    let Some(weaviate) = backend_dsn("UDB_WEAVIATE_DSN") else {
        return;
    };
    let _guard = live_native_service_db_lock().lock().await;
    let runtime = runtime().await;
    let http = reqwest::Client::new();
    let base = weaviate.trim_end_matches('/').to_string();

    // EnsureResource class: scope properties are `tokenization: field`.
    let ensured = format!("UdbC5f{}", Uuid::new_v4().simple());
    runtime
        .vector_ensure_backend_kind_target(
            "weaviate",
            None,
            PROJECT,
            &ensured,
            4,
            "cosine",
            "",
            &[],
        )
        .await
        .expect("ensure Weaviate class");
    upsert(
        &runtime,
        "weaviate",
        &ensured,
        "acme",
        vec![point(
            &Uuid::new_v4().to_string(),
            [0.1, 0.2, 0.3, 0.4],
            "acme",
        )],
    )
    .await;
    upsert(
        &runtime,
        "weaviate",
        &ensured,
        "acme-eu",
        vec![point(
            &Uuid::new_v4().to_string(),
            [0.1, 0.2, 0.3, 0.41],
            "acme-eu",
        )],
    )
    .await;
    let hits = search(&runtime, "weaviate", &ensured, "acme", None)
        .await
        .expect("Weaviate search for acme");
    assert_eq!(
        owners(&hits),
        vec!["acme"],
        "tenant `acme` must never match `acme-eu`"
    );

    // A class NOT created by EnsureResource: Weaviate's default `word`
    // tokenization on the scope properties. Searching it is refused.
    let word = format!("UdbC5w{}", Uuid::new_v4().simple());
    let created = http
        .post(format!("{base}/v1/schema"))
        .json(&json!({
            "class": word,
            "vectorizer": "none",
            "properties": [
                { "name": "_tenant_id", "dataType": ["text"] },
                { "name": "_project_id", "dataType": ["text"] }
            ]
        }))
        .send()
        .await
        .expect("create word-tokenized class");
    assert!(
        created.status().is_success(),
        "create class: {}",
        created.status()
    );
    let err = search(&runtime, "weaviate", &word, "acme", None)
        .await
        .expect_err("a word-tokenized scope must refuse the scoped search");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert!(err.message().contains("tokenization"), "{err:?}");

    for class in [&ensured, &word] {
        let _ = http
            .delete(format!("{base}/v1/schema/{class}"))
            .send()
            .await;
    }
}

#[tokio::test]
#[ignore = "requires live Weaviate+Postgres; runs in the CI live lane (-- --ignored)"]
async fn vector_weaviate_deterministic_id_and_payload_live() {
    let Some(weaviate) = backend_dsn("UDB_WEAVIATE_DSN") else {
        return;
    };
    let _guard = live_native_service_db_lock().lock().await;
    let runtime = runtime().await;
    let class = format!("UdbC6{}", Uuid::new_v4().simple());
    let tenant = format!("t{}", Uuid::new_v4().simple());
    runtime
        .vector_ensure_backend_kind_target("weaviate", None, PROJECT, &class, 4, "cosine", "", &[])
        .await
        .expect("ensure Weaviate class");
    // The same caller id twice: the second write replaces the first.
    upsert(
        &runtime,
        "weaviate",
        &class,
        &tenant,
        vec![point("doc-1", [0.4, 0.3, 0.2, 0.1], "first")],
    )
    .await;
    upsert(
        &runtime,
        "weaviate",
        &class,
        &tenant,
        vec![point("doc-1", [0.1, 0.2, 0.3, 0.4], "second")],
    )
    .await;
    let hits = search(&runtime, "weaviate", &class, &tenant, None)
        .await
        .expect("Weaviate search");
    assert_eq!(
        hits.points.len(),
        1,
        "a re-upsert must replace, not duplicate: {hits:?}"
    );
    assert_eq!(
        hits.points[0].id, "doc-1",
        "the hit id must be the caller id"
    );
    assert_eq!(
        owners(&hits),
        vec!["second"],
        "the hit must carry the stored payload"
    );

    let _ = reqwest::Client::new()
        .delete(format!(
            "{}/v1/schema/{class}",
            weaviate.trim_end_matches('/')
        ))
        .send()
        .await;
}

/// One captured request to the Pinecone stub: `(path, JSON body)`.
type Captured = std::sync::Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>>;

/// A minimal HTTP/1.1 Pinecone data-plane stub: records every request's path and
/// JSON body and answers `/vectors/upsert` and `/query` with valid empty results.
async fn pinecone_stub() -> (String, Captured) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind pinecone stub");
    let addr = listener.local_addr().expect("stub addr");
    let captured: Captured = Default::default();
    let sink = captured.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let sink = sink.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let (head_end, content_length) = loop {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..pos]).to_ascii_lowercase();
                        let length = head
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length:"))
                            .and_then(|value| value.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        break (pos + 4, length);
                    }
                };
                while buf.len() < head_end + content_length {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                let path = head
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or_default()
                    .to_string();
                let end = (head_end + content_length).min(buf.len());
                let body: serde_json::Value =
                    serde_json::from_slice(&buf[head_end..end]).unwrap_or(serde_json::Value::Null);
                let reply = if path.starts_with("/vectors/upsert") {
                    json!({ "upsertedCount": 1 })
                } else if path.starts_with("/query") {
                    json!({ "matches": [], "namespace": body.get("namespace").cloned().unwrap_or_default() })
                } else {
                    json!({})
                };
                sink.lock().expect("stub capture").push((path, body));
                let reply = reply.to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    (format!("http://{addr}"), captured)
}

#[tokio::test]
#[ignore = "requires live Postgres for the runtime; runs in the CI live lane (-- --ignored)"]
async fn vector_pinecone_namespace_and_collection_filter_live() {
    use crate::runtime::core::setup_data::{PINECONE_COLLECTION_KEY, pinecone_namespace};
    use crate::runtime::executors::pinecone::PineconeHttpClient;

    let _guard = live_native_service_db_lock().lock().await;
    let (base, captured) = pinecone_stub().await;
    let mut runtime = runtime().await;
    let client = PineconeHttpClient::new(base, "stub-key");
    runtime
        .pinecone_instances
        .insert("primary".to_string(), client.clone());
    runtime.pinecone = Some(client);

    let collection = format!("udb_c7_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    upsert(
        &runtime,
        "pinecone",
        &collection,
        &tenant,
        vec![point("p-1", [0.1, 0.2, 0.3, 0.4], "A")],
    )
    .await;
    search(&runtime, "pinecone", &collection, &tenant, None)
        .await
        .expect("Pinecone search against the stub");

    let requests = captured.lock().expect("captured").clone();
    let namespace = pinecone_namespace(PROJECT);
    let (_, upsert_body) = requests
        .iter()
        .find(|(path, _)| path.starts_with("/vectors/upsert"))
        .unwrap_or_else(|| panic!("no upsert reached the stub: {requests:?}"));
    assert_eq!(upsert_body["namespace"], json!(namespace), "{upsert_body}");
    let metadata = &upsert_body["vectors"][0]["metadata"];
    assert_eq!(
        metadata[PINECONE_COLLECTION_KEY],
        json!(collection),
        "{upsert_body}"
    );
    assert_eq!(metadata["_tenant_id"], json!(tenant), "{upsert_body}");

    let (_, query_body) = requests
        .iter()
        .find(|(path, _)| path.starts_with("/query"))
        .unwrap_or_else(|| panic!("no query reached the stub: {requests:?}"));
    assert_eq!(query_body["namespace"], json!(namespace), "{query_body}");
    let filter = query_body["filter"].to_string();
    assert!(
        filter.contains(PINECONE_COLLECTION_KEY) && filter.contains(&collection),
        "the query must filter on the logical collection: {filter}"
    );
    assert!(
        filter.contains("_tenant_id") && filter.contains(&tenant),
        "the query must filter on the tenant: {filter}"
    );
}
