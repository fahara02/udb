use std::collections::BTreeMap;

use serde_json::{Value as Json, json};

use crate::backend::BackendKind;
use crate::generation::{CatalogManifest, ManifestColumn};
use crate::ir::compile::{
    CompileContext, CompileOperation, CompiledRendering, HttpMethod, compile_for_backend,
};
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    ConflictStrategy, LogicalDelete, LogicalRecord, LogicalResourceOp, LogicalSearch, LogicalWrite,
    ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;
use crate::runtime::executors::qdrant::{QdrantExecutor, QdrantHttpClient};
use crate::runtime::executors::{MutationExecutor, ResourceAdminExecutor, SearchExecutor};

use super::support::{customer_manifest, live_ir_enabled};

const MESSAGE: &str = "acme.billing.v1.Customer";

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1, UDB_QDRANT_URL, and feature=qdrant"]
async fn qdrant_compiled_search_resource_delete_match_live_points() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Ok(url) = std::env::var("UDB_QDRANT_URL").or_else(|_| std::env::var("QDRANT_URL")) else {
        eprintln!("UDB_QDRANT_URL / QDRANT_URL unset - skipping live Qdrant IR golden");
        return;
    };

    let api_key = std::env::var("UDB_QDRANT_API_KEY")
        .or_else(|_| std::env::var("QDRANT_API_KEY"))
        .ok()
        .map(|key| key.trim().to_string())
        .filter(|key| !key.is_empty());
    let client = QdrantHttpClient {
        base_url: url.trim_end_matches('/').to_string(),
        api_key,
        http: crate::runtime::executors::http::HttpClientSpec::with_timeout(
            std::time::Duration::from_secs(30),
        )
        .build(),
    };
    let executor = QdrantExecutor(client);
    let collection = format!("udb_ir_live_{}", uuid::Uuid::new_v4().simple());
    let manifest = qdrant_manifest(&collection);

    let _ = ResourceAdminExecutor::drop_resource(&executor, &collection).await;
    let ensure = compile_resource(
        &manifest,
        LogicalResourceOp {
            op: ResourceOpKind::Ensure,
            resource_kind: ResourceKind::Collection,
            resource_name: collection.clone(),
            spec: Some(json!({
                "vectors": {
                    "size": 3,
                    "distance": "Cosine"
                }
            })),
        },
    );
    assert_eq!(ensure.0, HttpMethod::Put);
    ResourceAdminExecutor::ensure_resource(
        &executor,
        qdrant_collection_from_path(&ensure.1).expect("compiled Qdrant ensure path"),
        &ensure.2.to_string(),
    )
    .await
    .expect("execute compiled Qdrant collection ensure");

    let list = compile_resource(
        &manifest,
        LogicalResourceOp {
            op: ResourceOpKind::List,
            resource_kind: ResourceKind::Collection,
            resource_name: String::new(),
            spec: None,
        },
    );
    assert_eq!(list.0, HttpMethod::Get);
    assert!(
        ResourceAdminExecutor::list_resources(&executor)
            .await
            .expect("execute compiled Qdrant resource list")
            .iter()
            .any(|name| name == &collection),
        "compiled Qdrant resource-op list must observe {collection}"
    );

    assert_eq!(
        execute_mutation(
            &executor,
            compile_write(
                &manifest,
                "tenant-a",
                vec![
                    record(
                        "cust-1",
                        "Alice",
                        "alice@example.com",
                        "tenant-a",
                        [0.9, 0.1, 0.0]
                    ),
                    record(
                        "cust-2",
                        "Ana",
                        "ana@example.com",
                        "tenant-a",
                        [0.1, 0.9, 0.0]
                    ),
                ],
            ),
        )
        .await["affected_rows"]
            .as_i64(),
        Some(2)
    );
    assert_eq!(
        execute_mutation(
            &executor,
            compile_write(
                &manifest,
                "tenant-b",
                vec![record(
                    "cust-3",
                    "Alice",
                    "alice@other.example.com",
                    "tenant-b",
                    [0.9, 0.1, 0.0],
                )],
            ),
        )
        .await["affected_rows"]
            .as_i64(),
        Some(1)
    );

    assert_eq!(
        search_names(&executor, &manifest, "tenant-a", [0.9, 0.1, 0.0]).await,
        vec!["Alice".to_string()],
        "compiled Qdrant vector search must respect tenant-a context"
    );

    let deleted = execute_mutation(&executor, compile_delete(&manifest, "tenant-a", "Alice")).await;
    assert_eq!(deleted["matched_by_filter"].as_bool(), Some(true));

    assert!(
        search_names(&executor, &manifest, "tenant-a", [0.9, 0.1, 0.0])
            .await
            .is_empty(),
        "compiled Qdrant delete must remove only tenant-a Alice"
    );
    assert_eq!(
        search_names(&executor, &manifest, "tenant-b", [0.9, 0.1, 0.0]).await,
        vec!["Alice".to_string()],
        "tenant-b Alice must remain after tenant-a compiled Qdrant delete"
    );

    ResourceAdminExecutor::drop_resource(&executor, &collection)
        .await
        .expect("drop live Qdrant collection");
}

fn qdrant_manifest(collection: &str) -> CatalogManifest {
    let mut manifest = customer_manifest("qdrant_live");
    let table = &mut manifest.tables[0];
    table.table = collection.to_string();
    table.columns.push(ManifestColumn {
        field_name: "_vector".to_string(),
        column_name: "_vector".to_string(),
        proto_type: "array<float>".to_string(),
        sql_type: "vector(3)".to_string(),
        ..Default::default()
    });
    manifest
}

fn compile_write(
    manifest: &CatalogManifest,
    tenant: &'static str,
    records: Vec<LogicalRecord>,
) -> (HttpMethod, String, Json) {
    let write = LogicalWrite {
        message_type: MESSAGE.to_string(),
        records,
        conflict: ConflictStrategy::Replace,
        return_fields: Vec::new(),
    };
    compile_json(manifest, tenant, CompileOperation::Write(&write))
}

fn compile_search(
    manifest: &CatalogManifest,
    tenant: &'static str,
    vector: [f32; 3],
) -> (HttpMethod, String, Json) {
    let search = LogicalSearch {
        message_type: MESSAGE.to_string(),
        vector: Some(vector.into_iter().collect()),
        text_query: None,
        filter: None,
        top_k: 10,
        // Cosine similarity floor: the near-identical vector scores ~1.0 and
        // the orthogonal-ish other-tenant-a point ~0.22, so 0.5 keeps the
        // oracle's "only the matching point returns" assertions meaningful
        // (top_k alone would return every point in the tenant, scored).
        score_threshold: Some(0.5),
        require_hybrid: false,
        with_vector: false,
        with_payload: true,
    };
    compile_json(manifest, tenant, CompileOperation::Search(&search))
}

fn compile_delete(
    manifest: &CatalogManifest,
    tenant: &'static str,
    name: &str,
) -> (HttpMethod, String, Json) {
    let delete = LogicalDelete {
        message_type: MESSAGE.to_string(),
        filter: LogicalFilter::Comparison {
            field: "name".to_string(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String(name.to_string()),
        },
        return_fields: Vec::new(),
    };
    compile_json(manifest, tenant, CompileOperation::Delete(&delete))
}

fn compile_resource(
    manifest: &CatalogManifest,
    op: LogicalResourceOp,
) -> (HttpMethod, String, Json) {
    compile_json(manifest, "tenant-a", CompileOperation::ResourceOp(&op))
}

fn compile_json(
    manifest: &CatalogManifest,
    tenant: &'static str,
    operation: CompileOperation<'_>,
) -> (HttpMethod, String, Json) {
    let ctx = CompileContext::new(manifest).with_tenant(tenant);
    match compile_for_backend(&BackendKind::Qdrant, operation, &ctx)
        .expect("Qdrant compiler is registered")
        .expect("operation compiles")
    {
        CompiledRendering::Json {
            backend,
            method,
            path,
            body,
        } => {
            assert_eq!(backend, BackendKind::Qdrant);
            (method, path, body)
        }
        other => panic!("expected Qdrant JSON rendering, got {other:?}"),
    }
}

async fn execute_mutation(
    executor: &QdrantExecutor,
    rendering: (HttpMethod, String, Json),
) -> Json {
    let (method, path, body) = rendering;
    assert!(
        matches!(method, HttpMethod::Put | HttpMethod::Post),
        "Qdrant point mutations must compile to PUT/POST"
    );
    let raw = MutationExecutor::mutate(executor, &qdrant_dispatch_json("mutate", path, body))
        .await
        .expect("execute compiled Qdrant mutation");
    serde_json::from_str(&raw).expect("Qdrant mutation response must be JSON")
}

async fn search_names(
    executor: &QdrantExecutor,
    manifest: &CatalogManifest,
    tenant: &'static str,
    vector: [f32; 3],
) -> Vec<String> {
    let (method, path, body) = compile_search(manifest, tenant, vector);
    assert_eq!(method, HttpMethod::Post);
    let raw = SearchExecutor::search(executor, &qdrant_dispatch_json("search", path, body))
        .await
        .expect("execute compiled Qdrant search");
    let response: Vec<Json> = serde_json::from_str(&raw).expect("Qdrant search response JSON");
    let mut names = response
        .iter()
        .filter_map(|point| {
            point
                .get("payload")
                .and_then(|payload| payload.get("name"))
                .and_then(Json::as_str)
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn qdrant_dispatch_json(operation: &str, path: String, body: Json) -> String {
    let mut spec = body;
    let Json::Object(map) = &mut spec else {
        panic!("compiled Qdrant body must be a JSON object");
    };
    if let Some(collection) = qdrant_collection_from_path(&path) {
        map.insert("collection".to_string(), json!(collection));
    }
    match operation {
        "mutate" => {
            let request_op = if path.contains("/points/delete") {
                "delete"
            } else {
                "upsert"
            };
            map.entry("operation".to_string())
                .or_insert_with(|| json!(request_op));
        }
        "search" => {}
        other => panic!("unsupported Qdrant dispatch operation {other}"),
    }
    map.insert("compiler_mediated".to_string(), json!(true));
    spec.to_string()
}

fn qdrant_collection_from_path(path: &str) -> Option<&str> {
    let mut parts = path.split('/').filter(|part| !part.is_empty());
    while let Some(part) = parts.next() {
        if part == "collections" {
            return parts.next();
        }
    }
    None
}

fn record(id: &str, name: &str, email: &str, tenant_id: &str, vector: [f32; 3]) -> LogicalRecord {
    let mut record = BTreeMap::new();
    record.insert("id".to_string(), LogicalValue::String(id.to_string()));
    record.insert("name".to_string(), LogicalValue::String(name.to_string()));
    record.insert("email".to_string(), LogicalValue::String(email.to_string()));
    record.insert(
        "tenant_id".to_string(),
        LogicalValue::String(tenant_id.to_string()),
    );
    record.insert(
        "_vector".to_string(),
        LogicalValue::Array(
            vector
                .into_iter()
                .map(|value| LogicalValue::Float(value as f64))
                .collect(),
        ),
    );
    record
}
