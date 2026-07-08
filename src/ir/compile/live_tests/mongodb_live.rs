use std::collections::BTreeMap;

use serde_json::{Value as Json, json};

use crate::backend::BackendKind;
use crate::generation::CatalogManifest;
use crate::ir::compile::{
    CompileContext, CompileOperation, CompiledRendering, HttpMethod, compile_for_backend,
};
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    ConflictStrategy, LogicalDelete, LogicalRecord, LogicalResourceOp, LogicalSearch, LogicalWrite,
    ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;
use crate::runtime::executors::mongodb::MongoDbExecutor;
#[cfg(feature = "mongodb-native")]
use crate::runtime::executors::mongodb::{MongoDbConfig, MongoDbNativeConfig};
use crate::runtime::executors::{MutationExecutor, QueryExecutor, ResourceAdminExecutor};

use super::support::{customer_manifest, live_ir_enabled};

const MESSAGE: &str = "acme.billing.v1.Customer";

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1, MongoDB env, and feature=mongodb"]
async fn mongodb_compiled_text_search_resource_delete_match_live_documents() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Some(executor) = mongodb_live_executor().await else {
        eprintln!(
            "MongoDB env unset - set UDB_NOSQL_DSN/MONGODB_URI for native or UDB_NOSQL_API_URL for Data API"
        );
        return;
    };

    let collection = format!("udb_ir_live_{}", uuid::Uuid::new_v4().simple());
    let manifest = mongodb_manifest(&collection);

    let _ = ResourceAdminExecutor::drop_resource(&executor, &collection).await;
    execute_collection_resource(
        &executor,
        compile_resource(
            &manifest,
            LogicalResourceOp {
                op: ResourceOpKind::Ensure,
                resource_kind: ResourceKind::Collection,
                resource_name: collection.clone(),
                spec: None,
            },
        ),
    )
    .await;
    assert!(
        ResourceAdminExecutor::list_resources(&executor)
            .await
            .expect("execute compiled MongoDB collection list")
            .iter()
            .any(|name| name == &collection),
        "compiled MongoDB resource-op list must observe {collection}"
    );

    execute_mutation(
        &executor,
        mongodb_index_ensure_dispatch(compile_resource(
            &manifest,
            LogicalResourceOp {
                op: ResourceOpKind::Ensure,
                resource_kind: ResourceKind::Index,
                resource_name: "idx_customers_text".to_string(),
                spec: Some(json!({
                    "collection": collection.clone(),
                    "keys": { "name": "text", "email": "text" }
                })),
            },
        )),
    )
    .await;
    assert!(
        query_rows(
            &executor,
            mongodb_index_list_dispatch(compile_resource(
                &manifest,
                LogicalResourceOp {
                    op: ResourceOpKind::List,
                    resource_kind: ResourceKind::Index,
                    resource_name: String::new(),
                    spec: Some(json!({ "collection": collection.clone() })),
                },
            )),
        )
        .await
        .iter()
        .any(|row| row.get("name").and_then(Json::as_str) == Some("idx_customers_text")),
        "compiled MongoDB index list must observe idx_customers_text"
    );

    assert_eq!(
        execute_mutation(
            &executor,
            mongodb_mutation_dispatch(
                compile_write(
                    &manifest,
                    "tenant-a",
                    vec![
                        record("cust-1", "Alice", "alice@example.com", "tenant-a"),
                        record("cust-2", "Ana", "ana@example.com", "tenant-a"),
                    ],
                ),
                LogicalOpFamily::Write,
            ),
        )
        .await["affected_rows"]
            .as_i64(),
        Some(2)
    );
    assert_eq!(
        execute_mutation(
            &executor,
            mongodb_mutation_dispatch(
                compile_write(
                    &manifest,
                    "tenant-b",
                    vec![record(
                        "cust-3",
                        "Alice",
                        "alice@other.example.com",
                        "tenant-b",
                    )],
                ),
                LogicalOpFamily::Write,
            ),
        )
        .await["affected_rows"]
            .as_i64(),
        Some(1)
    );

    assert_eq!(
        search_names(&executor, &manifest, "tenant-a", "Alice").await,
        vec!["Alice".to_string()],
        "compiled MongoDB text search must respect tenant-a context"
    );

    assert_eq!(
        execute_mutation(
            &executor,
            mongodb_mutation_dispatch(
                compile_delete(&manifest, "tenant-a", "Alice"),
                LogicalOpFamily::Delete,
            ),
        )
        .await["affected_rows"]
            .as_i64(),
        Some(1)
    );

    assert!(
        search_names(&executor, &manifest, "tenant-a", "Alice")
            .await
            .is_empty(),
        "compiled MongoDB delete must remove only tenant-a Alice"
    );
    assert_eq!(
        search_names(&executor, &manifest, "tenant-b", "Alice").await,
        vec!["Alice".to_string()],
        "tenant-b Alice must remain after tenant-a compiled MongoDB delete"
    );

    ResourceAdminExecutor::drop_resource(&executor, &collection)
        .await
        .expect("drop live MongoDB collection");
}

async fn mongodb_live_executor() -> Option<MongoDbExecutor> {
    #[cfg(feature = "mongodb-native")]
    if let Ok(dsn) = std::env::var("UDB_NOSQL_DSN").or_else(|_| std::env::var("MONGODB_URI")) {
        let database = std::env::var("UDB_NOSQL_DATABASE")
            .ok()
            .or_else(|| MongoDbConfig::db_from_dsn(&dsn))
            .unwrap_or_else(|| "udb".to_string());
        return Some(
            MongoDbExecutor::new_native(MongoDbNativeConfig {
                dsn,
                database,
                timeout_secs: 30,
                app_name: Some("udb-ir-live-golden".to_string()),
                max_pool_size: None,
                direct_connection: None,
                retry_writes: None,
            })
            .await
            .expect("create live MongoDB native executor"),
        );
    }

    MongoDbExecutor::from_env()
}

fn mongodb_manifest(collection: &str) -> CatalogManifest {
    let mut manifest = customer_manifest("mongodb_live");
    manifest.tables[0].table = collection.to_string();
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
        conflict: ConflictStrategy::Error,
        return_fields: Vec::new(),
    };
    compile_json(manifest, tenant, CompileOperation::Write(&write))
}

fn compile_search(
    manifest: &CatalogManifest,
    tenant: &'static str,
    text_query: &str,
) -> (HttpMethod, String, Json) {
    let search = LogicalSearch {
        message_type: MESSAGE.to_string(),
        vector: None,
        text_query: Some(text_query.to_string()),
        filter: None,
        top_k: 10,
        score_threshold: None,
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
    match compile_for_backend(&BackendKind::Mongodb, operation, &ctx)
        .expect("MongoDB compiler is registered")
        .expect("operation compiles")
    {
        CompiledRendering::Json {
            backend,
            method,
            path,
            body,
        } => {
            assert_eq!(backend, BackendKind::Mongodb);
            (method, path, body)
        }
        other => panic!("expected MongoDB JSON rendering, got {other:?}"),
    }
}

#[derive(Debug, Clone, Copy)]
enum LogicalOpFamily {
    Write,
    Delete,
}

async fn execute_collection_resource(
    executor: &MongoDbExecutor,
    rendering: (HttpMethod, String, Json),
) {
    let (method, path, body) = rendering;
    assert_eq!(method, HttpMethod::Post);
    assert!(path.ends_with("createCollection"));
    let collection = body["collection"]
        .as_str()
        .expect("compiled MongoDB createCollection collection");
    ResourceAdminExecutor::ensure_resource(executor, collection, "{}")
        .await
        .expect("execute compiled MongoDB collection ensure");
}

fn mongodb_mutation_dispatch(
    rendering: (HttpMethod, String, Json),
    family: LogicalOpFamily,
) -> String {
    let (method, path, mut body) = rendering;
    assert_eq!(method, HttpMethod::Post);
    let Json::Object(map) = &mut body else {
        panic!("compiled MongoDB mutation body must be an object");
    };
    match family {
        LogicalOpFamily::Write => {
            if path.ends_with("insertMany") || map.contains_key("documents") {
                map.insert("operation".to_string(), json!("insert_many"));
            } else if path.ends_with("insertOne") || map.contains_key("document") {
                map.insert("operation".to_string(), json!("insert"));
            } else {
                panic!("unexpected MongoDB write path {path}");
            }
        }
        LogicalOpFamily::Delete => {
            assert!(path.ends_with("deleteMany"));
            map.insert("operation".to_string(), json!("delete_many"));
        }
    }
    map.insert("compiler_mediated".to_string(), json!(true));
    body.to_string()
}

fn mongodb_index_ensure_dispatch(rendering: (HttpMethod, String, Json)) -> String {
    let (method, path, body) = rendering;
    assert_eq!(method, HttpMethod::Post);
    assert!(path.ends_with("createIndex"));
    json!({
        "collection": body["collection"].clone(),
        "operation": "create_indexes",
        "indexes": [{
            "key": body["keys"].clone(),
            "name": body["name"].clone()
        }],
        "compiler_mediated": true,
    })
    .to_string()
}

fn mongodb_index_list_dispatch(rendering: (HttpMethod, String, Json)) -> String {
    let (method, path, body) = rendering;
    assert_eq!(method, HttpMethod::Post);
    assert!(path.ends_with("listIndexes"));
    json!({
        "collection": body["collection"].clone(),
        "operation": "list_indexes",
        "compiler_mediated": true,
    })
    .to_string()
}

fn mongodb_query_dispatch(rendering: (HttpMethod, String, Json)) -> String {
    let (method, path, mut body) = rendering;
    assert_eq!(method, HttpMethod::Post);
    assert!(path.ends_with("find"));
    let Json::Object(map) = &mut body else {
        panic!("compiled MongoDB query body must be an object");
    };
    map.insert("compiler_mediated".to_string(), json!(true));
    body.to_string()
}

async fn execute_mutation(executor: &MongoDbExecutor, spec_json: String) -> Json {
    let raw = MutationExecutor::mutate(executor, &spec_json)
        .await
        .expect("execute compiled MongoDB mutation/resource op");
    serde_json::from_str(&raw).expect("MongoDB mutation response JSON")
}

async fn query_rows(executor: &MongoDbExecutor, spec_json: String) -> Vec<Json> {
    let raw = QueryExecutor::query(executor, &spec_json)
        .await
        .expect("execute compiled MongoDB query");
    serde_json::from_str(&raw).expect("MongoDB query response JSON")
}

async fn search_names(
    executor: &MongoDbExecutor,
    manifest: &CatalogManifest,
    tenant: &'static str,
    text_query: &str,
) -> Vec<String> {
    let mut names = query_rows(
        executor,
        mongodb_query_dispatch(compile_search(manifest, tenant, text_query)),
    )
    .await
    .iter()
    .filter_map(|row| row.get("name").and_then(Json::as_str).map(str::to_string))
    .collect::<Vec<_>>();
    names.sort();
    names
}

fn record(id: &str, name: &str, email: &str, tenant_id: &str) -> LogicalRecord {
    let mut record = BTreeMap::new();
    record.insert("id".to_string(), LogicalValue::String(id.to_string()));
    record.insert("name".to_string(), LogicalValue::String(name.to_string()));
    record.insert("email".to_string(), LogicalValue::String(email.to_string()));
    record.insert(
        "tenant_id".to_string(),
        LogicalValue::String(tenant_id.to_string()),
    );
    record
}
