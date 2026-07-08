use std::collections::BTreeMap;
use std::time::{Duration, Instant};

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
use crate::runtime::executors::weaviate::{WeaviateExecutor, WeaviateHttpClient};
use crate::runtime::executors::{MutationExecutor, SearchExecutor};

use super::support::{customer_manifest, live_ir_enabled};

const MESSAGE: &str = "acme.billing.v1.Customer";

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1, UDB_WEAVIATE_DSN, and feature=weaviate"]
async fn weaviate_compiled_search_resource_delete_match_live_objects() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Ok(dsn) = std::env::var("UDB_WEAVIATE_DSN") else {
        eprintln!("UDB_WEAVIATE_DSN unset - skipping live Weaviate IR golden");
        return;
    };

    let (base_url, api_key) = crate::runtime::core::setup_data::parse_weaviate_dsn(&dsn);
    let executor = WeaviateExecutor::new(WeaviateHttpClient::new(base_url, api_key));
    let table_name = format!("udbIrLive{}", uuid::Uuid::new_v4().simple());
    let class_name = weaviate_class_from_table(&table_name);
    let manifest = weaviate_manifest(&table_name);

    let _ = execute_mutation(&executor, compile_resource_drop(&manifest, &class_name)).await;
    execute_mutation(
        &executor,
        compile_resource_ensure(&manifest, &class_name, customer_class_schema(&class_name)),
    )
    .await;
    let listed = execute_mutation(&executor, compile_resource_list(&manifest)).await;
    assert!(
        listed
            .get("classes")
            .and_then(Json::as_array)
            .expect("Weaviate schema list must include classes")
            .iter()
            .any(|class| class.get("class").and_then(Json::as_str) == Some(class_name.as_str())),
        "compiled Weaviate resource-op list must observe {class_name}"
    );

    execute_mutation(
        &executor,
        compile_write(
            &manifest,
            "tenant-a",
            vec![
                record(
                    "11111111-1111-4111-8111-111111111111",
                    "Alice",
                    "alice@example.com",
                    "tenant-a",
                ),
                record(
                    "22222222-2222-4222-8222-222222222222",
                    "Ana",
                    "ana@example.com",
                    "tenant-a",
                ),
            ],
        ),
    )
    .await;
    execute_mutation(
        &executor,
        compile_write(
            &manifest,
            "tenant-b",
            vec![record(
                "33333333-3333-4333-8333-333333333333",
                "Alice",
                "alice@other.example.com",
                "tenant-b",
            )],
        ),
    )
    .await;

    assert_eq!(
        eventually_search_names(
            &executor,
            &manifest,
            &class_name,
            "tenant-a",
            "Alice",
            has_only_alice,
        )
        .await,
        vec!["Alice".to_string()],
        "compiled Weaviate BM25 search must respect tenant-a context"
    );

    execute_mutation(&executor, compile_delete(&manifest, "tenant-a", "Alice")).await;

    assert!(
        eventually_search_names(
            &executor,
            &manifest,
            &class_name,
            "tenant-a",
            "Alice",
            |names| { names.is_empty() }
        )
        .await
        .is_empty(),
        "compiled Weaviate delete must remove only tenant-a Alice"
    );
    assert_eq!(
        eventually_search_names(
            &executor,
            &manifest,
            &class_name,
            "tenant-b",
            "Alice",
            has_only_alice,
        )
        .await,
        vec!["Alice".to_string()],
        "tenant-b Alice must remain after tenant-a compiled Weaviate delete"
    );

    execute_mutation(&executor, compile_resource_drop(&manifest, &class_name)).await;
}

fn weaviate_manifest(table_name: &str) -> CatalogManifest {
    let mut manifest = customer_manifest("weaviate_live");
    manifest.tables[0].table = table_name.to_string();
    manifest
}

fn weaviate_class_from_table(table_name: &str) -> String {
    let mut chars = table_name.chars();
    match chars.next() {
        None => String::new(),
        Some(ch) => ch.to_ascii_uppercase().to_string() + chars.as_str(),
    }
}

fn customer_class_schema(class_name: &str) -> Json {
    json!({
        "class": class_name,
        "vectorizer": "none",
        // No `id` property: Weaviate reserves the name (422) — the manifest
        // primary key rides as the OBJECT id (uuid5-derived by the compiler).
        "properties": [
            { "name": "name", "dataType": ["text"] },
            { "name": "email", "dataType": ["text"] },
            { "name": "tenant_id", "dataType": ["text"] }
        ]
    })
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

fn compile_resource_ensure(
    manifest: &CatalogManifest,
    class_name: &str,
    spec: Json,
) -> (HttpMethod, String, Json) {
    compile_resource(
        manifest,
        LogicalResourceOp {
            op: ResourceOpKind::Ensure,
            resource_kind: ResourceKind::Collection,
            resource_name: class_name.to_string(),
            spec: Some(spec),
        },
    )
}

fn compile_resource_drop(
    manifest: &CatalogManifest,
    class_name: &str,
) -> (HttpMethod, String, Json) {
    compile_resource(
        manifest,
        LogicalResourceOp {
            op: ResourceOpKind::Drop,
            resource_kind: ResourceKind::Collection,
            resource_name: class_name.to_string(),
            spec: None,
        },
    )
}

fn compile_resource_list(manifest: &CatalogManifest) -> (HttpMethod, String, Json) {
    compile_resource(
        manifest,
        LogicalResourceOp {
            op: ResourceOpKind::List,
            resource_kind: ResourceKind::Collection,
            resource_name: String::new(),
            spec: None,
        },
    )
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
    match compile_for_backend(&BackendKind::Weaviate, operation, &ctx)
        .expect("Weaviate compiler is registered")
        .expect("operation compiles")
    {
        CompiledRendering::Json {
            backend,
            method,
            path,
            body,
        } => {
            assert_eq!(backend, BackendKind::Weaviate);
            (method, path, body)
        }
        other => panic!("expected Weaviate JSON rendering, got {other:?}"),
    }
}

async fn execute_mutation(
    executor: &WeaviateExecutor,
    rendering: (HttpMethod, String, Json),
) -> Json {
    let (method, path, body) = rendering;
    let raw = MutationExecutor::mutate(executor, &dispatch_json(method, path, body))
        .await
        .expect("execute compiled Weaviate mutation/resource op");
    serde_json::from_str(&raw).expect("Weaviate mutation response must be JSON")
}

async fn search_names(
    executor: &WeaviateExecutor,
    manifest: &CatalogManifest,
    class_name: &str,
    tenant: &'static str,
    text_query: &str,
) -> Vec<String> {
    let (method, path, body) = compile_search(manifest, tenant, text_query);
    let raw = SearchExecutor::search(executor, &dispatch_json(method, path, body))
        .await
        .expect("execute compiled Weaviate search");
    let response: Json = serde_json::from_str(&raw).expect("Weaviate search response JSON");
    let mut names = response["data"]["Get"][class_name]
        .as_array()
        .expect("Weaviate search response must contain data.Get.<class>")
        .iter()
        .filter_map(|row| row.get("name").and_then(Json::as_str).map(str::to_string))
        .collect::<Vec<_>>();
    names.sort();
    names
}

async fn eventually_search_names(
    executor: &WeaviateExecutor,
    manifest: &CatalogManifest,
    class_name: &str,
    tenant: &'static str,
    text_query: &str,
    predicate: impl Fn(&[String]) -> bool,
) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let names = search_names(executor, manifest, class_name, tenant, text_query).await;
        if predicate(&names) || Instant::now() >= deadline {
            return names;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn has_only_alice(names: &[String]) -> bool {
    names.len() == 1 && names[0] == "Alice"
}

fn dispatch_json(method: HttpMethod, path: String, body: Json) -> String {
    json!({
        "method": method_token(method),
        "path": path,
        "body": body,
        "compiler_mediated": true,
    })
    .to_string()
}

fn method_token(method: HttpMethod) -> &'static str {
    match method {
        HttpMethod::Get => "GET",
        HttpMethod::Post => "POST",
        HttpMethod::Put => "PUT",
        HttpMethod::Patch => "PATCH",
        HttpMethod::Delete => "DELETE",
    }
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
