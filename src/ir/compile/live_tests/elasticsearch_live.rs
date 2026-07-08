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
use crate::runtime::executors::elasticsearch::{ElasticsearchExecutor, ElasticsearchHttpClient};
use crate::runtime::executors::{MutationExecutor, SearchExecutor};

use super::support::{customer_manifest, live_ir_enabled};

const MESSAGE: &str = "acme.billing.v1.Customer";

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1, UDB_ELASTIC_DSN, and feature=elasticsearch"]
async fn elasticsearch_compiled_search_resource_delete_match_live_rows() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Ok(dsn) = std::env::var("UDB_ELASTIC_DSN") else {
        eprintln!("UDB_ELASTIC_DSN unset - skipping live Elasticsearch IR golden");
        return;
    };

    let (base_url, auth) = crate::runtime::core::setup_data::parse_elasticsearch_dsn(&dsn);
    let client = ElasticsearchHttpClient::new(base_url, auth);
    let executor = ElasticsearchExecutor::new(client.clone());
    let index = format!("udb-ir-live-{}", uuid::Uuid::new_v4().simple());
    let manifest = elasticsearch_manifest(&index);

    drop_index_if_exists(&client, &index).await;
    execute_mutation(
        &executor,
        compile_resource(
            &manifest,
            "tenant-a",
            LogicalResourceOp {
                op: ResourceOpKind::Ensure,
                resource_kind: ResourceKind::Index,
                resource_name: index.clone(),
                spec: Some(customer_index_mapping()),
            },
        ),
    )
    .await;

    let listed = execute_mutation(
        &executor,
        compile_resource(
            &manifest,
            "tenant-a",
            LogicalResourceOp {
                op: ResourceOpKind::List,
                resource_kind: ResourceKind::Index,
                resource_name: String::new(),
                spec: None,
            },
        ),
    )
    .await;
    assert!(
        listed
            .as_array()
            .expect("ES index list must return an array")
            .iter()
            .any(|row| row.get("index").and_then(Json::as_str) == Some(index.as_str())),
        "compiled Elasticsearch resource-op list must observe {index}"
    );

    let tenant_a_seed = execute_mutation(
        &executor,
        compile_write(
            &manifest,
            "tenant-a",
            vec![
                record([
                    ("id", "cust-1"),
                    ("name", "Alice"),
                    ("email", "alice@example.com"),
                    ("tenant_id", "tenant-a"),
                ]),
                record([
                    ("id", "cust-2"),
                    ("name", "Ana"),
                    ("email", "ana@example.com"),
                    ("tenant_id", "tenant-a"),
                ]),
            ],
        ),
    )
    .await;
    assert_bulk_ok(&tenant_a_seed);

    let tenant_b_seed = execute_mutation(
        &executor,
        compile_write(
            &manifest,
            "tenant-b",
            vec![record([
                ("id", "cust-3"),
                ("name", "Alice"),
                ("email", "alice@other.example.com"),
                ("tenant_id", "tenant-b"),
            ])],
        ),
    )
    .await;
    assert_bulk_ok(&tenant_b_seed);

    refresh_index(&client, &index).await;

    assert_eq!(
        search_customer_ids(&executor, &manifest, "tenant-a", "Alice").await,
        vec!["cust-1".to_string()],
        "compiled Elasticsearch search must respect the tenant context predicate"
    );

    let deleted =
        execute_mutation(&executor, compile_delete(&manifest, "tenant-a", "cust-1")).await;
    assert_eq!(deleted["deleted"].as_i64(), Some(1));
    refresh_index(&client, &index).await;

    assert!(
        search_customer_ids(&executor, &manifest, "tenant-a", "Alice")
            .await
            .is_empty(),
        "compiled Elasticsearch delete must remove only tenant-a Alice"
    );
    assert_eq!(
        search_customer_ids(&executor, &manifest, "tenant-b", "Alice").await,
        vec!["cust-3".to_string()],
        "tenant-b Alice must remain after tenant-a compiled delete"
    );

    drop_index_if_exists(&client, &index).await;
}

fn elasticsearch_manifest(index: &str) -> CatalogManifest {
    let mut manifest = customer_manifest("elasticsearch_live");
    manifest.tables[0].table = index.to_string();
    manifest
}

fn customer_index_mapping() -> Json {
    json!({
        "mappings": {
            "properties": {
                "id": { "type": "keyword" },
                "name": { "type": "text" },
                "email": { "type": "text" },
                "tenant_id": { "type": "keyword" }
            }
        }
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
    id: &str,
) -> (HttpMethod, String, Json) {
    let delete = LogicalDelete {
        message_type: MESSAGE.to_string(),
        filter: LogicalFilter::Comparison {
            field: "id".to_string(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String(id.to_string()),
        },
        return_fields: Vec::new(),
    };
    compile_json(manifest, tenant, CompileOperation::Delete(&delete))
}

fn compile_resource(
    manifest: &CatalogManifest,
    tenant: &'static str,
    op: LogicalResourceOp,
) -> (HttpMethod, String, Json) {
    compile_json(manifest, tenant, CompileOperation::ResourceOp(&op))
}

fn compile_json(
    manifest: &CatalogManifest,
    tenant: &'static str,
    operation: CompileOperation<'_>,
) -> (HttpMethod, String, Json) {
    let ctx = CompileContext::new(manifest).with_tenant(tenant);
    match compile_for_backend(&BackendKind::Elasticsearch, operation, &ctx)
        .expect("Elasticsearch compiler is registered")
        .expect("operation compiles")
    {
        CompiledRendering::Json {
            backend,
            method,
            path,
            body,
        } => {
            assert_eq!(backend, BackendKind::Elasticsearch);
            (method, path, body)
        }
        other => panic!("expected Elasticsearch JSON rendering, got {other:?}"),
    }
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

async fn execute_mutation(
    executor: &ElasticsearchExecutor,
    rendering: (HttpMethod, String, Json),
) -> Json {
    let (method, path, body) = rendering;
    let raw = executor
        .mutate(&dispatch_json(method, path, body))
        .await
        .expect("execute compiled Elasticsearch mutation/resource op");
    serde_json::from_str(&raw).expect("Elasticsearch mutation response must be JSON")
}

async fn search_customer_ids(
    executor: &ElasticsearchExecutor,
    manifest: &CatalogManifest,
    tenant: &'static str,
    text_query: &str,
) -> Vec<String> {
    let (method, path, body) = compile_search(manifest, tenant, text_query);
    let raw = executor
        .search(&dispatch_json(method, path, body))
        .await
        .expect("execute compiled Elasticsearch search");
    let response: Json = serde_json::from_str(&raw).expect("Elasticsearch search response JSON");
    let mut ids = response["hits"]["hits"]
        .as_array()
        .expect("ES search response must contain hits.hits")
        .iter()
        .filter_map(|hit| {
            hit.get("_source")
                .and_then(|source| source.get("id"))
                .and_then(Json::as_str)
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

async fn refresh_index(client: &ElasticsearchHttpClient, index: &str) {
    client
        .request_json(
            reqwest::Method::POST,
            &format!("/{index}/_refresh"),
            &Json::Null,
        )
        .await
        .expect("refresh Elasticsearch index");
}

async fn drop_index_if_exists(client: &ElasticsearchHttpClient, index: &str) {
    let _ = client
        .request_json(reqwest::Method::DELETE, &format!("/{index}"), &Json::Null)
        .await;
}

fn assert_bulk_ok(response: &Json) {
    assert_eq!(
        response.get("errors").and_then(Json::as_bool),
        Some(false),
        "Elasticsearch _bulk response must not contain item errors: {response}"
    );
}

fn record<const N: usize>(fields: [(&str, &str); N]) -> LogicalRecord {
    let mut record = BTreeMap::new();
    for (field, value) in fields {
        record.insert(field.to_string(), LogicalValue::String(value.to_string()));
    }
    record
}
