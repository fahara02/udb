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
use crate::runtime::executors::neo4j::Neo4jExecutor;
use crate::runtime::executors::{MutationExecutor, QueryExecutor};

use super::support::{customer_manifest, live_ir_enabled};

const MESSAGE: &str = "acme.billing.v1.Customer";

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1, UDB_GRAPH_DSN/UDB_GRAPH_HTTP_URL, and feature=neo4j"]
async fn neo4j_compiled_text_search_resource_delete_match_live_nodes() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Some(executor) = Neo4jExecutor::from_env() else {
        eprintln!("Neo4j env unset - skipping live Neo4j IR golden");
        return;
    };

    let label = format!("UdbIrLive{}", uuid::Uuid::new_v4().simple());
    let index_name = format!("{label}_fulltext");
    let manifest = neo4j_manifest(&label);

    execute_mutation(
        &executor,
        raw_cypher_mutation(format!("MATCH (n:`{label}`) DETACH DELETE n")),
    )
    .await;
    let _ = execute_mutation(
        &executor,
        raw_cypher_mutation(format!("DROP INDEX `{index_name}` IF EXISTS")),
    )
    .await;

    execute_mutation(
        &executor,
        neo4j_mutation_dispatch(compile_resource(
            &manifest,
            LogicalResourceOp {
                op: ResourceOpKind::Ensure,
                resource_kind: ResourceKind::Index,
                resource_name: index_name.clone(),
                spec: Some(json!({
                    "label": label.clone(),
                    "properties": ["name", "email"],
                    "type": "fulltext"
                })),
            },
        )),
    )
    .await;
    execute_query(&executor, raw_cypher_query("CALL db.awaitIndexes(10)")).await;
    assert!(
        execute_query(
            &executor,
            neo4j_query_dispatch(compile_resource(
                &manifest,
                LogicalResourceOp {
                    op: ResourceOpKind::List,
                    resource_kind: ResourceKind::Index,
                    resource_name: String::new(),
                    spec: None,
                },
            )),
        )
        .await
        .iter()
        .any(|row| row.get("name").and_then(Json::as_str) == Some(index_name.as_str())),
        "compiled Neo4j index list must observe {index_name}"
    );

    for record in [
        record("cust-1", "Alice", "alice@example.com", "tenant-a"),
        record("cust-2", "Ana", "ana@example.com", "tenant-a"),
        record("cust-3", "Alice", "alice@other.example.com", "tenant-b"),
    ] {
        let tenant = record_tenant(&record);
        execute_mutation(
            &executor,
            neo4j_mutation_dispatch(compile_write(&manifest, tenant, record)),
        )
        .await;
    }

    assert_eq!(
        eventually_search_names(&executor, &manifest, "tenant-a", "Alice", has_only_alice).await,
        vec!["Alice".to_string()],
        "compiled Neo4j fulltext search must respect tenant-a context"
    );

    execute_mutation(
        &executor,
        neo4j_mutation_dispatch(compile_delete(&manifest, "tenant-a", "Alice")),
    )
    .await;

    assert!(
        eventually_search_names(&executor, &manifest, "tenant-a", "Alice", |names| {
            names.is_empty()
        })
        .await
        .is_empty(),
        "compiled Neo4j delete must remove only tenant-a Alice"
    );
    assert_eq!(
        eventually_search_names(&executor, &manifest, "tenant-b", "Alice", has_only_alice).await,
        vec!["Alice".to_string()],
        "tenant-b Alice must remain after tenant-a compiled Neo4j delete"
    );

    execute_mutation(
        &executor,
        raw_cypher_mutation(format!("MATCH (n:`{label}`) DETACH DELETE n")),
    )
    .await;
    let _ = execute_mutation(
        &executor,
        raw_cypher_mutation(format!("DROP INDEX `{index_name}` IF EXISTS")),
    )
    .await;
}

fn neo4j_manifest(label: &str) -> CatalogManifest {
    let mut manifest = customer_manifest("neo4j_live");
    manifest.tables[0].table = label.to_string();
    manifest
}

fn compile_write(
    manifest: &CatalogManifest,
    tenant: &'static str,
    record: LogicalRecord,
) -> (HttpMethod, String, Json) {
    let write = LogicalWrite {
        message_type: MESSAGE.to_string(),
        records: vec![record],
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
    match compile_for_backend(&BackendKind::Neo4j, operation, &ctx)
        .expect("Neo4j compiler is registered")
        .expect("operation compiles")
    {
        CompiledRendering::Json {
            backend,
            method,
            path,
            body,
        } => {
            assert_eq!(backend, BackendKind::Neo4j);
            (method, path, body)
        }
        other => panic!("expected Neo4j JSON rendering, got {other:?}"),
    }
}

fn neo4j_query_dispatch(rendering: (HttpMethod, String, Json)) -> String {
    let (method, path, body) = rendering;
    assert_eq!(method, HttpMethod::Post);
    assert_eq!(path, "/tx/commit");
    let (cypher, parameters) = extract_statement(body);
    json!({
        "cypher": cypher,
        "parameters": parameters,
        "compiler_mediated": true,
    })
    .to_string()
}

fn neo4j_mutation_dispatch(rendering: (HttpMethod, String, Json)) -> String {
    let (method, path, body) = rendering;
    assert_eq!(method, HttpMethod::Post);
    assert_eq!(path, "/tx/commit");
    let (cypher, parameters) = extract_statement(body);
    json!({
        "operation": "cypher",
        "cypher": cypher,
        "parameters": parameters,
        "compiler_mediated": true,
    })
    .to_string()
}

fn raw_cypher_query(statement: &str) -> String {
    json!({
        "cypher": statement,
        "parameters": {},
        "compiler_mediated": true,
    })
    .to_string()
}

fn raw_cypher_mutation(statement: String) -> String {
    json!({
        "operation": "cypher",
        "cypher": statement,
        "parameters": {},
        "compiler_mediated": true,
    })
    .to_string()
}

fn extract_statement(body: Json) -> (String, Json) {
    let statement = &body["statements"][0];
    (
        statement["statement"]
            .as_str()
            .expect("compiled Neo4j statement text")
            .to_string(),
        statement["parameters"].clone(),
    )
}

async fn execute_mutation(executor: &Neo4jExecutor, spec_json: String) -> Json {
    let raw = MutationExecutor::mutate(executor, &spec_json)
        .await
        .expect("execute compiled Neo4j mutation/resource op");
    serde_json::from_str(&raw).expect("Neo4j mutation response JSON")
}

async fn execute_query(executor: &Neo4jExecutor, spec_json: String) -> Vec<Json> {
    let raw = QueryExecutor::query(executor, &spec_json)
        .await
        .expect("execute compiled Neo4j query");
    serde_json::from_str(&raw).expect("Neo4j query response JSON")
}

async fn search_names(
    executor: &Neo4jExecutor,
    manifest: &CatalogManifest,
    tenant: &'static str,
    text_query: &str,
) -> Vec<String> {
    let mut names = execute_query(
        executor,
        neo4j_query_dispatch(compile_search(manifest, tenant, text_query)),
    )
    .await
    .iter()
    .filter_map(|row| {
        row.get("n")
            .and_then(|node| node.get("name"))
            .and_then(Json::as_str)
            .map(str::to_string)
    })
    .collect::<Vec<_>>();
    names.sort();
    names
}

async fn eventually_search_names(
    executor: &Neo4jExecutor,
    manifest: &CatalogManifest,
    tenant: &'static str,
    text_query: &str,
    predicate: impl Fn(&[String]) -> bool,
) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let names = search_names(executor, manifest, tenant, text_query).await;
        if predicate(&names) || Instant::now() >= deadline {
            return names;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn has_only_alice(names: &[String]) -> bool {
    names.len() == 1 && names[0] == "Alice"
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

fn record_tenant(record: &LogicalRecord) -> &'static str {
    match record.get("tenant_id") {
        Some(LogicalValue::String(value)) if value == "tenant-a" => "tenant-a",
        Some(LogicalValue::String(value)) if value == "tenant-b" => "tenant-b",
        other => panic!("unexpected fixture tenant {other:?}"),
    }
}
