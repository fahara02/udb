use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::{Value as Json, json};

use crate::backend::BackendKind;
use crate::generation::{CatalogManifest, ManifestColumn};
use crate::ir::compile::{
    CompileContext, CompileOperation, CompiledRendering, HttpMethod, compile_for_backend,
};
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    ConflictStrategy, LogicalDelete, LogicalRecord, LogicalSearch, LogicalWrite,
};
use crate::ir::value::LogicalValue;
use crate::runtime::executors::pinecone::{PineconeExecutor, PineconeHttpClient};
use crate::runtime::executors::{MutationExecutor, SearchExecutor};

use super::support::{customer_manifest, live_ir_enabled};

const MESSAGE: &str = "acme.billing.v1.Customer";

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1, Pinecone env, and feature=pinecone"]
async fn pinecone_compiled_vector_search_delete_match_live_vectors() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Some(executor) = pinecone_live_executor() else {
        eprintln!(
            "Pinecone env unset - set UDB_PINECONE_DSN or UDB_PINECONE_URL/UDB_PINECONE_API_KEY"
        );
        return;
    };

    let dimension = std::env::var("UDB_PINECONE_TEST_DIMENSION")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(3);
    let namespace = format!("udb-ir-live-{}", uuid::Uuid::new_v4().simple());
    let manifest = pinecone_manifest();

    let alice_a = format!("cust-a-{}", uuid::Uuid::new_v4().simple());
    let ana_a = format!("cust-a-{}", uuid::Uuid::new_v4().simple());
    let alice_b = format!("cust-b-{}", uuid::Uuid::new_v4().simple());
    let dense_a = unit_vector(dimension, 0);
    let dense_b = unit_vector(dimension, dimension.saturating_sub(1));

    execute_mutation(
        &executor,
        compile_write(
            &manifest,
            "tenant-a",
            &namespace,
            vec![
                record(&alice_a, "Alice", "alice@example.com", "tenant-a", &dense_a),
                record(&ana_a, "Ana", "ana@example.com", "tenant-a", &dense_b),
            ],
        ),
    )
    .await;
    execute_mutation(
        &executor,
        compile_write(
            &manifest,
            "tenant-b",
            &namespace,
            vec![record(
                &alice_b,
                "Alice",
                "alice@other.example.com",
                "tenant-b",
                &dense_a,
            )],
        ),
    )
    .await;

    wait_for_names(
        &executor,
        &manifest,
        "tenant-a",
        &namespace,
        &dense_a,
        "Alice",
        vec!["Alice".to_string()],
    )
    .await;
    assert_eq!(
        search_names(
            &executor, &manifest, "tenant-b", &namespace, &dense_a, "Alice"
        )
        .await,
        vec!["Alice".to_string()],
        "tenant-b Pinecone search sanity check"
    );

    execute_mutation(
        &executor,
        compile_delete(&manifest, "tenant-a", &namespace, "Alice"),
    )
    .await;
    wait_for_names(
        &executor,
        &manifest,
        "tenant-a",
        &namespace,
        &dense_a,
        "Alice",
        Vec::new(),
    )
    .await;
    assert_eq!(
        search_names(
            &executor, &manifest, "tenant-b", &namespace, &dense_a, "Alice"
        )
        .await,
        vec!["Alice".to_string()],
        "tenant-b Pinecone vector must survive tenant-a compiled delete"
    );

    let _ = execute_mutation(
        &executor,
        compile_delete(&manifest, "tenant-a", &namespace, "Ana"),
    )
    .await;
    let _ = execute_mutation(
        &executor,
        compile_delete(&manifest, "tenant-b", &namespace, "Alice"),
    )
    .await;
}

fn pinecone_live_executor() -> Option<PineconeExecutor> {
    if let Ok(raw) = std::env::var("UDB_PINECONE_DSN")
        && !raw.trim().is_empty()
    {
        if let Some(rest) = raw.strip_prefix("apikey://")
            && let Some((key, host)) = rest.split_once('@')
        {
            return Some(PineconeExecutor::new(PineconeHttpClient::new(
                format!("https://{host}"),
                key,
            )));
        }
    }
    let base_url = std::env::var("UDB_PINECONE_URL")
        .or_else(|_| std::env::var("PINECONE_URL"))
        .ok()?;
    let api_key = std::env::var("UDB_PINECONE_API_KEY")
        .or_else(|_| std::env::var("PINECONE_API_KEY"))
        .ok()?;
    Some(PineconeExecutor::new(PineconeHttpClient::new(
        base_url, api_key,
    )))
}

fn pinecone_manifest() -> CatalogManifest {
    let mut manifest = customer_manifest("pinecone_live");
    manifest.tables[0].columns.push(ManifestColumn {
        field_name: "_vector".to_string(),
        column_name: "_vector".to_string(),
        proto_type: "array<float>".to_string(),
        sql_type: "vector".to_string(),
        ..Default::default()
    });
    manifest
}

fn compile_write(
    manifest: &CatalogManifest,
    tenant: &'static str,
    namespace: &str,
    records: Vec<LogicalRecord>,
) -> (HttpMethod, String, Json) {
    let write = LogicalWrite {
        message_type: MESSAGE.to_string(),
        records,
        conflict: ConflictStrategy::Replace,
        return_fields: Vec::new(),
    };
    compile_json(manifest, tenant, namespace, CompileOperation::Write(&write))
}

fn compile_search(
    manifest: &CatalogManifest,
    tenant: &'static str,
    namespace: &str,
    vector: &[f64],
    name: &str,
) -> (HttpMethod, String, Json) {
    let search = LogicalSearch {
        message_type: MESSAGE.to_string(),
        vector: Some(vector.iter().map(|value| *value as f32).collect()),
        text_query: None,
        filter: Some(LogicalFilter::Comparison {
            field: "name".to_string(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String(name.to_string()),
        }),
        top_k: 10,
        score_threshold: None,
        require_hybrid: false,
        with_vector: false,
        with_payload: true,
    };
    compile_json(
        manifest,
        tenant,
        namespace,
        CompileOperation::Search(&search),
    )
}

fn compile_delete(
    manifest: &CatalogManifest,
    tenant: &'static str,
    namespace: &str,
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
    compile_json(
        manifest,
        tenant,
        namespace,
        CompileOperation::Delete(&delete),
    )
}

fn compile_json(
    manifest: &CatalogManifest,
    tenant: &'static str,
    namespace: &str,
    operation: CompileOperation<'_>,
) -> (HttpMethod, String, Json) {
    let ctx = CompileContext::new(manifest)
        .with_tenant(tenant)
        .with_project(namespace);
    match compile_for_backend(&BackendKind::Pinecone, operation, &ctx)
        .expect("Pinecone compiler is present")
        .expect("Pinecone operation compiles")
    {
        CompiledRendering::Json {
            backend,
            method,
            path,
            body,
        } => {
            assert_eq!(backend, BackendKind::Pinecone);
            (method, path, body)
        }
        other => panic!("expected Pinecone JSON rendering, got {other:?}"),
    }
}

async fn execute_mutation(executor: &PineconeExecutor, req: (HttpMethod, String, Json)) -> Json {
    let (method, path, body) = req;
    let response = MutationExecutor::mutate(
        executor,
        &json!({
            "method": http_method_token(method),
            "path": path,
            "body": body,
            "compiler_mediated": true
        })
        .to_string(),
    )
    .await
    .expect("execute compiled Pinecone mutation");
    serde_json::from_str(&response).expect("Pinecone mutation response JSON")
}

async fn search_names(
    executor: &PineconeExecutor,
    manifest: &CatalogManifest,
    tenant: &'static str,
    namespace: &str,
    vector: &[f64],
    name: &str,
) -> Vec<String> {
    let (method, path, body) = compile_search(manifest, tenant, namespace, vector, name);
    let response = SearchExecutor::search(
        executor,
        &json!({
            "method": http_method_token(method),
            "path": path,
            "body": body,
            "compiler_mediated": true
        })
        .to_string(),
    )
    .await
    .expect("execute compiled Pinecone search");
    let parsed: Json = serde_json::from_str(&response).expect("Pinecone search response JSON");
    let mut names = parsed
        .get("matches")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|matched| {
            matched
                .get("metadata")
                .and_then(|metadata| metadata.get("name"))
                .and_then(Json::as_str)
                .map(ToString::to_string)
        })
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

async fn wait_for_names(
    executor: &PineconeExecutor,
    manifest: &CatalogManifest,
    tenant: &'static str,
    namespace: &str,
    vector: &[f64],
    name: &str,
    expected: Vec<String>,
) {
    for _ in 0..24 {
        let observed = search_names(executor, manifest, tenant, namespace, vector, name).await;
        if observed == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("compiled Pinecone search did not observe expected names {expected:?}");
}

fn http_method_token(method: HttpMethod) -> &'static str {
    match method {
        HttpMethod::Get => "GET",
        HttpMethod::Post => "POST",
        HttpMethod::Put => "PUT",
        HttpMethod::Patch => "PATCH",
        HttpMethod::Delete => "DELETE",
    }
}

fn unit_vector(dimension: usize, hot_index: usize) -> Vec<f64> {
    let mut values = vec![0.0; dimension];
    let idx = hot_index.min(dimension.saturating_sub(1));
    values[idx] = 1.0;
    values
}

fn record(id: &str, name: &str, email: &str, tenant: &str, vector: &[f64]) -> LogicalRecord {
    let mut record = BTreeMap::new();
    record.insert("id".to_string(), LogicalValue::String(id.to_string()));
    record.insert("name".to_string(), LogicalValue::String(name.to_string()));
    record.insert("email".to_string(), LogicalValue::String(email.to_string()));
    record.insert(
        "tenant_id".to_string(),
        LogicalValue::String(tenant.to_string()),
    );
    record.insert(
        "_vector".to_string(),
        LogicalValue::Array(
            vector
                .iter()
                .map(|value| LogicalValue::Float(*value))
                .collect(),
        ),
    );
    record
}
