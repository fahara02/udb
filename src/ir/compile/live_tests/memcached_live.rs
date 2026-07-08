use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use serde_json::{Value as Json, json};

use crate::backend::BackendKind;
use crate::generation::CatalogManifest;
use crate::ir::compile::{
    CompileContext, CompileOperation, CompiledRendering, KeyValueOp, compile_for_backend,
};
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    ConflictStrategy, LogicalDelete, LogicalRead, LogicalRecord, LogicalWrite,
};
use crate::ir::value::LogicalValue;
use crate::runtime::executors::memcached::{MemcachedClient, MemcachedExecutor};
use crate::runtime::executors::{MutationExecutor, QueryExecutor};

use super::support::{customer_manifest, live_ir_enabled};

const MESSAGE: &str = "acme.billing.v1.Customer";
const PROJECT: &str = "memcached-live";

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1, UDB_MEMCACHED_DSN/MEMCACHED_URL, and feature=memcached"]
async fn memcached_compiled_kv_read_write_delete_match_live_keys() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Ok(dsn) = std::env::var("UDB_MEMCACHED_DSN").or_else(|_| std::env::var("MEMCACHED_URL"))
    else {
        eprintln!("UDB_MEMCACHED_DSN / MEMCACHED_URL unset - skipping live Memcached IR golden");
        return;
    };

    let client = MemcachedClient::connect(&dsn).expect("create Memcached client");
    let executor = MemcachedExecutor::new(client);
    let manifest = customer_manifest("memcached_live");
    let pk = format!("cust-{}", uuid::Uuid::new_v4().simple());

    let write_a = compile_write(&manifest, "tenant-a", &pk, "Alice", "alice@example.com");
    let write_b = compile_write(&manifest, "tenant-b", &pk, "Bob", "bob@example.com");
    assert_eq!(write_a.op, KeyValueOp::Set);
    assert_eq!(write_b.op, KeyValueOp::Set);
    assert_ne!(
        write_a.key.as_str(),
        write_b.key.as_str(),
        "compiled Memcached keys must include tenant context"
    );
    assert!(
        !write_a.key.contains('{') && !write_a.key.contains('}'),
        "compiled Memcached dispatch must carry a concrete key, not a placeholder"
    );
    assert_eq!(
        write_a.key.as_str(),
        format!("udb:{PROJECT}:tenant-a:{MESSAGE}:{pk}"),
        "compiled Memcached key must include project, tenant, message type, and pk"
    );

    execute_mutation(&executor, write_a).await;
    execute_mutation(&executor, write_b).await;

    let tenant_a = read_record(&executor, &manifest, "tenant-a", &pk).await;
    assert_eq!(tenant_a["name"].as_str(), Some("Alice"));
    assert_eq!(tenant_a["tenant_id"].as_str(), Some("tenant-a"));

    let tenant_b = read_record(&executor, &manifest, "tenant-b", &pk).await;
    assert_eq!(tenant_b["name"].as_str(), Some("Bob"));
    assert_eq!(tenant_b["tenant_id"].as_str(), Some("tenant-b"));

    assert_eq!(
        execute_mutation(&executor, compile_delete(&manifest, "tenant-a", &pk)).await["deleted"]
            .as_bool(),
        Some(true)
    );
    assert!(
        !execute_query(&executor, compile_read(&manifest, "tenant-a", &pk)).await["hit"]
            .as_bool()
            .unwrap_or(true),
        "tenant-a key must be gone after compiled Memcached delete"
    );
    assert_eq!(
        read_record(&executor, &manifest, "tenant-b", &pk).await["name"].as_str(),
        Some("Bob"),
        "tenant-b key must remain after tenant-a compiled Memcached delete"
    );

    let _ = execute_mutation(&executor, compile_delete(&manifest, "tenant-b", &pk)).await;
}

#[derive(Debug)]
struct KvDispatch {
    op: KeyValueOp,
    key: String,
    value: Option<String>,
    ttl_seconds: Option<u64>,
}

fn compile_write(
    manifest: &CatalogManifest,
    tenant: &'static str,
    id: &str,
    name: &str,
    email: &str,
) -> KvDispatch {
    let write = LogicalWrite {
        message_type: MESSAGE.to_string(),
        records: vec![record([
            ("id", id),
            ("name", name),
            ("email", email),
            ("tenant_id", tenant),
        ])],
        conflict: ConflictStrategy::Replace,
        return_fields: Vec::new(),
    };
    compile_kv(manifest, tenant, CompileOperation::Write(&write))
}

fn compile_read(manifest: &CatalogManifest, tenant: &'static str, id: &str) -> KvDispatch {
    let read = LogicalRead::message(MESSAGE).with_filter(LogicalFilter::Comparison {
        field: "id".to_string(),
        op: ComparisonOp::Eq,
        value: LogicalValue::String(id.to_string()),
    });
    compile_kv(manifest, tenant, CompileOperation::Read(&read))
}

fn compile_delete(manifest: &CatalogManifest, tenant: &'static str, id: &str) -> KvDispatch {
    let delete = LogicalDelete {
        message_type: MESSAGE.to_string(),
        filter: LogicalFilter::Comparison {
            field: "id".to_string(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String(id.to_string()),
        },
        return_fields: Vec::new(),
    };
    compile_kv(manifest, tenant, CompileOperation::Delete(&delete))
}

fn compile_kv(
    manifest: &CatalogManifest,
    tenant: &'static str,
    operation: CompileOperation<'_>,
) -> KvDispatch {
    let ctx = CompileContext::new(manifest)
        .with_project(PROJECT)
        .with_tenant(tenant);
    match compile_for_backend(&BackendKind::Memcached, operation, &ctx)
        .expect("Memcached compiler is present")
        .expect("Memcached operation compiles")
    {
        CompiledRendering::KeyValue {
            backend,
            op,
            key_template,
            value,
            ttl_seconds,
        } => {
            assert_eq!(backend, BackendKind::Memcached);
            KvDispatch {
                op,
                key: key_template,
                value: value.map(|bytes| {
                    String::from_utf8(bytes).expect("Memcached live golden writes JSON values")
                }),
                ttl_seconds,
            }
        }
        other => panic!("expected Memcached KeyValue rendering, got {other:?}"),
    }
}

async fn read_record(
    executor: &MemcachedExecutor,
    manifest: &CatalogManifest,
    tenant: &'static str,
    id: &str,
) -> Json {
    let response = execute_query(executor, compile_read(manifest, tenant, id)).await;
    assert_eq!(response["hit"].as_bool(), Some(true));
    let encoded = response["value"]
        .as_str()
        .expect("Memcached value string")
        .strip_prefix("base64:")
        .expect("Memcached response uses base64 value");
    serde_json::from_slice(&B64.decode(encoded).expect("Memcached value base64"))
        .expect("Memcached value is JSON")
}

async fn execute_query(executor: &MemcachedExecutor, dispatch: KvDispatch) -> Json {
    let op = match dispatch.op {
        KeyValueOp::Get => "get",
        KeyValueOp::Exists => "exists",
        KeyValueOp::Scan => "scan",
        other => panic!("expected Memcached query op, got {other:?}"),
    };
    let response = QueryExecutor::query(
        executor,
        &json!({
            "operation": op,
            "key": dispatch.key,
            "compiler_mediated": true
        })
        .to_string(),
    )
    .await
    .expect("execute compiled Memcached query");
    serde_json::from_str(&response).expect("Memcached query response JSON")
}

async fn execute_mutation(executor: &MemcachedExecutor, dispatch: KvDispatch) -> Json {
    let op = match dispatch.op {
        KeyValueOp::Set => "set",
        KeyValueOp::Delete => "delete",
        other => panic!("expected Memcached mutation op, got {other:?}"),
    };
    let mut request = json!({
        "operation": op,
        "key": dispatch.key,
        "compiler_mediated": true
    });
    if let Some(value) = dispatch.value {
        request["value"] = Json::String(value);
    }
    if let Some(ttl) = dispatch.ttl_seconds {
        request["ttl"] = Json::from(ttl);
    }
    let response = MutationExecutor::mutate(executor, &request.to_string())
        .await
        .expect("execute compiled Memcached mutation");
    serde_json::from_str(&response).expect("Memcached mutation response JSON")
}

fn record<const N: usize>(fields: [(&str, &str); N]) -> LogicalRecord {
    let mut record = BTreeMap::new();
    for (field, value) in fields {
        record.insert(field.to_string(), LogicalValue::String(value.to_string()));
    }
    record
}
