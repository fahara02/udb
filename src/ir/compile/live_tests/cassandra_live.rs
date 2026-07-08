use std::collections::BTreeMap;

use serde_json::{Value as Json, json};

use crate::backend::BackendKind;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::ir::compile::{
    CompileContext, CompileOperation, CompiledRendering, compile_for_backend,
};
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    AggregateExpr, AggregateFunc, ConflictStrategy, LogicalAggregate, LogicalDelete, LogicalRead,
    LogicalRecord, LogicalResourceOp, LogicalWrite, ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;
use crate::runtime::executors::cassandra::{CassandraClient, CassandraExecutor};
use crate::runtime::executors::{MutationExecutor, QueryExecutor};

use super::support::live_ir_enabled;

const MESSAGE: &str = "acme.billing.v1.Customer";

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1, UDB_CASSANDRA_DSN, and feature=cassandra"]
async fn cassandra_compiled_resource_read_write_delete_match_live_rows() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Ok(dsn) = std::env::var("UDB_CASSANDRA_DSN") else {
        eprintln!("UDB_CASSANDRA_DSN unset - skipping live Cassandra IR golden");
        return;
    };

    let executor = CassandraExecutor::new(
        CassandraClient::connect(&dsn)
            .await
            .expect("connect to live Cassandra"),
    );
    let keyspace = format!("udb_ir_live_{}", uuid::Uuid::new_v4().simple());
    let table = "customers";
    let manifest = cassandra_manifest(&keyspace, table);
    let id = format!("cust_{}", uuid::Uuid::new_v4().simple());

    execute_setup(
        &executor,
        format!(
            "CREATE KEYSPACE IF NOT EXISTS \"{keyspace}\" WITH replication = \
             {{'class':'SimpleStrategy','replication_factor':1}}"
        ),
    )
    .await;
    execute_mutation(&executor, compile_ensure_table(&manifest, table)).await;
    let listed = execute_query(&executor, compile_list_tables(&manifest)).await;
    assert!(
        listed
            .as_array()
            .expect("Cassandra table list returns rows")
            .iter()
            .any(|row| row.get("table_name").and_then(Json::as_str) == Some(table)),
        "compiled Cassandra table list must include the ensured table"
    );

    execute_mutation(
        &executor,
        compile_write(&manifest, "tenant-a", &id, "Alice", "alice@example.com"),
    )
    .await;
    execute_mutation(
        &executor,
        compile_write(&manifest, "tenant-b", &id, "Bob", "bob@example.com"),
    )
    .await;

    let tenant_a = read_one(&executor, &manifest, "tenant-a", &id).await;
    assert_eq!(tenant_a["name"].as_str(), Some("Alice"));
    assert_eq!(tenant_a["tenant_id"].as_str(), Some("tenant-a"));

    let tenant_b = read_one(&executor, &manifest, "tenant-b", &id).await;
    assert_eq!(tenant_b["name"].as_str(), Some("Bob"));
    assert_eq!(tenant_b["tenant_id"].as_str(), Some("tenant-b"));

    let aggregate = execute_query(&executor, compile_count(&manifest, "tenant-a")).await;
    assert_eq!(
        first_count(&aggregate),
        Some(1),
        "compiled Cassandra aggregate must be tenant-scoped"
    );

    execute_mutation(&executor, compile_delete(&manifest, "tenant-a", &id)).await;
    assert!(
        execute_query(&executor, compile_read(&manifest, "tenant-a", &id))
            .await
            .as_array()
            .is_some_and(Vec::is_empty),
        "tenant-a row must be gone after compiled Cassandra delete"
    );
    assert_eq!(
        read_one(&executor, &manifest, "tenant-b", &id).await["name"].as_str(),
        Some("Bob"),
        "tenant-b row must remain after tenant-a compiled Cassandra delete"
    );

    let _ = execute_mutation(&executor, compile_delete(&manifest, "tenant-b", &id)).await;
    let _ = execute_mutation(&executor, compile_drop_table(&manifest, table)).await;
    let _ = execute_setup(&executor, format!("DROP KEYSPACE IF EXISTS \"{keyspace}\"")).await;
}

fn cassandra_manifest(keyspace: &str, table: &str) -> CatalogManifest {
    CatalogManifest {
        tables: vec![ManifestTable {
            message_name: MESSAGE.to_string(),
            schema: keyspace.to_string(),
            table: table.to_string(),
            primary_key: vec!["tenant_id".to_string(), "id".to_string()],
            columns: vec![
                column("tenant_id", true),
                column("id", true),
                column("name", false),
                column("email", false),
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn column(name: &str, primary: bool) -> ManifestColumn {
    ManifestColumn {
        field_name: name.to_string(),
        column_name: name.to_string(),
        proto_type: "string".to_string(),
        sql_type: "text".to_string(),
        is_primary: primary,
        is_tenant_column: name == "tenant_id",
        ..Default::default()
    }
}

fn compile_ensure_table(manifest: &CatalogManifest, table: &str) -> SqlDispatch {
    let keyspace = manifest.tables[0].schema.as_str();
    let op = LogicalResourceOp {
        op: ResourceOpKind::Ensure,
        resource_kind: ResourceKind::Table,
        resource_name: table.to_string(),
        spec: Some(json!({
            "keyspace": keyspace,
            "columns": [
                {"name": "tenant_id", "type": "text", "primary_key": true},
                {"name": "id", "type": "text", "primary_key": true},
                {"name": "name", "type": "text"},
                {"name": "email", "type": "text"}
            ]
        })),
    };
    compile_sql(manifest, "tenant-a", CompileOperation::ResourceOp(&op))
}

fn compile_list_tables(manifest: &CatalogManifest) -> SqlDispatch {
    let op = LogicalResourceOp {
        op: ResourceOpKind::List,
        resource_kind: ResourceKind::Table,
        resource_name: String::new(),
        spec: Some(json!({ "keyspace": manifest.tables[0].schema.as_str() })),
    };
    compile_sql(manifest, "tenant-a", CompileOperation::ResourceOp(&op))
}

fn compile_drop_table(manifest: &CatalogManifest, table: &str) -> SqlDispatch {
    let op = LogicalResourceOp {
        op: ResourceOpKind::Drop,
        resource_kind: ResourceKind::Table,
        resource_name: table.to_string(),
        spec: Some(json!({ "keyspace": manifest.tables[0].schema.as_str() })),
    };
    compile_sql(manifest, "tenant-a", CompileOperation::ResourceOp(&op))
}

fn compile_write(
    manifest: &CatalogManifest,
    tenant: &'static str,
    id: &str,
    name: &str,
    email: &str,
) -> SqlDispatch {
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
    compile_sql(manifest, tenant, CompileOperation::Write(&write))
}

fn compile_read(manifest: &CatalogManifest, tenant: &'static str, id: &str) -> SqlDispatch {
    let read = LogicalRead::message(MESSAGE).with_filter(LogicalFilter::Comparison {
        field: "id".to_string(),
        op: ComparisonOp::Eq,
        value: LogicalValue::String(id.to_string()),
    });
    compile_sql(manifest, tenant, CompileOperation::Read(&read))
}

fn compile_count(manifest: &CatalogManifest, tenant: &'static str) -> SqlDispatch {
    let aggregate = LogicalAggregate {
        message_type: MESSAGE.to_string(),
        filter: None,
        group_by: Vec::new(),
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Count,
            field: "*".to_string(),
            alias: "row_count".to_string(),
        }],
        having: None,
        sort: Vec::new(),
        pagination: None,
    };
    compile_sql(manifest, tenant, CompileOperation::Aggregate(&aggregate))
}

fn compile_delete(manifest: &CatalogManifest, tenant: &'static str, id: &str) -> SqlDispatch {
    let delete = LogicalDelete {
        message_type: MESSAGE.to_string(),
        filter: LogicalFilter::Comparison {
            field: "id".to_string(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String(id.to_string()),
        },
        return_fields: Vec::new(),
    };
    compile_sql(manifest, tenant, CompileOperation::Delete(&delete))
}

#[derive(Debug)]
struct SqlDispatch {
    statement: String,
    params: Vec<Json>,
    query: bool,
}

fn compile_sql(
    manifest: &CatalogManifest,
    tenant: &'static str,
    operation: CompileOperation<'_>,
) -> SqlDispatch {
    let ctx = CompileContext::new(manifest).with_tenant(tenant);
    match compile_for_backend(&BackendKind::Cassandra, operation, &ctx)
        .expect("Cassandra compiler is present")
        .expect("Cassandra operation compiles")
    {
        CompiledRendering::Sql {
            backend,
            statement,
            params,
        } => {
            assert_eq!(backend, BackendKind::Cassandra);
            let query = statement
                .trim_start()
                .to_ascii_uppercase()
                .starts_with("SELECT");
            SqlDispatch {
                statement,
                params: params.iter().map(logical_value_to_json).collect(),
                query,
            }
        }
        other => panic!("expected Cassandra SQL rendering, got {other:?}"),
    }
}

async fn execute_setup(executor: &CassandraExecutor, statement: String) -> Json {
    execute_mutation(
        executor,
        SqlDispatch {
            statement,
            params: Vec::new(),
            query: false,
        },
    )
    .await
}

async fn read_one(
    executor: &CassandraExecutor,
    manifest: &CatalogManifest,
    tenant: &'static str,
    id: &str,
) -> Json {
    let rows = execute_query(executor, compile_read(manifest, tenant, id)).await;
    let rows = rows.as_array().expect("Cassandra read returns rows");
    assert_eq!(rows.len(), 1);
    rows[0].clone()
}

async fn execute_query(executor: &CassandraExecutor, dispatch: SqlDispatch) -> Json {
    assert!(dispatch.query);
    let response = QueryExecutor::query(executor, &dispatch.spec_json().to_string())
        .await
        .expect("execute compiled Cassandra query");
    serde_json::from_str(&response).expect("Cassandra query response JSON")
}

async fn execute_mutation(executor: &CassandraExecutor, dispatch: SqlDispatch) -> Json {
    assert!(!dispatch.query);
    let response = MutationExecutor::mutate(executor, &dispatch.spec_json().to_string())
        .await
        .expect("execute compiled Cassandra mutation");
    serde_json::from_str(&response).expect("Cassandra mutation response JSON")
}

impl SqlDispatch {
    fn spec_json(&self) -> Json {
        json!({
            "sql": self.statement,
            "params": self.params,
            "compiler_mediated": true
        })
    }
}

fn first_count(rows: &Json) -> Option<i64> {
    let row = rows.as_array()?.first()?;
    for key in ["row_count", "count", "system.count(*)", "col_0"] {
        if let Some(value) = row.get(key).and_then(Json::as_i64) {
            return Some(value);
        }
    }
    row.as_object()?.values().find_map(Json::as_i64)
}

fn logical_value_to_json(value: &LogicalValue) -> Json {
    match value {
        LogicalValue::Null => Json::Null,
        LogicalValue::Bool(value) => Json::Bool(*value),
        LogicalValue::Int(value) => json!(value),
        LogicalValue::Float(value) => serde_json::Number::from_f64(*value)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        LogicalValue::String(value) => Json::String(value.clone()),
        LogicalValue::Bytes(value) => {
            use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
            Json::String(format!("base64:{}", B64.encode(value)))
        }
        LogicalValue::Timestamp(value) => Json::String(value.to_rfc3339()),
        LogicalValue::Json(value) => value.clone(),
        LogicalValue::Array(values) => {
            Json::Array(values.iter().map(logical_value_to_json).collect())
        }
    }
}

fn record<const N: usize>(fields: [(&str, &str); N]) -> LogicalRecord {
    let mut record = BTreeMap::new();
    for (field, value) in fields {
        record.insert(field.to_string(), LogicalValue::String(value.to_string()));
    }
    record
}
