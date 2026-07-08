use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::{Value as Json, json};

use crate::backend::BackendKind;
use crate::generation::CatalogManifest;
use crate::ir::compile::{
    CompileContext, CompileOperation, CompiledRendering, compile_for_backend,
};
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    AggregateExpr, AggregateFunc, ConflictStrategy, LogicalAggregate, LogicalDelete, LogicalRecord,
    LogicalResourceOp, LogicalSearch, LogicalWrite, ResourceKind, ResourceOpKind,
};
use crate::ir::projection::{LogicalSort, NullOrder, SortDirection};
use crate::ir::value::LogicalValue;
use crate::runtime::executors::clickhouse::{ClickHouseConfig, ClickHouseExecutor};
use crate::runtime::executors::{MutationExecutor, QueryExecutor, ResourceAdminExecutor};

use super::support::{customer_manifest, live_ir_enabled};

const MESSAGE: &str = "acme.billing.v1.Customer";

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1, ClickHouse env, and feature=clickhouse"]
async fn clickhouse_compiled_search_resource_delete_match_live_rows() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Some(executor) = clickhouse_live_executor() else {
        eprintln!(
            "ClickHouse env unset - set UDB_COLUMN_DSN/UDB_COLUMN_HTTP_URL or UDB_CLICKHOUSE_DSN"
        );
        return;
    };

    let database = executor.database().to_string();
    let table = format!("udb_ir_live_{}", uuid::Uuid::new_v4().simple());
    let manifest = clickhouse_manifest(&database, &table);

    let _ = executor.drop_resource(&table).await;
    execute_mutation(
        &executor,
        compile_table_resource(&manifest, &database, &table),
    )
    .await;

    let listed = execute_query(
        &executor,
        compile_sql(
            &manifest,
            "tenant-a",
            CompileOperation::ResourceOp(&LogicalResourceOp {
                op: ResourceOpKind::List,
                resource_kind: ResourceKind::Table,
                resource_name: String::new(),
                spec: Some(json!({ "database": database })),
            }),
        ),
    )
    .await;
    assert!(
        listed
            .iter()
            .filter_map(Json::as_object)
            .flat_map(|row| row.values())
            .any(|value| value.as_str() == Some(table.as_str())),
        "compiled ClickHouse table list must observe {table}"
    );

    execute_mutation(
        &executor,
        compile_write(
            &manifest,
            "tenant-a",
            vec![
                record("cust-1", "Alice", "alice@example.com", "tenant-a"),
                record("cust-2", "Ana", "ana@example.com", "tenant-a"),
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
                "cust-3",
                "Alice",
                "alice@other.example.com",
                "tenant-b",
            )],
        ),
    )
    .await;

    assert_eq!(
        query_names(&executor, compile_search(&manifest, "tenant-a", "Alice")).await,
        vec!["Alice".to_string()],
        "compiled ClickHouse text search must respect tenant-a context"
    );
    assert_eq!(
        query_names(&executor, compile_search(&manifest, "tenant-b", "Alice")).await,
        vec!["Alice".to_string()],
        "tenant-b search sanity check"
    );

    assert_eq!(
        aggregate_count(&executor, &manifest, "tenant-a").await,
        Some(2),
        "compiled ClickHouse aggregate must include tenant context"
    );

    execute_mutation(&executor, compile_delete(&manifest, "tenant-a", "Alice")).await;
    wait_until_gone(&executor, &manifest, "tenant-a", "Alice").await;

    assert_eq!(
        query_names(&executor, compile_search(&manifest, "tenant-b", "Alice")).await,
        vec!["Alice".to_string()],
        "tenant-b row must remain after tenant-a compiled ClickHouse delete"
    );
    assert_eq!(
        aggregate_count(&executor, &manifest, "tenant-a").await,
        Some(1),
        "compiled ClickHouse delete must affect only tenant-a"
    );

    executor
        .drop_resource(&table)
        .await
        .expect("drop live ClickHouse table");
}

fn clickhouse_live_executor() -> Option<ClickHouseExecutor> {
    if let Some(executor) = ClickHouseExecutor::from_env() {
        return Some(executor);
    }
    let dsn = std::env::var("UDB_CLICKHOUSE_DSN").ok()?;
    let http_base = std::env::var("UDB_CLICKHOUSE_HTTP_URL")
        .ok()
        .unwrap_or_else(|| ClickHouseConfig::http_base_from_dsn(&dsn));
    let username = std::env::var("UDB_CLICKHOUSE_USER")
        .or_else(|_| std::env::var("UDB_COLUMN_USER"))
        .unwrap_or_else(|_| "default".to_string());
    let password = std::env::var("UDB_CLICKHOUSE_PASSWORD")
        .or_else(|_| std::env::var("UDB_COLUMN_PASSWORD"))
        .unwrap_or_default();
    let database = std::env::var("UDB_CLICKHOUSE_DATABASE")
        .or_else(|_| std::env::var("UDB_COLUMN_DATABASE"))
        .ok()
        .or_else(|| ClickHouseConfig::db_from_dsn(&dsn))
        .unwrap_or_else(|| "default".to_string());
    let is_cloud = crate::runtime::executors::http::is_cloud(
        "UDB_CH_DEPLOY_MODE",
        &http_base,
        ".clickhouse.cloud",
    );
    Some(ClickHouseExecutor::new(ClickHouseConfig {
        http_base,
        username: username.trim().to_string(),
        password: password.trim().to_string(),
        database,
        is_cloud,
        connect_timeout_secs: 10,
        query_timeout_secs: 30,
    }))
}

fn clickhouse_manifest(database: &str, table_name: &str) -> CatalogManifest {
    let mut manifest = customer_manifest(database);
    manifest.tables[0].table = table_name.to_string();
    manifest
}

fn compile_table_resource(manifest: &CatalogManifest, database: &str, table: &str) -> String {
    compile_sql(
        manifest,
        "tenant-a",
        CompileOperation::ResourceOp(&LogicalResourceOp {
            op: ResourceOpKind::Ensure,
            resource_kind: ResourceKind::Table,
            resource_name: table.to_string(),
            spec: Some(json!({
                "database": database,
                "columns": [
                    { "name": "id", "type": "String" },
                    { "name": "name", "type": "String" },
                    { "name": "email", "type": "String" },
                    { "name": "tenant_id", "type": "String" }
                ],
                "engine": "MergeTree() ORDER BY (`id`)"
            })),
        }),
    )
}

fn compile_write(
    manifest: &CatalogManifest,
    tenant: &'static str,
    records: Vec<LogicalRecord>,
) -> String {
    let write = LogicalWrite {
        message_type: MESSAGE.to_string(),
        records,
        conflict: ConflictStrategy::Error,
        return_fields: Vec::new(),
    };
    compile_sql(manifest, tenant, CompileOperation::Write(&write))
}

fn compile_search(manifest: &CatalogManifest, tenant: &'static str, text: &str) -> String {
    let search = LogicalSearch {
        message_type: MESSAGE.to_string(),
        vector: None,
        text_query: Some(text.to_string()),
        filter: None,
        top_k: 10,
        score_threshold: None,
        require_hybrid: false,
        with_vector: false,
        with_payload: true,
    };
    compile_sql(manifest, tenant, CompileOperation::Search(&search))
}

fn compile_delete(manifest: &CatalogManifest, tenant: &'static str, name: &str) -> String {
    let delete = LogicalDelete {
        message_type: MESSAGE.to_string(),
        filter: LogicalFilter::Comparison {
            field: "name".to_string(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String(name.to_string()),
        },
        return_fields: Vec::new(),
    };
    compile_sql(manifest, tenant, CompileOperation::Delete(&delete))
}

fn compile_aggregate(manifest: &CatalogManifest, tenant: &'static str) -> String {
    let aggregate = LogicalAggregate {
        message_type: MESSAGE.to_string(),
        filter: None,
        group_by: vec!["tenant_id".to_string()],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Count,
            field: "*".to_string(),
            alias: "row_count".to_string(),
        }],
        having: None,
        sort: vec![LogicalSort {
            field: "tenant_id".to_string(),
            direction: SortDirection::Asc,
            nulls: NullOrder::Default,
        }],
        pagination: None,
    };
    compile_sql(manifest, tenant, CompileOperation::Aggregate(&aggregate))
}

fn compile_sql(
    manifest: &CatalogManifest,
    tenant: &'static str,
    operation: CompileOperation<'_>,
) -> String {
    let ctx = CompileContext::new(manifest).with_tenant(tenant);
    match compile_for_backend(&BackendKind::Clickhouse, operation, &ctx)
        .expect("ClickHouse compiler is present")
        .expect("ClickHouse operation compiles")
    {
        CompiledRendering::Sql {
            backend,
            statement,
            params,
        } => {
            assert_eq!(backend, BackendKind::Clickhouse);
            inline_clickhouse_params(&statement, &params)
        }
        other => panic!("expected ClickHouse SQL rendering, got {other:?}"),
    }
}

async fn execute_query(executor: &ClickHouseExecutor, sql: String) -> Vec<Json> {
    let response = QueryExecutor::query(
        executor,
        &json!({
            "sql": sql,
            "compiler_mediated": true
        })
        .to_string(),
    )
    .await
    .expect("execute compiled ClickHouse query");
    serde_json::from_str(&response).expect("ClickHouse query response JSON")
}

async fn execute_mutation(executor: &ClickHouseExecutor, sql: String) -> Json {
    let response = MutationExecutor::mutate(
        executor,
        &json!({
            "sql": sql,
            "compiler_mediated": true
        })
        .to_string(),
    )
    .await
    .expect("execute compiled ClickHouse mutation");
    serde_json::from_str(&response).expect("ClickHouse mutation response JSON")
}

async fn query_names(executor: &ClickHouseExecutor, sql: String) -> Vec<String> {
    let mut names = execute_query(executor, sql)
        .await
        .into_iter()
        .filter_map(|row| {
            row.get("name")
                .and_then(Json::as_str)
                .map(ToString::to_string)
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}

async fn aggregate_count(
    executor: &ClickHouseExecutor,
    manifest: &CatalogManifest,
    tenant: &'static str,
) -> Option<i64> {
    execute_query(executor, compile_aggregate(manifest, tenant))
        .await
        .into_iter()
        .find_map(|row| row.get("row_count").and_then(Json::as_i64))
}

async fn wait_until_gone(
    executor: &ClickHouseExecutor,
    manifest: &CatalogManifest,
    tenant: &'static str,
    name: &str,
) {
    for _ in 0..20 {
        if query_names(executor, compile_search(manifest, tenant, name))
            .await
            .is_empty()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("compiled ClickHouse delete did not remove tenant-scoped row before timeout");
}

fn inline_clickhouse_params(statement: &str, params: &[LogicalValue]) -> String {
    let mut out = String::with_capacity(statement.len() + params.len() * 8);
    let mut params = params.iter();
    for ch in statement.chars() {
        if ch == '?' {
            let value = params.next().expect("compiled SQL placeholder has param");
            out.push_str(&clickhouse_literal(value));
        } else {
            out.push(ch);
        }
    }
    assert!(
        params.next().is_none(),
        "compiled SQL must not have unused params"
    );
    out
}

fn clickhouse_literal(value: &LogicalValue) -> String {
    match value {
        LogicalValue::Null => "NULL".to_string(),
        LogicalValue::Bool(value) => {
            if *value {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        LogicalValue::Int(value) => value.to_string(),
        LogicalValue::Float(value) => value.to_string(),
        LogicalValue::String(value) => format!("'{}'", value.replace('\'', "''")),
        LogicalValue::Bytes(value) => {
            use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
            format!("'{}'", B64.encode(value))
        }
        LogicalValue::Timestamp(value) => format!("'{}'", value.to_rfc3339()),
        LogicalValue::Json(value) => format!("'{}'", value.to_string().replace('\'', "''")),
        LogicalValue::Array(values) => {
            let values = values
                .iter()
                .map(clickhouse_literal)
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{values}]")
        }
    }
}

fn record(id: &str, name: &str, email: &str, tenant: &str) -> LogicalRecord {
    let mut record = BTreeMap::new();
    record.insert("id".to_string(), LogicalValue::String(id.to_string()));
    record.insert("name".to_string(), LogicalValue::String(name.to_string()));
    record.insert("email".to_string(), LogicalValue::String(email.to_string()));
    record.insert(
        "tenant_id".to_string(),
        LogicalValue::String(tenant.to_string()),
    );
    record
}
