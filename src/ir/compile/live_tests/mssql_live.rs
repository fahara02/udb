use crate::backend::BackendKind;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestForeignKey, ManifestTable};
use crate::ir::compile::{
    CompileContext, CompileOperation, CompiledRendering, compile_for_backend,
};
use crate::ir::operations::LogicalRead;
use crate::ir::projection::{LogicalProjection, LogicalSort, SortDirection};
use crate::ir::value::LogicalValue;
use crate::runtime::executors::{
    SearchExecutor,
    mssql::{MssqlClient, SqlParam},
};

use super::support::{
    GoldenAggregateRow, GoldenRow, compile_aggregate_sql, compile_delete_sql,
    compile_ensure_name_index_sql, compile_insert_sql, compile_list_customer_indexes_sql,
    compile_read_sql, compile_search_sql, compile_update_sql, customer_manifest,
    expected_after_delete_aggregate_rows, expected_after_delete_rows, expected_after_insert_rows,
    expected_after_update_rows, expected_seed_rows, live_ir_enabled, unsupported_bind,
};

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1, UDB_MSSQL_DSN, and feature=mssql"]
async fn mssql_compiled_read_write_delete_match_live_golden_rows() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Ok(dsn) = std::env::var("UDB_MSSQL_DSN") else {
        eprintln!("UDB_MSSQL_DSN unset - skipping live SQL Server IR golden");
        return;
    };

    let client = MssqlClient::new(dsn);
    let schema = format!("udb_ir_live_{}", uuid::Uuid::new_v4().simple());
    create_customer_table(&client, &schema).await;
    seed(&client, &schema).await;

    let manifest = customer_manifest(&schema);
    assert_eq!(fetch_rows(&client, &manifest).await, expected_seed_rows());

    execute(
        &client,
        compile_ensure_name_index_sql(BackendKind::Mssql, &manifest),
    )
    .await;
    assert!(
        fetch_index_names(&client, &manifest)
            .await
            .iter()
            .any(|name| name == "idx_customers_name"),
        "compiled SQL Server resource-op index must be visible in sys.indexes"
    );

    if create_full_text_index(&client, &schema).await {
        let executor = crate::runtime::executors::mssql::MssqlExecutor::new(client.clone());
        assert_eq!(
            eventually_fetch_search_names(&executor, &manifest).await,
            vec!["Alice"],
            "compiled SQL Server full-text search must run through SearchExecutor and respect tenant context"
        );
    }

    execute(&client, compile_insert_sql(BackendKind::Mssql, &manifest)).await;
    assert_eq!(
        fetch_rows(&client, &manifest).await,
        expected_after_insert_rows()
    );

    execute(&client, compile_update_sql(BackendKind::Mssql, &manifest)).await;
    assert_eq!(
        fetch_rows(&client, &manifest).await,
        expected_after_update_rows()
    );

    execute(&client, compile_delete_sql(BackendKind::Mssql, &manifest)).await;
    assert_eq!(
        fetch_rows(&client, &manifest).await,
        expected_after_delete_rows()
    );
    assert_eq!(
        fetch_aggregate_rows(&client, &manifest).await,
        expected_after_delete_aggregate_rows()
    );

    drop_customer_table(&client, &schema).await;
}

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1, UDB_MSSQL_DSN, and feature=mssql"]
async fn mssql_eager_include_loads_belongs_to_in_one_compiled_query_live() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Ok(dsn) = std::env::var("UDB_MSSQL_DSN") else {
        eprintln!("UDB_MSSQL_DSN unset - skipping live SQL Server include golden");
        return;
    };

    let client = MssqlClient::new(dsn);
    let schema = format!("udb_ir_include_{}", uuid::Uuid::new_v4().simple());
    create_invoice_customer_tables(&client, &schema).await;
    seed_invoice_customer_rows(&client, &schema).await;

    let manifest = invoice_relation_manifest(&schema);
    let read = LogicalRead::message("billing.v1.Invoice")
        .with_projection(LogicalProjection::fields([
            "invoice_id".to_string(),
            "customer_id".to_string(),
            "total_cents".to_string(),
        ]))
        .with_include("customer")
        .with_sort(vec![LogicalSort {
            field: "invoice_id".to_string(),
            direction: SortDirection::Asc,
            nulls: crate::ir::projection::NullOrder::Default,
        }]);
    let ctx = CompileContext::new(&manifest);
    let (statement, params) =
        match compile_for_backend(&BackendKind::Mssql, CompileOperation::Read(&read), &ctx)
            .expect("SQL Server compiler registered")
            .expect("include read compiles")
        {
            CompiledRendering::Sql {
                backend,
                statement,
                params,
            } => {
                assert_eq!(backend, BackendKind::Mssql);
                (statement, params)
            }
            other => panic!("expected SQL Server SQL rendering, got {other:?}"),
        };
    assert!(params.is_empty(), "include query should not add binds");
    assert!(
        statement.contains("FOR JSON PATH, WITHOUT_ARRAY_WRAPPER"),
        "include must be projected by the compiled SQL; got: {statement}"
    );

    let rows = client
        .fetch_rows(&statement, &[])
        .await
        .expect("execute compiled SQL Server eager include read");
    assert_eq!(rows.len(), 2);
    let first_invoice = mssql_string(&rows[0], "invoice_id");
    let first_customer = mssql_json_value(&rows[0], "customer");
    assert_eq!(first_invoice, "inv-1");
    assert_eq!(first_customer["customer_id"], "cust-1");
    assert_eq!(first_customer["name"], "Alice");
    let second_invoice = mssql_string(&rows[1], "invoice_id");
    let second_customer = mssql_json_value(&rows[1], "customer");
    assert_eq!(second_invoice, "inv-2");
    assert_eq!(second_customer["customer_id"], "cust-2");
    assert_eq!(second_customer["name"], "Ana");

    let read_many = LogicalRead::message("crm.v1.Customer")
        .with_projection(LogicalProjection::fields([
            "customer_id".to_string(),
            "name".to_string(),
        ]))
        .with_include("invoices")
        .with_sort(vec![LogicalSort {
            field: "customer_id".to_string(),
            direction: SortDirection::Asc,
            nulls: crate::ir::projection::NullOrder::Default,
        }]);
    let (statement, params) = match compile_for_backend(
        &BackendKind::Mssql,
        CompileOperation::Read(&read_many),
        &ctx,
    )
    .expect("SQL Server compiler registered")
    .expect("has-many include read compiles")
    {
        CompiledRendering::Sql {
            backend,
            statement,
            params,
        } => {
            assert_eq!(backend, BackendKind::Mssql);
            (statement, params)
        }
        other => panic!("expected SQL Server SQL rendering, got {other:?}"),
    };
    assert!(
        params.is_empty(),
        "has-many include query should not add binds"
    );
    assert!(
        statement.contains("FOR JSON PATH) AS [invoices]"),
        "has-many include must be projected by one compiled SQL query; got: {statement}"
    );
    let rows = client
        .fetch_rows(&statement, &[])
        .await
        .expect("execute compiled SQL Server has-many eager include read");
    assert_eq!(rows.len(), 2);
    let first_customer_id = mssql_string(&rows[0], "customer_id");
    let first_invoices = mssql_json_value(&rows[0], "invoices");
    assert_eq!(first_customer_id, "cust-1");
    assert_eq!(first_invoices.as_array().expect("invoices array").len(), 1);
    assert_eq!(first_invoices[0]["invoice_id"], "inv-1");

    drop_invoice_customer_tables(&client, &schema).await;
}

async fn create_invoice_customer_tables(client: &MssqlClient, schema: &str) {
    let sql = format!(
        "IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');\
         CREATE TABLE [{schema}].[customers] (\
            [customer_id] NVARCHAR(64) NOT NULL PRIMARY KEY, \
            [name] NVARCHAR(255) NOT NULL);\
         CREATE TABLE [{schema}].[invoices] (\
            [invoice_id] NVARCHAR(64) NOT NULL PRIMARY KEY, \
            [customer_id] NVARCHAR(64) NOT NULL, \
            [total_cents] BIGINT NOT NULL, \
            CONSTRAINT [fk_invoice_customer] FOREIGN KEY ([customer_id]) \
                REFERENCES [{schema}].[customers] ([customer_id]));"
    );
    client
        .simple_batch(&sql)
        .await
        .expect("create SQL Server include tables");
}

async fn fetch_rows(
    client: &MssqlClient,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<GoldenRow> {
    let (statement, params) = compile_read_sql(BackendKind::Mssql, manifest);
    client
        .fetch_rows(&statement, &mssql_params(&params))
        .await
        .expect("execute compiled SQL Server read")
        .into_iter()
        .map(|row| GoldenRow {
            id: mssql_string(&row, "id"),
            name: mssql_string(&row, "name"),
            email: mssql_string(&row, "email"),
        })
        .collect()
}

async fn fetch_aggregate_rows(
    client: &MssqlClient,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<GoldenAggregateRow> {
    let (statement, params) = compile_aggregate_sql(BackendKind::Mssql, manifest);
    client
        .fetch_rows(&statement, &mssql_params(&params))
        .await
        .expect("execute compiled SQL Server aggregate")
        .into_iter()
        .map(|row| GoldenAggregateRow {
            tenant_id: mssql_string(&row, "tenant_id"),
            row_count: mssql_i64(&row, "row_count"),
        })
        .collect()
}

async fn fetch_index_names(
    client: &MssqlClient,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<String> {
    let (statement, params) = compile_list_customer_indexes_sql(BackendKind::Mssql, manifest);
    client
        .fetch_rows(&statement, &mssql_params(&params))
        .await
        .expect("execute compiled SQL Server resource-op list indexes")
        .into_iter()
        .map(|row| mssql_string(&row, "name"))
        .collect()
}

async fn eventually_fetch_search_names(
    executor: &crate::runtime::executors::mssql::MssqlExecutor,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<String> {
    for _ in 0..30 {
        let names = fetch_search_names(executor, manifest).await;
        if names.len() == 1 && names[0] == "Alice" {
            return names;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    fetch_search_names(executor, manifest).await
}

async fn fetch_search_names(
    executor: &crate::runtime::executors::mssql::MssqlExecutor,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<String> {
    let (statement, params) = compile_search_sql(BackendKind::Mssql, manifest);
    let params = params
        .iter()
        .map(logical_value_to_json)
        .collect::<Vec<serde_json::Value>>();
    let dispatch = serde_json::json!({
        "sql": statement,
        "params": params,
    });
    let body = SearchExecutor::search(executor, &dispatch.to_string())
        .await
        .expect("execute compiled SQL Server full-text search through SearchExecutor");
    let rows: serde_json::Value =
        serde_json::from_str(&body).expect("SQL Server search response JSON");
    let mut names = rows
        .as_array()
        .expect("SQL Server search response must be an array")
        .iter()
        .map(|row| {
            row.get("name")
                .and_then(|value| value.as_str())
                .expect("SQL Server search row must include name")
                .to_string()
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}

async fn execute(client: &MssqlClient, (statement, params): (String, Vec<LogicalValue>)) {
    let params = mssql_params(&params);
    if params.is_empty() {
        client
            .simple_batch(&statement)
            .await
            .expect("execute compiled SQL Server batch");
    } else {
        client
            .execute_sql(&statement, &params)
            .await
            .expect("execute compiled SQL Server mutation");
    }
}

async fn create_customer_table(client: &MssqlClient, schema: &str) {
    let pk = customer_pk_name(schema);
    client
        .simple_batch(&format!(
            "IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');\
             CREATE TABLE [{schema}].[customers] (\
                [id] NVARCHAR(191) NOT NULL, \
                [name] NVARCHAR(191) NOT NULL, \
                [email] NVARCHAR(191) NOT NULL, \
                [tenant_id] NVARCHAR(191) NOT NULL, \
                CONSTRAINT [{pk}] PRIMARY KEY ([id]));"
        ))
        .await
        .expect("create SQL Server golden customers table");
}

async fn create_full_text_index(client: &MssqlClient, schema: &str) -> bool {
    if !sql_server_full_text_installed(client).await {
        eprintln!(
            "SQL Server Full-Text Search is not installed - skipping live SQL Server FTS oracle"
        );
        return false;
    }
    let catalog = full_text_catalog_name(schema);
    let pk = customer_pk_name(schema);
    client
        .simple_batch(&format!(
            "IF NOT EXISTS (SELECT 1 FROM sys.fulltext_catalogs WHERE name = N'{catalog}') \
                EXEC(N'CREATE FULLTEXT CATALOG [{catalog}]');\
             IF NOT EXISTS (\
                SELECT 1 FROM sys.fulltext_indexes \
                WHERE object_id = OBJECT_ID(N'[{schema}].[customers]')) \
                EXEC(N'CREATE FULLTEXT INDEX ON [{schema}].[customers] \
                    ([name] LANGUAGE 1033, [email] LANGUAGE 1033) \
                    KEY INDEX [{pk}] ON [{catalog}] WITH CHANGE_TRACKING AUTO');"
        ))
        .await
        .expect("create SQL Server full-text catalog/index");
    true
}

async fn sql_server_full_text_installed(client: &MssqlClient) -> bool {
    let rows = client
        .fetch_rows(
            "SELECT FULLTEXTSERVICEPROPERTY('IsFullTextInstalled') AS installed",
            &[],
        )
        .await
        .expect("query SQL Server full-text installation status");
    rows.first()
        .map(|row| mssql_i64(row, "installed") != 0)
        .unwrap_or(false)
}

fn invoice_relation_manifest(schema: &str) -> CatalogManifest {
    CatalogManifest {
        tables: vec![
            ManifestTable {
                message_name: "Invoice".to_string(),
                proto_package: "billing.v1".to_string(),
                schema: schema.to_string(),
                table: "invoices".to_string(),
                primary_key: vec!["invoice_id".to_string()],
                columns: vec![
                    include_column("invoice_id", "string", "nvarchar(64)", true),
                    include_column("customer_id", "string", "nvarchar(64)", false),
                    include_column("total_cents", "int64", "bigint", false),
                ],
                foreign_keys: vec![ManifestForeignKey {
                    name: "fk_invoice_customer".to_string(),
                    columns: vec!["customer_id".to_string()],
                    ref_table: "customers".to_string(),
                    ref_columns: vec!["customer_id".to_string()],
                    ..Default::default()
                }],
                ..Default::default()
            },
            ManifestTable {
                message_name: "Customer".to_string(),
                proto_package: "crm.v1".to_string(),
                schema: schema.to_string(),
                table: "customers".to_string(),
                primary_key: vec!["customer_id".to_string()],
                columns: vec![
                    include_column("customer_id", "string", "nvarchar(64)", true),
                    include_column("name", "string", "nvarchar(255)", false),
                ],
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

fn include_column(name: &str, proto_type: &str, sql_type: &str, primary: bool) -> ManifestColumn {
    ManifestColumn {
        field_name: name.to_string(),
        column_name: name.to_string(),
        proto_type: proto_type.to_string(),
        sql_type: sql_type.to_string(),
        is_primary: primary,
        not_null: true,
        ..Default::default()
    }
}

async fn seed_invoice_customer_rows(client: &MssqlClient, schema: &str) {
    client
        .simple_batch(&format!(
            "INSERT INTO [{schema}].[customers] ([customer_id], [name]) \
             VALUES (N'cust-1', N'Alice'), (N'cust-2', N'Ana');\
             INSERT INTO [{schema}].[invoices] ([invoice_id], [customer_id], [total_cents]) \
             VALUES (N'inv-1', N'cust-1', 1200), (N'inv-2', N'cust-2', 3400);"
        ))
        .await
        .expect("seed SQL Server include rows");
}

async fn seed(client: &MssqlClient, schema: &str) {
    client
        .simple_batch(&format!(
            "INSERT INTO [{schema}].[customers] ([id], [name], [email], [tenant_id]) \
             VALUES \
                (N'cust-1', N'Alice', N'alice@example.com', N'tenant-a'), \
                (N'cust-2', N'Ana', N'ana@example.com', N'tenant-a'), \
                (N'cust-3', N'Bob', N'bob@example.com', N'tenant-b');"
        ))
        .await
        .expect("seed SQL Server golden rows");
}

async fn drop_invoice_customer_tables(client: &MssqlClient, schema: &str) {
    let _ = client
        .simple_batch(&format!(
            "IF OBJECT_ID(N'[{schema}].[invoices]', N'U') IS NOT NULL \
                DROP TABLE [{schema}].[invoices];\
             IF OBJECT_ID(N'[{schema}].[customers]', N'U') IS NOT NULL \
                DROP TABLE [{schema}].[customers];\
             IF SCHEMA_ID(N'{schema}') IS NOT NULL EXEC(N'DROP SCHEMA [{schema}]');"
        ))
        .await;
}

async fn drop_customer_table(client: &MssqlClient, schema: &str) {
    let catalog = full_text_catalog_name(schema);
    let _ = client
        .simple_batch(&format!(
            "IF EXISTS (\
                SELECT 1 FROM sys.fulltext_indexes \
                WHERE object_id = OBJECT_ID(N'[{schema}].[customers]')) \
                DROP FULLTEXT INDEX ON [{schema}].[customers];\
             IF OBJECT_ID(N'[{schema}].[customers]', N'U') IS NOT NULL \
                DROP TABLE [{schema}].[customers];\
             IF EXISTS (SELECT 1 FROM sys.fulltext_catalogs WHERE name = N'{catalog}') \
                DROP FULLTEXT CATALOG [{catalog}];\
             IF SCHEMA_ID(N'{schema}') IS NOT NULL EXEC(N'DROP SCHEMA [{schema}]');"
        ))
        .await;
}

fn customer_pk_name(schema: &str) -> String {
    format!("pk_{schema}_customers")
}

fn full_text_catalog_name(schema: &str) -> String {
    format!("ftc_{schema}")
}

fn mssql_params(values: &[LogicalValue]) -> Vec<SqlParam> {
    values
        .iter()
        .map(|value| match value {
            LogicalValue::String(s) => SqlParam::Str(s.clone()),
            other => unsupported_bind(other),
        })
        .collect()
}

fn logical_value_to_json(value: &LogicalValue) -> serde_json::Value {
    match value {
        LogicalValue::String(s) => serde_json::Value::String(s.clone()),
        other => unsupported_bind(other),
    }
}

fn mssql_string(row: &tiberius::Row, column: &str) -> String {
    row.try_get::<&str, _>(column)
        .expect("read SQL Server string cell")
        .expect("SQL Server string cell was NULL")
        .to_string()
}

fn mssql_json_value(row: &tiberius::Row, column: &str) -> serde_json::Value {
    let raw = mssql_string(row, column);
    serde_json::from_str(&raw).expect("parse SQL Server FOR JSON include result")
}

fn mssql_i64(row: &tiberius::Row, column: &str) -> i64 {
    if let Ok(Some(value)) = row.try_get::<i64, _>(column) {
        return value;
    }
    if let Ok(Some(value)) = row.try_get::<i32, _>(column) {
        return i64::from(value);
    }
    panic!("read SQL Server integer cell {column}");
}
