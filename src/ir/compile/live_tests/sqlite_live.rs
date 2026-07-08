use crate::backend::BackendKind;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestForeignKey, ManifestTable};
use crate::ir::compile::{
    CompileContext, CompileOperation, CompiledRendering, compile_for_backend,
};
use crate::ir::operations::LogicalRead;
use crate::ir::projection::{LogicalProjection, LogicalSort, SortDirection};
use crate::ir::value::LogicalValue;

use super::support::{
    GoldenAggregateRow, GoldenRow, compile_aggregate_sql, compile_delete_sql,
    compile_ensure_name_index_sql, compile_insert_sql, compile_list_customer_indexes_sql,
    compile_read_sql, compile_search_sql, compile_update_sql, customer_manifest,
    expected_after_delete_aggregate_rows, expected_after_delete_rows, expected_after_insert_rows,
    expected_after_update_rows, expected_seed_rows, live_ir_enabled, unsupported_bind,
};

fn bind<'q>(
    q: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    value: &LogicalValue,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match value {
        LogicalValue::String(s) => q.bind(s.clone()),
        other => unsupported_bind(other),
    }
}

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1"]
async fn sqlite_compiled_read_write_delete_match_file_backed_golden_rows() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }

    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    let path = std::env::temp_dir().join(format!(
        "udb-ir-live-{}.sqlite",
        uuid::Uuid::new_v4().simple()
    ));
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("connect to file-backed SQLite");
    sqlx::query(
        "CREATE TABLE customers (\
            id text PRIMARY KEY, name text NOT NULL, email text NOT NULL, tenant_id text NOT NULL)",
    )
    .execute(&pool)
    .await
    .expect("create customers table");
    seed(&pool).await;
    create_search_table(&pool).await;
    seed_search_table(&pool).await;

    let manifest = customer_manifest("main");
    assert_eq!(fetch_rows(&pool, &manifest).await, expected_seed_rows());
    assert_eq!(
        fetch_search_rows(&pool, &manifest).await,
        vec![GoldenRow::new("cust-1", "Alice", "alice@example.com")]
    );

    execute(
        &pool,
        compile_ensure_name_index_sql(BackendKind::Sqlite, &manifest),
    )
    .await;
    assert!(
        fetch_index_names(&pool, &manifest)
            .await
            .iter()
            .any(|name| name == "idx_customers_name"),
        "compiled SQLite resource-op index must be visible in sqlite_master"
    );

    execute(&pool, compile_insert_sql(BackendKind::Sqlite, &manifest)).await;
    assert_eq!(
        fetch_rows(&pool, &manifest).await,
        expected_after_insert_rows()
    );

    execute(&pool, compile_update_sql(BackendKind::Sqlite, &manifest)).await;
    assert_eq!(
        fetch_rows(&pool, &manifest).await,
        expected_after_update_rows()
    );

    execute(&pool, compile_delete_sql(BackendKind::Sqlite, &manifest)).await;
    assert_eq!(
        fetch_rows(&pool, &manifest).await,
        expected_after_delete_rows()
    );
    assert_eq!(
        fetch_aggregate_rows(&pool, &manifest).await,
        expected_after_delete_aggregate_rows()
    );

    pool.close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1"]
async fn sqlite_eager_include_loads_belongs_to_in_one_compiled_query_live() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }

    use sqlx::Row;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    let path = std::env::temp_dir().join(format!(
        "udb-ir-include-{}.sqlite",
        uuid::Uuid::new_v4().simple()
    ));
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("connect to file-backed SQLite include DB");
    create_invoice_customer_tables(&pool).await;
    seed_invoice_customer_rows(&pool).await;

    let manifest = invoice_relation_manifest();
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
        match compile_for_backend(&BackendKind::Sqlite, CompileOperation::Read(&read), &ctx)
            .expect("SQLite compiler registered")
            .expect("include read compiles")
        {
            CompiledRendering::Sql {
                backend,
                statement,
                params,
            } => {
                assert_eq!(backend, BackendKind::Sqlite);
                (statement, params)
            }
            other => panic!("expected SQLite SQL rendering, got {other:?}"),
        };
    assert!(params.is_empty(), "include query should not add binds");
    assert!(
        statement.contains("json_object("),
        "include must be projected by the compiled SQL; got: {statement}"
    );

    let rows = sqlx::query(&statement)
        .fetch_all(&pool)
        .await
        .expect("execute compiled SQLite eager include read");
    assert_eq!(rows.len(), 2);
    let first_invoice: String = rows[0].try_get("invoice_id").expect("invoice_id");
    let first_customer_json: String = rows[0].try_get("customer").expect("customer json");
    let first_customer: serde_json::Value =
        serde_json::from_str(&first_customer_json).expect("customer JSON");
    assert_eq!(first_invoice, "inv-1");
    assert_eq!(first_customer["customer_id"], "cust-1");
    assert_eq!(first_customer["name"], "Alice");
    let second_invoice: String = rows[1].try_get("invoice_id").expect("invoice_id");
    let second_customer_json: String = rows[1].try_get("customer").expect("customer json");
    let second_customer: serde_json::Value =
        serde_json::from_str(&second_customer_json).expect("customer JSON");
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
        &BackendKind::Sqlite,
        CompileOperation::Read(&read_many),
        &ctx,
    )
    .expect("SQLite compiler registered")
    .expect("has-many include read compiles")
    {
        CompiledRendering::Sql {
            backend,
            statement,
            params,
        } => {
            assert_eq!(backend, BackendKind::Sqlite);
            (statement, params)
        }
        other => panic!("expected SQLite SQL rendering, got {other:?}"),
    };
    assert!(
        params.is_empty(),
        "has-many include query should not add binds"
    );
    assert!(
        statement.contains("json_group_array(json_object("),
        "has-many include must be projected by one compiled SQL query; got: {statement}"
    );
    let rows = sqlx::query(&statement)
        .fetch_all(&pool)
        .await
        .expect("execute compiled SQLite has-many eager include read");
    assert_eq!(rows.len(), 2);
    let first_customer_id: String = rows[0].try_get("customer_id").expect("customer_id");
    let first_invoices_json: String = rows[0].try_get("invoices").expect("invoices json");
    let first_invoices: serde_json::Value =
        serde_json::from_str(&first_invoices_json).expect("invoices JSON");
    assert_eq!(first_customer_id, "cust-1");
    assert_eq!(first_invoices.as_array().expect("invoices array").len(), 1);
    assert_eq!(first_invoices[0]["invoice_id"], "inv-1");

    pool.close().await;
    let _ = std::fs::remove_file(path);
}

async fn fetch_rows(
    pool: &sqlx::SqlitePool,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<GoldenRow> {
    use sqlx::Row;

    let (statement, params) = compile_read_sql(BackendKind::Sqlite, manifest);
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .fetch_all(pool)
        .await
        .expect("execute compiled SQLite read")
        .into_iter()
        .map(|row| GoldenRow {
            id: row.try_get("id").expect("id"),
            name: row.try_get("name").expect("name"),
            email: row.try_get("email").expect("email"),
        })
        .collect()
}

async fn fetch_aggregate_rows(
    pool: &sqlx::SqlitePool,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<GoldenAggregateRow> {
    use sqlx::Row;

    let (statement, params) = compile_aggregate_sql(BackendKind::Sqlite, manifest);
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .fetch_all(pool)
        .await
        .expect("execute compiled SQLite aggregate")
        .into_iter()
        .map(|row| GoldenAggregateRow {
            tenant_id: row.try_get("tenant_id").expect("tenant_id"),
            row_count: row.try_get("row_count").expect("row_count"),
        })
        .collect()
}

async fn fetch_search_rows(
    pool: &sqlx::SqlitePool,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<GoldenRow> {
    use sqlx::Row;

    let (statement, params) = compile_search_sql(BackendKind::Sqlite, manifest);
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .fetch_all(pool)
        .await
        .expect("execute compiled SQLite FTS search")
        .into_iter()
        .map(|row| GoldenRow {
            id: row.try_get("id").expect("id"),
            name: row.try_get("name").expect("name"),
            email: row.try_get("email").expect("email"),
        })
        .collect()
}

async fn fetch_index_names(
    pool: &sqlx::SqlitePool,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<String> {
    use sqlx::Row;

    let (statement, params) = compile_list_customer_indexes_sql(BackendKind::Sqlite, manifest);
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .fetch_all(pool)
        .await
        .expect("execute compiled SQLite resource-op list indexes")
        .into_iter()
        .map(|row| row.try_get("name").expect("index name"))
        .collect()
}

async fn execute(pool: &sqlx::SqlitePool, (statement, params): (String, Vec<LogicalValue>)) {
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .execute(pool)
        .await
        .expect("execute compiled SQLite mutation");
}

async fn create_invoice_customer_tables(pool: &sqlx::SqlitePool) {
    sqlx::query("CREATE TABLE customers (customer_id text PRIMARY KEY, name text NOT NULL)")
        .execute(pool)
        .await
        .expect("create include customers table");
    sqlx::query(
        "CREATE TABLE invoices (\
            invoice_id text PRIMARY KEY, customer_id text NOT NULL REFERENCES customers(customer_id), \
            total_cents integer NOT NULL)",
    )
    .execute(pool)
    .await
    .expect("create include invoices table");
}

fn invoice_relation_manifest() -> CatalogManifest {
    CatalogManifest {
        tables: vec![
            ManifestTable {
                message_name: "Invoice".to_string(),
                proto_package: "billing.v1".to_string(),
                schema: "main".to_string(),
                table: "invoices".to_string(),
                primary_key: vec!["invoice_id".to_string()],
                columns: vec![
                    ManifestColumn {
                        field_name: "invoice_id".to_string(),
                        column_name: "invoice_id".to_string(),
                        proto_type: "string".to_string(),
                        sql_type: "TEXT".to_string(),
                        is_primary: true,
                        not_null: true,
                        ..Default::default()
                    },
                    ManifestColumn {
                        field_name: "customer_id".to_string(),
                        column_name: "customer_id".to_string(),
                        proto_type: "string".to_string(),
                        sql_type: "TEXT".to_string(),
                        not_null: true,
                        ..Default::default()
                    },
                    ManifestColumn {
                        field_name: "total_cents".to_string(),
                        column_name: "total_cents".to_string(),
                        proto_type: "int64".to_string(),
                        sql_type: "INTEGER".to_string(),
                        not_null: true,
                        ..Default::default()
                    },
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
                schema: "main".to_string(),
                table: "customers".to_string(),
                primary_key: vec!["customer_id".to_string()],
                columns: vec![
                    ManifestColumn {
                        field_name: "customer_id".to_string(),
                        column_name: "customer_id".to_string(),
                        proto_type: "string".to_string(),
                        sql_type: "TEXT".to_string(),
                        is_primary: true,
                        not_null: true,
                        ..Default::default()
                    },
                    ManifestColumn {
                        field_name: "name".to_string(),
                        column_name: "name".to_string(),
                        proto_type: "string".to_string(),
                        sql_type: "TEXT".to_string(),
                        not_null: true,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

async fn seed(pool: &sqlx::SqlitePool) {
    sqlx::query(
        "INSERT INTO customers (id, name, email, tenant_id) \
         VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)",
    )
    .bind("cust-1")
    .bind("Alice")
    .bind("alice@example.com")
    .bind("tenant-a")
    .bind("cust-2")
    .bind("Ana")
    .bind("ana@example.com")
    .bind("tenant-a")
    .bind("cust-3")
    .bind("Bob")
    .bind("bob@example.com")
    .bind("tenant-b")
    .execute(pool)
    .await
    .expect("seed SQLite golden rows");
}

async fn seed_invoice_customer_rows(pool: &sqlx::SqlitePool) {
    sqlx::query("INSERT INTO customers (customer_id, name) VALUES (?, ?), (?, ?)")
        .bind("cust-1")
        .bind("Alice")
        .bind("cust-2")
        .bind("Ana")
        .execute(pool)
        .await
        .expect("seed include customers");
    sqlx::query(
        "INSERT INTO invoices (invoice_id, customer_id, total_cents) VALUES (?, ?, ?), (?, ?, ?)",
    )
    .bind("inv-1")
    .bind("cust-1")
    .bind(1200_i64)
    .bind("inv-2")
    .bind("cust-2")
    .bind(3400_i64)
    .execute(pool)
    .await
    .expect("seed include invoices");
}

async fn create_search_table(pool: &sqlx::SqlitePool) {
    sqlx::query("CREATE VIRTUAL TABLE customers_fts USING fts5(id, name, email, tenant_id)")
        .execute(pool)
        .await
        .expect("create SQLite FTS5 table");
}

async fn seed_search_table(pool: &sqlx::SqlitePool) {
    sqlx::query(
        "INSERT INTO customers_fts(rowid, id, name, email, tenant_id) \
         SELECT rowid, id, name, email, tenant_id FROM customers",
    )
    .execute(pool)
    .await
    .expect("seed SQLite FTS5 table");
}
