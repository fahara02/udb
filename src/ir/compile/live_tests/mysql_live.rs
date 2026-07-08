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
    expected_after_update_rows, expected_seed_rows, live_ir_enabled, swap_database,
    unsupported_bind,
};

fn bind<'q>(
    q: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    value: &LogicalValue,
) -> sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    match value {
        LogicalValue::String(s) => q.bind(s.clone()),
        other => unsupported_bind(other),
    }
}

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1 and UDB_MYSQL_DSN"]
async fn mysql_compiled_read_write_delete_match_live_golden_rows() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Ok(dsn) = std::env::var("UDB_MYSQL_DSN") else {
        eprintln!("UDB_MYSQL_DSN unset - skipping live MySQL IR golden");
        return;
    };

    use sqlx::mysql::MySqlPoolOptions;

    let admin = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
        .expect("connect to live MySQL");
    let db = format!("udb_ir_live_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE `{db}`"))
        .execute(&admin)
        .await
        .expect("create throwaway database");
    let pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&swap_database(&dsn, &db))
        .await
        .expect("connect to throwaway MySQL database");
    sqlx::query(&format!(
        "CREATE TABLE `{db}`.`customers` (\
            id varchar(191) PRIMARY KEY, name varchar(191) NOT NULL, \
            email varchar(191) NOT NULL, tenant_id varchar(191) NOT NULL)"
    ))
    .execute(&pool)
    .await
    .expect("create customers table");
    create_search_index(&pool, &db).await;
    seed(&pool, &db).await;

    let manifest = customer_manifest(&db);
    assert_eq!(fetch_rows(&pool, &manifest).await, expected_seed_rows());
    assert_eq!(
        fetch_search_rows(&pool, &manifest).await,
        vec![GoldenRow::new("cust-1", "Alice", "alice@example.com")]
    );

    execute(
        &pool,
        compile_ensure_name_index_sql(BackendKind::Mysql, &manifest),
    )
    .await;
    assert!(
        fetch_index_names(&pool, &manifest)
            .await
            .iter()
            .any(|name| name == "idx_customers_name"),
        "compiled MySQL resource-op index must be visible in SHOW INDEX"
    );

    execute(&pool, compile_insert_sql(BackendKind::Mysql, &manifest)).await;
    assert_eq!(
        fetch_rows(&pool, &manifest).await,
        expected_after_insert_rows()
    );

    execute(&pool, compile_update_sql(BackendKind::Mysql, &manifest)).await;
    assert_eq!(
        fetch_rows(&pool, &manifest).await,
        expected_after_update_rows()
    );

    execute(&pool, compile_delete_sql(BackendKind::Mysql, &manifest)).await;
    assert_eq!(
        fetch_rows(&pool, &manifest).await,
        expected_after_delete_rows()
    );
    assert_eq!(
        fetch_aggregate_rows(&pool, &manifest).await,
        expected_after_delete_aggregate_rows()
    );

    pool.close().await;
    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS `{db}`"))
        .execute(&admin)
        .await;
}

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1 and UDB_MYSQL_DSN"]
async fn mysql_eager_include_loads_belongs_to_in_one_compiled_query_live() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Ok(dsn) = std::env::var("UDB_MYSQL_DSN") else {
        eprintln!("UDB_MYSQL_DSN unset - skipping live MySQL include golden");
        return;
    };

    use sqlx::Row;
    use sqlx::mysql::MySqlPoolOptions;

    let admin = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
        .expect("connect to live MySQL");
    let db = format!("udb_ir_include_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE `{db}`"))
        .execute(&admin)
        .await
        .expect("create throwaway include database");
    let pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&swap_database(&dsn, &db))
        .await
        .expect("connect to throwaway MySQL include database");
    create_invoice_customer_tables(&pool, &db).await;
    seed_invoice_customer_rows(&pool, &db).await;

    let manifest = invoice_relation_manifest(&db);
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
        match compile_for_backend(&BackendKind::Mysql, CompileOperation::Read(&read), &ctx)
            .expect("MySQL compiler registered")
            .expect("include read compiles")
        {
            CompiledRendering::Sql {
                backend,
                statement,
                params,
            } => {
                assert_eq!(backend, BackendKind::Mysql);
                (statement, params)
            }
            other => panic!("expected MySQL SQL rendering, got {other:?}"),
        };
    assert!(params.is_empty(), "include query should not add binds");
    assert!(
        statement.contains("JSON_OBJECT("),
        "include must be projected by the compiled SQL; got: {statement}"
    );

    let rows = sqlx::query(&statement)
        .fetch_all(&pool)
        .await
        .expect("execute compiled MySQL eager include read");
    assert_eq!(rows.len(), 2);
    let first_invoice: String = rows[0].try_get("invoice_id").expect("invoice_id");
    let first_customer = mysql_json_value(&rows[0], "customer");
    assert_eq!(first_invoice, "inv-1");
    assert_eq!(first_customer["customer_id"], "cust-1");
    assert_eq!(first_customer["name"], "Alice");
    let second_invoice: String = rows[1].try_get("invoice_id").expect("invoice_id");
    let second_customer = mysql_json_value(&rows[1], "customer");
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
        &BackendKind::Mysql,
        CompileOperation::Read(&read_many),
        &ctx,
    )
    .expect("MySQL compiler registered")
    .expect("has-many include read compiles")
    {
        CompiledRendering::Sql {
            backend,
            statement,
            params,
        } => {
            assert_eq!(backend, BackendKind::Mysql);
            (statement, params)
        }
        other => panic!("expected MySQL SQL rendering, got {other:?}"),
    };
    assert!(
        params.is_empty(),
        "has-many include query should not add binds"
    );
    assert!(
        statement.contains("JSON_ARRAYAGG(JSON_OBJECT("),
        "has-many include must be projected by one compiled SQL query; got: {statement}"
    );
    let rows = sqlx::query(&statement)
        .fetch_all(&pool)
        .await
        .expect("execute compiled MySQL has-many eager include read");
    assert_eq!(rows.len(), 2);
    let first_customer_id: String = rows[0].try_get("customer_id").expect("customer_id");
    let first_invoices = mysql_json_value(&rows[0], "invoices");
    assert_eq!(first_customer_id, "cust-1");
    assert_eq!(first_invoices.as_array().expect("invoices array").len(), 1);
    assert_eq!(first_invoices[0]["invoice_id"], "inv-1");

    pool.close().await;
    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS `{db}`"))
        .execute(&admin)
        .await;
}

async fn fetch_rows(
    pool: &sqlx::MySqlPool,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<GoldenRow> {
    use sqlx::Row;

    let (statement, params) = compile_read_sql(BackendKind::Mysql, manifest);
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .fetch_all(pool)
        .await
        .expect("execute compiled MySQL read")
        .into_iter()
        .map(|row| GoldenRow {
            id: row.try_get("id").expect("id"),
            name: row.try_get("name").expect("name"),
            email: row.try_get("email").expect("email"),
        })
        .collect()
}

async fn fetch_aggregate_rows(
    pool: &sqlx::MySqlPool,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<GoldenAggregateRow> {
    use sqlx::Row;

    let (statement, params) = compile_aggregate_sql(BackendKind::Mysql, manifest);
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .fetch_all(pool)
        .await
        .expect("execute compiled MySQL aggregate")
        .into_iter()
        .map(|row| GoldenAggregateRow {
            tenant_id: row.try_get("tenant_id").expect("tenant_id"),
            row_count: row.try_get("row_count").expect("row_count"),
        })
        .collect()
}

async fn fetch_search_rows(
    pool: &sqlx::MySqlPool,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<GoldenRow> {
    use sqlx::Row;

    let (statement, params) = compile_search_sql(BackendKind::Mysql, manifest);
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .fetch_all(pool)
        .await
        .expect("execute compiled MySQL fulltext search")
        .into_iter()
        .map(|row| GoldenRow {
            id: row.try_get("id").expect("id"),
            name: row.try_get("name").expect("name"),
            email: row.try_get("email").expect("email"),
        })
        .collect()
}

async fn fetch_index_names(
    pool: &sqlx::MySqlPool,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<String> {
    use sqlx::Row;

    let (statement, params) = compile_list_customer_indexes_sql(BackendKind::Mysql, manifest);
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .fetch_all(pool)
        .await
        .expect("execute compiled MySQL resource-op list indexes")
        .into_iter()
        .map(|row| row.try_get("Key_name").expect("index name"))
        .collect()
}

async fn execute(pool: &sqlx::MySqlPool, (statement, params): (String, Vec<LogicalValue>)) {
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .execute(pool)
        .await
        .expect("execute compiled MySQL mutation");
}

async fn seed(pool: &sqlx::MySqlPool, db: &str) {
    sqlx::query(&format!(
        "INSERT INTO `{db}`.`customers` (id, name, email, tenant_id) \
         VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)"
    ))
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
    .expect("seed MySQL golden rows");
}

async fn create_search_index(pool: &sqlx::MySqlPool, db: &str) {
    sqlx::query(&format!(
        "CREATE FULLTEXT INDEX `idx_customers_fulltext` \
         ON `{db}`.`customers` (`id`, `name`, `email`, `tenant_id`)"
    ))
    .execute(pool)
    .await
    .expect("create MySQL fulltext index");
}

async fn create_invoice_customer_tables(pool: &sqlx::MySqlPool, db: &str) {
    sqlx::query(&format!(
        "CREATE TABLE `{db}`.`customers` (\
            customer_id varchar(191) PRIMARY KEY, name varchar(191) NOT NULL)"
    ))
    .execute(pool)
    .await
    .expect("create include customers table");
    sqlx::query(&format!(
        "CREATE TABLE `{db}`.`invoices` (\
            invoice_id varchar(191) PRIMARY KEY, \
            customer_id varchar(191) NOT NULL, \
            total_cents bigint NOT NULL, \
            CONSTRAINT `fk_invoice_customer` FOREIGN KEY (`customer_id`) \
                REFERENCES `{db}`.`customers` (`customer_id`))"
    ))
    .execute(pool)
    .await
    .expect("create include invoices table");
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
                    include_column("invoice_id", "string", "varchar(191)", true),
                    include_column("customer_id", "string", "varchar(191)", false),
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
                    include_column("customer_id", "string", "varchar(191)", true),
                    include_column("name", "string", "varchar(191)", false),
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

async fn seed_invoice_customer_rows(pool: &sqlx::MySqlPool, db: &str) {
    sqlx::query(&format!(
        "INSERT INTO `{db}`.`customers` (customer_id, name) VALUES (?, ?), (?, ?)"
    ))
    .bind("cust-1")
    .bind("Alice")
    .bind("cust-2")
    .bind("Ana")
    .execute(pool)
    .await
    .expect("seed include customers");
    sqlx::query(&format!(
        "INSERT INTO `{db}`.`invoices` (invoice_id, customer_id, total_cents) \
         VALUES (?, ?, ?), (?, ?, ?)"
    ))
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

fn mysql_json_value(row: &sqlx::mysql::MySqlRow, column: &str) -> serde_json::Value {
    use sqlx::Row;

    // MySQL 8.4 types JSON_OBJECT()/JSON_ARRAYAGG() projections as JSON, which
    // sqlx refuses to decode as `String` (mismatched types); decode the typed
    // JSON value first and keep the text decode as a fallback for engines that
    // report the projection as VARCHAR.
    row.try_get::<serde_json::Value, _>(column)
        .or_else(|_| {
            row.try_get::<String, _>(column)
                .map(|raw| serde_json::from_str(&raw).expect("parse MySQL JSON_OBJECT text"))
        })
        .expect("JSON_OBJECT result")
}
