use crate::backend::BackendKind;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestForeignKey, ManifestTable};
use crate::ir::compile::{
    CompileContext, CompileOperation, CompiledRendering, compile_for_backend,
};
use crate::ir::operations::LogicalRead;
use crate::ir::projection::{LogicalProjection, LogicalSort, SortDirection};
use crate::ir::value::LogicalValue;
use crate::planning::broker::{
    DeletePlanRequest, RequestContext, SelectPlanRequest, SortSpec, UpsertPlanRequest,
    build_delete_logical_delete, build_delete_plan, build_select_logical_read,
    build_select_query_plan, build_upsert_logical_write, build_upsert_plan, table_for_message,
};
use crate::runtime::postgres_helpers::{bind_values, filter_bind_values, record_values};

use super::support::{
    GoldenAggregateRow, GoldenRow, compile_aggregate_sql, compile_delete_sql,
    compile_ensure_name_index_sql, compile_insert_sql, compile_list_customer_indexes_sql,
    compile_read_sql, compile_search_sql, compile_update_sql, customer_manifest,
    expected_after_delete_aggregate_rows, expected_after_delete_rows, expected_after_insert_rows,
    expected_after_update_rows, expected_seed_rows, live_ir_enabled, unsupported_bind,
};

fn bind<'q>(
    q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    value: &LogicalValue,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match value {
        LogicalValue::String(s) => q.bind(s.clone()),
        other => unsupported_bind(other),
    }
}

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1 and UDB_PG_DSN"]
async fn postgres_compiled_read_write_delete_match_live_golden_rows() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Ok(dsn) = std::env::var("UDB_PG_DSN").or_else(|_| std::env::var("DATABASE_URL")) else {
        eprintln!("UDB_PG_DSN / DATABASE_URL unset - skipping live Postgres IR golden");
        return;
    };

    use sqlx::postgres::PgPoolOptions;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect to live Postgres");
    let schema = format!("udb_ir_live_{}", uuid::Uuid::new_v4().simple());
    create_customer_table(&pool, &schema).await;
    seed(&pool, &schema).await;

    let manifest = customer_manifest(&schema);
    assert_eq!(fetch_rows(&pool, &manifest).await, expected_seed_rows());
    assert_eq!(
        fetch_search_rows(&pool, &manifest).await,
        vec![GoldenRow::new("cust-1", "Alice", "alice@example.com")]
    );

    execute(
        &pool,
        compile_ensure_name_index_sql(BackendKind::Postgres, &manifest),
    )
    .await;
    assert!(
        fetch_index_names(&pool, &manifest)
            .await
            .iter()
            .any(|name| name == "idx_customers_name"),
        "compiled Postgres resource-op index must be visible in pg_indexes"
    );

    execute(&pool, compile_insert_sql(BackendKind::Postgres, &manifest)).await;
    assert_eq!(
        fetch_rows(&pool, &manifest).await,
        expected_after_insert_rows()
    );

    execute(&pool, compile_update_sql(BackendKind::Postgres, &manifest)).await;
    assert_eq!(
        fetch_rows(&pool, &manifest).await,
        expected_after_update_rows()
    );

    execute(&pool, compile_delete_sql(BackendKind::Postgres, &manifest)).await;
    assert_eq!(
        fetch_rows(&pool, &manifest).await,
        expected_after_delete_rows()
    );
    assert_eq!(
        fetch_aggregate_rows(&pool, &manifest).await,
        expected_after_delete_aggregate_rows()
    );

    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(&pool)
        .await;
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1 and UDB_PG_DSN"]
async fn postgres_data_plane_planner_and_bridged_ir_match_live_rows() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Ok(dsn) = std::env::var("UDB_PG_DSN").or_else(|_| std::env::var("DATABASE_URL")) else {
        eprintln!("UDB_PG_DSN / DATABASE_URL unset - skipping live Postgres planner/IR A/B");
        return;
    };

    use sqlx::postgres::PgPoolOptions;

    let pool = PgPoolOptions::new()
        .max_connections(3)
        .connect(&dsn)
        .await
        .expect("connect to live Postgres");
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let legacy_schema = format!("udb_pg_merge_legacy_{run_id}");
    let ir_schema = format!("udb_pg_merge_ir_{run_id}");
    create_customer_table(&pool, &legacy_schema).await;
    create_customer_table(&pool, &ir_schema).await;
    seed(&pool, &legacy_schema).await;
    seed(&pool, &ir_schema).await;

    let legacy_manifest = customer_manifest(&legacy_schema);
    let ir_manifest = customer_manifest(&ir_schema);
    let read_ctx = read_context();
    let write_ctx = write_context();

    let select_request = SelectPlanRequest {
        context: read_ctx.clone(),
        message_type: "acme.billing.v1.Customer".to_string(),
        filter: serde_json::json!({"tenant_id": "tenant-a"}),
        fields: vec!["id".to_string(), "name".to_string(), "email".to_string()],
        sort: vec![SortSpec {
            field: "name".to_string(),
            descending: false,
        }],
        limit: 10,
    };
    assert_eq!(
        fetch_legacy_select_rows(&pool, &legacy_manifest, &select_request).await,
        fetch_bridged_select_rows(&pool, &ir_manifest, &select_request).await
    );

    let upsert_request = UpsertPlanRequest {
        context: write_ctx.clone(),
        message_type: "acme.billing.v1.Customer".to_string(),
        record: serde_json::json!({
            "id": "cust-4",
            "name": "Abe",
            "email": "abe@example.com",
            "tenant_id": "tenant-a"
        }),
        ..Default::default()
    };
    execute_legacy_upsert(&pool, &legacy_manifest, &upsert_request).await;
    execute_bridged_upsert(&pool, &ir_manifest, &upsert_request).await;
    assert_eq!(
        fetch_all_rows_direct(&pool, &legacy_schema).await,
        fetch_all_rows_direct(&pool, &ir_schema).await
    );

    let delete_request = DeletePlanRequest {
        context: write_ctx,
        message_type: "acme.billing.v1.Customer".to_string(),
        filter: serde_json::json!({
            "tenant_id": "tenant-a",
            "id": "cust-2"
        }),
    };
    execute_legacy_delete(&pool, &legacy_manifest, &delete_request).await;
    execute_bridged_delete(&pool, &ir_manifest, &delete_request).await;
    assert_eq!(
        fetch_all_rows_direct(&pool, &legacy_schema).await,
        fetch_all_rows_direct(&pool, &ir_schema).await
    );

    let _ = sqlx::query(&format!(
        "DROP SCHEMA IF EXISTS \"{legacy_schema}\" CASCADE"
    ))
    .execute(&pool)
    .await;
    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{ir_schema}\" CASCADE"))
        .execute(&pool)
        .await;
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1 and UDB_PG_DSN"]
async fn postgres_eager_include_loads_belongs_to_in_one_compiled_query_live() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Ok(dsn) = std::env::var("UDB_PG_DSN").or_else(|_| std::env::var("DATABASE_URL")) else {
        eprintln!("UDB_PG_DSN / DATABASE_URL unset - skipping live Postgres include golden");
        return;
    };

    use sqlx::Row;
    use sqlx::postgres::PgPoolOptions;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect to live Postgres");
    let schema = format!("udb_ir_include_{}", uuid::Uuid::new_v4().simple());
    create_invoice_customer_tables(&pool, &schema).await;
    seed_invoice_customer_rows(&pool, &schema).await;

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
        match compile_for_backend(&BackendKind::Postgres, CompileOperation::Read(&read), &ctx)
            .expect("Postgres compiler registered")
            .expect("include read compiles")
        {
            CompiledRendering::Sql {
                backend,
                statement,
                params,
            } => {
                assert_eq!(backend, BackendKind::Postgres);
                (statement, params)
            }
            other => panic!("expected Postgres SQL rendering, got {other:?}"),
        };
    assert!(params.is_empty(), "include query should not add binds");
    assert!(
        statement.contains("to_jsonb(\"_udb_include_customer\".*)"),
        "include must be projected by the compiled SQL; got: {statement}"
    );

    let rows = sqlx::query(&statement)
        .fetch_all(&pool)
        .await
        .expect("execute compiled Postgres eager include read");
    assert_eq!(rows.len(), 2);
    let first_invoice: String = rows[0].try_get("invoice_id").expect("invoice_id");
    let first_customer: serde_json::Value = rows[0].try_get("customer").expect("customer json");
    assert_eq!(first_invoice, "inv-1");
    assert_eq!(first_customer["customer_id"], "cust-1");
    assert_eq!(first_customer["name"], "Alice");
    let second_invoice: String = rows[1].try_get("invoice_id").expect("invoice_id");
    let second_customer: serde_json::Value = rows[1].try_get("customer").expect("customer json");
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
        &BackendKind::Postgres,
        CompileOperation::Read(&read_many),
        &ctx,
    )
    .expect("Postgres compiler registered")
    .expect("has-many include read compiles")
    {
        CompiledRendering::Sql {
            backend,
            statement,
            params,
        } => {
            assert_eq!(backend, BackendKind::Postgres);
            (statement, params)
        }
        other => panic!("expected Postgres SQL rendering, got {other:?}"),
    };
    assert!(
        params.is_empty(),
        "has-many include query should not add binds"
    );
    assert!(
        statement.contains("jsonb_agg(to_jsonb(\"_udb_include_invoices\".*))"),
        "has-many include must be projected by one compiled SQL query; got: {statement}"
    );
    let rows = sqlx::query(&statement)
        .fetch_all(&pool)
        .await
        .expect("execute compiled Postgres has-many eager include read");
    assert_eq!(rows.len(), 2);
    let first_customer_id: String = rows[0].try_get("customer_id").expect("customer_id");
    let first_invoices: serde_json::Value = rows[0].try_get("invoices").expect("invoices json");
    assert_eq!(first_customer_id, "cust-1");
    assert_eq!(first_invoices.as_array().expect("invoices array").len(), 1);
    assert_eq!(first_invoices[0]["invoice_id"], "inv-1");

    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(&pool)
        .await;
    pool.close().await;
}

async fn fetch_rows(
    pool: &sqlx::PgPool,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<GoldenRow> {
    use sqlx::Row;

    let (statement, params) = compile_read_sql(BackendKind::Postgres, manifest);
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .fetch_all(pool)
        .await
        .expect("execute compiled Postgres read")
        .into_iter()
        .map(|row| GoldenRow {
            id: row.try_get("id").expect("id"),
            name: row.try_get("name").expect("name"),
            email: row.try_get("email").expect("email"),
        })
        .collect()
}

async fn fetch_legacy_select_rows(
    pool: &sqlx::PgPool,
    manifest: &crate::generation::CatalogManifest,
    request: &SelectPlanRequest,
) -> Vec<GoldenRow> {
    use sqlx::Row;

    let plan = build_select_query_plan(manifest, request);
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    let table = table_for_message(manifest, &request.message_type).expect("manifest table");
    let values = filter_bind_values(&request.filter);
    let query = bind_values(
        sqlx::query(&plan.sql),
        table,
        &plan.parameter_columns,
        &values,
    )
    .expect("bind legacy select");
    query
        .fetch_all(pool)
        .await
        .expect("execute legacy planner select")
        .into_iter()
        .map(|row| GoldenRow {
            id: row.try_get("id").expect("id"),
            name: row.try_get("name").expect("name"),
            email: row.try_get("email").expect("email"),
        })
        .collect()
}

async fn fetch_bridged_select_rows(
    pool: &sqlx::PgPool,
    manifest: &crate::generation::CatalogManifest,
    request: &SelectPlanRequest,
) -> Vec<GoldenRow> {
    use sqlx::Row;

    let read =
        build_select_logical_read(manifest, request).expect("planner select lowers to neutral IR");
    let (statement, params) =
        compile_bridged_postgres_sql(manifest, &request.context, CompileOperation::Read(&read));
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .fetch_all(pool)
        .await
        .expect("execute bridged IR select")
        .into_iter()
        .map(|row| GoldenRow {
            id: row.try_get("id").expect("id"),
            name: row.try_get("name").expect("name"),
            email: row.try_get("email").expect("email"),
        })
        .collect()
}

async fn fetch_aggregate_rows(
    pool: &sqlx::PgPool,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<GoldenAggregateRow> {
    use sqlx::Row;

    let (statement, params) = compile_aggregate_sql(BackendKind::Postgres, manifest);
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .fetch_all(pool)
        .await
        .expect("execute compiled Postgres aggregate")
        .into_iter()
        .map(|row| GoldenAggregateRow {
            tenant_id: row.try_get("tenant_id").expect("tenant_id"),
            row_count: row.try_get("row_count").expect("row_count"),
        })
        .collect()
}

async fn fetch_search_rows(
    pool: &sqlx::PgPool,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<GoldenRow> {
    use sqlx::Row;

    let (statement, params) = compile_search_sql(BackendKind::Postgres, manifest);
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .fetch_all(pool)
        .await
        .expect("execute compiled Postgres text search")
        .into_iter()
        .map(|row| GoldenRow {
            id: row.try_get("id").expect("id"),
            name: row.try_get("name").expect("name"),
            email: row.try_get("email").expect("email"),
        })
        .collect()
}

async fn fetch_index_names(
    pool: &sqlx::PgPool,
    manifest: &crate::generation::CatalogManifest,
) -> Vec<String> {
    use sqlx::Row;

    let (statement, params) = compile_list_customer_indexes_sql(BackendKind::Postgres, manifest);
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .fetch_all(pool)
        .await
        .expect("execute compiled Postgres resource-op list indexes")
        .into_iter()
        .map(|row| row.try_get("indexname").expect("index name"))
        .collect()
}

async fn execute(pool: &sqlx::PgPool, (statement, params): (String, Vec<LogicalValue>)) {
    let mut query = sqlx::query(&statement);
    for value in &params {
        query = bind(query, value);
    }
    query
        .execute(pool)
        .await
        .expect("execute compiled Postgres mutation");
}

async fn execute_legacy_upsert(
    pool: &sqlx::PgPool,
    manifest: &crate::generation::CatalogManifest,
    request: &UpsertPlanRequest,
) {
    let plan = build_upsert_plan(manifest, request);
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    let table = table_for_message(manifest, &request.message_type).expect("manifest table");
    let values = record_values(&request.record, &plan.parameter_columns).expect("record values");
    let query = bind_values(
        sqlx::query(&plan.sql),
        table,
        &plan.parameter_columns,
        &values,
    )
    .expect("bind legacy upsert");
    query
        .execute(pool)
        .await
        .expect("execute legacy planner upsert");
}

async fn execute_bridged_upsert(
    pool: &sqlx::PgPool,
    manifest: &crate::generation::CatalogManifest,
    request: &UpsertPlanRequest,
) {
    let write =
        build_upsert_logical_write(manifest, request).expect("planner upsert lowers to neutral IR");
    execute(
        pool,
        compile_bridged_postgres_sql(manifest, &request.context, CompileOperation::Write(&write)),
    )
    .await;
}

async fn execute_legacy_delete(
    pool: &sqlx::PgPool,
    manifest: &crate::generation::CatalogManifest,
    request: &DeletePlanRequest,
) {
    let plan = build_delete_plan(manifest, request);
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    let table = table_for_message(manifest, &request.message_type).expect("manifest table");
    // Mirror the production planner fallback exactly: bind physical-column
    // filter values first, then the verified tenant/project predicates that
    // `build_delete_plan` appends after the caller-owned filter placeholders.
    let normalized_filter = crate::planning::broker::normalize_filter_keys(
        &crate::planning::broker::column_resolver(table),
        &request.filter,
    );
    let mut values = filter_bind_values(&normalized_filter);
    values.extend(
        plan.context_parameter_values
            .iter()
            .map(|value| serde_json::Value::String(value.clone())),
    );
    let query = bind_values(
        sqlx::query(&plan.sql),
        table,
        &plan.parameter_columns,
        &values,
    )
    .expect("bind legacy delete");
    query
        .execute(pool)
        .await
        .expect("execute legacy planner delete");
}

async fn execute_bridged_delete(
    pool: &sqlx::PgPool,
    manifest: &crate::generation::CatalogManifest,
    request: &DeletePlanRequest,
) {
    let delete = build_delete_logical_delete(manifest, request)
        .expect("planner delete lowers to neutral IR");
    execute(
        pool,
        compile_bridged_postgres_sql(
            manifest,
            &request.context,
            CompileOperation::Delete(&delete),
        ),
    )
    .await;
}

fn compile_bridged_postgres_sql(
    manifest: &crate::generation::CatalogManifest,
    context: &RequestContext,
    operation: CompileOperation<'_>,
) -> (String, Vec<LogicalValue>) {
    let compile_ctx = CompileContext::new(manifest)
        .with_tenant(&context.tenant_id)
        .with_project(&context.project_id);
    match compile_for_backend(&BackendKind::Postgres, operation, &compile_ctx)
        .expect("Postgres compiler registered")
        .expect("bridged planner IR compiles")
    {
        CompiledRendering::Sql {
            backend,
            statement,
            params,
        } => {
            assert_eq!(backend, BackendKind::Postgres);
            (statement, params)
        }
        other => panic!("expected Postgres SQL rendering, got {other:?}"),
    }
}

async fn fetch_all_rows_direct(pool: &sqlx::PgPool, schema: &str) -> Vec<GoldenRow> {
    use sqlx::Row;

    sqlx::query(&format!(
        "SELECT id, name, email FROM \"{schema}\".customers ORDER BY id"
    ))
    .fetch_all(pool)
    .await
    .expect("fetch all comparison rows")
    .into_iter()
    .map(|row| GoldenRow {
        id: row.try_get("id").expect("id"),
        name: row.try_get("name").expect("name"),
        email: row.try_get("email").expect("email"),
    })
    .collect()
}

async fn create_customer_table(pool: &sqlx::PgPool, schema: &str) {
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(pool)
        .await
        .expect("create throwaway schema");
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".customers (\
            id text PRIMARY KEY, name text NOT NULL, email text NOT NULL, tenant_id text NOT NULL, \
            _search_tsv tsvector GENERATED ALWAYS AS \
            (to_tsvector('simple', coalesce(id, '') || ' ' || coalesce(name, '') || ' ' || coalesce(email, '') || ' ' || coalesce(tenant_id, ''))) STORED)"
    ))
    .execute(pool)
    .await
    .expect("create customers table");
}

async fn create_invoice_customer_tables(pool: &sqlx::PgPool, schema: &str) {
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(pool)
        .await
        .expect("create throwaway include schema");
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".customers (\
            customer_id text PRIMARY KEY, name text NOT NULL)"
    ))
    .execute(pool)
    .await
    .expect("create include customers table");
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".invoices (\
            invoice_id text PRIMARY KEY, customer_id text NOT NULL REFERENCES \"{schema}\".customers(customer_id), \
            total_cents bigint NOT NULL)"
    ))
    .execute(pool)
    .await
    .expect("create invoices table");
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
                    ManifestColumn {
                        field_name: "invoice_id".to_string(),
                        column_name: "invoice_id".to_string(),
                        proto_type: "string".to_string(),
                        sql_type: "text".to_string(),
                        is_primary: true,
                        not_null: true,
                        ..Default::default()
                    },
                    ManifestColumn {
                        field_name: "customer_id".to_string(),
                        column_name: "customer_id".to_string(),
                        proto_type: "string".to_string(),
                        sql_type: "text".to_string(),
                        not_null: true,
                        ..Default::default()
                    },
                    ManifestColumn {
                        field_name: "total_cents".to_string(),
                        column_name: "total_cents".to_string(),
                        proto_type: "int64".to_string(),
                        sql_type: "bigint".to_string(),
                        not_null: true,
                        ..Default::default()
                    },
                ],
                foreign_keys: vec![ManifestForeignKey {
                    name: "fk_invoice_customer".to_string(),
                    columns: vec!["customer_id".to_string()],
                    ref_schema: schema.to_string(),
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
                    ManifestColumn {
                        field_name: "customer_id".to_string(),
                        column_name: "customer_id".to_string(),
                        proto_type: "string".to_string(),
                        sql_type: "text".to_string(),
                        is_primary: true,
                        not_null: true,
                        ..Default::default()
                    },
                    ManifestColumn {
                        field_name: "name".to_string(),
                        column_name: "name".to_string(),
                        proto_type: "string".to_string(),
                        sql_type: "text".to_string(),
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

fn read_context() -> RequestContext {
    RequestContext {
        tenant_id: "tenant-a".to_string(),
        purpose: "live-pg-merge".to_string(),
        scopes: vec!["udb:read".to_string()],
        ..Default::default()
    }
}

fn write_context() -> RequestContext {
    RequestContext {
        tenant_id: "tenant-a".to_string(),
        purpose: "live-pg-merge".to_string(),
        scopes: vec!["udb:write".to_string()],
        ..Default::default()
    }
}

async fn seed(pool: &sqlx::PgPool, schema: &str) {
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".customers (id, name, email, tenant_id) \
         VALUES ($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)"
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
    .expect("seed Postgres golden rows");
}

async fn seed_invoice_customer_rows(pool: &sqlx::PgPool, schema: &str) {
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".customers (customer_id, name) \
         VALUES ($1, $2), ($3, $4)"
    ))
    .bind("cust-1")
    .bind("Alice")
    .bind("cust-2")
    .bind("Ana")
    .execute(pool)
    .await
    .expect("seed include customers");
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".invoices (invoice_id, customer_id, total_cents) \
         VALUES ($1, $2, $3), ($4, $5, $6)"
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
