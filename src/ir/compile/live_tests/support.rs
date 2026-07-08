use std::collections::BTreeMap;

use crate::backend::BackendKind;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::ir::compile::{
    CompileContext, CompileOperation, CompiledRendering, compile_for_backend,
};
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    AggregateExpr, AggregateFunc, ConflictStrategy, LogicalAggregate, LogicalDelete, LogicalRead,
    LogicalRecord, LogicalResourceOp, LogicalSearch, LogicalWrite, ResourceKind, ResourceOpKind,
};
use crate::ir::projection::{
    LogicalPagination, LogicalProjection, LogicalSort, NullOrder, SortDirection,
};
use crate::ir::value::LogicalValue;

const MESSAGE: &str = "acme.billing.v1.Customer";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GoldenRow {
    pub id: String,
    pub name: String,
    pub email: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GoldenAggregateRow {
    pub tenant_id: String,
    pub row_count: i64,
}

impl GoldenAggregateRow {
    pub(super) fn new(tenant_id: &str, row_count: i64) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            row_count,
        }
    }
}

impl GoldenRow {
    pub(super) fn new(id: &str, name: &str, email: &str) -> Self {
        Self {
            id: id.to_string(),
            name: name.to_string(),
            email: email.to_string(),
        }
    }
}

pub(super) fn live_ir_enabled() -> bool {
    std::env::var("UDB_IR_LIVE_GOLDEN_TESTS")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

pub(super) fn swap_database(dsn: &str, new_db: &str) -> String {
    match dsn.rsplit_once('/') {
        Some((prefix, _db)) => format!("{prefix}/{new_db}"),
        None => format!("{dsn}/{new_db}"),
    }
}

pub(super) fn customer_manifest(schema: &str) -> CatalogManifest {
    CatalogManifest {
        tables: vec![ManifestTable {
            message_name: MESSAGE.to_string(),
            schema: schema.to_string(),
            table: "customers".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec![
                column("id", true),
                column("name", false),
                column("email", false),
                ManifestColumn {
                    field_name: "_search_tsv".to_string(),
                    column_name: "_search_tsv".to_string(),
                    proto_type: "tsvector".to_string(),
                    sql_type: "tsvector".to_string(),
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "tenant_id".to_string(),
                    column_name: "tenant_id".to_string(),
                    proto_type: "string".to_string(),
                    sql_type: "text".to_string(),
                    not_null: true,
                    is_tenant_column: true,
                    ..Default::default()
                },
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
        not_null: true,
        ..Default::default()
    }
}

pub(super) fn expected_seed_rows() -> Vec<GoldenRow> {
    vec![
        GoldenRow::new("cust-1", "Alice", "alice@example.com"),
        GoldenRow::new("cust-2", "Ana", "ana@example.com"),
    ]
}

pub(super) fn expected_after_insert_rows() -> Vec<GoldenRow> {
    vec![
        GoldenRow::new("cust-4", "Abe", "abe@example.com"),
        GoldenRow::new("cust-1", "Alice", "alice@example.com"),
        GoldenRow::new("cust-2", "Ana", "ana@example.com"),
    ]
}

pub(super) fn expected_after_update_rows() -> Vec<GoldenRow> {
    vec![
        GoldenRow::new("cust-4", "Abe", "abe@example.com"),
        GoldenRow::new("cust-1", "Alicia", "alicia@example.com"),
        GoldenRow::new("cust-2", "Ana", "ana@example.com"),
    ]
}

pub(super) fn expected_after_delete_rows() -> Vec<GoldenRow> {
    vec![
        GoldenRow::new("cust-4", "Abe", "abe@example.com"),
        GoldenRow::new("cust-1", "Alicia", "alicia@example.com"),
    ]
}

pub(super) fn expected_after_delete_aggregate_rows() -> Vec<GoldenAggregateRow> {
    vec![GoldenAggregateRow::new("tenant-a", 2)]
}

pub(super) fn compile_read_sql(
    backend: BackendKind,
    manifest: &CatalogManifest,
) -> (String, Vec<LogicalValue>) {
    let read = LogicalRead::message(MESSAGE)
        .with_filter(LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "tenant_id".to_string(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("tenant-a".to_string()),
            },
            LogicalFilter::Comparison {
                field: "email".to_string(),
                op: ComparisonOp::StartsWith,
                value: LogicalValue::String("a".to_string()),
            },
        ]))
        .with_projection(LogicalProjection::fields([
            "id".to_string(),
            "name".to_string(),
            "email".to_string(),
        ]))
        .with_sort(vec![LogicalSort {
            field: "name".to_string(),
            direction: SortDirection::Asc,
            nulls: NullOrder::Default,
        }])
        .with_pagination(LogicalPagination::limit(10));
    compile_sql(backend, manifest, CompileOperation::Read(&read))
}

pub(super) fn compile_insert_sql(
    backend: BackendKind,
    manifest: &CatalogManifest,
) -> (String, Vec<LogicalValue>) {
    let write = LogicalWrite {
        message_type: MESSAGE.to_string(),
        records: vec![record([
            ("id", "cust-4"),
            ("name", "Abe"),
            ("email", "abe@example.com"),
            ("tenant_id", "tenant-a"),
        ])],
        conflict: ConflictStrategy::Error,
        return_fields: Vec::new(),
    };
    compile_sql(backend, manifest, CompileOperation::Write(&write))
}

pub(super) fn compile_update_sql(
    backend: BackendKind,
    manifest: &CatalogManifest,
) -> (String, Vec<LogicalValue>) {
    let write = LogicalWrite {
        message_type: MESSAGE.to_string(),
        records: vec![record([
            ("id", "cust-1"),
            ("name", "Alicia"),
            ("email", "alicia@example.com"),
            ("tenant_id", "tenant-a"),
        ])],
        conflict: ConflictStrategy::update(vec!["name".to_string(), "email".to_string()]),
        return_fields: Vec::new(),
    };
    compile_sql(backend, manifest, CompileOperation::Write(&write))
}

pub(super) fn compile_delete_sql(
    backend: BackendKind,
    manifest: &CatalogManifest,
) -> (String, Vec<LogicalValue>) {
    let delete = LogicalDelete {
        message_type: MESSAGE.to_string(),
        filter: LogicalFilter::Comparison {
            field: "id".to_string(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String("cust-2".to_string()),
        },
        return_fields: Vec::new(),
    };
    compile_sql(backend, manifest, CompileOperation::Delete(&delete))
}

pub(super) fn compile_aggregate_sql(
    backend: BackendKind,
    manifest: &CatalogManifest,
) -> (String, Vec<LogicalValue>) {
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
    compile_sql(backend, manifest, CompileOperation::Aggregate(&aggregate))
}

pub(super) fn compile_search_sql(
    backend: BackendKind,
    manifest: &CatalogManifest,
) -> (String, Vec<LogicalValue>) {
    let search = LogicalSearch {
        message_type: MESSAGE.to_string(),
        vector: None,
        text_query: Some("Alice".to_string()),
        filter: Some(LogicalFilter::Comparison {
            field: "tenant_id".to_string(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String("tenant-a".to_string()),
        }),
        top_k: 10,
        score_threshold: None,
        require_hybrid: false,
        with_vector: false,
        with_payload: true,
    };
    compile_sql(backend, manifest, CompileOperation::Search(&search))
}

pub(super) fn compile_ensure_name_index_sql(
    backend: BackendKind,
    manifest: &CatalogManifest,
) -> (String, Vec<LogicalValue>) {
    let schema = manifest
        .tables
        .first()
        .map(|table| table.schema.as_str())
        .unwrap_or("public");
    let op = LogicalResourceOp {
        op: ResourceOpKind::Ensure,
        resource_kind: ResourceKind::Index,
        resource_name: "idx_customers_name".to_string(),
        spec: Some(serde_json::json!({
            "schema": schema,
            "table": "customers",
            "columns": ["name"],
            "unique": false,
        })),
    };
    compile_sql(backend, manifest, CompileOperation::ResourceOp(&op))
}

pub(super) fn compile_list_customer_indexes_sql(
    backend: BackendKind,
    manifest: &CatalogManifest,
) -> (String, Vec<LogicalValue>) {
    let schema = manifest
        .tables
        .first()
        .map(|table| table.schema.as_str())
        .unwrap_or("public");
    let op = LogicalResourceOp {
        op: ResourceOpKind::List,
        resource_kind: ResourceKind::Index,
        resource_name: String::new(),
        spec: Some(serde_json::json!({
            "schema": schema,
            "table": "customers",
        })),
    };
    compile_sql(backend, manifest, CompileOperation::ResourceOp(&op))
}

fn record<const N: usize>(fields: [(&str, &str); N]) -> LogicalRecord {
    let mut record = BTreeMap::new();
    for (field, value) in fields {
        record.insert(field.to_string(), LogicalValue::String(value.to_string()));
    }
    record
}

fn compile_sql(
    backend: BackendKind,
    manifest: &CatalogManifest,
    operation: CompileOperation<'_>,
) -> (String, Vec<LogicalValue>) {
    let ctx = CompileContext::new(manifest).with_tenant("tenant-a");
    match compile_for_backend(&backend, operation, &ctx)
        .expect("compiler is registered for backend")
        .expect("operation compiles")
    {
        CompiledRendering::Sql {
            backend: actual,
            statement,
            params,
        } => {
            assert_eq!(actual, backend);
            (statement, params)
        }
        other => panic!("expected SQL rendering for {backend:?}, got {other:?}"),
    }
}

pub(super) fn unsupported_bind(value: &LogicalValue) -> ! {
    panic!(
        "live SQL golden only binds strings today, got {}",
        value.type_token()
    )
}
