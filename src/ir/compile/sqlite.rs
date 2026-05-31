//! SQLite compiler (parity-matrix completion).
//!
//! Lowers `LogicalRead`/`Write`/`Delete`/`Aggregate`/`Search`/`ResourceOp`
//! to SQLite SQL. Conceptually close to Postgres with three notable
//! dialect differences:
//!
//! - **Quoting**: SQLite accepts both `"col"` and `[col]` and `` `col` ``;
//!   we use double-quotes for ANSI portability.
//! - **Placeholders**: positional `?` (anonymous) — SQLite also supports
//!   `?N` and `:name` but `?` is universally accepted by sqlx-sqlite.
//! - **Upsert**: `ON CONFLICT (cols) DO UPDATE SET col = excluded.col`
//!   — same shape as Postgres. SQLite has supported this since 3.24.
//! - **Search**: SQLite's portable search surface is FTS5 virtual
//!   tables (`MATCH ?`). We require an FTS5 virtual table to exist with
//!   the manifest name suffixed `_fts`; the compiler emits a join.
//! - **Schema**: SQLite has only `main` / `temp` / attached-db schemas.
//!   We ignore the manifest's `schema` and emit unqualified table names
//!   so a freshly attached database works without rewrites.

use std::collections::HashSet;

use crate::backend::BackendKind;
use crate::generation::ManifestTable;
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    AggregateExpr, AggregateFunc, ConflictStrategy, LogicalAggregate, LogicalDelete, LogicalRead,
    LogicalResourceOp, LogicalSearch, LogicalWrite, ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;

use super::{CompileContext, CompileError, CompiledRendering, Compiler};

/// SQLite SQL compiler (3.24+, FTS5-aware).
#[derive(Debug, Default, Clone, Copy)]
pub struct SqliteCompiler;

impl SqliteCompiler {
    fn resolve_table<'a>(
        &self,
        message_type: &str,
        ctx: &'a CompileContext<'_>,
    ) -> Result<&'a ManifestTable, CompileError> {
        crate::broker::table_for_message(ctx.manifest, message_type).ok_or_else(|| {
            CompileError::UnknownMessageType {
                message_type: message_type.to_string(),
            }
        })
    }

    fn column_for<'a>(
        &self,
        table: &'a ManifestTable,
        field: &str,
        message_type: &str,
    ) -> Result<&'a str, CompileError> {
        table
            .columns
            .iter()
            .find(|c| c.field_name.eq_ignore_ascii_case(field) || c.column_name == field)
            .map(|c| c.column_name.as_str())
            .ok_or_else(|| CompileError::UnknownField {
                message_type: message_type.to_string(),
                field: field.to_string(),
            })
    }

    fn render_where(
        &self,
        filter: &LogicalFilter,
        table: &ManifestTable,
        message_type: &str,
        params: &mut Vec<LogicalValue>,
    ) -> Result<Option<String>, CompileError> {
        match filter {
            LogicalFilter::And(c) if c.is_empty() => Ok(None),
            LogicalFilter::Or(c) if c.is_empty() => Ok(Some("0".to_string())),
            _ => Ok(Some(self.render_filter(
                filter,
                table,
                message_type,
                params,
            )?)),
        }
    }

    fn render_filter(
        &self,
        filter: &LogicalFilter,
        table: &ManifestTable,
        message_type: &str,
        params: &mut Vec<LogicalValue>,
    ) -> Result<String, CompileError> {
        match filter {
            LogicalFilter::And(clauses) => {
                let parts = clauses
                    .iter()
                    .map(|c| self.render_filter(c, table, message_type, params))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(format!("({})", parts.join(" AND ")))
            }
            LogicalFilter::Or(clauses) => {
                let parts = clauses
                    .iter()
                    .map(|c| self.render_filter(c, table, message_type, params))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(format!("({})", parts.join(" OR ")))
            }
            LogicalFilter::Not(inner) => {
                let r = self.render_filter(inner, table, message_type, params)?;
                Ok(format!("(NOT {r})"))
            }
            LogicalFilter::Comparison { field, op, value } => {
                let column = self.column_for(table, field, message_type)?;
                if value.is_null() {
                    return Err(CompileError::Malformed {
                        reason: format!(
                            "comparison with NULL on field '{field}' must use IsNull, not {}",
                            op.token()
                        ),
                    });
                }
                params.push(value.clone());
                let sql_op = sql_op_for(*op);
                let rhs = wrap_value_for_op(*op);
                // SQLite's LIKE is case-insensitive ASCII by default; for
                // case-sensitive needs the pragma `case_sensitive_like=ON`
                // must be set. We don't pretend to a different default
                // — `ILIKE` lowers to plain `LIKE` and the operator
                // controls the pragma.
                Ok(format!("\"{column}\" {sql_op} {rhs}"))
            }
            LogicalFilter::IsNull(field) => {
                let column = self.column_for(table, field, message_type)?;
                Ok(format!("\"{column}\" IS NULL"))
            }
            LogicalFilter::InList { field, values } => {
                if values.is_empty() {
                    return Ok("0".to_string());
                }
                let column = self.column_for(table, field, message_type)?;
                let placeholders = values
                    .iter()
                    .map(|v| {
                        params.push(v.clone());
                        "?".to_string()
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("\"{column}\" IN ({placeholders})"))
            }
        }
    }
}

impl Compiler for SqliteCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::Sqlite
    }

    fn compile_read(
        &self,
        op: &LogicalRead,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        let select = match &op.projection {
            Some(p) if !p.is_select_all() => p
                .fields
                .iter()
                .map(|f| {
                    let col = self.column_for(table, f, &op.message_type)?;
                    Ok(format!("\"{col}\""))
                })
                .collect::<Result<Vec<_>, CompileError>>()?
                .join(", "),
            _ => "*".to_string(),
        };

        // SQLite: unqualified table name — see module doc.
        let mut sql = format!("SELECT {select} FROM \"{table}\"", table = table.table);

        if let Some(filter) = &op.filter
            && let Some(body) = self.render_where(filter, table, &op.message_type, &mut params)?
        {
            sql.push_str(&format!(" WHERE {body}"));
        }

        if !op.sort.is_empty() {
            let parts = op
                .sort
                .iter()
                .map(|s| {
                    let col = self.column_for(table, &s.field, &op.message_type)?;
                    let direction = s.direction.token().to_uppercase();
                    let nulls = match s.nulls {
                        crate::ir::projection::NullOrder::First => " NULLS FIRST",
                        crate::ir::projection::NullOrder::Last => " NULLS LAST",
                        crate::ir::projection::NullOrder::Default => "",
                    };
                    Ok(format!("\"{col}\" {direction}{nulls}"))
                })
                .collect::<Result<Vec<_>, CompileError>>()?;
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Sqlite,
                    op: "keyset_cursor",
                });
            }
            if let Some(limit) = pag.limit {
                sql.push_str(&format!(" LIMIT {limit}"));
            }
            if let Some(offset) = pag.offset
                && offset > 0
            {
                sql.push_str(&format!(" OFFSET {offset}"));
            }
        }

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Sqlite,
            statement: sql,
            params,
        })
    }

    fn compile_write(
        &self,
        op: &LogicalWrite,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        if op.records.is_empty() {
            return Err(CompileError::Malformed {
                reason: "LogicalWrite::records must be non-empty".into(),
            });
        }
        let table = self.resolve_table(&op.message_type, ctx)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        let first = &op.records[0];
        let columns: Vec<&str> = first
            .keys()
            .map(|k| self.column_for(table, k, &op.message_type))
            .collect::<Result<Vec<_>, _>>()?;
        let column_list = columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");

        let mut value_rows = Vec::with_capacity(op.records.len());
        for (idx, record) in op.records.iter().enumerate() {
            if record.len() != first.len() || !first.keys().all(|k| record.contains_key(k)) {
                return Err(CompileError::Malformed {
                    reason: format!(
                        "record {idx} has different field set than record 0; \
                         all records in one LogicalWrite must share the same fields"
                    ),
                });
            }
            let row = first
                .keys()
                .map(|k| {
                    params.push(record[k].clone());
                    "?".to_string()
                })
                .collect::<Vec<_>>()
                .join(", ");
            value_rows.push(format!("({row})"));
        }

        let mut sql = format!(
            "INSERT INTO \"{table}\" ({column_list}) VALUES {values}",
            table = table.table,
            values = value_rows.join(", "),
        );

        match &op.conflict {
            ConflictStrategy::Error => {}
            ConflictStrategy::Ignore => sql.push_str(" ON CONFLICT DO NOTHING"),
            ConflictStrategy::Replace | ConflictStrategy::Update { .. } => {
                if table.primary_key.is_empty() {
                    return Err(CompileError::Malformed {
                        reason: format!(
                            "upsert requested but message '{}' has no primary key in manifest",
                            op.message_type
                        ),
                    });
                }
                let pk_cols: Vec<String> = table
                    .primary_key
                    .iter()
                    .map(|f| {
                        let c = self.column_for(table, f, &op.message_type)?;
                        Ok(format!("\"{c}\""))
                    })
                    .collect::<Result<Vec<_>, CompileError>>()?;
                let target_cols: Vec<&str> = match &op.conflict {
                    ConflictStrategy::Update { fields } => fields
                        .iter()
                        .map(|f| self.column_for(table, f, &op.message_type))
                        .collect::<Result<Vec<_>, _>>()?,
                    ConflictStrategy::Replace => columns.clone(),
                    _ => unreachable!(),
                };
                let set_clause = target_cols
                    .iter()
                    .map(|c| format!("\"{c}\" = excluded.\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                sql.push_str(&format!(
                    " ON CONFLICT ({}) DO UPDATE SET {set_clause}",
                    pk_cols.join(", ")
                ));
            }
        }

        if !op.return_fields.is_empty() {
            // SQLite 3.35+ supports RETURNING — emit it. The pool we
            // build defaults to bundled sqlx-sqlite which ships 3.40+.
            let cols = op
                .return_fields
                .iter()
                .map(|f| {
                    let c = self.column_for(table, f, &op.message_type)?;
                    Ok(format!("\"{c}\""))
                })
                .collect::<Result<Vec<_>, CompileError>>()?;
            sql.push_str(&format!(" RETURNING {}", cols.join(", ")));
        }

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Sqlite,
            statement: sql,
            params,
        })
    }

    fn compile_delete(
        &self,
        op: &LogicalDelete,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        let body = self
            .render_where(&op.filter, table, &op.message_type, &mut params)?
            .ok_or_else(|| CompileError::Malformed {
                reason: "LogicalDelete::filter cannot be empty; use Drop resource to truncate"
                    .into(),
            })?;
        if body == "0" {
            return Err(CompileError::Malformed {
                reason: "LogicalDelete::filter resolves to FALSE; refusing no-op delete".into(),
            });
        }
        let mut sql = format!("DELETE FROM \"{table}\" WHERE {body}", table = table.table,);
        if !op.return_fields.is_empty() {
            let cols = op
                .return_fields
                .iter()
                .map(|f| {
                    let c = self.column_for(table, f, &op.message_type)?;
                    Ok(format!("\"{c}\""))
                })
                .collect::<Result<Vec<_>, CompileError>>()?;
            sql.push_str(&format!(" RETURNING {}", cols.join(", ")));
        }
        Ok(CompiledRendering::Sql {
            backend: BackendKind::Sqlite,
            statement: sql,
            params,
        })
    }

    fn compile_aggregate(
        &self,
        op: &LogicalAggregate,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        if op.aggregates.is_empty() {
            return Err(CompileError::Malformed {
                reason: "LogicalAggregate::aggregates must be non-empty".into(),
            });
        }
        let mut seen: HashSet<&str> = HashSet::new();
        for agg in &op.aggregates {
            if !seen.insert(agg.alias.as_str()) {
                return Err(CompileError::Malformed {
                    reason: format!("duplicate aggregate alias '{}'", agg.alias),
                });
            }
        }
        let table = self.resolve_table(&op.message_type, ctx)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        let mut select_parts: Vec<String> = Vec::new();
        let group_columns: Vec<String> = op
            .group_by
            .iter()
            .map(|f| {
                let col = self.column_for(table, f, &op.message_type)?;
                Ok::<_, CompileError>(format!("\"{col}\""))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for col in &group_columns {
            select_parts.push(col.clone());
        }
        for agg in &op.aggregates {
            select_parts.push(render_aggregate(agg, table, &op.message_type)?);
        }
        let mut sql = format!(
            "SELECT {sel} FROM \"{table}\"",
            sel = select_parts.join(", "),
            table = table.table,
        );

        if let Some(filter) = &op.filter
            && let Some(body) = self.render_where(filter, table, &op.message_type, &mut params)?
        {
            sql.push_str(&format!(" WHERE {body}"));
        }

        if !group_columns.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", group_columns.join(", ")));
        }

        if let Some(having) = &op.having {
            let body = render_having(having, op, &mut params)?;
            sql.push_str(&format!(" HAVING {body}"));
        }

        if !op.sort.is_empty() {
            let parts = op
                .sort
                .iter()
                .map(|s| {
                    let token = resolve_sort_field(&s.field, op, table)?;
                    let direction = s.direction.token().to_uppercase();
                    Ok::<_, CompileError>(format!("{token} {direction}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Sqlite,
                    op: "keyset_cursor",
                });
            }
            if let Some(limit) = pag.limit {
                sql.push_str(&format!(" LIMIT {limit}"));
            }
            if let Some(offset) = pag.offset
                && offset > 0
            {
                sql.push_str(&format!(" OFFSET {offset}"));
            }
        }

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Sqlite,
            statement: sql,
            params,
        })
    }

    fn compile_search(
        &self,
        op: &LogicalSearch,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        // SQLite's portable text-search surface is FTS5. Convention:
        // a virtual table named `<table>_fts` exists with the same row
        // identity (rowid linkage to the base table). The compiler
        // emits a join so callers get back rows from the base table
        // ranked by `bm25(<table>_fts)`.
        let text = op
            .text_query
            .as_deref()
            .ok_or_else(|| CompileError::OperatorUnsupported {
                backend: BackendKind::Sqlite,
                op: "vector_search",
            })?;
        if text.trim().is_empty() {
            return Err(CompileError::Malformed {
                reason: "text_query must be non-empty for SQLite FTS5 search".into(),
            });
        }
        let table = self.resolve_table(&op.message_type, ctx)?;
        let mut params: Vec<LogicalValue> = Vec::new();
        params.push(LogicalValue::String(text.to_string()));

        let mut sql = format!(
            "SELECT t.*, bm25(\"{table}_fts\") AS _score \
             FROM \"{table}\" t \
             JOIN \"{table}_fts\" ON t.rowid = \"{table}_fts\".rowid \
             WHERE \"{table}_fts\" MATCH ?",
            table = table.table,
        );

        if let Some(filter) = &op.filter
            && let Some(body) = self.render_where(filter, table, &op.message_type, &mut params)?
        {
            // The base-table columns are quoted via `t.<col>` for the
            // join — re-render with a base alias.
            sql.push_str(&format!(" AND {}", body.replace("\"", "t.\"")));
        }

        if let Some(threshold) = op.score_threshold {
            // Higher bm25 = worse match in SQLite (lower is better).
            // Surface threshold semantically: callers pass "minimum
            // relevance" which we interpret as max bm25 score.
            params.push(LogicalValue::Float(threshold as f64));
            sql.push_str(" AND bm25(\"{table}_fts\") <= ?");
        }

        sql.push_str(&format!(
            " ORDER BY bm25(\"{table}_fts\") ASC LIMIT {top_k}",
            table = table.table,
            top_k = op.top_k,
        ));

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Sqlite,
            statement: sql,
            params,
        })
    }

    fn compile_resource_op(
        &self,
        op: &LogicalResourceOp,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        if !matches!(op.resource_kind, ResourceKind::Table | ResourceKind::Index) {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Sqlite,
                op: "non_table_resource",
            });
        }
        let sql = match (op.op, op.resource_kind) {
            (ResourceOpKind::Ensure, ResourceKind::Table) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "Ensure Table requires a spec with column definitions".into(),
                })?;
                let cols = spec
                    .get("columns")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| CompileError::Malformed {
                        reason: "Ensure Table spec.columns must be an array".into(),
                    })?;
                let mut column_defs = Vec::with_capacity(cols.len());
                let mut pk_cols: Vec<String> = Vec::new();
                for c in cols {
                    let name = c.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
                        CompileError::Malformed {
                            reason: "column missing 'name'".into(),
                        }
                    })?;
                    let ty = c.get("type").and_then(|v| v.as_str()).ok_or_else(|| {
                        CompileError::Malformed {
                            reason: "column missing 'type'".into(),
                        }
                    })?;
                    let not_null = c.get("not_null").and_then(|v| v.as_bool()).unwrap_or(false);
                    let pk = c
                        .get("primary_key")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    if pk {
                        pk_cols.push(format!("\"{name}\""));
                    }
                    let null_clause = if not_null { " NOT NULL" } else { "" };
                    column_defs.push(format!("\"{name}\" {ty}{null_clause}"));
                }
                if !pk_cols.is_empty() {
                    column_defs.push(format!("PRIMARY KEY ({})", pk_cols.join(", ")));
                }
                format!(
                    "CREATE TABLE IF NOT EXISTS \"{}\" ({})",
                    op.resource_name,
                    column_defs.join(", ")
                )
            }
            (ResourceOpKind::Drop, ResourceKind::Table) => {
                format!("DROP TABLE IF EXISTS \"{}\"", op.resource_name)
            }
            (ResourceOpKind::List, ResourceKind::Table) => {
                // SQLite catalog: sqlite_master.
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name".to_string()
            }
            (ResourceOpKind::Ensure, ResourceKind::Index) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "Ensure Index requires a spec".into(),
                })?;
                let tbl = spec.get("table").and_then(|v| v.as_str()).ok_or_else(|| {
                    CompileError::Malformed {
                        reason: "Ensure Index spec missing 'table'".into(),
                    }
                })?;
                let cols = spec
                    .get("columns")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| CompileError::Malformed {
                        reason: "Ensure Index spec.columns must be an array".into(),
                    })?;
                let col_list = cols
                    .iter()
                    .filter_map(|c| c.as_str())
                    .map(|c| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                let unique = spec
                    .get("unique")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let kind = if unique { "UNIQUE INDEX" } else { "INDEX" };
                format!(
                    "CREATE {kind} IF NOT EXISTS \"{name}\" ON \"{tbl}\" ({col_list})",
                    name = op.resource_name,
                )
            }
            (ResourceOpKind::Drop, ResourceKind::Index) => {
                format!("DROP INDEX IF EXISTS \"{}\"", op.resource_name)
            }
            (ResourceOpKind::List, ResourceKind::Index) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "List Index requires a spec with 'table'".into(),
                })?;
                let tbl = spec.get("table").and_then(|v| v.as_str()).ok_or_else(|| {
                    CompileError::Malformed {
                        reason: "List Index spec missing 'table'".into(),
                    }
                })?;
                format!(
                    "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = '{tbl}' ORDER BY name"
                )
            }
            _ => {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Sqlite,
                    op: "unhandled_resource_op",
                });
            }
        };
        Ok(CompiledRendering::Sql {
            backend: BackendKind::Sqlite,
            statement: sql,
            params: Vec::new(),
        })
    }
}

fn sql_op_for(op: ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::Eq => "=",
        ComparisonOp::Ne => "<>",
        ComparisonOp::Lt => "<",
        ComparisonOp::Le => "<=",
        ComparisonOp::Gt => ">",
        ComparisonOp::Ge => ">=",
        ComparisonOp::Like
        | ComparisonOp::Contains
        | ComparisonOp::StartsWith
        | ComparisonOp::EndsWith
        | ComparisonOp::ILike => "LIKE",
    }
}

fn wrap_value_for_op(op: ComparisonOp) -> String {
    match op {
        ComparisonOp::Contains => "('%' || ? || '%')".to_string(),
        ComparisonOp::StartsWith => "(? || '%')".to_string(),
        ComparisonOp::EndsWith => "('%' || ?)".to_string(),
        _ => "?".to_string(),
    }
}

fn render_aggregate(
    agg: &AggregateExpr,
    table: &ManifestTable,
    message_type: &str,
) -> Result<String, CompileError> {
    let token = agg.func.sql_token();
    let body = match agg.func {
        AggregateFunc::Count if agg.field == "*" => "*".to_string(),
        AggregateFunc::Count => {
            let col = resolve_column(table, &agg.field, message_type)?;
            format!("\"{col}\"")
        }
        AggregateFunc::CountDistinct => {
            if agg.field == "*" {
                return Err(CompileError::Malformed {
                    reason: "COUNT(DISTINCT *) is not allowed; specify a field".into(),
                });
            }
            let col = resolve_column(table, &agg.field, message_type)?;
            format!("DISTINCT \"{col}\"")
        }
        AggregateFunc::Sum | AggregateFunc::Avg | AggregateFunc::Min | AggregateFunc::Max => {
            if agg.field == "*" {
                return Err(CompileError::Malformed {
                    reason: format!("{} requires a field name, not '*'", agg.func.sql_token()),
                });
            }
            let col = resolve_column(table, &agg.field, message_type)?;
            format!("\"{col}\"")
        }
    };
    Ok(format!("{token}({body}) AS \"{}\"", agg.alias))
}

fn resolve_sort_field(
    field: &str,
    op: &LogicalAggregate,
    table: &ManifestTable,
) -> Result<String, CompileError> {
    if op.aggregates.iter().any(|a| a.alias == field) {
        return Ok(format!("\"{field}\""));
    }
    if op.group_by.iter().any(|f| f == field) {
        let col = resolve_column(table, field, &op.message_type)?;
        return Ok(format!("\"{col}\""));
    }
    Err(CompileError::Malformed {
        reason: format!(
            "ORDER BY field '{field}' is neither an aggregate alias nor a GROUP BY column"
        ),
    })
}

fn render_having(
    filter: &LogicalFilter,
    op: &LogicalAggregate,
    params: &mut Vec<LogicalValue>,
) -> Result<String, CompileError> {
    match filter {
        LogicalFilter::And(c) if c.is_empty() => Ok("1".to_string()),
        LogicalFilter::Or(c) if c.is_empty() => Ok("0".to_string()),
        LogicalFilter::And(clauses) => {
            let parts = clauses
                .iter()
                .map(|c| render_having(c, op, params))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", parts.join(" AND ")))
        }
        LogicalFilter::Or(clauses) => {
            let parts = clauses
                .iter()
                .map(|c| render_having(c, op, params))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", parts.join(" OR ")))
        }
        LogicalFilter::Not(inner) => {
            let r = render_having(inner, op, params)?;
            Ok(format!("(NOT {r})"))
        }
        LogicalFilter::Comparison {
            field,
            op: cmp,
            value,
        } => {
            if value.is_null() {
                return Err(CompileError::Malformed {
                    reason: format!(
                        "HAVING comparison with NULL on '{field}' must use IsNull, not {}",
                        cmp.token()
                    ),
                });
            }
            let token = resolve_having_field(field, op)?;
            params.push(value.clone());
            let sql_op = sql_op_for(*cmp);
            let rhs = wrap_value_for_op(*cmp);
            Ok(format!("{token} {sql_op} {rhs}"))
        }
        LogicalFilter::IsNull(field) => {
            let token = resolve_having_field(field, op)?;
            Ok(format!("{token} IS NULL"))
        }
        LogicalFilter::InList { field, values } => {
            if values.is_empty() {
                return Ok("0".to_string());
            }
            let token = resolve_having_field(field, op)?;
            let placeholders = values
                .iter()
                .map(|v| {
                    params.push(v.clone());
                    "?".to_string()
                })
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!("{token} IN ({placeholders})"))
        }
    }
}

fn resolve_having_field(field: &str, op: &LogicalAggregate) -> Result<String, CompileError> {
    if op.aggregates.iter().any(|a| a.alias == field) {
        return Ok(format!("\"{field}\""));
    }
    if op.group_by.iter().any(|f| f == field) {
        return Ok(format!("\"{field}\""));
    }
    Err(CompileError::Malformed {
        reason: format!(
            "HAVING field '{field}' is neither an aggregate alias nor a GROUP BY column"
        ),
    })
}

fn resolve_column<'a>(
    table: &'a ManifestTable,
    field: &str,
    message_type: &str,
) -> Result<&'a str, CompileError> {
    table
        .columns
        .iter()
        .find(|c| c.field_name.eq_ignore_ascii_case(field) || c.column_name == field)
        .map(|c| c.column_name.as_str())
        .ok_or_else(|| CompileError::UnknownField {
            message_type: message_type.to_string(),
            field: field.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
    use crate::ir::filter::{ComparisonOp, LogicalFilter};
    use crate::ir::operations::{
        AggregateExpr, AggregateFunc, ConflictStrategy, LogicalAggregate, LogicalRead,
        LogicalRecord, LogicalSearch, LogicalWrite,
    };
    use crate::ir::projection::{LogicalPagination, LogicalSort, SortDirection};
    use crate::ir::value::LogicalValue;
    use serde_json::json;

    fn fixture() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.notes.v1.Note".into(),
            schema: "main".into(),
            table: "notes".into(),
            primary_key: vec!["id".into()],
            columns: vec![
                ManifestColumn {
                    field_name: "id".into(),
                    column_name: "id".into(),
                    proto_type: "string".into(),
                    sql_type: "TEXT".into(),
                    is_primary: true,
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "title".into(),
                    column_name: "title".into(),
                    proto_type: "string".into(),
                    sql_type: "TEXT".into(),
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "score".into(),
                    column_name: "score".into(),
                    proto_type: "int64".into(),
                    sql_type: "INTEGER".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        CatalogManifest {
            tables: vec![table],
            ..Default::default()
        }
    }

    fn sql(rendering: CompiledRendering) -> (String, Vec<LogicalValue>) {
        match rendering {
            CompiledRendering::Sql {
                statement, params, ..
            } => (statement, params),
            other => panic!("expected Sql, got {other:?}"),
        }
    }

    #[test]
    fn select_uses_double_quotes_and_question_marks() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("acme.notes.v1.Note")
            .with_filter(LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("abc".into()),
            })
            .with_pagination(LogicalPagination::limit(5));
        let (statement, params) = sql(SqliteCompiler.compile_read(&read, &ctx).unwrap());
        assert_eq!(
            statement,
            "SELECT * FROM \"notes\" WHERE \"id\" = ? LIMIT 5"
        );
        assert_eq!(params, vec![LogicalValue::String("abc".into())]);
    }

    #[test]
    fn upsert_uses_on_conflict_do_update_with_excluded() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("abc".into()));
        rec.insert("title".into(), LogicalValue::String("Hello".into()));
        let write = LogicalWrite {
            message_type: "acme.notes.v1.Note".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Update {
                fields: vec!["title".into()],
            },
            return_fields: vec![],
        };
        let (statement, _) = sql(SqliteCompiler.compile_write(&write, &ctx).unwrap());
        assert!(statement.contains("INSERT INTO \"notes\""));
        assert!(
            statement.contains("ON CONFLICT (\"id\") DO UPDATE SET \"title\" = excluded.\"title\"")
        );
    }

    #[test]
    fn returning_compiles_for_sqlite_3_35_plus() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("abc".into()));
        let write = LogicalWrite {
            message_type: "acme.notes.v1.Note".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Error,
            return_fields: vec!["id".into()],
        };
        let (statement, _) = sql(SqliteCompiler.compile_write(&write, &ctx).unwrap());
        assert!(statement.ends_with("RETURNING \"id\""));
    }

    #[test]
    fn aggregate_group_by_renders() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let agg = LogicalAggregate {
            message_type: "acme.notes.v1.Note".into(),
            filter: None,
            group_by: vec!["title".into()],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Count,
                field: "*".into(),
                alias: "n".into(),
            }],
            having: None,
            sort: vec![LogicalSort {
                field: "n".into(),
                direction: SortDirection::Desc,
                nulls: crate::ir::projection::NullOrder::Default,
            }],
            pagination: Some(LogicalPagination::limit(10)),
        };
        let (statement, _) = sql(SqliteCompiler.compile_aggregate(&agg, &ctx).unwrap());
        assert_eq!(
            statement,
            "SELECT \"title\", COUNT(*) AS \"n\" FROM \"notes\" \
             GROUP BY \"title\" ORDER BY \"n\" DESC LIMIT 10"
        );
    }

    #[test]
    fn search_emits_fts5_match() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let search = LogicalSearch {
            message_type: "acme.notes.v1.Note".into(),
            vector: None,
            text_query: Some("hello world".into()),
            filter: None,
            top_k: 10,
            score_threshold: None,
            require_hybrid: false,
            with_vector: false,
            with_payload: true,
        };
        let (statement, params) = sql(SqliteCompiler.compile_search(&search, &ctx).unwrap());
        assert!(statement.contains("FROM \"notes\" t"));
        assert!(statement.contains("JOIN \"notes_fts\" ON t.rowid = \"notes_fts\".rowid"));
        assert!(statement.contains("\"notes_fts\" MATCH ?"));
        assert!(statement.ends_with("ORDER BY bm25(\"notes_fts\") ASC LIMIT 10"));
        assert_eq!(params, vec![LogicalValue::String("hello world".into())]);
    }

    #[test]
    fn search_vector_only_rejects() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let search = LogicalSearch {
            message_type: "acme.notes.v1.Note".into(),
            vector: Some(vec![0.1]),
            text_query: None,
            filter: None,
            top_k: 5,
            score_threshold: None,
            require_hybrid: false,
            with_vector: false,
            with_payload: true,
        };
        let err = SqliteCompiler.compile_search(&search, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Sqlite,
                op: "vector_search"
            }
        ));
    }

    #[test]
    fn resource_op_create_table() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let op = LogicalResourceOp {
            op: ResourceOpKind::Ensure,
            resource_kind: ResourceKind::Table,
            resource_name: "items".into(),
            spec: Some(json!({
                "columns": [
                    {"name": "id", "type": "INTEGER", "not_null": true, "primary_key": true},
                    {"name": "label", "type": "TEXT"}
                ]
            })),
        };
        let (statement, _) = sql(SqliteCompiler.compile_resource_op(&op, &ctx).unwrap());
        assert_eq!(
            statement,
            "CREATE TABLE IF NOT EXISTS \"items\" (\"id\" INTEGER NOT NULL, \"label\" TEXT, PRIMARY KEY (\"id\"))"
        );
    }

    #[test]
    fn resource_op_list_tables() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let op = LogicalResourceOp {
            op: ResourceOpKind::List,
            resource_kind: ResourceKind::Table,
            resource_name: String::new(),
            spec: None,
        };
        let (statement, _) = sql(SqliteCompiler.compile_resource_op(&op, &ctx).unwrap());
        assert!(statement.contains("sqlite_master"));
        assert!(statement.contains("type = 'table'"));
    }
}
