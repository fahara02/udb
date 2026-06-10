//! Postgres compiler (U2 step 3).
//!
//! Lowers `LogicalRead`/`Write`/`Delete` to bind-safe SQL. No string
//! interpolation of values — every operand becomes `$N` and is pushed into
//! the `params` vector so the executor binds via `sqlx::query`/`bind`. The
//! same compiler runs against any Postgres-flavoured backend (Postgres,
//! CockroachDB, AlloyDB, Aurora) — ClickHouse uses a separate compiler
//! because its INSERT/UPSERT shapes diverge.
//!
//! Manifest-driven field resolution: the IR's field names are proto field
//! names; the compiler maps them to `ManifestColumn.column_name` via
//! `ManifestTable.columns`. Unknown fields surface `CompileError::UnknownField`
//! before any SQL is emitted.

use crate::backend::BackendKind;
use crate::ir::filter::ComparisonOp;
use crate::ir::operations::{
    ConflictStrategy, LogicalAggregate, LogicalDelete, LogicalRead, LogicalResourceOp,
    LogicalSearch, LogicalWrite, ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;

use super::sql_dialect::{SqlCompiler, SqlDialect};
use super::{CompileContext, CompileError, CompiledRendering, Compiler};

/// Postgres dialect marker for the generic [`SqlCompiler`]. Captures the
/// only axes that differ from the other relational backends: `"x"` quoting,
/// `$N` placeholders, `FALSE` false-literal, and the `|| … ESCAPE '\'`
/// LIKE-concat idiom.
struct Postgres;

impl SqlDialect for Postgres {
    fn quote(ident: &str) -> String {
        format!("\"{ident}\"")
    }
    fn placeholder(index: usize) -> String {
        format!("${index}")
    }
    fn false_literal() -> &'static str {
        "FALSE"
    }
    fn having_true_literal() -> &'static str {
        "TRUE"
    }
    fn having_false_literal() -> &'static str {
        "FALSE"
    }
    /// Some operators want the value transformed before binding (`Contains` →
    /// `'%' || $N || '%'`). Returns the rendered RHS using `placeholder`.
    ///
    /// For substring matches the bound user value has its LIKE metacharacters
    /// (`\`, `%`, `_`) escaped in-SQL via `replace()` and the predicate is
    /// closed with `ESCAPE '\'`, so a value like `50%` matches the literal text
    /// rather than over-matching every row. (Backslashes are doubled first,
    /// then `%`/`_` prefixed, relying on `standard_conforming_strings=on`.)
    fn wrap_value_for_op(op: ComparisonOp, placeholder: &str) -> String {
        let escaped = format!(
            "replace(replace(replace({placeholder}, '\\', '\\\\'), '%', '\\%'), '_', '\\_')"
        );
        match op {
            ComparisonOp::Contains => format!("'%' || {escaped} || '%' ESCAPE '\\'"),
            ComparisonOp::StartsWith => format!("{escaped} || '%' ESCAPE '\\'"),
            ComparisonOp::EndsWith => format!("'%' || {escaped} ESCAPE '\\'"),
            _ => placeholder.to_string(),
        }
    }
}

type Pg = SqlCompiler<Postgres>;

/// Postgres / postgres-compatible SQL compiler.
#[derive(Debug, Default, Clone, Copy)]
pub struct PostgresCompiler;

impl Compiler for PostgresCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::Postgres
    }

    fn compile_read(
        &self,
        op: &LogicalRead,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = Pg::resolve_table(&op.message_type, ctx.manifest)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        // Projection.
        let select = match &op.projection {
            Some(p) if !p.is_select_all() => p
                .fields
                .iter()
                .map(|f| {
                    let col = Pg::column_for(table, f, &op.message_type)?;
                    Ok(format!("\"{col}\""))
                })
                .collect::<Result<Vec<_>, CompileError>>()?
                .join(", "),
            _ => "*".to_string(),
        };

        let mut sql = format!(
            "SELECT {select} FROM \"{schema}\".\"{table}\"",
            schema = table.schema,
            table = table.table,
        );

        // WHERE.
        if let Some(filter) = &op.filter
            && let Some(body) = Pg::render_where(filter, table, &op.message_type, &mut params)?
        {
            sql.push_str(&format!(" WHERE {body}"));
        }

        // ORDER BY.
        if !op.sort.is_empty() {
            let parts = op
                .sort
                .iter()
                .map(|s| {
                    let col = Pg::column_for(table, &s.field, &op.message_type)?;
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

        // LIMIT / OFFSET.
        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Postgres,
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
            backend: BackendKind::Postgres,
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
        let table = Pg::resolve_table(&op.message_type, ctx.manifest)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        // Use the first record's keys as the column set — every record must
        // match (we don't sparse-insert across the multi-row VALUES list).
        let first = &op.records[0];
        let columns: Vec<&str> = first
            .keys()
            .map(|k| Pg::column_for(table, k, &op.message_type))
            .collect::<Result<Vec<_>, _>>()?;
        let column_list = columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");

        // Bind every record's values in column order, writing the VALUES list in a
        // single pass into one preallocated String — no intermediate per-row
        // `Vec<String>` and no per-row `format!` allocation (#106).
        let mut value_rows = String::with_capacity(op.records.len() * columns.len() * 6);
        for (idx, record) in op.records.iter().enumerate() {
            if record.len() != first.len() || !first.keys().all(|k| record.contains_key(k)) {
                return Err(CompileError::Malformed {
                    reason: format!(
                        "record {idx} has different field set than record 0; \
                         all records in one LogicalWrite must share the same fields"
                    ),
                });
            }
            if idx > 0 {
                value_rows.push_str(", ");
            }
            value_rows.push('(');
            for (col_idx, k) in first.keys().enumerate() {
                if col_idx > 0 {
                    value_rows.push_str(", ");
                }
                value_rows.push_str(&Pg::push_param(&mut params, record[k].clone()));
            }
            value_rows.push(')');
        }

        let mut sql = format!(
            "INSERT INTO \"{schema}\".\"{table}\" ({column_list}) VALUES {values}",
            schema = table.schema,
            table = table.table,
            values = value_rows,
        );

        // ON CONFLICT.
        match &op.conflict {
            ConflictStrategy::Error => { /* default — no clause */ }
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
                        let c = Pg::column_for(table, f, &op.message_type)?;
                        Ok(format!("\"{c}\""))
                    })
                    .collect::<Result<Vec<_>, CompileError>>()?;

                let target_cols: Vec<&str> = match &op.conflict {
                    ConflictStrategy::Update { fields } => fields
                        .iter()
                        .map(|f| Pg::column_for(table, f, &op.message_type))
                        .collect::<Result<Vec<_>, _>>()?,
                    ConflictStrategy::Replace => columns.clone(),
                    _ => unreachable!(),
                };
                let set_clause = target_cols
                    .iter()
                    .map(|c| format!("\"{c}\" = EXCLUDED.\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                sql.push_str(&format!(
                    " ON CONFLICT ({}) DO UPDATE SET {set_clause}",
                    pk_cols.join(", ")
                ));
            }
        }

        // RETURNING.
        if !op.return_fields.is_empty() {
            let cols = op
                .return_fields
                .iter()
                .map(|f| {
                    let c = Pg::column_for(table, f, &op.message_type)?;
                    Ok(format!("\"{c}\""))
                })
                .collect::<Result<Vec<_>, CompileError>>()?;
            sql.push_str(&format!(" RETURNING {}", cols.join(", ")));
        }

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Postgres,
            statement: sql,
            params,
        })
    }

    fn compile_delete(
        &self,
        op: &LogicalDelete,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = Pg::resolve_table(&op.message_type, ctx.manifest)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        // DELETE without a real WHERE is forbidden by the IR contract.
        let body = Pg::render_where(&op.filter, table, &op.message_type, &mut params)?.ok_or_else(
            || CompileError::Malformed {
                reason: "LogicalDelete::filter cannot be empty; use Drop resource to truncate"
                    .into(),
            },
        )?;
        if body == "FALSE" {
            // Empty Or — refuse rather than emit a guaranteed-no-op DELETE
            // that callers might mistake for a successful one.
            return Err(CompileError::Malformed {
                reason: "LogicalDelete::filter resolves to FALSE; refusing no-op delete".into(),
            });
        }
        let mut sql = format!(
            "DELETE FROM \"{schema}\".\"{table}\" WHERE {body}",
            schema = table.schema,
            table = table.table,
        );

        if !op.return_fields.is_empty() {
            let cols = op
                .return_fields
                .iter()
                .map(|f| {
                    let c = Pg::column_for(table, f, &op.message_type)?;
                    Ok(format!("\"{c}\""))
                })
                .collect::<Result<Vec<_>, CompileError>>()?;
            sql.push_str(&format!(" RETURNING {}", cols.join(", ")));
        }

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Postgres,
            statement: sql,
            params,
        })
    }

    fn compile_aggregate(
        &self,
        op: &LogicalAggregate,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        // Empty aggregates list is meaningless — we'd emit `SELECT FROM x
        // GROUP BY y` which is a Postgres parse error. Refuse early with a
        // typed Malformed instead of letting the database error out.
        if op.aggregates.is_empty() {
            return Err(CompileError::Malformed {
                reason: "LogicalAggregate::aggregates must be non-empty".into(),
            });
        }
        // Aliases must be unique so the result row has unambiguous keys.
        super::util::validate_aggregate_aliases(&op.aggregates)?;

        let table = Pg::resolve_table(&op.message_type, ctx.manifest)?;
        // #151: a GROUP BY column resolving to the same name as an aggregate
        // alias would emit two identically-keyed result columns. Reject it.
        let group_names: Vec<&str> = op
            .group_by
            .iter()
            .map(|f| Pg::column_for(table, f, &op.message_type))
            .collect::<Result<Vec<_>, _>>()?;
        super::util::validate_no_groupby_alias_collision(&group_names, &op.aggregates)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        // Render every aggregate expression. The group-by columns come
        // first in the SELECT list, then the aggregates, matching the
        // order callers read result rows in.
        let mut select_parts: Vec<String> =
            Vec::with_capacity(op.group_by.len().saturating_add(op.aggregates.len()));
        let group_columns: Vec<String> = op
            .group_by
            .iter()
            .map(|f| {
                let col = Pg::column_for(table, f, &op.message_type)?;
                Ok::<_, CompileError>(format!("\"{col}\""))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for col in &group_columns {
            // Group-by columns are emitted with their column name as the
            // result alias (e.g. `"region"`). Callers can identify them
            // because their alias matches the IR field name.
            select_parts.push(col.clone());
        }
        for agg in &op.aggregates {
            select_parts.push(Pg::render_aggregate(agg, table, &op.message_type)?);
        }
        let mut sql = format!(
            "SELECT {sel} FROM \"{schema}\".\"{table}\"",
            sel = select_parts.join(", "),
            schema = table.schema,
            table = table.table,
        );

        // WHERE — applied before grouping, so it references manifest
        // columns just like a normal read.
        if let Some(filter) = &op.filter
            && let Some(body) = Pg::render_where(filter, table, &op.message_type, &mut params)?
        {
            sql.push_str(&format!(" WHERE {body}"));
        }

        // GROUP BY.
        if !group_columns.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", group_columns.join(", ")));
        }

        // HAVING — applied after grouping, so its field references resolve
        // to aggregate aliases / group-by columns, not raw manifest fields.
        // Render via a small dedicated walker so we don't accidentally
        // route through `column_for` (which would reject the alias).
        if let Some(having) = &op.having {
            let body = Pg::render_having(having, op, table, &mut params)?;
            sql.push_str(&format!(" HAVING {body}"));
        }

        // ORDER BY — resolve against aggregate aliases first, then
        // group-by columns, then manifest fields. This matches SQL's own
        // resolution order and lets callers sort on a derived measure
        // without re-rendering it.
        if !op.sort.is_empty() {
            let parts = op
                .sort
                .iter()
                .map(|s| {
                    let column_token = Pg::resolve_sort_field(s.field.as_str(), op, table)?;
                    let direction = s.direction.token().to_uppercase();
                    let nulls = match s.nulls {
                        crate::ir::projection::NullOrder::First => " NULLS FIRST",
                        crate::ir::projection::NullOrder::Last => " NULLS LAST",
                        crate::ir::projection::NullOrder::Default => "",
                    };
                    Ok::<_, CompileError>(format!("{column_token} {direction}{nulls}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        // LIMIT / OFFSET — same shape as compile_read.
        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Postgres,
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
            backend: BackendKind::Postgres,
            statement: sql,
            params,
        })
    }

    fn compile_search(
        &self,
        op: &LogicalSearch,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        // Postgres surface: pgvector for dense (`<=>` cosine distance,
        // `<->` L2, `<#>` inner product), tsvector for lexical text.
        // Convention: pgvector column is `_vector`, tsvector column is
        // `_search_tsv` (matches what the migration generator emits).
        let table = Pg::resolve_table(&op.message_type, ctx.manifest)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        let has_vec_col = table
            .columns
            .iter()
            .any(|c| c.column_name == "_vector" || c.sql_type.to_lowercase().starts_with("vector"));
        let has_tsv_col = table
            .columns
            .iter()
            .any(|c| c.column_name == "_search_tsv" || c.sql_type.to_lowercase() == "tsvector");

        // Vector-only or hybrid: pgvector with cosine distance.
        if let Some(vector) = &op.vector {
            if !has_vec_col {
                return Err(CompileError::Malformed {
                    reason: format!(
                        "vector search on '{}' requires a pgvector column (e.g. '_vector')",
                        op.message_type
                    ),
                });
            }
            // Encode the literal pgvector input as a string: '[v1,v2,...]'.
            let literal = format!(
                "[{}]",
                vector
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            Pg::push_param(&mut params, LogicalValue::String(literal));
            let mut sql = format!(
                "SELECT *, (\"_vector\" <=> $1::vector) AS _score FROM \"{schema}\".\"{table}\"",
                schema = table.schema,
                table = table.table,
            );
            let mut where_parts: Vec<String> = Vec::new();
            if let Some(threshold) = op.score_threshold {
                // pgvector returns distance; lower = closer. Convert
                // threshold (caller's "min similarity") to "max distance".
                Pg::push_param(&mut params, LogicalValue::Float((1.0 - threshold) as f64));
                where_parts.push(format!("(\"_vector\" <=> $1::vector) <= ${}", params.len()));
            }
            if op.require_hybrid {
                if op.text_query.is_none() {
                    return Err(CompileError::Malformed {
                        reason: "require_hybrid set but text_query missing".into(),
                    });
                }
                if !has_tsv_col {
                    return Err(CompileError::Malformed {
                        reason: format!(
                            "hybrid search on '{}' requires a tsvector column (e.g. '_search_tsv')",
                            op.message_type
                        ),
                    });
                }
                let text = op.text_query.as_deref().unwrap();
                Pg::push_param(&mut params, LogicalValue::String(text.to_string()));
                where_parts.push(format!(
                    "\"_search_tsv\" @@ plainto_tsquery(${})",
                    params.len()
                ));
            }
            if let Some(filter) = &op.filter
                && let Some(body) = Pg::render_where(filter, table, &op.message_type, &mut params)?
            {
                where_parts.push(body);
            }
            if !where_parts.is_empty() {
                sql.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));
            }
            sql.push_str(&format!(
                " ORDER BY \"_vector\" <=> $1::vector ASC LIMIT {}",
                op.top_k
            ));
            return Ok(CompiledRendering::Sql {
                backend: BackendKind::Postgres,
                statement: sql,
                params,
            });
        }

        // Text-only path: tsvector / plainto_tsquery + ts_rank_cd.
        let text = op
            .text_query
            .as_deref()
            .ok_or_else(|| CompileError::Malformed {
                reason: "Postgres search requires either a vector or text_query".into(),
            })?;
        if text.trim().is_empty() {
            return Err(CompileError::Malformed {
                reason: "text_query must be non-empty".into(),
            });
        }
        if !has_tsv_col {
            return Err(CompileError::Malformed {
                reason: format!(
                    "text search on '{}' requires a tsvector column (e.g. '_search_tsv')",
                    op.message_type
                ),
            });
        }
        Pg::push_param(&mut params, LogicalValue::String(text.to_string()));
        let mut sql = format!(
            "SELECT *, ts_rank_cd(\"_search_tsv\", plainto_tsquery($1)) AS _score \
             FROM \"{schema}\".\"{table}\" \
             WHERE \"_search_tsv\" @@ plainto_tsquery($1)",
            schema = table.schema,
            table = table.table,
        );
        if let Some(threshold) = op.score_threshold {
            Pg::push_param(&mut params, LogicalValue::Float(threshold as f64));
            sql.push_str(&format!(
                " AND ts_rank_cd(\"_search_tsv\", plainto_tsquery($1)) >= ${}",
                params.len()
            ));
        }
        if let Some(filter) = &op.filter
            && let Some(body) = Pg::render_where(filter, table, &op.message_type, &mut params)?
        {
            sql.push_str(&format!(" AND {body}"));
        }
        sql.push_str(&format!(" ORDER BY _score DESC LIMIT {}", op.top_k));
        Ok(CompiledRendering::Sql {
            backend: BackendKind::Postgres,
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
                backend: BackendKind::Postgres,
                op: "non_table_resource",
            });
        }
        let sql = match (op.op, op.resource_kind) {
            (ResourceOpKind::Ensure, ResourceKind::Table) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "Ensure Table requires a spec with column definitions".into(),
                })?;
                let schema = spec
                    .get("schema")
                    .and_then(|v| v.as_str())
                    .unwrap_or("public");
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
                    "CREATE TABLE IF NOT EXISTS \"{schema}\".\"{name}\" ({defs})",
                    name = op.resource_name,
                    defs = column_defs.join(", "),
                )
            }
            (ResourceOpKind::Drop, ResourceKind::Table) => {
                let schema = op
                    .spec
                    .as_ref()
                    .and_then(|s| s.get("schema"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("public");
                format!("DROP TABLE IF EXISTS \"{schema}\".\"{}\"", op.resource_name)
            }
            (ResourceOpKind::List, ResourceKind::Table) => {
                let schema = op
                    .spec
                    .as_ref()
                    .and_then(|s| s.get("schema"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("public");
                format!(
                    "SELECT tablename FROM pg_tables WHERE schemaname = '{schema}' ORDER BY tablename"
                )
            }
            (ResourceOpKind::Ensure, ResourceKind::Index) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "Ensure Index requires a spec".into(),
                })?;
                let schema = spec
                    .get("schema")
                    .and_then(|v| v.as_str())
                    .unwrap_or("public");
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
                    "CREATE {kind} IF NOT EXISTS \"{name}\" ON \"{schema}\".\"{tbl}\" ({col_list})",
                    name = op.resource_name,
                )
            }
            (ResourceOpKind::Drop, ResourceKind::Index) => {
                let schema = op
                    .spec
                    .as_ref()
                    .and_then(|s| s.get("schema"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("public");
                format!("DROP INDEX IF EXISTS \"{schema}\".\"{}\"", op.resource_name)
            }
            (ResourceOpKind::List, ResourceKind::Index) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "List Index requires a spec with 'table'".into(),
                })?;
                let schema = spec
                    .get("schema")
                    .and_then(|v| v.as_str())
                    .unwrap_or("public");
                let tbl = spec.get("table").and_then(|v| v.as_str()).ok_or_else(|| {
                    CompileError::Malformed {
                        reason: "List Index spec missing 'table'".into(),
                    }
                })?;
                format!(
                    "SELECT indexname FROM pg_indexes WHERE schemaname = '{schema}' AND tablename = '{tbl}' ORDER BY indexname"
                )
            }
            _ => {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Postgres,
                    op: "unhandled_resource_op",
                });
            }
        };
        Ok(CompiledRendering::Sql {
            backend: BackendKind::Postgres,
            statement: sql,
            params: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
    use crate::ir::filter::{ComparisonOp, LogicalFilter};
    use crate::ir::operations::{
        AggregateExpr, AggregateFunc, ConflictStrategy, LogicalAggregate, LogicalDelete,
        LogicalRead, LogicalWrite,
    };
    use crate::ir::projection::{LogicalPagination, LogicalSort, SortDirection};
    use crate::ir::value::LogicalValue;

    fn fixture_manifest() -> CatalogManifest {
        // Minimal manifest with one table "customers" carrying id/name/email.
        let table = ManifestTable {
            message_name: "acme.billing.v1.Customer".to_string(),
            schema: "public".to_string(),
            table: "customers".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec![
                ManifestColumn {
                    field_name: "id".into(),
                    column_name: "id".into(),
                    proto_type: "string".into(),
                    sql_type: "uuid".into(),
                    not_null: true,
                    unique: true,
                    is_primary: true,
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "name".into(),
                    column_name: "name".into(),
                    proto_type: "string".into(),
                    sql_type: "text".into(),
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "email".into(),
                    column_name: "email".into(),
                    proto_type: "string".into(),
                    sql_type: "text".into(),
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

    fn extract_sql(rendering: CompiledRendering) -> (String, Vec<LogicalValue>) {
        match rendering {
            CompiledRendering::Sql {
                statement, params, ..
            } => (statement, params),
            other => panic!("expected Sql rendering, got {other:?}"),
        }
    }

    #[test]
    fn select_all_with_filter_and_limit() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("acme.billing.v1.Customer")
            .with_filter(LogicalFilter::Comparison {
                field: "email".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("a@b.com".into()),
            })
            .with_sort(vec![LogicalSort {
                field: "name".into(),
                direction: SortDirection::Asc,
                nulls: crate::ir::projection::NullOrder::Last,
            }])
            .with_pagination(LogicalPagination::limit(10));

        let (sql, params) =
            extract_sql(PostgresCompiler.compile_read(&read, &ctx).expect("compile"));
        assert_eq!(
            sql,
            "SELECT * FROM \"public\".\"customers\" WHERE \"email\" = $1 \
             ORDER BY \"name\" ASC NULLS LAST LIMIT 10"
        );
        assert_eq!(params, vec![LogicalValue::String("a@b.com".into())]);
    }

    #[test]
    fn select_with_in_list_emits_one_placeholder_per_value() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let read =
            LogicalRead::message("acme.billing.v1.Customer").with_filter(LogicalFilter::InList {
                field: "name".into(),
                values: vec![
                    LogicalValue::String("a".into()),
                    LogicalValue::String("b".into()),
                    LogicalValue::String("c".into()),
                ],
            });
        let (sql, params) =
            extract_sql(PostgresCompiler.compile_read(&read, &ctx).expect("compile"));
        assert!(sql.contains("WHERE \"name\" IN ($1, $2, $3)"));
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn unknown_field_is_rejected() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("acme.billing.v1.Customer").with_filter(
            LogicalFilter::Comparison {
                field: "nonexistent".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::Int(1),
            },
        );
        let err = PostgresCompiler.compile_read(&read, &ctx).unwrap_err();
        assert!(matches!(err, CompileError::UnknownField { .. }));
    }

    #[test]
    fn compare_with_null_is_rejected() {
        // `email = NULL` is the classic SQL trap; the compiler refuses it
        // and points to `IsNull` instead.
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("acme.billing.v1.Customer").with_filter(
            LogicalFilter::Comparison {
                field: "email".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::Null,
            },
        );
        let err = PostgresCompiler.compile_read(&read, &ctx).unwrap_err();
        assert!(matches!(err, CompileError::Malformed { .. }));
    }

    #[test]
    fn upsert_emits_on_conflict_update() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let mut rec = crate::ir::operations::LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("abc".into()));
        rec.insert("name".into(), LogicalValue::String("Alice".into()));
        let write = LogicalWrite {
            message_type: "acme.billing.v1.Customer".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Update {
                fields: vec!["name".into()],
            },
            return_fields: vec!["id".into()],
        };
        let (sql, params) = extract_sql(
            PostgresCompiler
                .compile_write(&write, &ctx)
                .expect("compile"),
        );
        assert!(sql.starts_with("INSERT INTO \"public\".\"customers\" (\"id\", \"name\")"));
        assert!(sql.contains("ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""));
        assert!(sql.ends_with("RETURNING \"id\""));
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn delete_without_filter_is_rejected() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let del = LogicalDelete {
            message_type: "acme.billing.v1.Customer".into(),
            filter: LogicalFilter::always(),
            return_fields: vec![],
        };
        let err = PostgresCompiler.compile_delete(&del, &ctx).unwrap_err();
        assert!(matches!(err, CompileError::Malformed { .. }));
    }

    #[test]
    fn search_without_vector_column_is_malformed() {
        // The fixture has no `_vector` pgvector column, so a vector
        // search against it must surface a typed Malformed error
        // rather than silently emit invalid SQL.
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let search = crate::ir::operations::LogicalSearch {
            message_type: "acme.billing.v1.Customer".into(),
            vector: Some(vec![0.0; 3]),
            text_query: None,
            filter: None,
            top_k: 5,
            score_threshold: None,
            require_hybrid: false,
            with_vector: false,
            with_payload: true,
        };
        let err = PostgresCompiler.compile_search(&search, &ctx).unwrap_err();
        assert!(matches!(err, CompileError::Malformed { .. }));
    }

    #[test]
    fn search_with_vector_column_emits_pgvector_query() {
        // Build a manifest that includes a `_vector` pgvector column so
        // the search lowers cleanly.
        let table = ManifestTable {
            message_name: "acme.docs.v1.Doc".into(),
            schema: "public".into(),
            table: "docs".into(),
            primary_key: vec!["id".into()],
            columns: vec![
                ManifestColumn {
                    field_name: "id".into(),
                    column_name: "id".into(),
                    proto_type: "string".into(),
                    sql_type: "uuid".into(),
                    is_primary: true,
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "_vector".into(),
                    column_name: "_vector".into(),
                    proto_type: "bytes".into(),
                    sql_type: "vector(3)".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let m = CatalogManifest {
            tables: vec![table],
            ..Default::default()
        };
        let ctx = CompileContext::new(&m);
        let search = crate::ir::operations::LogicalSearch {
            message_type: "acme.docs.v1.Doc".into(),
            vector: Some(vec![0.1, 0.2, 0.3]),
            text_query: None,
            filter: None,
            top_k: 5,
            score_threshold: None,
            require_hybrid: false,
            with_vector: false,
            with_payload: true,
        };
        let (sql, _) = extract_sql(PostgresCompiler.compile_search(&search, &ctx).unwrap());
        assert!(sql.contains("\"_vector\" <=> $1::vector"));
        assert!(sql.ends_with("LIMIT 5"));
    }

    #[test]
    fn resource_op_create_table_renders_pg_ddl() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let op = crate::ir::operations::LogicalResourceOp {
            op: crate::ir::operations::ResourceOpKind::Ensure,
            resource_kind: crate::ir::operations::ResourceKind::Table,
            resource_name: "orders".into(),
            spec: Some(serde_json::json!({
                "schema": "billing",
                "columns": [
                    {"name": "id", "type": "uuid", "not_null": true, "primary_key": true},
                    {"name": "total", "type": "numeric(10,2)"}
                ]
            })),
        };
        let (sql, _) = extract_sql(PostgresCompiler.compile_resource_op(&op, &ctx).unwrap());
        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS \"billing\".\"orders\""));
        assert!(sql.contains("\"id\" uuid NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (\"id\")"));
    }

    // --- NW2: aggregate compile tests ---------------------------------

    #[test]
    fn aggregate_count_all_no_group_by() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let agg = LogicalAggregate::count_all("acme.billing.v1.Customer", "total");
        let (sql, params) = extract_sql(
            PostgresCompiler
                .compile_aggregate(&agg, &ctx)
                .expect("compile"),
        );
        assert_eq!(
            sql,
            "SELECT COUNT(*) AS \"total\" FROM \"public\".\"customers\""
        );
        assert!(params.is_empty());
    }

    #[test]
    fn aggregate_group_by_with_having_and_order() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let agg = LogicalAggregate {
            message_type: "acme.billing.v1.Customer".into(),
            filter: Some(LogicalFilter::Comparison {
                field: "email".into(),
                op: ComparisonOp::Like,
                value: LogicalValue::String("%@b.com".into()),
            }),
            group_by: vec!["name".into()],
            aggregates: vec![
                AggregateExpr {
                    func: AggregateFunc::Count,
                    field: "*".into(),
                    alias: "n".into(),
                },
                AggregateExpr {
                    func: AggregateFunc::CountDistinct,
                    field: "email".into(),
                    alias: "distinct_emails".into(),
                },
            ],
            having: Some(LogicalFilter::Comparison {
                field: "n".into(),
                op: ComparisonOp::Gt,
                value: LogicalValue::Int(1),
            }),
            sort: vec![LogicalSort {
                field: "n".into(),
                direction: SortDirection::Desc,
                nulls: crate::ir::projection::NullOrder::Default,
            }],
            pagination: Some(LogicalPagination::limit(20)),
        };

        let (sql, params) = extract_sql(
            PostgresCompiler
                .compile_aggregate(&agg, &ctx)
                .expect("compile"),
        );
        assert_eq!(
            sql,
            "SELECT \"name\", COUNT(*) AS \"n\", \
             COUNT(DISTINCT \"email\") AS \"distinct_emails\" \
             FROM \"public\".\"customers\" \
             WHERE \"email\" LIKE $1 GROUP BY \"name\" HAVING \"n\" > $2 \
             ORDER BY \"n\" DESC LIMIT 20"
        );
        // $1 = like-pattern, $2 = HAVING threshold (in WHERE-then-HAVING order).
        assert_eq!(
            params,
            vec![LogicalValue::String("%@b.com".into()), LogicalValue::Int(1),]
        );
    }

    #[test]
    fn aggregate_empty_aggregates_is_rejected() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let agg = LogicalAggregate {
            message_type: "acme.billing.v1.Customer".into(),
            filter: None,
            group_by: vec!["name".into()],
            aggregates: vec![],
            having: None,
            sort: vec![],
            pagination: None,
        };
        let err = PostgresCompiler.compile_aggregate(&agg, &ctx).unwrap_err();
        assert!(matches!(err, CompileError::Malformed { .. }));
    }

    #[test]
    fn aggregate_duplicate_alias_is_rejected() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let agg = LogicalAggregate {
            message_type: "acme.billing.v1.Customer".into(),
            filter: None,
            group_by: vec![],
            aggregates: vec![
                AggregateExpr {
                    func: AggregateFunc::Count,
                    field: "*".into(),
                    alias: "x".into(),
                },
                AggregateExpr {
                    func: AggregateFunc::Sum,
                    field: "name".into(),
                    alias: "x".into(),
                },
            ],
            having: None,
            sort: vec![],
            pagination: None,
        };
        let err = PostgresCompiler.compile_aggregate(&agg, &ctx).unwrap_err();
        match err {
            CompileError::Malformed { reason } => {
                assert!(reason.contains("duplicate aggregate alias"))
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn aggregate_count_distinct_star_is_rejected() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let agg = LogicalAggregate {
            message_type: "acme.billing.v1.Customer".into(),
            filter: None,
            group_by: vec![],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::CountDistinct,
                field: "*".into(),
                alias: "n".into(),
            }],
            having: None,
            sort: vec![],
            pagination: None,
        };
        let err = PostgresCompiler.compile_aggregate(&agg, &ctx).unwrap_err();
        assert!(matches!(err, CompileError::Malformed { .. }));
    }

    #[test]
    fn aggregate_sort_by_ungrouped_column_is_rejected() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let agg = LogicalAggregate {
            message_type: "acme.billing.v1.Customer".into(),
            filter: None,
            group_by: vec!["name".into()],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Count,
                field: "*".into(),
                alias: "n".into(),
            }],
            having: None,
            sort: vec![LogicalSort {
                field: "email".into(), // not in group_by, not an aggregate alias
                direction: SortDirection::Asc,
                nulls: crate::ir::projection::NullOrder::Default,
            }],
            pagination: None,
        };
        let err = PostgresCompiler.compile_aggregate(&agg, &ctx).unwrap_err();
        assert!(matches!(err, CompileError::Malformed { .. }));
    }
}
