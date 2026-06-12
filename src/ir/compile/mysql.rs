//! MySQL compiler (parity-matrix completion).
//!
//! Lowers `LogicalRead`/`Write`/`Delete`/`Aggregate`/`Search`/`ResourceOp`
//! to bind-safe MySQL SQL. The shape mirrors the Postgres compiler with
//! three dialect differences:
//!
//! - **Quoting**: identifiers wrapped in backticks (`` ` ``) not
//!   double-quotes. Reserved words and case-folding rules differ on
//!   MySQL, and backticks are universally accepted by 5.7 / 8.x.
//! - **Placeholders**: positional `?` rather than `$N`. The executor
//!   binds in order off the `params` vec.
//! - **Upsert**: `ON DUPLICATE KEY UPDATE` rather than
//!   `ON CONFLICT (...) DO UPDATE SET`. The semantics are equivalent
//!   for `Replace` / `Update`, but the SQL is laid out differently:
//!   per-column `col = VALUES(col)` assignments (5.7) or
//!   `col = NEW.col` (8.0.20+). We emit `VALUES(col)` for portability.
//!
//! Field resolution is identical to Postgres — manifest column lookup
//! with `field_name` / `column_name` either-or matching.

use crate::backend::BackendKind;
use crate::ir::filter::ComparisonOp;
use crate::ir::operations::{
    ConflictStrategy, LogicalAggregate, LogicalDelete, LogicalRead, LogicalResourceOp,
    LogicalSearch, LogicalWrite, ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;

use super::sql_dialect::{SqlCompiler, SqlDialect};
use super::{CompileContext, CompileError, CompiledRendering, Compiler};

/// MySQL dialect marker for the generic [`SqlCompiler`]: backtick quoting,
/// positional `?` placeholders, `FALSE` false-literal, and the
/// `CONCAT(...) ESCAPE '\\'` LIKE idiom. `ILike` folds to plain `LIKE`.
struct Mysql;

impl SqlDialect for Mysql {
    fn quote(ident: &str) -> String {
        format!("`{ident}`")
    }
    fn placeholder(_index: usize) -> String {
        "?".to_string()
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
    /// MySQL `?` placeholder with optional wildcard wrapping for substring
    /// operators. MySQL is case-insensitive by default for typical
    /// collations (utf8mb4_0900_ai_ci, utf8mb4_unicode_ci), so `ILIKE` lowers
    /// to plain `LIKE` — no LOWER() wrapper needed unless the column has a
    /// case-sensitive collation, which is operator-controlled.
    fn wrap_value_for_op(op: ComparisonOp, _placeholder: &str) -> String {
        // Escape LIKE metacharacters (`\`, `%`, `_`) in the bound value for
        // substring operators so a literal `%`/`_` in the term matches literally
        // instead of acting as a wildcard (e.g. `50%` must not match every row).
        // MySQL string literals treat `\` as an escape character, so `'\\'` is one
        // backslash here; the backslash is doubled first, then `%`/`_` prefixed,
        // and the predicate closed with `ESCAPE '\\'`.
        let escaped = r"REPLACE(REPLACE(REPLACE(?, '\\', '\\\\'), '%', '\\%'), '_', '\\_')";
        match op {
            ComparisonOp::Contains => format!(r"CONCAT('%', {escaped}, '%') ESCAPE '\\'"),
            ComparisonOp::StartsWith => format!(r"CONCAT({escaped}, '%') ESCAPE '\\'"),
            ComparisonOp::EndsWith => format!(r"CONCAT('%', {escaped}) ESCAPE '\\'"),
            _ => "?".to_string(),
        }
    }
}

type My = SqlCompiler<Mysql>;

/// MySQL SQL compiler (5.7+, 8.x).
#[derive(Debug, Default, Clone, Copy)]
pub struct MysqlCompiler;

impl Compiler for MysqlCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::Mysql
    }

    fn compile_read(
        &self,
        op: &LogicalRead,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = My::resolve_table(&op.message_type, ctx.manifest)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        let select = match &op.projection {
            Some(p) if !p.is_select_all() => p
                .fields
                .iter()
                .map(|f| {
                    let col = My::column_for(table, f, &op.message_type)?;
                    Ok(format!("`{col}`"))
                })
                .collect::<Result<Vec<_>, CompileError>>()?
                .join(", "),
            _ => "*".to_string(),
        };

        // MySQL: `schema`.`table` — schema is the database name.
        let mut sql = format!(
            "SELECT {select} FROM `{schema}`.`{table}`",
            schema = table.schema,
            table = table.table,
        );

        if let Some(filter) = &op.filter
            && let Some(body) = My::render_where(filter, table, &op.message_type, &mut params)?
        {
            sql.push_str(&format!(" WHERE {body}"));
        }

        if !op.sort.is_empty() {
            let parts = op
                .sort
                .iter()
                .map(|s| {
                    let col = My::column_for(table, &s.field, &op.message_type)?;
                    let direction = s.direction.token().to_uppercase();
                    // MySQL has no NULLS FIRST/LAST — emulate with a
                    // pre-sort key so the SDK contract is honoured.
                    let null_prefix = match s.nulls {
                        crate::ir::projection::NullOrder::First => {
                            format!("ISNULL(`{col}`) DESC, ")
                        }
                        crate::ir::projection::NullOrder::Last => format!("ISNULL(`{col}`) ASC, "),
                        crate::ir::projection::NullOrder::Default => String::new(),
                    };
                    Ok(format!("{null_prefix}`{col}` {direction}"))
                })
                .collect::<Result<Vec<_>, CompileError>>()?;
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Mysql,
                    op: "keyset_cursor",
                });
            }
            let offset = pag.offset.filter(|&o| o > 0);
            match (pag.limit, offset) {
                (Some(limit), Some(offset)) => {
                    sql.push_str(&format!(" LIMIT {limit} OFFSET {offset}"));
                }
                (Some(limit), None) => {
                    sql.push_str(&format!(" LIMIT {limit}"));
                }
                // MySQL rejects a bare OFFSET; the max-BIGINT LIMIT is the
                // documented idiom for "offset with no limit".
                (None, Some(offset)) => {
                    sql.push_str(&format!(" LIMIT 18446744073709551615 OFFSET {offset}"));
                }
                (None, None) => {}
            }
        }

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Mysql,
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
        let table = My::resolve_table(&op.message_type, ctx.manifest)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        let first = &op.records[0];
        let columns: Vec<&str> = first
            .keys()
            .map(|k| My::column_for(table, k, &op.message_type))
            .collect::<Result<Vec<_>, _>>()?;
        let column_list = columns
            .iter()
            .map(|c| format!("`{c}`"))
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
                .map(|k| My::push_param(&mut params, record[k].clone()))
                .collect::<Vec<_>>()
                .join(", ");
            value_rows.push(format!("({row})"));
        }

        let mut sql = format!(
            "INSERT INTO `{schema}`.`{table}` ({column_list}) VALUES {values}",
            schema = table.schema,
            table = table.table,
            values = value_rows.join(", "),
        );

        match &op.conflict {
            ConflictStrategy::Error => {}
            ConflictStrategy::Ignore => {
                // MySQL `INSERT IGNORE` swallows ALL errors (dup-key,
                // type-mismatch, etc.), which is a meaningful change to
                // the IR contract. We rewrite the leading INSERT to
                // INSERT IGNORE — that's the idiomatic MySQL form.
                sql = sql.replacen("INSERT INTO", "INSERT IGNORE INTO", 1);
            }
            ConflictStrategy::Replace | ConflictStrategy::Update { .. } => {
                if table.primary_key.is_empty() {
                    return Err(CompileError::Malformed {
                        reason: format!(
                            "upsert requested but message '{}' has no primary key in manifest",
                            op.message_type
                        ),
                    });
                }
                // `conflict_on` is intentionally ignored here: MySQL's
                // `ON DUPLICATE KEY UPDATE` arbitrates on ANY unique/primary key
                // automatically, so an alternate-unique target (e.g. `key_hash`)
                // already triggers the upsert without naming it.
                let target_cols: Vec<&str> = match &op.conflict {
                    ConflictStrategy::Update { fields, .. } => fields
                        .iter()
                        .map(|f| My::column_for(table, f, &op.message_type))
                        .collect::<Result<Vec<_>, _>>()?,
                    ConflictStrategy::Replace => columns.clone(),
                    _ => unreachable!(),
                };
                let set_clause = target_cols
                    .iter()
                    .map(|c| format!("`{c}` = VALUES(`{c}`)"))
                    .collect::<Vec<_>>()
                    .join(", ");
                sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {set_clause}"));
            }
        }

        // MySQL has no `RETURNING` (until 8.0.20 in dev branch, not
        // production). Reject so callers know to fetch separately
        // rather than silently dropping the request.
        if !op.return_fields.is_empty() {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Mysql,
                op: "returning",
            });
        }

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Mysql,
            statement: sql,
            params,
        })
    }

    fn compile_delete(
        &self,
        op: &LogicalDelete,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = My::resolve_table(&op.message_type, ctx.manifest)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        let body = My::render_where(&op.filter, table, &op.message_type, &mut params)?.ok_or_else(
            || CompileError::Malformed {
                reason: "LogicalDelete::filter cannot be empty; use Drop resource to truncate"
                    .into(),
            },
        )?;
        if body == "FALSE" {
            return Err(CompileError::Malformed {
                reason: "LogicalDelete::filter resolves to FALSE; refusing no-op delete".into(),
            });
        }
        if !op.return_fields.is_empty() {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Mysql,
                op: "returning",
            });
        }
        let sql = format!(
            "DELETE FROM `{schema}`.`{table}` WHERE {body}",
            schema = table.schema,
            table = table.table,
        );
        Ok(CompiledRendering::Sql {
            backend: BackendKind::Mysql,
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
        super::util::validate_aggregate_aliases(&op.aggregates)?;
        let table = My::resolve_table(&op.message_type, ctx.manifest)?;
        // #151: reject a GROUP BY column colliding with an aggregate alias.
        let group_names: Vec<&str> = op
            .group_by
            .iter()
            .map(|f| My::column_for(table, f, &op.message_type))
            .collect::<Result<Vec<_>, _>>()?;
        super::util::validate_no_groupby_alias_collision(&group_names, &op.aggregates)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        let mut select_parts: Vec<String> = Vec::new();
        let group_columns: Vec<String> = op
            .group_by
            .iter()
            .map(|f| {
                let col = My::column_for(table, f, &op.message_type)?;
                Ok::<_, CompileError>(format!("`{col}`"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for col in &group_columns {
            select_parts.push(col.clone());
        }
        for agg in &op.aggregates {
            select_parts.push(My::render_aggregate(agg, table, &op.message_type)?);
        }
        let mut sql = format!(
            "SELECT {sel} FROM `{schema}`.`{table}`",
            sel = select_parts.join(", "),
            schema = table.schema,
            table = table.table,
        );

        if let Some(filter) = &op.filter
            && let Some(body) = My::render_where(filter, table, &op.message_type, &mut params)?
        {
            sql.push_str(&format!(" WHERE {body}"));
        }

        if !group_columns.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", group_columns.join(", ")));
        }

        if let Some(having) = &op.having {
            let body = My::render_having(having, op, table, &mut params)?;
            sql.push_str(&format!(" HAVING {body}"));
        }

        if !op.sort.is_empty() {
            let parts = op
                .sort
                .iter()
                .map(|s| {
                    let token = My::resolve_sort_field(&s.field, op, table)?;
                    let direction = s.direction.token().to_uppercase();
                    Ok::<_, CompileError>(format!("{token} {direction}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Mysql,
                    op: "keyset_cursor",
                });
            }
            let offset = pag.offset.filter(|&o| o > 0);
            match (pag.limit, offset) {
                (Some(limit), Some(offset)) => {
                    sql.push_str(&format!(" LIMIT {limit} OFFSET {offset}"));
                }
                (Some(limit), None) => {
                    sql.push_str(&format!(" LIMIT {limit}"));
                }
                // MySQL rejects a bare OFFSET; the max-BIGINT LIMIT is the
                // documented idiom for "offset with no limit".
                (None, Some(offset)) => {
                    sql.push_str(&format!(" LIMIT 18446744073709551615 OFFSET {offset}"));
                }
                (None, None) => {}
            }
        }

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Mysql,
            statement: sql,
            params,
        })
    }

    fn compile_search(
        &self,
        op: &LogicalSearch,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        // MySQL's portable search surface is FULLTEXT indexes
        // (`MATCH … AGAINST …`). Vector search requires HeatWave or
        // 8.0.32+ with the VECTOR type; we lower lexical text queries
        // and refuse vector-only requests with a typed capability error.
        let text = op.text_query.as_deref().ok_or_else(|| {
            // Vector-only with no text query is not portable on stock
            // MySQL — surface clearly rather than silently degrade.
            CompileError::OperatorUnsupported {
                backend: BackendKind::Mysql,
                op: "vector_search",
            }
        })?;
        if text.trim().is_empty() {
            return Err(CompileError::Malformed {
                reason: "text_query must be non-empty for MySQL fulltext search".into(),
            });
        }
        let table = My::resolve_table(&op.message_type, ctx.manifest)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        // MATCH(<text columns>) AGAINST(? IN NATURAL LANGUAGE MODE).
        // The set of text columns is every column whose proto_type is
        // `string` — the manifest is the source of truth for what's
        // indexable.
        let text_columns: Vec<String> = table
            .columns
            .iter()
            .filter(|c| c.proto_type.eq_ignore_ascii_case("string"))
            .map(|c| format!("`{}`", c.column_name))
            .collect();
        if text_columns.is_empty() {
            return Err(CompileError::Malformed {
                reason: format!(
                    "fulltext search requires at least one string column in '{}'",
                    op.message_type
                ),
            });
        }
        My::push_param(&mut params, LogicalValue::String(text.to_string()));
        let score_expr = format!(
            "MATCH({}) AGAINST(? IN NATURAL LANGUAGE MODE)",
            text_columns.join(", ")
        );
        let mut sql = format!(
            "SELECT *, {score_expr} AS _score FROM `{schema}`.`{table}` WHERE {score_expr}",
            schema = table.schema,
            table = table.table,
        );
        // Re-bind the text for the WHERE clause — MySQL doesn't reuse a
        // single placeholder across two AGAINST positions.
        My::push_param(&mut params, LogicalValue::String(text.to_string()));

        if let Some(filter) = &op.filter
            && let Some(body) = My::render_where(filter, table, &op.message_type, &mut params)?
        {
            sql.push_str(&format!(" AND {body}"));
        }

        if let Some(threshold) = op.score_threshold {
            // Add `HAVING _score >= ?` so callers can filter weak hits.
            My::push_param(&mut params, LogicalValue::Float(threshold as f64));
            sql.push_str(" HAVING _score >= ?");
        }

        sql.push_str(&format!(" ORDER BY _score DESC LIMIT {}", op.top_k));

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Mysql,
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
                backend: BackendKind::Mysql,
                op: "non_table_resource",
            });
        }
        let sql = match (op.op, op.resource_kind) {
            (ResourceOpKind::Ensure, ResourceKind::Table) => {
                // Spec format: { "columns": [{"name":"...","type":"...","not_null":bool,"primary_key":bool}, ...] }
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
                        pk_cols.push(format!("`{name}`"));
                    }
                    let null_clause = if not_null { " NOT NULL" } else { "" };
                    column_defs.push(format!("`{name}` {ty}{null_clause}"));
                }
                if !pk_cols.is_empty() {
                    column_defs.push(format!("PRIMARY KEY ({})", pk_cols.join(", ")));
                }
                // NW-universal: engine, charset, and collation are
                // operator-controllable via spec fields. Defaults
                // match the historical MySQL 8.0 install: InnoDB +
                // utf8mb4 (no explicit collation — let the server
                // pick its default for the charset, which is
                // `utf8mb4_0900_ai_ci` on 8.0+).
                let engine = spec
                    .get("engine")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or("InnoDB");
                let charset = spec
                    .get("charset")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or("utf8mb4");
                let collate = spec.get("collate").and_then(|v| v.as_str());
                let mut suffix = format!(" ENGINE={engine} DEFAULT CHARSET={charset}");
                if let Some(collation) = collate.filter(|s| !s.trim().is_empty()) {
                    suffix.push_str(&format!(" COLLATE={collation}"));
                }
                format!(
                    "CREATE TABLE IF NOT EXISTS `{name}` ({defs}){suffix}",
                    name = op.resource_name,
                    defs = column_defs.join(", "),
                )
            }
            (ResourceOpKind::Drop, ResourceKind::Table) => {
                format!("DROP TABLE IF EXISTS `{}`", op.resource_name)
            }
            (ResourceOpKind::List, ResourceKind::Table) => "SHOW TABLES".to_string(),
            (ResourceOpKind::Ensure, ResourceKind::Index) => {
                // Spec format: { "table": "...", "columns": ["..."], "unique": bool }
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
                    .map(|c| format!("`{c}`"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let unique = spec
                    .get("unique")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let kind = if unique { "UNIQUE INDEX" } else { "INDEX" };
                format!(
                    "CREATE {kind} `{name}` ON `{tbl}` ({col_list})",
                    name = op.resource_name,
                )
            }
            (ResourceOpKind::Drop, ResourceKind::Index) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "Drop Index requires a spec with 'table'".into(),
                })?;
                let tbl = spec.get("table").and_then(|v| v.as_str()).ok_or_else(|| {
                    CompileError::Malformed {
                        reason: "Drop Index spec missing 'table'".into(),
                    }
                })?;
                format!("DROP INDEX `{}` ON `{tbl}`", op.resource_name)
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
                format!("SHOW INDEX FROM `{tbl}`")
            }
            _ => {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Mysql,
                    op: "unhandled_resource_op",
                });
            }
        };
        Ok(CompiledRendering::Sql {
            backend: BackendKind::Mysql,
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
        LogicalRead, LogicalRecord, LogicalSearch, LogicalWrite,
    };
    use crate::ir::projection::{LogicalPagination, LogicalSort, SortDirection};
    use crate::ir::value::LogicalValue;
    use serde_json::json;

    fn fixture() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.billing.v1.Customer".into(),
            schema: "billing".into(),
            table: "customers".into(),
            primary_key: vec!["id".into()],
            columns: vec![
                ManifestColumn {
                    field_name: "id".into(),
                    column_name: "id".into(),
                    proto_type: "string".into(),
                    sql_type: "varchar(64)".into(),
                    is_primary: true,
                    not_null: true,
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "name".into(),
                    column_name: "name".into(),
                    proto_type: "string".into(),
                    sql_type: "varchar(255)".into(),
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "email".into(),
                    column_name: "email".into(),
                    proto_type: "string".into(),
                    sql_type: "varchar(255)".into(),
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "amount".into(),
                    column_name: "amount".into(),
                    proto_type: "int64".into(),
                    sql_type: "bigint".into(),
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
    fn select_uses_backticks_and_question_marks() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("acme.billing.v1.Customer")
            .with_filter(LogicalFilter::Comparison {
                field: "email".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("a@b.com".into()),
            })
            .with_pagination(LogicalPagination::limit(10));
        let (statement, params) = sql(MysqlCompiler.compile_read(&read, &ctx).unwrap());
        assert!(statement.starts_with("SELECT * FROM `billing`.`customers`"));
        assert!(statement.contains("WHERE `email` = ?"));
        assert!(statement.ends_with("LIMIT 10"));
        assert_eq!(params, vec![LogicalValue::String("a@b.com".into())]);
    }

    #[test]
    fn upsert_emits_on_duplicate_key_update() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("abc".into()));
        rec.insert("name".into(), LogicalValue::String("Alice".into()));
        let write = LogicalWrite {
            message_type: "acme.billing.v1.Customer".into(),
            records: vec![rec],
            conflict: ConflictStrategy::update(vec!["name".into()]),
            return_fields: vec![],
        };
        let (statement, _) = sql(MysqlCompiler.compile_write(&write, &ctx).unwrap());
        assert!(statement.contains("INSERT INTO `billing`.`customers`"));
        assert!(statement.contains("ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"));
    }

    #[test]
    fn ignore_rewrites_insert_to_insert_ignore() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("abc".into()));
        let write = LogicalWrite {
            message_type: "acme.billing.v1.Customer".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Ignore,
            return_fields: vec![],
        };
        let (statement, _) = sql(MysqlCompiler.compile_write(&write, &ctx).unwrap());
        assert!(statement.starts_with("INSERT IGNORE INTO"));
    }

    #[test]
    fn returning_is_rejected() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("abc".into()));
        let write = LogicalWrite {
            message_type: "acme.billing.v1.Customer".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Error,
            return_fields: vec!["id".into()],
        };
        let err = MysqlCompiler.compile_write(&write, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Mysql,
                op: "returning"
            }
        ));
    }

    #[test]
    fn aggregate_group_by_with_having() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let agg = LogicalAggregate {
            message_type: "acme.billing.v1.Customer".into(),
            filter: None,
            group_by: vec!["name".into()],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                field: "amount".into(),
                alias: "total".into(),
            }],
            having: Some(LogicalFilter::Comparison {
                field: "total".into(),
                op: ComparisonOp::Gt,
                value: LogicalValue::Int(100),
            }),
            sort: vec![LogicalSort {
                field: "total".into(),
                direction: SortDirection::Desc,
                nulls: crate::ir::projection::NullOrder::Default,
            }],
            pagination: Some(LogicalPagination::limit(5)),
        };
        let (statement, params) = sql(MysqlCompiler.compile_aggregate(&agg, &ctx).unwrap());
        assert_eq!(
            statement,
            "SELECT `name`, SUM(`amount`) AS `total` FROM `billing`.`customers` \
             GROUP BY `name` HAVING `total` > ? ORDER BY `total` DESC LIMIT 5"
        );
        assert_eq!(params, vec![LogicalValue::Int(100)]);
    }

    #[test]
    fn search_emits_fulltext_match_against() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let search = LogicalSearch {
            message_type: "acme.billing.v1.Customer".into(),
            vector: None,
            text_query: Some("alice".into()),
            filter: None,
            top_k: 5,
            score_threshold: None,
            require_hybrid: false,
            with_vector: false,
            with_payload: true,
        };
        let (statement, params) = sql(MysqlCompiler.compile_search(&search, &ctx).unwrap());
        // Three string columns: id, name, email.
        assert!(
            statement.contains("MATCH(`id`, `name`, `email`) AGAINST(? IN NATURAL LANGUAGE MODE)")
        );
        assert!(statement.ends_with("ORDER BY _score DESC LIMIT 5"));
        assert_eq!(params.len(), 2); // SELECT-MATCH bind + WHERE-MATCH bind
    }

    #[test]
    fn search_vector_only_is_typed_capability_error() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let search = LogicalSearch {
            message_type: "acme.billing.v1.Customer".into(),
            vector: Some(vec![0.1, 0.2]),
            text_query: None,
            filter: None,
            top_k: 5,
            score_threshold: None,
            require_hybrid: false,
            with_vector: false,
            with_payload: true,
        };
        let err = MysqlCompiler.compile_search(&search, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Mysql,
                op: "vector_search"
            }
        ));
    }

    #[test]
    fn resource_op_create_table_renders_mysql_ddl() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let op = LogicalResourceOp {
            op: ResourceOpKind::Ensure,
            resource_kind: ResourceKind::Table,
            resource_name: "orders".into(),
            spec: Some(json!({
                "columns": [
                    {"name": "id", "type": "bigint", "not_null": true, "primary_key": true},
                    {"name": "amount", "type": "decimal(10,2)"}
                ]
            })),
        };
        let (statement, _) = sql(MysqlCompiler.compile_resource_op(&op, &ctx).unwrap());
        assert!(statement.starts_with("CREATE TABLE IF NOT EXISTS `orders`"));
        assert!(statement.contains("`id` bigint NOT NULL"));
        assert!(statement.contains("PRIMARY KEY (`id`)"));
        assert!(statement.contains("ENGINE=InnoDB"));
    }

    #[test]
    fn resource_op_create_table_honors_custom_engine_and_charset() {
        // NW-universal: operators can override the default
        // InnoDB + utf8mb4 via spec fields. Useful for legacy
        // deployments on MyISAM, or tenants pinning a non-utf8mb4
        // collation for compatibility.
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let op = LogicalResourceOp {
            op: ResourceOpKind::Ensure,
            resource_kind: ResourceKind::Table,
            resource_name: "events".into(),
            spec: Some(json!({
                "engine": "MyISAM",
                "charset": "latin1",
                "collate": "latin1_swedish_ci",
                "columns": [
                    {"name": "id", "type": "bigint", "not_null": true, "primary_key": true}
                ]
            })),
        };
        let (statement, _) = sql(MysqlCompiler.compile_resource_op(&op, &ctx).unwrap());
        assert!(statement.contains("ENGINE=MyISAM"));
        assert!(statement.contains("DEFAULT CHARSET=latin1"));
        assert!(statement.contains("COLLATE=latin1_swedish_ci"));
    }

    #[test]
    fn resource_op_drop_table() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let op = LogicalResourceOp {
            op: ResourceOpKind::Drop,
            resource_kind: ResourceKind::Table,
            resource_name: "orders".into(),
            spec: None,
        };
        let (statement, _) = sql(MysqlCompiler.compile_resource_op(&op, &ctx).unwrap());
        assert_eq!(statement, "DROP TABLE IF EXISTS `orders`");
    }

    #[test]
    fn delete_with_filter_compiles() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let del = LogicalDelete {
            message_type: "acme.billing.v1.Customer".into(),
            filter: LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("abc".into()),
            },
            return_fields: vec![],
        };
        let (statement, params) = sql(MysqlCompiler.compile_delete(&del, &ctx).unwrap());
        assert_eq!(
            statement,
            "DELETE FROM `billing`.`customers` WHERE `id` = ?"
        );
        assert_eq!(params, vec![LogicalValue::String("abc".into())]);
    }
}
