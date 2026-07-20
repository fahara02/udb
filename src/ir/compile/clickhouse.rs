//! ClickHouse compiler (U2 step 6a).
//!
//! ClickHouse SQL is mostly Postgres-shaped but with three operational
//! differences the compiler has to honour:
//!
//! - **No native UPSERT.** `ON CONFLICT` doesn't exist; the common
//!   replacement is a `ReplacingMergeTree` table that dedupes on a sort
//!   key during background merges. The compiler rejects `Replace` /
//!   `Update` and points operators at the right engine choice.
//! - **DELETE is async.** Real-world ClickHouse uses `ALTER TABLE …
//!   DELETE WHERE …` (a mutation). This compiler emits that shape; the
//!   executor handles the optional `SYNC` setting.
//! - **Bind placeholders are `?`** by default in the HTTP interface (the
//!   executor already issues parameterised statements). We keep the IR
//!   contract identical to Postgres: render `?` placeholders and stash
//!   values in the `params` vec for positional binding.
//!
//! Schema / table comes from the manifest like every other compiler.

use crate::backend::BackendKind;
use crate::generation::ManifestTable;
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    AggregateExpr, AggregateFunc, ConflictStrategy, LogicalAggregate, LogicalDelete, LogicalRead,
    LogicalResourceOp, LogicalSearch, LogicalWrite, ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;

use super::{CompileContext, CompileError, CompiledRendering, Compiler};

/// ClickHouse SQL compiler.
#[derive(Debug, Default, Clone, Copy)]
pub struct ClickHouseCompiler;

impl ClickHouseCompiler {
    fn resolve_table<'a>(
        &self,
        message_type: &str,
        ctx: &'a CompileContext<'_>,
    ) -> Result<&'a ManifestTable, CompileError> {
        match crate::broker::table_lookup(ctx.manifest, message_type) {
            crate::broker::TableLookup::Found(table) => Ok(table),
            // fix_plan §4.1: an ambiguous short name names its candidates so the
            // caller can FQN-qualify — never a silent first-wins misroute.
            crate::broker::TableLookup::Ambiguous { .. } => Err(CompileError::Malformed {
                reason: crate::broker::describe_table_lookup_miss(ctx.manifest, message_type),
            }),
            crate::broker::TableLookup::Missing => Err(CompileError::UnknownMessageType {
                message_type: message_type.to_string(),
            }),
        }
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

    /// A2: detect whether the manifest's table declares a tenant_id
    /// column. ClickHouse has no native session-context (it can do
    /// per-user row policies, but those are operator-installed and
    /// not portable), so RLS is performed at the compiler layer:
    /// AND `_tenant_id = ?` (or `tenant_id` — whatever the
    /// manifest names it) into every read/delete/aggregate, and
    /// stamp on writes. Resolution policy is shared with the other
    /// compiler-layer-RLS backends — see `util::resolve_tenant_column`.
    fn tenant_column(table: &ManifestTable) -> Option<&str> {
        super::util::resolve_tenant_column(table)
    }

    fn project_column(table: &ManifestTable) -> Option<&str> {
        super::util::resolve_project_column(table)
    }

    /// A2: append tenant/project predicates to a WHERE body. Returns
    /// None when nothing to inject AND no existing body. Each
    /// injection adds one `?` placeholder to `params`. ClickHouse
    /// quotes identifiers with backticks.
    fn append_context_predicates(
        body: Option<String>,
        table: &ManifestTable,
        ctx: &CompileContext<'_>,
        params: &mut Vec<LogicalValue>,
    ) -> Option<String> {
        super::util::append_context_predicates(body, table, ctx, params, '`')
    }

    fn render_filter(
        &self,
        filter: &LogicalFilter,
        table: &ManifestTable,
        message_type: &str,
        params: &mut Vec<LogicalValue>,
    ) -> Result<String, CompileError> {
        match filter {
            LogicalFilter::And(c) if c.is_empty() => Ok("1=1".into()),
            LogicalFilter::Or(c) if c.is_empty() => Ok("1=0".into()),
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
            LogicalFilter::IsNull(field) => {
                let c = self.column_for(table, field, message_type)?;
                // ClickHouse stores null as a distinct value type; isNull(c)
                // is the canonical check.
                Ok(format!("isNull(`{c}`)"))
            }
            LogicalFilter::InList { field, values } => {
                if values.is_empty() {
                    return Ok("1=0".into());
                }
                let c = self.column_for(table, field, message_type)?;
                let placeholders = values
                    .iter()
                    .map(|v| {
                        params.push(v.clone());
                        "?".to_string()
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("`{c}` IN ({placeholders})"))
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
                let lhs = format!("`{column}`");
                let rendered = match op {
                    ComparisonOp::Eq => format!("{lhs} = ?"),
                    ComparisonOp::Ne => format!("{lhs} != ?"),
                    ComparisonOp::Lt => format!("{lhs} < ?"),
                    ComparisonOp::Le => format!("{lhs} <= ?"),
                    ComparisonOp::Gt => format!("{lhs} > ?"),
                    ComparisonOp::Ge => format!("{lhs} >= ?"),
                    // ClickHouse uses `LIKE` / `ILIKE` like SQL, plus
                    // `position()`/`startsWith`/`endsWith` for substring ops.
                    ComparisonOp::Like => format!("{lhs} LIKE ?"),
                    ComparisonOp::ILike => format!("lowerUTF8({lhs}) LIKE lowerUTF8(?)"),
                    ComparisonOp::Contains => format!("position({lhs}, ?) > 0"),
                    ComparisonOp::StartsWith => format!("startsWith({lhs}, ?)"),
                    ComparisonOp::EndsWith => format!("endsWith({lhs}, ?)"),
                };
                Ok(rendered)
            }
        }
    }
}

impl Compiler for ClickHouseCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::Clickhouse
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
                    let c = self.column_for(table, f, &op.message_type)?;
                    Ok(format!("`{c}`"))
                })
                .collect::<Result<Vec<_>, CompileError>>()?
                .join(", "),
            _ => "*".to_string(),
        };

        // ClickHouse: `database`.`table`. Reuse manifest's `schema` as DB.
        let mut sql = format!(
            "SELECT {select} FROM `{db}`.`{table}`",
            db = table.schema,
            table = table.table,
        );

        let user_body: Option<String> = if let Some(filter) = &op.filter {
            let body = self.render_filter(filter, table, &op.message_type, &mut params)?;
            if body == "1=1" { None } else { Some(body) }
        } else {
            None
        };
        // A2: inject tenant/project predicates so cross-tenant reads
        // are impossible even if the user's filter omits them.
        if let Some(body) = Self::append_context_predicates(user_body, table, ctx, &mut params) {
            sql.push_str(&format!(" WHERE {body}"));
        }

        if !op.sort.is_empty() {
            let parts = op
                .sort
                .iter()
                .map(|s| {
                    let c = self.column_for(table, &s.field, &op.message_type)?;
                    let dir = s.direction.token().to_uppercase();
                    Ok(format!("`{c}` {dir}"))
                })
                .collect::<Result<Vec<_>, CompileError>>()?;
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Clickhouse,
                    op: "keyset_cursor",
                });
            }
            // ClickHouse: LIMIT n OFFSET m (or LIMIT m, n).
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
            backend: BackendKind::Clickhouse,
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
        // ClickHouse: no UPSERT in the SQL surface.
        match &op.conflict {
            ConflictStrategy::Error => {}
            ConflictStrategy::Ignore => {
                // The `IGNORE` keyword exists for INSERT but its semantics
                // (skip rows with type mismatches) aren't what callers want
                // here. Reject explicitly so they reach for the right tool.
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Clickhouse,
                    op: "insert_ignore",
                });
            }
            ConflictStrategy::Replace | ConflictStrategy::Update { .. } => {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Clickhouse,
                    op: "upsert",
                });
            }
        }
        if !op.return_fields.is_empty() {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Clickhouse,
                op: "returning",
            });
        }
        let table = self.resolve_table(&op.message_type, ctx)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        // A2: clone records so we can stamp tenant_id / project_id
        // before computing the column set. ClickHouse INSERT is the
        // only place values land in the row, so stamping here
        // closes the write side of the RLS loop.
        let stamp_tenant = ctx
            .tenant_id
            .filter(|s| !s.is_empty())
            .and_then(|tid| Self::tenant_column(table).map(|col| (col, tid)));
        let stamp_project = ctx
            .project_id
            .filter(|s| !s.is_empty())
            .and_then(|pid| Self::project_column(table).map(|col| (col, pid)));
        let mut records: Vec<crate::ir::operations::LogicalRecord> = op.records.clone();
        for rec in records.iter_mut() {
            // Records are keyed by field names; a caller may supply the
            // tenant/project value under any field alias that maps to the
            // physical column. Resolve every existing key to its column so
            // we never stamp a column that is already present — stamping
            // by literal key would emit a duplicate column in the INSERT.
            let existing_cols: std::collections::HashSet<String> = rec
                .keys()
                .filter_map(|k| self.column_for(table, k, &op.message_type).ok())
                .map(|c| c.to_string())
                .collect();
            if let Some((col, tid)) = stamp_tenant
                && !existing_cols.contains(col)
            {
                rec.insert(col.to_string(), LogicalValue::String(tid.to_string()));
            }
            if let Some((col, pid)) = stamp_project
                && !existing_cols.contains(col)
            {
                rec.insert(col.to_string(), LogicalValue::String(pid.to_string()));
            }
        }

        let first = &records[0];
        let columns: Vec<&str> = first
            .keys()
            .map(|k| self.column_for(table, k, &op.message_type))
            .collect::<Result<Vec<_>, _>>()?;
        let column_list = columns
            .iter()
            .map(|c| format!("`{c}`"))
            .collect::<Vec<_>>()
            .join(", ");

        let mut value_rows = Vec::with_capacity(records.len());
        for (idx, record) in records.iter().enumerate() {
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

        let sql = format!(
            "INSERT INTO `{db}`.`{table}` ({column_list}) VALUES {values}",
            db = table.schema,
            table = table.table,
            values = value_rows.join(", "),
        );
        Ok(CompiledRendering::Sql {
            backend: BackendKind::Clickhouse,
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
        let body = self.render_filter(&op.filter, table, &op.message_type, &mut params)?;
        if body == "1=1" || body == "1=0" {
            return Err(CompileError::Malformed {
                reason: "LogicalDelete::filter cannot be empty or false-constant".into(),
            });
        }
        if !op.return_fields.is_empty() {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Clickhouse,
                op: "returning",
            });
        }
        // A2: AND tenant/project so a DELETE by id can't reach into
        // another tenant's rows.
        let final_body = Self::append_context_predicates(Some(body), table, ctx, &mut params)
            .expect("body was Some so result is Some");
        // `ALTER TABLE … DELETE` is the canonical mutation form; the
        // executor controls whether to set `mutations_sync=2`.
        let sql = format!(
            "ALTER TABLE `{db}`.`{table}` DELETE WHERE {body}",
            db = table.schema,
            table = table.table,
            body = final_body,
        );
        Ok(CompiledRendering::Sql {
            backend: BackendKind::Clickhouse,
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
        let table = self.resolve_table(&op.message_type, ctx)?;
        // #151: a GROUP BY column resolving to an aggregate alias name would emit
        // two identically-keyed result columns. Reject (SQL backends already do).
        let group_names: Vec<&str> = op
            .group_by
            .iter()
            .map(|f| self.column_for(table, f, &op.message_type))
            .collect::<Result<Vec<_>, _>>()?;
        super::util::validate_no_groupby_alias_collision(&group_names, &op.aggregates)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        let mut select_parts: Vec<String> = Vec::new();
        let group_columns: Vec<String> = op
            .group_by
            .iter()
            .map(|f| {
                let col = self.column_for(table, f, &op.message_type)?;
                Ok::<_, CompileError>(format!("`{col}`"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for col in &group_columns {
            select_parts.push(col.clone());
        }
        for agg in &op.aggregates {
            select_parts.push(render_ch_aggregate(agg, table, &op.message_type)?);
        }
        let mut sql = format!(
            "SELECT {sel} FROM `{db}`.`{table}`",
            sel = select_parts.join(", "),
            db = table.schema,
            table = table.table,
        );

        let user_body: Option<String> = if let Some(filter) = &op.filter {
            let body = self.render_filter(filter, table, &op.message_type, &mut params)?;
            if body == "1=1" { None } else { Some(body) }
        } else {
            None
        };
        // A2: tenant/project predicates AND'd into aggregate WHERE.
        if let Some(body) = Self::append_context_predicates(user_body, table, ctx, &mut params) {
            sql.push_str(&format!(" WHERE {body}"));
        }

        if !group_columns.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", group_columns.join(", ")));
        }

        if let Some(having) = &op.having {
            let body = render_ch_having(having, op, &mut params)?;
            sql.push_str(&format!(" HAVING {body}"));
        }

        if !op.sort.is_empty() {
            let parts = op
                .sort
                .iter()
                .map(|s| {
                    let token = resolve_ch_sort_field(&s.field, op, table)?;
                    let direction = s.direction.token().to_uppercase();
                    Ok::<_, CompileError>(format!("{token} {direction}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Clickhouse,
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
            backend: BackendKind::Clickhouse,
            statement: sql,
            params,
        })
    }

    fn compile_search(
        &self,
        op: &LogicalSearch,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        // ClickHouse has no native vector type in OSS; rejecting vector
        // requests is honest. Text-search lowers to `multiSearchAny` /
        // `positionCaseInsensitive` across all string columns and ranks
        // by a simple match-count score.
        let text = op
            .text_query
            .as_deref()
            .ok_or_else(|| CompileError::OperatorUnsupported {
                backend: BackendKind::Clickhouse,
                op: "vector_search",
            })?;
        if text.trim().is_empty() {
            return Err(CompileError::Malformed {
                reason: "text_query must be non-empty for ClickHouse search".into(),
            });
        }
        let table = self.resolve_table(&op.message_type, ctx)?;
        let text_columns: Vec<String> = table
            .columns
            .iter()
            .filter(|c| {
                c.proto_type.eq_ignore_ascii_case("string")
                    || c.sql_type.to_lowercase().contains("string")
            })
            .map(|c| format!("`{}`", c.column_name))
            .collect();
        if text_columns.is_empty() {
            return Err(CompileError::Malformed {
                reason: format!(
                    "text search requires at least one string column in '{}'",
                    op.message_type
                ),
            });
        }
        let mut params: Vec<LogicalValue> = Vec::new();
        // Build OR-of-positionCaseInsensitive(<col>, ?) > 0 across cols
        // and a score = sum of position hits. One bind per column for
        // both the predicate and the score expression, so we push the
        // text twice per column.
        let mut where_parts = Vec::with_capacity(text_columns.len());
        let mut score_parts = Vec::with_capacity(text_columns.len());
        for col in &text_columns {
            params.push(LogicalValue::String(text.to_string()));
            where_parts.push(format!("positionCaseInsensitive({col}, ?) > 0"));
        }
        for col in &text_columns {
            params.push(LogicalValue::String(text.to_string()));
            score_parts.push(format!("(positionCaseInsensitive({col}, ?) > 0)"));
        }
        let score_expr = score_parts.join(" + ");
        let mut sql = format!(
            "SELECT *, ({score_expr}) AS _score FROM `{db}`.`{table}` WHERE ({})",
            where_parts.join(" OR "),
            db = table.schema,
            table = table.table,
        );

        if let Some(filter) = &op.filter {
            let body = self.render_filter(filter, table, &op.message_type, &mut params)?;
            if body != "1=1" {
                sql.push_str(&format!(" AND {body}"));
            }
        }

        // A2: also AND tenant/project into the search WHERE so
        // text search respects RLS too.
        if let Some(extra) = Self::append_context_predicates(None, table, ctx, &mut params) {
            sql.push_str(&format!(" AND {extra}"));
        }

        if let Some(threshold) = op.score_threshold {
            params.push(LogicalValue::Float(threshold as f64));
            sql.push_str(" HAVING _score >= ?");
        }
        sql.push_str(&format!(" ORDER BY _score DESC LIMIT {}", op.top_k));
        Ok(CompiledRendering::Sql {
            backend: BackendKind::Clickhouse,
            statement: sql,
            params,
        })
    }

    fn compile_resource_op(
        &self,
        op: &LogicalResourceOp,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        if !matches!(op.resource_kind, ResourceKind::Table) {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Clickhouse,
                op: "non_table_resource",
            });
        }
        let sql = match op.op {
            ResourceOpKind::Ensure => {
                // Spec: { "database": "...", "columns": [...], "engine":
                // "MergeTree() ORDER BY (id)" }. Engine string is
                // operator-supplied — ClickHouse engine choice is
                // semantic (Replacing/Summing/etc.) and we don't pick
                // for them.
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "Ensure Table requires a spec".into(),
                })?;
                let db = spec
                    .get("database")
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");
                let engine = spec
                    .get("engine")
                    .and_then(|v| v.as_str())
                    .unwrap_or("MergeTree() ORDER BY tuple()");
                let cols = spec
                    .get("columns")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| CompileError::Malformed {
                        reason: "Ensure Table spec.columns must be an array".into(),
                    })?;
                let column_defs = cols
                    .iter()
                    .map(|c| {
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
                        Ok(format!("`{name}` {ty}"))
                    })
                    .collect::<Result<Vec<_>, CompileError>>()?
                    .join(", ");
                format!(
                    "CREATE TABLE IF NOT EXISTS `{db}`.`{name}` ({defs}) ENGINE = {engine}",
                    name = op.resource_name,
                    defs = column_defs,
                )
            }
            ResourceOpKind::Drop => {
                let db = op
                    .spec
                    .as_ref()
                    .and_then(|s| s.get("database"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");
                format!("DROP TABLE IF EXISTS `{db}`.`{}`", op.resource_name)
            }
            ResourceOpKind::List => {
                let db = op
                    .spec
                    .as_ref()
                    .and_then(|s| s.get("database"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");
                format!("SHOW TABLES FROM `{db}`")
            }
        };
        Ok(CompiledRendering::Sql {
            backend: BackendKind::Clickhouse,
            statement: sql,
            params: Vec::new(),
        })
    }
}

fn render_ch_aggregate(
    agg: &AggregateExpr,
    table: &ManifestTable,
    message_type: &str,
) -> Result<String, CompileError> {
    let body = match agg.func {
        AggregateFunc::Count if agg.field == "*" => "COUNT(*)".to_string(),
        AggregateFunc::Count => {
            let col = resolve_ch_column(table, &agg.field, message_type)?;
            format!("COUNT(`{col}`)")
        }
        AggregateFunc::CountDistinct => {
            if agg.field == "*" {
                return Err(CompileError::Malformed {
                    reason: "COUNT(DISTINCT *) is not allowed; specify a field".into(),
                });
            }
            let col = resolve_ch_column(table, &agg.field, message_type)?;
            // ClickHouse prefers `uniqExact` over `COUNT(DISTINCT)`
            // for accuracy; both compile but uniqExact is the canonical
            // CH form.
            format!("uniqExact(`{col}`)")
        }
        AggregateFunc::Sum | AggregateFunc::Avg | AggregateFunc::Min | AggregateFunc::Max => {
            if agg.field == "*" {
                return Err(CompileError::Malformed {
                    reason: format!("{} requires a field name, not '*'", agg.func.sql_token()),
                });
            }
            let col = resolve_ch_column(table, &agg.field, message_type)?;
            format!("{}(`{col}`)", agg.func.sql_token())
        }
    };
    Ok(format!("{body} AS `{}`", agg.alias))
}

fn resolve_ch_sort_field(
    field: &str,
    op: &LogicalAggregate,
    table: &ManifestTable,
) -> Result<String, CompileError> {
    if op.aggregates.iter().any(|a| a.alias == field) {
        return Ok(format!("`{field}`"));
    }
    if op.group_by.iter().any(|f| f == field) {
        let col = resolve_ch_column(table, field, &op.message_type)?;
        return Ok(format!("`{col}`"));
    }
    Err(CompileError::Malformed {
        reason: format!(
            "ORDER BY field '{field}' is neither an aggregate alias nor a GROUP BY column"
        ),
    })
}

fn render_ch_having(
    filter: &LogicalFilter,
    op: &LogicalAggregate,
    params: &mut Vec<LogicalValue>,
) -> Result<String, CompileError> {
    match filter {
        LogicalFilter::And(c) if c.is_empty() => Ok("1=1".to_string()),
        LogicalFilter::Or(c) if c.is_empty() => Ok("1=0".to_string()),
        LogicalFilter::And(clauses) => {
            let parts = clauses
                .iter()
                .map(|c| render_ch_having(c, op, params))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", parts.join(" AND ")))
        }
        LogicalFilter::Or(clauses) => {
            let parts = clauses
                .iter()
                .map(|c| render_ch_having(c, op, params))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", parts.join(" OR ")))
        }
        LogicalFilter::Not(inner) => {
            let r = render_ch_having(inner, op, params)?;
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
            let token = resolve_ch_having_field(field, op)?;
            params.push(value.clone());
            let sql_op = match cmp {
                ComparisonOp::Eq => "=",
                ComparisonOp::Ne => "!=",
                ComparisonOp::Lt => "<",
                ComparisonOp::Le => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Ge => ">=",
                ComparisonOp::Like => "LIKE",
                ComparisonOp::ILike => "ILIKE",
                _ => {
                    return Err(CompileError::OperatorUnsupported {
                        backend: BackendKind::Clickhouse,
                        op: "having_text_match",
                    });
                }
            };
            Ok(format!("{token} {sql_op} ?"))
        }
        LogicalFilter::IsNull(field) => {
            let token = resolve_ch_having_field(field, op)?;
            Ok(format!("isNull({token})"))
        }
        LogicalFilter::InList { field, values } => {
            if values.is_empty() {
                return Ok("1=0".to_string());
            }
            let token = resolve_ch_having_field(field, op)?;
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

fn resolve_ch_having_field(field: &str, op: &LogicalAggregate) -> Result<String, CompileError> {
    if op.aggregates.iter().any(|a| a.alias == field) {
        return Ok(format!("`{field}`"));
    }
    if op.group_by.iter().any(|f| f == field) {
        return Ok(format!("`{field}`"));
    }
    Err(CompileError::Malformed {
        reason: format!(
            "HAVING field '{field}' is neither an aggregate alias nor a GROUP BY column"
        ),
    })
}

fn resolve_ch_column<'a>(
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
        ConflictStrategy, LogicalDelete, LogicalRead, LogicalRecord, LogicalWrite,
    };
    use crate::ir::projection::LogicalPagination;
    use crate::ir::value::LogicalValue;

    fn fixture() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.billing.v1.Event".to_string(),
            schema: "analytics".into(),
            table: "events".into(),
            primary_key: vec!["id".into()],
            columns: vec![
                ManifestColumn {
                    field_name: "id".into(),
                    column_name: "id".into(),
                    proto_type: "string".into(),
                    sql_type: "String".into(),
                    is_primary: true,
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "amount".into(),
                    column_name: "amount".into(),
                    proto_type: "int64".into(),
                    sql_type: "Int64".into(),
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
            other => panic!("expected Sql rendering, got {other:?}"),
        }
    }

    #[test]
    fn select_uses_question_mark_placeholders_and_backticks() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("acme.billing.v1.Event")
            .with_filter(LogicalFilter::Comparison {
                field: "amount".into(),
                op: ComparisonOp::Gt,
                value: LogicalValue::Int(100),
            })
            .with_pagination(LogicalPagination::limit(50));
        let (statement, params) = sql(ClickHouseCompiler.compile_read(&read, &ctx).unwrap());
        assert!(statement.starts_with("SELECT * FROM `analytics`.`events`"));
        assert!(statement.contains("WHERE `amount` > ?"));
        assert!(statement.ends_with("LIMIT 50"));
        assert_eq!(params, vec![LogicalValue::Int(100)]);
    }

    #[test]
    fn delete_emits_alter_table_delete() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let del = LogicalDelete {
            message_type: "acme.billing.v1.Event".into(),
            filter: LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("abc".into()),
            },
            return_fields: vec![],
        };
        let (statement, _) = sql(ClickHouseCompiler.compile_delete(&del, &ctx).unwrap());
        assert!(statement.starts_with("ALTER TABLE `analytics`.`events` DELETE"));
        assert!(statement.contains("WHERE `id` = ?"));
    }

    #[test]
    fn upsert_is_typed_capability_error() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("abc".into()));
        let write = LogicalWrite {
            message_type: "acme.billing.v1.Event".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Replace,
            return_fields: vec![],
        };
        let err = ClickHouseCompiler.compile_write(&write, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Clickhouse,
                op: "upsert"
            }
        ));
    }

    /// A2 fixtures: table with tenant_id + project_id columns.
    fn tenant_fixture() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.billing.v1.Event".to_string(),
            schema: "analytics".into(),
            table: "events".into(),
            primary_key: vec!["id".into()],
            columns: vec![
                ManifestColumn {
                    field_name: "id".into(),
                    column_name: "id".into(),
                    proto_type: "string".into(),
                    sql_type: "String".into(),
                    is_primary: true,
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "tenant_id".into(),
                    column_name: "tenant_id".into(),
                    proto_type: "string".into(),
                    sql_type: "String".into(),
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "project_id".into(),
                    column_name: "project_id".into(),
                    proto_type: "string".into(),
                    sql_type: "String".into(),
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "amount".into(),
                    column_name: "amount".into(),
                    proto_type: "int64".into(),
                    sql_type: "Int64".into(),
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

    #[test]
    fn a2_read_with_tenant_ctx_appends_predicate() {
        let m = tenant_fixture();
        let mut ctx = CompileContext::new(&m);
        ctx.tenant_id = Some("acme");
        ctx.project_id = Some("p1");
        let read =
            LogicalRead::message("acme.billing.v1.Event").with_filter(LogicalFilter::Comparison {
                field: "amount".into(),
                op: ComparisonOp::Gt,
                value: LogicalValue::Int(100),
            });
        let (statement, params) = sql(ClickHouseCompiler.compile_read(&read, &ctx).unwrap());
        assert!(statement.contains("WHERE `amount` > ?"));
        assert!(statement.contains("AND `tenant_id` = ?"));
        assert!(statement.contains("AND `project_id` = ?"));
        assert_eq!(params.len(), 3);
        assert_eq!(params[0], LogicalValue::Int(100));
        assert_eq!(params[1], LogicalValue::String("acme".into()));
        assert_eq!(params[2], LogicalValue::String("p1".into()));
    }

    #[test]
    fn a2_read_no_user_filter_still_emits_tenant_where() {
        let m = tenant_fixture();
        let mut ctx = CompileContext::new(&m);
        ctx.tenant_id = Some("acme");
        let read = LogicalRead::message("acme.billing.v1.Event");
        let (statement, params) = sql(ClickHouseCompiler.compile_read(&read, &ctx).unwrap());
        assert_eq!(
            statement,
            "SELECT * FROM `analytics`.`events` WHERE `tenant_id` = ?"
        );
        assert_eq!(params, vec![LogicalValue::String("acme".into())]);
    }

    #[test]
    fn a2_read_without_tenant_column_no_injection() {
        let m = fixture(); // no tenant_id column
        let mut ctx = CompileContext::new(&m);
        ctx.tenant_id = Some("acme");
        let read = LogicalRead::message("acme.billing.v1.Event");
        let (statement, params) = sql(ClickHouseCompiler.compile_read(&read, &ctx).unwrap());
        assert_eq!(statement, "SELECT * FROM `analytics`.`events`");
        assert!(params.is_empty());
    }

    #[test]
    fn a2_write_stamps_tenant_id_when_absent() {
        let m = tenant_fixture();
        let mut ctx = CompileContext::new(&m);
        ctx.tenant_id = Some("acme");
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("abc".into()));
        rec.insert("amount".into(), LogicalValue::Int(42));
        let write = LogicalWrite {
            message_type: "acme.billing.v1.Event".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Error,
            return_fields: vec![],
        };
        let (statement, params) = sql(ClickHouseCompiler.compile_write(&write, &ctx).unwrap());
        assert!(statement.contains("`tenant_id`"));
        assert!(params.contains(&LogicalValue::String("acme".into())));
    }

    #[test]
    fn a2_delete_appends_tenant_predicate() {
        let m = tenant_fixture();
        let mut ctx = CompileContext::new(&m);
        ctx.tenant_id = Some("acme");
        let del = LogicalDelete {
            message_type: "acme.billing.v1.Event".into(),
            filter: LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("abc".into()),
            },
            return_fields: vec![],
        };
        let (statement, params) = sql(ClickHouseCompiler.compile_delete(&del, &ctx).unwrap());
        assert!(statement.contains("WHERE `id` = ?"));
        assert!(statement.contains("AND `tenant_id` = ?"));
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn plain_insert_compiles_clean() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("abc".into()));
        rec.insert("amount".into(), LogicalValue::Int(42));
        let write = LogicalWrite {
            message_type: "acme.billing.v1.Event".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Error,
            return_fields: vec![],
        };
        let (statement, params) = sql(ClickHouseCompiler.compile_write(&write, &ctx).unwrap());
        assert!(statement.starts_with("INSERT INTO `analytics`.`events` (`amount`, `id`)"));
        assert!(statement.ends_with("VALUES (?, ?)"));
        assert_eq!(params.len(), 2);
    }
}
