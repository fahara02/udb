//! Microsoft SQL Server (T-SQL) compiler (C9).
//!
//! Lowers `LogicalRead`/`Write`/`Delete`/`Aggregate`/`ResourceOp` to
//! T-SQL targeted at SQL Server 2008+ (TDS 7.3 wire protocol). Shape
//! is close to Postgres with the dialect differences:
//!
//! - **Quoting**: `[bracketed]` identifiers (T-SQL's canonical form).
//!   Double-quoted ANSI form works too if `QUOTED_IDENTIFIER ON` but
//!   bracketed is universally accepted.
//! - **Placeholders**: named `@P1` / `@P2` / `…` (Tiberius requires
//!   named parameters; positional `?` isn't a thing in T-SQL).
//! - **Upsert**: T-SQL has no `ON CONFLICT` — the canonical idiom is
//!   `MERGE INTO … USING (VALUES …) ON … WHEN MATCHED THEN UPDATE …
//!   WHEN NOT MATCHED THEN INSERT …`. We emit that for `Replace` /
//!   `Update` conflict strategies.
//! - **OFFSET pagination**: T-SQL needs `OFFSET n ROWS FETCH NEXT m
//!   ROWS ONLY` and requires an `ORDER BY` to use OFFSET — we surface
//!   a typed `Malformed` error when the caller offsets without sort.
//! - **Schema**: T-SQL schemas are first-class (`schema.table`).
//! - **Search**: T-SQL supports `CONTAINS()` / `FREETEXT()` against
//!   Full-Text Search catalogs. We compile to `CONTAINS(*, @P1)` over
//!   any string column when text_query is set; vector search isn't a
//!   native SQL Server primitive — refused with `OperatorUnsupported`.

use crate::backend::BackendKind;
use crate::generation::ManifestTable;
use crate::ir::filter::ComparisonOp;
use crate::ir::operations::{
    ConflictStrategy, LogicalAggregate, LogicalDelete, LogicalRead, LogicalResourceOp,
    LogicalSearch, LogicalWrite, ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;

use super::sql_dialect::{SqlCompiler, SqlDialect};
use super::{CompileContext, CompileError, CompiledRendering, Compiler};

/// T-SQL dialect marker for the generic [`SqlCompiler`]: `[bracketed]`
/// quoting, named `@PN` placeholders, `1=0` false-literal, and the
/// `'%' + … ESCAPE '\'` LIKE idiom. `ILike` folds to plain `LIKE`.
struct Mssql;

impl SqlDialect for Mssql {
    fn quote(ident: &str) -> String {
        format!("[{ident}]")
    }
    fn placeholder(index: usize) -> String {
        format!("@P{index}")
    }
    fn false_literal() -> &'static str {
        "1=0"
    }
    fn having_true_literal() -> &'static str {
        "1=1"
    }
    fn having_false_literal() -> &'static str {
        "1=0"
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
    /// T-SQL `LIKE` is case-insensitive when the column collation is
    /// case-insensitive (typical default). `ILIKE` maps to plain `LIKE`
    /// with the same semantics for default collations.
    fn wrap_value_for_op(op: ComparisonOp, placeholder: &str) -> String {
        // Escape LIKE metacharacters (`\`, `%`, `_`, and `[` which opens a T-SQL
        // character class) so a literal value matches literally instead of acting
        // as a wildcard. T-SQL string literals do not process backslash escapes,
        // so `'\'` is one literal backslash: double it, prefix the wildcard chars,
        // and close the predicate with `ESCAPE '\'`.
        let escaped = format!(
            r"REPLACE(REPLACE(REPLACE(REPLACE({placeholder}, '\', '\\'), '%', '\%'), '_', '\_'), '[', '\[')"
        );
        match op {
            ComparisonOp::Contains => format!(r"'%' + {escaped} + '%' ESCAPE '\'"),
            ComparisonOp::StartsWith => format!(r"{escaped} + '%' ESCAPE '\'"),
            ComparisonOp::EndsWith => format!(r"'%' + {escaped} ESCAPE '\'"),
            _ => placeholder.to_string(),
        }
    }
}

type Ms = SqlCompiler<Mssql>;

/// T-SQL compiler for Microsoft SQL Server.
#[derive(Debug, Default, Clone, Copy)]
pub struct MssqlCompiler;

impl Compiler for MssqlCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::Mssql
    }

    fn compile_read(
        &self,
        op: &LogicalRead,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = Ms::resolve_table(&op.message_type, ctx.manifest)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        let select = match &op.projection {
            Some(p) if !p.is_select_all() => p
                .fields
                .iter()
                .map(|f| {
                    let col = Ms::column_for(table, f, &op.message_type)?;
                    Ok(format!("[{col}]"))
                })
                .collect::<Result<Vec<_>, CompileError>>()?
                .join(", "),
            _ => "*".to_string(),
        };

        let mut sql = format!(
            "SELECT {select} FROM [{schema}].[{table}]",
            schema = table.schema,
            table = table.table,
        );

        if let Some(filter) = &op.filter
            && let Some(body) = Ms::render_where(filter, table, &op.message_type, &mut params)?
        {
            sql.push_str(&format!(" WHERE {body}"));
        }

        if !op.sort.is_empty() {
            let parts = op
                .sort
                .iter()
                .map(|s| {
                    let col = Ms::column_for(table, &s.field, &op.message_type)?;
                    let direction = s.direction.token().to_uppercase();
                    // T-SQL has no NULLS FIRST/LAST; emulate via prefix sort.
                    let null_prefix = match s.nulls {
                        crate::ir::projection::NullOrder::First => {
                            format!("(CASE WHEN [{col}] IS NULL THEN 0 ELSE 1 END), ")
                        }
                        crate::ir::projection::NullOrder::Last => {
                            format!("(CASE WHEN [{col}] IS NULL THEN 1 ELSE 0 END), ")
                        }
                        crate::ir::projection::NullOrder::Default => String::new(),
                    };
                    Ok(format!("{null_prefix}[{col}] {direction}"))
                })
                .collect::<Result<Vec<_>, CompileError>>()?;
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        // T-SQL OFFSET / FETCH NEXT requires ORDER BY. Refuse explicitly
        // when offset/limit are set without sort — silently emitting
        // SQL that fails at execution time is worse than a clear pre-
        // execute error.
        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Mssql,
                    op: "keyset_cursor",
                });
            }
            if (pag.limit.is_some() || pag.offset.is_some_and(|o| o > 0)) && op.sort.is_empty() {
                return Err(CompileError::Malformed {
                    reason: "T-SQL OFFSET/FETCH NEXT requires ORDER BY; add a sort clause".into(),
                });
            }
            let offset = pag.offset.unwrap_or(0);
            if pag.limit.is_some() || offset > 0 {
                sql.push_str(&format!(" OFFSET {offset} ROWS"));
                if let Some(limit) = pag.limit {
                    sql.push_str(&format!(" FETCH NEXT {limit} ROWS ONLY"));
                }
            }
        }

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Mssql,
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
        let table = Ms::resolve_table(&op.message_type, ctx.manifest)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        let first = &op.records[0];
        let columns: Vec<&str> = first
            .keys()
            .map(|k| Ms::column_for(table, k, &op.message_type))
            .collect::<Result<Vec<_>, _>>()?;
        let column_list = columns
            .iter()
            .map(|c| format!("[{c}]"))
            .collect::<Vec<_>>()
            .join(", ");

        // For Error (plain INSERT) and Ignore (special MERGE shape)
        // we collect values row-by-row.
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
                .map(|k| Ms::push_param(&mut params, record[k].clone()))
                .collect::<Vec<_>>()
                .join(", ");
            value_rows.push(format!("({row})"));
        }

        match &op.conflict {
            ConflictStrategy::Error => {
                let sql = format!(
                    "INSERT INTO [{schema}].[{table}] ({column_list}) VALUES {values}",
                    schema = table.schema,
                    table = table.table,
                    values = value_rows.join(", "),
                );
                Ok(CompiledRendering::Sql {
                    backend: BackendKind::Mssql,
                    statement: sql,
                    params,
                })
            }
            ConflictStrategy::Ignore => {
                // T-SQL "insert or ignore" via MERGE: if a row with the
                // primary key already exists, do nothing. Requires a
                // primary key in the manifest.
                if table.primary_key.is_empty() {
                    return Err(CompileError::Malformed {
                        reason: format!(
                            "INSERT IGNORE on '{}' requires a primary key in the manifest",
                            op.message_type
                        ),
                    });
                }
                if op.records.len() != 1 {
                    return Err(CompileError::OperatorUnsupported {
                        backend: BackendKind::Mssql,
                        op: "batch_insert_ignore",
                    });
                }
                let on_clause = build_merge_on_clause(table, &op.message_type, &table.primary_key)?;
                let sql = format!(
                    "MERGE INTO [{schema}].[{table}] AS target \
                     USING (VALUES {values}) AS source({column_list}) \
                     ON {on_clause} \
                     WHEN NOT MATCHED THEN \
                         INSERT ({column_list}) VALUES {source_values};",
                    schema = table.schema,
                    table = table.table,
                    values = value_rows.join(", "),
                    source_values = columns
                        .iter()
                        .map(|c| format!("source.[{c}]"))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
                Ok(CompiledRendering::Sql {
                    backend: BackendKind::Mssql,
                    statement: sql,
                    params,
                })
            }
            ConflictStrategy::Replace | ConflictStrategy::Update { .. } => {
                // Conflict arbiter: explicit alternate-unique target when given
                // (MERGE … ON target.[col] = source.[col]), else the primary key.
                let conflict_fields: Vec<String> = match op.conflict.conflict_target() {
                    Some(cols) => cols.to_vec(),
                    None => {
                        if table.primary_key.is_empty() {
                            return Err(CompileError::Malformed {
                                reason: format!(
                                    "upsert on '{}' requires a primary key in the manifest or \
                                     an explicit conflict_on target",
                                    op.message_type
                                ),
                            });
                        }
                        table.primary_key.clone()
                    }
                };
                if op.records.len() != 1 {
                    return Err(CompileError::OperatorUnsupported {
                        backend: BackendKind::Mssql,
                        op: "batch_upsert",
                    });
                }
                let on_clause = build_merge_on_clause(table, &op.message_type, &conflict_fields)?;
                let target_cols: Vec<&str> = match &op.conflict {
                    ConflictStrategy::Update { fields, .. } => fields
                        .iter()
                        .map(|f| Ms::column_for(table, f, &op.message_type))
                        .collect::<Result<Vec<_>, _>>()?,
                    ConflictStrategy::Replace => columns.clone(),
                    _ => unreachable!(),
                };
                let set_clause = target_cols
                    .iter()
                    .map(|c| format!("target.[{c}] = source.[{c}]"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "MERGE INTO [{schema}].[{table}] AS target \
                     USING (VALUES {values}) AS source({column_list}) \
                     ON {on_clause} \
                     WHEN MATCHED THEN UPDATE SET {set_clause} \
                     WHEN NOT MATCHED THEN \
                         INSERT ({column_list}) VALUES {source_values};",
                    schema = table.schema,
                    table = table.table,
                    values = value_rows.join(", "),
                    source_values = columns
                        .iter()
                        .map(|c| format!("source.[{c}]"))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
                Ok(CompiledRendering::Sql {
                    backend: BackendKind::Mssql,
                    statement: sql,
                    params,
                })
            }
        }
    }

    fn compile_delete(
        &self,
        op: &LogicalDelete,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = Ms::resolve_table(&op.message_type, ctx.manifest)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        let body = Ms::render_where(&op.filter, table, &op.message_type, &mut params)?.ok_or_else(
            || CompileError::Malformed {
                reason: "LogicalDelete::filter cannot be empty; use Drop resource to truncate"
                    .into(),
            },
        )?;
        if body == "1=0" {
            return Err(CompileError::Malformed {
                reason: "LogicalDelete::filter resolves to FALSE; refusing no-op delete".into(),
            });
        }
        let sql = format!(
            "DELETE FROM [{schema}].[{table}] WHERE {body}",
            schema = table.schema,
            table = table.table,
        );
        Ok(CompiledRendering::Sql {
            backend: BackendKind::Mssql,
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
        let table = Ms::resolve_table(&op.message_type, ctx.manifest)?;
        // #151: reject a GROUP BY column colliding with an aggregate alias.
        let group_names: Vec<&str> = op
            .group_by
            .iter()
            .map(|f| Ms::column_for(table, f, &op.message_type))
            .collect::<Result<Vec<_>, _>>()?;
        super::util::validate_no_groupby_alias_collision(&group_names, &op.aggregates)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        let mut select_parts: Vec<String> = Vec::new();
        let group_columns: Vec<String> = op
            .group_by
            .iter()
            .map(|f| {
                let col = Ms::column_for(table, f, &op.message_type)?;
                Ok::<_, CompileError>(format!("[{col}]"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for col in &group_columns {
            select_parts.push(col.clone());
        }
        for agg in &op.aggregates {
            select_parts.push(Ms::render_aggregate(agg, table, &op.message_type)?);
        }
        let mut sql = format!(
            "SELECT {sel} FROM [{schema}].[{table}]",
            sel = select_parts.join(", "),
            schema = table.schema,
            table = table.table,
        );

        if let Some(filter) = &op.filter
            && let Some(body) = Ms::render_where(filter, table, &op.message_type, &mut params)?
        {
            sql.push_str(&format!(" WHERE {body}"));
        }

        if !group_columns.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", group_columns.join(", ")));
        }

        if let Some(having) = &op.having {
            let body = Ms::render_having(having, op, table, &mut params)?;
            sql.push_str(&format!(" HAVING {body}"));
        }

        if !op.sort.is_empty() {
            let parts = op
                .sort
                .iter()
                .map(|s| {
                    let token = Ms::resolve_sort_field(&s.field, op, table)?;
                    let direction = s.direction.token().to_uppercase();
                    Ok::<_, CompileError>(format!("{token} {direction}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Mssql,
                    op: "keyset_cursor",
                });
            }
            if (pag.limit.is_some() || pag.offset.is_some_and(|o| o > 0)) && op.sort.is_empty() {
                return Err(CompileError::Malformed {
                    reason: "T-SQL OFFSET/FETCH NEXT requires ORDER BY".into(),
                });
            }
            let offset = pag.offset.unwrap_or(0);
            if pag.limit.is_some() || offset > 0 {
                sql.push_str(&format!(" OFFSET {offset} ROWS"));
                if let Some(limit) = pag.limit {
                    sql.push_str(&format!(" FETCH NEXT {limit} ROWS ONLY"));
                }
            }
        }

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Mssql,
            statement: sql,
            params,
        })
    }

    fn compile_search(
        &self,
        op: &LogicalSearch,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let text = op.text_query.as_deref().ok_or_else(|| {
            // T-SQL has no native vector primitive; reject cleanly.
            CompileError::OperatorUnsupported {
                backend: BackendKind::Mssql,
                op: "vector_search",
            }
        })?;
        if text.trim().is_empty() {
            return Err(CompileError::Malformed {
                reason: "text_query must be non-empty for T-SQL CONTAINS()".into(),
            });
        }
        let table = Ms::resolve_table(&op.message_type, ctx.manifest)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        // CONTAINS(*, @P1) — searches every fulltext-indexed column.
        // T-SQL requires Full-Text Search to be enabled and a fulltext
        // catalog on the table; that's an operator-side setup outside
        // the broker's purview.
        Ms::push_param(&mut params, LogicalValue::String(text.to_string()));
        let mut sql = format!(
            "SELECT TOP({top_k}) * FROM [{schema}].[{table}] WHERE CONTAINS(*, @P1)",
            top_k = op.top_k,
            schema = table.schema,
            table = table.table,
        );

        if let Some(filter) = &op.filter
            && let Some(body) = Ms::render_where(filter, table, &op.message_type, &mut params)?
        {
            sql.push_str(&format!(" AND {body}"));
        }

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Mssql,
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
                backend: BackendKind::Mssql,
                op: "non_table_resource",
            });
        }
        let sql = match (op.op, op.resource_kind) {
            (ResourceOpKind::Ensure, ResourceKind::Table) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "Ensure Table requires a spec with column definitions".into(),
                })?;
                let schema = spec.get("schema").and_then(|v| v.as_str()).unwrap_or("dbo");
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
                        pk_cols.push(format!("[{name}]"));
                    }
                    let null_clause = if not_null { " NOT NULL" } else { " NULL" };
                    column_defs.push(format!("[{name}] {ty}{null_clause}"));
                }
                if !pk_cols.is_empty() {
                    column_defs.push(format!(
                        "CONSTRAINT [PK_{name}] PRIMARY KEY ({pk_list})",
                        name = op.resource_name,
                        pk_list = pk_cols.join(", "),
                    ));
                }
                // T-SQL has no `CREATE TABLE IF NOT EXISTS` until 2016.
                // The widely-compatible idiom is the catalog check.
                format!(
                    "IF NOT EXISTS (SELECT 1 FROM sys.tables t \
                     JOIN sys.schemas s ON t.schema_id = s.schema_id \
                     WHERE s.name = '{schema}' AND t.name = '{name}') \
                     CREATE TABLE [{schema}].[{name}] ({defs});",
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
                    .unwrap_or("dbo");
                format!(
                    "IF OBJECT_ID('[{schema}].[{name}]', 'U') IS NOT NULL \
                     DROP TABLE [{schema}].[{name}];",
                    name = op.resource_name,
                )
            }
            (ResourceOpKind::List, ResourceKind::Table) => {
                let schema = op
                    .spec
                    .as_ref()
                    .and_then(|s| s.get("schema"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("dbo");
                format!(
                    "SELECT t.name FROM sys.tables t \
                     JOIN sys.schemas s ON t.schema_id = s.schema_id \
                     WHERE s.name = '{schema}' ORDER BY t.name"
                )
            }
            (ResourceOpKind::Ensure, ResourceKind::Index) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "Ensure Index requires a spec".into(),
                })?;
                let schema = spec.get("schema").and_then(|v| v.as_str()).unwrap_or("dbo");
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
                    .map(|c| format!("[{c}]"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let unique = spec
                    .get("unique")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let kind = if unique { "UNIQUE INDEX" } else { "INDEX" };
                format!(
                    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{name}') \
                     CREATE {kind} [{name}] ON [{schema}].[{tbl}] ({col_list});",
                    name = op.resource_name,
                )
            }
            (ResourceOpKind::Drop, ResourceKind::Index) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "Drop Index requires a spec with 'table'".into(),
                })?;
                let schema = spec.get("schema").and_then(|v| v.as_str()).unwrap_or("dbo");
                let tbl = spec.get("table").and_then(|v| v.as_str()).ok_or_else(|| {
                    CompileError::Malformed {
                        reason: "Drop Index spec missing 'table'".into(),
                    }
                })?;
                format!("DROP INDEX [{}] ON [{schema}].[{tbl}];", op.resource_name)
            }
            (ResourceOpKind::List, ResourceKind::Index) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "List Index requires a spec with 'table'".into(),
                })?;
                let schema = spec.get("schema").and_then(|v| v.as_str()).unwrap_or("dbo");
                let tbl = spec.get("table").and_then(|v| v.as_str()).ok_or_else(|| {
                    CompileError::Malformed {
                        reason: "List Index spec missing 'table'".into(),
                    }
                })?;
                format!(
                    "SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID('[{schema}].[{tbl}]') ORDER BY name"
                )
            }
            _ => {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Mssql,
                    op: "unhandled_resource_op",
                });
            }
        };
        Ok(CompiledRendering::Sql {
            backend: BackendKind::Mssql,
            statement: sql,
            params: Vec::new(),
        })
    }
}

/// Build the `MERGE … ON` predicate matching `source` to `target` on the given
/// conflict columns (the manifest primary key, or an explicit alternate-unique
/// `conflict_on` target).
fn build_merge_on_clause(
    table: &ManifestTable,
    message_type: &str,
    conflict_fields: &[String],
) -> Result<String, CompileError> {
    let parts = conflict_fields
        .iter()
        .map(|pk| {
            let col = Ms::column_for(table, pk, message_type)?;
            Ok(format!("target.[{col}] = source.[{col}]"))
        })
        .collect::<Result<Vec<_>, CompileError>>()?;
    Ok(parts.join(" AND "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
    use crate::ir::filter::{ComparisonOp, LogicalFilter};
    use crate::ir::operations::{
        AggregateExpr, AggregateFunc, ConflictStrategy, LogicalAggregate, LogicalDelete,
        LogicalRead, LogicalRecord, LogicalResourceOp, LogicalSearch, LogicalWrite, ResourceKind,
        ResourceOpKind,
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
                    sql_type: "nvarchar(64)".into(),
                    is_primary: true,
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "name".into(),
                    column_name: "name".into(),
                    proto_type: "string".into(),
                    sql_type: "nvarchar(255)".into(),
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
    fn select_uses_brackets_and_at_p_placeholders() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("acme.billing.v1.Customer").with_filter(
            LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("abc".into()),
            },
        );
        let (statement, params) = sql(MssqlCompiler.compile_read(&read, &ctx).unwrap());
        assert_eq!(
            statement,
            "SELECT * FROM [billing].[customers] WHERE [id] = @P1"
        );
        assert_eq!(params, vec![LogicalValue::String("abc".into())]);
    }

    #[test]
    fn pagination_without_order_by_is_rejected() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("acme.billing.v1.Customer")
            .with_pagination(LogicalPagination::limit(10));
        let err = MssqlCompiler.compile_read(&read, &ctx).unwrap_err();
        assert!(matches!(err, CompileError::Malformed { .. }));
    }

    #[test]
    fn pagination_with_order_by_emits_offset_fetch_next() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("acme.billing.v1.Customer")
            .with_sort(vec![LogicalSort {
                field: "id".into(),
                direction: SortDirection::Asc,
                nulls: crate::ir::projection::NullOrder::Default,
            }])
            .with_pagination(LogicalPagination::page(5, 10));
        let (statement, _) = sql(MssqlCompiler.compile_read(&read, &ctx).unwrap());
        assert!(statement.contains("ORDER BY [id] ASC"));
        assert!(statement.contains("OFFSET 5 ROWS"));
        assert!(statement.contains("FETCH NEXT 10 ROWS ONLY"));
    }

    #[test]
    fn upsert_emits_merge_with_update_and_insert() {
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
        let (statement, _) = sql(MssqlCompiler.compile_write(&write, &ctx).unwrap());
        assert!(statement.starts_with("MERGE INTO [billing].[customers]"));
        assert!(statement.contains("ON target.[id] = source.[id]"));
        assert!(statement.contains("WHEN MATCHED THEN UPDATE SET target.[name] = source.[name]"));
        assert!(statement.contains("WHEN NOT MATCHED THEN"));
    }

    #[test]
    fn ignore_emits_merge_with_insert_only() {
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
        let (statement, _) = sql(MssqlCompiler.compile_write(&write, &ctx).unwrap());
        assert!(statement.contains("WHEN NOT MATCHED THEN"));
        assert!(!statement.contains("WHEN MATCHED"));
    }

    #[test]
    fn batch_upsert_rejected_with_typed_error() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec_a = LogicalRecord::new();
        rec_a.insert("id".into(), LogicalValue::String("a".into()));
        let mut rec_b = LogicalRecord::new();
        rec_b.insert("id".into(), LogicalValue::String("b".into()));
        let write = LogicalWrite {
            message_type: "acme.billing.v1.Customer".into(),
            records: vec![rec_a, rec_b],
            conflict: ConflictStrategy::Replace,
            return_fields: vec![],
        };
        let err = MssqlCompiler.compile_write(&write, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Mssql,
                op: "batch_upsert"
            }
        ));
    }

    #[test]
    fn aggregate_group_by_emits_bracketed_columns() {
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
            having: None,
            sort: vec![],
            pagination: None,
        };
        let (statement, _) = sql(MssqlCompiler.compile_aggregate(&agg, &ctx).unwrap());
        assert_eq!(
            statement,
            "SELECT [name], SUM([amount]) AS [total] FROM [billing].[customers] GROUP BY [name]"
        );
    }

    #[test]
    fn search_emits_contains_with_top() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let search = LogicalSearch {
            message_type: "acme.billing.v1.Customer".into(),
            vector: None,
            text_query: Some("alice".into()),
            filter: None,
            top_k: 10,
            score_threshold: None,
            require_hybrid: false,
            with_vector: false,
            with_payload: true,
        };
        let (statement, _) = sql(MssqlCompiler.compile_search(&search, &ctx).unwrap());
        assert!(statement.starts_with("SELECT TOP(10) * FROM [billing].[customers]"));
        assert!(statement.contains("WHERE CONTAINS(*, @P1)"));
    }

    #[test]
    fn search_vector_only_rejected() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let search = LogicalSearch {
            message_type: "acme.billing.v1.Customer".into(),
            vector: Some(vec![0.0]),
            text_query: None,
            filter: None,
            top_k: 5,
            score_threshold: None,
            require_hybrid: false,
            with_vector: false,
            with_payload: true,
        };
        let err = MssqlCompiler.compile_search(&search, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Mssql,
                op: "vector_search"
            }
        ));
    }

    #[test]
    fn resource_op_create_table_uses_object_id_guard() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let op = LogicalResourceOp {
            op: ResourceOpKind::Ensure,
            resource_kind: ResourceKind::Table,
            resource_name: "orders".into(),
            spec: Some(json!({
                "schema": "billing",
                "columns": [
                    {"name": "id", "type": "bigint", "not_null": true, "primary_key": true}
                ]
            })),
        };
        let (statement, _) = sql(MssqlCompiler.compile_resource_op(&op, &ctx).unwrap());
        assert!(statement.starts_with("IF NOT EXISTS"));
        assert!(statement.contains("sys.tables"));
        assert!(statement.contains("CREATE TABLE [billing].[orders]"));
        assert!(statement.contains("CONSTRAINT [PK_orders] PRIMARY KEY"));
    }

    #[test]
    fn delete_with_filter_emits_bracketed_where() {
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
        let (statement, _) = sql(MssqlCompiler.compile_delete(&del, &ctx).unwrap());
        assert_eq!(
            statement,
            "DELETE FROM [billing].[customers] WHERE [id] = @P1"
        );
    }
}
