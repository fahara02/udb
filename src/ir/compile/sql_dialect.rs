//! Generic SQL compiler core shared by the four relational backends
//! (Postgres, MySQL, SQLite, MSSQL) — the #12 dedup.
//!
//! The four `postgres`/`mysql`/`sqlite`/`mssql` compilers were ~90%
//! identical: the same `resolve_table` / `column_for` / `render_where` /
//! `render_filter` / `render_aggregate` / `resolve_sort_field` /
//! `render_having` / `resolve_having_field` / `sql_op_for` logic, differing
//! only along a handful of dialect axes. Those axes are captured by the
//! [`SqlDialect`] trait; everything structurally identical lives here on
//! [`SqlCompiler`] so each backend keeps only a small `impl SqlDialect`
//! plus its genuinely-divergent statement assembly (upsert idiom,
//! pagination idiom, search surface, DDL).
//!
//! The emitted SQL is byte-identical to the hand-written per-backend code:
//! every quote, placeholder, false-literal, and LIKE-escape idiom routes
//! through a `SqlDialect` hook whose output matches the original.

use crate::backend::BackendKind;
use crate::generation::ManifestTable;
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{AggregateExpr, AggregateFunc, LogicalAggregate};
use crate::ir::value::LogicalValue;

use super::CompileError;

/// The per-dialect hooks that distinguish the four relational compilers.
///
/// Only the axes that actually differ live here; everything else is shared
/// on [`SqlCompiler`]. Implementors are zero-sized marker types
/// (`Postgres`, `Mysql`, `Sqlite`, `Mssql`).
pub(super) trait SqlDialect {
    /// The backend this dialect targets (for typed error variants).
    fn backend() -> BackendKind;

    /// Quote an identifier: `"x"` (Postgres/SQLite), `` `x` `` (MySQL),
    /// `[x]` (MSSQL). Returns the full quoted token including delimiters.
    fn quote(ident: &str) -> String;

    /// Render the placeholder for the value that has just been pushed,
    /// where `index` is the 1-based position in the params vec
    /// (`params.len()` after the push). `$N` (Postgres), `?` (MySQL,
    /// SQLite), `@PN` (MSSQL).
    fn placeholder(index: usize) -> String;

    /// The literal a guaranteed-false WHERE body lowers to: `FALSE`
    /// (Postgres/MySQL), `0` (SQLite), `1=0` (MSSQL).
    fn false_literal() -> &'static str;

    /// HAVING-position true / false literals (an empty AND / empty OR).
    /// Postgres/MySQL use `TRUE`/`FALSE`; SQLite `1`/`0`; MSSQL `1=1`/`1=0`.
    fn having_true_literal() -> &'static str;
    fn having_false_literal() -> &'static str;

    /// Transform the RHS of a comparison for substring operators
    /// (`Contains`/`StartsWith`/`EndsWith`) — the LIKE-concat + escape
    /// idiom (`||` vs `CONCAT` vs `+`, plus the per-backend metacharacter
    /// escaping). For all other operators this returns `placeholder`
    /// unchanged. `placeholder` is the already-rendered placeholder token.
    fn wrap_value_for_op(op: ComparisonOp, placeholder: &str) -> String;

    /// Map a comparison operator to its SQL token. The default matches
    /// Postgres (which is the only dialect with a distinct `ILIKE`); the
    /// other three fold `ILike` into `LIKE` by overriding.
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
            | ComparisonOp::EndsWith => "LIKE",
            ComparisonOp::ILike => "ILIKE",
        }
    }
}

/// Generic SQL compiler parameterised over a [`SqlDialect`]. Holds no
/// state — it's a namespace for the shared lowering logic, called by each
/// backend's thin `Compiler` impl.
pub(super) struct SqlCompiler<D: SqlDialect> {
    _dialect: std::marker::PhantomData<D>,
}

impl<D: SqlDialect> SqlCompiler<D> {
    /// Resolve a logical message type to its physical manifest table.
    pub(super) fn resolve_table<'a>(
        message_type: &str,
        manifest: &'a crate::generation::CatalogManifest,
    ) -> Result<&'a ManifestTable, CompileError> {
        crate::broker::table_for_message(manifest, message_type).ok_or_else(|| {
            CompileError::UnknownMessageType {
                message_type: message_type.to_string(),
            }
        })
    }

    /// Map an IR field name to the manifest column name (field-name or
    /// column-name match, case-insensitive on the field name).
    pub(super) fn column_for<'a>(
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

    /// Push a value and return its dialect placeholder token.
    pub(super) fn push_param(params: &mut Vec<LogicalValue>, v: LogicalValue) -> String {
        params.push(v);
        D::placeholder(params.len())
    }

    /// Render the `WHERE ...` body. Returns `None` for an empty filter so
    /// the caller can decide whether to emit `WHERE` at all; an empty OR
    /// lowers to the dialect false-literal.
    pub(super) fn render_where(
        filter: &LogicalFilter,
        table: &ManifestTable,
        message_type: &str,
        params: &mut Vec<LogicalValue>,
    ) -> Result<Option<String>, CompileError> {
        match filter {
            LogicalFilter::And(clauses) if clauses.is_empty() => Ok(None),
            LogicalFilter::Or(clauses) if clauses.is_empty() => {
                Ok(Some(D::false_literal().to_string()))
            }
            _ => Ok(Some(Self::render_filter(
                filter,
                table,
                message_type,
                params,
            )?)),
        }
    }

    /// Render a filter expression. Always non-empty; callers dispatch to
    /// `render_where` for the empty-filter case.
    pub(super) fn render_filter(
        filter: &LogicalFilter,
        table: &ManifestTable,
        message_type: &str,
        params: &mut Vec<LogicalValue>,
    ) -> Result<String, CompileError> {
        match filter {
            LogicalFilter::And(clauses) => {
                let parts = clauses
                    .iter()
                    .map(|c| Self::render_filter(c, table, message_type, params))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(format!("({})", parts.join(" AND ")))
            }
            LogicalFilter::Or(clauses) => {
                let parts = clauses
                    .iter()
                    .map(|c| Self::render_filter(c, table, message_type, params))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(format!("({})", parts.join(" OR ")))
            }
            LogicalFilter::Not(inner) => {
                let rendered = Self::render_filter(inner, table, message_type, params)?;
                Ok(format!("(NOT {rendered})"))
            }
            LogicalFilter::Comparison { field, op, value } => {
                let column = Self::column_for(table, field, message_type)?;
                // SQL surprise: `col = NULL` is always UNKNOWN. Reject it
                // here so callers explicitly use `IsNull`.
                if value.is_null() {
                    return Err(CompileError::Malformed {
                        reason: format!(
                            "comparison with NULL on field '{field}' must use IsNull, not {}",
                            op.token()
                        ),
                    });
                }
                let placeholder = Self::push_param(params, value.clone());
                let sql_op = D::sql_op_for(*op);
                let rhs = D::wrap_value_for_op(*op, &placeholder);
                Ok(format!("{} {sql_op} {rhs}", D::quote(column)))
            }
            LogicalFilter::IsNull(field) => {
                let column = Self::column_for(table, field, message_type)?;
                Ok(format!("{} IS NULL", D::quote(column)))
            }
            LogicalFilter::InList { field, values } => {
                if values.is_empty() {
                    return Ok(D::false_literal().to_string());
                }
                for value in values {
                    if matches!(value, LogicalValue::Array(_) | LogicalValue::Json(_)) {
                        return Err(CompileError::Malformed {
                            reason: format!(
                                "IN list for field '{field}' accepts scalar values only; got {}",
                                value.type_token()
                            ),
                        });
                    }
                    if value.is_null() {
                        return Err(CompileError::Malformed {
                            reason: format!(
                                "IN list for field '{field}' cannot contain NULL; use IsNull explicitly"
                            ),
                        });
                    }
                }
                let column = Self::column_for(table, field, message_type)?;
                let placeholders = values
                    .iter()
                    .map(|v| Self::push_param(params, v.clone()))
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{} IN ({placeholders})", D::quote(column)))
            }
        }
    }

    /// Render one aggregate expression as `FUNC(arg) AS "alias"`.
    pub(super) fn render_aggregate(
        agg: &AggregateExpr,
        table: &ManifestTable,
        message_type: &str,
    ) -> Result<String, CompileError> {
        let token = agg.func.sql_token();
        let body = match agg.func {
            AggregateFunc::Count if agg.field == "*" => "*".to_string(),
            AggregateFunc::Count => {
                let col = Self::column_for(table, &agg.field, message_type)?;
                D::quote(col)
            }
            AggregateFunc::CountDistinct => {
                if agg.field == "*" {
                    return Err(CompileError::Malformed {
                        reason: "COUNT(DISTINCT *) is not allowed; specify a field".into(),
                    });
                }
                let col = Self::column_for(table, &agg.field, message_type)?;
                format!("DISTINCT {}", D::quote(col))
            }
            AggregateFunc::Sum | AggregateFunc::Avg | AggregateFunc::Min | AggregateFunc::Max => {
                if agg.field == "*" {
                    return Err(CompileError::Malformed {
                        reason: format!("{} requires a field name, not '*'", agg.func.sql_token()),
                    });
                }
                let col = Self::column_for(table, &agg.field, message_type)?;
                D::quote(col)
            }
        };
        Ok(format!("{token}({body}) AS {}", D::quote(&agg.alias)))
    }

    /// Resolve a field-or-alias for ORDER BY in an aggregate query:
    /// aggregate aliases win, then group-by manifest columns. Anything
    /// else is rejected (can't sort by an ungrouped raw column).
    pub(super) fn resolve_sort_field(
        field: &str,
        op: &LogicalAggregate,
        table: &ManifestTable,
    ) -> Result<String, CompileError> {
        if op.aggregates.iter().any(|a| a.alias == field) {
            return Ok(D::quote(field));
        }
        if op.group_by.iter().any(|f| f == field) {
            let col = Self::column_for(table, field, &op.message_type)?;
            return Ok(D::quote(col));
        }
        Err(CompileError::Malformed {
            reason: format!(
                "ORDER BY field '{field}' is neither an aggregate alias nor a GROUP BY column; \
                 aggregated queries can only sort by grouped or computed values"
            ),
        })
    }

    /// Render a HAVING filter — like `render_filter` but field references
    /// resolve to aggregate aliases / group-by columns rather than raw
    /// manifest columns.
    pub(super) fn render_having(
        filter: &LogicalFilter,
        op: &LogicalAggregate,
        table: &ManifestTable,
        params: &mut Vec<LogicalValue>,
    ) -> Result<String, CompileError> {
        match filter {
            LogicalFilter::And(clauses) if clauses.is_empty() => {
                Ok(D::having_true_literal().to_string())
            }
            LogicalFilter::Or(clauses) if clauses.is_empty() => {
                Ok(D::having_false_literal().to_string())
            }
            LogicalFilter::And(clauses) => {
                let parts = clauses
                    .iter()
                    .map(|c| Self::render_having(c, op, table, params))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(format!("({})", parts.join(" AND ")))
            }
            LogicalFilter::Or(clauses) => {
                let parts = clauses
                    .iter()
                    .map(|c| Self::render_having(c, op, table, params))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(format!("({})", parts.join(" OR ")))
            }
            LogicalFilter::Not(inner) => {
                let rendered = Self::render_having(inner, op, table, params)?;
                Ok(format!("(NOT {rendered})"))
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
                let token = Self::resolve_having_field(field, op, table, &op.message_type)?;
                let placeholder = Self::push_param(params, value.clone());
                let sql_op = D::sql_op_for(*cmp);
                let rhs = D::wrap_value_for_op(*cmp, &placeholder);
                Ok(format!("{token} {sql_op} {rhs}"))
            }
            LogicalFilter::IsNull(field) => {
                let token = Self::resolve_having_field(field, op, table, &op.message_type)?;
                Ok(format!("{token} IS NULL"))
            }
            LogicalFilter::InList { field, values } => {
                if values.is_empty() {
                    return Ok(D::having_false_literal().to_string());
                }
                let token = Self::resolve_having_field(field, op, table, &op.message_type)?;
                let placeholders = values
                    .iter()
                    .map(|v| Self::push_param(params, v.clone()))
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{token} IN ({placeholders})"))
            }
        }
    }

    /// Resolve a HAVING field reference: aggregate alias or group-by
    /// column, both re-emitted as the quoted identifier (every dialect
    /// references the column/alias expression, not a positional index).
    pub(super) fn resolve_having_field(
        field: &str,
        op: &LogicalAggregate,
        table: &ManifestTable,
        message_type: &str,
    ) -> Result<String, CompileError> {
        if op.aggregates.iter().any(|a| a.alias == field) {
            return Ok(D::quote(field));
        }
        if op.group_by.iter().any(|f| f == field) {
            let column = Self::column_for(table, field, message_type)?;
            return Ok(D::quote(column));
        }
        Err(CompileError::Malformed {
            reason: format!(
                "HAVING field '{field}' is neither an aggregate alias nor a GROUP BY column"
            ),
        })
    }
}
