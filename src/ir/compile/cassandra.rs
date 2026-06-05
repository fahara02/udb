//! Cassandra / ScyllaDB CQL compiler (C9).
//!
//! Lowers IR ops to Cassandra Query Language. CQL is SQL-shaped but
//! the wide-column model imposes hard constraints:
//!
//! - **Primary key is partition key + clustering keys.** Filters that
//!   don't restrict the partition key require `ALLOW FILTERING` and
//!   trigger full-cluster scans — the compiler refuses non-PK
//!   predicates by default and surfaces `OperatorUnsupported` so
//!   operators can opt in via explicit `ALLOW FILTERING` (not done
//!   here — Cassandra's worst footgun).
//! - **No JOINs**. Single-table operations only.
//! - **No transactions across rows.** LWT (`IF NOT EXISTS`) is
//!   per-row atomic; `BEGIN BATCH` groups single-partition writes.
//! - **`UPDATE` and `INSERT` are both upserts** — Cassandra writes
//!   are time-stamped overwrites. `ConflictStrategy::Ignore` is `IF
//!   NOT EXISTS` (LWT). `Replace`/`Update` is plain INSERT (which
//!   IS upsert in CQL).
//! - **Aggregate** is limited to `COUNT(*)`, `MIN`, `MAX`, `SUM`,
//!   `AVG`. `COUNT(DISTINCT)` isn't supported.
//! - **Search** is rejected — Cassandra has SASI/SAI indexes but
//!   they're version-specific and operator-installed. Route to
//!   Elasticsearch alongside.
//!
//! Dialect:
//! - Identifiers quoted with `"..."` (case-sensitive when quoted).
//! - Parameters are positional `?` placeholders.
//! - Keyspace + table separated by `.`.

use crate::backend::BackendKind;
use crate::generation::ManifestTable;
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    AggregateExpr, AggregateFunc, ConflictStrategy, LogicalAggregate, LogicalDelete, LogicalRead,
    LogicalResourceOp, LogicalSearch, LogicalWrite, ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;

use super::{CompileContext, CompileError, CompiledRendering, Compiler};

#[derive(Debug, Default, Clone, Copy)]
pub struct CassandraCompiler;

impl CassandraCompiler {
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

    /// Check that every column referenced in a WHERE clause is part of
    /// the table's primary key. Cassandra refuses non-PK filters
    /// without `ALLOW FILTERING` — we surface the typed error rather
    /// than emit the magic clause silently.
    fn validate_pk_filter(
        &self,
        filter: &LogicalFilter,
        table: &ManifestTable,
        message_type: &str,
    ) -> Result<(), CompileError> {
        // Resolve the PK field names to their column names once, up front — the
        // per-leaf membership check is then an O(1) set lookup instead of a nested
        // `primary_key × columns` scan per filter leaf (#95).
        let mut pk_columns = std::collections::HashSet::new();
        for pk in &table.primary_key {
            pk_columns.insert(self.column_for(table, pk, message_type)?);
        }
        self.validate_pk_filter_inner(filter, table, message_type, &pk_columns)
    }

    fn validate_pk_filter_inner(
        &self,
        filter: &LogicalFilter,
        table: &ManifestTable,
        message_type: &str,
        pk_columns: &std::collections::HashSet<&str>,
    ) -> Result<(), CompileError> {
        match filter {
            LogicalFilter::And(clauses) | LogicalFilter::Or(clauses) => {
                for c in clauses {
                    self.validate_pk_filter_inner(c, table, message_type, pk_columns)?;
                }
                Ok(())
            }
            LogicalFilter::Not(inner) => {
                self.validate_pk_filter_inner(inner, table, message_type, pk_columns)
            }
            LogicalFilter::Comparison { field, .. }
            | LogicalFilter::IsNull(field)
            | LogicalFilter::InList { field, .. } => {
                let col = self.column_for(table, field, message_type)?;
                if !pk_columns.contains(col) {
                    return Err(CompileError::OperatorUnsupported {
                        backend: BackendKind::Cassandra,
                        op: "non_pk_filter",
                    });
                }
                Ok(())
            }
        }
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
            LogicalFilter::Or(c) if c.is_empty() => Ok(Some("false".to_string())),
            _ => Ok(Some(self.render_filter(
                filter,
                table,
                message_type,
                params,
            )?)),
        }
    }

    /// A1: Does the manifest declare a `tenant_id` column? If yes,
    /// Cassandra RLS can be ENFORCED by auto-AND-ing
    /// `tenant_id = ?` into every WHERE. If no, the operator's
    /// schema doesn't model multi-tenancy at the row level and the
    /// compiler falls back to advisory mode (no injection).
    ///
    /// Resolution policy is shared with the other compiler-layer-RLS
    /// backends — see `util::resolve_tenant_column`. Both `tenant_id`
    /// and `_tenant_id` are accepted (and the `is_tenant_column`
    /// manifest flag wins when set).
    fn tenant_column(table: &ManifestTable) -> Option<&str> {
        super::util::resolve_tenant_column(table)
    }

    /// Same for project_id — second optional injection layer.
    fn project_column(table: &ManifestTable) -> Option<&str> {
        super::util::resolve_project_column(table)
    }

    /// A1: append tenant + project predicates to an existing WHERE
    /// body. Returns the augmented WHERE expression OR `None` when
    /// context is empty / table has no tenant/project columns.
    /// Each injection adds one `?` placeholder to `params`. Cassandra
    /// quotes identifiers with double-quotes.
    fn append_context_predicates(
        body: Option<String>,
        table: &ManifestTable,
        ctx: &CompileContext<'_>,
        params: &mut Vec<LogicalValue>,
    ) -> Option<String> {
        super::util::append_context_predicates(body, table, ctx, params, '"')
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
                Ok(parts.join(" AND "))
            }
            LogicalFilter::Or(_) => Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Cassandra,
                op: "or_filter",
            }),
            LogicalFilter::Not(_) => Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Cassandra,
                op: "not_filter",
            }),
            LogicalFilter::IsNull(_) => Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Cassandra,
                op: "is_null_filter",
            }),
            LogicalFilter::Comparison { field, op, value } => {
                let column = self.column_for(table, field, message_type)?;
                if value.is_null() {
                    return Err(CompileError::Malformed {
                        reason: format!("CQL comparison with NULL on '{field}' is invalid"),
                    });
                }
                params.push(value.clone());
                let cql_op = match op {
                    ComparisonOp::Eq => "=",
                    ComparisonOp::Ne => {
                        return Err(CompileError::OperatorUnsupported {
                            backend: BackendKind::Cassandra,
                            op: "ne_predicate",
                        });
                    }
                    ComparisonOp::Lt => "<",
                    ComparisonOp::Le => "<=",
                    ComparisonOp::Gt => ">",
                    ComparisonOp::Ge => ">=",
                    _ => {
                        return Err(CompileError::OperatorUnsupported {
                            backend: BackendKind::Cassandra,
                            op: "text_match_predicate",
                        });
                    }
                };
                Ok(format!("\"{column}\" {cql_op} ?"))
            }
            LogicalFilter::InList { field, values } => {
                if values.is_empty() {
                    return Ok("false".to_string());
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

impl Compiler for CassandraCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::Cassandra
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

        let mut cql = format!(
            "SELECT {select} FROM \"{ks}\".\"{table}\"",
            ks = table.schema,
            table = table.table,
        );

        let user_where: Option<String> = if let Some(filter) = &op.filter {
            self.validate_pk_filter(filter, table, &op.message_type)?;
            self.render_where(filter, table, &op.message_type, &mut params)?
        } else {
            None
        };
        // A1: AND tenant_id/project_id predicates into the WHERE
        // when the manifest models them as columns and context
        // carries values. Promotes RLS from Advisory to Enforced.
        if let Some(body) = Self::append_context_predicates(user_where, table, ctx, &mut params) {
            cql.push_str(&format!(" WHERE {body}"));
        }

        // CQL's clustering-key ORDER BY is constrained to the table's
        // clustering order; for simplicity we emit ORDER BY and let
        // the server validate.
        if !op.sort.is_empty() {
            let parts = op
                .sort
                .iter()
                .map(|s| {
                    let col = self.column_for(table, &s.field, &op.message_type)?;
                    let dir = s.direction.token().to_uppercase();
                    Ok(format!("\"{col}\" {dir}"))
                })
                .collect::<Result<Vec<_>, CompileError>>()?;
            cql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                // CQL paging tokens are server-managed (driver hands
                // a `PagingState`). Caller-supplied cursors aren't
                // a thing — refuse explicitly.
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Cassandra,
                    op: "explicit_cursor_pagination",
                });
            }
            if let Some(limit) = pag.limit {
                cql.push_str(&format!(" LIMIT {limit}"));
            }
            // CQL has no OFFSET — the driver's PagingState handles it.
            if pag.offset.is_some_and(|o| o > 0) {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Cassandra,
                    op: "offset_pagination",
                });
            }
        }

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Cassandra,
            statement: cql,
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
        if op.records.len() > 1 {
            // BATCH writes are possible (BEGIN BATCH ... APPLY BATCH)
            // but only safe within a single partition. Refuse multi-
            // record by default — caller can issue N single writes.
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Cassandra,
                op: "batch_write",
            });
        }
        let table = self.resolve_table(&op.message_type, ctx)?;
        let mut params: Vec<LogicalValue> = Vec::new();

        // A1: stamp tenant_id / project_id onto the record if the
        // table models them and the caller hasn't supplied an
        // explicit value. Don't overwrite a caller-provided value;
        // that's a forgery attempt the runtime cross-check should
        // raise — here we just don't clobber it silently.
        let mut record = op.records[0].clone();
        if let Some(tid) = ctx.tenant_id
            && !tid.is_empty()
            && let Some(col) = Self::tenant_column(table)
            && !record.contains_key("tenant_id")
            && !record.contains_key(col)
        {
            record.insert("tenant_id".into(), LogicalValue::String(tid.to_string()));
        }
        if let Some(pid) = ctx.project_id
            && !pid.is_empty()
            && let Some(col) = Self::project_column(table)
            && !record.contains_key("project_id")
            && !record.contains_key(col)
        {
            record.insert("project_id".into(), LogicalValue::String(pid.to_string()));
        }
        let columns: Vec<&str> = record
            .keys()
            .map(|k| self.column_for(table, k, &op.message_type))
            .collect::<Result<Vec<_>, _>>()?;
        let column_list = columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = record
            .keys()
            .map(|k| {
                params.push(record[k].clone());
                "?".to_string()
            })
            .collect::<Vec<_>>()
            .join(", ");

        // Cassandra INSERT is an UPSERT by default; `Replace`/`Update`
        // is the same statement. `Error` is a no-op (we can't refuse
        // duplicates without LWT). `Ignore` adds `IF NOT EXISTS`.
        let mut cql = format!(
            "INSERT INTO \"{ks}\".\"{table}\" ({column_list}) VALUES ({placeholders})",
            ks = table.schema,
            table = table.table,
        );
        if matches!(op.conflict, ConflictStrategy::Ignore) {
            cql.push_str(" IF NOT EXISTS");
        }

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Cassandra,
            statement: cql,
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
        self.validate_pk_filter(&op.filter, table, &op.message_type)?;
        let body = self
            .render_where(&op.filter, table, &op.message_type, &mut params)?
            .ok_or_else(|| CompileError::Malformed {
                reason: "LogicalDelete::filter cannot be empty".into(),
            })?;
        if body == "false" {
            return Err(CompileError::Malformed {
                reason: "LogicalDelete::filter resolves to FALSE; refusing no-op delete".into(),
            });
        }
        // A1: AND tenant/project predicates so cross-tenant
        // DELETE-by-id is impossible at the CQL layer.
        let final_body = Self::append_context_predicates(Some(body), table, ctx, &mut params)
            .expect("body was Some so result is Some");
        let cql = format!(
            "DELETE FROM \"{ks}\".\"{table}\" WHERE {body}",
            ks = table.schema,
            table = table.table,
            body = final_body,
        );
        Ok(CompiledRendering::Sql {
            backend: BackendKind::Cassandra,
            statement: cql,
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
        for agg in &op.aggregates {
            if matches!(agg.func, AggregateFunc::CountDistinct) {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Cassandra,
                    op: "count_distinct",
                });
            }
        }
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
                Ok::<_, CompileError>(format!("\"{col}\""))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for col in &group_columns {
            select_parts.push(col.clone());
        }
        for agg in &op.aggregates {
            select_parts.push(render_cql_aggregate(agg, table, &op.message_type)?);
        }
        let mut cql = format!(
            "SELECT {sel} FROM \"{ks}\".\"{table}\"",
            sel = select_parts.join(", "),
            ks = table.schema,
            table = table.table,
        );

        let user_where: Option<String> = if let Some(filter) = &op.filter {
            self.validate_pk_filter(filter, table, &op.message_type)?;
            self.render_where(filter, table, &op.message_type, &mut params)?
        } else {
            None
        };
        if let Some(body) = Self::append_context_predicates(user_where, table, ctx, &mut params) {
            cql.push_str(&format!(" WHERE {body}"));
        }

        // CQL GROUP BY requires the columns to be a prefix of the
        // primary key. The server validates this; we just emit.
        if !group_columns.is_empty() {
            cql.push_str(&format!(" GROUP BY {}", group_columns.join(", ")));
        }

        // CQL has no HAVING; reject if the caller supplies one.
        if op.having.is_some() {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Cassandra,
                op: "having_clause",
            });
        }

        if let Some(pag) = &op.pagination
            && let Some(limit) = pag.limit
        {
            cql.push_str(&format!(" LIMIT {limit}"));
        }

        Ok(CompiledRendering::Sql {
            backend: BackendKind::Cassandra,
            statement: cql,
            params,
        })
    }

    fn compile_search(
        &self,
        _op: &LogicalSearch,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        Err(CompileError::OperatorUnsupported {
            backend: BackendKind::Cassandra,
            op: "search",
        })
    }

    fn compile_resource_op(
        &self,
        op: &LogicalResourceOp,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        if !matches!(op.resource_kind, ResourceKind::Table | ResourceKind::Index) {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Cassandra,
                op: "non_table_resource",
            });
        }
        let cql = match (op.op, op.resource_kind) {
            (ResourceOpKind::Ensure, ResourceKind::Table) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "Ensure Table requires a spec".into(),
                })?;
                let ks = spec
                    .get("keyspace")
                    .or_else(|| spec.get("schema"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");
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
                    let pk = c
                        .get("primary_key")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    if pk {
                        pk_cols.push(format!("\"{name}\""));
                    }
                    column_defs.push(format!("\"{name}\" {ty}"));
                }
                if !pk_cols.is_empty() {
                    column_defs.push(format!("PRIMARY KEY ({})", pk_cols.join(", ")));
                }
                format!(
                    "CREATE TABLE IF NOT EXISTS \"{ks}\".\"{name}\" ({defs})",
                    name = op.resource_name,
                    defs = column_defs.join(", "),
                )
            }
            (ResourceOpKind::Drop, ResourceKind::Table) => {
                let ks = op
                    .spec
                    .as_ref()
                    .and_then(|s| s.get("keyspace").or_else(|| s.get("schema")))
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");
                format!("DROP TABLE IF EXISTS \"{ks}\".\"{}\"", op.resource_name)
            }
            (ResourceOpKind::List, ResourceKind::Table) => {
                let ks = op
                    .spec
                    .as_ref()
                    .and_then(|s| s.get("keyspace").or_else(|| s.get("schema")))
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");
                format!("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{ks}'")
            }
            (ResourceOpKind::Ensure, ResourceKind::Index) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "Ensure Index requires a spec".into(),
                })?;
                let ks = spec
                    .get("keyspace")
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");
                let tbl = spec.get("table").and_then(|v| v.as_str()).ok_or_else(|| {
                    CompileError::Malformed {
                        reason: "Ensure Index spec missing 'table'".into(),
                    }
                })?;
                let col = spec.get("column").and_then(|v| v.as_str()).ok_or_else(|| {
                    CompileError::Malformed {
                        reason:
                            "Ensure Index spec missing 'column' (CQL indexes are single-column)"
                                .into(),
                    }
                })?;
                format!(
                    "CREATE INDEX IF NOT EXISTS \"{name}\" ON \"{ks}\".\"{tbl}\" (\"{col}\")",
                    name = op.resource_name,
                )
            }
            (ResourceOpKind::Drop, ResourceKind::Index) => {
                let ks = op
                    .spec
                    .as_ref()
                    .and_then(|s| s.get("keyspace"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");
                format!("DROP INDEX IF EXISTS \"{ks}\".\"{}\"", op.resource_name)
            }
            (ResourceOpKind::List, ResourceKind::Index) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "List Index requires a spec with 'table'".into(),
                })?;
                let ks = spec
                    .get("keyspace")
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");
                let tbl = spec.get("table").and_then(|v| v.as_str()).ok_or_else(|| {
                    CompileError::Malformed {
                        reason: "List Index spec missing 'table'".into(),
                    }
                })?;
                format!(
                    "SELECT index_name FROM system_schema.indexes WHERE keyspace_name = '{ks}' AND table_name = '{tbl}'"
                )
            }
            _ => {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Cassandra,
                    op: "unhandled_resource_op",
                });
            }
        };
        Ok(CompiledRendering::Sql {
            backend: BackendKind::Cassandra,
            statement: cql,
            params: Vec::new(),
        })
    }
}

fn render_cql_aggregate(
    agg: &AggregateExpr,
    table: &ManifestTable,
    message_type: &str,
) -> Result<String, CompileError> {
    let body = match agg.func {
        AggregateFunc::Count if agg.field == "*" => "COUNT(*)".to_string(),
        AggregateFunc::Count => {
            let col = resolve_column(table, &agg.field, message_type)?;
            format!("COUNT(\"{col}\")")
        }
        AggregateFunc::Sum | AggregateFunc::Avg | AggregateFunc::Min | AggregateFunc::Max => {
            if agg.field == "*" {
                return Err(CompileError::Malformed {
                    reason: format!("{} requires a field name, not '*'", agg.func.sql_token()),
                });
            }
            let col = resolve_column(table, &agg.field, message_type)?;
            format!("{}(\"{col}\")", agg.func.sql_token())
        }
        AggregateFunc::CountDistinct => unreachable!("rejected earlier"),
    };
    Ok(format!("{body} AS \"{}\"", agg.alias))
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
        AggregateExpr, AggregateFunc, ConflictStrategy, LogicalAggregate, LogicalDelete,
        LogicalRead, LogicalRecord, LogicalResourceOp, LogicalWrite, ResourceKind, ResourceOpKind,
    };
    use crate::ir::value::LogicalValue;
    use serde_json::json;

    fn fixture() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.events.v1.Event".into(),
            schema: "events".into(), // keyspace
            table: "events".into(),
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
                    field_name: "title".into(),
                    column_name: "title".into(),
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

    fn sql(r: CompiledRendering) -> (String, Vec<LogicalValue>) {
        match r {
            CompiledRendering::Sql {
                statement, params, ..
            } => (statement, params),
            other => panic!("expected Sql, got {other:?}"),
        }
    }

    #[test]
    fn read_pk_emits_select_with_double_quotes() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read =
            LogicalRead::message("acme.events.v1.Event").with_filter(LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("e1".into()),
            });
        let (cql, params) = sql(CassandraCompiler.compile_read(&read, &ctx).unwrap());
        assert_eq!(cql, "SELECT * FROM \"events\".\"events\" WHERE \"id\" = ?");
        assert_eq!(params, vec![LogicalValue::String("e1".into())]);
    }

    #[test]
    fn non_pk_filter_is_rejected() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read =
            LogicalRead::message("acme.events.v1.Event").with_filter(LogicalFilter::Comparison {
                field: "title".into(), // not in PK
                op: ComparisonOp::Eq,
                value: LogicalValue::String("foo".into()),
            });
        let err = CassandraCompiler.compile_read(&read, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Cassandra,
                op: "non_pk_filter"
            }
        ));
    }

    #[test]
    fn write_is_upsert_by_default() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("e1".into()));
        rec.insert("title".into(), LogicalValue::String("hi".into()));
        let write = LogicalWrite {
            message_type: "acme.events.v1.Event".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Replace,
            return_fields: vec![],
        };
        let (cql, _) = sql(CassandraCompiler.compile_write(&write, &ctx).unwrap());
        assert!(cql.starts_with("INSERT INTO \"events\".\"events\""));
        assert!(!cql.contains("IF NOT EXISTS"));
    }

    #[test]
    fn ignore_adds_if_not_exists() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("e1".into()));
        let write = LogicalWrite {
            message_type: "acme.events.v1.Event".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Ignore,
            return_fields: vec![],
        };
        let (cql, _) = sql(CassandraCompiler.compile_write(&write, &ctx).unwrap());
        assert!(cql.ends_with("IF NOT EXISTS"));
    }

    #[test]
    fn delete_pk_emits_correct_cql() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let del = LogicalDelete {
            message_type: "acme.events.v1.Event".into(),
            filter: LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("e1".into()),
            },
            return_fields: vec![],
        };
        let (cql, _) = sql(CassandraCompiler.compile_delete(&del, &ctx).unwrap());
        assert_eq!(cql, "DELETE FROM \"events\".\"events\" WHERE \"id\" = ?");
    }

    #[test]
    fn search_rejected() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let s = crate::ir::operations::LogicalSearch {
            message_type: "acme.events.v1.Event".into(),
            vector: Some(vec![0.0]),
            text_query: None,
            filter: None,
            top_k: 1,
            score_threshold: None,
            require_hybrid: false,
            with_vector: false,
            with_payload: false,
        };
        let err = CassandraCompiler.compile_search(&s, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Cassandra,
                op: "search"
            }
        ));
    }

    #[test]
    fn count_distinct_rejected() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let agg = LogicalAggregate {
            message_type: "acme.events.v1.Event".into(),
            filter: None,
            group_by: vec![],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::CountDistinct,
                field: "title".into(),
                alias: "n".into(),
            }],
            having: None,
            sort: vec![],
            pagination: None,
        };
        let err = CassandraCompiler.compile_aggregate(&agg, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Cassandra,
                op: "count_distinct"
            }
        ));
    }

    #[test]
    fn count_all_emits_cql() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let agg = LogicalAggregate {
            message_type: "acme.events.v1.Event".into(),
            filter: None,
            group_by: vec![],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Count,
                field: "*".into(),
                alias: "total".into(),
            }],
            having: None,
            sort: vec![],
            pagination: None,
        };
        let (cql, _) = sql(CassandraCompiler.compile_aggregate(&agg, &ctx).unwrap());
        assert_eq!(
            cql,
            "SELECT COUNT(*) AS \"total\" FROM \"events\".\"events\""
        );
    }

    #[test]
    fn offset_pagination_rejected() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("acme.events.v1.Event")
            .with_pagination(crate::ir::projection::LogicalPagination::page(10, 5));
        let err = CassandraCompiler.compile_read(&read, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Cassandra,
                op: "offset_pagination"
            }
        ));
    }

    #[test]
    fn batch_write_rejected() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut a = LogicalRecord::new();
        a.insert("id".into(), LogicalValue::String("a".into()));
        let mut b = LogicalRecord::new();
        b.insert("id".into(), LogicalValue::String("b".into()));
        let write = LogicalWrite {
            message_type: "acme.events.v1.Event".into(),
            records: vec![a, b],
            conflict: ConflictStrategy::Replace,
            return_fields: vec![],
        };
        let err = CassandraCompiler.compile_write(&write, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Cassandra,
                op: "batch_write"
            }
        ));
    }

    /// A1: when the manifest has a `tenant_id` column and the
    /// compile context carries a tenant_id, every read AND-injects
    /// the predicate so cross-tenant statements are impossible.
    fn tenant_fixture() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.events.v1.Event".into(),
            schema: "events".into(),
            table: "events".into(),
            primary_key: vec!["tenant_id".into(), "id".into()],
            columns: vec![
                ManifestColumn {
                    field_name: "tenant_id".into(),
                    column_name: "tenant_id".into(),
                    proto_type: "string".into(),
                    sql_type: "text".into(),
                    is_primary: true,
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "id".into(),
                    column_name: "id".into(),
                    proto_type: "string".into(),
                    sql_type: "uuid".into(),
                    is_primary: true,
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "title".into(),
                    column_name: "title".into(),
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

    #[test]
    fn a1_read_with_tenant_ctx_injects_predicate() {
        let m = tenant_fixture();
        let mut ctx = CompileContext::new(&m);
        ctx.tenant_id = Some("acme");
        let read =
            LogicalRead::message("acme.events.v1.Event").with_filter(LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("e1".into()),
            });
        let (cql, params) = sql(CassandraCompiler.compile_read(&read, &ctx).unwrap());
        assert_eq!(
            cql,
            "SELECT * FROM \"events\".\"events\" WHERE \"id\" = ? AND \"tenant_id\" = ?"
        );
        assert_eq!(
            params,
            vec![
                LogicalValue::String("e1".into()),
                LogicalValue::String("acme".into()),
            ]
        );
    }

    #[test]
    fn a1_read_without_tenant_column_no_injection() {
        // fixture() table has no tenant_id column
        let m = fixture();
        let mut ctx = CompileContext::new(&m);
        ctx.tenant_id = Some("acme");
        let read =
            LogicalRead::message("acme.events.v1.Event").with_filter(LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("e1".into()),
            });
        let (cql, _) = sql(CassandraCompiler.compile_read(&read, &ctx).unwrap());
        // No tenant_id column in the manifest -> no injection.
        assert_eq!(cql, "SELECT * FROM \"events\".\"events\" WHERE \"id\" = ?");
    }

    #[test]
    fn a1_write_stamps_tenant_id_when_absent() {
        let m = tenant_fixture();
        let mut ctx = CompileContext::new(&m);
        ctx.tenant_id = Some("acme");
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("e1".into()));
        rec.insert("title".into(), LogicalValue::String("hi".into()));
        let write = LogicalWrite {
            message_type: "acme.events.v1.Event".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Replace,
            return_fields: vec![],
        };
        let (cql, params) = sql(CassandraCompiler.compile_write(&write, &ctx).unwrap());
        // BTreeMap sorts keys alphabetically: id, tenant_id, title
        assert!(cql.contains("\"tenant_id\""));
        assert!(params.contains(&LogicalValue::String("acme".into())));
    }

    #[test]
    fn a1_delete_with_tenant_ctx_injects_predicate() {
        let m = tenant_fixture();
        let mut ctx = CompileContext::new(&m);
        ctx.tenant_id = Some("acme");
        let del = LogicalDelete {
            message_type: "acme.events.v1.Event".into(),
            filter: LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("e1".into()),
            },
            return_fields: vec![],
        };
        let (cql, params) = sql(CassandraCompiler.compile_delete(&del, &ctx).unwrap());
        assert_eq!(
            cql,
            "DELETE FROM \"events\".\"events\" WHERE \"id\" = ? AND \"tenant_id\" = ?"
        );
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn a1_aggregate_with_tenant_ctx_injects_predicate() {
        let m = tenant_fixture();
        let mut ctx = CompileContext::new(&m);
        ctx.tenant_id = Some("acme");
        let agg = LogicalAggregate {
            message_type: "acme.events.v1.Event".into(),
            filter: None,
            group_by: vec![],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Count,
                field: "*".into(),
                alias: "total".into(),
            }],
            having: None,
            sort: vec![],
            pagination: None,
        };
        let (cql, params) = sql(CassandraCompiler.compile_aggregate(&agg, &ctx).unwrap());
        assert_eq!(
            cql,
            "SELECT COUNT(*) AS \"total\" FROM \"events\".\"events\" WHERE \"tenant_id\" = ?"
        );
        assert_eq!(params, vec![LogicalValue::String("acme".into())]);
    }

    #[test]
    fn create_table_emits_cql_ddl() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let op = LogicalResourceOp {
            op: ResourceOpKind::Ensure,
            resource_kind: ResourceKind::Table,
            resource_name: "users".into(),
            spec: Some(json!({
                "keyspace": "myks",
                "columns": [
                    {"name": "id", "type": "uuid", "primary_key": true},
                    {"name": "email", "type": "text"}
                ]
            })),
        };
        let (cql, _) = sql(CassandraCompiler.compile_resource_op(&op, &ctx).unwrap());
        assert!(cql.starts_with("CREATE TABLE IF NOT EXISTS \"myks\".\"users\""));
        assert!(cql.contains("PRIMARY KEY (\"id\")"));
    }
}
