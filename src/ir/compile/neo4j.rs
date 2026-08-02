//! Neo4j compiler (U2 step 6a).
//!
//! Lowers `LogicalRead`/`Write`/`Delete` to parameterised Cypher posted via
//! the HTTP Transactional Cypher API the existing `Neo4jExecutor` already
//! speaks (`/db/<database>/tx/commit` with a `{"statements":[{"statement":
//! "MATCH … RETURN …","parameters":{...}}]}` body).
//!
//! The IR's `message_type` is mapped to a node **label** rather than a
//! table — the manifest already declares that for graph stores via
//! `ManifestTable.table`. Field-name resolution mirrors the Mongo
//! compiler: proto field names go straight onto the wire (Neo4j is
//! schema-flexible, not column-oriented).

use serde_json::{Map, Value as Json, json};

use crate::backend::BackendKind;
use crate::generation::ManifestTable;
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    AggregateExpr, AggregateFunc, ConflictStrategy, LogicalAggregate, LogicalDelete, LogicalRead,
    LogicalResourceOp, LogicalSearch, LogicalWrite, ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;

use super::{CompileContext, CompileError, CompiledRendering, Compiler, HttpMethod};

/// Neo4j Cypher compiler.
#[derive(Debug, Default, Clone, Copy)]
pub struct Neo4jCompiler;

/// Bind context: emits `$pN` placeholders and keeps a parallel parameter
/// map. Neo4j parameters are named, so a counter ensures uniqueness.
struct CypherBind {
    params: Map<String, Json>,
    counter: usize,
}

impl CypherBind {
    fn new() -> Self {
        Self {
            params: Map::new(),
            counter: 0,
        }
    }
    fn push(&mut self, v: &LogicalValue) -> String {
        let name = format!("p{}", self.counter);
        self.counter += 1;
        self.params
            .insert(name.clone(), super::util::value_to_json(v));
        format!("${name}")
    }
}

impl Neo4jCompiler {
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

    /// Validate that a field is declared on the message. Returns the proto
    /// field name (Neo4j uses it verbatim on properties).
    fn field_for<'a>(
        &self,
        table: &'a ManifestTable,
        field: &str,
        message_type: &str,
    ) -> Result<&'a str, CompileError> {
        table
            .columns
            .iter()
            .find(|c| c.field_name.eq_ignore_ascii_case(field) || c.column_name == field)
            .map(|c| c.field_name.as_str())
            .ok_or_else(|| CompileError::UnknownField {
                message_type: message_type.to_string(),
                field: field.to_string(),
            })
    }

    /// Render a filter clause for an `n:Label` binding. Always non-empty.
    fn render_filter(
        &self,
        filter: &LogicalFilter,
        table: &ManifestTable,
        message_type: &str,
        bind: &mut CypherBind,
    ) -> Result<String, CompileError> {
        match filter {
            LogicalFilter::And(clauses) if clauses.is_empty() => Ok("true".to_string()),
            LogicalFilter::Or(clauses) if clauses.is_empty() => Ok("false".to_string()),
            LogicalFilter::And(clauses) => {
                let parts = clauses
                    .iter()
                    .map(|c| self.render_filter(c, table, message_type, bind))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(format!("({})", parts.join(" AND ")))
            }
            LogicalFilter::Or(clauses) => {
                let parts = clauses
                    .iter()
                    .map(|c| self.render_filter(c, table, message_type, bind))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(format!("({})", parts.join(" OR ")))
            }
            LogicalFilter::Not(inner) => {
                let r = self.render_filter(inner, table, message_type, bind)?;
                Ok(format!("(NOT {r})"))
            }
            LogicalFilter::IsNull(field) => {
                let f = self.field_for(table, field, message_type)?;
                Ok(format!("n.`{f}` IS NULL"))
            }
            LogicalFilter::InList { field, values } => {
                if values.is_empty() {
                    return Ok("false".to_string());
                }
                let f = self.field_for(table, field, message_type)?;
                let placeholders: Vec<String> = values.iter().map(|v| bind.push(v)).collect();
                Ok(format!("n.`{f}` IN [{}]", placeholders.join(", ")))
            }
            LogicalFilter::Comparison { field, op, value } => {
                let f = self.field_for(table, field, message_type)?;
                if value.is_null() {
                    return Err(CompileError::Malformed {
                        reason: format!(
                            "comparison with NULL on field '{field}' must use IsNull, not {}",
                            op.token()
                        ),
                    });
                }
                let placeholder = bind.push(value);
                let lhs = format!("n.`{f}`");
                let rendered = match op {
                    ComparisonOp::Eq => format!("{lhs} = {placeholder}"),
                    ComparisonOp::Ne => format!("{lhs} <> {placeholder}"),
                    ComparisonOp::Lt => format!("{lhs} < {placeholder}"),
                    ComparisonOp::Le => format!("{lhs} <= {placeholder}"),
                    ComparisonOp::Gt => format!("{lhs} > {placeholder}"),
                    ComparisonOp::Ge => format!("{lhs} >= {placeholder}"),
                    ComparisonOp::Contains => format!("{lhs} CONTAINS {placeholder}"),
                    ComparisonOp::StartsWith => format!("{lhs} STARTS WITH {placeholder}"),
                    ComparisonOp::EndsWith => format!("{lhs} ENDS WITH {placeholder}"),
                    ComparisonOp::Like | ComparisonOp::ILike => {
                        // Cypher has no LIKE; lower to =~ regex with the
                        // SQL pattern translated.
                        let pattern = match value {
                            LogicalValue::String(s) => super::util::like_to_regex(s),
                            _ => {
                                return Err(CompileError::Malformed {
                                    reason: format!(
                                        "{} requires a String value on field '{field}'",
                                        op.token()
                                    ),
                                });
                            }
                        };
                        // Replace the just-pushed placeholder with the
                        // computed pattern; cleaner than threading through.
                        let name = format!("p{}", bind.counter - 1);
                        bind.params.insert(name.clone(), json!(pattern));
                        let case = if matches!(op, ComparisonOp::ILike) {
                            "(?i)"
                        } else {
                            ""
                        };
                        format!("{lhs} =~ '{case}' + ${name}")
                    }
                };
                Ok(rendered)
            }
        }
    }

    fn label_for(table: &ManifestTable) -> &str {
        // Manifest's `table` is the storage-side label for graph stores.
        // We could also drive from a future `node_label` field; today this
        // mirrors how Neo4jExecutor names labels in resource-op specs.
        &table.table
    }

    fn render_cypher(statement: String, bind: CypherBind) -> CompiledRendering {
        CompiledRendering::Json {
            backend: BackendKind::Neo4j,
            method: HttpMethod::Post,
            path: "/tx/commit".to_string(),
            body: json!({
                "statements": [{
                    "statement": statement,
                    "parameters": Json::Object(bind.params),
                }]
            }),
        }
    }
}

impl Compiler for Neo4jCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::Neo4j
    }

    fn compile_read(
        &self,
        op: &LogicalRead,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        let label = Self::label_for(table);
        let mut bind = CypherBind::new();

        let mut cypher = format!("MATCH (n:`{label}`)");
        // C7/C8: AND in the active tenant + project predicate ahead of
        // the user filter so a permissive Cypher caller can't escape
        // the tenant boundary at protocol level.
        let mut where_parts: Vec<String> = Vec::new();
        if let Some(tid) = ctx.tenant_id
            && !tid.is_empty()
            && let Some(column) = Some(super::util::tenant_system_field(table))
        {
            let p = bind.push(&LogicalValue::String(tid.to_string()));
            where_parts.push(format!("n.`{column}` = {p}"));
        }
        if let Some(pid) = ctx.project_id
            && !pid.is_empty()
            && let Some(column) = Some(super::util::project_system_field(table))
        {
            let p = bind.push(&LogicalValue::String(pid.to_string()));
            where_parts.push(format!("n.`{column}` = {p}"));
        }
        if let Some(f) = &op.filter {
            let body = self.render_filter(f, table, &op.message_type, &mut bind)?;
            if body != "true" {
                where_parts.push(body);
            }
        }
        if !where_parts.is_empty() {
            cypher.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));
        }

        // RETURN clause.
        let return_clause = match &op.projection {
            Some(p) if !p.is_select_all() => p
                .fields
                .iter()
                .map(|f| {
                    let resolved = self.field_for(table, f, &op.message_type)?;
                    Ok(format!("n.`{resolved}` AS `{resolved}`"))
                })
                .collect::<Result<Vec<_>, CompileError>>()?
                .join(", "),
            _ => "n".to_string(),
        };
        cypher.push_str(&format!(" RETURN {return_clause}"));

        if !op.sort.is_empty() {
            let parts = op
                .sort
                .iter()
                .map(|s| {
                    let f = self.field_for(table, &s.field, &op.message_type)?;
                    let dir = s.direction.token().to_uppercase();
                    Ok(format!("n.`{f}` {dir}"))
                })
                .collect::<Result<Vec<_>, CompileError>>()?;
            cypher.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Neo4j,
                    op: "keyset_cursor",
                });
            }
            if let Some(offset) = pag.offset
                && offset > 0
            {
                cypher.push_str(&format!(" SKIP {offset}"));
            }
            if let Some(limit) = pag.limit {
                cypher.push_str(&format!(" LIMIT {limit}"));
            }
        }

        Ok(Self::render_cypher(cypher, bind))
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
            // Multi-record MERGE is best done in a UNWIND; deferred.
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Neo4j,
                op: "batch_write",
            });
        }
        if matches!(op.conflict, ConflictStrategy::Ignore) {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Neo4j,
                op: "insert_ignore",
            });
        }
        let table = self.resolve_table(&op.message_type, ctx)?;
        if table.primary_key.is_empty() {
            return Err(CompileError::Malformed {
                reason: format!(
                    "Neo4j MERGE requires a primary key for '{}'",
                    op.message_type
                ),
            });
        }
        let label = Self::label_for(table);
        let record = &op.records[0];
        let mut bind = CypherBind::new();

        // MERGE key clause: `(n:Label {pk1: $p0, pk2: $p1, _tenant_id: $pN, _project_id: $pM})`.
        // C7/C8: tenant + project go into the MERGE key so a write
        // can never collide with another tenant's PK-equivalent node.
        let mut key_parts = Vec::with_capacity(table.primary_key.len() + 2);
        for pk in &table.primary_key {
            let f = self.field_for(table, pk, &op.message_type)?;
            let value = record.get(pk).ok_or_else(|| CompileError::Malformed {
                reason: format!("record missing primary-key field '{pk}'"),
            })?;
            let p = bind.push(value);
            key_parts.push(format!("`{f}`: {p}"));
        }
        if let Some(tid) = ctx.tenant_id
            && !tid.is_empty()
            && let Some(column) = Some(super::util::tenant_system_field(table))
        {
            let p = bind.push(&LogicalValue::String(tid.to_string()));
            key_parts.push(format!("`{column}`: {p}"));
        }
        if let Some(pid) = ctx.project_id
            && !pid.is_empty()
            && let Some(column) = Some(super::util::project_system_field(table))
        {
            let p = bind.push(&LogicalValue::String(pid.to_string()));
            key_parts.push(format!("`{column}`: {p}"));
        }
        let mut cypher = format!("MERGE (n:`{label}` {{{}}})", key_parts.join(", "));

        // SET clause for non-PK fields (Update) or all fields (Replace).
        let set_fields: Vec<&str> = match &op.conflict {
            ConflictStrategy::Update { fields, .. } => fields.iter().map(String::as_str).collect(),
            ConflictStrategy::Replace | ConflictStrategy::Error => record
                .keys()
                .filter(|k| !table.primary_key.iter().any(|pk| pk == *k))
                .map(String::as_str)
                .collect(),
            ConflictStrategy::Ignore => unreachable!(),
        };
        if !set_fields.is_empty() {
            let mut assignments = Vec::with_capacity(set_fields.len());
            for field in set_fields {
                let f = self.field_for(table, field, &op.message_type)?;
                let value = record.get(field).ok_or_else(|| CompileError::Malformed {
                    reason: format!("record missing field '{field}'"),
                })?;
                let p = bind.push(value);
                assignments.push(format!("n.`{f}` = {p}"));
            }
            cypher.push_str(&format!(" SET {}", assignments.join(", ")));
        }

        // RETURN if requested.
        if !op.return_fields.is_empty() {
            let parts = op
                .return_fields
                .iter()
                .map(|f| {
                    let resolved = self.field_for(table, f, &op.message_type)?;
                    Ok(format!("n.`{resolved}` AS `{resolved}`"))
                })
                .collect::<Result<Vec<_>, CompileError>>()?;
            cypher.push_str(&format!(" RETURN {}", parts.join(", ")));
        }

        Ok(Self::render_cypher(cypher, bind))
    }

    fn compile_delete(
        &self,
        op: &LogicalDelete,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        let label = Self::label_for(table);
        let mut bind = CypherBind::new();
        let user_body = self.render_filter(&op.filter, table, &op.message_type, &mut bind)?;
        if user_body == "true" || user_body == "false" {
            return Err(CompileError::Malformed {
                reason: "LogicalDelete::filter cannot be empty or false-constant".into(),
            });
        }
        // C7/C8: AND tenant + project so a permissive caller filter
        // can't DETACH DELETE another tenant's nodes.
        let mut where_parts: Vec<String> = vec![user_body];
        if let Some(tid) = ctx.tenant_id
            && !tid.is_empty()
            && let Some(column) = Some(super::util::tenant_system_field(table))
        {
            let p = bind.push(&LogicalValue::String(tid.to_string()));
            where_parts.push(format!("n.`{column}` = {p}"));
        }
        if let Some(pid) = ctx.project_id
            && !pid.is_empty()
            && let Some(column) = Some(super::util::project_system_field(table))
        {
            let p = bind.push(&LogicalValue::String(pid.to_string()));
            where_parts.push(format!("n.`{column}` = {p}"));
        }
        let body = where_parts.join(" AND ");
        let mut cypher = format!("MATCH (n:`{label}`) WHERE {body} DETACH DELETE n");
        if !op.return_fields.is_empty() {
            let parts = op
                .return_fields
                .iter()
                .map(|f| {
                    let resolved = self.field_for(table, f, &op.message_type)?;
                    Ok(format!("n.`{resolved}` AS `{resolved}`"))
                })
                .collect::<Result<Vec<_>, CompileError>>()?;
            // Use WITH ... DETACH DELETE n RETURN ... pattern. The simplest
            // form: capture before delete.
            cypher = format!(
                "MATCH (n:`{label}`) WHERE {body} WITH n, {return_list} DETACH DELETE n RETURN {return_list}",
                return_list = parts.join(", "),
            );
        }
        Ok(Self::render_cypher(cypher, bind))
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
        // #151: a GROUP BY field resolving to an aggregate alias name would emit
        // two identically-keyed RETURN columns. Reject (SQL backends already do).
        let group_names: Vec<&str> = op
            .group_by
            .iter()
            .map(|f| self.field_for(table, f, &op.message_type))
            .collect::<Result<Vec<_>, _>>()?;
        super::util::validate_no_groupby_alias_collision(&group_names, &op.aggregates)?;
        let label = Self::label_for(table);
        let mut bind = CypherBind::new();

        let mut cypher = format!("MATCH (n:`{label}`)");
        // C17: AND the active tenant + project predicate into the aggregate WHERE
        // like every sibling op (read/delete/search), else COUNT/SUM/AVG span
        // tenants at protocol level.
        let mut where_parts: Vec<String> = Vec::new();
        if let Some(tid) = ctx.tenant_id
            && !tid.is_empty()
            && let Some(column) = Some(super::util::tenant_system_field(table))
        {
            let p = bind.push(&LogicalValue::String(tid.to_string()));
            where_parts.push(format!("n.`{column}` = {p}"));
        }
        if let Some(pid) = ctx.project_id
            && !pid.is_empty()
            && let Some(column) = Some(super::util::project_system_field(table))
        {
            let p = bind.push(&LogicalValue::String(pid.to_string()));
            where_parts.push(format!("n.`{column}` = {p}"));
        }
        if let Some(filter) = &op.filter {
            let body = self.render_filter(filter, table, &op.message_type, &mut bind)?;
            if body != "true" {
                where_parts.push(body);
            }
        }
        if !where_parts.is_empty() {
            cypher.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));
        }

        // Cypher aggregation: group-by columns appear in WITH alongside
        // aggregate functions.
        let mut with_parts: Vec<String> = Vec::new();
        for f in &op.group_by {
            let resolved = self.field_for(table, f, &op.message_type)?;
            with_parts.push(format!("n.`{resolved}` AS `{resolved}`"));
        }
        for agg in &op.aggregates {
            with_parts.push(render_cypher_aggregate(agg, table, &op.message_type)?);
        }
        cypher.push_str(&format!(" WITH {}", with_parts.join(", ")));

        if let Some(having) = &op.having {
            let body = render_cypher_having(having, op, &mut bind)?;
            cypher.push_str(&format!(" WHERE {body}"));
        }

        // RETURN: every group-by column + every aggregate alias.
        let mut return_parts: Vec<String> = Vec::new();
        for f in &op.group_by {
            let resolved = self.field_for(table, f, &op.message_type)?;
            return_parts.push(format!("`{resolved}`"));
        }
        for agg in &op.aggregates {
            return_parts.push(format!("`{}`", agg.alias));
        }
        cypher.push_str(&format!(" RETURN {}", return_parts.join(", ")));

        if !op.sort.is_empty() {
            let parts = op
                .sort
                .iter()
                .map(|s| {
                    let token = resolve_cypher_sort_field(&s.field, op, table)?;
                    let dir = s.direction.token().to_uppercase();
                    Ok::<_, CompileError>(format!("{token} {dir}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            cypher.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Neo4j,
                    op: "keyset_cursor",
                });
            }
            if let Some(offset) = pag.offset
                && offset > 0
            {
                cypher.push_str(&format!(" SKIP {offset}"));
            }
            if let Some(limit) = pag.limit {
                cypher.push_str(&format!(" LIMIT {limit}"));
            }
        }

        Ok(Self::render_cypher(cypher, bind))
    }

    fn compile_search(
        &self,
        op: &LogicalSearch,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        // Neo4j fulltext search: `CALL db.index.fulltext.queryNodes(name,
        // query) YIELD node, score`. Convention: an index named
        // `<label>_fulltext` exists. Vector path uses Neo4j 5.13+
        // `db.index.vector.queryNodes` — for portability we fall through
        // to fulltext when only text_query is set.
        let table = self.resolve_table(&op.message_type, ctx)?;
        let label = Self::label_for(table);

        if let Some(vector) = &op.vector {
            // Vector index name convention: `<label>_vector`.
            let mut bind = CypherBind::new();
            let top_k_bound = bind.push(&LogicalValue::Int(op.top_k as i64));
            let v_bound = bind.push(&LogicalValue::Array(
                vector
                    .iter()
                    .map(|f| LogicalValue::Float(*f as f64))
                    .collect(),
            ));
            let mut cypher = format!(
                "CALL db.index.vector.queryNodes('{label}_vector', {top_k_bound}, {v_bound}) \
                 YIELD node, score \
                 WITH node AS n, score AS _score"
            );
            let mut where_parts = Vec::new();
            if let Some(threshold) = op.score_threshold {
                let t_bound = bind.push(&LogicalValue::Float(threshold as f64));
                where_parts.push(format!("_score >= {t_bound}"));
            }
            if let Some(tid) = ctx.tenant_id
                && !tid.is_empty()
                && let Some(column) = Some(super::util::tenant_system_field(table))
            {
                let p = bind.push(&LogicalValue::String(tid.to_string()));
                where_parts.push(format!("n.`{column}` = {p}"));
            }
            if let Some(pid) = ctx.project_id
                && !pid.is_empty()
                && let Some(column) = Some(super::util::project_system_field(table))
            {
                let p = bind.push(&LogicalValue::String(pid.to_string()));
                where_parts.push(format!("n.`{column}` = {p}"));
            }
            if let Some(filter) = &op.filter {
                let body = self.render_filter(filter, table, &op.message_type, &mut bind)?;
                if body != "true" {
                    where_parts.push(body);
                }
            }
            if !where_parts.is_empty() {
                cypher.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));
            }
            cypher.push_str(" RETURN n, _score ORDER BY _score DESC");
            return Ok(Self::render_cypher(cypher, bind));
        }

        let text = op
            .text_query
            .as_deref()
            .ok_or_else(|| CompileError::Malformed {
                reason: "Neo4j search requires either a vector or text_query".into(),
            })?;
        if text.trim().is_empty() {
            return Err(CompileError::Malformed {
                reason: "text_query must be non-empty for Neo4j fulltext".into(),
            });
        }
        let mut bind = CypherBind::new();
        let q_bound = bind.push(&LogicalValue::String(text.to_string()));
        let mut cypher = format!(
            "CALL db.index.fulltext.queryNodes('{label}_fulltext', {q_bound}) \
             YIELD node, score \
             WITH node AS n, score AS _score"
        );
        let mut where_parts = Vec::new();
        if let Some(threshold) = op.score_threshold {
            let t_bound = bind.push(&LogicalValue::Float(threshold as f64));
            where_parts.push(format!("_score >= {t_bound}"));
        }
        if let Some(tid) = ctx.tenant_id
            && !tid.is_empty()
            && let Some(column) = Some(super::util::tenant_system_field(table))
        {
            let p = bind.push(&LogicalValue::String(tid.to_string()));
            where_parts.push(format!("n.`{column}` = {p}"));
        }
        if let Some(pid) = ctx.project_id
            && !pid.is_empty()
            && let Some(column) = Some(super::util::project_system_field(table))
        {
            let p = bind.push(&LogicalValue::String(pid.to_string()));
            where_parts.push(format!("n.`{column}` = {p}"));
        }
        if let Some(filter) = &op.filter {
            let body = self.render_filter(filter, table, &op.message_type, &mut bind)?;
            if body != "true" {
                where_parts.push(body);
            }
        }
        if !where_parts.is_empty() {
            cypher.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));
        }
        cypher.push_str(&format!(
            " RETURN n, _score ORDER BY _score DESC LIMIT {}",
            op.top_k
        ));
        Ok(Self::render_cypher(cypher, bind))
    }

    fn compile_resource_op(
        &self,
        op: &LogicalResourceOp,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        if !matches!(
            op.resource_kind,
            ResourceKind::Constraint | ResourceKind::Index
        ) {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Neo4j,
                op: "non_constraint_resource",
            });
        }
        let cypher = match (op.op, op.resource_kind) {
            (ResourceOpKind::Ensure, ResourceKind::Constraint) => {
                // Spec: { "label": "Person", "property": "id", "type": "uniqueness"|"existence" }
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "Ensure Constraint requires a spec".into(),
                })?;
                let label = spec.get("label").and_then(|v| v.as_str()).ok_or_else(|| {
                    CompileError::Malformed {
                        reason: "Ensure Constraint spec missing 'label'".into(),
                    }
                })?;
                let property = spec
                    .get("property")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| CompileError::Malformed {
                        reason: "Ensure Constraint spec missing 'property'".into(),
                    })?;
                let kind = spec
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("uniqueness");
                match kind {
                    "uniqueness" => format!(
                        "CREATE CONSTRAINT `{name}` IF NOT EXISTS \
                         FOR (n:`{label}`) REQUIRE n.`{property}` IS UNIQUE",
                        name = op.resource_name,
                    ),
                    "existence" => format!(
                        "CREATE CONSTRAINT `{name}` IF NOT EXISTS \
                         FOR (n:`{label}`) REQUIRE n.`{property}` IS NOT NULL",
                        name = op.resource_name,
                    ),
                    other => {
                        return Err(CompileError::Malformed {
                            reason: format!("unknown constraint type '{other}'"),
                        });
                    }
                }
            }
            (ResourceOpKind::Drop, ResourceKind::Constraint) => {
                format!("DROP CONSTRAINT `{}` IF EXISTS", op.resource_name)
            }
            (ResourceOpKind::List, ResourceKind::Constraint) => "SHOW CONSTRAINTS".to_string(),
            (ResourceOpKind::Ensure, ResourceKind::Index) => {
                let spec = op.spec.as_ref().ok_or_else(|| CompileError::Malformed {
                    reason: "Ensure Index requires a spec".into(),
                })?;
                let label = spec.get("label").and_then(|v| v.as_str()).ok_or_else(|| {
                    CompileError::Malformed {
                        reason: "Ensure Index spec missing 'label'".into(),
                    }
                })?;
                let properties = spec
                    .get("properties")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| CompileError::Malformed {
                        reason: "Ensure Index spec.properties must be an array".into(),
                    })?;
                let prop_list = properties
                    .iter()
                    .filter_map(|p| p.as_str())
                    .map(|p| format!("n.`{p}`"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let kind = spec.get("type").and_then(|v| v.as_str()).unwrap_or("range");
                match kind {
                    "fulltext" => format!(
                        "CREATE FULLTEXT INDEX `{name}` IF NOT EXISTS \
                         FOR (n:`{label}`) ON EACH [{prop_list}]",
                        name = op.resource_name,
                    ),
                    "vector" => {
                        let dim =
                            spec.get("dimensions")
                                .and_then(|v| v.as_u64())
                                .ok_or_else(|| CompileError::Malformed {
                                    reason: "vector index spec requires 'dimensions'".into(),
                                })?;
                        let sim = spec
                            .get("similarity")
                            .and_then(|v| v.as_str())
                            .unwrap_or("cosine");
                        format!(
                            "CREATE VECTOR INDEX `{name}` IF NOT EXISTS \
                             FOR (n:`{label}`) ON ({prop_list}) \
                             OPTIONS {{indexConfig: {{`vector.dimensions`: {dim}, `vector.similarity_function`: '{sim}'}}}}",
                            name = op.resource_name,
                        )
                    }
                    _ => format!(
                        "CREATE INDEX `{name}` IF NOT EXISTS \
                         FOR (n:`{label}`) ON ({prop_list})",
                        name = op.resource_name,
                    ),
                }
            }
            (ResourceOpKind::Drop, ResourceKind::Index) => {
                format!("DROP INDEX `{}` IF EXISTS", op.resource_name)
            }
            (ResourceOpKind::List, ResourceKind::Index) => "SHOW INDEXES".to_string(),
            _ => {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Neo4j,
                    op: "unhandled_resource_op",
                });
            }
        };
        Ok(Self::render_cypher(cypher, CypherBind::new()))
    }
}

fn render_cypher_aggregate(
    agg: &AggregateExpr,
    table: &ManifestTable,
    message_type: &str,
) -> Result<String, CompileError> {
    let body = match agg.func {
        AggregateFunc::Count if agg.field == "*" => "count(*)".to_string(),
        AggregateFunc::Count => {
            let f = resolve_cypher_field(table, &agg.field, message_type)?;
            format!("count(n.`{f}`)")
        }
        AggregateFunc::CountDistinct => {
            if agg.field == "*" {
                return Err(CompileError::Malformed {
                    reason: "COUNT(DISTINCT *) is not allowed; specify a field".into(),
                });
            }
            let f = resolve_cypher_field(table, &agg.field, message_type)?;
            format!("count(DISTINCT n.`{f}`)")
        }
        AggregateFunc::Sum | AggregateFunc::Avg | AggregateFunc::Min | AggregateFunc::Max => {
            if agg.field == "*" {
                return Err(CompileError::Malformed {
                    reason: format!("{} requires a field name, not '*'", agg.func.sql_token()),
                });
            }
            let f = resolve_cypher_field(table, &agg.field, message_type)?;
            let fn_name = match agg.func {
                AggregateFunc::Sum => "sum",
                AggregateFunc::Avg => "avg",
                AggregateFunc::Min => "min",
                AggregateFunc::Max => "max",
                _ => unreachable!(),
            };
            format!("{fn_name}(n.`{f}`)")
        }
    };
    Ok(format!("{body} AS `{}`", agg.alias))
}

fn render_cypher_having(
    filter: &LogicalFilter,
    op: &LogicalAggregate,
    bind: &mut CypherBind,
) -> Result<String, CompileError> {
    match filter {
        LogicalFilter::And(c) if c.is_empty() => Ok("true".to_string()),
        LogicalFilter::Or(c) if c.is_empty() => Ok("false".to_string()),
        LogicalFilter::And(clauses) => {
            let parts = clauses
                .iter()
                .map(|c| render_cypher_having(c, op, bind))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", parts.join(" AND ")))
        }
        LogicalFilter::Or(clauses) => {
            let parts = clauses
                .iter()
                .map(|c| render_cypher_having(c, op, bind))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", parts.join(" OR ")))
        }
        LogicalFilter::Not(inner) => {
            let r = render_cypher_having(inner, op, bind)?;
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
            if !op.aggregates.iter().any(|a| a.alias == *field)
                && !op.group_by.iter().any(|f| f == field)
            {
                return Err(CompileError::Malformed {
                    reason: format!(
                        "HAVING field '{field}' is neither an aggregate alias nor a GROUP BY column"
                    ),
                });
            }
            let lhs = format!("`{field}`");
            let p = bind.push(value);
            let token = match cmp {
                ComparisonOp::Eq => format!("{lhs} = {p}"),
                ComparisonOp::Ne => format!("{lhs} <> {p}"),
                ComparisonOp::Lt => format!("{lhs} < {p}"),
                ComparisonOp::Le => format!("{lhs} <= {p}"),
                ComparisonOp::Gt => format!("{lhs} > {p}"),
                ComparisonOp::Ge => format!("{lhs} >= {p}"),
                _ => {
                    return Err(CompileError::OperatorUnsupported {
                        backend: BackendKind::Neo4j,
                        op: "having_text_match",
                    });
                }
            };
            Ok(token)
        }
        LogicalFilter::IsNull(field) => {
            if !op.aggregates.iter().any(|a| a.alias == *field)
                && !op.group_by.iter().any(|f| f == field)
            {
                return Err(CompileError::Malformed {
                    reason: format!(
                        "HAVING field '{field}' is neither an aggregate alias nor a GROUP BY column"
                    ),
                });
            }
            Ok(format!("`{field}` IS NULL"))
        }
        LogicalFilter::InList { field, values } => {
            if values.is_empty() {
                return Ok("false".to_string());
            }
            if !op.aggregates.iter().any(|a| a.alias == *field)
                && !op.group_by.iter().any(|f| f == field)
            {
                return Err(CompileError::Malformed {
                    reason: format!(
                        "HAVING field '{field}' is neither an aggregate alias nor a GROUP BY column"
                    ),
                });
            }
            let placeholders: Vec<String> = values.iter().map(|v| bind.push(v)).collect();
            Ok(format!("`{field}` IN [{}]", placeholders.join(", ")))
        }
    }
}

fn resolve_cypher_sort_field(
    field: &str,
    op: &LogicalAggregate,
    _table: &ManifestTable,
) -> Result<String, CompileError> {
    if op.aggregates.iter().any(|a| a.alias == field) || op.group_by.iter().any(|f| f == field) {
        return Ok(format!("`{field}`"));
    }
    Err(CompileError::Malformed {
        reason: format!(
            "ORDER BY field '{field}' is neither an aggregate alias nor a GROUP BY column"
        ),
    })
}

fn resolve_cypher_field<'a>(
    table: &'a ManifestTable,
    field: &str,
    message_type: &str,
) -> Result<&'a str, CompileError> {
    table
        .columns
        .iter()
        .find(|c| c.field_name.eq_ignore_ascii_case(field) || c.column_name == field)
        .map(|c| c.field_name.as_str())
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
    use crate::ir::operations::{ConflictStrategy, LogicalRead, LogicalRecord, LogicalWrite};
    use crate::ir::value::LogicalValue;

    fn fixture() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.billing.v1.Customer".to_string(),
            schema: "graph".into(),
            table: "Customer".into(),
            primary_key: vec!["id".to_string()],
            columns: vec![
                ManifestColumn {
                    field_name: "id".into(),
                    column_name: "id".into(),
                    proto_type: "string".into(),
                    sql_type: "text".into(),
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
            ],
            ..Default::default()
        };
        CatalogManifest {
            tables: vec![table],
            ..Default::default()
        }
    }

    fn cypher(rendering: CompiledRendering) -> (String, Map<String, Json>) {
        match rendering {
            CompiledRendering::Json { body, .. } => {
                let statement = body["statements"][0]["statement"]
                    .as_str()
                    .unwrap()
                    .to_string();
                let params = body["statements"][0]["parameters"]
                    .as_object()
                    .cloned()
                    .unwrap_or_default();
                (statement, params)
            }
            other => panic!("expected Json rendering, got {other:?}"),
        }
    }

    #[test]
    fn read_emits_match_where_return() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("acme.billing.v1.Customer").with_filter(
            LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("abc".into()),
            },
        );
        let (statement, params) = cypher(Neo4jCompiler.compile_read(&read, &ctx).unwrap());
        assert!(statement.starts_with("MATCH (n:`Customer`)"));
        assert!(statement.contains("WHERE n.`id` = $p0"));
        assert!(statement.ends_with("RETURN n"));
        assert_eq!(params["p0"], "abc");
    }

    #[test]
    fn merge_emits_set_for_non_pk_fields() {
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
        let (statement, params) = cypher(Neo4jCompiler.compile_write(&write, &ctx).unwrap());
        assert!(statement.starts_with("MERGE (n:`Customer` {`id`: $p0})"));
        assert!(statement.contains("SET n.`name` = $p1"));
        assert_eq!(params["p0"], "abc");
        assert_eq!(params["p1"], "Alice");
    }

    #[test]
    fn batch_write_is_capability_rejected() {
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
        let err = Neo4jCompiler.compile_write(&write, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Neo4j,
                op: "batch_write"
            }
        ));
    }

    // ── C7/C8 — tenant + project context enforcement ────────────────

    #[test]
    fn read_with_tenant_context_ands_n_dot_tenant() {
        let m = fixture();
        let ctx = CompileContext::new(&m)
            .with_tenant("acme")
            .with_project("p1");
        let read = LogicalRead::message("acme.billing.v1.Customer").with_filter(
            LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("abc".into()),
            },
        );
        let (statement, params) = cypher(Neo4jCompiler.compile_read(&read, &ctx).unwrap());
        assert!(statement.contains("n.`_tenant_id` = $p0"));
        assert!(statement.contains("n.`_project_id` = $p1"));
        assert_eq!(params["p0"], "acme");
        assert_eq!(params["p1"], "p1");
    }

    #[test]
    fn merge_includes_tenant_in_merge_key() {
        let m = fixture();
        let ctx = CompileContext::new(&m)
            .with_tenant("acme")
            .with_project("p1");
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("abc".into()));
        rec.insert("name".into(), LogicalValue::String("Alice".into()));
        let write = LogicalWrite {
            message_type: "acme.billing.v1.Customer".into(),
            records: vec![rec],
            conflict: ConflictStrategy::update(vec!["name".into()]),
            return_fields: vec![],
        };
        let (statement, params) = cypher(Neo4jCompiler.compile_write(&write, &ctx).unwrap());
        assert!(statement.starts_with(
            "MERGE (n:`Customer` {`id`: $p0, `_tenant_id`: $p1, `_project_id`: $p2})"
        ));
        assert_eq!(params["p1"], "acme");
        assert_eq!(params["p2"], "p1");
    }

    #[test]
    fn search_with_tenant_context_filters_fulltext_nodes() {
        let m = fixture();
        let ctx = CompileContext::new(&m).with_tenant("tenant-a");
        let search = LogicalSearch {
            message_type: "acme.billing.v1.Customer".into(),
            vector: None,
            text_query: Some("Alice".into()),
            filter: None,
            top_k: 10,
            score_threshold: None,
            require_hybrid: false,
            with_vector: false,
            with_payload: true,
        };
        let (statement, params) = cypher(Neo4jCompiler.compile_search(&search, &ctx).unwrap());
        assert!(statement.contains("db.index.fulltext.queryNodes('Customer_fulltext'"));
        assert!(statement.contains("WHERE n.`_tenant_id` = $p1"));
        assert_eq!(params["p0"], "Alice");
        assert_eq!(params["p1"], "tenant-a");
    }

    /// C17: the aggregate WHERE must carry the tenant/project predicate like the
    /// sibling ops, or COUNT/SUM span tenants.
    #[test]
    fn aggregate_with_tenant_context_ands_predicate() {
        let m = fixture();
        let ctx = CompileContext::new(&m)
            .with_tenant("acme")
            .with_project("p1");
        let agg = LogicalAggregate {
            message_type: "acme.billing.v1.Customer".into(),
            filter: None,
            group_by: vec![],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Count,
                field: "*".into(),
                alias: "n".into(),
            }],
            having: None,
            sort: vec![],
            pagination: None,
        };
        let (statement, params) = cypher(Neo4jCompiler.compile_aggregate(&agg, &ctx).unwrap());
        assert!(
            statement.contains("n.`_tenant_id` = $p0"),
            "aggregate WHERE must scope tenant: {statement}"
        );
        assert!(statement.contains("n.`_project_id` = $p1"));
        assert_eq!(params["p0"], "acme");
        assert_eq!(params["p1"], "p1");
    }
}
