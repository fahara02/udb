//! Elasticsearch compiler (C9).
//!
//! Lowers `LogicalRead`/`Write`/`Delete`/`Aggregate`/`Search`/`ResourceOp`
//! to the Elasticsearch Query DSL over HTTP+JSON. ES is both a
//! full-text search engine and a document store with native
//! aggregation, so every IR op has a natural lowering:
//!
//! - **Read**       → `POST /<index>/_search` with a `bool/must` query
//!                     + `_source` field projection + `sort` + `from/size` pagination
//! - **Write**      → `POST /<index>/_bulk` body of `{"index": {...}}\n<doc>\n…`
//!                     OR `POST /<index>/_update/<id>` for partial updates
//! - **Delete**     → `POST /<index>/_delete_by_query` with the filter
//! - **Search**     → `POST /<index>/_search` with hybrid (vector via `knn`
//!                     + lexical via `multi_match`) plus text relevance scoring
//! - **Aggregate**  → `POST /<index>/_search` with `size: 0` + `aggs`
//!                     (terms / sum / avg / min / max / cardinality)
//! - **ResourceOp** → `PUT /<index>` (create), `DELETE /<index>` (drop),
//!                     `GET /_cat/indices?format=json` (list)
//!
//! ## C7/C8 — RLS injection
//!
//! The compiler reads `ctx.tenant_id` / `ctx.project_id` from
//! `CompileContext` and ANDs them into every `bool/must` clause as
//! `term` predicates on `_tenant_id` / `_project_id`. Writes stamp
//! the same fields onto each document so the read-side predicate
//! has something to match. Mirrors the Mongo / Neo4j / Qdrant
//! enforcement model — Elasticsearch is `Enforced` from day one.

use std::collections::HashSet;

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

/// Elasticsearch Query DSL compiler.
#[derive(Debug, Default, Clone, Copy)]
pub struct ElasticsearchCompiler;

impl ElasticsearchCompiler {
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

    /// Validate a field exists on the message and return the canonical
    /// proto field name (ES uses field names directly in queries).
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

    /// Index name: ES uses lowercase index names. Conventionally we
    /// use the manifest's `table` (already snake_case lowercase).
    fn index_for(table: &ManifestTable) -> String {
        table.table.to_ascii_lowercase()
    }

    /// Lower a `LogicalFilter` to ES Query DSL. Returns a JSON value
    /// suitable for the `query` field of `_search`.
    fn render_filter(
        &self,
        filter: &LogicalFilter,
        table: &ManifestTable,
        message_type: &str,
    ) -> Result<Json, CompileError> {
        match filter {
            LogicalFilter::And(clauses) if clauses.is_empty() => {
                // Empty AND = match all (ES convention).
                Ok(json!({ "match_all": {} }))
            }
            LogicalFilter::Or(clauses) if clauses.is_empty() => {
                // Empty OR = match nothing.
                Ok(json!({ "match_none": {} }))
            }
            LogicalFilter::And(clauses) => {
                let must: Vec<Json> = clauses
                    .iter()
                    .map(|c| self.render_filter(c, table, message_type))
                    .collect::<Result<_, _>>()?;
                Ok(json!({ "bool": { "must": must } }))
            }
            LogicalFilter::Or(clauses) => {
                let should: Vec<Json> = clauses
                    .iter()
                    .map(|c| self.render_filter(c, table, message_type))
                    .collect::<Result<_, _>>()?;
                // `minimum_should_match: 1` so SHOULD acts like OR (not
                // a relevance booster, which is the default ES semantic).
                Ok(json!({ "bool": { "should": should, "minimum_should_match": 1 } }))
            }
            LogicalFilter::Not(inner) => {
                let body = self.render_filter(inner, table, message_type)?;
                Ok(json!({ "bool": { "must_not": [body] } }))
            }
            LogicalFilter::IsNull(field) => {
                let f = self.field_for(table, field, message_type)?;
                // ES "is null" = field doesn't exist OR is explicitly
                // null. `must_not exists` covers both for the common case.
                Ok(json!({ "bool": { "must_not": [{ "exists": { "field": f } }] } }))
            }
            LogicalFilter::InList { field, values } => {
                let f = self.field_for(table, field, message_type)?;
                let arr: Vec<Json> = values.iter().map(value_to_es_json).collect();
                Ok(json!({ "terms": { f: arr } }))
            }
            LogicalFilter::Comparison { field, op, value } => {
                let f = self.field_for(table, field, message_type)?;
                let v = value_to_es_json(value);
                let predicate = match op {
                    ComparisonOp::Eq => json!({ "term": { f: v } }),
                    ComparisonOp::Ne => json!({
                        "bool": { "must_not": [{ "term": { f: v } }] }
                    }),
                    ComparisonOp::Lt => json!({ "range": { f: { "lt": v } } }),
                    ComparisonOp::Le => json!({ "range": { f: { "lte": v } } }),
                    ComparisonOp::Gt => json!({ "range": { f: { "gt": v } } }),
                    ComparisonOp::Ge => json!({ "range": { f: { "gte": v } } }),
                    ComparisonOp::Like | ComparisonOp::ILike => {
                        // `wildcard` query — ES is case-insensitive
                        // when `case_insensitive: true` is set on the
                        // query (5.x+).
                        let pattern = match value {
                            LogicalValue::String(s) => like_to_wildcard(s),
                            _ => {
                                return Err(CompileError::Malformed {
                                    reason: format!(
                                        "{} requires a String value on field '{field}'",
                                        op.token()
                                    ),
                                });
                            }
                        };
                        json!({
                            "wildcard": {
                                f: {
                                    "value": pattern,
                                    "case_insensitive": matches!(op, ComparisonOp::ILike)
                                }
                            }
                        })
                    }
                    ComparisonOp::Contains => {
                        let needle = string_or_err(value, op.token(), field)?;
                        json!({ "wildcard": { f: { "value": format!("*{needle}*") } } })
                    }
                    ComparisonOp::StartsWith => {
                        let needle = string_or_err(value, op.token(), field)?;
                        json!({ "prefix": { f: needle } })
                    }
                    ComparisonOp::EndsWith => {
                        let needle = string_or_err(value, op.token(), field)?;
                        json!({ "wildcard": { f: { "value": format!("*{needle}") } } })
                    }
                };
                Ok(predicate)
            }
        }
    }

    /// AND the active request context (tenant/project) into a query.
    /// C7/C8: tenant boundary protocol-enforced at the ES layer; reads
    /// can't cross tenants even with a permissive user filter.
    fn and_with_context(&self, user_query: Json, ctx: &CompileContext<'_>) -> Json {
        let mut ctx_terms: Vec<Json> = Vec::new();
        if let Some(tid) = ctx.tenant_id
            && !tid.is_empty()
        {
            ctx_terms.push(json!({ "term": { "_tenant_id": tid } }));
        }
        if let Some(pid) = ctx.project_id
            && !pid.is_empty()
        {
            ctx_terms.push(json!({ "term": { "_project_id": pid } }));
        }
        if ctx_terms.is_empty() {
            return user_query;
        }
        // Combine: { bool: { must: [user_query, ...ctx_terms] } }
        let mut must = ctx_terms;
        must.insert(0, user_query);
        json!({ "bool": { "must": must } })
    }
}

impl Compiler for ElasticsearchCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::Elasticsearch
    }

    fn compile_read(
        &self,
        op: &LogicalRead,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        let index = Self::index_for(table);

        let user_query = match &op.filter {
            Some(f) => self.render_filter(f, table, &op.message_type)?,
            None => json!({ "match_all": {} }),
        };
        let query = self.and_with_context(user_query, ctx);
        let mut body = json!({ "query": query });

        // _source projection.
        if let Some(p) = &op.projection
            && !p.is_select_all()
        {
            let fields: Vec<String> = p
                .fields
                .iter()
                .map(|f| {
                    self.field_for(table, f, &op.message_type)
                        .map(str::to_string)
                })
                .collect::<Result<_, _>>()?;
            body["_source"] = json!(fields);
        }

        // Sort.
        if !op.sort.is_empty() {
            let sort: Vec<Json> = op
                .sort
                .iter()
                .map(|s| {
                    let f = self.field_for(table, &s.field, &op.message_type)?;
                    let dir = if matches!(s.direction, crate::ir::projection::SortDirection::Asc) {
                        "asc"
                    } else {
                        "desc"
                    };
                    Ok(json!({ f: { "order": dir } }))
                })
                .collect::<Result<_, CompileError>>()?;
            body["sort"] = Json::Array(sort);
        }

        // from / size pagination.
        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                // ES has `search_after` for keyset; needs the previous
                // page's sort values as the cursor. Out of scope for
                // first cut; surface clearly.
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Elasticsearch,
                    op: "keyset_cursor",
                });
            }
            if let Some(limit) = pag.limit {
                body["size"] = json!(limit);
            }
            if let Some(offset) = pag.offset
                && offset > 0
            {
                body["from"] = json!(offset);
            }
        }

        Ok(CompiledRendering::Json {
            backend: BackendKind::Elasticsearch,
            method: HttpMethod::Post,
            path: format!("/{index}/_search"),
            body,
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
        let index = Self::index_for(table);

        // Validate every field name once.
        for record in &op.records {
            for field in record.keys() {
                self.field_for(table, field, &op.message_type)?;
            }
        }

        if matches!(op.conflict, ConflictStrategy::Ignore) {
            // ES bulk supports `op_type: "create"` which fails on
            // existing docs but doesn't silently skip. Reject so the
            // caller doesn't get misleading semantics.
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Elasticsearch,
                op: "insert_ignore",
            });
        }

        // _bulk body: alternating action-line + document-line, each
        // newline-terminated. We use `index` (upsert) which matches
        // ConflictStrategy::Replace; `create` is too strict for the
        // common case.
        let pk_field = if table.primary_key.is_empty() {
            None
        } else {
            Some(table.primary_key[0].as_str())
        };

        let mut bulk = String::new();
        for record in &op.records {
            // Action line. When the record has a primary-key value,
            // use it as `_id` so the document is addressable later.
            let mut action = Map::new();
            let mut idx_meta = Map::new();
            idx_meta.insert("_index".into(), json!(index));
            if let Some(pk) = pk_field
                && let Some(pk_value) = record.get(pk)
            {
                idx_meta.insert("_id".into(), value_to_es_json(pk_value));
            }
            action.insert("index".into(), Json::Object(idx_meta));
            bulk.push_str(&serde_json::to_string(&action).map_err(|e| {
                CompileError::BackendSpecific {
                    backend: BackendKind::Elasticsearch,
                    message: format!("bulk action serialise failed: {e}"),
                }
            })?);
            bulk.push('\n');

            // Document line. C7/C8: stamp tenant + project so reads
            // can scope.
            let doc = record_to_es_json_with_context(record, ctx);
            bulk.push_str(&serde_json::to_string(&doc).map_err(|e| {
                CompileError::BackendSpecific {
                    backend: BackendKind::Elasticsearch,
                    message: format!("bulk doc serialise failed: {e}"),
                }
            })?);
            bulk.push('\n');
        }

        Ok(CompiledRendering::Json {
            backend: BackendKind::Elasticsearch,
            method: HttpMethod::Post,
            // _bulk takes NDJSON body — the executor sends it with
            // Content-Type: application/x-ndjson; serde_json::Value
            // is a stand-in carrying the rendered NDJSON string.
            path: format!("/{index}/_bulk"),
            body: json!({ "ndjson": bulk }),
        })
    }

    fn compile_delete(
        &self,
        op: &LogicalDelete,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        let index = Self::index_for(table);
        let user_query = self.render_filter(&op.filter, table, &op.message_type)?;
        // Refuse empty / match-all to avoid wiping the index.
        if user_query == json!({ "match_all": {} }) {
            return Err(CompileError::Malformed {
                reason: "LogicalDelete::filter cannot be empty; use Drop resource to truncate"
                    .into(),
            });
        }
        // C7/C8: AND the tenant predicate so a permissive caller
        // filter can't delete another tenant's documents.
        let query = self.and_with_context(user_query, ctx);
        Ok(CompiledRendering::Json {
            backend: BackendKind::Elasticsearch,
            method: HttpMethod::Post,
            path: format!("/{index}/_delete_by_query"),
            body: json!({ "query": query }),
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
        let index = Self::index_for(table);

        // Pre-aggregation filter (WHERE before GROUP BY).
        let user_query = match &op.filter {
            Some(f) => self.render_filter(f, table, &op.message_type)?,
            None => json!({ "match_all": {} }),
        };
        let query = self.and_with_context(user_query, ctx);

        // Build the `aggs` block. When `group_by` is non-empty, wrap
        // every aggregate inside a `terms` bucket over the first
        // group-by field (ES nests aggregations differently from SQL
        // — multi-field GROUP BY uses `composite` aggregation).
        let aggs = if op.group_by.is_empty() {
            // No grouping → flat metric aggregations.
            let mut metrics = Map::new();
            for agg in &op.aggregates {
                metrics.insert(
                    agg.alias.clone(),
                    render_es_metric_aggregate(agg, table, &op.message_type)?,
                );
            }
            Json::Object(metrics)
        } else if op.group_by.len() == 1 {
            // Single GROUP BY column → `terms` aggregation with nested
            // metrics.
            let group_field = self.field_for(table, &op.group_by[0], &op.message_type)?;
            let mut nested_metrics = Map::new();
            for agg in &op.aggregates {
                nested_metrics.insert(
                    agg.alias.clone(),
                    render_es_metric_aggregate(agg, table, &op.message_type)?,
                );
            }
            json!({
                "by_group": {
                    "terms": { "field": group_field, "size": 10_000 },
                    "aggs": Json::Object(nested_metrics)
                }
            })
        } else {
            // Multi-column GROUP BY → `composite` aggregation; ES's
            // canonical answer to "SQL GROUP BY a, b, c".
            let sources: Vec<Json> = op
                .group_by
                .iter()
                .map(|f| {
                    let resolved = self.field_for(table, f, &op.message_type)?;
                    Ok(json!({ resolved: { "terms": { "field": resolved } } }))
                })
                .collect::<Result<_, CompileError>>()?;
            let mut nested_metrics = Map::new();
            for agg in &op.aggregates {
                nested_metrics.insert(
                    agg.alias.clone(),
                    render_es_metric_aggregate(agg, table, &op.message_type)?,
                );
            }
            json!({
                "by_composite": {
                    "composite": {
                        "size": 10_000,
                        "sources": sources
                    },
                    "aggs": Json::Object(nested_metrics)
                }
            })
        };

        let body = json!({
            "size": 0, // we only want the aggregations, no document hits
            "query": query,
            "aggs": aggs
        });

        Ok(CompiledRendering::Json {
            backend: BackendKind::Elasticsearch,
            method: HttpMethod::Post,
            path: format!("/{index}/_search"),
            body,
        })
    }

    fn compile_search(
        &self,
        op: &LogicalSearch,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        let index = Self::index_for(table);

        // ES supports three search modes:
        //   1. Dense vector via `knn` (ES 8.x+); needs an index with
        //      a `dense_vector` mapping.
        //   2. Lexical text via `multi_match` over every analysed
        //      string field.
        //   3. Hybrid (vector + text) via the `_search` body's
        //      `knn` + `query` combination — ES sums the scores.
        let mut body = Map::new();
        body.insert("size".into(), json!(op.top_k));

        // Optional filter (tenant + user filter).
        let user_query = match &op.filter {
            Some(f) => Some(self.render_filter(f, table, &op.message_type)?),
            None => None,
        };
        let filter_with_ctx = self.and_with_context(
            user_query.unwrap_or_else(|| json!({ "match_all": {} })),
            ctx,
        );

        let has_vector = op.vector.is_some();
        let has_text = op
            .text_query
            .as_ref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);

        if !has_vector && !has_text {
            return Err(CompileError::Malformed {
                reason: "Elasticsearch search requires either a vector or text_query".into(),
            });
        }

        if let Some(vector) = &op.vector {
            // knn search. Convention: vector field is `_vector`.
            let mut knn = Map::new();
            knn.insert("field".into(), json!("_vector"));
            knn.insert("query_vector".into(), json!(vector));
            knn.insert("k".into(), json!(op.top_k));
            knn.insert("num_candidates".into(), json!((op.top_k * 10).max(100)));
            // Per-knn filter — ES applies it before kNN ranking.
            knn.insert("filter".into(), filter_with_ctx.clone());
            body.insert("knn".into(), Json::Object(knn));
        }

        if has_text {
            let text = op.text_query.as_deref().unwrap();
            // multi_match over all analysed string columns ANDed with
            // the tenant predicate. When both knn and query are
            // present ES combines scores; this is the hybrid path.
            let text_fields: Vec<Json> = table
                .columns
                .iter()
                .filter(|c| c.proto_type.eq_ignore_ascii_case("string"))
                .map(|c| json!(c.field_name))
                .collect();
            if text_fields.is_empty() {
                return Err(CompileError::Malformed {
                    reason: format!(
                        "text search requires at least one string column in '{}'",
                        op.message_type
                    ),
                });
            }
            body.insert(
                "query".into(),
                json!({
                    "bool": {
                        "must": [
                            { "multi_match": { "query": text, "fields": text_fields } }
                        ],
                        "filter": filter_with_ctx
                    }
                }),
            );
        } else if has_vector {
            // Vector-only — apply tenant filter via the knn body's
            // `filter` field (set above). No top-level `query`.
        }

        if let Some(threshold) = op.score_threshold {
            body.insert("min_score".into(), json!(threshold));
        }

        Ok(CompiledRendering::Json {
            backend: BackendKind::Elasticsearch,
            method: HttpMethod::Post,
            path: format!("/{index}/_search"),
            body: Json::Object(body),
        })
    }

    fn compile_resource_op(
        &self,
        op: &LogicalResourceOp,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        if !matches!(
            op.resource_kind,
            ResourceKind::Index | ResourceKind::Collection
        ) {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Elasticsearch,
                op: "non_index_resource",
            });
        }
        let index_name = op.resource_name.to_ascii_lowercase();
        let (method, path, body) = match op.op {
            ResourceOpKind::Ensure => {
                // Create-index. Spec passes through as the request
                // body (mappings, settings, aliases). When no spec
                // is supplied we emit an empty body — ES creates the
                // index with dynamic mappings.
                let body = op.spec.clone().unwrap_or_else(|| json!({}));
                (HttpMethod::Put, format!("/{index_name}"), body)
            }
            ResourceOpKind::Drop => (HttpMethod::Delete, format!("/{index_name}"), json!({})),
            ResourceOpKind::List => (
                HttpMethod::Get,
                "/_cat/indices?format=json".to_string(),
                json!({}),
            ),
        };
        Ok(CompiledRendering::Json {
            backend: BackendKind::Elasticsearch,
            method,
            path,
            body,
        })
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn render_es_metric_aggregate(
    agg: &AggregateExpr,
    table: &ManifestTable,
    message_type: &str,
) -> Result<Json, CompileError> {
    Ok(match agg.func {
        AggregateFunc::Count => {
            if agg.field == "*" {
                // value_count needs a field; use a constant_field via
                // the document's _id which is always present.
                json!({ "value_count": { "field": "_id" } })
            } else {
                let f = resolve_es_field(table, &agg.field, message_type)?;
                json!({ "value_count": { "field": f } })
            }
        }
        AggregateFunc::CountDistinct => {
            if agg.field == "*" {
                return Err(CompileError::Malformed {
                    reason: "COUNT(DISTINCT *) is not allowed; specify a field".into(),
                });
            }
            let f = resolve_es_field(table, &agg.field, message_type)?;
            json!({ "cardinality": { "field": f } })
        }
        AggregateFunc::Sum => {
            require_real_field(agg)?;
            let f = resolve_es_field(table, &agg.field, message_type)?;
            json!({ "sum": { "field": f } })
        }
        AggregateFunc::Avg => {
            require_real_field(agg)?;
            let f = resolve_es_field(table, &agg.field, message_type)?;
            json!({ "avg": { "field": f } })
        }
        AggregateFunc::Min => {
            require_real_field(agg)?;
            let f = resolve_es_field(table, &agg.field, message_type)?;
            json!({ "min": { "field": f } })
        }
        AggregateFunc::Max => {
            require_real_field(agg)?;
            let f = resolve_es_field(table, &agg.field, message_type)?;
            json!({ "max": { "field": f } })
        }
    })
}

fn require_real_field(agg: &AggregateExpr) -> Result<(), CompileError> {
    if agg.field == "*" {
        Err(CompileError::Malformed {
            reason: format!("{} requires a field name, not '*'", agg.func.sql_token()),
        })
    } else {
        Ok(())
    }
}

fn resolve_es_field<'a>(
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

fn value_to_es_json(v: &LogicalValue) -> Json {
    match v {
        LogicalValue::Null => Json::Null,
        LogicalValue::Bool(b) => Json::Bool(*b),
        LogicalValue::Int(i) => Json::Number((*i).into()),
        LogicalValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        LogicalValue::String(s) => Json::String(s.clone()),
        LogicalValue::Bytes(b) => {
            use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
            Json::String(B64.encode(b))
        }
        LogicalValue::Timestamp(t) => Json::String(t.to_rfc3339()),
        LogicalValue::Json(j) => j.clone(),
        LogicalValue::Array(values) => Json::Array(values.iter().map(value_to_es_json).collect()),
    }
}

fn record_to_es_json_with_context(
    record: &crate::ir::operations::LogicalRecord,
    ctx: &CompileContext<'_>,
) -> Json {
    let mut map = Map::new();
    for (k, v) in record {
        map.insert(k.clone(), value_to_es_json(v));
    }
    if let Some(tid) = ctx.tenant_id
        && !tid.is_empty()
    {
        map.insert("_tenant_id".into(), json!(tid));
    }
    if let Some(pid) = ctx.project_id
        && !pid.is_empty()
    {
        map.insert("_project_id".into(), json!(pid));
    }
    Json::Object(map)
}

fn string_or_err(v: &LogicalValue, op_token: &str, field: &str) -> Result<String, CompileError> {
    match v {
        LogicalValue::String(s) => Ok(s.clone()),
        _ => Err(CompileError::Malformed {
            reason: format!("{op_token} requires a String value on field '{field}'"),
        }),
    }
}

/// Translate SQL `LIKE` (`%`, `_`) to ES `wildcard` (`*`, `?`).
fn like_to_wildcard(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    for ch in pattern.chars() {
        match ch {
            '%' => out.push('*'),
            '_' => out.push('?'),
            // ES `wildcard` treats `*` and `?` as wildcards in the
            // value, so escape literal stars / question-marks
            // present in the user input.
            '*' | '?' => {
                out.push('\\');
                out.push(ch);
            }
            other => out.push(other),
        }
    }
    out
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
    use crate::ir::projection::LogicalPagination;
    use crate::ir::value::LogicalValue;

    fn fixture() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.docs.v1.Doc".into(),
            schema: "public".into(),
            table: "documents".into(),
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
                ManifestColumn {
                    field_name: "score".into(),
                    column_name: "score".into(),
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

    fn extract_json(rendering: CompiledRendering) -> (String, Json) {
        match rendering {
            CompiledRendering::Json { path, body, .. } => (path, body),
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn read_emits_search_with_term_filter() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("acme.docs.v1.Doc")
            .with_filter(LogicalFilter::Comparison {
                field: "title".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("rust".into()),
            })
            .with_pagination(LogicalPagination::limit(10));
        let (path, body) = extract_json(ElasticsearchCompiler.compile_read(&read, &ctx).unwrap());
        assert_eq!(path, "/documents/_search");
        assert_eq!(body["query"]["term"]["title"], "rust");
        assert_eq!(body["size"], 10);
    }

    #[test]
    fn read_with_tenant_context_ands_in_term() {
        let m = fixture();
        let ctx = CompileContext::new(&m)
            .with_tenant("acme")
            .with_project("p1");
        let read = LogicalRead::message("acme.docs.v1.Doc");
        let (_, body) = extract_json(ElasticsearchCompiler.compile_read(&read, &ctx).unwrap());
        let must = body["query"]["bool"]["must"]
            .as_array()
            .expect("must array");
        // [0] = user query (match_all), [1] = tenant term, [2] = project term
        assert_eq!(must.len(), 3);
        assert_eq!(must[0]["match_all"], json!({}));
        assert_eq!(must[1]["term"]["_tenant_id"], "acme");
        assert_eq!(must[2]["term"]["_project_id"], "p1");
    }

    #[test]
    fn write_bulk_stamps_tenant_in_every_document() {
        let m = fixture();
        let ctx = CompileContext::new(&m).with_tenant("acme");
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("doc1".into()));
        rec.insert("title".into(), LogicalValue::String("hello".into()));
        let write = LogicalWrite {
            message_type: "acme.docs.v1.Doc".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Replace,
            return_fields: vec![],
        };
        let (path, body) = extract_json(ElasticsearchCompiler.compile_write(&write, &ctx).unwrap());
        assert_eq!(path, "/documents/_bulk");
        let ndjson = body["ndjson"].as_str().expect("ndjson");
        assert!(ndjson.contains("\"_index\":\"documents\""));
        assert!(ndjson.contains("\"_id\":\"doc1\""));
        assert!(ndjson.contains("\"_tenant_id\":\"acme\""));
    }

    #[test]
    fn delete_uses_delete_by_query_with_tenant() {
        let m = fixture();
        let ctx = CompileContext::new(&m).with_tenant("acme");
        let del = LogicalDelete {
            message_type: "acme.docs.v1.Doc".into(),
            filter: LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("doc1".into()),
            },
            return_fields: vec![],
        };
        let (path, body) = extract_json(ElasticsearchCompiler.compile_delete(&del, &ctx).unwrap());
        assert_eq!(path, "/documents/_delete_by_query");
        let must = body["query"]["bool"]["must"].as_array().expect("must");
        assert_eq!(must[0]["term"]["id"], "doc1");
        assert_eq!(must[1]["term"]["_tenant_id"], "acme");
    }

    #[test]
    fn delete_refuses_empty_filter() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let del = LogicalDelete {
            message_type: "acme.docs.v1.Doc".into(),
            filter: LogicalFilter::And(vec![]),
            return_fields: vec![],
        };
        let err = ElasticsearchCompiler
            .compile_delete(&del, &ctx)
            .unwrap_err();
        assert!(matches!(err, CompileError::Malformed { .. }));
    }

    #[test]
    fn aggregate_count_all_with_group_by_emits_terms_bucket() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let agg = LogicalAggregate {
            message_type: "acme.docs.v1.Doc".into(),
            filter: None,
            group_by: vec!["title".into()],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Count,
                field: "*".into(),
                alias: "n".into(),
            }],
            having: None,
            sort: vec![],
            pagination: None,
        };
        let (path, body) =
            extract_json(ElasticsearchCompiler.compile_aggregate(&agg, &ctx).unwrap());
        assert_eq!(path, "/documents/_search");
        assert_eq!(body["size"], 0);
        assert_eq!(body["aggs"]["by_group"]["terms"]["field"], "title");
        assert_eq!(
            body["aggs"]["by_group"]["aggs"]["n"]["value_count"]["field"],
            "_id"
        );
    }

    #[test]
    fn aggregate_multi_group_by_uses_composite() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let agg = LogicalAggregate {
            message_type: "acme.docs.v1.Doc".into(),
            filter: None,
            group_by: vec!["title".into(), "id".into()],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                field: "score".into(),
                alias: "total".into(),
            }],
            having: None,
            sort: vec![],
            pagination: None,
        };
        let (_, body) = extract_json(ElasticsearchCompiler.compile_aggregate(&agg, &ctx).unwrap());
        assert!(body["aggs"]["by_composite"]["composite"]["sources"].is_array());
        assert_eq!(
            body["aggs"]["by_composite"]["aggs"]["total"]["sum"]["field"],
            "score"
        );
    }

    #[test]
    fn aggregate_no_group_by_emits_flat_metrics() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let agg = LogicalAggregate {
            message_type: "acme.docs.v1.Doc".into(),
            filter: None,
            group_by: vec![],
            aggregates: vec![
                AggregateExpr {
                    func: AggregateFunc::Sum,
                    field: "score".into(),
                    alias: "total".into(),
                },
                AggregateExpr {
                    func: AggregateFunc::Avg,
                    field: "score".into(),
                    alias: "mean".into(),
                },
            ],
            having: None,
            sort: vec![],
            pagination: None,
        };
        let (_, body) = extract_json(ElasticsearchCompiler.compile_aggregate(&agg, &ctx).unwrap());
        assert_eq!(body["aggs"]["total"]["sum"]["field"], "score");
        assert_eq!(body["aggs"]["mean"]["avg"]["field"], "score");
    }

    #[test]
    fn search_text_only_uses_multi_match() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let search = LogicalSearch {
            message_type: "acme.docs.v1.Doc".into(),
            vector: None,
            text_query: Some("hello world".into()),
            filter: None,
            top_k: 5,
            score_threshold: None,
            require_hybrid: false,
            with_vector: false,
            with_payload: true,
        };
        let (path, body) =
            extract_json(ElasticsearchCompiler.compile_search(&search, &ctx).unwrap());
        assert_eq!(path, "/documents/_search");
        assert_eq!(
            body["query"]["bool"]["must"][0]["multi_match"]["query"],
            "hello world"
        );
        assert_eq!(body["size"], 5);
    }

    #[test]
    fn search_vector_only_uses_knn() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let search = LogicalSearch {
            message_type: "acme.docs.v1.Doc".into(),
            vector: Some(vec![0.1, 0.2, 0.3]),
            text_query: None,
            filter: None,
            top_k: 5,
            score_threshold: Some(0.7),
            require_hybrid: false,
            with_vector: false,
            with_payload: true,
        };
        let (_, body) = extract_json(ElasticsearchCompiler.compile_search(&search, &ctx).unwrap());
        assert_eq!(body["knn"]["field"], "_vector");
        assert_eq!(body["knn"]["k"], 5);
        let threshold = body["min_score"].as_f64().expect("min_score");
        assert!((threshold - 0.7).abs() < 1e-5);
        assert!(body.get("query").is_none()); // vector-only, no text query
    }

    #[test]
    fn search_hybrid_emits_both_knn_and_query() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let search = LogicalSearch {
            message_type: "acme.docs.v1.Doc".into(),
            vector: Some(vec![0.1, 0.2]),
            text_query: Some("rust".into()),
            filter: None,
            top_k: 10,
            score_threshold: None,
            require_hybrid: true,
            with_vector: false,
            with_payload: true,
        };
        let (_, body) = extract_json(ElasticsearchCompiler.compile_search(&search, &ctx).unwrap());
        assert!(body["knn"].is_object());
        assert!(body["query"].is_object());
    }

    #[test]
    fn resource_op_ensure_creates_index_with_spec_body() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let op = LogicalResourceOp {
            op: ResourceOpKind::Ensure,
            resource_kind: ResourceKind::Index,
            resource_name: "Orders".into(),
            spec: Some(json!({
                "mappings": { "properties": { "id": { "type": "keyword" } } }
            })),
        };
        match ElasticsearchCompiler
            .compile_resource_op(&op, &ctx)
            .unwrap()
        {
            CompiledRendering::Json {
                method, path, body, ..
            } => {
                assert_eq!(method, HttpMethod::Put);
                assert_eq!(path, "/orders");
                assert_eq!(body["mappings"]["properties"]["id"]["type"], "keyword");
            }
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn resource_op_drop_deletes_index() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let op = LogicalResourceOp {
            op: ResourceOpKind::Drop,
            resource_kind: ResourceKind::Index,
            resource_name: "orders".into(),
            spec: None,
        };
        match ElasticsearchCompiler
            .compile_resource_op(&op, &ctx)
            .unwrap()
        {
            CompiledRendering::Json { method, path, .. } => {
                assert_eq!(method, HttpMethod::Delete);
                assert_eq!(path, "/orders");
            }
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn like_pattern_translation_handles_wildcards_and_escapes() {
        // SQL `%` → ES `*`, SQL `_` → ES `?`, literal `*` escaped.
        assert_eq!(like_to_wildcard("ab%"), "ab*");
        assert_eq!(like_to_wildcard("a_b"), "a?b");
        assert_eq!(like_to_wildcard("a*b"), "a\\*b");
        assert_eq!(like_to_wildcard("a?b"), "a\\?b");
    }
}
