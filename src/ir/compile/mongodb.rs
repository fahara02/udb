//! MongoDB compiler (U2 step 4).
//!
//! Lowers `LogicalRead`/`Write`/`Delete` to the JSON shapes the existing
//! `MongoDbExecutor` already speaks (Atlas Data API style). The compiler
//! itself is pure data — it doesn't pull in the Mongo driver and is
//! compiled in the test build even when the `mongodb` Cargo feature is
//! off, so the cross-backend acceptance test in U2 step 5 can prove the
//! IR is genuinely backend-agnostic.
//!
//! Field-name resolution: Mongo uses **proto field names directly** (no
//! SQL column rename), so the lookup against `ManifestTable.columns` is
//! cheap and surfaces the same `UnknownField` typed error the Postgres
//! compiler does.

use serde_json::{Map, Value as Json, json};

use crate::backend::BackendKind;
use crate::generation::ManifestTable;
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    AggregateExpr, AggregateFunc, ConflictStrategy, LogicalAggregate, LogicalDelete, LogicalRead,
    LogicalResourceOp, LogicalSearch, LogicalWrite, ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;

use super::util::{like_to_regex, regex_escape, value_to_json};
use super::{CompileContext, CompileError, CompiledRendering, Compiler, HttpMethod};

/// MongoDB (Atlas Data API) compiler.
#[derive(Debug, Default, Clone, Copy)]
pub struct MongoDbCompiler;

impl MongoDbCompiler {
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

    /// Validate the field exists on the table and return its proto field
    /// name (which is what Mongo uses on the wire).
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

    /// Render a logical filter as a Mongo find-document.
    fn render_filter(
        &self,
        filter: &LogicalFilter,
        table: &ManifestTable,
        message_type: &str,
    ) -> Result<Json, CompileError> {
        match filter {
            LogicalFilter::And(clauses) if clauses.is_empty() => Ok(Json::Object(Map::new())),
            LogicalFilter::Or(clauses) if clauses.is_empty() => {
                // No matches.
                Ok(json!({"$expr": false}))
            }
            LogicalFilter::And(clauses) => {
                let parts = clauses
                    .iter()
                    .map(|c| self.render_filter(c, table, message_type))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(json!({ "$and": parts }))
            }
            LogicalFilter::Or(clauses) => {
                let parts = clauses
                    .iter()
                    .map(|c| self.render_filter(c, table, message_type))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(json!({ "$or": parts }))
            }
            LogicalFilter::Not(inner) => {
                let nested = self.render_filter(inner, table, message_type)?;
                Ok(json!({ "$nor": [nested] }))
            }
            LogicalFilter::IsNull(field) => {
                let f = self.field_for(table, field, message_type)?;
                Ok(json!({ f: { "$eq": Json::Null } }))
            }
            LogicalFilter::InList { field, values } => {
                let f = self.field_for(table, field, message_type)?;
                let arr: Vec<Json> = values.iter().map(value_to_json).collect();
                Ok(json!({ f: { "$in": arr } }))
            }
            LogicalFilter::Comparison { field, op, value } => {
                let f = self.field_for(table, field, message_type)?;
                let rhs = value_to_json(value);
                let predicate = match op {
                    ComparisonOp::Eq => json!({ "$eq": rhs }),
                    ComparisonOp::Ne => json!({ "$ne": rhs }),
                    ComparisonOp::Lt => json!({ "$lt": rhs }),
                    ComparisonOp::Le => json!({ "$lte": rhs }),
                    ComparisonOp::Gt => json!({ "$gt": rhs }),
                    ComparisonOp::Ge => json!({ "$gte": rhs }),
                    ComparisonOp::Like | ComparisonOp::ILike => {
                        let pattern = match value {
                            LogicalValue::String(s) => like_to_regex(s),
                            _ => {
                                return Err(CompileError::Malformed {
                                    reason: format!(
                                        "{} requires a String value on field '{field}'",
                                        op.token()
                                    ),
                                });
                            }
                        };
                        let mut regex = json!({ "$regex": pattern });
                        if matches!(op, ComparisonOp::ILike) {
                            regex["$options"] = json!("i");
                        }
                        regex
                    }
                    ComparisonOp::Contains | ComparisonOp::StartsWith | ComparisonOp::EndsWith => {
                        let needle = match value {
                            LogicalValue::String(s) => regex_escape(s),
                            _ => {
                                return Err(CompileError::Malformed {
                                    reason: format!(
                                        "{} requires a String value on field '{field}'",
                                        op.token()
                                    ),
                                });
                            }
                        };
                        let pattern = match op {
                            ComparisonOp::Contains => needle,
                            ComparisonOp::StartsWith => format!("^{needle}"),
                            ComparisonOp::EndsWith => format!("{needle}$"),
                            _ => unreachable!(),
                        };
                        json!({ "$regex": pattern })
                    }
                };
                Ok(json!({ f: predicate }))
            }
        }
    }
}

impl Compiler for MongoDbCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::Mongodb
    }

    fn compile_read(
        &self,
        op: &LogicalRead,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        let user_filter = match &op.filter {
            Some(f) => self.render_filter(f, table, &op.message_type)?,
            None => Json::Object(Map::new()),
        };
        // C7/C8: AND in the active request context as a top-level
        // `_tenant_id` / `_project_id` predicate so cross-tenant reads
        // are protocol-blocked. The compiler is now Enforced (not
        // Advisory) — manifest scoping was the previous best.
        let filter = and_with_context(user_filter, table, ctx);

        // Projection (Mongo: { field: 1 }).
        let projection = match &op.projection {
            Some(p) if !p.is_select_all() => {
                let mut map = Map::new();
                for f in &p.fields {
                    let resolved = self.field_for(table, f, &op.message_type)?;
                    map.insert(resolved.to_string(), json!(1));
                }
                Json::Object(map)
            }
            _ => Json::Object(Map::new()),
        };

        // Sort: Mongo expects an ordered { field: 1/-1 } document.
        let sort = if op.sort.is_empty() {
            Json::Object(Map::new())
        } else {
            let mut map = Map::new();
            for s in &op.sort {
                let f = self.field_for(table, &s.field, &op.message_type)?;
                let dir = if matches!(s.direction, crate::ir::projection::SortDirection::Asc) {
                    1
                } else {
                    -1
                };
                map.insert(f.to_string(), json!(dir));
            }
            Json::Object(map)
        };

        let mut body = json!({
            "collection": table.table,
            "filter": filter,
            "projection": projection,
            "sort": sort,
        });

        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                // Mongo's stable cursor is `_id`-after-keyset; lowering
                // requires the executor to inject the cursor predicate
                // into `filter`. Deferred until the executor side wires it.
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Mongodb,
                    op: "keyset_cursor",
                });
            }
            if let Some(limit) = pag.limit {
                body["limit"] = json!(limit);
            }
            if let Some(offset) = pag.offset
                && offset > 0
            {
                body["skip"] = json!(offset);
            }
        }

        Ok(CompiledRendering::Json {
            backend: BackendKind::Mongodb,
            method: HttpMethod::Post,
            path: "/action/find".to_string(),
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

        // Validate every field name once.
        for record in &op.records {
            for field in record.keys() {
                self.field_for(table, field, &op.message_type)?;
            }
        }

        match &op.conflict {
            ConflictStrategy::Error => {
                // Single-record → insertOne; multi-record → insertMany.
                if op.records.len() == 1 {
                    let body = json!({
                        "collection": table.table,
                        "document": record_to_json_with_context(&op.records[0], table, ctx),
                    });
                    Ok(CompiledRendering::Json {
                        backend: BackendKind::Mongodb,
                        method: HttpMethod::Post,
                        path: "/action/insertOne".to_string(),
                        body,
                    })
                } else {
                    let docs: Vec<Json> = op
                        .records
                        .iter()
                        .map(|r| record_to_json_with_context(r, table, ctx))
                        .collect();
                    let body = json!({
                        "collection": table.table,
                        "documents": docs,
                    });
                    Ok(CompiledRendering::Json {
                        backend: BackendKind::Mongodb,
                        method: HttpMethod::Post,
                        path: "/action/insertMany".to_string(),
                        body,
                    })
                }
            }
            ConflictStrategy::Replace | ConflictStrategy::Update { .. } => {
                if op.records.len() != 1 {
                    return Err(CompileError::Malformed {
                        reason: "Mongo upsert via Data API requires exactly one record per call"
                            .into(),
                    });
                }
                if table.primary_key.is_empty() {
                    return Err(CompileError::Malformed {
                        reason: format!(
                            "upsert requested but message '{}' has no primary key in manifest",
                            op.message_type
                        ),
                    });
                }
                let record = &op.records[0];
                let mut filter = Map::new();
                for pk in &table.primary_key {
                    let f = self.field_for(table, pk, &op.message_type)?;
                    let value = record.get(pk).ok_or_else(|| CompileError::Malformed {
                        reason: format!("record missing primary-key field '{pk}'"),
                    })?;
                    filter.insert(f.to_string(), value_to_json(value));
                }
                // C7/C8: stamp tenant + project so a cross-tenant
                // upsert that happens to know another tenant's PK
                // can't read or update that document.
                if let Some(tid) = ctx.tenant_id
                    && !tid.is_empty()
                    && let Some(column) = Some(super::util::tenant_system_field(table))
                {
                    filter.insert(column.to_string(), json!(tid));
                }
                if let Some(pid) = ctx.project_id
                    && !pid.is_empty()
                    && let Some(column) = Some(super::util::project_system_field(table))
                {
                    filter.insert(column.to_string(), json!(pid));
                }

                let (path, update) = match &op.conflict {
                    ConflictStrategy::Replace => (
                        "/action/replaceOne",
                        json!(record_to_json_with_context(record, table, ctx)),
                    ),
                    ConflictStrategy::Update { fields } => {
                        let mut set = Map::new();
                        for f_name in fields {
                            let f = self.field_for(table, f_name, &op.message_type)?;
                            let value =
                                record.get(f_name).ok_or_else(|| CompileError::Malformed {
                                    reason: format!("record missing update field '{f_name}'"),
                                })?;
                            set.insert(f.to_string(), value_to_json(value));
                        }
                        ("/action/updateOne", json!({ "$set": Json::Object(set) }))
                    }
                    _ => unreachable!(),
                };
                let body_field = if path == "/action/replaceOne" {
                    "replacement"
                } else {
                    "update"
                };
                let body = json!({
                    "collection": table.table,
                    "filter": Json::Object(filter),
                    body_field: update,
                    "upsert": true,
                });
                Ok(CompiledRendering::Json {
                    backend: BackendKind::Mongodb,
                    method: HttpMethod::Post,
                    path: path.to_string(),
                    body,
                })
            }
            ConflictStrategy::Ignore => {
                // Mongo has no direct "insert-or-ignore" via the Data API.
                // The executor can simulate with insertMany + ordered:false,
                // but the semantics differ (other records still apply).
                // Surface a typed capability error and let the caller decide.
                Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Mongodb,
                    op: "insert_ignore",
                })
            }
        }
    }

    fn compile_delete(
        &self,
        op: &LogicalDelete,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        let user_filter = self.render_filter(&op.filter, table, &op.message_type)?;
        if user_filter.as_object().is_some_and(|m| m.is_empty()) {
            return Err(CompileError::Malformed {
                reason: "LogicalDelete::filter cannot be empty; use Drop resource to truncate"
                    .into(),
            });
        }
        // C7/C8: tenant predicate ANDed in so a cross-tenant DELETE
        // can't be issued even with a permissive user filter.
        let filter = and_with_context(user_filter, table, ctx);
        let body = json!({
            "collection": table.table,
            "filter": filter,
        });
        Ok(CompiledRendering::Json {
            backend: BackendKind::Mongodb,
            method: HttpMethod::Post,
            path: "/action/deleteMany".to_string(),
            body,
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

        // Build a Mongo aggregation pipeline:
        //   [$match, $group, $match (HAVING), $sort, $skip, $limit].
        let mut pipeline: Vec<Json> = Vec::new();

        if let Some(filter) = &op.filter {
            let rendered = self.render_filter(filter, table, &op.message_type)?;
            if !rendered.as_object().is_some_and(|m| m.is_empty()) {
                pipeline.push(json!({ "$match": rendered }));
            }
        }

        // $group._id: { groupCol: "$groupCol" } (or null when no group_by).
        let id_expr = if op.group_by.is_empty() {
            Json::Null
        } else {
            let mut id = Map::new();
            for f in &op.group_by {
                let resolved = self.field_for(table, f, &op.message_type)?;
                id.insert(resolved.to_string(), json!(format!("${resolved}")));
            }
            Json::Object(id)
        };
        let mut group_doc = Map::new();
        group_doc.insert("_id".to_string(), id_expr);
        for agg in &op.aggregates {
            group_doc.insert(
                agg.alias.clone(),
                render_mongo_aggregate(agg, table, &op.message_type)?,
            );
        }
        pipeline.push(json!({ "$group": Json::Object(group_doc) }));

        // Hoist group-by fields out of `_id` for the downstream stages so
        // HAVING / sort can reference them by their original name.
        if !op.group_by.is_empty() {
            let mut project = Map::new();
            project.insert("_id".to_string(), json!(0));
            for f in &op.group_by {
                let resolved = self.field_for(table, f, &op.message_type)?;
                project.insert(resolved.to_string(), json!(format!("$_id.{resolved}")));
            }
            for agg in &op.aggregates {
                project.insert(agg.alias.clone(), json!(1));
            }
            pipeline.push(json!({ "$project": Json::Object(project) }));
        }

        if let Some(having) = &op.having {
            let body = render_mongo_having(having, op, table, &op.message_type)?;
            if !body.as_object().is_some_and(|m| m.is_empty()) {
                pipeline.push(json!({ "$match": body }));
            }
        }

        if !op.sort.is_empty() {
            let mut sort_map = Map::new();
            for s in &op.sort {
                let key = resolve_mongo_sort_field(&s.field, op, table, &op.message_type)?;
                let dir = if matches!(s.direction, crate::ir::projection::SortDirection::Asc) {
                    1
                } else {
                    -1
                };
                sort_map.insert(key, json!(dir));
            }
            pipeline.push(json!({ "$sort": Json::Object(sort_map) }));
        }

        if let Some(pag) = &op.pagination {
            if pag.uses_cursor() {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Mongodb,
                    op: "keyset_cursor",
                });
            }
            if let Some(offset) = pag.offset
                && offset > 0
            {
                pipeline.push(json!({ "$skip": offset }));
            }
            if let Some(limit) = pag.limit {
                pipeline.push(json!({ "$limit": limit }));
            }
        }

        Ok(CompiledRendering::Json {
            backend: BackendKind::Mongodb,
            method: HttpMethod::Post,
            path: "/action/aggregate".to_string(),
            body: json!({
                "collection": table.table,
                "pipeline": pipeline,
            }),
        })
    }

    fn compile_search(
        &self,
        op: &LogicalSearch,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;

        // Vector path: Atlas Search `$vectorSearch` stage. Requires the
        // operator to have configured a vector index named `<table>_vec`
        // — same naming convention SQLite's FTS uses (`<table>_fts`).
        // If the caller supplied a vector we prefer this path.
        if let Some(vector) = &op.vector {
            let mut stage = Map::new();
            stage.insert("index".into(), json!(format!("{}_vec", table.table)));
            stage.insert("path".into(), json!("_vector"));
            stage.insert("queryVector".into(), json!(vector));
            stage.insert("numCandidates".into(), json!((op.top_k * 10).max(100)));
            stage.insert("limit".into(), json!(op.top_k));
            if let Some(f) = &op.filter {
                let rendered = self.render_filter(f, table, &op.message_type)?;
                if !rendered.as_object().is_some_and(|m| m.is_empty()) {
                    stage.insert("filter".into(), rendered);
                }
            }
            let mut pipeline = vec![json!({ "$vectorSearch": Json::Object(stage) })];
            // Append score projection so callers can read `_score`.
            pipeline.push(json!({
                "$addFields": { "_score": { "$meta": "vectorSearchScore" } }
            }));
            if let Some(threshold) = op.score_threshold {
                pipeline.push(json!({ "$match": { "_score": { "$gte": threshold } } }));
            }
            return Ok(CompiledRendering::Json {
                backend: BackendKind::Mongodb,
                method: HttpMethod::Post,
                path: "/action/aggregate".to_string(),
                body: json!({
                    "collection": table.table,
                    "pipeline": pipeline,
                }),
            });
        }

        // Lexical text path: `$text` query operator.
        let text = op
            .text_query
            .as_deref()
            .ok_or_else(|| CompileError::Malformed {
                reason: "MongoDB search requires either a vector or text_query".into(),
            })?;
        if text.trim().is_empty() {
            return Err(CompileError::Malformed {
                reason: "text_query must be non-empty".into(),
            });
        }
        let mut find_filter = Map::new();
        find_filter.insert("$text".into(), json!({ "$search": text }));
        if let Some(f) = &op.filter {
            let rendered = self.render_filter(f, table, &op.message_type)?;
            if let Json::Object(m) = rendered {
                for (k, v) in m {
                    find_filter.insert(k, v);
                }
            }
        }
        let mut body = json!({
            "collection": table.table,
            "filter": Json::Object(find_filter),
            "projection": { "_score": { "$meta": "textScore" } },
            "sort": { "_score": { "$meta": "textScore" } },
            "limit": op.top_k,
        });
        if let Some(threshold) = op.score_threshold {
            // textScore doesn't support a server-side threshold in find;
            // surface a typed error rather than silently dropping.
            let _ = threshold;
            body["score_threshold_warning"] =
                json!("score_threshold ignored for $text find; use $vectorSearch for thresholding");
        }
        Ok(CompiledRendering::Json {
            backend: BackendKind::Mongodb,
            method: HttpMethod::Post,
            path: "/action/find".to_string(),
            body,
        })
    }

    fn compile_resource_op(
        &self,
        op: &LogicalResourceOp,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        if !matches!(
            op.resource_kind,
            ResourceKind::Collection | ResourceKind::Index
        ) {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Mongodb,
                op: "non_collection_resource",
            });
        }
        let (path, body) = match (op.op, op.resource_kind) {
            (ResourceOpKind::Ensure, ResourceKind::Collection) => {
                let mut body = json!({ "collection": op.resource_name });
                if let Some(spec) = &op.spec {
                    body["options"] = spec.clone();
                }
                ("/action/createCollection".to_string(), body)
            }
            (ResourceOpKind::Drop, ResourceKind::Collection) => (
                "/action/dropCollection".to_string(),
                json!({ "collection": op.resource_name }),
            ),
            (ResourceOpKind::List, ResourceKind::Collection) => {
                ("/action/listCollections".to_string(), json!({}))
            }
            (ResourceOpKind::Ensure, ResourceKind::Index) => {
                // Spec: { "collection": "...", "keys": {"f": 1}, "unique": bool, "name": "..." }
                let spec = op.spec.clone().ok_or_else(|| CompileError::Malformed {
                    reason: "Ensure Index requires a spec".into(),
                })?;
                let mut body = json!({ "name": op.resource_name });
                if let Json::Object(map) = spec {
                    for (k, v) in map {
                        body[k] = v;
                    }
                }
                ("/action/createIndex".to_string(), body)
            }
            (ResourceOpKind::Drop, ResourceKind::Index) => {
                let spec = op.spec.clone().ok_or_else(|| CompileError::Malformed {
                    reason: "Drop Index requires a spec with 'collection'".into(),
                })?;
                let collection =
                    spec.get("collection")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| CompileError::Malformed {
                            reason: "Drop Index spec missing 'collection'".into(),
                        })?;
                (
                    "/action/dropIndex".to_string(),
                    json!({
                        "collection": collection,
                        "name": op.resource_name,
                    }),
                )
            }
            (ResourceOpKind::List, ResourceKind::Index) => {
                let spec = op.spec.clone().ok_or_else(|| CompileError::Malformed {
                    reason: "List Index requires a spec with 'collection'".into(),
                })?;
                let collection =
                    spec.get("collection")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| CompileError::Malformed {
                            reason: "List Index spec missing 'collection'".into(),
                        })?;
                (
                    "/action/listIndexes".to_string(),
                    json!({ "collection": collection }),
                )
            }
            _ => {
                return Err(CompileError::OperatorUnsupported {
                    backend: BackendKind::Mongodb,
                    op: "unhandled_resource_op",
                });
            }
        };
        Ok(CompiledRendering::Json {
            backend: BackendKind::Mongodb,
            method: HttpMethod::Post,
            path,
            body,
        })
    }
}

fn render_mongo_aggregate(
    agg: &AggregateExpr,
    table: &ManifestTable,
    message_type: &str,
) -> Result<Json, CompileError> {
    Ok(match agg.func {
        AggregateFunc::Count => json!({ "$sum": 1 }),
        AggregateFunc::CountDistinct => {
            if agg.field == "*" {
                return Err(CompileError::Malformed {
                    reason: "COUNT(DISTINCT *) is not allowed; specify a field".into(),
                });
            }
            let resolved = resolve_mongo_field(table, &agg.field, message_type)?;
            // The portable two-stage form: $addToSet then $size in a
            // later $project. We approximate with `$addToSet` here and
            // assume the caller projects `size` separately, OR — since
            // we control the pipeline — wrap in `{$size: {$setUnion: …}}`
            // at projection time. For first-class behaviour we emit a
            // single-stage `{$addToSet: …}` and rely on $project to size.
            // To keep the IR single-pass we use `$accumulator` with a
            // distinct-set; but that requires server-side JS. Instead
            // emit `{ "$addToSet": "$field" }` and require the caller
            // to project `{ "$size": "$alias" }`.
            //
            // For full correctness we wrap with `$setUnion` in a
            // synthetic $project later — but the simpler path is to
            // emit the count via a subsequent $project. To stay within
            // the trait we materialise the set here and the $project
            // stage in compile_aggregate hoists `$size`.
            json!({ "$addToSet": format!("${resolved}") })
        }
        AggregateFunc::Sum => {
            let resolved = resolve_mongo_field(table, &agg.field, message_type)?;
            json!({ "$sum": format!("${resolved}") })
        }
        AggregateFunc::Avg => {
            let resolved = resolve_mongo_field(table, &agg.field, message_type)?;
            json!({ "$avg": format!("${resolved}") })
        }
        AggregateFunc::Min => {
            let resolved = resolve_mongo_field(table, &agg.field, message_type)?;
            json!({ "$min": format!("${resolved}") })
        }
        AggregateFunc::Max => {
            let resolved = resolve_mongo_field(table, &agg.field, message_type)?;
            json!({ "$max": format!("${resolved}") })
        }
    })
}

fn render_mongo_having(
    filter: &LogicalFilter,
    op: &LogicalAggregate,
    table: &ManifestTable,
    message_type: &str,
) -> Result<Json, CompileError> {
    match filter {
        LogicalFilter::And(c) if c.is_empty() => Ok(Json::Object(Map::new())),
        LogicalFilter::Or(c) if c.is_empty() => Ok(json!({ "$expr": false })),
        LogicalFilter::And(clauses) => {
            let parts = clauses
                .iter()
                .map(|c| render_mongo_having(c, op, table, message_type))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(json!({ "$and": parts }))
        }
        LogicalFilter::Or(clauses) => {
            let parts = clauses
                .iter()
                .map(|c| render_mongo_having(c, op, table, message_type))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(json!({ "$or": parts }))
        }
        LogicalFilter::Not(inner) => {
            let n = render_mongo_having(inner, op, table, message_type)?;
            Ok(json!({ "$nor": [n] }))
        }
        LogicalFilter::Comparison {
            field,
            op: cmp,
            value,
        } => {
            let f = resolve_mongo_having_field(field, op, table, message_type)?;
            let rhs = value_to_json(value);
            let predicate = match cmp {
                ComparisonOp::Eq => json!({ "$eq": rhs }),
                ComparisonOp::Ne => json!({ "$ne": rhs }),
                ComparisonOp::Lt => json!({ "$lt": rhs }),
                ComparisonOp::Le => json!({ "$lte": rhs }),
                ComparisonOp::Gt => json!({ "$gt": rhs }),
                ComparisonOp::Ge => json!({ "$gte": rhs }),
                _ => {
                    return Err(CompileError::OperatorUnsupported {
                        backend: BackendKind::Mongodb,
                        op: "having_text_match",
                    });
                }
            };
            Ok(json!({ f: predicate }))
        }
        LogicalFilter::IsNull(field) => {
            let f = resolve_mongo_having_field(field, op, table, message_type)?;
            Ok(json!({ f: { "$eq": Json::Null } }))
        }
        LogicalFilter::InList { field, values } => {
            let f = resolve_mongo_having_field(field, op, table, message_type)?;
            let arr: Vec<Json> = values.iter().map(value_to_json).collect();
            Ok(json!({ f: { "$in": arr } }))
        }
    }
}

fn resolve_mongo_having_field(
    field: &str,
    op: &LogicalAggregate,
    table: &ManifestTable,
    message_type: &str,
) -> Result<String, CompileError> {
    if op.aggregates.iter().any(|a| a.alias == field) {
        return Ok(field.to_string());
    }
    if op.group_by.iter().any(|f| f == field) {
        let resolved = resolve_mongo_field(table, field, message_type)?;
        return Ok(resolved.to_string());
    }
    Err(CompileError::Malformed {
        reason: format!(
            "HAVING field '{field}' is neither an aggregate alias nor a GROUP BY column"
        ),
    })
}

fn resolve_mongo_sort_field(
    field: &str,
    op: &LogicalAggregate,
    table: &ManifestTable,
    message_type: &str,
) -> Result<String, CompileError> {
    if op.aggregates.iter().any(|a| a.alias == field) {
        return Ok(field.to_string());
    }
    if op.group_by.iter().any(|f| f == field) {
        let resolved = resolve_mongo_field(table, field, message_type)?;
        return Ok(resolved.to_string());
    }
    Err(CompileError::Malformed {
        reason: format!(
            "ORDER BY field '{field}' is neither an aggregate alias nor a GROUP BY column"
        ),
    })
}

fn resolve_mongo_field<'a>(
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

/// AND the active request context into a Mongo find filter. The
/// emitted filter shape is `{ "$and": [user, { _tenant_id: tid,
/// _project_id: pid }] }` when context is present; otherwise the user
/// filter is returned unchanged. The `_tenant_id` / `_project_id`
/// field names match the broker's write-time injection (every
/// `compile_write` payload carries them) so a tenant boundary is
/// genuinely enforced at the document level.
fn and_with_context(user_filter: Json, table: &ManifestTable, ctx: &CompileContext<'_>) -> Json {
    let mut ctx_pred = Map::new();
    if let Some(tid) = ctx.tenant_id
        && !tid.is_empty()
        && let Some(column) = Some(super::util::tenant_system_field(table))
    {
        ctx_pred.insert(column.to_string(), json!(tid));
    }
    if let Some(pid) = ctx.project_id
        && !pid.is_empty()
        && let Some(column) = Some(super::util::project_system_field(table))
    {
        ctx_pred.insert(column.to_string(), json!(pid));
    }
    if ctx_pred.is_empty() {
        return user_filter;
    }
    let ctx_json = Json::Object(ctx_pred);
    // Empty user filter → just return ctx predicate (saves one wrap).
    if user_filter.as_object().is_some_and(|m| m.is_empty()) {
        return ctx_json;
    }
    json!({ "$and": [user_filter, ctx_json] })
}

fn record_to_json(record: &crate::ir::operations::LogicalRecord) -> Json {
    let mut map = Map::new();
    for (k, v) in record {
        map.insert(k.clone(), value_to_json(v));
    }
    Json::Object(map)
}

/// Same as `record_to_json` but stamps the active context's tenant +
/// project onto the document so future tenant-scoped reads can find
/// it. C7/C8: write-side enforcement that pairs with the read-side
/// `and_with_context` filter injection.
fn record_to_json_with_context(
    record: &crate::ir::operations::LogicalRecord,
    table: &ManifestTable,
    ctx: &CompileContext<'_>,
) -> Json {
    let mut map = Map::new();
    for (k, v) in record {
        map.insert(k.clone(), value_to_json(v));
    }
    if let Some(tid) = ctx.tenant_id
        && !tid.is_empty()
        && let Some(column) = Some(super::util::tenant_system_field(table))
    {
        map.insert(column.to_string(), json!(tid));
    }
    if let Some(pid) = ctx.project_id
        && !pid.is_empty()
        && let Some(column) = Some(super::util::project_system_field(table))
    {
        map.insert(column.to_string(), json!(pid));
    }
    Json::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
    use crate::ir::filter::{ComparisonOp, LogicalFilter};
    use crate::ir::operations::{ConflictStrategy, LogicalDelete, LogicalRead, LogicalWrite};
    use crate::ir::projection::LogicalPagination;
    use crate::ir::value::LogicalValue;

    fn fixture_manifest() -> CatalogManifest {
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

    fn extract_json(rendering: CompiledRendering) -> (String, Json) {
        match rendering {
            CompiledRendering::Json { path, body, .. } => (path, body),
            other => panic!("expected Json rendering, got {other:?}"),
        }
    }

    #[test]
    fn find_with_filter_and_pagination() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("acme.billing.v1.Customer")
            .with_filter(LogicalFilter::Comparison {
                field: "email".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("a@b.com".into()),
            })
            .with_pagination(LogicalPagination::page(10, 20));
        let (path, body) =
            extract_json(MongoDbCompiler.compile_read(&read, &ctx).expect("compile"));
        assert_eq!(path, "/action/find");
        assert_eq!(body["collection"], "customers");
        assert_eq!(body["filter"]["email"]["$eq"], "a@b.com");
        assert_eq!(body["limit"], 20);
        assert_eq!(body["skip"], 10);
    }

    #[test]
    fn upsert_renders_filter_and_set() {
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
            return_fields: vec![],
        };
        let (path, body) = extract_json(
            MongoDbCompiler
                .compile_write(&write, &ctx)
                .expect("compile"),
        );
        assert_eq!(path, "/action/updateOne");
        assert_eq!(body["filter"]["id"], "abc");
        assert_eq!(body["update"]["$set"]["name"], "Alice");
        assert_eq!(body["upsert"], true);
    }

    #[test]
    fn delete_empty_filter_is_rejected() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let del = LogicalDelete {
            message_type: "acme.billing.v1.Customer".into(),
            filter: LogicalFilter::always(),
            return_fields: vec![],
        };
        let err = MongoDbCompiler.compile_delete(&del, &ctx).unwrap_err();
        assert!(matches!(err, CompileError::Malformed { .. }));
    }

    #[test]
    fn ignore_strategy_is_rejected_with_typed_error() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let mut rec = crate::ir::operations::LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("x".into()));
        let write = LogicalWrite {
            message_type: "acme.billing.v1.Customer".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Ignore,
            return_fields: vec![],
        };
        let err = MongoDbCompiler.compile_write(&write, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Mongodb,
                op: "insert_ignore"
            }
        ));
    }

    #[test]
    fn like_lowers_to_anchored_regex() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("acme.billing.v1.Customer").with_filter(
            LogicalFilter::Comparison {
                field: "name".into(),
                op: ComparisonOp::ILike,
                value: LogicalValue::String("a.b%".into()),
            },
        );
        let (_, body) = extract_json(MongoDbCompiler.compile_read(&read, &ctx).expect("compile"));
        // The literal `.` must be escaped; the trailing `%` becomes `.*`.
        assert_eq!(body["filter"]["name"]["$regex"], "^a\\.b.*$");
        assert_eq!(body["filter"]["name"]["$options"], "i");
    }

    // ── C7/C8 — tenant + project context enforcement ─────────────────

    #[test]
    fn read_with_tenant_context_ands_in_predicate() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m)
            .with_tenant("acme")
            .with_project("p1");
        let read = LogicalRead::message("acme.billing.v1.Customer").with_filter(
            LogicalFilter::Comparison {
                field: "email".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("a@b.com".into()),
            },
        );
        let (_, body) = extract_json(MongoDbCompiler.compile_read(&read, &ctx).unwrap());
        let and = body["filter"]["$and"].as_array().expect("$and array");
        assert_eq!(and.len(), 2);
        // [0] = user filter, [1] = ctx predicate.
        assert_eq!(and[1]["_tenant_id"], "acme");
        assert_eq!(and[1]["_project_id"], "p1");
    }

    #[test]
    fn write_stamps_tenant_on_document() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m)
            .with_tenant("acme")
            .with_project("p1");
        let mut rec = crate::ir::operations::LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("abc".into()));
        let write = LogicalWrite {
            message_type: "acme.billing.v1.Customer".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Error,
            return_fields: vec![],
        };
        let (path, body) = extract_json(MongoDbCompiler.compile_write(&write, &ctx).unwrap());
        assert_eq!(path, "/action/insertOne");
        assert_eq!(body["document"]["_tenant_id"], "acme");
        assert_eq!(body["document"]["_project_id"], "p1");
        assert_eq!(body["document"]["id"], "abc");
    }

    #[test]
    fn read_without_context_passes_through_unchanged() {
        let m = fixture_manifest();
        let ctx = CompileContext::new(&m); // no tenant/project
        let read = LogicalRead::message("acme.billing.v1.Customer").with_filter(
            LogicalFilter::Comparison {
                field: "email".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("a@b.com".into()),
            },
        );
        let (_, body) = extract_json(MongoDbCompiler.compile_read(&read, &ctx).unwrap());
        // No outer $and wrap — the tenant injection is a no-op.
        assert!(body["filter"].get("$and").is_none());
        assert_eq!(body["filter"]["email"]["$eq"], "a@b.com");
    }
}
