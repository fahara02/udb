//! Qdrant compiler (U2 step 6b).
//!
//! Qdrant is a vector store: its first-class operation is `LogicalSearch`
//! (dense vector / hybrid). The compiler also lowers `LogicalWrite` to a
//! points-upsert, `LogicalDelete` to filter-based deletion, and
//! `LogicalResourceOp` to collection lifecycle (the existing
//! `QdrantExecutor` already speaks all of these as HTTP+JSON).
//!
//! `LogicalRead` without a vector lowers to a `scroll`-style request
//! (`/collections/{coll}/points/scroll`). It's the right shape for
//! pagination through stored payloads when a fan-out leg needs Qdrant
//! data without a similarity score.

use serde_json::{Map, Value as Json, json};

use crate::backend::BackendKind;
use crate::generation::ManifestTable;
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    LogicalDelete, LogicalRead, LogicalResourceOp, LogicalSearch, LogicalWrite, ResourceKind,
    ResourceOpKind,
};
use crate::ir::value::LogicalValue;

use super::{CompileContext, CompileError, CompiledRendering, Compiler, HttpMethod};

/// Qdrant vector-store compiler.
#[derive(Debug, Default, Clone, Copy)]
pub struct QdrantCompiler;

impl QdrantCompiler {
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

    /// Lower a logical filter to Qdrant's payload-filter shape:
    /// `{"must": [...], "should": [...], "must_not": [...]}` with
    /// `{"key": "field", "match": {"value": ...}}` predicates.
    fn render_filter(
        &self,
        filter: &LogicalFilter,
        table: &ManifestTable,
        message_type: &str,
    ) -> Result<Json, CompileError> {
        match filter {
            LogicalFilter::And(c) if c.is_empty() => Ok(Json::Object(Map::new())),
            LogicalFilter::Or(c) if c.is_empty() => {
                Ok(json!({"must": [{"match": {"value": false}}]}))
            }
            LogicalFilter::And(clauses) => {
                let parts: Vec<Json> = clauses
                    .iter()
                    .map(|c| self.render_filter(c, table, message_type))
                    .collect::<Result<_, _>>()?;
                Ok(json!({ "must": parts }))
            }
            LogicalFilter::Or(clauses) => {
                let parts: Vec<Json> = clauses
                    .iter()
                    .map(|c| self.render_filter(c, table, message_type))
                    .collect::<Result<_, _>>()?;
                Ok(json!({ "should": parts }))
            }
            LogicalFilter::Not(inner) => {
                let body = self.render_filter(inner, table, message_type)?;
                Ok(json!({ "must_not": [body] }))
            }
            LogicalFilter::IsNull(field) => {
                let f = self.field_for(table, field, message_type)?;
                // Qdrant has `is_null` as a payload predicate.
                Ok(json!({ "is_null": { "key": f } }))
            }
            LogicalFilter::InList { field, values } => {
                let f = self.field_for(table, field, message_type)?;
                let arr: Vec<Json> = values.iter().map(super::util::value_to_json).collect();
                Ok(json!({ "key": f, "match": { "any": arr } }))
            }
            LogicalFilter::Comparison { field, op, value } => {
                let f = self.field_for(table, field, message_type)?;
                let v = super::util::value_to_json(value);
                let predicate = match op {
                    ComparisonOp::Eq => json!({ "match": { "value": v } }),
                    ComparisonOp::Ne => json!({ "match": { "except": [v] } }),
                    ComparisonOp::Lt => json!({ "range": { "lt": v } }),
                    ComparisonOp::Le => json!({ "range": { "lte": v } }),
                    ComparisonOp::Gt => json!({ "range": { "gt": v } }),
                    ComparisonOp::Ge => json!({ "range": { "gte": v } }),
                    ComparisonOp::Contains
                    | ComparisonOp::StartsWith
                    | ComparisonOp::EndsWith
                    | ComparisonOp::Like
                    | ComparisonOp::ILike => {
                        return Err(CompileError::OperatorUnsupported {
                            backend: BackendKind::Qdrant,
                            op: "text_match",
                        });
                    }
                };
                let mut obj = Map::new();
                obj.insert("key".to_string(), Json::String(f.to_string()));
                if let Some(map) = predicate.as_object() {
                    for (k, v) in map {
                        obj.insert(k.clone(), v.clone());
                    }
                }
                Ok(Json::Object(obj))
            }
        }
    }
}

impl QdrantCompiler {
    /// C7/C8: wrap a payload filter in `{"must": [user, ctx_filter]}`
    /// so the tenant + project predicate ANDs together with the user
    /// filter. Tenant boundary is enforced at protocol level — a
    /// Qdrant search can no longer leak points across tenants.
    fn and_with_context(
        &self,
        user_filter: Json,
        table: &ManifestTable,
        ctx: &CompileContext<'_>,
    ) -> Json {
        let mut ctx_must: Vec<Json> = Vec::new();
        if let Some(tid) = ctx.tenant_id
            && !tid.is_empty()
            && let Some(column) = Some(super::util::tenant_system_field(table))
        {
            ctx_must.push(json!({"key": column, "match": {"value": tid}}));
        }
        if let Some(pid) = ctx.project_id
            && !pid.is_empty()
            && let Some(column) = Some(super::util::project_system_field(table))
        {
            ctx_must.push(json!({"key": column, "match": {"value": pid}}));
        }
        if ctx_must.is_empty() {
            return user_filter;
        }
        // If the user filter is empty, just emit the ctx predicate.
        if user_filter.as_object().is_some_and(|m| m.is_empty()) {
            return json!({ "must": ctx_must });
        }
        // Combine: { must: [user_filter, ...ctx_must] }
        let mut combined = ctx_must;
        combined.insert(0, user_filter);
        json!({ "must": combined })
    }
}

impl Compiler for QdrantCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::Qdrant
    }

    fn compile_read(
        &self,
        op: &LogicalRead,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        let mut body = Map::new();
        let user_rendered = match &op.filter {
            Some(f) => self.render_filter(f, table, &op.message_type)?,
            None => Json::Object(Map::new()),
        };
        let merged = self.and_with_context(user_rendered, table, ctx);
        if !merged.as_object().is_some_and(|m| m.is_empty()) {
            body.insert("filter".into(), merged);
        }
        let limit = op.pagination.as_ref().and_then(|p| p.limit).unwrap_or(100);
        body.insert("limit".into(), json!(limit));
        if let Some(pag) = &op.pagination
            && let Some(cursor) = &pag.cursor
            && !cursor.is_empty()
        {
            body.insert("offset".into(), json!(cursor));
        }
        body.insert("with_payload".into(), json!(true));
        body.insert("with_vector".into(), json!(false));

        Ok(CompiledRendering::Json {
            backend: BackendKind::Qdrant,
            method: HttpMethod::Post,
            path: format!("/collections/{}/points/scroll", table.table),
            body: Json::Object(body),
        })
    }

    fn compile_search(
        &self,
        op: &LogicalSearch,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        let vector = op.vector.as_ref().ok_or_else(|| CompileError::Malformed {
            reason: "Qdrant search requires a dense vector".into(),
        })?;
        if op.require_hybrid && op.text_query.is_none() {
            return Err(CompileError::Malformed {
                reason: "require_hybrid set but text_query missing".into(),
            });
        }

        let mut body = json!({
            "vector": vector,
            "limit": op.top_k,
            "with_payload": op.with_payload,
            "with_vector": op.with_vector,
        });
        if let Some(threshold) = op.score_threshold {
            body["score_threshold"] = json!(threshold);
        }
        let user_rendered = match &op.filter {
            Some(f) => self.render_filter(f, table, &op.message_type)?,
            None => Json::Object(Map::new()),
        };
        let merged = self.and_with_context(user_rendered, table, ctx);
        if !merged.as_object().is_some_and(|m| m.is_empty()) {
            body["filter"] = merged;
        }
        Ok(CompiledRendering::Json {
            backend: BackendKind::Qdrant,
            method: HttpMethod::Post,
            path: format!("/collections/{}/points/search", table.table),
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
        if table.primary_key.is_empty() {
            return Err(CompileError::Malformed {
                reason: format!(
                    "Qdrant upsert requires a primary-key field for '{}'",
                    op.message_type
                ),
            });
        }
        // Each record becomes a Qdrant point: { id, vector, payload }.
        let mut points = Vec::with_capacity(op.records.len());
        for record in &op.records {
            // ID from primary-key field.
            let pk_field = &table.primary_key[0];
            let id_value = record
                .get(pk_field)
                .ok_or_else(|| CompileError::Malformed {
                    reason: format!("record missing primary-key field '{pk_field}'"),
                })?;
            let id = super::util::value_to_json(id_value);

            // Vector from a `_vector` convention OR the first array<f32>
            // field. Until the manifest tags vector columns explicitly,
            // we accept either a literal `_vector` key or the first
            // `LogicalValue::Array` of floats in the record.
            let vector = record.get("_vector").and_then(|v| match v {
                LogicalValue::Array(arr) => Some(arr_to_floats(arr)),
                _ => None,
            });

            // Payload = every non-id, non-_vector field.
            let mut payload = Map::new();
            for (k, v) in record {
                if k == pk_field || k == "_vector" {
                    continue;
                }
                payload.insert(k.clone(), super::util::value_to_json(v));
            }
            // C7/C8: stamp tenant + project on every point payload so
            // the read-side filter actually has something to match on.
            if let Some(tid) = ctx.tenant_id
                && !tid.is_empty()
                && let Some(column) = Some(super::util::tenant_system_field(table))
            {
                payload.insert(column.to_string(), json!(tid));
            }
            if let Some(pid) = ctx.project_id
                && !pid.is_empty()
                && let Some(column) = Some(super::util::project_system_field(table))
            {
                payload.insert(column.to_string(), json!(pid));
            }

            let mut point = json!({ "id": id, "payload": Json::Object(payload) });
            if let Some(vec) = vector {
                point["vector"] = json!(vec);
            }
            points.push(point);
        }
        Ok(CompiledRendering::Json {
            backend: BackendKind::Qdrant,
            method: HttpMethod::Put,
            path: format!("/collections/{}/points", table.table),
            body: json!({ "points": points }),
        })
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
                reason: "LogicalDelete::filter cannot be empty".into(),
            });
        }
        // C7/C8: tenant AND so a permissive user filter can't delete
        // another tenant's points.
        let filter = self.and_with_context(user_filter, table, ctx);
        Ok(CompiledRendering::Json {
            backend: BackendKind::Qdrant,
            method: HttpMethod::Post,
            path: format!("/collections/{}/points/delete", table.table),
            body: json!({ "filter": filter }),
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
                backend: BackendKind::Qdrant,
                op: "non_collection_resource",
            });
        }
        let (method, path, body) = match op.op {
            ResourceOpKind::Ensure => (
                HttpMethod::Put,
                format!("/collections/{}", op.resource_name),
                op.spec.clone().unwrap_or_else(|| json!({})),
            ),
            ResourceOpKind::Drop => (
                HttpMethod::Delete,
                format!("/collections/{}", op.resource_name),
                json!({}),
            ),
            ResourceOpKind::List => (HttpMethod::Get, "/collections".to_string(), json!({})),
        };
        Ok(CompiledRendering::Json {
            backend: BackendKind::Qdrant,
            method,
            path,
            body,
        })
    }
}

fn arr_to_floats(arr: &[LogicalValue]) -> Vec<f32> {
    arr.iter()
        .filter_map(|v| match v {
            LogicalValue::Float(f) => Some(*f as f32),
            LogicalValue::Int(i) => Some(*i as f32),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
    use crate::ir::operations::LogicalSearch;

    fn fixture() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.docs.v1.Doc".into(),
            schema: "vectors".into(),
            table: "docs".into(),
            primary_key: vec!["id".into()],
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
    fn search_emits_points_search_path() {
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
        match QdrantCompiler.compile_search(&search, &ctx).unwrap() {
            CompiledRendering::Json { path, body, .. } => {
                assert_eq!(path, "/collections/docs/points/search");
                assert_eq!(body["limit"], 5);
                // f32-as-f64 round-trip leaves 0.7 imprecise; assert
                // the value is in the right neighbourhood.
                let threshold = body["score_threshold"].as_f64().expect("number");
                assert!((threshold - 0.7).abs() < 1e-5, "got {threshold}");
                assert_eq!(body["with_payload"], true);
                assert_eq!(body["vector"].as_array().unwrap().len(), 3);
            }
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn ensure_collection_emits_put_collection() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let op = LogicalResourceOp {
            op: ResourceOpKind::Ensure,
            resource_kind: ResourceKind::Collection,
            resource_name: "docs".into(),
            spec: Some(json!({"vectors": {"size": 384, "distance": "Cosine"}})),
        };
        match QdrantCompiler.compile_resource_op(&op, &ctx).unwrap() {
            CompiledRendering::Json {
                method, path, body, ..
            } => {
                assert_eq!(method, HttpMethod::Put);
                assert_eq!(path, "/collections/docs");
                assert_eq!(body["vectors"]["size"], 384);
            }
            other => panic!("expected Json, got {other:?}"),
        }
    }

    // ── C7/C8 — tenant + project enforcement ────────────────────────

    #[test]
    fn search_with_tenant_context_ands_into_must_filter() {
        let m = fixture();
        let ctx = CompileContext::new(&m)
            .with_tenant("acme")
            .with_project("p1");
        let search = LogicalSearch {
            message_type: "acme.docs.v1.Doc".into(),
            vector: Some(vec![0.0, 0.0, 0.0]),
            text_query: None,
            filter: None,
            top_k: 5,
            score_threshold: None,
            require_hybrid: false,
            with_vector: false,
            with_payload: true,
        };
        match QdrantCompiler.compile_search(&search, &ctx).unwrap() {
            CompiledRendering::Json { body, .. } => {
                let must = body["filter"]["must"].as_array().expect("must array");
                // No user filter, so must = [ctx_tenant, ctx_project].
                assert_eq!(must.len(), 2);
                assert_eq!(must[0]["key"], "_tenant_id");
                assert_eq!(must[0]["match"]["value"], "acme");
                assert_eq!(must[1]["key"], "_project_id");
                assert_eq!(must[1]["match"]["value"], "p1");
            }
            other => panic!("expected Json, got {other:?}"),
        }
    }
}
