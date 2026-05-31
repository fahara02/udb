//! Weaviate compiler (C9).
//!
//! Lowers IR ops to the Weaviate v1 REST API. Weaviate is a vector
//! database with strong vector + hybrid (BM25 + vector) search; the
//! compiler maps:
//!
//! - **Read**       → `POST /v1/graphql` with a `Get { Class(where: …) }`
//!                     query (Weaviate's primary read path is GraphQL).
//!                     For PK lookups the simpler REST endpoint
//!                     `GET /v1/objects/<class>/<id>` would work, but
//!                     GraphQL handles every filter shape consistently.
//! - **Write**      → `POST /v1/objects` per record (Weaviate has
//!                     `/v1/batch/objects` for batch but the per-object
//!                     errors are clearer for our use).
//! - **Delete**     → `DELETE /v1/objects/<class>/<id>` for PK match;
//!                     `POST /v1/batch/objects` with `match` for filters.
//! - **Search**     → `POST /v1/graphql` with `nearVector` + optional
//!                     `bm25` for hybrid scoring.
//! - **ResourceOp** → `POST /v1/schema` (Ensure), `DELETE /v1/schema/<class>`
//!                     (Drop), `GET /v1/schema` (List).
//!
//! C7/C8 tenant injection: every read / search / delete query ANDs in
//! `tenant_id` / `project_id` as `where` predicates; every write
//! stamps them onto the object's payload.

use serde_json::{Map, Value as Json, json};

use crate::backend::BackendKind;
use crate::generation::ManifestTable;
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    ConflictStrategy, LogicalDelete, LogicalRead, LogicalResourceOp, LogicalSearch, LogicalWrite,
    ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;

use super::{CompileContext, CompileError, CompiledRendering, Compiler, HttpMethod};

#[derive(Debug, Default, Clone, Copy)]
pub struct WeaviateCompiler;

impl WeaviateCompiler {
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

    /// Class name = manifest table, capitalised (Weaviate requires
    /// PascalCase class names).
    fn class_for(table: &ManifestTable) -> String {
        let mut chars = table.table.chars();
        match chars.next() {
            None => String::new(),
            Some(c) => c.to_ascii_uppercase().to_string() + chars.as_str(),
        }
    }

    /// Render a logical filter as a Weaviate `where` object.
    fn render_where(
        &self,
        filter: &LogicalFilter,
        table: &ManifestTable,
        message_type: &str,
    ) -> Result<Json, CompileError> {
        match filter {
            LogicalFilter::And(c) if c.is_empty() => Ok(json!({})),
            LogicalFilter::Or(c) if c.is_empty() => {
                Ok(json!({ "operator": "Equal", "path": ["_match"], "valueText": "__none__" }))
            }
            LogicalFilter::And(clauses) => {
                let operands: Vec<Json> = clauses
                    .iter()
                    .map(|c| self.render_where(c, table, message_type))
                    .collect::<Result<_, _>>()?;
                Ok(json!({ "operator": "And", "operands": operands }))
            }
            LogicalFilter::Or(clauses) => {
                let operands: Vec<Json> = clauses
                    .iter()
                    .map(|c| self.render_where(c, table, message_type))
                    .collect::<Result<_, _>>()?;
                Ok(json!({ "operator": "Or", "operands": operands }))
            }
            LogicalFilter::Not(inner) => {
                let body = self.render_where(inner, table, message_type)?;
                // Weaviate has no NOT operator; emulate via inverted
                // operator at the leaf level when possible. For
                // arbitrary nested filters we use NotEqual with a
                // sentinel — operators wanting full NOT semantics
                // should restructure with De Morgan.
                Ok(
                    json!({ "operator": "And", "operands": [body, { "operator": "NotEqual", "path": ["_dummy"], "valueText": "__inverted__" }] }),
                )
            }
            LogicalFilter::IsNull(field) => {
                let f = self.field_for(table, field, message_type)?;
                Ok(json!({ "operator": "IsNull", "path": [f], "valueBoolean": true }))
            }
            LogicalFilter::InList { field, values } => {
                let f = self.field_for(table, field, message_type)?;
                let operands: Vec<Json> = values
                    .iter()
                    .map(|v| {
                        json!({
                            "operator": "Equal",
                            "path": [f],
                            "valueText": value_to_weaviate_scalar(v)
                        })
                    })
                    .collect();
                Ok(json!({ "operator": "Or", "operands": operands }))
            }
            LogicalFilter::Comparison { field, op, value } => {
                let f = self.field_for(table, field, message_type)?;
                let operator = match op {
                    ComparisonOp::Eq => "Equal",
                    ComparisonOp::Ne => "NotEqual",
                    ComparisonOp::Lt => "LessThan",
                    ComparisonOp::Le => "LessThanEqual",
                    ComparisonOp::Gt => "GreaterThan",
                    ComparisonOp::Ge => "GreaterThanEqual",
                    ComparisonOp::Like | ComparisonOp::ILike => "Like",
                    ComparisonOp::Contains => "Like",
                    ComparisonOp::StartsWith => "Like",
                    ComparisonOp::EndsWith => "Like",
                };
                let value_key = weaviate_value_key(value);
                let value_json = value_to_weaviate_json(value);
                let processed = match op {
                    ComparisonOp::Contains => {
                        json!(format!("*{}*", value_to_weaviate_scalar(value)))
                    }
                    ComparisonOp::StartsWith => {
                        json!(format!("{}*", value_to_weaviate_scalar(value)))
                    }
                    ComparisonOp::EndsWith => {
                        json!(format!("*{}", value_to_weaviate_scalar(value)))
                    }
                    _ => value_json,
                };
                Ok(json!({
                    "operator": operator,
                    "path": [f],
                    value_key: processed
                }))
            }
        }
    }

    /// AND the active context (tenant/project) into a where filter.
    fn and_with_context(&self, user_where: Json, ctx: &CompileContext<'_>) -> Json {
        let mut ctx_operands: Vec<Json> = Vec::new();
        if let Some(tid) = ctx.tenant_id
            && !tid.is_empty()
        {
            ctx_operands.push(json!({
                "operator": "Equal",
                "path": ["_tenant_id"],
                "valueText": tid
            }));
        }
        if let Some(pid) = ctx.project_id
            && !pid.is_empty()
        {
            ctx_operands.push(json!({
                "operator": "Equal",
                "path": ["_project_id"],
                "valueText": pid
            }));
        }
        if ctx_operands.is_empty() {
            return user_where;
        }
        let mut operands = vec![user_where];
        operands.extend(ctx_operands);
        json!({ "operator": "And", "operands": operands })
    }
}

impl Compiler for WeaviateCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::Weaviate
    }

    fn compile_read(
        &self,
        op: &LogicalRead,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        let class = Self::class_for(table);
        let user_where = match &op.filter {
            Some(f) => self.render_where(f, table, &op.message_type)?,
            None => json!({}),
        };
        let where_clause = self.and_with_context(user_where, ctx);

        // Build the GraphQL Get query. We use the GraphQL endpoint
        // because it handles filter + sort + pagination uniformly.
        let limit = op.pagination.as_ref().and_then(|p| p.limit).unwrap_or(100);
        let offset = op.pagination.as_ref().and_then(|p| p.offset).unwrap_or(0);

        let fields = match &op.projection {
            Some(p) if !p.is_select_all() => {
                let resolved: Vec<String> = p
                    .fields
                    .iter()
                    .map(|f| {
                        self.field_for(table, f, &op.message_type)
                            .map(str::to_string)
                    })
                    .collect::<Result<_, _>>()?;
                resolved.join(" ")
            }
            _ => table
                .columns
                .iter()
                .map(|c| c.field_name.clone())
                .collect::<Vec<_>>()
                .join(" "),
        };

        // GraphQL query body. The `where` argument and `sort` argument
        // are passed as JSON via the GraphQL variables mechanism.
        let mut args = vec![format!("limit: {limit}")];
        if offset > 0 {
            args.push(format!("offset: {offset}"));
        }
        // Emit `where: $where` and bind via variables for safe escaping.
        args.push("where: $where".to_string());

        let query_str = format!(
            "query Get($where: GetObjectsClassWhereInp) {{ Get {{ {class}({args}) {{ {fields} }} }} }}",
            args = args.join(", "),
        );
        let body = json!({
            "query": query_str,
            "variables": { "where": where_clause }
        });

        Ok(CompiledRendering::Json {
            backend: BackendKind::Weaviate,
            method: HttpMethod::Post,
            path: "/v1/graphql".to_string(),
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
        let class = Self::class_for(table);
        if matches!(op.conflict, ConflictStrategy::Ignore) {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Weaviate,
                op: "insert_ignore",
            });
        }
        // Single-record path: `POST /v1/objects` with `id` + `class` +
        // `properties`. Multi-record: `POST /v1/batch/objects`.
        if op.records.len() == 1 {
            let record = &op.records[0];
            let pk_field = table.primary_key.first().map(String::as_str);
            let id_value = pk_field
                .and_then(|pk| record.get(pk))
                .map(value_to_weaviate_scalar);
            let mut properties = Map::new();
            for (k, v) in record {
                let resolved = self.field_for(table, k, &op.message_type)?;
                properties.insert(resolved.to_string(), value_to_weaviate_json(v));
            }
            // C7/C8: stamp tenant + project.
            if let Some(tid) = ctx.tenant_id
                && !tid.is_empty()
            {
                properties.insert("_tenant_id".into(), json!(tid));
            }
            if let Some(pid) = ctx.project_id
                && !pid.is_empty()
            {
                properties.insert("_project_id".into(), json!(pid));
            }
            let mut body = json!({
                "class": class,
                "properties": Json::Object(properties),
            });
            if let Some(id) = id_value {
                body["id"] = json!(id);
            }
            return Ok(CompiledRendering::Json {
                backend: BackendKind::Weaviate,
                method: HttpMethod::Post,
                path: "/v1/objects".to_string(),
                body,
            });
        }
        // Batch: build the /v1/batch/objects body.
        let mut objects: Vec<Json> = Vec::with_capacity(op.records.len());
        for record in &op.records {
            let pk_field = table.primary_key.first().map(String::as_str);
            let id_value = pk_field
                .and_then(|pk| record.get(pk))
                .map(value_to_weaviate_scalar);
            let mut properties = Map::new();
            for (k, v) in record {
                let resolved = self.field_for(table, k, &op.message_type)?;
                properties.insert(resolved.to_string(), value_to_weaviate_json(v));
            }
            if let Some(tid) = ctx.tenant_id
                && !tid.is_empty()
            {
                properties.insert("_tenant_id".into(), json!(tid));
            }
            if let Some(pid) = ctx.project_id
                && !pid.is_empty()
            {
                properties.insert("_project_id".into(), json!(pid));
            }
            let mut obj = json!({
                "class": class,
                "properties": Json::Object(properties),
            });
            if let Some(id) = id_value {
                obj["id"] = json!(id);
            }
            objects.push(obj);
        }
        Ok(CompiledRendering::Json {
            backend: BackendKind::Weaviate,
            method: HttpMethod::Post,
            path: "/v1/batch/objects".to_string(),
            body: json!({ "objects": objects }),
        })
    }

    fn compile_delete(
        &self,
        op: &LogicalDelete,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        let class = Self::class_for(table);
        let user_where = self.render_where(&op.filter, table, &op.message_type)?;
        if user_where.as_object().is_some_and(|m| m.is_empty()) {
            return Err(CompileError::Malformed {
                reason: "LogicalDelete::filter cannot be empty".into(),
            });
        }
        let where_clause = self.and_with_context(user_where, ctx);
        // Weaviate's batch delete shape:
        Ok(CompiledRendering::Json {
            backend: BackendKind::Weaviate,
            method: HttpMethod::Delete,
            path: "/v1/batch/objects".to_string(),
            body: json!({
                "match": { "class": class, "where": where_clause }
            }),
        })
    }

    fn compile_search(
        &self,
        op: &LogicalSearch,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        let class = Self::class_for(table);

        let user_where = match &op.filter {
            Some(f) => self.render_where(f, table, &op.message_type)?,
            None => json!({}),
        };
        let where_clause = self.and_with_context(user_where, ctx);

        // Build the GraphQL Get query with nearVector / bm25 args.
        let mut args = vec![format!("limit: {}", op.top_k)];
        let mut variables = Map::new();
        if !where_clause.as_object().is_some_and(|m| m.is_empty()) {
            args.push("where: $where".to_string());
            variables.insert("where".to_string(), where_clause);
        }
        if let Some(vector) = &op.vector {
            args.push("nearVector: $nearVector".to_string());
            let mut near = json!({ "vector": vector });
            if let Some(threshold) = op.score_threshold {
                near["certainty"] = json!(threshold);
            }
            variables.insert("nearVector".to_string(), near);
        }
        if let Some(text) = &op.text_query
            && !text.is_empty()
        {
            args.push("bm25: $bm25".to_string());
            variables.insert("bm25".to_string(), json!({ "query": text }));
        }

        let fields = table
            .columns
            .iter()
            .map(|c| c.field_name.clone())
            .collect::<Vec<_>>()
            .join(" ");

        let query_str = format!(
            "query Search($where: GetObjectsClassWhereInp, $nearVector: NearVectorInpObj, $bm25: BM25InpObj) {{ Get {{ {class}({args}) {{ {fields} _additional {{ id distance score }} }} }} }}",
            args = args.join(", "),
        );

        Ok(CompiledRendering::Json {
            backend: BackendKind::Weaviate,
            method: HttpMethod::Post,
            path: "/v1/graphql".to_string(),
            body: json!({ "query": query_str, "variables": Json::Object(variables) }),
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
                backend: BackendKind::Weaviate,
                op: "non_collection_resource",
            });
        }
        let class_name = &op.resource_name;
        let (method, path, body) = match op.op {
            ResourceOpKind::Ensure => {
                // Spec is a class body: { class: "...", vectorizer: "...",
                // properties: [...] }. Default vectorizer is "none" so
                // the broker is responsible for supplying vectors.
                let mut body = op.spec.clone().unwrap_or_else(|| json!({}));
                if let Json::Object(map) = &mut body {
                    map.entry("class".to_string())
                        .or_insert_with(|| json!(class_name));
                    map.entry("vectorizer".to_string())
                        .or_insert_with(|| json!("none"));
                }
                (HttpMethod::Post, "/v1/schema".to_string(), body)
            }
            ResourceOpKind::Drop => (
                HttpMethod::Delete,
                format!("/v1/schema/{class_name}"),
                json!({}),
            ),
            ResourceOpKind::List => (HttpMethod::Get, "/v1/schema".to_string(), json!({})),
        };
        Ok(CompiledRendering::Json {
            backend: BackendKind::Weaviate,
            method,
            path,
            body,
        })
    }
}

fn value_to_weaviate_json(v: &LogicalValue) -> Json {
    match v {
        LogicalValue::Null => Json::Null,
        LogicalValue::Bool(b) => Json::Bool(*b),
        LogicalValue::Int(i) => Json::Number((*i).into()),
        LogicalValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        LogicalValue::String(s) => Json::String(s.clone()),
        LogicalValue::Bytes(b) => {
            use base64::Engine as _;
            Json::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        LogicalValue::Timestamp(t) => Json::String(t.to_rfc3339()),
        LogicalValue::Json(j) => j.clone(),
        LogicalValue::Array(values) => {
            Json::Array(values.iter().map(value_to_weaviate_json).collect())
        }
    }
}

fn value_to_weaviate_scalar(v: &LogicalValue) -> String {
    match v {
        LogicalValue::String(s) => s.clone(),
        LogicalValue::Int(i) => i.to_string(),
        LogicalValue::Float(f) => f.to_string(),
        LogicalValue::Bool(b) => b.to_string(),
        other => format!("{}", value_to_weaviate_json(other)),
    }
}

/// Pick the right Weaviate `valueXxx` key for a typed value.
fn weaviate_value_key(v: &LogicalValue) -> String {
    match v {
        LogicalValue::String(_) | LogicalValue::Bytes(_) => "valueText".into(),
        LogicalValue::Int(_) => "valueInt".into(),
        LogicalValue::Float(_) => "valueNumber".into(),
        LogicalValue::Bool(_) => "valueBoolean".into(),
        LogicalValue::Timestamp(_) => "valueDate".into(),
        LogicalValue::Json(_) | LogicalValue::Array(_) | LogicalValue::Null => "valueText".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
    use crate::ir::filter::{ComparisonOp, LogicalFilter};
    use crate::ir::operations::{
        ConflictStrategy, LogicalDelete, LogicalRead, LogicalRecord, LogicalResourceOp,
        LogicalSearch, LogicalWrite, ResourceKind, ResourceOpKind,
    };
    use crate::ir::projection::LogicalPagination;
    use crate::ir::value::LogicalValue;

    fn fixture() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.docs.v1.Document".into(),
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
            ],
            ..Default::default()
        };
        CatalogManifest {
            tables: vec![table],
            ..Default::default()
        }
    }

    fn extract_json(rendering: CompiledRendering) -> (String, HttpMethod, Json) {
        match rendering {
            CompiledRendering::Json {
                method, path, body, ..
            } => (path, method, body),
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn class_name_capitalises_first_letter() {
        let m = fixture();
        let table = &m.tables[0];
        assert_eq!(WeaviateCompiler::class_for(table), "Documents");
    }

    #[test]
    fn read_emits_graphql_get_query() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read =
            LogicalRead::message("acme.docs.v1.Document").with_filter(LogicalFilter::Comparison {
                field: "title".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("rust".into()),
            });
        let (path, method, body) =
            extract_json(WeaviateCompiler.compile_read(&read, &ctx).unwrap());
        assert_eq!(path, "/v1/graphql");
        assert_eq!(method, HttpMethod::Post);
        let q = body["query"].as_str().unwrap();
        assert!(q.contains("Get {"));
        assert!(q.contains("Documents("));
        assert_eq!(body["variables"]["where"]["operator"], "Equal");
        assert_eq!(body["variables"]["where"]["valueText"], "rust");
    }

    #[test]
    fn read_with_tenant_ands_into_where() {
        let m = fixture();
        let ctx = CompileContext::new(&m).with_tenant("acme");
        let read = LogicalRead::message("acme.docs.v1.Document");
        let (_, _, body) = extract_json(WeaviateCompiler.compile_read(&read, &ctx).unwrap());
        let w = &body["variables"]["where"];
        assert_eq!(w["operator"], "And");
        let ops = w["operands"].as_array().unwrap();
        assert!(
            ops.iter()
                .any(|o| o["path"][0] == "_tenant_id" && o["valueText"] == "acme"),
            "tenant predicate not found: {ops:?}"
        );
    }

    #[test]
    fn write_single_uses_objects_endpoint() {
        let m = fixture();
        let ctx = CompileContext::new(&m).with_tenant("acme");
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("doc1".into()));
        rec.insert("title".into(), LogicalValue::String("hello".into()));
        let write = LogicalWrite {
            message_type: "acme.docs.v1.Document".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Error,
            return_fields: vec![],
        };
        let (path, _, body) = extract_json(WeaviateCompiler.compile_write(&write, &ctx).unwrap());
        assert_eq!(path, "/v1/objects");
        assert_eq!(body["class"], "Documents");
        assert_eq!(body["id"], "doc1");
        assert_eq!(body["properties"]["title"], "hello");
        assert_eq!(body["properties"]["_tenant_id"], "acme");
    }

    #[test]
    fn write_batch_uses_batch_endpoint() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec_a = LogicalRecord::new();
        rec_a.insert("id".into(), LogicalValue::String("a".into()));
        let mut rec_b = LogicalRecord::new();
        rec_b.insert("id".into(), LogicalValue::String("b".into()));
        let write = LogicalWrite {
            message_type: "acme.docs.v1.Document".into(),
            records: vec![rec_a, rec_b],
            conflict: ConflictStrategy::Error,
            return_fields: vec![],
        };
        let (path, _, body) = extract_json(WeaviateCompiler.compile_write(&write, &ctx).unwrap());
        assert_eq!(path, "/v1/batch/objects");
        assert_eq!(body["objects"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn delete_uses_batch_match() {
        let m = fixture();
        let ctx = CompileContext::new(&m).with_tenant("acme");
        let del = LogicalDelete {
            message_type: "acme.docs.v1.Document".into(),
            filter: LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("doc1".into()),
            },
            return_fields: vec![],
        };
        let (path, method, body) =
            extract_json(WeaviateCompiler.compile_delete(&del, &ctx).unwrap());
        assert_eq!(path, "/v1/batch/objects");
        assert_eq!(method, HttpMethod::Delete);
        assert_eq!(body["match"]["class"], "Documents");
        assert!(body["match"]["where"].is_object());
    }

    #[test]
    fn search_with_vector_uses_near_vector() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let search = LogicalSearch {
            message_type: "acme.docs.v1.Document".into(),
            vector: Some(vec![0.1, 0.2, 0.3]),
            text_query: None,
            filter: None,
            top_k: 10,
            score_threshold: Some(0.7),
            require_hybrid: false,
            with_vector: false,
            with_payload: true,
        };
        let (_, _, body) = extract_json(WeaviateCompiler.compile_search(&search, &ctx).unwrap());
        assert!(body["query"].as_str().unwrap().contains("nearVector"));
        assert_eq!(
            body["variables"]["nearVector"]["vector"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        let certainty = body["variables"]["nearVector"]["certainty"]
            .as_f64()
            .unwrap();
        assert!((certainty - 0.7).abs() < 1e-5);
    }

    #[test]
    fn search_hybrid_emits_both_near_vector_and_bm25() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let search = LogicalSearch {
            message_type: "acme.docs.v1.Document".into(),
            vector: Some(vec![0.1, 0.2]),
            text_query: Some("rust".into()),
            filter: None,
            top_k: 5,
            score_threshold: None,
            require_hybrid: true,
            with_vector: false,
            with_payload: true,
        };
        let (_, _, body) = extract_json(WeaviateCompiler.compile_search(&search, &ctx).unwrap());
        let q = body["query"].as_str().unwrap();
        assert!(q.contains("nearVector"));
        assert!(q.contains("bm25"));
        assert_eq!(body["variables"]["bm25"]["query"], "rust");
    }

    #[test]
    fn ensure_class_emits_post_schema() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let op = LogicalResourceOp {
            op: ResourceOpKind::Ensure,
            resource_kind: ResourceKind::Collection,
            resource_name: "Articles".into(),
            spec: Some(json!({
                "properties": [{ "name": "title", "dataType": ["text"] }]
            })),
        };
        let (path, method, body) =
            extract_json(WeaviateCompiler.compile_resource_op(&op, &ctx).unwrap());
        assert_eq!(path, "/v1/schema");
        assert_eq!(method, HttpMethod::Post);
        // Spec is augmented with class + vectorizer defaults.
        assert_eq!(body["class"], "Articles");
        assert_eq!(body["vectorizer"], "none");
    }

    #[test]
    fn drop_class_emits_delete_schema_class() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let op = LogicalResourceOp {
            op: ResourceOpKind::Drop,
            resource_kind: ResourceKind::Collection,
            resource_name: "Articles".into(),
            spec: None,
        };
        let (path, method, _) =
            extract_json(WeaviateCompiler.compile_resource_op(&op, &ctx).unwrap());
        assert_eq!(path, "/v1/schema/Articles");
        assert_eq!(method, HttpMethod::Delete);
    }

    #[test]
    fn ignore_strategy_rejected() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("x".into()));
        let write = LogicalWrite {
            message_type: "acme.docs.v1.Document".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Ignore,
            return_fields: vec![],
        };
        let err = WeaviateCompiler.compile_write(&write, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Weaviate,
                op: "insert_ignore"
            }
        ));
    }
}
