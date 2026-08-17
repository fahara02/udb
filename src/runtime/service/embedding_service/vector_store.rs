use std::sync::Arc;

use serde_json::{Value as JsonValue, json};

use crate::proto::{
    VectorHybridSearchRequest, VectorPointMutation, VectorSearchRequest, VectorSet,
};
use crate::runtime::DataBrokerRuntime;

use super::model::StoredModel;

#[async_trait::async_trait]
pub(crate) trait VectorStore: Send + Sync {
    async fn ensure_collection(
        &self,
        collection: &str,
        dimensions: i32,
        distance: &str,
        output_dtype: &str,
        vector_names: &[String],
    ) -> Result<(), tonic::Status>;
    async fn upsert(
        &self,
        collection: &str,
        dimensions: i32,
        distance: &str,
        output_dtype: &str,
        points: Vec<VectorPointMutation>,
    ) -> Result<(), tonic::Status>;
    async fn search(&self, request: &VectorSearchRequest) -> Result<VectorSet, tonic::Status>;
    async fn hybrid_search(
        &self,
        request: &VectorHybridSearchRequest,
    ) -> Result<VectorSet, tonic::Status>;
    async fn delete_points(
        &self,
        collection: &str,
        point_ids: Vec<String>,
    ) -> Result<(), tonic::Status>;
    async fn delete_by_filter(
        &self,
        collection: &str,
        filter: serde_json::Value,
    ) -> Result<(), tonic::Status>;
    async fn swap_alias(&self, alias: &str, collection: &str) -> Result<(), tonic::Status>;
}

pub(crate) struct RuntimeVectorStore {
    runtime: Arc<DataBrokerRuntime>,
    backend: String,
    instance: String,
    project_id: String,
}

impl RuntimeVectorStore {
    pub(crate) fn for_routing(
        runtime: Arc<DataBrokerRuntime>,
        project_id: &str,
        backend: &str,
        instance: &str,
    ) -> Self {
        Self {
            runtime,
            backend: backend.trim().to_ascii_lowercase(),
            instance: instance.trim().to_string(),
            project_id: project_id.to_string(),
        }
    }

    pub(crate) fn for_model(
        runtime: Arc<DataBrokerRuntime>,
        project_id: &str,
        model: &StoredModel,
    ) -> Self {
        Self::for_routing(
            runtime,
            project_id,
            &model.vector_backend,
            &model.vector_instance,
        )
    }

    fn instance(&self) -> Option<&str> {
        (!self.instance.is_empty()).then_some(self.instance.as_str())
    }

    fn is_qdrant(&self) -> bool {
        self.backend == "qdrant"
    }

    /// Dispatch a portable `{method, path, body}` HTTP delete spec at the routed
    /// non-Qdrant backend through the shared generic mutation seam
    /// (`mutate_backend_target`) — the SAME seam the generic vector upsert uses.
    /// This is the portable teardown path that closes the GDPR-erasure hole for
    /// Elasticsearch/Pinecone/Weaviate models (Qdrant keeps its typed seam).
    async fn dispatch_delete(&self, spec: &JsonValue) -> Result<(), tonic::Status> {
        let request_json = serde_json::to_string(spec).map_err(|err| {
            crate::runtime::executor_utils::capability_status(
                "embedding",
                "embedding_vector_portable_delete",
                "vector_backend_portable_delete",
                format!("failed to encode portable vector-delete spec: {err}"),
            )
        })?;
        self.runtime
            .mutate_backend_target_for_project(
                &self.backend,
                self.instance(),
                &self.project_id,
                &request_json,
            )
            .await
            .map(|_| ())
    }
}

#[async_trait::async_trait]
impl VectorStore for RuntimeVectorStore {
    async fn ensure_collection(
        &self,
        collection: &str,
        dimensions: i32,
        distance: &str,
        output_dtype: &str,
        vector_names: &[String],
    ) -> Result<(), tonic::Status> {
        self.runtime
            .vector_ensure_backend_kind_target(
                &self.backend,
                self.instance(),
                &self.project_id,
                collection,
                dimensions,
                distance,
                output_dtype,
                vector_names,
            )
            .await
    }

    async fn upsert(
        &self,
        collection: &str,
        dimensions: i32,
        distance: &str,
        output_dtype: &str,
        points: Vec<VectorPointMutation>,
    ) -> Result<(), tonic::Status> {
        let mut vector_names = points
            .iter()
            .map(|point| point.vector_name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        vector_names.sort_unstable();
        vector_names.dedup();
        if !vector_names.is_empty() {
            self.ensure_collection(
                collection,
                dimensions,
                distance,
                output_dtype,
                &vector_names,
            )
            .await?;
        }
        self.runtime
            .vector_upsert_existing_backend_kind_target(
                &self.backend,
                self.instance(),
                &self.project_id,
                collection,
                points,
            )
            .await
    }

    async fn search(&self, request: &VectorSearchRequest) -> Result<VectorSet, tonic::Status> {
        self.runtime
            .vector_search_backend_kind_target(
                &self.backend,
                self.instance(),
                &self.project_id,
                request,
            )
            .await
    }

    async fn hybrid_search(
        &self,
        request: &VectorHybridSearchRequest,
    ) -> Result<VectorSet, tonic::Status> {
        self.runtime
            .vector_hybrid_backend_kind_target(
                &self.backend,
                self.instance(),
                &self.project_id,
                request,
            )
            .await
    }

    async fn delete_points(
        &self,
        collection: &str,
        point_ids: Vec<String>,
    ) -> Result<(), tonic::Status> {
        if point_ids.is_empty() {
            return Ok(());
        }
        if self.is_qdrant() {
            return self
                .runtime
                .vector_delete_backend_target(
                    self.instance(),
                    &self.project_id,
                    collection,
                    point_ids,
                )
                .await;
        }
        let spec = portable_delete_by_ids_spec(&self.backend, collection, &point_ids)?;
        self.dispatch_delete(&spec).await
    }

    async fn delete_by_filter(
        &self,
        collection: &str,
        filter: serde_json::Value,
    ) -> Result<(), tonic::Status> {
        if self.is_qdrant() {
            return self
                .runtime
                .vector_delete_by_filter_backend_target(
                    self.instance(),
                    &self.project_id,
                    collection,
                    filter,
                )
                .await;
        }
        let spec = portable_delete_by_filter_spec(&self.backend, collection, &filter)?;
        self.dispatch_delete(&spec).await
    }

    async fn swap_alias(&self, alias: &str, collection: &str) -> Result<(), tonic::Status> {
        if self.is_qdrant() {
            return self
                .runtime
                .vector_swap_alias_backend_target(
                    self.instance(),
                    &self.project_id,
                    alias,
                    collection,
                )
                .await;
        }
        let spec = portable_alias_swap_spec(&self.backend, alias, collection)?;
        self.dispatch_delete(&spec).await
    }
}

/// Qdrant `MatchAny`/`match: {value}` `must` clauses carry a `{key, match}` shape;
/// extract the flat `(key, value)` equality terms from a delete filter's `must`
/// group so a portable backend can AND them into its native delete. Only string
/// equality (`match.value`) is translated — the teardown filters are all
/// `_tenant_id`/`_source`/`_parent_pk` string scopes. Pure.
fn extract_scope_terms(filter: &JsonValue) -> Vec<(String, String)> {
    let mut terms = Vec::new();
    let Some(must) = filter.get("must").and_then(JsonValue::as_array) else {
        return terms;
    };
    for clause in must {
        let Some(key) = clause.get("key").and_then(JsonValue::as_str) else {
            continue;
        };
        if let Some(value) = clause
            .get("match")
            .and_then(|matcher| matcher.get("value"))
            .and_then(JsonValue::as_str)
        {
            terms.push((key.to_string(), value.to_string()));
        }
    }
    terms
}

/// Weaviate class name for a logical collection — MUST mirror
/// `core/setup_data.rs::vector_weaviate_class_name` (the upsert/ensure path) so a
/// delete addresses the same class the writes created. Kept in lockstep by the
/// `weaviate_class_name_matches_setup_data` unit test. Pure.
fn weaviate_class_name(resource_name: &str) -> String {
    let mut out = String::new();
    for ch in resource_name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
        } else if ch == '_' || ch == '-' {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push_str("UdbVector");
    }
    if !out.chars().next().is_some_and(|ch| ch.is_ascii_uppercase()) {
        out.insert_str(0, "Udb");
    }
    out
}

fn portable_delete_unsupported(backend: &str, operation: &'static str) -> tonic::Status {
    crate::runtime::executor_utils::capability_status(
        "embedding",
        operation,
        "vector_backend_portable_delete",
        format!("portable vector delete is not wired for backend '{backend}'"),
    )
}

/// Shape a portable delete-by-id spec for a non-Qdrant backend as a generic
/// `{method, path, body}` HTTP dispatch (consumed by the backend's
/// `MutationExecutor`). Pure — unit-tested. `point_ids` is guaranteed non-empty
/// by the caller.
fn portable_delete_by_ids_spec(
    backend: &str,
    collection: &str,
    point_ids: &[String],
) -> Result<JsonValue, tonic::Status> {
    match backend {
        "elasticsearch" => Ok(json!({
            "method": "POST",
            "path": format!("/{}/_delete_by_query?refresh=true", collection.to_ascii_lowercase()),
            "body": { "query": { "terms": { "_id": point_ids } } }
        })),
        "pinecone" => Ok(json!({
            "method": "POST",
            "path": "/vectors/delete",
            "body": { "ids": point_ids }
        })),
        // Weaviate self-assigns object UUIDs on upsert (the setup_data.rs weaviate
        // upsert sends no explicit id), so our point id is NOT the Weaviate object
        // id — a `DELETE /v1/objects/{class}/{id}` cannot target it. Instead we
        // delete by the server-stamped chunk provenance (`_parent_pk`+`_chunk_seq`)
        // via a batch `where`, which needs no object id. NOTE: this relies on
        // Weaviate auto-schema (default on) having typed `_chunk_seq` as an int; if
        // an operator disables auto-schema, setup_data.rs's weaviate `ensure_resource`
        // must declare `_source`(text)/`_parent_pk`(text)/`_chunk_seq`(int) so the
        // where matches. The GDPR erasure path (delete_by_filter) is unaffected.
        "weaviate" => {
            // One batch-delete round-trip: match the named chunks by their
            // server-stamped `_parent_pk`+`_chunk_seq` provenance (an OR of the
            // ids), which removes exactly those chunks without needing the
            // Weaviate object id and without over-deleting a parent's survivors.
            Ok(json!({
                "method": "DELETE",
                "path": "/v1/batch/objects",
                "body": {
                    "match": {
                        "class": weaviate_class_name(collection),
                        "where": weaviate_ids_where(point_ids)
                    }
                }
            }))
        }
        other => Err(portable_delete_unsupported(
            other,
            "embedding_vector_delete",
        )),
    }
}

/// Weaviate `where` matching any of `point_ids` by their server-stamped chunk
/// provenance (`_parent_pk` + `_chunk_seq`), so a stale-chunk trim removes exactly
/// the named chunks without an explicit object id and without over-deleting a
/// parent's surviving chunks. Pure.
fn weaviate_ids_where(point_ids: &[String]) -> JsonValue {
    let operands: Vec<JsonValue> = point_ids
        .iter()
        .map(|id| {
            let (parent, seq) = super::chunking::parse_chunk_point_id(id);
            json!({
                "operator": "And",
                "operands": [
                    { "path": ["_parent_pk"], "operator": "Equal", "valueText": parent },
                    { "path": ["_chunk_seq"], "operator": "Equal", "valueInt": seq }
                ]
            })
        })
        .collect();
    if operands.len() == 1 {
        operands.into_iter().next().unwrap_or(JsonValue::Null)
    } else {
        json!({ "operator": "Or", "operands": operands })
    }
}

/// Translate a Qdrant-style scope filter into a native filtered-delete for a
/// non-Qdrant backend (the GDPR source/row erasure path). Pure — unit-tested.
fn portable_delete_by_filter_spec(
    backend: &str,
    collection: &str,
    filter: &JsonValue,
) -> Result<JsonValue, tonic::Status> {
    let terms = extract_scope_terms(filter);
    if terms.is_empty() {
        // Refuse an unscoped delete — an empty filter would erase the whole
        // collection (cross-tenant). The teardown callers always pass a
        // tenant+source scope; a missing one is a bug, fail closed.
        return Err(crate::runtime::executor_utils::capability_status(
            "embedding",
            "embedding_vector_delete_by_filter",
            "vector_backend_portable_delete",
            "refusing an unscoped portable vector delete (no tenant/source filter terms)"
                .to_string(),
        ));
    }
    match backend {
        "elasticsearch" => {
            let filter_terms: Vec<JsonValue> = terms
                .iter()
                .map(|(key, value)| {
                    // Mirror `core/setup_data.rs::es_payload_filter_terms`: match the
                    // stamped `payload.<key>.keyword` sub-field for exact equality.
                    let mut term = serde_json::Map::new();
                    term.insert(
                        format!("payload.{key}.keyword"),
                        JsonValue::String(value.clone()),
                    );
                    json!({ "term": JsonValue::Object(term) })
                })
                .collect();
            Ok(json!({
                "method": "POST",
                "path": format!(
                    "/{}/_delete_by_query?refresh=true",
                    collection.to_ascii_lowercase()
                ),
                "body": { "query": { "bool": { "filter": filter_terms } } }
            }))
        }
        "pinecone" => {
            let mut metadata_filter = serde_json::Map::new();
            for (key, value) in &terms {
                metadata_filter.insert(key.clone(), json!({ "$eq": value }));
            }
            Ok(json!({
                "method": "POST",
                "path": "/vectors/delete",
                "body": { "filter": JsonValue::Object(metadata_filter) }
            }))
        }
        "weaviate" => {
            let operands: Vec<JsonValue> = terms
                .iter()
                .map(|(key, value)| {
                    json!({ "path": [key], "operator": "Equal", "valueText": value })
                })
                .collect();
            let where_clause = if operands.len() == 1 {
                operands.into_iter().next().unwrap_or(JsonValue::Null)
            } else {
                json!({ "operator": "And", "operands": operands })
            };
            Ok(json!({
                "method": "DELETE",
                "path": "/v1/batch/objects",
                "body": {
                    "match": {
                        "class": weaviate_class_name(collection),
                        "where": where_clause
                    }
                }
            }))
        }
        other => Err(portable_delete_unsupported(
            other,
            "embedding_vector_delete_by_filter",
        )),
    }
}

/// Shape a portable alias-cutover spec. Elasticsearch has real aliases (atomic
/// remove-all + add). Pinecone/Weaviate have no collection-alias primitive, so a
/// cutover there is a capability limit (not an erasure hole). Pure.
fn portable_alias_swap_spec(
    backend: &str,
    alias: &str,
    collection: &str,
) -> Result<JsonValue, tonic::Status> {
    match backend {
        "elasticsearch" => Ok(json!({
            "method": "POST",
            "path": "/_aliases",
            "body": {
                "actions": [
                    { "remove": { "index": "*", "alias": alias } },
                    { "add": { "index": collection.to_ascii_lowercase(), "alias": alias } }
                ]
            }
        })),
        other => Err(crate::runtime::executor_utils::capability_status(
            "embedding",
            "embedding_vector_alias_cutover",
            "vector_backend_alias_cutover",
            format!(
                "vector backend '{other}' has no collection-alias primitive; a Matryoshka/reindex \
                 cutover addresses the physical collection directly"
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weaviate_class_name_matches_setup_data() {
        // Mirror of the setup_data.rs derivation — drift here breaks deletes.
        assert_eq!(
            weaviate_class_name("udb_asset_embeddings"),
            "Udbudb_asset_embeddings"
        );
        assert_eq!(weaviate_class_name("Vectors"), "Vectors");
        assert_eq!(weaviate_class_name("123"), "Udb123");
        assert_eq!(weaviate_class_name("!!!"), "UdbVector");
    }

    #[test]
    fn extract_scope_terms_reads_must_equality_clauses() {
        let filter = json!({
            "must": [
                { "key": "_tenant_id", "match": { "value": "acme" } },
                { "key": "_source", "match": { "value": "orders" } },
                { "key": "_ignored", "match": { "any": ["x"] } }
            ]
        });
        let terms = extract_scope_terms(&filter);
        assert_eq!(
            terms,
            vec![
                ("_tenant_id".to_string(), "acme".to_string()),
                ("_source".to_string(), "orders".to_string()),
            ],
            "only string-equality must clauses are translated"
        );
    }

    #[test]
    fn es_delete_by_ids_targets_the_id_terms() {
        let spec = portable_delete_by_ids_spec(
            "elasticsearch",
            "Corpus",
            &["a".to_string(), "b".to_string()],
        )
        .expect("es spec");
        assert_eq!(spec["method"], "POST");
        assert_eq!(spec["path"], "/corpus/_delete_by_query?refresh=true");
        assert_eq!(spec["body"]["query"]["terms"]["_id"], json!(["a", "b"]));
    }

    #[test]
    fn pinecone_delete_by_ids_uses_ids_body() {
        let spec = portable_delete_by_ids_spec("pinecone", "c", &["p1".to_string()])
            .expect("pinecone spec");
        assert_eq!(spec["path"], "/vectors/delete");
        assert_eq!(spec["body"]["ids"], json!(["p1"]));
    }

    #[test]
    fn es_delete_by_filter_ands_payload_keyword_terms() {
        let filter = json!({
            "must": [
                { "key": "_tenant_id", "match": { "value": "acme" } },
                { "key": "_source", "match": { "value": "orders" } }
            ]
        });
        let spec = portable_delete_by_filter_spec("elasticsearch", "Corpus", &filter)
            .expect("es filter spec");
        let terms = spec["body"]["query"]["bool"]["filter"]
            .as_array()
            .expect("filter array");
        assert_eq!(terms.len(), 2);
        assert_eq!(terms[0]["term"]["payload._tenant_id.keyword"], "acme");
        assert_eq!(terms[1]["term"]["payload._source.keyword"], "orders");
    }

    #[test]
    fn pinecone_delete_by_filter_uses_eq_metadata() {
        let filter = json!({
            "must": [ { "key": "_tenant_id", "match": { "value": "acme" } } ]
        });
        let spec =
            portable_delete_by_filter_spec("pinecone", "c", &filter).expect("pinecone filter spec");
        assert_eq!(spec["body"]["filter"]["_tenant_id"]["$eq"], "acme");
    }

    #[test]
    fn weaviate_delete_by_filter_ands_equal_operands() {
        let filter = json!({
            "must": [
                { "key": "_tenant_id", "match": { "value": "acme" } },
                { "key": "_source", "match": { "value": "orders" } }
            ]
        });
        let spec = portable_delete_by_filter_spec("weaviate", "corpus", &filter)
            .expect("weaviate filter spec");
        assert_eq!(spec["method"], "DELETE");
        assert_eq!(spec["body"]["match"]["class"], "Udbcorpus");
        let where_clause = &spec["body"]["match"]["where"];
        assert_eq!(where_clause["operator"], "And");
        assert_eq!(where_clause["operands"][0]["path"], json!(["_tenant_id"]));
        assert_eq!(where_clause["operands"][0]["valueText"], "acme");
    }

    #[test]
    fn empty_filter_is_refused_fail_closed() {
        let err = portable_delete_by_filter_spec("elasticsearch", "c", &json!({ "must": [] }))
            .expect_err("empty filter must be refused");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn es_alias_swap_removes_all_then_adds() {
        let spec = portable_alias_swap_spec("elasticsearch", "corpus-alias", "Corpus-v2")
            .expect("es alias spec");
        let actions = spec["body"]["actions"].as_array().expect("actions");
        assert_eq!(actions[0]["remove"]["alias"], "corpus-alias");
        assert_eq!(actions[1]["add"]["index"], "corpus-v2");
        assert_eq!(actions[1]["add"]["alias"], "corpus-alias");
    }

    #[test]
    fn alias_swap_unsupported_backend_is_a_capability_error() {
        let err = portable_alias_swap_spec("pinecone", "a", "c")
            .expect_err("pinecone has no alias primitive");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn unknown_backend_delete_is_a_capability_error() {
        let err = portable_delete_by_ids_spec("cassandra", "c", &["x".to_string()])
            .expect_err("unknown backend");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }
}
