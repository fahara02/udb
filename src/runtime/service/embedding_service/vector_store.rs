use std::sync::Arc;

use serde_json::{Value as JsonValue, json};

use crate::proto::{
    VectorHybridSearchRequest, VectorPointMutation, VectorSearchRequest, VectorSet,
};
use crate::runtime::DataBrokerRuntime;

use super::model::StoredModel;
use crate::runtime::core::setup_data::{
    PINECONE_COLLECTION_KEY, pinecone_namespace, pinecone_vector_id,
};

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
    /// Delete LOGICAL point ids (as the durable work items / journal carry them)
    /// owned by `tenant_id`. The store maps them to the tenant-scoped engine ids
    /// the write path stored, so a delete can never reach another tenant's
    /// same-pk vector. A blank tenant is refused (no unscoped id delete).
    async fn delete_points(
        &self,
        tenant_id: &str,
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
        tenant_id: &str,
        collection: &str,
        point_ids: Vec<String>,
    ) -> Result<(), tonic::Status> {
        if point_ids.is_empty() {
            return Ok(());
        }
        let tenant = require_delete_tenant(tenant_id)?;
        if self.is_qdrant() {
            return self
                .runtime
                .vector_delete_backend_target(
                    self.instance(),
                    &self.project_id,
                    collection,
                    scoped_point_ids(tenant, &point_ids),
                )
                .await;
        }
        let spec = portable_delete_by_ids_spec(
            &self.backend,
            &self.project_id,
            collection,
            tenant,
            &point_ids,
        )?;
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
        let spec =
            portable_delete_by_filter_spec(&self.backend, &self.project_id, collection, &filter)?;
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

/// Refuse a point-id delete without a verified tenant: the engine ids are
/// tenant-scoped, so a blank tenant would address legacy unscoped ids that any
/// tenant could own.
fn require_delete_tenant(tenant_id: &str) -> Result<&str, tonic::Status> {
    let tenant = tenant_id.trim();
    if tenant.is_empty() {
        return Err(crate::runtime::executor_utils::capability_status(
            "embedding",
            "embedding_vector_delete",
            "verified_tenant_required",
            "refusing a vector point delete without a verified tenant".to_string(),
        ));
    }
    Ok(tenant)
}

/// Map logical point ids to the tenant-scoped engine ids the write path stored
/// (`model::build_embedding_point`). Pure.
fn scoped_point_ids(tenant_id: &str, point_ids: &[String]) -> Vec<String> {
    point_ids
        .iter()
        .map(|id| super::chunking::tenant_scoped_point_id(tenant_id, id))
        .collect()
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
/// `MutationExecutor`). `point_ids` are LOGICAL ids (guaranteed non-empty by the
/// caller); ES/Pinecone address the tenant-scoped engine ids, Weaviate matches
/// the logical provenance AND the tenant tag. Pure — unit-tested.
fn portable_delete_by_ids_spec(
    backend: &str,
    project_id: &str,
    collection: &str,
    tenant_id: &str,
    point_ids: &[String],
) -> Result<JsonValue, tonic::Status> {
    match backend {
        "elasticsearch" => Ok(json!({
            "method": "POST",
            "path": format!("/{}/_delete_by_query?refresh=true", collection.to_ascii_lowercase()),
            "body": { "query": { "terms": { "_id": scoped_point_ids(tenant_id, point_ids) } } }
        })),
        // Pinecone ids carry the collection prefix and live in the project
        // namespace (mirrors the generic upsert in `core/setup_data.rs`).
        "pinecone" => Ok(json!({
            "method": "POST",
            "path": "/vectors/delete",
            "body": {
                "ids": scoped_point_ids(tenant_id, point_ids)
                    .iter()
                    .map(|id| pinecone_vector_id(collection, id))
                    .collect::<Vec<_>>(),
                "namespace": pinecone_namespace(project_id)
            }
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
                        "where": {
                            "operator": "And",
                            "operands": [
                                {
                                    "path": ["_tenant_id"],
                                    "operator": "Equal",
                                    "valueText": tenant_id
                                },
                                weaviate_ids_where(point_ids)
                            ]
                        }
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
    project_id: &str,
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
            // Explicit `$and` operands (never a merged key map), plus the
            // logical collection: one index backs every collection.
            let mut operands = vec![json!({ PINECONE_COLLECTION_KEY: { "$eq": collection } })];
            for (key, value) in &terms {
                let mut clause = serde_json::Map::new();
                clause.insert(key.clone(), json!({ "$eq": value }));
                operands.push(JsonValue::Object(clause));
            }
            Ok(json!({
                "method": "POST",
                "path": "/vectors/delete",
                "body": {
                    "filter": { "$and": operands },
                    "namespace": pinecone_namespace(project_id)
                }
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

/// One physical vector collection a tenant's points may live in. The hard
/// tenant purge deletes the tenant's points from every target by the
/// `_tenant_id` stamp (never by id enumeration, which misses points whose
/// source rows or work events are already gone).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TenantVectorTarget {
    pub(crate) project_id: String,
    pub(crate) backend: String,
    pub(crate) instance: String,
    pub(crate) collection: String,
}

impl TenantVectorTarget {
    /// `None` when the backend or collection is blank (nothing addressable).
    pub(crate) fn new(
        project_id: &str,
        backend: &str,
        instance: &str,
        collection: &str,
    ) -> Option<Self> {
        let backend = backend.trim().to_ascii_lowercase();
        let collection = collection.trim();
        if backend.is_empty() || collection.is_empty() {
            return None;
        }
        Some(Self {
            project_id: project_id.trim().to_string(),
            backend,
            instance: instance.trim().to_string(),
            collection: collection.to_string(),
        })
    }
}

/// The filter a hard tenant purge deletes by: every point stamped with the
/// tenant. `None` for a blank tenant — an empty filter would erase the whole
/// collection across tenants. Pure.
pub(crate) fn tenant_vector_purge_filter(tenant_id: &str) -> Option<JsonValue> {
    let tenant = tenant_id.trim();
    (!tenant.is_empty()).then(|| {
        json!({
            "must": [ { "key": "_tenant_id", "match": { "value": tenant } } ]
        })
    })
}

/// Delete every `_tenant_id`-stamped point of `tenant_id` from each target
/// through the shared vector seam (Qdrant typed delete, or the portable
/// ES/Pinecone/Weaviate filtered delete). Every target is attempted; each
/// outcome is returned so the caller can REPORT a failure (a purge that leaves
/// vectors behind must say so) rather than abort half-way.
pub(crate) async fn purge_tenant_vectors(
    runtime: &Arc<DataBrokerRuntime>,
    tenant_id: &str,
    targets: &std::collections::BTreeSet<TenantVectorTarget>,
) -> Vec<(TenantVectorTarget, Result<(), String>)> {
    let Some(filter) = tenant_vector_purge_filter(tenant_id) else {
        return targets
            .iter()
            .map(|target| {
                (
                    target.clone(),
                    Err("refusing a vector purge without a tenant".to_string()),
                )
            })
            .collect();
    };
    let mut outcomes = Vec::with_capacity(targets.len());
    for target in targets {
        let store = RuntimeVectorStore::for_routing(
            Arc::clone(runtime),
            &target.project_id,
            &target.backend,
            &target.instance,
        );
        let result = store
            .delete_by_filter(&target.collection, filter.clone())
            .await
            .map_err(|status| status.message().to_string());
        outcomes.push((target.clone(), result));
    }
    outcomes
}

/// Page size for enumerating a tenant's embedding models during a purge.
const TENANT_PURGE_MODEL_PAGE: u32 = 200;

/// The collections the tenant's registered embedding models write to, read from
/// the durable model registry. Must run BEFORE the relational purge deletes the
/// registry rows.
pub(crate) async fn tenant_embedding_vector_targets(
    runtime: &DataBrokerRuntime,
    tenant_id: &str,
    project_id: &str,
) -> Result<Vec<TenantVectorTarget>, tonic::Status> {
    let context = super::super::native_helpers::native_service_context(
        &tonic::metadata::MetadataMap::new(),
        tenant_id,
        project_id,
    );
    let mut targets = Vec::new();
    let mut offset = 0u64;
    loop {
        let rows = runtime
            .native_entity_read_for_service(
                "embedding",
                &context,
                super::store::models_read(tenant_id, None, offset, TENANT_PURGE_MODEL_PAGE),
            )
            .await?;
        for row in &rows {
            let model = super::model::stored_model_from_json(row);
            if let Some(target) = TenantVectorTarget::new(
                &context.project_id,
                &model.vector_backend,
                &model.vector_instance,
                &model.active_collection,
            ) {
                targets.push(target);
            }
        }
        if rows.len() < TENANT_PURGE_MODEL_PAGE as usize {
            break;
        }
        offset = offset.saturating_add(u64::from(TENANT_PURGE_MODEL_PAGE));
    }
    Ok(targets)
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
            "p1",
            "Corpus",
            "acme",
            &["a".to_string(), "b".to_string()],
        )
        .expect("es spec");
        assert_eq!(spec["method"], "POST");
        assert_eq!(spec["path"], "/corpus/_delete_by_query?refresh=true");
        // The tenant-scoped engine ids the write path stored, never bare pks.
        assert_eq!(
            spec["body"]["query"]["terms"]["_id"],
            json!(["acme:a", "acme:b"])
        );
    }

    #[test]
    fn pinecone_delete_by_ids_uses_ids_body() {
        let spec =
            portable_delete_by_ids_spec("pinecone", "proj", "c", "acme", &["p1".to_string()])
                .expect("pinecone spec");
        assert_eq!(spec["path"], "/vectors/delete");
        // Collection-prefixed, tenant-scoped id in the project namespace.
        assert_eq!(spec["body"]["ids"], json!(["c::acme:p1"]));
        assert_eq!(spec["body"]["namespace"], "proj");
    }

    #[test]
    fn weaviate_delete_by_ids_is_anded_with_the_tenant() {
        let spec =
            portable_delete_by_ids_spec("weaviate", "p1", "corpus", "acme", &["row-1".to_string()])
                .expect("weaviate spec");
        let where_clause = &spec["body"]["match"]["where"];
        assert_eq!(where_clause["operator"], "And");
        assert_eq!(where_clause["operands"][0]["path"], json!(["_tenant_id"]));
        assert_eq!(where_clause["operands"][0]["valueText"], "acme");
        // The provenance match uses the LOGICAL id (parent pk + chunk seq).
        assert_eq!(
            where_clause["operands"][1]["operands"][0]["valueText"],
            "row-1"
        );
    }

    #[test]
    fn point_delete_without_a_tenant_is_refused() {
        let err = require_delete_tenant("  ").expect_err("blank tenant must be refused");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(require_delete_tenant(" acme ").expect("tenant"), "acme");
    }

    #[test]
    fn es_delete_by_filter_ands_payload_keyword_terms() {
        let filter = json!({
            "must": [
                { "key": "_tenant_id", "match": { "value": "acme" } },
                { "key": "_source", "match": { "value": "orders" } }
            ]
        });
        let spec = portable_delete_by_filter_spec("elasticsearch", "p1", "Corpus", &filter)
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
        let spec = portable_delete_by_filter_spec("pinecone", "p1", "c", &filter)
            .expect("pinecone filter spec");
        assert_eq!(
            spec["body"]["filter"],
            json!({ "$and": [
                { "_collection": { "$eq": "c" } },
                { "_tenant_id": { "$eq": "acme" } }
            ] })
        );
        assert_eq!(spec["body"]["namespace"], "p1");
    }

    #[test]
    fn weaviate_delete_by_filter_ands_equal_operands() {
        let filter = json!({
            "must": [
                { "key": "_tenant_id", "match": { "value": "acme" } },
                { "key": "_source", "match": { "value": "orders" } }
            ]
        });
        let spec = portable_delete_by_filter_spec("weaviate", "p1", "corpus", &filter)
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
        let err =
            portable_delete_by_filter_spec("elasticsearch", "p1", "c", &json!({ "must": [] }))
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
    fn tenant_purge_filter_scopes_to_the_tenant_and_refuses_blank() {
        let filter = tenant_vector_purge_filter(" acme ").expect("filter");
        assert_eq!(
            filter,
            json!({ "must": [ { "key": "_tenant_id", "match": { "value": "acme" } } ] })
        );
        assert!(tenant_vector_purge_filter("  ").is_none());
        // The portable translation keeps the tenant term (no unscoped delete).
        let spec = portable_delete_by_filter_spec("pinecone", "p1", "c", &filter).expect("spec");
        assert_eq!(
            spec["body"]["filter"]["$and"][1]["_tenant_id"]["$eq"],
            "acme"
        );
    }

    #[test]
    fn tenant_vector_target_normalizes_and_rejects_blank() {
        let target = TenantVectorTarget::new(" p1 ", " Qdrant ", "", " docs ").expect("target");
        assert_eq!(target.backend, "qdrant");
        assert_eq!(target.collection, "docs");
        assert_eq!(target.project_id, "p1");
        assert!(TenantVectorTarget::new("p1", "", "", "docs").is_none());
        assert!(TenantVectorTarget::new("p1", "qdrant", "", "  ").is_none());
    }

    #[test]
    fn unknown_backend_delete_is_a_capability_error() {
        let err = portable_delete_by_ids_spec("cassandra", "p1", "c", "acme", &["x".to_string()])
            .expect_err("unknown backend");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }
}
