//! The five `SearchService` RPC handlers (create/delete/list index, search,
//! reindex) plus the per-request search helpers, extracted from the trait impl
//! as free `pub(crate) async fn`s taking `svc` where the trait method took
//! `&self`. `mod.rs` delegates one line to each. The two `self`-using helpers
//! (`resolve_source_tenant_column`, `query_one_index`) stay inherent methods so
//! their bodies are byte-identical. Bodies are verbatim — the same cross-tenant
//! guard, fail-closed tenant-column gate, per-tenant quota, engine provisioning,
//! mediated dispatch, server-side tenant filter, and RRF fusion as the former
//! god file.

use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::search::services::v1 as search_pb;
use crate::proto::{VectorHybridSearchRequest, VectorPoint, VectorSearchRequest, VectorSet};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, native_next_page_token, native_offset_page_window,
    native_service_context, non_empty_json, validate_request_tenant,
};
use super::SearchServiceImpl;
use super::config::{
    BACKEND_ELASTICSEARCH, BACKEND_QDRANT, ENGINE_VECTOR_DISTANCE, MAX_INDEXES_PER_TENANT,
    SEARCH_INDEX_MSG, STATUS_ACTIVE, STATUS_DELETED, STATUS_REINDEXING, TENANT_SCOPE_PAYLOAD_KEY,
    TOPIC_CREATED, TOPIC_DELETED, TOPIC_REINDEX, resolve_top_k,
};
use super::errors::{
    full_text_only_requires_mediated_ir_status, require_source_tenant_column,
    search_field_violation, search_index_not_found_status, search_required_field,
    validate_create_index_required_fields, validate_search_query,
};
use super::fusion::reciprocal_rank_fusion;
use super::model::{StoredIndex, collection_name, stored_index_from_json};
use super::store::{active_indexes_read, index_conflict, index_read_by_name, index_record};

impl SearchServiceImpl {
    /// Resolve the SOURCE entity's tenant column through the project-active
    /// manifest using the shared catalog resolver. Returns the column name or a
    /// fail-closed error when the source is unknown or not tenant-isolated.
    pub(crate) fn resolve_source_tenant_column(
        &self,
        project_id: &str,
        source_message_type: &str,
    ) -> Result<(String, String), Status> {
        let catalog = self.require_catalog()?;
        let state = catalog.active_for(project_id);
        let manifest = &state.manifest;
        let table = crate::broker::table_for_message(manifest, source_message_type).ok_or_else(
            || {
                search_field_violation(
                    "source_message_type",
                    "must be present in the active catalog manifest",
                    format!(
                        "source_message_type '{source_message_type}' is not present in the active catalog \
                         manifest"
                    ),
                )
            },
        )?;
        // SHARED resolver (no duplicate): same `util::resolve_tenant_column` family
        // behind `native_catalog::NativeModel::tenant_column`.
        let resolved = crate::runtime::postgres_helpers::tenant_column_ref(table)
            .map(|column| column.column_name.clone());
        let tenant_column = require_source_tenant_column(resolved, source_message_type)?;
        let cdc_topic = table.cdc_topic.clone();
        Ok((tenant_column, cdc_topic))
    }

    /// Query one index through the runtime's mediated vector dispatch, returning
    /// the ranked points (ids + engine payloads). The tenant filter is injected
    /// server-side from the verified claim; this never hand-builds a raw engine
    /// query. Returns an empty list for an index whose backend is not
    /// query-wired here.
    pub(crate) async fn query_one_index(
        &self,
        runtime: &DataBrokerRuntime,
        manifest: &crate::generation::CatalogManifest,
        context: &crate::RequestContext,
        index: &StoredIndex,
        req: &search_pb::SearchRequest,
        top_k: i32,
        tenant_filter: Option<prost_types::Struct>,
    ) -> Result<Vec<VectorPoint>, Status> {
        validate_search_query(req)?;
        let has_vector = !req.query_vector.is_empty();
        let has_text = !req.query_text.trim().is_empty();
        // Lexical relevance rides the hybrid dispatch (Qdrant's `_full_text`
        // prefetch + RRF), which requires a query vector. A pure text-only query
        // with no vector has no reachable mediated full-text path in this build
        // (P2.2): fail closed rather than silently route an empty vector.
        if has_text && !has_vector {
            return Err(full_text_only_requires_mediated_ir_status());
        }
        let collection = index.collection();
        // Hybrid (text + vector): the runtime's mediated hybrid dispatch applies
        // Qdrant native RRF; the tenant filter is injected server-side.
        let result: VectorSet = if has_text && has_vector {
            let request = VectorHybridSearchRequest {
                context: None,
                collection,
                vector: req.query_vector.clone(),
                text_query: req.query_text.clone(),
                filter: tenant_filter,
                limit: top_k,
                fusion_weights: Vec::new(),
                with_payload: true,
            };
            runtime
                .vector_hybrid_search(manifest, request, context.clone())
                .await?
        } else {
            // Vector-only: the mediated vector dispatch routes to the index backend
            // (Qdrant `/points/search`, or the Elasticsearch executor for an
            // ES-backed index), with the tenant filter injected server-side.
            let request = VectorSearchRequest {
                context: None,
                collection,
                vector: req.query_vector.clone(),
                filter: tenant_filter,
                limit: top_k,
                score_threshold: 0.0,
                with_payload: true,
            };
            runtime
                .vector_search(manifest, request, context.clone())
                .await?
        };
        Ok(result.points)
    }
}

/// Build the server-side tenant `must`/term filter as a protobuf `Struct` from
/// the VERIFIED claim tenant. Stamped onto every engine query so a caller can
/// never widen past their tenant. The `_tenant_id` payload key is the same one
/// the Qdrant IR compiler stamps at write time and ANDs into the `must` clause;
/// the Elasticsearch executor ANDs the equivalent term.
pub(crate) fn tenant_scope_filter(tenant_id: &str) -> Option<prost_types::Struct> {
    let body = serde_json::json!({
        "must": [
            { "key": TENANT_SCOPE_PAYLOAD_KEY, "match": { "value": tenant_id } }
        ]
    });
    crate::runtime::executor_utils::json_to_struct(&body)
}

/// Serialize a returned engine payload into the hit's `payload_json`, stripping
/// the broker-stamped tenant-scope key ([`TENANT_SCOPE_PAYLOAD_KEY`]) — it is
/// write-time bookkeeping, never application data. Empty/absent payloads map to
/// an empty string. Pure — unit-tested.
pub(crate) fn hit_payload_json(payload: Option<&prost_types::Struct>) -> String {
    let Some(payload) = payload else {
        return String::new();
    };
    let mut value = crate::runtime::executor_utils::struct_to_json(payload);
    let Some(object) = value.as_object_mut() else {
        return String::new();
    };
    object.remove(TENANT_SCOPE_PAYLOAD_KEY);
    if object.is_empty() {
        return String::new();
    }
    value.to_string()
}

pub(crate) async fn create_index(
    svc: &SearchServiceImpl,
    request: Request<search_pb::CreateIndexRequest>,
) -> Result<Response<search_pb::CreateIndexResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // Cross-tenant guard FIRST: the body tenant_id must match the verified
    // claim/header before any catalog/store access.
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let index_name = req.index_name.trim().to_string();
    let source_message_type = req.source_message_type.trim().to_string();
    let backend = req.backend.trim().to_ascii_lowercase();
    validate_create_index_required_fields(&index_name, &source_message_type)?;
    if backend != BACKEND_QDRANT && backend != BACKEND_ELASTICSEARCH {
        return Err(search_field_violation(
            "backend",
            format!("must be '{BACKEND_QDRANT}' or '{BACKEND_ELASTICSEARCH}'"),
            format!(
                "unsupported search backend '{backend}' (expected '{BACKEND_QDRANT}' or \
                 '{BACKEND_ELASTICSEARCH}')"
            ),
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "search",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    // FAIL CLOSED: resolve the source table's tenant column via the shared
    // catalog resolver. No tenant column ⇒ no index.
    let (tenant_column, source_cdc_topic) =
        svc.resolve_source_tenant_column(&context.project_id, &source_message_type)?;

    // Per-tenant index quota (only a brand-new index counts).
    let existing = runtime
        .native_entity_read_for_service(
            "search",
            &context,
            index_read_by_name(&tenant_id, &index_name),
        )
        .await?
        .first()
        .map(stored_index_from_json);
    if existing.is_none() {
        let active = runtime
            .native_entity_read_for_service(
                "search",
                &context,
                active_indexes_read(&tenant_id, 0, (MAX_INDEXES_PER_TENANT as u32) + 1),
            )
            .await?;
        if active.len() >= MAX_INDEXES_PER_TENANT {
            return Err(crate::runtime::executor_utils::quota_refusal_status(
                "search",
                "tenant search-index quota",
                format!("tenant search-index quota exhausted ({MAX_INDEXES_PER_TENANT})"),
            ));
        }
    }

    let resource_name = req.resource_name.trim().to_string();

    // Engine lifecycle (provision on create): a brand-new vector index
    // ensures its backing engine resource up front through the runtime's
    // EXISTING resource-admin seam (`ensure_resource_backend_target` →
    // Qdrant `ensure_collection` / Elasticsearch index PUT) — never a
    // parallel engine client. FAIL CLOSED: an unregistered or unreachable
    // backend rejects the registration instead of recording a capability
    // lie. Re-registration of an existing index skips the ensure (the
    // resource already exists, and the Elasticsearch index PUT is not
    // idempotent); a full-text-only index (vector_dims == 0) is provisioned
    // engine-side on first write.
    if existing.is_none() && req.vector_dims > 0 {
        let engine_spec = serde_json::json!({
            "dimension": req.vector_dims,
            "distance": ENGINE_VECTOR_DISTANCE,
        })
        .to_string();
        runtime
            .ensure_resource_backend_target(
                &backend,
                None,
                &collection_name(&resource_name, &index_name),
                &engine_spec,
            )
            .await?;
    }

    let index_id = existing
        .as_ref()
        .map(|row| row.index_id.clone())
        .filter(|id| !id.trim().is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let metadata_json = non_empty_json(&req.metadata_json);

    runtime
        .native_entity_write_for_service(
            "search",
            &context,
            SEARCH_INDEX_MSG,
            index_record(
                &index_id,
                &tenant_id,
                &index_name,
                &source_message_type,
                &backend,
                &resource_name,
                req.vector_dims,
                &tenant_column,
                &source_cdc_topic,
                STATUS_ACTIVE,
                &metadata_json,
            ),
            index_conflict(),
        )
        .await?;

    svc.emit_index_event(
        TOPIC_CREATED,
        &tenant_id,
        &context.project_id,
        &index_name,
        serde_json::json!({
            "source_message_type": source_message_type,
            "backend": backend,
            "tenant_column": tenant_column,
        }),
    )
    .await;

    Ok(Response::new(search_pb::CreateIndexResponse {
        index_id,
        index_name,
        tenant_column,
        message: "search index registered".to_string(),
        error: None,
    }))
}

pub(crate) async fn delete_index(
    svc: &SearchServiceImpl,
    request: Request<search_pb::DeleteIndexRequest>,
) -> Result<Response<search_pb::DeleteIndexResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let index_name = req.index_name.trim().to_string();
    if index_name.is_empty() {
        return Err(search_required_field(
            "index_name",
            "must be a non-empty search index name",
            "index_name is required",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "search",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let stored = runtime
        .native_entity_read_for_service(
            "search",
            &context,
            index_read_by_name(&tenant_id, &index_name),
        )
        .await?
        .first()
        .map(stored_index_from_json);
    let Some(stored) = stored.filter(|row| row.status != STATUS_DELETED) else {
        // Idempotent: nothing to delete.
        return Ok(Response::new(search_pb::DeleteIndexResponse {
            deleted: true,
            message: "search index not found".to_string(),
            error: None,
        }));
    };

    runtime
        .native_entity_write_for_service(
            "search",
            &context,
            SEARCH_INDEX_MSG,
            index_record(
                &stored.index_id,
                &tenant_id,
                &stored.index_name,
                &stored.source_message_type,
                &stored.backend,
                &stored.resource_name,
                stored.vector_dims,
                &stored.tenant_column,
                &stored.source_cdc_topic,
                STATUS_DELETED,
                "{}",
            ),
            index_conflict(),
        )
        .await?;

    // Tear down on delete: this deleted-event IS the teardown job — the
    // leader-owned `run_search_reindex_once` pass consumes it, purges the
    // index's tenant-scoped engine points through the existing per-point
    // vector delete seam, and emits `TOPIC_TEARDOWN_COMPLETED` when done.
    svc.emit_index_event(
        TOPIC_DELETED,
        &tenant_id,
        &context.project_id,
        &index_name,
        serde_json::json!({
            "backend": stored.backend,
            "resource_name": stored.resource_name,
            "source_message_type": stored.source_message_type,
        }),
    )
    .await;

    Ok(Response::new(search_pb::DeleteIndexResponse {
        deleted: true,
        message: "search index deleted".to_string(),
        error: None,
    }))
}

pub(crate) async fn list_indexes(
    svc: &SearchServiceImpl,
    request: Request<search_pb::ListIndexesRequest>,
) -> Result<Response<search_pb::ListIndexesResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "search",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");
    let page_window = native_offset_page_window(
        1,
        req.page_size,
        &req.page_token,
        MAX_INDEXES_PER_TENANT as i32,
    );

    let rows = runtime
        .native_entity_read_for_service(
            "search",
            &context,
            active_indexes_read(
                &tenant_id,
                page_window.offset as u64,
                (page_window.limit as u32).min(MAX_INDEXES_PER_TENANT as u32),
            ),
        )
        .await?;
    let indexes = rows
        .iter()
        .map(stored_index_from_json)
        .map(|index| search_pb::SearchIndexSummary {
            index_id: index.index_id,
            index_name: index.index_name,
            source_message_type: index.source_message_type,
            backend: index.backend,
            resource_name: index.resource_name,
            vector_dims: index.vector_dims,
            status: index.status,
        })
        .collect::<Vec<_>>();
    let next_page_token =
        native_next_page_token(page_window.offset, page_window.limit, indexes.len());

    Ok(Response::new(search_pb::ListIndexesResponse {
        indexes,
        message: "ok".to_string(),
        error: None,
        next_page_token,
    }))
}

pub(crate) async fn search(
    svc: &SearchServiceImpl,
    request: Request<search_pb::SearchRequest>,
) -> Result<Response<search_pb::SearchResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "search",
        OperationChannel::Vector,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let catalog = svc.require_catalog()?;
    let mut context = native_service_context(&metadata, &tenant_id, "");
    context.scopes.push("udb:vector:read".to_string());
    let top_k = resolve_top_k(req.top_k);

    // Resolve the target index(es): a single named index, or all of the
    // tenant's active indexes when none is named ("one search box").
    let target_name = req.index_name.trim();
    let targets: Vec<StoredIndex> = if target_name.is_empty() {
        runtime
            .native_entity_read_for_service(
                "search",
                &context,
                active_indexes_read(&tenant_id, 0, MAX_INDEXES_PER_TENANT as u32),
            )
            .await?
            .iter()
            .map(stored_index_from_json)
            .collect()
    } else {
        runtime
            .native_entity_read_for_service(
                "search",
                &context,
                index_read_by_name(&tenant_id, target_name),
            )
            .await?
            .first()
            .map(stored_index_from_json)
            .filter(|index| index.status == STATUS_ACTIVE)
            .into_iter()
            .collect()
    };
    if targets.is_empty() {
        return Ok(Response::new(search_pb::SearchResponse {
            hits: Vec::new(),
            message: "no matching index".to_string(),
            error: None,
            next_page_token: String::new(),
        }));
    }

    // SERVER-SIDE tenant filter built from the VERIFIED claim, injected into
    // every engine query (Qdrant `must` / ES term). Never from the body.
    let tenant_filter = tenant_scope_filter(&tenant_id);
    let state = catalog.active_for(&context.project_id);
    let manifest = &state.manifest;

    // Query each target through the mediated dispatch, then fuse the ranked
    // id lists across indexes with pure RRF. Per hit id the first-seen
    // (index name, tenant-stripped payload_json) pair is kept for mapping.
    let mut ranked_lists: Vec<Vec<String>> = Vec::with_capacity(targets.len());
    let mut hit_meta: std::collections::HashMap<String, (String, String)> =
        std::collections::HashMap::new();
    for index in &targets {
        let points = svc
            .query_one_index(
                runtime,
                manifest,
                &context,
                index,
                &req,
                top_k,
                tenant_filter.clone(),
            )
            .await?;
        let mut ids = Vec::with_capacity(points.len());
        for point in points {
            hit_meta.entry(point.id.clone()).or_insert_with(|| {
                (
                    index.index_name.clone(),
                    hit_payload_json(point.payload.as_ref()),
                )
            });
            ids.push(point.id);
        }
        ranked_lists.push(ids);
    }

    let fused = reciprocal_rank_fusion(&ranked_lists);
    let requested_page_size = if req.page_size > 0 {
        req.page_size
    } else {
        top_k
    };
    let page_window =
        native_offset_page_window(1, requested_page_size, &req.page_token, top_k.max(1));
    let hits = fused
        .into_iter()
        .skip(page_window.offset)
        .take(page_window.limit.min(top_k as usize))
        .map(|(id, score)| {
            let (index_name, payload_json) = hit_meta.get(&id).cloned().unwrap_or_default();
            search_pb::SearchHit {
                index_name,
                id,
                score,
                payload_json,
            }
        })
        .collect::<Vec<_>>();
    let next_page_token = native_next_page_token(
        page_window.offset,
        page_window.limit.min(top_k as usize),
        hits.len(),
    );

    Ok(Response::new(search_pb::SearchResponse {
        hits,
        message: "ok".to_string(),
        error: None,
        next_page_token,
    }))
}

pub(crate) async fn reindex(
    svc: &SearchServiceImpl,
    request: Request<search_pb::ReindexRequest>,
) -> Result<Response<search_pb::ReindexResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let index_name = req.index_name.trim().to_string();
    if index_name.is_empty() {
        return Err(search_required_field(
            "index_name",
            "must be a non-empty search index name",
            "index_name is required",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "search",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let stored = runtime
        .native_entity_read_for_service(
            "search",
            &context,
            index_read_by_name(&tenant_id, &index_name),
        )
        .await?
        .first()
        .map(stored_index_from_json)
        .filter(|index| index.status != STATUS_DELETED)
        .ok_or_else(|| search_index_not_found_status("reindex"))?;

    // Mark REINDEXING and emit the work event. The backfill loop (which reads
    // source rows ONLY through the mediated IR path) runs in the leader-owned
    // `run_search_reindex_once` pass, which restores ACTIVE and emits
    // `TOPIC_REINDEX_COMPLETED`; this RPC just admits the request idempotently.
    runtime
        .native_entity_write_for_service(
            "search",
            &context,
            SEARCH_INDEX_MSG,
            index_record(
                &stored.index_id,
                &tenant_id,
                &stored.index_name,
                &stored.source_message_type,
                &stored.backend,
                &stored.resource_name,
                stored.vector_dims,
                &stored.tenant_column,
                &stored.source_cdc_topic,
                STATUS_REINDEXING,
                "{}",
            ),
            index_conflict(),
        )
        .await?;

    let reindex_id = Uuid::new_v4().to_string();
    svc.emit_index_event(
        TOPIC_REINDEX,
        &tenant_id,
        &context.project_id,
        &index_name,
        serde_json::json!({
            "reindex_id": reindex_id,
            "source_message_type": stored.source_message_type,
        }),
    )
    .await;

    Ok(Response::new(search_pb::ReindexResponse {
        reindex_id,
        accepted: true,
        message: "reindex requested".to_string(),
        error: None,
    }))
}
