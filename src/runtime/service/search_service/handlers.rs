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
    admit_on as native_admit_on, native_next_page_token, native_next_page_token_for_total,
    native_offset_page_window, native_service_context, non_empty_json, validate_request_tenant,
};
use super::SearchServiceImpl;
use super::config::{
    BACKEND_ELASTICSEARCH, BACKEND_QDRANT, ENGINE_VECTOR_DISTANCE, SEARCH_INDEX_MSG, STATUS_ACTIVE,
    STATUS_DELETED, STATUS_REINDEXING, TENANT_SCOPE_PAYLOAD_KEY, TOPIC_CREATED, TOPIC_DELETED,
    TOPIC_REINDEX, max_indexes_per_tenant, max_top_k, resolve_top_k,
};
use super::errors::{
    full_text_only_requires_mediated_ir_status, require_source_tenant_column,
    search_field_violation, search_index_not_found_status, search_required_field,
    validate_create_index_required_fields, validate_search_query,
};
use super::fusion::fuse_ranked_lists;
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
        let table = crate::broker::resolve_table_for_message(manifest, source_message_type)
            .map_err(|error| {
                search_field_violation(
                    "source_message_type",
                    "must identify exactly one entity in the active catalog manifest",
                    error.to_string(),
                )
            })?;
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
                with_vector: false,
                vector_name: String::new(),
                fusion_strategy: 0,
                prefetch_limit: 0,
                quantization_rescore: false,
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
                with_vector: false,
                vector_name: String::new(),
                quantization_rescore: false,
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
                active_indexes_read(&tenant_id, 0, (max_indexes_per_tenant() as u32) + 1),
            )
            .await?;
        let quota = max_indexes_per_tenant();
        if active.len() >= quota {
            return Err(crate::runtime::executor_utils::quota_refusal_status(
                "search",
                "tenant search-index quota",
                format!("tenant search-index quota exhausted ({quota})"),
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
        max_indexes_per_tenant() as i32,
    );

    let rows = runtime
        .native_entity_read_for_service(
            "search",
            &context,
            active_indexes_read(
                &tenant_id,
                page_window.offset as u64,
                (page_window.limit as u32).min(max_indexes_per_tenant() as u32),
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
    // Honor the declared SearchMode: reject a request whose inputs contradict the
    // mode (e.g. VECTOR mode with no vector). UNSPECIFIED infers from the inputs
    // (the historical behavior). Full-text-only execution is still gated below.
    validate_search_mode(
        req.mode(),
        !req.query_text.trim().is_empty(),
        !req.query_vector.is_empty(),
    )?;
    let top_k = resolve_top_k(req.top_k);

    // Resolve the target index(es): a single named index, or all of the
    // tenant's active indexes when none is named ("one search box").
    let target_name = req.index_name.trim();
    let targets: Vec<StoredIndex> = if target_name.is_empty() {
        runtime
            .native_entity_read_for_service(
                "search",
                &context,
                active_indexes_read(&tenant_id, 0, max_indexes_per_tenant() as u32),
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

    // Compute the page window BEFORE querying so each index fetches enough
    // candidates to REACH the requested page. Fetching a fixed `top_k` per index
    // made any page past the first empty (offset >= top_k skipped the whole
    // fetched set) while still advertising a next-page token. We fetch
    // `offset + limit + 1` (the +1 detects a further page), bounded by the max.
    let requested_page_size = if req.page_size > 0 {
        req.page_size
    } else {
        top_k
    };
    let page_window =
        native_offset_page_window(1, requested_page_size, &req.page_token, top_k.max(1));
    let fetch_depth = page_window
        .offset
        .saturating_add(page_window.limit)
        .saturating_add(1)
        .min(max_top_k() as usize)
        .max(1) as i32;

    // Query each target through the mediated dispatch, then fuse the ranked
    // lists across indexes. Per hit id the first-seen (index name,
    // tenant-stripped payload_json) pair is kept for mapping. A single failing
    // index is skipped (not fatal) so "one search box" still returns the healthy
    // indexes' hits; the whole search fails only if EVERY target errored.
    let mut ranked_lists: Vec<Vec<(String, f64)>> = Vec::with_capacity(targets.len());
    let mut hit_meta: std::collections::HashMap<String, (String, String)> =
        std::collections::HashMap::new();
    let mut succeeded = 0usize;
    let mut last_error: Option<Status> = None;
    for index in &targets {
        let points = match svc
            .query_one_index(
                runtime,
                manifest,
                &context,
                index,
                &req,
                fetch_depth,
                tenant_filter.clone(),
            )
            .await
        {
            Ok(points) => points,
            Err(err) => {
                tracing::warn!(
                    index = %index.index_name,
                    error = %err,
                    "search: index query failed; skipping this index"
                );
                last_error = Some(err);
                continue;
            }
        };
        succeeded += 1;
        // Carry the engine's own relevance score so a single-index search keeps
        // it (see `fuse_ranked_lists`); cross-index fusion uses only the rank.
        let mut scored = Vec::with_capacity(points.len());
        for point in points {
            hit_meta.entry(point.id.clone()).or_insert_with(|| {
                (
                    index.index_name.clone(),
                    hit_payload_json(point.payload.as_ref()),
                )
            });
            scored.push((point.id, f64::from(point.score)));
        }
        ranked_lists.push(scored);
    }
    // Every targeted index errored → surface the failure rather than a silent
    // empty result.
    if succeeded == 0 {
        if let Some(err) = last_error {
            return Err(err);
        }
    }

    let fused = fuse_ranked_lists(&ranked_lists);
    let total = fused.len();
    let hits = fused
        .into_iter()
        .skip(page_window.offset)
        .take(page_window.limit)
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
    // Emit a next-page token only when a further page genuinely exists
    // (offset + limit < total fused results), never merely because the page is
    // full.
    let next_page_token =
        native_next_page_token_for_total(page_window.offset, page_window.limit, total as i64);

    Ok(Response::new(search_pb::SearchResponse {
        hits,
        message: "ok".to_string(),
        error: None,
        next_page_token,
    }))
}

/// Validate a `SearchRequest.mode` against the supplied inputs. `UNSPECIFIED`
/// infers from the inputs (legacy behavior); the explicit modes reject a request
/// whose inputs contradict them (e.g. `VECTOR` with no vector, `HYBRID` without
/// both). Kept pure so the contract is unit-tested without a runtime.
fn validate_search_mode(
    mode: search_pb::SearchMode,
    has_text: bool,
    has_vector: bool,
) -> Result<(), Status> {
    use search_pb::SearchMode;
    let violation = |message: &str| {
        search_field_violation(
            "mode",
            "must be consistent with query_text / query_vector",
            message.to_string(),
        )
    };
    match mode {
        SearchMode::Unspecified => Ok(()),
        SearchMode::Text if !has_text => Err(violation(
            "SEARCH_MODE_TEXT requires a non-empty query_text",
        )),
        SearchMode::Vector if !has_vector => Err(violation(
            "SEARCH_MODE_VECTOR requires a non-empty query_vector",
        )),
        SearchMode::Hybrid if !(has_text && has_vector) => Err(violation(
            "SEARCH_MODE_HYBRID requires both query_text and query_vector",
        )),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod search_mode_tests {
    use super::validate_search_mode;
    use crate::proto::udb::core::search::services::v1::SearchMode;

    #[test]
    fn unspecified_infers_from_inputs() {
        assert!(validate_search_mode(SearchMode::Unspecified, false, false).is_ok());
    }

    #[test]
    fn explicit_modes_reject_contradictory_inputs() {
        assert!(validate_search_mode(SearchMode::Vector, true, false).is_err());
        assert!(validate_search_mode(SearchMode::Text, false, true).is_err());
        assert!(validate_search_mode(SearchMode::Hybrid, true, false).is_err());
        assert!(validate_search_mode(SearchMode::Hybrid, false, true).is_err());
    }

    #[test]
    fn explicit_modes_accept_matching_inputs() {
        assert!(validate_search_mode(SearchMode::Vector, false, true).is_ok());
        assert!(validate_search_mode(SearchMode::Text, true, false).is_ok());
        assert!(validate_search_mode(SearchMode::Hybrid, true, true).is_ok());
    }
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
