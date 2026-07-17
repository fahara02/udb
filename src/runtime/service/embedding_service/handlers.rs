//! The six `EmbeddingService` RPC handlers (register/list/delete source,
//! backfill, report-embedding, retrieve) plus the per-request `Retrieve`
//! deadline/hit helpers, extracted from the trait impl as free
//! `pub(crate) async fn`s taking `svc` where the trait method took `&self`.
//! `mod.rs` delegates one line to each. The two `self`-using helpers
//! (`resolve_source_tenant_column`, `upsert_reported_embedding`) stay inherent
//! methods so their bodies are byte-identical. Bodies are verbatim — the same
//! cross-tenant guard, fail-closed tenant-column gate, per-tenant quota, dimension
//! truth gate, mediated dispatch, server-side tenant filter, and reindex-on-model-
//! change guard as the former god file.

use std::time::{Duration, Instant};

use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::embedding::services::v1 as embedding_pb;
use crate::proto::{VectorHybridSearchRequest, VectorSearchRequest, VectorSet};
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, native_next_page_token, native_offset_page_window,
    native_service_context, non_empty_json, validate_request_tenant,
};
use super::EmbeddingServiceImpl;
use super::config::{
    EMBEDDING_SOURCE_MSG, MAX_SOURCES_PER_TENANT, STATUS_ACTIVE, STATUS_DELETED,
    TOPIC_BACKFILL_REQUESTED, TOPIC_SOURCE_DELETED, TOPIC_SOURCE_REGISTERED, resolve_top_k,
    retrieve_fusion_weights, retrieve_score_threshold,
};
use super::errors::{
    embedding_field_violation, embedding_required_field, embedding_source_not_found_status,
    require_source_tenant_column, validate_register_source_required_fields,
    validate_report_embedding_required_fields, validate_reported_vector,
};
use super::model::{build_embedding_point, merge_retrieve_filter, stored_source_from_json};
use super::store::{active_sources_read, source_conflict, source_read_by_name, source_record};

impl EmbeddingServiceImpl {
    /// Resolve the SOURCE entity's tenant column through the project-active
    /// manifest using the SHARED catalog resolver (no duplicate). Returns the
    /// column name + the source CDC topic, or a fail-closed error when the source
    /// is unknown or not tenant-isolated. Mirrors `search_service`.
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
                embedding_field_violation(
                    "source_message_type",
                    "must be present in the active catalog manifest",
                    format!(
                        "source_message_type '{source_message_type}' is not present in the active catalog \
                         manifest"
                    ),
                )
            },
        )?;
        // SHARED resolver: same `util::resolve_tenant_column` family behind
        // `native_catalog::NativeModel::tenant_column`.
        let resolved = crate::runtime::postgres_helpers::tenant_column_ref(table)
            .map(|column| column.column_name.clone());
        let tenant_column = require_source_tenant_column(resolved, source_message_type)?;
        let cdc_topic = table.cdc_topic.clone();
        Ok((tenant_column, cdc_topic))
    }

    /// Upsert a reported embedding through the SHARED runtime vector seam — the
    /// exact path `asset_service::AssetServiceImpl::upsert_embedding` wraps
    /// ([`choose_instance_name_for_project`] + `vector_upsert_backend_target`) — so
    /// there is no second vector-upsert. The point is tenant-tagged; an empty tenant
    /// fails closed before any backend call (no fail-open).
    pub(crate) async fn upsert_reported_embedding(
        &self,
        project_id: &str,
        collection: &str,
        tenant_id: &str,
        row_pk: &str,
        vector: Vec<f32>,
        dims: i32,
    ) -> Result<(), Status> {
        // Dimension truth gate: empty vectors and a `dims` claim contradicting
        // the actual vector length are rejected before any backend call.
        validate_reported_vector(dims, &vector)?;
        let runtime = self.require_runtime()?;
        let dim = if dims > 0 { dims } else { vector.len() as i32 };
        // Fail closed + tenant-tag (mirrors search's `_tenant_id` write stamp).
        let point = build_embedding_point(row_pk, vector, tenant_id)?;
        let vector_instance = runtime
            .choose_instance_name_for_project("qdrant", true, project_id)
            .map(str::to_string)
            .unwrap_or_else(|| "default".to_string());
        runtime
            .vector_upsert_backend_target(
                Some(&vector_instance),
                project_id,
                collection,
                dim,
                vec![point],
            )
            .await
    }
}

/// Serialize a retrieved point's payload for a `RetrieveHit`: the internal
/// `_tenant_id` write-stamp is stripped (it only mirrors the verified claim the
/// caller already holds — never expose the isolation key), and the remaining
/// user payload is serialized as JSON. No payload, a non-object payload, or a
/// tag-only payload all yield an empty string. Pure — unit-tested.
pub(crate) fn retrieve_hit_payload_json(payload: Option<&prost_types::Struct>) -> String {
    let Some(payload) = payload else {
        return String::new();
    };
    let mut json = crate::runtime::executor_utils::struct_to_json(payload);
    let Some(object) = json.as_object_mut() else {
        return String::new();
    };
    object.remove("_tenant_id");
    if object.is_empty() {
        return String::new();
    }
    serde_json::Value::Object(std::mem::take(object)).to_string()
}

/// Parse a gRPC `grpc-timeout` header value (`<digits><unit>`, units
/// H/M/S/m/u/n) into a `Duration`. Returns `None` for a malformed value.
pub(crate) fn parse_grpc_timeout(value: &str) -> Option<Duration> {
    let value = value.trim();
    if value.len() < 2 {
        return None;
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    let amount: u64 = digits.parse().ok()?;
    let nanos = match unit {
        "H" => amount.checked_mul(3_600_000_000_000)?,
        "M" => amount.checked_mul(60_000_000_000)?,
        "S" => amount.checked_mul(1_000_000_000)?,
        "m" => amount.checked_mul(1_000_000)?,
        "u" => amount.checked_mul(1_000)?,
        "n" => amount,
        _ => return None,
    };
    Some(Duration::from_nanos(nanos))
}

/// Deadline gate for `Retrieve`. Given the optional absolute deadline (derived
/// from the request's gRPC timeout) and now, returns the remaining budget, or
/// `deadline_exceeded` when the deadline is already past. Pure — unit-tested.
pub(crate) fn remaining_before_deadline(
    deadline: Option<Instant>,
    now: Instant,
) -> Result<Option<Duration>, Status> {
    match deadline {
        None => Ok(None),
        Some(deadline) => match deadline.checked_duration_since(now) {
            Some(remaining) if !remaining.is_zero() => Ok(Some(remaining)),
            _ => Err(crate::runtime::executor_utils::deadline_exceeded_status(
                "embedding",
                "retrieve",
                crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                "retrieve deadline exceeded before semantic search dispatch",
            )),
        },
    }
}

pub(crate) async fn register_source(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::RegisterSourceRequest>,
) -> Result<Response<embedding_pb::RegisterSourceResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // Cross-tenant guard FIRST: the body tenant_id must match the verified
    // claim/header before any catalog/store access.
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let source_name = req.source_name.trim().to_string();
    let source_message_type = req.source_message_type.trim().to_string();
    let target_collection = req.target_collection.trim().to_string();
    validate_register_source_required_fields(&source_name, &source_message_type)?;
    if target_collection.is_empty() {
        return Err(embedding_required_field(
            "target_collection",
            "must be a non-empty target vector collection",
            "target_collection is required",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "embedding",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    // FAIL CLOSED: resolve the source table's tenant column via the shared
    // catalog resolver. No tenant column ⇒ no source registration.
    let (tenant_column, source_cdc_topic) =
        svc.resolve_source_tenant_column(&context.project_id, &source_message_type)?;

    // Per-tenant source quota (only a brand-new source counts).
    let existing = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            source_read_by_name(&tenant_id, &source_name),
        )
        .await?
        .first()
        .map(stored_source_from_json);
    if existing.is_none() {
        let active = runtime
            .native_entity_read_for_service(
                "embedding",
                &context,
                active_sources_read(&tenant_id, 0, (MAX_SOURCES_PER_TENANT as u32) + 1),
            )
            .await?;
        if active.len() >= MAX_SOURCES_PER_TENANT {
            return Err(crate::runtime::executor_utils::quota_refusal_status(
                "embedding",
                "tenant embedding-source quota",
                format!("tenant embedding-source quota exhausted ({MAX_SOURCES_PER_TENANT})"),
            ));
        }
    }

    let source_id = existing
        .as_ref()
        .map(|row| row.source_id.clone())
        .filter(|id| !id.trim().is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    // Persist the text-field list as a JSON array (trimmed, non-empty).
    let text_fields: Vec<String> = req
        .text_fields
        .iter()
        .map(|field| field.trim().to_string())
        .filter(|field| !field.is_empty())
        .collect();
    let text_fields_json = serde_json::to_string(&text_fields).unwrap_or_else(|_| "[]".to_string());
    let model_id = req.model_id.trim().to_string();
    let metadata_json = non_empty_json(&req.metadata_json);

    runtime
        .native_entity_write_for_service(
            "embedding",
            &context,
            EMBEDDING_SOURCE_MSG,
            source_record(
                &source_id,
                &tenant_id,
                &source_name,
                &source_message_type,
                &text_fields_json,
                &target_collection,
                &model_id,
                &tenant_column,
                &source_cdc_topic,
                STATUS_ACTIVE,
                &metadata_json,
            ),
            source_conflict(),
        )
        .await?;

    svc.emit_source_event(
        TOPIC_SOURCE_REGISTERED,
        &tenant_id,
        &context.project_id,
        &source_name,
        serde_json::json!({
            "source_message_type": source_message_type,
            "target_collection": target_collection,
            "model_id": model_id,
            "tenant_column": tenant_column,
        }),
    )
    .await;

    // Reindex-on-model-change guard (Part B.1.3): a re-register that switches
    // the model or the target collection would otherwise leave the collection
    // silently MIXING vectors from the old model/dims with newly-reported ones
    // — corrupting retrieval (different models embed into incomparable spaces).
    // Auto-enqueue a backfill using the SAME control event the Backfill RPC
    // emits (the leader work-emitter re-embeds every existing row under the new
    // binding). A brand-new source, or a re-register that changes neither the
    // model nor the collection, needs no reindex.
    if let Some(prev) = existing.as_ref() {
        let model_changed = prev.model_id.trim() != model_id;
        let collection_changed = prev.target_collection.trim() != target_collection;
        if model_changed || collection_changed {
            let reindex_id = Uuid::new_v4().to_string();
            svc.emit_source_event(
                TOPIC_BACKFILL_REQUESTED,
                &tenant_id,
                &context.project_id,
                &source_name,
                serde_json::json!({
                    "backfill_id": reindex_id,
                    "source_message_type": source_message_type,
                    "target_collection": target_collection,
                    "model_id": model_id,
                    "reason": "model_or_collection_changed",
                    "previous_model_id": prev.model_id,
                    "previous_collection": prev.target_collection,
                }),
            )
            .await;
        }
    }

    Ok(Response::new(embedding_pb::RegisterSourceResponse {
        source_id,
        source_name,
        tenant_column,
        message: "embedding source registered".to_string(),
        error: None,
    }))
}

pub(crate) async fn list_sources(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::ListSourcesRequest>,
) -> Result<Response<embedding_pb::ListSourcesResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    // READ_ONLY RPC ⇒ the shared Read admission lane (like sibling list
    // RPCs), not Admin — listing must never contend with control mutations.
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "embedding",
        OperationChannel::Read,
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
        MAX_SOURCES_PER_TENANT as i32,
    );

    let rows = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            active_sources_read(
                &tenant_id,
                page_window.offset as u64,
                (page_window.limit as u32).min(MAX_SOURCES_PER_TENANT as u32),
            ),
        )
        .await?;
    let sources = rows
        .iter()
        .map(stored_source_from_json)
        .map(|source| embedding_pb::EmbeddingSourceSummary {
            source_id: source.source_id,
            source_name: source.source_name,
            source_message_type: source.source_message_type,
            target_collection: source.target_collection,
            model_id: source.model_id,
            status: source.status,
        })
        .collect::<Vec<_>>();
    let next_page_token =
        native_next_page_token(page_window.offset, page_window.limit, sources.len());

    Ok(Response::new(embedding_pb::ListSourcesResponse {
        sources,
        message: "ok".to_string(),
        error: None,
        next_page_token,
    }))
}

pub(crate) async fn delete_source(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::DeleteSourceRequest>,
) -> Result<Response<embedding_pb::DeleteSourceResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let source_name = req.source_name.trim().to_string();
    if source_name.is_empty() {
        return Err(embedding_required_field(
            "source_name",
            "must be a non-empty embedding source name",
            "source_name is required",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "embedding",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let stored = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            source_read_by_name(&tenant_id, &source_name),
        )
        .await?
        .first()
        .map(stored_source_from_json);
    let Some(stored) = stored.filter(|row| row.status != STATUS_DELETED) else {
        // Idempotent: nothing to delete.
        return Ok(Response::new(embedding_pb::DeleteSourceResponse {
            deleted: true,
            message: "embedding source not found".to_string(),
            error: None,
        }));
    };

    runtime
        .native_entity_write_for_service(
            "embedding",
            &context,
            EMBEDDING_SOURCE_MSG,
            source_record(
                &stored.source_id,
                &tenant_id,
                &stored.source_name,
                &stored.source_message_type,
                &stored.text_fields_json,
                &stored.target_collection,
                &stored.model_id,
                &stored.tenant_column,
                &stored.source_cdc_topic,
                STATUS_DELETED,
                "{}",
            ),
            source_conflict(),
        )
        .await?;

    // Vector teardown runs on the leader work-emitter pass: it consumes this
    // source-deleted event from the durable journal, enumerates the source's
    // emitted work-event point ids, and deletes them per collection through
    // the shared vector seam, then marks the event done with a
    // `teardown.completed` marker (see `process_embedding_teardown_job`).
    svc.emit_source_event(
        TOPIC_SOURCE_DELETED,
        &tenant_id,
        &context.project_id,
        &source_name,
        serde_json::json!({ "target_collection": stored.target_collection }),
    )
    .await;

    Ok(Response::new(embedding_pb::DeleteSourceResponse {
        deleted: true,
        message: "embedding source deleted".to_string(),
        error: None,
    }))
}

pub(crate) async fn backfill(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::BackfillRequest>,
) -> Result<Response<embedding_pb::BackfillResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let source_name = req.source_name.trim().to_string();
    if source_name.is_empty() {
        return Err(embedding_required_field(
            "source_name",
            "must be a non-empty embedding source name",
            "source_name is required",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "embedding",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    let stored = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            source_read_by_name(&tenant_id, &source_name),
        )
        .await?
        .first()
        .map(stored_source_from_json)
        .filter(|source| source.status == STATUS_ACTIVE)
        .ok_or_else(|| embedding_source_not_found_status("backfill"))?;

    // Emit only the backfill-request control event from the admitting RPC. The
    // leader-spawned work emitter enumerates the source's existing rows through
    // the served DataBroker SELECT path and emits per-row work out-of-band.
    let backfill_id = Uuid::new_v4().to_string();
    svc.emit_source_event(
        TOPIC_BACKFILL_REQUESTED,
        &tenant_id,
        &context.project_id,
        &source_name,
        serde_json::json!({
            "backfill_id": backfill_id,
            "source_message_type": stored.source_message_type,
            "target_collection": stored.target_collection,
            "model_id": stored.model_id,
        }),
    )
    .await;

    Ok(Response::new(embedding_pb::BackfillResponse {
        backfill_id,
        accepted: true,
        message: "embedding backfill requested".to_string(),
        error: None,
    }))
}

pub(crate) async fn report_embedding(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::ReportEmbeddingRequest>,
) -> Result<Response<embedding_pb::ReportEmbeddingResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // Cross-tenant guard FIRST: the body tenant_id must match the VERIFIED
    // claim/header. After this passes the body value IS the verified tenant, so
    // the stored vector is tagged from the verified claim, never raw body.
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let source_name = req.source_name.trim().to_string();
    let row_pk = req.row_pk.trim().to_string();
    validate_report_embedding_required_fields(&source_name, &row_pk)?;
    // Dimension truth: empty vector / contradictory `dims` rejected up front.
    validate_reported_vector(req.dims, &req.vector)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "embedding",
        OperationChannel::Vector,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");

    // Resolve the source the sidecar reported against — bounded to THIS tenant
    // (the read filters on the verified tenant), so a foreign source cannot be
    // targeted. An unknown/deleted source is rejected (no blind upsert).
    let stored = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            source_read_by_name(&tenant_id, &source_name),
        )
        .await?
        .first()
        .map(stored_source_from_json)
        .filter(|source| source.status == STATUS_ACTIVE)
        .ok_or_else(|| embedding_source_not_found_status("report_embedding"))?;

    svc.upsert_reported_embedding(
        &context.project_id,
        &stored.collection(),
        &tenant_id,
        &row_pk,
        req.vector,
        req.dims,
    )
    .await?;

    Ok(Response::new(embedding_pb::ReportEmbeddingResponse {
        upserted: true,
        message: "embedding upserted".to_string(),
        error: None,
    }))
}

pub(crate) async fn retrieve(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::RetrieveRequest>,
) -> Result<Response<embedding_pb::RetrieveResponse>, Status> {
    let metadata = request.metadata().clone();
    // Derive the absolute deadline from the request's gRPC timeout BEFORE any
    // work, so the budget covers the whole semantic-search delegation.
    let deadline = metadata
        .get("grpc-timeout")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_grpc_timeout)
        .map(|budget| Instant::now() + budget);
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let source_name = req.source_name.trim().to_string();
    if source_name.is_empty() {
        return Err(embedding_required_field(
            "source_name",
            "must be a non-empty embedding source name",
            "source_name is required",
        ));
    }
    // The broker NEVER embeds the query: a Retrieve must carry an already-
    // embedded query vector (the only mediated semantic path in this build).
    if req.query_vector.is_empty() {
        return Err(embedding_required_field(
            "query_vector",
            "must contain at least one embedding dimension",
            "query_vector is required (the broker does not embed queries; supply a vector)",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "embedding",
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
    // Per-query minimum similarity: a positive request threshold RAISES the
    // server-side floor (never lowers it), so a caller can demand higher-precision
    // hits without being able to weaken the operator's minimum. Enforced uniformly
    // on the returned hits below (and pushed into the vector engine as an
    // optimization), so it applies to both the vector and hybrid paths.
    let score_floor = {
        let env_floor = retrieve_score_threshold();
        let requested = req.score_threshold as f32;
        if requested > 0.0 {
            requested.max(env_floor)
        } else {
            env_floor
        }
    };

    let stored = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            source_read_by_name(&tenant_id, &source_name),
        )
        .await?
        .first()
        .map(stored_source_from_json)
        .filter(|source| source.status == STATUS_ACTIVE)
        .ok_or_else(|| embedding_source_not_found_status("retrieve"))?;

    // SERVER-SIDE tenant filter built from the VERIFIED claim, injected into the
    // delegated engine query (the `_tenant_id` must-clause). Never from the body.
    // Any caller-supplied `filter_json` is merged UNDER this clause: the tenant
    // condition stays first and authoritative and internal `_`-prefixed keys are
    // rejected, so a caller filter can narrow but never broaden tenant scope.
    let merged_filter = merge_retrieve_filter(&tenant_id, &req.filter_json)?;
    let tenant_filter = crate::runtime::executor_utils::json_to_struct(&merged_filter);
    let state = catalog.active_for(&context.project_id);
    let manifest = &state.manifest;
    let collection = stored.collection();

    // Deadline gate before dispatch, then bound the delegated 9.5 hybrid search
    // by the remaining budget. `query_text` present ⇒ hybrid fusion; otherwise
    // a mediated vector search — never a raw engine query in either case.
    let remaining = remaining_before_deadline(deadline, Instant::now())?;
    let has_text = !req.query_text.trim().is_empty();
    let result: VectorSet = if has_text {
        let search = VectorHybridSearchRequest {
            context: None,
            collection,
            vector: req.query_vector.clone(),
            text_query: req.query_text.clone(),
            filter: tenant_filter,
            limit: top_k,
            fusion_weights: retrieve_fusion_weights(),
            // Hits carry the stored user payload (minus the internal
            // `_tenant_id` write-stamp, stripped in the hit mapping below).
            with_payload: true,
        };
        let fut = runtime.vector_hybrid_search(manifest, search, context.clone());
        match remaining {
            Some(budget) => tokio::time::timeout(budget, fut).await.map_err(|_| {
                crate::runtime::executor_utils::deadline_exceeded_status(
                    "embedding",
                    "retrieve_hybrid_search",
                    crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                    "retrieve exceeded its deadline during hybrid search",
                )
            })??,
            None => fut.await?,
        }
    } else {
        let search = VectorSearchRequest {
            context: None,
            collection,
            vector: req.query_vector.clone(),
            filter: tenant_filter,
            limit: top_k,
            score_threshold: score_floor,
            // Hits carry the stored user payload (minus the internal
            // `_tenant_id` write-stamp, stripped in the hit mapping below).
            with_payload: true,
        };
        let fut = runtime.vector_search(manifest, search, context.clone());
        match remaining {
            Some(budget) => tokio::time::timeout(budget, fut).await.map_err(|_| {
                crate::runtime::executor_utils::deadline_exceeded_status(
                    "embedding",
                    "retrieve_vector_search",
                    crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                    "retrieve exceeded its deadline during vector search",
                )
            })??,
            None => fut.await?,
        }
    };

    let hits = result
        .points
        .into_iter()
        // Enforce the (possibly per-request-raised) score floor uniformly — the
        // hybrid path does not push a threshold into the engine, so this is where
        // both paths honor it.
        .filter(|point| point.score >= score_floor)
        .take(top_k as usize)
        .map(|point| embedding_pb::RetrieveHit {
            payload_json: retrieve_hit_payload_json(point.payload.as_ref()),
            id: point.id,
            score: f64::from(point.score),
        })
        .collect::<Vec<_>>();

    Ok(Response::new(embedding_pb::RetrieveResponse {
        hits,
        message: "ok".to_string(),
        error: None,
    }))
}
