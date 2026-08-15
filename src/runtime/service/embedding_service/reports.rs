use tonic::{Request, Response, Status};

use crate::proto::VectorPoint;
use crate::proto::udb::core::embedding::services::v1 as embedding_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, project_scoped_native_service_context, validate_request_tenant,
};
use super::EmbeddingServiceImpl;
use super::config::{MAX_EMBEDDING_REPORT_BATCH, STATUS_ACTIVE, TOPIC_METERED};
use super::errors::{
    embedding_field_violation, embedding_required_field, embedding_source_not_found_status,
    validate_report_embedding_required_fields, validate_reported_vector,
};
use super::model::{
    EmbeddingPointMetadata, build_embedding_point_with_metadata, stored_model_from_json,
    stored_source_from_json, truncate_embedding_vector,
};
use super::queue::{AckMeteringEvent, DurableWorkItem, ack_work_item, fail_work_item};
use super::store::{model_read_by_id, source_read_by_name, work_item_read_by_id};
use super::vector_store::VectorStore as _;

/// Per-batch memo of the (tenant, source_name) → source and
/// (tenant, model_id) → model reads, so a report batch resolves each distinct
/// source/model ONCE instead of once per item (a 256-item batch for one source
/// was ~3 mediated reads per item). Caches the post-filter result — including
/// a negative (`None`) — so per-item semantics are unchanged within a batch.
#[derive(Default)]
struct ReportBatchCache {
    sources: std::collections::HashMap<(String, String), Option<super::model::StoredSource>>,
    models: std::collections::HashMap<(String, String), Option<super::model::StoredModel>>,
}

async fn process_report(
    svc: &EmbeddingServiceImpl,
    metadata: &tonic::metadata::MetadataMap,
    req: embedding_pb::ReportEmbeddingRequest,
    mut cache: Option<&mut ReportBatchCache>,
) -> Result<(), Status> {
    validate_request_tenant(metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let source_name = req.source_name.trim().to_string();
    let row_pk = req.row_pk.trim().to_string();
    validate_report_embedding_required_fields(&source_name, &row_pk)?;
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
    let context = project_scoped_native_service_context(metadata, &tenant_id);
    let work = if req.work_item_id.trim().is_empty() {
        None
    } else {
        Some(
            runtime
                .native_entity_read_for_service(
                    "embedding",
                    &context,
                    work_item_read_by_id(&tenant_id, req.work_item_id.trim()),
                )
                .await?
                .first()
                .map(DurableWorkItem::from_native_json)
                .ok_or_else(|| {
                    embedding_field_violation(
                        "work_item_id",
                        "must identify durable pending work",
                        "embedding work item not found",
                    )
                })?,
        )
    };
    let source_key = (tenant_id.clone(), source_name.clone());
    let source = match cache
        .as_mut()
        .and_then(|cache| cache.sources.get(&source_key).cloned())
    {
        Some(cached) => cached,
        None => {
            let loaded = runtime
                .native_entity_read_for_service(
                    "embedding",
                    &context,
                    source_read_by_name(&tenant_id, &source_name),
                )
                .await?
                .first()
                .map(stored_source_from_json)
                .filter(|source| source.status == STATUS_ACTIVE);
            if let Some(cache) = cache.as_mut() {
                cache.sources.insert(source_key, loaded.clone());
            }
            loaded
        }
    };
    if source.is_none()
        && !work
            .as_ref()
            .is_some_and(|work| !work.document_id.is_empty())
    {
        return Err(embedding_source_not_found_status("report_embedding"));
    }
    let bound_model_id = source.as_ref().map_or_else(
        || {
            work.as_ref()
                .map_or_else(String::new, |work| work.model_id.clone())
        },
        |source| source.model_id.clone(),
    );
    let model_key = (tenant_id.clone(), bound_model_id.clone());
    let model = match cache
        .as_mut()
        .and_then(|cache| cache.models.get(&model_key).cloned())
    {
        Some(cached) => cached,
        None => {
            let loaded = runtime
                .native_entity_read_for_service(
                    "embedding",
                    &context,
                    model_read_by_id(&tenant_id, &bound_model_id),
                )
                .await?
                .first()
                .map(stored_model_from_json)
                .filter(|model| {
                    model.status == STATUS_ACTIVE && model.tenant_state == STATUS_ACTIVE
                });
            if let Some(cache) = cache.as_mut() {
                cache.models.insert(model_key, loaded.clone());
            }
            loaded
        }
    }
    .ok_or_else(|| {
        embedding_field_violation(
            "model",
            "must identify the active model bound to the source",
            "source embedding model is unavailable",
        )
    })?;
    if req.model.trim() != model.model_id {
        return Err(embedding_field_violation(
            "model",
            "must exactly match the source's registered model id",
            "reported model does not match source binding",
        ));
    }
    if req.vector.len() != model.dimensions as usize
        || (req.dims > 0 && req.dims != model.dimensions)
    {
        return Err(embedding_field_violation(
            "vector",
            format!(
                "must contain exactly {} dimensions registered for model {}",
                model.dimensions, model.model_id
            ),
            format!(
                "reported vector has {} dimensions; model requires {}",
                req.vector.len(),
                model.dimensions
            ),
        ));
    }
    if let Some(source) = source.as_ref() {
        if source.target_collection != model.active_collection
            && source.target_collection != model.collection_alias
        {
            return Err(embedding_field_violation(
                "source_name",
                "source collection must remain bound to its registered model collection",
                "source/model collection binding is inconsistent",
            ));
        }
    }
    if let Some(work) = work.as_ref() {
        if work.source_name != source_name
            || work.point_id != row_pk
            || work.model_id != model.model_id
        {
            return Err(embedding_field_violation(
                "work_item_id",
                "must match the reported source, point, and model",
                "embedding work item identity mismatch",
            ));
        }
        if !req.chunk_hash.trim().is_empty() && req.chunk_hash.trim() != work.chunk_hash {
            return Err(embedding_field_violation(
                "chunk_hash",
                "must match the durable work content hash",
                "reported chunk hash mismatch",
            ));
        }
    }
    let metadata = work.as_ref().map_or(
        EmbeddingPointMetadata {
            chunk_hash: req.chunk_hash.trim(),
            chunk_text: "",
            document_id: "",
            doc_version: "",
            model_id: &model.model_id,
            vector_name: req.vector_name.trim(),
        },
        |work| EmbeddingPointMetadata {
            chunk_hash: &work.chunk_hash,
            chunk_text: &work.chunk_text,
            document_id: &work.document_id,
            doc_version: &work.doc_version,
            model_id: &model.model_id,
            vector_name: req.vector_name.trim(),
        },
    );
    // Part B.2.3 Matryoshka: the sidecar reports the FULL embedding (validated at
    // `model.dimensions` above); the server stores the truncated SERVING-dim
    // prefix so the point matches the collection geometry created at register
    // time. A non-Matryoshka model has serving_dim == dimensions ⇒ no truncation.
    let serving_dim = model.serving_dim();
    let stored_vector = truncate_embedding_vector(req.vector, serving_dim);
    let point = build_embedding_point_with_metadata(
        &row_pk,
        stored_vector,
        &tenant_id,
        &source_name,
        metadata,
    )?;
    let fresh_point = VectorPoint {
        id: point.id.clone(),
        score: 0.0,
        payload: point.payload.clone(),
        vector: point.vector.clone(),
        vector_name: point.vector_name.clone(),
    };
    svc.vector_store_for_model(&context.project_id, &model)?
        .upsert(
            &model.active_collection,
            serving_dim,
            &model.distance_metric,
            &model.output_dtype,
            vec![point],
        )
        .await?;
    svc.fresh_vectors
        .insert(&tenant_id, &source_name, &model.model_id, fresh_point);
    // Metering is ACCOUNTING, not telemetry — it must never silently drop.
    // Same payload shape `emit_source_event` produced (base tenant/project/
    // source fields + the metering body), so downstream billing consumers see
    // an unchanged event.
    let metering_payload = serde_json::json!({
        "tenant_id": tenant_id, "project_id": context.project_id, "source": source_name,
        "model_id": model.model_id, "collection": model.active_collection,
        "vectors_upserted": 1, "tokens": req.token_count.max(0), "work_item_id": req.work_item_id,
    });
    if let (Some(pool), Some(work)) = (svc.pg_pool.as_ref(), work.as_ref()) {
        // Exactly-once metering: the outbox row commits in the SAME transaction
        // as the fresh ACK (see `AckMeteringEvent`) — an insert failure rolls
        // the ack back for a clean retry, and a replayed report (already ACKED)
        // skips both, so billing can neither under- nor double-count.
        let freshly_acked = ack_work_item(
            pool,
            &tenant_id,
            &work.work_item_id,
            req.token_count.max(work.token_count),
            Some(AckMeteringEvent {
                outbox_relation: svc.outbox_relation.clone(),
                topic: TOPIC_METERED,
                project_id: context.project_id.clone(),
                source_name: source_name.clone(),
                payload: metering_payload,
            }),
        )
        .await?;
        if freshly_acked {
            svc.metrics.inc_embedding_work("acked");
        }
    } else if let Some(pool) = svc.pg_pool.as_ref() {
        // Workless ad-hoc report: still strict (at-least-once) — a metering
        // enqueue failure fails the report so the sidecar retries, instead of
        // silently under-billing. The vector upsert above is idempotent by
        // point id, so a retry is safe.
        crate::runtime::service::native_helpers::enqueue_outbox_event_in_tx(
            pool,
            svc.outbox_relation.as_deref(),
            TOPIC_METERED,
            &source_name,
            &tenant_id,
            &context.project_id,
            metering_payload,
            crate::runtime::service::native_helpers::NativeEventContext {
                target_resource: source_name.clone(),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| {
            super::errors::embedding_policy_status_with_code(
                tonic::Code::Internal,
                "embedding_metering",
                "embedding_metering_enqueue",
                format!("metering enqueue failed: {error}"),
            )
        })?;
    }
    Ok(())
}

pub(crate) async fn report_embedding(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::ReportEmbeddingRequest>,
) -> Result<Response<embedding_pb::ReportEmbeddingResponse>, Status> {
    let metadata = request.metadata().clone();
    process_report(svc, &metadata, request.into_inner(), None).await?;
    Ok(Response::new(embedding_pb::ReportEmbeddingResponse {
        upserted: true,
        message: "embedding upserted and acknowledged".to_string(),
        error: None,
    }))
}

pub(crate) async fn report_embedding_batch(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::ReportEmbeddingBatchRequest>,
) -> Result<Response<embedding_pb::ReportEmbeddingBatchResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    if req.items.is_empty() {
        return Err(embedding_required_field(
            "items",
            "must contain at least one embedding",
            "items are required",
        ));
    }
    if req.items.len() > MAX_EMBEDDING_REPORT_BATCH {
        return Err(embedding_field_violation(
            "items",
            format!("must contain at most {MAX_EMBEDDING_REPORT_BATCH} embeddings"),
            "embedding report batch is too large",
        ));
    }
    if req.declared_capacity <= 0 || req.items.len() > req.declared_capacity as usize {
        return Err(embedding_field_violation(
            "declared_capacity",
            "must be positive and no smaller than the submitted batch",
            "embedding batch exceeds sidecar declared capacity",
        ));
    }
    let mut results = Vec::with_capacity(req.items.len());
    let mut upserted = 0i32;
    let mut failed = 0i32;
    // One source/model resolution per distinct key across the whole batch.
    let mut batch_cache = ReportBatchCache::default();
    for mut item in req.items {
        if item.tenant_id.trim().is_empty() {
            item.tenant_id = req.tenant_id.clone();
        }
        let work_item_id = item.work_item_id.clone();
        let row_pk = item.row_pk.clone();
        match process_report(svc, &metadata, item, Some(&mut batch_cache)).await {
            Ok(()) => {
                upserted += 1;
                results.push(embedding_pb::ReportEmbeddingBatchItemResult {
                    work_item_id,
                    row_pk,
                    upserted: true,
                    error: String::new(),
                });
            }
            Err(error) => {
                failed += 1;
                results.push(embedding_pb::ReportEmbeddingBatchItemResult {
                    work_item_id,
                    row_pk,
                    upserted: false,
                    error: error.message().to_string(),
                });
            }
        }
    }
    Ok(Response::new(embedding_pb::ReportEmbeddingBatchResponse {
        results,
        upserted,
        failed,
        message: if failed == 0 {
            "embedding batch stored"
        } else {
            "embedding batch completed with failures"
        }
        .to_string(),
        error: None,
    }))
}

pub(crate) async fn report_embedding_failure(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::ReportEmbeddingFailureRequest>,
) -> Result<Response<embedding_pb::ReportEmbeddingFailureResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    if req.work_item_id.trim().is_empty() {
        return Err(embedding_required_field(
            "work_item_id",
            "must identify durable work",
            "work_item_id is required",
        ));
    }
    let pool = svc.pg_pool.as_ref().ok_or_else(|| {
        super::errors::embedding_capability_status(
            "report_embedding_failure",
            "postgres",
            "embedding failure tracking requires Postgres",
        )
    })?;
    let context = project_scoped_native_service_context(&metadata, req.tenant_id.trim());
    let result = fail_work_item(
        pool,
        svc.outbox_relation.as_deref(),
        req.tenant_id.trim(),
        &context.project_id,
        req.work_item_id.trim(),
        req.error.trim(),
        req.retryable,
        req.error_code.trim(),
    )
    .await?;
    svc.metrics
        .inc_embedding_work(if result.dead { "dead" } else { "retry" });
    Ok(Response::new(
        embedding_pb::ReportEmbeddingFailureResponse {
            recorded: true,
            dead_lettered: result.dead,
            attempt_count: result.attempt_count,
            message: if result.dead {
                "embedding work dead-lettered"
            } else {
                "embedding work scheduled for retry"
            }
            .to_string(),
            error: None,
        },
    ))
}
