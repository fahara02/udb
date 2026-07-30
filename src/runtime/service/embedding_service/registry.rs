use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{ComparisonOp, LogicalDelete, LogicalFilter, LogicalValue};
use crate::proto::udb::core::embedding::services::v1 as embedding_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, native_next_page_token, native_offset_page_window,
    native_service_context, non_empty_json, validate_request_tenant,
};
use super::EmbeddingServiceImpl;
use super::config::{
    EMBEDDING_MODEL_MSG, MAX_MODELS_PER_TENANT, MAX_SOURCES_PER_TENANT, STATUS_ACTIVE,
    STATUS_DEPRECATED, STATUS_RETIRED, TOPIC_MODEL_ALIAS_CUTOVER, TOPIC_MODEL_REGISTERED,
    TOPIC_MODEL_STATUS_CHANGED, embedding_default_chunk_tokens,
    embedding_default_chunking_strategy, embedding_default_distance_metric,
    embedding_default_output_dtype, embedding_default_overlap_pct, embedding_default_task_type,
    embedding_default_vector_backend, select_matryoshka_dim,
};
use super::errors::{embedding_field_violation, embedding_required_field};
use super::model::{StoredModel, stored_model_from_json, stored_source_from_json};
use super::store::{
    active_sources_read, model_conflict, model_read_by_collection, model_read_by_id, model_record,
    models_read,
};
use super::vector_store::{RuntimeVectorStore, VectorStore};

fn model_status_name(value: i32) -> Result<&'static str, Status> {
    match value {
        1 => Ok(STATUS_ACTIVE),
        2 => Ok(STATUS_DEPRECATED),
        3 => Ok(STATUS_RETIRED),
        _ => Err(embedding_field_violation(
            "status",
            "must be ACTIVE, DEPRECATED, or RETIRED",
            "valid model status is required",
        )),
    }
}

fn model_status_value(value: &str) -> i32 {
    match value {
        STATUS_ACTIVE => 1,
        STATUS_DEPRECATED => 2,
        STATUS_RETIRED => 3,
        _ => 0,
    }
}

fn tenant_state_name(value: i32, current: &str) -> Result<String, Status> {
    match value {
        0 => Ok(current.to_string()),
        1 => Ok(STATUS_ACTIVE.to_string()),
        2 => Ok("INACTIVE".to_string()),
        3 => Ok("OFFLOADED".to_string()),
        _ => Err(embedding_field_violation(
            "tenant_state",
            "must be ACTIVE, INACTIVE, or OFFLOADED",
            "invalid embedding tenant lifecycle state",
        )),
    }
}

fn tenant_state_value(value: &str) -> i32 {
    match value {
        STATUS_ACTIVE => 1,
        "INACTIVE" => 2,
        "OFFLOADED" => 3,
        _ => 0,
    }
}

fn validate_model_request(req: &embedding_pb::RegisterModelRequest) -> Result<(), Status> {
    let mut missing = Vec::new();
    for (field, value) in [
        ("provider", req.provider.as_str()),
        ("model_name", req.model_name.as_str()),
        ("version", req.version.as_str()),
        ("tokenizer", req.tokenizer.as_str()),
        ("provider_endpoint_ref", req.provider_endpoint_ref.as_str()),
        ("active_collection", req.active_collection.as_str()),
    ] {
        if value.trim().is_empty() {
            missing.push((field, "must be non-empty"));
        }
    }
    if !missing.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "embedding model identity is incomplete",
            missing,
        ));
    }
    if req.dimensions <= 0 {
        return Err(embedding_field_violation(
            "dimensions",
            "must be greater than zero",
            "dimensions must be positive",
        ));
    }
    if req.max_input_tokens <= 0 {
        return Err(embedding_field_violation(
            "max_input_tokens",
            "must be greater than zero",
            "max_input_tokens must be positive",
        ));
    }
    let chunk_tokens = if req.chunk_tokens <= 0 {
        512.min(req.max_input_tokens)
    } else {
        req.chunk_tokens
    };
    let overlap = if req.chunk_overlap_tokens < 0 {
        0
    } else {
        req.chunk_overlap_tokens
    };
    if chunk_tokens > req.max_input_tokens || overlap >= chunk_tokens {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "model chunking exceeds its token envelope",
            [
                (
                    "chunk_tokens",
                    "must be positive and no greater than max_input_tokens",
                ),
                (
                    "chunk_overlap_tokens",
                    "must be non-negative and less than chunk_tokens",
                ),
            ],
        ));
    }
    if !req.provider_endpoint_ref.trim().starts_with("vault://") {
        return Err(embedding_field_violation(
            "provider_endpoint_ref",
            "must be a vault:// secret reference, never a credential or raw endpoint",
            "provider_endpoint_ref must use the vault:// scheme",
        ));
    }
    let metric = req.distance_metric.trim().to_ascii_uppercase();
    if !metric.is_empty() && !matches!(metric.as_str(), "COSINE" | "DOT" | "EUCLID") {
        return Err(embedding_field_violation(
            "distance_metric",
            "must be COSINE, DOT, or EUCLID",
            "unsupported distance metric",
        ));
    }
    let dtype = req.output_dtype.trim().to_ascii_uppercase();
    if !dtype.is_empty() && !matches!(dtype.as_str(), "FLOAT32" | "INT8" | "UINT8" | "BINARY") {
        return Err(embedding_field_violation(
            "output_dtype",
            "must be FLOAT32, INT8, UINT8, or BINARY",
            "unsupported output dtype",
        ));
    }
    if dtype == "BINARY" && !req.rescore {
        return Err(embedding_field_violation(
            "rescore",
            "must be enabled for binary embeddings",
            "binary embeddings require a rescore pass",
        ));
    }
    if req
        .matryoshka_dims
        .iter()
        .any(|dim| *dim <= 0 || *dim > req.dimensions)
    {
        return Err(embedding_field_violation(
            "matryoshka_dims",
            "every truncation must be positive and <= dimensions",
            "invalid Matryoshka truncation",
        ));
    }
    if !req.collection_alias.trim().is_empty()
        && req.collection_alias.trim() == req.active_collection.trim()
    {
        return Err(embedding_field_violation(
            "collection_alias",
            "must differ from the versioned active_collection",
            "collection alias cannot be the physical collection name",
        ));
    }
    Ok(())
}

fn vector_identity_matches(
    existing: &StoredModel,
    req: &embedding_pb::RegisterModelRequest,
) -> bool {
    let matryoshka_dims_json =
        serde_json::to_string(&req.matryoshka_dims).unwrap_or_else(|_| "[]".to_string());
    existing.provider == req.provider.trim()
        && existing.model_name == req.model_name.trim()
        && existing.version == req.version.trim()
        && existing.dimensions == req.dimensions
        && existing
            .distance_metric
            .eq_ignore_ascii_case(if req.distance_metric.trim().is_empty() {
                embedding_default_distance_metric()
            } else {
                req.distance_metric.trim()
            })
        && existing.normalize == req.normalize
        && existing
            .output_dtype
            .eq_ignore_ascii_case(if req.output_dtype.trim().is_empty() {
                embedding_default_output_dtype()
            } else {
                req.output_dtype.trim()
            })
        && existing.matryoshka_dims_json == matryoshka_dims_json
        && existing.rescore == req.rescore
        && existing
            .task_type
            .eq_ignore_ascii_case(if req.task_type.trim().is_empty() {
                embedding_default_task_type()
            } else {
                req.task_type.trim()
            })
        && existing.asymmetric == req.asymmetric
        && existing
            .vector_backend
            .eq_ignore_ascii_case(if req.vector_backend.trim().is_empty() {
                embedding_default_vector_backend()
            } else {
                req.vector_backend.trim()
            })
        && existing.vector_instance
            == if req.vector_instance.trim().is_empty() {
                "default"
            } else {
                req.vector_instance.trim()
            }
        && existing.active_collection == req.active_collection.trim()
}

pub(crate) async fn register_model(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::RegisterModelRequest>,
) -> Result<Response<embedding_pb::RegisterModelResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    validate_model_request(&req)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "embedding",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime_handle()?;
    let context = native_service_context(&metadata, &tenant_id, "");
    let model_id = if req.model_id.trim().is_empty() {
        Uuid::new_v4().to_string()
    } else {
        req.model_id.trim().to_string()
    };

    let existing = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            model_read_by_id(&tenant_id, &model_id),
        )
        .await?
        .first()
        .map(stored_model_from_json);
    if let Some(existing) = existing.as_ref()
        && !vector_identity_matches(existing, &req)
    {
        return Err(embedding_field_violation(
            "model_id",
            "an existing model id cannot change vector geometry; register a new model/version",
            "model vector identity is immutable",
        ));
    }
    if existing.is_none() {
        let models = runtime
            .native_entity_read_for_service(
                "embedding",
                &context,
                models_read(&tenant_id, None, 0, (MAX_MODELS_PER_TENANT + 1) as u32),
            )
            .await?;
        if models.len() >= MAX_MODELS_PER_TENANT {
            return Err(crate::runtime::executor_utils::quota_refusal_status(
                "embedding",
                "tenant embedding-model quota",
                format!("tenant embedding-model quota exhausted ({MAX_MODELS_PER_TENANT})"),
            ));
        }
    }
    let collection_rows = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            model_read_by_collection(&tenant_id, req.active_collection.trim()),
        )
        .await?;
    if collection_rows
        .iter()
        .map(stored_model_from_json)
        .any(|model| model.model_id != model_id)
    {
        return Err(embedding_field_violation(
            "active_collection",
            "a vector collection is bound to exactly one registered model identity",
            "active_collection is already bound to another model",
        ));
    }

    let matryoshka_dims_json =
        serde_json::to_string(&req.matryoshka_dims).unwrap_or_else(|_| "[]".to_string());
    let distance_metric = if req.distance_metric.trim().is_empty() {
        embedding_default_distance_metric()
    } else {
        req.distance_metric.trim()
    };
    let output_dtype = if req.output_dtype.trim().is_empty() {
        embedding_default_output_dtype()
    } else {
        req.output_dtype.trim()
    };
    let task_type = if req.task_type.trim().is_empty() {
        embedding_default_task_type()
    } else {
        req.task_type.trim()
    };
    let vector_backend = if req.vector_backend.trim().is_empty() {
        embedding_default_vector_backend()
    } else {
        req.vector_backend.trim()
    };
    let vector_instance = if req.vector_instance.trim().is_empty() {
        "default"
    } else {
        req.vector_instance.trim()
    };
    let collection_alias = if req.collection_alias.trim().is_empty() {
        format!(
            "{}-alias",
            req.active_collection
                .trim()
                .chars()
                .take(249)
                .collect::<String>()
        )
    } else {
        req.collection_alias.trim().to_string()
    };
    let chunking_strategy = if req.chunking_strategy.trim().is_empty() {
        embedding_default_chunking_strategy()
    } else {
        req.chunking_strategy.trim()
    };
    let chunk_tokens = if req.chunk_tokens <= 0 {
        embedding_default_chunk_tokens().min(req.max_input_tokens)
    } else {
        req.chunk_tokens
    };
    let chunk_overlap_tokens = if req.chunk_overlap_tokens < 0 {
        0
    } else if req.chunk_overlap_tokens == 0 {
        chunk_tokens.saturating_mul(embedding_default_overlap_pct()) / 100
    } else {
        req.chunk_overlap_tokens
    };
    // Part B.2.3 Matryoshka: create the physical collection at the SERVING dim
    // (the chosen truncation when `matryoshka_dims` is configured, else full
    // `dimensions`) so its geometry matches the truncated vectors ReportEmbedding
    // stores and the truncated query Retrieve issues.
    let serving_dim = select_matryoshka_dim(
        req.dimensions,
        &matryoshka_dims_json,
        super::config::embedding_matryoshka_strategy(),
    );
    let vector_store = RuntimeVectorStore::for_routing(
        runtime.clone(),
        &context.project_id,
        vector_backend,
        vector_instance,
    );
    vector_store
        .ensure_collection(
            req.active_collection.trim(),
            serving_dim,
            distance_metric,
            output_dtype,
            &[],
        )
        .await?;
    // The model advertises collection() = the ALIAS (see StoredModel::collection),
    // and the whole read path (Retrieve/search) queries that alias. Point it at
    // the physical collection NOW, so a freshly registered model is immediately
    // retrievable — otherwise the alias only appears on a later activation/cutover
    // and every Retrieve 404s until then (a divergence that only shows up against
    // a fresh vector store, e.g. CI). Idempotent: creates the alias when absent.
    // Qdrant and Elasticsearch expose a named-alias primitive; pre-create the
    // alias so a freshly registered model is retrievable immediately (portable
    // `swap_alias` shapes the ES `_aliases` action). Pinecone/Weaviate have no
    // alias concept, so there is nothing to pre-create — skip it there.
    if matches!(vector_backend, "qdrant" | "elasticsearch") {
        vector_store
            .swap_alias(&collection_alias, req.active_collection.trim())
            .await?;
    }
    runtime
        .native_entity_write_for_service(
            "embedding",
            &context,
            EMBEDDING_MODEL_MSG,
            model_record(
                &model_id,
                &tenant_id,
                req.provider.trim(),
                req.model_name.trim(),
                req.version.trim(),
                req.dimensions,
                &matryoshka_dims_json,
                distance_metric,
                req.normalize,
                output_dtype,
                req.rescore,
                req.max_input_tokens,
                req.tokenizer.trim(),
                task_type,
                req.asymmetric,
                req.provider_endpoint_ref.trim(),
                STATUS_ACTIVE,
                "",
                vector_backend,
                vector_instance,
                &collection_alias,
                req.active_collection.trim(),
                chunking_strategy,
                chunk_tokens,
                chunk_overlap_tokens,
                req.contextual_retrieval,
                req.late_chunking,
                STATUS_ACTIVE,
                0,
                &non_empty_json(&req.metadata_json),
            ),
            model_conflict(),
        )
        .await?;
    svc.emit_source_event(
        TOPIC_MODEL_REGISTERED, &tenant_id, &context.project_id, &model_id,
        serde_json::json!({"model_id": model_id, "provider": req.provider, "model_name": req.model_name,
            "version": req.version, "dimensions": req.dimensions, "active_collection": req.active_collection}),
    ).await;
    Ok(Response::new(embedding_pb::RegisterModelResponse {
        model_id,
        active_collection: req.active_collection,
        message: "embedding model registered".to_string(),
        error: None,
    }))
}

pub(crate) async fn list_models(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::ListModelsRequest>,
) -> Result<Response<embedding_pb::ListModelsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "embedding",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let context = native_service_context(&metadata, &tenant_id, "");
    let page = native_offset_page_window(
        1,
        req.page_size,
        &req.page_token,
        MAX_MODELS_PER_TENANT as i32,
    );
    let status = match req.status {
        0 => None,
        value => Some(model_status_name(value)?),
    };
    let rows = svc
        .require_runtime()?
        .native_entity_read_for_service(
            "embedding",
            &context,
            models_read(&tenant_id, status, page.offset as u64, page.limit as u32),
        )
        .await?;
    let models = rows
        .iter()
        .map(stored_model_from_json)
        .map(|model| embedding_pb::EmbeddingModelSummary {
            model_id: model.model_id,
            provider: model.provider,
            model_name: model.model_name,
            version: model.version,
            dimensions: model.dimensions,
            distance_metric: model.distance_metric,
            output_dtype: model.output_dtype,
            task_type: model.task_type,
            status: model_status_value(&model.status),
            vector_backend: model.vector_backend,
            collection_alias: model.collection_alias,
            active_collection: model.active_collection,
            tenant_state: tenant_state_value(&model.tenant_state),
        })
        .collect::<Vec<_>>();
    let next_page_token = native_next_page_token(page.offset, page.limit, models.len());
    Ok(Response::new(embedding_pb::ListModelsResponse {
        models,
        next_page_token,
        message: "ok".to_string(),
        error: None,
    }))
}

pub(crate) async fn delete_model(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::DeleteModelRequest>,
) -> Result<Response<embedding_pb::DeleteModelResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let model_id = req.model_id.trim().to_string();
    if model_id.is_empty() {
        return Err(embedding_required_field(
            "model_id",
            "must be non-empty",
            "model_id is required",
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
    if runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            model_read_by_id(&tenant_id, &model_id),
        )
        .await?
        .is_empty()
    {
        return Ok(Response::new(embedding_pb::DeleteModelResponse {
            deleted: true,
            message: "embedding model not found".to_string(),
            error: None,
        }));
    }
    let sources = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            active_sources_read(&tenant_id, 0, (MAX_SOURCES_PER_TENANT + 1) as u32),
        )
        .await?;
    if sources
        .iter()
        .map(stored_source_from_json)
        .any(|source| source.model_id == model_id)
    {
        return Err(embedding_field_violation(
            "model_id",
            "must not be referenced by an active embedding source",
            "retire or rebind sources before deleting the model",
        ));
    }
    runtime
        .native_entity_delete_for_service(
            "embedding",
            &context,
            LogicalDelete {
                message_type: EMBEDDING_MODEL_MSG.to_string(),
                filter: LogicalFilter::And(vec![
                    LogicalFilter::Comparison {
                        field: "tenant_id".to_string(),
                        op: ComparisonOp::Eq,
                        value: LogicalValue::String(tenant_id.clone()),
                    },
                    LogicalFilter::Comparison {
                        field: "model_id".to_string(),
                        op: ComparisonOp::Eq,
                        value: LogicalValue::String(model_id),
                    },
                ]),
                return_fields: Vec::new(),
            },
        )
        .await?;
    Ok(Response::new(embedding_pb::DeleteModelResponse {
        deleted: true,
        message: "embedding model deleted".to_string(),
        error: None,
    }))
}

pub(crate) async fn set_model_status(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::SetModelStatusRequest>,
) -> Result<Response<embedding_pb::SetModelStatusResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let status = model_status_name(req.status)?;
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
    let model = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            model_read_by_id(&tenant_id, req.model_id.trim()),
        )
        .await?
        .first()
        .map(stored_model_from_json)
        .ok_or_else(|| {
            embedding_field_violation(
                "model_id",
                "must identify a registered tenant model",
                "embedding model not found",
            )
        })?;
    let tenant_state = tenant_state_name(req.tenant_state, &model.tenant_state)?;
    if status == STATUS_RETIRED && req.replacement_model_id.trim().is_empty() {
        return Err(embedding_field_violation(
            "replacement_model_id",
            "is required before retirement",
            "retired models require a replacement",
        ));
    }
    if !req.replacement_model_id.trim().is_empty()
        && runtime
            .native_entity_read_for_service(
                "embedding",
                &context,
                model_read_by_id(&tenant_id, req.replacement_model_id.trim()),
            )
            .await?
            .is_empty()
    {
        return Err(embedding_field_violation(
            "replacement_model_id",
            "must identify a registered tenant model",
            "replacement model not found",
        ));
    }
    let replacement_model_id = if req.replacement_model_id.trim().is_empty() {
        model.replacement_model_id.as_str()
    } else {
        req.replacement_model_id.trim()
    };
    let retire_after_unix_ms = if req.retire_after_unix_ms > 0 {
        req.retire_after_unix_ms
    } else {
        model.retire_after_unix_ms
    };
    runtime
        .native_entity_write_for_service(
            "embedding",
            &context,
            EMBEDDING_MODEL_MSG,
            model_record(
                &model.model_id,
                &tenant_id,
                &model.provider,
                &model.model_name,
                &model.version,
                model.dimensions,
                &model.matryoshka_dims_json,
                &model.distance_metric,
                model.normalize,
                &model.output_dtype,
                model.rescore,
                model.max_input_tokens,
                &model.tokenizer,
                &model.task_type,
                model.asymmetric,
                &model.provider_endpoint_ref,
                status,
                replacement_model_id,
                &model.vector_backend,
                &model.vector_instance,
                &model.collection_alias,
                &model.active_collection,
                &model.chunking_strategy,
                model.chunk_tokens,
                model.chunk_overlap_tokens,
                model.contextual_retrieval,
                model.late_chunking,
                &tenant_state,
                retire_after_unix_ms,
                &model.metadata_json,
            ),
            model_conflict(),
        )
        .await?;
    svc.emit_source_event(TOPIC_MODEL_STATUS_CHANGED, &tenant_id, &context.project_id, &model.model_id,
        serde_json::json!({"model_id": model.model_id, "status": status, "tenant_state": tenant_state, "replacement_model_id": replacement_model_id, "retire_after_unix_ms": retire_after_unix_ms})).await;
    Ok(Response::new(embedding_pb::SetModelStatusResponse {
        updated: true,
        message: "embedding model status updated".to_string(),
        error: None,
        tenant_state: tenant_state_value(&tenant_state),
    }))
}

pub(crate) async fn cutover_model_alias(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::CutoverModelAliasRequest>,
) -> Result<Response<embedding_pb::CutoverModelAliasResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "embedding",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let context = native_service_context(&metadata, &tenant_id, "");
    let model = svc
        .require_runtime()?
        .native_entity_read_for_service(
            "embedding",
            &context,
            model_read_by_id(&tenant_id, req.model_id.trim()),
        )
        .await?
        .first()
        .map(stored_model_from_json)
        .ok_or_else(|| {
            embedding_field_violation(
                "model_id",
                "must identify a registered tenant model",
                "embedding model not found",
            )
        })?;
    if model.status != STATUS_ACTIVE {
        return Err(embedding_field_violation(
            "model_id",
            "must be ACTIVE for alias cutover",
            "cannot cut over a non-active model",
        ));
    }
    if !req.expected_collection.trim().is_empty()
        && req.expected_collection.trim() != model.active_collection
    {
        return Err(embedding_field_violation(
            "expected_collection",
            "must match the model active collection",
            "model collection changed before cutover",
        ));
    }
    svc.swap_model_collection_alias(&context.project_id, &model)
        .await?;
    svc.emit_source_event(TOPIC_MODEL_ALIAS_CUTOVER, &tenant_id, &context.project_id, &model.model_id,
        serde_json::json!({"model_id": model.model_id, "collection_alias": model.collection_alias, "active_collection": model.active_collection})).await;
    Ok(Response::new(embedding_pb::CutoverModelAliasResponse {
        cutover: true,
        collection_alias: model.collection_alias,
        active_collection: model.active_collection,
        message: "embedding model alias cut over".to_string(),
        error: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_chunk_size_clamps_to_small_model_context() {
        let request = embedding_pb::RegisterModelRequest {
            provider: "deterministic".to_string(),
            model_name: "fixture".to_string(),
            version: "1".to_string(),
            dimensions: 3,
            max_input_tokens: 128,
            tokenizer: "whitespace".to_string(),
            provider_endpoint_ref: "vault://embedding/fixture".to_string(),
            active_collection: "fixture-v1".to_string(),
            ..Default::default()
        };

        assert!(validate_model_request(&request).is_ok());
    }
}
