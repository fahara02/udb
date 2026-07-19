use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::embedding::services::v1 as embedding_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, native_service_context, non_empty_json, validate_request_tenant,
};
use super::EmbeddingServiceImpl;
use super::chunking::{chunk_content_hash, chunk_source_text_for_model};
use super::config::{
    EMBEDDING_DOCUMENT_MSG, EMBEDDING_JOB_MSG, JOB_PENDING, MAX_DOCUMENT_INGEST_BATCH,
    STATUS_ACTIVE, TOPIC_DOCUMENT_INGESTED, TOPIC_DOCUMENT_PARSE,
};
use super::errors::{embedding_field_violation, embedding_required_field};
use super::model::{json_str, native_json_object, stored_model_from_json};
use super::queue::{WorkBatch, complete_job_enumeration, update_job_emission};
use super::store::{
    document_conflict, document_read_by_external, document_read_by_id, document_record,
    job_conflict, job_read_by_id, job_record, model_read_by_id,
};

pub(crate) fn document_source_name(model_id: &str) -> String {
    format!("documents:{model_id}")
}

fn validate_ingest(req: &embedding_pb::IngestDocumentRequest) -> Result<(), Status> {
    if req.external_id.trim().is_empty() {
        return Err(embedding_required_field(
            "external_id",
            "must be a stable caller document id",
            "external_id is required",
        ));
    }
    if req.model_id.trim().is_empty() {
        return Err(embedding_required_field(
            "model_id",
            "must identify a registered model",
            "model_id is required",
        ));
    }
    let has_text = !req.raw_text.trim().is_empty();
    let has_object = !req.storage_object_ref.trim().is_empty();
    if has_text == has_object {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "exactly one document input is required",
            [
                (
                    "raw_text",
                    "provide raw_text or storage_object_ref, but not both",
                ),
                (
                    "storage_object_ref",
                    "provide storage_object_ref or raw_text, but not both",
                ),
            ],
        ));
    }
    if has_object && !req.storage_object_ref.trim().starts_with("udb://") {
        return Err(embedding_field_violation(
            "storage_object_ref",
            "must be a tenant-scoped udb:// storage object reference",
            "unsupported storage object reference",
        ));
    }
    Ok(())
}

async fn ingest_one(
    svc: &EmbeddingServiceImpl,
    metadata: &tonic::metadata::MetadataMap,
    req: embedding_pb::IngestDocumentRequest,
) -> Result<embedding_pb::IngestDocumentResponse, Status> {
    validate_request_tenant(metadata, &req.tenant_id)?;
    validate_ingest(&req)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let runtime = svc.require_runtime()?;
    let context = native_service_context(metadata, &tenant_id, "");
    let model = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            model_read_by_id(&tenant_id, req.model_id.trim()),
        )
        .await?
        .first()
        .map(stored_model_from_json)
        .filter(|model| model.status == STATUS_ACTIVE && model.tenant_state == STATUS_ACTIVE)
        .ok_or_else(|| {
            embedding_field_violation(
                "model_id",
                "must identify an ACTIVE tenant model",
                "embedding model not found or inactive",
            )
        })?;
    let existing_id = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            document_read_by_external(&tenant_id, req.external_id.trim()),
        )
        .await?
        .first()
        .map(|row| json_str(native_json_object(row), "document_id"));
    let document_id = existing_id
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let job_id = Uuid::new_v4().to_string();
    let doc_version = if req.doc_version.trim().is_empty() {
        "1".to_string()
    } else {
        req.doc_version.trim().to_string()
    };
    let source_name = document_source_name(&model.model_id);
    let document_status = if req.raw_text.trim().is_empty() {
        "PARSING"
    } else {
        "EMBEDDING"
    };
    runtime
        .native_entity_write_for_service(
            "embedding",
            &context,
            EMBEDDING_DOCUMENT_MSG,
            document_record(
                &document_id,
                &tenant_id,
                &context.project_id,
                req.external_id.trim(),
                req.title.trim(),
                req.raw_text.trim(),
                req.storage_object_ref.trim(),
                req.content_type.trim(),
                &doc_version,
                &model.model_id,
                &model.active_collection,
                document_status,
                &non_empty_json(&req.metadata_json),
            ),
            document_conflict(),
        )
        .await?;
    runtime
        .native_entity_write_for_service(
            "embedding",
            &context,
            EMBEDDING_JOB_MSG,
            job_record(
                &job_id,
                &tenant_id,
                &context.project_id,
                &source_name,
                &document_id,
                "DOCUMENT_INGEST",
                "INCREMENTAL",
                JOB_PENDING,
            ),
            job_conflict(),
        )
        .await?;

    if !req.raw_text.trim().is_empty() {
        let chunks = chunk_source_text_for_model(req.raw_text.trim(), &model);
        if chunks.is_empty() {
            return Err(embedding_field_violation(
                "raw_text",
                "must contain indexable text",
                "document text is empty after normalization",
            ));
        }
        let result = svc
            .persist_and_emit_work_batch(WorkBatch {
                tenant_id: &tenant_id,
                project_id: &context.project_id,
                job_id: &job_id,
                source_name: &source_name,
                parent_pk: &document_id,
                document_id: &document_id,
                doc_version: &doc_version,
                target_collection: &model.active_collection,
                model: &model,
                chunks: &chunks,
                parent_text: req.raw_text.trim(),
                force: false,
            })
            .await?;
        if let Some(pool) = svc.pg_pool.as_ref() {
            update_job_emission(pool, &tenant_id, &job_id, 1, result.emitted).await?;
            complete_job_enumeration(pool, &tenant_id, &job_id).await?;
        }
    } else {
        svc.emit_source_event(
            TOPIC_DOCUMENT_PARSE,
            &tenant_id,
            &context.project_id,
            &document_id,
            serde_json::json!({
                "document_id": document_id, "job_id": job_id, "external_id": req.external_id,
                "storage_object_ref": req.storage_object_ref, "content_type": req.content_type,
                "doc_version": doc_version, "model_id": model.model_id, "source": source_name,
            }),
        )
        .await;
    }
    svc.emit_source_event(
        TOPIC_DOCUMENT_INGESTED, &tenant_id, &context.project_id, &document_id,
        serde_json::json!({"document_id": document_id, "job_id": job_id, "source": source_name, "model_id": model.model_id}),
    ).await;
    Ok(embedding_pb::IngestDocumentResponse {
        document_id,
        job_id,
        accepted: true,
        message: "document ingestion accepted".to_string(),
        error: None,
        source_name,
    })
}

pub(crate) async fn ingest_document(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::IngestDocumentRequest>,
) -> Result<Response<embedding_pb::IngestDocumentResponse>, Status> {
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
    Ok(Response::new(ingest_one(svc, &metadata, req).await?))
}

pub(crate) async fn ingest_document_batch(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::IngestDocumentBatchRequest>,
) -> Result<Response<embedding_pb::IngestDocumentBatchResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    if req.documents.is_empty() {
        return Err(embedding_required_field(
            "documents",
            "must contain at least one document",
            "documents are required",
        ));
    }
    if req.documents.len() > MAX_DOCUMENT_INGEST_BATCH {
        return Err(embedding_field_violation(
            "documents",
            format!("must contain at most {MAX_DOCUMENT_INGEST_BATCH} documents"),
            "document batch is too large",
        ));
    }
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
    let mut documents = Vec::with_capacity(req.documents.len());
    let mut accepted = 0;
    let mut failed = 0;
    for mut document in req.documents {
        if document.tenant_id.trim().is_empty() {
            document.tenant_id = tenant_id.clone();
        }
        match ingest_one(svc, &metadata, document).await {
            Ok(response) => {
                accepted += 1;
                documents.push(response);
            }
            Err(error) => {
                failed += 1;
                documents.push(embedding_pb::IngestDocumentResponse {
                    document_id: String::new(),
                    job_id: String::new(),
                    accepted: false,
                    message: error.message().to_string(),
                    error: None,
                    source_name: String::new(),
                });
            }
        }
    }
    Ok(Response::new(embedding_pb::IngestDocumentBatchResponse {
        documents,
        accepted,
        failed,
        message: if failed == 0 {
            "document batch accepted"
        } else {
            "document batch completed with failures"
        }
        .to_string(),
        error: None,
    }))
}

pub(crate) async fn report_parsed_document(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::ReportParsedDocumentRequest>,
) -> Result<Response<embedding_pb::ReportParsedDocumentResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    if req.text.trim().is_empty() {
        return Err(embedding_required_field(
            "text",
            "must contain parsed document text",
            "parsed text is required",
        ));
    }
    let actual_hash = chunk_content_hash(req.text.trim());
    if !req.content_hash.trim().is_empty() && req.content_hash.trim() != actual_hash {
        return Err(embedding_field_violation(
            "content_hash",
            "must match normalized parsed text",
            "parsed document content hash mismatch",
        ));
    }
    let tenant_id = req.tenant_id.trim().to_string();
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");
    let row = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            document_read_by_id(&tenant_id, req.document_id.trim()),
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| {
            embedding_field_violation(
                "document_id",
                "must identify a tenant document",
                "embedding document not found",
            )
        })?;
    let map = native_json_object(&row);
    let model_id = json_str(map, "model_id");
    let model = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            model_read_by_id(&tenant_id, &model_id),
        )
        .await?
        .first()
        .map(stored_model_from_json)
        .filter(|model| model.status == STATUS_ACTIVE && model.tenant_state == STATUS_ACTIVE)
        .ok_or_else(|| {
            embedding_field_violation(
                "document_id",
                "document model must remain active",
                "embedding document model unavailable",
            )
        })?;
    let job_id = req.job_id.trim();
    if job_id.is_empty() {
        return Err(embedding_required_field(
            "job_id",
            "must identify the parser job",
            "job_id is required",
        ));
    }
    let document_id = json_str(map, "document_id");
    let job = runtime
        .native_entity_read_for_service("embedding", &context, job_read_by_id(&tenant_id, job_id))
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| {
            embedding_field_violation(
                "job_id",
                "must identify a durable tenant embedding job",
                "embedding parser job not found",
            )
        })?;
    let job_map = native_json_object(&job);
    if json_str(job_map, "document_id") != document_id
        || json_str(job_map, "job_type") != "DOCUMENT_INGEST"
    {
        return Err(embedding_field_violation(
            "job_id",
            "must belong to this document ingestion",
            "embedding parser job does not match document",
        ));
    }
    let doc_version = json_str(map, "doc_version");
    let external_id = json_str(map, "external_id");
    let source_name = document_source_name(&model.model_id);
    runtime
        .native_entity_write_for_service(
            "embedding",
            &context,
            EMBEDDING_DOCUMENT_MSG,
            document_record(
                &document_id,
                &tenant_id,
                &context.project_id,
                &external_id,
                &json_str(map, "title"),
                req.text.trim(),
                &json_str(map, "storage_object_ref"),
                &json_str(map, "content_type"),
                &doc_version,
                &model.model_id,
                &model.active_collection,
                "EMBEDDING",
                &json_str(map, "metadata_json"),
            ),
            document_conflict(),
        )
        .await?;
    let chunks = chunk_source_text_for_model(req.text.trim(), &model);
    let result = svc
        .persist_and_emit_work_batch(WorkBatch {
            tenant_id: &tenant_id,
            project_id: &context.project_id,
            job_id,
            source_name: &source_name,
            parent_pk: &document_id,
            document_id: &document_id,
            doc_version: &doc_version,
            target_collection: &model.active_collection,
            model: &model,
            chunks: &chunks,
            parent_text: req.text.trim(),
            force: false,
        })
        .await?;
    if let Some(pool) = svc.pg_pool.as_ref() {
        update_job_emission(pool, &tenant_id, job_id, 1, result.emitted).await?;
        complete_job_enumeration(pool, &tenant_id, job_id).await?;
    }
    Ok(Response::new(embedding_pb::ReportParsedDocumentResponse {
        accepted: true,
        chunks_emitted: result.emitted as i32,
        message: "parsed document queued for embedding".to_string(),
        error: None,
    }))
}
