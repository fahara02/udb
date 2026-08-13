use tonic::{Request, Response, Status};

use crate::proto::udb::core::embedding::services::v1 as embedding_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, native_next_page_token, native_offset_page_window,
    project_scoped_native_service_context, validate_request_tenant,
};
use super::EmbeddingServiceImpl;
use super::config::EMBEDDING_WORK_EMITTER_BATCH;
use super::errors::{embedding_field_violation, embedding_required_field};
use super::model::{json_i32, json_i64, json_str, native_json_object};
use super::store::{job_read_by_id, work_items_read};

fn timestamp_ms(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    let raw = json_str(map, key);
    chrono::DateTime::parse_from_rfc3339(&raw)
        .map(|value| value.timestamp_millis())
        .unwrap_or_default()
}

pub(crate) async fn get_embedding_job_status(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::GetEmbeddingJobStatusRequest>,
) -> Result<Response<embedding_pb::GetEmbeddingJobStatusResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    if req.job_id.trim().is_empty() {
        return Err(embedding_required_field(
            "job_id",
            "must identify an embedding job",
            "job_id is required",
        ));
    }
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
    let context = project_scoped_native_service_context(&metadata, &tenant_id);
    let row = svc
        .require_runtime()?
        .native_entity_read_for_service(
            "embedding",
            &context,
            job_read_by_id(&tenant_id, req.job_id.trim()),
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| {
            embedding_field_violation(
                "job_id",
                "must identify a job owned by the verified tenant",
                "embedding job not found",
            )
        })?;
    let map = native_json_object(&row);
    let job = embedding_pb::EmbeddingJobStatus {
        job_id: json_str(map, "job_id"),
        source_name: json_str(map, "source_name"),
        document_id: json_str(map, "document_id"),
        job_type: json_str(map, "job_type"),
        mode: json_str(map, "mode"),
        status: json_str(map, "status"),
        rows_enumerated: json_i64(map, "rows_enumerated"),
        chunks_emitted: json_i64(map, "chunks_emitted"),
        vectors_stored: json_i64(map, "vectors_stored"),
        failed: json_i64(map, "failed"),
        error: json_str(map, "error"),
        started_at_unix_ms: timestamp_ms(map, "started_at"),
        finished_at_unix_ms: timestamp_ms(map, "finished_at"),
    };
    Ok(Response::new(embedding_pb::GetEmbeddingJobStatusResponse {
        job: Some(job),
        message: "ok".to_string(),
        error: None,
    }))
}

pub(crate) async fn list_embedding_work_items(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::ListEmbeddingWorkItemsRequest>,
) -> Result<Response<embedding_pb::ListEmbeddingWorkItemsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    if !req.status.trim().is_empty()
        && !matches!(
            req.status.trim(),
            "PENDING" | "ACKED" | "DEAD" | "SUPERSEDED"
        )
    {
        return Err(embedding_field_violation(
            "status",
            "must be PENDING, ACKED, DEAD, or SUPERSEDED",
            "unsupported work-item status",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "embedding",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let context = project_scoped_native_service_context(&metadata, &tenant_id);
    let page = native_offset_page_window(
        1,
        req.page_size,
        &req.page_token,
        EMBEDDING_WORK_EMITTER_BATCH as i32,
    );
    let rows = svc
        .require_runtime()?
        .native_entity_read_for_service(
            "embedding",
            &context,
            work_items_read(
                &tenant_id,
                req.job_id.trim(),
                Some(req.status.trim()),
                page.offset as u64,
                page.limit as u32,
            ),
        )
        .await?;
    let work_items = rows
        .iter()
        .map(|row| {
            let map = native_json_object(row);
            embedding_pb::EmbeddingWorkItemSummary {
                work_item_id: json_str(map, "work_item_id"),
                point_id: json_str(map, "point_id"),
                source_name: json_str(map, "source_name"),
                parent_pk: json_str(map, "parent_pk"),
                chunk_seq: json_i32(map, "chunk_seq"),
                chunk_hash: json_str(map, "chunk_hash"),
                status: json_str(map, "status"),
                attempt_count: json_i32(map, "attempt_count"),
                max_attempts: json_i32(map, "max_attempts"),
                last_error: json_str(map, "last_error"),
                next_attempt_at_unix_ms: timestamp_ms(map, "next_attempt_at"),
            }
        })
        .collect::<Vec<_>>();
    let next_page_token = native_next_page_token(page.offset, page.limit, work_items.len());
    Ok(Response::new(
        embedding_pb::ListEmbeddingWorkItemsResponse {
            work_items,
            next_page_token,
            message: "ok".to_string(),
            error: None,
        },
    ))
}
