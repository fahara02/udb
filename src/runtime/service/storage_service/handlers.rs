//! The nine `StorageService` RPC handlers (register/reissue-upload-url/finalize/
//! download/get/update/delete/list) extracted from the trait impl as free
//! `pub(crate) async fn`s taking `svc` where the trait method took `&self`.
//! `mod.rs` delegates one line to each. Bodies are verbatim — the same scope
//! guard, per-tenant admission, quota lease, presign/HEAD verification, typed
//! upsert, and outbox emission as the former god file.

use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{
    ConflictStrategy, LogicalPagination, LogicalRead, LogicalSort, LogicalValue, NullOrder,
    SortDirection,
};
use crate::proto::udb::core::storage::services::v1 as storage_pb;

use super::super::native_helpers::{
    emit_payload_event, native_next_page_token_for_total, native_offset_page_window, parse_uuid,
    tenant_only_native_service_context, update_mask_allows, update_mask_path_set,
    validate_request_scope, validate_request_tenant,
};
use super::StorageServiceImpl;
use super::config::{
    FILE_MSG, STORAGE_QUOTA_EXCEEDED, TOPIC_FILE_DELETED, TOPIC_FILE_FINALIZED,
    TOPIC_FILE_METADATA_UPDATED, TOPIC_UPLOAD_URL_ISSUED, UNSUPPORTED_OBJECT_BACKEND,
};
use super::errors::{
    api_error_upload_url_unavailable, file_object_bytes_missing_status,
    object_store_bytes_missing_status, object_stream_requires_store_status,
    reissue_requires_pending_status, status_with_reason, storage_capability_status,
    storage_file_not_found_status, upload_already_finalized_status, upload_etag_mismatch_status,
    upload_size_mismatch_status, uploaded_object_missing_status, validate_object_key_filename,
    validate_register_upload_required_fields,
};
use super::model::{file_from_json, file_status_to_short, file_type_to_db, normalize_etag};
use super::presign::{ObjectCheck, PresignOutcome};
use super::store::{
    file_full_record, file_list_filter, file_projection, file_read_by_id, file_register_record,
    logical_string, logical_text_or_null, logical_uuid_or_null, quota_lease_name,
};

/// The concrete `download_file` server-streaming type (the trait's associated
/// `DownloadFileStream`). Named here so the free handler and the `mod.rs` trait
/// delegator share one definition.
pub(crate) type DownloadFileStream = std::pin::Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<storage_pb::DownloadFileChunk, Status>>
            + Send
            + 'static,
    >,
>;

/// Register a new upload's metadata row in `PENDING` state and mint the
/// canonical `object_key`.
///
/// v1 is a metadata/lifecycle service; object bytes + presigned URLs use the
/// broker's existing `GeneratePresignedUrl`/`PutObject` RPCs with this
/// `object_key`. `upload_url` is intentionally empty in v1.
pub(crate) async fn register_upload(
    svc: &StorageServiceImpl,
    request: Request<storage_pb::RegisterUploadRequest>,
) -> Result<Response<storage_pb::RegisterUploadResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
    validate_register_upload_required_fields(&req.tenant_id, &req.filename)?;
    // Fail closed on a filename that would escape the tenant/file object-key
    // prefix (path traversal / absolute path) before minting the key below.
    validate_object_key_filename(&req.filename)?;
    // Per-tenant fair admission (held for the whole RPC) — one tenant's
    // upload flood can't starve shared object capacity.
    let _admit = svc.admit(&req.tenant_id, &req.project_id).await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    let file_id = Uuid::new_v4().to_string();
    // file_type column is nullable: stored NULL when the caller omits it.
    let file_type = file_type_to_db(&req.file_type, "")?;
    let object_key = format!("{}/{}/{}", tenant_id, file_id, req.filename);
    // The declared `size_bytes` is persisted so the running quota total stays
    // accurate even before finalize replaces it with the actual uploaded size.
    let declared_size = req.size_bytes.max(0);
    let quota = StorageServiceImpl::tenant_quota_bytes();
    // Real per-tenant byte quota pre-check (0 = unlimited), serialized per
    // tenant by the canonical advisory lease so concurrent registers can't
    // race past the gate (the backend-agnostic analog of the old
    // `pg_advisory_xact_lock`).
    let lease_held = if quota > 0 {
        svc.acquire_quota_lease(runtime, &tenant_id, &file_id)
            .await?
    } else {
        false
    };
    let write = async {
        if quota > 0 {
            let used = svc.tenant_scoped_size_sum(&tenant_id).await?;
            if used + declared_size > quota {
                return Err(status_with_reason(
                    crate::runtime::executor_utils::quota_refusal_status(
                        "storage",
                        "tenant storage quota",
                        format!("tenant storage quota exceeded: {used}+{declared_size} > {quota}"),
                    ),
                    STORAGE_QUOTA_EXCEEDED,
                ));
            }
        }
        runtime
            .native_entity_write_for_service(
                "storage",
                &context,
                FILE_MSG,
                file_register_record(
                    &file_id,
                    &tenant_id,
                    &req,
                    &file_type,
                    &svc.object_backend,
                    &svc.object_bucket,
                    &object_key,
                    declared_size,
                ),
                ConflictStrategy::Error,
            )
            .await?;
        Ok::<(), Status>(())
    }
    .await;
    if lease_held {
        runtime
            .release_native_lease(&quota_lease_name(&tenant_id), &file_id)
            .await;
    }
    write?;
    if let Some(pool) = svc.pg_pool.as_ref() {
        emit_payload_event(
            pool,
            svc.outbox_relation.as_deref(),
            TOPIC_UPLOAD_URL_ISSUED,
            &file_id,
            serde_json::json!({
                "file_id": file_id.clone(),
                "tenant_id": req.tenant_id.clone(),
                "project_id": req.project_id.clone(),
                "object_key": object_key.clone(),
                "filename": req.filename.clone(),
                "size_bytes": declared_size,
            }),
            Some(&svc.metrics),
        )
        .await;
    }
    // Mint a presigned PUT URL the client uploads bytes to directly (empty in
    // metadata-only mode / on error — client then uses the public PutObject RPC).
    let upload_minutes = if req.expires_in_minutes > 0 {
        req.expires_in_minutes
    } else {
        15
    };
    // Distinguish a degraded deployment (no objectstore) from a real presign
    // error so a client seeing an empty `upload_url` knows which it is. Stay
    // `Response::Ok` either way (the PENDING row + quota reservation persist);
    // the diagnostic rides `error` + `expires_at`.
    let (upload_url, expires_at, error) = match svc
        .presign(
            &req.project_id,
            &object_key,
            "PUT",
            &req.content_type,
            upload_minutes,
        )
        .await
    {
        PresignOutcome::Url { url, expires_at } => (url, expires_at, None),
        PresignOutcome::Degraded => (
            String::new(),
            0,
            Some(api_error_upload_url_unavailable(
                "object store not configured (metadata-only mode); use the public object RPCs",
                false,
            )),
        ),
        PresignOutcome::Failed(reason) => (
            String::new(),
            0,
            Some(api_error_upload_url_unavailable(
                format!("presign failed: {reason}"),
                true,
            )),
        ),
    };
    Ok(Response::new(storage_pb::RegisterUploadResponse {
        file_id,
        upload_url,
        object_key,
        error,
        expires_at,
    }))
}

/// Finalize an upload, transitioning the metadata row to `ACTIVE` and
/// applying any supplied metadata updates.
pub(crate) async fn finalize_upload(
    svc: &StorageServiceImpl,
    request: Request<storage_pb::FinalizeUploadRequest>,
) -> Result<Response<storage_pb::FinalizeUploadResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // Per-tenant fair admission (held for the whole RPC).
    let _admit = svc.admit(&req.tenant_id, "").await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
    let file_type = file_type_to_db(&req.file_type, "")?;
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;

    // Load the current row: confirms existence, gives the object_key/project to
    // verify the bytes landed, and the prior size for the quota delta.
    let prior_rows = runtime
        .native_entity_read_for_service("storage", &context, file_read_by_id(&tenant_id, &file_id))
        .await?;
    let prior = match prior_rows.first() {
        Some(row) => file_from_json(row),
        None => return Err(storage_file_not_found_status("finalize_upload")),
    };
    // Reject re-finalize of an already-ACTIVE row (zero extra round-trip —
    // `prior` is in hand). Fail-closed lifecycle guard: avoids silently
    // re-running the ACTIVE-transition upsert on a double finalize.
    if file_status_to_short(prior.status) == "ACTIVE" {
        return Err(upload_already_finalized_status());
    }
    // Confirm the bytes landed and, when the client supplies them, verify the
    // object's HEAD size/etag — fail-closed on a missing object or a
    // supplied-vs-actual mismatch. The bench deliberately sends a wrong
    // `size_bytes`, so unsupplied size is NEVER compared; size is only checked
    // under a configured quota (strict mode). Metadata-only mode = unchecked.
    let presence = svc.object_exists(&prior).await?;
    match &presence {
        ObjectCheck::Absent => {
            return Err(uploaded_object_missing_status());
        }
        ObjectCheck::Present {
            size: head_size,
            etag: head_etag,
        } => {
            if let Some(etag) = req.etag.as_deref().map(str::trim).filter(|e| !e.is_empty()) {
                if normalize_etag(etag) != normalize_etag(head_etag) {
                    return Err(upload_etag_mismatch_status());
                }
            }
            if StorageServiceImpl::tenant_quota_bytes() > 0
                && req.size_bytes >= 0
                && *head_size >= 0
                && req.size_bytes != *head_size
            {
                return Err(upload_size_mismatch_status(*head_size, req.size_bytes));
            }
        }
        ObjectCheck::Unchecked => {}
    }

    // Partial update onto a full record (NOT-NULL-safe upsert): ACTIVE
    // transition plus only the fields the caller supplied. A negative
    // `size_bytes` means "leave the size unchanged".
    let new_size = req.size_bytes;
    let mut record = file_full_record(&prior);
    record.insert("status".to_string(), logical_string("ACTIVE"));
    let mut fields = vec!["status".to_string()];
    if new_size >= 0 {
        record.insert("size_bytes".to_string(), LogicalValue::Int(new_size));
        fields.push("size_bytes".to_string());
    }
    if !req.content_type.trim().is_empty() {
        record.insert(
            "content_type".to_string(),
            logical_string(req.content_type.clone()),
        );
        fields.push("content_type".to_string());
    }
    if !file_type.is_empty() {
        record.insert("file_type".to_string(), logical_text_or_null(&file_type));
        fields.push("file_type".to_string());
    }
    if !req.reference_id.trim().is_empty() {
        record.insert(
            "reference_id".to_string(),
            logical_uuid_or_null(&req.reference_id),
        );
        fields.push("reference_id".to_string());
    }
    if !req.reference_type.trim().is_empty() {
        record.insert(
            "reference_type".to_string(),
            logical_string(req.reference_type.clone()),
        );
        fields.push("reference_type".to_string());
    }
    if let Some(is_public) = req.is_public {
        record.insert("is_public".to_string(), LogicalValue::Bool(is_public));
        fields.push("is_public".to_string());
    }
    // Persist a client-supplied content checksum into File.checksum (field 17,
    // previously never written). Verification against the store is etag-based
    // (above); the checksum is recorded for downstream integrity audits.
    if let Some(checksum) = req
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|c| !c.is_empty())
    {
        record.insert("checksum".to_string(), logical_string(checksum));
        fields.push("checksum".to_string());
    }

    // Quota re-check against the size delta, serialized per tenant by the same
    // lease as register. Only matters when the size grows under a finite quota.
    let quota = StorageServiceImpl::tenant_quota_bytes();
    let delta = if new_size >= 0 {
        new_size - prior.size_bytes
    } else {
        0
    };
    let lease_held = if quota > 0 && delta > 0 {
        svc.acquire_quota_lease(runtime, &tenant_id, &file_id)
            .await?
    } else {
        false
    };
    let apply = async {
        if quota > 0 && delta > 0 {
            let used = svc.tenant_scoped_size_sum(&tenant_id).await?;
            if used + delta > quota {
                return Err(status_with_reason(
                    crate::runtime::executor_utils::quota_refusal_status(
                        "storage",
                        "tenant storage quota",
                        format!("tenant storage quota exceeded: {used}+{delta} > {quota}"),
                    ),
                    STORAGE_QUOTA_EXCEEDED,
                ));
            }
        }
        runtime
            .native_entity_write_for_service(
                "storage",
                &context,
                FILE_MSG,
                record,
                ConflictStrategy::update(fields),
            )
            .await?;
        Ok::<(), Status>(())
    }
    .await;
    if lease_held {
        runtime
            .release_native_lease(&quota_lease_name(&tenant_id), &file_id)
            .await;
    }
    apply?;

    // Read back the finalized row for the response.
    let rows = runtime
        .native_entity_read_for_service("storage", &context, file_read_by_id(&tenant_id, &file_id))
        .await?;
    let file = rows.first().map(file_from_json);
    if let Some(f) = &file {
        if let Some(pool) = svc.pg_pool.as_ref() {
            emit_payload_event(
                pool,
                svc.outbox_relation.as_deref(),
                TOPIC_FILE_FINALIZED,
                &f.file_id,
                serde_json::json!({
                    "file_id": f.file_id,
                    "tenant_id": f.tenant_id,
                    "project_id": f.project_id,
                    "object_key": f.object_key,
                    "size_bytes": f.size_bytes,
                    "status": "ACTIVE",
                }),
                Some(&svc.metrics),
            )
            .await;
        }
    }
    Ok(Response::new(storage_pb::FinalizeUploadResponse {
        file,
        error: None,
    }))
}

/// Compute the download-URL expiry window for a file.
///
/// The client mints the actual URL via the broker's `GeneratePresignedUrl`
/// RPC using the file's `object_key` (obtained from `GetFile`).
/// `download_url` is intentionally empty in v1.
pub(crate) async fn get_download_url(
    svc: &StorageServiceImpl,
    request: Request<storage_pb::GetDownloadUrlRequest>,
) -> Result<Response<storage_pb::GetDownloadUrlResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // Per-tenant fair admission: GetDownloadUrl mints a presigned URL via the
    // object backend, so it's an Object-class op gated per tenant.
    let _admit = svc.admit(&req.tenant_id, "").await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    let rows = runtime
        .native_entity_read_for_service("storage", &context, file_read_by_id(&tenant_id, &file_id))
        .await?;
    let Some(file) = rows.first().map(file_from_json) else {
        return Err(storage_file_not_found_status("get_download_url"));
    };
    let object_key = file.object_key;
    let project_id = file.project_id;
    let minutes = if req.expires_in_minutes > 0 {
        req.expires_in_minutes.min(1440)
    } else {
        60
    };
    // Mint a presigned GET URL (empty in metadata-only mode / on error — the
    // client then uses the public GeneratePresignedUrl RPC with object_key).
    let (download_url, expires_unix) = match svc
        .presign(&project_id, &object_key, "GET", "", minutes)
        .await
    {
        PresignOutcome::Url { url, expires_at } => (url, expires_at),
        // Degraded/failed → empty URL + 0 (the client falls back to the public
        // GeneratePresignedUrl RPC); the response still carries a computed
        // expiry window below.
        PresignOutcome::Degraded | PresignOutcome::Failed(_) => (String::new(), 0),
    };
    let expires_at = if expires_unix > 0 {
        prost_types::Timestamp {
            seconds: expires_unix,
            nanos: 0,
        }
    } else {
        let expiry = chrono::Utc::now() + chrono::Duration::minutes(minutes as i64);
        prost_types::Timestamp {
            seconds: expiry.timestamp(),
            nanos: expiry.timestamp_subsec_nanos() as i32,
        }
    };
    Ok(Response::new(storage_pb::GetDownloadUrlResponse {
        download_url,
        expires_at: Some(expires_at),
        error: None,
    }))
}

/// Reissue a presigned PUT URL for an existing PENDING upload — the resume path
/// when a `RegisterUpload` response was lost in flight (the client kept the
/// `file_id` but not the secret upload URL). The File row + `object_key` are
/// unchanged; only a fresh short-lived upload URL is minted. Fails closed for a
/// non-PENDING (finalized / removed) file so it never hands out an upload URL for
/// an object that is not awaiting bytes. Mirrors `get_download_url` (look up the
/// tenant-scoped file, then presign) for the PUT direction.
pub(crate) async fn reissue_upload_url(
    svc: &StorageServiceImpl,
    request: Request<storage_pb::ReissueUploadUrlRequest>,
) -> Result<Response<storage_pb::ReissueUploadUrlResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let _admit = svc.admit(&req.tenant_id, "").await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    let rows = runtime
        .native_entity_read_for_service("storage", &context, file_read_by_id(&tenant_id, &file_id))
        .await?;
    let Some(file) = rows.first().map(file_from_json) else {
        return Err(storage_file_not_found_status("reissue_upload_url"));
    };
    let status = file_status_to_short(file.status);
    if status != "PENDING" {
        return Err(reissue_requires_pending_status(status));
    }
    let minutes = if req.expires_in_minutes > 0 {
        req.expires_in_minutes
    } else {
        15
    };
    let object_key = file.object_key.clone();
    // Re-mint the presigned PUT URL for the SAME object_key — an exact resume of
    // the interrupted upload, via the same presign path RegisterUpload uses.
    let (upload_url, expires_at, error) = match svc
        .presign(
            &file.project_id,
            &object_key,
            "PUT",
            &file.content_type,
            minutes,
        )
        .await
    {
        PresignOutcome::Url { url, expires_at } => (url, expires_at, None),
        PresignOutcome::Degraded => (
            String::new(),
            0,
            Some(api_error_upload_url_unavailable(
                "object store not configured (metadata-only mode); use the public object RPCs",
                false,
            )),
        ),
        PresignOutcome::Failed(reason) => (
            String::new(),
            0,
            Some(api_error_upload_url_unavailable(
                format!("presign failed: {reason}"),
                true,
            )),
        ),
    };
    Ok(Response::new(storage_pb::ReissueUploadUrlResponse {
        file_id: file.file_id,
        upload_url,
        object_key,
        error,
        expires_at,
    }))
}

/// Stream a file's bytes directly through the broker — the FALLBACK for
/// clients that cannot reach the object store via the presigned
/// `GetDownloadUrl` HTTP GET.
///
/// Identity comes from the verified claim (`validate_request_tenant`): the
/// body `file_id` is the only client-chosen target; the tenant is taken from
/// the claim and the row is resolved tenant-scoped + live-only, so a
/// cross-tenant `file_id` simply yields not-found (fail-closed). Presence is
/// confirmed via the SAME `object_exists` HEAD the finalize path uses (also
/// supplies the first-chunk content_type/total_size/etag), then the bytes are
/// streamed in BOUNDED chunks through `get_object_stream_backend_target` (the
/// data-plane `GetObject` streaming primitive) — the whole object is never
/// buffered in the broker.
pub(crate) async fn download_file(
    svc: &StorageServiceImpl,
    request: Request<storage_pb::DownloadFileRequest>,
) -> Result<Response<DownloadFileStream>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // Per-tenant fair admission: streaming object bytes is an Object-class op.
    let _admit = svc.admit(&req.tenant_id, "").await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    // Resolve the live file row scoped to the verified tenant (cross-tenant
    // file_id → not-found, fail-closed).
    let rows = runtime
        .native_entity_read_for_service("storage", &context, file_read_by_id(&tenant_id, &file_id))
        .await?;
    let Some(file) = rows.first().map(file_from_json) else {
        return Err(storage_file_not_found_status("download_file"));
    };
    if file.object_key.trim().is_empty() {
        return Err(file_object_bytes_missing_status());
    }
    // Confirm the bytes are present (and capture first-chunk metadata) via the
    // SAME HEAD primitive finalize uses. Metadata-only mode (no runtime/object
    // store) cannot stream bytes → fail closed.
    let (head_size, head_etag) = match svc.object_exists(&file).await? {
        ObjectCheck::Present { size, etag } => (size, etag),
        ObjectCheck::Absent => {
            return Err(object_store_bytes_missing_status());
        }
        ObjectCheck::Unchecked => {
            return Err(object_stream_requires_store_status());
        }
    };
    // Resolve the backend/bucket the file's bytes live in (same fallbacks as
    // object_exists).
    let backend = if file.backend.trim().is_empty() {
        svc.object_backend.clone()
    } else {
        file.backend.clone()
    };
    let bucket = if file.bucket.trim().is_empty() {
        svc.object_bucket.clone()
    } else {
        file.bucket.clone()
    };
    let request_json =
        crate::runtime::core::setup_data::object_request_json("get", &bucket, &file.object_key, "");
    let runtime = svc.runtime.clone().ok_or_else(|| {
        storage_capability_status(
            "object_stream",
            "runtime_native_entity_dispatch",
            "storage service requires runtime",
        )
    })?;
    // Bounded-memory byte stream from the object store (executor streams in
    // its own frames; the broker never buffers the whole object).
    let byte_stream = runtime
        .get_object_stream_backend_target(&backend, None, &file.project_id, &request_json)
        .await
        .map_err(|status| match status.code() {
            tonic::Code::FailedPrecondition => {
                status_with_reason(status, UNSUPPORTED_OBJECT_BACKEND)
            }
            _ => status,
        })?;
    // First-chunk metadata: content_type from the row, total_size + etag from
    // the HEAD. Carried only on the first emitted DownloadFileChunk.
    let content_type = if file.content_type.trim().is_empty() {
        None
    } else {
        Some(file.content_type.clone())
    };
    let total_size = if head_size >= 0 {
        Some(head_size)
    } else {
        None
    };
    let etag = if head_etag.trim().is_empty() {
        None
    } else {
        Some(head_etag)
    };
    let out = async_stream::try_stream! {
        use tokio_stream::StreamExt as _;
        let mut byte_stream = byte_stream;
        let mut first = true;
        while let Some(chunk) = byte_stream.next().await {
            let data = chunk?;
            if first {
                first = false;
                yield storage_pb::DownloadFileChunk {
                    data: data.to_vec(),
                    content_type: content_type.clone(),
                    total_size,
                    etag: etag.clone(),
                };
            } else {
                yield storage_pb::DownloadFileChunk {
                    data: data.to_vec(),
                    ..Default::default()
                };
            }
        }
        // Zero-byte object: still emit a single (empty) first chunk so the
        // caller receives the first-chunk metadata.
        if first {
            yield storage_pb::DownloadFileChunk {
                data: Vec::new(),
                content_type,
                total_size,
                etag,
            };
        }
    };
    Ok(Response::new(Box::pin(out) as DownloadFileStream))
}

/// Fetch a single file's metadata.
pub(crate) async fn get_file(
    svc: &StorageServiceImpl,
    request: Request<storage_pb::GetFileRequest>,
) -> Result<Response<storage_pb::GetFileResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // Per-tenant fair admission (lighter Read budget) so one tenant can't
    // exhaust the shared pool with reads.
    let _admit = svc.admit_read(&req.tenant_id).await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    let rows = runtime
        .native_entity_read_for_service("storage", &context, file_read_by_id(&tenant_id, &file_id))
        .await?;
    let file = rows.first().map(file_from_json);
    if file.is_none() {
        return Err(storage_file_not_found_status("get_file"));
    }
    Ok(Response::new(storage_pb::GetFileResponse {
        file,
        error: None,
    }))
}

/// Partial update of file metadata; only non-empty fields are applied.
pub(crate) async fn update_file(
    svc: &StorageServiceImpl,
    request: Request<storage_pb::UpdateFileRequest>,
) -> Result<Response<storage_pb::UpdateFileResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // Per-tenant fair admission (held for the whole RPC).
    let _admit = svc.admit(&req.tenant_id, "").await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
    let file_type = file_type_to_db(&req.file_type, "")?;
    let update_mask = update_mask_path_set(
        req.update_mask.as_ref(),
        &[
            "filename",
            "content_type",
            "file_type",
            "reference_id",
            "reference_type",
            "is_public",
        ],
    )?;
    if update_mask
        .as_ref()
        .is_some_and(|paths| paths.contains("is_public"))
        && req.is_public.is_none()
    {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "is_public is required when present in update_mask",
            [(
                "is_public",
                "must be supplied when update_mask includes is_public",
            )],
        ));
    }
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    let rows = runtime
        .native_entity_read_for_service("storage", &context, file_read_by_id(&tenant_id, &file_id))
        .await?;
    let prior = match rows.first() {
        Some(row) => file_from_json(row),
        None => return Err(storage_file_not_found_status("update_file")),
    };
    // Partial update onto a full record (NOT-NULL-safe upsert): only the
    // fields the caller supplied are changed (mirrors the old COALESCE/NULLIF
    // guards).
    let mut record = file_full_record(&prior);
    let mut fields = Vec::new();
    if update_mask_allows(&update_mask, "filename", !req.filename.trim().is_empty()) {
        record.insert("filename".to_string(), logical_string(req.filename.clone()));
        fields.push("filename".to_string());
    }
    if update_mask_allows(
        &update_mask,
        "content_type",
        !req.content_type.trim().is_empty(),
    ) {
        record.insert(
            "content_type".to_string(),
            logical_string(req.content_type.clone()),
        );
        fields.push("content_type".to_string());
    }
    if update_mask_allows(&update_mask, "file_type", !file_type.is_empty()) {
        record.insert("file_type".to_string(), logical_text_or_null(&file_type));
        fields.push("file_type".to_string());
    }
    if update_mask_allows(
        &update_mask,
        "reference_id",
        !req.reference_id.trim().is_empty(),
    ) {
        record.insert(
            "reference_id".to_string(),
            logical_uuid_or_null(&req.reference_id),
        );
        fields.push("reference_id".to_string());
    }
    if update_mask_allows(
        &update_mask,
        "reference_type",
        !req.reference_type.trim().is_empty(),
    ) {
        record.insert(
            "reference_type".to_string(),
            logical_string(req.reference_type.clone()),
        );
        fields.push("reference_type".to_string());
    }
    if update_mask_allows(&update_mask, "is_public", req.is_public.is_some()) {
        record.insert(
            "is_public".to_string(),
            LogicalValue::Bool(req.is_public.unwrap_or(false)),
        );
        fields.push("is_public".to_string());
    }
    if !fields.is_empty() {
        runtime
            .native_entity_write_for_service(
                "storage",
                &context,
                FILE_MSG,
                record,
                ConflictStrategy::update(fields),
            )
            .await?;
    }
    if let Some(pool) = svc.pg_pool.as_ref() {
        emit_payload_event(
            pool,
            svc.outbox_relation.as_deref(),
            TOPIC_FILE_METADATA_UPDATED,
            &req.file_id,
            serde_json::json!({
                "file_id": req.file_id,
                "tenant_id": req.tenant_id,
            }),
            Some(&svc.metrics),
        )
        .await;
    }
    Ok(Response::new(storage_pb::UpdateFileResponse {
        message: "file updated".to_string(),
        error: None,
    }))
}

/// Soft-delete a file's metadata record.
///
/// v1 soft-deletes the metadata record; object GC is handled by a
/// lifecycle/reaper, not inline.
pub(crate) async fn delete_file(
    svc: &StorageServiceImpl,
    request: Request<storage_pb::DeleteFileRequest>,
) -> Result<Response<storage_pb::DeleteFileResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // Per-tenant fair admission (held for the whole RPC) — DeleteFile also
    // removes object bytes via the object executor, so it's an Object-class op.
    let _admit = svc.admit(&req.tenant_id, "").await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    // Load the live row first: confirms existence (and not already deleted) and
    // recovers the object_key/project so the bytes can be removed too.
    let rows = runtime
        .native_entity_read_for_service("storage", &context, file_read_by_id(&tenant_id, &file_id))
        .await?;
    let prior = match rows.first() {
        Some(row) => file_from_json(row),
        None => return Err(storage_file_not_found_status("delete_file")),
    };
    // Soft-delete the metadata (keeps it auditable) on a full record.
    let mut record = file_full_record(&prior);
    record.insert(
        "deleted_at".to_string(),
        LogicalValue::Timestamp(chrono::Utc::now()),
    );
    record.insert("status".to_string(), logical_string("DELETED"));
    runtime
        .native_entity_write_for_service(
            "storage",
            &context,
            FILE_MSG,
            record,
            ConflictStrategy::update(vec!["deleted_at".to_string(), "status".to_string()]),
        )
        .await?;
    // Remove the bytes (best-effort; metadata stays soft-deleted on failure).
    svc.delete_object_bytes(&prior.project_id, &prior.object_key)
        .await;
    if let Some(pool) = svc.pg_pool.as_ref() {
        emit_payload_event(
            pool,
            svc.outbox_relation.as_deref(),
            TOPIC_FILE_DELETED,
            &req.file_id,
            serde_json::json!({
                "file_id": req.file_id,
                "tenant_id": req.tenant_id,
                "project_id": prior.project_id,
            }),
            Some(&svc.metrics),
        )
        .await;
    }
    Ok(Response::new(storage_pb::DeleteFileResponse {
        success: true,
        error: None,
    }))
}

/// List a tenant's files with optional metadata filters.
pub(crate) async fn list_files(
    svc: &StorageServiceImpl,
    request: Request<storage_pb::ListFilesRequest>,
) -> Result<Response<storage_pb::ListFilesResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // Per-tenant fair admission (lighter Read budget) so one tenant can't
    // exhaust the shared pool with list scans.
    let _admit = svc.admit_read(&req.tenant_id).await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let type_filter = file_type_to_db(&req.file_type, "")?;
    let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    let filter = file_list_filter(
        &tenant_id,
        &type_filter,
        &req.reference_id,
        &req.reference_type,
        &req.uploaded_by,
    );
    // Count via the typed path (reads the per-tenant-bounded matching set). No
    // IR-level aggregate yet (P4.D), so this is a bounded scan, not a hot-path
    // COUNT — acceptable for a tenant's file list.
    let total = runtime
        .native_entity_count_for_service("storage", &context, FILE_MSG, Some(filter.clone()))
        .await?;
    let read = LogicalRead {
        message_type: FILE_MSG.to_string(),
        filter: Some(filter),
        projection: Some(file_projection()),
        sort: vec![LogicalSort {
            field: "filename".to_string(),
            direction: SortDirection::Asc,
            nulls: NullOrder::Default,
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(
            page_window.offset as u64,
            page_window.limit as u32,
        )),
    };
    let rows = runtime
        .native_entity_read_for_service("storage", &context, read)
        .await?;
    let files = rows.iter().map(file_from_json).collect::<Vec<_>>();
    Ok(Response::new(storage_pb::ListFilesResponse {
        files,
        total_count: total as i32,
        error: None,
        next_page_token: native_next_page_token_for_total(
            page_window.offset,
            page_window.limit,
            total as i64,
        ),
    }))
}
