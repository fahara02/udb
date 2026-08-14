//! The nine `StorageService` RPC handlers (register/reissue-upload-url/finalize/
//! download/get/update/delete/list) extracted from the trait impl as free
//! `pub(crate) async fn`s taking `svc` where the trait method took `&self`.
//! `mod.rs` delegates one line to each. Bodies are verbatim — the same scope
//! guard, per-tenant admission, quota lease, presign/HEAD verification, typed
//! upsert, and outbox emission as the former god file.

use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalAssignment, LogicalFilter, LogicalPagination,
    LogicalRead, LogicalSort, LogicalUpdate, LogicalValue, NullOrder, SortDirection,
};
use crate::proto::udb::core::storage::entity::v1 as storage_entity_pb;
use crate::proto::udb::core::storage::services::v1 as storage_pb;

use super::super::native_helpers::{
    emit_payload_event, metadata_project_id, native_next_page_token_for_total,
    native_offset_page_window, parse_uuid, tenant_only_native_service_context, update_mask_allows,
    update_mask_path_set, validate_request_scope, validate_request_tenant,
};
use super::StorageServiceImpl;
use super::config::{
    FILE_MSG, STORAGE_QUOTA_EXCEEDED, TOPIC_FILE_DELETED, TOPIC_FILE_FINALIZED,
    TOPIC_FILE_METADATA_UPDATED, TOPIC_UPLOAD_URL_ISSUED, UNSUPPORTED_OBJECT_BACKEND,
};
use super::errors::{
    api_error_upload_url_unavailable, delete_expected_status_mismatch_status,
    file_object_bytes_missing_status, finalize_immutable_mismatch_status,
    object_delete_failed_status, object_store_bytes_missing_status,
    object_stream_requires_store_status, reissue_requires_pending_status, status_with_reason,
    storage_capability_status, storage_file_not_found_status, storage_idempotency_conflict_status,
    storage_internal_status, upload_already_finalized_status, upload_etag_mismatch_status,
    upload_size_mismatch_status, uploaded_object_missing_status, validate_object_key_filename,
    validate_register_upload_required_fields,
};
use super::model::{
    file_from_json, file_status_to_short, file_type_to_db, file_type_to_short, normalize_etag,
};
use super::presign::{ObjectCheck, PresignOutcome};
use super::store::{
    GcIntentRow, file_full_record, file_list_filter, file_projection, file_read_by_id,
    file_register_record, gc_intent_fingerprint, logical_string, logical_text_or_null,
    logical_uuid_or_null, quota_lease_name,
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

/// Resolve Storage's authorization project claim-first while preserving the
/// existing tenant-wide placement model. A tenant-scoped credential with no
/// project claim/header gets an empty project and keeps tenant-wide behavior;
/// every non-empty value is validated for the UUID column before it reaches IR.
fn resolved_storage_project_scope(
    metadata: &MetadataMap,
    request_project_id: &str,
) -> Result<String, Status> {
    let project_id = metadata_project_id(metadata)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| request_project_id.trim().to_string());
    if project_id.is_empty() {
        Ok(String::new())
    } else {
        Ok(parse_uuid("project_id", &project_id)?.to_string())
    }
}

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
    let mut req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
    req.project_id = resolved_storage_project_scope(&metadata, &req.project_id)?;
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

/// True when a re-finalize request re-asserts (or omits) every field the finalize
/// transition fixes — i.e. it is a SAME-INPUT replay of an upload that is already
/// ACTIVE. Drives the idempotent-replay path: a matching request returns the
/// stored File, while one that would change the finalized file falls through to
/// the fail-closed already-finalized rejection. Mirrors the immutability
/// assertions on the PENDING path, comparing against the STORED row (no object
/// HEAD needed — the persisted size is the HEAD-trusted size from the first
/// finalize).
fn finalize_replay_matches(
    req: &storage_pb::FinalizeUploadRequest,
    prior: &storage_entity_pb::File,
    file_type: &str,
) -> bool {
    let content_type_ok =
        req.content_type.trim().is_empty() || req.content_type.trim() == prior.content_type.trim();
    let file_type_ok = file_type.is_empty() || file_type == file_type_to_short(prior.file_type);
    let reference_id_ok = req.reference_id.trim().is_empty()
        || req
            .reference_id
            .trim()
            .eq_ignore_ascii_case(prior.reference_id.trim());
    let reference_type_ok = req.reference_type.trim().is_empty()
        || req.reference_type.trim() == prior.reference_type.trim();
    let is_public_ok = match req.is_public {
        Some(v) => v == prior.is_public,
        None => true,
    };
    // An unspecified declared size (< 0) matches; otherwise it must equal the
    // finalized size already on record. A different declared size is a conflict.
    let size_ok = req.size_bytes < 0 || req.size_bytes == prior.size_bytes;
    content_type_ok
        && file_type_ok
        && reference_id_ok
        && reference_type_ok
        && is_public_ok
        && size_ok
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
    let project_id = resolved_storage_project_scope(&metadata, "")?;
    // Per-tenant fair admission (held for the whole RPC).
    let _admit = svc.admit(&req.tenant_id, &project_id).await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
    let file_type = file_type_to_db(&req.file_type, "")?;
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;

    // Load the current row: confirms existence, gives the object_key/project to
    // verify the bytes landed, and the prior size for the quota delta.
    let prior_rows = runtime
        .native_entity_read_for_service(
            "storage",
            &context,
            file_read_by_id(&tenant_id, &project_id, &file_id),
        )
        .await?;
    let prior = match prior_rows.first() {
        Some(row) => file_from_json(row),
        None => return Err(storage_file_not_found_status("finalize_upload")),
    };
    // Re-finalize of an already-ACTIVE row (zero extra round-trip — `prior` is in
    // hand). A SAME-INPUT replay — the caller re-sends the finalize it already
    // succeeded with — is idempotent: return the STORED File instead of erroring,
    // so an at-least-once / retried finalize is safe. A replay that would CHANGE
    // the finalized file (a different size or a re-pointed immutable field) is
    // still rejected fail-closed as a conflicting double-finalize.
    if file_status_to_short(prior.status) == "ACTIVE" {
        if finalize_replay_matches(&req, &prior, &file_type) {
            return Ok(Response::new(storage_pb::FinalizeUploadResponse {
                file: Some(prior),
                error: None,
            }));
        }
        return Err(upload_already_finalized_status());
    }
    // Confirm the bytes landed and, when the object store returns a HEAD, verify
    // the object's etag and size — fail-closed on a missing object or a
    // supplied-vs-actual mismatch. The object store's HEAD size is AUTHORITATIVE:
    // it is compared regardless of whether a tenant quota is configured, and it
    // (never the client's declaration) is what gets persisted, so a caller cannot
    // record a false size under an unlimited quota. Metadata-only mode (no object
    // store configured) has no HEAD to trust and falls back to the declared size.
    let presence = svc.object_exists(&prior).await?;
    let trusted_size: Option<i64> = match &presence {
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
            // Always compare a supplied size against the actual object size — a
            // mismatch is a fail-closed error, not a silently-accepted override,
            // independent of quota enforcement.
            if req.size_bytes >= 0 && *head_size >= 0 && req.size_bytes != *head_size {
                return Err(upload_size_mismatch_status(*head_size, req.size_bytes));
            }
            if *head_size >= 0 {
                Some(*head_size)
            } else {
                None
            }
        }
        ObjectCheck::Unchecked => None,
    };

    // Finalize is an ownership-PRESERVING lifecycle transition, NOT a
    // metadata-update endpoint: the identity/ownership tuple fixed at
    // register_upload (reference id/type, content/file type, visibility) is
    // IMMUTABLE here. The caller may re-assert the registered value, but any
    // change is rejected fail-closed — a finalize-scoped caller must not be able
    // to re-point ownership/reference or escalate visibility on a PENDING row.
    // (Tenant RLS alone does not stop a same-tenant reference/owner swap.)
    if !req.content_type.trim().is_empty() && req.content_type.trim() != prior.content_type.trim() {
        return Err(finalize_immutable_mismatch_status(
            "content_type",
            prior.content_type.trim(),
            req.content_type.trim(),
        ));
    }
    if !file_type.is_empty() {
        let registered = file_type_to_short(prior.file_type);
        if file_type != registered {
            return Err(finalize_immutable_mismatch_status(
                "file_type",
                registered,
                &file_type,
            ));
        }
    }
    // reference_id is a UUID: compare case-insensitively so a re-asserted id in a
    // different case is not treated as a change (registration canonicalizes it).
    if !req.reference_id.trim().is_empty()
        && !req
            .reference_id
            .trim()
            .eq_ignore_ascii_case(prior.reference_id.trim())
    {
        return Err(finalize_immutable_mismatch_status(
            "reference_id",
            prior.reference_id.trim(),
            req.reference_id.trim(),
        ));
    }
    if !req.reference_type.trim().is_empty()
        && req.reference_type.trim() != prior.reference_type.trim()
    {
        return Err(finalize_immutable_mismatch_status(
            "reference_type",
            prior.reference_type.trim(),
            req.reference_type.trim(),
        ));
    }
    if let Some(is_public) = req.is_public {
        if is_public != prior.is_public {
            return Err(finalize_immutable_mismatch_status(
                "is_public",
                &prior.is_public.to_string(),
                &is_public.to_string(),
            ));
        }
    }

    // Status-guarded CAS transition to ACTIVE. Instead of an unconditional upsert,
    // build a conditional UPDATE whose filter pins tenant + file + still-live +
    // `status = 'PENDING'`, so a concurrent or replayed finalize that already won
    // the transition matches ZERO rows here and cannot double-run the ACTIVE write.
    // The TRUSTED size (object-store HEAD when available, else the declared size)
    // and the optional integrity checksum are the only mutations; the immutable
    // identity/ownership columns asserted above are never touched. A negative
    // declared size in metadata-only mode means "leave the size unchanged".
    let new_size = trusted_size.unwrap_or(req.size_bytes);
    let mut assignments: std::collections::BTreeMap<String, LogicalAssignment> =
        std::collections::BTreeMap::new();
    assignments.insert(
        "status".to_string(),
        LogicalAssignment::Set {
            value: logical_string("ACTIVE"),
        },
    );
    if new_size >= 0 {
        assignments.insert(
            "size_bytes".to_string(),
            LogicalAssignment::Set {
                value: LogicalValue::Int(new_size),
            },
        );
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
        assignments.insert(
            "checksum".to_string(),
            LogicalAssignment::Set {
                value: logical_string(checksum),
            },
        );
    }
    // Fail-closed CAS predicate: same tenant, same file, still live, still PENDING.
    // Tenant scope is enforced both here and by the tenant-bound request context.
    let cas_filter = LogicalFilter::And(vec![
        LogicalFilter::Comparison {
            field: "tenant_id".to_string(),
            op: ComparisonOp::Eq,
            value: logical_uuid_or_null(&tenant_id),
        },
        LogicalFilter::Comparison {
            field: "file_id".to_string(),
            op: ComparisonOp::Eq,
            value: logical_uuid_or_null(&file_id),
        },
        LogicalFilter::IsNull("deleted_at".to_string()),
        LogicalFilter::Comparison {
            field: "status".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string("PENDING"),
        },
    ]);

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
        // `require_affected: false` — a lost CAS (0 rows) is an idempotent replay,
        // NOT an error; it is handled at the read-back below (no event re-emit).
        let (affected, _) = runtime
            .native_entity_update_for_service(
                "storage",
                &context,
                LogicalUpdate {
                    message_type: FILE_MSG.to_string(),
                    filter: cas_filter,
                    assignments,
                    return_fields: Vec::new(),
                    require_affected: false,
                },
            )
            .await?;
        Ok::<u64, Status>(affected)
    }
    .await;
    if lease_held {
        runtime
            .release_native_lease(&quota_lease_name(&tenant_id), &file_id)
            .await;
    }
    let affected = apply?;

    // Read back the current row for the response. On a WON CAS (affected == 1)
    // this is the freshly-ACTIVE row; on a LOST CAS (affected == 0, a concurrent
    // finalize already transitioned it) it is that same finalized row — an
    // idempotent outcome, not a double-run. If the row vanished (concurrently
    // soft-deleted between the CAS and this read) fail closed as not-found.
    let rows = runtime
        .native_entity_read_for_service(
            "storage",
            &context,
            file_read_by_id(&tenant_id, &project_id, &file_id),
        )
        .await?;
    let file = match rows.first().map(file_from_json) {
        Some(f) => f,
        None => return Err(storage_file_not_found_status("finalize_upload")),
    };
    // Emit the finalize event ONLY for the transition WE performed, so a lost CAS
    // (idempotent replay) never double-publishes FILE_FINALIZED.
    if affected > 0 {
        if let Some(pool) = svc.pg_pool.as_ref() {
            emit_payload_event(
                pool,
                svc.outbox_relation.as_deref(),
                TOPIC_FILE_FINALIZED,
                &file.file_id,
                serde_json::json!({
                    "file_id": file.file_id,
                    "tenant_id": file.tenant_id,
                    "project_id": file.project_id,
                    "object_key": file.object_key,
                    "size_bytes": file.size_bytes,
                    "status": "ACTIVE",
                }),
                Some(&svc.metrics),
            )
            .await;
        }
    }
    Ok(Response::new(storage_pb::FinalizeUploadResponse {
        file: Some(file),
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
    let project_id = resolved_storage_project_scope(&metadata, "")?;
    // Per-tenant fair admission: GetDownloadUrl mints a presigned URL via the
    // object backend, so it's an Object-class op gated per tenant.
    let _admit = svc.admit(&req.tenant_id, &project_id).await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    let rows = runtime
        .native_entity_read_for_service(
            "storage",
            &context,
            file_read_by_id(&tenant_id, &project_id, &file_id),
        )
        .await?;
    let Some(file) = rows.first().map(file_from_json) else {
        return Err(storage_file_not_found_status("get_download_url"));
    };
    let object_key = file.object_key;
    let object_project_id = file.project_id;
    let minutes = if req.expires_in_minutes > 0 {
        req.expires_in_minutes.min(1440)
    } else {
        60
    };
    // Mint a presigned GET URL (empty in metadata-only mode / on error — the
    // client then uses the public GeneratePresignedUrl RPC with object_key).
    let (download_url, expires_unix) = match svc
        .presign(&object_project_id, &object_key, "GET", "", minutes)
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
    let project_id = resolved_storage_project_scope(&metadata, "")?;
    let _admit = svc.admit(&req.tenant_id, &project_id).await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    let rows = runtime
        .native_entity_read_for_service(
            "storage",
            &context,
            file_read_by_id(&tenant_id, &project_id, &file_id),
        )
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
/// the claim and the row is resolved tenant-scoped + live-only, with an
/// additional project-ownership predicate whenever the verified credential
/// carries one. A cross-tenant or cross-project `file_id` therefore yields
/// not-found (fail-closed). Presence is confirmed via the SAME `object_exists`
/// HEAD the finalize path uses (also supplies the first-chunk
/// content_type/total_size/etag), then the bytes are streamed in BOUNDED chunks
/// through `get_object_stream_backend_target` (the data-plane `GetObject`
/// streaming primitive) — the whole object is never buffered in the broker.
pub(crate) async fn download_file(
    svc: &StorageServiceImpl,
    request: Request<storage_pb::DownloadFileRequest>,
) -> Result<Response<DownloadFileStream>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let project_id = resolved_storage_project_scope(&metadata, "")?;
    // Per-tenant fair admission: streaming object bytes is an Object-class op.
    let _admit = svc.admit(&req.tenant_id, &project_id).await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    // Resolve the live file row scoped to the verified tenant (cross-tenant
    // file_id → not-found, fail-closed).
    let rows = runtime
        .native_entity_read_for_service(
            "storage",
            &context,
            file_read_by_id(&tenant_id, &project_id, &file_id),
        )
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
    let project_id = resolved_storage_project_scope(&metadata, "")?;
    // Per-tenant fair admission (lighter Read budget) so one tenant can't
    // exhaust the shared pool with reads.
    let _admit = svc.admit_read(&req.tenant_id).await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    let rows = runtime
        .native_entity_read_for_service(
            "storage",
            &context,
            file_read_by_id(&tenant_id, &project_id, &file_id),
        )
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
    let project_id = resolved_storage_project_scope(&metadata, "")?;
    // Per-tenant fair admission (held for the whole RPC).
    let _admit = svc.admit(&req.tenant_id, &project_id).await?;
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
        .native_entity_read_for_service(
            "storage",
            &context,
            file_read_by_id(&tenant_id, &project_id, &file_id),
        )
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

/// Delete a file. `mode` selects SOFT (default — metadata tombstone + best-effort
/// byte removal) or HARD (a durable object-GC intent committed atomically with the
/// tombstone, then driven to convergence so object bytes can never leak while the
/// metadata says deleted).
pub(crate) async fn delete_file(
    svc: &StorageServiceImpl,
    request: Request<storage_pb::DeleteFileRequest>,
) -> Result<Response<storage_pb::DeleteFileResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let project_id = resolved_storage_project_scope(&metadata, "")?;
    // Per-tenant fair admission (held for the whole RPC) — DeleteFile also
    // removes object bytes via the object executor, so it's an Object-class op.
    let _admit = svc.admit(&req.tenant_id, &project_id).await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    // Additive: an absent/UNSPECIFIED mode is SOFT, so pre-existing clients keep
    // their historical behavior.
    let hard = matches!(
        storage_pb::DeleteMode::try_from(req.mode).unwrap_or(storage_pb::DeleteMode::Unspecified),
        storage_pb::DeleteMode::Hard
    );
    if hard {
        delete_file_hard(
            svc,
            runtime,
            &context,
            &tenant_id,
            &project_id,
            &file_id,
            &req,
        )
        .await
    } else {
        delete_file_soft(
            svc,
            runtime,
            &context,
            &tenant_id,
            &project_id,
            &file_id,
            &req,
        )
        .await
    }
}

/// Optimistic-concurrency guard shared by both delete modes: when
/// `expected_status` is supplied it must equal the file's current status token,
/// else the delete is refused fail-closed.
fn enforce_expected_status(
    req: &storage_pb::DeleteFileRequest,
    prior: &storage_entity_pb::File,
) -> Result<(), Status> {
    let expected = req.expected_status.trim();
    if expected.is_empty() {
        return Ok(());
    }
    let current = file_status_to_short(prior.status);
    if expected.eq_ignore_ascii_case(current) {
        Ok(())
    } else {
        Err(delete_expected_status_mismatch_status(current, expected))
    }
}

/// Emit the `FILE_DELETED` domain event through the transactional outbox. Emitted
/// once per delete (never on an idempotent HARD replay).
async fn emit_file_deleted_event(
    svc: &StorageServiceImpl,
    req: &storage_pb::DeleteFileRequest,
    project_id: &str,
) {
    if let Some(pool) = svc.pg_pool.as_ref() {
        emit_payload_event(
            pool,
            svc.outbox_relation.as_deref(),
            TOPIC_FILE_DELETED,
            &req.file_id,
            serde_json::json!({
                "file_id": req.file_id,
                "tenant_id": req.tenant_id,
                "project_id": project_id,
            }),
            Some(&svc.metrics),
        )
        .await;
    }
}

/// SOFT delete (default): soft-delete the metadata record; object GC is handled by
/// the lifecycle/reaper, not inline (byte removal is best-effort, metadata stays
/// soft-deleted and auditable on failure).
async fn delete_file_soft(
    svc: &StorageServiceImpl,
    runtime: &crate::runtime::DataBrokerRuntime,
    context: &crate::RequestContext,
    tenant_id: &str,
    project_id: &str,
    file_id: &str,
    req: &storage_pb::DeleteFileRequest,
) -> Result<Response<storage_pb::DeleteFileResponse>, Status> {
    // Load the live row first: confirms existence (and not already deleted) and
    // recovers the object_key/project so the bytes can be removed too.
    let rows = runtime
        .native_entity_read_for_service(
            "storage",
            context,
            file_read_by_id(tenant_id, project_id, file_id),
        )
        .await?;
    let prior = match rows.first() {
        Some(row) => file_from_json(row),
        None => return Err(storage_file_not_found_status("delete_file")),
    };
    enforce_expected_status(req, &prior)?;
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
            context,
            FILE_MSG,
            record,
            ConflictStrategy::update(vec!["deleted_at".to_string(), "status".to_string()]),
        )
        .await?;
    // Remove the bytes (best-effort; metadata stays soft-deleted on failure).
    svc.delete_object_bytes(&prior.project_id, &prior.object_key)
        .await;
    emit_file_deleted_event(svc, req, &prior.project_id).await;
    Ok(Response::new(storage_pb::DeleteFileResponse {
        success: true,
        error: None,
    }))
}

/// HARD delete: durably record an object-GC intent ATOMICALLY with the metadata
/// tombstone, then drive object deletion to convergence. A byte-delete failure
/// returns an error (never `success: true`) and leaves the PENDING intent for the
/// leader-elected sweep to converge — so object bytes can never leak while the
/// metadata says deleted. An idempotency-key replay returns the ORIGINAL outcome; a
/// same-key/different-target reuse conflicts fail-closed.
async fn delete_file_hard(
    svc: &StorageServiceImpl,
    runtime: &crate::runtime::DataBrokerRuntime,
    context: &crate::RequestContext,
    tenant_id: &str,
    project_id: &str,
    file_id: &str,
    req: &storage_pb::DeleteFileRequest,
) -> Result<Response<storage_pb::DeleteFileResponse>, Status> {
    svc.ensure_gc_intents_table().await?;
    let max_attempts = StorageServiceImpl::gc_max_attempts();
    let fingerprint = gc_intent_fingerprint(file_id, "HARD");
    let idempotency_key = req.idempotency_key.trim();

    // Idempotent replay: a prior intent under this key returns its recorded outcome
    // only when it belongs to the caller's verified project. The project check
    // precedes the fingerprint/outcome response so a known key cannot expose a
    // different project's delete result.
    if !idempotency_key.is_empty() {
        if let Some(prior) = svc.gc_intent_by_key(tenant_id, idempotency_key).await? {
            if !project_id.is_empty() && prior.project_id != project_id {
                return Err(storage_file_not_found_status("delete_file"));
            }
            return resolve_replayed_gc_intent(svc, prior, &fingerprint, max_attempts).await;
        }
    }

    // Fresh delete: resolve the live row (existence + object coordinates), enforce
    // the optimistic guard, then commit the tombstone + PENDING intent atomically.
    let rows = runtime
        .native_entity_read_for_service(
            "storage",
            context,
            file_read_by_id(tenant_id, project_id, file_id),
        )
        .await?;
    let prior = match rows.first() {
        Some(row) => file_from_json(row),
        None => return Err(storage_file_not_found_status("delete_file")),
    };
    enforce_expected_status(req, &prior)?;
    let backend = if prior.backend.trim().is_empty() {
        svc.object_backend.clone()
    } else {
        prior.backend.clone()
    };
    let bucket = if prior.bucket.trim().is_empty() {
        svc.object_bucket.clone()
    } else {
        prior.bucket.clone()
    };
    let key = if idempotency_key.is_empty() {
        None
    } else {
        Some(idempotency_key)
    };
    let intent_id = match svc
        .insert_gc_intent_and_tombstone(
            tenant_id,
            file_id,
            &prior.project_id,
            &backend,
            &bucket,
            &prior.object_key,
            req.reason.trim(),
            key,
            &fingerprint,
        )
        .await?
    {
        Some(id) => id,
        None => {
            // Lost the idempotency race — replay the concurrent winner's intent.
            let winner = svc
                .gc_intent_by_key(tenant_id, idempotency_key)
                .await?
                .ok_or_else(|| {
                    storage_internal_status(
                        "gc_intent_race",
                        "idempotency race left no intent row to replay",
                    )
                })?;
            if !project_id.is_empty() && winner.project_id != project_id {
                return Err(storage_file_not_found_status("delete_file"));
            }
            return resolve_replayed_gc_intent(svc, winner, &fingerprint, max_attempts).await;
        }
    };
    // The tombstone is committed (metadata is deleted), so emit FILE_DELETED once
    // here — parity with the SOFT path, and never re-emitted on a replay.
    emit_file_deleted_event(svc, req, &prior.project_id).await;
    converge_gc_intent(
        svc,
        &intent_id,
        &backend,
        &bucket,
        &prior.project_id,
        &prior.object_key,
        max_attempts,
    )
    .await
}

/// Replay path for an existing GC intent under a repeated idempotency key. A
/// fingerprint mismatch conflicts fail-closed. A resolved success returns the
/// ORIGINAL outcome without touching bytes or metadata; a still-PENDING or dead-
/// lettered intent is re-driven to convergence (self-healing) without re-emitting
/// the delete event or re-tombstoning.
async fn resolve_replayed_gc_intent(
    svc: &StorageServiceImpl,
    intent: GcIntentRow,
    fingerprint: &str,
    max_attempts: i64,
) -> Result<Response<storage_pb::DeleteFileResponse>, Status> {
    if intent.fingerprint != fingerprint {
        return Err(storage_idempotency_conflict_status());
    }
    if intent.status == "DONE" && intent.outcome_success == Some(true) {
        return Ok(Response::new(storage_pb::DeleteFileResponse {
            success: true,
            error: None,
        }));
    }
    converge_gc_intent(
        svc,
        &intent.intent_id,
        &intent.backend,
        &intent.bucket,
        &intent.project_id,
        &intent.object_key,
        max_attempts,
    )
    .await
}

/// Drive one GC intent's object deletion to convergence. On success the immutable
/// success outcome is recorded and the RPC returns success; on failure the attempt
/// is recorded (intent stays PENDING for the sweep, dead-lettered at the cap) and a
/// retryable error is returned — success is NEVER reported for an unremoved object.
/// Ledger updates are best-effort (the sweep reconciles), so a post-delete ledger
/// blip does not mask that the bytes are gone.
async fn converge_gc_intent(
    svc: &StorageServiceImpl,
    intent_id: &str,
    backend: &str,
    bucket: &str,
    project_id: &str,
    object_key: &str,
    max_attempts: i64,
) -> Result<Response<storage_pb::DeleteFileResponse>, Status> {
    match svc
        .try_delete_object_bytes(backend, bucket, project_id, object_key)
        .await
    {
        Ok(()) => {
            if let Err(err) = svc.mark_gc_intent_done(intent_id).await {
                tracing::warn!(
                    error = %err,
                    intent_id,
                    "storage GC intent bytes removed but marking the ledger DONE failed; the sweep will reconcile"
                );
            }
            Ok(Response::new(storage_pb::DeleteFileResponse {
                success: true,
                error: None,
            }))
        }
        Err(err) => {
            let message = err.message().to_string();
            if let Err(update_err) = svc
                .record_gc_intent_failure(intent_id, &message, max_attempts)
                .await
            {
                tracing::warn!(
                    error = %update_err,
                    intent_id,
                    "storage GC intent byte delete failed AND recording the attempt failed; the sweep will retry"
                );
            }
            Err(object_delete_failed_status(message))
        }
    }
}

/// List a tenant's files with optional metadata filters.
pub(crate) async fn list_files(
    svc: &StorageServiceImpl,
    request: Request<storage_pb::ListFilesRequest>,
) -> Result<Response<storage_pb::ListFilesResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let project_id = resolved_storage_project_scope(&metadata, "")?;
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
        &project_id,
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
