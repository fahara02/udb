//! Typed `Status` / `ApiError` constructors for the native `StorageService`:
//! the `error-reason` trailer attachment, the policy / capability / schema /
//! internal envelopes, the presign/finalize/download denials, the field
//! validators, and the `RegisterUpload` required-field guard. Extracted
//! verbatim; each returns the same typed `ErrorDetail`/reason trailer as before.

use tonic::Status;

use crate::proto::udb::core::common::v1 as common_pb;

use super::config::{
    ALREADY_FINALIZED, DELETE_PRECONDITION_FAILED, FINALIZE_IMMUTABLE_MISMATCH,
    IDEMPOTENCY_KEY_CONFLICT, OBJECT_DELETE_FAILED, OBJECT_KEY_TRAVERSAL, OBJECT_NOT_PRESENT,
    REISSUE_REQUIRES_PENDING, UNSUPPORTED_OBJECT_BACKEND, UPLOAD_SIZE_MISMATCH,
    UPLOAD_URL_UNAVAILABLE,
};

/// Attach a stable machine-readable `reason` to a non-OK gRPC `Status` via the
/// `error-reason` metadata trailer (the body is discarded on non-OK statuses).
pub(crate) fn status_with_reason(mut status: Status, reason: &'static str) -> Status {
    status.metadata_mut().insert(
        "error-reason",
        tonic::metadata::MetadataValue::from_static(reason),
    );
    status
}

fn storage_policy_status_with_reason(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
    reason: &'static str,
) -> Status {
    status_with_reason(
        crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message),
        reason,
    )
}

pub(crate) fn upload_already_finalized_status() -> Status {
    storage_policy_status_with_reason(
        "finalize_upload",
        "upload_already_finalized",
        "upload already finalized",
        ALREADY_FINALIZED,
    )
}

/// Only a PENDING upload can have its presigned PUT URL reissued: an ACTIVE
/// (finalized) or removed file is not resumable. Fail closed so a reissue never
/// hands out a fresh upload URL for an object that is not awaiting bytes.
pub(crate) fn reissue_requires_pending_status(current_status: &str) -> Status {
    storage_policy_status_with_reason(
        "reissue_upload_url",
        "reissue_requires_pending",
        format!(
            "cannot reissue an upload URL for a {current_status} file; only a PENDING upload can \
             be resumed"
        ),
        REISSUE_REQUIRES_PENDING,
    )
}

pub(crate) fn uploaded_object_missing_status() -> Status {
    storage_policy_status_with_reason(
        "finalize_upload",
        "uploaded_object_present",
        "uploaded object is not present in the configured object store",
        OBJECT_NOT_PRESENT,
    )
}

pub(crate) fn upload_etag_mismatch_status() -> Status {
    status_with_reason(
        crate::runtime::executor_utils::failed_precondition_fields(
            "uploaded object etag does not match",
            [("etag", "must match the uploaded object's store ETag")],
        ),
        UPLOAD_SIZE_MISMATCH,
    )
}

/// Finalize supplied a value for a field that was fixed at `register_upload`
/// (reference id/type, content/file type, visibility). Finalize is an
/// ownership-preserving lifecycle transition — the caller may re-assert the
/// registered value, but any change is rejected fail-closed so a finalize-scoped
/// caller cannot re-point ownership/reference or escalate visibility on an
/// existing PENDING row.
pub(crate) fn finalize_immutable_mismatch_status(
    field: &'static str,
    registered: &str,
    supplied: &str,
) -> Status {
    status_with_reason(
        crate::runtime::executor_utils::failed_precondition_fields(
            format!(
                "finalize cannot change {field}: it was established as \"{registered}\" at \
                 register_upload but finalize supplied \"{supplied}\""
            ),
            [(
                field,
                "must equal the value established at register_upload (or be omitted)",
            )],
        ),
        FINALIZE_IMMUTABLE_MISMATCH,
    )
}

/// HARD `DeleteFile` replayed an idempotency key first claimed by a delete with a
/// DIFFERENT target. Reused fail-closed (mirrors the data-plane idempotency-key
/// mismatch: a `FailedPrecondition` validation on `idempotency_key`, never a
/// replay of the mismatched outcome), with the stable `IDEMPOTENCY_KEY_CONFLICT`
/// reason on the trailer.
pub(crate) fn storage_idempotency_conflict_status() -> Status {
    status_with_reason(
        crate::runtime::executor_utils::failed_precondition_fields(
            "idempotency_key was already used for a different delete; reuse a key only to retry \
             an identical delete",
            [(
                "idempotency_key",
                "the same idempotency_key was already claimed by a delete with different inputs",
            )],
        ),
        IDEMPOTENCY_KEY_CONFLICT,
    )
}

/// DeleteFile's `expected_status` optimistic guard did not match the file's
/// current status token — refused fail-closed so a stale client cannot delete a
/// file that changed under it.
pub(crate) fn delete_expected_status_mismatch_status(current: &str, expected: &str) -> Status {
    status_with_reason(
        crate::runtime::executor_utils::failed_precondition_fields(
            format!(
                "delete precondition failed: expected file status \"{expected}\" but the file is \
                 currently \"{current}\""
            ),
            [(
                "expected_status",
                "must equal the file's current status token (or be omitted)",
            )],
        ),
        DELETE_PRECONDITION_FAILED,
    )
}

/// HARD `DeleteFile` committed the tombstone + durable GC intent but the object
/// bytes could not be removed. Success is NOT reported; the retryable status tells
/// the client the delete is durably recorded and will converge via the sweep.
pub(crate) fn object_delete_failed_status(message: impl Into<String>) -> Status {
    status_with_reason(
        crate::runtime::executor_utils::retryable_status(
            "storage",
            "object_delete",
            crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
            format!(
                "file metadata was deleted and a durable object-GC intent recorded, but object \
                 byte removal failed and will be retried by the GC sweep: {}",
                message.into()
            ),
        ),
        OBJECT_DELETE_FAILED,
    )
}

pub(crate) fn upload_size_mismatch_status(head_size: i64, declared_size: i64) -> Status {
    status_with_reason(
        crate::runtime::executor_utils::failed_precondition_fields(
            format!("uploaded object size {head_size} does not match declared {declared_size}"),
            [(
                "size_bytes",
                "must match the uploaded object's store content length",
            )],
        ),
        UPLOAD_SIZE_MISMATCH,
    )
}

/// Build an `ApiError` carrying `UPLOAD_URL_UNAVAILABLE` for the degraded/failed
/// presign branches of `register_upload` (kept as `Response::Ok` so the persisted
/// PENDING row + quota reservation survive).
pub(crate) fn api_error_upload_url_unavailable(
    message: impl Into<String>,
    retryable: bool,
) -> common_pb::ApiError {
    common_pb::ApiError {
        code: UPLOAD_URL_UNAVAILABLE.to_string(),
        message: message.into(),
        retryable,
        ..Default::default()
    }
}

pub(crate) fn storage_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "storage",
        operation,
        capability_required,
        message,
    )
}

fn storage_capability_status_with_reason(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
    reason: &'static str,
) -> Status {
    status_with_reason(
        storage_capability_status(operation, capability_required, message),
        reason,
    )
}

pub(crate) fn storage_file_not_found_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "storage",
        operation,
        "file_not_found",
        "file not found",
    )
}

pub(crate) fn storage_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("storage", operation, message)
}

pub(crate) fn object_stream_requires_store_status() -> Status {
    storage_capability_status_with_reason(
        "object_stream",
        "object_store",
        "object byte streaming requires a configured object store",
        UNSUPPORTED_OBJECT_BACKEND,
    )
}

pub(crate) fn file_object_bytes_missing_status() -> Status {
    storage_policy_status_with_reason(
        "download_file",
        "file_object_bytes_present",
        "file has no object bytes",
        OBJECT_NOT_PRESENT,
    )
}

pub(crate) fn object_store_bytes_missing_status() -> Status {
    storage_policy_status_with_reason(
        "download_file",
        "object_store_bytes_present",
        "object is not present in the configured object store",
        OBJECT_NOT_PRESENT,
    )
}

/// Normalize a file-type string to the canonical SHORT stored token (e.g.
/// "IMAGE"), accepting either the short or the proto-prefixed ("FILE_TYPE_IMAGE")
/// form. Empty → `default`. Unknown non-empty input is rejected so it never
/// silently overflows VARCHAR(20) or reads back as Unspecified. Storing the
/// short form keeps every value within VARCHAR(20) and makes write/read/filter
/// round-trip.
pub(crate) fn storage_field_violation<F, D, M>(field: F, description: D, message: M) -> Status
where
    F: Into<String>,
    D: Into<String>,
    M: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

/// Fail-closed guard against object-key path traversal. The register mint builds
/// the canonical key as `"{tenant_id}/{file_id}/{filename}"`, so a client
/// `filename` carrying its own path separators, a `..` traversal sequence, an
/// absolute/drive root, or control bytes could climb out of the tenant/file
/// prefix and read or clobber another tenant's bytes on a path-addressed backend
/// (local FS / MinIO gateway). The filename must be a single bare path segment;
/// anything else is rejected before any row or object is created.
pub(crate) fn validate_object_key_filename(filename: &str) -> Result<(), Status> {
    let name = filename.trim();
    let suspicious = name.is_empty()
        || name == "."
        || name.contains("..")
        || name.contains('/')
        || name.contains('\\')
        || name.contains(':')
        || name.chars().any(char::is_control);
    if suspicious {
        return Err(status_with_reason(
            crate::runtime::executor_utils::invalid_argument_fields(
                "filename must be a single path segment without traversal or separators",
                [(
                    "filename",
                    "must not contain '/', '\\', ':', '..', or control characters",
                )],
            ),
            OBJECT_KEY_TRAVERSAL,
        ));
    }
    Ok(())
}

pub(crate) fn validate_register_upload_required_fields(
    tenant_id: &str,
    filename: &str,
) -> Result<(), Status> {
    let mut fields = Vec::new();
    if tenant_id.trim().is_empty() {
        fields.push(("tenant_id", "must be a non-empty tenant id"));
    }
    if filename.trim().is_empty() {
        fields.push(("filename", "must be a non-empty filename"));
    }
    if !fields.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "tenant_id and filename are required",
            fields,
        ));
    }
    Ok(())
}
