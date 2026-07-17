//! Unit guards for the native `StorageService`: request-body cross-tenant
//! rejection, the field-violation shapes, the typed policy/schema/capability/
//! internal details with their `error-reason` trailers, the enum-token
//! validators, and the `is_public` presence bind. Copied verbatim from the
//! former god file's `tenant_scope_tests` + `is_public_presence_tests`; imports
//! are explicit (no `use super::*`).

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::proto::udb::core::storage::services::v1 as storage_pb;
use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::StorageServiceImpl;
use super::config::{
    ALREADY_FINALIZED, OBJECT_NOT_PRESENT, UNSUPPORTED_OBJECT_BACKEND, UPLOAD_SIZE_MISMATCH,
};
use super::errors::{
    file_object_bytes_missing_status, object_store_bytes_missing_status,
    object_stream_requires_store_status, storage_capability_status, storage_file_not_found_status,
    storage_internal_status, upload_already_finalized_status, upload_etag_mismatch_status,
    upload_size_mismatch_status, uploaded_object_missing_status,
};
use super::model::{file_status_to_db, file_type_to_db, register_is_public_bind};

fn decode_detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("error-detail trailer present")
        .to_bytes()
        .expect("trailer decodes to bytes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

fn assert_single_field_violation(status: &Status, field: &str, description: &str) {
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, field);
    assert_eq!(detail.field_violations[0].description, description);
}

fn assert_policy_detail_with_reason(
    status: &Status,
    operation: &str,
    policy_decision_id: &str,
    reason: &'static str,
    message: &str,
) {
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(status.message(), message);
    assert_eq!(
        status
            .metadata()
            .get("error-reason")
            .and_then(|value| value.to_str().ok()),
        Some(reason)
    );
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.policy_decision_id, policy_decision_id);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_capability_detail_with_reason(
    status: &Status,
    operation: &str,
    capability_required: &str,
    reason: &'static str,
    message: &str,
) {
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(status.message(), message);
    assert_eq!(
        status
            .metadata()
            .get("error-reason")
            .and_then(|value| value.to_str().ok()),
        Some(reason)
    );
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Capability as i32);
    assert_eq!(detail.backend, "storage");
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.capability_required, capability_required);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_schema_detail(status: &Status, operation: &str, schema_code: &str, message: &str) {
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Schema as i32);
    assert_eq!(detail.backend, "storage");
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.capability_required, schema_code);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Internal as i32);
    assert_eq!(detail.backend, "storage");
    assert_eq!(detail.operation, operation);
    assert!(detail.capability_required.is_empty());
    assert!(detail.policy_decision_id.is_empty());
    assert!(detail.field_violations.is_empty());
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_validation_detail_with_reason(
    status: &Status,
    field: &str,
    description: &str,
    reason: &'static str,
    message: &str,
) {
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(status.message(), message);
    assert_eq!(
        status
            .metadata()
            .get("error-reason")
            .and_then(|value| value.to_str().ok()),
        Some(reason)
    );
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, field);
    assert_eq!(detail.field_violations[0].description, description);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

/// A caller scoped to tenant-a (x-tenant-id) must not operate on tenant-b by
/// putting a foreign tenant_id in the request BODY. The scope guard rejects
/// this BEFORE any pool/DB access, so the test needs no Postgres.
#[tokio::test]
async fn get_file_rejects_cross_tenant_body() {
    let svc = StorageServiceImpl::new(); // no pool, no channels (admit no-op)
    let mut request = Request::new(storage_pb::GetFileRequest {
        tenant_id: "tenant-b".to_string(),
        file_id: "00000000-0000-0000-0000-000000000001".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .get_file(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[test]
fn storage_file_not_found_statuses_carry_schema_detail() {
    for operation in [
        "finalize_upload",
        "get_download_url",
        "download_file",
        "get_file",
        "update_file",
        "delete_file",
    ] {
        assert_schema_detail(
            &storage_file_not_found_status(operation),
            operation,
            "file_not_found",
            "file not found",
        );
    }
}

#[tokio::test]
async fn register_upload_missing_filename_carries_field_violation() {
    let svc = StorageServiceImpl::new(); // no runtime, no channels (admit no-op)
    let mut request = Request::new(storage_pb::RegisterUploadRequest {
        tenant_id: "tenant-a".to_string(),
        filename: "  ".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .register_upload(request)
        .await
        .expect_err("missing filename must be rejected before runtime access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "tenant_id and filename are required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "filename");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty filename"
    );
}

#[test]
fn file_type_and_status_validation_carry_field_violations() {
    let file_type =
        file_type_to_db("directory", "OTHER").expect_err("unknown file type must be rejected");
    assert_eq!(file_type.code(), tonic::Code::InvalidArgument);
    assert_eq!(file_type.message(), "unknown file type: DIRECTORY");
    assert_single_field_violation(
        &file_type,
        "file_type",
        "must be a supported FileType enum value",
    );

    let status =
        file_status_to_db("archived", "PENDING").expect_err("unknown file status must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(status.message(), "unknown file status: ARCHIVED");
    assert_single_field_violation(
        &status,
        "status",
        "must be a supported FileStatus enum value",
    );
}

#[test]
fn upload_already_finalized_carries_policy_detail_and_reason() {
    assert_policy_detail_with_reason(
        &upload_already_finalized_status(),
        "finalize_upload",
        "upload_already_finalized",
        ALREADY_FINALIZED,
        "upload already finalized",
    );
}

#[test]
fn upload_presence_denial_carries_policy_detail_and_reason() {
    assert_policy_detail_with_reason(
        &uploaded_object_missing_status(),
        "finalize_upload",
        "uploaded_object_present",
        OBJECT_NOT_PRESENT,
        "uploaded object is not present in the configured object store",
    );
}

#[test]
fn upload_head_mismatches_carry_validation_detail_and_reason() {
    assert_validation_detail_with_reason(
        &upload_etag_mismatch_status(),
        "etag",
        "must match the uploaded object's store ETag",
        UPLOAD_SIZE_MISMATCH,
        "uploaded object etag does not match",
    );
    assert_validation_detail_with_reason(
        &upload_size_mismatch_status(12, 9),
        "size_bytes",
        "must match the uploaded object's store content length",
        UPLOAD_SIZE_MISMATCH,
        "uploaded object size 12 does not match declared 9",
    );
}

#[test]
fn object_stream_requires_store_carries_capability_detail_and_reason() {
    assert_capability_detail_with_reason(
        &object_stream_requires_store_status(),
        "object_stream",
        "object_store",
        UNSUPPORTED_OBJECT_BACKEND,
        "object byte streaming requires a configured object store",
    );
}

#[test]
fn download_object_absence_carries_policy_detail_and_reason() {
    assert_policy_detail_with_reason(
        &file_object_bytes_missing_status(),
        "download_file",
        "file_object_bytes_present",
        OBJECT_NOT_PRESENT,
        "file has no object bytes",
    );
    assert_policy_detail_with_reason(
        &object_store_bytes_missing_status(),
        "download_file",
        "object_store_bytes_present",
        OBJECT_NOT_PRESENT,
        "object is not present in the configured object store",
    );
}

#[test]
fn storage_missing_runtime_capabilities_carry_typed_detail() {
    for (operation, message) in [
        (
            "native_entity_dispatch",
            "storage service requires runtime native entity dispatch",
        ),
        ("object_stream", "storage service requires runtime"),
    ] {
        let err = storage_capability_status(operation, "runtime_native_entity_dispatch", message);
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(err.message(), message);
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "storage");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, "runtime_native_entity_dispatch");
        assert!(!detail.retryable);
    }
}

#[test]
fn storage_internal_status_carries_typed_detail() {
    assert_internal_detail(
        &storage_internal_status(
            "tenant_size_sum_aggregate",
            "storage usage aggregate failed: database is unavailable",
        ),
        "tenant_size_sum_aggregate",
        "storage usage aggregate failed: database is unavailable",
    );
}

/// `is_public` presence handling after the P4 typed-entity migration.
///
/// `update_file`/`finalize_upload` no longer build raw `COALESCE` SQL; they
/// presence-guard `is_public` *structurally* on the typed `LogicalWrite`
/// field set — an absent proto3-optional `is_public` is never added to
/// `fields` (`if let Some(is_public) = req.is_public { … }`), so the stored
/// visibility is left untouched. That is the v0.3.2 #47 guarantee, now
/// enforced by the type system rather than a hand-built SQL string. The
/// register INSERT differs: the column is NOT NULL, so an absent value must
/// default to private — that decision lives in the pure
/// `register_is_public_bind` helper (still called from `register_upload`),
/// covered here. End-to-end preservation of stored visibility on an absent
/// update is exercised by the env-gated storage live tests.
#[test]
fn register_is_public_defaults_to_private_when_absent() {
    // register_upload INSERT: absent field defaults to private, never NULL.
    assert!(!register_is_public_bind(None));
    assert!(register_is_public_bind(Some(true)));
    assert!(!register_is_public_bind(Some(false)));
}
