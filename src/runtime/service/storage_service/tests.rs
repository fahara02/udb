//! Unit guards for the native `StorageService`: request-body cross-tenant
//! rejection, the field-violation shapes, the typed policy/schema/capability/
//! internal details with their `error-reason` trailers, the enum-token
//! validators, and the `is_public` presence bind. Copied verbatim from the
//! former god file's `tenant_scope_tests` + `is_public_presence_tests`; imports
//! are explicit (no `use super::*`).

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::ir::{ComparisonOp, LogicalFilter, LogicalValue};
use crate::proto::udb::core::storage::services::v1 as storage_pb;
use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::StorageServiceImpl;
use super::config::{
    ALREADY_FINALIZED, DELETE_PRECONDITION_FAILED, IDEMPOTENCY_KEY_CONFLICT, OBJECT_DELETE_FAILED,
    OBJECT_KEY_TRAVERSAL, OBJECT_NOT_PRESENT, UNSUPPORTED_OBJECT_BACKEND, UPLOAD_SIZE_MISMATCH,
};
use super::errors::{
    delete_expected_status_mismatch_status, file_object_bytes_missing_status,
    object_delete_failed_status, object_store_bytes_missing_status,
    object_stream_requires_store_status, storage_capability_status, storage_file_not_found_status,
    storage_idempotency_conflict_status, storage_internal_status, upload_already_finalized_status,
    upload_etag_mismatch_status, upload_size_mismatch_status, uploaded_object_missing_status,
    validate_object_key_filename,
};
use super::model::{file_status_to_db, file_type_to_db, register_is_public_bind};
use super::store::{file_list_filter, file_read_by_id, gc_intent_fingerprint};

fn comparison_value<'a>(filter: &'a LogicalFilter, wanted_field: &str) -> Option<&'a LogicalValue> {
    match filter {
        LogicalFilter::And(clauses) | LogicalFilter::Or(clauses) => clauses
            .iter()
            .find_map(|clause| comparison_value(clause, wanted_field)),
        LogicalFilter::Not(clause) => comparison_value(clause, wanted_field),
        LogicalFilter::Comparison { field, op, value }
            if field == wanted_field && *op == ComparisonOp::Eq =>
        {
            Some(value)
        }
        LogicalFilter::Comparison { .. }
        | LogicalFilter::IsNull(_)
        | LogicalFilter::InList { .. } => None,
    }
}

fn has_null_guard(filter: &LogicalFilter, wanted_field: &str) -> bool {
    match filter {
        LogicalFilter::And(clauses) | LogicalFilter::Or(clauses) => clauses
            .iter()
            .any(|clause| has_null_guard(clause, wanted_field)),
        LogicalFilter::Not(clause) => has_null_guard(clause, wanted_field),
        LogicalFilter::IsNull(field) => field == wanted_field,
        LogicalFilter::Comparison { .. } | LogicalFilter::InList { .. } => false,
    }
}

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

/// M4 — a client filename that would climb out of the `{tenant}/{file}/` object
/// key prefix (traversal / absolute / separator / control byte) is rejected fail
/// closed with the stable `OBJECT_KEY_TRAVERSAL` reason; a plain filename passes.
#[test]
fn object_key_filename_rejects_traversal_and_separators() {
    for bad in [
        "../../etc/passwd",
        "..",
        "/etc/passwd",
        "a/b.png",
        "a\\b.png",
        "C:\\secret",
        ".",
        "with\0null",
        "  ", // empty after trim
    ] {
        let err = validate_object_key_filename(bad)
            .expect_err(&format!("suspicious filename must be rejected: {bad:?}"));
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.metadata()
                .get("error-reason")
                .and_then(|value| value.to_str().ok()),
            Some(OBJECT_KEY_TRAVERSAL),
            "reason trailer for {bad:?}"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "filename");
    }
    // Legitimate bare filenames (including dotfiles and unicode) are accepted.
    for good in [
        "report.pdf",
        "photo.png",
        ".env",
        "résumé.txt",
        "a.b.c.tar.gz",
    ] {
        assert!(
            validate_object_key_filename(good).is_ok(),
            "legit filename must pass: {good:?}"
        );
    }
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
/// Additive DeleteFile mode: an absent/UNSPECIFIED mode resolves to SOFT (so
/// pre-existing clients keep soft-delete), and only an explicit HARD is hard.
#[test]
fn delete_mode_defaults_to_soft() {
    use storage_pb::DeleteMode;
    let is_hard = |mode: i32| {
        matches!(
            DeleteMode::try_from(mode).unwrap_or(DeleteMode::Unspecified),
            DeleteMode::Hard
        )
    };
    assert!(!is_hard(DeleteMode::Unspecified as i32));
    assert!(!is_hard(DeleteMode::Soft as i32));
    assert!(!is_hard(0)); // default proto value
    assert!(is_hard(DeleteMode::Hard as i32));
}

/// The GC-intent fingerprint is stable for the same target and discriminates a
/// different file or mode — the basis of the same-key/different-input conflict.
#[test]
fn gc_intent_fingerprint_is_stable_and_discriminating() {
    let a = gc_intent_fingerprint("file-1", "HARD");
    assert_eq!(a, gc_intent_fingerprint(" file-1 ", " HARD ")); // trimmed
    assert_ne!(a, gc_intent_fingerprint("file-2", "HARD")); // different file
    assert_ne!(a, gc_intent_fingerprint("file-1", "SOFT")); // different mode
}

/// A project-scoped Storage credential must add ownership to every logical
/// object lookup and list predicate. An intentionally tenant-wide credential
/// keeps the historical tenant-only view instead of accidentally matching
/// `project_id IS NULL`.
#[test]
fn storage_file_filters_bind_project_ownership_when_present() {
    let tenant_id = "11111111-1111-4111-8111-111111111111";
    let project_id = "22222222-2222-4222-8222-222222222222";
    let file_id = "33333333-3333-4333-8333-333333333333";

    let project_read = file_read_by_id(tenant_id, project_id, file_id);
    let project_filter = project_read.filter.as_ref().expect("read filter");
    assert_eq!(
        comparison_value(project_filter, "tenant_id"),
        Some(&LogicalValue::String(tenant_id.to_string()))
    );
    assert_eq!(
        comparison_value(project_filter, "project_id"),
        Some(&LogicalValue::String(project_id.to_string()))
    );
    assert_eq!(
        comparison_value(project_filter, "file_id"),
        Some(&LogicalValue::String(file_id.to_string()))
    );
    assert!(has_null_guard(project_filter, "deleted_at"));

    let tenant_read = file_read_by_id(tenant_id, "", file_id);
    let tenant_filter = tenant_read.filter.as_ref().expect("read filter");
    assert_eq!(comparison_value(tenant_filter, "project_id"), None);
    assert!(has_null_guard(tenant_filter, "deleted_at"));

    let project_list = file_list_filter(tenant_id, project_id, "", "", "", "");
    assert_eq!(
        comparison_value(&project_list, "project_id"),
        Some(&LogicalValue::String(project_id.to_string()))
    );
    let tenant_list = file_list_filter(tenant_id, "", "", "", "", "");
    assert_eq!(comparison_value(&tenant_list, "project_id"), None);
}

/// A same-key/different-target HARD delete replay conflicts fail-closed, mirroring
/// the data-plane idempotency-mismatch contract (FailedPrecondition validation on
/// `idempotency_key`, non-retryable, stable reason trailer).
#[test]
fn idempotency_conflict_carries_validation_detail_and_reason() {
    assert_validation_detail_with_reason(
        &storage_idempotency_conflict_status(),
        "idempotency_key",
        "the same idempotency_key was already claimed by a delete with different inputs",
        IDEMPOTENCY_KEY_CONFLICT,
        "idempotency_key was already used for a different delete; reuse a key only to retry an \
         identical delete",
    );
}

/// The `expected_status` optimistic guard mismatch is a fail-closed
/// FailedPrecondition validation naming the offending field + stable reason.
#[test]
fn delete_expected_status_mismatch_carries_validation_detail_and_reason() {
    assert_validation_detail_with_reason(
        &delete_expected_status_mismatch_status("DELETED", "ACTIVE"),
        "expected_status",
        "must equal the file's current status token (or be omitted)",
        DELETE_PRECONDITION_FAILED,
        "delete precondition failed: expected file status \"ACTIVE\" but the file is currently \
         \"DELETED\"",
    );
}

/// A HARD delete whose byte removal failed reports a RETRYABLE error (never
/// success) carrying the stable `OBJECT_DELETE_FAILED` reason — the durable intent
/// guarantees eventual convergence via the sweep.
#[test]
fn object_delete_failed_status_is_retryable_with_reason() {
    let status = object_delete_failed_status("backend timeout");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert_eq!(
        status
            .metadata()
            .get("error-reason")
            .and_then(|value| value.to_str().ok()),
        Some(OBJECT_DELETE_FAILED)
    );
    assert!(status.message().contains("durable object-GC intent"));
    let detail = decode_detail(&status);
    assert_eq!(detail.kind, ErrorKind::Retryable as i32);
    assert!(detail.retryable);
}

#[test]
fn register_is_public_defaults_to_private_when_absent() {
    // register_upload INSERT: absent field defaults to private, never NULL.
    assert!(!register_is_public_bind(None));
    assert!(register_is_public_bind(Some(true)));
    assert!(!register_is_public_bind(Some(false)));
}

/// UDB's project id is an OPAQUE non-empty string everywhere — control plane
/// (`udb_projects.project_id` is TEXT), AuthN principals, policy documents and
/// the DataBroker. Storage alone parsed it as a UUID, so a registered project
/// such as `billing` was rejected at `RegisterUpload` before any object or
/// metadata write, even though that same authenticated identity had already
/// completed broker-mediated CRUD.
///
/// The value must be preserved EXACTLY — never hashed or remapped, which would
/// create a second identity namespace and make policy/audit lineage
/// unverifiable.
#[test]
fn storage_project_scope_accepts_opaque_and_uuid_projects() {
    use tonic::metadata::MetadataMap;

    let md = MetadataMap::new();
    let resolve = |body: &str| super::handlers::resolved_storage_project_scope(&md, body);

    // The reported failure: a valid opaque project.
    assert_eq!(
        resolve("billing").expect("opaque project must be accepted"),
        "billing"
    );

    // Preserved byte-for-byte, not normalized into another namespace.
    assert_eq!(resolve("Ambu-Life_01").unwrap(), "Ambu-Life_01");

    // UUID-shaped projects keep working; their text form is a valid opaque id.
    let uuid = "8f14e45f-ea8d-4b1a-9c2e-1f0a5b7d3c22";
    assert_eq!(resolve(uuid).unwrap(), uuid);

    // A tenant-wide credential with no project keeps tenant-wide behavior.
    assert_eq!(resolve("").unwrap(), "");
    assert_eq!(resolve("   ").unwrap(), "");

    // Bounded to the column width, as a typed InvalidArgument rather than a
    // backend value-too-long surfaced as INTERNAL.
    let too_long = "p".repeat(121);
    let err = resolve(&too_long).expect_err("over-length project must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(resolve(&"p".repeat(120)).unwrap().len(), 120);
}
