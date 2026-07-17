//! Typed `Status` constructors and the `error-reason` trailer helper for the
//! native `AssetService`: the capability / schema / internal envelopes, the
//! field-violation helpers, the native-state crypto failures, and the
//! active-storage-file validator. Extracted verbatim from the former god file;
//! each returns the same typed `ErrorDetail` trailer / trailers as before.

use tonic::Status;

/// Attach a stable machine-readable `reason` to a non-OK gRPC `Status` via the
/// `error-reason` metadata trailer — uniform with the storage/webrtc/notification
/// services (a non-OK status is trailers-only, so the sub-code rides a trailer).
pub(crate) fn status_with_reason(mut status: Status, reason: &'static str) -> Status {
    status.metadata_mut().insert(
        "error-reason",
        tonic::metadata::MetadataValue::from_static(reason),
    );
    status
}

pub(crate) fn asset_invalid_field(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field.into(), description.into())],
    )
}

pub(crate) fn asset_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    asset_invalid_field(field, description, message)
}

pub(crate) fn asset_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "asset",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn asset_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("asset", operation, message)
}

pub(crate) fn asset_schema_not_found_status(
    operation: &'static str,
    schema_code: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "asset",
        operation,
        schema_code,
        message,
    )
}

pub(crate) fn native_state_encryption_failed_status(err: impl std::fmt::Display) -> Status {
    asset_capability_status(
        "native_state_encrypt",
        "native_state_encryption",
        format!("native-state encryption failed: {err}"),
    )
}

pub(crate) fn native_state_decryption_failed_status(err: impl std::fmt::Display) -> Status {
    asset_capability_status(
        "native_state_decrypt",
        "native_state_encryption",
        format!("native-state decryption failed: {err}"),
    )
}

pub(crate) fn active_storage_file_required_status() -> Status {
    asset_invalid_field(
        "file_id",
        "must reference an active storage file owned by this tenant",
        "file_id does not reference an active storage file owned by this tenant",
    )
}
