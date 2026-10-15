//! Typed `Status` constructors for the native `MessagingService`. Every refusal
//! carries an `ErrorDetail` trailer (backend `messaging`), so SDKs branch on the
//! typed fields, never on message text.

use tonic::Status;

pub(crate) fn messaging_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "messaging",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn messaging_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("messaging", operation, message)
}

/// A store failure. Recognised SQLSTATEs (unique violation, ...) keep their
/// specific code; anything else is `Internal` naming the operation.
pub(crate) fn messaging_store_status(operation: &'static str, err: &sqlx::Error) -> Status {
    crate::runtime::executor_utils::sqlx_error_to_status(
        &format!("messaging {operation} failed"),
        err,
    )
}

pub(crate) fn messaging_policy_status(
    code: tonic::Code,
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        code,
        operation,
        policy_decision_id,
        message,
    )
}

pub(crate) fn messaging_not_found_status(
    operation: &'static str,
    schema_code: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "messaging",
        operation,
        schema_code,
        message,
    )
}

pub(crate) fn messaging_already_exists_status(
    operation: &'static str,
    schema_code: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::AlreadyExists,
        "messaging",
        operation,
        schema_code,
        message,
    )
}

pub(crate) fn invalid_field(field: &str, description: impl Into<String>) -> Status {
    let description = description.into();
    crate::runtime::executor_utils::invalid_argument_fields(
        format!("{field}: {description}"),
        [(field.to_string(), description)],
    )
}

/// An MLS object failed validation (bad encoding, bad signature, wrong
/// credential, wrong group). `InvalidArgument` with the field it came from.
pub(crate) fn mls_invalid(field: &str, err: impl std::fmt::Display) -> Status {
    invalid_field(field, format!("rejected by MLS validation: {err}"))
}

/// The submitted commit was built on an epoch the group has left. The caller
/// must fetch the commits it missed, re-base, and retry. `FailedPrecondition`
/// with `missing.current_epoch` so the SDK can re-base without parsing text.
pub(crate) fn stale_epoch_status(operation: &'static str, submitted: i64, current: i64) -> Status {
    let mut missing = std::collections::HashMap::new();
    missing.insert("current_epoch".to_string(), current.to_string());
    let detail = crate::proto::ErrorDetail {
        backend: "messaging".to_string(),
        operation: operation.to_string(),
        kind: crate::proto::ErrorKind::Conflict as i32,
        missing,
        ..Default::default()
    };
    crate::runtime::executor_utils::status_with_typed_detail(
        tonic::Code::FailedPrecondition,
        format!("epoch {submitted} is stale; the group is at epoch {current}"),
        detail,
    )
}

pub(crate) fn device_not_found_status(operation: &'static str, device_id: &str) -> Status {
    messaging_not_found_status(
        operation,
        "messaging_device_not_found",
        format!("device '{device_id}' is not registered in this tenant"),
    )
}

pub(crate) fn device_not_active_status(operation: &'static str, device_id: &str) -> Status {
    messaging_policy_status(
        tonic::Code::FailedPrecondition,
        operation,
        "messaging_device_revoked",
        format!("device '{device_id}' is revoked"),
    )
}

pub(crate) fn group_not_found_status(operation: &'static str, group_id: &str) -> Status {
    messaging_not_found_status(
        operation,
        "messaging_group_not_found",
        format!("group '{group_id}' does not exist in this tenant"),
    )
}

pub(crate) fn not_a_member_status(operation: &'static str, device_id: &str) -> Status {
    messaging_policy_status(
        tonic::Code::PermissionDenied,
        operation,
        "messaging_not_a_member",
        format!("device '{device_id}' is not a member of the group"),
    )
}

pub(crate) fn device_owner_mismatch_status(operation: &'static str) -> Status {
    messaging_policy_status(
        tonic::Code::PermissionDenied,
        operation,
        "messaging_device_owner_mismatch",
        "the device belongs to a different principal",
    )
}

pub(crate) fn backup_not_found_status(operation: &'static str, backup_id: &str) -> Status {
    messaging_not_found_status(
        operation,
        "messaging_backup_not_found",
        format!("no messaging backup '{backup_id}' is stored for this principal"),
    )
}

/// A compare-and-swap backup write lost: another device replaced the backup
/// first. `FailedPrecondition` with `missing.current_version` (0 = none stored),
/// so the SDK can re-read and decide without parsing text.
pub(crate) fn backup_version_conflict_status(
    operation: &'static str,
    expected: i64,
    current: i64,
) -> Status {
    let mut missing = std::collections::HashMap::new();
    missing.insert("current_version".to_string(), current.to_string());
    let detail = crate::proto::ErrorDetail {
        backend: "messaging".to_string(),
        operation: operation.to_string(),
        kind: crate::proto::ErrorKind::Conflict as i32,
        missing,
        ..Default::default()
    };
    crate::runtime::executor_utils::status_with_typed_detail(
        tonic::Code::FailedPrecondition,
        format!("backup version {expected} is stale; the stored version is {current}"),
        detail,
    )
}
