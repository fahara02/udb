//! Typed `Status` constructors for the native `NotificationService`: capability /
//! policy / not-found / internal envelopes, the field-violation validator, and the
//! `error-reason` attachment helper (`status_with_reason`) that rides a stable
//! machine-readable reason in trailing metadata. Extracted verbatim; each returns
//! the same typed `ErrorDetail` trailer as before.

use tonic::Status;

use super::config::{NOT_RETRYABLE_STATE, TEMPLATE_NOT_FOUND};

/// Attach a stable machine-readable `reason` (and optional metadata pairs) to a
/// `Status` without changing its gRPC `Code`. The reason rides in the trailing
/// `error-reason` metadata key; extra `(key, value)` pairs (e.g. the offending
/// variable name) ride alongside it. Bad ASCII keys/values are skipped so this
/// can never itself fail.
pub(crate) fn status_with_reason(
    mut status: Status,
    reason: &str,
    extra: &[(&str, &str)],
) -> Status {
    let md = status.metadata_mut();
    if let Ok(value) = reason.parse::<tonic::metadata::MetadataValue<_>>() {
        md.insert("error-reason", value);
    }
    for (key, raw) in extra {
        if let (Ok(name), Ok(value)) = (
            key.parse::<tonic::metadata::MetadataKey<_>>(),
            raw.parse::<tonic::metadata::MetadataValue<_>>(),
        ) {
            md.insert(name, value);
        }
    }
    status
}

pub(crate) fn notification_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "notification",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn notification_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("notification", operation, message)
}

fn notification_policy_status_with_reason(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: &'static str,
    reason: &'static str,
) -> Status {
    status_with_reason(
        crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message),
        reason,
        &[],
    )
}

fn notification_policy_status_with_code(
    code: tonic::Code,
    operation: &'static str,
    policy_decision_id: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        code,
        operation,
        policy_decision_id,
        message,
    )
}

pub(crate) fn notification_tenant_metadata_required_status(operation: &'static str) -> Status {
    notification_policy_status_with_code(
        tonic::Code::PermissionDenied,
        operation,
        "tenant_metadata_required",
        "tenant-scoped metadata is required",
    )
}

pub(crate) fn notification_not_retryable_status() -> Status {
    notification_policy_status_with_reason(
        "retry_notification",
        "notification_not_retryable",
        "notification not found or not in a retryable (FAILED) state",
        NOT_RETRYABLE_STATE,
    )
}

pub(crate) fn notification_schema_not_found_status(
    operation: &'static str,
    schema_code: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "notification",
        operation,
        schema_code,
        message,
    )
}

pub(crate) fn notification_log_not_found_status(operation: &'static str) -> Status {
    // NotFound (not PermissionDenied) so a cross-tenant caller cannot use the
    // error to probe whether another tenant's log id exists.
    notification_schema_not_found_status(
        operation,
        "notification_log_not_found",
        "notification log not found for this tenant",
    )
}

pub(crate) fn notification_template_not_found_status(
    operation: &'static str,
    message: impl Into<String>,
) -> Status {
    status_with_reason(
        notification_schema_not_found_status(operation, "notification_template_not_found", message),
        TEMPLATE_NOT_FOUND,
        &[],
    )
}

pub(crate) fn notification_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}
