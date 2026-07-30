//! Typed error constructors for `SchedulerService` (capability / not-found /
//! internal / required-field), all carrying the shared typed error detail.

use tonic::Status;

pub(crate) fn scheduler_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "scheduler",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn scheduler_not_found_status(
    operation: &'static str,
    schema_code: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "scheduler",
        operation,
        schema_code,
        message,
    )
}

pub(crate) fn scheduler_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("scheduler", operation, message)
}

pub(crate) fn scheduler_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

/// Reject an invalid explicit per-job timezone at create (fail closed): the
/// `payload` carried a `"timezone"` that is not a resolvable IANA name. Carries a
/// structured `timezone` field violation so the caller knows exactly which input
/// to fix; the bad name is echoed in the message (not the description) so the
/// field descriptor stays a stable `&'static str`.
pub(crate) fn scheduler_invalid_timezone(name: &str) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        format!("timezone is not a valid IANA time zone: {name}"),
        [(
            "timezone",
            "must be a valid IANA time zone name (e.g. America/New_York)",
        )],
    )
}
