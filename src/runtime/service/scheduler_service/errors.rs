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
