//! Typed error constructors and request-field validation for `MeteringService`.

use tonic::Status;

pub(crate) fn metering_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "metering",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn metering_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("metering", operation, message)
}

pub(crate) fn metering_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

pub(crate) fn metering_nonnegative_field(field: &'static str, message: &'static str) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field, "must be greater than or equal to 0")],
    )
}
