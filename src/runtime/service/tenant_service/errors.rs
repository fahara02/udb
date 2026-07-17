//! Typed `Status` constructors for the native `TenantService`: capability /
//! not-found / internal envelopes plus the field-violation validators. Extracted
//! verbatim; each returns the same typed `ErrorDetail` trailer as before.

use tonic::Status;

pub(crate) fn tenant_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "tenant",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn tenant_not_found_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "tenant",
        operation,
        "tenant_not_found",
        "tenant not found",
    )
}

pub(crate) fn tenant_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("tenant", operation, message)
}

pub(crate) fn tenant_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

pub(crate) fn tenant_field_violation(
    field: &'static str,
    description: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field.to_string(), description.into())],
    )
}

pub(crate) fn validate_create_tenant_required_fields(code: &str, name: &str) -> Result<(), Status> {
    let mut fields = Vec::new();
    if code.trim().is_empty() {
        fields.push(("code", "must be a non-empty tenant code"));
    }
    if name.trim().is_empty() {
        fields.push(("name", "must be a non-empty tenant name"));
    }
    if !fields.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "code and name are required",
            fields,
        ));
    }
    Ok(())
}
