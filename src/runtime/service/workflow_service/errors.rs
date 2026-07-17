//! Typed error constructors for `WorkflowService` (capability / policy /
//! not-found / internal / required-field / size / terminal-state), all carrying
//! the shared typed error detail.

use tonic::Status;

pub(crate) fn workflow_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "workflow",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn workflow_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message)
}

pub(crate) fn workflow_not_found_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "workflow",
        operation,
        "workflow_not_found",
        "workflow not found",
    )
}

pub(crate) fn workflow_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("workflow", operation, message)
}

pub(crate) fn workflow_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

pub(crate) fn workflow_size_field(field: &'static str, limit: usize, message: String) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field, format!("must be no larger than {limit} bytes"))],
    )
}

pub(crate) fn workflow_cancel_terminal_status() -> Status {
    workflow_policy_status(
        "cancel_workflow",
        "workflow_terminal_state",
        "workflow is in a terminal state and cannot be cancelled",
    )
}

pub(crate) fn workflow_signal_terminal_status() -> Status {
    workflow_policy_status(
        "signal_workflow",
        "workflow_terminal_state",
        "workflow is in a terminal state and cannot be signalled",
    )
}
