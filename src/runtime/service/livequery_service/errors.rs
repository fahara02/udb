//! Typed `Status` constructors for LiveQueryService — all carry an `ErrorDetail`
//! trailer via the shared `executor_utils` helpers.

use tonic::Status;

pub(crate) fn livequery_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "livequery",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn livequery_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

/// Bounded-forwarder backpressure refusal: `ResourceExhausted` with a QUOTA
/// `ErrorDetail`. Used when the broadcast feed lags or the subscriber channel is
/// saturated — the stream is closed rather than buffering without bound.
pub(crate) fn livequery_backpressure_status(
    operation: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::quota_status("livequery", operation, 0, message)
}
