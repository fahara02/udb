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

/// The shared CDC journal — the delta source every replica tails, because the
/// in-process broadcast is fed only on the CDC tailer's leader — cannot be
/// read. `Unavailable` with a RETRYABLE `ErrorDetail` (operation names the
/// failing step) instead of degrading to a broadcast-only or snapshot-only
/// stream, which a client cannot tell apart from "nothing changed". A client
/// retries, resuming with its last delivered event id.
pub(crate) fn livequery_journal_unavailable_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::retryable_status(
        "livequery",
        operation,
        1_000,
        "live query deltas are unavailable: the shared CDC journal cannot be read, \
         so this replica cannot deliver change events; retry the subscription \
         (pass the last delivered event id in x-udb-livequery-resume to resume)",
    )
}
