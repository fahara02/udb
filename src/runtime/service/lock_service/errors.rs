//! Typed `Status` constructors for the native `LockService`: capability / policy /
//! schema / internal envelopes, the fail-closed fencing-token refusal, plus the
//! lock-identity validator and the pure fencing-token freshness check. Extracted
//! verbatim; each returns the same typed `ErrorDetail` trailer as before.

use tonic::Status;

pub(crate) fn lock_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "lock",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn lock_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("lock", operation, message)
}

/// Fail-closed refusal shape for the fencing path when a monotone token cannot
/// be established. Wall-clock seconds are NOT an acceptable fallback: they
/// collide within a second and regress across clock steps, so a time-derived
/// token could duplicate or undercut an already-issued token and let a
/// fenced-off holder write again. Per-lock tokens are now sourced from the
/// durable lock row (a store read that fails closed on its own), so no live call
/// site remains — the constructor is retained as the canonical fail-closed shape
/// asserted by the unit tests.
#[allow(dead_code)]
pub(crate) fn fencing_token_unavailable_status() -> Status {
    lock_capability_status(
        "lock_fencing",
        "canonical_store_monotone_counter",
        "lock fencing requires the canonical-store monotone token source; \
         refusing to mint a wall-clock fencing token",
    )
}

pub(crate) fn lock_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    lock_policy_status_with_code(
        tonic::Code::FailedPrecondition,
        operation,
        policy_decision_id,
        message,
    )
}

pub(crate) fn lock_policy_status_with_code(
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

pub(crate) fn lock_not_held_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "lock",
        operation,
        "lock_not_held",
        "lock not held",
    )
}

pub(crate) fn lock_already_held_status() -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::AlreadyExists,
        "lock",
        "acquire_lock",
        "lock_already_held",
        "lock is already held by another owner",
    )
}

pub(crate) fn validate_lock_identity(
    lock_name: &str,
    owner_id: &str,
) -> Result<(String, String), Status> {
    let lock_name = lock_name.trim();
    let owner_id = owner_id.trim();
    let mut fields = Vec::new();
    if lock_name.is_empty() {
        fields.push(("lock_name", "must be a non-empty lock name"));
    }
    if owner_id.is_empty() {
        fields.push(("owner_id", "must be a non-empty owner id"));
    }
    if !fields.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "lock_name and owner_id are required",
            fields,
        ));
    }
    Ok((lock_name.to_string(), owner_id.to_string()))
}

/// Reject a stale fencing token: a holder presenting a token lower than the
/// stored one has been fenced off (a newer holder superseded it). Equal or
/// greater is accepted (the current holder). Pure — unit-tested without PG.
pub(crate) fn ensure_fencing_token_fresh(provided: i64, stored: i64) -> Result<(), Status> {
    if provided < stored {
        return Err(stale_fencing_token_status(provided, stored));
    }
    Ok(())
}

pub(crate) fn stale_fencing_token_status(provided: i64, stored: i64) -> Status {
    lock_policy_status(
        "lock_fencing",
        "stale_fencing_token",
        format!("stale fencing token {provided}; the lock has been fenced to token {stored}"),
    )
}

pub(crate) fn lock_lease_lost_status() -> Status {
    lock_policy_status(
        "renew_lock",
        "lock_lease_lost",
        "lock lease lost; it is now held by another owner",
    )
}

pub(crate) fn lock_held_by_different_owner_status(operation: &'static str) -> Status {
    lock_policy_status_with_code(
        tonic::Code::PermissionDenied,
        operation,
        "lock_owner_mismatch",
        "lock is held by a different owner",
    )
}
