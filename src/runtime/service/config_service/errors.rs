//! Typed error constructors and request-field validation for `ConfigService`.

use tonic::Status;

use super::config::MAX_EVALUATE_KEYS;

/// Capability-class status for a missing config dependency (e.g. no runtime).
pub(crate) fn config_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "config",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn require_flag_key(flag_key: &str) -> Result<String, Status> {
    let flag_key = flag_key.trim();
    if flag_key.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "flag_key is required",
            [("flag_key", "must be a non-empty flag key")],
        ));
    }
    Ok(flag_key.to_string())
}

pub(crate) fn ensure_evaluate_key_limit(keys_len: usize) -> Result<(), Status> {
    if keys_len > MAX_EVALUATE_KEYS {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!("too many keys (max {MAX_EVALUATE_KEYS})"),
            [("keys", "must contain at most 256 keys")],
        ));
    }
    Ok(())
}
