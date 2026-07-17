//! Typed error constructors and request-field validation for `CacheService`.

use tonic::Status;

/// Reject an empty namespace, or one carrying the reserved key separator `:`
/// (which would let a caller escape its tenant/namespace prefix).
pub(crate) fn validate_namespace(namespace: &str) -> Result<String, Status> {
    let ns = namespace.trim();
    if ns.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "namespace is required",
            [("namespace", "must be a non-empty namespace")],
        ));
    }
    if ns.contains(':') || ns.contains(char::is_whitespace) {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "namespace must not contain ':' or whitespace",
            [("namespace", "must not contain ':' or whitespace")],
        ));
    }
    Ok(ns.to_string())
}

/// Reject an empty required string field.
pub(crate) fn require_field(name: &str, value: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!("{name} is required"),
            [(name, "must be a non-empty string")],
        ));
    }
    Ok(v.to_string())
}

#[cfg(not(feature = "redis"))]
pub(crate) fn no_redis_status() -> Status {
    redis_capability_status(
        "service_startup",
        "redis_feature",
        "cache service requires the `redis` feature/backend",
    )
}

pub(crate) fn redis_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "cache",
        operation,
        capability_required,
        message,
    )
}
