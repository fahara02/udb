//! Typed `Status` constructors for the native `WebhookService`: capability /
//! policy / not-found / internal envelopes, the field-violation validators, and
//! the delivery-time SSRF policy statuses. Extracted verbatim; each returns the
//! same typed `ErrorDetail` trailer as before.

use std::net::IpAddr;

use tonic::Status;

pub(crate) fn webhook_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "webhook",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn webhook_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message)
}

pub(crate) fn webhook_endpoint_not_found_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "webhook",
        operation,
        "webhook_endpoint_not_found",
        "webhook endpoint not found",
    )
}

pub(crate) fn webhook_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("webhook", operation, message)
}

pub(crate) fn webhook_host_unresolved_status(host: &str, err: impl std::fmt::Display) -> Status {
    webhook_policy_status(
        "webhook_delivery_ssrf",
        "webhook_host_unresolved",
        format!("webhook host {host} did not resolve: {err}"),
    )
}

pub(crate) fn webhook_host_blocked_address_status(host: &str, ip: IpAddr) -> Status {
    webhook_policy_status(
        "webhook_delivery_ssrf",
        "webhook_host_blocked_address",
        format!(
            "webhook host {host} resolved to a blocked address {ip} (SSRF/DNS-rebinding blocked)"
        ),
    )
}

pub(crate) fn webhook_host_no_addresses_status(host: &str) -> Status {
    webhook_policy_status(
        "webhook_delivery_ssrf",
        "webhook_host_no_addresses",
        format!("webhook host {host} resolved to no addresses"),
    )
}

pub(crate) fn webhook_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

pub(crate) fn webhook_url_violation(
    description: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [("url".to_string(), description.into())],
    )
}
