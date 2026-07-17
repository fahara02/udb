//! Unit guards for the native `WebhookService`: the WRITE-time SSRF guard across
//! both IP families, field-violation shapes, delivery-time SSRF policy details,
//! the pure IP classifier boundaries, tenant-bound subscription matching, HMAC
//! signature round-trips, cross-tenant body rejection, and the typed
//! capability/not-found/internal details. Copied verbatim from the former god
//! file; imports are explicit (no `use super::*`).

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::proto::udb::core::webhook::services::v1 as webhook_pb;
use crate::proto::udb::core::webhook::services::v1::webhook_service_server::WebhookService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::WebhookServiceImpl;
use super::config::{DELIVERY_BACKOFF_BASE, DELIVERY_BACKOFF_CAP, delivery_backoff};
use super::errors::{
    webhook_capability_status, webhook_endpoint_not_found_status,
    webhook_host_blocked_address_status, webhook_host_no_addresses_status,
    webhook_host_unresolved_status, webhook_internal_status,
};
use super::security::{
    ip_is_blocked, sign_webhook_body, validate_webhook_target_url,
    webhook_event_matches_endpoint_scope,
};

fn decode_detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("error-detail trailer present")
        .to_bytes()
        .expect("trailer decodes to bytes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

fn assert_url_field_violation(status: &Status, description: &str) {
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "url");
    assert_eq!(detail.field_violations[0].description, description);
}

fn assert_policy_detail(status: &Status, policy_decision_id: &str) {
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.operation, "webhook_delivery_ssrf");
    assert_eq!(detail.policy_decision_id, policy_decision_id);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_schema_not_found_detail(status: &Status, operation: &str) {
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert_eq!(status.message(), "webhook endpoint not found");
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Schema as i32);
    assert_eq!(detail.backend, "webhook");
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.capability_required, "webhook_endpoint_not_found");
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Internal as i32);
    assert_eq!(detail.backend, "webhook");
    assert_eq!(detail.operation, operation);
    assert!(detail.capability_required.is_empty());
    assert!(detail.policy_decision_id.is_empty());
    assert!(detail.field_violations.is_empty());
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

/// Every private/loopback/link-local/CGNAT/unspecified range — in both IP
/// families and as raw-IP literal targets — is rejected by the WRITE-time SSRF
/// guard; a public https target with a hostname is accepted.
#[test]
fn ssrf_guard_rejects_private_ranges() {
    let blocked = [
        "https://127.0.0.1/hook",         // 127/8 loopback
        "https://127.255.255.254/hook",   // 127/8 (upper)
        "https://10.0.0.1/hook",          // 10/8
        "https://10.255.255.255/hook",    // 10/8 (upper)
        "https://172.16.0.1/hook",        // 172.16/12 (lower)
        "https://172.31.255.254/hook",    // 172.16/12 (upper)
        "https://192.168.1.1/hook",       // 192.168/16
        "https://169.254.169.254/hook",   // 169.254/16 (cloud metadata)
        "https://100.64.0.1/hook",        // 100.64/10 CGNAT (lower)
        "https://100.127.255.254/hook",   // 100.64/10 CGNAT (upper)
        "https://0.0.0.0/hook",           // 0/8 + unspecified
        "https://0.1.2.3/hook",           // 0/8 "this network"
        "https://[::1]/hook",             // IPv6 loopback
        "https://[::]/hook",              // IPv6 unspecified
        "https://[fc00::1]/hook",         // fc00::/7 ULA
        "https://[fd12:3456::1]/hook",    // fc00::/7 ULA (fd)
        "https://[fe80::1]/hook",         // fe80::/10 link-local
        "https://[::ffff:10.0.0.1]/hook", // IPv4-mapped private
        "https://localhost/hook",         // local hostname
    ];
    for url in blocked {
        let err =
            validate_webhook_target_url(url).expect_err(&format!("SSRF guard must reject {url}"));
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "wrong code for {url}"
        );
    }
    // Non-https is rejected even for a public host.
    assert_eq!(
        validate_webhook_target_url("http://hooks.example.com/x")
            .expect_err("cleartext http must be rejected")
            .code(),
        tonic::Code::InvalidArgument
    );
    // A public https hostname target is accepted (DNS is checked at delivery).
    validate_webhook_target_url("https://hooks.example.com/events")
        .expect("public https target should be accepted at write time");
}

#[test]
fn webhook_url_validation_carries_field_violations() {
    let non_https = validate_webhook_target_url("http://hooks.example.com/x")
        .expect_err("cleartext http must fail");
    assert_eq!(
        non_https.message(),
        "webhook url must use https (cleartext http and non-http schemes are rejected)"
    );
    assert_url_field_violation(&non_https, "must use https scheme");

    let malformed_ipv6 = validate_webhook_target_url("https://[::1/hook")
        .expect_err("malformed IPv6 host must fail");
    assert_eq!(
        malformed_ipv6.message(),
        "webhook url has a malformed IPv6 host"
    );
    assert_url_field_violation(
        &malformed_ipv6,
        "must contain a well-formed bracketed IPv6 host",
    );

    let missing_host =
        validate_webhook_target_url("https:///hook").expect_err("missing host must fail");
    assert_eq!(
        missing_host.message(),
        "webhook url must include a valid host"
    );
    assert_url_field_violation(&missing_host, "must include a valid external host");

    let private = validate_webhook_target_url("https://10.0.0.1/hook")
        .expect_err("private IP target must fail");
    assert_eq!(
        private.message(),
        "webhook url host 10.0.0.1 resolves to a private/loopback/link-local address (SSRF blocked)"
    );
    assert_url_field_violation(
        &private,
        "must not target private, loopback, link-local, CGNAT, unspecified, multicast, or reserved IP ranges",
    );

    let localhost = validate_webhook_target_url("https://localhost/hook")
        .expect_err("localhost target must fail");
    assert_eq!(
        localhost.message(),
        "webhook url host localhost is not an allowed external target (SSRF blocked)"
    );
    assert_url_field_violation(&localhost, "must not target localhost hostnames");
}

#[test]
fn delivery_time_ssrf_denials_carry_policy_detail() {
    let unresolved = webhook_host_unresolved_status("hooks.example.test", "dns timeout");
    assert_eq!(
        unresolved.message(),
        "webhook host hooks.example.test did not resolve: dns timeout"
    );
    assert_policy_detail(&unresolved, "webhook_host_unresolved");

    let blocked =
        webhook_host_blocked_address_status("hooks.example.test", "10.0.0.1".parse().unwrap());
    assert_eq!(
        blocked.message(),
        "webhook host hooks.example.test resolved to a blocked address 10.0.0.1 (SSRF/DNS-rebinding blocked)"
    );
    assert_policy_detail(&blocked, "webhook_host_blocked_address");

    let empty = webhook_host_no_addresses_status("hooks.example.test");
    assert_eq!(
        empty.message(),
        "webhook host hooks.example.test resolved to no addresses"
    );
    assert_policy_detail(&empty, "webhook_host_no_addresses");
}

/// Direct range coverage of the pure IP classifier (boundaries included).
#[test]
fn ip_classifier_boundaries() {
    let blocked: &[&str] = &[
        "10.0.0.0",
        "10.255.255.255",
        "172.16.0.0",
        "172.31.255.255",
        "192.168.0.0",
        "192.168.255.255",
        "127.0.0.1",
        "169.254.0.1",
        "100.64.0.0",
        "100.127.255.255",
        "0.0.0.0",
        "::1",
        "::",
        "fc00::",
        "fdff::1",
        "fe80::abcd",
    ];
    for ip in blocked {
        assert!(ip_is_blocked(ip.parse().unwrap()), "{ip} should be blocked");
    }
    let allowed: &[&str] = &[
        "8.8.8.8",
        "1.1.1.1",
        "203.0.113.10",
        "100.63.255.255",
        "100.128.0.0",
        "172.15.255.255",
        "172.32.0.1",
        "2606:4700:4700::1111",
    ];
    for ip in allowed {
        assert!(
            !ip_is_blocked(ip.parse().unwrap()),
            "{ip} should be allowed"
        );
    }
}

/// A tenant-B event never matches a tenant-A endpoint scope (fail closed,
/// tenant BOUND to the endpoint — never pattern-only). Same tenant + matching
/// topic delivers; a tenant-less event is dropped.
#[test]
fn delivery_scope_is_tenant_bound() {
    let pattern = "udb.*";
    let tenant_a_event = serde_json::json!({ "tenant_id": "tenant-a", "x": 1 });
    let tenant_b_event = serde_json::json!({ "tenant_id": "tenant-b", "x": 1 });
    let tenantless = serde_json::json!({ "x": 1 });

    // Same tenant + subscribed topic → delivered.
    assert!(webhook_event_matches_endpoint_scope(
        "tenant-a",
        pattern,
        "udb.invoice.created.v1",
        &tenant_a_event
    ));
    // Cross-tenant event must NEVER reach tenant-a's endpoint.
    assert!(!webhook_event_matches_endpoint_scope(
        "tenant-a",
        pattern,
        "udb.invoice.created.v1",
        &tenant_b_event
    ));
    // Tenant-less event is dropped (no pattern-only fan-out, the fail-open trap).
    assert!(!webhook_event_matches_endpoint_scope(
        "tenant-a",
        pattern,
        "udb.invoice.created.v1",
        &tenantless
    ));
    // Subscribed tenant but non-matching topic → not delivered.
    assert!(!webhook_event_matches_endpoint_scope(
        "tenant-a",
        "udb.payment.*",
        "udb.invoice.created.v1",
        &tenant_a_event
    ));
    // An empty pattern subscribes to nothing (fail closed).
    assert!(!webhook_event_matches_endpoint_scope(
        "tenant-a",
        "",
        "udb.invoice.created.v1",
        &tenant_a_event
    ));
}

/// The `X-Udb-Signature` verifies against an in-test receiver recomputing the
/// HMAC with the shared secret; a wrong secret or tampered body does not.
#[test]
fn hmac_signature_round_trips() {
    let secret = "shhh-per-endpoint-secret";
    let body = br#"{"tenant_id":"tenant-a","event":"invoice.created"}"#;
    let signature = sign_webhook_body(secret, body);
    assert!(signature.starts_with("sha256="));

    // Receiver side: recompute and compare (constant-form string compare here;
    // the header value is the canonical hex digest).
    let expected = sign_webhook_body(secret, body);
    assert_eq!(signature, expected, "same secret + body must verify");

    // Wrong secret does not verify.
    assert_ne!(signature, sign_webhook_body("wrong-secret", body));
    // Tampered body does not verify.
    let tampered = br#"{"tenant_id":"tenant-b","event":"invoice.created"}"#;
    assert_ne!(signature, sign_webhook_body(secret, tampered));
}

/// A caller scoped to tenant-a must not register/read an endpoint for tenant-b
/// by putting a foreign tenant_id in the request BODY; the scope guard rejects
/// this before any pool/DB access — mirrors `tenant_service`/`lock_service`.
#[tokio::test]
async fn create_endpoint_rejects_cross_tenant_body() {
    let svc = WebhookServiceImpl::new(); // no pool, no channels (admit no-op)
    let mut request = Request::new(webhook_pb::CreateEndpointRequest {
        tenant_id: "tenant-b".to_string(),
        url: "https://hooks.example.com/x".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .create_endpoint(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn create_endpoint_missing_url_carries_field_violation() {
    let svc = WebhookServiceImpl::new(); // no pool, no channels (admit no-op)
    let mut request = Request::new(webhook_pb::CreateEndpointRequest {
        tenant_id: "tenant-a".to_string(),
        url: "  ".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .create_endpoint(request)
        .await
        .expect_err("missing url must be rejected before pool access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "url is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "url");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty HTTPS webhook URL"
    );
}

#[test]
fn webhook_missing_postgres_capability_carries_typed_detail() {
    let err = webhook_capability_status(
        "postgres_store",
        "postgres_store",
        "webhook service requires a Postgres-backed store (no PG pool configured)",
    );
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "webhook service requires a Postgres-backed store (no PG pool configured)"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Capability as i32);
    assert_eq!(detail.backend, "webhook");
    assert_eq!(detail.operation, "postgres_store");
    assert_eq!(detail.capability_required, "postgres_store");
    assert!(!detail.retryable);
}

#[test]
fn webhook_endpoint_not_found_statuses_carry_schema_detail() {
    for operation in ["get_endpoint", "update_endpoint", "delete_endpoint"] {
        assert_schema_not_found_detail(&webhook_endpoint_not_found_status(operation), operation);
    }
}

#[test]
fn webhook_internal_status_carries_typed_detail() {
    assert_internal_detail(
        &webhook_internal_status(
            "list_webhook_deliveries",
            "list webhook deliveries failed: database is unavailable",
        ),
        "list_webhook_deliveries",
        "list webhook deliveries failed: database is unavailable",
    );
}

/// Backoff grows monotonically and is capped.
#[test]
fn backoff_is_bounded() {
    assert_eq!(delivery_backoff(1), DELIVERY_BACKOFF_BASE);
    assert!(delivery_backoff(2) > delivery_backoff(1));
    assert!(delivery_backoff(100) <= DELIVERY_BACKOFF_CAP);
}
