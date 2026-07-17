//! Unit guards for the native `CacheService`: claim-derived per-tenant key
//! isolation, field-violation shapes, the typed missing-Redis capability detail,
//! the pure budget gate, SCAN-not-KEYS sweeps, byte-counter reconciliation math,
//! meta-key round-tripping, scan-limit clamping, fail-closed CDC invalidation
//! scoping, and request-body cross-tenant rejection. Copied verbatim from the
//! former god file; imports are explicit (no `use super::*`).

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::proto::udb::core::cache::services::v1 as cache_pb;
use crate::proto::udb::core::cache::services::v1::cache_service_server::CacheService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::CacheServiceImpl;
use super::config::{
    DEFAULT_NAMESPACE_MAX_BYTES, MAX_SCAN_PAGE_LIMIT, META_SUFFIX, SWEEP_COMMAND, SWEEP_COUNT,
};
use super::errors::{redis_capability_status, require_field, validate_namespace};
use super::keys::{
    bytes_counter_key, clamped_scan_count, data_key, data_match, effective_max_bytes, meta_key,
    meta_match_all, namespace_match_all, parse_meta_key, reconcile_cursor_key, reconciled_sum,
    would_exceed_budget,
};
use super::worker::{
    invalidation_source_from_payload, invalidation_tenant_scope, namespace_for_source_table,
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

/// Claim-derived key namespacing: the same logical namespace+key for two
/// tenants produces DISTINCT Redis keys, so tenant-a can never read tenant-b's
/// entry. The tenant is always the first path segment.
#[test]
fn keys_are_isolated_per_tenant() {
    let a = data_key("tenant-a", "sessions", "u1");
    let b = data_key("tenant-b", "sessions", "u1");
    assert_ne!(a, b);
    assert_eq!(a, "udb:cache:tenant-a:sessions:k:u1");
    assert!(a.starts_with("udb:cache:tenant-a:"));
    assert!(b.starts_with("udb:cache:tenant-b:"));
    // A tenant's sweep pattern can never match another tenant's keys.
    let sweep_a = namespace_match_all("tenant-a", "sessions");
    assert!(!b.starts_with(sweep_a.trim_end_matches('*')));
}

#[test]
fn cache_validation_statuses_carry_field_violations() {
    let missing_namespace = validate_namespace("  ").expect_err("empty namespace must be rejected");
    assert_eq!(missing_namespace.code(), tonic::Code::InvalidArgument);
    assert_eq!(missing_namespace.message(), "namespace is required");
    let detail = decode_detail(&missing_namespace);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "namespace");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty namespace"
    );

    let invalid_namespace =
        validate_namespace("bad:namespace").expect_err("reserved separator rejected");
    let detail = decode_detail(&invalid_namespace);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations[0].field, "namespace");
    assert_eq!(
        detail.field_violations[0].description,
        "must not contain ':' or whitespace"
    );

    let missing_key = require_field("key", "  ").expect_err("empty key must be rejected");
    assert_eq!(missing_key.code(), tonic::Code::InvalidArgument);
    assert_eq!(missing_key.message(), "key is required");
    let detail = decode_detail(&missing_key);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "key");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty string"
    );
}

#[tokio::test]
async fn delete_namespace_missing_confirmation_token_carries_field_violation() {
    let svc = CacheServiceImpl::new();
    let mut request = Request::new(cache_pb::DeleteNamespaceRequest {
        tenant_id: "tenant-a".to_string(),
        namespace: "sessions".to_string(),
        confirmation_token: " ".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .delete_namespace(request)
        .await
        .expect_err("missing confirmation token must fail before backend access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "DeleteNamespace flushes the whole namespace; confirmation_token is required"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "confirmation_token");
    assert_eq!(
        detail.field_violations[0].description,
        "must be present to flush a cache namespace"
    );
}

#[test]
fn cache_missing_redis_capability_carries_typed_detail() {
    let status = redis_capability_status(
        "request_dispatch",
        "redis_backend",
        "cache service requires a configured Redis backend",
    );
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        status.message(),
        "cache service requires a configured Redis backend"
    );
    let detail = decode_detail(&status);
    assert_eq!(detail.kind, ErrorKind::Capability as i32);
    assert_eq!(detail.backend, "cache");
    assert_eq!(detail.operation, "request_dispatch");
    assert_eq!(detail.capability_required, "redis_backend");
    assert!(!detail.retryable);
}

/// `Set` over budget → resource_exhausted (the pure budget gate). A shrink or
/// same-size overwrite (`delta <= 0`) is always allowed.
#[test]
fn over_budget_is_rejected() {
    // 900 used, +200 delta, max 1000 → 1100 > 1000 → exceeds.
    assert!(would_exceed_budget(900, 200, 1000));
    // Exactly at the budget is allowed.
    assert!(!would_exceed_budget(800, 200, 1000));
    // A same-size or shrinking overwrite is always allowed, even at budget.
    assert!(!would_exceed_budget(1000, 0, 1000));
    assert!(!would_exceed_budget(1000, -10, 1000));
    // Unconfigured namespace falls back to the positive default bound.
    assert_eq!(effective_max_bytes(0), DEFAULT_NAMESPACE_MAX_BYTES);
    assert_eq!(effective_max_bytes(2048), 2048);
}

/// Prefix sweeps MUST use SCAN, never KEYS, and the data-key match pattern is
/// namespace-prefixed + wildcard-terminated.
#[test]
fn sweep_uses_scan_not_keys() {
    assert_eq!(SWEEP_COMMAND, "SCAN");
    assert_ne!(SWEEP_COMMAND, "KEYS");
    let pattern = data_match("tenant-a", "sessions", "u");
    assert_eq!(pattern, "udb:cache:tenant-a:sessions:k:u*");
    assert!(pattern.ends_with('*'));
}

/// Byte-counter reconciliation math: TTL expiry never runs the DECRBY
/// bookkeeping, so the worker recomputes usage from ground truth — the
/// counter is SET to the saturating sum of observed live data-key lengths,
/// and the stale counter value plays NO part in the math.
#[test]
fn reconciliation_sum_math() {
    assert_eq!(reconciled_sum(&[]), 0);
    assert_eq!(reconciled_sum(&[128, 64, 8]), 200);
    // Expired-between-SCAN-and-STRLEN keys read as length 0: a no-op term.
    assert_eq!(reconciled_sum(&[100, 0, 50]), 150);
    // Defensive: a bogus negative length never subtracts real usage.
    assert_eq!(reconciled_sum(&[-16, 32]), 32);
    // Adversarially huge values saturate instead of wrapping.
    assert_eq!(reconciled_sum(&[i64::MAX, 1]), i64::MAX);
}

/// Reconciliation namespace discovery parses `(tenant, namespace)` back out
/// of the meta key. The namespace (colon-free by validation) is always the
/// LAST segment; data keys, counters, and the rotation cursor never parse as
/// namespaces, so the pass can never recompute (or create) bookkeeping for a
/// non-namespace key.
#[test]
fn reconciliation_meta_key_round_trip() {
    assert_eq!(
        parse_meta_key(&meta_key("tenant-a", "sessions")),
        Some(("tenant-a".to_string(), "sessions".to_string()))
    );
    // A ':' inside the tenant segment stays with the tenant.
    assert_eq!(
        parse_meta_key("udb:cache:ten:ant:sessions:__meta__"),
        Some(("ten:ant".to_string(), "sessions".to_string()))
    );
    assert_eq!(parse_meta_key(&data_key("t", "ns", "k1")), None);
    assert_eq!(parse_meta_key(&bytes_counter_key("t", "ns")), None);
    assert_eq!(parse_meta_key(&reconcile_cursor_key()), None);
    assert_eq!(parse_meta_key("udb:cache::__meta__"), None);
    assert_eq!(parse_meta_key("udb:cache:only-one-segment:__meta__"), None);
    // The discovery pattern targets ONLY meta keys.
    assert_eq!(meta_match_all(), "udb:cache:*:__meta__");
    assert!(meta_match_all().ends_with(META_SUFFIX));
}

/// The `Scan` page limit is clamped to the named cap: a caller can never
/// push an unbounded COUNT hint into the SCAN cursor walk, and a
/// non-positive limit falls back to the default page size.
#[test]
fn scan_limit_is_clamped() {
    assert_eq!(clamped_scan_count(0), SWEEP_COUNT);
    assert_eq!(clamped_scan_count(-1), SWEEP_COUNT);
    assert_eq!(clamped_scan_count(10), 10);
    assert_eq!(
        clamped_scan_count(MAX_SCAN_PAGE_LIMIT as i32),
        MAX_SCAN_PAGE_LIMIT
    );
    assert_eq!(clamped_scan_count(i32::MAX), MAX_SCAN_PAGE_LIMIT);
    assert!(SWEEP_COUNT <= MAX_SCAN_PAGE_LIMIT);
}

/// CDC invalidation is tenant-scoped fail-closed: a tenant-less event yields no
/// scope (it is skipped, never sweeping a cross-tenant namespace); an event
/// stamped with a tenant yields exactly that tenant.
#[test]
fn invalidation_scope_fails_closed() {
    assert_eq!(invalidation_tenant_scope(&serde_json::json!({})), None);
    assert_eq!(
        invalidation_tenant_scope(&serde_json::json!({"tenant_id": "  "})),
        None
    );
    assert_eq!(
        invalidation_tenant_scope(&serde_json::json!({"tenant_id": "tenant-a"})),
        Some("tenant-a".to_string())
    );
    assert_eq!(
        namespace_for_source_table("invoices"),
        Some("invoices".to_string())
    );
    assert_eq!(namespace_for_source_table("  "), None);
}

#[test]
fn invalidation_source_uses_source_table_then_table_then_message_type() {
    assert_eq!(
        invalidation_source_from_payload(
            &serde_json::json!({"source_table": "invoices", "message_type": "ignored"})
        ),
        "invoices"
    );
    assert_eq!(
        invalidation_source_from_payload(&serde_json::json!({"table_name": "orders"})),
        "orders"
    );
    assert_eq!(
        invalidation_source_from_payload(&serde_json::json!({"message_type": "billing.Invoice"})),
        "billing.Invoice"
    );
    assert_eq!(invalidation_source_from_payload(&serde_json::json!({})), "");
}

/// A caller scoped to tenant-a must not target tenant-b's namespace by putting
/// a foreign tenant_id in the request BODY; the scope guard rejects this before
/// any Redis access — mirrors `tenant_service`/`lock_service`.
#[tokio::test]
async fn get_rejects_cross_tenant_body() {
    let svc = CacheServiceImpl::new(); // no redis, no channels (admit no-op)
    let mut request = Request::new(cache_pb::GetRequest {
        tenant_id: "tenant-b".to_string(),
        namespace: "sessions".to_string(),
        key: "u1".to_string(),
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .get(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}
