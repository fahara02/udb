//! Unit guards for the native `LockService`: request-body cross-tenant rejection,
//! field-violation shapes, the pure fencing-token freshness check, the typed
//! policy/schema/capability details, the fail-closed fencing-token refusal, the
//! quota-count read excluding lapsed rows, the no-double-expire claim SQL, and
//! the inventory DTO/read shaping. Copied verbatim from the former god file;
//! imports are explicit (no `use super::*`).

use chrono::Utc;
use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::ir::{ComparisonOp, LogicalFilter, LogicalValue};
use crate::proto::udb::core::lock::services::v1 as lock_pb;
use crate::proto::udb::core::lock::services::v1::lock_service_server::LockService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::LockServiceImpl;
use super::config::{MAX_ACTIVE_LOCKS_PER_TENANT, STATUS_HELD};
use super::errors::{
    ensure_fencing_token_fresh, fencing_token_unavailable_status, lock_already_held_status,
    lock_capability_status, lock_held_by_different_owner_status, lock_lease_lost_status,
    lock_not_held_status,
};
use super::model::lock_dto_from_json;
use super::store::{held_locks_read, lock_filter, lock_inventory_read, lock_model};
use super::workers::expired_locks_claim_sql;

fn decode_detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("error-detail trailer present")
        .to_bytes()
        .expect("trailer decodes to bytes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

fn assert_policy_detail(status: &Status, operation: &str, policy_decision_id: &str) {
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.policy_decision_id, policy_decision_id);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

/// A caller scoped to tenant-a must not target tenant-b's lock by putting a
/// foreign tenant_id in the request BODY; the scope guard rejects this before
/// any lease/store access (no Postgres needed) — mirrors `tenant_service`.
#[tokio::test]
async fn acquire_lock_rejects_cross_tenant_body() {
    let svc = LockServiceImpl::new(); // no runtime, no channels (admit no-op)
    let mut request = Request::new(lock_pb::AcquireLockRequest {
        tenant_id: "tenant-b".to_string(),
        lock_name: "orders".to_string(),
        owner_id: "worker-1".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .acquire_lock(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn acquire_lock_missing_identity_carries_field_violations() {
    let svc = LockServiceImpl::new(); // no runtime, no channels (admit no-op)
    let mut request = Request::new(lock_pb::AcquireLockRequest {
        tenant_id: "tenant-a".to_string(),
        lock_name: "  ".to_string(),
        owner_id: String::new(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .acquire_lock(request)
        .await
        .expect_err("missing lock identity must be rejected before runtime access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "lock_name and owner_id are required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 2);
    assert_eq!(detail.field_violations[0].field, "lock_name");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty lock name"
    );
    assert_eq!(detail.field_violations[1].field, "owner_id");
    assert_eq!(
        detail.field_violations[1].description,
        "must be a non-empty owner id"
    );
}

/// A stale (lower) fencing token is fenced off; an equal or greater token from
/// the current holder is accepted. Pure check — no PG.
#[test]
fn stale_fencing_token_is_rejected() {
    // Holder advanced to token 7; a partitioned caller still on 5 is fenced.
    let err = ensure_fencing_token_fresh(5, 7).expect_err("stale token must be rejected");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "stale fencing token 5; the lock has been fenced to token 7"
    );
    assert_policy_detail(&err, "lock_fencing", "stale_fencing_token");
    // The current holder (equal) and a future token (greater) pass.
    ensure_fencing_token_fresh(7, 7).expect("current token must pass");
    ensure_fencing_token_fresh(8, 7).expect("newer token must pass");
}

#[test]
fn lock_lease_lost_carries_policy_detail() {
    let err = lock_lease_lost_status();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "lock lease lost; it is now held by another owner"
    );
    assert_policy_detail(&err, "renew_lock", "lock_lease_lost");
}

#[test]
fn lock_owner_mismatch_carries_permission_denied_policy_detail() {
    for operation in ["renew_lock", "release_lock"] {
        let err = lock_held_by_different_owner_status(operation);
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_eq!(err.message(), "lock is held by a different owner");
        assert_policy_detail(&err, operation, "lock_owner_mismatch");
    }
}

#[test]
fn lock_not_held_carries_schema_detail() {
    let err = lock_not_held_status("renew_lock");
    assert_eq!(err.code(), tonic::Code::NotFound);
    assert_eq!(err.message(), "lock not held");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Schema as i32);
    assert_eq!(detail.backend, "lock");
    assert_eq!(detail.operation, "renew_lock");
    assert_eq!(detail.capability_required, "lock_not_held");
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

#[test]
fn lock_already_held_carries_schema_detail() {
    let err = lock_already_held_status();
    assert_eq!(err.code(), tonic::Code::AlreadyExists);
    assert_eq!(err.message(), "lock is already held by another owner");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Schema as i32);
    assert_eq!(detail.backend, "lock");
    assert_eq!(detail.operation, "acquire_lock");
    assert_eq!(detail.capability_required, "lock_already_held");
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

/// 16.5.1a — the quota-count read must exclude rows whose lease already
/// lapsed: a lapsed lock no longer excludes anyone, so counting it toward
/// `MAX_ACTIVE_LOCKS_PER_TENANT` would lock the tenant out between sweeps.
/// Pure filter-shape assertion (no PG).
#[test]
fn held_locks_quota_read_excludes_expired_rows() {
    let now = Utc::now();
    let read = held_locks_read("tenant-a", (MAX_ACTIVE_LOCKS_PER_TENANT as u32) + 1, now);
    let expected = LogicalFilter::And(vec![
        lock_filter("tenant-a", None, Some(STATUS_HELD)),
        LogicalFilter::Comparison {
            field: "expires_at".to_string(),
            op: ComparisonOp::Gt,
            value: LogicalValue::Timestamp(now),
        },
    ]);
    assert_eq!(read.filter, Some(expected));
}

/// 16.5.2 — with no canonical store (or a failed counter read) the fencing
/// path must return this typed capability refusal, never a wall-clock token:
/// `next_fencing_token`'s only non-store exit is
/// `fencing_token_unavailable_status()` (the `now_unix()` fallback is gone).
#[test]
fn missing_fencing_token_source_returns_typed_refusal_not_a_token() {
    let err = fencing_token_unavailable_status();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Capability as i32);
    assert_eq!(detail.backend, "lock");
    assert_eq!(detail.operation, "lock_fencing");
    assert_eq!(
        detail.capability_required,
        "canonical_store_monotone_counter"
    );
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

/// 16.5.1b — the expiry-claim SQL must claim with `FOR UPDATE SKIP LOCKED`
/// (two leaders never double-expire the same row), flip only lapsed HELD
/// rows, bound the batch, and return the columns the expired event needs.
#[test]
fn expired_locks_claim_sql_shape() {
    let sql = expired_locks_claim_sql(&lock_model());
    assert!(
        sql.starts_with("UPDATE"),
        "flip and claim in one statement: {sql}"
    );
    assert!(
        sql.contains("FOR UPDATE SKIP LOCKED"),
        "no-double-expire: {sql}"
    );
    assert!(sql.contains("\"status\" = $1"), "flip target bind: {sql}");
    assert!(
        sql.contains("\"status\" = $2"),
        "HELD claim filter bind: {sql}"
    );
    assert!(
        sql.contains("\"expires_at\" < NOW()"),
        "lapsed-only filter: {sql}"
    );
    assert!(sql.contains("LIMIT $3"), "bounded batch: {sql}");
    for column in [
        "RETURNING",
        "lock_id",
        "tenant_id",
        "lock_name",
        "owner_id",
        "fencing_token",
    ] {
        assert!(sql.contains(column), "event needs {column}: {sql}");
    }
}

#[test]
fn lock_missing_runtime_capability_carries_typed_detail() {
    let err = lock_capability_status(
        "native_entity_dispatch",
        "runtime_native_entity_dispatch",
        "lock service requires runtime native-entity dispatch (no runtime configured)",
    );
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "lock service requires runtime native-entity dispatch (no runtime configured)"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Capability as i32);
    assert_eq!(detail.backend, "lock");
    assert_eq!(detail.operation, "native_entity_dispatch");
    assert_eq!(detail.capability_required, "runtime_native_entity_dispatch");
    assert!(!detail.retryable);
}

// ── ch17 inventory reads (GetLock / ListLocks) ────────────────────────────

#[tokio::test]
async fn get_lock_rejects_cross_tenant_body() {
    let svc = LockServiceImpl::new();
    let mut request = Request::new(lock_pb::GetLockRequest {
        tenant_id: "tenant-b".to_string(),
        lock_name: "orders".to_string(),
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .get_lock(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn list_locks_rejects_cross_tenant_body() {
    let svc = LockServiceImpl::new();
    let mut request = Request::new(lock_pb::ListLocksRequest {
        tenant_id: "tenant-b".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .list_locks(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn get_lock_empty_name_is_field_violation() {
    let svc = LockServiceImpl::new();
    let mut request = Request::new(lock_pb::GetLockRequest {
        tenant_id: "tenant-a".to_string(),
        lock_name: "   ".to_string(),
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .get_lock(request)
        .await
        .expect_err("empty lock_name must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    let detail = decode_detail(&err);
    assert!(
        detail
            .field_violations
            .iter()
            .any(|f| f.field == "lock_name"),
        "expected a lock_name field violation"
    );
}

#[test]
fn lock_dto_from_json_maps_all_fields() {
    let row = serde_json::json!({
        "lock_id": "11111111-1111-1111-1111-111111111111",
        "tenant_id": "tenant-a",
        "lock_name": "orders",
        "owner_id": "worker-1",
        "fencing_token": 42,
        "lease_ttl_seconds": 30,
        "status": "HELD",
        "acquired_at": "2026-07-16T00:00:00Z",
        "expires_at": "2026-07-16T00:00:30Z",
        "metadata_json": { "region": "eu" },
    });
    let dto = lock_dto_from_json(&row);
    assert_eq!(dto.lock_name, "orders");
    assert_eq!(dto.owner_id, "worker-1");
    assert_eq!(dto.fencing_token, 42);
    assert_eq!(dto.lease_ttl_seconds, 30);
    assert_eq!(dto.status, "HELD");
    assert_eq!(dto.acquired_at_unix, 1_784_160_000);
    assert_eq!(dto.expires_at_unix, 1_784_160_030);
    // JSONB round-trips as a compact JSON string, never the empty default.
    assert_eq!(dto.metadata_json, "{\"region\":\"eu\"}");
}

#[test]
fn lock_inventory_read_applies_status_filter_sort_and_pagination() {
    let read = lock_inventory_read("tenant-a", None, Some("HELD"), 100, 50);
    // Newest first + bounded page window.
    assert_eq!(read.sort.len(), 1);
    assert_eq!(read.sort[0].field, "acquired_at");
    let pg = read.pagination.expect("pagination present");
    assert_eq!(pg.limit, Some(50));
    assert_eq!(pg.offset, Some(100));
    // The status filter is threaded into the tenant-scoped predicate.
    let rendered = format!("{:?}", read.filter);
    assert!(
        rendered.contains("HELD"),
        "status filter must be present: {rendered}"
    );
    assert!(
        rendered.contains("tenant-a"),
        "tenant filter must be present"
    );
}
