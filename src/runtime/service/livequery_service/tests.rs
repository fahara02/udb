//! LiveQueryService unit tests — cross-tenant rejection, field validation, the
//! per-event tenant-scope guard (the cross-tenant-leak boundary), the concurrent
//! stream budget, and the single-row IR evaluator. Pure/validation paths, no
//! runtime or CDC engine required.

use serde_json::json;
use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::ir::{ComparisonOp, LogicalFilter, LogicalValue};
use crate::proto::udb::core::livequery::services::v1 as lq_pb;
// The `LiveQueryService` trait must be in scope for the `.subscribe()` calls below.
use crate::proto::udb::core::livequery::services::v1::live_query_service_server::LiveQueryService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::LiveQueryServiceImpl;
use super::budget::{active_streams, try_acquire_stream_slot};
use super::config::max_streams_per_tenant;
use super::errors::{livequery_backpressure_status, livequery_capability_status};
use super::predicate::{event_matches_tenant_scope, filter_matches_row, resolve_source};

fn decode_detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("error-detail trailer present")
        .to_bytes()
        .expect("trailer decodes to bytes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

/// A caller scoped to tenant-a must not subscribe to tenant-b by putting a
/// foreign tenant_id in the request BODY; the scope guard rejects this before
/// any read/stream access (no runtime/CDC needed).
#[tokio::test]
async fn subscribe_rejects_cross_tenant_body() {
    let svc = LiveQueryServiceImpl::new();
    let mut request = Request::new(lq_pb::SubscribeRequest {
        tenant_id: "tenant-b".to_string(),
        message_type: "udb.core.lock.entity.v1.Lock".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    // `.err()` first: the Ok variant (a boxed Subscribe stream) is not `Debug`,
    // so `expect_err` can't format it — only the `Status` error is needed here.
    let err = svc
        .subscribe(request)
        .await
        .err()
        .expect("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn subscribe_rejects_cross_project_body() {
    let svc = LiveQueryServiceImpl::new();
    let mut request = Request::new(lq_pb::SubscribeRequest {
        tenant_id: "tenant-a".to_string(),
        project_id: "project-b".to_string(),
        message_type: "udb.core.lock.entity.v1.Lock".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    request
        .metadata_mut()
        .insert("x-udb-project-id", MetadataValue::from_static("project-a"));
    let err = svc
        .subscribe(request)
        .await
        .err()
        .expect("cross-project body must be rejected before stream allocation");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        decode_detail(&err).policy_decision_id,
        "project_metadata_mismatch"
    );
}

#[tokio::test]
async fn subscribe_missing_message_type_carries_field_violation() {
    let svc = LiveQueryServiceImpl::new(); // no runtime/CDC; validation runs first
    let mut request = Request::new(lq_pb::SubscribeRequest {
        tenant_id: "tenant-a".to_string(),
        message_type: " ".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

    let err = svc
        .subscribe(request)
        .await
        .err()
        .expect("missing message_type must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "message_type is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "message_type");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty source message type"
    );
}

#[tokio::test]
async fn subscribe_empty_predicate_field_carries_field_violation() {
    let svc = LiveQueryServiceImpl::new(); // no runtime/CDC; filter validation runs first
    let mut request = Request::new(lq_pb::SubscribeRequest {
        tenant_id: "tenant-a".to_string(),
        message_type: "udb.core.lock.entity.v1.Lock".to_string(),
        filters: vec![lq_pb::LiveQueryPredicate {
            field: " ".to_string(),
            op: lq_pb::LiveQueryComparison::Eq as i32,
            value: "HELD".to_string(),
        }],
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

    let err = svc
        .subscribe(request)
        .await
        .err()
        .expect("empty predicate field must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "live query predicate field must not be empty"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "filters.field");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty live query predicate field"
    );
}

#[tokio::test]
async fn subscribe_unspecified_predicate_op_carries_field_violation() {
    let svc = LiveQueryServiceImpl::new(); // no runtime/CDC; filter validation runs first
    let mut request = Request::new(lq_pb::SubscribeRequest {
        tenant_id: "tenant-a".to_string(),
        message_type: "udb.core.lock.entity.v1.Lock".to_string(),
        filters: vec![lq_pb::LiveQueryPredicate {
            field: "status".to_string(),
            op: lq_pb::LiveQueryComparison::Unspecified as i32,
            value: "HELD".to_string(),
        }],
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

    let err = svc
        .subscribe(request)
        .await
        .err()
        .expect("unspecified predicate op must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "live query predicate comparison op is unspecified"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "filters.op");
    assert_eq!(
        detail.field_violations[0].description,
        "must specify a live query predicate comparison operator"
    );
}

/// The per-event re-check is the cross-tenant-leak guard: a tenant-less event
/// and a foreign-tenant event are DROPPED; only a matching event is KEPT.
#[test]
fn per_event_scope_drops_foreign_and_tenantless_keeps_match() {
    let topic = "udb.lock.lock.acquired.v1";
    // Matching tenant (and project) is kept.
    assert!(event_matches_tenant_scope(
        topic,
        &json!({"tenant_id": "tenant-a", "project_id": "project-a"}),
        "tenant-a",
        "project-a",
    ));
    // Foreign tenant is dropped.
    assert!(!event_matches_tenant_scope(
        topic,
        &json!({"tenant_id": "tenant-b"}),
        "tenant-a",
        "",
    ));
    // Tenant-less payload is dropped (fail closed).
    assert!(!event_matches_tenant_scope(
        topic,
        &json!({"event_id": "e1"}),
        "tenant-a",
        "",
    ));
    // A non-`udb.` (non-tenant-scoped) topic is never consumed.
    assert!(!event_matches_tenant_scope(
        "app.customer.changed",
        &json!({"tenant_id": "tenant-a"}),
        "tenant-a",
        "",
    ));
    // An unscoped subscriber cannot receive a tenant-stamped event.
    assert!(!event_matches_tenant_scope(
        topic,
        &json!({"tenant_id": "tenant-a"}),
        "",
        "",
    ));
    // Project mismatch is dropped even when the tenant matches.
    assert!(!event_matches_tenant_scope(
        topic,
        &json!({"tenant_id": "tenant-a", "project_id": "project-b"}),
        "tenant-a",
        "project-a",
    ));
}

/// The source must resolve a tenant column or the subscription fails closed.
#[test]
fn unknown_source_fails_closed() {
    // `.err()` first: the Ok variant `SourceBinding` is not `Debug`.
    let err = resolve_source("not.a.real.Entity")
        .err()
        .expect("unknown source must fail closed");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "live query source 'not.a.real.Entity' is not a known UDB entity"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "message_type");
    assert_eq!(
        detail.field_violations[0].description,
        "must name exactly one known tenant-scoped UDB entity"
    );
}

#[test]
fn backpressure_status_carries_typed_quota_detail() {
    let status = livequery_backpressure_status(
        "subscriber_channel",
        "live query subscriber too slow; stream closed",
    );
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    let detail = decode_detail(&status);
    assert_eq!(detail.kind, ErrorKind::Quota as i32);
    assert!(detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
    assert_eq!(detail.backend, "livequery");
    assert_eq!(detail.operation, "subscriber_channel");
}

/// The per-tenant concurrent-stream budget: the N+1th subscribe slot is
/// refused with the typed quota refusal, a freed slot is immediately
/// reusable, and another tenant is unaffected. Pure logic on the counter
/// helper — tenant ids are unique to this test because the limiter map is
/// deliberately process-wide.
#[test]
fn stream_budget_refuses_over_budget_and_reuses_freed_slot() {
    let tenant = "tenant-stream-budget-a";
    let other_tenant = "tenant-stream-budget-b";
    let budget = max_streams_per_tenant();

    // Fill the budget.
    let mut slots = Vec::with_capacity(budget);
    for _ in 0..budget {
        slots.push(try_acquire_stream_slot(tenant).expect("within budget"));
    }

    // The N+1th stream is refused with the typed quota refusal.
    let err = try_acquire_stream_slot(tenant)
        .err()
        .expect("over-budget subscribe must be refused");
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Quota as i32);
    assert_eq!(detail.backend, "livequery");
    assert_eq!(detail.operation, "tenant_stream_budget");
    assert!(!detail.retryable);

    // The budget is PER TENANT: a saturated tenant never starves another.
    let other_slot = try_acquire_stream_slot(other_tenant).expect("other tenant unaffected");
    drop(other_slot);

    // Ending one stream (guard drop) frees a slot that is reusable.
    drop(slots.pop());
    let reacquired = try_acquire_stream_slot(tenant).expect("freed slot must be reusable");
    drop(reacquired);

    // Draining every stream removes the tenant's limiter entry entirely.
    drop(slots);
    let map = active_streams()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert!(!map.contains_key(tenant));
    assert!(!map.contains_key(other_tenant));
}

#[test]
fn livequery_missing_runtime_capability_carries_typed_detail() {
    let err = livequery_capability_status(
        "native_entity_dispatch",
        "runtime_native_entity_dispatch",
        "live query service requires runtime native-entity dispatch (no runtime configured)",
    );
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "live query service requires runtime native-entity dispatch (no runtime configured)"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Capability as i32);
    assert_eq!(detail.backend, "livequery");
    assert_eq!(detail.operation, "native_entity_dispatch");
    assert_eq!(detail.capability_required, "runtime_native_entity_dispatch");
    assert!(!detail.retryable);
}

/// The single-row IR evaluator gates deltas: only rows still matching the
/// subscription predicate are yielded.
#[test]
fn single_row_evaluator_matches_predicate() {
    let filter = LogicalFilter::And(vec![
        LogicalFilter::Comparison {
            field: "status".to_string(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String("HELD".to_string()),
        },
        LogicalFilter::Comparison {
            field: "fencing_token".to_string(),
            op: ComparisonOp::Ge,
            value: LogicalValue::Int(5),
        },
    ]);
    assert!(filter_matches_row(
        &filter,
        &json!({"status": "HELD", "fencing_token": 7})
    ));
    // Numeric predicate fails (token below threshold).
    assert!(!filter_matches_row(
        &filter,
        &json!({"status": "HELD", "fencing_token": 3})
    ));
    // String predicate fails (status differs).
    assert!(!filter_matches_row(
        &filter,
        &json!({"status": "RELEASED", "fencing_token": 9})
    ));
    // Missing field => comparison false => row excluded.
    assert!(!filter_matches_row(&filter, &json!({"status": "HELD"})));
}

#[test]
fn change_row_unwraps_the_udb_payload_envelope() {
    // Regression: both CDC emit paths (core/setup_data.rs and the native
    // compliance envelope) nest the row image under `payload`, with tenant and
    // operation stamped at the top level. Before `payload` was a ROW_KEY,
    // change_row returned the WHOLE envelope, so filters keyed on row columns
    // (e.g. `status`) evaluated against the envelope top level (None) and every
    // matching delta was silently dropped, and change frames shipped the
    // envelope instead of the row.
    let envelope = json!({
        "event_id": "e1",
        "topic": "udb.lock.acquired.v1",
        "tenant_id": "t1",
        "operation": "upsert",
        "payload": { "lock_id": "L1", "status": "HELD", "fencing_token": 7 }
    });
    let row = super::predicate::change_row(&envelope);
    assert_eq!(
        row,
        json!({ "lock_id": "L1", "status": "HELD", "fencing_token": 7 })
    );
    // The unwrapped row now matches the snapshot row shape, so a filter on a row
    // column resolves correctly instead of dropping the delta.
    assert!(super::predicate::filter_matches_row(
        &LogicalFilter::Comparison {
            field: "status".to_string(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String("HELD".to_string()),
        },
        &row
    ));
}

/// M1 resume/live predicate parity: the durable-resume replay gates a journalled
/// envelope through the SAME `change_row` + `filter_matches_row` pair the live
/// loop uses, so a reconnecting subscriber never receives replayed deltas its
/// `user_filter` would have dropped on the live feed. Exercised at the predicate
/// level (the exact expression the resume loop runs); the full forwarder needs a
/// runtime/CDC broadcast and is covered elsewhere.
#[test]
fn resume_replay_applies_user_filter_like_live_loop() {
    let filter = LogicalFilter::Comparison {
        field: "status".to_string(),
        op: ComparisonOp::Eq,
        value: LogicalValue::String("HELD".to_string()),
    };
    // A journalled envelope nests the row image under `payload` (both CDC emit
    // paths), exactly what the resume loop parses out of `envelope.payload_json`.
    let matching = json!({
        "tenant_id": "acme-tenant",
        "operation": "upsert",
        "payload": { "lock_id": "L1", "status": "HELD", "fencing_token": 7 }
    });
    let filtered = json!({
        "tenant_id": "acme-tenant",
        "operation": "upsert",
        "payload": { "lock_id": "L2", "status": "RELEASED", "fencing_token": 9 }
    });
    // In-scope + predicate match => replayed.
    assert!(filter_matches_row(
        &filter,
        &super::predicate::change_row(&matching)
    ));
    // In-scope but predicate mismatch => dropped on resume, same as live.
    assert!(!filter_matches_row(
        &filter,
        &super::predicate::change_row(&filtered)
    ));
}

#[test]
fn change_row_prefers_external_convention_keys_over_payload() {
    // A foreign (Debezium-style) envelope whose row is under `after` must still
    // unwrap to `after`, not the UDB `payload` fallback.
    let debezium = json!({ "op": "u", "after": { "id": 1, "v": "x" } });
    assert_eq!(
        super::predicate::change_row(&debezium),
        json!({ "id": 1, "v": "x" })
    );
}
