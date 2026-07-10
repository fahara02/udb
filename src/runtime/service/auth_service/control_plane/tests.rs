//! Pure-logic tests for the Phase 9 control-plane distribution core. These do
//! NOT touch a live database: they exercise the ACK/NACK ledger STATE MACHINE as
//! a pure transition function that mirrors `store::record_ack` /
//! `store::record_nack` semantics, plus the ordering + content-hash helpers in
//! [`super::resources`] (which also carry their own unit tests).
//!
//! The store SQL is verified end-to-end by the env-gated live-DB conformance
//! suite; here we lock the invariants the SQL must preserve:
//!   * NACK keeps `last_good_version` and does NOT advance `accepted_version`
//!     ("reject without silently diverging");
//!   * ACK advances `accepted_version` (+ `last_good_version`) and clears the
//!     NACK error;
//!   * an ACK/NACK whose nonce does not match the last response nonce is ignored.

use super::ControlPlaneServiceImpl;
use super::resources::{ResourceModel, aggregate_version, content_version, ordered_resource_types};
use crate::proto::udb::core::common::v1 as common_pb;
use crate::proto::udb::core::control::entity::v1::ResourceType;
use crate::proto::udb::core::control::services::v1 as control_pb;
use crate::proto::udb::core::control::services::v1::control_plane_service_server::ControlPlaneService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
use tonic::{Code, Request, Status};

/// Minimal mirror of one `ControlPlaneNodeState` row: the fields the ACK/NACK
/// transition reads and writes. Kept in lock-step with `store::NodeStateRow`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Ledger {
    accepted_version: String,
    last_good_version: String,
    last_response_nonce: String,
    nack_error_detail: String,
}

fn decode_detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("typed detail trailer is present");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

fn assert_validation_field(status: &Status, field: &str, description: &str) {
    assert_eq!(status.code(), Code::InvalidArgument);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, field);
    assert_eq!(detail.field_violations[0].description, description);
}

fn assert_capability_detail(
    status: &Status,
    backend: &str,
    operation: &str,
    capability_required: &str,
    message: &str,
) {
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Capability as i32);
    assert_eq!(detail.backend, backend);
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.capability_required, capability_required);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_schema_detail(
    status: &Status,
    backend: &str,
    operation: &str,
    schema_code: &str,
    message: &str,
) {
    assert_eq!(status.code(), Code::NotFound);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Schema as i32);
    assert_eq!(detail.backend, backend);
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.capability_required, schema_code);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_policy_detail(status: &Status, operation: &str, policy_decision_id: &str, message: &str) {
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.policy_decision_id, policy_decision_id);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_internal_detail(status: &Status, backend: &str, operation: &str, message: &str) {
    assert_eq!(status.code(), Code::Internal);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Internal as i32);
    assert_eq!(detail.backend, backend);
    assert_eq!(detail.operation, operation);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
    assert!(detail.field_violations.is_empty());
}

#[test]
fn control_plane_missing_postgres_store_capability_carries_typed_detail() {
    let svc = ControlPlaneServiceImpl::new();
    let err = match svc.require_pool() {
        Err(status) => status,
        Ok(_) => panic!("pool-less control plane must fail closed"),
    };

    assert_capability_detail(
        &err,
        "control_plane",
        "postgres_store",
        "postgres_store",
        "control-plane service requires a Postgres-backed store (no PG pool configured)",
    );
}

#[test]
fn control_plane_internal_status_carries_typed_detail() {
    let err = ControlPlaneServiceImpl::internal_status(
        "delta_resources",
        "control delta stream error: transport closed",
    );

    assert_internal_detail(
        &err,
        "control_plane",
        "delta_resources",
        "control delta stream error: transport closed",
    );
}

#[test]
fn control_plane_rollback_missing_target_carries_policy_detail() {
    assert_policy_detail(
        &ControlPlaneServiceImpl::rollback_target_required_status(),
        "control_plane_rollback",
        "rollback_target_required",
        "no retained snapshot to roll back to for this (node, resource_type, target_version)",
    );
}

#[test]
fn control_plane_node_state_not_found_carries_schema_detail() {
    assert_schema_detail(
        &ControlPlaneServiceImpl::node_state_not_found_status(),
        "control_plane",
        "AckStatus",
        "node_state_not_found",
        "no node state for this (node, resource_type)",
    );
}

#[tokio::test]
async fn get_resources_missing_resource_type_carries_field_violation() {
    let svc = ControlPlaneServiceImpl::new(); // no pool; validation runs first

    let err = svc
        .get_resources(Request::new(control_pb::GetResourcesRequest {
            resource_type: ResourceType::Unspecified as i32,
            page: Some(common_pb::PageRequest::default()),
            ..Default::default()
        }))
        .await
        .expect_err("missing resource_type must fail before Postgres availability");

    assert_eq!(err.message(), "resource_type is required");
    assert_validation_field(
        &err,
        "resource_type",
        "must specify a control-plane resource type",
    );
}

#[tokio::test]
async fn ack_status_missing_node_id_carries_field_violation() {
    let svc = ControlPlaneServiceImpl::new(); // no pool; validation runs first

    let err = svc
        .ack_status(Request::new(control_pb::AckStatusRequest {
            node_id: " ".to_string(),
            resource_type: ResourceType::BackendTargetDefinition as i32,
            ..Default::default()
        }))
        .await
        .expect_err("missing node_id must fail before Postgres availability");

    assert_eq!(err.message(), "node_id is required");
    assert_validation_field(&err, "node_id", "must be a non-empty control-plane node id");
}

#[tokio::test]
async fn ack_status_missing_resource_type_carries_field_violation() {
    let svc = ControlPlaneServiceImpl::new(); // no pool; validation runs first

    let err = svc
        .ack_status(Request::new(control_pb::AckStatusRequest {
            node_id: "node-a".to_string(),
            resource_type: ResourceType::Unspecified as i32,
            ..Default::default()
        }))
        .await
        .expect_err("missing resource_type must fail before Postgres availability");

    assert_eq!(err.message(), "resource_type is required");
    assert_validation_field(
        &err,
        "resource_type",
        "must specify a control-plane resource type",
    );
}

#[tokio::test]
async fn rollback_resources_missing_node_id_carries_field_violation() {
    let svc = ControlPlaneServiceImpl::new(); // no pool; validation runs first

    let err = svc
        .rollback_resources(Request::new(control_pb::RollbackResourcesRequest {
            node_id: " ".to_string(),
            resource_type: ResourceType::BackendTargetDefinition as i32,
            ..Default::default()
        }))
        .await
        .expect_err("missing node_id must fail before Postgres availability");

    assert_eq!(err.message(), "node_id is required");
    assert_validation_field(&err, "node_id", "must be a non-empty control-plane node id");
}

#[test]
fn stream_resources_empty_stream_status_carries_field_violation() {
    let err = ControlPlaneServiceImpl::empty_discovery_stream_status();

    assert_eq!(err.message(), "empty control discovery stream");
    assert_validation_field(&err, "stream", "must include an initial DiscoveryRequest");
}

#[test]
fn stream_resources_missing_node_id_status_carries_field_violation() {
    let err = ControlPlaneServiceImpl::missing_discovery_node_id_status();

    assert_eq!(
        err.message(),
        "node_id is required on the first DiscoveryRequest"
    );
    assert_validation_field(
        &err,
        "node_id",
        "must be a non-empty node id on the first DiscoveryRequest",
    );
}

#[test]
fn delta_resources_empty_stream_status_carries_field_violation() {
    let err = ControlPlaneServiceImpl::empty_delta_stream_status();

    assert_eq!(err.message(), "empty control delta stream");
    assert_validation_field(
        &err,
        "stream",
        "must include an initial DeltaDiscoveryRequest",
    );
}

#[test]
fn delta_resources_missing_node_id_status_carries_field_violation() {
    let err = ControlPlaneServiceImpl::missing_delta_node_id_status();

    assert_eq!(
        err.message(),
        "node_id is required on the first DeltaDiscoveryRequest"
    );
    assert_validation_field(
        &err,
        "node_id",
        "must be a non-empty node id on the first DeltaDiscoveryRequest",
    );
}

impl Ledger {
    /// Stamp a freshly-sent response nonce (what `next_response_nonce` does).
    fn send(&mut self, nonce: &str) {
        self.last_response_nonce = nonce.to_string();
    }

    /// Mirror of `store::record_ack`: honored only on a matching nonce; advances
    /// accepted + last_good, clears the NACK. Returns whether it was applied.
    fn record_ack(&mut self, accepted_version: &str, nonce: &str) -> bool {
        if self.last_response_nonce != nonce {
            return false; // stale ack — ignored (nonce pairing)
        }
        self.accepted_version = accepted_version.to_string();
        self.last_good_version = accepted_version.to_string();
        self.nack_error_detail.clear();
        true
    }

    /// Mirror of `store::record_nack`: honored only on a matching nonce; sets the
    /// error detail but DELIBERATELY does not touch accepted/last_good.
    fn record_nack(&mut self, nonce: &str, detail: &str) -> bool {
        if self.last_response_nonce != nonce {
            return false;
        }
        self.nack_error_detail = detail.to_string();
        true
    }
}

#[test]
fn ack_advances_accepted_version_and_clears_nack() {
    let mut led = Ledger::default();
    // First push v1, node applies it.
    led.send("n1");
    assert!(led.record_ack("v1", "n1"));
    assert_eq!(led.accepted_version, "v1");
    assert_eq!(led.last_good_version, "v1");
    assert!(led.nack_error_detail.is_empty());

    // A later bad push is NACKed, then a good push is ACKed: the NACK clears.
    led.send("n2");
    assert!(led.record_nack("n2", "boom"));
    assert_eq!(led.nack_error_detail, "boom");
    led.send("n3");
    assert!(led.record_ack("v3", "n3"));
    assert_eq!(led.accepted_version, "v3");
    assert!(
        led.nack_error_detail.is_empty(),
        "a successful ACK must clear the prior NACK error"
    );
}

// Acceptance: "A node can reject a bad policy without silently diverging."
#[test]
fn nack_keeps_last_good_and_does_not_advance_accepted() {
    let mut led = Ledger::default();
    // Node is healthy on v1.
    led.send("n1");
    assert!(led.record_ack("v1", "n1"));
    assert_eq!(led.accepted_version, "v1");
    assert_eq!(led.last_good_version, "v1");

    // A bad v2 is pushed and the node NACKs it.
    led.send("n2");
    assert!(led.record_nack("n2", r#"{"code":3,"message":"invalid policy"}"#));
    // INVARIANT: accepted_version is NOT advanced, last_good is preserved — the
    // node keeps running v1 instead of silently diverging onto the bad v2.
    assert_eq!(
        led.accepted_version, "v1",
        "NACK must not advance accepted_version"
    );
    assert_eq!(
        led.last_good_version, "v1",
        "NACK must preserve last_good_version"
    );
    assert!(
        !led.nack_error_detail.is_empty(),
        "NACK must record the structured error detail"
    );
}

#[test]
fn stale_nonce_ack_and_nack_are_ignored() {
    let mut led = Ledger::default();
    led.send("n5");
    // ACK with a non-matching nonce is ignored (no divergence on a stale echo).
    assert!(!led.record_ack("vX", "n4"));
    assert_eq!(led.accepted_version, "");
    // NACK with a non-matching nonce is likewise ignored.
    assert!(!led.record_nack("n4", "stale"));
    assert!(led.nack_error_detail.is_empty());
    // The matching nonce is honored.
    assert!(led.record_ack("v5", "n5"));
    assert_eq!(led.accepted_version, "v5");
}

#[test]
fn world_version_matches_pushed_then_acked_version() {
    // The version a node ACKs is the aggregate world version of what it was sent,
    // so after an ACK the ledger's accepted_version equals the registry's world.
    let resources = vec![
        ResourceModel {
            name: "pg-primary".into(),
            resource_type: "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION".into(),
            content_hash: content_version(r#"{"host":"a"}"#),
            payload_json: r#"{"host":"a"}"#.into(),
            ..Default::default()
        },
        ResourceModel {
            name: "pg-replica".into(),
            resource_type: "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION".into(),
            content_hash: content_version(r#"{"host":"b"}"#),
            payload_json: r#"{"host":"b"}"#.into(),
            ..Default::default()
        },
    ];
    let world = aggregate_version(&resources);
    let mut led = Ledger::default();
    led.send("n1");
    assert!(led.record_ack(&world, "n1"));
    assert_eq!(led.accepted_version, world);

    // Editing a target bumps the world version, so the node is now out of sync
    // until it ACKs the new world.
    let mut edited = resources.clone();
    edited[1].payload_json = r#"{"host":"c"}"#.into();
    edited[1].content_hash = content_version(&edited[1].payload_json);
    let world2 = aggregate_version(&edited);
    assert_ne!(world, world2);
    assert_ne!(
        led.accepted_version, world2,
        "node is out of sync until it ACKs the new world version"
    );
}

#[test]
fn push_order_sends_definitions_before_referencing_policies() {
    // The stream pushes per-type in this exact order; assert backend-target
    // definitions come before the routing/RLS policies that reference them.
    let order = ordered_resource_types();
    let pos = |rt: ResourceType| order.iter().position(|t| *t == rt).unwrap();
    assert!(pos(ResourceType::BackendTargetDefinition) < pos(ResourceType::RoutingPolicy));
    assert!(pos(ResourceType::BackendTargetDefinition) < pos(ResourceType::RlsTenantPolicy));
}
