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

use super::resources::{ResourceModel, aggregate_version, content_version, ordered_resource_types};
use crate::proto::udb::core::control::entity::v1::ResourceType;

/// Minimal mirror of one `ControlPlaneNodeState` row: the fields the ACK/NACK
/// transition reads and writes. Kept in lock-step with `store::NodeStateRow`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Ledger {
    accepted_version: String,
    last_good_version: String,
    last_response_nonce: String,
    nack_error_detail: String,
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
