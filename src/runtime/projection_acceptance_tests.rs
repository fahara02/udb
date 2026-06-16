//! U3 acceptance: projection-engine convergence + lag visibility.
//!
//! These tests cover deterministic, pure-logic projection invariants. The full
//! multi-backend convergence test (where one backend goes down, recovers,
//! replays, and converges) requires a live multi-backend environment; here we
//! pin the pure invariants that make such convergence possible:
//!
//! 1. A `CatalogManifest` with projections to all six target kinds
//!    (Mongo, Qdrant, Neo4j, ClickHouse, Redis, S3) produces a
//!    `ProjectionPlan` with one target per kind.
//! 2. The idempotency key is **stable** for the same logical write —
//!    replaying after a failure produces the same key, so the `ON
//!    CONFLICT DO NOTHING` insert dedups instead of double-projecting.
//! 3. The idempotency key **changes** when the source row changes, when
//!    the manifest changes (schema migration), or when the target
//!    changes — three different correctness boundaries.
//! 4. `extract_row_key` correctly pulls primary-key columns even from
//!    compound-key tables, so the same source row always hashes to the
//!    same row-key signature.
//! 5. The metrics-trait method `set_projection_oldest_pending_age_seconds`
//!    is on `MetricsRecorder` so the Prometheus impl gauges it under the
//!    `(project, backend, instance, kind)` label set demanded by U3's
//!    "lag visible per project/backend/resource" gate.

#![cfg(test)]

use serde_json::json;

use crate::generation::manifest::{
    CatalogManifest, ManifestProjection, ManifestStoreOption, ManifestTable,
};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::runtime::projection::{ProjectionEngine, ProjectionPlan};

fn opt(key: &str, value: &str) -> ManifestStoreOption {
    ManifestStoreOption {
        key: key.to_string(),
        value: value.to_string(),
    }
}

fn projection_for(
    message: &str,
    kind: &str,
    backend: &str,
    resource: &str,
    options: Vec<ManifestStoreOption>,
) -> ManifestProjection {
    ManifestProjection {
        message_type: message.to_string(),
        projection_kind: kind.to_string(),
        backend: backend.to_string(),
        instance: "default".to_string(),
        resource_name: resource.to_string(),
        write_policy: "primary".to_string(),
        fanout_policy: "outbox".to_string(),
        options,
        ..ManifestProjection::default()
    }
}

/// A `CatalogManifest` with one source table + projections to **every**
/// non-Postgres backend U3 names. The plan must surface one target per
/// projection (six total) so a write fan-out hits every backend.
fn full_multi_backend_manifest() -> CatalogManifest {
    let table = ManifestTable {
        message_name: "acme.billing.v1.Customer".to_string(),
        schema: "public".to_string(),
        table: "customers".to_string(),
        primary_key: vec!["id".to_string()],
        projections: vec![
            projection_for(
                "acme.billing.v1.Customer",
                "document",
                "mongodb",
                "customers",
                vec![],
            ),
            projection_for(
                "acme.billing.v1.Customer",
                "vector",
                "qdrant",
                "customers_vectors",
                vec![opt("vector_size", "384")],
            ),
            projection_for(
                "acme.billing.v1.Customer",
                "graph",
                "neo4j",
                "Customer",
                vec![],
            ),
            projection_for(
                "acme.billing.v1.Customer",
                "analytics",
                "clickhouse",
                "customers_events",
                vec![],
            ),
            projection_for(
                "acme.billing.v1.Customer",
                "cache",
                "redis",
                "udb:{tenant}:Customer:{id}",
                vec![opt("ttl_seconds", "300")],
            ),
            projection_for(
                "acme.billing.v1.Customer",
                "object",
                "s3",
                "customer-snapshots",
                vec![opt("key_prefix", "v1/customers")],
            ),
        ],
        ..Default::default()
    };
    CatalogManifest {
        checksum_sha256: "full-multi-backend-v1".to_string(),
        tables: vec![table],
        ..Default::default()
    }
}

/// U3 gate: "On write, produce deterministic projection tasks for every
/// projection." One source row → one target per projection — six tasks
/// for the full-fan-out manifest above.
#[test]
fn full_manifest_produces_one_target_per_backend() {
    let plans = ProjectionPlan::from_manifest(&full_multi_backend_manifest());
    assert_eq!(plans.len(), 1, "one source message type");
    let plan = &plans[0];
    let backends: Vec<&str> = plan.targets.iter().map(|t| t.backend.as_str()).collect();
    // Order follows manifest declaration order, which is deterministic.
    assert_eq!(
        backends,
        vec!["mongodb", "qdrant", "neo4j", "clickhouse", "redis", "s3"],
        "every U3-named backend must appear as a target"
    );
    // Per-kind resource names survive intact so the worker dispatches to
    // the right Mongo collection / Qdrant collection / S3 bucket.
    let by_backend: std::collections::BTreeMap<&str, &str> = plan
        .targets
        .iter()
        .map(|t| (t.backend.as_str(), t.resource_name.as_str()))
        .collect();
    assert_eq!(by_backend["mongodb"], "customers");
    assert_eq!(by_backend["qdrant"], "customers_vectors");
    assert_eq!(by_backend["clickhouse"], "customers_events");
}

/// U3 gate: "Persist task state with idempotency key …" — the **key**
/// must be stable across two enqueues of the same logical write, so a
/// retry or replay collides on the unique-constraint and dedups.
#[test]
fn idempotency_key_is_stable_across_replays() {
    let row = json!({ "id": "cust-1" });
    let key_1 = ProjectionEngine::idempotency_key(
        "billing",
        "customers",
        &row,
        "upsert",
        "mongodb",
        "default",
        "manifest-v1",
        "source-checksum-1",
    );
    let key_2 = ProjectionEngine::idempotency_key(
        "billing",
        "customers",
        &row,
        "upsert",
        "mongodb",
        "default",
        "manifest-v1",
        "source-checksum-1",
    );
    assert_eq!(
        key_1, key_2,
        "same logical write must produce the same idempotency key"
    );
}

/// Three independent invariants the key must satisfy, each tracking a
/// different correctness boundary:
/// - **source change** → new key (avoid stale dedup)
/// - **schema migration** → new key (every row needs re-projection)
/// - **different target** → new key (different backend, different task)
#[test]
fn idempotency_key_changes_at_each_correctness_boundary() {
    let row = json!({ "id": "cust-1" });
    let base = ProjectionEngine::idempotency_key(
        "billing",
        "customers",
        &row,
        "upsert",
        "mongodb",
        "default",
        "manifest-v1",
        "source-checksum-1",
    );

    // Source row payload changed (different checksum).
    let changed_source = ProjectionEngine::idempotency_key(
        "billing",
        "customers",
        &row,
        "upsert",
        "mongodb",
        "default",
        "manifest-v1",
        "source-checksum-2",
    );
    assert_ne!(
        base, changed_source,
        "source payload change must invalidate idempotency key"
    );

    // Schema/manifest checksum changed (migration).
    let migrated = ProjectionEngine::idempotency_key(
        "billing",
        "customers",
        &row,
        "upsert",
        "mongodb",
        "default",
        "manifest-v2",
        "source-checksum-1",
    );
    assert_ne!(
        base, migrated,
        "manifest migration must invalidate idempotency key"
    );

    // Different target backend → different task.
    let other_backend = ProjectionEngine::idempotency_key(
        "billing",
        "customers",
        &row,
        "upsert",
        "qdrant",
        "default",
        "manifest-v1",
        "source-checksum-1",
    );
    assert_ne!(
        base, other_backend,
        "different target backend must produce a different task"
    );

    // Different project_id → different task (project isolation).
    let other_project = ProjectionEngine::idempotency_key(
        "analytics",
        "customers",
        &row,
        "upsert",
        "mongodb",
        "default",
        "manifest-v1",
        "source-checksum-1",
    );
    assert_ne!(
        base, other_project,
        "different project must produce a different task"
    );
}

/// `extract_row_key` must isolate the primary-key columns even when the
/// source payload carries unrelated fields — so the same logical row
/// always hashes to the same row-key signature regardless of payload
/// noise.
#[test]
fn extract_row_key_handles_compound_keys() {
    let table = ManifestTable {
        message_name: "acme.billing.v1.OrderItem".to_string(),
        schema: "billing".to_string(),
        table: "order_items".to_string(),
        primary_key: vec!["order_id".to_string(), "item_id".to_string()],
        projections: vec![projection_for(
            "acme.billing.v1.OrderItem",
            "analytics",
            "clickhouse",
            "order_items_facts",
            vec![],
        )],
        ..Default::default()
    };
    let manifest = CatalogManifest {
        checksum_sha256: "compound-pk-v1".into(),
        tables: vec![table],
        ..Default::default()
    };
    let plan = &ProjectionPlan::from_manifest(&manifest)[0];

    let payload = json!({
        "order_id": "ord-1",
        "item_id": "sku-9",
        "quantity": 3,
        "price_cents": 4_999,
    });
    let key = plan.extract_row_key(&payload);
    let expected = json!({ "order_id": "ord-1", "item_id": "sku-9" });
    assert_eq!(
        key, expected,
        "compound key extraction must drop non-PK fields"
    );
}

/// `source_checksum` must be **deterministic** over the same payload so
/// the idempotency key remains stable, and must **change** when the
/// payload changes so stale dedup doesn't suppress a real update.
#[test]
fn source_checksum_is_payload_sensitive() {
    let a = json!({ "id": "cust-1", "email": "alice@acme.com" });
    let b = json!({ "id": "cust-1", "email": "alice@acme.com" });
    let c = json!({ "id": "cust-1", "email": "alice+changed@acme.com" });
    let nul = json!({ "id": "cust-1", "email": "alice@acme.com", "note": "ok\u{0}then" });
    let stripped = json!({ "id": "cust-1", "email": "alice@acme.com", "note": "okthen" });

    assert_eq!(
        ProjectionEngine::source_checksum(&a),
        ProjectionEngine::source_checksum(&b),
        "identical payloads → identical checksums"
    );
    assert_ne!(
        ProjectionEngine::source_checksum(&a),
        ProjectionEngine::source_checksum(&c),
        "changed payload → new checksum"
    );
    assert_eq!(
        ProjectionEngine::source_checksum(&nul),
        ProjectionEngine::source_checksum(&stripped),
        "NUL bytes are stripped before projection JSONB persistence"
    );
}

/// U3 gate: "Projection lag is visible per project/backend/resource."
/// The new gauge `set_projection_oldest_pending_age_seconds` carries
/// `(project, backend, instance, kind)` labels — the exact dimensions
/// the gate demands. NoopMetrics implements it (compile-checked) so the
/// trait method is reachable from every consumer.
#[test]
fn oldest_pending_age_metric_is_reachable_with_full_label_set() {
    // Compile-checked: if the trait method or its label arity changes,
    // this fails to build. The NoopMetrics impl proves the trait method
    // is dispatchable through `dyn MetricsRecorder` (i.e. the trait
    // method is object-safe).
    let metrics: std::sync::Arc<dyn MetricsRecorder> = std::sync::Arc::new(NoopMetrics);
    metrics.set_projection_oldest_pending_age_seconds(
        "billing", "mongodb", "default", "document", 42.5,
    );
    metrics.set_projection_oldest_pending_age_seconds("billing", "redis", "default", "cache", 1.0);
}
