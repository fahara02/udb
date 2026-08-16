//! Per-backend manifest diff (U14).
//!
//! The original `migration/diff.rs` is **1,400+ lines of Postgres-shaped
//! semantics** — CreateTable, AddColumn, AddCheck, EnableRls, … nothing
//! in it understands a Qdrant `vectors_config` change, a Mongo collection
//! validator update, a Neo4j constraint addition, a ClickHouse engine
//! swap, or an S3 lifecycle policy diff. U14's acceptance gate ("every
//! generated backend artifact has a diff, apply, verify, and rollback or
//! manual-review path") is impossible to satisfy without coverage for the
//! non-relational backends.
//!
//! This module is the per-backend complement to `diff.rs`. Each top-level
//! `diff_*_targets(old, new)` walks the `ManifestStore` entries for one
//! backend, computes Create / Update / Drop deltas, and emits
//! `ChangeOperation`s with a safety classification based on backend-specific
//! migration rules.
//!
//! The output type is the **same** `ChangeOperation` the Postgres path
//! emits, so the migration apply/verify pipeline doesn't need to know
//! which backend produced a given op. New `ChangeKind` variants are
//! added for backend-specific operations that don't fit the existing PG
//! variants.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::generation::manifest::{CatalogManifest, ManifestStore, ManifestStoreOption};
use crate::migration::diff::{ChangeKind, ChangeOperation, ChangeSafety, is_data_destructive};

// ── MigrationPhase ────────────────────────────────────────────────────────────

/// Online-migration lifecycle phases (U14).
///
/// Per the upgrade plan, every backend-aware change should advance through
/// these phases so the operator portal can show progress and the runtime
/// can apply per-phase guards (e.g. don't switch traffic until validate
/// passes). Each `ChangeOperation` can declare which phases apply via
/// `BackendChange::phases` below.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MigrationPhase {
    /// Stand up the new target resource (collection, bucket, index) so the
    /// backfill has something to write into. Idempotent.
    #[default]
    Prepare,
    /// Copy data from the old shape to the new one. Bulk, idempotent,
    /// resumable from the projection-task table.
    Backfill,
    /// Sample new vs old and assert the migration's correctness predicate
    /// before any traffic is routed.
    Validate,
    /// Atomically swap the live read/write path to the new target.
    Switch,
    /// Drop the old shape now that traffic has moved over.
    Cleanup,
}

impl MigrationPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prepare => "prepare",
            Self::Backfill => "backfill",
            Self::Validate => "validate",
            Self::Switch => "switch",
            Self::Cleanup => "cleanup",
        }
    }

    /// Ordered list — for "for every phase, …" iteration.
    pub fn all() -> &'static [MigrationPhase] {
        &[
            MigrationPhase::Prepare,
            MigrationPhase::Backfill,
            MigrationPhase::Validate,
            MigrationPhase::Switch,
            MigrationPhase::Cleanup,
        ]
    }
}

// ── Backend store extraction ─────────────────────────────────────────────────

/// Group `ManifestStore` entries from a manifest by backend token. Used by
/// every `diff_*_targets` to compute `(old_by_backend, new_by_backend)`
/// then walk the keys.
///
/// The key is `(logical_name, resource_name)` so a backend with multiple
/// resources of the same logical type (e.g. two Qdrant collections for
/// one message) doesn't collide.
fn group_stores_by_resource<'a>(
    manifest: &'a CatalogManifest,
    backend: &str,
) -> BTreeMap<(String, String), &'a ManifestStore> {
    let mut out = BTreeMap::new();
    for store in &manifest.stores {
        if !store.backend.eq_ignore_ascii_case(backend) {
            continue;
        }
        out.insert(
            (store.logical_name.clone(), store.resource_name.clone()),
            store,
        );
    }
    out
}

/// Walk `(old, new)` store sets and emit Create/Update/Drop operations.
///
/// The CB closure renders a backend-specific `ChangeOperation` from
/// the delta. The shared walker handles classification (new key → Create,
/// dropped key → Drop, same key with different options → Update) and
/// fingerprinting.
fn walk_store_delta<C, U, D>(
    old: Option<&CatalogManifest>,
    new: &CatalogManifest,
    backend: &str,
    on_create: C,
    on_update: U,
    on_drop: D,
) -> Vec<ChangeOperation>
where
    C: Fn(&ManifestStore) -> ChangeOperation,
    U: Fn(&ManifestStore, &ManifestStore) -> Option<ChangeOperation>,
    D: Fn(&ManifestStore) -> ChangeOperation,
{
    let new_stores = group_stores_by_resource(new, backend);
    let old_stores = match old {
        Some(m) => group_stores_by_resource(m, backend),
        None => BTreeMap::new(),
    };
    let mut ops = Vec::new();
    for (key, new_store) in &new_stores {
        match old_stores.get(key) {
            None => ops.push(on_create(new_store)),
            Some(old_store) => {
                if !options_equal(&old_store.options, &new_store.options)
                    || old_store.database_name != new_store.database_name
                    || old_store.namespace != new_store.namespace
                {
                    if let Some(op) = on_update(old_store, new_store) {
                        ops.push(op);
                    }
                }
            }
        }
    }
    for (key, old_store) in &old_stores {
        if !new_stores.contains_key(key) {
            ops.push(on_drop(old_store));
        }
    }
    ops
}

fn options_equal(a: &[ManifestStoreOption], b: &[ManifestStoreOption]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a_map: BTreeMap<&str, &str> = BTreeMap::new();
    for opt in a {
        a_map.insert(&opt.key, &opt.value);
    }
    for opt in b {
        if a_map.get(opt.key.as_str()) != Some(&opt.value.as_str()) {
            return false;
        }
    }
    true
}

fn option_value(opts: &[ManifestStoreOption], key: &str) -> Option<String> {
    opts.iter()
        .find(|o| o.key.eq_ignore_ascii_case(key))
        .map(|o| o.value.clone())
}

/// Compute a stable fingerprint string used by the apply pipeline to
/// match operations back to their stored migration ledger entries.
fn fp(backend: &str, kind: &str, resource: &str, options: &[ManifestStoreOption]) -> String {
    let mut parts = vec![backend.to_string(), kind.to_string(), resource.to_string()];
    let mut sorted: Vec<&ManifestStoreOption> = options.iter().collect();
    sorted.sort_by(|a, b| a.key.cmp(&b.key));
    for o in sorted {
        parts.push(format!("{}={}", o.key, o.value));
    }
    parts.join("|")
}

// ── Qdrant ────────────────────────────────────────────────────────────────────

/// Diff Qdrant `ManifestStore`s. Per the upgrade plan, Qdrant changes
/// cover: collection vector config, indexes, payload schema.
///
/// Safety classification:
/// - **Create collection**: SafeAuto (new resource, no data at risk).
/// - **Update vector size / distance**: RequiresReview (existing points
///   become incompatible if vector dim changes; needs a rebuild).
/// - **Update non-data options** (HNSW config, optimizer threshold):
///   SafeAuto (Qdrant can update these in-place).
/// - **Drop collection**: RequiresReview.
pub fn diff_qdrant_targets(
    old: Option<&CatalogManifest>,
    new: &CatalogManifest,
) -> Vec<ChangeOperation> {
    walk_store_delta(
        old,
        new,
        "qdrant",
        |store| ChangeOperation {
            kind: ChangeKind::CreateCollection,
            data_destructive: is_data_destructive(&ChangeKind::CreateCollection),
            safety: ChangeSafety::SafeAuto,
            priority: 100,
            schema: store.namespace.clone(),
            table: store.resource_name.clone(),
            column: String::new(),
            object_name: store.logical_name.clone(),
            reason: "Qdrant collection added by manifest diff".into(),
            blocked_reason: String::new(),
            fingerprint: fp(
                "qdrant",
                "create_collection",
                &store.resource_name,
                &store.options,
            ),
        },
        |old_store, new_store| {
            let old_size = option_value(&old_store.options, "vector_size");
            let new_size = option_value(&new_store.options, "vector_size");
            let old_distance = option_value(&old_store.options, "distance");
            let new_distance = option_value(&new_store.options, "distance");
            let breaking = (old_size.is_some() && new_size.is_some() && old_size != new_size)
                || (old_distance.is_some()
                    && new_distance.is_some()
                    && old_distance != new_distance);
            Some(ChangeOperation {
                kind: ChangeKind::UpdateCollection,
                data_destructive: is_data_destructive(&ChangeKind::UpdateCollection),
                safety: if breaking {
                    ChangeSafety::RequiresReview
                } else {
                    ChangeSafety::SafeAuto
                },
                priority: 110,
                schema: new_store.namespace.clone(),
                table: new_store.resource_name.clone(),
                column: String::new(),
                object_name: new_store.logical_name.clone(),
                reason: if breaking {
                    "Qdrant vector dimension or distance changed; existing points must be rebuilt"
                        .into()
                } else {
                    "Qdrant collection options updated".into()
                },
                blocked_reason: String::new(),
                fingerprint: fp(
                    "qdrant",
                    "update_collection",
                    &new_store.resource_name,
                    &new_store.options,
                ),
            })
        },
        |store| ChangeOperation {
            kind: ChangeKind::DropCollection,
            data_destructive: is_data_destructive(&ChangeKind::DropCollection),
            safety: ChangeSafety::RequiresReview,
            priority: 200,
            schema: store.namespace.clone(),
            table: store.resource_name.clone(),
            column: String::new(),
            object_name: store.logical_name.clone(),
            reason: "Qdrant collection dropped".into(),
            blocked_reason: "manifest removed this collection; review before dropping vector data"
                .into(),
            fingerprint: fp(
                "qdrant",
                "drop_collection",
                &store.resource_name,
                &store.options,
            ),
        },
    )
}

// ── MongoDB ──────────────────────────────────────────────────────────────────

/// Diff MongoDB `ManifestStore`s. Covers collection validator, indexes,
/// TTL changes.
pub fn diff_mongodb_targets(
    old: Option<&CatalogManifest>,
    new: &CatalogManifest,
) -> Vec<ChangeOperation> {
    walk_store_delta(
        old,
        new,
        "mongodb",
        |store| ChangeOperation {
            kind: ChangeKind::CreateCollection,
            data_destructive: is_data_destructive(&ChangeKind::CreateCollection),
            safety: ChangeSafety::SafeAuto,
            priority: 100,
            schema: store.database_name.clone(),
            table: store.resource_name.clone(),
            column: String::new(),
            object_name: store.logical_name.clone(),
            reason: "MongoDB collection added".into(),
            blocked_reason: String::new(),
            fingerprint: fp(
                "mongodb",
                "create_collection",
                &store.resource_name,
                &store.options,
            ),
        },
        |_old, new_store| {
            let validator_changed = option_value(&_old.options, "validator")
                != option_value(&new_store.options, "validator");
            Some(ChangeOperation {
                kind: ChangeKind::UpdateValidator,
                data_destructive: is_data_destructive(&ChangeKind::UpdateValidator),
                safety: if validator_changed {
                    ChangeSafety::RequiresReview
                } else {
                    ChangeSafety::SafeAuto
                },
                priority: 110,
                schema: new_store.database_name.clone(),
                table: new_store.resource_name.clone(),
                column: String::new(),
                object_name: new_store.logical_name.clone(),
                reason: if validator_changed {
                    "MongoDB validator changed; existing documents must be reviewed".into()
                } else {
                    "MongoDB collection options updated (indexes/TTL)".into()
                },
                blocked_reason: String::new(),
                fingerprint: fp(
                    "mongodb",
                    "update_validator",
                    &new_store.resource_name,
                    &new_store.options,
                ),
            })
        },
        |store| ChangeOperation {
            kind: ChangeKind::DropCollection,
            data_destructive: is_data_destructive(&ChangeKind::DropCollection),
            safety: ChangeSafety::RequiresReview,
            priority: 200,
            schema: store.database_name.clone(),
            table: store.resource_name.clone(),
            column: String::new(),
            object_name: store.logical_name.clone(),
            reason: "MongoDB collection dropped".into(),
            blocked_reason:
                "manifest removed this collection; review before dropping document data".into(),
            fingerprint: fp(
                "mongodb",
                "drop_collection",
                &store.resource_name,
                &store.options,
            ),
        },
    )
}

// ── Neo4j ────────────────────────────────────────────────────────────────────

/// Diff Neo4j `ManifestStore`s. Covers constraints, indexes, labels.
pub fn diff_neo4j_targets(
    old: Option<&CatalogManifest>,
    new: &CatalogManifest,
) -> Vec<ChangeOperation> {
    walk_store_delta(
        old,
        new,
        "neo4j",
        |store| ChangeOperation {
            kind: ChangeKind::CreateConstraint,
            data_destructive: is_data_destructive(&ChangeKind::CreateConstraint),
            safety: ChangeSafety::SafeAuto,
            priority: 100,
            schema: String::new(),
            table: store.resource_name.clone(),
            column: String::new(),
            object_name: store.logical_name.clone(),
            reason: "Neo4j constraint/label added".into(),
            blocked_reason: String::new(),
            fingerprint: fp(
                "neo4j",
                "create_constraint",
                &store.resource_name,
                &store.options,
            ),
        },
        |_old, new_store| {
            Some(ChangeOperation {
                kind: ChangeKind::UpdateConstraint,
                data_destructive: is_data_destructive(&ChangeKind::UpdateConstraint),
                safety: ChangeSafety::RequiresReview,
                priority: 110,
                schema: String::new(),
                table: new_store.resource_name.clone(),
                column: String::new(),
                object_name: new_store.logical_name.clone(),
                reason: "Neo4j constraint or index options changed".into(),
                blocked_reason: String::new(),
                fingerprint: fp(
                    "neo4j",
                    "update_constraint",
                    &new_store.resource_name,
                    &new_store.options,
                ),
            })
        },
        |store| ChangeOperation {
            kind: ChangeKind::DropConstraint,
            data_destructive: is_data_destructive(&ChangeKind::DropConstraint),
            safety: ChangeSafety::RequiresReview,
            priority: 200,
            schema: String::new(),
            table: store.resource_name.clone(),
            column: String::new(),
            object_name: store.logical_name.clone(),
            reason: "Neo4j constraint/label dropped".into(),
            blocked_reason: "review before dropping graph constraint".into(),
            fingerprint: fp(
                "neo4j",
                "drop_constraint",
                &store.resource_name,
                &store.options,
            ),
        },
    )
}

// ── ClickHouse ────────────────────────────────────────────────────────────────

/// Diff ClickHouse `ManifestStore`s. Covers engine, ORDER BY, PARTITION,
/// table-level changes.
pub fn diff_clickhouse_targets(
    old: Option<&CatalogManifest>,
    new: &CatalogManifest,
) -> Vec<ChangeOperation> {
    walk_store_delta(
        old,
        new,
        "clickhouse",
        |store| ChangeOperation {
            kind: ChangeKind::CreateTable,
            data_destructive: is_data_destructive(&ChangeKind::CreateTable),
            safety: ChangeSafety::SafeAuto,
            priority: 100,
            schema: store.database_name.clone(),
            table: store.resource_name.clone(),
            column: String::new(),
            object_name: store.logical_name.clone(),
            reason: "ClickHouse table added".into(),
            blocked_reason: String::new(),
            fingerprint: fp(
                "clickhouse",
                "create_table",
                &store.resource_name,
                &store.options,
            ),
        },
        |old_store, new_store| {
            // ENGINE / ORDER BY / PARTITION changes are blocking — they
            // require recreating the table, which means dropping and
            // re-ingesting data.
            let breaking = option_value(&old_store.options, "engine")
                != option_value(&new_store.options, "engine")
                || option_value(&old_store.options, "order_by")
                    != option_value(&new_store.options, "order_by")
                || option_value(&old_store.options, "partition_by")
                    != option_value(&new_store.options, "partition_by");
            Some(ChangeOperation {
                kind: ChangeKind::ChangeTableEngine,
                data_destructive: is_data_destructive(&ChangeKind::ChangeTableEngine),
                safety: if breaking {
                    ChangeSafety::Blocked
                } else {
                    ChangeSafety::SafeAuto
                },
                priority: 110,
                schema: new_store.database_name.clone(),
                table: new_store.resource_name.clone(),
                column: String::new(),
                object_name: new_store.logical_name.clone(),
                reason: if breaking {
                    "ClickHouse engine / ORDER BY / PARTITION change requires recreating the table"
                        .into()
                } else {
                    "ClickHouse table-level option updated".into()
                },
                blocked_reason: if breaking {
                    "ClickHouse cannot ALTER engine/ORDER BY/PARTITION in place; needs a new \
                     table + INSERT … SELECT migration"
                        .into()
                } else {
                    String::new()
                },
                fingerprint: fp(
                    "clickhouse",
                    "change_table_engine",
                    &new_store.resource_name,
                    &new_store.options,
                ),
            })
        },
        |store| ChangeOperation {
            kind: ChangeKind::DropTable,
            data_destructive: is_data_destructive(&ChangeKind::DropTable),
            safety: ChangeSafety::RequiresReview,
            priority: 200,
            schema: store.database_name.clone(),
            table: store.resource_name.clone(),
            column: String::new(),
            object_name: store.logical_name.clone(),
            reason: "ClickHouse table dropped".into(),
            blocked_reason: "review before dropping analytics data".into(),
            fingerprint: fp(
                "clickhouse",
                "drop_table",
                &store.resource_name,
                &store.options,
            ),
        },
    )
}

// ── S3 / MinIO ───────────────────────────────────────────────────────────────

/// Diff S3 / MinIO `ManifestStore`s. Covers bucket policy, lifecycle,
/// versioning. The actual bucket-level resources are usually created
/// once and rarely changed; the diff still surfaces them so the apply
/// pipeline can verify.
pub fn diff_s3_targets(
    old: Option<&CatalogManifest>,
    new: &CatalogManifest,
) -> Vec<ChangeOperation> {
    let mut ops = walk_store_delta(
        old,
        new,
        "s3",
        |store| ChangeOperation {
            kind: ChangeKind::CreateBucket,
            data_destructive: is_data_destructive(&ChangeKind::CreateBucket),
            safety: ChangeSafety::SafeAuto,
            priority: 100,
            schema: String::new(),
            table: store.resource_name.clone(),
            column: String::new(),
            object_name: store.logical_name.clone(),
            reason: "S3 bucket added".into(),
            blocked_reason: String::new(),
            fingerprint: fp("s3", "create_bucket", &store.resource_name, &store.options),
        },
        |_old, new_store| {
            Some(ChangeOperation {
                kind: ChangeKind::UpdateLifecyclePolicy,
                data_destructive: is_data_destructive(&ChangeKind::UpdateLifecyclePolicy),
                safety: ChangeSafety::RequiresReview,
                priority: 110,
                schema: String::new(),
                table: new_store.resource_name.clone(),
                column: String::new(),
                object_name: new_store.logical_name.clone(),
                reason: "S3 bucket policy / lifecycle / versioning changed".into(),
                blocked_reason: String::new(),
                fingerprint: fp(
                    "s3",
                    "update_lifecycle",
                    &new_store.resource_name,
                    &new_store.options,
                ),
            })
        },
        |store| ChangeOperation {
            kind: ChangeKind::DropBucket,
            data_destructive: is_data_destructive(&ChangeKind::DropBucket),
            safety: ChangeSafety::Blocked,
            priority: 200,
            schema: String::new(),
            table: store.resource_name.clone(),
            column: String::new(),
            object_name: store.logical_name.clone(),
            reason: "S3 bucket dropped".into(),
            blocked_reason:
                "S3 bucket deletion is irreversible and rarely automated; manual review required"
                    .into(),
            fingerprint: fp("s3", "drop_bucket", &store.resource_name, &store.options),
        },
    );
    // MinIO is the historical default S3-compatible store — fold its
    // diffs into the same s3 result so callers iterate one list.
    ops.extend(walk_store_delta(
        old,
        new,
        "minio",
        |store| ChangeOperation {
            kind: ChangeKind::CreateBucket,
            data_destructive: is_data_destructive(&ChangeKind::CreateBucket),
            safety: ChangeSafety::SafeAuto,
            priority: 100,
            schema: String::new(),
            table: store.resource_name.clone(),
            column: String::new(),
            object_name: store.logical_name.clone(),
            reason: "MinIO bucket added".into(),
            blocked_reason: String::new(),
            fingerprint: fp(
                "minio",
                "create_bucket",
                &store.resource_name,
                &store.options,
            ),
        },
        |_old, new_store| {
            Some(ChangeOperation {
                kind: ChangeKind::UpdateLifecyclePolicy,
                data_destructive: is_data_destructive(&ChangeKind::UpdateLifecyclePolicy),
                safety: ChangeSafety::RequiresReview,
                priority: 110,
                schema: String::new(),
                table: new_store.resource_name.clone(),
                column: String::new(),
                object_name: new_store.logical_name.clone(),
                reason: "MinIO bucket policy / lifecycle changed".into(),
                blocked_reason: String::new(),
                fingerprint: fp(
                    "minio",
                    "update_lifecycle",
                    &new_store.resource_name,
                    &new_store.options,
                ),
            })
        },
        |store| ChangeOperation {
            kind: ChangeKind::DropBucket,
            data_destructive: is_data_destructive(&ChangeKind::DropBucket),
            safety: ChangeSafety::Blocked,
            priority: 200,
            schema: String::new(),
            table: store.resource_name.clone(),
            column: String::new(),
            object_name: store.logical_name.clone(),
            reason: "MinIO bucket dropped".into(),
            blocked_reason: "bucket deletion is irreversible; manual review required".into(),
            fingerprint: fp("minio", "drop_bucket", &store.resource_name, &store.options),
        },
    ));
    ops
}

/// Run every per-backend diff in one shot. Used by the migration planner
/// and the U14 acceptance test to verify "every generated backend
/// artifact has a diff path."
pub fn diff_all_backends(
    old: Option<&CatalogManifest>,
    new: &CatalogManifest,
) -> Vec<ChangeOperation> {
    let mut ops = Vec::new();
    ops.extend(diff_qdrant_targets(old, new));
    ops.extend(diff_mongodb_targets(old, new));
    ops.extend(diff_neo4j_targets(old, new));
    ops.extend(diff_clickhouse_targets(old, new));
    ops.extend(diff_s3_targets(old, new));
    ops
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::manifest::ManifestStore;

    fn store(
        backend: &str,
        kind: &str,
        logical: &str,
        resource: &str,
        options: Vec<(&str, &str)>,
    ) -> ManifestStore {
        ManifestStore {
            store_kind: kind.into(),
            backend: backend.into(),
            logical_name: logical.into(),
            resource_name: resource.into(),
            namespace: "default".into(),
            database_name: "test".into(),
            options: options
                .into_iter()
                .map(|(k, v)| ManifestStoreOption {
                    key: k.into(),
                    value: v.into(),
                })
                .collect(),
            ..Default::default()
        }
    }

    fn manifest_with_stores(stores: Vec<ManifestStore>) -> CatalogManifest {
        CatalogManifest {
            checksum_sha256: "test".into(),
            stores,
            ..Default::default()
        }
    }

    #[test]
    fn migration_phases_have_canonical_order_and_tokens() {
        let names: Vec<&str> = MigrationPhase::all().iter().map(|p| p.as_str()).collect();
        assert_eq!(
            names,
            vec!["prepare", "backfill", "validate", "switch", "cleanup"]
        );
    }

    #[test]
    fn qdrant_create_collection_is_safe_auto() {
        let new = manifest_with_stores(vec![store(
            "qdrant",
            "vector",
            "customers",
            "customers_vec",
            vec![("vector_size", "384"), ("distance", "Cosine")],
        )]);
        let ops = diff_qdrant_targets(None, &new);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].kind, ChangeKind::CreateCollection);
        assert_eq!(ops[0].safety, ChangeSafety::SafeAuto);
    }

    #[test]
    fn qdrant_vector_size_change_is_requires_review() {
        let old = manifest_with_stores(vec![store(
            "qdrant",
            "vector",
            "customers",
            "customers_vec",
            vec![("vector_size", "384")],
        )]);
        let new = manifest_with_stores(vec![store(
            "qdrant",
            "vector",
            "customers",
            "customers_vec",
            vec![("vector_size", "768")],
        )]);
        let ops = diff_qdrant_targets(Some(&old), &new);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].kind, ChangeKind::UpdateCollection);
        assert_eq!(ops[0].safety, ChangeSafety::RequiresReview);
        assert!(ops[0].reason.contains("vector dimension"));
    }

    #[test]
    fn clickhouse_engine_change_is_blocked() {
        let old = manifest_with_stores(vec![store(
            "clickhouse",
            "analytics",
            "events",
            "events_facts",
            vec![("engine", "MergeTree")],
        )]);
        let new = manifest_with_stores(vec![store(
            "clickhouse",
            "analytics",
            "events",
            "events_facts",
            vec![("engine", "ReplacingMergeTree")],
        )]);
        let ops = diff_clickhouse_targets(Some(&old), &new);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].kind, ChangeKind::ChangeTableEngine);
        assert_eq!(ops[0].safety, ChangeSafety::Blocked);
        assert!(!ops[0].blocked_reason.is_empty());
    }

    #[test]
    fn s3_bucket_drop_is_blocked() {
        let old = manifest_with_stores(vec![store(
            "s3",
            "object",
            "snapshots",
            "snapshot-bucket",
            vec![],
        )]);
        let new = manifest_with_stores(vec![]);
        let ops = diff_s3_targets(Some(&old), &new);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].kind, ChangeKind::DropBucket);
        assert_eq!(ops[0].safety, ChangeSafety::Blocked);
    }

    #[test]
    fn minio_diffs_fold_into_s3_result() {
        let new = manifest_with_stores(vec![
            store("minio", "object", "snapshots", "snapshot-bucket", vec![]),
            store("s3", "object", "exports", "export-bucket", vec![]),
        ]);
        let ops = diff_s3_targets(None, &new);
        assert_eq!(ops.len(), 2);
        // Both buckets created.
        assert!(ops.iter().all(|o| o.kind == ChangeKind::CreateBucket));
    }

    #[test]
    fn mongodb_validator_change_is_requires_review() {
        let old = manifest_with_stores(vec![store(
            "mongodb",
            "document",
            "users",
            "users_coll",
            vec![("validator", r#"{"required":["email"]}"#)],
        )]);
        let new = manifest_with_stores(vec![store(
            "mongodb",
            "document",
            "users",
            "users_coll",
            vec![("validator", r#"{"required":["email","name"]}"#)],
        )]);
        let ops = diff_mongodb_targets(Some(&old), &new);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].kind, ChangeKind::UpdateValidator);
        assert_eq!(ops[0].safety, ChangeSafety::RequiresReview);
    }

    /// **U14 acceptance gate**: every generated backend artifact has a
    /// diff path. A multi-backend manifest going from empty to full
    /// must produce one create op per backend without panic / silent
    /// drops.
    #[test]
    fn all_backends_have_a_diff_path() {
        let new = manifest_with_stores(vec![
            store(
                "qdrant",
                "vector",
                "v",
                "v_coll",
                vec![("vector_size", "8")],
            ),
            store("mongodb", "document", "d", "d_coll", vec![]),
            store("neo4j", "graph", "g", "G", vec![]),
            store("clickhouse", "analytics", "a", "a_table", vec![]),
            store("s3", "object", "o", "o_bucket", vec![]),
            store("minio", "object", "om", "om_bucket", vec![]),
        ]);
        let ops = diff_all_backends(None, &new);
        // 6 stores → 6 create ops (s3 + minio aggregate to 2 in diff_s3_targets).
        assert_eq!(
            ops.len(),
            6,
            "every backend must emit at least one Create op"
        );
        for op in &ops {
            assert!(
                op.fingerprint.contains('|'),
                "op {:?} missing fingerprint",
                op.kind
            );
        }
    }
}
