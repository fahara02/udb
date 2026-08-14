//! Static identifiers for the native `BackupService`: the entity message names,
//! the versioned outbox topics, the run kind/status tokens, the list-row bound,
//! and the run-manifest object suffix. Extracted verbatim from the former god
//! file; every value is byte-stable for downstream audit/CDC consumers.

pub(crate) const BACKUP_RUN_MSG: &str = "udb.core.backup.entity.v1.BackupRun";
pub(crate) const BACKUP_POLICY_MSG: &str = "udb.core.backup.entity.v1.BackupPolicy";

pub(crate) const TOPIC_BACKUP_COMPLETED: &str = "udb.backup.run.completed.v1";
pub(crate) const TOPIC_BACKUP_RESTORED: &str = "udb.backup.run.restored.v1";
pub(crate) const TOPIC_POLICY_UPSERTED: &str = "udb.backup.policy.upserted.v1";
pub(crate) const TOPIC_POLICY_DELETED: &str = "udb.backup.policy.deleted.v1";

pub(crate) const KIND_BACKUP: &str = "BACKUP";
pub(crate) const KIND_RESTORE: &str = "RESTORE";

pub(crate) const STATUS_RUNNING: &str = "RUNNING";
pub(crate) const STATUS_COMPLETED: &str = "COMPLETED";

/// Upper bound on rows returned by a list RPC so one call can't unbound the store.
pub(crate) const MAX_LIST_ROWS: u32 = 500;

/// The object suffix for the (plaintext, non-secret) run manifest: it carries
/// only schema/table names, row counts, and ciphertext checksums — the integrity
/// anchor restore reads to find and verify each encrypted table artifact.
pub(crate) const MANIFEST_SUFFIX: &str = "manifest.json";

/// Maintenance cadence for the leader spawn site — env resolved ONCE via
/// `OnceLock` (a worker-spawn cadence knob, never a per-request read), mirroring
/// `lock_service::lock_expiry_interval` / `cache_service::cache_invalidation_interval`.
/// `UDB_BACKUP_MAINTENANCE_INTERVAL_SECS` overrides the 300s default; a
/// non-positive/unparseable value falls back to the default.
///
/// `allow(dead_code)`: called only by the leader-elected backup-maintenance spawn
/// in `service::serve()` (the `TODO(leader-wire)` in `retention.rs`), which is not
/// yet wired. Delete this allow once that spawn lands.
#[allow(dead_code)]
pub(crate) fn backup_maintenance_interval() -> std::time::Duration {
    const ENV_KNOB: &str = "UDB_BACKUP_MAINTENANCE_INTERVAL_SECS";
    const DEFAULT_SECS: u64 = 300;
    static SECS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let secs = *SECS.get_or_init(|| {
        std::env::var(ENV_KNOB)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_SECS)
    });
    std::time::Duration::from_secs(secs)
}
