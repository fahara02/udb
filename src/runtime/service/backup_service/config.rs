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

pub(crate) const STATUS_COMPLETED: &str = "COMPLETED";

/// Upper bound on rows returned by a list RPC so one call can't unbound the store.
pub(crate) const MAX_LIST_ROWS: u32 = 500;

/// The object suffix for the (plaintext, non-secret) run manifest: it carries
/// only schema/table names, row counts, and ciphertext checksums — the integrity
/// anchor restore reads to find and verify each encrypted table artifact.
pub(crate) const MANIFEST_SUFFIX: &str = "manifest.json";
