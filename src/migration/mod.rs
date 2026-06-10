pub mod apply;
pub mod diff;
pub mod diff_backends;
pub mod phase_runner; // U14: online migration phase orchestrator
pub mod plan;
pub mod sync; // relocated from crate-root db_ops_sync.rs (Phase H)

#[cfg(test)]
pub use apply::LoggingApplyTarget;
pub use apply::{
    ApplyError, ApplyFuture, ApplyTarget, ArtifactApplyResult, MigrationAuditSink,
    NoopMigrationAuditSink, apply_artifacts, apply_artifacts_audited, apply_artifacts_phased,
};
pub use diff::{ChangeKind, ChangeOperation, ChangeSafety, diff_manifests};
pub use phase_runner::PostgresPhaseLedger;
pub use plan::{
    LedgerEntry, MigrationFsmState, MigrationPlan, MigrationPlanConfig, ResourceAction,
    build_migration_plan,
};
