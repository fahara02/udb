//! Apply engine extension — `ApplyTarget` trait for multi-backend migration apply.
//!
//! Each backend implements `ApplyTarget` so the apply engine can run migrations
//! without knowing the backend's wire protocol.  After every apply the engine
//! writes an audit record to `udb_migration_runs` and `udb_migration_op_ledger`.
//!
//! # Implementations shipped
//!
//! | Backend | Status |
//! |---------|--------|
//! | Real backend apply targets | Registered by runtime/backend code; missing targets fail closed. |
//!
//! Runtime admin migrations use a backend-keyed apply registry in
//! `runtime::core::catalog_admin`, where structured ledger operations are
//! dispatched to the configured backend executors. This generic trait remains
//! the low-level artifact-apply seam used by tests and future external targets.
//!
//! # Audit flow
//!
//! `apply_artifacts_audited()` wraps `apply_artifacts()` and writes one row to
//! `udb_migration_runs` before executing and updates it on completion.  Each
//! artifact outcome writes one row to `udb_migration_op_ledger`.
//!
//! The audit sink is abstracted behind `MigrationAuditSink` so that:
//! - `PostgresMigrationAuditSink` (in `crate::runtime::migration_audit`) writes to PG.
//! - `NoopMigrationAuditSink` is used for dry-runs and unit tests.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use crate::generation::GeneratedArtifact;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ApplyError {
    /// Underlying I/O or network error.
    Io(String),
    /// The artifact was rejected by the backend (e.g. syntax error).
    BackendRejected { backend: String, message: String },
    /// The artifact has already been applied and `verify_applied` returned true.
    AlreadyApplied,
    /// The backend is not reachable.
    Unreachable(String),
}

impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApplyError::Io(msg) => write!(f, "I/O error: {msg}"),
            ApplyError::BackendRejected { backend, message } => {
                write!(f, "{backend} rejected artifact: {message}")
            }
            ApplyError::AlreadyApplied => write!(f, "artifact already applied"),
            ApplyError::Unreachable(addr) => write!(f, "backend unreachable: {addr}"),
        }
    }
}

impl std::error::Error for ApplyError {}

// ── ApplyTarget trait ─────────────────────────────────────────────────────────

/// A boxed async future that returns `Result<T, ApplyError>`.
pub type ApplyFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ApplyError>> + Send + 'a>>;

/// Trait implemented by every backend's apply executor.
///
/// Implementors receive a `&GeneratedArtifact` and are responsible for:
/// 1. Parsing the artifact format (SQL, JSON, YAML, Cypher).
/// 2. Sending the appropriate API call or DDL statement.
/// 3. Returning `Ok(())` on success.
///
/// The apply engine calls `verify_applied` before `apply_artifact` to avoid
/// re-applying already-idempotent resources.  When `verify_applied` returns
/// `true`, the apply is skipped and logged as a no-op.
pub trait ApplyTarget: Send + Sync {
    /// Short identifier for the backend (e.g. `"postgres"`, `"qdrant"`).
    fn backend_name(&self) -> &'static str;

    /// Apply a single artifact to the backend.
    ///
    /// Must be idempotent — calling apply twice on the same artifact must not
    /// corrupt state.
    fn apply_artifact<'a>(&'a self, artifact: &'a GeneratedArtifact) -> ApplyFuture<'a, ()>;

    /// Return `true` when the artifact has already been applied (e.g. table
    /// exists, collection exists, bucket exists).
    ///
    /// When this returns `true` the apply engine skips `apply_artifact` and
    /// records a `verified` ledger entry.
    fn verify_applied<'a>(&'a self, artifact: &'a GeneratedArtifact) -> ApplyFuture<'a, bool>;
}

// ── LoggingApplyTarget ────────────────────────────────────────────────────────

/// Test-only no-op `ApplyTarget` that logs the artifact and returns `Ok(())`.
#[cfg(test)]
pub struct LoggingApplyTarget {
    name: &'static str,
}

#[cfg(test)]
impl LoggingApplyTarget {
    pub fn new(name: &'static str) -> Self {
        Self { name }
    }
}

#[cfg(test)]
impl ApplyTarget for LoggingApplyTarget {
    fn backend_name(&self) -> &'static str {
        self.name
    }

    fn apply_artifact<'a>(&'a self, artifact: &'a GeneratedArtifact) -> ApplyFuture<'a, ()> {
        Box::pin(async move {
            tracing::info!(
                backend = self.name,
                rel_path = %artifact.rel_path,
                kind = %artifact.kind,
                "UDB apply (logging stub): would apply artifact"
            );
            Ok(())
        })
    }

    fn verify_applied<'a>(&'a self, _artifact: &'a GeneratedArtifact) -> ApplyFuture<'a, bool> {
        Box::pin(async { Ok(false) })
    }
}

// ── ApplyResult ───────────────────────────────────────────────────────────────

/// Result of applying a single artifact.
#[derive(Debug, Clone)]
pub struct ArtifactApplyResult {
    pub rel_path: String,
    pub backend: String,
    pub applied: bool,
    pub skipped: bool,
    pub error: Option<String>,
}

/// Apply a slice of artifacts through a target, collecting results.
///
/// Never panics — errors are captured into `ArtifactApplyResult.error`.
pub async fn apply_artifacts(
    target: &dyn ApplyTarget,
    artifacts: &[GeneratedArtifact],
) -> Vec<ArtifactApplyResult> {
    let mut results = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        // Fail-closed: a verify_applied error (connection lost, permission denied)
        // must NOT be downgraded to "not applied" — that would re-run
        // apply_artifact and duplicate an already-applied resource. On verify
        // failure, record the error and skip the apply for this artifact.
        let already_applied = match target.verify_applied(artifact).await {
            Ok(applied) => applied,
            Err(err) => {
                results.push(ArtifactApplyResult {
                    rel_path: artifact.rel_path.clone(),
                    backend: target.backend_name().to_string(),
                    applied: false,
                    skipped: false,
                    error: Some(format!(
                        "verify_applied failed (not applied to avoid duplicate): {err}"
                    )),
                });
                continue;
            }
        };

        if already_applied {
            results.push(ArtifactApplyResult {
                rel_path: artifact.rel_path.clone(),
                backend: target.backend_name().to_string(),
                applied: false,
                skipped: true,
                error: None,
            });
            continue;
        }

        match target.apply_artifact(artifact).await {
            Ok(()) => results.push(ArtifactApplyResult {
                rel_path: artifact.rel_path.clone(),
                backend: target.backend_name().to_string(),
                applied: true,
                skipped: false,
                error: None,
            }),
            Err(err) => results.push(ArtifactApplyResult {
                rel_path: artifact.rel_path.clone(),
                backend: target.backend_name().to_string(),
                applied: false,
                skipped: false,
                error: Some(err.to_string()),
            }),
        }
    }
    results
}

// ── MigrationAuditSink ────────────────────────────────────────────────────────

/// Abstraction over the audit persistence layer.
///
/// Implementations write records to `udb_migration_runs` and
/// `udb_migration_op_ledger`.  The trait is intentionally thin — the apply
/// engine only needs to record what happened; it does not need to read back.
///
/// # Implementations
///
/// | Type | Where | Purpose |
/// |------|-------|---------|
/// | `NoopMigrationAuditSink` | this module | Dry-run / unit tests |
/// | `PostgresMigrationAuditSink` | `crate::runtime::migration_audit` | Writes to PG tables |
pub trait MigrationAuditSink: Send + Sync {
    /// Open a new run record in `udb_migration_runs`.
    ///
    /// Returns an opaque `run_id` string (UUID) that callers pass to
    /// `record_op` and `finish_run`.
    fn start_run<'a>(
        &'a self,
        catalog_version: &'a str,
        operations_hash: &'a str,
    ) -> ApplyFuture<'a, String>;

    /// Append one ledger row to `udb_migration_op_ledger`.
    ///
    /// `index` is the 0-based position of the artifact in the run's artifact
    /// slice.  `result` is the outcome from `apply_artifacts`.
    fn record_op<'a>(
        &'a self,
        run_id: &'a str,
        index: usize,
        result: &'a ArtifactApplyResult,
    ) -> ApplyFuture<'a, ()>;

    /// Update the run record in `udb_migration_runs` to a terminal state.
    ///
    /// `state` must be one of `"COMPLETED"`, `"ERROR"`, or `"DEAD_LETTER"`.
    /// `error` is an empty string on success.
    fn finish_run<'a>(
        &'a self,
        run_id: &'a str,
        state: &'a str,
        error: &'a str,
    ) -> ApplyFuture<'a, ()>;
}

// ── NoopMigrationAuditSink ────────────────────────────────────────────────────

/// A no-op audit sink that discards all records.
///
/// Use when a real database connection is not available (dry-runs, tests).
/// `start_run` returns a synthetic UUID so callers can treat it uniformly.
pub struct NoopMigrationAuditSink;

impl MigrationAuditSink for NoopMigrationAuditSink {
    fn start_run<'a>(
        &'a self,
        _catalog_version: &'a str,
        _operations_hash: &'a str,
    ) -> ApplyFuture<'a, String> {
        Box::pin(async { Ok("00000000-0000-0000-0000-000000000000".to_string()) })
    }

    fn record_op<'a>(
        &'a self,
        _run_id: &'a str,
        _index: usize,
        _result: &'a ArtifactApplyResult,
    ) -> ApplyFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn finish_run<'a>(
        &'a self,
        _run_id: &'a str,
        _state: &'a str,
        _error: &'a str,
    ) -> ApplyFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

// ── apply_artifacts_audited ───────────────────────────────────────────────────

/// Apply artifacts and write audit records to the provided sink.
///
/// This is the production entry point.  It:
/// 1. Calls `sink.start_run()` to open a `udb_migration_runs` row.
/// 2. Calls `apply_artifacts()` to execute each artifact.
/// 3. For each result calls `sink.record_op()` to append a `udb_migration_op_ledger` row.
/// 4. Calls `sink.finish_run("COMPLETED")` or `sink.finish_run("ERROR")`.
///
/// Returns the same `Vec<ArtifactApplyResult>` as `apply_artifacts()`.
/// Audit sink errors are logged but never propagate — they must not abort the
/// apply run itself.
pub async fn apply_artifacts_audited(
    target: &dyn ApplyTarget,
    artifacts: &[GeneratedArtifact],
    sink: &dyn MigrationAuditSink,
    catalog_version: &str,
    operations_hash: &str,
) -> Vec<ArtifactApplyResult> {
    let run_id = match sink.start_run(catalog_version, operations_hash).await {
        Ok(id) => id,
        Err(err) => {
            tracing::warn!(error = %err, "audit: failed to start migration run — continuing without audit");
            "00000000-0000-0000-0000-000000000000".to_string()
        }
    };

    let results = apply_artifacts(target, artifacts).await;

    let has_error = results.iter().any(|r| r.error.is_some());
    for (idx, result) in results.iter().enumerate() {
        if let Err(err) = sink.record_op(&run_id, idx, result).await {
            tracing::warn!(error = %err, run_id = %run_id, idx = idx, "audit: failed to record op ledger entry");
        }
    }

    let (state, error_msg) = if has_error {
        let errors: Vec<_> = results.iter().filter_map(|r| r.error.as_deref()).collect();
        ("ERROR", errors.join("; "))
    } else {
        ("COMPLETED", String::new())
    };

    if let Err(err) = sink.finish_run(&run_id, state, &error_msg).await {
        tracing::warn!(error = %err, run_id = %run_id, "audit: failed to finish migration run");
    }

    results
}

// ── apply_artifacts_phased (C: 2026-05-30) ─────────────────────────────────────

/// C (2026-05-30): wrap `apply_artifacts_audited` in the
/// `phase_runner` orchestrator so apply runs are recorded as
/// per-phase rows in the durable `PhaseLedger` and a crashed run
/// resumes from the last incomplete phase.
///
/// Phase mapping:
///
/// - `Prepare`   — pre-flight: verify artifact reachability. Artifacts that
///                 already verify are allowed through so crash-resume and
///                 verify-only plans can advance to `Validate`.
/// - `Backfill`  — the real `apply_artifacts_audited` call. This is
///                 where the SQL/DDL is executed and audit ledger
///                 rows are written.
/// - `Validate`  — post-flight: re-run `verify_applied` and refuse
///                 to advance if any artifact still doesn't verify
///                 (caller's idempotency contract is broken).
/// - `Switch`    — operator-controlled cut-over. The default hook
///                 records "Completed" — operators override via
///                 `apply_artifacts_phased_with_hook` to plug in
///                 e.g. traffic shifting.
/// - `Cleanup`   — final marker. Default no-op; operators can
///                 override to drop legacy resources.
///
/// Returns the `RunnerOutcome` from `phase_runner::run_to_completion`
/// alongside the per-artifact results from the Backfill phase.
pub async fn apply_artifacts_phased(
    run_id: &str,
    target: &dyn ApplyTarget,
    artifacts: &[GeneratedArtifact],
    sink: &dyn MigrationAuditSink,
    ledger: &dyn crate::migration::phase_runner::PhaseLedger,
    catalog_version: &str,
    operations_hash: &str,
) -> Result<
    (
        crate::migration::phase_runner::RunnerOutcome,
        Vec<ArtifactApplyResult>,
    ),
    String,
> {
    use std::sync::Mutex;

    use crate::migration::diff_backends::MigrationPhase;
    use crate::migration::phase_runner::{MigrationPhaseHook, run_to_completion};

    // The Backfill phase produces ArtifactApplyResults; the hook
    // stows them so the caller can return them after run completion.
    // Mutex (not RwLock) because we only borrow during sequential
    // phase execution.
    struct ApplyHook<'a> {
        target: &'a dyn ApplyTarget,
        artifacts: &'a [GeneratedArtifact],
        sink: &'a dyn MigrationAuditSink,
        catalog_version: &'a str,
        operations_hash: &'a str,
        results: Mutex<Vec<ArtifactApplyResult>>,
    }

    #[async_trait::async_trait]
    impl MigrationPhaseHook for ApplyHook<'_> {
        async fn run(&self, phase: MigrationPhase) -> Result<(), String> {
            match phase {
                MigrationPhase::Prepare => {
                    for art in self.artifacts {
                        if let Err(err) = self.target.verify_applied(art).await {
                            return Err(format!(
                                "prepare: verify_applied('{}') failed: {err}",
                                art.rel_path
                            ));
                        }
                    }
                    Ok(())
                }
                MigrationPhase::Backfill => {
                    let results = apply_artifacts_audited(
                        self.target,
                        self.artifacts,
                        self.sink,
                        self.catalog_version,
                        self.operations_hash,
                    )
                    .await;
                    let errs: Vec<String> =
                        results.iter().filter_map(|r| r.error.clone()).collect();
                    *self.results.lock().expect("ApplyHook.results poisoned") = results;
                    if errs.is_empty() {
                        Ok(())
                    } else {
                        Err(format!("backfill errors: {}", errs.join("; ")))
                    }
                }
                MigrationPhase::Validate => {
                    // Post-flight: every artifact must now verify.
                    for art in self.artifacts {
                        match self.target.verify_applied(art).await {
                            Ok(true) => {}
                            Ok(false) => {
                                return Err(format!(
                                    "validate: artifact '{}' did not verify after apply",
                                    art.rel_path
                                ));
                            }
                            Err(err) => {
                                return Err(format!(
                                    "validate: verify_applied('{}') failed: {err}",
                                    art.rel_path
                                ));
                            }
                        }
                    }
                    Ok(())
                }
                // Switch/Cleanup default to no-op success — operators
                // override via `apply_artifacts_phased_with_hook`.
                MigrationPhase::Switch | MigrationPhase::Cleanup => Ok(()),
            }
        }
    }

    let hook = ApplyHook {
        target,
        artifacts,
        sink,
        catalog_version,
        operations_hash,
        results: Mutex::new(Vec::new()),
    };
    let outcome = run_to_completion(run_id, ledger, &hook).await?;
    let results = hook
        .results
        .into_inner()
        .expect("ApplyHook.results poisoned");
    Ok((outcome, results))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_artifact(rel_path: &str) -> GeneratedArtifact {
        GeneratedArtifact {
            rel_path: rel_path.to_string(),
            kind: "bootstrap_test".to_string(),
            schema: "test".to_string(),
            table: "dummy".to_string(),
            content: "-- dummy\n".to_string(),
        }
    }

    #[tokio::test]
    async fn logging_target_apply_always_ok() {
        let target = LoggingApplyTarget::new("test");
        let artifact = dummy_artifact("test/001_dummy.sql");
        assert!(target.apply_artifact(&artifact).await.is_ok());
    }

    #[tokio::test]
    async fn logging_target_verify_always_false() {
        let target = LoggingApplyTarget::new("test");
        let artifact = dummy_artifact("test/001_dummy.sql");
        assert!(!target.verify_applied(&artifact).await.unwrap());
    }

    #[tokio::test]
    async fn apply_artifacts_collects_results() {
        let target = LoggingApplyTarget::new("test");
        let artifacts = vec![
            dummy_artifact("postgres/001_users.sql"),
            dummy_artifact("postgres/002_docs.sql"),
        ];
        let results = apply_artifacts(&target, &artifacts).await;
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.applied && r.error.is_none()));
    }

    #[tokio::test]
    async fn noop_audit_sink_start_run_returns_zero_uuid() {
        let sink = NoopMigrationAuditSink;
        let run_id = sink.start_run("v1.0", "hash123").await.unwrap();
        assert_eq!(run_id, "00000000-0000-0000-0000-000000000000");
    }

    #[tokio::test]
    async fn noop_audit_sink_record_op_is_ok() {
        let sink = NoopMigrationAuditSink;
        let result = ArtifactApplyResult {
            rel_path: "postgres/001.sql".to_string(),
            backend: "postgres".to_string(),
            applied: true,
            skipped: false,
            error: None,
        };
        assert!(sink.record_op("run-id", 0, &result).await.is_ok());
    }

    /// C (2026-05-30): phased apply runs through every
    /// MigrationPhase and records one row per phase in the ledger.
    /// Uses a target that tracks applied artifacts so `verify_applied`
    /// flips true after `apply_artifact` succeeds — required for
    /// the Validate phase to pass.
    #[tokio::test]
    async fn c_apply_artifacts_phased_records_all_five_phases() {
        use std::collections::HashSet;
        use std::sync::Mutex;

        use crate::migration::phase_runner::{
            MemoryPhaseLedger, PhaseLedger, PhaseStatus, RunnerOutcome,
        };

        struct TrackingTarget {
            applied: Mutex<HashSet<String>>,
        }
        impl ApplyTarget for TrackingTarget {
            fn backend_name(&self) -> &'static str {
                "tracking"
            }
            fn apply_artifact<'a>(
                &'a self,
                artifact: &'a GeneratedArtifact,
            ) -> ApplyFuture<'a, ()> {
                Box::pin(async move {
                    self.applied
                        .lock()
                        .unwrap()
                        .insert(artifact.rel_path.clone());
                    Ok(())
                })
            }
            fn verify_applied<'a>(
                &'a self,
                artifact: &'a GeneratedArtifact,
            ) -> ApplyFuture<'a, bool> {
                Box::pin(
                    async move { Ok(self.applied.lock().unwrap().contains(&artifact.rel_path)) },
                )
            }
        }

        let target = TrackingTarget {
            applied: Mutex::new(HashSet::new()),
        };
        let sink = NoopMigrationAuditSink;
        let ledger = MemoryPhaseLedger::default();
        let artifacts = vec![
            dummy_artifact("postgres/001_users.sql"),
            dummy_artifact("postgres/002_docs.sql"),
        ];

        let (outcome, results) = apply_artifacts_phased(
            "run-c-1", &target, &artifacts, &sink, &ledger, "v1.0", "hash-c-1",
        )
        .await
        .expect("phased apply failed");

        assert!(matches!(outcome, RunnerOutcome::Completed { .. }));
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.applied && r.error.is_none()));

        // Five phases, all Completed.
        let recs = ledger.load("run-c-1").await.unwrap();
        assert_eq!(recs.len(), 5, "expected 5 phase rows, got {recs:?}");
        for r in &recs {
            assert_eq!(
                r.status,
                PhaseStatus::Completed,
                "phase {} not completed: {r:?}",
                r.phase.as_str()
            );
        }
    }

    /// C: when verify_applied stays false post-apply, the Validate
    /// phase fails — pinning that the phase runner refuses to mark
    /// the run complete on broken idempotency.
    #[tokio::test]
    async fn c_phased_apply_pauses_when_validate_fails() {
        use crate::migration::diff_backends::MigrationPhase;
        use crate::migration::phase_runner::{MemoryPhaseLedger, RunnerOutcome};

        let target = LoggingApplyTarget::new("never_verifies");
        let sink = NoopMigrationAuditSink;
        let ledger = MemoryPhaseLedger::default();
        let artifacts = vec![dummy_artifact("test/001.sql")];
        let (outcome, _) = apply_artifacts_phased(
            "run-c-2", &target, &artifacts, &sink, &ledger, "v1.0", "hash-c-2",
        )
        .await
        .expect("runner errored");
        match outcome {
            RunnerOutcome::Paused { phase, .. } => {
                assert_eq!(phase, MigrationPhase::Validate);
            }
            other => panic!("expected Paused at Validate, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn c_phased_apply_all_already_applied_completes_as_skipped() {
        use crate::migration::phase_runner::{MemoryPhaseLedger, RunnerOutcome};

        struct AlreadyAppliedTarget;
        impl ApplyTarget for AlreadyAppliedTarget {
            fn backend_name(&self) -> &'static str {
                "already_applied"
            }

            fn apply_artifact<'a>(
                &'a self,
                _artifact: &'a GeneratedArtifact,
            ) -> ApplyFuture<'a, ()> {
                Box::pin(async { Ok(()) })
            }

            fn verify_applied<'a>(
                &'a self,
                _artifact: &'a GeneratedArtifact,
            ) -> ApplyFuture<'a, bool> {
                Box::pin(async { Ok(true) })
            }
        }

        let target = AlreadyAppliedTarget;
        let sink = NoopMigrationAuditSink;
        let ledger = MemoryPhaseLedger::default();
        let artifacts = vec![dummy_artifact("test/already.sql")];
        let (outcome, results) = apply_artifacts_phased(
            "run-c-already",
            &target,
            &artifacts,
            &sink,
            &ledger,
            "v1.0",
            "hash-already",
        )
        .await
        .expect("phased apply failed");

        assert!(matches!(outcome, RunnerOutcome::Completed { .. }));
        assert_eq!(results.len(), 1);
        assert!(results[0].skipped);
        assert!(!results[0].applied);
        assert!(results[0].error.is_none());
    }

    #[tokio::test]
    async fn apply_artifacts_audited_completes_without_error() {
        let target = LoggingApplyTarget::new("test");
        let sink = NoopMigrationAuditSink;
        let artifacts = vec![
            dummy_artifact("postgres/001_users.sql"),
            dummy_artifact("postgres/002_docs.sql"),
        ];
        let results = apply_artifacts_audited(&target, &artifacts, &sink, "v1.0", "hash123").await;
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.applied && r.error.is_none()));
    }
}
