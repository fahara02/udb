//! U14 — online migration phase orchestrator.
//!
//! `MigrationPhase` already enumerates `Prepare → Backfill → Validate
//! → Switch → Cleanup` (see `migration/diff_backends.rs`). What was
//! missing — and what the upgrade doc flagged as "follow-up" — was the
//! orchestrator that actually drives a migration through them:
//!
//! - records `(run_id, phase, status, started_at, finished_at, error)`
//!   in a durable ledger;
//! - calls per-phase user hooks (the actual `prepare_*` / `backfill_*`
//!   work for the backend);
//! - **resumes** from the last incomplete phase on restart;
//! - **gates** each phase on a capability check (refuses to advance
//!   into `Switch` if the validation phase didn't pass).
//!
//! This module is pure orchestration logic — no SQL embedded — so it's
//! testable end-to-end against an in-memory ledger and a fake plan.
//! The real Postgres-backed ledger plugs in via [`PhaseLedger`].
//!
//! ## Why a separate runner vs. extending `MigrationAuditSink`?
//!
//! `MigrationAuditSink` records one row per **artifact** (a single
//! backend-resource DDL). A phase records one row per **logical phase
//! of the whole migration** and is the unit operators monitor in the
//! dashboard. Conflating them would force every artifact to repeat
//! the phase context. Keeping them separate matches how the
//! production ledger is queried.
//!
//! ## State machine
//!
//! ```text
//!   ┌─────────┐  ok    ┌─────────┐ ok   ┌──────────┐ ok  ┌────────┐ ok  ┌─────────┐
//!   │ Prepare │──────▶ │ Backfill│────▶ │ Validate │───▶ │ Switch │───▶ │ Cleanup │──▶ Completed
//!   └────┬────┘        └────┬────┘      └────┬─────┘     └───┬────┘     └────┬────┘
//!        │ err              │ err            │ err           │ err           │ err
//!        ▼                  ▼                ▼               ▼               ▼
//!     PhaseFailed       PhaseFailed       PhaseFailed     PhaseFailed     PhaseFailed
//! ```
//!
//! `PhaseFailed` is terminal for the run. The operator either rolls
//! back manually, fixes the underlying issue and retries the failed
//! phase, or marks the run abandoned. The runner does NOT auto-retry
//! — phase failures usually need human judgement, not a bare loop.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::migration::diff_backends::MigrationPhase;

/// Per-phase status as recorded in the ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseStatus {
    /// Phase row exists but the runner hasn't started this phase yet.
    /// Useful for surfacing "next phase is X" in the dashboard.
    Pending,
    /// Runner is executing the user hook for this phase.
    Running,
    /// User hook returned `Ok(())`. Runner can advance.
    Completed,
    /// User hook returned `Err(_)`. Run is paused; operator must
    /// resolve and either resume or abandon.
    Failed,
    /// Operator marked the run abandoned after a failure. The phase
    /// stays in this state — resumes will not pick it up.
    Abandoned,
}

impl PhaseStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }

    pub fn terminal(self) -> bool {
        matches!(self, Self::Failed | Self::Abandoned | Self::Completed)
    }
}

/// One row in the phase ledger.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PhaseRecord {
    pub run_id: String,
    pub phase: MigrationPhase,
    pub status: PhaseStatus,
    /// Unix ms when the phase first transitioned to `Running`. `None`
    /// while still `Pending`.
    pub started_at_unix_ms: Option<i64>,
    /// Unix ms when the phase reached a terminal state. `None` while
    /// `Pending` or `Running`.
    pub finished_at_unix_ms: Option<i64>,
    /// Error message from the failing user hook. Empty when not failed.
    pub error: String,
    /// Free-form attempt counter — the operator can resume a failed
    /// phase and the runner bumps this before re-running. Lets the
    /// dashboard show "retry #N".
    pub attempt: u32,
}

/// Persistent ledger for phase records. The production impl writes to
/// `udb_migration_phase_ledger` (Postgres); tests use an in-memory
/// `MemoryPhaseLedger`. The trait is async-object-safe so the runner
/// can hold a `Box<dyn PhaseLedger>` if needed.
#[async_trait]
pub trait PhaseLedger: Send + Sync {
    async fn load(&self, run_id: &str) -> Result<Vec<PhaseRecord>, String>;
    async fn write(&self, record: PhaseRecord) -> Result<(), String>;
}

/// The per-phase user hook the runner calls. Returns `Ok(())` to
/// advance, `Err(msg)` to pause the run on this phase.
///
/// Hooks must be **idempotent** — a resume after restart will replay
/// the last incomplete phase.
#[async_trait]
pub trait MigrationPhaseHook: Send + Sync {
    /// Called with the current phase. Implementations dispatch by
    /// `phase` to whatever backend work is needed.
    async fn run(&self, phase: MigrationPhase) -> Result<(), String>;

    /// Capability check: is this phase allowed to run on the current
    /// deployment? Default `true` for every phase. Override to refuse
    /// e.g. `Switch` when the validation step has not been signed off.
    fn capable_of(&self, _phase: MigrationPhase) -> bool {
        true
    }
}

/// What the runner returns after a `run_to_completion` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunnerOutcome {
    /// Every phase reached `Completed`.
    Completed { run_id: String },
    /// A phase returned `Err(_)` and the runner stopped. The dashboard
    /// shows which phase failed and the error text from the hook.
    Paused {
        run_id: String,
        phase: MigrationPhase,
        error: String,
    },
    /// The runner refused to start a phase because the hook's
    /// `capable_of` returned false. Pins that the capability check
    /// runs **before** the phase executes — no partial side effects.
    Refused {
        run_id: String,
        phase: MigrationPhase,
    },
}

/// Drive `run_id` through every phase, starting at the first
/// non-completed phase (so a restart resumes where it left off).
///
/// Algorithm:
///
/// 1. Load every existing `PhaseRecord` for `run_id` from the ledger.
/// 2. For each phase in `MigrationPhase::all()`:
///    a. Look up the existing record (or treat as `Pending`).
///    b. If `Completed`, skip.
///    c. If `Abandoned`, return `Refused` (operator chose to stop).
///    d. Check `hook.capable_of(phase)`. If `false`, write a `Failed`
///       record and return `Refused`.
///    e. Write `Running` to the ledger, call `hook.run(phase)`.
///    f. On `Ok`, write `Completed`; on `Err`, write `Failed` and
///       return `Paused`.
/// 3. Return `Completed`.
pub async fn run_to_completion(
    run_id: &str,
    ledger: &dyn PhaseLedger,
    hook: &dyn MigrationPhaseHook,
) -> Result<RunnerOutcome, String> {
    let existing = ledger.load(run_id).await?;
    let by_phase: HashMap<MigrationPhase, PhaseRecord> =
        existing.into_iter().map(|r| (r.phase, r)).collect();

    for phase in MigrationPhase::all().iter().copied() {
        let prev = by_phase.get(&phase);
        match prev.map(|r| r.status) {
            Some(PhaseStatus::Completed) => continue,
            Some(PhaseStatus::Abandoned) => {
                return Ok(RunnerOutcome::Refused {
                    run_id: run_id.to_string(),
                    phase,
                });
            }
            _ => {}
        }

        if !hook.capable_of(phase) {
            let record = PhaseRecord {
                run_id: run_id.to_string(),
                phase,
                status: PhaseStatus::Failed,
                started_at_unix_ms: None,
                finished_at_unix_ms: Some(now_unix_ms()),
                error: format!("capability check refused phase {}", phase.as_str()),
                attempt: prev.map(|p| p.attempt).unwrap_or(0).saturating_add(1),
            };
            ledger.write(record).await?;
            return Ok(RunnerOutcome::Refused {
                run_id: run_id.to_string(),
                phase,
            });
        }

        let attempt = prev.map(|p| p.attempt).unwrap_or(0).saturating_add(1);
        let started = now_unix_ms();
        ledger
            .write(PhaseRecord {
                run_id: run_id.to_string(),
                phase,
                status: PhaseStatus::Running,
                started_at_unix_ms: Some(started),
                finished_at_unix_ms: None,
                error: String::new(),
                attempt,
            })
            .await?;
        match hook.run(phase).await {
            Ok(()) => {
                ledger
                    .write(PhaseRecord {
                        run_id: run_id.to_string(),
                        phase,
                        status: PhaseStatus::Completed,
                        started_at_unix_ms: Some(started),
                        finished_at_unix_ms: Some(now_unix_ms()),
                        error: String::new(),
                        attempt,
                    })
                    .await?;
            }
            Err(reason) => {
                ledger
                    .write(PhaseRecord {
                        run_id: run_id.to_string(),
                        phase,
                        status: PhaseStatus::Failed,
                        started_at_unix_ms: Some(started),
                        finished_at_unix_ms: Some(now_unix_ms()),
                        error: reason.clone(),
                        attempt,
                    })
                    .await?;
                return Ok(RunnerOutcome::Paused {
                    run_id: run_id.to_string(),
                    phase,
                    error: reason,
                });
            }
        }
    }

    Ok(RunnerOutcome::Completed {
        run_id: run_id.to_string(),
    })
}

/// C (2026-05-30): in-memory `PhaseLedger` for orchestration UNIT TESTS only.
/// Stores rows behind a `Mutex` so the trait methods stay `&self`.
///
/// IN-MEMORY-AUDIT [A1, no-in-memory rule]: now `#[cfg(test)]`-gated so it is
/// NOT compiled into the shipped binary (it was previously `pub` and shippable).
/// REMAINING FEATURE GAP (O4 — a missing feature, not an in-memory bug): no
/// durable Postgres-backed `PhaseLedger` exists yet and the production apply path
/// uses the non-phased `apply_artifacts_audited`, so online/phased migrations have
/// no crash-resumable state in production. Tracked as Wave A3 in
/// PARSER_AST_MIGRATION_MASTER_PLAN.md (add a canonical-store
/// `udb_migration_phase_ledger` + route `apply_migration` through
/// `apply_artifacts_phased`).
#[cfg(test)]
#[derive(Default, Debug)]
pub struct MemoryPhaseLedger {
    rows: std::sync::Mutex<Vec<PhaseRecord>>,
}

#[cfg(test)]
#[async_trait]
impl PhaseLedger for MemoryPhaseLedger {
    async fn load(&self, run_id: &str) -> Result<Vec<PhaseRecord>, String> {
        Ok(self
            .rows
            .lock()
            .map_err(|e| format!("MemoryPhaseLedger.rows poisoned: {e}"))?
            .iter()
            .filter(|r| r.run_id == run_id)
            .cloned()
            .collect())
    }

    async fn write(&self, record: PhaseRecord) -> Result<(), String> {
        let mut rows = self
            .rows
            .lock()
            .map_err(|e| format!("MemoryPhaseLedger.rows poisoned: {e}"))?;
        // Upsert by (run_id, phase) — phases are unique per run.
        rows.retain(|r| !(r.run_id == record.run_id && r.phase == record.phase));
        rows.push(record);
        Ok(())
    }
}

fn now_unix_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // ── Test doubles ────────────────────────────────────────────────
    // The public `MemoryPhaseLedger` lives in the parent module so
    // other crates can use it; tests just alias it locally.
    type MemoryLedger = MemoryPhaseLedger;

    /// Hook that runs every phase successfully. Records the order it
    /// was called so tests can assert phase ordering.
    struct OkHook {
        seen: Mutex<Vec<MigrationPhase>>,
    }
    impl OkHook {
        fn new() -> Self {
            Self {
                seen: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl MigrationPhaseHook for OkHook {
        async fn run(&self, phase: MigrationPhase) -> Result<(), String> {
            self.seen.lock().unwrap().push(phase);
            Ok(())
        }
    }

    /// Hook that fails the specified phase. Pins the pause/resume
    /// contract.
    struct FailAt {
        target: MigrationPhase,
    }

    #[async_trait]
    impl MigrationPhaseHook for FailAt {
        async fn run(&self, phase: MigrationPhase) -> Result<(), String> {
            if phase == self.target {
                Err(format!("synthetic failure at {}", phase.as_str()))
            } else {
                Ok(())
            }
        }
    }

    /// Hook that refuses the Switch phase via capability check —
    /// e.g. validate hasn't been signed off.
    struct RefuseSwitch;

    #[async_trait]
    impl MigrationPhaseHook for RefuseSwitch {
        async fn run(&self, _phase: MigrationPhase) -> Result<(), String> {
            Ok(())
        }
        fn capable_of(&self, phase: MigrationPhase) -> bool {
            phase != MigrationPhase::Switch
        }
    }

    // ── Tests ───────────────────────────────────────────────────────

    /// Happy path: every phase advances in order, ledger ends with
    /// 5 `Completed` rows.
    #[tokio::test]
    async fn run_to_completion_advances_through_every_phase() {
        let ledger = MemoryLedger::default();
        let hook = OkHook::new();
        let outcome = run_to_completion("run-1", &ledger, &hook).await.unwrap();
        assert_eq!(
            outcome,
            RunnerOutcome::Completed {
                run_id: "run-1".to_string()
            }
        );
        // Hook saw every phase in order.
        let seen = hook.seen.lock().unwrap().clone();
        assert_eq!(seen, MigrationPhase::all().to_vec());
        // Ledger has 5 rows, all completed.
        let rows = ledger.load("run-1").await.unwrap();
        assert_eq!(rows.len(), 5);
        for row in &rows {
            assert_eq!(row.status, PhaseStatus::Completed, "phase {:?}", row.phase);
            assert!(row.finished_at_unix_ms.is_some());
        }
    }

    /// Failure pauses the run on the failing phase. Resuming after
    /// the hook is fixed picks up exactly where it left off — no
    /// re-running completed phases.
    #[tokio::test]
    async fn failure_pauses_then_resume_continues_without_replay() {
        let ledger = MemoryLedger::default();
        let outcome = run_to_completion(
            "run-r",
            &ledger,
            &FailAt {
                target: MigrationPhase::Backfill,
            },
        )
        .await
        .unwrap();
        match outcome {
            RunnerOutcome::Paused { phase, error, .. } => {
                assert_eq!(phase, MigrationPhase::Backfill);
                assert!(error.contains("synthetic failure at backfill"));
            }
            other => panic!("expected Paused, got {:?}", other),
        }
        // Prepare must be Completed; Backfill must be Failed; others
        // must not exist yet.
        let rows = ledger.load("run-r").await.unwrap();
        let by: HashMap<MigrationPhase, PhaseRecord> =
            rows.into_iter().map(|r| (r.phase, r)).collect();
        assert_eq!(
            by.get(&MigrationPhase::Prepare).map(|r| r.status),
            Some(PhaseStatus::Completed)
        );
        assert_eq!(
            by.get(&MigrationPhase::Backfill).map(|r| r.status),
            Some(PhaseStatus::Failed)
        );
        assert!(by.get(&MigrationPhase::Validate).is_none());

        // Resume with a hook that succeeds everywhere. The runner
        // must NOT call `prepare` again because it's already Completed.
        let resume_hook = OkHook::new();
        let outcome = run_to_completion("run-r", &ledger, &resume_hook)
            .await
            .unwrap();
        assert!(matches!(outcome, RunnerOutcome::Completed { .. }));
        let seen = resume_hook.seen.lock().unwrap().clone();
        // Prepare was skipped (already completed); the rest ran.
        assert_eq!(
            seen,
            vec![
                MigrationPhase::Backfill,
                MigrationPhase::Validate,
                MigrationPhase::Switch,
                MigrationPhase::Cleanup,
            ]
        );
        // Backfill attempt counter bumped to 2 (1 failed + 1 retry).
        let rows = ledger.load("run-r").await.unwrap();
        let backfill = rows
            .iter()
            .find(|r| r.phase == MigrationPhase::Backfill)
            .unwrap();
        assert_eq!(backfill.status, PhaseStatus::Completed);
        assert_eq!(backfill.attempt, 2);
    }

    /// Capability refusal: `capable_of(Switch) == false` blocks the
    /// switch phase **before** any side effect. Pins that the hook
    /// `run` is not called for a refused phase.
    #[tokio::test]
    async fn capability_refusal_blocks_before_side_effects() {
        let ledger = MemoryLedger::default();
        let outcome = run_to_completion("run-c", &ledger, &RefuseSwitch)
            .await
            .unwrap();
        match outcome {
            RunnerOutcome::Refused { phase, .. } => {
                assert_eq!(phase, MigrationPhase::Switch);
            }
            other => panic!("expected Refused at Switch, got {:?}", other),
        }
        // Prepare/Backfill/Validate completed; Switch failed; Cleanup
        // never written.
        let rows = ledger.load("run-c").await.unwrap();
        let by: HashMap<MigrationPhase, PhaseRecord> =
            rows.into_iter().map(|r| (r.phase, r)).collect();
        assert_eq!(
            by.get(&MigrationPhase::Validate).map(|r| r.status),
            Some(PhaseStatus::Completed)
        );
        let switch = by.get(&MigrationPhase::Switch).unwrap();
        assert_eq!(switch.status, PhaseStatus::Failed);
        assert!(
            switch.error.contains("capability check refused"),
            "got: {}",
            switch.error
        );
        assert_eq!(
            switch.started_at_unix_ms, None,
            "no started timestamp = no side effect"
        );
        assert!(by.get(&MigrationPhase::Cleanup).is_none());
    }

    /// Abandoned phase short-circuits the runner without calling
    /// the hook — operator chose to stop. Pins the workflow.
    #[tokio::test]
    async fn abandoned_phase_refuses_to_advance() {
        let ledger = MemoryLedger::default();
        // Manually plant an Abandoned row for Switch.
        ledger
            .write(PhaseRecord {
                run_id: "run-a".to_string(),
                phase: MigrationPhase::Switch,
                status: PhaseStatus::Abandoned,
                started_at_unix_ms: None,
                finished_at_unix_ms: Some(now_unix_ms()),
                error: "operator chose to stop".to_string(),
                attempt: 1,
            })
            .await
            .unwrap();
        let outcome = run_to_completion("run-a", &ledger, &OkHook::new())
            .await
            .unwrap();
        match outcome {
            RunnerOutcome::Refused { phase, .. } => {
                assert_eq!(phase, MigrationPhase::Switch);
            }
            other => panic!("expected Refused, got {:?}", other),
        }
    }

    /// Pin: PhaseStatus tokens are the wire contract. Changing one of
    /// these breaks the dashboard.
    #[test]
    fn phase_status_tokens_are_pinned() {
        assert_eq!(PhaseStatus::Pending.as_str(), "pending");
        assert_eq!(PhaseStatus::Running.as_str(), "running");
        assert_eq!(PhaseStatus::Completed.as_str(), "completed");
        assert_eq!(PhaseStatus::Failed.as_str(), "failed");
        assert_eq!(PhaseStatus::Abandoned.as_str(), "abandoned");
    }
}
