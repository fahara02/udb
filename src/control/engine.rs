// src/engine.rs — Migration FSM state machine types.
//
// The Engine struct tracks the FSM state, run lifecycle, and error history for a
// single migration run.  This is a **pure data model** — no backend connections required.
// The Go UDB service drives the state transitions; the Rust library validates them
// and produces the run snapshot that the service persists to `migration_runtime_state`.
//
// APPLYING phase scope (spec §16.3.1, step 5):
//   • Tier 1 (PostgreSQL)  — apply SQL DDL migration files via `sqlx`
//   • Tier 3 (Qdrant)     — create / update vector collections and payload indexes
//   • Tier 4 (MinIO)      — create buckets, set lifecycle policies
//   Tier 2 (Redis) is managed by config/TTL only; no structural migration needed.
//
// Aligned with:
//   - legacy_sql  legacycore/db/migrate.go      (FSM states + validTransitions) — SQL-only reference
//   - UDB spec §16.1                        (four storage tiers)
//   - UDB spec §16.3.1                      (FSM Lifecycle)
//   - UDB spec §16.3.2                      (Double-Entry Ledger)

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Maximum number of automatic recovery retries before the engine halts.
/// Mirrors `maxRetries` in legacy_sql `migrate.go`.
pub const MAX_RETRIES: u32 = 3;

/// PostgreSQL advisory lock key used to prevent concurrent migration runs.
/// Stable across restarts; chosen so as not to conflict with application locks.
pub const PG_ADVISORY_LOCK_KEY: i64 = 0x7072_6973_6d76_6973; // "primvis" in hex

// ── FSM States ────────────────────────────────────────────────────────────────

/// The current FSM state of a migration run.
///
/// Each state corresponds to one phase of the double-entry migration lifecycle
/// described in the UDB spec §16.3 and legacy_sql's migrate.go.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FsmState {
    /// Quiescent — no migration in progress.
    #[default]
    Idle,
    /// Bootstrap: creating migration ledger tables, acquiring advisory lock.
    Initialising,
    /// Loading `.proto` files and building the desired AST.
    LoadProtoState,
    /// Comparing the parsed AST checksum against the `proto_schema_versions` ledger.
    ProtoChecksumLint,
    /// Diffing old manifest vs new to discover which schema changes are needed.
    PlanProtoDiff,
    /// Generating SQL delta files from the diff result.
    GenerateSql,
    /// Verifying checksums of all SQL files in the migration directory.
    ChecksumLint,
    /// Executing migration artefacts across all active backends:
    /// SQL DDL → PostgreSQL, Collections/Indexes → Qdrant, Buckets → MinIO.
    Applying,
    /// Running the proto linter against the live PostgreSQL `information_schema`
    /// and verifying Qdrant collections + MinIO bucket existence.
    Linting,
    /// Applying safe structural repairs: SQL DDL for Postgres, collection
    /// recreation for Qdrant, bucket creation for MinIO.
    AutoAltering,
    /// Cross-checking the live topology across ALL active backends against the
    /// desired proto AST (SQL tables, vector collections, object buckets).
    Verifying,
    /// Stepping back from `Error` state after a manual recovery action.
    Recovering,
    /// Migration run completed successfully — ledger entries recorded.
    Completed,
    /// The FSM encountered an unrecoverable error and has halted.
    Error,
}

impl std::fmt::Display for FsmState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FsmState {
    pub const ALL: [FsmState; 14] = [
        FsmState::Idle,
        FsmState::Initialising,
        FsmState::LoadProtoState,
        FsmState::ProtoChecksumLint,
        FsmState::PlanProtoDiff,
        FsmState::GenerateSql,
        FsmState::ChecksumLint,
        FsmState::Applying,
        FsmState::Linting,
        FsmState::AutoAltering,
        FsmState::Verifying,
        FsmState::Recovering,
        FsmState::Completed,
        FsmState::Error,
    ];

    pub const VARIANT_COUNT: usize = Self::ALL.len();

    /// Returns the canonical uppercase string stored in DB records / JSON.
    /// Mirrors the `MigrationState` constants in legacy_sql `migrate.go`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "IDLE",
            Self::Initialising => "INITIALISING",
            Self::LoadProtoState => "LOAD_PROTO_STATE",
            Self::ProtoChecksumLint => "PROTO_CHECKSUM_LINT",
            Self::PlanProtoDiff => "PLAN_PROTO_DIFF",
            Self::GenerateSql => "GENERATE_SQL",
            Self::ChecksumLint => "CHECKSUM_LINT",
            Self::Applying => "APPLYING",
            Self::Linting => "LINTING",
            Self::AutoAltering => "AUTO_ALTERING",
            Self::Verifying => "VERIFYING",
            Self::Recovering => "RECOVERING",
            Self::Completed => "COMPLETED",
            Self::Error => "ERROR",
        }
    }

    /// Parse a state from its canonical uppercase string.
    /// Returns `None` for unrecognised values.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "IDLE" => Self::Idle,
            "INITIALISING" => Self::Initialising,
            "LOAD_PROTO_STATE" => Self::LoadProtoState,
            "PROTO_CHECKSUM_LINT" => Self::ProtoChecksumLint,
            "PLAN_PROTO_DIFF" => Self::PlanProtoDiff,
            "GENERATE_SQL" => Self::GenerateSql,
            "CHECKSUM_LINT" => Self::ChecksumLint,
            "APPLYING" => Self::Applying,
            "LINTING" => Self::Linting,
            "AUTO_ALTERING" => Self::AutoAltering,
            "VERIFYING" => Self::Verifying,
            "RECOVERING" => Self::Recovering,
            "COMPLETED" => Self::Completed,
            "ERROR" => Self::Error,
            _ => return None,
        })
    }

    /// Returns all legal successor states from this state.
    ///
    /// Mirrors `validTransitions` in legacy_sql `migrate.go`.
    /// The Go service calls this to guard every `e.transition(next)` call.
    pub fn valid_transitions(&self) -> Vec<FsmState> {
        use FsmState::*;
        match self {
            Idle => vec![Initialising, Recovering],
            Initialising => vec![LoadProtoState, ChecksumLint, Error],
            LoadProtoState => vec![ProtoChecksumLint, Error],
            ProtoChecksumLint => vec![PlanProtoDiff, ChecksumLint, Error],
            PlanProtoDiff => vec![GenerateSql, ChecksumLint, Error],
            GenerateSql => vec![ChecksumLint, Error],
            ChecksumLint => vec![Applying, Error],
            Applying => vec![Verifying, Linting, Error],
            Verifying => vec![Completed, AutoAltering, Error],
            Linting => vec![AutoAltering, Completed, Error],
            AutoAltering => vec![Completed, Error],
            Recovering => vec![Idle, Error],
            Completed => vec![Idle],
            Error => vec![Recovering, Idle],
        }
    }

    /// Returns `true` when transitioning to `next` is legal from this state.
    pub fn can_transition_to(&self, next: &FsmState) -> bool {
        self.valid_transitions().contains(next)
    }

    /// Returns `true` for terminal states where the FSM run has ended.
    pub fn is_terminal(&self) -> bool {
        matches!(self, FsmState::Completed | FsmState::Error)
    }
}

// ── Run snapshot ──────────────────────────────────────────────────────────────

/// A point-in-time snapshot of a migration run, persisted to
/// `public.migration_runtime_state` by the Go UDB service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RuntimeStateSnapshot {
    pub run_id: String,
    pub state: FsmState,
    /// The migration artefact currently being applied (SQL filename, collection
    /// name, or bucket name depending on which backend tier is active).
    pub active_file: String,
    pub retry_count: u32,
    pub updated_at_unix: u64,
    /// Backend tiers active in the current run, e.g. `["postgres", "qdrant", "minio"]`.
    pub active_backends: Vec<String>,
}

// ── Error record ──────────────────────────────────────────────────────────────

/// A single migration error record, persisted to `public.migration_error_log`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EngineError {
    /// Matches the `run_id` of the `Engine` that produced the error.
    pub run_id: String,
    /// The FSM state the engine was in when the error occurred.
    pub fsm_state: String,
    /// The artefact being applied when the error occurred (SQL file, collection
    /// name, or bucket name; empty for non-apply errors).
    pub filename: String,
    /// Human-readable error message.
    pub message: String,
    /// Unix timestamp of the error.
    pub failed_at_unix: u64,
    /// `true` after `RecoverFromError()` has been called.
    pub resolved: bool,
}

// ── Engine ────────────────────────────────────────────────────────────────────

/// In-memory FSM driver used by the Go UDB service.
///
/// The Go service creates an `Engine` at startup, calls `transition()` to move
/// through the states, and serializes `RuntimeStateSnapshot` to the migration
/// ledger (PostgreSQL `public.migration_runtime_state`) after each transition.
/// Blocked/error transitions are refused with a descriptive error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Engine {
    /// Unique identifier for this run, formatted as `20060102T150405.000000000Z`.
    pub run_id: String,
    /// Current FSM state.
    pub state: FsmState,
    /// The migration file currently being applied (empty when not in `Applying`).
    pub active_file: String,
    /// Number of auto-recovery attempts in this run.
    pub retry_count: u32,
    /// Last recorded error, if any.
    pub error: Option<EngineError>,
    /// `true` once the migration ledger tables have been bootstrapped.
    pub tracker_ready: bool,
    /// Backend tiers active in this run, e.g. `["postgres", "qdrant", "minio"]`.
    /// Set by the Go service during INITIALISING based on `UdbConfig.active_tiers()`.
    pub active_backends: Vec<String>,
}

impl Engine {
    /// Create a new `Engine` in `IDLE` state with the given run ID.
    pub fn new(run_id: impl Into<String>) -> Self {
        Self {
            run_id: run_id.into(),
            state: FsmState::Idle,
            active_file: String::new(),
            retry_count: 0,
            error: None,
            tracker_ready: false,
            active_backends: vec!["postgres".to_string()], // Tier 1 always present
        }
    }

    /// Create a new `Engine` with an auto-generated run ID based on current UTC time.
    pub fn new_auto_id() -> Self {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        Self::new(format!("{ts:020}"))
    }

    /// Attempt to transition to `next`.
    ///
    /// Returns `Ok(())` on success and updates `self.state`.
    /// Returns `Err(String)` with a human-readable message for illegal transitions.
    pub fn transition(&mut self, next: FsmState) -> Result<(), String> {
        if self.state.can_transition_to(&next) {
            self.state = next;
            Ok(())
        } else {
            Err(format!(
                "[fsm] illegal transition {} → {} (run={})",
                self.state, next, self.run_id
            ))
        }
    }

    /// Record an error and unconditionally move to `Error` state.
    ///
    /// The previous state is captured in the `EngineError.fsm_state` field so
    /// the Go service can log which phase failed.
    pub fn fail(&mut self, filename: impl Into<String>, message: impl Into<String>) {
        self.error = Some(EngineError {
            run_id: self.run_id.clone(),
            fsm_state: self.state.as_str().to_string(),
            filename: filename.into(),
            message: message.into(),
            failed_at_unix: unix_now(),
            resolved: false,
        });
        self.state = FsmState::Error;
    }

    /// Attempt an auto-recovery: increment retry counter and transition to `Recovering`.
    ///
    /// Returns `Err` when `MAX_RETRIES` has been exceeded.
    pub fn recover(&mut self) -> Result<(), String> {
        if self.retry_count >= MAX_RETRIES {
            return Err(format!(
                "[fsm] max retries ({MAX_RETRIES}) exceeded — manual intervention required (run={})",
                self.run_id
            ));
        }
        self.retry_count += 1;
        self.transition(FsmState::Recovering)
    }

    /// Reset the engine back to `Idle` after a successful recovery.
    pub fn reset_to_idle(&mut self) {
        if let Some(err) = &mut self.error {
            err.resolved = true;
        }
        self.state = FsmState::Idle;
        self.active_file = String::new();
    }

    /// Produce a `RuntimeStateSnapshot` for persistence.
    pub fn snapshot(&self) -> RuntimeStateSnapshot {
        RuntimeStateSnapshot {
            run_id: self.run_id.clone(),
            state: self.state.clone(),
            active_file: self.active_file.clone(),
            retry_count: self.retry_count,
            updated_at_unix: unix_now(),
            active_backends: self.active_backends.clone(),
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_can_transition_to_initialising() {
        let mut engine = Engine::new("test-run-1");
        engine.transition(FsmState::Initialising).unwrap();
        assert_eq!(engine.state, FsmState::Initialising);
    }

    #[test]
    fn illegal_transition_returns_err() {
        let mut engine = Engine::new("test-run-2");
        let result = engine.transition(FsmState::Completed);
        assert!(result.is_err(), "IDLE → COMPLETED must be illegal");
    }

    #[test]
    fn fail_sets_error_state() {
        let mut engine = Engine::new("test-run-3");
        engine.transition(FsmState::Initialising).unwrap();
        engine.fail("001_init.sql", "syntax error at line 42");
        assert_eq!(engine.state, FsmState::Error);
        assert!(engine.error.is_some());
        let err = engine.error.as_ref().unwrap();
        assert_eq!(err.fsm_state, "INITIALISING");
        assert!(!err.resolved);
    }

    #[test]
    fn recover_increments_retry_count() {
        let mut engine = Engine::new("test-run-4");
        engine.state = FsmState::Error; // simulate error
        engine.recover().unwrap();
        assert_eq!(engine.state, FsmState::Recovering);
        assert_eq!(engine.retry_count, 1);
    }

    #[test]
    fn max_retries_exceeded_returns_err() {
        let mut engine = Engine::new("test-run-5");
        engine.retry_count = MAX_RETRIES;
        engine.state = FsmState::Error;
        let result = engine.recover();
        assert!(result.is_err());
    }

    #[test]
    fn from_str_round_trip() {
        assert_eq!(FsmState::VARIANT_COUNT, 14);
        for state in FsmState::ALL {
            let s = state.as_str();
            assert_eq!(
                FsmState::from_str(s).unwrap(),
                state,
                "round-trip failed for {s}"
            );
        }
    }

    #[test]
    fn full_happy_path_transitions() {
        let mut e = Engine::new("happy-run");
        // Walk the standard migration path end-to-end
        for next in [
            FsmState::Initialising,
            FsmState::LoadProtoState,
            FsmState::ProtoChecksumLint,
            FsmState::PlanProtoDiff,
            FsmState::GenerateSql,
            FsmState::ChecksumLint,
            FsmState::Applying,
            FsmState::Verifying,
            FsmState::Completed,
            FsmState::Idle,
        ] {
            e.transition(next).expect("unexpected illegal transition");
        }
    }
}
