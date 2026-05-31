// src/status.rs — Migration status snapshot types.
//
// `MigrationStatus` is a comprehensive point-in-time snapshot of the migration
// system state.  The Go UDB service builds this by querying the tracker tables
// and exposes it via both a gRPC health endpoint and a CLI `status` command.
//
// Aligned with:
//   - legacy_sql  legacycore/db/status.go  (MigrationStatus, ErrorSummary, GetMigrationStatus)
//   - UDB spec §16.3.1                 (FSM Lifecycle)

use serde::{Deserialize, Serialize};

use crate::engine::FsmState;

// ── Status snapshot ───────────────────────────────────────────────────────────

/// Comprehensive point-in-time snapshot of the migration system state.
///
/// Built by the Go UDB service querying the `schema_migrations`,
/// `proto_schema_versions`, and `migration_runtime_state` tracker tables.
/// Mirrors `MigrationStatus` in legacy_sql `status.go`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MigrationStatus {
    // ── FSM ──────────────────────────────────────────────────────────────────
    /// Current FSM state from `migration_runtime_state`.
    pub current_state: FsmState,
    /// Run ID of the in-progress (or last completed) migration.
    pub run_id: String,
    /// The SQL file currently being applied, if any.
    pub active_file: String,
    /// Number of auto-recovery retries in the current run.
    pub retry_count: u32,

    // ── Applied files ─────────────────────────────────────────────────────────
    /// Total number of SQL files recorded in `schema_migrations`.
    pub total_applied: usize,
    /// Filename of the most recently applied SQL file.
    pub last_applied: String,
    /// Unix timestamp of the most recently applied file.
    pub last_applied_at_unix: Option<u64>,

    // ── Pending ───────────────────────────────────────────────────────────────
    /// Number of SQL files on disk that have not yet been applied.
    pub pending_count: usize,

    // ── Proto manifest ────────────────────────────────────────────────────────
    /// SHA-256 checksum of the currently applied proto manifest.
    pub manifest_checksum: String,
    /// Generator version of the applied manifest.
    pub generator_version: String,

    // ── Drift ─────────────────────────────────────────────────────────────────
    /// Number of drift items detected in the last drift analysis.
    pub drift_count: usize,
    /// Short human-readable descriptions of the most recent drift items.
    pub drift_summary: Vec<String>,

    // ── Lint ─────────────────────────────────────────────────────────────────
    /// Number of lint errors in the last catalog lint.
    pub lint_error_count: usize,
    /// Number of lint warnings in the last catalog lint.
    pub lint_warning_count: usize,

    // ── Last error ────────────────────────────────────────────────────────────
    /// Summary of the last unresolved migration error, if any.
    pub last_error: Option<ErrorSummary>,

    // ── Timestamps ────────────────────────────────────────────────────────────
    /// Unix timestamp of the last successfully completed migration run.
    pub last_completed_at_unix: Option<u64>,
    /// Unix timestamp when this status snapshot was produced.
    pub checked_at_unix: u64,
}

impl Default for MigrationStatus {
    fn default() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        Self {
            current_state: FsmState::Idle,
            run_id: String::new(),
            active_file: String::new(),
            retry_count: 0,
            total_applied: 0,
            last_applied: String::new(),
            last_applied_at_unix: None,
            pending_count: 0,
            manifest_checksum: String::new(),
            generator_version: String::new(),
            drift_count: 0,
            drift_summary: Vec::new(),
            lint_error_count: 0,
            lint_warning_count: 0,
            last_error: None,
            last_completed_at_unix: None,
            checked_at_unix: now,
        }
    }
}

// ── Error summary ─────────────────────────────────────────────────────────────

/// Condensed view of the last migration error, sourced from `migration_error_log`.
/// Mirrors `ErrorSummary` in legacy_sql `status.go`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ErrorSummary {
    /// Auto-increment primary key of the error log entry.
    pub id: i64,
    /// Run ID that produced the error.
    pub run_id: String,
    /// FSM state the engine was in when the error occurred.
    pub fsm_state: String,
    /// SQL file being applied when the error occurred (empty for non-apply phases).
    pub filename: String,
    /// Human-readable error message.
    pub error_message: String,
    /// Unix timestamp of when the error occurred.
    pub failed_at_unix: u64,
    /// `true` when `resolved_at IS NOT NULL` in the error log.
    pub resolved: bool,
}

// ── Error history entry ───────────────────────────────────────────────────────

/// One row from the `migration_error_log` table, returned by `ErrorHistory`.
/// Extends `ErrorSummary` with the optional JSON detail blob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ErrorLogEntry {
    pub id: i64,
    pub run_id: String,
    pub fsm_state: String,
    pub filename: String,
    pub error_message: String,
    /// Optional structured JSON detail (may be `null` in the DB).
    pub error_detail_json: String,
    pub failed_at_unix: u64,
    pub resolved: bool,
    /// The note left by the engineer when resolving the error.
    pub recovery_note: String,
}

// ── Status printer ────────────────────────────────────────────────────────────

impl MigrationStatus {
    /// Formats the status as a human-readable multi-line string, matching the
    /// output of `PrintStatus()` in legacy_sql `status.go`.
    pub fn format_summary(&self) -> String {
        let mut lines = Vec::new();
        lines
            .push("━━━━━━━━━━━━━━━━━━━━━━━━ Migration Status ━━━━━━━━━━━━━━━━━━━━━━━━".to_string());
        lines.push(format!("  State        : {}", self.current_state));
        lines.push(format!("  Run ID       : {}", self.run_id));
        lines.push(format!("  Applied      : {} file(s)", self.total_applied));
        lines.push(format!("  Pending      : {} file(s)", self.pending_count));
        if !self.last_applied.is_empty() {
            lines.push(format!("  Last Applied : {}", self.last_applied));
        }
        if !self.active_file.is_empty() {
            lines.push(format!("  Active File  : {}", self.active_file));
        }
        if self.retry_count > 0 {
            lines.push(format!(
                "  Retry Count  : {}/{}",
                self.retry_count,
                crate::engine::MAX_RETRIES
            ));
        }
        if !self.manifest_checksum.is_empty() {
            lines.push(format!(
                "  Manifest     : {}",
                &self.manifest_checksum[..16.min(self.manifest_checksum.len())]
            ));
        }
        if self.drift_count > 0 {
            lines.push(format!("  Drift        : {} item(s)", self.drift_count));
            for summary in &self.drift_summary {
                lines.push(format!("    ↳ {summary}"));
            }
        }
        if self.lint_error_count > 0 || self.lint_warning_count > 0 {
            lines.push(format!(
                "  Lint         : {} error(s), {} warning(s)",
                self.lint_error_count, self.lint_warning_count
            ));
        }
        if let Some(err) = &self.last_error
            && !err.resolved
        {
            lines.push(format!(
                "  Last Error   : [{}] {} (file: {}, at: {})",
                err.fsm_state, err.error_message, err.filename, err.failed_at_unix
            ));
        }
        lines
            .push("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━".to_string());
        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_status_idle_state() {
        let s = MigrationStatus::default();
        assert_eq!(s.current_state, FsmState::Idle);
        assert_eq!(s.total_applied, 0);
        assert!(s.last_error.is_none());
    }

    #[test]
    fn format_summary_contains_state() {
        let s = MigrationStatus::default();
        let summary = s.format_summary();
        assert!(summary.contains("IDLE"), "summary must include FSM state");
        assert!(
            summary.contains("Migration Status"),
            "summary must include header"
        );
    }
}
