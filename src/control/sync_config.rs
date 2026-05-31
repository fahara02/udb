// src/sync_config.rs — Primary → backup data synchronisation config & result types.
//
// When sync is enabled, the Go UDB service runs a background worker that
// periodically copies changed rows from the primary DB to the backup DB using
// per-table row-count comparison and INSERT … ON CONFLICT DO NOTHING.
//
// The Rust library exposes the configuration and result types so the CLI tool
// can validate sync settings and display sync status without a live DB connection.
//
// Aligned with:
//   - legacy_sql  legacycore/db/sync.go   (StartSyncWorker, SyncResult, SyncConfig)

use serde::{Deserialize, Serialize};

// ── Sync config ───────────────────────────────────────────────────────────────

/// Configuration for the primary → backup data sync worker.
///
/// Read from `database.yaml` under the `sync:` key.
/// Mirrors the sync section of `Config` in legacy_sql `config.go`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncConfig {
    /// Enable the background sync worker.  Default: `false`.
    pub enabled: bool,
    /// Sync interval as a Go-style duration string, e.g. `"15m"`, `"1h"`.
    /// Default: `"15m"`.
    pub interval: String,
    /// Number of rows to copy per batch.  Default: 500.
    pub batch_size: usize,
    /// Explicit list of `"schema.table"` qualified names to sync.
    /// When empty, the worker auto-discovers all user tables from `pg_class`.
    pub tables_to_sync: Vec<String>,
    /// Skip tables whose row count difference is below this threshold.
    /// Helps reduce noise from high-frequency tables.  Default: 0 (sync all).
    pub min_row_diff_threshold: i64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval: "15m".to_string(),
            batch_size: 500,
            tables_to_sync: Vec::new(),
            min_row_diff_threshold: 0,
        }
    }
}

impl SyncConfig {
    /// Returns the effective batch size (>0), falling back to 500.
    pub fn effective_batch_size(&self) -> usize {
        if self.batch_size > 0 {
            self.batch_size
        } else {
            500
        }
    }

    /// Returns `true` when specific tables are configured (vs auto-discovery).
    pub fn has_explicit_tables(&self) -> bool {
        !self.tables_to_sync.is_empty()
    }
}

// ── Sync result ───────────────────────────────────────────────────────────────

/// Outcome of one primary → backup sync cycle.
/// Mirrors `SyncResult` in legacy_sql `sync.go`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SyncResult {
    /// Unix timestamp when the sync cycle started.
    pub started_at_unix: u64,
    /// Unix timestamp when the sync cycle finished.
    pub finished_at_unix: u64,
    /// Number of tables that were compared and (if needed) synced.
    pub tables_synced: usize,
    /// Total number of rows copied from primary to backup.
    pub rows_copied: i64,
    /// Non-fatal error messages accumulated during the cycle.
    pub errors: Vec<String>,
}

impl SyncResult {
    /// Returns `true` when the cycle completed without any errors.
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }

    /// Returns the elapsed duration in milliseconds.
    pub fn elapsed_ms(&self) -> u64 {
        self.finished_at_unix.saturating_sub(self.started_at_unix) * 1000
    }

    /// Returns a one-line human-readable summary of the cycle.
    pub fn summary(&self) -> String {
        if self.is_clean() {
            format!(
                "sync cycle ok: tables={} rows={} elapsed={}ms",
                self.tables_synced,
                self.rows_copied,
                self.elapsed_ms()
            )
        } else {
            format!(
                "sync cycle finished with {} error(s): tables={} rows={} elapsed={}ms",
                self.errors.len(),
                self.tables_synced,
                self.rows_copied,
                self.elapsed_ms()
            )
        }
    }
}

// ── Sync status ───────────────────────────────────────────────────────────────

/// Aggregated sync worker status exposed by the health endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SyncStatus {
    /// Whether the sync worker is configured and running.
    pub enabled: bool,
    /// Number of sync cycles completed since the worker started.
    pub cycles_completed: u64,
    /// Total rows copied across all cycles.
    pub total_rows_copied: i64,
    /// Total errors across all cycles.
    pub total_errors: usize,
    /// Result of the most recent sync cycle.
    pub last_result: Option<SyncResult>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_config_defaults() {
        let cfg = SyncConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.interval, "15m");
        assert_eq!(cfg.effective_batch_size(), 500);
    }

    #[test]
    fn sync_result_is_clean_when_no_errors() {
        let r = SyncResult {
            tables_synced: 5,
            rows_copied: 120,
            ..Default::default()
        };
        assert!(r.is_clean());
    }

    #[test]
    fn sync_result_not_clean_with_errors() {
        let r = SyncResult {
            errors: vec!["table app_intake.artifacts: PK discovery failed".to_string()],
            ..Default::default()
        };
        assert!(!r.is_clean());
    }

    #[test]
    fn sync_result_summary_clean() {
        let r = SyncResult {
            tables_synced: 3,
            rows_copied: 42,
            started_at_unix: 1000,
            finished_at_unix: 1002,
            ..Default::default()
        };
        let s = r.summary();
        assert!(s.contains("tables=3"));
        assert!(s.contains("rows=42"));
        assert!(s.contains("elapsed=2000ms"));
    }
}
