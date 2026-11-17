//! The cleanup service: retention and partition upkeep for UDB's own
//! append-only tables.
//!
//! The leader-elected worker ([`crate::runtime::singleton::WORKER_SYSTEM_CLEANUP`])
//! keeps UDB's own logs from growing without bound. Each pass does three
//! things: partition upkeep, retention and (optionally) disk reclaim.
//! `UDB_CLEANUP_ENABLED=false` (or `udb serve --cleanup-enabled false`) turns
//! the whole worker off; `udb cleanup run` still runs a pass on demand.
//!
//! **Partition upkeep, always on.** Every native table that is range-partitioned
//! by time (for example `udb_authz.access_decision_audits`, monthly) gets its
//! current partition and the declared number of future ones (`partition_premake`,
//! default 4). Without pg_partman, the migration creates only the partitions
//! that exist on the day it runs, and the first write past the last one fails;
//! the worker keeps the window moving. A table that pg_partman manages is left
//! to pg_partman.
//!
//! **Retention per table.** UDB's own logs follow the retention their proto
//! declares (`retention_days`) out of the box: the default list is
//! [`DEFAULT_CLEANUP_RETENTION`] (`access_decision_audits`, `auth_audit_log`,
//! `usage_events`, `api_key_usages`, `notification_logs` = `declared`;
//! `auth_audit_log` has no proto and follows the decision audit's), plus each
//! table's own default (below). `UDB_CLEANUP_RETENTION` (or `udb serve
//! --cleanup-retention`) is merged over that list, entry by entry, for example
//! `access_decision_audits=90d,usage_events=keep`. A value is a duration (`90d`,
//! `36h`), `declared` or `keep` (every row is kept). Any other time-partitioned
//! UDB table keeps every row unless it is listed. A table can be named by its
//! table name or as `schema.table`; the shortest retention accepted is one day.
//! A malformed entry pauses removal for the table it names (partition upkeep
//! continues there; an entry naming no known table pauses nothing); every
//! pass logs the error until it is fixed. It never falls back to the default
//! retention: a typo in a long retention must not delete what it meant to keep.
//!
//! - A partitioned table drops whole partitions whose upper bound is older than
//!   the retention (disk is given back at once). Only partitions UDB created
//!   (`<table>_p_<period>`) are dropped, never a default or hand-made partition.
//!   With pg_partman, the retention is written to `partman.part_config` instead.
//!   Rows past retention inside the partitions that are kept (the current
//!   month, a hand-made partition) are swept out once a day, one pass over each
//!   partition in page ranges.
//! - A table can keep one class of rows longer: `access_decision_audits:deny=90d`
//!   keeps denied decisions 90 days while allows follow
//!   `access_decision_audits`; `:deny=keep` keeps them for good. A class is
//!   never kept shorter than its table, and a partition is dropped only once
//!   its longest-kept rows expired.
//! - `udb_system.auth_audit_log` and `udb_metering.usage_events` delete rows
//!   older than the retention in bounded batches. When the compliance evidence
//!   export is on, `auth_audit_log` rows not yet exported are kept, so the
//!   exported hash chain stays complete. A new `auth_audit_log` is partitioned
//!   by month (an older one once `udb cleanup partition-audit-log` converted
//!   it, see [`audit_log`]): months are kept ahead, and a UDB-named month whose
//!   rows are all past retention (and, with the export on, all exported) is
//!   dropped whole; rows in the default partition and the current month are
//!   deleted as before.
//! - Every other native table that declares `retention_days` and whose rows
//!   have an end (sessions, token revocations, MFA challenges, webhook
//!   deliveries, revoked devices, ...) follows its declared retention, counted
//!   from the column that marks the end of a row's life ([`ROW_END_COLUMNS`]:
//!   `expires_at` first, so a token revocation outlives the token it blocks).
//!   A table with a default partition (sessions) is swept partition by
//!   partition. A soft-deleted entity table, a table other tables reference,
//!   or a table with no such column is not touched (see [`row_end_column`]).
//! - UDB's own bookkeeping tables keep only what is still needed: completed
//!   projection tasks (7 days), resolved CDC dead letters (30 days), finished
//!   sagas and XA decisions (30 days), catalog versions (30 days, never the
//!   active version nor the newest `UDB_CLEANUP_KEEP_VERSIONS`, default 20,
//!   per project), the catalog reload log (90 days, never the activation
//!   record of an active version nor the newest row), migration runs
//!   (completed and dry runs, 90 days, never a project's newest completed run
//!   nor a run a reviewed catalog transition names) and the admin audit log
//!   (365 days, oldest first and never its hash chain's head; each prune
//!   records an anchor, the id and hash of the newest row it removed, in the
//!   same statement, and chain verification starts from it).
//! - Those last two groups are swept once per `UDB_CLEANUP_SWEEP_INTERVAL`
//!   (default `24h`, the same interval as the kept-partition sweep), one pass
//!   over each table in page ranges, so they need no index on their time column.
//! - The CDC journal has its own sweep: `UDB_CDC_JOURNAL_RETENTION`.
//!
//! **Reclaim.** Deleted rows leave free space inside the table file.
//! `UDB_CLEANUP_RECLAIM` decides what happens after a pass deleted rows from a
//! table: `vacuum` (default) runs `VACUUM (ANALYZE)` on it so the space is
//! reused at once and nothing is locked; `full` runs `VACUUM FULL`, which gives
//! the space back to the operating system but locks the table while it runs;
//! `off` leaves it to autovacuum; `online` gives the space back to the
//! operating system while the table stays in use: it copies the live rows into
//! a compact twin page range by page range, then swaps the two under a lock
//! held only for the swap (bounded by a 3 s `lock_timeout`, retried when the
//! table is busy). A table it cannot move faithfully (triggers, foreign keys,
//! identity columns, views over it, no primary key...) is refused with the
//! reason. `udb cleanup run --reclaim full` (or `online`) reclaims once, for a
//! maintenance window. Dropped partitions free their space immediately.
//!
//! **Size cap.** `UDB_CLEANUP_MAX_SIZE` (default `1GB`, `off` turns it off)
//! caps the estimated size of the log tables that have a retention. While
//! they are over it, each pass deletes their oldest rows, in proportion to
//! each table's size, even rows still inside retention, down to 90% of the
//! cap. Rows written in the last hour, rows the evidence export has not
//! exported and rows of a table's kept class (denied decisions) are never
//! deleted for the cap; when they alone exceed it, the pass says so. A table
//! set to `keep` is outside the cap. The CDC journal has its own retention.
//!
//! A table the database role cannot see across tenants (row-level security
//! applies to the role and the table's policy has no `app.platform_admin`
//! clause) is reported as an error instead of silently deleting nothing.
//!
//! `UDB_CLEANUP_INTERVAL` sets how often a pass runs (default `10m`)
//! and `UDB_CLEANUP_BATCH` how many rows one delete removes (default
//! 5000, at most 20 deletes per table per pass). Each setting is also a
//! `udb serve` flag; `udb cleanup show` prints what is in force and
//! `udb cleanup run` runs one pass now that takes the whole backlog
//! (`--reclaim [full]` also reclaims every log table and the CDC journal).

use std::collections::BTreeMap;
use std::time::Duration;

use sqlx::{PgPool, Row};

use crate::generation::manifest::ManifestTable;

/// Per-table retention list, merged over [`DEFAULT_CLEANUP_RETENTION`].
pub const CLEANUP_RETENTION_ENV: &str = "UDB_CLEANUP_RETENTION";
/// Pass interval.
pub const CLEANUP_INTERVAL_ENV: &str = "UDB_CLEANUP_INTERVAL";
/// Rows one delete statement removes.
pub const CLEANUP_BATCH_ENV: &str = "UDB_CLEANUP_BATCH";
/// Whether the broker runs the cleanup worker (default on).
pub const CLEANUP_ENABLED_ENV: &str = "UDB_CLEANUP_ENABLED";
/// What happens to the space deleted rows leave (`off`, `vacuum`, `full`).
pub const CLEANUP_RECLAIM_ENV: &str = "UDB_CLEANUP_RECLAIM";
/// `declared`: drop app-table partitions past the table's declared
/// `retention_days`. Default `off`: app partitions are created ahead of
/// writes and never dropped.
pub const CLEANUP_APP_RETENTION_ENV: &str = "UDB_CLEANUP_APP_RETENTION";
/// `statement_timeout` for cleanup's own statements (`0` = none, the default):
/// a database or role timeout must not cancel a long sweep or VACUUM FULL.
pub const CLEANUP_STATEMENT_TIMEOUT_ENV: &str = "UDB_CLEANUP_STATEMENT_TIMEOUT";
/// Heap pages one sweep statement covers (default 4096 = 32 MiB).
pub const CLEANUP_SWEEP_PAGES_ENV: &str = "UDB_CLEANUP_SWEEP_PAGES";
/// Cap on the estimated size of UDB's own log tables (`1GB`, `500MB`, `off`).
pub const CLEANUP_MAX_SIZE_ENV: &str = "UDB_CLEANUP_MAX_SIZE";
/// The default size cap: 1 GiB.
pub const DEFAULT_MAX_SIZE: u64 = 1 << 30;
/// How often kept partitions and the swept tables are swept (`24h`, `6h`, `30m`).
pub const CLEANUP_SWEEP_INTERVAL_ENV: &str = "UDB_CLEANUP_SWEEP_INTERVAL";
/// Catalog versions always kept per project, newest first.
pub const CLEANUP_KEEP_VERSIONS_ENV: &str = "UDB_CLEANUP_KEEP_VERSIONS";
pub const DEFAULT_KEEP_VERSIONS: u32 = 20;
/// The `service` of a target in the system catalog (the primary pool), not a
/// native service's store.
const SYSTEM_STORE: &str = "udb_system";
/// Rows newer than this are never deleted for the size cap.
const SIZE_CAP_FLOOR: Duration = Duration::from_secs(3600);
/// Rows one table's size estimate samples.
const SIZE_SAMPLE_ROWS: f64 = 20_000.0;

/// `1GB`, `500MB`, `2G`, `1048576` (bytes) → a cap; `off`/`0` → none.
pub fn parse_size(raw: &str) -> Result<Option<u64>, String> {
    let value = raw.trim().to_ascii_lowercase().replace(' ', "");
    if matches!(value.as_str(), "off" | "none" | "0" | "false") {
        return Ok(None);
    }
    let split = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(split);
    let number: u64 = number
        .parse()
        .map_err(|_| format!("{CLEANUP_MAX_SIZE_ENV}: `{raw}` is not a size like 1GB or 500MB"))?;
    let scale: u64 = match unit {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1 << 10,
        "m" | "mb" | "mib" => 1 << 20,
        "g" | "gb" | "gib" => 1 << 30,
        "t" | "tb" | "tib" => 1 << 40,
        _ => {
            return Err(format!(
                "{CLEANUP_MAX_SIZE_ENV}: `{raw}`: unit `{unit}` is not KB, MB, GB or TB"
            ));
        }
    };
    number
        .checked_mul(scale)
        .filter(|bytes| *bytes > 0)
        .map(Some)
        .ok_or_else(|| format!("{CLEANUP_MAX_SIZE_ENV}: `{raw}` is too large"))
}

fn describe_size(bytes: u64) -> String {
    if bytes.is_multiple_of(1 << 30) {
        format!("{}GB", bytes >> 30)
    } else if bytes.is_multiple_of(1 << 20) {
        format!("{}MB", bytes >> 20)
    } else {
        format!("{bytes}B")
    }
}

/// How the space of deleted rows is handled after a pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reclaim {
    /// Leave it to autovacuum.
    Off,
    /// `VACUUM (ANALYZE)`: the space is reused at once; nothing is locked.
    Vacuum,
    /// `VACUUM FULL`: the space goes back to the operating system; the table
    /// is locked while it runs.
    Full,
    /// Online compaction: the space goes back to the operating system and
    /// the table stays in use; it is locked only for a short swap at the end
    /// (see [`compact`]), a partitioned table partition by partition. A table
    /// too small or not bloated enough to be worth copying only gets
    /// `VACUUM (ANALYZE)`.
    Online,
}

impl Reclaim {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" | "none" | "0" | "false" => Ok(Reclaim::Off),
            "vacuum" | "" => Ok(Reclaim::Vacuum),
            "full" => Ok(Reclaim::Full),
            "online" => Ok(Reclaim::Online),
            other => Err(format!(
                "{CLEANUP_RECLAIM_ENV}: `{other}` is not off, vacuum, full or online"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Reclaim::Off => "off",
            Reclaim::Vacuum => "vacuum",
            Reclaim::Full => "full",
            Reclaim::Online => "online",
        }
    }
}

/// Every cleanup setting, resolved ONCE (at worker spawn, or by the CLI) from
/// the environment, which `.env` and the `udb serve` setting flags feed. A
/// malformed retention entry pauses removal for its table; a malformed reclaim
/// mode or size cap is kept as its error. The pass reports every one.
#[derive(Debug, Clone)]
pub struct CleanupSettings {
    pub enabled: bool,
    pub interval: Duration,
    pub batch: i64,
    pub reclaim: Result<Reclaim, String>,
    /// The size cap in bytes (`None` = off).
    pub max_size: Result<Option<u64>, String>,
    /// Drop app-table partitions past their declared `retention_days`.
    pub app_retention: bool,
    /// How often kept partitions and the swept tables are swept.
    pub sweep_interval: Duration,
    /// Catalog versions always kept per project, newest first.
    pub keep_versions: u32,
    pub(crate) targets: Vec<Target>,
    pub(crate) retention: RetentionConfig,
}

impl CleanupSettings {
    /// Read the settings from the environment.
    pub fn from_env() -> Self {
        let read = |key: &str| std::env::var(key).ok();
        let evidence_export =
            crate::runtime::evidence_export::EvidenceExportConfig::from_env().enabled;
        Self::from_values(
            read(CLEANUP_ENABLED_ENV).as_deref(),
            read(CLEANUP_INTERVAL_ENV).as_deref(),
            read(CLEANUP_BATCH_ENV).as_deref(),
            read(CLEANUP_RECLAIM_ENV).as_deref(),
            read(CLEANUP_RETENTION_ENV).as_deref(),
            evidence_export,
        )
        .with_max_size(read(CLEANUP_MAX_SIZE_ENV).as_deref())
        .with_app_retention(read(CLEANUP_APP_RETENTION_ENV).as_deref())
        .with_session_settings(
            read(CLEANUP_STATEMENT_TIMEOUT_ENV).as_deref(),
            read(CLEANUP_SWEEP_PAGES_ENV).as_deref(),
        )
        .with_sweep_interval(read(CLEANUP_SWEEP_INTERVAL_ENV).as_deref())
        .with_keep_versions(read(CLEANUP_KEEP_VERSIONS_ENV).as_deref())
    }

    /// Set the sweep interval from its raw value (unset or malformed → 24h).
    pub(crate) fn with_sweep_interval(mut self, raw: Option<&str>) -> Self {
        self.sweep_interval = raw
            .and_then(parse_interval)
            .unwrap_or(DEFAULT_SWEEP_INTERVAL);
        self
    }

    /// Set how many catalog versions each project keeps (unset or malformed
    /// → 20, at least 1) and rebuild the targets whose rule names it.
    pub(crate) fn with_keep_versions(mut self, raw: Option<&str>) -> Self {
        self.keep_versions = raw
            .and_then(|raw| raw.trim().parse::<u32>().ok())
            .map(|keep| keep.max(1))
            .unwrap_or(DEFAULT_KEEP_VERSIONS);
        for rebuilt in system_targets(self.keep_versions) {
            if let Some(target) = self
                .targets
                .iter_mut()
                .find(|target| target.qualified() == rebuilt.qualified())
            {
                target.kind = rebuilt.kind;
            }
        }
        self
    }

    /// Install cleanup's statement timeout and sweep range for this process
    /// (read once; a malformed value logs and keeps the default).
    pub(crate) fn with_session_settings(self, timeout: Option<&str>, pages: Option<&str>) -> Self {
        let timeout = match timeout.map(parse_statement_timeout) {
            Some(Ok(value)) => value,
            Some(Err(why)) => {
                tracing::warn!("{why}; cleanup runs without a statement timeout");
                "0".to_string()
            }
            None => "0".to_string(),
        };
        let _ = STATEMENT_TIMEOUT.set(timeout);
        let pages = pages
            .and_then(|raw| raw.trim().parse::<i64>().ok())
            .filter(|pages| *pages > 0)
            .unwrap_or(DEFAULT_SWEEP_PAGES);
        let _ = SWEEP_PAGES.set(pages);
        self
    }

    /// `declared` drops app partitions past their declared retention; anything
    /// else (default) keeps them.
    pub(crate) fn with_app_retention(mut self, raw: Option<&str>) -> Self {
        self.app_retention = raw
            .map(|raw| raw.trim().eq_ignore_ascii_case("declared"))
            .unwrap_or(false);
        self
    }

    /// Set the size cap from its raw value (`None` = unset → 1GB).
    pub(crate) fn with_max_size(mut self, raw: Option<&str>) -> Self {
        self.max_size = raw.map_or(Ok(Some(DEFAULT_MAX_SIZE)), parse_size);
        self
    }

    /// The size cap as `udb cleanup show` prints it.
    pub fn describe_max_size(&self) -> String {
        match &self.max_size {
            Ok(Some(bytes)) => describe_size(*bytes),
            Ok(None) => "off".to_string(),
            Err(why) => why.clone(),
        }
    }

    /// Every malformed retention entry, as the pass reports it.
    pub fn retention_errors(&self) -> &[String] {
        &self.retention.errors
    }

    /// Build the settings from raw values (`None` = unset → the default).
    pub(crate) fn from_values(
        enabled: Option<&str>,
        interval: Option<&str>,
        batch: Option<&str>,
        reclaim: Option<&str>,
        retention: Option<&str>,
        evidence_export: bool,
    ) -> Self {
        let targets = targets(evidence_export);
        // Each table's own default first, then the default list, then the
        // operator's list, entry by entry.
        let mut config = RetentionConfig::default();
        for target in &targets {
            if let Some(default) = target.default_retention {
                config.retention.insert(target.qualified(), Some(default));
            }
        }
        let listed = parse_retention_entries(DEFAULT_CLEANUP_RETENTION, &targets);
        config.retention.extend(listed.retention);
        config.paused.extend(listed.paused);
        config.errors.extend(listed.errors);
        if let Some(raw) = retention {
            let set = parse_retention_entries(raw, &targets);
            config.retention.extend(set.retention);
            config.paused.extend(set.paused);
            config.errors.extend(set.errors);
        }
        let retention = config;
        Self {
            enabled: !matches!(
                enabled.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
                Some("0" | "false" | "no" | "off")
            ),
            interval: interval
                .and_then(parse_interval)
                .unwrap_or(DEFAULT_INTERVAL),
            batch: batch
                .and_then(|raw| raw.trim().parse::<i64>().ok())
                .filter(|batch| *batch > 0)
                .unwrap_or(DEFAULT_ROW_BATCH),
            reclaim: reclaim.map_or(Ok(Reclaim::Vacuum), Reclaim::parse),
            max_size: Ok(Some(DEFAULT_MAX_SIZE)),
            app_retention: false,
            sweep_interval: DEFAULT_SWEEP_INTERVAL,
            keep_versions: DEFAULT_KEEP_VERSIONS,
            targets,
            retention,
        }
    }

    /// One entry per table, for `udb cleanup show` and the startup log: the
    /// retention in force (`keep`, days or hours) and the declared one. A
    /// table a malformed entry paused says so.
    pub fn describe(&self) -> Result<Vec<serde_json::Value>, String> {
        let config = &self.retention;
        Ok(self
            .targets
            .iter()
            .map(|target| {
                let shown = |retention: Option<Duration>| {
                    retention
                        .map(describe_duration)
                        .unwrap_or_else(|| "keep".to_string())
                };
                let retention = config.retention_of(target);
                let mut entry = serde_json::json!({
                    "table": target.qualified(),
                    "kind": match target.kind {
                        TargetKind::Partitioned { .. } => "partitioned",
                        TargetKind::Rows { .. } => "rows",
                    },
                    "retention": shown(retention.rows),
                    "declared": target.declared.map(describe_duration),
                });
                if let Some(why) = config.paused.get(&target.qualified()) {
                    entry["paused"] = serde_json::Value::String(why.clone());
                }
                if target.periodic {
                    entry["swept_every"] =
                        serde_json::Value::String(describe_duration(self.sweep_interval));
                }
                if let Some(class) = &target.kept_class {
                    entry["kept_class"] = serde_json::json!({
                        "name": class.name,
                        "retention": shown(retention.kept),
                    });
                }
                entry
            })
            .collect())
    }
}

/// `30s`, `10m`, `1h` or plain seconds; zero or malformed → `None`.
fn parse_interval(raw: &str) -> Option<Duration> {
    let raw = raw.trim().to_ascii_lowercase();
    let split = raw.find(|c: char| !c.is_ascii_digit()).unwrap_or(raw.len());
    let (number, unit) = raw.split_at(split);
    let number: u64 = number.parse().ok()?;
    let seconds = match unit {
        "" | "s" => number,
        "m" => number.checked_mul(60)?,
        "h" => number.checked_mul(3600)?,
        _ => return None,
    };
    (seconds > 0).then(|| Duration::from_secs(seconds))
}
/// The retention in force when nothing is configured: UDB's own logs follow
/// their declared retention.
pub const DEFAULT_CLEANUP_RETENTION: &str = "access_decision_audits=declared,\
     access_decision_audits:deny=declared,auth_audit_log=declared,usage_events=declared,\
     api_key_usages=declared,notification_logs=declared";
const DEFAULT_INTERVAL: Duration = Duration::from_secs(600);
const MIN_RETENTION: Duration = Duration::from_secs(24 * 3600);
const DEFAULT_PREMAKE: i32 = 4;
const DEFAULT_ROW_BATCH: i64 = 5_000;
const MAX_BATCHES_PER_PASS: usize = 20;
/// Heap pages one sweep statement covers (32 MiB at the default block size).
const DEFAULT_SWEEP_PAGES: i64 = 4_096;

/// Cleanup's statement timeout and sweep range, set once from the settings
/// (`UDB_CLEANUP_STATEMENT_TIMEOUT`, `UDB_CLEANUP_SWEEP_PAGES`).
static STATEMENT_TIMEOUT: std::sync::OnceLock<String> = std::sync::OnceLock::new();
static SWEEP_PAGES: std::sync::OnceLock<i64> = std::sync::OnceLock::new();

pub(crate) mod audit_log;
mod compact;

fn statement_timeout() -> &'static str {
    STATEMENT_TIMEOUT.get().map_or("0", String::as_str)
}

fn sweep_pages() -> i64 {
    *SWEEP_PAGES.get().unwrap_or(&DEFAULT_SWEEP_PAGES)
}

/// A PostgreSQL duration for `statement_timeout`: `0`/`off`/`none` (no
/// timeout), plain milliseconds, or a number with `ms`, `s`, `min` or `h`.
fn parse_statement_timeout(raw: &str) -> Result<String, String> {
    let value = raw.trim().to_ascii_lowercase();
    if matches!(value.as_str(), "" | "0" | "off" | "none") {
        return Ok("0".to_string());
    }
    let split = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(split);
    let number: u64 = number.parse().map_err(|_| {
        format!("{CLEANUP_STATEMENT_TIMEOUT_ENV}: `{raw}` is not a duration like 30min or 0")
    })?;
    match unit.trim() {
        "" | "ms" => Ok(format!("{number}ms")),
        "s" => Ok(format!("{number}s")),
        "m" | "min" => Ok(format!("{number}min")),
        "h" => Ok(format!("{number}h")),
        other => Err(format!(
            "{CLEANUP_STATEMENT_TIMEOUT_ENV}: unit `{other}` is not ms, s, min or h"
        )),
    }
}
/// How often the scheduled pass sweeps rows out of the partitions it keeps
/// and out of the swept tables (`UDB_CLEANUP_SWEEP_INTERVAL`). Retention is
/// set in days, so once a day is enough; `udb cleanup run` always sweeps.
const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(24 * 3600);
/// Columns that mark the end of a native row's life, in order of preference.
/// `expires_at` comes first: a row that expires (a token revocation, a
/// session, a challenge) is only aged from its expiry, never from an earlier
/// revocation or use, so a revocation is never deleted while the token it
/// blocks can still be presented.
pub(crate) const ROW_END_COLUMNS: &[&str] = &[
    "expires_at",
    "occurred_at",
    "revoked_at",
    "consumed_at",
    "acked_at",
    "delivered_at",
    "completed_at",
    "finished_at",
];
/// Native tables whose rows are complete records from the moment they are
/// written, so their creation time is the time they age from.
const APPEND_ONLY_RECORDS: &[&str] = &["notification_delivery_attempts", "policy_simulations"];
/// Native tables that declare a retention but whose rows must not age out on
/// a timestamp alone: a database credential lease row is how its login is
/// found and revoked, so an expired lease whose revocation failed stays.
const NO_ROW_EXPIRY: &[&str] = &["vault_db_credential_leases"];
const DECISION_AUDIT_MESSAGE: &str = "udb.core.authz.entity.v1.AccessDecisionAudit";
const USAGE_EVENT_MESSAGE: &str = "udb.core.metering.entity.v1.UsageEvent";

#[cfg(test)]
const AUTH_AUDIT_LOG: &str = "udb_system.auth_audit_log";
const EVIDENCE_STATE: &str = "udb_system.evidence_export_state";
const CDC_JOURNAL_TABLE: &str = "udb_cdc_event_journal";

/// How a table's rows age out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TargetKind {
    /// Range-partitioned by time: partitions are kept ahead and expired ones
    /// dropped.
    Partitioned {
        /// The partition key column.
        column: String,
        /// `date_trunc` unit (`month`, `day`, ...).
        unit: String,
        /// PostgreSQL interval of one partition (`1 month`).
        interval: String,
        /// `to_char` format of a partition suffix, as the migration names it.
        suffix_format: String,
        premake: i32,
    },
    /// Rows deleted in batches by a timestamp column.
    Rows {
        column: String,
        /// Keep rows the evidence export has not reached yet.
        evidence_state_key: Option<String>,
        /// A further SQL condition a row must meet to be deleted (a finished
        /// status, not the active version, ...). Written by this module only.
        only_when: Option<String>,
        /// What else decides which expired rows may go.
        guard: RowGuard,
        /// The table may be range-partitioned by month on `column` (the auth
        /// audit log, once converted): while it is, months are kept ahead and
        /// UDB-named months past retention (and, with an evidence export,
        /// fully exported) are dropped whole; rows past retention are still
        /// deleted from the partitions that stay. While it is not, nothing
        /// changes.
        monthly_partitions: bool,
    },
}

/// What else decides which of a table's expired rows may go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RowGuard {
    /// Nothing: every expired row that meets `only_when` goes.
    Unguarded,
    /// Another table whose rows point at this one's, as `(relation,
    /// column)`: a row it points at is kept. That table may not exist yet (it
    /// is created on first use); then no row is pointed at.
    ReferencedBy(String, String),
    /// The admin audit hash chain: rows go oldest first, always a contiguous
    /// prefix of the chain that stops before its head, and each delete
    /// records the prune anchor verification starts from, in the same
    /// statement ([`admin_audit_prune_sql`]).
    AdminAuditChain,
}

/// One UDB-owned table the worker looks after.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Target {
    pub schema: String,
    pub table: String,
    /// Native service whose store holds the table.
    pub service: String,
    pub kind: TargetKind,
    /// The proto-declared retention (`retention_days`), if any.
    pub declared: Option<Duration>,
    /// Rows kept at least as long as the table and longer when configured.
    pub kept_class: Option<KeptClass>,
    /// The retention in force when no retention list names the table
    /// (`None`: the table keeps every row unless listed).
    pub default_retention: Option<Duration>,
    /// Counted against the size cap (UDB's own logs only).
    pub capped: bool,
    /// Rows are removed by one page-range sweep per sweep interval instead of
    /// bounded batches every pass (no index on the time column is needed).
    pub periodic: bool,
}

/// A class of rows a table keeps longer than the rest, named in the retention
/// list after a colon: `access_decision_audits:deny=90d` keeps denied
/// decisions 90 days while allows follow `access_decision_audits`. A class is
/// never kept shorter than its table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeptClass {
    pub name: String,
    /// SQL condition true for the class's rows (never NULL).
    pub predicate: String,
}

/// The retention of one table: `rows` for its rows, `kept` for its kept class
/// (equal to `rows` when the table has none). `None` keeps the rows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Retention {
    pub rows: Option<Duration>,
    pub kept: Option<Duration>,
}

impl Retention {
    #[cfg(test)]
    fn uniform(retention: Duration) -> Self {
        Self {
            rows: Some(retention),
            kept: Some(retention),
        }
    }
}

/// What one pass did to one table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Outcome {
    pub created: i64,
    pub dropped: i64,
    pub deleted: i64,
    /// Partitions brought in line with the parent's row-level security.
    pub secured: i64,
}

impl Outcome {
    fn total(self) -> i64 {
        self.created + self.dropped + self.deleted + self.secured
    }
}

/// How hard one pass works on one table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PassMode {
    /// Rows one batched delete removes.
    pub batch: i64,
    /// `udb cleanup run`: no per-pass cap, the whole backlog goes, and an
    /// unpartitioned table is swept page range by page range.
    pub drain: bool,
    /// Sweep expired rows out of the partitions that are kept.
    pub sweep_partitions: bool,
}

impl Target {
    fn qualified(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    fn relation(&self) -> String {
        format!("{}.{}", qi(&self.schema), qi(&self.table))
    }
}

/// The retention in force: `schema.table` → retention (`None` = keep). A
/// table that is not in the map keeps every row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RetentionConfig {
    pub retention: BTreeMap<String, Option<Duration>>,
    /// Tables a malformed entry names → the error: nothing is removed there.
    pub paused: BTreeMap<String, String>,
    /// Every malformed entry.
    pub errors: Vec<String>,
}

impl RetentionConfig {
    pub(crate) fn retention_for(&self, target: &Target) -> Option<Duration> {
        self.retention.get(&target.qualified()).copied().flatten()
    }

    /// The table's retention plus its kept class's, never shorter than the
    /// table's. An unset class follows the table.
    pub(crate) fn retention_of(&self, target: &Target) -> Retention {
        let rows = self.retention_for(target);
        let kept = match &target.kept_class {
            None => rows,
            Some(class) => match self.retention.get(&class_key(target, class)) {
                None => rows,
                Some(class_retention) => match (rows, *class_retention) {
                    (Some(rows), Some(class_retention)) => Some(rows.max(class_retention)),
                    _ => None,
                },
            },
        };
        Retention { rows, kept }
    }
}

fn class_key(target: &Target, class: &KeptClass) -> String {
    format!("{}:{}", target.qualified(), class.name)
}

fn declared_days(days: i32) -> Option<Duration> {
    (days > 0).then(|| Duration::from_secs(u64::from(days.unsigned_abs()) * 86_400))
}

fn manifest_table(message_type: &str) -> Option<&'static ManifestTable> {
    crate::broker::resolve_table_for_message(
        crate::runtime::native_catalog::native_manifest(),
        message_type,
    )
    .ok()
}

fn describe_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs.is_multiple_of(86_400) {
        format!("{}d", secs / 86_400)
    } else if secs.is_multiple_of(3_600) {
        format!("{}h", secs / 3_600)
    } else {
        format!("{}m", secs / 60)
    }
}

/// Every table the worker looks after: the time-partitioned native tables, the
/// two unpartitioned append-only logs, the other native tables whose rows have
/// an end ([`row_end_column`]) and UDB's bookkeeping tables
/// ([`system_targets`]). With the evidence export on, the auth audit log keeps
/// rows it has not exported.
pub(crate) fn targets(evidence_export: bool) -> Vec<Target> {
    let mut targets: Vec<Target> = crate::runtime::native_catalog::native_manifest()
        .tables
        .iter()
        .filter_map(partitioned_target)
        .collect();
    // Denied decisions are the security record: they can be kept longer than
    // the routine allows (`access_decision_audits:deny=...`).
    if let Some(audit) = manifest_table(DECISION_AUDIT_MESSAGE)
        && let Some(target) = targets
            .iter_mut()
            .find(|target| target.schema == audit.schema && target.table == audit.table)
    {
        let model =
            crate::runtime::native_catalog::native_model(DECISION_AUDIT_MESSAGE, &["effect"]);
        target.kept_class = Some(KeptClass {
            name: "deny".to_string(),
            predicate: format!("{} IS DISTINCT FROM 'ALLOW'", qi(model.column("effect"))),
        });
    }
    // The auth audit log has no proto: it is kept as long as the decision audit.
    let audit_declared = manifest_table(DECISION_AUDIT_MESSAGE)
        .and_then(|table| declared_days(table.retention_days));
    targets.push(Target {
        schema: "udb_system".to_string(),
        table: "auth_audit_log".to_string(),
        service: "compliance".to_string(),
        declared: audit_declared,
        kept_class: None,
        kind: TargetKind::Rows {
            column: "occurred_at".to_string(),
            evidence_state_key: evidence_export
                .then(|| crate::runtime::singleton::WORKER_EVIDENCE_EXPORT.to_string()),
            only_when: None,
            guard: RowGuard::Unguarded,
            monthly_partitions: true,
        },
        default_retention: None,
        capped: true,
        periodic: false,
    });
    let usage = crate::runtime::native_catalog::native_model(USAGE_EVENT_MESSAGE, &["occurred_at"]);
    if let Some((schema, table)) = split_relation(&usage.relation) {
        targets.push(Target {
            schema,
            table,
            service: "metering".to_string(),
            declared: manifest_table(USAGE_EVENT_MESSAGE)
                .and_then(|table| declared_days(table.retention_days)),
            kept_class: None,
            kind: TargetKind::Rows {
                column: usage.column("occurred_at").to_string(),
                evidence_state_key: None,
                only_when: None,
                guard: RowGuard::Unguarded,
                monthly_partitions: false,
            },
            default_retention: None,
            capped: true,
            periodic: false,
        });
    }
    // Every other native table whose rows have an end follows its declared
    // retention by default.
    let manifest = crate::runtime::native_catalog::native_manifest();
    let mut taken: std::collections::BTreeSet<String> =
        targets.iter().map(Target::qualified).collect();
    for table in &manifest.tables {
        let Ok(column) = row_end_column(table, manifest) else {
            continue;
        };
        let target = Target {
            schema: table.schema.clone(),
            table: table.table.clone(),
            service: native_service(table),
            declared: declared_days(table.retention_days),
            kept_class: None,
            kind: TargetKind::Rows {
                column,
                evidence_state_key: None,
                only_when: None,
                guard: RowGuard::Unguarded,
                monthly_partitions: false,
            },
            default_retention: declared_days(table.retention_days),
            capped: false,
            periodic: true,
        };
        if taken.insert(target.qualified()) {
            targets.push(target);
        }
    }
    targets.extend(system_targets(DEFAULT_KEEP_VERSIONS));
    targets
}

/// The native service whose store holds a UDB table (`udb.core.<service>...`).
fn native_service(table: &ManifestTable) -> String {
    table
        .proto_package
        .split('.')
        .nth(2)
        .unwrap_or_default()
        .to_string()
}

/// The column a native table's rows age from, or why the table's rows do not
/// age out. Only tables whose rows have an end qualify: a row expires, records
/// an event, or was revoked, consumed, acknowledged, delivered or finished
/// ([`ROW_END_COLUMNS`]); a row whose end column is empty is still live and is
/// never deleted. A creation time alone never qualifies (an entity created
/// long ago is not stale) except for append-only records.
pub(crate) fn row_end_column(
    table: &ManifestTable,
    manifest: &crate::generation::CatalogManifest,
) -> Result<String, &'static str> {
    if table.retention_days <= 0 {
        return Err("declares no retention");
    }
    if partitioned_target(table).is_some() {
        return Err("time-partitioned: whole partitions are dropped instead");
    }
    let timestamp = |name: &str| {
        table.columns.iter().any(|column| {
            column.column_name == name && column.sql_type.to_ascii_uppercase().contains("TIMESTAMP")
        })
    };
    if table.soft_delete || timestamp("deleted_at") {
        return Err("a soft-deleted entity: its rows end by deletion, not by age");
    }
    if NO_ROW_EXPIRY.contains(&table.table.as_str()) {
        return Err("its rows track live external state");
    }
    let referenced = manifest.tables.iter().any(|other| {
        !(other.schema == table.schema && other.table == table.table)
            && other.foreign_keys.iter().any(|fk| {
                fk.ref_table == table.table
                    && (fk.ref_schema.is_empty() || fk.ref_schema == table.schema)
            })
    });
    if referenced {
        return Err("other tables reference its rows");
    }
    if let Some(column) = ROW_END_COLUMNS.iter().find(|column| timestamp(**column)) {
        return Ok((*column).to_string());
    }
    if APPEND_ONLY_RECORDS.contains(&table.table.as_str()) && timestamp("created_at") {
        return Ok("created_at".to_string());
    }
    Err("no column marks the end of a row's life")
}

/// UDB's bookkeeping tables in the system catalog, each with the rows it may
/// lose and its default retention. They are swept once per sweep interval.
fn system_targets(keep_versions: u32) -> Vec<Target> {
    let config = crate::runtime::system::SystemCatalogConfig::current();
    let schema = config.cdc.system_schema.clone();
    let days = |n: u64| Some(Duration::from_secs(n * 86_400));
    let target = |table: &str, column: &str, only_when: String, default: Option<Duration>| Target {
        schema: schema.clone(),
        table: table.to_string(),
        service: SYSTEM_STORE.to_string(),
        declared: None,
        kept_class: None,
        kind: TargetKind::Rows {
            column: column.to_string(),
            evidence_state_key: None,
            only_when: Some(only_when),
            guard: RowGuard::Unguarded,
            monthly_partitions: false,
        },
        default_retention: default,
        capped: false,
        periodic: true,
    };
    let bindings = config.project_catalog_bindings_relation();
    let versions = config.catalog_versions_relation();
    let reloads = config.catalog_reload_log_relation();
    // A run a reviewed catalog transition names is that transition's record.
    let mut migration_runs = target(
        &config.migration_runs_table,
        "started_at",
        migration_runs_removable(&config.migration_runs_relation()),
        days(90),
    );
    if let TargetKind::Rows { guard, .. } = &mut migration_runs.kind {
        *guard = RowGuard::ReferencedBy(
            format!(
                "{}.{}",
                qi(&schema),
                qi(crate::runtime::core::REVIEWED_TRANSITIONS_TABLE)
            ),
            "run_id".to_string(),
        );
    }
    // The admin audit log is pruned in chain order, never past its head, and
    // every prune records the anchor verification starts from.
    let mut admin_audit = target(
        &config.admin_audit_log_table,
        "created_at",
        String::new(),
        days(365),
    );
    if let TargetKind::Rows {
        only_when, guard, ..
    } = &mut admin_audit.kind
    {
        *only_when = None;
        *guard = RowGuard::AdminAuditChain;
    }
    vec![
        // A completed task only clears read fences of writes that recent.
        target(
            &config.projection_tasks_table,
            "completed_at",
            "status = 'COMPLETED'".to_string(),
            days(7),
        ),
        // Open, retrying and quarantined dead letters stay until resolved.
        target(
            &config.dlq_table,
            "updated_at",
            "status IN ('REPLAYED', 'DISMISSED')".to_string(),
            days(30),
        ),
        target(
            &config.saga_table,
            "updated_at",
            "status IN ('committed', 'compensated')".to_string(),
            days(30),
        ),
        // An in-doubt decision is what recovery resolves: it stays.
        target(
            &config.xa_ledger_table,
            "updated_at",
            "decision IN ('committed', 'rolled_back')".to_string(),
            days(30),
        ),
        target(
            &config.catalog_versions_table,
            "created_at",
            catalog_versions_removable(&versions, &bindings, keep_versions),
            days(30),
        ),
        target(
            &config.catalog_reload_log_table,
            "created_at",
            catalog_reloads_removable(&reloads, &bindings),
            days(90),
        ),
        migration_runs,
        admin_audit,
    ]
}

/// A migration run may go once it completed or was only a dry run, unless it
/// is its project's newest completed run (the record of the schema the
/// project runs now). A run that failed, waits for approval or is applying
/// stays, and so does a run a reviewed catalog transition names.
fn migration_runs_removable(runs: &str) -> String {
    format!(
        "state IN ('COMPLETED', 'DRY_RUN') \
         AND run_id NOT IN (SELECT DISTINCT ON (m.project_id) m.run_id FROM {runs} m \
                            WHERE m.state = 'COMPLETED' \
                            ORDER BY m.project_id, m.started_at DESC, m.run_id DESC)"
    )
}

/// One page of the admin audit log's prune, as one statement: the oldest
/// `batch` rows past retention that come before the chain's head (the newest
/// hashed row, which the next append links to) are deleted, in chain order
/// (`created_at`, `audit_id`), so what goes is always a contiguous prefix of
/// the chain. The newest of them becomes the prune anchor in
/// `udb_system.evidence_export_state` under `$1`
/// ([`admin_audit_prune_anchor_key`]): its id, time and hash, with the rows
/// pruned so far counted in `exported_count` and the time in `updated_at`.
/// The verifier starts the chain from that anchor. Returns the rows deleted.
fn admin_audit_prune_sql(log: &str, column: &str, retention: Duration, batch: i64) -> String {
    let column = qi(column);
    let seconds = retention.as_secs();
    format!(
        "WITH head AS ( \
             SELECT h.{column} AS at, h.audit_id FROM {log} h WHERE h.current_hash <> '' \
             ORDER BY h.{column} DESC, h.audit_id DESC LIMIT 1), \
         doomed AS ( \
             SELECT l.audit_id FROM {log} l, head \
             WHERE l.{column} < now() - interval '{seconds} seconds' \
               AND (l.{column}, l.audit_id) < (head.at, head.audit_id) \
             ORDER BY l.{column}, l.audit_id LIMIT {batch}), \
         gone AS ( \
             DELETE FROM {log} d USING doomed WHERE d.audit_id = doomed.audit_id \
             RETURNING d.audit_id, d.{column} AS at, d.current_hash), \
         newest AS ( \
             SELECT g.audit_id, g.at, g.current_hash FROM gone g \
             ORDER BY g.at DESC, g.audit_id DESC LIMIT 1), \
         anchored AS ( \
             INSERT INTO {EVIDENCE_STATE} AS s \
                 (worker, last_event_id, last_occurred_at, chain_head, exported_count, updated_at) \
             SELECT $1, n.audit_id::text, n.at, n.current_hash, (SELECT count(*) FROM gone), NOW() \
             FROM newest n \
             ON CONFLICT (worker) DO UPDATE SET \
                 last_event_id = EXCLUDED.last_event_id, \
                 last_occurred_at = EXCLUDED.last_occurred_at, \
                 chain_head = EXCLUDED.chain_head, \
                 exported_count = s.exported_count + EXCLUDED.exported_count, \
                 updated_at = NOW() \
             RETURNING 1) \
         SELECT count(*) FROM gone"
    )
}

/// Prune the admin audit log past `retention`, page by page
/// ([`admin_audit_prune_sql`]). Each page is its own transaction holding the
/// chain's advisory lock, so no append runs while the head and the anchor
/// move. A scheduled pass takes at most [`MAX_BATCHES_PER_PASS`] pages; a
/// drain takes the whole backlog.
async fn prune_admin_audit_chain(
    pool: &PgPool,
    target: &Target,
    column: &str,
    retention: Duration,
    mode: PassMode,
) -> Result<i64, String> {
    let relation = target.relation();
    ensure_rows_reachable(pool, &relation).await?;
    crate::runtime::evidence_export::ensure_state_table(pool).await?;
    let batch = mode.batch.max(1);
    let sql = admin_audit_prune_sql(&relation, column, retention, batch);
    let key =
        crate::runtime::canonical_store::system_store::admin_audit_prune_anchor_key(&relation);
    let mut deleted = 0i64;
    let mut pages = 0usize;
    loop {
        let mut tx = admin_tx(pool).await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(crate::runtime::canonical_store::system_store::ADMIN_AUDIT_CHAIN_LOCK_KEY)
            .execute(&mut *tx)
            .await
            .map_err(|err| format!("lock the admin audit chain: {err}"))?;
        let rows: i64 = sqlx::query_scalar(&sql)
            .bind(&key)
            .fetch_one(&mut *tx)
            .await
            .map_err(|err| format!("prune the admin audit chain: {err}"))?;
        tx.commit().await.map_err(|err| err.to_string())?;
        deleted += rows;
        pages += 1;
        if rows < batch || (!mode.drain && pages >= MAX_BATCHES_PER_PASS) {
            break;
        }
    }
    if deleted > 0 {
        tracing::info!(table = %target.qualified(), deleted, retention_secs = retention.as_secs(), "pruned the admin audit chain past retention");
    }
    Ok(deleted)
}

/// A catalog version may go only when it is not active, no project binds it
/// and it is not among its project's `keep` newest versions.
fn catalog_versions_removable(versions: &str, bindings: &str, keep: u32) -> String {
    format!(
        "status <> 'ACTIVE' \
         AND catalog_id NOT IN (SELECT b.active_catalog_id FROM {bindings} b \
                                WHERE b.active_catalog_id IS NOT NULL) \
         AND catalog_id NOT IN (SELECT ranked.catalog_id FROM ( \
               SELECT v.catalog_id, row_number() OVER ( \
                      PARTITION BY v.project_id ORDER BY v.created_at DESC) AS position \
               FROM {versions} v) AS ranked \
             WHERE ranked.position <= {keep})"
    )
}

/// A reload-log row may go unless it records a project's active version (the
/// catalog loader requires that activation record) or is the newest row (its
/// sequence is the cluster's reload generation).
fn catalog_reloads_removable(reloads: &str, bindings: &str) -> String {
    format!(
        "(project_id, catalog_id) NOT IN (SELECT b.project_id, b.active_catalog_id \
                                          FROM {bindings} b \
                                          WHERE b.active_catalog_id IS NOT NULL) \
         AND reload_seq < (SELECT max(r.reload_seq) FROM {reloads} r)"
    )
}

fn partitioned_target(table: &ManifestTable) -> Option<Target> {
    if table.partition_column.trim().is_empty() || table.partition_default {
        return None;
    }
    let strategy = table.partition_strategy.to_ascii_uppercase();
    let interval_name = if !table.partition_interval.trim().is_empty() {
        table.partition_interval.trim().to_ascii_uppercase()
    } else if strategy.contains("RANGE_YEAR") {
        "YEAR".to_string()
    } else if strategy.contains("RANGE_MONTH") {
        "MONTH".to_string()
    } else if strategy.contains("RANGE_WEEK") {
        "WEEK".to_string()
    } else if strategy.contains("RANGE_DAY") {
        "DAY".to_string()
    } else if strategy.contains("RANGE_HOUR") {
        "HOUR".to_string()
    } else {
        return None;
    };
    let (unit, suffix_format) = match interval_name.trim_start_matches("1 ") {
        "YEARLY" | "YEAR" => ("year", "YYYY"),
        "MONTHLY" | "MONTH" => ("month", "YYYYMM"),
        "WEEKLY" | "WEEK" => ("week", "YYYYMMDD"),
        "DAILY" | "DAY" => ("day", "YYYYMMDD"),
        "HOURLY" | "HOUR" => ("hour", "YYYYMMDDHH24"),
        _ => return None,
    };
    // The native service whose store holds a UDB table; app tables are
    // reached through the primary pool instead.
    let service = native_service(table);
    Some(Target {
        schema: table.schema.clone(),
        table: table.table.clone(),
        service,
        declared: declared_days(table.retention_days),
        kept_class: None,
        default_retention: None,
        capped: true,
        periodic: false,
        kind: TargetKind::Partitioned {
            column: table.partition_column.trim().to_string(),
            unit: unit.to_string(),
            interval: format!("1 {unit}"),
            suffix_format: suffix_format.to_string(),
            premake: if table.partition_premake > 0 {
                table.partition_premake
            } else {
                DEFAULT_PREMAKE
            },
        },
    })
}

/// Parse `UDB_CLEANUP_RETENTION` against the known targets.
pub(crate) fn parse_retention(raw: &str, targets: &[Target]) -> Result<RetentionConfig, String> {
    let mut config = RetentionConfig::default();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let Some((name, value)) = entry.split_once('=') else {
            return Err(format!(
                "{CLEANUP_RETENTION_ENV}: `{entry}` is not `table=duration` (e.g. access_decision_audits=90d)"
            ));
        };
        let name = name.trim().to_ascii_lowercase();
        let (name, class) = match name.split_once(':') {
            Some((table, class)) => (table.trim().to_string(), Some(class.trim().to_string())),
            None => (name, None),
        };
        if name.ends_with(CDC_JOURNAL_TABLE) {
            return Err(format!(
                "{CLEANUP_RETENTION_ENV}: the CDC journal has its own sweep; set UDB_CDC_JOURNAL_RETENTION"
            ));
        }
        let matches: Vec<&Target> = targets
            .iter()
            .filter(|target| target.qualified() == name || target.table == name)
            .collect();
        let target = match matches.as_slice() {
            [target] => *target,
            [] => {
                let known: Vec<String> = targets.iter().map(Target::qualified).collect();
                return Err(format!(
                    "{CLEANUP_RETENTION_ENV}: `{name}` is not a table this worker keeps; known: {}",
                    known.join(", ")
                ));
            }
            _ => {
                return Err(format!(
                    "{CLEANUP_RETENTION_ENV}: `{name}` names more than one table; write it as schema.table"
                ));
            }
        };
        let retention = if value.trim().eq_ignore_ascii_case("declared") {
            match target.declared {
                Some(declared) => Some(declared),
                None => {
                    return Err(format!(
                        "{CLEANUP_RETENTION_ENV}: `{entry}`: {} declares no retention; give a duration",
                        target.qualified()
                    ));
                }
            }
        } else {
            parse_retention_value(value.trim())
                .map_err(|why| format!("{CLEANUP_RETENTION_ENV}: `{entry}`: {why}"))?
        };
        let key = match class {
            None => target.qualified(),
            Some(class) => match &target.kept_class {
                Some(kept) if kept.name == class => class_key(target, kept),
                Some(kept) => {
                    return Err(format!(
                        "{CLEANUP_RETENTION_ENV}: `{entry}`: {} keeps only the row class `{}`",
                        target.qualified(),
                        kept.name
                    ));
                }
                None => {
                    return Err(format!(
                        "{CLEANUP_RETENTION_ENV}: `{entry}`: {} has no row class to keep longer",
                        target.qualified()
                    ));
                }
            },
        };
        config.retention.insert(key, retention);
    }
    Ok(config)
}

/// Parse a retention list entry by entry: a malformed entry pauses the table
/// it names (if any) and is kept as an error; the others apply.
pub(crate) fn parse_retention_entries(raw: &str, targets: &[Target]) -> RetentionConfig {
    let mut config = RetentionConfig::default();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        match parse_retention(entry, targets) {
            Ok(parsed) => config.retention.extend(parsed.retention),
            Err(why) => {
                let name = entry
                    .split('=')
                    .next()
                    .unwrap_or_default()
                    .split(':')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase();
                let named: Vec<&Target> = targets
                    .iter()
                    .filter(|target| target.qualified() == name || target.table == name)
                    .collect();
                if let [target] = named.as_slice() {
                    config.paused.insert(target.qualified(), why.clone());
                }
                config.errors.push(why);
            }
        }
    }
    config
}

/// `90d`, `36h`, `keep` (or `0`) → retention; below one day is refused.
/// (`declared` is resolved by the caller.)
fn parse_retention_value(value: &str) -> Result<Option<Duration>, String> {
    let value = value.to_ascii_lowercase();
    if value == "keep" || value == "0" {
        return Ok(None);
    }
    let split = value
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| "give a unit: d (days) or h (hours)".to_string())?;
    let (number, unit) = value.split_at(split);
    let number: u64 = number
        .parse()
        .map_err(|_| "expected a number and a unit (d or h), e.g. 30d".to_string())?;
    let seconds = match unit {
        "d" => number.checked_mul(86_400),
        "h" => number.checked_mul(3_600),
        _ => return Err(format!("unit `{unit}` is not d (days) or h (hours)")),
    }
    .ok_or_else(|| "too large".to_string())?;
    let retention = Duration::from_secs(seconds);
    if retention < MIN_RETENTION {
        return Err("the shortest retention is 1d".to_string());
    }
    Ok(Some(retention))
}

/// One scheduled pass over every target whose store this broker can reach,
/// with the configured reclaim. Returns the partitions created and dropped
/// plus the rows deleted.
pub async fn run_cleanup_once(
    runtime: &crate::runtime::DataBrokerRuntime,
    settings: &CleanupSettings,
) -> Result<i64, String> {
    let reclaim = settings.reclaim.clone();
    run_cleanup_with(
        runtime,
        settings,
        reclaim.clone().unwrap_or(Reclaim::Off),
        false,
        false,
    )
    .await
    .and_then(|changed| reclaim.map(|_| changed))
}

/// Partition upkeep for every application catalogue's time-partitioned
/// PostgreSQL tables (UDB's own are [`run_cleanup_once`]'s): the current
/// period and the declared number of future partitions are created ahead of
/// writes, as the migration names them, so a write past the window the
/// migration created never fails. A table pg_partman manages is left to it.
/// With `UDB_CLEANUP_APP_RETENTION=declared`, UDB-named partitions past the
/// table's declared `retention_days` are dropped; by default nothing is.
pub async fn run_app_partition_upkeep(
    runtime: &crate::runtime::DataBrokerRuntime,
    catalog: &crate::runtime::catalog::CatalogManager,
    settings: &CleanupSettings,
) -> Result<i64, String> {
    let Some(pool) = runtime.pg_pool_clone() else {
        return Ok(0);
    };
    let manifests: Vec<_> = catalog
        .active_project_ids()
        .into_iter()
        .filter_map(|project| catalog.active_exact_for(&project))
        .collect();
    let mut changed = 0i64;
    let mut failures = Vec::new();
    for (target, retention) in app_partition_targets(
        manifests.iter().map(|state| state.manifest.as_ref()),
        settings.app_retention,
    ) {
        let mode = PassMode {
            batch: settings.batch,
            drain: false,
            // App rows are never deleted one by one: whole partitions only.
            sweep_partitions: false,
        };
        match maintain_target(&pool, &target, Ok(retention), mode).await {
            Ok(outcome) => changed += outcome.total(),
            Err(why) => failures.push(format!("{}: {why}", target.qualified())),
        }
    }
    if failures.is_empty() {
        Ok(changed)
    } else {
        Err(format!("app partition upkeep: {}", failures.join("; ")))
    }
}

/// The time-partitioned app tables of `manifests`, once each, with the
/// retention the upkeep applies: none (partitions are only created) unless
/// `app_retention`, then the table's declared `retention_days`. UDB's own
/// schemas are left to the native pass.
pub(crate) fn app_partition_targets<'a>(
    manifests: impl Iterator<Item = &'a crate::generation::CatalogManifest>,
    app_retention: bool,
) -> Vec<(Target, Retention)> {
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    for manifest in manifests {
        for table in &manifest.tables {
            if table.schema.starts_with("udb") {
                continue;
            }
            let Some(target) = partitioned_target(table) else {
                continue;
            };
            if !seen.insert(target.qualified()) {
                continue;
            }
            let retention = if app_retention {
                let declared = declared_days(table.retention_days);
                Retention {
                    rows: declared,
                    kept: declared,
                }
            } else {
                Retention::default()
            };
            out.push((target, retention));
        }
    }
    out
}

/// Per table, when the scheduled pass last swept it (kept partitions, or a
/// periodic table's rows).
fn last_sweep() -> &'static std::sync::Mutex<std::collections::HashMap<String, std::time::Instant>>
{
    static LAST: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, std::time::Instant>>,
    > = std::sync::OnceLock::new();
    LAST.get_or_init(Default::default)
}

/// Whether the scheduled pass sweeps this table now (at most once per
/// `every`, the sweep interval).
fn sweep_due(target: &Target, every: Duration) -> bool {
    let mut last = last_sweep()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let now = std::time::Instant::now();
    match last.get(&target.qualified()) {
        Some(at) if now.duration_since(*at) < every => false,
        _ => {
            last.insert(target.qualified(), now);
            true
        }
    }
}

/// One pass with an explicit reclaim mode. `drain` (`udb cleanup run`) takes
/// the whole backlog: no per-pass cap, kept partitions are always swept.
/// `reclaim_all` (`udb cleanup run --reclaim`) reclaims every log table plus
/// the CDC journal, deleted rows or not; otherwise only tables that lost rows
/// are reclaimed.
pub async fn run_cleanup_with(
    runtime: &crate::runtime::DataBrokerRuntime,
    settings: &CleanupSettings,
    reclaim_mode: Reclaim,
    drain: bool,
    reclaim_all: bool,
) -> Result<i64, String> {
    let config = &settings.retention;
    let mut changed = 0i64;
    let mut failures = config.errors.clone();
    for target in &settings.targets {
        let Some(pool) = store_pool(runtime, target) else {
            continue;
        };
        let retention = match config.paused.get(&target.qualified()) {
            Some(why) => Err(why.clone()),
            None => Ok(config.retention_of(target)),
        };
        if target.periodic && config.retention_of(target).rows.is_none() {
            // Kept for good: a periodic table has nothing else to do.
            continue;
        }
        let partitioned = matches!(target.kind, TargetKind::Partitioned { .. });
        let swept = (partitioned || target.periodic)
            && (drain || sweep_due(target, settings.sweep_interval));
        if target.periodic && !swept {
            // Between sweeps a periodic table is left alone.
            continue;
        }
        let mode = PassMode {
            batch: settings.batch,
            // A periodic table is swept whole, page range by page range.
            drain: drain || target.periodic,
            sweep_partitions: partitioned && swept,
        };
        match maintain_target(&pool, target, retention, mode).await {
            Ok(outcome) => {
                changed += outcome.total();
                // A dropped partition already gave its space back; deleted
                // rows leave free space in the table file.
                if (outcome.deleted > 0 || reclaim_all)
                    && let Err(why) =
                        reclaim(&pool, &target.relation(), reclaim_mode, reclaim_all).await
                {
                    failures.push(format!("{}: {why}", target.qualified()));
                }
            }
            Err(why) => failures.push(format!("{}: {why}", target.qualified())),
        }
    }
    // `udb cleanup run` also runs the CDC journal's retention sweep (the
    // running broker's CDC worker does it on its own tick), so the journal can
    // be trimmed in a maintenance window without a broker.
    if drain && let Some(pool) = runtime.pg_pool_clone() {
        match crate::runtime::cdc::sweep_journal_now(
            &pool,
            &runtime.config().cdc,
            statement_timeout(),
        )
        .await
        {
            Ok(removed) => {
                if removed > 0 {
                    tracing::info!(removed, "cleanup run trimmed the CDC journal");
                }
                changed += removed as i64;
            }
            Err(why) => failures.push(format!("CDC journal: {why}")),
        }
    }
    // An explicit reclaim gives the space the journal sweep freed back too.
    if reclaim_all && let Some(pool) = runtime.pg_pool_clone() {
        let journal = crate::runtime::system::SystemCatalogConfig::current().cdc_journal_relation();
        if let Err(why) = reclaim(&pool, &journal, reclaim_mode, true).await {
            failures.push(format!("{journal}: {why}"));
        }
    }
    match &settings.max_size {
        Ok(Some(max)) => match enforce_size_cap(runtime, settings, *max, reclaim_mode).await {
            Ok(deleted) => changed += deleted,
            Err(why) => failures.push(why),
        },
        Ok(None) => {}
        Err(why) => failures.push(why.clone()),
    }
    if failures.is_empty() {
        Ok(changed)
    } else {
        Err(format!(
            "cleanup ({changed} change(s) made): {}",
            failures.join("; ")
        ))
    }
}

/// The pool of the store that holds `target`: the primary pool for the system
/// catalog, else the native service's store. `None` when this broker cannot
/// reach it.
fn store_pool(runtime: &crate::runtime::DataBrokerRuntime, target: &Target) -> Option<PgPool> {
    if target.service == SYSTEM_STORE {
        runtime.pg_pool_clone()
    } else {
        runtime
            .native_store_pool_for_service(&target.service, true, "")
            .ok()
    }
}

/// `udb cleanup partition-audit-log`: convert the durable auth audit log
/// (`udb_system.auth_audit_log`) from one plain table into monthly
/// partitions, online and without moving a row (see [`audit_log`]). A log
/// that is already partitioned is left as it is. Returns what was done; a
/// refusal or a busy table is an error that names why, with nothing changed.
pub async fn partition_auth_audit_log(
    runtime: &crate::runtime::DataBrokerRuntime,
) -> Result<serde_json::Value, String> {
    let target = targets(false)
        .into_iter()
        .find(|target| {
            matches!(
                target.kind,
                TargetKind::Rows {
                    monthly_partitions: true,
                    ..
                }
            )
        })
        .ok_or_else(|| "the auth audit log is not among the cleanup targets".to_string())?;
    let pool = store_pool(runtime, &target).ok_or_else(|| {
        format!(
            "{}: this process cannot reach the store that holds it",
            target.qualified()
        )
    })?;
    let done = audit_log::convert_online(&pool, &target.schema, &target.table).await?;
    Ok(done.to_json())
}

/// Give the free space of `relation` back the way `mode` says. `force`
/// (an explicit `udb cleanup run`) compacts any table worth copying, not
/// only a bloated one.
async fn reclaim(pool: &PgPool, relation: &str, mode: Reclaim, force: bool) -> Result<(), String> {
    match mode {
        Reclaim::Off => Ok(()),
        Reclaim::Vacuum => reclaim_space(pool, relation, "VACUUM (ANALYZE)").await,
        Reclaim::Full => reclaim_space(pool, relation, "VACUUM (FULL, ANALYZE)").await,
        Reclaim::Online => {
            // The plain VACUUM first: it makes the space reusable at once and
            // refreshes the statistics the bloat estimate reads.
            reclaim_space(pool, relation, "VACUUM (ANALYZE)").await?;
            compact::compact_online(pool, relation, force)
                .await
                .map(|_| ())
        }
    }
}

/// Run `vacuum` on a relation that exists. VACUUM cannot run in a
/// transaction, so it goes over the simple query protocol. On a partitioned
/// table it covers every partition.
async fn reclaim_space(pool: &PgPool, relation: &str, vacuum: &str) -> Result<(), String> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(relation)
        .fetch_one(pool)
        .await
        .map_err(|err| format!("look up the table: {err}"))?;
    if !exists {
        return Ok(());
    }
    // VACUUM runs outside a transaction, so the timeout is set on the
    // session it uses. The pool gives every connection its own timeout when it
    // opens (30 s), which a DSN or role setting cannot lift; cleanup lifts it
    // for this statement and puts the pool's value back afterwards. `RESET`
    // would not: it returns to the connection's startup value, not the pool's.
    let mut conn = pool.acquire().await.map_err(|err| err.to_string())?;
    let previous: String = sqlx::query_scalar("SELECT current_setting('statement_timeout')")
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| format!("read statement_timeout: {err}"))?;
    sqlx::query("SELECT set_config('statement_timeout', $1, false)")
        .bind(statement_timeout())
        .execute(&mut *conn)
        .await
        .map_err(|err| format!("set statement_timeout: {err}"))?;
    let sql = format!("{vacuum} {relation}");
    let result = sqlx::Executor::execute(&mut *conn, sqlx::raw_sql(&sql))
        .await
        .map(|_| {
            tracing::info!(table = %relation, statement = vacuum, "cleanup reclaimed space");
        })
        .map_err(|err| format!("{vacuum}: {err}"));
    let restored = sqlx::query("SELECT set_config('statement_timeout', $1, false)")
        .bind(&previous)
        .execute(&mut *conn)
        .await;
    if restored.is_err() {
        // Never hand the pool a connection with cleanup's timeout on it.
        conn.close_on_drop();
    }
    result
}

/// Upkeep for one table. `retention` is `Err` when the retention list is
/// malformed: partitions are still kept ahead, nothing is removed.
pub(crate) async fn maintain_target(
    pool: &PgPool,
    target: &Target,
    retention: Result<Retention, String>,
    mode: PassMode,
) -> Result<Outcome, String> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(target.relation())
        .fetch_one(pool)
        .await
        .map_err(|err| format!("look up the table: {err}"))?;
    if !exists {
        return Ok(Outcome::default());
    }
    let upkeep = upkeep_target(pool, target, retention.unwrap_or_default(), mode).await;
    // Every pass, whatever the upkeep did: a partition pg_partman (or anyone)
    // created without the parent's row-level security is brought in line.
    let secured = secure_partitions(pool, target).await;
    match (upkeep, secured) {
        (Ok(mut outcome), Ok(secured)) => {
            outcome.secured = secured;
            Ok(outcome)
        }
        (Err(why), Ok(_)) | (Ok(_), Err(why)) => Err(why),
        (Err(upkeep), Err(secured)) => Err(format!("{upkeep}; {secured}")),
    }
}

async fn upkeep_target(
    pool: &PgPool,
    target: &Target,
    retention: Retention,
    mode: PassMode,
) -> Result<Outcome, String> {
    match &target.kind {
        TargetKind::Partitioned {
            column,
            unit,
            interval,
            suffix_format,
            premake,
        } => {
            let partitioned: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass($1))",
            )
            .bind(target.relation())
            .fetch_one(pool)
            .await
            .map_err(|err| format!("read the partitioning: {err}"))?;
            if !partitioned {
                return Ok(Outcome::default());
            }
            let mut outcome = Outcome::default();
            if partman_manages(pool, target).await? {
                // pg_partman drops whole partitions; rows past retention in
                // the partitions it keeps are still swept below.
                if let Some(kept) = retention.kept {
                    set_partman_retention(pool, target, kept).await?;
                }
            } else {
                outcome.created =
                    create_partitions_ahead(pool, target, unit, interval, suffix_format, *premake)
                        .await?;
                // A partition goes whole once even its longest-kept rows expired.
                if let Some(kept) = retention.kept {
                    outcome.dropped = drop_expired_partitions(pool, target, kept, None).await?;
                }
            }
            if mode.sweep_partitions
                && let Some(rows) = retention.rows
            {
                let expiry = Expiry {
                    column,
                    rows,
                    kept: target
                        .kept_class
                        .as_ref()
                        .map(|class| (class, retention.kept)),
                    evidence_state_key: None,
                    only_when: None,
                };
                for child in partitions_with_expired_rows(pool, target, rows).await? {
                    outcome.deleted += sweep_expired_rows(pool, &child, &expiry).await?;
                }
                if outcome.deleted > 0 {
                    tracing::info!(table = %target.qualified(), deleted = outcome.deleted, retention_secs = rows.as_secs(), "deleted rows past retention from kept partitions");
                }
            }
            Ok(outcome)
        }
        TargetKind::Rows {
            column,
            evidence_state_key,
            only_when,
            guard,
            monthly_partitions,
        } => {
            let mut outcome = Outcome::default();
            // A log converted to monthly partitions: months ahead, and whole
            // months past retention dropped (only UDB-named ones, never the
            // default partition that holds the rows from before the
            // conversion). Rows past retention in the partitions that stay
            // are deleted below, leaf by leaf.
            if *monthly_partitions
                && audit_log::is_partitioned(pool, &target.relation()).await?
                && !partman_manages(pool, target).await?
            {
                outcome.created = audit_log::create_months_ahead(
                    pool,
                    &target.schema,
                    &target.table,
                    DEFAULT_PREMAKE,
                )
                .await?;
                if let Some(kept) = retention.kept {
                    outcome.dropped =
                        drop_expired_partitions(pool, target, kept, evidence_state_key.as_deref())
                            .await?;
                }
            }
            let Some(rows) = retention.rows else {
                return Ok(outcome);
            };
            let only_when = match guard {
                RowGuard::AdminAuditChain => {
                    outcome.deleted =
                        prune_admin_audit_chain(pool, target, column, rows, mode).await?;
                    return Ok(outcome);
                }
                RowGuard::ReferencedBy(relation, key) => {
                    let present: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                        .bind(relation.as_str())
                        .fetch_one(pool)
                        .await
                        .map_err(|err| format!("look up {relation}: {err}"))?;
                    unreferenced(
                        only_when.as_deref(),
                        present.then_some((relation.as_str(), key.as_str())),
                    )
                }
                RowGuard::Unguarded => only_when.clone(),
            };
            let expiry = Expiry {
                column,
                rows,
                kept: target
                    .kept_class
                    .as_ref()
                    .map(|class| (class, retention.kept)),
                evidence_state_key: evidence_state_key.as_deref(),
                only_when: only_when.as_deref(),
            };
            outcome.deleted = delete_expired_rows(pool, target, &expiry, mode).await?;
            Ok(outcome)
        }
    }
}

/// `only_when` narrowed to the rows `reference` (`relation`, `column`) does
/// not point at; `None` when the referencing table does not exist.
fn unreferenced(only_when: Option<&str>, reference: Option<(&str, &str)>) -> Option<String> {
    let Some((relation, key)) = reference else {
        return only_when.map(str::to_string);
    };
    let key = qi(key);
    let unreferenced =
        format!("{key} NOT IN (SELECT r.{key} FROM {relation} r WHERE r.{key} IS NOT NULL)");
    Some(match only_when {
        Some(condition) => format!("({condition}) AND {unreferenced}"),
        None => unreferenced,
    })
}

async fn partman_manages(pool: &PgPool, target: &Target) -> Result<bool, String> {
    let installed: bool =
        sqlx::query_scalar("SELECT to_regclass('partman.part_config') IS NOT NULL")
            .fetch_one(pool)
            .await
            .map_err(|err| format!("look up pg_partman: {err}"))?;
    if !installed {
        return Ok(false);
    }
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM partman.part_config WHERE parent_table = $1)")
        .bind(target.qualified())
        .fetch_one(pool)
        .await
        .map_err(|err| format!("read partman.part_config: {err}"))
}

async fn set_partman_retention(
    pool: &PgPool,
    target: &Target,
    retention: Duration,
) -> Result<(), String> {
    sqlx::query(
        "UPDATE partman.part_config SET retention = $2, retention_keep_table = false \
         WHERE parent_table = $1 AND retention IS DISTINCT FROM $2",
    )
    .bind(target.qualified())
    .bind(format!("{} seconds", retention.as_secs()))
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|err| format!("set the pg_partman retention: {err}"))
}

/// Create the current partition and `premake` future ones, named as the
/// migration names them. A period an existing partition already covers is
/// skipped.
async fn create_partitions_ahead(
    pool: &PgPool,
    target: &Target,
    unit: &str,
    interval: &str,
    suffix_format: &str,
    premake: i32,
) -> Result<i64, String> {
    let rows = sqlx::query(
        "SELECT g::text AS start_ts, (g + $2::interval)::text AS end_ts, \
                to_char(g, $3) AS suffix \
         FROM generate_series(date_trunc($1, now()), \
                              date_trunc($1, now()) + ($4::int * $2::interval), \
                              $2::interval) AS g",
    )
    .bind(unit)
    .bind(interval)
    .bind(suffix_format)
    .bind(premake)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("plan partitions: {err}"))?;
    let mut created = 0i64;
    let mut parent_rls: Option<RlsState> = None;
    for row in rows {
        let start: String = row.try_get("start_ts").map_err(|err| err.to_string())?;
        let end: String = row.try_get("end_ts").map_err(|err| err.to_string())?;
        let suffix: String = row.try_get("suffix").map_err(|err| err.to_string())?;
        let child = format!("{}_p_{suffix}", target.table);
        let present: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(format!("{}.{}", qi(&target.schema), qi(&child)))
            .fetch_one(pool)
            .await
            .map_err(|err| format!("look up partition {child}: {err}"))?;
        if present {
            continue;
        }
        let child_relation = format!("{}.{}", qi(&target.schema), qi(&child));
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {child_relation} PARTITION OF {parent} \
             FOR VALUES FROM ({start}) TO ({end})",
            parent = target.relation(),
            start = ql(&start),
            end = ql(&end),
        );
        // The partition is created with the parent's row-level security in one
        // transaction: it is never visible without it.
        if parent_rls.is_none() {
            parent_rls = Some(parent_rls_state(pool, target).await?);
        }
        let secure = rls_sync_statements(
            &child_relation,
            parent_rls.as_ref().expect("read above"),
            &RlsState::default(),
        );
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| format!("create partition {child}: {err}"))?;
        match sqlx::query(&ddl).execute(&mut *tx).await {
            Ok(_) => {}
            // Another partition already covers the period (42P17), or a racing
            // creator won (42P07): nothing to do.
            Err(sqlx::Error::Database(db))
                if matches!(db.code().as_deref(), Some("42P17" | "42P07")) =>
            {
                continue;
            }
            Err(err) => return Err(format!("create partition {child}: {err}")),
        }
        for statement in &secure {
            sqlx::query(statement)
                .execute(&mut *tx)
                .await
                .map_err(|err| format!("secure partition {child}: {statement}: {err}"))?;
        }
        tx.commit()
            .await
            .map_err(|err| format!("create partition {child}: {err}"))?;
        created += 1;
        tracing::info!(table = %target.qualified(), partition = %child, rls_statements = secure.len(), "created partition ahead of writes");
    }
    Ok(created)
}

/// A table's row-level security as the catalog holds it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RlsState {
    pub enabled: bool,
    pub forced: bool,
    pub policies: Vec<PolicyDef>,
}

/// One policy as `pg_policies` shows it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PolicyDef {
    pub name: String,
    pub permissive: bool,
    /// `ALL`, `SELECT`, `INSERT`, `UPDATE` or `DELETE`.
    pub command: String,
    /// Role names; `public` is PUBLIC.
    pub roles: Vec<String>,
    pub using: Option<String>,
    pub check: Option<String>,
}

impl PolicyDef {
    fn same_as(&self, other: &PolicyDef) -> bool {
        let roles = |policy: &PolicyDef| {
            let mut roles = policy.roles.clone();
            roles.sort();
            roles
        };
        self.permissive == other.permissive
            && self.command.eq_ignore_ascii_case(&other.command)
            && roles(self) == roles(other)
            && self.using == other.using
            && self.check == other.check
    }

    fn create_on(&self, relation: &str) -> String {
        let command = match self.command.to_ascii_uppercase().as_str() {
            "SELECT" => "SELECT",
            "INSERT" => "INSERT",
            "UPDATE" => "UPDATE",
            "DELETE" => "DELETE",
            _ => "ALL",
        };
        let roles = if self.roles.is_empty() {
            "PUBLIC".to_string()
        } else {
            self.roles
                .iter()
                .map(|role| {
                    if role == "public" {
                        "PUBLIC".to_string()
                    } else {
                        qi(role)
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut sql = format!(
            "CREATE POLICY {} ON {relation} AS {} FOR {command} TO {roles}",
            qi(&self.name),
            if self.permissive {
                "PERMISSIVE"
            } else {
                "RESTRICTIVE"
            },
        );
        if let Some(using) = &self.using {
            sql.push_str(&format!(" USING ({using})"));
        }
        if let Some(check) = &self.check {
            sql.push_str(&format!(" WITH CHECK ({check})"));
        }
        sql
    }
}

/// The statements that make the partition `relation` carry exactly the
/// parent's row-level security: enable/force flags, and the policy set
/// (missing created, changed replaced, extra dropped). Empty when it already
/// does, or when the parent has row-level security off.
pub(crate) fn rls_sync_statements(
    relation: &str,
    parent: &RlsState,
    child: &RlsState,
) -> Vec<String> {
    let mut out = Vec::new();
    if !parent.enabled {
        return out;
    }
    if !child.enabled {
        out.push(format!("ALTER TABLE {relation} ENABLE ROW LEVEL SECURITY"));
    }
    if parent.forced != child.forced {
        out.push(format!(
            "ALTER TABLE {relation} {}FORCE ROW LEVEL SECURITY",
            if parent.forced { "" } else { "NO " }
        ));
    }
    let mut child_policies: Vec<&PolicyDef> = child.policies.iter().collect();
    child_policies.sort_by(|a, b| a.name.cmp(&b.name));
    let mut parent_policies: Vec<&PolicyDef> = parent.policies.iter().collect();
    parent_policies.sort_by(|a, b| a.name.cmp(&b.name));
    let mut create = Vec::new();
    for have in &child_policies {
        match parent_policies.iter().find(|want| want.name == have.name) {
            Some(want) if want.same_as(have) => {}
            Some(want) => {
                out.push(format!("DROP POLICY {} ON {relation}", qi(&have.name)));
                create.push(want.create_on(relation));
            }
            None => out.push(format!("DROP POLICY {} ON {relation}", qi(&have.name))),
        }
    }
    for want in &parent_policies {
        if !child_policies.iter().any(|have| have.name == want.name) {
            create.push(want.create_on(relation));
        }
    }
    out.extend(create);
    out
}

/// The parent's row-level security, read from the catalog: what is live.
async fn parent_rls_state(pool: &PgPool, target: &Target) -> Result<RlsState, String> {
    let (enabled, forced): (bool, bool) = sqlx::query_as(
        "SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = $1::regclass",
    )
    .bind(target.relation())
    .fetch_one(pool)
    .await
    .map_err(|err| format!("read the row-level security: {err}"))?;
    let policies = relation_policies(pool, &target.schema, &target.table).await?;
    Ok(RlsState {
        enabled,
        forced,
        policies,
    })
}

type PolicyRow = (
    String,
    String,
    Option<Vec<String>>,
    String,
    Option<String>,
    Option<String>,
);

fn policy_from_row((name, permissive, roles, command, using, check): PolicyRow) -> PolicyDef {
    PolicyDef {
        name,
        permissive: permissive.eq_ignore_ascii_case("PERMISSIVE"),
        command,
        roles: roles.unwrap_or_default(),
        using,
        check,
    }
}

async fn relation_policies(
    pool: &PgPool,
    schema: &str,
    table: &str,
) -> Result<Vec<PolicyDef>, String> {
    let rows: Vec<PolicyRow> = sqlx::query_as(
        "SELECT policyname::text, permissive, roles::text[], cmd, qual, with_check \
         FROM pg_policies WHERE schemaname = $1 AND tablename = $2",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("read the policies of {schema}.{table}: {err}"))?;
    Ok(rows.into_iter().map(policy_from_row).collect())
}

/// Bring every partition of a partitioned table whose parent has row-level
/// security on in line with it: the flags and the exact policy set, copied
/// from the parent's live catalog state. Idempotent; returns the partitions
/// it changed.
async fn secure_partitions(pool: &PgPool, target: &Target) -> Result<i64, String> {
    let partitioned: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass($1))",
    )
    .bind(target.relation())
    .fetch_one(pool)
    .await
    .map_err(|err| format!("read the partitioning: {err}"))?;
    if !partitioned {
        return Ok(0);
    }
    let parent = parent_rls_state(pool, target).await?;
    if !parent.enabled {
        return Ok(0);
    }
    let children: Vec<(String, String, bool, bool)> = sqlx::query_as(
        "SELECT n.nspname::text, c.relname::text, c.relrowsecurity, c.relforcerowsecurity \
         FROM pg_partition_tree($1::regclass) t \
         JOIN pg_class c ON c.oid = t.relid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE t.relid <> $1::regclass ORDER BY c.relname",
    )
    .bind(target.relation())
    .fetch_all(pool)
    .await
    .map_err(|err| format!("list partitions: {err}"))?;
    let mut fixed = 0i64;
    for (schema, table, enabled, forced) in children {
        let child = RlsState {
            enabled,
            forced,
            policies: relation_policies(pool, &schema, &table).await?,
        };
        let relation = format!("{}.{}", qi(&schema), qi(&table));
        let statements = rls_sync_statements(&relation, &parent, &child);
        if statements.is_empty() {
            continue;
        }
        // One transaction per partition: a changed policy is never missing.
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| format!("secure partition {relation}: {err}"))?;
        for statement in &statements {
            sqlx::query(statement)
                .execute(&mut *tx)
                .await
                .map_err(|err| format!("secure partition {relation}: {statement}: {err}"))?;
        }
        tx.commit()
            .await
            .map_err(|err| format!("secure partition {relation}: {err}"))?;
        fixed += 1;
        tracing::warn!(
            table = %target.qualified(),
            partition = %relation,
            fixes = ?statements,
            "partition did not carry its parent's row-level security; brought in line"
        );
    }
    Ok(fixed)
}

/// Drop UDB-created partitions whose whole range is older than the retention.
/// With `evidence_state_key` (the compliance evidence export is on), a
/// partition goes only once every row in it was exported too: its whole range
/// is before the export's high-water mark, the same cutoff the row delete
/// keeps to. Before the export ever ran, nothing is dropped.
async fn drop_expired_partitions(
    pool: &PgPool,
    target: &Target,
    retention: Duration,
    evidence_state_key: Option<&str>,
) -> Result<i64, String> {
    let exported = match evidence_state_key {
        None => String::new(),
        Some(key) => {
            let ready: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                .bind(EVIDENCE_STATE)
                .fetch_one(pool)
                .await
                .map_err(|err| format!("look up the evidence export state: {err}"))?;
            if !ready {
                return Ok(0);
            }
            format!(
                " AND upper_bound::timestamptz <= COALESCE((SELECT last_occurred_at \
                 FROM {EVIDENCE_STATE} WHERE worker = {}), 'epoch'::timestamptz)",
                ql(key)
            )
        }
    };
    let prefix = format!("{}_p_", target.table)
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    let sql = format!(
        "SELECT child FROM ( \
             SELECT c.relname::text AS child, \
                    (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'TO \\(''([^'']+)''\\)'))[1] AS upper_bound \
             FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
             WHERE i.inhparent = to_regclass($1) AND c.relname LIKE $2 ESCAPE '\\' \
         ) AS bounds \
         WHERE upper_bound IS NOT NULL \
           AND upper_bound::timestamptz <= now() - $3::interval{exported} \
         ORDER BY upper_bound"
    );
    let children: Vec<String> = sqlx::query_scalar(&sql)
        .bind(target.relation())
        .bind(format!("{prefix}%"))
        .bind(format!("{} seconds", retention.as_secs()))
        .fetch_all(pool)
        .await
        .map_err(|err| format!("list expired partitions: {err}"))?;
    let mut dropped = 0i64;
    for child in children {
        sqlx::query(&format!(
            "DROP TABLE IF EXISTS {}.{}",
            qi(&target.schema),
            qi(&child)
        ))
        .execute(pool)
        .await
        .map_err(|err| format!("drop partition {child}: {err}"))?;
        dropped += 1;
        tracing::info!(table = %target.qualified(), partition = %child, retention_secs = retention.as_secs(), "dropped partition past retention");
    }
    Ok(dropped)
}

/// Which rows of a table are past retention.
struct Expiry<'a> {
    column: &'a str,
    rows: Duration,
    /// The kept class and its retention (`None` keeps the class's rows).
    kept: Option<(&'a KeptClass, Option<Duration>)>,
    /// With an evidence export, rows it has not exported are kept.
    evidence_state_key: Option<&'a str>,
    /// A further condition a row must meet to be deleted.
    only_when: Option<&'a str>,
}

impl Expiry<'_> {
    /// The SQL condition true for a row past retention. Every value in it is
    /// a number or a constant of this module, so it is written inline.
    fn condition(&self) -> String {
        let column = qi(self.column);
        let before =
            |retention: Duration| format!("now() - interval '{} seconds'", retention.as_secs());
        let cutoff = match self.evidence_state_key {
            Some(key) => format!(
                "LEAST({}, COALESCE((SELECT last_occurred_at FROM {EVIDENCE_STATE} \
                 WHERE worker = {}), 'epoch'::timestamptz))",
                before(self.rows),
                ql(key)
            ),
            None => before(self.rows),
        };
        let class = match self.kept {
            None => String::new(),
            Some((class, None)) => format!(" AND NOT ({})", class.predicate),
            Some((class, Some(kept))) => format!(
                " AND (NOT ({}) OR {column} < {})",
                class.predicate,
                before(kept)
            ),
        };
        let only_when = self
            .only_when
            .map(|condition| format!(" AND ({condition})"))
            .unwrap_or_default();
        format!("{column} < {cutoff}{class}{only_when}")
    }
}

/// Begin a transaction with the platform-admin escape: these tables FORCE
/// row-level security and retention is cross-tenant system work.
async fn admin_tx(pool: &PgPool) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, String> {
    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;
    sqlx::query("SELECT set_config('app.platform_admin', 'true', true)")
        .execute(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    // A database or role statement_timeout must not cancel cleanup's work.
    sqlx::query("SELECT set_config('statement_timeout', $1, true)")
        .bind(statement_timeout())
        .execute(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    Ok(tx)
}

/// The leaf tables that hold `target`'s rows: the table itself when it is not
/// partitioned, else every partition (a default partition included). Leaves
/// are deleted from directly: the parent's tenant policy would hide other
/// tenants' rows from a delete through it.
/// The leaf tables of `$1`: every leaf partition of a partitioned table, or
/// the table itself when it is not partitioned (`pg_partition_tree` returns no
/// row for a plain table, so without this every unpartitioned target would be
/// skipped).
const LEAF_RELIDS: &str = "SELECT relid FROM pg_partition_tree($1::regclass) WHERE isleaf \
     UNION ALL SELECT $1::regclass AS relid \
     WHERE NOT EXISTS (SELECT 1 FROM pg_partition_tree($1::regclass))";

async fn leaf_relations(pool: &PgPool, target: &Target) -> Result<Vec<String>, String> {
    let leaves: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT n.nspname::text, c.relname::text \
         FROM ({LEAF_RELIDS}) t \
         JOIN pg_class c ON c.oid = t.relid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         ORDER BY c.relname"
    ))
    .bind(target.relation())
    .fetch_all(pool)
    .await
    .map_err(|err| format!("list partitions: {err}"))?;
    Ok(leaves
        .into_iter()
        .map(|(schema, table)| format!("{}.{}", qi(&schema), qi(&table)))
        .collect())
}

/// Refuse loudly when row-level security would hide other tenants' rows from
/// this role on `relation` (the policy applies to the role and has no
/// `app.platform_admin` clause for [`admin_tx`] to open): a delete there
/// would quietly remove nothing past retention.
async fn ensure_rows_reachable(pool: &PgPool, relation: &str) -> Result<(), String> {
    let hidden: bool = sqlx::query_scalar(
        "SELECT row_security_active($1::regclass) \
            AND NOT EXISTS (SELECT 1 FROM pg_policy p \
                            WHERE p.polrelid = $1::regclass \
                              AND pg_get_expr(p.polqual, p.polrelid) LIKE '%app.platform_admin%')",
    )
    .bind(relation)
    .fetch_one(pool)
    .await
    .map_err(|err| format!("read the row-level security of {relation}: {err}"))?;
    if hidden {
        return Err(format!(
            "{relation}: row-level security hides other tenants' rows from the broker's \
             database role, so nothing past retention can be deleted; give the role \
             BYPASSRLS or add an app.platform_admin clause to the table's policy"
        ));
    }
    Ok(())
}

/// Delete a table's rows past retention, leaf table by leaf table. The
/// scheduled pass deletes bounded batches (at most [`MAX_BATCHES_PER_PASS`]
/// per leaf); `udb cleanup run` and periodic tables sweep each leaf once, page
/// range by page range, so a large backlog costs one read of the table.
async fn delete_expired_rows(
    pool: &PgPool,
    target: &Target,
    expiry: &Expiry<'_>,
    mode: PassMode,
) -> Result<i64, String> {
    if expiry.evidence_state_key.is_some() {
        let ready: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(EVIDENCE_STATE)
            .fetch_one(pool)
            .await
            .map_err(|err| format!("look up the evidence export state: {err}"))?;
        if !ready {
            // The export is on but has never run: nothing is exported, so
            // nothing may be deleted.
            return Ok(0);
        }
    }
    let mut deleted = 0i64;
    for relation in leaf_relations(pool, target).await? {
        ensure_rows_reachable(pool, &relation).await?;
        if mode.drain {
            deleted += sweep_expired_rows(pool, &relation, expiry).await?;
            continue;
        }
        let batch = mode.batch.max(1);
        let sql = format!(
            "DELETE FROM {relation} WHERE ctid IN ( \
                 SELECT ctid FROM {relation} WHERE {} LIMIT {batch})",
            expiry.condition()
        );
        for _ in 0..MAX_BATCHES_PER_PASS {
            let mut tx = admin_tx(pool).await?;
            let done = sqlx::query(&sql)
                .execute(&mut *tx)
                .await
                .map_err(|err| format!("delete expired rows: {err}"))?;
            tx.commit().await.map_err(|err| err.to_string())?;
            let rows = done.rows_affected() as i64;
            deleted += rows;
            if rows < batch {
                break;
            }
        }
    }
    if deleted > 0 {
        tracing::info!(table = %target.qualified(), deleted, retention_secs = expiry.rows.as_secs(), "deleted rows past retention");
    }
    Ok(deleted)
}

/// Delete every row of `relation` past retention in one pass over the heap,
/// `UDB_CLEANUP_SWEEP_PAGES` pages per statement (a TID range scan), each statement its
/// own short transaction. Rows written after the pass started are newer than
/// any cutoff, so the page count read at the start is enough.
async fn sweep_expired_rows(
    pool: &PgPool,
    relation: &str,
    expiry: &Expiry<'_>,
) -> Result<i64, String> {
    let pages: i64 = sqlx::query_scalar(
        "SELECT pg_relation_size($1::regclass) / current_setting('block_size')::bigint",
    )
    .bind(relation)
    .fetch_one(pool)
    .await
    .map_err(|err| format!("size {relation}: {err}"))?;
    let condition = expiry.condition();
    let mut deleted = 0i64;
    let mut first = 0i64;
    while first < pages {
        let last = first + sweep_pages();
        let sql = format!(
            "DELETE FROM {relation} WHERE ctid >= '({first},0)'::tid \
             AND ctid < '({last},0)'::tid AND {condition}"
        );
        let mut tx = admin_tx(pool).await?;
        let done = sqlx::query(&sql)
            .execute(&mut *tx)
            .await
            .map_err(|err| format!("delete expired rows from {relation}: {err}"))?;
        tx.commit().await.map_err(|err| err.to_string())?;
        deleted += done.rows_affected() as i64;
        first = last;
    }
    Ok(deleted)
}

/// The partitions of `target` that can hold rows older than `rows`: those
/// whose lower bound is before the cutoff (a default partition always can).
/// Each is swept directly; the parent's tenant policy would hide other
/// tenants' rows from a delete through it.
async fn partitions_with_expired_rows(
    pool: &PgPool,
    target: &Target,
    rows: Duration,
) -> Result<Vec<String>, String> {
    let children: Vec<(String, String)> = sqlx::query_as(
        "SELECT n.nspname::text, c.relname::text FROM ( \
             SELECT i.inhrelid AS oid, \
                    (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'FROM \\(''([^'']+)''\\)'))[1] AS lower_bound \
             FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
             WHERE i.inhparent = to_regclass($1) \
         ) AS bounds \
         JOIN pg_class c ON c.oid = bounds.oid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE bounds.lower_bound IS NULL \
            OR bounds.lower_bound::timestamptz < now() - $2::interval \
         ORDER BY c.relname",
    )
    .bind(target.relation())
    .bind(format!("{} seconds", rows.as_secs()))
    .fetch_all(pool)
    .await
    .map_err(|err| format!("list partitions: {err}"))?;
    Ok(children
        .into_iter()
        .map(|(schema, table)| format!("{}.{}", qi(&schema), qi(&table)))
        .collect())
}

/// One table's size estimate: rows sampled per leaf partition, each with its
/// age and its stored size (TOAST included, as stored, compressed).
struct SizeSample {
    /// Estimated bytes of the table's rows.
    bytes: f64,
    /// `(epoch seconds of the row's time column, bytes it stands for)`.
    rows: Vec<(f64, f64)>,
}

/// Estimate the size of `target`'s rows from a sample of each leaf partition.
/// Leaves are read directly (the parent's tenant policy would hide other
/// tenants' rows), inside the platform-admin escape for unpartitioned logs.
async fn sample_size(pool: &PgPool, target: &Target, column: &str) -> Result<SizeSample, String> {
    let leaves: Vec<(String, String, f32)> = sqlx::query_as(&format!(
        "SELECT n.nspname::text, c.relname::text, c.reltuples \
         FROM ({LEAF_RELIDS}) t \
         JOIN pg_class c ON c.oid = t.relid \
         JOIN pg_namespace n ON n.oid = c.relnamespace"
    ))
    .bind(target.relation())
    .fetch_all(pool)
    .await
    .map_err(|err| format!("list partitions: {err}"))?;
    let mut sample = SizeSample {
        bytes: 0.0,
        rows: Vec::new(),
    };
    for (schema, table, reltuples) in leaves {
        let leaf = format!("{}.{}", qi(&schema), qi(&table));
        let mut estimated = f64::from(reltuples);
        if estimated < 0.0 {
            // Never analyzed: count it once.
            sqlx::Executor::execute(pool, sqlx::raw_sql(&format!("ANALYZE {leaf}")))
                .await
                .map_err(|err| format!("analyze {leaf}: {err}"))?;
            estimated = sqlx::query_scalar::<_, f32>(
                "SELECT reltuples FROM pg_class WHERE oid = $1::regclass",
            )
            .bind(&leaf)
            .fetch_one(pool)
            .await
            .map(f64::from)
            .map_err(|err| format!("count {leaf}: {err}"))?;
        }
        if estimated <= 0.0 {
            continue;
        }
        let size_expr: Option<String> = sqlx::query_scalar(
            "SELECT string_agg(format('COALESCE(pg_column_size(%I), 0)', attname), ' + ' ORDER BY attnum) \
             FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped",
        )
        .bind(&leaf)
        .fetch_one(pool)
        .await
        .map_err(|err| format!("read the columns of {leaf}: {err}"))?;
        let Some(size_expr) = size_expr else { continue };
        let percent = (SIZE_SAMPLE_ROWS / estimated * 100.0).clamp(0.01, 100.0);
        let sql = format!(
            "SELECT EXTRACT(EPOCH FROM {column})::float8, (24 + {size_expr})::float8 \
             FROM {leaf} TABLESAMPLE SYSTEM ({percent}) LIMIT {limit}",
            column = qi(column),
            limit = (SIZE_SAMPLE_ROWS as i64) * 2,
        );
        let mut tx = admin_tx(pool).await?;
        let mut rows: Vec<(f64, f64)> = sqlx::query_as(&sql)
            .fetch_all(&mut *tx)
            .await
            .map_err(|err| format!("sample {leaf}: {err}"))?;
        if rows.is_empty() {
            // A sample of a few pages can miss every row: read the start.
            let sql = format!(
                "SELECT EXTRACT(EPOCH FROM {column})::float8, (24 + {size_expr})::float8 \
                 FROM {leaf} LIMIT {limit}",
                column = qi(column),
                limit = SIZE_SAMPLE_ROWS as i64,
            );
            rows = sqlx::query_as(&sql)
                .fetch_all(&mut *tx)
                .await
                .map_err(|err| format!("sample {leaf}: {err}"))?;
        }
        tx.commit().await.map_err(|err| err.to_string())?;
        if rows.is_empty() {
            continue;
        }
        let per_row = estimated / rows.len() as f64;
        for (at, bytes) in rows {
            sample.bytes += bytes * per_row;
            sample.rows.push((at, bytes * per_row));
        }
    }
    Ok(sample)
}

/// The age of the oldest rows to keep so that `remove` bytes of the sample go:
/// walk the rows oldest first until that many bytes are behind.
fn cap_cutoff_age(sample: &SizeSample, remove: f64, now: f64) -> Option<Duration> {
    let mut rows = sample.rows.clone();
    rows.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut removed = 0.0;
    for (at, bytes) in rows {
        removed += bytes;
        if removed >= remove {
            // Deletion is `older than now - age`: one second past this row
            // takes the row itself too.
            let age = (now - at - 1.0).max(0.0);
            return Some(Duration::from_secs(age as u64));
        }
    }
    Some(Duration::ZERO)
}

/// Keep the estimated size of the log tables that have a retention under
/// `max`: when over it, delete each table's oldest rows, in proportion to its
/// size, down to 90% of `max`. Returns the rows deleted.
async fn enforce_size_cap(
    runtime: &crate::runtime::DataBrokerRuntime,
    settings: &CleanupSettings,
    max: u64,
    reclaim_mode: Reclaim,
) -> Result<i64, String> {
    let config = &settings.retention;
    let mut tables = Vec::new();
    let mut failures = Vec::new();
    for target in &settings.targets {
        // Only UDB's own logs are capped; entity and bookkeeping tables follow
        // their retention alone.
        if !target.capped || config.paused.contains_key(&target.qualified()) {
            continue;
        }
        let retention = config.retention_of(target);
        let Some(rows) = retention.rows else {
            // `keep` (or a table outside the retention list) is outside the cap.
            continue;
        };
        let Some(pool) = store_pool(runtime, target) else {
            continue;
        };
        let exists: bool = match sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(target.relation())
            .fetch_one(&pool)
            .await
        {
            Ok(exists) => exists,
            Err(err) => {
                failures.push(format!("{}: {err}", target.qualified()));
                continue;
            }
        };
        if !exists {
            continue;
        }
        let (column, evidence_state_key) = match &target.kind {
            TargetKind::Partitioned { column, .. } => (column.clone(), None),
            TargetKind::Rows {
                column,
                evidence_state_key,
                ..
            } => (column.clone(), evidence_state_key.clone()),
        };
        match sample_size(&pool, target, &column).await {
            Ok(sample) => tables.push((
                target,
                pool,
                column,
                evidence_state_key,
                retention,
                rows,
                sample,
            )),
            Err(why) => failures.push(format!("{}: size estimate: {why}", target.qualified())),
        }
    }
    let total: f64 = tables.iter().map(|table| table.6.bytes).sum();
    let mut deleted = 0i64;
    if total > max as f64 {
        let keep = (max as f64 * 0.9) / total;
        tracing::warn!(
            estimated_bytes = total as u64,
            max_bytes = max,
            "UDB's log tables are over {CLEANUP_MAX_SIZE_ENV}; deleting their oldest rows"
        );
        for (target, pool, column, evidence_state_key, retention, rows, sample) in &tables {
            let now: f64 = sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM now())::float8")
                .fetch_one(pool)
                .await
                .map_err(|err| err.to_string())?;
            let Some(age) = cap_cutoff_age(sample, sample.bytes * (1.0 - keep), now) else {
                continue;
            };
            let age = age.max(SIZE_CAP_FLOOR);
            if age >= *rows {
                continue;
            }
            let expiry = Expiry {
                column,
                rows: age,
                kept: target
                    .kept_class
                    .as_ref()
                    .map(|class| (class, retention.kept)),
                evidence_state_key: evidence_state_key.as_deref(),
                only_when: None,
            };
            let removed = match &target.kind {
                TargetKind::Partitioned { .. } => {
                    let mut removed = 0i64;
                    for child in partitions_with_expired_rows(pool, target, age).await? {
                        removed += sweep_expired_rows(pool, &child, &expiry).await?;
                    }
                    removed
                }
                TargetKind::Rows { .. } => {
                    if evidence_state_key.is_some() {
                        let ready: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                            .bind(EVIDENCE_STATE)
                            .fetch_one(pool)
                            .await
                            .map_err(|err| err.to_string())?;
                        if !ready {
                            continue;
                        }
                    }
                    // Leaf by leaf: a log converted to monthly partitions has
                    // no rows of its own (the leaf list of a plain table is
                    // the table itself).
                    let mut removed = 0i64;
                    for leaf in leaf_relations(pool, target).await? {
                        removed += sweep_expired_rows(pool, &leaf, &expiry).await?;
                    }
                    removed
                }
            };
            if removed > 0 {
                tracing::warn!(
                    table = %target.qualified(),
                    deleted = removed,
                    kept_secs = age.as_secs(),
                    "deleted the oldest rows to stay under {CLEANUP_MAX_SIZE_ENV}"
                );
                deleted += removed;
                if let Err(why) = reclaim(pool, &target.relation(), reclaim_mode, false).await {
                    failures.push(format!("{}: {why}", target.qualified()));
                }
            } else if age == SIZE_CAP_FLOOR {
                tracing::warn!(
                    table = %target.qualified(),
                    "over {CLEANUP_MAX_SIZE_ENV}, but only rows of the last hour, kept-class rows or rows not yet exported are left"
                );
            }
        }
    }
    if failures.is_empty() {
        Ok(deleted)
    } else {
        Err(format!("size cap: {}", failures.join("; ")))
    }
}

fn split_relation(relation: &str) -> Option<(String, String)> {
    let (schema, table) = relation.split_once('.')?;
    let unquote = |part: &str| part.trim_matches('"').replace("\"\"", "\"");
    Some((unquote(schema), unquote(table)))
}

fn qi(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn ql(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_targets_include_the_decision_audit_and_both_logs() {
        let targets = targets(false);
        let audit = targets
            .iter()
            .find(|t| t.qualified() == "udb_authz.access_decision_audits")
            .expect("decision audit is a target");
        assert_eq!(audit.service, "authz");
        assert!(matches!(
            &audit.kind,
            TargetKind::Partitioned { unit, suffix_format, premake, .. }
                if unit == "month" && suffix_format == "YYYYMM" && *premake == DEFAULT_PREMAKE
        ));
        assert_eq!(audit.declared, Some(Duration::from_secs(30 * 86_400)));
        let log = targets
            .iter()
            .find(|t| t.qualified() == AUTH_AUDIT_LOG)
            .expect("auth audit log is a target");
        assert_eq!(log.declared, audit.declared, "follows the decision audit");
        assert!(
            targets
                .iter()
                .any(|t| t.table == "usage_events" && matches!(t.kind, TargetKind::Rows { .. }))
        );
    }

    #[test]
    fn the_retention_list_names_tables_and_refuses_mistakes() {
        let targets = targets(false);
        let config = parse_retention(
            "access_decision_audits=90d, udb_system.auth_audit_log=36h,usage_events=keep",
            &targets,
        )
        .unwrap();
        assert_eq!(
            config.retention.get("udb_authz.access_decision_audits"),
            Some(&Some(Duration::from_secs(90 * 86_400)))
        );
        assert_eq!(
            config.retention.get(AUTH_AUDIT_LOG),
            Some(&Some(Duration::from_secs(36 * 3_600)))
        );
        assert!(config.retention.values().any(Option::is_none));

        // Denies follow the table unless set, and are never kept shorter.
        let audit = targets
            .iter()
            .find(|t| t.qualified() == "udb_authz.access_decision_audits")
            .unwrap();
        assert!(audit.kept_class.is_some());
        let days = |n: u64| Some(Duration::from_secs(n * 86_400));
        let config = parse_retention("access_decision_audits=7d", &targets).unwrap();
        assert_eq!(
            config.retention_of(audit),
            Retention {
                rows: days(7),
                kept: days(7)
            }
        );
        let config = parse_retention(
            "access_decision_audits=7d,access_decision_audits:deny=90d",
            &targets,
        )
        .unwrap();
        assert_eq!(
            config.retention_of(audit),
            Retention {
                rows: days(7),
                kept: days(90)
            }
        );
        let config = parse_retention(
            "access_decision_audits=30d,udb_authz.access_decision_audits:deny=7d",
            &targets,
        )
        .unwrap();
        assert_eq!(config.retention_of(audit).kept, days(30), "never shorter");
        let config = parse_retention(
            "access_decision_audits=7d,access_decision_audits:deny=keep",
            &targets,
        )
        .unwrap();
        assert_eq!(
            config.retention_of(audit),
            Retention {
                rows: days(7),
                kept: None
            }
        );

        // The default list follows the declared retention; a setting overrides
        // one entry and keeps the others.
        let defaults = parse_retention(DEFAULT_CLEANUP_RETENTION, &targets).unwrap();
        assert_eq!(
            defaults.retention.get("udb_authz.access_decision_audits"),
            Some(&Some(Duration::from_secs(30 * 86_400)))
        );
        let usage = targets.iter().find(|t| t.table == "usage_events").unwrap();
        assert_eq!(
            defaults.retention.get(&usage.qualified()),
            Some(&usage.declared)
        );
        assert!(usage.declared.is_some());
        // A time-partitioned table that is not listed keeps every row.
        let other = targets
            .iter()
            .find(|t| {
                matches!(t.kind, TargetKind::Partitioned { .. })
                    && ![
                        "access_decision_audits",
                        "api_key_usages",
                        "notification_logs",
                    ]
                    .contains(&t.table.as_str())
            })
            .expect("another time-partitioned UDB table");
        assert_eq!(defaults.retention_for(other), None, "{}", other.qualified());

        for (raw, why) in [
            ("access_decision_audits", "table=duration"),
            ("nope=30d", "not a table this worker keeps"),
            ("access_decision_audits=12h", "shortest retention is 1d"),
            ("access_decision_audits=30", "unit"),
            ("access_decision_audits=3w", "not d (days) or h (hours)"),
            (
                "udb_system.udb_cdc_event_journal=1d",
                "UDB_CDC_JOURNAL_RETENTION",
            ),
            ("api_key_usages=soon", "unit"),
            ("usage_events:deny=30d", "no row class"),
            (
                "access_decision_audits:allow=30d",
                "only the row class `deny`",
            ),
        ] {
            let err = parse_retention(raw, &targets).unwrap_err();
            assert!(err.contains(why), "{raw}: {err}");
        }
    }

    fn tenant_policy(name: &str, using: &str) -> PolicyDef {
        PolicyDef {
            name: name.to_string(),
            permissive: true,
            command: "ALL".to_string(),
            roles: vec!["public".to_string()],
            using: Some(using.to_string()),
            check: None,
        }
    }

    // A partition is made to carry exactly its parent's row-level security:
    // flags, missing policies created, changed ones replaced, extra dropped.
    #[test]
    fn rls_sync_statements_make_a_partition_match_its_parent() {
        let relation = "\"s\".\"audit_p_202611\"";
        let parent = RlsState {
            enabled: true,
            forced: true,
            policies: vec![
                tenant_policy("tenant_isolation", "(tenant_id = 'a'::text)"),
                PolicyDef {
                    name: "admin".to_string(),
                    permissive: false,
                    command: "SELECT".to_string(),
                    roles: vec!["udb_admin".to_string(), "public".to_string()],
                    using: Some("true".to_string()),
                    check: Some("false".to_string()),
                },
            ],
        };
        // A bare partition: everything is added.
        assert_eq!(
            rls_sync_statements(relation, &parent, &RlsState::default()),
            vec![
                format!("ALTER TABLE {relation} ENABLE ROW LEVEL SECURITY"),
                format!("ALTER TABLE {relation} FORCE ROW LEVEL SECURITY"),
                format!(
                    "CREATE POLICY \"admin\" ON {relation} AS RESTRICTIVE FOR SELECT \
                     TO \"udb_admin\", PUBLIC USING (true) WITH CHECK (false)"
                ),
                format!(
                    "CREATE POLICY \"tenant_isolation\" ON {relation} AS PERMISSIVE FOR ALL \
                     TO PUBLIC USING ((tenant_id = 'a'::text))"
                ),
            ]
        );
        // Already in line (role order aside): nothing to do.
        let mut same = parent.clone();
        same.policies[1].roles.reverse();
        assert!(rls_sync_statements(relation, &parent, &same).is_empty());
        // A changed policy is replaced, an extra one dropped, FORCE set.
        let drifted = RlsState {
            enabled: true,
            forced: false,
            policies: vec![
                tenant_policy("tenant_isolation", "true"),
                parent.policies[1].clone(),
                tenant_policy("leftover", "true"),
            ],
        };
        assert_eq!(
            rls_sync_statements(relation, &parent, &drifted),
            vec![
                format!("ALTER TABLE {relation} FORCE ROW LEVEL SECURITY"),
                format!("DROP POLICY \"leftover\" ON {relation}"),
                format!("DROP POLICY \"tenant_isolation\" ON {relation}"),
                format!(
                    "CREATE POLICY \"tenant_isolation\" ON {relation} AS PERMISSIVE FOR ALL \
                     TO PUBLIC USING ((tenant_id = 'a'::text))"
                ),
            ]
        );
        // A forced child of an unforced parent loses FORCE.
        let mut unforced = parent.clone();
        unforced.forced = false;
        assert_eq!(
            rls_sync_statements(relation, &unforced, &same),
            vec![format!(
                "ALTER TABLE {relation} NO FORCE ROW LEVEL SECURITY"
            )]
        );
        // A parent without row-level security leaves its partitions alone.
        assert!(rls_sync_statements(relation, &RlsState::default(), &drifted).is_empty());
    }

    /// The pool sets its own statement timeout on every connection it opens,
    /// which a DSN option cannot lift. Cleanup's VACUUM FULL must run without
    /// it, and the connection must go back to the pool with the pool's timeout
    /// (not `RESET` to the startup value). Here the pool's timeout is 1 ms, far
    /// shorter than rewriting 200k rows.
    #[tokio::test]
    #[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_PG_DSN=... cargo test --lib live_postgres_cleanup_vacuum -- --ignored --nocapture"]
    async fn live_postgres_cleanup_vacuum_lifts_the_pool_timeout_and_restores_it() {
        let dsn = std::env::var("UDB_LIVE_AUTH_PG_DSN")
            .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
            .unwrap_or_else(|_| "postgres://udb:udb@127.0.0.1:55432/udb".to_string());
        let setup = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await
            .expect("connect live postgres");
        let schema = format!("udb_vacuum_{}", uuid::Uuid::new_v4().simple());
        let relation = format!("{schema}.journal");
        for sql in [
            format!("CREATE SCHEMA {schema}"),
            format!("CREATE TABLE {relation} (id BIGINT, body TEXT)"),
            format!(
                "INSERT INTO {relation} SELECT g, repeat('x', 200) FROM generate_series(1, 200000) g"
            ),
            format!("DELETE FROM {relation} WHERE id % 2 = 0"),
        ] {
            sqlx::query(&sql)
                .execute(&setup)
                .await
                .unwrap_or_else(|err| panic!("{sql}: {err}"));
        }
        // One connection, so the VACUUM and the check below share a session.
        let timed = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    sqlx::Executor::execute(conn, "SET statement_timeout = 1").await?;
                    Ok(())
                })
            })
            .connect(&dsn)
            .await
            .expect("connect timed pool");
        // Without the lift the pool's 1 ms timeout cancels the rewrite.
        let refused =
            sqlx::Executor::execute(&timed, sqlx::raw_sql(&format!("VACUUM (FULL) {relation}")))
                .await;
        assert!(
            refused.is_err_and(|err| err.to_string().contains("statement timeout")),
            "the fixture must be too large for the pool's 1 ms timeout"
        );
        reclaim_space(&timed, &relation, "VACUUM (FULL, ANALYZE)")
            .await
            .expect("cleanup's VACUUM FULL runs without the pool's timeout");
        let after: String = sqlx::query_scalar("SELECT current_setting('statement_timeout')")
            .fetch_one(&timed)
            .await
            .expect("read the pooled connection's timeout");
        assert_eq!(
            after, "1ms",
            "the pool's own timeout is back on the connection"
        );
        timed.close().await;
        sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&setup)
            .await
            .expect("drop fixture schema");
    }

    /// Against a real Postgres, on fixture tables of a throwaway schema:
    /// partitions are created ahead (so a write lands), only UDB-named expired
    /// partitions are dropped, a hand-made one is kept, a second pass changes
    /// nothing, and the row sweep deletes only rows past the retention.
    #[tokio::test]
    #[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_PG_DSN=... cargo test --lib live_postgres_cleanup_keeps_partitions_ahead_and_prunes -- --ignored --nocapture"]
    async fn live_postgres_cleanup_keeps_partitions_ahead_and_prunes() {
        let dsn = std::env::var("UDB_LIVE_AUTH_PG_DSN")
            .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
            .unwrap_or_else(|_| "postgres://udb:udb@127.0.0.1:55432/udb".to_string());
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await
            .expect("connect live postgres");
        let schema = format!("udb_retention_{}", uuid::Uuid::new_v4().simple());
        let run = |sql: String| {
            let pool = pool.clone();
            async move {
                sqlx::query(&sql)
                    .execute(&pool)
                    .await
                    .unwrap_or_else(|err| panic!("{sql}: {err}"));
            }
        };
        let exists = |name: String| {
            let pool = pool.clone();
            async move {
                sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
                    .bind(name)
                    .fetch_one(&pool)
                    .await
                    .expect("to_regclass")
            }
        };
        run(format!("CREATE SCHEMA {schema}")).await;
        run(format!(
            "CREATE TABLE {schema}.audit (id INT, decided_at TIMESTAMPTZ NOT NULL) \
             PARTITION BY RANGE (decided_at)"
        ))
        .await;
        run(format!(
            "CREATE TABLE {schema}.audit_p_202001 PARTITION OF {schema}.audit \
             FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')"
        ))
        .await;
        run(format!(
            "CREATE TABLE {schema}.audit_manual PARTITION OF {schema}.audit \
             FOR VALUES FROM ('2020-02-01') TO ('2020-03-01')"
        ))
        .await;
        // The parent carries row-level security; its partitions start without
        // it, and the hand-made one with a policy the parent does not have.
        run(format!(
            "ALTER TABLE {schema}.audit ENABLE ROW LEVEL SECURITY"
        ))
        .await;
        run(format!(
            "ALTER TABLE {schema}.audit FORCE ROW LEVEL SECURITY"
        ))
        .await;
        run(format!(
            "CREATE POLICY tenant_scope ON {schema}.audit USING (id > 0)"
        ))
        .await;
        run(format!(
            "CREATE POLICY stray ON {schema}.audit_manual USING (true)"
        ))
        .await;
        // Every partition: (name, RLS on, forced, policy names, tenant_scope's USING).
        let partition_rls = || {
            let pool = pool.clone();
            let schema = schema.clone();
            async move {
                sqlx::query_as::<_, (String, bool, bool, Vec<String>, Option<String>)>(
                    "SELECT c.relname::text, c.relrowsecurity, c.relforcerowsecurity, \
                            COALESCE((SELECT array_agg(p.policyname::text ORDER BY p.policyname) \
                                      FROM pg_policies p \
                                      WHERE p.schemaname = $1::text AND p.tablename = c.relname), '{}'), \
                            (SELECT p.qual FROM pg_policies p WHERE p.schemaname = $1::text \
                               AND p.tablename = c.relname AND p.policyname = 'tenant_scope') \
                     FROM pg_partition_tree(format('%I.%I', $1::text, 'audit')::regclass) t \
                     JOIN pg_class c ON c.oid = t.relid \
                     WHERE t.relid <> format('%I.%I', $1::text, 'audit')::regclass \
                     ORDER BY 1",
                )
                .bind(schema)
                .fetch_all(&pool)
                .await
                .expect("read partition row-level security")
            }
        };
        let parent_qual: String = sqlx::query_scalar(
            "SELECT qual FROM pg_policies WHERE schemaname = $1 AND tablename = 'audit'",
        )
        .bind(&schema)
        .fetch_one(&pool)
        .await
        .expect("parent policy");
        let assert_secured = |rows: Vec<(String, bool, bool, Vec<String>, Option<String>)>| {
            assert!(!rows.is_empty());
            for (name, enabled, forced, policies, qual) in rows {
                assert!(enabled && forced, "{name} carries the parent's RLS flags");
                assert_eq!(
                    policies,
                    ["tenant_scope"],
                    "{name}: exactly the parent's policies"
                );
                assert_eq!(qual.as_deref(), Some(parent_qual.as_str()), "{name}");
            }
        };
        let audit = Target {
            schema: schema.clone(),
            table: "audit".to_string(),
            service: "authz".to_string(),
            declared: None,
            kept_class: None,
            default_retention: None,
            capped: true,
            periodic: false,
            kind: TargetKind::Partitioned {
                column: "decided_at".to_string(),
                unit: "month".to_string(),
                interval: "1 month".to_string(),
                suffix_format: "YYYYMM".to_string(),
                premake: 4,
            },
        };
        let month = Duration::from_secs(30 * 86_400);
        let scheduled = PassMode {
            batch: 5_000,
            drain: false,
            sweep_partitions: false,
        };

        // Retention unset: partitions are created ahead, nothing is dropped.
        let created = maintain_target(&pool, &audit, Ok(Retention::default()), scheduled)
            .await
            .unwrap();
        assert_eq!(created.created, 5, "the current month and four ahead");
        // The partitions created ahead carried RLS from the start; the two
        // that were there before (one hand-made, with a stray policy) were
        // brought in line.
        assert_eq!(created.secured, 2);
        assert_secured(partition_rls().await);
        assert!(exists(format!("{schema}.audit_p_202001")).await);
        run(format!(
            "INSERT INTO {schema}.audit VALUES (1, now()), (2, now() + interval '3 months')"
        ))
        .await;

        // Retention set: the expired UDB partition goes, the hand-made one stays.
        let dropped = maintain_target(&pool, &audit, Ok(Retention::uniform(month)), scheduled)
            .await
            .unwrap();
        assert_eq!(dropped.dropped, 1);
        assert!(!exists(format!("{schema}.audit_p_202001")).await);
        assert!(exists(format!("{schema}.audit_manual")).await);
        // A malformed retention list never removes anything.
        assert_eq!(
            maintain_target(&pool, &audit, Err("bad".to_string()), scheduled)
                .await
                .unwrap()
                .total(),
            0
        );
        assert_eq!(
            maintain_target(&pool, &audit, Ok(Retention::uniform(month)), scheduled)
                .await
                .unwrap()
                .total(),
            0,
            "a second pass changes nothing"
        );
        // A partition someone else creates (pg_partman, an operator) without
        // the parent's RLS is brought in line on the next pass.
        run(format!(
            "CREATE TABLE {schema}.audit_late PARTITION OF {schema}.audit \
             FOR VALUES FROM ('2019-01-01') TO ('2019-02-01')"
        ))
        .await;
        let late = maintain_target(&pool, &audit, Ok(Retention::default()), scheduled)
            .await
            .unwrap();
        assert_eq!(late.secured, 1);
        assert_secured(partition_rls().await);

        run(format!(
            "CREATE TABLE {schema}.log (id INT, occurred_at TIMESTAMPTZ NOT NULL)"
        ))
        .await;
        run(format!(
            "INSERT INTO {schema}.log SELECT g, now() - interval '40 days' FROM generate_series(1, 3) g"
        ))
        .await;
        run(format!(
            "INSERT INTO {schema}.log SELECT g, now() - interval '1 day' FROM generate_series(4, 5) g"
        ))
        .await;
        let log = Target {
            schema: schema.clone(),
            table: "log".to_string(),
            service: "metering".to_string(),
            declared: None,
            kept_class: None,
            default_retention: None,
            capped: true,
            periodic: false,
            kind: TargetKind::Rows {
                column: "occurred_at".to_string(),
                evidence_state_key: None,
                only_when: None,
                guard: RowGuard::Unguarded,
                monthly_partitions: false,
            },
        };
        let small = PassMode {
            batch: 2,
            ..scheduled
        };
        let deleted = |outcome: Outcome| outcome.deleted;
        assert_eq!(
            deleted(
                maintain_target(&pool, &log, Ok(Retention::default()), small)
                    .await
                    .unwrap()
            ),
            0
        );
        assert_eq!(
            deleted(
                maintain_target(&pool, &log, Ok(Retention::uniform(month)), small)
                    .await
                    .unwrap()
            ),
            3
        );
        let count = |relation: String| {
            let pool = pool.clone();
            async move {
                sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {relation}"))
                    .fetch_one(&pool)
                    .await
                    .unwrap()
            }
        };
        assert_eq!(count(format!("{schema}.log")).await, 2);
        // `udb cleanup run` sweeps the whole table in page ranges.
        run(format!(
            "INSERT INTO {schema}.log SELECT g, now() - interval '40 days' FROM generate_series(6, 9) g"
        ))
        .await;
        let drain = PassMode {
            drain: true,
            ..small
        };
        assert_eq!(
            deleted(
                maintain_target(&pool, &log, Ok(Retention::uniform(month)), drain)
                    .await
                    .unwrap()
            ),
            4
        );
        assert_eq!(count(format!("{schema}.log")).await, 2);

        // Rows past retention inside a partition that is kept are swept out,
        // and denied decisions are kept longer (or for good) when asked.
        run(format!(
            "CREATE TABLE {schema}.decisions (id INT, effect TEXT NOT NULL, decided_at TIMESTAMPTZ NOT NULL) \
             PARTITION BY RANGE (decided_at)"
        ))
        .await;
        run(format!(
            "CREATE TABLE {schema}.decisions_all PARTITION OF {schema}.decisions \
             FOR VALUES FROM ('2000-01-01') TO ('2100-01-01')"
        ))
        .await;
        run(format!(
            "INSERT INTO {schema}.decisions VALUES \
             (1, 'ALLOW', now() - interval '40 days'), (2, 'DENY', now() - interval '40 days'), \
             (3, 'DENY', now() - interval '400 days'), (4, 'ALLOW', now() - interval '1 hour')"
        ))
        .await;
        let decisions = Target {
            schema: schema.clone(),
            table: "decisions".to_string(),
            service: "authz".to_string(),
            declared: None,
            kept_class: Some(KeptClass {
                name: "deny".to_string(),
                predicate: "effect IS DISTINCT FROM 'ALLOW'".to_string(),
            }),
            default_retention: None,
            capped: true,
            periodic: false,
            kind: TargetKind::Partitioned {
                column: "decided_at".to_string(),
                unit: "month".to_string(),
                interval: "1 month".to_string(),
                suffix_format: "YYYYMM".to_string(),
                premake: 0,
            },
        };
        let year = Duration::from_secs(365 * 86_400);
        let sweep = PassMode {
            sweep_partitions: true,
            ..scheduled
        };
        let allows_month_denies_year = Retention {
            rows: Some(month),
            kept: Some(year),
        };
        // Without a sweep (the scheduled pass between daily sweeps) nothing goes.
        assert_eq!(
            deleted(
                maintain_target(&pool, &decisions, Ok(allows_month_denies_year), scheduled)
                    .await
                    .unwrap()
            ),
            0
        );
        let outcome = maintain_target(&pool, &decisions, Ok(allows_month_denies_year), sweep)
            .await
            .unwrap();
        assert_eq!(outcome.deleted, 2, "the old allow and the deny past a year");
        assert_eq!(outcome.dropped, 0, "a hand-made partition is never dropped");
        let left: Vec<i32> =
            sqlx::query_scalar(&format!("SELECT id FROM {schema}.decisions ORDER BY id"))
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(left, vec![2, 4]);
        run(format!(
            "INSERT INTO {schema}.decisions VALUES (5, 'DENY', now() - interval '900 days')"
        ))
        .await;
        let denies_kept = Retention {
            rows: Some(month),
            kept: None,
        };
        assert_eq!(
            deleted(
                maintain_target(&pool, &decisions, Ok(denies_kept), sweep)
                    .await
                    .unwrap()
            ),
            0,
            "denies kept for good"
        );
        assert_eq!(count(format!("{schema}.decisions")).await, 3);

        // A table with a default partition (sessions) is swept partition by
        // partition from its end column; a row whose end is empty or recent
        // stays, and so does a row that is old but has not ended.
        run(format!(
            "CREATE TABLE {schema}.sessions (id INT, created_at TIMESTAMPTZ NOT NULL, \
             expires_at TIMESTAMPTZ) PARTITION BY RANGE (created_at)"
        ))
        .await;
        run(format!(
            "CREATE TABLE {schema}.sessions_p_old PARTITION OF {schema}.sessions \
             FOR VALUES FROM ('2000-01-01') TO ('2001-01-01')"
        ))
        .await;
        run(format!(
            "CREATE TABLE {schema}.sessions_default PARTITION OF {schema}.sessions DEFAULT"
        ))
        .await;
        run(format!(
            "INSERT INTO {schema}.sessions VALUES \
             (1, '2000-06-01', now() - interval '40 days'), \
             (2, now() - interval '50 days', now() - interval '40 days'), \
             (3, now() - interval '50 days', now() + interval '1 day'), \
             (4, now() - interval '50 days', NULL), \
             (5, now(), now() - interval '1 day')"
        ))
        .await;
        let sessions = Target {
            schema: schema.clone(),
            table: "sessions".to_string(),
            service: "authn".to_string(),
            declared: Some(month),
            kept_class: None,
            default_retention: Some(month),
            capped: false,
            periodic: true,
            kind: TargetKind::Rows {
                column: "expires_at".to_string(),
                evidence_state_key: None,
                only_when: None,
                guard: RowGuard::Unguarded,
                monthly_partitions: false,
            },
        };
        let swept = PassMode {
            drain: true,
            ..scheduled
        };
        assert_eq!(
            deleted(
                maintain_target(&pool, &sessions, Ok(Retention::uniform(month)), swept)
                    .await
                    .unwrap()
            ),
            2,
            "the expired session in each partition"
        );
        let left: Vec<i32> =
            sqlx::query_scalar(&format!("SELECT id FROM {schema}.sessions ORDER BY id"))
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(left, vec![3, 4, 5]);

        // A bookkeeping table loses only finished rows, and the catalog rule
        // keeps the active version, the bound one and the newest per project.
        run(format!(
            "CREATE TABLE {schema}.tasks (id INT, status TEXT NOT NULL, completed_at TIMESTAMPTZ)"
        ))
        .await;
        run(format!(
            "INSERT INTO {schema}.tasks VALUES \
             (1, 'COMPLETED', now() - interval '10 days'), (2, 'FAILED', now() - interval '10 days'), \
             (3, 'COMPLETED', now() - interval '1 day'), (4, 'PENDING', NULL)"
        ))
        .await;
        let tasks = Target {
            schema: schema.clone(),
            table: "tasks".to_string(),
            service: SYSTEM_STORE.to_string(),
            declared: None,
            kept_class: None,
            default_retention: None,
            capped: false,
            periodic: true,
            kind: TargetKind::Rows {
                column: "completed_at".to_string(),
                evidence_state_key: None,
                only_when: Some("status = 'COMPLETED'".to_string()),
                guard: RowGuard::Unguarded,
                monthly_partitions: false,
            },
        };
        let week = Duration::from_secs(7 * 86_400);
        assert_eq!(
            deleted(
                maintain_target(&pool, &tasks, Ok(Retention::uniform(week)), swept)
                    .await
                    .unwrap()
            ),
            1
        );
        assert_eq!(count(format!("{schema}.tasks")).await, 3);

        run(format!(
            "CREATE TABLE {schema}.versions (catalog_id INT PRIMARY KEY, project_id TEXT NOT NULL, \
             status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL)"
        ))
        .await;
        run(format!(
            "CREATE TABLE {schema}.bindings (project_id TEXT PRIMARY KEY, active_catalog_id INT)"
        ))
        .await;
        // Project a: 1 (active, old), 2 (bound, old), 3 and 4 (old), 5 and 6
        // (old but the newest two). Project b: 7 (old, its only version).
        run(format!(
            "INSERT INTO {schema}.versions VALUES \
             (1, 'a', 'ACTIVE', now() - interval '200 days'), \
             (2, 'a', 'ROLLED_BACK', now() - interval '190 days'), \
             (3, 'a', 'STAGED', now() - interval '180 days'), \
             (4, 'a', 'REJECTED', now() - interval '170 days'), \
             (5, 'a', 'STAGED', now() - interval '160 days'), \
             (6, 'a', 'STAGED', now() - interval '150 days'), \
             (7, 'b', 'STAGED', now() - interval '150 days')"
        ))
        .await;
        run(format!("INSERT INTO {schema}.bindings VALUES ('a', 2)")).await;
        let versions = Target {
            table: "versions".to_string(),
            kind: TargetKind::Rows {
                column: "created_at".to_string(),
                evidence_state_key: None,
                only_when: Some(catalog_versions_removable(
                    &format!("{schema}.versions"),
                    &format!("{schema}.bindings"),
                    2,
                )),
                guard: RowGuard::Unguarded,
                monthly_partitions: false,
            },
            ..tasks.clone()
        };
        assert_eq!(
            deleted(
                maintain_target(&pool, &versions, Ok(Retention::uniform(month)), swept)
                    .await
                    .unwrap()
            ),
            2,
            "versions 3 and 4"
        );
        let left: Vec<i32> = sqlx::query_scalar(&format!(
            "SELECT catalog_id FROM {schema}.versions ORDER BY catalog_id"
        ))
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(left, vec![1, 2, 5, 6, 7]);

        // Migration runs lose only old completed and dry runs: never a run
        // still in flight or failed, a project's newest completed run, nor a
        // run a reviewed transition names (its foreign key would refuse).
        run(format!(
            "CREATE TABLE {schema}.runs (run_id INT PRIMARY KEY, project_id TEXT NOT NULL, \
             state TEXT NOT NULL, started_at TIMESTAMPTZ NOT NULL)"
        ))
        .await;
        run(format!(
            "INSERT INTO {schema}.runs VALUES \
             (1, 'a', 'COMPLETED', now() - interval '200 days'), \
             (2, 'a', 'COMPLETED', now() - interval '150 days'), \
             (3, 'a', 'DRY_RUN', now() - interval '190 days'), \
             (4, 'a', 'ERROR', now() - interval '190 days'), \
             (5, 'a', 'PREFLIGHT', now() - interval '190 days'), \
             (6, 'b', 'COMPLETED', now() - interval '180 days'), \
             (7, 'b', 'COMPLETED', now() - interval '190 days'), \
             (8, 'b', 'DRY_RUN', now() - interval '10 days')"
        ))
        .await;
        run(format!(
            "CREATE TABLE {schema}.transitions (run_id INT PRIMARY KEY REFERENCES {schema}.runs (run_id))"
        ))
        .await;
        run(format!("INSERT INTO {schema}.transitions VALUES (7)")).await;
        let runs = Target {
            table: "runs".to_string(),
            kind: TargetKind::Rows {
                column: "started_at".to_string(),
                evidence_state_key: None,
                only_when: Some(migration_runs_removable(&format!("{schema}.runs"))),
                guard: RowGuard::ReferencedBy(
                    format!("{schema}.transitions"),
                    "run_id".to_string(),
                ),
                monthly_partitions: false,
            },
            ..tasks.clone()
        };
        assert_eq!(
            deleted(
                maintain_target(&pool, &runs, Ok(Retention::uniform(month)), swept)
                    .await
                    .unwrap()
            ),
            2,
            "runs 1 and 3"
        );
        let left: Vec<i32> =
            sqlx::query_scalar(&format!("SELECT run_id FROM {schema}.runs ORDER BY run_id"))
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(left, vec![2, 4, 5, 6, 7, 8]);

        run(format!("DROP SCHEMA {schema} CASCADE")).await;
    }

    /// The admin audit log is pruned from the front of its hash chain in
    /// pages, the prune anchor is written with each page, and the chain still
    /// verifies afterwards. Without the anchor, or with a forged one, it fails.
    #[cfg(feature = "postgres")]
    #[tokio::test]
    #[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_PG_DSN=... cargo test --lib live_postgres_cleanup_prunes_the_admin_audit_chain -- --ignored --nocapture"]
    async fn live_postgres_cleanup_prunes_the_admin_audit_chain_with_an_anchor() {
        use crate::runtime::canonical_store::postgres::PostgresCanonicalStore;
        use crate::runtime::canonical_store::system_store::{
            AdminAuditBreakReason, AdminAuditChainReport, AdminAuditInsert, AdminAuditStore,
            admin_audit_prune_anchor_key,
        };

        let dsn = std::env::var("UDB_LIVE_AUTH_PG_DSN")
            .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
            .unwrap_or_else(|_| "postgres://udb:udb@127.0.0.1:55432/udb".to_string());
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await
            .expect("connect live postgres");
        let schema = format!("udb_admin_prune_{}", uuid::Uuid::new_v4().simple());
        let run = |sql: String| {
            let pool = pool.clone();
            async move {
                sqlx::query(&sql)
                    .execute(&pool)
                    .await
                    .unwrap_or_else(|err| panic!("{sql}: {err}"));
            }
        };
        run(format!("CREATE SCHEMA {schema}")).await;
        let relation = format!("{schema}.admin_audit");
        let store = PostgresCanonicalStore::new(pool.clone(), "primary", "")
            .with_admin_audit_relation(relation.clone());
        AdminAuditStore::ensure_admin_audit_tables(&store)
            .await
            .expect("admin audit table");
        let mut ids = Vec::new();
        for i in 0..5 {
            ids.push(
                AdminAuditStore::append_admin_audit(
                    &store,
                    &AdminAuditInsert {
                        actor: "operator".to_string(),
                        operation: format!("Op{i}"),
                        target: "project".to_string(),
                        request_json: serde_json::json!({"step": i}),
                        result: "ok".to_string(),
                        tenant_id: "tenant".to_string(),
                        project_id: "project".to_string(),
                        correlation_id: format!("corr-{i}"),
                        signer_key_id: String::new(),
                        external_anchor: String::new(),
                    },
                )
                .await
                .expect("append"),
            );
        }
        // The three oldest rows age past a year; chain order is unchanged.
        run(format!(
            "UPDATE {relation} SET created_at = created_at - interval '400 days' \
             WHERE audit_id IN ('{}', '{}', '{}')",
            ids[0], ids[1], ids[2]
        ))
        .await;
        let third_hash: String = sqlx::query_scalar(&format!(
            "SELECT current_hash FROM {relation} WHERE audit_id = $1"
        ))
        .bind(ids[2])
        .fetch_one(&pool)
        .await
        .expect("third row's hash");
        assert!(
            AdminAuditStore::verify_admin_audit_chain(&store, None)
                .await
                .expect("verify")
                .is_passed(),
            "the unpruned chain verifies"
        );

        let target = Target {
            schema: schema.clone(),
            table: "admin_audit".to_string(),
            service: SYSTEM_STORE.to_string(),
            declared: None,
            kept_class: None,
            default_retention: None,
            capped: false,
            periodic: true,
            kind: TargetKind::Rows {
                column: "created_at".to_string(),
                evidence_state_key: None,
                only_when: None,
                guard: RowGuard::AdminAuditChain,
                monthly_partitions: false,
            },
        };
        // Pages of two: the anchor is written with each page.
        let paged = PassMode {
            batch: 2,
            drain: true,
            sweep_partitions: false,
        };
        let outcome = maintain_target(
            &pool,
            &target,
            Ok(Retention::uniform(Duration::from_secs(365 * 86_400))),
            paged,
        )
        .await
        .expect("prune");
        assert_eq!(outcome.deleted, 3, "the three rows past a year");
        let left: Vec<uuid::Uuid> = sqlx::query_scalar(&format!(
            "SELECT audit_id FROM {relation} ORDER BY created_at, audit_id"
        ))
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(left, ids[3..].to_vec());

        let key = admin_audit_prune_anchor_key(&relation);
        let (anchor_id, anchor_hash, pruned): (String, String, i64) = sqlx::query_as(&format!(
            "SELECT last_event_id, chain_head, exported_count FROM {EVIDENCE_STATE} WHERE worker = $1"
        ))
        .bind(&key)
        .fetch_one(&pool)
        .await
        .expect("the prune anchor");
        assert_eq!(anchor_id, ids[2].to_string(), "the newest row pruned");
        assert_eq!(anchor_hash, third_hash);
        assert_eq!(pruned, 3);
        match AdminAuditStore::verify_admin_audit_chain(&store, None)
            .await
            .expect("verify")
        {
            AdminAuditChainReport::Passed { checked_count, .. } => assert_eq!(checked_count, 2),
            other => panic!("the pruned chain verifies from its anchor: {other:?}"),
        }

        // A forged anchor breaks verification.
        run(format!(
            "UPDATE {EVIDENCE_STATE} SET chain_head = 'forged' WHERE worker = '{key}'"
        ))
        .await;
        let reason = |report: AdminAuditChainReport| match report {
            AdminAuditChainReport::Failed { reason, .. } => reason,
            other => panic!("expected a break: {other:?}"),
        };
        assert_eq!(
            reason(
                AdminAuditStore::verify_admin_audit_chain(&store, None)
                    .await
                    .expect("verify")
            ),
            AdminAuditBreakReason::AnchorMismatch
        );
        // Rows gone from the front without an anchor break it too.
        run(format!(
            "DELETE FROM {EVIDENCE_STATE} WHERE worker = '{key}'"
        ))
        .await;
        assert_eq!(
            reason(
                AdminAuditStore::verify_admin_audit_chain(&store, None)
                    .await
                    .expect("verify")
            ),
            AdminAuditBreakReason::PrunedWithoutAnchor
        );

        run(format!("DROP SCHEMA {schema} CASCADE")).await;
    }

    #[test]
    fn settings_come_from_values_with_declared_defaults() {
        let defaults = CleanupSettings::from_values(None, None, None, None, None, false);
        assert!(defaults.enabled);
        assert_eq!(defaults.interval, DEFAULT_INTERVAL);
        assert_eq!(defaults.batch, DEFAULT_ROW_BATCH);
        assert_eq!(defaults.reclaim, Ok(Reclaim::Vacuum));
        let shown = defaults.describe().unwrap();
        let audit = shown
            .iter()
            .find(|t| t["table"] == "udb_authz.access_decision_audits")
            .unwrap();
        assert_eq!(audit["retention"], "30d");
        assert_eq!(audit["declared"], "30d");
        assert_eq!(audit["kept_class"]["name"], "deny");
        assert_eq!(audit["kept_class"]["retention"], "30d");

        let set = CleanupSettings::from_values(
            Some("false"),
            Some("1h"),
            Some("250"),
            Some("full"),
            Some("access_decision_audits=90d,usage_events=keep"),
            true,
        );
        assert!(!set.enabled);
        assert_eq!(set.interval, Duration::from_secs(3600));
        assert_eq!(set.batch, 250);
        assert_eq!(set.reclaim, Ok(Reclaim::Full));
        let shown = set.describe().unwrap();
        let find = |name: &str| {
            shown
                .iter()
                .find(|t| t["table"].as_str().unwrap().ends_with(name))
                .unwrap()["retention"]
                .clone()
        };
        assert_eq!(find("access_decision_audits"), "90d");
        assert_eq!(find("usage_events"), "keep");
        // Not overridden: still the declared default.
        assert_eq!(find("auth_audit_log"), "30d");
        let log = set
            .targets
            .iter()
            .find(|t| t.qualified() == AUTH_AUDIT_LOG)
            .unwrap();
        assert!(matches!(
            &log.kind,
            TargetKind::Rows {
                evidence_state_key: Some(_),
                ..
            }
        ));

        let bad = CleanupSettings::from_values(
            None,
            Some("soon"),
            Some("-3"),
            Some("x"),
            Some("nope=1d"),
            false,
        );
        assert_eq!(bad.interval, DEFAULT_INTERVAL);
        assert_eq!(bad.batch, DEFAULT_ROW_BATCH);
        assert!(bad.reclaim.is_err());
        // An entry naming no known table pauses nothing; the error stays.
        assert_eq!(bad.retention_errors().len(), 1);
        assert!(bad.retention.paused.is_empty());
        assert!(bad.describe().is_ok());
    }

    #[test]
    fn a_bad_entry_pauses_only_the_table_it_names() {
        let settings = CleanupSettings::from_values(
            None,
            None,
            None,
            None,
            Some("access_decision_audits=12h,usage_events=90d,access_decision_audits:deny=9x"),
            false,
        );
        assert_eq!(settings.retention_errors().len(), 2);
        assert!(
            settings
                .retention
                .paused
                .contains_key("udb_authz.access_decision_audits"),
            "a malformed entry never falls back to the default retention"
        );
        let shown = settings.describe().unwrap();
        let find = |name: &str| {
            shown
                .iter()
                .find(|t| t["table"].as_str().unwrap().ends_with(name))
                .unwrap()
                .clone()
        };
        assert!(find("access_decision_audits")["paused"].is_string());
        assert_eq!(find("usage_events")["retention"], "90d");
        assert!(find("usage_events").get("paused").is_none());
        assert_eq!(find("auth_audit_log")["retention"], "30d");
    }

    fn find_target<'a>(targets: &'a [Target], qualified: &str) -> &'a Target {
        targets
            .iter()
            .find(|t| t.qualified() == qualified)
            .unwrap_or_else(|| panic!("{qualified} is a target"))
    }

    fn rows_column(target: &Target) -> &str {
        match &target.kind {
            TargetKind::Rows { column, .. } => column.as_str(),
            TargetKind::Partitioned { .. } => panic!("{} is partitioned", target.qualified()),
        }
    }

    /// Native tables whose rows end follow their declared retention by
    /// default, aged from the column that marks the end of a row's life.
    #[test]
    fn native_tables_with_an_end_follow_their_declared_retention() {
        let settings = CleanupSettings::from_values(None, None, None, None, None, false);
        let targets = &settings.targets;
        let days = |n: u64| Some(Duration::from_secs(n * 86_400));
        for (table, column, retention) in [
            // A default-partitioned table is a rows target, swept per partition.
            ("udb_authn.sessions", "expires_at", days(180)),
            // A revocation is aged from the token's expiry, never from when it
            // was revoked, so a still-valid token cannot be un-revoked.
            ("udb_authn.token_revocations", "expires_at", days(90)),
            ("udb_authn.token_families", "revoked_at", days(180)),
            ("udb_authn.mfa_challenges", "expires_at", days(30)),
            ("udb_authn.devices", "revoked_at", days(365)),
            ("udb_webhook.webhook_deliveries", "delivered_at", days(2555)),
        ] {
            let target = find_target(targets, table);
            assert_eq!(rows_column(target), column, "{table}");
            assert!(target.periodic && !target.capped, "{table}");
            assert_eq!(
                settings.retention.retention_for(target),
                retention,
                "{table}"
            );
        }
        // Partitioned logs with a declared retention now follow it by default.
        for table in ["api_key_usages", "notification_logs"] {
            let target = targets.iter().find(|t| t.table == table).unwrap();
            assert!(matches!(target.kind, TargetKind::Partitioned { .. }));
            assert!(
                settings.retention.retention_for(target).is_some(),
                "{table}"
            );
        }
        // Entities never age out by creation time; a lease tracks a live login.
        let manifest = crate::runtime::native_catalog::native_manifest();
        let table = |name: &str| {
            manifest
                .tables
                .iter()
                .find(|t| t.table == name)
                .unwrap_or_else(|| panic!("{name} is a native table"))
        };
        for name in [
            "users",
            "api_keys",
            "tenants",
            "vault_db_credential_leases",
            "roles",
        ] {
            assert!(
                row_end_column(table(name), manifest).is_err(),
                "{name} must not age out"
            );
            assert!(
                !targets.iter().any(|t| t.table == name && t.periodic),
                "{name}"
            );
        }
        // The list still overrides a table's own default.
        let set = CleanupSettings::from_values(
            None,
            None,
            None,
            None,
            Some("token_revocations=keep,webhook_deliveries=30d"),
            false,
        );
        assert_eq!(
            set.retention
                .retention_for(find_target(&set.targets, "udb_authn.token_revocations")),
            None
        );
        assert_eq!(
            set.retention
                .retention_for(find_target(&set.targets, "udb_webhook.webhook_deliveries")),
            days(30)
        );
    }

    /// UDB's bookkeeping tables keep only what is still needed, each with a
    /// default the retention list can change; they are outside the size cap.
    #[test]
    fn bookkeeping_tables_lose_only_finished_rows() {
        let settings = CleanupSettings::from_values(None, None, None, None, None, false);
        let config = crate::runtime::system::SystemCatalogConfig::current();
        let system = |table: &str| format!("{}.{table}", config.cdc.system_schema);
        let days = |n: u64| Some(Duration::from_secs(n * 86_400));
        for (table, only, retention) in [
            (
                &config.projection_tasks_table,
                "status = 'COMPLETED'",
                days(7),
            ),
            (&config.dlq_table, "'REPLAYED', 'DISMISSED'", days(30)),
            (&config.saga_table, "'committed', 'compensated'", days(30)),
            (
                &config.xa_ledger_table,
                "'committed', 'rolled_back'",
                days(30),
            ),
            (
                &config.catalog_versions_table,
                "status <> 'ACTIVE'",
                days(30),
            ),
            (&config.catalog_reload_log_table, "reload_seq <", days(90)),
            (
                &config.migration_runs_table,
                "state IN ('COMPLETED', 'DRY_RUN')",
                days(90),
            ),
        ] {
            let target = find_target(&settings.targets, &system(table));
            assert_eq!(target.service, SYSTEM_STORE);
            assert!(target.periodic && !target.capped);
            let TargetKind::Rows { only_when, .. } = &target.kind else {
                panic!("{table} is a rows target");
            };
            assert!(only_when.as_deref().unwrap().contains(only), "{table}");
            assert_eq!(
                settings.retention.retention_for(target),
                retention,
                "{table}"
            );
        }
        assert!(catalog_versions_removable("v", "b", 20).contains("ranked.position <= 20"));
        let kept = settings.clone().with_keep_versions(Some("5"));
        assert_eq!(kept.keep_versions, 5);
        let TargetKind::Rows { only_when, .. } =
            &find_target(&kept.targets, &system(&config.catalog_versions_table)).kind
        else {
            panic!("rows target");
        };
        assert!(
            only_when
                .as_deref()
                .unwrap()
                .contains("ranked.position <= 5")
        );
        assert_eq!(
            settings.clone().with_keep_versions(Some("0")).keep_versions,
            1
        );

        assert_eq!(settings.sweep_interval, DEFAULT_SWEEP_INTERVAL);
        let sweep = settings.clone().with_sweep_interval(Some("6h"));
        assert_eq!(sweep.sweep_interval, Duration::from_secs(6 * 3_600));
        assert_eq!(
            settings.with_sweep_interval(Some("soon")).sweep_interval,
            DEFAULT_SWEEP_INTERVAL
        );
    }

    /// Migration runs keep what the migration engine reads back: runs that
    /// are not finished, failed runs, each project's newest completed run and
    /// every run a reviewed catalog transition names. The admin audit log
    /// keeps the head of its hash chain.
    #[test]
    fn migration_runs_and_the_admin_audit_log_keep_what_is_read_back() {
        let settings = CleanupSettings::from_values(None, None, None, None, None, false);
        let config = crate::runtime::system::SystemCatalogConfig::current();
        let system = |table: &str| format!("{}.{table}", config.cdc.system_schema);

        let runs = find_target(&settings.targets, &system(&config.migration_runs_table));
        let TargetKind::Rows {
            column,
            only_when,
            guard,
            ..
        } = &runs.kind
        else {
            panic!("migration runs are a rows target");
        };
        assert_eq!(column, "started_at");
        let only_when = only_when.as_deref().unwrap();
        for kept in [
            "PREFLIGHT",
            "APPROVED",
            "APPLYING",
            "VERIFYING",
            "ERROR",
            "DEAD_LETTER",
        ] {
            assert!(!only_when.contains(kept), "{kept} runs stay");
        }
        assert!(only_when.contains("DISTINCT ON (m.project_id)"));
        assert!(only_when.contains(&config.migration_runs_relation()));
        let RowGuard::ReferencedBy(relation, key) = guard else {
            panic!("reviewed transitions keep runs");
        };
        assert!(relation.ends_with("\"udb_catalog_reviewed_transitions\""));
        assert_eq!(key, "run_id");

        let log = find_target(&settings.targets, &system(&config.admin_audit_log_table));
        let TargetKind::Rows {
            column,
            only_when,
            guard,
            ..
        } = &log.kind
        else {
            panic!("the admin audit log is a rows target");
        };
        assert_eq!(column, "created_at");
        assert_eq!(guard, &RowGuard::AdminAuditChain);
        assert!(only_when.is_none(), "the chain prune has its own rule");
        assert!(log.periodic && !log.capped);
        assert_eq!(
            settings.retention.retention_for(log),
            Some(Duration::from_secs(365 * 86_400))
        );

        // Both can be set or kept like any other table.
        let set = CleanupSettings::from_values(
            None,
            None,
            None,
            None,
            Some(&format!(
                "{}=keep,{}=30d",
                config.admin_audit_log_table, config.migration_runs_table
            )),
            false,
        );
        assert!(
            set.retention_errors().is_empty(),
            "{:?}",
            set.retention_errors()
        );
        assert_eq!(set.retention.retention_for(log), None);
        assert_eq!(
            set.retention.retention_for(runs),
            Some(Duration::from_secs(30 * 86_400))
        );
    }

    /// The admin audit prune takes a contiguous oldest-first prefix that stops
    /// before the chain's head and records the anchor in the same statement.
    #[test]
    fn the_admin_audit_prune_takes_the_front_of_the_chain_and_anchors_it() {
        let sql = admin_audit_prune_sql(
            "\"s\".\"log\"",
            "created_at",
            Duration::from_secs(365 * 86_400),
            500,
        );
        // The head: the newest hashed row.
        assert!(sql.contains(
            "SELECT h.\"created_at\" AS at, h.audit_id FROM \"s\".\"log\" h \
             WHERE h.current_hash <> '' ORDER BY h.\"created_at\" DESC, h.audit_id DESC LIMIT 1"
        ));
        // Oldest first, past retention, strictly before the head.
        assert!(sql.contains("l.\"created_at\" < now() - interval '31536000 seconds'"));
        assert!(sql.contains("(l.\"created_at\", l.audit_id) < (head.at, head.audit_id)"));
        assert!(sql.contains("ORDER BY l.\"created_at\", l.audit_id LIMIT 500"));
        // The anchor is the newest row deleted, written by the same statement.
        assert!(sql.contains("ORDER BY g.at DESC, g.audit_id DESC LIMIT 1"));
        assert!(sql.contains(&format!("INSERT INTO {EVIDENCE_STATE} AS s")));
        assert!(sql.contains("SELECT $1, n.audit_id::text, n.at, n.current_hash"));
        assert!(sql.contains("exported_count = s.exported_count + EXCLUDED.exported_count"));
        assert!(sql.ends_with("SELECT count(*) FROM gone"));
    }

    #[test]
    fn a_referenced_row_is_kept_only_when_the_referencing_table_exists() {
        assert_eq!(unreferenced(None, None), None);
        assert_eq!(
            unreferenced(Some("state = 'COMPLETED'"), None).as_deref(),
            Some("state = 'COMPLETED'")
        );
        assert_eq!(
            unreferenced(Some("a OR b"), Some(("s.t", "run_id"))).as_deref(),
            Some(
                "(a OR b) AND \"run_id\" NOT IN (SELECT r.\"run_id\" FROM s.t r \
                 WHERE r.\"run_id\" IS NOT NULL)"
            )
        );
        assert_eq!(
            unreferenced(None, Some(("s.t", "run_id"))).as_deref(),
            Some(
                "\"run_id\" NOT IN (SELECT r.\"run_id\" FROM s.t r WHERE r.\"run_id\" IS NOT NULL)"
            )
        );
    }

    #[test]
    fn a_further_condition_narrows_the_expired_rows() {
        let expiry = Expiry {
            column: "completed_at",
            rows: Duration::from_secs(86_400),
            kept: None,
            evidence_state_key: None,
            only_when: Some("status = 'COMPLETED'"),
        };
        assert_eq!(
            expiry.condition(),
            "\"completed_at\" < now() - interval '86400 seconds' AND (status = 'COMPLETED')"
        );
    }

    #[test]
    fn the_size_cap_parses_and_defaults_to_one_gigabyte() {
        assert_eq!(parse_size("1GB"), Ok(Some(1 << 30)));
        assert_eq!(parse_size(" 500 mb "), Ok(Some(500 << 20)));
        assert_eq!(parse_size("2g"), Ok(Some(2 << 30)));
        assert_eq!(parse_size("1048576"), Ok(Some(1 << 20)));
        assert_eq!(parse_size("off"), Ok(None));
        assert_eq!(parse_size("0"), Ok(None));
        assert!(parse_size("lots").unwrap_err().contains("1GB"));
        assert!(parse_size("3PB").unwrap_err().contains("unit"));
        let settings = CleanupSettings::from_values(None, None, None, None, None, false);
        assert_eq!(settings.max_size, Ok(Some(DEFAULT_MAX_SIZE)));
        assert_eq!(settings.describe_max_size(), "1GB");
        let off = settings.clone().with_max_size(Some("off"));
        assert_eq!(off.describe_max_size(), "off");
        let set = settings.with_max_size(Some("250MB"));
        assert_eq!(set.describe_max_size(), "250MB");
    }

    #[test]
    fn the_size_cap_cuts_the_oldest_bytes_first() {
        let sample = SizeSample {
            bytes: 100.0,
            rows: vec![(1_000.0, 40.0), (900.0, 30.0), (950.0, 30.0)],
        };
        // Remove 50 bytes: the rows at 900 (30) and 950 (30) go, 1000 stays.
        assert_eq!(
            cap_cutoff_age(&sample, 50.0, 1_100.0),
            Some(Duration::from_secs(149))
        );
        // Remove 10 bytes: the oldest row goes.
        assert_eq!(
            cap_cutoff_age(&sample, 10.0, 1_100.0),
            Some(Duration::from_secs(199))
        );
    }

    #[test]
    fn app_partition_upkeep_takes_app_tables_once_and_drops_only_when_asked() {
        let table = |schema: &str, name: &str, column: &str, days: i32| ManifestTable {
            schema: schema.to_string(),
            table: name.to_string(),
            proto_package: format!("meddesk.{schema}.entity.v1"),
            partition_strategy: "PARTITION_STRATEGY_RANGE_MONTH".to_string(),
            partition_column: column.to_string(),
            retention_days: days,
            ..ManifestTable::default()
        };
        let first = crate::generation::CatalogManifest {
            tables: vec![
                table("visits", "events", "occurred_at", 90),
                table("visits", "notes", "", 0),
                table("udb_authz", "access_decision_audits", "decided_at", 30),
            ],
            ..crate::generation::CatalogManifest::default()
        };
        let second = crate::generation::CatalogManifest {
            tables: vec![table("visits", "events", "occurred_at", 90)],
            ..crate::generation::CatalogManifest::default()
        };
        let kept = app_partition_targets([&first, &second].into_iter(), false);
        assert_eq!(kept.len(), 1, "one partitioned app table, once: {kept:?}");
        assert_eq!(kept[0].0.qualified(), "visits.events");
        assert!(matches!(
            &kept[0].0.kind,
            TargetKind::Partitioned { column, unit, .. } if column == "occurred_at" && unit == "month"
        ));
        assert_eq!(
            kept[0].1,
            Retention::default(),
            "nothing is dropped by default"
        );
        let dropped = app_partition_targets([&first].into_iter(), true);
        assert_eq!(
            dropped[0].1,
            Retention::uniform(Duration::from_secs(90 * 86_400))
        );
        let settings = CleanupSettings::from_values(None, None, None, None, None, false);
        assert!(!settings.app_retention);
        assert!(
            settings
                .clone()
                .with_app_retention(Some("declared"))
                .app_retention
        );
        assert!(!settings.with_app_retention(Some("on")).app_retention);
    }

    #[test]
    fn the_cleanup_statement_timeout_parses() {
        assert_eq!(parse_statement_timeout("0").unwrap(), "0");
        assert_eq!(parse_statement_timeout("off").unwrap(), "0");
        assert_eq!(parse_statement_timeout("30min").unwrap(), "30min");
        assert_eq!(parse_statement_timeout("90s").unwrap(), "90s");
        assert_eq!(parse_statement_timeout("1500").unwrap(), "1500ms");
        assert_eq!(parse_statement_timeout("2h").unwrap(), "2h");
        assert!(parse_statement_timeout("soon").is_err());
        assert!(parse_statement_timeout("3d").is_err());
    }

    #[test]
    fn reclaim_modes_parse() {
        assert_eq!(Reclaim::parse("vacuum"), Ok(Reclaim::Vacuum));
        assert_eq!(Reclaim::parse(" FULL "), Ok(Reclaim::Full));
        assert_eq!(Reclaim::parse("off"), Ok(Reclaim::Off));
        assert_eq!(Reclaim::parse("Online"), Ok(Reclaim::Online));
        assert!(
            Reclaim::parse("shrink")
                .unwrap_err()
                .contains("off, vacuum, full or online")
        );
    }

    #[test]
    fn relations_split_and_quote() {
        assert_eq!(
            split_relation("\"udb_metering\".\"usage_events\""),
            Some(("udb_metering".to_string(), "usage_events".to_string()))
        );
        assert_eq!(qi("a\"b"), "\"a\"\"b\"");
        assert_eq!(ql("it's"), "'it''s'");
    }
}
