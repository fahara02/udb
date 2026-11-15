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
//! `usage_events` = `declared`; `auth_audit_log` has no proto and follows the
//! decision audit's). `UDB_CLEANUP_RETENTION` (or `udb serve
//! --cleanup-retention`) is merged over that list, entry by entry, for example
//! `access_decision_audits=90d,usage_events=keep`. A value is a duration (`90d`,
//! `36h`), `declared` or `keep` (every row is kept). Any other time-partitioned
//! UDB table keeps every row unless it is listed. A table can be named by its
//! table name or as `schema.table`; the shortest retention accepted is one day.
//! An unknown table or a malformed value stops every retention pass with an
//! error naming it (partition upkeep continues).
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
//!   exported hash chain stays complete.
//! - The CDC journal has its own sweep: `UDB_CDC_JOURNAL_RETENTION`.
//!
//! **Reclaim.** Deleted rows leave free space inside the table file.
//! `UDB_CLEANUP_RECLAIM` decides what happens after a pass deleted rows from a
//! table: `vacuum` (default) runs `VACUUM (ANALYZE)` on it so the space is
//! reused at once and nothing is locked; `full` runs `VACUUM FULL`, which gives
//! the space back to the operating system but locks the table while it runs;
//! `off` leaves it to autovacuum. `udb cleanup run --reclaim full` does a full
//! reclaim once, for a maintenance window. Dropped partitions free their space
//! immediately.
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
}

impl Reclaim {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" | "none" | "0" | "false" => Ok(Reclaim::Off),
            "vacuum" | "" => Ok(Reclaim::Vacuum),
            "full" => Ok(Reclaim::Full),
            other => Err(format!(
                "{CLEANUP_RECLAIM_ENV}: `{other}` is not off, vacuum or full"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Reclaim::Off => "off",
            Reclaim::Vacuum => "vacuum",
            Reclaim::Full => "full",
        }
    }
}

/// Every cleanup setting, resolved ONCE (at worker spawn, or by the CLI) from
/// the environment, which `.env` and the `udb serve` setting flags feed. A
/// malformed retention list or reclaim mode is kept as its error: the pass
/// still keeps partitions ahead, removes nothing, and reports it.
#[derive(Debug, Clone)]
pub struct CleanupSettings {
    pub enabled: bool,
    pub interval: Duration,
    pub batch: i64,
    pub reclaim: Result<Reclaim, String>,
    pub(crate) targets: Vec<Target>,
    pub(crate) retention: Result<RetentionConfig, String>,
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
        let retention =
            parse_retention(DEFAULT_CLEANUP_RETENTION, &targets).and_then(|mut config| {
                if let Some(raw) = retention {
                    config
                        .retention
                        .extend(parse_retention(raw, &targets)?.retention);
                }
                Ok(config)
            });
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
            targets,
            retention,
        }
    }

    /// One entry per table, for `udb cleanup show` and the startup log: the
    /// retention in force (`keep`, days or hours) and the declared one.
    pub fn describe(&self) -> Result<Vec<serde_json::Value>, String> {
        let config = self.retention.clone()?;
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
     access_decision_audits:deny=declared,auth_audit_log=declared,usage_events=declared";
const DEFAULT_INTERVAL: Duration = Duration::from_secs(600);
const MIN_RETENTION: Duration = Duration::from_secs(24 * 3600);
const DEFAULT_PREMAKE: i32 = 4;
const DEFAULT_ROW_BATCH: i64 = 5_000;
const MAX_BATCHES_PER_PASS: usize = 20;
/// Heap pages one sweep statement covers (32 MiB at the default block size).
const SWEEP_PAGES: i64 = 4_096;
/// How often the scheduled pass sweeps rows out of the partitions it keeps.
/// Retention is set in days, so once a day is enough; `udb cleanup run`
/// always sweeps.
const PARTITION_SWEEP_EVERY: Duration = Duration::from_secs(24 * 3600);
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
    },
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
}

impl Outcome {
    fn total(self) -> i64 {
        self.created + self.dropped + self.deleted
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
    } else {
        format!("{}h", secs / 3_600)
    }
}

/// Every table the worker looks after: the time-partitioned native tables plus
/// the two unpartitioned append-only logs. With the evidence export on, the
/// auth audit log keeps rows it has not exported.
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
        },
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
            },
        });
    }
    targets
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
    let service = table
        .proto_package
        .split('.')
        .nth(2)
        .filter(|segment| !segment.is_empty())?
        .to_string();
    Some(Target {
        schema: table.schema.clone(),
        table: table.table.clone(),
        service,
        declared: declared_days(table.retention_days),
        kept_class: None,
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

/// Per table, when the scheduled pass last swept rows out of kept partitions.
fn last_partition_sweep()
-> &'static std::sync::Mutex<std::collections::HashMap<String, std::time::Instant>> {
    static LAST: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, std::time::Instant>>,
    > = std::sync::OnceLock::new();
    LAST.get_or_init(Default::default)
}

/// Whether the scheduled pass sweeps this table's kept partitions now (at most
/// once per [`PARTITION_SWEEP_EVERY`]).
fn partition_sweep_due(target: &Target) -> bool {
    let mut last = last_partition_sweep()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let now = std::time::Instant::now();
    match last.get(&target.qualified()) {
        Some(at) if now.duration_since(*at) < PARTITION_SWEEP_EVERY => false,
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
    reclaim: Reclaim,
    drain: bool,
    reclaim_all: bool,
) -> Result<i64, String> {
    let config = &settings.retention;
    let mut changed = 0i64;
    let mut failures = Vec::new();
    for target in &settings.targets {
        let Ok(pool) = runtime.native_store_pool_for_service(&target.service, true, "") else {
            continue;
        };
        let retention = match &config {
            Ok(config) => Ok(config.retention_of(target)),
            Err(why) => Err(why.clone()),
        };
        let partitioned = matches!(target.kind, TargetKind::Partitioned { .. });
        let mode = PassMode {
            batch: settings.batch,
            drain,
            sweep_partitions: partitioned && (drain || partition_sweep_due(target)),
        };
        match maintain_target(&pool, target, retention, mode).await {
            Ok(outcome) => {
                changed += outcome.total();
                // A dropped partition already gave its space back; deleted
                // rows leave free space in the table file.
                let vacuum = match reclaim {
                    Reclaim::Off => None,
                    Reclaim::Vacuum => {
                        (outcome.deleted > 0 || reclaim_all).then_some("VACUUM (ANALYZE)")
                    }
                    Reclaim::Full => {
                        (outcome.deleted > 0 || reclaim_all).then_some("VACUUM (FULL, ANALYZE)")
                    }
                };
                if let Some(vacuum) = vacuum
                    && let Err(why) = reclaim_space(&pool, &target.relation(), vacuum).await
                {
                    failures.push(format!("{}: {why}", target.qualified()));
                }
            }
            Err(why) => failures.push(format!("{}: {why}", target.qualified())),
        }
    }
    // The CDC journal prunes itself (UDB_CDC_JOURNAL_RETENTION); an explicit
    // reclaim gives the space its sweep freed back as well.
    if reclaim_all
        && reclaim != Reclaim::Off
        && let Some(pool) = runtime.pg_pool_clone()
    {
        let journal = crate::runtime::system::SystemCatalogConfig::current().cdc_journal_relation();
        let vacuum = if reclaim == Reclaim::Full {
            "VACUUM (FULL, ANALYZE)"
        } else {
            "VACUUM (ANALYZE)"
        };
        if let Err(why) = reclaim_space(&pool, &journal, vacuum).await {
            failures.push(format!("{journal}: {why}"));
        }
    }
    if let Err(why) = config {
        failures.insert(0, why.clone());
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
    let sql = format!("{vacuum} {relation}");
    sqlx::Executor::execute(pool, sqlx::raw_sql(&sql))
        .await
        .map(|_| {
            tracing::info!(table = %relation, statement = vacuum, "cleanup reclaimed space");
        })
        .map_err(|err| format!("{vacuum}: {err}"))
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
    let retention = retention.unwrap_or_default();
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
                    outcome.dropped = drop_expired_partitions(pool, target, kept).await?;
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
        } => {
            let Some(rows) = retention.rows else {
                return Ok(Outcome::default());
            };
            let expiry = Expiry {
                column,
                rows,
                kept: target
                    .kept_class
                    .as_ref()
                    .map(|class| (class, retention.kept)),
                evidence_state_key: evidence_state_key.as_deref(),
            };
            Ok(Outcome {
                deleted: delete_expired_rows(pool, target, &expiry, mode).await?,
                ..Outcome::default()
            })
        }
    }
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
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {schema}.{child} PARTITION OF {parent} \
             FOR VALUES FROM ({start}) TO ({end})",
            schema = qi(&target.schema),
            child = qi(&child),
            parent = target.relation(),
            start = ql(&start),
            end = ql(&end),
        );
        match sqlx::query(&ddl).execute(pool).await {
            Ok(_) => {
                created += 1;
                tracing::info!(table = %target.qualified(), partition = %child, "created partition ahead of writes");
            }
            // Another partition already covers the period (42P17), or a racing
            // creator won (42P07): nothing to do.
            Err(sqlx::Error::Database(db))
                if matches!(db.code().as_deref(), Some("42P17" | "42P07")) => {}
            Err(err) => return Err(format!("create partition {child}: {err}")),
        }
    }
    Ok(created)
}

/// Drop UDB-created partitions whose whole range is older than the retention.
async fn drop_expired_partitions(
    pool: &PgPool,
    target: &Target,
    retention: Duration,
) -> Result<i64, String> {
    let prefix = format!("{}_p_", target.table)
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    let children: Vec<String> = sqlx::query_scalar(
        "SELECT child FROM ( \
             SELECT c.relname::text AS child, \
                    (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'TO \\(''([^'']+)''\\)'))[1] AS upper_bound \
             FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
             WHERE i.inhparent = to_regclass($1) AND c.relname LIKE $2 ESCAPE '\\' \
         ) AS bounds \
         WHERE upper_bound IS NOT NULL \
           AND upper_bound::timestamptz <= now() - $3::interval \
         ORDER BY upper_bound",
    )
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
        format!("{column} < {cutoff}{class}")
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
    Ok(tx)
}

/// Delete an unpartitioned table's rows past retention. The scheduled pass
/// deletes bounded batches (at most [`MAX_BATCHES_PER_PASS`]); `udb cleanup
/// run` sweeps the whole table once, page range by page range, so a large
/// backlog costs one read of the table.
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
    let relation = target.relation();
    let deleted = if mode.drain {
        sweep_expired_rows(pool, &relation, expiry).await?
    } else {
        let batch = mode.batch.max(1);
        let sql = format!(
            "DELETE FROM {relation} WHERE ctid IN ( \
                 SELECT ctid FROM {relation} WHERE {} LIMIT {batch})",
            expiry.condition()
        );
        let mut deleted = 0i64;
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
        deleted
    };
    if deleted > 0 {
        tracing::info!(table = %target.qualified(), deleted, retention_secs = expiry.rows.as_secs(), "deleted rows past retention");
    }
    Ok(deleted)
}

/// Delete every row of `relation` past retention in one pass over the heap,
/// [`SWEEP_PAGES`] pages per statement (a TID range scan), each statement its
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
        let last = first + SWEEP_PAGES;
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
                    && t.table != "access_decision_audits"
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
        let audit = Target {
            schema: schema.clone(),
            table: "audit".to_string(),
            service: "authz".to_string(),
            declared: None,
            kept_class: None,
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
            kind: TargetKind::Rows {
                column: "occurred_at".to_string(),
                evidence_state_key: None,
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
        assert!(bad.describe().is_err());
    }

    #[test]
    fn reclaim_modes_parse() {
        assert_eq!(Reclaim::parse("vacuum"), Ok(Reclaim::Vacuum));
        assert_eq!(Reclaim::parse(" FULL "), Ok(Reclaim::Full));
        assert_eq!(Reclaim::parse("off"), Ok(Reclaim::Off));
        assert!(
            Reclaim::parse("shrink")
                .unwrap_err()
                .contains("off, vacuum or full")
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
