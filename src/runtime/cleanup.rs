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
//! `udb serve` flag; `udb retention show` prints what is in force and
//! `udb retention run` runs one pass now.

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
                serde_json::json!({
                    "table": target.qualified(),
                    "kind": match target.kind {
                        TargetKind::Partitioned { .. } => "partitioned",
                        TargetKind::Rows { .. } => "rows",
                    },
                    "retention": config
                        .retention_for(target)
                        .map(describe_duration)
                        .unwrap_or_else(|| "keep".to_string()),
                    "declared": target.declared.map(describe_duration),
                })
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
pub const DEFAULT_CLEANUP_RETENTION: &str =
    "access_decision_audits=declared,auth_audit_log=declared,usage_events=declared";
const DEFAULT_INTERVAL: Duration = Duration::from_secs(600);
const MIN_RETENTION: Duration = Duration::from_secs(24 * 3600);
const DEFAULT_PREMAKE: i32 = 4;
const DEFAULT_ROW_BATCH: i64 = 5_000;
const MAX_BATCHES_PER_PASS: usize = 20;
const DECISION_AUDIT_MESSAGE: &str = "udb.core.authz.entity.v1.AccessDecisionAudit";
const USAGE_EVENT_MESSAGE: &str = "udb.core.metering.entity.v1.UsageEvent";

const AUTH_AUDIT_LOG: &str = "udb_system.auth_audit_log";
const EVIDENCE_STATE: &str = "udb_system.evidence_export_state";
const CDC_JOURNAL_TABLE: &str = "udb_cdc_event_journal";

/// How a table's rows age out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TargetKind {
    /// Range-partitioned by time: partitions are kept ahead and expired ones
    /// dropped.
    Partitioned {
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
    if secs % 86_400 == 0 {
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
    // The auth audit log has no proto: it is kept as long as the decision audit.
    let audit_declared = manifest_table(DECISION_AUDIT_MESSAGE)
        .and_then(|table| declared_days(table.retention_days));
    targets.push(Target {
        schema: "udb_system".to_string(),
        table: "auth_audit_log".to_string(),
        service: "compliance".to_string(),
        declared: audit_declared,
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
        kind: TargetKind::Partitioned {
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
        config.retention.insert(target.qualified(), retention);
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

/// One pass over every target whose store this broker can reach, with the
/// configured reclaim. Returns the partitions created and dropped plus the
/// rows deleted.
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
    )
    .await
    .and_then(|changed| reclaim.map(|_| changed))
}

/// One pass with an explicit reclaim mode. `reclaim_all` (`udb cleanup run
/// --reclaim full`) fully vacuums every log table, deleted rows or not; the
/// scheduled pass only reclaims tables it deleted rows from.
pub async fn run_cleanup_with(
    runtime: &crate::runtime::DataBrokerRuntime,
    settings: &CleanupSettings,
    reclaim: Reclaim,
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
            Ok(config) => Ok(config.retention_for(target)),
            Err(why) => Err(why.clone()),
        };
        match maintain_target(&pool, target, retention, settings.batch).await {
            Ok(count) => {
                changed += count;
                // Only row-deleted tables carry free space: a dropped
                // partition already gave its space back.
                let row_table = matches!(target.kind, TargetKind::Rows { .. });
                let vacuum = match reclaim {
                    Reclaim::Off => None,
                    Reclaim::Vacuum => (row_table && count > 0).then_some("VACUUM (ANALYZE)"),
                    Reclaim::Full => (row_table && (count > 0 || reclaim_all))
                        .then_some("VACUUM (FULL, ANALYZE)"),
                };
                if let Some(vacuum) = vacuum
                    && let Err(why) = reclaim_space(&pool, target, vacuum).await
                {
                    failures.push(format!("{}: {why}", target.qualified()));
                }
            }
            Err(why) => failures.push(format!("{}: {why}", target.qualified())),
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

/// Run `vacuum` on a table that exists. VACUUM cannot run in a transaction, so
/// it goes over the simple query protocol.
async fn reclaim_space(pool: &PgPool, target: &Target, vacuum: &str) -> Result<(), String> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(target.relation())
        .fetch_one(pool)
        .await
        .map_err(|err| format!("look up the table: {err}"))?;
    if !exists {
        return Ok(());
    }
    let sql = format!("{vacuum} {}", target.relation());
    sqlx::Executor::execute(pool, sqlx::raw_sql(&sql))
        .await
        .map(|_| {
            tracing::info!(table = %target.qualified(), statement = vacuum, "cleanup reclaimed space");
        })
        .map_err(|err| format!("{vacuum}: {err}"))
}

/// Upkeep for one table. `retention` is `Err` when the retention list is
/// malformed: partitions are still kept ahead, nothing is removed.
pub(crate) async fn maintain_target(
    pool: &PgPool,
    target: &Target,
    retention: Result<Option<Duration>, String>,
    batch: i64,
) -> Result<i64, String> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(target.relation())
        .fetch_one(pool)
        .await
        .map_err(|err| format!("look up the table: {err}"))?;
    if !exists {
        return Ok(0);
    }
    let retention = retention.ok().flatten();
    match &target.kind {
        TargetKind::Partitioned {
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
                return Ok(0);
            }
            if partman_manages(pool, target).await? {
                if let Some(retention) = retention {
                    set_partman_retention(pool, target, retention).await?;
                }
                return Ok(0);
            }
            let created =
                create_partitions_ahead(pool, target, unit, interval, suffix_format, *premake)
                    .await?;
            let dropped = match retention {
                Some(retention) => drop_expired_partitions(pool, target, retention).await?,
                None => 0,
            };
            Ok(created + dropped)
        }
        TargetKind::Rows {
            column,
            evidence_state_key,
        } => match retention {
            Some(retention) => {
                delete_expired_rows(
                    pool,
                    target,
                    column,
                    evidence_state_key.as_deref(),
                    retention,
                    batch,
                )
                .await
            }
            None => Ok(0),
        },
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

/// Delete rows older than the retention in bounded batches, each its own
/// transaction with the platform-admin escape (the tables FORCE row-level
/// security and retention is cross-tenant system work). With an evidence
/// export, rows it has not exported yet are kept.
async fn delete_expired_rows(
    pool: &PgPool,
    target: &Target,
    column: &str,
    evidence_state_key: Option<&str>,
    retention: Duration,
    batch: i64,
) -> Result<i64, String> {
    let relation = target.relation();
    let cutoff = match evidence_state_key {
        Some(_) => format!(
            "LEAST(now() - $1::interval, COALESCE((SELECT last_occurred_at FROM {EVIDENCE_STATE} \
             WHERE worker = $3), 'epoch'::timestamptz))"
        ),
        None => "now() - $1::interval".to_string(),
    };
    let sql = format!(
        "DELETE FROM {relation} WHERE ctid IN ( \
             SELECT ctid FROM {relation} WHERE {column} < {cutoff} LIMIT $2)",
        column = qi(column),
    );
    let evidence_ready = match evidence_state_key {
        Some(_) => sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
            .bind(EVIDENCE_STATE)
            .fetch_one(pool)
            .await
            .map_err(|err| format!("look up the evidence export state: {err}"))?,
        None => true,
    };
    if !evidence_ready {
        // The export is on but has never run: nothing is exported, so nothing
        // may be deleted.
        return Ok(0);
    }
    let batch = batch.max(1);
    let mut deleted = 0i64;
    for _ in 0..MAX_BATCHES_PER_PASS {
        let mut tx = pool.begin().await.map_err(|err| err.to_string())?;
        sqlx::query("SELECT set_config('app.platform_admin', 'true', true)")
            .execute(&mut *tx)
            .await
            .map_err(|err| err.to_string())?;
        let mut query = sqlx::query(&sql)
            .bind(format!("{} seconds", retention.as_secs()))
            .bind(batch);
        if let Some(key) = evidence_state_key {
            query = query.bind(key);
        }
        let done = query
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
    if deleted > 0 {
        tracing::info!(table = %target.qualified(), deleted, retention_secs = retention.as_secs(), "deleted rows past retention");
    }
    Ok(deleted)
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
            kind: TargetKind::Partitioned {
                unit: "month".to_string(),
                interval: "1 month".to_string(),
                suffix_format: "YYYYMM".to_string(),
                premake: 4,
            },
        };
        let month = Duration::from_secs(30 * 86_400);

        // Retention unset: partitions are created ahead, nothing is dropped.
        let created = maintain_target(&pool, &audit, Ok(None), 5_000)
            .await
            .unwrap();
        assert_eq!(created, 5, "the current month and four ahead");
        assert!(exists(format!("{schema}.audit_p_202001")).await);
        run(format!(
            "INSERT INTO {schema}.audit VALUES (1, now()), (2, now() + interval '3 months')"
        ))
        .await;

        // Retention set: the expired UDB partition goes, the hand-made one stays.
        let dropped = maintain_target(&pool, &audit, Ok(Some(month)), 5_000)
            .await
            .unwrap();
        assert_eq!(dropped, 1);
        assert!(!exists(format!("{schema}.audit_p_202001")).await);
        assert!(exists(format!("{schema}.audit_manual")).await);
        // A malformed retention list never removes anything.
        assert_eq!(
            maintain_target(&pool, &audit, Err("bad".to_string()), 5_000)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            maintain_target(&pool, &audit, Ok(Some(month)), 5_000)
                .await
                .unwrap(),
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
            kind: TargetKind::Rows {
                column: "occurred_at".to_string(),
                evidence_state_key: None,
            },
        };
        assert_eq!(maintain_target(&pool, &log, Ok(None), 2).await.unwrap(), 0);
        assert_eq!(
            maintain_target(&pool, &log, Ok(Some(month)), 2)
                .await
                .unwrap(),
            3
        );
        let left: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {schema}.log"))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(left, 2);

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
