//! Native `MeteringService` (master-plan 9.9) — usage metering and quotas.
//!
//! Mirrors `lock_service`/`config_service`: proto-driven, no in-memory store, no
//! hand-mapped schema. Usage is an append-only, durable stream of `UsageEvent`
//! rows; quotas (`QuotaRule`) cap a metric over a rolling window. The tenant is
//! always taken from the VERIFIED claim, never the request body.
//!
//! Doctrine (Phase 9):
//! - **Metering NEVER fails the metered request.** The ingest seam
//!   [`record_usage`] is a single cheap INSERT that log-and-swallows on error and
//!   returns `Ok(())` — it is the seam the leader calls from the admission hook
//!   (`native_helpers::admit_on`).
//! - **Durable, not in-memory.** Usage is summed from durable rows (a counter in
//!   RAM lies across restarts and replicas). [`CheckQuota`](MeteringServiceImpl::
//!   check_quota) is PURE aggregation: a windowed durable SUM compared against the
//!   rule limit via the deterministic [`quota_decision`].
//! - **Quota checks fail closed.** Usage ingest remains best-effort so metering
//!   never fails the metered request, but explicit usage/quota reads must not
//!   fabricate a lower total when the durable aggregate is unavailable.
//! - **Resolve-once / outbox on change.** Window defaults are named consts;
//!   quota mutations bump a monotone per-row revision and emit a versioned
//!   dot-topic outbox event.

use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalValue,
};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::metering::services::v1 as metering_pb;
use crate::proto::udb::core::metering::services::v1::metering_service_server::MeteringService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, OperationChannel};
use crate::runtime::native_catalog::{NativeModel, native_model};

pub use crate::proto::udb::core::metering::services::v1::metering_service_server::MeteringServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    NativeEventContext, admit_on as native_admit_on, enqueue_outbox_event_with_context,
    native_next_page_token, native_offset_page_window, native_service_context, non_empty_json,
    validate_request_tenant,
};

const USAGE_EVENT_MSG: &str = "udb.core.metering.entity.v1.UsageEvent";
const QUOTA_RULE_MSG: &str = "udb.core.metering.entity.v1.QuotaRule";

const TOPIC_QUOTA_CHANGED: &str = "udb.metering.quota.changed.v1";
const TOPIC_USAGE_ROLLUP: &str = "udb.metering.rollup.v1";

/// Default rolling window when a caller/rule does not specify one (24h).
const DEFAULT_WINDOW_SECONDS: i64 = 86_400;
/// Billing/export rollup bucket width. Closed hourly windows are emitted by
/// default so a restarted leader can replay a bounded recent range.
const DEFAULT_ROLLUP_WINDOW_SECONDS: i64 = 3_600;
const DEFAULT_ROLLUP_LOOKBACK_SECONDS: i64 = 86_400;
const DEFAULT_ROLLUP_INTERVAL_SECS: u64 = 300;
const ROLLUP_WINDOW_ENV: &str = "UDB_METERING_ROLLUP_WINDOW_SECS";
const ROLLUP_LOOKBACK_ENV: &str = "UDB_METERING_ROLLUP_LOOKBACK_SECS";
const ROLLUP_INTERVAL_ENV: &str = "UDB_METERING_ROLLUP_INTERVAL_SECS";
pub(crate) const METERING_ROLLUP_BATCH: i64 = 200;
/// Default unit for an event with no explicit unit.
const DEFAULT_UNIT: &str = "request";
/// Unit emitted by the automatic fair-admission hook. The quantity is the same
/// bounded operation cost that `ChannelManager` accounts in metrics.
pub(crate) const ADMISSION_METERING_UNIT: &str = "admission_cost";
/// List defaults/caps so one tenant cannot scan an unbounded quota table.
const DEFAULT_LIST_LIMIT: u32 = 100;
const MAX_LIST_LIMIT: u32 = 1_000;

// ── pure helpers (unit-tested without Postgres) ────────────────────────────────

/// Inclusive lower bound (unix seconds) of a rolling window ending at `now`.
/// Saturating and floored at 0 so a giant window never underflows.
fn window_start_unix(now_unix: i64, window_seconds: i64) -> i64 {
    let window = window_seconds.max(0);
    now_unix.saturating_sub(window).max(0)
}

/// The window membership predicate the SQL filter mirrors: an event at exactly
/// the window start is INCLUDED (inclusive lower bound, `>=`). The aggregate
/// filter below uses [`ComparisonOp::Ge`] on the same boundary so this pure
/// predicate is the single definition of the boundary semantics. Used by the
/// boundary unit test; the runtime path expresses the same `>=` as a SQL filter.
#[cfg(test)]
fn event_in_window(event_unix: i64, window_start: i64) -> bool {
    event_unix >= window_start
}

/// The pure quota decision: a rule allows the request iff `used < limit`, and the
/// remaining headroom is `max(0, limit - used)`. Deterministic, no I/O — shared
/// with the test fixtures. (A `limit == 0` rule denies all; a negative limit is
/// rejected at PutQuota and never reaches here.)
fn quota_decision(used: i64, limit: i64) -> (bool, i64) {
    let allowed = used < limit;
    let remaining = limit.saturating_sub(used).max(0);
    (allowed, remaining)
}

/// Monotone per-row revision bump (saturating; never wraps/panics).
fn bump_revision(current: i64) -> i64 {
    current.saturating_add(1)
}

/// Wall-clock unix seconds (used only when the caller passes a non-positive ts).
fn wall_now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Public monotone-seconds source the leader can pass to [`record_usage`] from the
/// admission hook (kept here so the call site has a single obvious time source).
pub(crate) fn now_unix() -> i64 {
    wall_now_unix()
}

fn positive_i64_env(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

pub(crate) fn metering_rollup_interval() -> Duration {
    static INTERVAL: OnceLock<Duration> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        Duration::from_secs(
            std::env::var(ROLLUP_INTERVAL_ENV)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_ROLLUP_INTERVAL_SECS),
        )
    })
}

fn metering_rollup_window_seconds() -> i64 {
    static WINDOW: OnceLock<i64> = OnceLock::new();
    *WINDOW.get_or_init(|| positive_i64_env(ROLLUP_WINDOW_ENV, DEFAULT_ROLLUP_WINDOW_SECONDS))
}

fn metering_rollup_lookback_seconds() -> i64 {
    static LOOKBACK: OnceLock<i64> = OnceLock::new();
    *LOOKBACK.get_or_init(|| positive_i64_env(ROLLUP_LOOKBACK_ENV, DEFAULT_ROLLUP_LOOKBACK_SECONDS))
}

fn closed_rollup_upper_bound(now_unix: i64, window_seconds: i64) -> i64 {
    let window = window_seconds.max(1);
    (now_unix / window) * window
}

static ADMISSION_METERING_POOL: OnceLock<RwLock<Option<PgPool>>> = OnceLock::new();

fn admission_metering_cell() -> &'static RwLock<Option<PgPool>> {
    ADMISSION_METERING_POOL.get_or_init(|| RwLock::new(None))
}

/// Install or clear the process-local metering sink used by the native admission
/// hook. The pool itself is still resolved by `native_store_pool_for_service`
/// during service construction; hot-path callers only clone this already-routed
/// `PgPool` handle and never read env/config.
pub(crate) fn install_admission_metering_pool(pool: Option<PgPool>) {
    if let Ok(mut guard) = admission_metering_cell().write() {
        *guard = pool;
    }
}

/// Clone the installed admission-metering pool, if any. Poison/unconfigured both
/// fail open because usage metering must never fail an admitted native request.
pub(crate) fn admission_metering_pool() -> Option<PgPool> {
    admission_metering_cell().read().ok()?.clone()
}

/// Canonical metric/method label for the automatic admission hook. Kept pure so
/// quotas can target the same string deterministically.
pub(crate) fn admission_metering_method(service_label: &str, op_label: &str) -> String {
    let service = service_label.trim();
    let op = op_label.trim();
    match (service.is_empty(), op.is_empty()) {
        (true, true) => "native.unknown".to_string(),
        (true, false) => format!("native.{op}"),
        (false, true) => service.to_string(),
        (false, false) => format!("{service}.{op}"),
    }
}

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

fn metering_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

fn metering_nonnegative_field(field: &'static str, message: &'static str) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field, "must be greater than or equal to 0")],
    )
}

fn usage_event_model() -> NativeModel {
    native_model(
        USAGE_EVENT_MSG,
        &[
            "tenant_id",
            "method",
            "unit",
            "quantity",
            "occurred_at_unix",
        ],
    )
}

fn install_metering_tenant_scope_sql() -> &'static str {
    "SELECT set_config('app.current_tenant_id', $1, true)"
}

fn windowed_usage_sum_sql() -> &'static str {
    "SELECT COALESCE(SUM(quantity), 0)::bigint \
     FROM udb_metering.usage_events \
     WHERE tenant_id = $1 AND method = $2 AND occurred_at_unix >= $3"
}

// ── the durable ingest seam (the admission-hook call target) ───────────────────

/// Append ONE durable usage event. This is the seam the leader calls from
/// `native_helpers::admit_on`: a single cheap INSERT, no read.
///
/// Metering must NEVER fail the request that triggered it, so any store error is
/// logged and swallowed and the function returns `Ok(())`. The INSERT sets the
/// per-statement tenant GUC via `set_config(..., is_local=true)` so the row's RLS
/// `WITH CHECK` passes even on a raw pooled connection (correct whether or not the
/// table is `FORCE ROW LEVEL SECURITY`); `usage_id`/audit columns fall to their
/// DB defaults.
pub(crate) async fn record_usage(
    pool: &PgPool,
    tenant_id: &str,
    principal_id: &str,
    method: &str,
    unit: &str,
    quantity: i64,
    now_unix: i64,
) -> Result<(), Status> {
    let tenant_id = tenant_id.trim();
    let method = method.trim();
    if tenant_id.is_empty() || method.is_empty() {
        // Nothing to attribute — never an error surfaced to the metered caller.
        return Ok(());
    }
    let unit = {
        let u = unit.trim();
        if u.is_empty() { DEFAULT_UNIT } else { u }
    };
    let quantity = quantity.max(0);
    let occurred = if now_unix > 0 {
        now_unix
    } else {
        wall_now_unix()
    };

    // Single statement: the WHERE both sets the tenant GUC (so RLS WITH CHECK
    // passes) and is always true for a non-empty tenant, then the SELECT row is
    // inserted. No separate read; no timestamptz/uuid text bind.
    let res = sqlx::query(
        "INSERT INTO udb_metering.usage_events \
         (tenant_id, principal_id, method, unit, quantity, occurred_at, occurred_at_unix) \
         SELECT $1, $2, $3, $4, $5, to_timestamp($6), $7 \
         WHERE set_config('app.current_tenant_id', $1, true) IS NOT NULL",
    )
    .bind(tenant_id)
    .bind(principal_id.trim())
    .bind(method)
    .bind(unit)
    .bind(quantity)
    .bind(occurred as f64)
    .bind(occurred)
    .execute(pool)
    .await;

    if let Err(err) = res {
        tracing::warn!(
            target: "udb::metering",
            error = %err,
            tenant_id = %tenant_id,
            method = %method,
            "usage metering insert failed; swallowing so metering never fails the metered request",
        );
    }
    Ok(())
}

// ── JSON row decoders (mirror lock_service/config_service) ─────────────────────

fn row_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

fn json_str(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(serde_json::Value::Number(value)) => value.to_string(),
        Some(serde_json::Value::Bool(value)) => value.to_string(),
        _ => String::new(),
    }
}

fn json_i64(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    match row.get(key) {
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0),
        Some(serde_json::Value::String(value)) => value.trim().parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

fn json_bool(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> bool {
    match row.get(key) {
        Some(serde_json::Value::Bool(value)) => *value,
        Some(serde_json::Value::String(value)) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "t"
        ),
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0) != 0,
        _ => false,
    }
}

/// A durable quota rule decoded from a native read.
struct StoredQuota {
    quota_id: String,
    limit_value: i64,
    window_seconds: i64,
    enabled: bool,
    revision: i64,
}

fn stored_quota_from_json(row: &serde_json::Value) -> StoredQuota {
    let map = row_object(row);
    StoredQuota {
        quota_id: json_str(map, "quota_id"),
        limit_value: json_i64(map, "limit_value"),
        window_seconds: json_i64(map, "window_seconds"),
        enabled: json_bool(map, "enabled"),
        revision: json_i64(map, "revision"),
    }
}

fn quota_state_from_json(row: &serde_json::Value, tenant_id: &str) -> metering_pb::QuotaState {
    let map = row_object(row);
    metering_pb::QuotaState {
        tenant_id: {
            let stored = json_str(map, "tenant_id");
            if stored.is_empty() {
                tenant_id.to_string()
            } else {
                stored
            }
        },
        project_id: json_str(map, "project_id"),
        metric: json_str(map, "metric"),
        limit_value: json_i64(map, "limit_value"),
        window_seconds: json_i64(map, "window_seconds"),
        enabled: json_bool(map, "enabled"),
        revision: json_i64(map, "revision"),
        metadata_json: {
            let m = json_str(map, "metadata_json");
            if m.is_empty() { "{}".to_string() } else { m }
        },
    }
}

// ── IR builders (tenant-scoped reads/writes via the neutral dispatch) ──────────

fn quota_filter(tenant_id: &str, project_id: &str, metric: Option<&str>) -> LogicalFilter {
    let mut filters = vec![
        LogicalFilter::Comparison {
            field: "tenant_id".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(tenant_id),
        },
        LogicalFilter::Comparison {
            field: "project_id".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(project_id),
        },
    ];
    if let Some(metric) = metric {
        filters.push(LogicalFilter::Comparison {
            field: "metric".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(metric),
        });
    }
    LogicalFilter::And(filters)
}

fn quota_read_exact(tenant_id: &str, project_id: &str, metric: &str) -> LogicalRead {
    LogicalRead {
        message_type: QUOTA_RULE_MSG.to_string(),
        filter: Some(quota_filter(tenant_id, project_id, Some(metric))),
        projection: Some(LogicalProjection::fields([
            "quota_id".to_string(),
            "tenant_id".to_string(),
            "project_id".to_string(),
            "metric".to_string(),
            "limit_value".to_string(),
            "window_seconds".to_string(),
            "enabled".to_string(),
            "revision".to_string(),
            "metadata_json".to_string(),
        ])),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

fn quota_list_read(tenant_id: &str, project_id: &str, offset: u64, limit: u32) -> LogicalRead {
    // project_id empty narrows to tenant-wide rules of that exact (empty) scope;
    // a non-empty project narrows to that project. Either way RLS scopes to tenant.
    LogicalRead {
        message_type: QUOTA_RULE_MSG.to_string(),
        filter: Some(quota_filter(tenant_id, project_id, None)),
        projection: Some(LogicalProjection::fields([
            "tenant_id".to_string(),
            "project_id".to_string(),
            "metric".to_string(),
            "limit_value".to_string(),
            "window_seconds".to_string(),
            "enabled".to_string(),
            "revision".to_string(),
            "metadata_json".to_string(),
        ])),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

/// Windowed usage filter: tenant + metric (matched against `method`) +
/// `occurred_at_unix >= window_start` (inclusive lower bound — same `>=`
/// boundary as the `event_in_window` test predicate).
fn usage_window_filter(tenant_id: &str, metric: &str, window_start: i64) -> LogicalFilter {
    LogicalFilter::And(vec![
        LogicalFilter::Comparison {
            field: "tenant_id".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(tenant_id),
        },
        LogicalFilter::Comparison {
            field: "method".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(metric),
        },
        LogicalFilter::Comparison {
            field: "occurred_at_unix".to_string(),
            op: ComparisonOp::Ge,
            value: LogicalValue::Int(window_start),
        },
    ])
}

#[allow(clippy::too_many_arguments)]
fn quota_record(
    quota_id: &str,
    tenant_id: &str,
    project_id: &str,
    metric: &str,
    limit_value: i64,
    window_seconds: i64,
    enabled: bool,
    revision: i64,
    metadata_json: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("quota_id".to_string(), logical_string(quota_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("project_id".to_string(), logical_string(project_id));
    record.insert("metric".to_string(), logical_string(metric));
    record.insert("limit_value".to_string(), LogicalValue::Int(limit_value));
    record.insert(
        "window_seconds".to_string(),
        LogicalValue::Int(window_seconds),
    );
    record.insert("enabled".to_string(), LogicalValue::Bool(enabled));
    record.insert("revision".to_string(), LogicalValue::Int(revision));
    record.insert("metadata_json".to_string(), logical_string(metadata_json));
    record
}

fn quota_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "limit_value".to_string(),
        "window_seconds".to_string(),
        "enabled".to_string(),
        "revision".to_string(),
        "metadata_json".to_string(),
    ])
}

#[derive(Debug, Clone)]
struct UsageRollup {
    rollup_id: String,
    tenant_id: String,
    method: String,
    unit: String,
    window_start_unix: i64,
    window_end_unix: i64,
    quantity: i64,
    event_count: i64,
}

#[cfg(test)]
fn rollup_id(
    tenant_id: &str,
    method: &str,
    unit: &str,
    window_start_unix: i64,
    window_end_unix: i64,
) -> String {
    format!("{tenant_id}:{method}:{unit}:{window_start_unix}:{window_end_unix}")
}

async fn load_usage_rollups(
    pool: &PgPool,
    outbox_relation: &str,
    journal_relation: &str,
    batch: i64,
    window_seconds: i64,
    lookback_seconds: i64,
) -> Result<Vec<UsageRollup>, String> {
    let usage = usage_event_model();
    let rel = usage.relation.clone();
    let tenant = usage.q("tenant_id");
    let method = usage.q("method");
    let unit = usage.q("unit");
    let quantity = usage.q("quantity");
    let ts = usage.q("occurred_at_unix");
    let window = window_seconds.max(1);
    let upper = closed_rollup_upper_bound(now_unix(), window);
    if upper <= 0 {
        return Ok(Vec::new());
    }
    let lower = upper.saturating_sub(lookback_seconds.max(window));
    let limit = batch.max(1);
    let window_expr = format!("(({ts} / $1::BIGINT) * $1::BIGINT)");
    let rollup_id_expr = "CONCAT(r.tenant_id, ':', r.method, ':', r.unit, ':', r.window_start_unix::TEXT, ':', r.window_end_unix::TEXT)";
    let rows = sqlx::query(&format!(
        "WITH rollups AS ( \
             SELECT \
               {tenant}::TEXT AS tenant_id, \
               {method}::TEXT AS method, \
               COALESCE(NULLIF({unit}::TEXT, ''), $6) AS unit, \
               {window_expr}::BIGINT AS window_start_unix, \
               ({window_expr} + $1::BIGINT)::BIGINT AS window_end_unix, \
               COALESCE(SUM(GREATEST({quantity}, 0)), 0)::BIGINT AS quantity, \
               COUNT(*)::BIGINT AS event_count \
             FROM {rel} \
             WHERE {ts} >= $2 \
               AND {ts} < $3 \
               AND COALESCE({tenant}::TEXT, '') <> '' \
               AND COALESCE({method}::TEXT, '') <> '' \
             GROUP BY {tenant}, {method}, COALESCE(NULLIF({unit}::TEXT, ''), $6), {window_expr} \
         ) \
         SELECT r.*, {rollup_id_expr} AS rollup_id \
         FROM rollups r \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM {outbox_relation} o \
             WHERE o.topic = $4 \
               AND COALESCE(o.payload->>'rollup_id', o.payload->'payload'->>'rollup_id') = {rollup_id_expr} \
         ) \
           AND NOT EXISTS ( \
             SELECT 1 FROM {journal_relation} j \
             WHERE j.topic = $4 \
               AND COALESCE(j.payload->>'rollup_id', j.payload->'payload'->>'rollup_id') = {rollup_id_expr} \
         ) \
         ORDER BY r.window_start_unix ASC, r.tenant_id ASC, r.method ASC, r.unit ASC \
         LIMIT $5"
    ))
    .bind(window)
    .bind(lower)
    .bind(upper)
    .bind(TOPIC_USAGE_ROLLUP)
    .bind(limit)
    .bind(DEFAULT_UNIT)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load metering usage rollups failed: {err}"))?;

    rows.into_iter()
        .map(|row| {
            Ok(UsageRollup {
                rollup_id: row
                    .try_get::<String, _>("rollup_id")
                    .map_err(|err| format!("metering rollup_id decode failed: {err}"))?,
                tenant_id: row
                    .try_get::<String, _>("tenant_id")
                    .map_err(|err| format!("metering rollup tenant_id decode failed: {err}"))?,
                method: row
                    .try_get::<String, _>("method")
                    .map_err(|err| format!("metering rollup method decode failed: {err}"))?,
                unit: row
                    .try_get::<String, _>("unit")
                    .map_err(|err| format!("metering rollup unit decode failed: {err}"))?,
                window_start_unix: row.try_get::<i64, _>("window_start_unix").map_err(|err| {
                    format!("metering rollup window_start_unix decode failed: {err}")
                })?,
                window_end_unix: row.try_get::<i64, _>("window_end_unix").map_err(|err| {
                    format!("metering rollup window_end_unix decode failed: {err}")
                })?,
                quantity: row
                    .try_get::<i64, _>("quantity")
                    .map_err(|err| format!("metering rollup quantity decode failed: {err}"))?,
                event_count: row
                    .try_get::<i64, _>("event_count")
                    .map_err(|err| format!("metering rollup event_count decode failed: {err}"))?,
            })
        })
        .collect()
}

/// Run one leader-owned rollup pass over durable UsageEvent rows. The pass emits
/// one `udb.metering.rollup.v1` outbox record per closed
/// tenant/method/unit/window bucket, deduped by deterministic `rollup_id` against
/// both outbox and CDC journal. It never deletes raw usage events.
pub(crate) async fn run_metering_rollup_once(
    pool: &PgPool,
    outbox_relation: &str,
    journal_relation: &str,
    batch: i64,
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) -> Result<i64, String> {
    let rollups = load_usage_rollups(
        pool,
        outbox_relation,
        journal_relation,
        batch,
        metering_rollup_window_seconds(),
        metering_rollup_lookback_seconds(),
    )
    .await?;
    let mut emitted = 0i64;
    for rollup in rollups {
        let payload = serde_json::json!({
            "rollup_id": &rollup.rollup_id,
            "tenant_id": &rollup.tenant_id,
            "method": &rollup.method,
            "unit": &rollup.unit,
            "window_start_unix": rollup.window_start_unix,
            "window_end_unix": rollup.window_end_unix,
            "quantity": rollup.quantity,
            "event_count": rollup.event_count,
            "window_seconds": rollup.window_end_unix.saturating_sub(rollup.window_start_unix),
        });
        enqueue_outbox_event_with_context(
            pool,
            Some(outbox_relation),
            TOPIC_USAGE_ROLLUP,
            &rollup.rollup_id,
            &rollup.tenant_id,
            "",
            payload,
            NativeEventContext {
                operation: "metering.rollup".to_string(),
                target_resource: rollup.method,
                ..NativeEventContext::default()
            },
            metrics,
        )
        .await;
        emitted = emitted.saturating_add(1);
    }
    Ok(emitted)
}

/// Postgres-backed `MeteringService` handler.
pub struct MeteringServiceImpl {
    /// Outbox-event Postgres pool (also the raw target for `record_usage`).
    pg_pool: Option<PgPool>,
    /// Runtime handle for typed native-entity dispatch (reads/writes/aggregates).
    runtime: Option<Arc<DataBrokerRuntime>>,
    /// Configured outbox relation; `None` disables event emission (best-effort).
    outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
}

fn metering_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "metering",
        operation,
        capability_required,
        message,
    )
}

fn metering_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("metering", operation, message)
}

impl MeteringServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Quota state is durable-only: fail closed when no runtime dispatch exists.
    fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            metering_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "metering service requires runtime native-entity dispatch (no runtime configured)",
            )
        })
    }

    /// Emit the per-mutation versioned dot-topic outbox event (best-effort).
    async fn emit_quota_changed(
        &self,
        tenant_id: &str,
        project_id: &str,
        metric: &str,
        revision: i64,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        let payload = serde_json::json!({
            "tenant_id": tenant_id,
            "project_id": project_id,
            "metric": metric,
            "revision": revision,
        });
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_QUOTA_CHANGED,
            metric,
            tenant_id,
            project_id,
            payload,
            NativeEventContext {
                target_resource: metric.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }

    /// The durable windowed usage SUM for (tenant, metric). `Ok(used)` on success;
    /// `Err` is propagated so callers can choose their availability posture.
    async fn windowed_usage(
        &self,
        runtime: &DataBrokerRuntime,
        context: &crate::RequestContext,
        tenant_id: &str,
        metric: &str,
        window_start: i64,
    ) -> Result<i64, Status> {
        // Install the tenant RLS GUC before scanning usage_events. Installing it
        // inside the aggregate predicate is too late for PostgreSQL RLS planning:
        // the policy may evaluate current_setting(...) before the user WHERE
        // expression runs, which under-reports live usage as zero.
        let _ = (runtime, context);
        let Some(pool) = self.pg_pool.as_ref() else {
            return Err(metering_capability_status(
                "query_usage",
                "postgres_store",
                "metering usage pool is not configured",
            ));
        };
        let mut tx = pool.begin().await.map_err(|err| {
            metering_internal_status(
                "windowed_usage_begin",
                format!("windowed usage transaction failed: {err}"),
            )
        })?;
        sqlx::query(install_metering_tenant_scope_sql())
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                metering_internal_status(
                    "windowed_usage_tenant_scope",
                    format!("windowed usage tenant scope failed: {err}"),
                )
            })?;
        let total: i64 = sqlx::query_scalar(windowed_usage_sum_sql())
            .bind(tenant_id)
            .bind(metric)
            .bind(window_start)
            .fetch_one(&mut *tx)
            .await
            .map_err(|err| {
                metering_internal_status(
                    "windowed_usage_aggregate",
                    format!("windowed usage aggregate failed: {err}"),
                )
            })?;
        tx.commit().await.map_err(|err| {
            metering_internal_status(
                "windowed_usage_commit",
                format!("windowed usage transaction commit failed: {err}"),
            )
        })?;
        Ok(total)
    }
}

impl Default for MeteringServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl MeteringService for MeteringServiceImpl {
    async fn record_usage(
        &self,
        request: Request<metering_pb::RecordUsageRequest>,
    ) -> Result<Response<metering_pb::RecordUsageResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // Cross-tenant guard FIRST: the body tenant_id must match the verified
        // claim/header. After this passes, the body value IS the verified tenant.
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let method = req.method.trim().to_string();
        if method.is_empty() {
            return Err(metering_required_field(
                "method",
                "must be a non-empty usage method",
                "method is required",
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "metering",
            OperationChannel::Write,
            &tenant_id,
            None,
        )
        .await?;

        // No store → metering is best-effort; never an error to the caller.
        let Some(pool) = self.pg_pool.as_ref() else {
            return Ok(Response::new(metering_pb::RecordUsageResponse {
                recorded: false,
                message: "no metering store configured".to_string(),
                error: None,
            }));
        };

        let occurred = if req.occurred_at_unix > 0 {
            req.occurred_at_unix
        } else {
            now_unix()
        };
        // Single durable append through the shared ingest seam (swallows errors).
        record_usage(
            pool,
            &tenant_id,
            req.principal_id.trim(),
            &method,
            req.unit.trim(),
            req.quantity,
            occurred,
        )
        .await?;

        Ok(Response::new(metering_pb::RecordUsageResponse {
            recorded: true,
            message: "usage recorded".to_string(),
            error: None,
        }))
    }

    async fn query_usage(
        &self,
        request: Request<metering_pb::QueryUsageRequest>,
    ) -> Result<Response<metering_pb::QueryUsageResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let metric = req.metric.trim().to_string();
        if metric.is_empty() {
            return Err(metering_required_field(
                "metric",
                "must be a non-empty metric name",
                "metric is required",
            ));
        }
        let window_seconds = if req.window_seconds > 0 {
            req.window_seconds
        } else {
            DEFAULT_WINDOW_SECONDS
        };
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "metering",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let to_unix = now_unix();
        let from_unix = window_start_unix(to_unix, window_seconds);
        let used = self
            .windowed_usage(runtime, &context, &tenant_id, &metric, from_unix)
            .await
            .map_err(|err| {
                tracing::warn!(
                    target: "udb::metering",
                    error = %err,
                    tenant_id = %tenant_id,
                    metric = %metric,
                    "windowed usage aggregate failed; refusing to fabricate usage total",
                );
                err
            })?;

        Ok(Response::new(metering_pb::QueryUsageResponse {
            metric,
            used,
            window_seconds,
            from_unix,
            to_unix,
            message: String::new(),
            error: None,
        }))
    }

    async fn put_quota(
        &self,
        request: Request<metering_pb::PutQuotaRequest>,
    ) -> Result<Response<metering_pb::PutQuotaResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let project_id = req.project_id.trim().to_string();
        let metric = req.metric.trim().to_string();
        if metric.is_empty() {
            return Err(metering_required_field(
                "metric",
                "must be a non-empty metric name",
                "metric is required",
            ));
        }
        if req.limit_value < 0 {
            return Err(metering_nonnegative_field(
                "limit_value",
                "limit_value must be >= 0",
            ));
        }
        if req.window_seconds < 0 {
            return Err(metering_nonnegative_field(
                "window_seconds",
                "window_seconds must be >= 0",
            ));
        }
        let window_seconds = if req.window_seconds > 0 {
            req.window_seconds
        } else {
            DEFAULT_WINDOW_SECONDS
        };
        let metadata_json = non_empty_json(&req.metadata_json);

        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "metering",
            OperationChannel::Write,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, &project_id);

        // Existing row at this exact scope (for stable quota_id + revision bump).
        let existing = runtime
            .native_entity_read_for_service(
                "metering",
                &context,
                quota_read_exact(&tenant_id, &project_id, &metric),
            )
            .await?
            .first()
            .map(stored_quota_from_json);

        let quota_id = existing
            .as_ref()
            .map(|q| q.quota_id.clone())
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let revision = bump_revision(existing.as_ref().map(|q| q.revision).unwrap_or(0));

        runtime
            .native_entity_write_for_service(
                "metering",
                &context,
                QUOTA_RULE_MSG,
                quota_record(
                    &quota_id,
                    &tenant_id,
                    &project_id,
                    &metric,
                    req.limit_value,
                    window_seconds,
                    req.enabled,
                    revision,
                    &metadata_json,
                ),
                quota_conflict(),
            )
            .await?;

        self.emit_quota_changed(&tenant_id, &project_id, &metric, revision)
            .await;

        Ok(Response::new(metering_pb::PutQuotaResponse {
            stored: true,
            metric,
            revision,
            message: "quota stored".to_string(),
            error: None,
        }))
    }

    async fn get_quota(
        &self,
        request: Request<metering_pb::GetQuotaRequest>,
    ) -> Result<Response<metering_pb::GetQuotaResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let project_id = req.project_id.trim().to_string();
        let metric = req.metric.trim().to_string();
        if metric.is_empty() {
            return Err(metering_required_field(
                "metric",
                "must be a non-empty metric name",
                "metric is required",
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "metering",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, &project_id);

        let found = runtime
            .native_entity_read_for_service(
                "metering",
                &context,
                quota_read_exact(&tenant_id, &project_id, &metric),
            )
            .await?
            .first()
            .map(|row| quota_state_from_json(row, &tenant_id));

        Ok(Response::new(metering_pb::GetQuotaResponse {
            found: found.is_some(),
            quota: found,
            message: String::new(),
            error: None,
        }))
    }

    async fn list_quotas(
        &self,
        request: Request<metering_pb::ListQuotasRequest>,
    ) -> Result<Response<metering_pb::ListQuotasResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let project_id = req.project_id.trim().to_string();
        let legacy_limit = if req.limit == 0 {
            DEFAULT_LIST_LIMIT
        } else {
            req.limit.min(MAX_LIST_LIMIT)
        };
        let requested_page_size = if req.page_size > 0 {
            req.page_size
        } else {
            legacy_limit as i32
        };
        let page_window = native_offset_page_window(
            1,
            requested_page_size,
            &req.page_token,
            DEFAULT_LIST_LIMIT as i32,
        );
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "metering",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, &project_id);

        let rows = runtime
            .native_entity_read_for_service(
                "metering",
                &context,
                quota_list_read(
                    &tenant_id,
                    &project_id,
                    page_window.offset as u64,
                    (page_window.limit as u32).min(MAX_LIST_LIMIT),
                ),
            )
            .await?;
        let quotas = rows
            .iter()
            .map(|row| quota_state_from_json(row, &tenant_id))
            .collect::<Vec<_>>();
        let next_page_token =
            native_next_page_token(page_window.offset, page_window.limit, quotas.len());

        Ok(Response::new(metering_pb::ListQuotasResponse {
            quotas,
            message: String::new(),
            error: None,
            next_page_token,
        }))
    }

    async fn check_quota(
        &self,
        request: Request<metering_pb::CheckQuotaRequest>,
    ) -> Result<Response<metering_pb::CheckQuotaResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let project_id = req.project_id.trim().to_string();
        let metric = req.metric.trim().to_string();
        if metric.is_empty() {
            return Err(metering_required_field(
                "metric",
                "must be a non-empty metric name",
                "metric is required",
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "metering",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, &project_id);

        // Resolve the governing rule. No enabled rule → unenforced (allowed).
        let rule = runtime
            .native_entity_read_for_service(
                "metering",
                &context,
                quota_read_exact(&tenant_id, &project_id, &metric),
            )
            .await?
            .first()
            .map(stored_quota_from_json)
            .filter(|q| q.enabled);

        let Some(rule) = rule else {
            return Ok(Response::new(metering_pb::CheckQuotaResponse {
                allowed: true,
                used: 0,
                limit_value: 0,
                remaining: 0,
                unlimited: true,
                message: "no enabled quota rule for metric".to_string(),
                error: None,
            }));
        };

        let window_seconds = if rule.window_seconds > 0 {
            rule.window_seconds
        } else {
            DEFAULT_WINDOW_SECONDS
        };
        let window_start = window_start_unix(now_unix(), window_seconds);

        match self
            .windowed_usage(runtime, &context, &tenant_id, &metric, window_start)
            .await
        {
            Ok(used) => {
                let (allowed, remaining) = quota_decision(used, rule.limit_value);
                Ok(Response::new(metering_pb::CheckQuotaResponse {
                    allowed,
                    used,
                    limit_value: rule.limit_value,
                    remaining,
                    unlimited: false,
                    message: if allowed {
                        "within quota".to_string()
                    } else {
                        "quota exceeded".to_string()
                    },
                    error: None,
                }))
            }
            Err(err) => {
                tracing::warn!(
                    target: "udb::metering",
                    error = %err,
                    tenant_id = %tenant_id,
                    metric = %metric,
                    "quota usage aggregate failed; failing closed",
                );
                Err(crate::runtime::executor_utils::retryable_status(
                    "metering",
                    "quota_aggregate",
                    crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                    "quota usage aggregate unavailable; failing closed",
                ))
            }
        }
    }
}

impl DataBrokerService {
    /// Build the native `MeteringService`, wired to the broker's Postgres pool,
    /// the native-entity dispatch runtime, and the shared outbox.
    pub(crate) fn build_metering_service(&self) -> MeteringServiceImpl {
        let runtime = self.runtime.load_full();
        let pg_pool = runtime
            .native_store_pool_for_service("metering", true, "")
            .ok();
        install_admission_metering_pool(pg_pool.clone());
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        MeteringServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}

#[cfg(test)]
mod metering_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use prost::Message as _;
    use tonic::metadata::MetadataValue;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "metering");
        assert_eq!(detail.operation, operation);
        assert!(detail.capability_required.is_empty());
        assert!(detail.policy_decision_id.is_empty());
        assert!(detail.field_violations.is_empty());
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    async fn live_metering_fixture() -> (sqlx::PgPool, MeteringServiceImpl, String, String) {
        let dsn = std::env::var("UDB_LIVE_NATIVE_PG_DSN")
            .or_else(|_| std::env::var("UDB_LIVE_AUTH_PG_DSN"))
            .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
            .unwrap_or_else(|_| "postgres://udb:udb@127.0.0.1:55432/udb".to_string());
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&dsn)
            .await
            .unwrap_or_else(|err| panic!("connect live metering postgres at {dsn}: {err}"));
        let schemas: Vec<String> = sqlx::query_scalar(
            "SELECT nspname FROM pg_namespace WHERE nspname LIKE 'udb\\_%' ESCAPE '\\'",
        )
        .fetch_all(&pool)
        .await
        .expect("list live metering schemas");
        for schema in schemas {
            let stmt = format!(
                "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
                schema.replace('"', "\"\"")
            );
            sqlx::query(&stmt)
                .execute(&pool)
                .await
                .unwrap_or_else(|err| panic!("drop live metering schema {schema}: {err}"));
        }
        for stmt in crate::runtime::native_catalog::native_service_catalog_ddl() {
            sqlx::raw_sql(&stmt)
                .execute(&pool)
                .await
                .unwrap_or_else(|err| panic!("native service DDL failed: {err}\nSQL:\n{stmt}"));
        }
        crate::runtime::system::ensure_system_catalog(&pool)
            .await
            .expect("ensure live metering system catalog");

        let mut config = crate::runtime::config::UdbConfig::from_env();
        config.primary.direct_dsn = dsn;
        let runtime = Arc::new(DataBrokerRuntime::from_config(config).await);
        let outbox = runtime.config().cdc.outbox_relation();
        let journal = crate::runtime::system::SystemCatalogConfig::current().cdc_journal_relation();
        let svc = MeteringServiceImpl::new()
            .with_postgres(Some(pool.clone()))
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox.clone()));
        (pool, svc, outbox, journal)
    }

    /// The pure quota decision: under-limit is allowed, at/over-limit is denied,
    /// and remaining is always `max(0, limit - used)`.
    #[test]
    fn quota_decision_math() {
        // used < limit -> allowed, remaining = limit - used.
        let (allowed, remaining) = quota_decision(3, 10);
        assert!(allowed);
        assert_eq!(remaining, 7);
        // used == limit -> denied (boundary), remaining 0.
        let (allowed, remaining) = quota_decision(10, 10);
        assert!(!allowed);
        assert_eq!(remaining, 0);
        // used > limit -> denied, remaining clamped to 0 (never negative).
        let (allowed, remaining) = quota_decision(15, 10);
        assert!(!allowed);
        assert_eq!(remaining, 0);
        // limit 0 -> deny all.
        let (allowed, remaining) = quota_decision(0, 0);
        assert!(!allowed);
        assert_eq!(remaining, 0);
    }

    /// The window lower bound is inclusive: an event at exactly `window_start` is
    /// counted; one second earlier is not. The SQL aggregate uses `Ge` on the same
    /// boundary, so this predicate is the single source of the boundary semantics.
    #[test]
    fn window_aggregation_boundary() {
        let now = 1_000_000;
        let start = window_start_unix(now, 3_600); // last hour
        assert_eq!(start, now - 3_600);
        assert!(event_in_window(start, start)); // exactly on the boundary: included
        assert!(event_in_window(now, start)); // inside: included
        assert!(!event_in_window(start - 1, start)); // just before: excluded
        // A window larger than `now` floors at 0 (no underflow).
        assert_eq!(window_start_unix(100, 10_000), 0);
    }

    #[test]
    fn windowed_usage_installs_rls_scope_before_aggregate_scan() {
        let install_scope = install_metering_tenant_scope_sql();
        assert!(
            install_scope.contains("set_config('app.current_tenant_id', $1, true)"),
            "tenant scope must be installed before scanning usage_events"
        );

        let aggregate = windowed_usage_sum_sql();
        assert!(
            !aggregate.contains("set_config("),
            "aggregate scan must not install tenant scope inside its WHERE clause"
        );
        assert!(
            aggregate.contains("FROM udb_metering.usage_events")
                && aggregate.contains("tenant_id = $1")
                && aggregate.contains("method = $2")
                && aggregate.contains("occurred_at_unix >= $3"),
            "aggregate must keep the tenant/method/window filters"
        );
    }

    #[test]
    fn admission_metering_method_is_canonical() {
        assert_eq!(admission_metering_method("cache", "read"), "cache.read");
        assert_eq!(admission_metering_method("", "write"), "native.write",);
        assert_eq!(admission_metering_method("  ", "  "), "native.unknown",);
    }

    #[test]
    fn rollup_window_uses_only_closed_buckets() {
        assert_eq!(closed_rollup_upper_bound(3_599, 3_600), 0);
        assert_eq!(closed_rollup_upper_bound(3_600, 3_600), 3_600);
        assert_eq!(closed_rollup_upper_bound(3_601, 3_600), 3_600);
    }

    #[test]
    fn rollup_id_is_stable_for_deduplication() {
        assert_eq!(
            rollup_id("tenant-a", "storage.RegisterUpload", "request", 0, 3_600),
            "tenant-a:storage.RegisterUpload:request:0:3600"
        );
    }

    /// `record_usage` must NEVER propagate a store error: against a pool pointed at
    /// a closed port the INSERT fails, but the function swallows it and returns Ok.
    #[tokio::test]
    async fn record_usage_swallows_store_error() {
        // Lazy pool: constructed without I/O; the connection (refused) only fails on
        // first use, exactly the "metering store outage" we must not propagate.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_millis(250))
            .connect_lazy("postgres://127.0.0.1:1/udb_metering_test")
            .expect("lazy pool builds without connecting");
        let result = record_usage(
            &pool,
            "tenant-a",
            "principal-1",
            "data.Select",
            "request",
            1,
            0,
        )
        .await;
        assert!(
            result.is_ok(),
            "metering must never fail the metered request",
        );
        // An empty tenant/method is a no-op (still Ok, no panic, no insert attempt).
        assert!(
            record_usage(&pool, "", "p", "m", "request", 1, 0)
                .await
                .is_ok()
        );
        assert!(
            record_usage(&pool, "t", "p", "  ", "request", 1, 0)
                .await
                .is_ok()
        );
    }

    /// Live rollup/export oracle for master-plan 9.9: served RecordUsage writes
    /// durable rows, QueryUsage sums the same rows, and the leader rollup worker
    /// exports exactly one closed-window outbox event with deterministic dedupe.
    #[tokio::test]
    #[ignore = "requires live Postgres; run with cargo test --lib live_postgres_metering_rollup_exports_closed_window_once -- --ignored --nocapture"]
    async fn live_postgres_metering_rollup_exports_closed_window_once() {
        let (pool, svc, outbox, journal) = live_metering_fixture().await;
        let tenant_id = Uuid::new_v4().to_string();
        let method = "storage.RegisterUpload";
        let unit = "bytes";
        let window = DEFAULT_ROLLUP_WINDOW_SECONDS;
        let upper = closed_rollup_upper_bound(now_unix(), window);
        assert!(
            upper >= window,
            "live clock must have at least one closed rollup window"
        );
        let occurred = upper - 60;

        for quantity in [7_i64, 11_i64] {
            MeteringService::record_usage(
                &svc,
                Request::new(metering_pb::RecordUsageRequest {
                    tenant_id: tenant_id.clone(),
                    principal_id: "principal-live".to_string(),
                    method: method.to_string(),
                    unit: unit.to_string(),
                    quantity,
                    occurred_at_unix: occurred,
                    metadata_json: "{}".to_string(),
                }),
            )
            .await
            .expect("record_usage")
            .into_inner();
        }

        let usage = MeteringService::query_usage(
            &svc,
            Request::new(metering_pb::QueryUsageRequest {
                tenant_id: tenant_id.clone(),
                metric: method.to_string(),
                window_seconds: DEFAULT_WINDOW_SECONDS,
            }),
        )
        .await
        .expect("query_usage")
        .into_inner();
        assert_eq!(
            usage.used, 18,
            "QueryUsage must sum durable UsageEvent rows"
        );

        let emitted = run_metering_rollup_once(&pool, &outbox, &journal, 10, None)
            .await
            .expect("rollup pass");
        assert_eq!(emitted, 1, "first pass must emit the closed usage bucket");
        let emitted_again = run_metering_rollup_once(&pool, &outbox, &journal, 10, None)
            .await
            .expect("dedupe rollup pass");
        assert_eq!(
            emitted_again, 0,
            "second pass must dedupe against the outbox rollup id"
        );

        let payload: serde_json::Value = sqlx::query_scalar(&format!(
            "SELECT payload FROM {outbox} WHERE topic = $1 ORDER BY event_seq DESC LIMIT 1"
        ))
        .bind(TOPIC_USAGE_ROLLUP)
        .fetch_one(&pool)
        .await
        .expect("read rollup outbox payload");
        assert_eq!(payload["event_type"], TOPIC_USAGE_ROLLUP);
        let rollup_payload = &payload["payload"];
        assert_eq!(rollup_payload["tenant_id"], tenant_id);
        assert_eq!(rollup_payload["method"], method);
        assert_eq!(rollup_payload["unit"], unit);
        assert_eq!(rollup_payload["quantity"], 18);
        assert_eq!(rollup_payload["event_count"], 2);
        assert_eq!(
            rollup_payload["rollup_id"],
            rollup_id(&tenant_id, method, unit, upper - window, upper)
        );
    }

    /// A caller scoped to tenant-a must not write tenant-b's quota by putting a
    /// foreign tenant_id in the request BODY; the guard rejects before any store
    /// access (no Postgres needed) — mirrors `lock_service`/`config_service`.
    #[tokio::test]
    async fn put_quota_rejects_cross_tenant_body() {
        let svc = MeteringServiceImpl::new(); // no runtime, no channels (admit no-op)
        let mut request = Request::new(metering_pb::PutQuotaRequest {
            tenant_id: "tenant-b".to_string(),
            metric: "data.Select".to_string(),
            limit_value: 100,
            window_seconds: 3_600,
            enabled: true,
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .put_quota(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn record_usage_missing_method_carries_field_violation() {
        let svc = MeteringServiceImpl::new(); // no pool/runtime; validation must fire first
        let mut request = Request::new(metering_pb::RecordUsageRequest {
            tenant_id: "tenant-a".to_string(),
            method: "  ".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .record_usage(request)
            .await
            .expect_err("missing method must be rejected before admission/store access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "method is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "method");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty usage method"
        );
    }

    #[tokio::test]
    async fn query_usage_missing_metric_carries_field_violation() {
        let svc = MeteringServiceImpl::new(); // no runtime, no channels (admit no-op)
        let mut request = Request::new(metering_pb::QueryUsageRequest {
            tenant_id: "tenant-a".to_string(),
            metric: String::new(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .query_usage(request)
            .await
            .expect_err("missing metric must be rejected before runtime access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "metric is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "metric");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty metric name"
        );
    }

    #[tokio::test]
    async fn put_quota_negative_limit_carries_field_violation() {
        let svc = MeteringServiceImpl::new(); // no runtime, no channels (admit no-op)
        let mut request = Request::new(metering_pb::PutQuotaRequest {
            tenant_id: "tenant-a".to_string(),
            metric: "data.Select".to_string(),
            limit_value: -1,
            window_seconds: 3_600,
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .put_quota(request)
            .await
            .expect_err("negative limit must be rejected before runtime access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "limit_value must be >= 0");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "limit_value");
        assert_eq!(
            detail.field_violations[0].description,
            "must be greater than or equal to 0"
        );
    }

    /// CheckQuota likewise rejects a cross-tenant body before any store access.
    #[tokio::test]
    async fn check_quota_rejects_cross_tenant_body() {
        let svc = MeteringServiceImpl::new();
        let mut request = Request::new(metering_pb::CheckQuotaRequest {
            tenant_id: "tenant-b".to_string(),
            metric: "data.Select".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .check_quota(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn metering_missing_runtime_capability_carries_typed_detail() {
        let err = metering_capability_status(
            "native_entity_dispatch",
            "runtime_native_entity_dispatch",
            "metering service requires runtime native-entity dispatch (no runtime configured)",
        );
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "metering service requires runtime native-entity dispatch (no runtime configured)"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "metering");
        assert_eq!(detail.operation, "native_entity_dispatch");
        assert_eq!(detail.capability_required, "runtime_native_entity_dispatch");
        assert!(!detail.retryable);
    }

    #[test]
    fn metering_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &metering_internal_status(
                "windowed_usage_aggregate",
                "windowed usage aggregate failed: database is unavailable",
            ),
            "windowed_usage_aggregate",
            "windowed usage aggregate failed: database is unavailable",
        );
    }
}
