//! Static configuration for the native `AnalyticsService`: entity message names,
//! the proto-declared outbox topic + event types, read/aggregation caps, and the
//! resolve-once rollup-window env knob (never a per-request env read).

use std::sync::OnceLock;

pub(crate) const PMS_MSG: &str = "udb.core.analytics.entity.v1.PipelineMetricSnapshot";
pub(crate) const EPS_MSG: &str = "udb.core.analytics.entity.v1.ExecutorPerformanceSummary";
pub(crate) const RAS_MSG: &str = "udb.core.analytics.entity.v1.ReconciliationAnalyticsSummary";

/// Outbox topic + event types exactly as the proto `method_event_contract`
/// declares them on the two mutating RPCs (`analytics_service.proto`); the
/// contract this service must actually fulfil.
pub(crate) const TOPIC_ANALYTICS_EVENTS: &str = "analytics.events";
pub(crate) const EVENT_TYPE_PIPELINE_METRIC_RECORDED: &str = "analytics.RecordPipelineMetric";
pub(crate) const EVENT_TYPE_SNAPSHOT_TRIGGERED: &str = "analytics.TriggerSnapshot";

pub(crate) const MAX_ANALYTICS_READ_ROWS: u32 = 10_000;

/// Denominator for the online `throughput_rps` derivation (requests per hourly
/// bucket).
pub(crate) const SECONDS_PER_HOUR: i64 = 3_600;

/// Default page size for pipeline-summary listings.
pub(crate) const PIPELINE_SUMMARY_PAGE_SIZE: i32 = 50;

/// Trailing window (hours) the percentile rollup pass scans. Overridable once
/// via `UDB_ANALYTICS_ROLLUP_WINDOW_HOURS` (OnceLock — never a per-request env
/// read).
pub(crate) const DEFAULT_ROLLUP_WINDOW_HOURS: i32 = 24;

pub(crate) fn rollup_window_hours() -> i32 {
    static HOURS: OnceLock<i32> = OnceLock::new();
    *HOURS.get_or_init(|| {
        std::env::var("UDB_ANALYTICS_ROLLUP_WINDOW_HOURS")
            .ok()
            .and_then(|v| v.trim().parse::<i32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_ROLLUP_WINDOW_HOURS)
    })
}
