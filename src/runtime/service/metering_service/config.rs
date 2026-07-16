//! Static configuration for the native `MeteringService`: entity/topic names,
//! window + rollup defaults, list caps, and the resolve-once rollup env knobs
//! (never a per-request env read).

use std::sync::OnceLock;
use std::time::Duration;

pub(crate) const USAGE_EVENT_MSG: &str = "udb.core.metering.entity.v1.UsageEvent";
pub(crate) const QUOTA_RULE_MSG: &str = "udb.core.metering.entity.v1.QuotaRule";

pub(crate) const TOPIC_QUOTA_CHANGED: &str = "udb.metering.quota.changed.v1";
pub(crate) const TOPIC_USAGE_ROLLUP: &str = "udb.metering.rollup.v1";

/// Default rolling window when a caller/rule does not specify one (24h).
pub(crate) const DEFAULT_WINDOW_SECONDS: i64 = 86_400;
/// Billing/export rollup bucket width. Closed hourly windows are emitted by
/// default so a restarted leader can replay a bounded recent range.
pub(crate) const DEFAULT_ROLLUP_WINDOW_SECONDS: i64 = 3_600;
const DEFAULT_ROLLUP_LOOKBACK_SECONDS: i64 = 86_400;
const DEFAULT_ROLLUP_INTERVAL_SECS: u64 = 300;
const ROLLUP_WINDOW_ENV: &str = "UDB_METERING_ROLLUP_WINDOW_SECS";
const ROLLUP_LOOKBACK_ENV: &str = "UDB_METERING_ROLLUP_LOOKBACK_SECS";
const ROLLUP_INTERVAL_ENV: &str = "UDB_METERING_ROLLUP_INTERVAL_SECS";
pub(crate) const METERING_ROLLUP_BATCH: i64 = 200;
/// Default unit for an event with no explicit unit.
pub(crate) const DEFAULT_UNIT: &str = "request";
/// Unit emitted by the automatic fair-admission hook. The quantity is the same
/// bounded operation cost that `ChannelManager` accounts in metrics.
pub(crate) const ADMISSION_METERING_UNIT: &str = "admission_cost";
/// List defaults/caps so one tenant cannot scan an unbounded quota table.
pub(crate) const DEFAULT_LIST_LIMIT: u32 = 100;
pub(crate) const MAX_LIST_LIMIT: u32 = 1_000;

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

pub(crate) fn metering_rollup_window_seconds() -> i64 {
    static WINDOW: OnceLock<i64> = OnceLock::new();
    *WINDOW.get_or_init(|| positive_i64_env(ROLLUP_WINDOW_ENV, DEFAULT_ROLLUP_WINDOW_SECONDS))
}

pub(crate) fn metering_rollup_lookback_seconds() -> i64 {
    static LOOKBACK: OnceLock<i64> = OnceLock::new();
    *LOOKBACK.get_or_init(|| positive_i64_env(ROLLUP_LOOKBACK_ENV, DEFAULT_ROLLUP_LOOKBACK_SECONDS))
}
