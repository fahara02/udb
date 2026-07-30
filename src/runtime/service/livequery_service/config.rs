//! LiveQueryService process-static configuration — named consts and env knobs
//! resolved exactly once (never per-subscribe).

use std::sync::OnceLock;
use std::time::Duration;

use crate::runtime::executor_utils::env_first;

/// Service id this handler dispatches its mediated snapshot reads under. The
/// dispatch target only selects the backend/instance; the table is resolved from
/// the request `message_type` via the embedded manifest, so a snapshot reads the
/// source entity's table on the resolved (postgres) store.
pub(crate) const SERVICE_ID: &str = "livequery";

/// Default bound on the initial snapshot row count when the caller omits one.
/// The env-overridable resolver is [`default_snapshot_limit`].
pub(crate) const DEFAULT_SNAPSHOT_LIMIT: u32 = 1_000;
/// Hard cap on the initial snapshot so one subscription cannot scan unbounded.
/// The env-overridable resolver is [`max_snapshot_limit`].
pub(crate) const MAX_SNAPSHOT_LIMIT: u32 = 10_000;

/// Default bounded per-subscription delta buffer. Overridable via
/// `UDB_LIVEQUERY_BUFFER_EVENTS` (canonical) or the deprecated `LIVEQUERY_BUFFER_EVENTS`
/// alias; on overflow the stream is closed (never grown).
const DEFAULT_BUFFER_EVENTS: usize = 1_024;

/// Default keepalive cadence for an otherwise-idle delta stream (seconds). `0`
/// disables keepalives. Overridable via `UDB_LIVEQUERY_KEEPALIVE_SECS`.
const DEFAULT_KEEPALIVE_SECS: u64 = 30;

/// Parse `raw` as a strictly-positive `usize`, falling back to `default` when it
/// is absent, unparsable, or non-positive (so the channel is always bounded).
/// Pure so the resolution is unit-testable without the process environment.
fn positive_usize_or(raw: Option<&str>, default: usize) -> usize {
    raw.and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Parse `raw` as a strictly-positive `u32`, falling back to `default`. Pure so
/// the snapshot-limit resolution is unit-testable without the environment.
fn positive_u32_or(raw: Option<&str>, default: u32) -> u32 {
    raw.and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Resolve the bounded delta-buffer depth. The canonical `UDB_LIVEQUERY_BUFFER_EVENTS`
/// wins; the un-prefixed `LIVEQUERY_BUFFER_EVENTS` is accepted as a DEPRECATED
/// alias (only consulted when the canonical name is unset) so the knob gains the
/// `UDB_` prefix its sibling budget knobs already carry without breaking existing
/// deployments. First-present precedence is `env_first`'s contract. A non-positive
/// / unparsable value uses [`DEFAULT_BUFFER_EVENTS`] so the channel is always bounded.
fn resolve_buffer_events() -> usize {
    let raw = env_first(&["UDB_LIVEQUERY_BUFFER_EVENTS", "LIVEQUERY_BUFFER_EVENTS"]);
    positive_usize_or(raw.as_deref(), DEFAULT_BUFFER_EVENTS)
}

/// Bounded delta-buffer depth, resolved exactly once (no per-subscribe env reads):
/// the buffer bound is process-static config.
pub(crate) fn buffer_events() -> usize {
    static BUF: OnceLock<usize> = OnceLock::new();
    *BUF.get_or_init(resolve_buffer_events)
}

/// Resolve the default initial-snapshot row bound from
/// `UDB_LIVEQUERY_DEFAULT_SNAPSHOT_LIMIT`, falling back to [`DEFAULT_SNAPSHOT_LIMIT`].
fn resolve_default_snapshot_limit() -> u32 {
    let raw = env_first(&["UDB_LIVEQUERY_DEFAULT_SNAPSHOT_LIMIT"]);
    positive_u32_or(raw.as_deref(), DEFAULT_SNAPSHOT_LIMIT)
}

/// Default initial-snapshot row bound, resolved exactly once.
pub(crate) fn default_snapshot_limit() -> u32 {
    static LIMIT: OnceLock<u32> = OnceLock::new();
    *LIMIT.get_or_init(resolve_default_snapshot_limit)
}

/// Resolve the hard initial-snapshot cap from `UDB_LIVEQUERY_MAX_SNAPSHOT_LIMIT`,
/// falling back to [`MAX_SNAPSHOT_LIMIT`].
fn resolve_max_snapshot_limit() -> u32 {
    let raw = env_first(&["UDB_LIVEQUERY_MAX_SNAPSHOT_LIMIT"]);
    positive_u32_or(raw.as_deref(), MAX_SNAPSHOT_LIMIT)
}

/// Hard initial-snapshot cap, resolved exactly once.
pub(crate) fn max_snapshot_limit() -> u32 {
    static LIMIT: OnceLock<u32> = OnceLock::new();
    *LIMIT.get_or_init(resolve_max_snapshot_limit)
}

/// Clamp a caller-requested snapshot limit into the configured `[?, max]` band:
/// a non-positive request (unset / negative) takes `default`, an over-cap request
/// is capped at `max`, and `default` itself is never allowed to exceed `max` (in
/// case a misconfiguration sets the default above the cap). Pure — the `i32`
/// request never truncates because it is compared as `i64` before the `u32` cast.
pub(crate) fn clamp_snapshot_limit(requested: i32, default: u32, max: u32) -> u32 {
    if requested <= 0 {
        default.min(max)
    } else {
        (requested as i64).min(max as i64) as u32
    }
}

/// Resolve the idle-stream keepalive cadence from `UDB_LIVEQUERY_KEEPALIVE_SECS`,
/// falling back to [`DEFAULT_KEEPALIVE_SECS`]. `0` (or an unparsable value that a
/// deployment might use to mean "off") disables keepalives entirely (`None`).
fn resolve_keepalive_interval() -> Option<Duration> {
    let secs = env_first(&["UDB_LIVEQUERY_KEEPALIVE_SECS"])
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_KEEPALIVE_SECS);
    if secs == 0 {
        None
    } else {
        Some(Duration::from_secs(secs))
    }
}

/// Idle-stream keepalive cadence, resolved exactly once. `None` = keepalives off.
/// The delta forwarder uses this to put a heartbeat frame on an otherwise-silent
/// stream so an L4/L7 load balancer's idle timeout does not reap a healthy
/// long-lived subscription.
pub(crate) fn livequery_keepalive_interval() -> Option<Duration> {
    static KEEPALIVE: OnceLock<Option<Duration>> = OnceLock::new();
    *KEEPALIVE.get_or_init(resolve_keepalive_interval)
}

/// Default per-tenant budget of CONCURRENT long-lived subscription streams.
/// The fair-admission permit taken at `subscribe` entry only rate-bounds
/// subscription SETUP (it is dropped immediately, by design, so a long-lived
/// stream never pins an admission slot); this budget is what bounds how many
/// open streams one tenant can hold at once.
const DEFAULT_MAX_STREAMS_PER_TENANT: usize = 64;

/// Resolve the per-tenant concurrent-stream budget from
/// `UDB_LIVEQUERY_MAX_STREAMS_PER_TENANT`, falling back to
/// [`DEFAULT_MAX_STREAMS_PER_TENANT`]. A non-positive / unparsable value uses
/// the default so the budget is always finite. Resolved exactly once (no
/// per-subscribe env reads): the budget is process-static config.
pub(crate) fn max_streams_per_tenant() -> usize {
    static BUDGET: OnceLock<usize> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        positive_usize_or(
            env_first(&["UDB_LIVEQUERY_MAX_STREAMS_PER_TENANT"]).as_deref(),
            DEFAULT_MAX_STREAMS_PER_TENANT,
        )
    })
}

/// Default GLOBAL ceiling on concurrent live-query streams across ALL tenants.
/// The per-tenant budget alone does not bound the process: N tenants × 64 each
/// would spawn N×64 forwarder tasks + N×64 bounded channels. This aggregate cap
/// keeps a large tenant fan-out from exhausting process memory.
const DEFAULT_MAX_STREAMS_GLOBAL: usize = 4_096;

/// Resolve the global concurrent-stream ceiling from
/// `UDB_LIVEQUERY_MAX_STREAMS_GLOBAL`, falling back to
/// [`DEFAULT_MAX_STREAMS_GLOBAL`]. A non-positive / unparsable value uses the
/// default so the ceiling is always finite. Resolved exactly once.
pub(crate) fn max_streams_global() -> usize {
    static GLOBAL: OnceLock<usize> = OnceLock::new();
    *GLOBAL.get_or_init(|| {
        positive_usize_or(
            env_first(&["UDB_LIVEQUERY_MAX_STREAMS_GLOBAL"]).as_deref(),
            DEFAULT_MAX_STREAMS_GLOBAL,
        )
    })
}

/// Default bound on the number of missed journalled deltas replayed on a durable
/// resume (`x-udb-livequery-resume`). Bounds the one-shot backlog a reconnecting
/// client receives before the live feed hands off; a client with a larger gap
/// simply re-resumes from its new last-delivered cursor. Overridable via
/// `UDB_LIVEQUERY_RESUME_REPLAY_LIMIT`.
const DEFAULT_RESUME_REPLAY_LIMIT: u32 = 10_000;

/// Bound on the durable-resume replay backlog, resolved exactly once. A
/// non-positive / unparsable value uses [`DEFAULT_RESUME_REPLAY_LIMIT`] so the
/// replay is always bounded.
pub(crate) fn resume_replay_limit() -> u32 {
    static LIMIT: OnceLock<u32> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        positive_u32_or(
            env_first(&["UDB_LIVEQUERY_RESUME_REPLAY_LIMIT"]).as_deref(),
            DEFAULT_RESUME_REPLAY_LIMIT,
        )
    })
}

#[cfg(test)]
mod config_tests {
    use super::{
        DEFAULT_BUFFER_EVENTS, DEFAULT_SNAPSHOT_LIMIT, MAX_SNAPSHOT_LIMIT, clamp_snapshot_limit,
        positive_u32_or, positive_usize_or, resolve_buffer_events,
    };

    /// Restore a single process-wide env var to a captured pre-test value.
    fn restore_env(key: &str, value: Option<String>) {
        #[allow(unused_unsafe)]
        unsafe {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }

    #[test]
    fn positive_usize_or_rejects_non_positive_and_unparsable() {
        assert_eq!(positive_usize_or(Some("2048"), 1_024), 2_048);
        // Whitespace (e.g. a CRLF-tainted .env) is trimmed before parsing.
        assert_eq!(positive_usize_or(Some(" 2048 "), 1_024), 2_048);
        // Zero, negative, unparsable, and absent all fall back to the default so
        // the channel is always bounded.
        assert_eq!(positive_usize_or(Some("0"), 1_024), 1_024);
        assert_eq!(positive_usize_or(Some("-5"), 1_024), 1_024);
        assert_eq!(positive_usize_or(Some("abc"), 1_024), 1_024);
        assert_eq!(positive_usize_or(None, 1_024), 1_024);
    }

    #[test]
    fn positive_u32_or_rejects_non_positive_and_unparsable() {
        assert_eq!(positive_u32_or(Some("500"), 1_000), 500);
        assert_eq!(positive_u32_or(Some("0"), 1_000), 1_000);
        assert_eq!(positive_u32_or(Some("nope"), 1_000), 1_000);
        assert_eq!(positive_u32_or(None, 1_000), 1_000);
    }

    /// The snapshot-limit clamp: unset/negative → default, over-cap → max, a
    /// default above the cap is itself capped, and a large positive request never
    /// truncates through the `u32` cast.
    #[test]
    fn clamp_snapshot_limit_bands_the_request() {
        // Unset / negative take the default.
        assert_eq!(clamp_snapshot_limit(0, 1_000, 10_000), 1_000);
        assert_eq!(clamp_snapshot_limit(-1, 1_000, 10_000), 1_000);
        // In-band request is honored verbatim.
        assert_eq!(clamp_snapshot_limit(500, 1_000, 10_000), 500);
        // Over-cap request is capped at max.
        assert_eq!(clamp_snapshot_limit(50_000, 1_000, 10_000), 10_000);
        assert_eq!(clamp_snapshot_limit(i32::MAX, 1_000, 10_000), 10_000);
        // A misconfigured default above the cap is itself capped.
        assert_eq!(clamp_snapshot_limit(0, 20_000, 10_000), 10_000);
    }

    /// Alias precedence for the buffer knob: the canonical `UDB_`-prefixed name
    /// wins when both are set; the deprecated un-prefixed alias is honored only
    /// when the canonical name is unset; neither set falls back to the default.
    /// Uses the non-cached resolver (`resolve_buffer_events`) so it re-reads the
    /// environment each call; env is captured and restored around the test.
    #[test]
    fn buffer_events_canonical_wins_then_alias_then_default() {
        let prior_canonical = std::env::var("UDB_LIVEQUERY_BUFFER_EVENTS").ok();
        let prior_alias = std::env::var("LIVEQUERY_BUFFER_EVENTS").ok();

        // Neither set: the default.
        restore_env("UDB_LIVEQUERY_BUFFER_EVENTS", None);
        restore_env("LIVEQUERY_BUFFER_EVENTS", None);
        assert_eq!(resolve_buffer_events(), DEFAULT_BUFFER_EVENTS);

        // Only the deprecated un-prefixed alias set: honored.
        restore_env("LIVEQUERY_BUFFER_EVENTS", Some("512".to_string()));
        assert_eq!(resolve_buffer_events(), 512);

        // Both set: the canonical `UDB_`-prefixed name wins.
        restore_env("UDB_LIVEQUERY_BUFFER_EVENTS", Some("4096".to_string()));
        assert_eq!(resolve_buffer_events(), 4_096);

        restore_env("UDB_LIVEQUERY_BUFFER_EVENTS", prior_canonical);
        restore_env("LIVEQUERY_BUFFER_EVENTS", prior_alias);
    }

    #[test]
    fn snapshot_limit_consts_are_the_resolver_defaults() {
        // Guard the const→resolver-default wiring so a future edit that renames a
        // const cannot silently drop the fallback.
        assert_eq!(positive_u32_or(None, DEFAULT_SNAPSHOT_LIMIT), 1_000);
        assert_eq!(positive_u32_or(None, MAX_SNAPSHOT_LIMIT), 10_000);
    }
}
