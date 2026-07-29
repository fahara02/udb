//! LiveQueryService process-static configuration — named consts and env knobs
//! resolved exactly once (never per-subscribe).

use std::sync::OnceLock;

/// Service id this handler dispatches its mediated snapshot reads under. The
/// dispatch target only selects the backend/instance; the table is resolved from
/// the request `message_type` via the embedded manifest, so a snapshot reads the
/// source entity's table on the resolved (postgres) store.
pub(crate) const SERVICE_ID: &str = "livequery";

/// Default bound on the initial snapshot row count when the caller omits one.
pub(crate) const DEFAULT_SNAPSHOT_LIMIT: u32 = 1_000;
/// Hard cap on the initial snapshot so one subscription cannot scan unbounded.
pub(crate) const MAX_SNAPSHOT_LIMIT: u32 = 10_000;

/// Default bounded per-subscription delta buffer. Overridable via
/// `LIVEQUERY_BUFFER_EVENTS`; on overflow the stream is closed (never grown).
const DEFAULT_BUFFER_EVENTS: usize = 1_024;

/// Resolve the bounded delta-buffer depth from `LIVEQUERY_BUFFER_EVENTS`,
/// falling back to [`DEFAULT_BUFFER_EVENTS`]. A non-positive / unparsable value
/// uses the default so the channel is always bounded. Resolved exactly once
/// (no per-subscribe env reads): the buffer bound is process-static config.
pub(crate) fn buffer_events() -> usize {
    static BUF: OnceLock<usize> = OnceLock::new();
    *BUF.get_or_init(|| {
        std::env::var("LIVEQUERY_BUFFER_EVENTS")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_BUFFER_EVENTS)
    })
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
        std::env::var("UDB_LIVEQUERY_MAX_STREAMS_PER_TENANT")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_STREAMS_PER_TENANT)
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
        std::env::var("UDB_LIVEQUERY_MAX_STREAMS_GLOBAL")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_STREAMS_GLOBAL)
    })
}
