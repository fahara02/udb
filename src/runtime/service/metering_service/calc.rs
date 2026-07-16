//! PURE metering math (unit-tested without Postgres) and the wall-clock time
//! source: window boundaries, the quota decision, the revision bump, and the
//! closed-rollup bucket.

/// Inclusive lower bound (unix seconds) of a rolling window ending at `now`.
/// Saturating and floored at 0 so a giant window never underflows.
pub(crate) fn window_start_unix(now_unix: i64, window_seconds: i64) -> i64 {
    let window = window_seconds.max(0);
    now_unix.saturating_sub(window).max(0)
}

/// The window membership predicate the SQL filter mirrors: an event at exactly
/// the window start is INCLUDED (inclusive lower bound, `>=`). The aggregate
/// filter uses `Ge` on the same boundary so this pure predicate is the single
/// definition of the boundary semantics. Used by the boundary unit test; the
/// runtime path expresses the same `>=` as a SQL filter.
#[cfg(test)]
pub(crate) fn event_in_window(event_unix: i64, window_start: i64) -> bool {
    event_unix >= window_start
}

/// The pure quota decision: a rule allows the request iff `used < limit`, and the
/// remaining headroom is `max(0, limit - used)`. Deterministic, no I/O — shared
/// with the test fixtures. (A `limit == 0` rule denies all; a negative limit is
/// rejected at PutQuota and never reaches here.)
pub(crate) fn quota_decision(used: i64, limit: i64) -> (bool, i64) {
    let allowed = used < limit;
    let remaining = limit.saturating_sub(used).max(0);
    (allowed, remaining)
}

/// Monotone per-row revision bump (saturating; never wraps/panics).
pub(crate) fn bump_revision(current: i64) -> i64 {
    current.saturating_add(1)
}

/// Wall-clock unix seconds (used only when the caller passes a non-positive ts).
pub(crate) fn wall_now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Public monotone-seconds source the leader can pass to `record_usage` from the
/// admission hook (kept here so the call site has a single obvious time source).
pub(crate) fn now_unix() -> i64 {
    wall_now_unix()
}

pub(crate) fn closed_rollup_upper_bound(now_unix: i64, window_seconds: i64) -> i64 {
    let window = window_seconds.max(1);
    (now_unix / window) * window
}
