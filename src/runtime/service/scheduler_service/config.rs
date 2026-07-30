//! Static configuration for the native `SchedulerService`: the canonical entity
//! name, the six outbox topics, the tick batch bound, and the process-wide default
//! cron timezone (all resolve-once — no per-request env reads).

use chrono_tz::Tz;

pub(crate) const SCHEDULED_JOB_MSG: &str = "udb.core.scheduler.entity.v1.ScheduledJob";

// ── outbox topics ─────────────────────────────────────────────────────────────
pub(crate) const TOPIC_JOB_CREATED: &str = "udb.scheduler.job.created.v1";
pub(crate) const TOPIC_JOB_DELETED: &str = "udb.scheduler.job.deleted.v1";
pub(crate) const TOPIC_JOB_PAUSED: &str = "udb.scheduler.job.paused.v1";
pub(crate) const TOPIC_JOB_RESUMED: &str = "udb.scheduler.job.resumed.v1";
/// FIRE event: emitted once per due job; consumers do the actual work.
pub(crate) const TOPIC_JOB_FIRED: &str = "udb.scheduler.job.fired.v1";
/// Dead-letter event: emitted when a job exhausts `max_attempts`.
pub(crate) const TOPIC_JOB_DEAD: &str = "udb.scheduler.job.dead.v1";

/// Default batch the tick claims per pass — a named constant (no per-request env
/// reads). Bounds how many DUE jobs one tick fires so a backlog can't starve the
/// transaction.
pub(crate) const SCHEDULER_TICK_BATCH: i64 = 200;

/// Process-wide default cron timezone, applied to a CRON job whose `payload`
/// carries no `"timezone"`. Resolved EXACTLY ONCE from `UDB_SCHEDULER_TZ` (an IANA
/// name parsed case-insensitively) through a `OnceLock` — never read per request.
/// Unset, empty, or unparseable → `None`, which means UTC (the historical default,
/// so an operator typo degrades to UTC rather than silently picking a wrong zone).
pub(crate) fn scheduler_default_tz() -> Option<Tz> {
    static DEFAULT_TZ: std::sync::OnceLock<Option<Tz>> = std::sync::OnceLock::new();
    *DEFAULT_TZ.get_or_init(|| {
        let raw = std::env::var("UDB_SCHEDULER_TZ").ok()?;
        let name = raw.trim();
        if name.is_empty() {
            return None;
        }
        match Tz::from_str_insensitive(name) {
            Ok(tz) => Some(tz),
            Err(_) => {
                tracing::warn!(
                    timezone = %name,
                    "UDB_SCHEDULER_TZ is not a valid IANA time zone; scheduler defaults to UTC"
                );
                None
            }
        }
    })
}
