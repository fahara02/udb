//! Static configuration for the native `SchedulerService`: the canonical entity
//! name, the six outbox topics, and the tick batch bound (no per-request env
//! reads).

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
