//! Static configuration for the native `WorkflowService`: the canonical entity
//! name, the eight per-transition outbox topics, the stored status tokens, the
//! master-plan 9.12 bounds, and the once-resolved step-timeout / tick-batch knobs
//! (no per-request/per-tick env reads).

use std::sync::OnceLock;

pub(crate) const WORKFLOW_MSG: &str = "udb.core.workflow.entity.v1.WorkflowInstance";

// ── outbox topics (one per state transition) ───────────────────────────────────
pub(crate) const TOPIC_STARTED: &str = "udb.workflow.started.v1";
pub(crate) const TOPIC_STEP_ADVANCED: &str = "udb.workflow.step.advanced.v1";
pub(crate) const TOPIC_COMPLETED: &str = "udb.workflow.completed.v1";
pub(crate) const TOPIC_SIGNALED: &str = "udb.workflow.signaled.v1";
pub(crate) const TOPIC_CANCELLED: &str = "udb.workflow.cancelled.v1";
pub(crate) const TOPIC_COMPENSATE_STEP: &str = "udb.workflow.compensate.step.v1";
pub(crate) const TOPIC_COMPENSATED: &str = "udb.workflow.compensated.v1";
pub(crate) const TOPIC_FAILED: &str = "udb.workflow.failed.v1";

// ── stored status tokens (VARCHAR, short form) ─────────────────────────────────
// RUNNING/COMPLETED are written as SQL literals in the tick's forward UPDATEs;
// the cancel-path and tick-writeback targets are named constants.
pub(crate) const STATUS_COMPENSATING: &str = "COMPENSATING";
pub(crate) const STATUS_CANCELLED: &str = "CANCELLED";
pub(crate) const STATUS_COMPENSATED: &str = "COMPENSATED";
pub(crate) const STATUS_FAILED: &str = "FAILED";

/// Upper bound on forward steps per workflow (master-plan 9.12: ≤20 steps). A
/// request beyond this is clamped down rather than rejected.
pub(crate) const MAX_WORKFLOW_STEPS: i32 = 20;
/// Upper bound on the opaque payload (master-plan 9.12: ≤8 KiB). Bounds the durable
/// row and the fired event so one workflow cannot bloat the outbox.
pub(crate) const MAX_PAYLOAD_BYTES: usize = 8 * 1024;
/// Same bound on the compensation list handed to the saga engine.
pub(crate) const MAX_COMPENSATIONS_BYTES: usize = 8 * 1024;

/// Default upper bound (seconds) a RUNNING instance may sit without a state
/// transition before the tick's timeout sweep (16.3.4) fails it. Overridable via
/// `UDB_WORKFLOW_STEP_TIMEOUT_SECS`, resolved ONCE through
/// [`workflow_step_timeout_secs`] — never a per-tick env read.
pub(crate) const WORKFLOW_STEP_TIMEOUT_SECS: i64 = 3600;

/// Payload key stamping how many `compensate.step` events have already been
/// emitted for a COMPENSATING instance, so a re-tick never re-emits (16.3.2
/// exactly-once guard; belt-and-braces on top of the atomic emit+transition).
pub(crate) const COMPENSATE_EMITTED_KEY: &str = "compensate_emitted_steps";

/// Resolve the step timeout exactly once (no per-tick env reads).
pub(crate) fn workflow_step_timeout_secs() -> i64 {
    static SECS: OnceLock<i64> = OnceLock::new();
    *SECS.get_or_init(|| {
        std::env::var("UDB_WORKFLOW_STEP_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(WORKFLOW_STEP_TIMEOUT_SECS)
    })
}

/// Default batch the tick advances per pass — a named constant (no per-request env
/// reads). Bounds how many DUE instances one tick advances so a backlog can't
/// starve the transaction. Referenced by the serve() leader-spawn block (follow-up).
#[allow(dead_code)]
pub(crate) const WORKFLOW_TICK_BATCH: i64 = 200;
