//! Phase 9 progressive rollout: canary scoping + metric-based auto-rollback.
//!
//! A policy *canary* exposes a candidate [`PolicyVersion`] to a SUBSET of the
//! fleet (by node id, tenant id, or a percentage slice) and watches a success
//! metric over a bake window before fleet-wide promotion. The evaluator's
//! verdict each cycle is one of:
//!
//!   * **Rollback** — the success metric breached `metric_threshold` *inside*
//!     the window: auto-roll back to the policy set's prior version BEFORE the
//!     bad policy reaches the whole fleet.
//!   * **PromoteEligible** — the window elapsed within threshold: the canary may
//!     now be promoted fleet-wide (`PromoteCanary`).
//!   * **Pause** — the signal is inconclusive (fewer than `min_samples`
//!     observations): hold; neither promote nor roll back.
//!   * **Hold** — still baking and healthy; keep waiting.
//!
//! This module is intentionally split into a **pure** core (scope membership +
//! the verdict function, both fully unit-testable with no DB / clock / metrics
//! backend) and a thin async driver ([`spawn_canary_evaluator`]) that polls the
//! durable canary rows, asks a [`CanaryMetricSource`] for the live signal, and
//! drives the real governance rollback / audit path through [`CanaryExecutor`].

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use crate::proto::udb::core::authz::entity::v1::{
    self as authz_entity_pb, CanaryScopeKind, CanaryState,
};

/// Default poll cadence for the background evaluator (kept short so a bad canary
/// is caught well before its bake window would otherwise elapse).
pub const CANARY_EVAL_INTERVAL: Duration = Duration::from_secs(5);

// ── Pure scope membership ───────────────────────────────────────────────────

/// Decoded canary scope: the kind plus its concrete values (node/tenant ids, or
/// a single percentage). Kept separate from the proto row so the membership test
/// is a pure function over plain data.
#[derive(Debug, Clone, PartialEq)]
pub enum CanaryScope {
    /// Served only to these control-plane node ids.
    Nodes(Vec<String>),
    /// Served only to these tenant ids.
    Tenants(Vec<String>),
    /// Served to a stable `percent`% slice (1..=100) of the population, bucketed
    /// by a hash of the candidate id (node or tenant) so membership is sticky.
    Percent(u8),
}

impl CanaryScope {
    /// Decode `(scope_kind, scope_values)` (as stored in the row) into a scope.
    /// `PERCENT` clamps its first value into `1..=100`; an empty/garbage percent
    /// list yields `Percent(0)` (nobody in scope), which fails closed.
    pub fn from_row(kind: CanaryScopeKind, values: &[String]) -> CanaryScope {
        match kind {
            CanaryScopeKind::Node => CanaryScope::Nodes(clean(values)),
            CanaryScopeKind::Tenant => CanaryScope::Tenants(clean(values)),
            CanaryScopeKind::Percent => {
                let pct = values
                    .first()
                    .and_then(|v| v.trim().parse::<i64>().ok())
                    .unwrap_or(0)
                    .clamp(0, 100) as u8;
                CanaryScope::Percent(pct)
            }
            CanaryScopeKind::Unspecified => CanaryScope::Percent(0),
        }
    }

    /// Whether a candidate node is in this canary's scope.
    ///
    /// * `Nodes` — exact id membership.
    /// * `Tenants` — node scoping is not tenant-addressed, so a tenant-scoped
    ///   canary is NOT served by node (use [`tenant_in_scope`]); returns false.
    /// * `Percent` — sticky hash bucket of the node id.
    pub fn node_in_scope(&self, node_id: &str) -> bool {
        match self {
            CanaryScope::Nodes(ids) => ids.iter().any(|id| id == node_id),
            CanaryScope::Tenants(_) => false,
            CanaryScope::Percent(pct) => in_percent_bucket(node_id, *pct),
        }
    }

    /// Whether a candidate tenant is in this canary's scope.
    ///
    /// * `Tenants` — exact id membership.
    /// * `Nodes` — node-scoped canaries are not tenant-addressed; returns false.
    /// * `Percent` — sticky hash bucket of the tenant id.
    ///
    /// The tenant-addressed counterpart of [`Self::node_in_scope`]. Tenant-scoped
    /// canaries are validated/created today, but the only live canary DECISION
    /// (the nack-rate metric loop in `governance_activate`) reads the
    /// node-addressed `ControlPlaneNodeState` ledger and explicitly skips
    /// `Tenants` scopes — so this check is currently reached only by unit tests
    /// and is `#[cfg(test)]`-scoped (honestly not-dead, not `#[allow]`-silenced).
    /// Un-gate it when a tenant-addressed signal/serve source lands.
    #[cfg(test)]
    pub fn tenant_in_scope(&self, tenant_id: &str) -> bool {
        match self {
            CanaryScope::Tenants(ids) => ids.iter().any(|id| id == tenant_id),
            CanaryScope::Nodes(_) => false,
            CanaryScope::Percent(pct) => in_percent_bucket(tenant_id, *pct),
        }
    }

    /// The `scope_kind` enum this scope serializes back to.
    pub fn kind(&self) -> CanaryScopeKind {
        match self {
            CanaryScope::Nodes(_) => CanaryScopeKind::Node,
            CanaryScope::Tenants(_) => CanaryScopeKind::Tenant,
            CanaryScope::Percent(_) => CanaryScopeKind::Percent,
        }
    }

    /// The `scope_values` list this scope serializes back to (for persistence).
    pub fn values(&self) -> Vec<String> {
        match self {
            CanaryScope::Nodes(ids) | CanaryScope::Tenants(ids) => ids.clone(),
            CanaryScope::Percent(pct) => vec![pct.to_string()],
        }
    }
}

fn clean(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect()
}

/// Stable 0..=99 bucket for `id`; in-scope iff `bucket < percent`. A 0% canary
/// includes nobody; 100% includes everybody. Membership is sticky across
/// evaluations because it is a pure hash of the id.
fn in_percent_bucket(id: &str, percent: u8) -> bool {
    if percent == 0 {
        return false;
    }
    if percent >= 100 {
        return true;
    }
    let mut h = DefaultHasher::new();
    id.hash(&mut h);
    let bucket = (h.finish() % 100) as u8;
    bucket < percent
}

// ── Pure metric verdict ─────────────────────────────────────────────────────

/// A point-in-time reading of the canary's success signal.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CanarySignal {
    /// The measured success metric (e.g. authz deny rate / error rate) for the
    /// in-scope slice. Higher = worse.
    pub value: f64,
    /// How many observations the value was computed from. Used to decide whether
    /// the signal is conclusive.
    pub samples: i64,
}

/// The evaluator's verdict for one canary on one cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanaryVerdict {
    /// Metric breached threshold inside the window → auto-rollback NOW.
    Rollback,
    /// Window elapsed within threshold → eligible for fleet-wide promotion.
    PromoteEligible,
    /// Insufficient samples → pause (hold, do not promote/rollback).
    Pause,
    /// Healthy and still inside the bake window → keep waiting.
    Hold,
}

/// The tunables that drive a verdict, lifted out of the proto row so the verdict
/// is a pure function (testable without a DB row or a wall clock).
#[derive(Debug, Clone, Copy)]
pub struct CanaryPolicy {
    pub success_window_secs: i64,
    pub metric_threshold: f64,
    pub min_samples: i64,
}

impl CanaryPolicy {
    /// Build from a persisted row, applying floors so a zero/garbage row still
    /// behaves safely (a single sample suffices; a non-positive window means
    /// "promote-eligible as soon as conclusive").
    pub fn from_canary(c: &authz_entity_pb::PolicyCanary) -> CanaryPolicy {
        CanaryPolicy {
            success_window_secs: c.success_window_secs.max(0),
            metric_threshold: if c.metric_threshold.is_finite() && c.metric_threshold >= 0.0 {
                c.metric_threshold
            } else {
                0.0
            },
            min_samples: c.min_samples.max(1),
        }
    }
}

/// THE decision. Pure: given the bake settings, how long the canary has been
/// running (`elapsed_secs`), and the live signal, return the verdict.
///
/// Ordering of checks matters and encodes the safety contract:
///   1. A **breach** (value strictly above threshold) with a **conclusive**
///      signal rolls back immediately, even before the window elapses — a bad
///      canary must not wait out its full window.
///   2. An **inconclusive** signal (too few samples) pauses — we never promote
///      or roll back on noise.
///   3. A healthy signal promotes only once the window has fully elapsed.
///   4. Otherwise keep baking.
pub fn evaluate_canary(
    policy: &CanaryPolicy,
    elapsed_secs: i64,
    signal: CanarySignal,
) -> CanaryVerdict {
    let conclusive = signal.samples >= policy.min_samples;
    let breached = signal.value > policy.metric_threshold;

    if breached && conclusive {
        return CanaryVerdict::Rollback;
    }
    if !conclusive {
        return CanaryVerdict::Pause;
    }
    // Conclusive and within threshold.
    if elapsed_secs >= policy.success_window_secs {
        CanaryVerdict::PromoteEligible
    } else {
        CanaryVerdict::Hold
    }
}

/// Whether `PromoteCanary` should be allowed for a canary right now (used by the
/// RPC handler). Promote is allowed only for an ACTIVE canary whose bake window
/// has elapsed; the actual health gate is enforced continuously by the evaluator
/// (a breached canary is already ROLLED_BACK before it can be promoted).
pub fn promote_eligible(c: &authz_entity_pb::PolicyCanary, now_unix: i64) -> bool {
    if c.state != CanaryState::Active as i32 {
        return false;
    }
    now_unix.saturating_sub(canary_started_at_unix(c)) >= c.success_window_secs.max(0)
}

/// Seconds remaining in the bake window (0 once elapsed).
pub fn window_remaining_secs(c: &authz_entity_pb::PolicyCanary, now_unix: i64) -> i64 {
    let elapsed = now_unix.saturating_sub(canary_started_at_unix(c));
    (c.success_window_secs.max(0) - elapsed).max(0)
}

/// Extract `started_at` as a unix epoch (0 when unset).
pub fn canary_started_at_unix(c: &authz_entity_pb::PolicyCanary) -> i64 {
    c.started_at.as_ref().map(|t| t.seconds).unwrap_or(0)
}

// ── Async driver: metric source + executor + background task ─────────────────

/// Source of the live success/failure signal for a canary's in-scope slice.
///
/// Production wires a reader over the metrics registry (e.g. the authz deny /
/// error rate exposed via `udb_authz_denies_total`); tests pass a canned source.
/// Returning a signal with `samples == 0` is the explicit "no data" case and
/// drives a PAUSE rather than a false promotion.
#[async_trait::async_trait]
pub trait CanaryMetricSource: Send + Sync {
    /// Read the current signal for one canary (identified by id + scope), over
    /// the last `window_secs` of data.
    async fn read(&self, canary: &authz_entity_pb::PolicyCanary, window_secs: i64) -> CanarySignal;
}

/// A metric source that always reports "no samples" → every canary PAUSES. This
/// is the fail-safe default used when no real metric backend is wired: a canary
/// is never auto-promoted on the strength of zero evidence.
#[derive(Debug, Default, Clone)]
pub struct NoSignalSource;

#[async_trait::async_trait]
impl CanaryMetricSource for NoSignalSource {
    async fn read(
        &self,
        _canary: &authz_entity_pb::PolicyCanary,
        _window_secs: i64,
    ) -> CanarySignal {
        CanarySignal {
            value: 0.0,
            samples: 0,
        }
    }
}

/// The side-effecting actions the evaluator drives. Implemented by the
/// `AuthzService` governance layer (governance_activate.rs) so the evaluator
/// itself stays free of SQL — and so it can be unit-tested against a fake.
#[async_trait::async_trait]
pub trait CanaryExecutor: Send + Sync {
    /// Every ACTIVE canary the evaluator should consider this cycle.
    async fn list_active_canaries(&self) -> Vec<authz_entity_pb::PolicyCanary>;

    /// Auto-rollback: transition the canary to ROLLED_BACK and restore the
    /// policy set's prior version through the real governance rollback path,
    /// emitting the high-severity audit + governance event. `reason` describes
    /// the breach.
    async fn auto_rollback(&self, canary: &authz_entity_pb::PolicyCanary, reason: &str);

    /// Inconclusive: move the canary to PAUSED (idempotent) + audit, but only the
    /// first time it transitions (so we don't re-emit every cycle).
    async fn pause(&self, canary: &authz_entity_pb::PolicyCanary, reason: &str);
}

/// One evaluation pass over a single canary. Pure orchestration: read signal →
/// decide → drive the executor. Returns the verdict (for metrics/tests).
pub async fn evaluate_one(
    canary: &authz_entity_pb::PolicyCanary,
    now_unix: i64,
    metrics_src: &dyn CanaryMetricSource,
    executor: &dyn CanaryExecutor,
    recorder: &Arc<dyn crate::metrics::MetricsRecorder>,
) -> CanaryVerdict {
    let policy = CanaryPolicy::from_canary(canary);
    let elapsed = now_unix.saturating_sub(canary_started_at_unix(canary));
    let signal = metrics_src.read(canary, policy.success_window_secs).await;
    let verdict = evaluate_canary(&policy, elapsed, signal);

    let breached = matches!(verdict, CanaryVerdict::Rollback);
    recorder.record_canary_evaluation(breached);

    match verdict {
        CanaryVerdict::Rollback => {
            let reason = format!(
                "canary metric breach: value {:.4} > threshold {:.4} ({} samples)",
                signal.value, policy.metric_threshold, signal.samples
            );
            recorder.inc_canary_auto_rollback(&reason);
            executor.auto_rollback(canary, &reason).await;
        }
        CanaryVerdict::Pause => {
            let reason = format!(
                "canary signal inconclusive: {} samples < {} required",
                signal.samples, policy.min_samples
            );
            executor.pause(canary, &reason).await;
        }
        // PromoteEligible / Hold: the evaluator does not auto-promote (promotion
        // is an explicit operator RPC); it just leaves the canary ACTIVE.
        CanaryVerdict::PromoteEligible | CanaryVerdict::Hold => {}
    }
    verdict
}

/// Spawn the background canary evaluator. Each cycle it lists ACTIVE canaries,
/// reads each one's success signal, and — per [`evaluate_canary`] — auto-rolls
/// back breaching canaries (restoring the prior version BEFORE fleet-wide
/// impact), pauses inconclusive ones, and leaves healthy ones to bake until an
/// operator promotes them. Calls `set_canary_active(active)` once per cycle to
/// reflect whether any canary is still baking.
///
/// The returned [`tokio::task::JoinHandle`] runs until the process exits; the
/// broker spawns it in `serve()` and may drop the handle (detached) or abort it
/// on shutdown.
pub fn spawn_canary_evaluator(
    executor: Arc<dyn CanaryExecutor>,
    metrics_src: Arc<dyn CanaryMetricSource>,
    recorder: Arc<dyn crate::metrics::MetricsRecorder>,
    interval: Duration,
    now_fn: Arc<dyn Fn() -> i64 + Send + Sync>,
) -> tokio::task::JoinHandle<()> {
    let interval = if interval.is_zero() {
        CANARY_EVAL_INTERVAL
    } else {
        interval
    };
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let canaries = executor.list_active_canaries().await;
            recorder.set_canary_active(!canaries.is_empty());
            let now = (now_fn)();
            for c in &canaries {
                evaluate_one(c, now, metrics_src.as_ref(), executor.as_ref(), &recorder).await;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signal(value: f64, samples: i64) -> CanarySignal {
        CanarySignal { value, samples }
    }

    fn policy(window: i64, threshold: f64, min_samples: i64) -> CanaryPolicy {
        CanaryPolicy {
            success_window_secs: window,
            metric_threshold: threshold,
            min_samples,
        }
    }

    // ── verdict: metric breach → rollback (even before window elapses) ──────
    #[test]
    fn breach_in_window_rolls_back() {
        let p = policy(300, 0.05, 10);
        // 50 samples, deny-rate 0.20 > 0.05, only 10s into a 300s window.
        let v = evaluate_canary(&p, 10, signal(0.20, 50));
        assert_eq!(v, CanaryVerdict::Rollback);
    }

    #[test]
    fn breach_rolls_back_at_exact_threshold_is_not_a_breach() {
        let p = policy(300, 0.05, 10);
        // Exactly at threshold is NOT a breach (strictly-greater contract).
        let v = evaluate_canary(&p, 10, signal(0.05, 50));
        assert_eq!(v, CanaryVerdict::Hold);
    }

    // ── verdict: within threshold + window elapsed → promote-eligible ───────
    #[test]
    fn healthy_after_window_is_promote_eligible() {
        let p = policy(300, 0.05, 10);
        let v = evaluate_canary(&p, 300, signal(0.01, 100));
        assert_eq!(v, CanaryVerdict::PromoteEligible);
    }

    #[test]
    fn healthy_inside_window_holds() {
        let p = policy(300, 0.05, 10);
        let v = evaluate_canary(&p, 120, signal(0.01, 100));
        assert_eq!(v, CanaryVerdict::Hold);
    }

    // ── verdict: insufficient samples → pause (never promote/rollback) ──────
    #[test]
    fn insufficient_samples_pauses() {
        let p = policy(300, 0.05, 100);
        // Window elapsed and value is healthy, but only 3 samples (< 100).
        let v = evaluate_canary(&p, 999, signal(0.0, 3));
        assert_eq!(v, CanaryVerdict::Pause);
    }

    #[test]
    fn insufficient_samples_even_with_high_value_pauses_not_rollback() {
        let p = policy(300, 0.05, 100);
        // High value but inconclusive: must NOT roll back on noise.
        let v = evaluate_canary(&p, 10, signal(0.9, 2));
        assert_eq!(v, CanaryVerdict::Pause);
    }

    #[test]
    fn zero_samples_pauses() {
        let p = policy(300, 0.05, 1);
        let v = evaluate_canary(&p, 400, signal(0.0, 0));
        assert_eq!(v, CanaryVerdict::Pause);
    }

    // ── scope membership: node ──────────────────────────────────────────────
    #[test]
    fn node_scope_membership() {
        let s = CanaryScope::from_row(
            CanaryScopeKind::Node,
            &["n1".into(), "n2".into(), "  ".into()],
        );
        assert!(s.node_in_scope("n1"));
        assert!(s.node_in_scope("n2"));
        assert!(!s.node_in_scope("n3"));
        // Node-scoped canary is not tenant-addressed.
        assert!(!s.tenant_in_scope("n1"));
    }

    // ── scope membership: tenant ────────────────────────────────────────────
    #[test]
    fn tenant_scope_membership() {
        let s = CanaryScope::from_row(CanaryScopeKind::Tenant, &["t1".into(), "t2".into()]);
        assert!(s.tenant_in_scope("t1"));
        assert!(!s.tenant_in_scope("t9"));
        assert!(!s.node_in_scope("t1"));
    }

    // ── scope membership: percent ───────────────────────────────────────────
    #[test]
    fn percent_zero_includes_nobody_hundred_includes_everybody() {
        let none = CanaryScope::from_row(CanaryScopeKind::Percent, &["0".into()]);
        let all = CanaryScope::from_row(CanaryScopeKind::Percent, &["100".into()]);
        for id in ["a", "b", "node-xyz", "tenant-42"] {
            assert!(!none.node_in_scope(id), "0% must exclude {id}");
            assert!(all.node_in_scope(id), "100% must include {id}");
            assert!(all.tenant_in_scope(id));
        }
    }

    #[test]
    fn percent_bucket_is_sticky_and_monotonic() {
        // A member at 10% must still be a member at 50% (bucket < percent).
        let ten = CanaryScope::from_row(CanaryScopeKind::Percent, &["10".into()]);
        let fifty = CanaryScope::from_row(CanaryScopeKind::Percent, &["50".into()]);
        let mut included_at_ten = 0usize;
        for i in 0..1000 {
            let id = format!("node-{i}");
            if ten.node_in_scope(&id) {
                included_at_ten += 1;
                assert!(fifty.node_in_scope(&id), "{id} in 10% must remain in 50%");
            }
        }
        // Roughly 10% of 1000 ids land in the 10% bucket (loose bounds).
        assert!(
            (40..=160).contains(&included_at_ten),
            "expected ~100 in-scope at 10%, got {included_at_ten}"
        );
    }

    #[test]
    fn percent_out_of_range_clamps() {
        let over = CanaryScope::from_row(CanaryScopeKind::Percent, &["250".into()]);
        assert_eq!(over, CanaryScope::Percent(100));
        let neg = CanaryScope::from_row(CanaryScopeKind::Percent, &["-5".into()]);
        assert_eq!(neg, CanaryScope::Percent(0));
        let garbage = CanaryScope::from_row(CanaryScopeKind::Percent, &["abc".into()]);
        assert_eq!(garbage, CanaryScope::Percent(0));
    }

    #[test]
    fn unspecified_scope_fails_closed() {
        let s = CanaryScope::from_row(CanaryScopeKind::Unspecified, &["n1".into()]);
        assert_eq!(s, CanaryScope::Percent(0));
        assert!(!s.node_in_scope("n1"));
    }

    #[test]
    fn scope_roundtrips_kind_and_values() {
        let nodes = CanaryScope::Nodes(vec!["n1".into(), "n2".into()]);
        assert_eq!(nodes.kind(), CanaryScopeKind::Node);
        assert_eq!(nodes.values(), vec!["n1".to_string(), "n2".to_string()]);
        let pct = CanaryScope::Percent(25);
        assert_eq!(pct.kind(), CanaryScopeKind::Percent);
        assert_eq!(pct.values(), vec!["25".to_string()]);
    }
}
