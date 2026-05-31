//! Multi-leg execution plans (U2 step 7).
//!
//! A logical operation that touches more than one backend (read PG rows +
//! Qdrant vectors, write PG canonical + Mongo projection, fan-out search
//! across two replicas) lowers to an `ExecutionPlan` of independent legs.
//! The dispatcher executes legs concurrently, collects per-leg results,
//! and applies a configurable partial-failure policy.
//!
//! `ExecutionPlan` itself is data — it carries `(backend, instance,
//! CompiledRendering, deadline, consistency_fence)` tuples. The actual
//! execution is `execute_plan`, which takes a closure `(leg) → Future` so
//! the runtime (or a unit test) can supply its own executor without the
//! IR knowing about `DispatchExecutor`.

use std::time::Duration;

use serde::Serialize;

use super::compile::CompiledRendering;
use crate::backend::BackendKind;

/// One leg of a multi-leg plan. A leg is the unit of scheduling, channel
/// admission, and circuit-breaker scoping.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Leg {
    pub backend: BackendKind,
    /// Named instance, or `None` for the default for this backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
    /// The pre-compiled wire shape the executor binds.
    pub rendering: CompiledRendering,
    /// Optional per-leg deadline. The runtime falls back to the channel
    /// deadline when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<Duration>,
    /// Optional consistency fence — caller must wait for the named outbox
    /// LSN (or projection task ID) to land before reading from this leg.
    /// Empty means "no fence."
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub consistency_fence: String,
    /// Stable name for tracing / debugging (e.g. `"customer_row"` for the
    /// PG leg vs `"customer_vector"` for the Qdrant leg).
    pub label: String,
}

impl Leg {
    pub fn new(label: impl Into<String>, rendering: CompiledRendering) -> Self {
        Self {
            backend: rendering.backend(),
            instance: None,
            rendering,
            deadline: None,
            consistency_fence: String::new(),
            label: label.into(),
        }
    }

    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = Some(instance.into());
        self
    }

    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub fn with_fence(mut self, fence: impl Into<String>) -> Self {
        self.consistency_fence = fence.into();
        self
    }
}

/// What the dispatcher should do when a leg fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartialFailurePolicy {
    /// First failing leg fails the whole plan. The default — safest for
    /// writes.
    FailFast,
    /// Wait for all legs; surface a vector of `(label, Result)` to the
    /// caller. Useful for reads where stale-on-one-leg is acceptable.
    Continue,
    /// At least N legs must succeed; otherwise fail. Mirrors a
    /// quorum-style read.
    Quorum {
        /// Minimum number of legs that must return Ok for the plan to
        /// be considered successful.
        required: usize,
    },
}

impl Default for PartialFailurePolicy {
    fn default() -> Self {
        Self::FailFast
    }
}

/// A multi-leg plan. The dispatcher iterates `legs` in declaration order
/// but executes them concurrently — the order is only used as a tie-break
/// when merging results.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionPlan {
    pub legs: Vec<Leg>,
    #[serde(default)]
    pub policy: PartialFailurePolicy,
    /// Hard upper bound on the whole plan; if reached, in-flight legs are
    /// cancelled and the plan fails. `None` = inherit from operation channel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overall_deadline: Option<Duration>,
}

impl ExecutionPlan {
    pub fn single(leg: Leg) -> Self {
        Self {
            legs: vec![leg],
            policy: PartialFailurePolicy::FailFast,
            overall_deadline: None,
        }
    }

    pub fn fanout(legs: Vec<Leg>) -> Self {
        Self {
            legs,
            policy: PartialFailurePolicy::Continue,
            overall_deadline: None,
        }
    }

    pub fn with_policy(mut self, policy: PartialFailurePolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn with_overall_deadline(mut self, deadline: Duration) -> Self {
        self.overall_deadline = Some(deadline);
        self
    }
}

/// Per-leg outcome surfaced by `execute_plan`. Carries the original `label`
/// so the merger can match legs back to their declared role.
#[derive(Debug, Clone)]
pub struct LegResult<T> {
    pub label: String,
    pub backend: BackendKind,
    pub instance: Option<String>,
    pub outcome: Result<T, LegError>,
}

/// A leg failure. Distinguishes "the executor said this didn't work" from
/// "we cancelled before the executor returned" — the partial-failure
/// policy treats them differently.
#[derive(Debug, Clone)]
pub enum LegError {
    /// The closure returned a typed error.
    Executor(String),
    /// The leg deadline elapsed before the closure returned.
    DeadlineExceeded,
    /// The overall plan deadline expired or the caller cancelled.
    Cancelled,
}

impl std::fmt::Display for LegError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Executor(m) => write!(f, "executor error: {m}"),
            Self::DeadlineExceeded => write!(f, "leg deadline exceeded"),
            Self::Cancelled => write!(f, "leg cancelled"),
        }
    }
}

impl std::error::Error for LegError {}

/// Aggregate verdict for a whole plan.
#[derive(Debug, Clone)]
pub struct PlanVerdict<T> {
    pub legs: Vec<LegResult<T>>,
    /// True if the policy considers the plan satisfied. The caller decides
    /// whether to bubble individual leg errors to the client.
    pub satisfied: bool,
}

/// Execute a plan by calling `run` once per leg concurrently and applying
/// the partial-failure policy.
///
/// `run` takes a cloned `Leg` (cheap — the rendering is the only big
/// field) so the returned future is `'static` and can outlive the borrow
/// of `plan`. The dispatcher closes over its own `DispatchExecutor`
/// handle inside `run`; tests plug a deterministic stub. This module
/// deliberately doesn't call into the runtime — that's the dispatcher's
/// job — so it stays in `crate::ir` cleanly.
pub async fn execute_plan<T, F, Fut>(plan: &ExecutionPlan, run: F) -> PlanVerdict<T>
where
    T: Send + 'static,
    F: Fn(Leg) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<T, String>> + Send,
{
    let mut handles = Vec::with_capacity(plan.legs.len());
    for leg in &plan.legs {
        let owned_leg = leg.clone();
        let leg_label = leg.label.clone();
        let leg_backend = leg.backend.clone();
        let leg_instance = leg.instance.clone();
        let leg_deadline = leg.deadline;
        let plan_deadline = plan.overall_deadline;
        let fut = run(owned_leg);
        // We don't actually need spawning — concurrent awaits via
        // `join_all` are sufficient and don't require Send-ness of T to
        // tokio. But for true parallelism across threads the runtime can
        // wrap this in `tokio::spawn`; keep the surface flexible.
        handles.push(async move {
            // Apply the tighter of the leg deadline and the overall plan
            // deadline. None means "no limit."
            let timeout = match (leg_deadline, plan_deadline) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(d), None) | (None, Some(d)) => Some(d),
                (None, None) => None,
            };
            let outcome = match timeout {
                Some(t) => match tokio::time::timeout(t, fut).await {
                    Ok(Ok(v)) => Ok(v),
                    Ok(Err(e)) => Err(LegError::Executor(e)),
                    Err(_) => Err(LegError::DeadlineExceeded),
                },
                None => match fut.await {
                    Ok(v) => Ok(v),
                    Err(e) => Err(LegError::Executor(e)),
                },
            };
            LegResult {
                label: leg_label,
                backend: leg_backend,
                instance: leg_instance,
                outcome,
            }
        });
    }

    let mut results: Vec<LegResult<T>> = futures::future::join_all(handles).await;

    // Sort by the original leg declaration order so the merger gets a
    // deterministic stream. (join_all preserves order today, but pinning
    // it here documents the contract.)
    results.sort_by_key(|r| {
        plan.legs
            .iter()
            .position(|l| l.label == r.label)
            .unwrap_or(usize::MAX)
    });

    let ok_count = results.iter().filter(|r| r.outcome.is_ok()).count();
    let satisfied = match plan.policy {
        PartialFailurePolicy::FailFast => ok_count == results.len(),
        PartialFailurePolicy::Continue => true,
        PartialFailurePolicy::Quorum { required } => ok_count >= required,
    };

    PlanVerdict {
        legs: results,
        satisfied,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::BackendKind;
    use crate::ir::compile::CompiledRendering;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn fake_sql(label: &str) -> Leg {
        Leg::new(
            label,
            CompiledRendering::Sql {
                backend: BackendKind::Postgres,
                statement: "SELECT 1".into(),
                params: vec![],
            },
        )
    }

    #[tokio::test]
    async fn fanout_returns_one_result_per_leg() {
        let plan = ExecutionPlan::fanout(vec![fake_sql("a"), fake_sql("b"), fake_sql("c")]);
        let verdict = execute_plan(
            &plan,
            |leg| async move { Ok::<_, String>(leg.label.clone()) },
        )
        .await;
        assert!(verdict.satisfied);
        assert_eq!(verdict.legs.len(), 3);
        let labels: Vec<_> = verdict.legs.iter().map(|r| r.label.as_str()).collect();
        assert_eq!(labels, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn fail_fast_marks_plan_unsatisfied_on_one_leg_failure() {
        let plan = ExecutionPlan::single(fake_sql("only"));
        let verdict = execute_plan(
            &plan,
            |_leg| async move { Err::<String, _>("boom".to_string()) },
        )
        .await;
        assert!(!verdict.satisfied);
        assert!(matches!(
            verdict.legs[0].outcome,
            Err(LegError::Executor(_))
        ));
    }

    #[tokio::test]
    async fn quorum_satisfied_when_enough_legs_succeed() {
        let plan = ExecutionPlan::fanout(vec![fake_sql("a"), fake_sql("b"), fake_sql("c")])
            .with_policy(PartialFailurePolicy::Quorum { required: 2 });

        // First two succeed, third fails.
        let counter = Arc::new(AtomicUsize::new(0));
        let verdict = execute_plan(&plan, |_leg| {
            let c = counter.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Ok::<_, String>(n as i32)
                } else {
                    Err("boom".into())
                }
            }
        })
        .await;
        assert!(verdict.satisfied);
    }

    #[tokio::test]
    async fn leg_deadline_triggers_deadline_exceeded() {
        let plan = ExecutionPlan::single(fake_sql("slow").with_deadline(Duration::from_millis(10)));
        let verdict = execute_plan(&plan, |_leg| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<_, String>(42)
        })
        .await;
        assert!(!verdict.satisfied);
        assert!(matches!(
            verdict.legs[0].outcome,
            Err(LegError::DeadlineExceeded)
        ));
    }

    #[tokio::test]
    async fn results_are_in_declaration_order() {
        let plan = ExecutionPlan::fanout(vec![
            fake_sql("a").with_deadline(Duration::from_millis(20)),
            fake_sql("b").with_deadline(Duration::from_millis(5)),
            fake_sql("c").with_deadline(Duration::from_millis(15)),
        ]);
        let verdict = execute_plan(&plan, |leg| {
            let lbl = leg.label.clone();
            async move {
                // Different sleeps so completion order != declaration order.
                let ms = match lbl.as_str() {
                    "a" => 10,
                    "b" => 1,
                    "c" => 5,
                    _ => 0,
                };
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok::<_, String>(lbl)
            }
        })
        .await;
        let labels: Vec<_> = verdict.legs.iter().map(|r| r.label.as_str()).collect();
        // Must be a, b, c (declaration order) even though completion
        // order was b, c, a.
        assert_eq!(labels, vec!["a", "b", "c"]);
    }
}
