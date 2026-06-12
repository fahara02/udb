//! Shared lifecycle host for native-service background workers (P6.4).
//!
//! Every native service that runs a periodic singleton task (storage orphan
//! reaper, webrtc stale-peer reaper, …) previously hand-rolled the same
//! `tokio::spawn` + interval loop + [`singleton::run_while_leader`] +
//! five-arm result-logging boilerplate. [`NativeWorkerHost`] is the one place
//! that boilerplate lives, so a native worker is declared as just: a Postgres
//! pool, the singleton relation, a `WORKER_*` lease key, a tick interval, and a
//! task that returns `Result<i64, E>` (the count of rows it acted on).
//!
//! It *composes* [`crate::runtime::singleton`] — it never re-implements lease
//! acquisition, heartbeating, or fencing. The architecture guard requires native
//! background work to go through this host rather than a bare `tokio::spawn`
//! loop, so leader-election can never be silently dropped.

use std::future::Future;
use std::time::Duration;

use sqlx::PgPool;

/// Lifecycle host for native-service singleton workers.
pub(crate) struct NativeWorkerHost;

impl NativeWorkerHost {
    /// Spawn a periodic, leader-elected native worker.
    ///
    /// On every `interval` tick the host attempts to acquire the singleton lease
    /// keyed by `worker_name` against `relation`; the leader runs `task` once and
    /// the result count is logged under `label`. Non-leaders skip silently. The
    /// task is best-effort: a failure is logged, never fatal, and the loop
    /// continues — identical posture to the reapers it replaces.
    ///
    /// `task` is a factory invoked once per tick so each run gets a fresh future
    /// (callers typically `clone()` their service handle inside it).
    pub(crate) fn spawn_while_leader<F, Fut, E>(
        worker_name: &'static str,
        label: &'static str,
        pool: PgPool,
        relation: String,
        interval: Duration,
        task: F,
    ) where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<i64, E>> + Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let fut = task();
                match crate::runtime::singleton::run_while_leader(
                    &pool,
                    &relation,
                    worker_name,
                    crate::runtime::singleton::WORKER_SINGLETON_LEASE_TTL,
                    move || fut,
                )
                .await
                {
                    Ok(Some(Ok(n))) if n > 0 => {
                        tracing::info!(worker = worker_name, acted = n, "{}", label)
                    }
                    Ok(Some(Ok(_))) => {}
                    Ok(Some(Err(err))) => {
                        tracing::warn!(worker = worker_name, error = %err, "{}: run failed", label)
                    }
                    Ok(None) => tracing::debug!(
                        worker = worker_name,
                        "{}: skipped, singleton lease held by peer",
                        label
                    ),
                    Err(err) => tracing::warn!(
                        worker = worker_name,
                        error = %err,
                        "{}: singleton lease failed",
                        label
                    ),
                }
            }
        });
    }
}
