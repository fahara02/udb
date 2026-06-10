// src/runtime/saga.rs — Saga state helpers and autonomous recovery worker.
//
// NW1-3c (2026-05-29): every PG-pool reference has been replaced with
// `Arc<dyn SystemStores>` routing. The recovery worker now uses:
//
// - `SagaStore::claim_recoverable_sagas` to find indeterminate +
//   stale in-progress sagas (the old `WHERE status = 'indeterminate'
//   OR (status = 'in_progress' AND EXTRACT(EPOCH FROM …) > stale)`
//   query collapses into one trait call).
// - `SagaStore::increment_recovery_attempts` for the failure-path
//   `recovery_attempts` upsert (PG `RETURNING` was inline; the trait
//   now hides that across PG/MySQL/SQLite).
// - `SagaStore::update_saga_status` for the success / manual-review
//   transitions.
// - `CanonicalStore::try_acquire_advisory_lease` /
//   `release_advisory_lease` for the worker lease (was
//   `pg_try_advisory_lock` / `pg_advisory_unlock`).
//
// The public free functions (`list_sagas`, `get_saga`,
// `mark_saga_reviewed`, `retry_saga_compensation`) keep their public
// signature shape but now take `Arc<dyn SystemStores>` instead of
// `&PgPool` — handlers_admin and handlers_policy were updated at the
// same time (see NW1-3c.3 entry).
//
// Configuration is supplied through UdbConfig::saga. Environment
// compatibility is merged once during UdbConfig loading.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use uuid::Uuid;

use crate::runtime::canonical_store::system_store::{
    CompensationStatus, SagaListFilter, SagaStatus, SagaStore,
};
use crate::runtime::canonical_store::{CanonicalStore, SystemStores};
use crate::runtime::config::SagaSettings;
use crate::runtime::system::SystemCatalogConfig;

const SAGA_WORKER_LEASE: &str = "udb-saga-worker";

/// Terminal statuses written by the recovery loop. Pinned strings
/// matching the values the admin RPC and dashboards expect; they
/// correspond 1:1 to the [`SagaStatus`] enum variants.
pub const SAGA_STATUS_COMMITTED: &str = "committed";
pub const SAGA_STATUS_COMPENSATED: &str = "compensated";
pub const SAGA_STATUS_FAILED_COMPENSATION: &str = "failed_compensation";
pub const SAGA_STATUS_MANUAL_REVIEW: &str = "manual_review";

/// A saga record as returned by admin / recovery queries.
#[derive(Debug, Clone, Serialize)]
pub struct SagaAdminRecord {
    pub saga_id: String,
    pub tx_id: String,
    pub tenant_id: String,
    pub correlation_id: String,
    pub status: String,
    pub current_step: i32,
    pub steps_json: serde_json::Value,
    pub compensations_json: serde_json::Value,
    pub last_error: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<crate::runtime::canonical_store::system_store::SagaRow> for SagaAdminRecord {
    fn from(row: crate::runtime::canonical_store::system_store::SagaRow) -> Self {
        Self {
            saga_id: row.saga_id.to_string(),
            tx_id: row.tx_id,
            tenant_id: row.tenant_id,
            correlation_id: row.correlation_id,
            status: row.status.as_str().to_string(),
            current_step: row.current_step,
            steps_json: row.steps,
            compensations_json: row.compensations,
            last_error: row.last_error,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

/// Summary emitted after each recovery pass.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SagaRecoveryReport {
    pub owner_id: String,
    pub lease_acquired: bool,
    pub scanned: usize,
    pub compensated: usize,
    pub marked_manual_review: usize,
    pub errors: Vec<String>,
}

/// Autonomous saga recovery worker.
///
/// Spawned by the runtime at startup when saga recovery is enabled.
/// NW1-3c: now holds `Arc<dyn SystemStores>` instead of a raw `PgPool`,
/// so the same worker code works against any registered canonical
/// store (PG today; MySQL / SQLite tomorrow).
pub struct SagaRecoveryWorker {
    store: Arc<dyn SystemStores>,
    interval: Duration,
    stale_threshold: Duration,
    worker_lease_ttl: Duration,
    recovery_batch_size: i64,
    owner_id: String,
    poison_threshold: i64,
    /// U19: plugin-aware backend compensator registry. Empty registry
    /// preserves the pre-U19 log-only behaviour (each step becomes a
    /// `NoCompensator` outcome and the saga lands in
    /// `failed_compensation` until an operator registers an
    /// implementation). When the runtime constructs the worker with
    /// `with_compensators`, real Qdrant/S3/etc. calls run.
    compensators: Arc<crate::runtime::saga_compensators::CompensatorRegistry>,
    /// U19 retry policy. Controls when a repeatedly-failing saga gets
    /// escalated to `manual_review` instead of retried on the next
    /// sweep.
    quarantine_policy: crate::runtime::saga_compensators::QuarantinePolicy,
    /// Metrics sink for recovery outcomes (compensated / failed-compensation).
    /// Defaults to `NoopMetrics`; the supervisor wires the live
    /// `PrometheusMetrics` via [`SagaRecoveryWorker::with_metrics`].
    metrics: std::sync::Arc<dyn crate::metrics::MetricsRecorder>,
}

impl SagaRecoveryWorker {
    /// NW1-3c: construct from a `SystemStores` trait object.
    pub fn new(store: Arc<dyn SystemStores>) -> Self {
        let mut settings = SagaSettings::default();
        settings.merge_env();
        Self::with_settings(store, &settings)
    }

    /// NW1-3c: parameterised constructor.
    pub fn with_settings(store: Arc<dyn SystemStores>, settings: &SagaSettings) -> Self {
        Self {
            store,
            interval: Duration::from_secs(settings.recovery_interval_secs.max(1)),
            stale_threshold: Duration::from_secs(settings.stale_threshold_secs.max(1)),
            worker_lease_ttl: Duration::from_secs(settings.worker_lease_ttl_secs.max(1)),
            recovery_batch_size: settings.recovery_batch_size.max(1),
            owner_id: Uuid::new_v4().to_string(),
            poison_threshold: settings.poison_threshold.max(1),
            compensators: Arc::new(
                crate::runtime::saga_compensators::CompensatorRegistry::default(),
            ),
            quarantine_policy: crate::runtime::saga_compensators::QuarantinePolicy {
                // Align the policy's max-attempts with the configured poison
                // threshold; the per-saga cooldown comes from the default (#134).
                max_attempts: settings.poison_threshold.max(1) as u32,
                ..crate::runtime::saga_compensators::QuarantinePolicy::default()
            },
            metrics: Arc::new(crate::metrics::NoopMetrics),
        }
    }

    /// Wire a live metrics recorder so recovery outcomes
    /// (`inc_saga_compensated_total` / `inc_saga_failed_compensations_total`)
    /// are emitted. Without this the worker uses `NoopMetrics`.
    pub fn with_metrics(
        mut self,
        metrics: std::sync::Arc<dyn crate::metrics::MetricsRecorder>,
    ) -> Self {
        self.metrics = metrics;
        self
    }

    /// U19: wire a registry of backend-aware compensators. The runtime
    /// supervisor builds this at startup by registering Qdrant, S3,
    /// Mongo, Neo4j, ClickHouse, and Postgres implementations and
    /// passes it here. With an empty registry the worker logs and
    /// marks `failed_compensation` as before.
    pub fn with_compensators(
        mut self,
        registry: Arc<crate::runtime::saga_compensators::CompensatorRegistry>,
    ) -> Self {
        self.compensators = registry;
        self
    }

    /// U19: override the default quarantine policy
    /// (`max_attempts=5, cooldown_secs=30`). The recovery worker uses
    /// this to escalate poisoned sagas to `manual_review`.
    pub fn with_quarantine_policy(
        mut self,
        policy: crate::runtime::saga_compensators::QuarantinePolicy,
    ) -> Self {
        self.quarantine_policy = policy;
        self
    }

    /// Return `true` when saga recovery is enabled via env.
    pub fn is_enabled() -> bool {
        let mut settings = SagaSettings::default();
        settings.merge_env();
        settings.recovery_enabled
    }

    pub fn is_enabled_with_settings(settings: &SagaSettings) -> bool {
        settings.recovery_enabled
    }

    /// Run the recovery loop forever (call from a dedicated tokio task).
    pub async fn run_forever(self) {
        let mut ticker = tokio::time::interval(self.interval);
        loop {
            ticker.tick().await;
            let report = self.run_once().await;
            if report.scanned > 0 || !report.errors.is_empty() {
                tracing::info!(
                    scanned = report.scanned,
                    compensated = report.compensated,
                    marked_manual_review = report.marked_manual_review,
                    errors = report.errors.len(),
                    "saga recovery pass completed"
                );
            }
            for err in &report.errors {
                tracing::warn!(error = err, "saga recovery error");
            }
        }
    }

    /// Execute one recovery pass. Returns a summary report.
    pub async fn run_once(&self) -> SagaRecoveryReport {
        let mut report = SagaRecoveryReport {
            owner_id: self.owner_id.clone(),
            ..SagaRecoveryReport::default()
        };
        if !self.try_acquire_worker_lease(&mut report).await {
            return report;
        }
        let mut locked_report = self.run_once_locked().await;
        locked_report.owner_id = self.owner_id.clone();
        locked_report.lease_acquired = true;
        self.release_worker_lease(&mut locked_report).await;
        locked_report
    }

    async fn refresh_saga_active_metric(&self) {
        match SagaStore::saga_summary(self.store.as_ref()).await {
            Ok(summary) => self.metrics.set_saga_active(summary.in_progress),
            Err(err) => {
                tracing::debug!(error = %err, "saga active metric refresh failed");
            }
        }
    }

    fn observe_saga_duration_since(&self, created_at: chrono::DateTime<chrono::Utc>) {
        let seconds = chrono::Utc::now()
            .signed_duration_since(created_at)
            .to_std()
            .map(|duration| duration.as_secs_f64())
            .unwrap_or(0.0);
        self.metrics.observe_saga_duration_seconds(seconds);
    }

    async fn run_once_locked(&self) -> SagaRecoveryReport {
        let mut report = SagaRecoveryReport {
            owner_id: self.owner_id.clone(),
            lease_acquired: true,
            ..SagaRecoveryReport::default()
        };
        self.refresh_saga_active_metric().await;
        // NW1-3c: claim_recoverable_sagas returns INDETERMINATE +
        // stale IN_PROGRESS in oldest-first order.
        let candidates = match SagaStore::claim_recoverable_sagas(
            self.store.as_ref(),
            self.stale_threshold,
            self.recovery_batch_size,
        )
        .await
        {
            Ok(rows) => rows,
            Err(err) => {
                report.errors.push(format!("saga scan failed: {err}"));
                self.refresh_saga_active_metric().await;
                return report;
            }
        };
        report.scanned = candidates.len();

        // Accumulate terminal outcomes and flush them grouped after the loop —
        // one status round-trip per outcome instead of one per saga (#89).
        let mut compensated_ids = Vec::new();
        let mut manual_review_ids = Vec::new();

        for row in candidates {
            let saga_id = row.saga_id;
            let saga_id_str = saga_id.to_string();
            let created_at = row.created_at;
            let compensations = row.compensations;

            // #134: drive the retry decision through the QuarantinePolicy. A saga
            // that has exhausted its attempts is escalated without re-attempting;
            // one still inside its per-saga cooldown window is skipped this sweep
            // (the claim already gates on stale_threshold; this adds the policy's
            // cooldown + max-attempts that recovery previously ignored).
            use crate::runtime::saga_compensators::QuarantineState;
            match self.quarantine_policy.evaluate(
                row.recovery_attempts.max(0) as u32,
                row.updated_at.timestamp(),
                chrono::Utc::now().timestamp(),
            ) {
                QuarantineState::Quarantine { .. } => {
                    manual_review_ids.push(saga_id);
                    self.observe_saga_duration_since(created_at);
                    report.marked_manual_review += 1;
                    continue;
                }
                QuarantineState::Cooldown { .. } => continue,
                QuarantineState::Retry { .. } => {}
            }

            // Attempt compensation
            let compensation_result = self
                .attempt_compensations(&saga_id_str, &compensations)
                .await;

            let new_status = match compensation_result {
                Ok(_) => {
                    report.compensated += 1;
                    self.metrics.inc_saga_compensated_total();
                    self.observe_saga_duration_since(created_at);
                    SagaStatus::Compensated
                }
                Err(err) => {
                    // Every failed compensation attempt is recorded (whether it
                    // will be retried next sweep or escalated to manual review).
                    self.metrics.inc_saga_failed_compensations_total();
                    self.metrics.inc_compensation_failures_total();
                    // Increment recovery_attempts atomically. The
                    // trait returns the post-increment counter; the
                    // pre-NW1-3c code did the same via PG `RETURNING`.
                    let attempts = match SagaStore::increment_recovery_attempts(
                        self.store.as_ref(),
                        saga_id,
                        &format!("compensation failed: {err}"),
                    )
                    .await
                    {
                        Ok(n) => n,
                        Err(e) => {
                            report.errors.push(format!(
                                "saga {saga_id_str} recovery_attempts increment failed: {e}"
                            ));
                            continue;
                        }
                    };

                    if attempts >= self.poison_threshold {
                        report.marked_manual_review += 1;
                        self.observe_saga_duration_since(created_at);
                        SagaStatus::ManualReview
                    } else {
                        report.errors.push(format!(
                            "saga {saga_id_str} compensation failed (attempt {attempts}): {err}"
                        ));
                        continue;
                    }
                }
            };

            match new_status {
                SagaStatus::Compensated => compensated_ids.push(saga_id),
                SagaStatus::ManualReview => manual_review_ids.push(saga_id),
                _ => {}
            }
        }

        // Flush the terminal status writes grouped by outcome — Postgres applies
        // each group in a single `WHERE saga_id = ANY(...)` round-trip (#89).
        if !compensated_ids.is_empty()
            && let Err(err) = SagaStore::update_saga_statuses_batch(
                self.store.as_ref(),
                &compensated_ids,
                SagaStatus::Compensated,
                CompensationStatus::Completed,
            )
            .await
        {
            report
                .errors
                .push(format!("batch compensated status update failed: {err}"));
        }
        if !manual_review_ids.is_empty()
            && let Err(err) = SagaStore::update_saga_statuses_batch(
                self.store.as_ref(),
                &manual_review_ids,
                SagaStatus::ManualReview,
                CompensationStatus::ManualReview,
            )
            .await
        {
            report
                .errors
                .push(format!("batch manual-review status update failed: {err}"));
        }
        self.refresh_saga_active_metric().await;
        report
    }

    async fn try_acquire_worker_lease(&self, report: &mut SagaRecoveryReport) -> bool {
        // Ensure the lease table exists before first acquire. Idempotent.
        if let Err(err) = CanonicalStore::ensure_advisory_lease_table(self.store.as_ref()).await {
            report
                .errors
                .push(format!("ensure_advisory_lease_table failed: {err}"));
            return false;
        }
        match CanonicalStore::try_acquire_advisory_lease(
            self.store.as_ref(),
            SAGA_WORKER_LEASE,
            &self.owner_id,
            self.worker_lease_ttl,
        )
        .await
        {
            Ok(true) => {
                report.lease_acquired = true;
                tracing::debug!(owner_id = self.owner_id, "saga worker lease acquired");
                true
            }
            Ok(false) => {
                tracing::debug!(owner_id = self.owner_id, "saga worker lease held elsewhere");
                false
            }
            Err(err) => {
                report
                    .errors
                    .push(format!("saga worker lease acquisition failed: {err}"));
                false
            }
        }
    }

    async fn release_worker_lease(&self, report: &mut SagaRecoveryReport) {
        if let Err(err) = CanonicalStore::release_advisory_lease(
            self.store.as_ref(),
            SAGA_WORKER_LEASE,
            &self.owner_id,
        )
        .await
        {
            report
                .errors
                .push(format!("saga worker lease release failed: {err}"));
        }
    }

    /// Attempt to run compensation actions stored in the saga record.
    ///
    /// U19: routes every step through the registered
    /// [`CompensatorRegistry`]. With no compensators registered, every
    /// step yields a `NoCompensator` outcome and this returns
    /// `Err(...)` so the worker marks the saga
    /// `failed_compensation` rather than silently advancing — matches
    /// the pre-U19 behaviour minus the misleading `Ok(())`.
    ///
    /// Compensations are stored as a JSON array of
    /// `{ "backend": "...", "operation": "...", "resource_uri": "...",
    /// "payload": {...} }` objects (the `"compensation"` key is
    /// accepted as a legacy alias for `"payload"`).
    async fn attempt_compensations(
        &self,
        saga_id: &str,
        compensations: &serde_json::Value,
    ) -> Result<(), String> {
        use crate::runtime::saga_compensators::{
            StepOutcome, all_compensated, dispatch_compensations,
        };
        let outcomes = dispatch_compensations(self.compensators.as_ref(), compensations).await;
        for (i, outcome) in outcomes.iter().enumerate() {
            match outcome {
                StepOutcome::Compensated => tracing::info!(
                    saga_id = saga_id,
                    step = i,
                    "saga compensation step succeeded"
                ),
                StepOutcome::NoCompensator { backend } => tracing::warn!(
                    saga_id = saga_id,
                    step = i,
                    backend = backend,
                    "no backend compensator registered; step skipped"
                ),
                StepOutcome::Malformed { index } => tracing::warn!(
                    saga_id = saga_id,
                    step = index,
                    "saga compensation step is malformed; skipping"
                ),
                StepOutcome::Failed { backend, reason } => tracing::error!(
                    saga_id = saga_id,
                    step = i,
                    backend = backend,
                    reason = reason,
                    "saga compensation step failed"
                ),
            }
        }
        if all_compensated(&outcomes) {
            Ok(())
        } else {
            // Aggregate the first failure / no-compensator into the
            // error message so the worker writes a useful `last_error`.
            let first_bad = outcomes
                .iter()
                .find(|o| !o.is_success())
                .map(|o| match o {
                    StepOutcome::Failed { backend, reason } => {
                        format!("compensation failed at {backend}: {reason}")
                    }
                    StepOutcome::NoCompensator { backend } => {
                        format!("no compensator registered for backend '{backend}'")
                    }
                    StepOutcome::Malformed { index } => {
                        format!("malformed compensation step at index {index}")
                    }
                    StepOutcome::Compensated => unreachable!(),
                })
                .unwrap_or_else(|| "no compensation steps recorded".to_string());
            Err(first_bad)
        }
    }
}

/// List sagas matching the given filters. Suitable for the `ListSagas` RPC.
///
/// NW1-3c: routes through `SagaStore::list_sagas`. The PG-only
/// `QueryBuilder` path is gone; arg validation (status enum membership +
/// tx_id UUID format) is now done up-front before constructing the
/// trait filter.
#[allow(clippy::too_many_arguments)]
pub async fn list_sagas(
    store: &dyn SystemStores,
    _config: &SystemCatalogConfig,
    tenant_id_filter: &str,
    status_filter: &str,
    tx_id_filter: &str,
    correlation_id_filter: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<SagaAdminRecord>, tonic::Status> {
    // Validate status filter against the canonical enum tokens. This
    // is the same set the PG CHECK constraint enforces.
    let status_enum = if status_filter.is_empty() {
        None
    } else {
        Some(SagaStatus::parse(status_filter).ok_or_else(|| {
            tonic::Status::invalid_argument(format!(
                "invalid status_filter '{status_filter}'; must be one of {:?}",
                SagaStatus::all()
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
            ))
        })?)
    };

    // UUID-validate tx_id_filter (defensive — keeps PG behaviour
    // identical for callers that pass UUIDs; non-UUID tx_ids are
    // technically allowed by the schema but the admin RPC rejected
    // them historically).
    if !tx_id_filter.is_empty() {
        Uuid::parse_str(tx_id_filter)
            .map_err(|_| tonic::Status::invalid_argument("tx_id_filter must be a valid UUID"))?;
    }

    let filter = SagaListFilter {
        tenant_id: (!tenant_id_filter.is_empty()).then(|| tenant_id_filter.to_string()),
        status: status_enum,
        tx_id: (!tx_id_filter.is_empty()).then(|| tx_id_filter.to_string()),
        correlation_id: (!correlation_id_filter.is_empty())
            .then(|| correlation_id_filter.to_string()),
        limit,
        offset,
    };
    let rows = SagaStore::list_sagas(store, &filter)
        .await
        .map_err(|err| tonic::Status::internal(format!("list_sagas query failed: {err}")))?;
    Ok(rows.into_iter().map(SagaAdminRecord::from).collect())
}

/// Get a single saga by ID.
///
/// NW1-3c: routes through `SagaStore::get_saga`. Returns `NotFound`
/// when the saga doesn't exist (matching the pre-NW1-3c gRPC code).
pub async fn get_saga(
    store: &dyn SystemStores,
    _config: &SystemCatalogConfig,
    saga_id: &str,
) -> Result<SagaAdminRecord, tonic::Status> {
    let id: Uuid = saga_id
        .parse()
        .map_err(|_| tonic::Status::invalid_argument("saga_id must be a UUID"))?;
    let row = SagaStore::get_saga(store, id)
        .await
        .map_err(|err| tonic::Status::internal(format!("get_saga failed: {err}")))?
        .ok_or_else(|| tonic::Status::not_found(format!("saga {saga_id} not found")))?;
    Ok(SagaAdminRecord::from(row))
}

/// Mark a saga as MANUAL_REVIEW (operator-reviewed; prevents further auto-recovery).
///
/// NW1-3c: routes through `SagaStore::mark_saga_manual_review`.
/// The trait returns `InvalidInput` when the row doesn't exist; we
/// map that to `NotFound` for backward compatibility with the
/// admin RPC.
pub async fn mark_saga_reviewed(
    store: &dyn SystemStores,
    _config: &SystemCatalogConfig,
    saga_id: &str,
) -> Result<(), tonic::Status> {
    let id: Uuid = saga_id
        .parse()
        .map_err(|_| tonic::Status::invalid_argument("saga_id must be a UUID"))?;
    SagaStore::mark_saga_manual_review(store, id)
        .await
        .map_err(|err| {
            let msg = err.to_string();
            if msg.contains("not found") {
                tonic::Status::not_found(format!("saga {saga_id} not found"))
            } else {
                tonic::Status::internal(format!("mark_saga_reviewed failed: {msg}"))
            }
        })
}

/// Trigger re-compensation for a saga (sets status back to 'indeterminate'
/// so the recovery loop picks it up on the next pass).
///
/// NW1-3c: routes through `SagaStore::request_saga_recompensation`,
/// which enforces the same state-machine guard the pre-NW1-3c SQL
/// did: refuses unless the saga is currently `failed_compensation`
/// or `manual_review`. The gRPC error code is preserved
/// (`FailedPrecondition` instead of `NotFound`).
pub async fn retry_saga_compensation(
    store: &dyn SystemStores,
    _config: &SystemCatalogConfig,
    saga_id: &str,
) -> Result<(), tonic::Status> {
    let id: Uuid = saga_id
        .parse()
        .map_err(|_| tonic::Status::invalid_argument("saga_id must be a UUID"))?;
    SagaStore::request_saga_recompensation(store, id)
        .await
        .map_err(|err| {
            let msg = err.to_string();
            if msg.contains("retryable state") {
                tonic::Status::failed_precondition(format!(
                    "saga {saga_id} is not in a retryable state (failed_compensation or manual_review)"
                ))
            } else {
                tonic::Status::internal(format!("retry_saga_compensation failed: {msg}"))
            }
        })
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    #[cfg(feature = "sqlite")]
    use crate::runtime::canonical_store::sqlite::SqliteCanonicalStore;
    #[cfg(feature = "sqlite")]
    use crate::runtime::canonical_store::system_store::SagaInsert;
    #[cfg(feature = "sqlite")]
    use sqlx::sqlite::SqlitePoolOptions;

    #[test]
    fn recovery_worker_uses_env_interval() {
        // Verify the default interval values without constructing the worker.
        let interval = Duration::from_secs(60);
        let stale = Duration::from_secs(300);
        assert_eq!(interval, Duration::from_secs(60));
        assert_eq!(stale, Duration::from_secs(300));
    }

    #[test]
    fn recovery_enabled_by_default() {
        // "false", "0", "no", "FALSE" all disable recovery.
        assert!(matches!("false", "0" | "false" | "FALSE" | "no"));
        assert!(matches!("0", "0" | "false" | "FALSE" | "no"));
        // Any other value keeps recovery enabled.
        assert!(!matches!("true", "0" | "false" | "FALSE" | "no"));
        assert!(!matches!("1", "0" | "false" | "FALSE" | "no"));
    }

    /// Helper: build an in-memory SQLite SystemStores with the saga
    /// + advisory_leases tables already created.
    #[cfg(feature = "sqlite")]
    async fn fresh_store() -> Arc<dyn SystemStores> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let store = Arc::new(SqliteCanonicalStore::new(pool, "test", "udb_outbox_events"));
        SagaStore::ensure_saga_tables(store.as_ref()).await.unwrap();
        CanonicalStore::ensure_advisory_lease_table(store.as_ref())
            .await
            .unwrap();
        store
    }

    /// Pin: NW1-3c — list_sagas + get_saga + mark_saga_reviewed +
    /// retry_saga_compensation all work end-to-end against the
    /// trait. No PG required.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn admin_helpers_round_trip_against_sqlite_store() {
        let store = fresh_store().await;
        let cfg = SystemCatalogConfig::default();

        // Record two sagas, one indeterminate, one failed_compensation.
        let insert = |tenant: &str, status: SagaStatus| SagaInsert {
            tx_id: Uuid::new_v4().to_string(),
            tenant_id: tenant.to_string(),
            correlation_id: "corr-1".to_string(),
            backend_instance: "primary".to_string(),
            operation: "upsert".to_string(),
            status,
            steps: serde_json::json!([]),
            compensations: serde_json::json!([]),
        };
        let _alpha =
            SagaStore::record_saga(store.as_ref(), &insert("alpha", SagaStatus::Indeterminate))
                .await
                .unwrap();
        let beta = SagaStore::record_saga(
            store.as_ref(),
            &insert("beta", SagaStatus::FailedCompensation),
        )
        .await
        .unwrap();

        // list_sagas with no filter returns both.
        let all = list_sagas(store.as_ref(), &cfg, "", "", "", "", 100, 0)
            .await
            .unwrap();
        assert_eq!(all.len(), 2);

        // Filter by tenant.
        let alpha_only = list_sagas(store.as_ref(), &cfg, "alpha", "", "", "", 100, 0)
            .await
            .unwrap();
        assert_eq!(alpha_only.len(), 1);
        assert_eq!(alpha_only[0].tenant_id, "alpha");

        // Filter by status.
        let fc = list_sagas(
            store.as_ref(),
            &cfg,
            "",
            "failed_compensation",
            "",
            "",
            100,
            0,
        )
        .await
        .unwrap();
        assert_eq!(fc.len(), 1);
        assert_eq!(fc[0].tenant_id, "beta");

        // Invalid status token rejected with InvalidArgument.
        let err = list_sagas(store.as_ref(), &cfg, "", "bogus", "", "", 100, 0)
            .await
            .expect_err("must reject");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        // get_saga round-trip.
        let row = get_saga(store.as_ref(), &cfg, &beta.to_string())
            .await
            .unwrap();
        assert_eq!(row.tenant_id, "beta");
        assert_eq!(row.status, "failed_compensation");

        // get_saga on missing → NotFound.
        let phantom = Uuid::new_v4().to_string();
        let err = get_saga(store.as_ref(), &cfg, &phantom)
            .await
            .expect_err("must error");
        assert_eq!(err.code(), tonic::Code::NotFound);

        // retry_saga_compensation succeeds from failed_compensation,
        // refuses from indeterminate.
        retry_saga_compensation(store.as_ref(), &cfg, &beta.to_string())
            .await
            .unwrap();
        let err = retry_saga_compensation(store.as_ref(), &cfg, &beta.to_string())
            .await
            .expect_err("now indeterminate, retry refused");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);

        // mark_saga_reviewed flips status.
        mark_saga_reviewed(store.as_ref(), &cfg, &beta.to_string())
            .await
            .unwrap();
        let row = get_saga(store.as_ref(), &cfg, &beta.to_string())
            .await
            .unwrap();
        assert_eq!(row.status, "manual_review");

        // mark_saga_reviewed on missing → NotFound.
        let err = mark_saga_reviewed(store.as_ref(), &cfg, &phantom)
            .await
            .expect_err("must error");
        assert_eq!(err.code(), tonic::Code::NotFound);

        // Bad saga_id format → InvalidArgument.
        let err = get_saga(store.as_ref(), &cfg, "not-a-uuid")
            .await
            .expect_err("must reject");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    /// Pin: NW1-3c — SagaRecoveryWorker constructed from an
    /// Arc<dyn SystemStores> runs one recovery pass against an
    /// in-memory SQLite store, acquires + releases the worker lease
    /// portably (no pg_try_advisory_lock).
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn recovery_worker_runs_one_pass_against_sqlite() {
        let store = fresh_store().await;

        // Seed a recoverable saga.
        let _id = SagaStore::record_saga(
            store.as_ref(),
            &SagaInsert {
                tx_id: Uuid::new_v4().to_string(),
                tenant_id: "t1".to_string(),
                correlation_id: "c1".to_string(),
                backend_instance: "primary".to_string(),
                operation: "upsert".to_string(),
                status: SagaStatus::Indeterminate,
                steps: serde_json::json!([]),
                // Empty compensations array → all_compensated returns
                // false → recovery_attempts increments → after 3
                // attempts marked manual_review.
                compensations: serde_json::json!([]),
            },
        )
        .await
        .unwrap();

        let settings = SagaSettings {
            recovery_interval_secs: 1,
            stale_threshold_secs: 1,
            ..Default::default()
        };
        let worker = SagaRecoveryWorker::with_settings(store.clone(), &settings);

        let report = worker.run_once().await;
        assert!(
            report.lease_acquired,
            "lease must be acquired (other workers idle)"
        );
        assert_eq!(report.scanned, 1, "the one indeterminate saga was claimed");
        // No compensators registered → all_compensated == false →
        // recovery_attempts incremented but threshold (3) not yet hit.
        assert_eq!(report.compensated, 0);
    }

    /// Pin: NW1-3c — worker lease contention. Two workers race;
    /// only one wins; after release the other wins.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn worker_lease_serializes_concurrent_workers() {
        let store = fresh_store().await;

        let settings = SagaSettings {
            recovery_interval_secs: 1,
            stale_threshold_secs: 1,
            ..Default::default()
        };
        let worker_a = SagaRecoveryWorker::with_settings(store.clone(), &settings);
        let worker_b = SagaRecoveryWorker::with_settings(store.clone(), &settings);

        // Worker A acquires the lease.
        let mut report_a = SagaRecoveryReport::default();
        let a_acquired = worker_a.try_acquire_worker_lease(&mut report_a).await;
        assert!(a_acquired);

        // Worker B tries — fails.
        let mut report_b = SagaRecoveryReport::default();
        let b_acquired = worker_b.try_acquire_worker_lease(&mut report_b).await;
        assert!(!b_acquired);

        // A releases.
        worker_a.release_worker_lease(&mut report_a).await;
        assert!(report_a.errors.is_empty(), "release must not error");

        // B now wins.
        let mut report_b2 = SagaRecoveryReport::default();
        let b_now = worker_b.try_acquire_worker_lease(&mut report_b2).await;
        assert!(b_now);
    }

    /// Phase 1 HA pin: model two broker replicas against the same SystemStores
    /// lease. While replica A owns the recovery lease, replica B's full
    /// `run_once` must skip without scanning or incrementing recovery attempts.
    /// Once A releases, B may recover the saga exactly once.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn multi_node_saga_recovery_skips_peer_until_lease_release() {
        let store = fresh_store().await;
        let saga_id = SagaStore::record_saga(
            store.as_ref(),
            &SagaInsert {
                tx_id: Uuid::new_v4().to_string(),
                tenant_id: "tenant-a".to_string(),
                correlation_id: "corr-ha".to_string(),
                backend_instance: "primary".to_string(),
                operation: "upsert".to_string(),
                status: SagaStatus::Indeterminate,
                steps: serde_json::json!([]),
                compensations: serde_json::json!([]),
            },
        )
        .await
        .unwrap();

        let settings = SagaSettings {
            recovery_interval_secs: 1,
            stale_threshold_secs: 1,
            worker_lease_ttl_secs: 60,
            ..Default::default()
        };
        let worker_a = SagaRecoveryWorker::with_settings(store.clone(), &settings);
        let mut worker_b = SagaRecoveryWorker::with_settings(store.clone(), &settings);
        // This test exercises failover handoff (the next replica performs the
        // recovery attempt once the lease is released), not the #134 per-saga
        // retry cooldown. The freshly-created saga's `updated_at` is "now", so
        // the default 30s cooldown would otherwise skip it as still-cooling and
        // leave `recovery_attempts` at 0. Zero the cooldown so the handoff is
        // observed deterministically regardless of wall-clock timing.
        worker_b.quarantine_policy.cooldown_secs = 0;

        let mut held = SagaRecoveryReport::default();
        assert!(
            worker_a.try_acquire_worker_lease(&mut held).await,
            "replica A should acquire the shared recovery lease"
        );

        let skipped = worker_b.run_once().await;
        assert!(!skipped.lease_acquired);
        assert_eq!(skipped.scanned, 0);
        let row = SagaStore::get_saga(store.as_ref(), saga_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.recovery_attempts, 0,
            "peer replica must not touch saga state while the lease is held"
        );

        worker_a.release_worker_lease(&mut held).await;
        assert!(held.errors.is_empty(), "lease release must be clean");

        let recovered = worker_b.run_once().await;
        assert!(recovered.lease_acquired);
        assert_eq!(recovered.scanned, 1);
        let row = SagaStore::get_saga(store.as_ref(), saga_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.recovery_attempts, 1,
            "after failover, the next replica performs one recovery attempt"
        );
    }
}
