// src/metrics.rs — MetricsRecorder interface for the migration engine.
//
// The `MetricsRecorder` trait decouples the engine from any specific metrics
// backend (Prometheus, OpenTelemetry, Datadog, etc.).
//
// Usage (Go UDB service side):
//   Pass a `Box<dyn MetricsRecorder + Send + Sync>` via `MigrationOptions.metrics`.
//   The engine calls the trait methods at key migration lifecycle events.
//
// A zero-allocation no-op implementation (`NoopMetrics`) is used by default
// when no recorder is configured, matching the legacy_sql pattern in metrics.go.
//
// Aligned with:
//   - legacy_sql  legacycore/db/metrics.go  (MetricsRecorder interface + noopMetrics)

// ── Trait ─────────────────────────────────────────────────────────────────────

/// Interface for emitting migration engine metrics.
///
/// Implement this trait and supply it via `MigrationOptions::metrics` to wire
/// in your preferred metrics backend (Prometheus, OTEL, Datadog, etc.).
pub trait MetricsRecorder: Send + Sync + std::fmt::Debug {
    fn record_grpc(&self, _method: &str, _status: &str, _seconds: f64) {}
    fn observe_pg_query(&self, _op: &str, _table: &str, _seconds: f64) {}
    fn inc_cache_op(&self, _op: &str, _hit: bool) {}
    fn inc_vector_op(&self, _collection: &str, _op: &str) {}
    /// Scatter-gather fan-out width: `_count` backend targets were resolved for a
    /// single data-plane request to `_backend`.
    fn inc_backend_fanout(&self, _backend: &str, _count: u64) {}
    fn inc_object_op(&self, _bucket: &str, _method: &str) {}
    fn inc_channel_inflight(&self, _channel: &str) {}
    fn dec_channel_inflight(&self, _channel: &str) {}
    fn inc_channel_rejected(&self, _channel: &str) {}
    fn inc_channel_timeout(&self, _channel: &str) {}
    fn observe_channel_latency(&self, _channel: &str, _seconds: f64) {}
    fn record_fair_admission(
        &self,
        _project: &str,
        _tenant_hash: &str,
        _backend: &str,
        _instance: &str,
        _op: &str,
        _status: &str,
    ) {
    }
    fn add_fair_cost(
        &self,
        _project: &str,
        _tenant_hash: &str,
        _backend: &str,
        _instance: &str,
        _op: &str,
        _cost: f64,
    ) {
    }

    /// Increment the migration run counter.
    ///
    /// `status` is one of: `"completed"`, `"failed"`, `"blocked"`.
    fn inc_runs_total(&self, status: &str);

    /// Increment the per-operation counter.
    ///
    /// * `kind`   — `ChangeKind` or repair kind label, e.g. `"add_column"`,
    ///   `"qdrant_create_collection"`, `"minio_bucket_create"`.
    /// * `schema` — Target resource namespace: SQL schema name, Qdrant collection,
    ///   or MinIO bucket depending on the backend tier.
    /// * `safety` — `"auto"`, `"requires_review"`, or `"blocked"`.
    fn inc_operations_total(&self, kind: &str, schema: &str, safety: &str);

    /// Record the wall-clock duration of applying one migration artefact, in seconds.
    ///
    /// * `schema`  — Target resource namespace (SQL schema, collection, or bucket).
    /// * `seconds` — Duration as a floating-point number of seconds.
    fn observe_file_duration(&self, schema: &str, seconds: f64);

    /// Set the current count of blocked (non-auto-applicable) operations.
    ///
    /// Called once after each diff pass with the latest blocked operation count.
    fn set_blocked_operations(&self, schema: &str, kind: &str, count: i64);

    /// Increment the lint warning counter.
    ///
    /// `kind` is the `LintItem.kind` string, e.g. `"missing_primary_key"`,
    /// `"qdrant_collection_missing"`, `"minio_bucket_missing"`.
    fn inc_lint_warnings(&self, kind: &str);

    /// Record the total duration of a complete migration run, in seconds.
    ///
    /// `status` is one of: `"completed"`, `"failed"`, `"blocked"`.
    fn observe_run_duration(&self, status: &str, seconds: f64);

    /// Emit a gauge for the number of pending migration artefacts (SQL files +
    /// Qdrant collections + MinIO buckets not yet applied).
    fn set_pending_files(&self, count: i64);

    // ── CDC Metrics ───────────────────────────────────────────────────────────────

    fn set_cdc_is_leader(&self, host: &str, is_leader: bool);
    fn inc_cdc_wal_messages_received_total(&self);
    fn inc_cdc_events_published_total(&self, topic: &str);
    fn inc_cdc_errors_total(&self, reason: &str);
    fn set_cdc_lag_seconds(&self, seconds: f64);
    fn set_cdc_outbox_depth(&self, depth: i64);
    fn set_cdc_dlq_depth(&self, depth: i64);
    fn observe_cdc_publish_duration_seconds(&self, seconds: f64);
    fn inc_cdc_dlq_replayed_total(&self);
    fn inc_cdc_duplicate_skipped_total(&self);

    // ── Saga Metrics ─────────────────────────────────────────────────────────

    /// Set the current count of active (IN_PROGRESS) sagas.
    fn set_saga_active(&self, count: i64);
    /// Increment the count of sagas that reached COMPENSATED terminal state.
    fn inc_saga_compensated_total(&self);
    /// Increment the count of compensation actions that failed.
    fn inc_saga_failed_compensations_total(&self);
    /// Record the wall-clock duration of a complete saga transaction, in seconds.
    fn observe_saga_duration_seconds(&self, seconds: f64);

    // ── Projection Metrics (U3) ───────────────────────────────────────

    /// Set the current count of PENDING projection tasks for a backend.
    fn set_projection_tasks_pending(&self, backend: &str, instance: &str, kind: &str, count: i64);
    /// Increment the completed projection task counter.
    fn inc_projection_tasks_completed_total(&self, backend: &str, instance: &str, kind: &str);
    /// Increment the failed projection task counter.
    fn inc_projection_tasks_failed_total(&self, backend: &str, instance: &str, kind: &str);
    /// Observe the wall-clock lag between task creation and completion, in seconds.
    fn observe_projection_lag_seconds(
        &self,
        backend: &str,
        instance: &str,
        kind: &str,
        seconds: f64,
    );
    /// Increment the reconciliation repair counter.
    fn inc_projection_reconciliation_repairs_total(&self, backend: &str, instance: &str);
    /// Set the current **oldest pending task age** for a `(project, backend,
    /// instance, kind)` group. Unlike `observe_projection_lag_seconds`, which
    /// only fires when a task completes, this gauge tracks the wall-clock age
    /// of the oldest still-PENDING/FAILED task — so operators see lag growing
    /// even when the worker is stalled or completion isn't happening.
    /// Closes the U3 "projection lag visible per project/backend/resource"
    /// acceptance gate.
    fn set_projection_oldest_pending_age_seconds(
        &self,
        project: &str,
        backend: &str,
        instance: &str,
        kind: &str,
        age_seconds: f64,
    );

    // ── Auth-plane Metrics (Phase 5) ──────────────────────────────────────────

    /// Increment the login-attempt counter, labelled by outcome.
    ///
    /// `success == true` counts a successful authentication; `false` counts a
    /// rejected credential / failed login.
    fn record_auth_login(&self, _success: bool) {}
    /// Increment the account-lockout counter (a principal locked out after too
    /// many failed attempts).
    fn record_auth_lockout(&self) {}
    /// Increment the MFA-verification-failure counter (a second-factor check that
    /// did not pass).
    fn record_auth_mfa_failure(&self) {}
    /// Increment the token-validation-failure counter (an access/refresh token
    /// that failed signature, expiry, or claim verification).
    fn record_auth_token_validation_failure(&self) {}
    /// Increment the authorization-deny counter (a Casbin / policy decision that
    /// denied access).
    fn record_authz_deny(&self) {}

    // ── Phase L (Compliance Audit / Monitoring) auth metrics ──────────────────
    /// Increment the refresh-token-family reuse-detection counter (a replayed or
    /// stolen refresh token was detected and the family revoked).
    fn record_refresh_reuse_detected(&self) {}
    /// Observe the wall-clock latency of an authz policy snapshot reload, in
    /// seconds.
    fn observe_policy_reload_seconds(&self, _seconds: f64) {}
    /// Observe the lag (seconds) between a policy-bundle invalidation being
    /// emitted and a node applying it.
    fn observe_policy_invalidation_lag_seconds(&self, _seconds: f64) {}
    /// Observe the lag (seconds) between a revocation (token/session/device) and
    /// its propagation taking effect cluster-wide.
    fn observe_revocation_propagation_seconds(&self, _seconds: f64) {}
    /// Increment the IdP discovery/JWKS refresh-failure counter, labelled by
    /// `kind` (`"discovery"` or `"jwks"`).
    fn record_idp_refresh_failure(&self, _kind: &str) {}
    /// Increment the SCIM provisioning-failure counter, labelled by `op`
    /// (`"provision"`, `"update"`, `"deactivate"`).
    fn record_scim_failure(&self, _op: &str) {}
    /// Increment the audit-export-sink failure counter, labelled by `sink`.
    fn record_audit_sink_failure(&self, _sink: &str) {}
    /// Set the current auth event outbox lag (oldest un-relayed auth event age),
    /// in seconds.
    fn set_auth_outbox_lag_seconds(&self, _seconds: f64) {}

    // ── Phase 10 (Audit / Observability / SLOs) metrics ───────────────────────
    /// Increment the per-RPC method-security denial counter, labelled by `reason`
    /// (e.g. `"missing_bearer"`, `"scope"`, `"role"`, `"csrf"`, `"tenant"`,
    /// `"disabled"`). Driven by the `method_security` tower layer.
    fn inc_method_security_denial(&self, _reason: &str) {}
    /// Increment the tenant-mismatch counter (a request whose tenant metadata did
    /// not match the bearer-token tenant). Driven by `method_security`.
    fn inc_tenant_mismatch(&self) {}
    /// Increment the revocation-lookup-failure counter (a token/session/device
    /// revocation check that could not complete — fail-closed lookups in the auth
    /// lifecycle path). Call site lives in `auth_service::lifecycle` (other lane).
    fn inc_revocation_lookup_failure(&self) {}
    /// Increment the outbox-enqueue-failure counter, labelled by `path`
    /// (`"native"`, `"auth"`, or `"native_compliance"`). Records when a durable
    /// audit/event row could not be enqueued (or was rejected pre-enqueue).
    fn inc_outbox_enqueue_failures_total(&self, _path: &str) {}
    /// Set the native-service degraded gauge for `service` (1 = degraded /
    /// fail-closed, 0 = healthy). Driven by readiness/doctor (other lane).
    fn set_native_service_degraded(&self, _service: &str, _degraded: bool) {}
    /// Set the per-tenant DB-connection budget starvation gauge for `_tenant`
    /// (1 = the tenant is at its connection budget and QUEUEING for a slot,
    /// 0 = it holds / just released a slot). Driven by the connection-pool
    /// tiering admission path (`connection_manager::acquire_tenant_connection`),
    /// which extends the channels.rs admission fairness down to the driver/pool
    /// layer so one tenant at budget queues on a bounded deadline instead of
    /// draining the shared pool. The tenant label is bounded so a noisy tenant
    /// id cannot mint unbounded series.
    fn set_tenant_connection_starved(&self, _tenant: &str, _starved: bool) {}
    /// Increment the compensation-failure counter (a saga/native compensation
    /// step that failed to undo its forward action). Distinct from
    /// `inc_saga_failed_compensations_total` so non-saga native compensations
    /// (e.g. vector-point rollback) are countable on the same SLO surface.
    fn inc_compensation_failures_total(&self) {}
    /// A CDC delivery-journal write failed (the durable publish-evidence row could
    /// not be updated). In strict modes this is the signal that ack/delete must
    /// not proceed — losing the journal write loses delivery proof.
    fn inc_cdc_journal_failures_total(&self) {}
    // ── Phase 9: control-plane policy distribution + canary ──────────────────
    /// A control-plane in-process reload was applied for `resource_type` (the
    /// subscriber detected a registry/world change and refreshed it).
    fn inc_control_reload_applied(&self, _resource_type: &str) {}
    /// A control-plane node NACKed a pushed version for `resource_type` — it
    /// rejected the distributed config and kept its last-good version (the
    /// `store::record_nack` path: reject-without-silently-diverging). Default
    /// no-op so `NoopMetrics` and custom recorders are unaffected.
    fn inc_control_nack(&self, _resource_type: &str) {}
    /// Current depth of the control-plane node-push queue (in-flight pushes).
    fn set_control_push_queue_depth(&self, _depth: u64) {}
    /// A control-plane push was throttled by the concurrent-push semaphore.
    fn inc_control_push_throttled(&self) {}
    /// A burst of registry version bumps was coalesced into one push (debounce).
    fn inc_control_debounce_coalesced(&self) {}
    /// A canary policy version auto-rolled back, labelled by `reason`.
    fn inc_canary_auto_rollback(&self, _reason: &str) {}
    /// One canary evaluation cycle observed a metric `breached`(=true) result.
    fn record_canary_evaluation(&self, _breached: bool) {}
    /// Set whether any canary is currently ACTIVE (1) or none (0).
    fn set_canary_active(&self, _active: bool) {}

    /// Increment the raw-dispatch counter, labelled by `backend` kind.
    ///
    /// Counts a generic-dispatch request that bypassed the neutral-IR compiler
    /// (no `ir` envelope) for a *mediated* backend. In production this path is
    /// gated off entirely; in dev it is permitted but counted here so drift away
    /// from the mediated (tenant/predicate-injected) path stays observable.
    fn inc_raw_dispatch_total(&self, _backend: &str) {}
}

use prometheus::Encoder;

// ── No-op implementation ──────────────────────────────────────────────────────

/// Zero-allocation no-op implementation used when no metrics backend is configured.
///
/// All methods are intentional no-ops and are expected to be inlined away by the
/// compiler.  Matches the `noopMetrics` struct in legacy_sql `metrics.go`.
#[derive(Debug, Clone, Default)]
pub struct NoopMetrics;

impl MetricsRecorder for NoopMetrics {
    #[inline]
    fn inc_runs_total(&self, _status: &str) {}

    #[inline]
    fn inc_operations_total(&self, _kind: &str, _schema: &str, _safety: &str) {}

    #[inline]
    fn observe_file_duration(&self, _schema: &str, _seconds: f64) {}

    #[inline]
    fn set_blocked_operations(&self, _schema: &str, _kind: &str, _count: i64) {}

    #[inline]
    fn inc_lint_warnings(&self, _kind: &str) {}

    #[inline]
    fn observe_run_duration(&self, _status: &str, _seconds: f64) {}

    #[inline]
    fn set_pending_files(&self, _count: i64) {}

    #[inline]
    fn set_cdc_is_leader(&self, _host: &str, _is_leader: bool) {}

    #[inline]
    fn inc_cdc_wal_messages_received_total(&self) {}

    #[inline]
    fn inc_cdc_events_published_total(&self, _topic: &str) {}

    #[inline]
    fn inc_cdc_errors_total(&self, _reason: &str) {}

    #[inline]
    fn set_cdc_lag_seconds(&self, _seconds: f64) {}

    #[inline]
    fn set_cdc_outbox_depth(&self, _depth: i64) {}

    #[inline]
    fn set_cdc_dlq_depth(&self, _depth: i64) {}

    #[inline]
    fn observe_cdc_publish_duration_seconds(&self, _seconds: f64) {}

    #[inline]
    fn inc_cdc_dlq_replayed_total(&self) {}

    #[inline]
    fn inc_cdc_duplicate_skipped_total(&self) {}

    #[inline]
    fn set_saga_active(&self, _count: i64) {}

    #[inline]
    fn inc_saga_compensated_total(&self) {}

    #[inline]
    fn inc_saga_failed_compensations_total(&self) {}

    #[inline]
    fn observe_saga_duration_seconds(&self, _seconds: f64) {}

    #[inline]
    fn set_projection_tasks_pending(
        &self,
        _backend: &str,
        _instance: &str,
        _kind: &str,
        _count: i64,
    ) {
    }
    #[inline]
    fn inc_projection_tasks_completed_total(&self, _backend: &str, _instance: &str, _kind: &str) {}
    #[inline]
    fn inc_projection_tasks_failed_total(&self, _backend: &str, _instance: &str, _kind: &str) {}
    #[inline]
    fn observe_projection_lag_seconds(
        &self,
        _backend: &str,
        _instance: &str,
        _kind: &str,
        _seconds: f64,
    ) {
    }
    #[inline]
    fn inc_projection_reconciliation_repairs_total(&self, _backend: &str, _instance: &str) {}
    fn set_projection_oldest_pending_age_seconds(
        &self,
        _project: &str,
        _backend: &str,
        _instance: &str,
        _kind: &str,
        _age_seconds: f64,
    ) {
    }
}

// ── Helper ────────────────────────────────────────────────────────────────────

/// Returns a reference to `recorder` if `Some`, otherwise falls back to a
/// shared static `NoopMetrics` instance.
///
/// Usage:
/// ```rust
/// use udb::metrics::{metrics_or_noop, NoopMetrics};
/// let m = metrics_or_noop(None);
/// m.inc_runs_total("completed");
/// ```
pub fn metrics_or_noop(recorder: Option<&dyn MetricsRecorder>) -> &dyn MetricsRecorder {
    static NOOP: NoopMetrics = NoopMetrics;
    recorder.unwrap_or(&NOOP)
}

// ── Safety labels ─────────────────────────────────────────────────────────────

/// Converts a `ChangeSafety` variant label into the canonical metrics string.
///
/// Used so the caller does not need to import the `migration` module when
/// recording metrics.
pub fn safety_label(safety: &str) -> &str {
    match safety {
        "SafeAuto" => "auto",
        "RequiresReview" => "requires_review",
        "Blocked" => "blocked",
        _ => safety,
    }
}

#[derive(Clone)]
pub struct PrometheusMetrics {
    registry: prometheus::Registry,
    grpc_requests: prometheus::IntCounterVec,
    grpc_duration: prometheus::HistogramVec,
    pg_duration: prometheus::HistogramVec,
    cache_ops: prometheus::IntCounterVec,
    vector_ops: prometheus::IntCounterVec,
    backend_fanout: prometheus::IntCounterVec,
    object_ops: prometheus::IntCounterVec,
    cdc_events: prometheus::IntCounterVec,
    cdc_errors: prometheus::IntCounterVec,
    cdc_wal_messages: prometheus::IntCounter,
    cdc_is_leader: prometheus::IntGaugeVec,
    cdc_lag: prometheus::Gauge,
    cdc_outbox_depth: prometheus::IntGauge,
    cdc_dlq_depth: prometheus::IntGauge,
    cdc_publish_latency: prometheus::Histogram,
    cdc_dlq_replayed: prometheus::IntCounter,
    cdc_duplicate_skipped: prometheus::IntCounter,
    saga_active: prometheus::IntGauge,
    saga_compensated: prometheus::IntCounter,
    saga_failed_compensations: prometheus::IntCounter,
    // Migration / startup-lifecycle metrics (item 8): collectors that back the
    // formerly no-op MetricsRecorder migration methods.
    migration_runs: prometheus::IntCounterVec,
    migration_operations: prometheus::IntCounterVec,
    migration_blocked: prometheus::IntGaugeVec,
    migration_lint_warnings: prometheus::IntCounterVec,
    migration_pending_files: prometheus::IntGauge,
    saga_duration: prometheus::Histogram,
    channel_inflight: prometheus::IntGaugeVec,
    channel_rejected: prometheus::IntCounterVec,
    channel_timeout: prometheus::IntCounterVec,
    channel_latency: prometheus::HistogramVec,
    fair_admission: prometheus::IntCounterVec,
    fair_cost: prometheus::CounterVec,
    // U3 — Projection metrics
    projection_tasks_pending: prometheus::IntGaugeVec,
    projection_tasks_completed: prometheus::IntCounterVec,
    projection_tasks_failed: prometheus::IntCounterVec,
    projection_lag: prometheus::HistogramVec,
    projection_reconciliation_repairs: prometheus::IntCounterVec,
    projection_oldest_pending_age: prometheus::GaugeVec,
    // Auth-plane metrics (Phase 5)
    auth_login: prometheus::IntCounterVec,
    auth_lockouts: prometheus::IntCounter,
    auth_mfa_failures: prometheus::IntCounter,
    auth_token_validation_failures: prometheus::IntCounter,
    authz_denies: prometheus::IntCounter,
    // Phase L (compliance audit / monitoring) auth-plane metrics
    auth_refresh_reuse: prometheus::IntCounter,
    authz_policy_reload: prometheus::Histogram,
    authz_policy_invalidation_lag: prometheus::Histogram,
    auth_revocation_propagation: prometheus::Histogram,
    idp_refresh_failures: prometheus::IntCounterVec,
    scim_failures: prometheus::IntCounterVec,
    audit_sink_failures: prometheus::IntCounterVec,
    auth_outbox_lag: prometheus::Gauge,
    // Phase 10 (audit / observability / SLOs) collectors
    method_security_denials: prometheus::IntCounterVec,
    tenant_mismatch: prometheus::IntCounter,
    revocation_lookup_failures: prometheus::IntCounter,
    outbox_enqueue_failures: prometheus::IntCounterVec,
    native_service_degraded: prometheus::IntGaugeVec,
    connection_tenant_budget_starved: prometheus::IntGaugeVec,
    compensation_failures: prometheus::IntCounter,
    cdc_journal_failures: prometheus::IntCounter,
    // Phase 9 (control-plane policy distribution + canary) collectors
    control_reload_applied: prometheus::IntCounterVec,
    control_nack: prometheus::IntCounterVec,
    control_push_queue_depth: prometheus::IntGauge,
    control_push_throttled: prometheus::IntCounter,
    control_debounce_coalesced: prometheus::IntCounter,
    canary_auto_rollback: prometheus::IntCounterVec,
    canary_evaluations: prometheus::IntCounterVec,
    canary_active: prometheus::IntGauge,
    // IR mediated-by-default (item 2.1): raw generic-dispatch fall-throughs that
    // bypassed the neutral-IR compiler for a mediated backend, by backend kind.
    raw_dispatch: prometheus::IntCounterVec,
}

impl std::fmt::Debug for PrometheusMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusMetrics").finish_non_exhaustive()
    }
}

impl PrometheusMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = prometheus::Registry::new();
        let grpc_requests = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_grpc_requests_total",
                "UDB gRPC requests by method and status",
            ),
            &["method", "status"],
        )?;
        let grpc_duration = prometheus::HistogramVec::new(
            prometheus::HistogramOpts::new(
                "udb_grpc_duration_seconds",
                "UDB gRPC request duration",
            ),
            &["method"],
        )?;
        let pg_duration = prometheus::HistogramVec::new(
            prometheus::HistogramOpts::new(
                "udb_pg_query_duration_seconds",
                "PostgreSQL query duration",
            ),
            &["op", "table"],
        )?;
        let cache_ops = prometheus::IntCounterVec::new(
            prometheus::Opts::new("udb_cache_ops_total", "Redis cache operations"),
            &["op", "hit"],
        )?;
        let vector_ops = prometheus::IntCounterVec::new(
            prometheus::Opts::new("udb_vector_ops_total", "Vector backend operations"),
            &["collection", "op"],
        )?;
        let backend_fanout = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_backend_fanout_total",
                "Scatter-gather data-plane fan-out: count of backend targets resolved, by backend kind",
            ),
            &["backend"],
        )?;
        let object_ops = prometheus::IntCounterVec::new(
            prometheus::Opts::new("udb_object_ops_total", "Object backend operations"),
            &["bucket", "method"],
        )?;
        let cdc_events = prometheus::IntCounterVec::new(
            prometheus::Opts::new("udb_cdc_events_published_total", "CDC events published"),
            &["topic"],
        )?;
        let cdc_errors = prometheus::IntCounterVec::new(
            prometheus::Opts::new("udb_cdc_errors_total", "CDC errors"),
            &["reason"],
        )?;
        let cdc_wal_messages = prometheus::IntCounter::new(
            "udb_cdc_wal_messages_received_total",
            "Total WAL messages decoded from the logical replication slot",
        )?;
        let cdc_is_leader = prometheus::IntGaugeVec::new(
            prometheus::Opts::new("udb_cdc_is_leader", "CDC leader state"),
            &["host"],
        )?;
        let cdc_lag = prometheus::Gauge::new("udb_cdc_lag_seconds", "CDC lag in seconds")?;
        let cdc_outbox_depth =
            prometheus::IntGauge::new("udb_cdc_outbox_depth", "CDC outbox depth")?;
        let cdc_dlq_depth =
            prometheus::IntGauge::new("udb_cdc_dlq_depth", "Current number of open DLQ events")?;
        let cdc_publish_latency = prometheus::Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "udb_cdc_publish_latency_seconds",
                "End-to-end Kafka publish latency per event (outbox row created → ack received)",
            )
            .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0]),
        )?;
        let cdc_dlq_replayed = prometheus::IntCounter::new(
            "udb_cdc_dlq_replayed_total",
            "Total DLQ events replayed successfully",
        )?;
        let cdc_duplicate_skipped = prometheus::IntCounter::new(
            "udb_cdc_duplicate_skipped_total",
            "Total duplicate CDC events skipped via Redis idempotency guard",
        )?;
        let saga_active =
            prometheus::IntGauge::new("udb_saga_active_total", "Active (IN_PROGRESS) sagas")?;
        let saga_compensated = prometheus::IntCounter::new(
            "udb_saga_compensated_total",
            "Sagas that reached COMPENSATED terminal state",
        )?;
        let saga_failed_compensations = prometheus::IntCounter::new(
            "udb_saga_failed_compensations_total",
            "Saga compensation actions that failed",
        )?;
        // Migration / startup-lifecycle collectors (item 8).
        let migration_runs = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_migration_runs_total",
                "Startup migration runs by terminal status",
            ),
            &["status"],
        )?;
        let migration_operations = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_migration_operations_total",
                "Migration schema operations by kind, schema, and safety",
            ),
            &["kind", "schema", "safety"],
        )?;
        let migration_blocked = prometheus::IntGaugeVec::new(
            prometheus::Opts::new(
                "udb_migration_blocked_operations",
                "Blocked / requires-review schema operations by schema and kind",
            ),
            &["schema", "kind"],
        )?;
        let migration_lint_warnings = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_migration_lint_warnings_total",
                "Migration lint warnings by kind",
            ),
            &["kind"],
        )?;
        let migration_pending_files = prometheus::IntGauge::new(
            "udb_migration_pending_files",
            "Pending migration files awaiting apply",
        )?;
        let saga_duration = prometheus::Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "udb_saga_duration_seconds",
                "Wall-clock duration of a complete saga transaction",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0]),
        )?;
        let channel_inflight = prometheus::IntGaugeVec::new(
            prometheus::Opts::new("udb_channel_inflight", "In-flight requests per channel"),
            &["channel"],
        )?;
        let channel_rejected = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_channel_rejected_total",
                "Rejected requests per channel",
            ),
            &["channel"],
        )?;
        let channel_timeout = prometheus::IntCounterVec::new(
            prometheus::Opts::new("udb_channel_timeout_total", "Timeout count per channel"),
            &["channel"],
        )?;
        let channel_latency = prometheus::HistogramVec::new(
            prometheus::HistogramOpts::new(
                "udb_channel_latency_seconds",
                "Latency histogram per channel",
            ),
            &["channel"],
        )?;
        let fair_admission = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_fair_admission_total",
                "Fair admission decisions by project, tenant hash, backend, instance, operation, and result",
            ),
            &[
                "project",
                "tenant_hash",
                "backend",
                "instance",
                "operation",
                "result",
            ],
        )?;
        let fair_cost = prometheus::CounterVec::new(
            prometheus::Opts::new(
                "udb_fair_cost_units_total",
                "Cost units admitted by project, tenant hash, backend, instance, and operation",
            ),
            &["project", "tenant_hash", "backend", "instance", "operation"],
        )?;
        // U3 — Projection metrics
        let projection_tasks_pending = prometheus::IntGaugeVec::new(
            prometheus::Opts::new(
                "udb_projection_tasks_pending",
                "Current count of PENDING projection tasks",
            ),
            &["backend", "instance", "kind"],
        )?;
        let projection_tasks_completed = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_projection_tasks_completed_total",
                "Total completed projection tasks",
            ),
            &["backend", "instance", "kind"],
        )?;
        let projection_tasks_failed = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_projection_tasks_failed_total",
                "Total failed projection tasks",
            ),
            &["backend", "instance", "kind"],
        )?;
        let projection_lag = prometheus::HistogramVec::new(
            prometheus::HistogramOpts::new(
                "udb_projection_lag_seconds",
                "Wall-clock lag between task creation and completion",
            )
            .buckets(vec![0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0]),
            &["backend", "instance", "kind"],
        )?;
        let projection_reconciliation_repairs = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_projection_reconciliation_repairs_total",
                "Total projection tasks re-enqueued by the reconciliation worker",
            ),
            &["backend", "instance"],
        )?;
        let projection_oldest_pending_age = prometheus::GaugeVec::new(
            prometheus::Opts::new(
                "udb_projection_oldest_pending_age_seconds",
                "Wall-clock age of the oldest PENDING/FAILED projection task per \
                 (project, backend, instance, kind). Unlike the completion-time \
                 lag histogram, this rises even when the worker is stalled.",
            ),
            &["project", "backend", "instance", "kind"],
        )?;
        // Auth-plane collectors (Phase 5)
        let auth_login = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_auth_login_total",
                "Authentication login attempts by outcome",
            ),
            &["outcome"],
        )?;
        let auth_lockouts = prometheus::IntCounter::new(
            "udb_auth_lockouts_total",
            "Total account lockouts triggered by failed-attempt thresholds",
        )?;
        let auth_mfa_failures = prometheus::IntCounter::new(
            "udb_auth_mfa_failures_total",
            "Total MFA second-factor verification failures",
        )?;
        let auth_token_validation_failures = prometheus::IntCounter::new(
            "udb_auth_token_validation_failures_total",
            "Total access/refresh token validation failures",
        )?;
        let authz_denies = prometheus::IntCounter::new(
            "udb_authz_denies_total",
            "Total authorization decisions that denied access",
        )?;
        // Phase L collectors
        let auth_refresh_reuse = prometheus::IntCounter::new(
            "udb_auth_refresh_reuse_detected_total",
            "Total refresh-token-family reuse detections (replayed/stolen tokens)",
        )?;
        let authz_policy_reload = prometheus::Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "udb_authz_policy_reload_seconds",
                "Wall-clock latency of an authz policy snapshot reload",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
        )?;
        let authz_policy_invalidation_lag = prometheus::Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "udb_authz_policy_invalidation_lag_seconds",
                "Lag between a policy-bundle invalidation emit and a node applying it",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0]),
        )?;
        let auth_revocation_propagation = prometheus::Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "udb_auth_revocation_propagation_seconds",
                "Lag between a token/session/device revocation and cluster-wide effect",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0]),
        )?;
        let idp_refresh_failures = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_idp_refresh_failures_total",
                "IdP discovery/JWKS refresh failures by kind",
            ),
            &["kind"],
        )?;
        let scim_failures = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_scim_failures_total",
                "SCIM provisioning failures by operation",
            ),
            &["op"],
        )?;
        let audit_sink_failures = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_audit_sink_failures_total",
                "Immutable audit-export sink failures by sink",
            ),
            &["sink"],
        )?;
        let auth_outbox_lag = prometheus::Gauge::new(
            "udb_auth_outbox_lag_seconds",
            "Age of the oldest un-relayed auth event in the outbox",
        )?;
        // Phase 10 collectors
        let method_security_denials = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_method_security_denials_total",
                "Per-RPC method-security denials by reason",
            ),
            &["reason"],
        )?;
        let tenant_mismatch = prometheus::IntCounter::new(
            "udb_tenant_mismatch_total",
            "Requests rejected because tenant metadata did not match the bearer token",
        )?;
        let revocation_lookup_failures = prometheus::IntCounter::new(
            "udb_revocation_lookup_failures_total",
            "Token/session/device revocation checks that could not complete (fail-closed)",
        )?;
        let outbox_enqueue_failures = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_outbox_enqueue_failures_total",
                "Outbox enqueue failures by path (native / auth / native_compliance)",
            ),
            &["path"],
        )?;
        let native_service_degraded = prometheus::IntGaugeVec::new(
            prometheus::Opts::new(
                "udb_native_service_degraded",
                "Native service degraded/fail-closed state (1 = degraded) by service",
            ),
            &["service"],
        )?;
        let connection_tenant_budget_starved = prometheus::IntGaugeVec::new(
            prometheus::Opts::new(
                "udb_connection_tenant_budget_starved",
                "Per-tenant DB-connection budget starvation (1 = tenant at budget and queueing for a slot) by tenant",
            ),
            &["tenant"],
        )?;
        let compensation_failures = prometheus::IntCounter::new(
            "udb_compensation_failures_total",
            "Saga/native compensation steps that failed to undo their forward action",
        )?;
        let cdc_journal_failures = prometheus::IntCounter::new(
            "udb_cdc_journal_failures_total",
            "CDC delivery-journal write failures (lost publish evidence)",
        )?;
        // ── Phase 9: control-plane policy distribution + canary ──────────────
        let control_reload_applied = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_control_reload_applied_total",
                "Control-plane in-process reloads applied by resource_type",
            ),
            &["resource_type"],
        )?;
        let control_nack = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_control_nack_total",
                "Control-plane node NACKs (rejected pushed versions) by resource_type",
            ),
            &["resource_type"],
        )?;
        let control_push_queue_depth = prometheus::IntGauge::new(
            "udb_control_push_queue_depth",
            "Control-plane node-push queue depth (in-flight pushes)",
        )?;
        let control_push_throttled = prometheus::IntCounter::new(
            "udb_control_push_throttled_total",
            "Control-plane pushes throttled by the concurrent-push semaphore",
        )?;
        let control_debounce_coalesced = prometheus::IntCounter::new(
            "udb_control_debounce_coalesced_total",
            "Control-plane registry version-bump bursts coalesced into one push",
        )?;
        let canary_auto_rollback = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_canary_auto_rollback_total",
                "Canary policy versions auto-rolled-back by reason",
            ),
            &["reason"],
        )?;
        let canary_evaluations = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_canary_evaluations_total",
                "Canary evaluation cycles by outcome (breached / ok)",
            ),
            &["outcome"],
        )?;
        let canary_active = prometheus::IntGauge::new(
            "udb_canary_active",
            "Whether any canary policy version is currently ACTIVE (1) or none (0)",
        )?;
        let raw_dispatch = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "udb_raw_dispatch_total",
                "Generic-dispatch requests that bypassed the neutral-IR compiler \
                 (raw spec) for a mediated backend, by backend kind",
            ),
            &["backend"],
        )?;

        for collector in [
            Box::new(grpc_requests.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(grpc_duration.clone()),
            Box::new(pg_duration.clone()),
            Box::new(cache_ops.clone()),
            Box::new(vector_ops.clone()),
            Box::new(backend_fanout.clone()),
            Box::new(object_ops.clone()),
            Box::new(cdc_events.clone()),
            Box::new(cdc_errors.clone()),
            Box::new(cdc_wal_messages.clone()),
            Box::new(cdc_is_leader.clone()),
            Box::new(cdc_lag.clone()),
            Box::new(cdc_outbox_depth.clone()),
            Box::new(cdc_dlq_depth.clone()),
            Box::new(cdc_publish_latency.clone()),
            Box::new(cdc_dlq_replayed.clone()),
            Box::new(cdc_duplicate_skipped.clone()),
            Box::new(saga_active.clone()),
            Box::new(saga_compensated.clone()),
            Box::new(saga_failed_compensations.clone()),
            Box::new(saga_duration.clone()),
            Box::new(migration_runs.clone()),
            Box::new(migration_operations.clone()),
            Box::new(migration_blocked.clone()),
            Box::new(migration_lint_warnings.clone()),
            Box::new(migration_pending_files.clone()),
            Box::new(channel_inflight.clone()),
            Box::new(channel_rejected.clone()),
            Box::new(channel_timeout.clone()),
            Box::new(channel_latency.clone()),
            Box::new(fair_admission.clone()),
            Box::new(fair_cost.clone()),
            Box::new(projection_tasks_pending.clone()),
            Box::new(projection_tasks_completed.clone()),
            Box::new(projection_tasks_failed.clone()),
            Box::new(projection_lag.clone()),
            Box::new(projection_reconciliation_repairs.clone()),
            Box::new(projection_oldest_pending_age.clone()),
            Box::new(auth_login.clone()),
            Box::new(auth_lockouts.clone()),
            Box::new(auth_mfa_failures.clone()),
            Box::new(auth_token_validation_failures.clone()),
            Box::new(authz_denies.clone()),
            Box::new(auth_refresh_reuse.clone()),
            Box::new(authz_policy_reload.clone()),
            Box::new(authz_policy_invalidation_lag.clone()),
            Box::new(auth_revocation_propagation.clone()),
            Box::new(idp_refresh_failures.clone()),
            Box::new(scim_failures.clone()),
            Box::new(audit_sink_failures.clone()),
            Box::new(auth_outbox_lag.clone()),
            Box::new(method_security_denials.clone()),
            Box::new(tenant_mismatch.clone()),
            Box::new(revocation_lookup_failures.clone()),
            Box::new(outbox_enqueue_failures.clone()),
            Box::new(native_service_degraded.clone()),
            Box::new(connection_tenant_budget_starved.clone()),
            Box::new(compensation_failures.clone()),
            Box::new(cdc_journal_failures.clone()),
            Box::new(control_reload_applied.clone()),
            Box::new(control_nack.clone()),
            Box::new(control_push_queue_depth.clone()),
            Box::new(control_push_throttled.clone()),
            Box::new(control_debounce_coalesced.clone()),
            Box::new(canary_auto_rollback.clone()),
            Box::new(canary_evaluations.clone()),
            Box::new(canary_active.clone()),
            Box::new(raw_dispatch.clone()),
        ] {
            registry.register(collector)?;
        }

        Ok(Self {
            registry,
            grpc_requests,
            grpc_duration,
            pg_duration,
            cache_ops,
            vector_ops,
            backend_fanout,
            object_ops,
            cdc_events,
            cdc_errors,
            cdc_wal_messages,
            cdc_is_leader,
            cdc_lag,
            cdc_outbox_depth,
            cdc_dlq_depth,
            cdc_publish_latency,
            cdc_dlq_replayed,
            cdc_duplicate_skipped,
            saga_active,
            saga_compensated,
            saga_failed_compensations,
            saga_duration,
            migration_runs,
            migration_operations,
            migration_blocked,
            migration_lint_warnings,
            migration_pending_files,
            channel_inflight,
            channel_rejected,
            channel_timeout,
            channel_latency,
            fair_admission,
            fair_cost,
            projection_tasks_pending,
            projection_tasks_completed,
            projection_tasks_failed,
            projection_lag,
            projection_reconciliation_repairs,
            projection_oldest_pending_age,
            auth_login,
            auth_lockouts,
            auth_mfa_failures,
            auth_token_validation_failures,
            authz_denies,
            auth_refresh_reuse,
            authz_policy_reload,
            authz_policy_invalidation_lag,
            auth_revocation_propagation,
            idp_refresh_failures,
            scim_failures,
            audit_sink_failures,
            auth_outbox_lag,
            method_security_denials,
            tenant_mismatch,
            revocation_lookup_failures,
            outbox_enqueue_failures,
            native_service_degraded,
            connection_tenant_budget_starved,
            compensation_failures,
            cdc_journal_failures,
            control_reload_applied,
            control_nack,
            control_push_queue_depth,
            control_push_throttled,
            control_debounce_coalesced,
            canary_auto_rollback,
            canary_evaluations,
            canary_active,
            raw_dispatch,
        })
    }

    pub fn record_grpc(&self, method: &str, status: &str, seconds: f64) {
        self.grpc_requests
            .with_label_values(&[method, status])
            .inc();
        self.grpc_duration
            .with_label_values(&[method])
            .observe(seconds);
    }

    pub fn observe_pg_query(&self, op: &str, table: &str, seconds: f64) {
        // D.4: `table` is operator/request-defined — bound its cardinality.
        self.pg_duration
            .with_label_values(&[op, bounded_label("pg_table", table).as_ref()])
            .observe(seconds);
    }

    pub fn inc_cache_op(&self, op: &str, hit: bool) {
        self.cache_ops
            .with_label_values(&[op, if hit { "true" } else { "false" }])
            .inc();
    }

    pub fn inc_vector_op(&self, collection: &str, op: &str) {
        // D.4: `collection` is untrusted — bound its cardinality.
        self.vector_ops
            .with_label_values(&[bounded_label("vector_collection", collection).as_ref(), op])
            .inc();
    }

    pub fn inc_backend_fanout(&self, backend: &str, count: u64) {
        // `backend` is a fixed backend-kind token (qdrant/s3/…), but bound it for
        // safety like the other backend labels.
        self.backend_fanout
            .with_label_values(&[bounded_label("backend", backend).as_ref()])
            .inc_by(count);
    }

    pub fn inc_object_op(&self, bucket: &str, method: &str) {
        // D.4: `bucket` is untrusted — bound its cardinality.
        self.object_ops
            .with_label_values(&[bounded_label("object_bucket", bucket).as_ref(), method])
            .inc();
    }

    pub fn gather_text(&self, extra: &str) -> String {
        let encoder = prometheus::TextEncoder::new();
        let mut bytes = Vec::new();
        if encoder.encode(&self.registry.gather(), &mut bytes).is_err() {
            return extra.to_string();
        }
        let mut text = String::from_utf8(bytes).unwrap_or_default();
        text.push_str(extra);
        text
    }

    pub fn inc_channel_inflight(&self, channel: &str) {
        self.channel_inflight.with_label_values(&[channel]).inc();
    }

    pub fn dec_channel_inflight(&self, channel: &str) {
        self.channel_inflight.with_label_values(&[channel]).dec();
    }

    pub fn inc_channel_rejected(&self, channel: &str) {
        self.channel_rejected.with_label_values(&[channel]).inc();
    }

    pub fn inc_channel_timeout(&self, channel: &str) {
        self.channel_timeout.with_label_values(&[channel]).inc();
    }

    pub fn observe_channel_latency(&self, channel: &str, seconds: f64) {
        self.channel_latency
            .with_label_values(&[channel])
            .observe(seconds);
    }

    pub fn record_fair_admission(
        &self,
        project: &str,
        tenant_hash: &str,
        backend: &str,
        instance: &str,
        operation: &str,
        result: &str,
    ) {
        // project/instance/tenant_hash are request-derived (D.4): bound them so a
        // caller cannot mint unbounded Prometheus series. backend/operation/result
        // are server-controlled enums and stay raw.
        self.fair_admission
            .with_label_values(&[
                bounded_label("fair_project", project).as_ref(),
                bounded_label("fair_tenant_hash", tenant_hash).as_ref(),
                backend,
                bounded_label("fair_instance", instance).as_ref(),
                operation,
                result,
            ])
            .inc();
    }

    pub fn add_fair_cost(
        &self,
        project: &str,
        tenant_hash: &str,
        backend: &str,
        instance: &str,
        operation: &str,
        cost: f64,
    ) {
        self.fair_cost
            .with_label_values(&[
                bounded_label("fair_project", project).as_ref(),
                bounded_label("fair_tenant_hash", tenant_hash).as_ref(),
                backend,
                bounded_label("fair_instance", instance).as_ref(),
                operation,
            ])
            .inc_by(cost.max(0.0));
    }
}

fn cdc_topic_label(topic: &str) -> &'static str {
    let topic = topic.trim();
    if topic == "workflow.dead_letter.v1" {
        "workflow.dead_letter"
    } else if topic.starts_with("workflow.retry") {
        "workflow.retry"
    } else if topic.starts_with("udb.cdc.") {
        "udb.cdc"
    } else if topic.starts_with("udb.") {
        "udb.other"
    } else if topic.is_empty() {
        "empty"
    } else {
        "external"
    }
}

fn cdc_error_reason_label(reason: &str) -> &'static str {
    match reason.trim() {
        "validation" | "schema_registry_rejected" | "source_identity_missing" => "validation",
        "dlq_routed" => "dlq_routed",
        "transient" | "source_stream_error" | "broadcast_lagged" => "transient",
        "duplicate" | "duplicate_skipped" => "duplicate",
        "" => "unknown",
        _ => "other",
    }
}

// D.4: bound metric-label cardinality under UNTRUSTED resource names (table /
// collection / bucket / project / instance). A tenant could otherwise blow up
// Prometheus cardinality (a time series per distinct name) by sending arbitrary
// names. Raw values still go to traces/logs (callers keep them); the METRIC gets
// only the bounded form.
// Bounds resolved from the central config (D.4); cached once so metric
// increments don't re-read env on every call.
fn max_label_len() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(crate::runtime::config::metric_label_max_len)
}
fn max_distinct_labels() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(crate::runtime::config::metric_max_distinct_labels)
}

/// Charset+length sanitize: lowercase, keep `[a-z0-9_.:-]` (others → `_`), cap
/// length; empty/whitespace → `"empty"`. Bounds the label VALUE (size/charset),
/// not yet its cardinality.
fn sanitize_label(raw: &str) -> std::borrow::Cow<'static, str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return std::borrow::Cow::Borrowed("empty");
    }
    std::borrow::Cow::Owned(
        trimmed
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':') {
                    c.to_ascii_lowercase()
                } else {
                    '_'
                }
            })
            .take(max_label_len())
            .collect(),
    )
}

/// Cap the number of DISTINCT labels admitted into `seen` at `cap`; anything
/// beyond collapses to `"overflow"`. Pure (takes the set) so the cardinality
/// bound is unit-testable without touching global state.
#[cfg(test)]
fn intern_bounded(
    label: std::borrow::Cow<'static, str>,
    seen: &std::sync::Mutex<std::collections::HashSet<String>>,
    cap: usize,
) -> std::borrow::Cow<'static, str> {
    let mut guard = seen.lock().unwrap_or_else(|e| e.into_inner());
    intern_bounded_set(label, &mut guard, cap)
}

fn intern_bounded_set(
    label: std::borrow::Cow<'static, str>,
    seen: &mut std::collections::HashSet<String>,
    cap: usize,
) -> std::borrow::Cow<'static, str> {
    if seen.contains(label.as_ref()) {
        return label;
    }
    if seen.len() < cap {
        seen.insert(label.clone().into_owned());
        return label;
    }
    std::borrow::Cow::Borrowed("overflow")
}

/// Bounded metric label for an untrusted resource name. Cardinality is scoped by
/// metric label namespace so one noisy dimension cannot force unrelated labels
/// into overflow.
fn bounded_label(scope: &'static str, raw: &str) -> std::borrow::Cow<'static, str> {
    static SEEN: std::sync::OnceLock<
        std::sync::Mutex<
            std::collections::HashMap<&'static str, std::collections::HashSet<String>>,
        >,
    > = std::sync::OnceLock::new();
    let seen = SEEN.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut guard = seen.lock().unwrap_or_else(|e| e.into_inner());
    let scoped = guard.entry(scope).or_default();
    intern_bounded_set(sanitize_label(raw), scoped, max_distinct_labels())
}

impl MetricsRecorder for PrometheusMetrics {
    fn record_grpc(&self, method: &str, status: &str, seconds: f64) {
        PrometheusMetrics::record_grpc(self, method, status, seconds);
    }
    fn observe_pg_query(&self, op: &str, table: &str, seconds: f64) {
        PrometheusMetrics::observe_pg_query(self, op, table, seconds);
    }
    fn inc_cache_op(&self, op: &str, hit: bool) {
        PrometheusMetrics::inc_cache_op(self, op, hit);
    }
    fn inc_vector_op(&self, collection: &str, op: &str) {
        PrometheusMetrics::inc_vector_op(self, collection, op);
    }
    fn inc_backend_fanout(&self, backend: &str, count: u64) {
        PrometheusMetrics::inc_backend_fanout(self, backend, count);
    }
    fn inc_object_op(&self, bucket: &str, method: &str) {
        PrometheusMetrics::inc_object_op(self, bucket, method);
    }
    fn inc_channel_inflight(&self, channel: &str) {
        PrometheusMetrics::inc_channel_inflight(self, channel);
    }
    fn dec_channel_inflight(&self, channel: &str) {
        PrometheusMetrics::dec_channel_inflight(self, channel);
    }
    fn inc_channel_rejected(&self, channel: &str) {
        PrometheusMetrics::inc_channel_rejected(self, channel);
    }
    fn inc_channel_timeout(&self, channel: &str) {
        PrometheusMetrics::inc_channel_timeout(self, channel);
    }
    fn observe_channel_latency(&self, channel: &str, seconds: f64) {
        PrometheusMetrics::observe_channel_latency(self, channel, seconds);
    }
    fn record_fair_admission(
        &self,
        project: &str,
        tenant_hash: &str,
        backend: &str,
        instance: &str,
        op: &str,
        status: &str,
    ) {
        PrometheusMetrics::record_fair_admission(
            self,
            project,
            tenant_hash,
            backend,
            instance,
            op,
            status,
        );
    }
    fn add_fair_cost(
        &self,
        project: &str,
        tenant_hash: &str,
        backend: &str,
        instance: &str,
        op: &str,
        cost: f64,
    ) {
        PrometheusMetrics::add_fair_cost(self, project, tenant_hash, backend, instance, op, cost);
    }

    fn inc_runs_total(&self, status: &str) {
        self.migration_runs.with_label_values(&[status]).inc();
    }
    fn inc_operations_total(&self, kind: &str, schema: &str, safety: &str) {
        self.migration_operations
            .with_label_values(&[kind, schema, safety])
            .inc();
    }
    fn observe_file_duration(&self, schema: &str, seconds: f64) {
        self.observe_pg_query("migration", schema, seconds);
    }
    fn set_blocked_operations(&self, schema: &str, kind: &str, count: i64) {
        self.migration_blocked
            .with_label_values(&[schema, kind])
            .set(count);
    }
    fn inc_lint_warnings(&self, kind: &str) {
        self.migration_lint_warnings
            .with_label_values(&[kind])
            .inc();
    }
    fn observe_run_duration(&self, status: &str, seconds: f64) {
        self.record_grpc("startup_lifecycle", status, seconds);
    }
    fn set_pending_files(&self, count: i64) {
        self.migration_pending_files.set(count);
    }
    fn set_cdc_is_leader(&self, host: &str, is_leader: bool) {
        self.cdc_is_leader
            .with_label_values(&[host])
            .set(i64::from(is_leader));
    }
    fn inc_cdc_wal_messages_received_total(&self) {
        self.cdc_wal_messages.inc();
    }
    fn inc_cdc_events_published_total(&self, topic: &str) {
        self.cdc_events
            .with_label_values(&[cdc_topic_label(topic)])
            .inc();
    }
    fn inc_cdc_errors_total(&self, reason: &str) {
        self.cdc_errors
            .with_label_values(&[cdc_error_reason_label(reason)])
            .inc();
    }
    fn set_cdc_lag_seconds(&self, seconds: f64) {
        self.cdc_lag.set(seconds);
    }
    fn set_cdc_outbox_depth(&self, depth: i64) {
        self.cdc_outbox_depth.set(depth);
    }
    fn set_cdc_dlq_depth(&self, depth: i64) {
        self.cdc_dlq_depth.set(depth);
    }
    fn observe_cdc_publish_duration_seconds(&self, seconds: f64) {
        self.cdc_publish_latency.observe(seconds);
    }
    fn inc_cdc_dlq_replayed_total(&self) {
        self.cdc_dlq_replayed.inc();
    }
    fn inc_cdc_duplicate_skipped_total(&self) {
        self.cdc_duplicate_skipped.inc();
    }
    fn set_saga_active(&self, count: i64) {
        self.saga_active.set(count);
    }
    fn inc_saga_compensated_total(&self) {
        self.saga_compensated.inc();
    }
    fn inc_saga_failed_compensations_total(&self) {
        self.saga_failed_compensations.inc();
    }
    fn observe_saga_duration_seconds(&self, seconds: f64) {
        self.saga_duration.observe(seconds);
    }
    fn set_projection_tasks_pending(&self, backend: &str, instance: &str, kind: &str, count: i64) {
        self.projection_tasks_pending
            .with_label_values(&[backend, instance, kind])
            .set(count);
    }
    fn inc_projection_tasks_completed_total(&self, backend: &str, instance: &str, kind: &str) {
        self.projection_tasks_completed
            .with_label_values(&[backend, instance, kind])
            .inc();
    }
    fn inc_projection_tasks_failed_total(&self, backend: &str, instance: &str, kind: &str) {
        self.projection_tasks_failed
            .with_label_values(&[backend, instance, kind])
            .inc();
    }
    fn observe_projection_lag_seconds(
        &self,
        backend: &str,
        instance: &str,
        kind: &str,
        seconds: f64,
    ) {
        self.projection_lag
            .with_label_values(&[backend, instance, kind])
            .observe(seconds);
    }
    fn inc_projection_reconciliation_repairs_total(&self, backend: &str, instance: &str) {
        self.projection_reconciliation_repairs
            .with_label_values(&[backend, instance])
            .inc();
    }
    fn set_projection_oldest_pending_age_seconds(
        &self,
        project: &str,
        backend: &str,
        instance: &str,
        kind: &str,
        age_seconds: f64,
    ) {
        self.projection_oldest_pending_age
            .with_label_values(&[project, backend, instance, kind])
            .set(age_seconds);
    }
    fn record_auth_login(&self, success: bool) {
        self.auth_login
            .with_label_values(&[if success { "success" } else { "failure" }])
            .inc();
    }
    fn record_auth_lockout(&self) {
        self.auth_lockouts.inc();
    }
    fn record_auth_mfa_failure(&self) {
        self.auth_mfa_failures.inc();
    }
    fn record_auth_token_validation_failure(&self) {
        self.auth_token_validation_failures.inc();
    }
    fn record_authz_deny(&self) {
        self.authz_denies.inc();
    }
    fn record_refresh_reuse_detected(&self) {
        self.auth_refresh_reuse.inc();
    }
    fn observe_policy_reload_seconds(&self, seconds: f64) {
        self.authz_policy_reload.observe(seconds.max(0.0));
    }
    fn observe_policy_invalidation_lag_seconds(&self, seconds: f64) {
        self.authz_policy_invalidation_lag.observe(seconds.max(0.0));
    }
    fn observe_revocation_propagation_seconds(&self, seconds: f64) {
        self.auth_revocation_propagation.observe(seconds.max(0.0));
    }
    fn record_idp_refresh_failure(&self, kind: &str) {
        self.idp_refresh_failures
            .with_label_values(&[bounded_label("idp_refresh_kind", kind).as_ref()])
            .inc();
    }
    fn record_scim_failure(&self, op: &str) {
        self.scim_failures
            .with_label_values(&[bounded_label("scim_op", op).as_ref()])
            .inc();
    }
    fn record_audit_sink_failure(&self, sink: &str) {
        self.audit_sink_failures
            .with_label_values(&[bounded_label("audit_sink", sink).as_ref()])
            .inc();
    }
    fn set_auth_outbox_lag_seconds(&self, seconds: f64) {
        self.auth_outbox_lag.set(seconds);
    }
    fn inc_method_security_denial(&self, reason: &str) {
        self.method_security_denials
            .with_label_values(&[reason])
            .inc();
    }
    fn inc_tenant_mismatch(&self) {
        self.tenant_mismatch.inc();
    }
    fn inc_revocation_lookup_failure(&self) {
        self.revocation_lookup_failures.inc();
    }
    fn inc_outbox_enqueue_failures_total(&self, path: &str) {
        self.outbox_enqueue_failures
            .with_label_values(&[bounded_label("outbox_path", path).as_ref()])
            .inc();
    }
    fn set_native_service_degraded(&self, service: &str, degraded: bool) {
        self.native_service_degraded
            .with_label_values(&[bounded_label("native_service", service).as_ref()])
            .set(i64::from(degraded));
    }
    fn set_tenant_connection_starved(&self, tenant: &str, starved: bool) {
        // Tenant id is operator/request-defined — bound its cardinality so a noisy
        // tenant label cannot mint unbounded series.
        self.connection_tenant_budget_starved
            .with_label_values(&[bounded_label("connection_budget_tenant", tenant).as_ref()])
            .set(i64::from(starved));
    }
    fn inc_compensation_failures_total(&self) {
        self.compensation_failures.inc();
    }
    fn inc_cdc_journal_failures_total(&self) {
        self.cdc_journal_failures.inc();
    }
    fn inc_control_reload_applied(&self, resource_type: &str) {
        self.control_reload_applied
            .with_label_values(&[resource_type])
            .inc();
    }
    fn inc_control_nack(&self, resource_type: &str) {
        // `resource_type` is a server-resolved enum token (the distributed
        // resource kind), but bound it like the other labels so a malformed type
        // string from the node's NACK frame can't mint unbounded series.
        self.control_nack
            .with_label_values(&[
                bounded_label("control_nack_resource_type", resource_type).as_ref()
            ])
            .inc();
    }
    fn set_control_push_queue_depth(&self, depth: u64) {
        self.control_push_queue_depth.set(depth as i64);
    }
    fn inc_control_push_throttled(&self) {
        self.control_push_throttled.inc();
    }
    fn inc_control_debounce_coalesced(&self) {
        self.control_debounce_coalesced.inc();
    }
    fn inc_canary_auto_rollback(&self, reason: &str) {
        self.canary_auto_rollback.with_label_values(&[reason]).inc();
    }
    fn record_canary_evaluation(&self, breached: bool) {
        self.canary_evaluations
            .with_label_values(&[if breached { "breached" } else { "ok" }])
            .inc();
    }
    fn set_canary_active(&self, active: bool) {
        self.canary_active.set(if active { 1 } else { 0 });
    }
    fn inc_raw_dispatch_total(&self, backend: &str) {
        // `backend` is a server-resolved backend-kind token, but bound it like the
        // other backend labels so a malformed selector can't mint unbounded series.
        self.raw_dispatch
            .with_label_values(&[bounded_label("raw_dispatch_backend", backend).as_ref()])
            .inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_label_bounds_charset_and_length() {
        assert_eq!(sanitize_label("My Table!"), "my_table_");
        assert_eq!(sanitize_label("   "), "empty");
        assert_eq!(sanitize_label(""), "empty");
        assert_eq!(sanitize_label("udb.core.v1:users-2"), "udb.core.v1:users-2");
        assert!(sanitize_label(&"x".repeat(500)).len() <= max_label_len());
    }

    #[test]
    fn intern_bounded_caps_cardinality_under_arbitrary_names() {
        use std::collections::HashSet;
        use std::sync::Mutex;
        // Local set so the global SEEN isn't polluted for other tests.
        let seen = Mutex::new(HashSet::new());
        let cap = 8;
        let mut distinct = HashSet::new();
        // Feed far more distinct names than the cap; the admitted label set must
        // stay bounded (cap admitted + the single "overflow" bucket).
        for i in 0..(cap * 10) {
            let label = intern_bounded(sanitize_label(&format!("tbl_{i}")), &seen, cap);
            distinct.insert(label.into_owned());
        }
        assert!(
            distinct.len() <= cap + 1,
            "cardinality must stay bounded, got {} distinct labels",
            distinct.len()
        );
        assert!(
            distinct.contains("overflow"),
            "overflow bucket must be used past the cap"
        );
        // A name admitted before the cap round-trips deterministically.
        assert_eq!(intern_bounded(sanitize_label("tbl_0"), &seen, cap), "tbl_0");
    }

    #[test]
    fn fair_admission_labels_are_cardinality_bounded() {
        // Item 12: project/instance/tenant_hash are request-controlled. Feeding 2x
        // the distinct-label bound of projects must NOT mint 2x the series. The
        // interner caps each metric label namespace independently, so the project
        // dimension stays <= cap+1 including the overflow bucket.
        let m = PrometheusMetrics::new().expect("build PrometheusMetrics");
        let cap = max_distinct_labels();
        for i in 0..(cap * 2) {
            m.record_fair_admission(
                &format!("attacker-project-{i}"),
                "tenant-hash",
                "postgres",
                "primary",
                "query",
                "admit",
            );
            m.add_fair_cost(
                &format!("attacker-project-{i}"),
                "tenant-hash",
                "postgres",
                "primary",
                "query",
                1.0,
            );
        }
        let text = m.gather_text("");
        let admission_series = text
            .lines()
            .filter(|line| line.starts_with("udb_fair_admission_total{"))
            .count();
        let cost_series = text
            .lines()
            .filter(|line| line.starts_with("udb_fair_cost_units_total{"))
            .count();
        assert!(
            admission_series <= cap + 1,
            "fair_admission series must cap at {} (+overflow), got {admission_series}",
            cap
        );
        assert!(
            cost_series <= cap + 1,
            "fair_cost series must cap at {} (+overflow), got {cost_series}",
            cap
        );
        assert!(
            text.lines().any(|line| {
                line.starts_with("udb_fair_admission_total{")
                    && line.contains("project=\"overflow\"")
            }),
            "past the cap, projects must collapse into the overflow bucket:\n{}",
            text.lines()
                .filter(|line| line.starts_with("udb_fair_admission_total"))
                .take(5)
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    #[test]
    fn bounded_labels_are_scoped_by_metric_namespace() {
        let cap = max_distinct_labels();
        for i in 0..(cap * 2) {
            let _ = bounded_label("test_noisy_namespace", &format!("attacker-value-{i}"));
        }

        assert_eq!(bounded_label("idp_refresh_kind", "jwks"), "jwks");
        assert_eq!(bounded_label("outbox_path", "native"), "native");
        assert_eq!(bounded_label("native_service", "authn"), "authn");
    }

    #[test]
    fn noop_metrics_is_callable() {
        let m = NoopMetrics;
        // All calls must compile and not panic.
        m.inc_runs_total("completed");
        m.inc_operations_total("add_column", "app_processing", "auto");
        m.observe_file_duration("app_processing", 0.042);
        m.set_blocked_operations("app_processing", "drop_column", 0);
        m.inc_lint_warnings("missing_primary_key");
        m.observe_run_duration("completed", 1.23);
        m.set_pending_files(0);
    }

    #[test]
    fn metrics_or_noop_with_none() {
        let m = metrics_or_noop(None);
        m.inc_runs_total("completed"); // must not panic
    }

    #[test]
    fn safety_label_known_values() {
        assert_eq!(safety_label("SafeAuto"), "auto");
        assert_eq!(safety_label("RequiresReview"), "requires_review");
        assert_eq!(safety_label("Blocked"), "blocked");
    }

    #[test]
    fn auth_metrics_register_and_increment() {
        let m = PrometheusMetrics::new().expect("build PrometheusMetrics");
        // Drive each auth-plane recorder method.
        m.record_auth_login(true);
        m.record_auth_login(false);
        m.record_auth_lockout();
        m.record_auth_mfa_failure();
        m.record_auth_token_validation_failure();
        m.record_authz_deny();

        let text = m.gather_text("");
        // Counters must be registered and surfaced with their incremented values.
        assert!(
            text.contains("udb_auth_login_total{outcome=\"success\"} 1"),
            "missing success login counter:\n{text}"
        );
        assert!(
            text.contains("udb_auth_login_total{outcome=\"failure\"} 1"),
            "missing failure login counter:\n{text}"
        );
        assert!(
            text.contains("udb_auth_lockouts_total 1"),
            "missing lockout counter:\n{text}"
        );
        assert!(
            text.contains("udb_auth_mfa_failures_total 1"),
            "missing mfa-failure counter:\n{text}"
        );
        assert!(
            text.contains("udb_auth_token_validation_failures_total 1"),
            "missing token-validation-failure counter:\n{text}"
        );
        assert!(
            text.contains("udb_authz_denies_total 1"),
            "missing authz-deny counter:\n{text}"
        );
    }

    #[test]
    fn phase_l_metrics_register_and_increment() {
        let m = PrometheusMetrics::new().expect("build PrometheusMetrics");
        m.record_refresh_reuse_detected();
        m.observe_policy_reload_seconds(0.01);
        m.observe_policy_invalidation_lag_seconds(0.2);
        m.observe_revocation_propagation_seconds(0.3);
        m.record_idp_refresh_failure("jwks");
        m.record_scim_failure("provision");
        m.record_audit_sink_failure("siem_webhook");
        m.set_auth_outbox_lag_seconds(4.0);

        let text = m.gather_text("");
        assert!(
            text.contains("udb_auth_refresh_reuse_detected_total 1"),
            "missing refresh-reuse counter:\n{text}"
        );
        assert!(
            text.contains("udb_idp_refresh_failures_total{kind=\"jwks\"} 1"),
            "missing idp-refresh-failure counter:\n{text}"
        );
        assert!(
            text.contains("udb_scim_failures_total{op=\"provision\"} 1"),
            "missing scim-failure counter:\n{text}"
        );
        assert!(
            text.contains("udb_audit_sink_failures_total{sink=\"siem_webhook\"} 1"),
            "missing audit-sink-failure counter:\n{text}"
        );
        assert!(
            text.contains("udb_auth_outbox_lag_seconds 4"),
            "missing outbox-lag gauge:\n{text}"
        );
        // Histograms surface their _count suffix once observed.
        assert!(
            text.contains("udb_authz_policy_reload_seconds_count 1"),
            "missing policy-reload histogram:\n{text}"
        );
    }

    #[test]
    fn phase_10_metrics_register_and_increment() {
        let m = PrometheusMetrics::new().expect("build PrometheusMetrics");
        m.inc_method_security_denial("scope");
        m.inc_tenant_mismatch();
        m.inc_revocation_lookup_failure();
        m.inc_outbox_enqueue_failures_total("native");
        m.set_native_service_degraded("storage", true);
        m.inc_compensation_failures_total();

        let text = m.gather_text("");
        assert!(
            text.contains("udb_method_security_denials_total{reason=\"scope\"} 1"),
            "missing method-security-denial counter:\n{text}"
        );
        assert!(
            text.contains("udb_tenant_mismatch_total 1"),
            "missing tenant-mismatch counter:\n{text}"
        );
        assert!(
            text.contains("udb_revocation_lookup_failures_total 1"),
            "missing revocation-lookup-failure counter:\n{text}"
        );
        assert!(
            text.contains("udb_outbox_enqueue_failures_total{path=\"native\"} 1"),
            "missing outbox-enqueue-failure counter:\n{text}"
        );
        assert!(
            text.contains("udb_native_service_degraded{service=\"storage\"} 1"),
            "missing native-service-degraded gauge:\n{text}"
        );
        assert!(
            text.contains("udb_compensation_failures_total 1"),
            "missing compensation-failures counter:\n{text}"
        );
    }

    #[test]
    fn control_plane_metrics_register_and_increment() {
        // Phase 9 / item 6.2: the control-plane distribution counters must register
        // and surface with the resource_type label the serving paths emit. This
        // guards the writer the `apply_inbound` NACK branch and the reload
        // subscriber depend on (a refactor dropping the field/registration reds
        // here; the SQL serving paths themselves are PG-gated, exercised by the
        // env-gated live conformance suite).
        let m = PrometheusMetrics::new().expect("build PrometheusMetrics");
        m.inc_control_reload_applied("RESOURCE_TYPE_ROUTING_POLICY");
        m.inc_control_nack("RESOURCE_TYPE_RLS_TENANT_POLICY");
        let text = m.gather_text("");
        assert!(
            text.contains(
                "udb_control_reload_applied_total{resource_type=\"RESOURCE_TYPE_ROUTING_POLICY\"} 1"
            ),
            "missing control-reload-applied counter:\n{text}"
        );
        // Assert the family registered, incremented, and surfaced with count 1 —
        // but do NOT pin the exact label value: `inc_control_nack` routes the
        // resource_type through the PROCESS-GLOBAL `bounded_label` cap (keyed
        // "control_nack_resource_type"), which the sibling
        // `control_nack_label_is_cardinality_bounded` test legitimately saturates.
        // Under concurrent test execution our value may land in the overflow
        // bucket, so we assert the incremented series exists rather than its label.
        assert!(
            text.lines()
                .any(|l| l.starts_with("udb_control_nack_total{") && l.ends_with(" 1")),
            "control-nack counter did not register/increment/surface:\n{text}"
        );
    }

    #[test]
    fn control_nack_label_is_cardinality_bounded() {
        // The NACK resource_type label is bounded so a node that NACKs with a
        // malformed/attacker-controlled type string cannot mint unbounded series.
        let m = PrometheusMetrics::new().expect("build PrometheusMetrics");
        let cap = max_distinct_labels();
        for i in 0..(cap * 2) {
            m.inc_control_nack(&format!("bogus-type-{i}"));
        }
        let text = m.gather_text("");
        let series = text
            .lines()
            .filter(|line| line.starts_with("udb_control_nack_total{"))
            .count();
        assert!(
            series <= cap + 1,
            "control_nack series must cap at {} (+overflow), got {series}",
            cap
        );
    }

    #[test]
    fn raw_dispatch_metric_registers_and_increments() {
        let m = PrometheusMetrics::new().expect("build PrometheusMetrics");
        m.inc_raw_dispatch_total("postgres");
        let text = m.gather_text("");
        assert!(
            text.contains("udb_raw_dispatch_total{backend=\"postgres\"} 1"),
            "missing raw-dispatch counter:\n{text}"
        );
    }
}
