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

        for collector in [
            Box::new(grpc_requests.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(grpc_duration.clone()),
            Box::new(pg_duration.clone()),
            Box::new(cache_ops.clone()),
            Box::new(vector_ops.clone()),
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
        self.pg_duration
            .with_label_values(&[op, table])
            .observe(seconds);
    }

    pub fn inc_cache_op(&self, op: &str, hit: bool) {
        self.cache_ops
            .with_label_values(&[op, if hit { "true" } else { "false" }])
            .inc();
    }

    pub fn inc_vector_op(&self, collection: &str, op: &str) {
        self.vector_ops.with_label_values(&[collection, op]).inc();
    }

    pub fn inc_object_op(&self, bucket: &str, method: &str) {
        self.object_ops.with_label_values(&[bucket, method]).inc();
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
        self.fair_admission
            .with_label_values(&[project, tenant_hash, backend, instance, operation, result])
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
            .with_label_values(&[project, tenant_hash, backend, instance, operation])
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
