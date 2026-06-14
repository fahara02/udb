//! Phase 10: Service-Level Objectives, error budgets, and the unified readiness
//! contract.
//!
//! This module is the single source of truth for two cross-cutting concerns the
//! acceptance criteria call out:
//!
//! 1. **SLO catalog** ([`slo_catalog`]) — a structured, code-defined table of
//!    latency/availability objectives for each user-visible operation, every one
//!    mapped to a *real* Prometheus metric emitted by
//!    [`crate::runtime::metrics`]. The catalog is the contract that
//!    `docs/slo-and-error-budgets.md` and the burn-rate alert rules are written
//!    against; a unit test asserts every objective points at a metric name that
//!    actually exists in `metrics.rs`, so the doc and the alerts can never drift
//!    from the runtime.
//!
//! 2. **Readiness contract** ([`ReadinessFacts`]) — one shape that
//!    `GetHealthReport`, `udb native doctor`, the gRPC health service, and the
//!    auth-plane readiness checks all derive from, so the four surfaces report
//!    the *same* readiness facts (the Phase 10 acceptance criterion: "doctor,
//!    GetHealthReport, gRPC health, metrics, and runbooks report the same
//!    readiness facts"). Each surface formats the facts differently but they are
//!    all computed once, here, from the same inputs.
//!
//! Nothing in this module performs I/O: callers pass in the already-probed
//! [`RuntimeInitReport`], the resolved native-service statuses, and the
//! auth-readiness check results, and this module folds them into a consistent
//! fact set. That keeps probe logic in exactly one place (the runtime) and makes
//! the readiness facts pure and unit-testable.

use crate::runtime::core::RuntimeInitReport;
use crate::runtime::service::native_registry::NativeServiceStatus;

// ─────────────────────────────────────────────────────────────────────────────
// Real metric names. These MUST match the collector names registered in
// `crate::runtime::metrics::PrometheusMetrics::new`. The `slo_metrics_exist`
// unit test guards every one of these against the live metrics registry, so a
// rename in metrics.rs that is not mirrored here fails the build's test step
// instead of silently shipping a doc/alert that points at a metric that does not
// exist.
// ─────────────────────────────────────────────────────────────────────────────

/// Universal per-RPC latency histogram (`{method}` label selects the operation).
pub const METRIC_GRPC_DURATION: &str = "udb_grpc_duration_seconds";
/// Universal per-RPC request counter (`{method,status}`); availability numerator
/// = non-error `status`, denominator = all.
pub const METRIC_GRPC_REQUESTS: &str = "udb_grpc_requests_total";
/// Login attempts by outcome — authn login success-rate SLI.
pub const METRIC_AUTH_LOGIN: &str = "udb_auth_login_total";
/// Token validation failures — authn validate error SLI.
pub const METRIC_AUTH_TOKEN_VALIDATION_FAILURES: &str = "udb_auth_token_validation_failures_total";
/// Authorization denials — authz check/batch-check outcome SLI.
pub const METRIC_AUTHZ_DENIES: &str = "udb_authz_denies_total";
/// Lag between a policy-bundle invalidation emit and a node applying it — the
/// xDS-style policy-distribution ACK-latency SLI.
pub const METRIC_AUTHZ_POLICY_INVALIDATION_LAG: &str = "udb_authz_policy_invalidation_lag_seconds";
/// Authz policy snapshot reload latency.
pub const METRIC_AUTHZ_POLICY_RELOAD: &str = "udb_authz_policy_reload_seconds";
/// Object-store operation counter (`{bucket,method}`) — storage presign/finalize/list SLI.
pub const METRIC_OBJECT_OPS: &str = "udb_object_ops_total";
/// Vector backend operation counter (`{collection,op}`) — asset EMBED-step SLI.
pub const METRIC_VECTOR_OPS: &str = "udb_vector_ops_total";
/// End-to-end CDC publish latency (outbox row → ack).
pub const METRIC_CDC_PUBLISH_LATENCY: &str = "udb_cdc_publish_latency_seconds";
/// Current open DLQ depth — CDC DLQ SLI.
pub const METRIC_CDC_DLQ_DEPTH: &str = "udb_cdc_dlq_depth";
/// CDC publish errors by reason.
pub const METRIC_CDC_ERRORS: &str = "udb_cdc_errors_total";

/// The latency-statistic an [`Slo`] objective is expressed over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyStat {
    /// p50 / median.
    P50,
    /// p95.
    P95,
    /// p99 — the default tail objective.
    P99,
}

impl LatencyStat {
    /// The quantile as used by a PromQL `histogram_quantile` expression.
    pub fn quantile(self) -> f64 {
        match self {
            Self::P50 => 0.50,
            Self::P95 => 0.95,
            Self::P99 => 0.99,
        }
    }

    /// Short label for docs/tables.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::P50 => "p50",
            Self::P95 => "p95",
            Self::P99 => "p99",
        }
    }
}

/// One Service-Level Objective: an availability target plus an optional latency
/// target, both bound to the real metric(s) that measure them.
#[derive(Debug, Clone)]
pub struct Slo {
    /// Stable objective identifier (e.g. `authn.login`). Used as the doc anchor
    /// and the alert-rule label.
    pub objective: &'static str,
    /// Human-readable description of the operation this objective covers.
    pub operation: &'static str,
    /// The gRPC method label (value of the `method` label on
    /// `udb_grpc_duration_seconds` / `udb_grpc_requests_total`) when this SLO is
    /// measured off the universal RPC metrics; empty for non-RPC objectives
    /// (e.g. CDC publish latency) that have their own dedicated metric.
    pub grpc_method: &'static str,
    /// Latency objective: which statistic, and the target in milliseconds. `None`
    /// for availability-only objectives (e.g. DLQ depth).
    pub latency: Option<(LatencyStat, f64)>,
    /// Availability objective as a fraction in (0, 1], e.g. 0.999 for three-nines.
    pub availability: f64,
    /// The primary metric the latency objective is computed from.
    pub latency_metric: &'static str,
    /// The metric the availability/error-budget is computed from.
    pub availability_metric: &'static str,
    /// 28-day error-budget alert windows (short and long) used for multi-window
    /// burn-rate alerting; see [`burn_rate`].
    pub rolling_window_days: u32,
}

impl Slo {
    /// Every metric name this SLO references, for the
    /// catalog↔metrics-registry consistency test.
    pub fn referenced_metrics(&self) -> Vec<&'static str> {
        let mut out = vec![self.latency_metric, self.availability_metric];
        out.retain(|m| !m.is_empty());
        out.sort_unstable();
        out.dedup();
        out
    }
}

/// The full SLO catalog. One row per Phase-10-required operation lane. Each row
/// names the real metric that measures it; `docs/slo-and-error-budgets.md` is the
/// prose mirror of this table and the alert rules are generated from the same
/// objective ids.
pub fn slo_catalog() -> Vec<Slo> {
    // Helper: an RPC-latency+availability SLO measured off the universal gRPC
    // metrics, keyed by the tonic method label.
    fn rpc(
        objective: &'static str,
        operation: &'static str,
        method: &'static str,
        stat: LatencyStat,
        p_ms: f64,
        availability: f64,
    ) -> Slo {
        Slo {
            objective,
            operation,
            grpc_method: method,
            latency: Some((stat, p_ms)),
            availability,
            latency_metric: METRIC_GRPC_DURATION,
            availability_metric: METRIC_GRPC_REQUESTS,
            rolling_window_days: 28,
        }
    }

    vec![
        // ── authn ──────────────────────────────────────────────────────────
        rpc(
            "authn.login",
            "Interactive credential login (password/MFA exchange → tokens)",
            "Login",
            LatencyStat::P99,
            400.0,
            0.999,
        ),
        rpc(
            "authn.refresh",
            "Refresh-token rotation → new access token",
            "RefreshToken",
            LatencyStat::P99,
            150.0,
            0.9995,
        ),
        rpc(
            "authn.validate",
            "Access-token validation (bearer introspection)",
            "ValidateToken",
            LatencyStat::P99,
            50.0,
            0.9995,
        ),
        // ── authz ──────────────────────────────────────────────────────────
        rpc(
            "authz.check",
            "Single authorization decision (Casbin enforce)",
            "Check",
            LatencyStat::P99,
            25.0,
            0.9999,
        ),
        rpc(
            "authz.batch_check",
            "Batched authorization decisions",
            "BatchCheck",
            LatencyStat::P99,
            75.0,
            0.9995,
        ),
        // ── storage ────────────────────────────────────────────────────────
        Slo {
            objective: "storage.presign",
            operation: "Presigned-URL mint for upload/download",
            grpc_method: "PresignObject",
            latency: Some((LatencyStat::P99, 100.0)),
            availability: 0.999,
            latency_metric: METRIC_GRPC_DURATION,
            availability_metric: METRIC_OBJECT_OPS,
            rolling_window_days: 28,
        },
        Slo {
            objective: "storage.finalize",
            operation: "Finalize a completed upload (commit object metadata)",
            grpc_method: "FinalizeUpload",
            latency: Some((LatencyStat::P99, 250.0)),
            availability: 0.999,
            latency_metric: METRIC_GRPC_DURATION,
            availability_metric: METRIC_OBJECT_OPS,
            rolling_window_days: 28,
        },
        Slo {
            objective: "storage.list",
            operation: "List objects under a prefix",
            grpc_method: "ListObjects",
            latency: Some((LatencyStat::P99, 300.0)),
            availability: 0.999,
            latency_metric: METRIC_GRPC_DURATION,
            availability_metric: METRIC_OBJECT_OPS,
            rolling_window_days: 28,
        },
        // ── asset pipeline ────────────────────────────────────────────────
        rpc(
            "asset.pipeline_start",
            "Start an asset-processing pipeline run",
            "StartPipeline",
            LatencyStat::P99,
            200.0,
            0.999,
        ),
        Slo {
            objective: "asset.pipeline_step",
            operation: "Execute one asset pipeline step (EMBED → vector upsert)",
            grpc_method: "ExecuteStep",
            latency: Some((LatencyStat::P95, 2000.0)),
            availability: 0.995,
            latency_metric: METRIC_GRPC_DURATION,
            availability_metric: METRIC_VECTOR_OPS,
            rolling_window_days: 28,
        },
        // ── webrtc signaling ──────────────────────────────────────────────
        rpc(
            "webrtc.signaling",
            "WebRTC signaling relay (offer/answer/ICE)",
            "Signal",
            LatencyStat::P99,
            100.0,
            0.999,
        ),
        // ── CDC ────────────────────────────────────────────────────────────
        Slo {
            objective: "cdc.publish",
            operation: "End-to-end CDC publish (outbox row created → Kafka ack)",
            grpc_method: "",
            latency: Some((LatencyStat::P99, 1000.0)),
            availability: 0.999,
            latency_metric: METRIC_CDC_PUBLISH_LATENCY,
            availability_metric: METRIC_CDC_ERRORS,
            rolling_window_days: 28,
        },
        Slo {
            objective: "cdc.dlq",
            operation: "CDC dead-letter backlog stays drained",
            grpc_method: "",
            // Availability-only: the objective is "DLQ depth near zero", measured
            // as a gauge, not a latency percentile.
            latency: None,
            availability: 0.999,
            latency_metric: METRIC_CDC_DLQ_DEPTH,
            availability_metric: METRIC_CDC_DLQ_DEPTH,
            rolling_window_days: 28,
        },
        // ── policy distribution ───────────────────────────────────────────
        Slo {
            objective: "policy.distribution_ack",
            operation: "Control-plane policy ACK latency (invalidation emit → node apply)",
            grpc_method: "",
            latency: Some((LatencyStat::P99, 5000.0)),
            availability: 0.999,
            latency_metric: METRIC_AUTHZ_POLICY_INVALIDATION_LAG,
            availability_metric: METRIC_AUTHZ_POLICY_RELOAD,
            rolling_window_days: 28,
        },
    ]
}

// ─────────────────────────────────────────────────────────────────────────────
// Error budgets
// ─────────────────────────────────────────────────────────────────────────────

/// An error budget derived from an availability objective and observed traffic.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ErrorBudget {
    /// The availability objective in (0, 1], e.g. 0.999.
    pub objective: f64,
    /// Total events observed in the window.
    pub total: u64,
    /// Failed events observed in the window.
    pub failed: u64,
}

impl ErrorBudget {
    /// Build from raw success/total counts. `success` is clamped to `total`.
    pub fn from_success_total(objective: f64, success: u64, total: u64) -> Self {
        let success = success.min(total);
        Self {
            objective,
            total,
            failed: total - success,
        }
    }

    /// Allowed failures in the window = `(1 - objective) * total`, rounded down.
    pub fn allowed_failures(&self) -> u64 {
        ((1.0 - self.objective) * self.total as f64).floor() as u64
    }

    /// Observed failure ratio in (0, 1]; 0 when no traffic.
    pub fn failure_ratio(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.failed as f64 / self.total as f64
        }
    }

    /// Fraction of the error budget consumed, in [0, ∞). 1.0 means the budget is
    /// exactly exhausted; > 1.0 means it is overspent. 0 when the objective
    /// allows no errors and the window is empty.
    pub fn consumed_fraction(&self) -> f64 {
        let allowed = (1.0 - self.objective).max(0.0);
        if allowed <= 0.0 {
            // A 100% objective: any failure overspends.
            return if self.failed == 0 { 0.0 } else { f64::INFINITY };
        }
        self.failure_ratio() / allowed
    }

    /// Remaining error budget as a fraction in (-∞, 1]; negative means overspent.
    pub fn remaining_fraction(&self) -> f64 {
        1.0 - self.consumed_fraction()
    }

    /// Whether the budget is exhausted (consumed ≥ 100%). A small epsilon absorbs
    /// float error at the exact boundary (e.g. `1% failures / (1 - 0.99)` lands a
    /// hair under 1.0 in IEEE-754 even though the budget is exactly spent).
    pub fn is_exhausted(&self) -> bool {
        self.consumed_fraction() >= 1.0 - 1e-9
    }
}

/// Multi-window burn rate: how fast the error budget is being spent relative to
/// the steady "spend the whole budget exactly over the SLO window" pace.
///
/// `burn_rate = observed_failure_ratio / (1 - objective)`. A burn rate of 1.0
/// spends the entire budget evenly across the window; 14.4 exhausts a 30-day
/// budget in ~2 hours (the canonical fast-burn page threshold).
pub fn burn_rate(objective: f64, observed_failure_ratio: f64) -> f64 {
    let allowed = (1.0 - objective).max(0.0);
    if allowed <= 0.0 {
        return if observed_failure_ratio > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };
    }
    observed_failure_ratio / allowed
}

// ─────────────────────────────────────────────────────────────────────────────
// Unified readiness contract
// ─────────────────────────────────────────────────────────────────────────────

/// One readiness fact: a named check, its pass/fail state, and a caller-safe
/// detail string. This mirrors the shape of an auth `AuthReadinessCheck` so the
/// auth-plane checks fold into the same list without translation loss.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadinessFact {
    /// Stable check id, e.g. `postgres`, `signing_keys`, `native.storage`.
    pub name: String,
    /// Whether the check is satisfied. A `false` here that is `required` makes
    /// the aggregate report fail.
    pub ok: bool,
    /// Whether a `false` result should fail overall readiness (`true`) or only
    /// warn (`false`). Optional dependencies warn; required ones fail.
    pub required: bool,
    /// Caller-safe human-readable detail (never secrets).
    pub detail: String,
}

impl ReadinessFact {
    fn new(name: impl Into<String>, ok: bool, required: bool, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ok,
            required,
            detail: detail.into(),
        }
    }

    /// A required dependency that must be present for the server to be ready.
    pub fn required(name: impl Into<String>, ok: bool, detail: impl Into<String>) -> Self {
        Self::new(name, ok, true, detail)
    }

    /// An optional dependency: a `false` warns but does not fail readiness.
    pub fn optional(name: impl Into<String>, ok: bool, detail: impl Into<String>) -> Self {
        Self::new(name, ok, false, detail)
    }
}

/// The unified readiness fact set that `GetHealthReport`, `udb native doctor`,
/// the gRPC health service, and the auth-plane checks all derive from.
#[derive(Debug, Clone, Default)]
pub struct ReadinessFacts {
    /// Every readiness fact, in a stable evaluation order.
    pub facts: Vec<ReadinessFact>,
}

impl ReadinessFacts {
    /// `true` iff every *required* fact passed. Optional failures are warnings.
    pub fn passed(&self) -> bool {
        self.facts.iter().all(|f| f.ok || !f.required)
    }

    /// The detail strings of failing *required* facts (the errors a surface
    /// should report).
    pub fn errors(&self) -> Vec<String> {
        self.facts
            .iter()
            .filter(|f| !f.ok && f.required)
            .map(|f| format!("{}: {}", f.name, f.detail))
            .collect()
    }

    /// The detail strings of failing *optional* facts (the warnings).
    pub fn warnings(&self) -> Vec<String> {
        self.facts
            .iter()
            .filter(|f| !f.ok && !f.required)
            .map(|f| format!("{}: {}", f.name, f.detail))
            .collect()
    }

    /// Look up a fact by name.
    pub fn get(&self, name: &str) -> Option<&ReadinessFact> {
        self.facts.iter().find(|f| f.name == name)
    }
}

/// Whether a configured object store (S3/MinIO/Azure/GCS) is present. UDB's
/// object lane is satisfied by any of these, so the readiness fact reads "object
/// store" rather than "s3".
fn object_store_configured(init: &RuntimeInitReport) -> bool {
    init.s3_configured || init.azureblob_configured || init.gcs_configured
}

/// Build the unified readiness fact set from already-gathered inputs.
///
/// Inputs (all pre-computed by the caller; this fn does no I/O):
/// - `init`: the runtime init report (which backends are configured).
/// - `native`: the resolved native-service statuses (mounted/degraded/missing
///   deps), used to emit one `native.<service_id>` fact per service.
/// - `auth_checks`: the auth-plane readiness checks as `(name, ok, detail)`
///   triples (the caller adapts `AuthReadinessReport` into this so this module
///   does not depend on the auth module). These become `signing_keys`-class
///   facts and fold directly into the same list.
///
/// PostgreSQL is the only hard-required backend (the broker cannot serve its
/// system catalog without it); every other backend is optional and surfaces as a
/// warning when absent, matching the existing doctor/GetHealthReport behaviour.
pub fn build_readiness_facts(
    init: &RuntimeInitReport,
    native: &[NativeServiceStatus],
    auth_checks: &[(String, bool, String)],
) -> ReadinessFacts {
    let mut facts = Vec::new();

    // ── Core backends ──────────────────────────────────────────────────────
    facts.push(ReadinessFact::required(
        "postgres",
        init.postgres_configured,
        if init.postgres_configured {
            "PostgreSQL configured".to_string()
        } else {
            "PostgreSQL is required: set UDB_PG_DSN or DATABASE_URL".to_string()
        },
    ));
    facts.push(ReadinessFact::optional(
        "redis",
        init.redis_configured,
        if init.redis_configured {
            "Redis configured"
        } else {
            "Redis not configured; read-through cache and CDC idempotency degrade"
        },
    ));
    facts.push(ReadinessFact::optional(
        "object_store",
        object_store_configured(init),
        if object_store_configured(init) {
            "object store (S3/MinIO/Azure/GCS) configured"
        } else {
            "no object store configured; object/storage RPCs unavailable"
        },
    ));
    facts.push(ReadinessFact::optional(
        "qdrant",
        init.qdrant_configured,
        if init.qdrant_configured {
            "Qdrant configured"
        } else {
            "Qdrant not configured; vector RPCs unavailable"
        },
    ));

    // S1: a FULL canonical system store (saga / admin-audit / migration /
    // projection tables) is required whenever a relational write backend is
    // configured. Missing it = `udb_system` not provisioned, so the
    // saga/audit/admin RPCs fail-fast with FAILED_PRECONDITION; fail readiness
    // so health/doctor name it instead of it surfacing only as a per-RPC error.
    let relational_store_expected = init.postgres_configured
        || init.mysql_configured
        || init.sqlite_configured
        || init.mssql_configured;
    let system_store_ok = !relational_store_expected || init.full_system_store_registered;
    facts.push(ReadinessFact::required(
        "system_store",
        system_store_ok,
        if !relational_store_expected {
            "no relational backend configured; canonical system store not required".to_string()
        } else if init.full_system_store_registered {
            "canonical system store registered".to_string()
        } else {
            "canonical system store NOT registered — udb_system not provisioned; \
             saga/audit/admin RPCs return FAILED_PRECONDITION"
                .to_string()
        },
    ));

    // ── Auth-plane facts (signing keys / JWKS / casbin / audit sink / policy
    //    snapshot). The caller passes the already-evaluated auth checks; an auth
    //    check failure is required-fail only for the keys/model, optional for the
    //    informational posture checks (token_issuance / audit_sink "dev"). We
    //    treat a failing auth check as required (it means a misconfiguration that
    //    will break the first request) and a passing one as informational. ──
    for (name, ok, detail) in auth_checks {
        // A passing informational check still records as a (passing) required
        // fact so the surfaces agree on its presence; only its failure matters.
        facts.push(ReadinessFact::required(
            format!("auth.{name}"),
            *ok,
            detail.clone(),
        ));
    }

    // ── Native-service mount facts. A native service that is enabled but not
    //    mounted (missing dependency / listener disabled) is a warning, not a
    //    hard failure — the broker still serves its other surfaces. ──
    for status in native {
        if !status.enabled {
            continue;
        }
        let detail = if status.mounted {
            format!("native service '{}' mounted and serving", status.service_id)
        } else if status.disabled_reason.is_empty() {
            format!(
                "native service '{}' enabled but not mounted",
                status.service_id
            )
        } else {
            format!(
                "native service '{}' enabled but not mounted: {}",
                status.service_id, status.disabled_reason
            )
        };
        facts.push(ReadinessFact::optional(
            format!("native.{}", status.service_id),
            status.mounted,
            detail,
        ));
    }

    ReadinessFacts { facts }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── SLO ↔ metric-registry consistency ──────────────────────────────────

    /// Every metric a catalog SLO references must be a real collector registered
    /// in `metrics.rs`. We assert against the live Prometheus registry rather
    /// than a hand-maintained list so a rename in metrics.rs cannot drift.
    #[test]
    fn slo_metrics_exist() {
        // We assert each SLO-referenced metric is actually registered in
        // metrics.rs by rendering the registry as Prometheus text and matching
        // the `# TYPE <name>` / `# HELP <name>` header lines. `Registry::gather`
        // omits *label-vec* families that have no observed series, so we first
        // emit one sample through each vec metric's public recorder method to
        // force the family to materialise. Plain (non-vec) histograms always
        // emit, so they need no warm-up. This still validates against the live
        // registry — a rename in metrics.rs fails this test rather than drifting.
        use crate::runtime::metrics::MetricsRecorder;
        let metrics =
            crate::runtime::metrics::PrometheusMetrics::new().expect("metrics registry builds");
        // Warm the label-vec metrics the catalog references.
        metrics.record_grpc("Login", "ok", 0.01); // grpc_duration + grpc_requests
        metrics.inc_object_op("bucket", "PutObject"); // object_ops
        metrics.inc_vector_op("collection", "upsert"); // vector_ops
        metrics.inc_cdc_errors_total("warmup"); // cdc_errors

        let exposition = metrics.gather_text("");
        for slo in slo_catalog() {
            for metric in slo.referenced_metrics() {
                let type_line = format!("# TYPE {metric} ");
                let help_line = format!("# HELP {metric} ");
                assert!(
                    exposition.contains(&type_line) || exposition.contains(&help_line),
                    "SLO '{}' references metric '{metric}' which is not registered in metrics.rs",
                    slo.objective,
                );
            }
        }
    }

    #[test]
    fn slo_catalog_covers_every_required_lane() {
        let ids: std::collections::HashSet<&str> =
            slo_catalog().iter().map(|s| s.objective).collect();
        for required in [
            "authn.login",
            "authn.refresh",
            "authn.validate",
            "authz.check",
            "authz.batch_check",
            "storage.presign",
            "storage.finalize",
            "storage.list",
            "asset.pipeline_start",
            "asset.pipeline_step",
            "webrtc.signaling",
            "cdc.publish",
            "cdc.dlq",
            "policy.distribution_ack",
        ] {
            assert!(
                ids.contains(required),
                "SLO catalog missing lane '{required}'"
            );
        }
    }

    #[test]
    fn slo_objectives_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for slo in slo_catalog() {
            assert!(
                seen.insert(slo.objective),
                "duplicate SLO objective id '{}'",
                slo.objective
            );
            assert!(
                slo.availability > 0.0 && slo.availability <= 1.0,
                "availability objective for '{}' out of (0,1]",
                slo.objective
            );
        }
    }

    // ── Error-budget math ──────────────────────────────────────────────────

    #[test]
    fn error_budget_allowed_failures() {
        // 99.9% over 100_000 events → 100 allowed failures.
        let b = ErrorBudget::from_success_total(0.999, 100_000, 100_000);
        assert_eq!(b.allowed_failures(), 100);
        assert!(!b.is_exhausted());
        assert_eq!(b.failed, 0);
        assert_eq!(b.consumed_fraction(), 0.0);
    }

    #[test]
    fn error_budget_exhaustion() {
        // 99% objective: allowed failure ratio = 1%. 1% observed → exactly
        // exhausted (consumed == 1.0).
        let b = ErrorBudget::from_success_total(0.99, 9_900, 10_000);
        assert_eq!(b.allowed_failures(), 100);
        assert_eq!(b.failed, 100);
        assert!((b.consumed_fraction() - 1.0).abs() < 1e-9);
        assert!(b.is_exhausted());
        assert!((b.remaining_fraction()).abs() < 1e-9);
    }

    #[test]
    fn error_budget_overspend_is_negative_remaining() {
        // 2% failures against a 1% budget → 200% consumed, -100% remaining.
        let b = ErrorBudget::from_success_total(0.99, 9_800, 10_000);
        assert!((b.consumed_fraction() - 2.0).abs() < 1e-9);
        assert!((b.remaining_fraction() + 1.0).abs() < 1e-9);
        assert!(b.is_exhausted());
    }

    #[test]
    fn error_budget_empty_window_is_healthy() {
        let b = ErrorBudget::from_success_total(0.999, 0, 0);
        assert_eq!(b.failure_ratio(), 0.0);
        assert_eq!(b.consumed_fraction(), 0.0);
        assert!(!b.is_exhausted());
    }

    #[test]
    fn error_budget_clamps_success_to_total() {
        let b = ErrorBudget::from_success_total(0.999, 200, 100);
        assert_eq!(b.failed, 0);
    }

    #[test]
    fn burn_rate_canonical_fast_burn() {
        // 99.9% objective (0.1% budget). Observing 1.44% failures = 14.4x burn,
        // the canonical 2-hour page threshold.
        let r = burn_rate(0.999, 0.0144);
        assert!((r - 14.4).abs() < 1e-6, "expected ~14.4x burn, got {r}");
    }

    #[test]
    fn burn_rate_steady_state_is_one() {
        // Spending exactly the budget ratio → burn rate 1.0.
        let r = burn_rate(0.99, 0.01);
        assert!((r - 1.0).abs() < 1e-9);
    }

    #[test]
    fn burn_rate_perfect_objective_overspends_on_any_error() {
        assert_eq!(burn_rate(1.0, 0.0), 0.0);
        assert!(burn_rate(1.0, 0.0001).is_infinite());
    }

    // ── Readiness facts ────────────────────────────────────────────────────

    fn pg_only_init() -> RuntimeInitReport {
        RuntimeInitReport {
            postgres_configured: true,
            // A healthy PG broker provisions udb_system, so the full canonical
            // store registers — model that so readiness reflects a provisioned
            // broker (S1). The unregistered case is covered by its own test.
            full_system_store_registered: true,
            ..RuntimeInitReport::default()
        }
    }

    /// Minimal `NativeServiceStatus` for readiness tests (the struct has no
    /// `Default`); only the fields `build_readiness_facts` reads are meaningful.
    fn native_status(
        service_id: &str,
        enabled: bool,
        mounted: bool,
        reason: &str,
    ) -> NativeServiceStatus {
        NativeServiceStatus {
            service_id: service_id.to_string(),
            display_name: service_id.to_string(),
            proto_services: Vec::new(),
            enabled,
            configured: enabled,
            mounted,
            healthy: mounted,
            degraded: enabled && !mounted,
            surface: String::new(),
            listener_kind: String::new(),
            supported_rpcs: Vec::new(),
            selected_capabilities: Vec::new(),
            required_backends: Vec::new(),
            missing_dependencies: Vec::new(),
            disabled_reason: reason.to_string(),
            migration_enabled: false,
            migration_status: "disabled".to_string(),
            owns_background_workers: false,
            background_worker_enabled: false,
            background_workers: Vec::new(),
            descriptor_version: String::new(),
            enablement_key: String::new(),
            service_enablement_key: String::new(),
        }
    }

    #[test]
    fn readiness_passes_with_pg_and_no_failing_auth_checks() {
        let init = pg_only_init();
        let auth = vec![
            ("casbin_model".to_string(), true, "model parsed".to_string()),
            (
                "token_issuance".to_string(),
                true,
                "sessions-only dev".to_string(),
            ),
        ];
        let facts = build_readiness_facts(&init, &[], &auth);
        assert!(facts.passed(), "errors: {:?}", facts.errors());
        // Optional missing backends surface as warnings, not errors.
        assert!(facts.warnings().iter().any(|w| w.starts_with("redis")));
        assert!(facts.errors().is_empty());
        // The auth check folded in under an `auth.` prefix.
        assert!(facts.get("auth.casbin_model").is_some());
    }

    #[test]
    fn readiness_fails_without_postgres() {
        let init = RuntimeInitReport::default();
        let facts = build_readiness_facts(&init, &[], &[]);
        assert!(!facts.passed());
        assert!(facts.errors().iter().any(|e| e.starts_with("postgres")));
    }

    #[test]
    fn readiness_fails_on_failing_auth_check() {
        let init = pg_only_init();
        let auth = vec![(
            "jwt_signing_key".to_string(),
            false,
            "invalid RSA PEM".to_string(),
        )];
        let facts = build_readiness_facts(&init, &[], &auth);
        assert!(!facts.passed());
        assert!(
            facts
                .errors()
                .iter()
                .any(|e| e.starts_with("auth.jwt_signing_key"))
        );
    }

    #[test]
    fn unmounted_enabled_native_service_is_a_warning_not_error() {
        let init = pg_only_init();
        let status = native_status(
            "storage",
            true,
            false,
            "missing dependencies: object_storage",
        );
        let facts = build_readiness_facts(&init, std::slice::from_ref(&status), &[]);
        // Still ready overall (postgres present, native is optional).
        assert!(facts.passed());
        assert!(
            facts
                .warnings()
                .iter()
                .any(|w| w.starts_with("native.storage"))
        );
    }

    #[test]
    fn disabled_native_service_emits_no_fact() {
        let init = pg_only_init();
        let status = native_status("asset", false, false, "");
        let facts = build_readiness_facts(&init, std::slice::from_ref(&status), &[]);
        assert!(facts.get("native.asset").is_none());
    }

    #[test]
    fn readiness_fails_when_pg_configured_but_system_store_unregistered() {
        // S1: a relational backend is configured but the FULL canonical store did
        // not register (udb_system not provisioned) → readiness FAILS and names
        // `system_store`, instead of the saga/audit/admin RPCs surfacing it lazily
        // as a per-RPC FAILED_PRECONDITION.
        let init = RuntimeInitReport {
            postgres_configured: true,
            full_system_store_registered: false,
            ..RuntimeInitReport::default()
        };
        let facts = build_readiness_facts(&init, &[], &[]);
        assert!(!facts.passed(), "expected readiness to fail");
        assert!(
            facts.errors().iter().any(|e| e.starts_with("system_store")),
            "errors: {:?}",
            facts.errors()
        );
    }

    #[test]
    fn system_store_not_required_without_relational_backend() {
        // A pure cache/object deployment (no relational backend) legitimately has
        // no full canonical store; `system_store` must NOT fail readiness there.
        let init = RuntimeInitReport {
            redis_configured: true,
            full_system_store_registered: false,
            ..RuntimeInitReport::default()
        };
        let facts = build_readiness_facts(&init, &[], &[]);
        // postgres is still a required fact and will fail, but the system_store
        // fact itself must be satisfied (not an error).
        let store_fact = facts.get("system_store").expect("system_store fact present");
        assert!(store_fact.ok, "system_store should be ok without a relational backend");
    }
}
