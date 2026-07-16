//! Per-tenant scheduled-job budget: a resolve-once cap and the PURE quota gate.

use tonic::Status;

/// Per-tenant scheduled-job budget (non-deleted rows). Bounds the durable table
/// so one tenant cannot exhaust the shared store; a new job beyond this fails
/// closed with the typed quota detail. Overridable once via
/// `UDB_MAX_JOBS_PER_TENANT`, resolved through a `OnceLock` — never read per
/// request. Mirrors `search_service`'s `MAX_INDEXES_PER_TENANT` gate.
pub(crate) const DEFAULT_MAX_JOBS_PER_TENANT: i64 = 1000;

/// Resolve the per-tenant job budget exactly once (no per-request env reads).
/// A non-positive / unparsable override falls back to the default so the gate
/// is always a real bound.
pub(crate) fn max_jobs_per_tenant() -> i64 {
    static BUDGET: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("UDB_MAX_JOBS_PER_TENANT")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_MAX_JOBS_PER_TENANT)
    })
}

/// PURE quota gate: refuse when the tenant's non-deleted job count has reached
/// the budget. `ResourceExhausted` + `kind = QUOTA` via the shared
/// `quota_refusal_status` typed detail (same shape as the search-index gate).
pub(crate) fn enforce_job_quota(active_jobs: i64, budget: i64) -> Result<(), Status> {
    if active_jobs >= budget {
        return Err(crate::runtime::executor_utils::quota_refusal_status(
            "scheduler",
            // Operation identifiers are normalized to underscore form on the wire
            // (matches the `tenant_storage_quota` convention); pass it explicitly.
            "tenant_scheduled-job_quota",
            format!("tenant scheduled-job quota exhausted ({budget})"),
        ));
    }
    Ok(())
}
