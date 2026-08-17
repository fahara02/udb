//! Per-tenant scheduled-job budget: a resolve-once cap and the shared typed
//! refusal. The gate itself lives in the guarded INSERT (see below).

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

/// The typed per-tenant scheduled-job quota refusal (`ResourceExhausted` +
/// `kind = QUOTA`) built through the shared `quota_refusal_status` detail (same
/// shape as the search-index gate). A single constructor so the PURE gate and
/// the atomic (0-rows-inserted) gate in `handlers` fail with the IDENTICAL
/// typed detail and message.
pub(crate) fn job_quota_exhausted_status(budget: i64) -> Status {
    crate::runtime::executor_utils::quota_refusal_status(
        "scheduler",
        // Operation identifiers are normalized to underscore form on the wire
        // (matches the `tenant_storage_quota` convention); pass it explicitly.
        "tenant_scheduled-job_quota",
        format!("tenant scheduled-job quota exhausted ({budget})"),
    )
}

// A `enforce_job_quota(active_jobs, budget)` helper used to live here: count the
// tenant's jobs, then refuse if the count had reached the budget. It had no
// caller, and it must not gain one — COUNT-then-INSERT is a TOCTOU race, as the
// create path documents: concurrent creates at budget-1 each read
// `count < budget` because their peers' inserts are invisible under READ
// COMMITTED, and every one of them proceeds. The quota is enforced instead by
// `guarded_insert_job_sql`, which counts inside the same statement as the INSERT
// under the per-tenant advisory lock, so 0 rows inserted means at/over budget.
// `job_quota_exhausted_status` above is the refusal the create path returns
// when that guarded INSERT reports 0 rows.
