//! Per-tenant CONCURRENT-stream budget: a process-local limiter over in-process
//! resources (spawned forwarder tasks + bounded channels), not a data store —
//! exactly like the `channels.rs` fair-admission semaphores. An RAII [`StreamSlot`]
//! releases the slot on every exit path of whoever owns it.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use tonic::Status;

use super::config::{max_streams_global, max_streams_per_tenant};

/// Per-tenant ACTIVE live-query stream counter. Poisoning is recovered
/// (`PoisonError::into_inner`, the `channels.rs` idiom) so a panicked holder can
/// never wedge the limiter.
pub(crate) fn active_streams() -> &'static Mutex<HashMap<String, usize>> {
    static ACTIVE: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    ACTIVE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Current number of ACTIVE live-query streams held by `tenant_id` (0 when the
/// tenant holds none). Read-only snapshot of the limiter map, used to drive the
/// per-tenant active-stream metrics gauge. Poisoning is recovered like the other
/// accessors so a panicked holder can never wedge the read.
pub(crate) fn active_stream_count(tenant_id: &str) -> usize {
    active_streams()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(tenant_id)
        .copied()
        .unwrap_or(0)
}

/// Typed hard refusal for an over-budget tenant: `ResourceExhausted` with the
/// QUOTA `ErrorDetail` (not retryable by itself — the caller must close an
/// existing stream to free a slot).
pub(crate) fn livequery_stream_budget_status(budget: usize) -> Status {
    crate::runtime::executor_utils::quota_refusal_status(
        "livequery",
        "tenant_stream_budget",
        format!("tenant live-query stream budget exhausted ({budget} concurrent streams)"),
    )
}

/// Typed hard refusal when the PROCESS-WIDE stream ceiling is reached (distinct
/// operation label so operators can tell a global saturation from a single
/// tenant's budget exhaustion).
pub(crate) fn livequery_global_budget_status(ceiling: usize) -> Status {
    crate::runtime::executor_utils::quota_refusal_status(
        "livequery",
        "global_stream_ceiling",
        format!("global live-query stream ceiling reached ({ceiling} concurrent streams)"),
    )
}

/// The admission decision for one new stream, factored out as a pure function so
/// the two-tier budget (global ceiling first, then per-tenant) is unit-tested
/// without the process-global counter. `Err(true)` = global ceiling hit,
/// `Err(false)` = the tenant's own budget hit.
fn admit_stream(
    total_active: usize,
    tenant_active: usize,
    per_tenant: usize,
    global: usize,
) -> Result<(), bool> {
    if total_active >= global {
        return Err(true);
    }
    if tenant_active >= per_tenant {
        return Err(false);
    }
    Ok(())
}

/// RAII slot on the per-tenant active-stream budget. Constructed only by
/// [`try_acquire_stream_slot`]; the count is decremented on `Drop`, so the slot
/// is released on EVERY exit path of whoever owns it — normal stream end,
/// error close, or task abort/panic. Owned by the spawned delta forwarder for
/// the lifetime of the live stream.
pub(crate) struct StreamSlot {
    tenant_id: String,
}

impl Drop for StreamSlot {
    fn drop(&mut self) {
        let mut map = active_streams()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match map.get_mut(&self.tenant_id) {
            Some(count) if *count > 1 => *count -= 1,
            // Last slot for this tenant: remove the entry so the limiter map
            // never accumulates dead tenants.
            _ => {
                map.remove(&self.tenant_id);
            }
        }
    }
}

/// Claim one active-stream slot for `tenant_id`, failing CLOSED with the typed
/// quota refusal when the tenant is already at [`max_streams_per_tenant`].
pub(crate) fn try_acquire_stream_slot(tenant_id: &str) -> Result<StreamSlot, Status> {
    let per_tenant = max_streams_per_tenant();
    let global = max_streams_global();
    let mut map = active_streams()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let total_active: usize = map.values().sum();
    let tenant_active = map.get(tenant_id).copied().unwrap_or(0);
    match admit_stream(total_active, tenant_active, per_tenant, global) {
        Err(true) => return Err(livequery_global_budget_status(global)),
        Err(false) => return Err(livequery_stream_budget_status(per_tenant)),
        Ok(()) => {}
    }
    *map.entry(tenant_id.to_string()).or_insert(0) += 1;
    Ok(StreamSlot {
        tenant_id: tenant_id.to_string(),
    })
}

#[cfg(test)]
mod admit_tests {
    use super::admit_stream;

    #[test]
    fn admits_below_both_budgets() {
        assert_eq!(admit_stream(10, 2, 64, 4096), Ok(()));
    }

    #[test]
    fn global_ceiling_wins_even_when_the_tenant_is_under_budget() {
        // A fresh tenant (0 streams, well under per-tenant 64) is still refused
        // once the process-wide total hits the global ceiling.
        assert_eq!(admit_stream(4096, 0, 64, 4096), Err(true));
    }

    #[test]
    fn per_tenant_budget_refuses_when_global_has_headroom() {
        assert_eq!(admit_stream(100, 64, 64, 4096), Err(false));
    }
}
