//! Per-tenant CONCURRENT-stream budget: a process-local limiter over in-process
//! resources (spawned forwarder tasks + bounded channels), not a data store —
//! exactly like the `channels.rs` fair-admission semaphores. An RAII [`StreamSlot`]
//! releases the slot on every exit path of whoever owns it.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use tonic::Status;

use super::config::max_streams_per_tenant;

/// Per-tenant ACTIVE live-query stream counter. Poisoning is recovered
/// (`PoisonError::into_inner`, the `channels.rs` idiom) so a panicked holder can
/// never wedge the limiter.
pub(crate) fn active_streams() -> &'static Mutex<HashMap<String, usize>> {
    static ACTIVE: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    ACTIVE.get_or_init(|| Mutex::new(HashMap::new()))
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
    let budget = max_streams_per_tenant();
    let mut map = active_streams()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let count = map.entry(tenant_id.to_string()).or_insert(0);
    if *count >= budget {
        return Err(livequery_stream_budget_status(budget));
    }
    *count += 1;
    Ok(StreamSlot {
        tenant_id: tenant_id.to_string(),
    })
}
