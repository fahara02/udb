//! Fail-closed request-time tenant-status gate for the native `TenantService`.
//!
//! Setting a tenant SUSPENDED/INACTIVE (via [`super::handlers::update_tenant`])
//! must take effect on LIVE bearer tokens IMMEDIATELY — not only when they expire
//! at their TTL. Persisted `tenants.status` is the durable source of truth, but a
//! request-time gate has to reject a suspended tenant's calls BEFORE dispatch,
//! otherwise the tenant keeps operating on its existing tokens for their full TTL.
//!
//! This module provides:
//!   * [`decide_tenant_status`] — the pure, fail-closed decision keyed on the
//!     canonical stored status token (only `ACTIVE` proceeds; SUSPENDED/INACTIVE
//!     and any unknown/empty token are denied);
//!   * a process-wide fast suspension signal ([`mark_tenant_status`] /
//!     [`tenant_status_gate`]) — the handler records a tenant's status whenever it
//!     processes a transition, so the request gate revokes a just-suspended
//!     tenant's live tokens on this node without a per-request durable read.
//!
//! The request gate lives in the shared method-security tower layer; it invokes
//! [`tenant_status_gate`] on the VALIDATED claim tenant before dispatch (see the
//! `TODO(leader-wire)` below). Keeping the decision + signal here means the
//! transport layer never grows tenant-domain knowledge.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use tonic::Status;

use super::config::TENANT_STATUS_ACTIVE_DB;

// TODO(leader-wire): the shared request-time enforcement gate in
// `src/runtime/service/method_security.rs` must invoke this gate on the VALIDATED
// claim tenant BEFORE dispatch, so a SUSPENDED/INACTIVE tenant is rejected while
// its bearer is still within TTL. In `enforce(...)`, immediately after the
// `VerifiedClaimContext` is built (`let claim_ctx = VerifiedClaimContext::from_principal(...)`,
// near the end of `enforce`) and only when `claim_ctx.authenticated`, call:
//
//     crate::runtime::service::tenant_service::tenant_status_gate(&claim_ctx.tenant_id)
//         .map_err(|status| (status, "tenant_suspended"))?;
//
// Do NOT gate public bootstrap RPCs (no validated claim → `claim_ctx.tenant_id`
// is empty; `tenant_status_gate` already no-ops on an empty tenant). Keying on the
// caller's OWN claim tenant means a cross-tenant/platform admin (whose claim
// tenant is their own, active tenant) still reaches UpdateTenant to REACTIVATE a
// suspended tenant — reactivation is never blocked by the suspended tenant's own
// signal. Add `"tenant_suspended"` to the `deny_reason` label set for the
// `udb_method_security_denials_total{reason}` metric.
//
// NOTE (cluster propagation): the signal below is per-process, so it revokes
// immediately on nodes that observed the transition. For cluster-wide immediacy
// the leader can additionally back the signal with the existing Redis cutoff
// denylist (`TenantServiceImpl::jti_denylist`), mirroring the PurgeTenant path;
// the durable `tenants.status` read remains the eventual backstop on every node.

/// The typed denial a suspended/inactive tenant receives at the request gate:
/// `FailedPrecondition` (the tenant object exists but is not in a serviceable
/// state), carrying the standard native policy detail for audit correlation. The
/// caller is always gated on its OWN claim tenant, so naming its status is not a
/// cross-tenant disclosure.
fn tenant_not_active_status(status: &str) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::FailedPrecondition,
        "tenant_status_gate",
        "tenant_not_active",
        format!("tenant is not active (status: {status}); access is suspended"),
    )
}

/// Whether a canonical stored status token is the serviceable ACTIVE state.
/// Matches `super::model::tenant_status_to_db` (ACTIVE / SUSPENDED / INACTIVE):
/// only ACTIVE is serviceable; every other token (SUSPENDED, INACTIVE, unknown,
/// empty) fails closed.
fn status_is_active(status: &str) -> bool {
    status.trim().eq_ignore_ascii_case(TENANT_STATUS_ACTIVE_DB)
}

/// Pure, fail-closed status decision keyed on the canonical stored status token.
/// `Ok(())` only for the ACTIVE token; SUSPENDED/INACTIVE and any unknown/empty
/// token are denied. Used by callers that already hold the durable status.
pub(crate) fn decide_tenant_status(status_db_token: &str) -> Result<(), Status> {
    if status_is_active(status_db_token) {
        Ok(())
    } else {
        Err(tenant_not_active_status(status_db_token.trim()))
    }
}

/// Process-wide fast suspension signal: `tenant_id → last-observed status token`.
/// Absent = this node has never observed the tenant's status (the gate falls
/// through to allow; the durable read on the handler path is the eventual
/// backstop). A poisoned lock is recovered in place — the only writer just
/// inserts a `String`, so it cannot leave inconsistent state.
fn suspension_registry() -> &'static RwLock<HashMap<String, String>> {
    static REG: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();
    REG.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Record a tenant's latest known status (called by the handler AFTER a durable
/// status write) so the request gate can revoke a just-suspended tenant's live
/// tokens immediately, without waiting for the bearer TTL. No-op for an empty id.
pub(crate) fn mark_tenant_status(tenant_id: &str, status_db_token: &str) {
    let tenant_id = tenant_id.trim();
    if tenant_id.is_empty() {
        return;
    }
    let mut reg = suspension_registry()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    reg.insert(tenant_id.to_string(), status_db_token.trim().to_string());
}

/// Request-time gate consulted by the shared method-security layer (see
/// `TODO(leader-wire)` above). Rejects a request whose claim tenant this node has
/// observed as non-ACTIVE (SUSPENDED/INACTIVE); returns `Ok(())` when the tenant
/// is known-active or has never been observed here. An empty tenant (public
/// bootstrap / in-process caller with no claim) is not gated.
///
/// `#[allow(dead_code)]`: alive via the unit tests + the leader-wired
/// method-security gate call (`TODO(leader-wire)` above); it is not yet invoked
/// from non-test code because the shared tower layer is edited by the leader.
#[allow(dead_code)]
pub(crate) fn tenant_status_gate(tenant_id: &str) -> Result<(), Status> {
    let tenant_id = tenant_id.trim();
    if tenant_id.is_empty() {
        return Ok(());
    }
    let reg = suspension_registry()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match reg.get(tenant_id) {
        // Observed on this node → the pure fail-closed decision applies (revokes
        // live tokens of a suspended/inactive tenant immediately).
        Some(status) => decide_tenant_status(status),
        // Never observed here → allow; the durable read on the handler path is
        // the eventual backstop for a suspend this node has not yet seen.
        None => Ok(()),
    }
}
