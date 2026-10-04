//! Fail-closed request-time tenant-status gate for the native `TenantService`.
//!
//! Setting a tenant SUSPENDED/INACTIVE (via [`super::handlers::update_tenant`])
//! must take effect on LIVE bearer tokens IMMEDIATELY — not only when they expire
//! at their TTL — and on EVERY replica, including one that restarted after the
//! suspension. Persisted `tenants.status` is the durable source of truth, so the
//! request gate reads it.
//!
//! This module provides:
//!   * [`decide_tenant_status`] — the pure, fail-closed decision keyed on the
//!     canonical stored status token (only `ACTIVE` proceeds; SUSPENDED/INACTIVE
//!     and any unknown/empty token are denied);
//!   * a short-TTL status cache ([`mark_tenant_status`] / [`tenant_status_gate`])
//!     — the handler writes a tenant's status into it whenever it processes a
//!     transition (immediate invalidation on this node), and the durable gate
//!     fills it from the tenant row;
//!   * [`tenant_status_gate_durable`] — the request gate the shared
//!     method-security layer awaits before dispatch: a fresh cache entry decides
//!     directly; otherwise (unknown tenant, or an entry older than the TTL) the
//!     tenant row is READ from the registered tenant store, so a suspension made
//!     on another replica — or before this process started — is enforced within
//!     one cache TTL ([`TENANT_STATUS_CACHE_TTL`], 5s). A store error
//!     with no previously known status fails closed (retryable `Unavailable`).
//!
//! Keying on the caller's OWN claim tenant means a cross-tenant/platform admin
//! (whose claim tenant is their own, active tenant) still reaches UpdateTenant
//! to REACTIVATE a suspended tenant. Public bootstrap RPCs carry no validated
//! claim tenant and are never gated. Keeping the decision + cache here means the
//! transport layer never grows tenant-domain knowledge.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tonic::Status;

use super::config::TENANT_STATUS_ACTIVE_DB;

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

/// Stored status token for a soft-deleted tenant row: never serviceable.
const TENANT_STATUS_DELETED: &str = "DELETED";
/// Freshness of a cached tenant status before the gate re-reads the row: the
/// bound on how long a suspension made on ANOTHER replica can go unenforced
/// here (a transition processed on this node invalidates its entry at once).
pub(crate) const TENANT_STATUS_CACHE_TTL: Duration = Duration::from_secs(5);
/// Bound on cached tenants; the cache is cleared (not grown) past it.
const TENANT_STATUS_CACHE_MAX: usize = 100_000;

/// One cached observation of a tenant's status. `status: None` records that the
/// tenant store has NO row for this tenant (an unmanaged tenant id — e.g. a
/// deployment that never registered tenants), which is not a suspension.
#[derive(Clone, Debug)]
struct CachedTenantStatus {
    status: Option<String>,
    observed_at: Instant,
}

/// Process-wide `tenant_id → last-observed status`. A poisoned lock is recovered
/// in place — writers only insert/replace whole entries, so it cannot be left
/// inconsistent.
fn status_cache() -> &'static RwLock<HashMap<String, CachedTenantStatus>> {
    static REG: OnceLock<RwLock<HashMap<String, CachedTenantStatus>>> = OnceLock::new();
    REG.get_or_init(|| RwLock::new(HashMap::new()))
}

fn cache_put(tenant_id: &str, status: Option<String>) {
    let mut cache = status_cache()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if cache.len() >= TENANT_STATUS_CACHE_MAX && !cache.contains_key(tenant_id) {
        cache.clear();
    }
    cache.insert(
        tenant_id.to_string(),
        CachedTenantStatus {
            status,
            observed_at: Instant::now(),
        },
    );
}

fn cache_get(tenant_id: &str) -> Option<CachedTenantStatus> {
    status_cache()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(tenant_id)
        .cloned()
}

/// Decision for one cached observation: no row ⇒ not a managed tenant ⇒ allow;
/// a row ⇒ the fail-closed status decision.
fn decide_cached(entry: &CachedTenantStatus) -> Result<(), Status> {
    match entry.status.as_deref() {
        Some(status) => decide_tenant_status(status),
        None => Ok(()),
    }
}

/// The tenant store the durable gate reads (`udb_tenant.tenants`), registered
/// when the native `TenantService` is built. Unregistered (no Postgres-backed
/// tenant store) ⇒ the gate can only apply statuses observed on this node.
fn status_store() -> &'static RwLock<Option<PgPool>> {
    static STORE: OnceLock<RwLock<Option<PgPool>>> = OnceLock::new();
    STORE.get_or_init(|| RwLock::new(None))
}

/// Register (or clear) the tenant store the durable request gate reads.
pub(crate) fn register_tenant_status_store(pool: Option<PgPool>) {
    *status_store()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = pool;
}

fn registered_status_store() -> Option<PgPool> {
    status_store()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// Record a tenant's latest known status (called by the handler AFTER a durable
/// status write) so the request gate on THIS node revokes a just-suspended
/// tenant's live tokens immediately — the cache entry is replaced, which is the
/// local invalidation; other replicas re-read the row within one TTL. No-op for
/// an empty id.
pub(crate) fn mark_tenant_status(tenant_id: &str, status_db_token: &str) {
    let tenant_id = tenant_id.trim();
    if tenant_id.is_empty() {
        return;
    }
    cache_put(tenant_id, Some(status_db_token.trim().to_string()));
}

/// Synchronous fast-path gate over the cache only (used where no await is
/// possible, e.g. inside an open transaction stream): rejects a tenant this node
/// has observed as non-ACTIVE; a tenant never observed here passes this fast
/// path — the awaited [`tenant_status_gate_durable`] at the transport layer is
/// what reads the row for it. An empty tenant is not gated.
pub(crate) fn tenant_status_gate(tenant_id: &str) -> Result<(), Status> {
    let tenant_id = tenant_id.trim();
    if tenant_id.is_empty() {
        return Ok(());
    }
    match cache_get(tenant_id) {
        Some(entry) => decide_cached(&entry),
        None => Ok(()),
    }
}

/// Read the tenant's durable status row. `Ok(None)` = no row (unmanaged tenant
/// id); a soft-deleted row reads as `DELETED` (never serviceable). The lookup
/// pins `app.current_tenant_id` to the caller's own tenant so the forced
/// tenant RLS policy admits exactly that row. Matches the canonical UUID or the
/// human `code` alias.
async fn read_tenant_status(pool: &PgPool, tenant_id: &str) -> Result<Option<String>, String> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|err| format!("begin tenant status read failed: {err}"))?;
    sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| format!("set tenant status read context failed: {err}"))?;
    let row: Option<(String, bool)> = sqlx::query_as(
        "SELECT COALESCE(status, ''), deleted_at IS NOT NULL \
         FROM udb_tenant.tenants WHERE tenant_id::text = $1 OR code = $1 \
         ORDER BY (tenant_id::text = $1) DESC LIMIT 1",
    )
    .bind(tenant_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|err| format!("tenant status read failed: {err}"))?;
    tx.commit()
        .await
        .map_err(|err| format!("commit tenant status read failed: {err}"))?;
    Ok(row.map(|(status, deleted)| {
        if deleted {
            TENANT_STATUS_DELETED.to_string()
        } else {
            status
        }
    }))
}

/// The retryable denial when the tenant store cannot be read and this node has
/// never observed the tenant's status: fail closed, but tell the client to retry.
fn tenant_status_unavailable() -> Status {
    crate::runtime::executor_utils::retryable_status(
        "tenant",
        "tenant_status_gate",
        crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
        "tenant status temporarily unavailable",
    )
}

/// Durable request-time gate (awaited by the method-security layer on the
/// VALIDATED claim tenant before dispatch). A cache entry younger than the TTL
/// decides directly; otherwise the tenant row is read and cached. On a store
/// error a previously observed (stale) status still decides; with none, the
/// request fails closed as retryable `Unavailable`. With no registered tenant
/// store the cache-only fast path applies. An empty tenant is not gated.
pub(crate) async fn tenant_status_gate_durable(tenant_id: &str) -> Result<(), Status> {
    let tenant_id = tenant_id.trim();
    if tenant_id.is_empty() {
        return Ok(());
    }
    let cached = cache_get(tenant_id);
    if let Some(entry) = cached.as_ref()
        && entry.observed_at.elapsed() < TENANT_STATUS_CACHE_TTL
    {
        return decide_cached(entry);
    }
    let Some(pool) = registered_status_store() else {
        return tenant_status_gate(tenant_id);
    };
    match read_tenant_status(&pool, tenant_id).await {
        Ok(status) => {
            cache_put(tenant_id, status.clone());
            match status {
                Some(status) => decide_tenant_status(&status),
                None => Ok(()),
            }
        }
        Err(error) => {
            tracing::warn!(tenant_id, %error, "tenant status gate: durable read failed");
            match cached {
                Some(entry) => decide_cached(&entry),
                None => Err(tenant_status_unavailable()),
            }
        }
    }
}
