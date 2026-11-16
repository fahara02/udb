//! Which keyspace a `CacheService` request addresses: the caller's own tenant
//! keyspace, or the shared namespace every tenant reads.
//!
//! The shared namespace lives under the reserved shared-tenant marker
//! ([`crate::broker::SHARED_TENANT_MARKER`]) in the tenant segment of the key
//! (`udb:cache:*shared*:<ns>:k:<key>`), the same marker shared table rows carry
//! in derived stores. A caller can never BE that tenant (the marker is refused
//! as a caller identity below), so a tenant-scoped request can never produce a
//! shared key and a shared request can never produce a tenant key.
//!
//! Reads of the shared namespace are open to any authenticated caller of any
//! tenant. Writes take the same authority as writing a shared table
//! (`broker::may_write_global_table`): a platform admin or a holder of
//! `udb:global:write`. A trusted in-process caller (no verified claim context
//! installed; every over-the-wire request has one) is let through, like the
//! other native handler guards.

use tonic::Status;
use tonic::metadata::MetadataMap;

use super::super::native_helpers::validate_request_tenant;

/// Whether the RPC reads or writes the addressed keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheAccess {
    Read,
    Write,
}

/// The resolved keyspace of one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CacheScope {
    /// The verified caller tenant. Fair admission is charged to it, so shared
    /// traffic from one tenant cannot starve another tenant's lane.
    pub(crate) caller_tenant: String,
    /// The tenant segment of every key this request touches: the caller tenant,
    /// or the shared marker when `shared` is set.
    pub(crate) key_tenant: String,
    pub(crate) shared: bool,
}

impl CacheScope {
    /// Stamp the scope onto an outbox event payload: `tenant_id` is the key
    /// tenant (the shared marker for a shared write, as on shared table rows),
    /// `shared` says which keyspace changed, and a shared write also names the
    /// tenant of the caller who made it.
    #[cfg_attr(not(feature = "redis"), allow(dead_code))]
    pub(crate) fn event_payload(&self, mut payload: serde_json::Value) -> serde_json::Value {
        if let Some(object) = payload.as_object_mut() {
            object.insert(
                "tenant_id".to_string(),
                serde_json::Value::String(self.key_tenant.clone()),
            );
            object.insert("shared".to_string(), serde_json::Value::Bool(self.shared));
            if self.shared {
                object.insert(
                    "actor_tenant_id".to_string(),
                    serde_json::Value::String(self.caller_tenant.clone()),
                );
            }
        }
        payload
    }
}

/// Resolve the keyspace of a request. The body tenant is checked against the
/// verified claim first (cross-tenant guard), the reserved shared marker is
/// refused as a caller identity, and a shared write must hold shared-write
/// authority. `operation` names the RPC in the typed refusal.
pub(crate) fn resolve_scope(
    metadata: &MetadataMap,
    body_tenant_id: &str,
    shared: bool,
    access: CacheAccess,
    operation: &'static str,
) -> Result<CacheScope, Status> {
    validate_request_tenant(metadata, body_tenant_id)?;
    let caller_tenant = body_tenant_id.trim().to_string();
    scope_for_caller(caller_tenant, shared, access, operation, || {
        require_shared_write_authority(operation)
    })
}

/// The pure part of [`resolve_scope`], with the claim check injected so it is
/// unit-testable without a task-local claim context.
pub(crate) fn scope_for_caller(
    caller_tenant: String,
    shared: bool,
    access: CacheAccess,
    operation: &'static str,
    authorize_shared_write: impl FnOnce() -> Result<(), Status>,
) -> Result<CacheScope, Status> {
    if crate::broker::is_shared_tenant_marker(&caller_tenant) {
        return Err(crate::runtime::executor_utils::policy_status_with_code(
            tonic::Code::PermissionDenied,
            operation,
            "shared_tenant_marker_reserved",
            format!(
                "permission denied: tenant id '{}' is reserved for shared data and is not a \
                 caller identity; set `shared` to address the shared namespace",
                crate::broker::SHARED_TENANT_MARKER
            ),
        ));
    }
    if !shared {
        return Ok(CacheScope {
            key_tenant: caller_tenant.clone(),
            caller_tenant,
            shared: false,
        });
    }
    if access == CacheAccess::Write {
        authorize_shared_write()?;
    }
    Ok(CacheScope {
        caller_tenant,
        key_tenant: crate::broker::SHARED_TENANT_MARKER.to_string(),
        shared: true,
    })
}

/// Shared-write authority for the current request: a trusted in-process caller
/// (no claim context installed) passes; an over-the-wire caller must satisfy
/// [`shared_write_authority_from_claim`].
pub(crate) fn require_shared_write_authority(operation: &'static str) -> Result<(), Status> {
    use crate::runtime::service::method_security::{claim_context_present, current_claim_context};
    if !claim_context_present() {
        return Ok(());
    }
    shared_write_authority_from_claim(&current_claim_context(), operation)
}

/// The decision behind [`require_shared_write_authority`] for one verified
/// claim: an authenticated platform admin or holder of `udb:global:write`, never
/// a claim whose tenant is the shared marker itself. Pure.
pub(crate) fn shared_write_authority_from_claim(
    claim: &crate::runtime::service::method_security::VerifiedClaimContext,
    operation: &'static str,
) -> Result<(), Status> {
    let context = crate::RequestContext {
        tenant_id: claim.tenant_id.clone(),
        project_id: claim.project_id.clone(),
        scopes: claim.scopes.clone(),
        ..crate::RequestContext::default()
    };
    let allowed = claim.authenticated
        && !crate::broker::is_shared_tenant_marker(&claim.tenant_id)
        && (claim.is_cross_tenant_admin() || crate::broker::may_write_global_table(&context));
    if allowed {
        return Ok(());
    }
    Err(shared_cache_write_denied_status(operation))
}

/// The refusal for a shared cache write by a caller without shared-write
/// authority. One definition for the native `CacheService` and the DataBroker
/// cache RPCs.
pub(crate) fn shared_cache_write_denied_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        operation,
        "shared_cache_write_denied",
        format!(
            "permission denied: writing the shared cache namespace requires a platform admin or \
             scope {}",
            crate::broker::GLOBAL_WRITE_SCOPE
        ),
    )
}
