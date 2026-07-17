//! Neutral-IR query builders and the `ListTenants` scope resolution for the
//! native `TenantService`. Extracted verbatim — the `LogicalRead`/`LogicalRecord`
//! shapes, the active-row filter, and the fail-closed subtree scoping are
//! byte-for-byte identical to the former god file.

use tonic::Status;

use crate::ir::{
    ComparisonOp, LogicalFilter, LogicalPagination, LogicalProjection, LogicalRead, LogicalRecord,
    LogicalValue,
};
use crate::proto::udb::core::tenant::services::v1 as tenant_pb;
use crate::runtime::service::method_security::VerifiedClaimContext;

use super::config::{TENANT_CONFIG_MSG, TENANT_MSG};

pub(crate) fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

/// Resolved listing scope for `ListTenants`.
#[derive(Debug)]
pub(crate) struct ListTenantsScope {
    /// Tenant used for fair admission — the caller's VALIDATED claim tenant
    /// (empty only for in-process callers with no claim context installed).
    pub(crate) admit_tenant: String,
    /// `Some(claim_tenant)` when the caller is NOT a cross-tenant admin: the
    /// listing is restricted to the caller's own row plus direct children
    /// (`tenant_id = claim OR parent_tenant_id = claim`).
    pub(crate) subtree_of: Option<String>,
}

/// Derive the `ListTenants` scope from the validated claim context (the SAME
/// method-security task-local the D1/D2 handler guards read — never from
/// request-supplied metadata). Fail-closed rules:
/// - no claim context installed → not an over-the-wire request (in-process /
///   trusted caller, see `claim_context_present`): historical unscoped list;
/// - genuine cross-tenant admin (`VerifiedClaimContext::is_cross_tenant_admin`,
///   the same helper the auth lane and native RLS escape hatch use) → unscoped
///   platform enumeration (current behavior), admitted on the claim tenant;
/// - non-admin with a tenant-bound claim → own-subtree listing, admitted on the
///   claim tenant;
/// - non-admin WITHOUT a tenant-bound claim → DENY (nothing to anchor on; a
///   tenant-less token must not enumerate the platform).
pub(crate) fn list_tenants_scope(
    claim_present: bool,
    claim: &VerifiedClaimContext,
) -> Result<ListTenantsScope, Status> {
    if !claim_present {
        return Ok(ListTenantsScope {
            admit_tenant: String::new(),
            subtree_of: None,
        });
    }
    let claim_tenant = claim.tenant_id.trim().to_string();
    if claim.is_cross_tenant_admin() {
        return Ok(ListTenantsScope {
            admit_tenant: claim_tenant,
            subtree_of: None,
        });
    }
    if claim_tenant.is_empty() {
        return Err(crate::runtime::executor_utils::policy_status_with_code(
            tonic::Code::PermissionDenied,
            "native_request_scope",
            "tenant_list_scope_required",
            "ListTenants requires a tenant-scoped bearer token or a cross-tenant admin role",
        ));
    }
    Ok(ListTenantsScope {
        admit_tenant: claim_tenant.clone(),
        subtree_of: Some(claim_tenant),
    })
}

/// The non-admin `ListTenants` subtree predicate: the caller's own tenant row
/// plus its direct children. Compared as TEXT so a malformed (non-UUID) claim
/// tenant matches zero rows instead of aborting the whole query on a UUID cast
/// error (fail closed, never fail open).
pub(crate) fn list_tenants_subtree_predicate(
    tenant_col: &str,
    parent_col: &str,
    bind: &str,
) -> String {
    format!("({tenant_col}::text = {bind} OR {parent_col}::text = {bind})")
}

pub(crate) fn active_tenant_filter(tenant_id: &str) -> LogicalFilter {
    LogicalFilter::And(vec![
        LogicalFilter::Comparison {
            field: "tenant_id".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(tenant_id),
        },
        LogicalFilter::IsNull("deleted_at".to_string()),
    ])
}

pub(crate) fn tenant_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "tenant_id".to_string(),
        "code".to_string(),
        "name".to_string(),
        "type".to_string(),
        "status".to_string(),
        "parent_tenant_id".to_string(),
        "config".to_string(),
        "branding".to_string(),
        "deleted_by".to_string(),
    ])
}

pub(crate) fn tenant_read_by_id(tenant_id: &str) -> LogicalRead {
    LogicalRead {
        message_type: TENANT_MSG.to_string(),
        filter: Some(active_tenant_filter(tenant_id)),
        projection: Some(tenant_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

pub(crate) fn tenant_config_filter(tenant_id: &str, config_key: Option<&str>) -> LogicalFilter {
    let mut filters = vec![LogicalFilter::Comparison {
        field: "tenant_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(tenant_id),
    }];
    if let Some(config_key) = config_key.filter(|value| !value.trim().is_empty()) {
        filters.push(LogicalFilter::Comparison {
            field: "config_key".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(config_key.to_string()),
        });
    }
    LogicalFilter::And(filters)
}

pub(crate) fn tenant_config_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "id".to_string(),
        "tenant_id".to_string(),
        "config_key".to_string(),
        "config_value".to_string(),
        "type".to_string(),
        "description".to_string(),
    ])
}

pub(crate) fn tenant_config_read(
    tenant_id: &str,
    config_key: Option<&str>,
    limit: u32,
) -> LogicalRead {
    LogicalRead {
        message_type: TENANT_CONFIG_MSG.to_string(),
        filter: Some(tenant_config_filter(tenant_id, config_key)),
        projection: Some(tenant_config_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(limit)),
    }
}

pub(crate) fn tenant_config_record(
    id: String,
    tenant_id: &str,
    req: &tenant_pb::UpdateTenantConfigRequest,
    kind: String,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("id".to_string(), logical_string(id));
    record.insert(
        "tenant_id".to_string(),
        logical_string(tenant_id.to_string()),
    );
    record.insert(
        "config_key".to_string(),
        logical_string(req.config_key.trim().to_string()),
    );
    record.insert(
        "config_value".to_string(),
        logical_string(req.config_value.clone()),
    );
    record.insert("type".to_string(), logical_string(kind));
    record.insert("description".to_string(), logical_string(String::new()));
    record
}
