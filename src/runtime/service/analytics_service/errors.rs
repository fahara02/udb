//! Typed error/status constructors, request-field validation, and the
//! platform-admin guards for the native `AnalyticsService`. Extracted verbatim
//! from the former god file.

use tonic::Status;

pub(crate) fn analytics_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "analytics",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn analytics_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("analytics", operation, message)
}

pub(crate) fn analytics_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

/// Pure admin gate for the two system-global reads. `executor_performance_summaries`
/// and `reconciliation_analytics_summaries` have NO tenant column (system-global
/// operational tables), so a WHERE predicate cannot scope them — only a genuine
/// cross-tenant / platform-admin identity may read them. Reuses the ONE existing
/// admin classifier ([`VerifiedClaimContext::is_cross_tenant_admin`], the same set
/// the apikey lane and the native RLS `app.platform_admin` escape hatch honor).
pub(crate) fn platform_admin_guard(
    claim: &crate::runtime::service::method_security::VerifiedClaimContext,
    operation: &'static str,
) -> Result<(), Status> {
    if claim.is_cross_tenant_admin() {
        Ok(())
    } else {
        Err(crate::runtime::executor_utils::policy_status_with_code(
            tonic::Code::PermissionDenied,
            operation,
            "analytics_platform_admin_required",
            "system-global analytics require a platform/cross-tenant admin identity",
        ))
    }
}

/// Enforce [`platform_admin_guard`] against the request's verified claim. Mirrors
/// the D1/D2 handler doctrine in `method_security`: enforcement is skipped ONLY
/// when no claim context is installed at all (in-process / trusted internal
/// caller — the tower gate ALWAYS installs one for transport requests), so an
/// over-the-wire caller can never dodge the gate.
pub(crate) fn require_platform_admin(operation: &'static str) -> Result<(), Status> {
    if !crate::runtime::service::method_security::claim_context_present() {
        return Ok(());
    }
    let claim = crate::runtime::service::method_security::current_claim_context();
    platform_admin_guard(&claim, operation)
}
