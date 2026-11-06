//! An audited read of one other tenant's rows.
//!
//! A platform operator names the tenant and the reason on the call. Only a verified platform
//! authority holding `udb:cross_tenant:read` may do it, and the record of the read commits to
//! the durable audit log before any row is read: with no record, there is no read. Masking,
//! soft delete and row security apply as they would to that tenant's own read.

use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::runtime::error_reasons::{Refusal, SCOPE_MISSING};
use crate::runtime::executor_utils::invalid_argument_fields;
use crate::runtime::security::SecurityContext;
use crate::runtime::service::auth_service::audit_export::{AuditExportSink, PostgresAuditLogSink};
use crate::runtime::service::method_security::{claim_context_present, current_claim_context};

pub(crate) const TARGET_HEADER: &str = "x-udb-cross-tenant-target";
pub(crate) const REASON_HEADER: &str = "x-udb-cross-tenant-reason";
pub(crate) const CROSS_TENANT_READ_SCOPE: &str = "udb:cross_tenant:read";
const MAX_REASON_CHARS: usize = 500;

pub(crate) struct CrossTenantRead {
    pub(crate) target: String,
    reason: String,
}

fn header(metadata: &MetadataMap, name: &str) -> String {
    metadata
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// The read the call asks for, if it asks for one.
pub(crate) fn requested(metadata: &MetadataMap) -> Result<Option<CrossTenantRead>, Status> {
    let target = header(metadata, TARGET_HEADER);
    let reason = header(metadata, REASON_HEADER);
    if target.is_empty() {
        if reason.is_empty() {
            return Ok(None);
        }
        return Err(invalid_argument_fields(
            format!("{REASON_HEADER} needs {TARGET_HEADER}"),
            [(TARGET_HEADER, "name the tenant to read")],
        ));
    }
    if reason.is_empty() || reason.chars().count() > MAX_REASON_CHARS {
        return Err(invalid_argument_fields(
            format!("a cross-tenant read needs a reason of 1 to {MAX_REASON_CHARS} characters"),
            [(REASON_HEADER, "say why this tenant is being read")],
        ));
    }
    Ok(Some(CrossTenantRead { target, reason }))
}

/// Whether this verified caller may read another tenant. Scope and platform authority come
/// from the claim, never from request metadata.
fn authority_refusal() -> Option<Status> {
    let refused = |message: &str| {
        Some(
            Refusal::new(SCOPE_MISSING, message)
                .missing("scope", CROSS_TENANT_READ_SCOPE)
                .into_status(),
        )
    };
    if !claim_context_present() {
        return refused("a cross-tenant read needs a verified credential");
    }
    let claim = current_claim_context();
    if !claim.is_cross_tenant_admin() {
        return refused("a cross-tenant read needs platform authority");
    }
    if !claim
        .scopes
        .iter()
        .any(|scope| scope.trim().eq_ignore_ascii_case(CROSS_TENANT_READ_SCOPE))
    {
        return refused("a cross-tenant read needs its own scope");
    }
    None
}

/// Check the caller and record the attempt. `Ok` means the record is durable and the read may
/// run. A refused attempt is recorded too, best effort, since the refusal stands either way.
pub(crate) async fn admit(
    pool: Option<sqlx::PgPool>,
    security: &SecurityContext,
    read: &CrossTenantRead,
    message_type: &str,
    decision_id: &str,
) -> Result<(), Status> {
    let refusal = authority_refusal();
    let audit_failure = |detail: &str| {
        tracing::error!(error = %detail, "cross-tenant read refused: its audit record failed");
        crate::runtime::executor_utils::retryable_status(
            "postgres",
            "cross_tenant_read_audit",
            1_000,
            "a cross-tenant read cannot run without its audit record",
        )
    };
    let Some(pool) = pool else {
        return Err(refusal.unwrap_or_else(|| audit_failure("no audit database")));
    };
    let outcome = if refusal.is_some() {
        "denied"
    } else {
        "allowed"
    };
    let envelope = serde_json::json!({
        "event_id": uuid::Uuid::new_v4().to_string(),
        "actor": security.user_id,
        "actor_tenant": security.tenant_id,
        "target_tenant": read.target,
        "target_resource": message_type,
        "operation": "cross_tenant_read",
        "outcome": outcome,
        "reason": read.reason,
        "correlation_id": security.correlation_id,
        "trace_id": security.trace_id,
        "decision_id": decision_id,
    });
    let written = PostgresAuditLogSink::new(pool)
        .export("udb.data.cross_tenant_read", &envelope)
        .await;
    if let Some(refusal) = refusal {
        return Err(refusal);
    }
    written.map_err(|err| audit_failure(&err))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(pairs: &[(&'static str, &str)]) -> MetadataMap {
        let mut map = MetadataMap::new();
        for (key, value) in pairs {
            map.insert(*key, value.parse().unwrap());
        }
        map
    }

    #[test]
    fn a_target_needs_a_reason_and_a_reason_needs_a_target() {
        assert!(requested(&metadata(&[])).unwrap().is_none());
        assert!(requested(&metadata(&[(TARGET_HEADER, "t")])).is_err());
        assert!(requested(&metadata(&[(REASON_HEADER, "why")])).is_err());
        let read = requested(&metadata(&[(TARGET_HEADER, "t"), (REASON_HEADER, "why")]))
            .unwrap()
            .unwrap();
        assert_eq!(read.target, "t");
        let long = "x".repeat(MAX_REASON_CHARS + 1);
        assert!(requested(&metadata(&[(TARGET_HEADER, "t"), (REASON_HEADER, &long)])).is_err());
    }
}
