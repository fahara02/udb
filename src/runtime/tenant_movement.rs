//! Tenant-scope guard for data movement paths.
//!
//! Backup/restore, replication setup, and tenant export/import are not normal
//! request handlers; they can bypass RLS by moving raw rows. This module keeps
//! their tenant-scope contract explicit and unit-tested so future workers call a
//! shared fail-closed validator.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantMovementOperation {
    BackupExport,
    RestoreImport,
    ReplicationPublication,
    TenantPurge,
}

impl TenantMovementOperation {
    fn policy_operation(self) -> &'static str {
        match self {
            Self::BackupExport => "tenant_movement_backup_export",
            Self::RestoreImport => "tenant_movement_restore_import",
            Self::ReplicationPublication => "tenant_movement_replication_publication",
            Self::TenantPurge => "tenant_movement_purge",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantMovementRequest<'a> {
    pub operation: TenantMovementOperation,
    pub tenant_id: &'a str,
    pub target_tenant_id: Option<&'a str>,
    pub tenant_filter_present: bool,
    pub privileged_cross_tenant: bool,
}

// NOTE (Tier-6 #30): `BackupExport` is wired into the broker startup gate in
// `service::serve()` (refuses to serve when a backup DB is configured without a
// tenant scope or an explicit privileged cross-tenant ack). `RestoreImport` and
// `ReplicationPublication` are ready-to-wire primitives: when a native
// restore/import path or an in-process replication-publication provisioner is
// built, call this validator at the TOP of that path before any rows move.
pub fn validate_tenant_movement_scope(request: &TenantMovementRequest<'_>) -> Result<(), String> {
    let tenant = request.tenant_id.trim();
    if tenant.is_empty() && !request.privileged_cross_tenant {
        return Err(format!(
            "{:?} requires a tenant scope or an explicit privileged cross-tenant approval",
            request.operation
        ));
    }
    if !request.tenant_filter_present && !request.privileged_cross_tenant {
        return Err(format!(
            "{:?} requires a tenant filter before data movement starts",
            request.operation
        ));
    }
    if let Some(target) = request.target_tenant_id {
        let target = target.trim();
        if !target.is_empty() && target != tenant && !request.privileged_cross_tenant {
            return Err(format!(
                "{:?} cannot move tenant '{}' data into tenant '{}' without privileged cross-tenant approval",
                request.operation, tenant, target
            ));
        }
    }
    Ok(())
}

pub fn tenant_movement_policy_status(
    operation: TenantMovementOperation,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        operation.policy_operation(),
        "tenant_movement_scope_required",
        message,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    #[test]
    fn backup_export_requires_tenant_filter() {
        let err = validate_tenant_movement_scope(&TenantMovementRequest {
            operation: TenantMovementOperation::BackupExport,
            tenant_id: "tenant-a",
            target_tenant_id: None,
            tenant_filter_present: false,
            privileged_cross_tenant: false,
        })
        .expect_err("tenant export without filter must fail closed");
        assert!(err.contains("tenant filter"));
    }

    #[test]
    fn restore_blocks_cross_tenant_target_by_default() {
        let err = validate_tenant_movement_scope(&TenantMovementRequest {
            operation: TenantMovementOperation::RestoreImport,
            tenant_id: "tenant-a",
            target_tenant_id: Some("tenant-b"),
            tenant_filter_present: true,
            privileged_cross_tenant: false,
        })
        .expect_err("cross-tenant restore must need explicit approval");
        assert!(err.contains("cannot move tenant"));
    }

    #[test]
    fn replication_publication_requires_known_scope() {
        let err = validate_tenant_movement_scope(&TenantMovementRequest {
            operation: TenantMovementOperation::ReplicationPublication,
            tenant_id: "",
            target_tenant_id: None,
            tenant_filter_present: false,
            privileged_cross_tenant: false,
        })
        .expect_err("unscoped replication must fail closed");
        assert!(err.contains("requires a tenant scope"));
    }

    #[test]
    fn tenant_purge_requires_tenant_filter() {
        let err = validate_tenant_movement_scope(&TenantMovementRequest {
            operation: TenantMovementOperation::TenantPurge,
            tenant_id: "tenant-a",
            target_tenant_id: None,
            tenant_filter_present: false,
            privileged_cross_tenant: false,
        })
        .expect_err("tenant purge without a tenant filter must fail closed");
        assert!(err.contains("tenant filter"));
    }

    #[test]
    fn privileged_cross_tenant_movement_is_explicit() {
        validate_tenant_movement_scope(&TenantMovementRequest {
            operation: TenantMovementOperation::RestoreImport,
            tenant_id: "tenant-a",
            target_tenant_id: Some("tenant-b"),
            tenant_filter_present: false,
            privileged_cross_tenant: true,
        })
        .expect("privileged cross-tenant movement should be explicit and allowed");
    }

    #[test]
    fn tenant_movement_scope_status_carries_policy_detail() {
        let err = validate_tenant_movement_scope(&TenantMovementRequest {
            operation: TenantMovementOperation::RestoreImport,
            tenant_id: "tenant-a",
            target_tenant_id: Some("tenant-b"),
            tenant_filter_present: true,
            privileged_cross_tenant: false,
        })
        .expect_err("cross-tenant restore must need explicit approval");
        let status = tenant_movement_policy_status(TenantMovementOperation::RestoreImport, err);
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert!(
            status
                .message()
                .contains("cannot move tenant 'tenant-a' data into tenant 'tenant-b'")
        );
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, "tenant_movement_restore_import");
        assert_eq!(detail.policy_decision_id, "tenant_movement_scope_required");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }
}
