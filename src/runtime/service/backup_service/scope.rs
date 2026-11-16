//! Which rows a backup or restore moves: one tenant's rows in the tenant-owned
//! tables (`BACKUP_SCOPE_TENANT`, the default), or every row of the active
//! project catalog's shared (global) tables (`BACKUP_SCOPE_SHARED`).
//!
//! A shared backup is the same pipeline as a tenant backup — encrypted JSONL
//! per table, checksummed manifest, object storage, journal row, outbox event —
//! with the shared table set ([`plan_shared_tables`]) instead of the tenant's
//! rows. It is platform-admin only: the caller's verified claim must be a
//! cross-tenant platform admin, and the SHARED movement validator is run with
//! that privilege and no tenant filter, so it fails closed for anyone else
//! (including an in-process caller with no claim).
//!
//! The run is journaled under the verified request tenant (the admin's tenant)
//! with the reserved shared marker as `source_tenant_id`, so `ListBackups` and
//! `GetBackup` show it and [`run_scope_from_source_tenant`] tells it apart.

use tonic::Status;

use crate::proto::udb::core::backup::services::v1 as backup_pb;
use crate::runtime::core::tenant_purge::{TenantPurgePlan, plan_shared_tables, plan_tenant_purge};
use crate::runtime::executor_utils::qi_runtime;

/// Object-key segment of shared backups: `backups/shared/<stamp>-<id>/`.
/// Tenant backups use the tenant id there, which is always a UUID
/// (`parse_uuid`), so the two can never share a prefix. The `*shared*` marker
/// itself is not used in object keys: `*` is not a legal file name character on
/// every object backend (a local-filesystem store on Windows refuses it).
pub(crate) const SHARED_OBJECT_SEGMENT: &str = "shared";

/// The `scope` value written to a shared run's manifest and journal metadata.
pub(crate) const SHARED_SCOPE_TAG: &str = "shared";

/// The resolved scope of one backup or restore request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackupDataScope {
    Tenant,
    Shared,
}

impl BackupDataScope {
    /// Decode the wire enum. `UNSPECIFIED` means tenant; a value this build
    /// does not know is refused naming the field, never guessed.
    pub(crate) fn from_proto(value: i32) -> Result<Self, Status> {
        match backup_pb::BackupScope::try_from(value) {
            Ok(backup_pb::BackupScope::Unspecified | backup_pb::BackupScope::Tenant) => {
                Ok(Self::Tenant)
            }
            Ok(backup_pb::BackupScope::Shared) => Ok(Self::Shared),
            Err(_) => Err(crate::runtime::executor_utils::invalid_argument_fields(
                format!("unknown backup scope {value}"),
                [(
                    "scope",
                    "must be BACKUP_SCOPE_TENANT or BACKUP_SCOPE_SHARED",
                )],
            )),
        }
    }

    pub(crate) fn is_shared(self) -> bool {
        self == Self::Shared
    }

    /// The `scope` value written to a run's manifest and journal metadata.
    pub(crate) fn tag(self) -> &'static str {
        match self {
            Self::Tenant => "tenant",
            Self::Shared => SHARED_SCOPE_TAG,
        }
    }

    /// The journal row's `source_tenant_id` at backup time: empty for a tenant
    /// backup (unchanged), the shared marker for a shared one, which is how
    /// [`run_scope_from_source_tenant`] recognises it.
    pub(crate) fn journal_source_tenant(self) -> &'static str {
        match self {
            Self::Tenant => "",
            Self::Shared => crate::broker::SHARED_TENANT_MARKER,
        }
    }

    /// The table plan this scope moves: the tenant-owned tables, or the shared
    /// tables. Both are children-first; restore walks them in reverse.
    pub(crate) fn plan(self, manifest: &crate::generation::CatalogManifest) -> TenantPurgePlan {
        match self {
            Self::Tenant => plan_tenant_purge(manifest),
            Self::Shared => plan_shared_tables(manifest),
        }
    }
}

/// The scope of a journaled run, from its `source_tenant_id` column: a shared
/// backup or restore records the shared marker there.
pub(crate) fn run_scope_from_source_tenant(source_tenant_id: &str) -> backup_pb::BackupScope {
    if crate::broker::is_shared_tenant_marker(source_tenant_id) {
        backup_pb::BackupScope::Shared
    } else {
        backup_pb::BackupScope::Tenant
    }
}

/// Platform authority for a shared backup or restore, from the verified claim.
/// Returns the `privileged_cross_tenant` flag for the movement validator: true
/// for a platform admin, false for an in-process caller with no claim (the
/// validator then refuses, because a shared move has no tenant filter). A
/// claim that is not a platform admin is refused here with a named reason.
pub(crate) fn shared_scope_privilege(operation: &'static str) -> Result<bool, Status> {
    use crate::runtime::service::method_security::{claim_context_present, current_claim_context};
    if !claim_context_present() {
        return Ok(false);
    }
    shared_scope_privilege_from_claim(&current_claim_context(), operation)
}

/// The decision behind [`shared_scope_privilege`] for one verified claim. Pure.
/// Shared-table backup and restore move platform data, so the narrower
/// `udb:global:write` scope is NOT enough: only a platform admin passes.
pub(crate) fn shared_scope_privilege_from_claim(
    claim: &crate::runtime::service::method_security::VerifiedClaimContext,
    operation: &'static str,
) -> Result<bool, Status> {
    if claim.is_cross_tenant_admin() && !crate::broker::is_shared_tenant_marker(&claim.tenant_id) {
        return Ok(true);
    }
    Err(crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        operation,
        "shared_backup_platform_admin_required",
        "permission denied: backing up or restoring shared tables requires a platform admin",
    ))
}

/// The export read for one shared table: every row, narrowed to the project
/// when the table carries a project column. There is no tenant predicate,
/// because shared rows have no tenant.
pub(crate) fn shared_table_select_sql(relation: &str, project_column: Option<&str>) -> String {
    match project_column {
        Some(project_column) => format!(
            "SELECT row_to_json(t)::text FROM {relation} t WHERE {project_col}::text = $1",
            project_col = qi_runtime(project_column),
        ),
        None => format!("SELECT row_to_json(t)::text FROM {relation} t"),
    }
}

/// The restore freshness probe for one shared table: any row (in the project,
/// when the table carries a project column) means the shared table is live and
/// the restore is refused.
pub(crate) fn shared_table_freshness_sql(relation: &str, project_column: Option<&str>) -> String {
    match project_column {
        Some(project_column) => format!(
            "SELECT 1 FROM {relation} WHERE {project_col}::text = $1 LIMIT 1",
            project_col = qi_runtime(project_column),
        ),
        None => format!("SELECT 1 FROM {relation} LIMIT 1"),
    }
}
