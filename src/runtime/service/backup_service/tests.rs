//! Unit guards for the native `BackupService`: the SHARED purge-planner backup
//! plan (excluded-not-skipped), the fresh-target restore guard, the typed
//! policy/schema/internal/capability details, and the request-body validators
//! (cross-tenant rejection, movement-scope, confirmation token, field
//! violations). Copied verbatim from the former god file; imports are explicit
//! (no `use super::*`).

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::proto::udb::core::backup::services::v1 as backup_pb;
use crate::proto::udb::core::backup::services::v1::backup_service_server::BackupService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::core::tenant_purge::plan_tenant_purge;
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
use crate::runtime::service::method_security;

use super::BackupServiceImpl;
use super::errors::{
    backup_capability_status, backup_internal_status, backup_not_found_status,
    backup_run_missing_object_prefix_status, ensure_target_is_fresh,
};
use super::import::is_restore_journal_relation;
use super::model::run_location_from_json;

fn decode_detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("error-detail trailer present")
        .to_bytes()
        .expect("trailer decodes to bytes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

fn assert_policy_detail(status: &Status, operation: &str, policy_decision_id: &str) {
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.policy_decision_id, policy_decision_id);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_schema_not_found_detail(
    status: &Status,
    operation: &str,
    schema_code: &str,
    message: &str,
) {
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Schema as i32);
    assert_eq!(detail.backend, "backup");
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.capability_required, schema_code);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Internal as i32);
    assert_eq!(detail.backend, "backup");
    assert_eq!(detail.operation, operation);
    assert!(detail.capability_required.is_empty());
    assert!(detail.policy_decision_id.is_empty());
    assert!(detail.field_violations.is_empty());
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn column(name: &str) -> ManifestColumn {
    ManifestColumn {
        field_name: name.to_string(),
        column_name: name.to_string(),
        ..ManifestColumn::default()
    }
}

fn table(schema: &str, name: &str, columns: Vec<ManifestColumn>) -> ManifestTable {
    ManifestTable {
        schema: schema.to_string(),
        table: name.to_string(),
        columns,
        ..ManifestTable::default()
    }
}

/// The backup PLAN must reuse the SHARED purge planner: tenant-columned
/// tables become targets, and a tenant-less table is REPORTED as excluded
/// with a human reason — never silently skipped (the capability-lie pattern).
#[test]
fn backup_plan_reports_excluded_not_skipped() {
    let mut owned = table("app", "invoice", vec![column("id"), column("tenant_id")]);
    owned.table_security.tenant_column = "tenant_id".to_string();
    let tenantless = table("app", "currency_rate", vec![column("id"), column("rate")]);
    let manifest = CatalogManifest {
        tables: vec![owned, tenantless],
        ..CatalogManifest::default()
    };

    let plan = plan_tenant_purge(&manifest);
    let targets: Vec<&str> = plan.targets.iter().map(|t| t.table.as_str()).collect();
    assert!(
        targets.contains(&"invoice"),
        "tenant table is a backup target"
    );
    assert!(
        !targets.contains(&"currency_rate"),
        "a tenant-less table is never backed up as tenant data"
    );
    assert_eq!(plan.excluded.len(), 1, "the tenant-less table is reported");
    assert_eq!(plan.excluded[0].table, "currency_rate");
    assert!(
        !plan.excluded[0].reason.trim().is_empty(),
        "excluded tables must carry a human reason, not be silently dropped"
    );
}

/// Restoring over a live tenant fails closed; a fresh (empty) target passes.
/// Pure check — no Postgres.
#[test]
fn restore_over_existing_tenant_is_rejected() {
    let err = ensure_target_is_fresh(3).expect_err("a non-empty target must be refused");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "restore target tenant already holds 3 row(s); restoring over a live tenant is refused — use a fresh tenant id"
    );
    assert_policy_detail(&err, "restore_tenant", "restore_target_not_fresh");
    ensure_target_is_fresh(0).expect("a fresh target must be allowed");
}

#[test]
fn restore_freshness_identifies_only_the_descriptor_backed_run_journal() {
    let run =
        crate::runtime::native_catalog::native_model(super::config::BACKUP_RUN_MSG, &["backup_id"]);
    let unquoted = run.relation.replace('"', "");
    let (schema, table) = unquoted
        .split_once('.')
        .expect("BackupRun relation must be schema-qualified");
    assert!(is_restore_journal_relation(schema, table));
    assert!(!is_restore_journal_relation(schema, "backup_policies"));
    assert!(!is_restore_journal_relation("customer", table));
}

#[test]
fn backup_run_missing_object_prefix_carries_policy_detail() {
    let err = backup_run_missing_object_prefix_status();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "backup run has no object prefix to restore from"
    );
    assert_policy_detail(&err, "restore_tenant", "backup_run_missing_object_prefix");
}

#[test]
fn run_location_requires_complete_immutable_topology() {
    let row = serde_json::json!({
        "metadata_json": {
            "object_backend": "minio",
            "object_bucket": "tenant-backups",
            "manifest_key": "backups/t/run/manifest.json",
            "project_id": "billing",
            "catalog_checksum": "catalog-sha",
            "postgres_instance": "billing-primary"
        }
    });
    let location = run_location_from_json(&row).expect("complete location must decode");
    assert_eq!(location.object_backend, "minio");
    assert_eq!(location.project_id, "billing");
    assert_eq!(location.postgres_instance, "billing-primary");

    let missing_instance = serde_json::json!({
        "metadata_json": {
            "object_backend": "minio",
            "object_bucket": "tenant-backups",
            "manifest_key": "backups/t/run/manifest.json",
            "project_id": "billing",
            "catalog_checksum": "catalog-sha"
        }
    });
    assert!(
        run_location_from_json(&missing_instance).is_none(),
        "partial location must never trigger default-instance guessing"
    );
}

#[test]
fn backup_not_found_statuses_carry_schema_detail() {
    for (operation, schema_code, message) in [
        (
            "restore_tenant",
            "backup_run_not_found",
            "backup run not found for source tenant",
        ),
        ("get_backup", "backup_run_not_found", "backup run not found"),
        (
            "get_backup_policy",
            "backup_policy_not_found",
            "backup policy not found",
        ),
    ] {
        assert_schema_not_found_detail(
            &backup_not_found_status(operation, schema_code, message),
            operation,
            schema_code,
            message,
        );
    }
}

#[test]
fn backup_internal_status_carries_typed_detail() {
    assert_internal_detail(
        &backup_internal_status(
            "restore_manifest_parse",
            "restore manifest parse failed: malformed manifest",
        ),
        "restore_manifest_parse",
        "restore manifest parse failed: malformed manifest",
    );
}

/// A caller scoped to tenant-a must not back up tenant-b by putting a foreign
/// tenant_id in the request BODY; the scope guard rejects it before any store
/// access (no Postgres needed). Mirrors `lock_service`.
#[tokio::test]
async fn start_backup_rejects_cross_tenant_body() {
    let svc = BackupServiceImpl::new(); // no runtime, no channels (admit no-op)
    let mut request = Request::new(backup_pb::StartTenantBackupRequest {
        tenant_id: "tenant-b".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .start_tenant_backup(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

/// A cross-tenant restore body is rejected by the shared movement validator
/// (target differs from the verified claim, and from the source, without a
/// privileged ack). No Postgres needed.
#[tokio::test]
async fn restore_rejects_cross_tenant_body() {
    let svc = BackupServiceImpl::new();
    let mut request = Request::new(backup_pb::RestoreTenantRequest {
        source_tenant_id: "tenant-a".to_string(),
        target_tenant_id: "tenant-b".to_string(),
        backup_id: "11111111-1111-1111-1111-111111111111".to_string(),
        confirmation_token: "yes".to_string(),
        allow_cross_tenant: false,
        ..Default::default()
    });
    // Claim is tenant-a but the body targets tenant-b: the cross-tenant body
    // guard rejects it first.
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .restore_tenant(request)
        .await
        .expect_err("cross-tenant restore body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

/// C1: a cross-tenant restore is authorized by the caller's VALIDATED CLAIM, not
/// the wire `allow_cross_tenant` bool. A tenant-scoped (non-admin) caller that
/// flips the bool is DENIED fail-closed before any store access.
#[tokio::test]
async fn restore_cross_tenant_requires_admin_claim() {
    let svc = BackupServiceImpl::new();
    let request = Request::new(backup_pb::RestoreTenantRequest {
        source_tenant_id: "11111111-1111-1111-1111-111111111111".to_string(),
        target_tenant_id: "22222222-2222-2222-2222-222222222222".to_string(),
        backup_id: "33333333-3333-3333-3333-333333333333".to_string(),
        confirmation_token: "yes".to_string(),
        allow_cross_tenant: true, // wire bool must NOT self-authorize the move
        ..Default::default()
    });
    // Claim is scoped to the source tenant with only a plain write scope — not a
    // cross-tenant admin.
    let claim = method_security::test_claim_context(
        "user-1",
        "11111111-1111-1111-1111-111111111111",
        "",
        &["udb:native:write"],
        &[],
    );
    let err = method_security::scope_claim_context_for_test(claim, async move {
        svc.restore_tenant(request)
            .await
            .expect_err("non-admin cross-tenant restore must be denied")
    })
    .await;
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert_policy_detail(
        &err,
        "restore_tenant",
        "restore_cross_tenant_admin_required",
    );
}

/// C1: a genuine cross-tenant admin passes the authorization gate (no
/// PermissionDenied) and only then hits the fail-closed capability guard for the
/// unconfigured runtime — proving the CLAIM, not the wire bool, gates the move.
#[tokio::test]
async fn restore_cross_tenant_admin_passes_auth_gate() {
    let svc = BackupServiceImpl::new(); // no runtime → capability failure AFTER auth
    let request = Request::new(backup_pb::RestoreTenantRequest {
        source_tenant_id: "11111111-1111-1111-1111-111111111111".to_string(),
        target_tenant_id: "22222222-2222-2222-2222-222222222222".to_string(),
        backup_id: "33333333-3333-3333-3333-333333333333".to_string(),
        confirmation_token: "yes".to_string(),
        allow_cross_tenant: true,
        ..Default::default()
    });
    let claim = method_security::test_claim_context(
        "admin-1",
        "11111111-1111-1111-1111-111111111111",
        "",
        &[],
        &["platform_admin"],
    );
    let err = method_security::scope_claim_context_for_test(claim, async move {
        svc.restore_tenant(request)
            .await
            .expect_err("no runtime configured must fail after the auth gate")
    })
    .await;
    // The auth gate PASSED (not PermissionDenied); the capability guard fails closed.
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn restore_cross_tenant_movement_carries_policy_detail() {
    let svc = BackupServiceImpl::new();
    let mut request = Request::new(backup_pb::RestoreTenantRequest {
        source_tenant_id: "tenant-a".to_string(),
        target_tenant_id: "tenant-b".to_string(),
        backup_id: "11111111-1111-1111-1111-111111111111".to_string(),
        confirmation_token: "yes".to_string(),
        allow_cross_tenant: false,
        ..Default::default()
    });
    // The claim matches the TARGET tenant, so the body guard passes and the
    // shared movement validator owns the cross-tenant source->target refusal.
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-b"));
    let err = svc
        .restore_tenant(request)
        .await
        .expect_err("cross-tenant movement must need explicit approval");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert!(
        err.message()
            .contains("cannot move tenant 'tenant-a' data into tenant 'tenant-b'")
    );
    assert_policy_detail(
        &err,
        "tenant_movement_restore_import",
        "tenant_movement_scope_required",
    );
}

/// RestoreTenant is DESTRUCTIVE: an empty confirmation token fails closed
/// before any store access (claim matches the target so the body guard
/// passes). No Postgres needed.
#[tokio::test]
async fn restore_requires_confirmation_token() {
    let svc = BackupServiceImpl::new();
    let mut request = Request::new(backup_pb::RestoreTenantRequest {
        source_tenant_id: "tenant-a".to_string(),
        target_tenant_id: "tenant-a".to_string(),
        backup_id: "11111111-1111-1111-1111-111111111111".to_string(),
        confirmation_token: String::new(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .restore_tenant(request)
        .await
        .expect_err("missing confirmation token must fail closed");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "RestoreTenant overwrites a tenant's data; confirmation_token is required"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "confirmation_token");
    assert_eq!(
        detail.field_violations[0].description,
        "must be present to restore over tenant data"
    );
}

#[tokio::test]
async fn start_backup_missing_tenant_id_carries_field_violation() {
    let svc = BackupServiceImpl::new();
    let err = svc
        .start_tenant_backup(Request::new(backup_pb::StartTenantBackupRequest {
            tenant_id: "  ".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("missing tenant_id must fail before runtime access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "tenant_id is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "tenant_id");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty tenant id"
    );
}

#[tokio::test]
async fn restore_tenant_missing_identity_carries_field_violations() {
    let svc = BackupServiceImpl::new();
    let err = svc
        .restore_tenant(Request::new(backup_pb::RestoreTenantRequest {
            source_tenant_id: " ".to_string(),
            target_tenant_id: " ".to_string(),
            backup_id: " ".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("missing restore identity must fail before tenant guard");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "source_tenant_id, target_tenant_id and backup_id are required"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 3);
    assert_eq!(detail.field_violations[0].field, "source_tenant_id");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty source tenant id"
    );
    assert_eq!(detail.field_violations[1].field, "target_tenant_id");
    assert_eq!(
        detail.field_violations[1].description,
        "must be a non-empty target tenant id"
    );
    assert_eq!(detail.field_violations[2].field, "backup_id");
    assert_eq!(
        detail.field_violations[2].description,
        "must be a non-empty backup id"
    );
}

#[tokio::test]
async fn get_backup_missing_backup_id_carries_field_violation() {
    let svc = BackupServiceImpl::new(); // no runtime, no channels (admit no-op)
    let mut request = Request::new(backup_pb::GetBackupRequest {
        tenant_id: "tenant-a".to_string(),
        backup_id: "  ".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .get_backup(request)
        .await
        .expect_err("missing backup_id must be rejected before runtime access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "backup_id is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "backup_id");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty backup id"
    );
}

#[tokio::test]
async fn get_backup_policy_missing_policy_name_carries_field_violation() {
    let svc = BackupServiceImpl::new(); // no runtime, no channels (admit no-op)
    let mut request = Request::new(backup_pb::GetBackupPolicyRequest {
        tenant_id: "tenant-a".to_string(),
        policy_name: "  ".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .get_backup_policy(request)
        .await
        .expect_err("missing policy_name must be rejected before runtime access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "policy_name is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "policy_name");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty policy name"
    );
}

#[test]
fn backup_missing_setup_capabilities_carry_typed_detail() {
    for (operation, capability, message) in [
        (
            "native_entity_dispatch",
            "runtime_native_entity_dispatch",
            "backup service requires runtime native-entity dispatch (no runtime configured)",
        ),
        (
            "postgres_store",
            "postgres_store",
            "backup service requires a Postgres-backed store (no PG pool configured)",
        ),
        (
            "tenant_table_enumeration",
            "catalog_manifest",
            "backup service requires the catalog manifest to enumerate tenant tables",
        ),
    ] {
        let err = backup_capability_status(operation, capability, message);
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(err.message(), message);
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "backup");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability);
        assert!(!detail.retryable);
    }
}
