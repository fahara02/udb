//! Unit guards for the native `BackupService`: the SHARED purge-planner backup
//! plan (excluded-not-skipped), the fresh-target restore guard, the typed
//! policy/schema/internal/capability details, and the request-body validators
//! (cross-tenant rejection, movement-scope, confirmation token, field
//! violations). Copied verbatim from the former god file; imports are explicit
//! (no `use super::*`).

use std::sync::Arc;

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::ir::{ComparisonOp, LogicalFilter, LogicalValue};
use crate::proto::udb::core::backup::services::v1 as backup_pb;
use crate::proto::udb::core::backup::services::v1::backup_service_server::BackupService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::catalog::CatalogManager;
use crate::runtime::core::tenant_purge::plan_tenant_purge;
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
use crate::runtime::service::method_security;

use super::BackupServiceImpl;
use super::errors::{
    backup_capability_status, backup_internal_status, backup_not_found_status,
    backup_run_missing_object_prefix_status, ensure_target_is_fresh,
};
use super::export::backup_table_select_sql;
use super::import::{is_restore_journal_relation, scope_restore_row_project};
use super::model::run_location_from_json;
use super::store::{
    policies_list_read, policy_conflict, policy_read_by_name, run_read_by_id, runs_list_read,
};

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

#[tokio::test]
async fn backup_project_binding_refuses_default_fallback_and_accepts_exact_activation() {
    let catalog = Arc::new(CatalogManager::new(CatalogManifest::default()));
    let svc = BackupServiceImpl::new().with_catalog(Some(Arc::clone(&catalog)));

    let err = svc
        .require_active_project("customer-project")
        .expect_err("missing project must not inherit the default catalog");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_policy_detail(
        &err,
        "resolve_project_topology",
        "backup_project_catalog_not_active",
    );

    catalog
        .stage_catalog(
            CatalogManifest::default(),
            "customer-project".to_string(),
            "1.0.0".to_string(),
            "backward".to_string(),
        )
        .await
        .expect("stage exact customer project");
    catalog
        .activate_catalog_for("customer-project", "1.0.0")
        .await
        .expect("activate exact customer project");

    assert_eq!(
        svc.require_active_project("customer-project")
            .expect("exact active customer project is accepted"),
        "customer-project"
    );
}

fn filter_has_eq(filter: &LogicalFilter, field: &str, value: &str) -> bool {
    match filter {
        LogicalFilter::Comparison {
            field: actual_field,
            op: ComparisonOp::Eq,
            value: LogicalValue::String(actual_value),
        } => actual_field == field && actual_value == value,
        LogicalFilter::And(filters) => filters
            .iter()
            .any(|nested| filter_has_eq(nested, field, value)),
        _ => false,
    }
}

#[test]
fn backup_entities_declare_project_security_and_unique_authority() {
    for (message, unique_columns) in [
        (
            super::config::BACKUP_RUN_MSG,
            ["tenant_id", "project_id", "backup_id"],
        ),
        (
            super::config::BACKUP_POLICY_MSG,
            ["tenant_id", "project_id", "policy_name"],
        ),
    ] {
        let model =
            crate::runtime::native_catalog::native_model(message, &["tenant_id", "project_id"]);
        assert_eq!(model.tenant_column.as_deref(), Some("tenant_id"));
        assert_eq!(model.project_column.as_deref(), Some("project_id"));
        assert_eq!(model.table_security.project_isolation_mode, "project");
        assert_eq!(model.table_security.project_column, "project_id");
        assert!(
            model
                .table_security
                .rls_policy_template
                .contains("app.current_project_id"),
            "{message} RLS must bind the request project"
        );

        let manifest = crate::runtime::native_catalog::native_service_manifest()
            .expect("embedded native manifest");
        let table = manifest
            .tables
            .iter()
            .find(|table| table.message_fqn() == message)
            .expect("backup native table");
        let project = table
            .columns
            .iter()
            .find(|column| column.field_name == "project_id")
            .expect("first-class project column");
        assert!(project.not_null);
        assert!(project.is_project_column);
        assert!(table.indexes.iter().any(|index| {
            index.unique
                && index.columns
                    == unique_columns
                        .iter()
                        .map(|value| value.to_string())
                        .collect::<Vec<_>>()
        }));
    }
}

#[test]
fn backup_store_reads_and_conflicts_bind_tenant_plus_project() {
    let reads = [
        run_read_by_id("tenant-a", "project-a", "run-a"),
        runs_list_read("tenant-a", "project-a", None, 10, 0),
        policy_read_by_name("tenant-a", "project-a", "daily"),
        policies_list_read("tenant-a", "project-a", 10, 0),
    ];
    for read in reads {
        let filter = read.filter.expect("project-scoped read filter");
        assert!(filter_has_eq(&filter, "tenant_id", "tenant-a"));
        assert!(filter_has_eq(&filter, "project_id", "project-a"));
    }
    assert_eq!(
        policy_conflict().conflict_target(),
        Some(
            ["tenant_id", "project_id", "policy_name"]
                .map(str::to_string)
                .as_slice()
        )
    );
}

#[test]
fn raw_backup_sql_and_restore_rows_preserve_project_scope() {
    let sql = backup_table_select_sql("\"app\".\"orders\"", "tenant_id", Some("project_id"));
    assert!(sql.contains("\"tenant_id\"::text = $1"));
    assert!(sql.contains("\"project_id\"::text = $2"));

    let tenant_only = backup_table_select_sql("\"app\".\"profile\"", "tenant_id", None);
    assert!(tenant_only.contains("\"tenant_id\"::text = $1"));
    assert!(!tenant_only.contains("$2"));

    let mut matching = serde_json::json!({"tenant_id": "tenant-a", "project_id": "project-a"})
        .as_object()
        .expect("object row")
        .clone();
    scope_restore_row_project(&mut matching, "project_id", "project-a", "app", "orders")
        .expect("matching project row");
    assert_eq!(matching["project_id"], "project-a");

    let mut foreign = serde_json::json!({"tenant_id": "tenant-a", "project_id": "project-b"})
        .as_object()
        .expect("object row")
        .clone();
    let err = scope_restore_row_project(&mut foreign, "project_id", "project-a", "app", "orders")
        .expect_err("foreign project row must be rejected");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
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
        "project_id": "billing",
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
        "project_id": "billing",
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

    let legacy_without_first_class_project = serde_json::json!({
        "metadata_json": row["metadata_json"].clone()
    });
    assert!(
        run_location_from_json(&legacy_without_first_class_project).is_none(),
        "legacy rows must remain quarantined rather than inherit a default project"
    );

    let mismatched_project = serde_json::json!({
        "project_id": "orders",
        "metadata_json": row["metadata_json"].clone()
    });
    assert!(
        run_location_from_json(&mismatched_project).is_none(),
        "first-class and immutable metadata project authority must agree"
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

/// The leader backup-maintenance single passes against Postgres + MinIO: the
/// scheduled-backup pass fires a due policy through the real backup routine,
/// and the retention pass prunes the run that exceeds `max_retained_backups`.
#[tokio::test]
#[ignore = "requires live Postgres + MinIO; run with cargo test --lib live_backup_scheduled_and_retention_passes -- --ignored --nocapture"]
async fn live_backup_scheduled_and_retention_passes() {
    use super::retention::{run_backup_retention_once, run_scheduled_backups_once};
    use crate::proto::udb::core::backup::services::v1 as backup_pb;
    use crate::proto::udb::core::backup::services::v1::backup_service_server::BackupService;
    use crate::runtime::service::live_tests::support::{
        live_native_service_db_lock, live_pg_dsn, live_pg_pool, migrate_native_service_db,
    };

    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;

    let tenant_id = uuid::Uuid::new_v4().to_string();
    let project_id = uuid::Uuid::new_v4().to_string();
    let mut config = crate::runtime::config::UdbConfig::from_env();
    config.primary.direct_dsn = live_pg_dsn();
    let broker = crate::runtime::service::DataBrokerService::with_runtime(
        crate::runtime::native_catalog::native_manifest().clone(),
        crate::runtime::DataBrokerRuntime::from_config(config).await,
    );
    let checksum = broker
        .catalog
        .stage_catalog(
            broker.manifest.clone(),
            project_id.clone(),
            "backup-live-maintenance".to_string(),
            "exact".to_string(),
        )
        .await
        .expect("stage backup catalog");
    broker
        .catalog
        .activate_catalog_for(&project_id, &checksum)
        .await
        .expect("activate backup catalog");
    let svc = broker.build_backup_service();

    fn scoped<T>(message: T, tenant_id: &str, project_id: &str) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request.metadata_mut().insert(
            "x-tenant-id",
            tenant_id.parse().expect("valid tenant metadata"),
        );
        request.metadata_mut().insert(
            "x-udb-project-id",
            project_id.parse().expect("valid project metadata"),
        );
        request
    }
    let request =
        |message: backup_pb::PutBackupPolicyRequest| scoped(message, &tenant_id, &project_id);
    let start_request =
        |message: backup_pb::StartTenantBackupRequest| scoped(message, &tenant_id, &project_id);
    let list_request =
        |message: backup_pb::ListBackupsRequest| scoped(message, &tenant_id, &project_id);
    svc.put_backup_policy(request(backup_pb::PutBackupPolicyRequest {
        tenant_id: tenant_id.clone(),
        policy_name: "every-minute".to_string(),
        schedule_cron: "* * * * *".to_string(),
        retention_days: 0,
        max_retained_backups: 1,
        enabled: true,
        object_backend: "minio".to_string(),
        object_bucket: "udb-storage".to_string(),
        metadata_json: "{}".to_string(),
    }))
    .await
    .expect("put backup policy");

    let fired = run_scheduled_backups_once(
        &svc,
        crate::runtime::service::scheduler_service::cron::next_cron_after,
    )
    .await
    .expect("scheduled backup pass");
    assert_eq!(fired, 1, "the never-backed-up due policy fires once");

    // A second, operator-triggered run puts the tenant over max_retained_backups.
    svc.start_tenant_backup(start_request(backup_pb::StartTenantBackupRequest {
        tenant_id: tenant_id.clone(),
        object_backend: "minio".to_string(),
        object_bucket: "udb-storage".to_string(),
        ..Default::default()
    }))
    .await
    .expect("manual backup run");

    let pruned = run_backup_retention_once(&svc)
        .await
        .expect("retention pass");
    assert_eq!(pruned, 1, "the run beyond max_retained_backups is pruned");
    let remaining = svc
        .list_backups(list_request(backup_pb::ListBackupsRequest {
            tenant_id: tenant_id.clone(),
            kind: "BACKUP".to_string(),
            ..Default::default()
        }))
        .await
        .expect("list backups")
        .into_inner();
    let backups = remaining.backups.len();
    assert_eq!(backups, 1, "exactly the newest backup run is retained");
}

// ── Shared-table (platform) backup scope ────────────────────────────────────

fn shared_table(schema: &str, name: &str, columns: Vec<ManifestColumn>) -> ManifestTable {
    let mut table = table(schema, name, columns);
    table.table_security.tenant_isolation_mode = "global".to_string();
    table
}

/// The shared plan targets exactly the shared tables (isolation declared
/// off, no tenant column), parents last so a restore inserts them first, with
/// no tenant column; every other table is REPORTED with its reason.
#[test]
fn shared_backup_plan_selects_only_shared_tables() {
    use crate::runtime::core::tenant_purge::plan_shared_tables;

    let currency = shared_table("ref", "currency", vec![column("code"), column("name")]);
    let mut rate = shared_table("ref", "fx_rate", vec![column("id"), column("code")]);
    rate.foreign_keys
        .push(crate::generation::ManifestForeignKey {
            name: "fx_rate_currency_fk".to_string(),
            columns: vec!["code".to_string()],
            ref_schema: "ref".to_string(),
            ref_table: "currency".to_string(),
            ref_columns: vec!["code".to_string()],
            ..crate::generation::ManifestForeignKey::default()
        });
    let mut owned = table("app", "invoice", vec![column("id"), column("tenant_id")]);
    owned.table_security.tenant_column = "tenant_id".to_string();
    // Tenant-less but isolation NOT declared off: not a shared table.
    let undeclared = table("app", "scratch", vec![column("id")]);
    // Declared global but still carries a tenant column: not shared either.
    let mut global_with_tenant =
        shared_table("app", "mixed", vec![column("id"), column("tenant_id")]);
    global_with_tenant.table_security.tenant_column = "tenant_id".to_string();

    let manifest = CatalogManifest {
        tables: vec![currency, rate, owned, undeclared, global_with_tenant],
        ..CatalogManifest::default()
    };
    let plan = plan_shared_tables(&manifest);
    let order: Vec<&str> = plan.targets.iter().map(|t| t.table.as_str()).collect();
    assert_eq!(order, vec!["fx_rate", "currency"], "children first");
    assert!(plan.fk_ordered);
    assert!(
        plan.targets.iter().all(|t| t.tenant_column.is_empty()),
        "shared rows carry no tenant column"
    );
    let excluded: std::collections::HashMap<&str, &str> = plan
        .excluded
        .iter()
        .map(|e| (e.table.as_str(), e.reason.as_str()))
        .collect();
    assert_eq!(excluded.len(), 3, "every non-shared table is reported");
    assert!(excluded["invoice"].contains("per-tenant backup"));
    assert!(excluded["scratch"].contains("not a shared table"));
    assert!(excluded.contains_key("mixed"));

    // The tenant plan is unchanged: shared tables stay excluded from it.
    let tenant_plan = super::scope::BackupDataScope::Tenant.plan(&manifest);
    assert!(
        tenant_plan
            .targets
            .iter()
            .all(|t| t.table != "currency" && t.table != "fx_rate")
    );
    assert_eq!(
        super::scope::BackupDataScope::Shared
            .plan(&manifest)
            .targets,
        plan.targets
    );
}

#[test]
fn backup_scope_decodes_and_refuses_unknown_values() {
    use super::scope::BackupDataScope;
    for (value, expected) in [
        (
            backup_pb::BackupScope::Unspecified as i32,
            BackupDataScope::Tenant,
        ),
        (
            backup_pb::BackupScope::Tenant as i32,
            BackupDataScope::Tenant,
        ),
        (
            backup_pb::BackupScope::Shared as i32,
            BackupDataScope::Shared,
        ),
    ] {
        assert_eq!(BackupDataScope::from_proto(value).expect("known"), expected);
    }
    let err = BackupDataScope::from_proto(99).expect_err("unknown scope is refused");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(decode_detail(&err).field_violations[0].field, "scope");

    assert_eq!(BackupDataScope::Tenant.journal_source_tenant(), "");
    assert_eq!(
        BackupDataScope::Shared.journal_source_tenant(),
        crate::broker::SHARED_TENANT_MARKER
    );
    assert_eq!(
        super::scope::run_scope_from_source_tenant(crate::broker::SHARED_TENANT_MARKER),
        backup_pb::BackupScope::Shared
    );
    for source in ["", "11111111-1111-1111-1111-111111111111"] {
        assert_eq!(
            super::scope::run_scope_from_source_tenant(source),
            backup_pb::BackupScope::Tenant
        );
    }
}

/// Shared backup/restore is platform-admin only; `udb:global:write` (enough to
/// write shared rows through the data plane) is not enough to move them all.
#[test]
fn shared_backup_needs_a_platform_admin() {
    use super::scope::shared_scope_privilege_from_claim;
    for (scopes, roles) in [
        (&["udb:platform_admin"][..], &[][..]),
        (&[][..], &["platform_admin"][..]),
    ] {
        let claim = method_security::test_claim_context("admin", "", "", scopes, roles);
        assert!(shared_scope_privilege_from_claim(&claim, "start_tenant_backup").expect("admin"));
    }
    for scopes in [
        &[crate::broker::GLOBAL_WRITE_SCOPE][..],
        &["udb:backup:start-tenant-backup"][..],
    ] {
        let claim = method_security::test_claim_context(
            "user",
            "11111111-1111-1111-1111-111111111111",
            "",
            scopes,
            &[],
        );
        let err = shared_scope_privilege_from_claim(&claim, "start_tenant_backup")
            .expect_err("not a platform admin");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_policy_detail(
            &err,
            "start_tenant_backup",
            "shared_backup_platform_admin_required",
        );
    }
}

#[test]
fn shared_table_sql_has_no_tenant_predicate() {
    use super::scope::{shared_table_freshness_sql, shared_table_select_sql};
    let all = shared_table_select_sql("\"ref\".\"currency\"", None);
    assert_eq!(
        all,
        "SELECT row_to_json(t)::text FROM \"ref\".\"currency\" t"
    );
    let scoped = shared_table_select_sql("\"ref\".\"currency\"", Some("project_id"));
    assert!(scoped.ends_with("WHERE \"project_id\"::text = $1"));
    assert!(!scoped.contains("$2"));
    assert_eq!(
        shared_table_freshness_sql("\"ref\".\"currency\"", None),
        "SELECT 1 FROM \"ref\".\"currency\" LIMIT 1"
    );
    assert!(
        shared_table_freshness_sql("\"ref\".\"currency\"", Some("project_id"))
            .contains("\"project_id\"::text = $1")
    );
}

/// A shared backup restores only with the shared scope and a tenant backup
/// only with the tenant scope, judged from the journal and the manifest.
#[test]
fn restore_scope_must_match_the_backup_scope() {
    use super::import::ensure_run_scope_matches;
    use super::scope::BackupDataScope;
    ensure_run_scope_matches(backup_pb::BackupScope::Shared, BackupDataScope::Shared)
        .expect("shared into shared");
    ensure_run_scope_matches(backup_pb::BackupScope::Tenant, BackupDataScope::Tenant)
        .expect("tenant into tenant");
    for (run, requested, hint) in [
        (
            backup_pb::BackupScope::Shared,
            BackupDataScope::Tenant,
            "BACKUP_SCOPE_SHARED",
        ),
        (
            backup_pb::BackupScope::Tenant,
            BackupDataScope::Shared,
            "BACKUP_SCOPE_TENANT",
        ),
    ] {
        let err = ensure_run_scope_matches(run, requested).expect_err("scope mismatch");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains(hint), "{}", err.message());
        assert_policy_detail(&err, "restore_tenant", "backup_topology_mismatch");
    }
}

/// A shared restore refuses the tenant-only fields instead of ignoring them,
/// and its freshness refusal names the occupied shared tables.
#[test]
fn shared_restore_refuses_tenant_fields_and_live_shared_tables() {
    use super::restore_shared::{
        shared_restore_target_not_fresh_status, shared_restore_unused_fields,
    };
    let clean = backup_pb::RestoreTenantRequest {
        source_tenant_id: "11111111-1111-1111-1111-111111111111".to_string(),
        backup_id: "33333333-3333-3333-3333-333333333333".to_string(),
        confirmation_token: "yes".to_string(),
        scope: backup_pb::BackupScope::Shared as i32,
        ..Default::default()
    };
    assert!(shared_restore_unused_fields(&clean).is_empty());
    let dirty = backup_pb::RestoreTenantRequest {
        target_tenant_id: "22222222-2222-2222-2222-222222222222".to_string(),
        allow_cross_tenant: true,
        ..clean
    };
    let fields: Vec<&str> = shared_restore_unused_fields(&dirty)
        .into_iter()
        .map(|(field, _)| field)
        .collect();
    assert_eq!(fields, vec!["target_tenant_id", "allow_cross_tenant"]);

    let err = shared_restore_target_not_fresh_status(&["\"ref\".\"currency\"".to_string()]);
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("\"ref\".\"currency\""));
    assert_policy_detail(&err, "restore_tenant", "restore_target_not_fresh");
}

/// Over the served path: a tenant token asking for a shared backup or a
/// shared restore is refused PERMISSION_DENIED before any store access; a
/// platform admin passes the gate and a shared restore that names a target
/// tenant is refused naming that field.
#[tokio::test]
async fn shared_backup_and_restore_are_refused_for_a_tenant_token() {
    let tenant = "11111111-1111-1111-1111-111111111111";
    let user = || method_security::test_claim_context("user", tenant, "", &["udb:admin"], &[]);
    let svc = BackupServiceImpl::new();
    let mut backup = Request::new(backup_pb::StartTenantBackupRequest {
        tenant_id: tenant.to_string(),
        scope: backup_pb::BackupScope::Shared as i32,
        ..Default::default()
    });
    backup
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static(tenant));
    let err =
        method_security::scope_claim_context_for_test(user(), svc.start_tenant_backup(backup))
            .await
            .expect_err("a tenant token may not back up shared tables");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert_policy_detail(
        &err,
        "start_tenant_backup",
        "shared_backup_platform_admin_required",
    );

    let restore = || backup_pb::RestoreTenantRequest {
        source_tenant_id: tenant.to_string(),
        backup_id: "33333333-3333-3333-3333-333333333333".to_string(),
        confirmation_token: "yes".to_string(),
        scope: backup_pb::BackupScope::Shared as i32,
        ..Default::default()
    };
    let err = method_security::scope_claim_context_for_test(
        user(),
        svc.restore_tenant(Request::new(restore())),
    )
    .await
    .expect_err("a tenant token may not restore shared tables");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert_policy_detail(
        &err,
        "restore_tenant",
        "shared_backup_platform_admin_required",
    );

    let admin = method_security::test_claim_context("admin", "", "", &["udb:platform_admin"], &[]);
    let err = method_security::scope_claim_context_for_test(
        admin,
        svc.restore_tenant(Request::new(backup_pb::RestoreTenantRequest {
            target_tenant_id: "22222222-2222-2222-2222-222222222222".to_string(),
            ..restore()
        })),
    )
    .await
    .expect_err("a shared restore has no target tenant");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        decode_detail(&err).field_violations[0].field,
        "target_tenant_id"
    );
}
