//! Restore of a shared-table backup (`RestoreTenant` with
//! `scope: BACKUP_SCOPE_SHARED`): the same journal lookup, manifest integrity
//! check, artifact checksum + decryption and `jsonb_populate_record` insert as a
//! tenant restore ([`super::import`]), into the active project catalog's shared
//! tables instead of a tenant's rows.
//!
//! Platform admin only. Shared rows belong to no tenant, so nothing is
//! rewritten on insert and there is no cross-tenant remapping; the target is the
//! shared tables themselves, and — exactly as a tenant restore refuses a live
//! target tenant — the restore refuses while any shared table already holds rows
//! (the freshness probe runs inside the restore transaction).

use chrono::Utc;
use tonic::Status;
use tonic::metadata::MetadataMap;
use uuid::Uuid;

use crate::proto::udb::core::backup::services::v1 as backup_pb;
use crate::runtime::channels::OperationChannel;
use crate::runtime::tenant_movement::{
    TenantMovementOperation, TenantMovementRequest, tenant_movement_policy_status,
    validate_tenant_movement_scope,
};

use super::super::native_helpers::{
    admit_on as native_admit_on, parse_uuid, project_scoped_native_service_context,
};
use super::BackupServiceImpl;
use super::config::{KIND_RESTORE, TOPIC_BACKUP_RESTORED};
use super::errors::{
    backup_internal_status, backup_policy_status, backup_topology_mismatch_status,
};
use super::events::emit_event;
use super::import::{
    fetch_verified_table_jsonl, load_restore_run, load_verified_restore_manifest,
    manifest_table_by_relation, parse_scoped_restore_row,
};
use super::model::qualified_relation;
use super::scope::{BackupDataScope, shared_scope_privilege, shared_table_freshness_sql};
use super::store::{journal_run, journal_run_started};

const OPERATION: &str = "restore_tenant";

/// The request fields a shared restore has no use for, each with the reason.
/// They are refused rather than ignored.
pub(crate) fn shared_restore_unused_fields(
    req: &backup_pb::RestoreTenantRequest,
) -> Vec<(&'static str, &'static str)> {
    let mut unused = Vec::new();
    if !req.target_tenant_id.trim().is_empty() {
        unused.push((
            "target_tenant_id",
            "must be empty for a shared restore: shared rows belong to no tenant",
        ));
    }
    if req.allow_cross_tenant {
        unused.push((
            "allow_cross_tenant",
            "must be false for a shared restore: it moves no tenant's rows",
        ));
    }
    unused
}

/// The refusal for a shared restore whose target shared tables already hold
/// rows, naming them. Same policy decision as the live-tenant refusal.
pub(crate) fn shared_restore_target_not_fresh_status(occupied: &[String]) -> Status {
    backup_policy_status(
        OPERATION,
        "restore_target_not_fresh",
        format!(
            "shared tables already hold rows (in {}); restoring over live shared data is refused — clear them first",
            occupied.join(", ")
        ),
    )
}

pub(crate) async fn restore_shared(
    svc: &BackupServiceImpl,
    metadata: &MetadataMap,
    req: backup_pb::RestoreTenantRequest,
) -> Result<backup_pb::RestoreTenantResponse, Status> {
    // Platform authority FIRST: a non-admin learns nothing about the run.
    let privileged = shared_scope_privilege(OPERATION)?;
    let source_tenant_id = req.source_tenant_id.trim().to_string();
    let backup_id = req.backup_id.trim().to_string();
    if source_tenant_id.is_empty() || backup_id.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "source_tenant_id and backup_id are required",
            [
                (
                    "source_tenant_id",
                    "must name the tenant whose journal holds the shared backup",
                ),
                ("backup_id", "must be a non-empty backup id"),
            ],
        ));
    }
    let unused = shared_restore_unused_fields(&req);
    if !unused.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "a shared restore targets the shared tables, not a tenant",
            unused,
        ));
    }
    // DESTRUCTIVE: a missing confirmation token fails CLOSED.
    if req.confirmation_token.trim().is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "RestoreTenant writes shared tables; confirmation_token is required",
            [(
                "confirmation_token",
                "must be present to restore shared tables",
            )],
        ));
    }

    // SHARED fail-closed movement validator: shared rows have no tenant filter,
    // so the move passes only with the caller's platform privilege.
    let marker = crate::broker::SHARED_TENANT_MARKER;
    let movement = TenantMovementRequest {
        operation: TenantMovementOperation::RestoreImport,
        tenant_id: marker,
        target_tenant_id: Some(marker),
        tenant_filter_present: false,
        privileged_cross_tenant: privileged,
    };
    validate_tenant_movement_scope(&movement)
        .map_err(|err| tenant_movement_policy_status(movement.operation, err))?;

    let runtime = svc.require_runtime()?;
    let _ = parse_uuid("source_tenant_id", &source_tenant_id)?;
    // The restore run is journaled next to the backup run it restores.
    let mut context = project_scoped_native_service_context(metadata, &source_tenant_id);
    let binding = svc.resolve_project_snapshot_for(&context.project_id, BackupDataScope::Shared)?;
    context.project_id = binding.project_id.clone();
    let manifest = binding.manifest.as_ref();

    let (run, location, object_prefix) = load_restore_run(
        runtime,
        &binding,
        metadata,
        &source_tenant_id,
        &backup_id,
        BackupDataScope::Shared,
    )
    .await?;
    let plan = BackupDataScope::Shared.plan(manifest);

    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "backup",
        OperationChannel::Admin,
        &source_tenant_id,
        None,
    )
    .await?;

    let (manifest_value, object_backend, object_bucket) =
        load_verified_restore_manifest(runtime, &binding, &run, &location, BackupDataScope::Shared)
            .await?;
    // Restore in REVERSE planner order: the planner emits children→parents;
    // inserting parents→children satisfies FK constraints.
    let mut ordered_tables = manifest_value
        .get("tables")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    ordered_tables.reverse();

    let restore_id = Uuid::new_v4().to_string();
    let restore_metadata = serde_json::json!({
        "source_backup_id": backup_id,
        "object_backend": location.object_backend,
        "object_bucket": location.object_bucket,
        "manifest_key": location.manifest_key,
        "project_id": binding.project_id,
        "catalog_version": binding.catalog_version,
        "catalog_checksum": binding.catalog_checksum,
        "manifest_catalog_checksum": binding.manifest_checksum,
        "postgres_instance": binding.postgres_instance,
        "scope": BackupDataScope::Shared.tag(),
    });
    journal_run_started(
        runtime,
        &context,
        &restore_id,
        &source_tenant_id,
        &binding.project_id,
        KIND_RESTORE,
        &object_prefix,
        marker,
        marker,
        &restore_metadata,
    )
    .await?;

    let mut tx = binding.pool.begin().await.map_err(|err| {
        backup_internal_status(
            "restore_transaction_begin",
            format!("failed to begin restore transaction: {err}"),
        )
    })?;

    // FRESH-target guard (authoritative, inside the restore tx): any row in a
    // shared table of the active catalog refuses the restore.
    let mut occupied: Vec<String> = Vec::new();
    for target in &plan.targets {
        let rel = qualified_relation(&target.schema, &target.table);
        let table_meta = manifest_table_by_relation(manifest, &target.schema, &target.table)
            .ok_or_else(|| {
                backup_internal_status(
                    "restore_freshness_project_scope",
                    format!(
                        "shared table planner target {}.{} is absent from the active project manifest",
                        target.schema, target.table
                    ),
                )
            })?;
        let project_column = crate::generation::sql::resolve_project_column(table_meta);
        let sql = shared_table_freshness_sql(&rel, project_column);
        let mut query = sqlx::query_scalar::<_, i32>(&sql);
        if project_column.is_some() {
            query = query.bind(&binding.project_id);
        }
        let present = query.fetch_optional(&mut *tx).await.map_err(|err| {
            backup_internal_status(
                "restore_freshness_probe",
                format!(
                    "restore freshness probe failed on {}.{}: {err}",
                    target.schema, target.table
                ),
            )
        })?;
        if present.is_some() {
            occupied.push(rel);
        }
    }
    if !occupied.is_empty() {
        return Err(shared_restore_target_not_fresh_status(&occupied));
    }

    let mut restored_rows: i64 = 0;
    let mut restored_table_count: i32 = 0;
    for entry in &ordered_tables {
        let schema = entry.get("schema").and_then(|v| v.as_str()).unwrap_or("");
        let table = entry.get("table").and_then(|v| v.as_str()).unwrap_or("");
        let tenant_column = entry
            .get("tenant_column")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let declared_project_column = entry
            .get("project_column")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let object_key = entry
            .get("object_key")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let expected_checksum = entry
            .get("checksum_sha256")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if schema.is_empty() || table.is_empty() || object_key.is_empty() {
            return Err(backup_topology_mismatch_status(
                OPERATION,
                "shared backup manifest has a table entry without schema, table or object key",
            ));
        }
        let manifest_table = manifest_table_by_relation(manifest, schema, table).ok_or_else(|| {
            backup_topology_mismatch_status(
                OPERATION,
                format!(
                    "backup manifest relation {schema}.{table} is absent from the active catalog"
                ),
            )
        })?;
        // Only shared tables are restored here: a table that has since become
        // tenant-owned would receive rows with no tenant.
        if !crate::broker::is_global_table(manifest_table) || !tenant_column.is_empty() {
            return Err(backup_topology_mismatch_status(
                OPERATION,
                format!(
                    "backup manifest relation {schema}.{table} is not a shared table in the active catalog"
                ),
            ));
        }
        let project_column =
            crate::generation::sql::resolve_project_column(manifest_table).unwrap_or_default();
        if declared_project_column != project_column {
            return Err(backup_topology_mismatch_status(
                OPERATION,
                format!(
                    "backup manifest project column '{declared_project_column}' for {schema}.{table} does not match active catalog column '{project_column}'"
                ),
            ));
        }
        let jsonl = fetch_verified_table_jsonl(
            runtime,
            &context.project_id,
            &object_backend,
            &object_bucket,
            schema,
            table,
            object_key,
            expected_checksum,
        )
        .await?;
        let rel = qualified_relation(schema, table);
        // `jsonb_populate_record` casts the row JSON into the table's row type,
        // so every column type is handled by Postgres without hand-mapping.
        let insert_sql =
            format!("INSERT INTO {rel} SELECT (jsonb_populate_record(NULL::{rel}, $1::jsonb)).*");
        for line in jsonl.lines().map(str::trim).filter(|line| !line.is_empty()) {
            // No tenant column: the row is inserted as backed up, with only the
            // project column checked against the restore project.
            let row = parse_scoped_restore_row(
                line,
                schema,
                table,
                "",
                "",
                project_column,
                &binding.project_id,
            )?;
            let row_json = serde_json::to_string(&row).map_err(|err| {
                backup_internal_status(
                    "restore_row_reserialize",
                    format!("restore row reserialize failed: {err}"),
                )
            })?;
            sqlx::query(&insert_sql)
                .bind(row_json)
                .execute(&mut *tx)
                .await
                .map_err(|err| {
                    backup_internal_status(
                        "restore_insert_row",
                        format!("restore insert failed for {schema}.{table}: {err}"),
                    )
                })?;
            restored_rows += 1;
        }
        restored_table_count += 1;
    }
    tx.commit().await.map_err(|err| {
        backup_internal_status(
            "restore_transaction_commit",
            format!("failed to commit restore transaction: {err}"),
        )
    })?;

    journal_run(
        runtime,
        &context,
        &restore_id,
        &source_tenant_id,
        &binding.project_id,
        KIND_RESTORE,
        &object_prefix,
        &run.manifest_checksum,
        restored_table_count as i64,
        restored_rows,
        0,
        marker,
        marker,
        &restore_metadata,
    )
    .await?;

    emit_event(
        svc,
        TOPIC_BACKUP_RESTORED,
        marker,
        marker,
        &context.project_id,
        &restore_id,
        serde_json::json!({
            "backup_id": restore_id,
            "source_backup_id": backup_id,
            "source_tenant_id": marker,
            "target_tenant_id": marker,
            "journal_tenant_id": source_tenant_id,
            "scope": BackupDataScope::Shared.tag(),
            "project_id": binding.project_id,
            "object_prefix": object_prefix,
            "restored_table_count": restored_table_count,
            "restored_rows": restored_rows,
            "restored_at": Utc::now().to_rfc3339(),
        }),
    )
    .await;

    Ok(backup_pb::RestoreTenantResponse {
        backup_id: restore_id,
        source_object_prefix: object_prefix,
        restored_table_count,
        restored_rows,
        message: "shared tables restored".to_string(),
        error: None,
    })
}
