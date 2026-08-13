//! The tenant backup EXPORT machinery for the native `BackupService`:
//! `start_tenant_backup` (shared movement gate, shared tenant-table enumeration,
//! per-table JSONL read, encrypt-at-rest, checksummed manifest, object IO,
//! journal + outbox) plus the object-target resolver it uses. Extracted verbatim
//! from the former god file — the SQL, crypto, and object-store contracts are
//! byte-for-byte identical; `svc` replaces `&self`.

use chrono::Utc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::backup::services::v1 as backup_pb;
use crate::runtime::channels::OperationChannel;
use crate::runtime::core::tenant_purge::plan_tenant_purge;
use crate::runtime::executor_utils::qi_runtime;
use crate::runtime::tenant_movement::{
    TenantMovementOperation, TenantMovementRequest, tenant_movement_policy_status,
    validate_tenant_movement_scope,
};

use super::super::native_helpers::{
    admit_on as native_admit_on, parse_uuid, project_scoped_native_service_context,
    validate_request_tenant,
};
use super::BackupServiceImpl;
use super::config::{KIND_BACKUP, MANIFEST_SUFFIX, TOPIC_BACKUP_COMPLETED};
use super::errors::backup_internal_status;
use super::events::emit_event;
use super::model::{qualified_relation, sha256_hex};
use super::store::journal_run;

/// Resolve the object-store target for a run: an explicit request override
/// wins, else the service default (which `build_backup_service` seeds from
/// `UDB_STORAGE_OBJECT_BACKEND`/`UDB_STORAGE_BUCKET`).
pub(crate) fn resolve_object_target(
    svc: &BackupServiceImpl,
    backend: &str,
    bucket: &str,
) -> (String, String) {
    let backend = if backend.trim().is_empty() {
        svc.object_backend.clone()
    } else {
        backend.trim().to_string()
    };
    let bucket = if bucket.trim().is_empty() {
        svc.object_bucket.clone()
    } else {
        bucket.trim().to_string()
    };
    (backend, bucket)
}

pub(crate) async fn start_tenant_backup(
    svc: &BackupServiceImpl,
    request: Request<backup_pb::StartTenantBackupRequest>,
) -> Result<Response<backup_pb::StartTenantBackupResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // Cross-tenant guard FIRST: the body tenant_id must match the verified
    // claim/header. After this passes the body value IS the verified tenant.
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    if tenant_id.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "tenant_id is required",
            [("tenant_id", "must be a non-empty tenant id")],
        ));
    }
    // The verified-tenant context carries project/correlation from request
    // metadata; the actual export mechanics live in the shared internal routine
    // so a background driver can fire the SAME backup without an inbound request.
    let context = project_scoped_native_service_context(&metadata, &tenant_id);
    let response = run_tenant_backup(
        svc,
        &tenant_id,
        &req.object_backend,
        &req.object_bucket,
        context,
    )
    .await?;
    Ok(Response::new(response))
}

/// The internal tenant-backup routine that BOTH the `StartTenantBackup` RPC and
/// the leader-elected scheduled-backup driver call. It performs every side
/// effect the RPC has — the SHARED movement-scope validation, fair admission,
/// capability guards, per-table encrypted export, checksummed manifest, journal
/// row, and outbox event — but takes an already-verified `tenant_id` and a
/// prepared [`crate::RequestContext`] instead of request metadata, so a
/// background driver with no inbound request can fire a backup through the SAME
/// mechanism without re-entering the gRPC layer.
///
/// The CALLER owns the cross-tenant body/claim guard: the RPC runs
/// `validate_request_tenant` against request metadata first; the scheduled-backup
/// driver only ever passes a policy's OWN tenant id (read cross-tenant in the
/// leader lane), so no request-scoped identity is smuggled here.
pub(crate) async fn run_tenant_backup(
    svc: &BackupServiceImpl,
    tenant_id: &str,
    req_object_backend: &str,
    req_object_bucket: &str,
    context: crate::RequestContext,
) -> Result<backup_pb::StartTenantBackupResponse, Status> {
    let tenant_id = tenant_id.trim().to_string();
    if tenant_id.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "tenant_id is required",
            [("tenant_id", "must be a non-empty tenant id")],
        ));
    }

    // SHARED fail-closed movement validator — never a bespoke scope check. A
    // backup always filters by the verified tenant (tenant_filter_present),
    // and is single-tenant (no target), so this fails closed on an unscoped
    // export exactly as the broker startup gate does.
    let movement = TenantMovementRequest {
        operation: TenantMovementOperation::BackupExport,
        tenant_id: &tenant_id,
        target_tenant_id: None,
        tenant_filter_present: true,
        privileged_cross_tenant: false,
    };
    validate_tenant_movement_scope(&movement)
        .map_err(|err| tenant_movement_policy_status(movement.operation, err))?;

    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "backup",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let pool = svc.require_pool()?;
    let manifest = svc.require_manifest()?;
    // Canonical tenant id (and a value that matches the `::text` filter).
    let _ = parse_uuid("tenant_id", &tenant_id)?;
    let (object_backend, object_bucket) =
        resolve_object_target(svc, req_object_backend, req_object_bucket);

    // SHARED enumeration: the same planner PurgeTenant uses. Tenant-owned
    // tables are `targets`; tenant-less tables are REPORTED in `excluded`.
    let plan = plan_tenant_purge(manifest);
    let excluded_count = plan.excluded.len() as i64;

    let backup_id = Uuid::new_v4().to_string();
    let started = Utc::now();
    let object_prefix = format!(
        "backups/{}/{}-{}/",
        tenant_id,
        started.format("%Y%m%dT%H%M%SZ"),
        &backup_id[..8.min(backup_id.len())]
    );

    let mut table_entries: Vec<backup_pb::BackupTableEntry> = Vec::new();
    let mut total_rows: i64 = 0;
    for target in &plan.targets {
        let rel = qualified_relation(&target.schema, &target.table);
        // Compare the tenant column as text so a UUID column and a VARCHAR
        // tenant column both match the bound tenant id (no text-cast trap).
        let select_sql = format!(
            "SELECT row_to_json(t)::text FROM {rel} t WHERE {col}::text = $1",
            col = qi_runtime(&target.tenant_column),
        );
        let rows: Vec<String> = sqlx::query_scalar(&select_sql)
            .bind(&tenant_id)
            .fetch_all(pool)
            .await
            .map_err(|err| {
                backup_internal_status(
                    "start_backup_read_table",
                    format!(
                        "backup failed reading {}.{}: {err}",
                        target.schema, target.table
                    ),
                )
            })?;
        let row_count = rows.len() as i64;
        total_rows += row_count;
        let jsonl = rows.join("\n");
        // D1: a tenant-data backup must NEVER be written in the clear under a
        // manifest that labels it `encrypted: true`. Use the MANDATORY variant so a
        // keyless deployment fails closed instead of producing a plaintext `.enc`
        // artifact (the base helper passes plaintext through when not fail-closed).
        let ciphertext = runtime
            .encrypt_secret_at_rest_required(&jsonl)
            .map_err(|err| {
                backup_internal_status(
                    "start_backup_encrypt_artifact",
                    format!("backup encryption failed: {err}"),
                )
            })?;
        let bytes = ciphertext.into_bytes();
        let checksum = sha256_hex(&bytes);
        let object_key = format!(
            "{object_prefix}{}.{}.jsonl.enc",
            target.schema, target.table
        );
        let put_req = crate::runtime::core::setup_data::object_request_json(
            "put",
            &object_bucket,
            &object_key,
            "application/octet-stream",
        );
        runtime
            .put_object_backend_target_for_project(
                &object_backend,
                None,
                &context.project_id,
                &put_req,
                bytes,
            )
            .await?;
        table_entries.push(backup_pb::BackupTableEntry {
            schema: target.schema.clone(),
            table: target.table.clone(),
            tenant_column: target.tenant_column.clone(),
            object_key,
            row_count,
            checksum_sha256: checksum,
        });
    }

    let excluded: Vec<backup_pb::BackupExcludedTable> = plan
        .excluded
        .iter()
        .map(|e| backup_pb::BackupExcludedTable {
            schema: e.schema.clone(),
            table: e.table.clone(),
            reason: e.reason.clone(),
        })
        .collect();

    // Checksummed run manifest (plaintext: schema/table names + ciphertext
    // checksums, no row data). Excluded tables are recorded here so an
    // operator sees exactly what the backup does NOT cover — never a silent
    // skip.
    let manifest_value = serde_json::json!({
        "backup_id": backup_id,
        "tenant_id": tenant_id,
        "created_at": started.to_rfc3339(),
        "object_prefix": object_prefix,
        "object_backend": object_backend,
        "object_bucket": object_bucket,
        "encrypted": true,
        "fk_ordered": plan.fk_ordered,
        "tables": table_entries.iter().map(|t| serde_json::json!({
            "schema": t.schema,
            "table": t.table,
            "tenant_column": t.tenant_column,
            "object_key": t.object_key,
            "row_count": t.row_count,
            "checksum_sha256": t.checksum_sha256,
        })).collect::<Vec<_>>(),
        "excluded": excluded.iter().map(|e| serde_json::json!({
            "schema": e.schema,
            "table": e.table,
            "reason": e.reason,
        })).collect::<Vec<_>>(),
    });
    let manifest_bytes = serde_json::to_vec(&manifest_value).map_err(|err| {
        backup_internal_status(
            "start_backup_serialize_manifest",
            format!("backup manifest serialize failed: {err}"),
        )
    })?;
    let manifest_checksum = sha256_hex(&manifest_bytes);
    let manifest_key = format!("{object_prefix}{MANIFEST_SUFFIX}");
    let manifest_put = crate::runtime::core::setup_data::object_request_json(
        "put",
        &object_bucket,
        &manifest_key,
        "application/json",
    );
    runtime
        .put_object_backend_target_for_project(
            &object_backend,
            None,
            &context.project_id,
            &manifest_put,
            manifest_bytes,
        )
        .await?;

    let table_count = table_entries.len() as i64;
    journal_run(
        runtime,
        &context,
        &backup_id,
        &tenant_id,
        KIND_BACKUP,
        &object_prefix,
        &manifest_checksum,
        table_count,
        total_rows,
        excluded_count,
        "",
        "",
    )
    .await?;

    emit_event(
        svc,
        TOPIC_BACKUP_COMPLETED,
        &tenant_id,
        &tenant_id,
        &context.project_id,
        &backup_id,
        serde_json::json!({
            "backup_id": backup_id,
            "tenant_id": tenant_id,
            "object_prefix": object_prefix,
            "manifest_checksum": manifest_checksum,
            "table_count": table_count,
            "total_rows": total_rows,
            "excluded_count": excluded_count,
        }),
    )
    .await;

    Ok(backup_pb::StartTenantBackupResponse {
        backup_id,
        object_prefix,
        manifest_checksum,
        table_count: table_count as i32,
        total_rows,
        excluded_count: excluded_count as i32,
        tables: table_entries,
        excluded,
        message: "tenant backup completed".to_string(),
        error: None,
    })
}
