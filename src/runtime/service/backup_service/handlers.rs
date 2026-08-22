//! The inventory/policy RPC handlers for the native `BackupService`
//! (list/get backups, put/get/list/delete backup policies), extracted from the
//! trait impl as free `pub(crate) async fn`s taking `svc` where the trait method
//! took `&self`. `mod.rs` delegates one line to each. Bodies are verbatim — the
//! same cross-tenant guard, admission, project-pinned native-entity
//! reads/writes, and outbox emission as the former god file.

use chrono::Utc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{ComparisonOp, LogicalDelete, LogicalFilter, LogicalRecord, LogicalValue};
use crate::proto::udb::core::backup::services::v1 as backup_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, non_empty_json, project_scoped_native_service_context,
    validate_request_tenant,
};
use super::BackupServiceImpl;
use super::config::{BACKUP_POLICY_MSG, TOPIC_POLICY_DELETED, TOPIC_POLICY_UPSERTED};
use super::errors::{
    backup_internal_status, backup_not_found_status, backup_run_location_missing_status,
    required_backup_field, restore_manifest_integrity_status,
};
use super::events::{emit_event, event_transaction_op};
use super::model::{
    json_str, policy_view_from_json, row_object, run_location_from_json, run_summary_from_json,
    sha256_hex,
};
use super::store::{
    clamp_limit, logical_string, next_page_token, parse_offset, policies_list_read,
    policy_conflict, policy_read_by_name, run_read_by_id, runs_list_read,
};

pub(crate) async fn list_backups(
    svc: &BackupServiceImpl,
    request: Request<backup_pb::ListBackupsRequest>,
) -> Result<Response<backup_pb::ListBackupsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "backup",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let mut context = project_scoped_native_service_context(&metadata, &tenant_id);
    context.project_id = svc.require_active_project(&context.project_id)?;
    let limit = clamp_limit(req.page_size);
    let offset = parse_offset(&req.page_token);
    let kind = match req.kind.trim() {
        "" => None,
        other => Some(other),
    };
    let rows = runtime
        .native_entity_read_for_service(
            "backup",
            &context,
            runs_list_read(&tenant_id, &context.project_id, kind, limit, offset),
        )
        .await?;
    let backups: Vec<backup_pb::BackupRunSummary> =
        rows.iter().map(run_summary_from_json).collect();
    let next = next_page_token(offset, limit, backups.len());
    Ok(Response::new(backup_pb::ListBackupsResponse {
        backups,
        next_page_token: next,
        error: None,
    }))
}

pub(crate) async fn get_backup(
    svc: &BackupServiceImpl,
    request: Request<backup_pb::GetBackupRequest>,
) -> Result<Response<backup_pb::GetBackupResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let backup_id =
        required_backup_field("backup_id", &req.backup_id, "must be a non-empty backup id")?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "backup",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let mut context = project_scoped_native_service_context(&metadata, &tenant_id);
    let binding = svc.resolve_project_snapshot(&context.project_id)?;
    context.project_id = binding.project_id.clone();
    let run_row = runtime
        .native_entity_read_for_service(
            "backup",
            &context,
            run_read_by_id(&tenant_id, &binding.project_id, &backup_id),
        )
        .await?
        .first()
        .cloned()
        .ok_or_else(|| {
            backup_not_found_status("get_backup", "backup_run_not_found", "backup run not found")
        })?;
    let run = run_summary_from_json(&run_row);
    if run.project_id != binding.project_id {
        return Err(super::errors::backup_topology_mismatch_status(
            "get_backup",
            "backup run project does not match the active request project",
        ));
    }

    // A completed run's detail is integrity-bearing, not best-effort. Locate it
    // exclusively from the immutable run metadata and propagate object-store or
    // checksum failures instead of returning a false empty detail set.
    let (mut tables, mut excluded) = (Vec::new(), Vec::new());
    if !run.object_prefix.trim().is_empty() {
        let location = run_location_from_json(&run_row)
            .ok_or_else(|| backup_run_location_missing_status("get_backup"))?;
        if location.project_id != binding.project_id
            || location.catalog_checksum != binding.catalog_checksum
            || location.postgres_instance != binding.postgres_instance
        {
            return Err(super::errors::backup_topology_mismatch_status(
                "get_backup",
                "backup run topology does not match the active request project",
            ));
        }
        let get_req = crate::runtime::core::setup_data::object_request_json(
            "get",
            &location.object_bucket,
            &location.manifest_key,
            "",
        );
        let bytes = runtime
            .get_object_backend_target_for_project(
                &location.object_backend,
                None,
                &location.project_id,
                &get_req,
            )
            .await?;
        if run.manifest_checksum.trim().is_empty()
            || sha256_hex(&bytes) != run.manifest_checksum.trim()
        {
            return Err(restore_manifest_integrity_status());
        }
        let value = serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|err| {
            backup_internal_status(
                "get_backup_manifest_parse",
                format!("backup manifest parse failed: {err}"),
            )
        })?;
        if let Some(arr) = value.get("tables").and_then(|v| v.as_array()) {
            tables = arr
                .iter()
                .map(|t| backup_pb::BackupTableEntry {
                    schema: t
                        .get("schema")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    table: t
                        .get("table")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    tenant_column: t
                        .get("tenant_column")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    object_key: t
                        .get("object_key")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    row_count: t.get("row_count").and_then(|v| v.as_i64()).unwrap_or(0),
                    checksum_sha256: t
                        .get("checksum_sha256")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                })
                .collect();
        }
        if let Some(arr) = value.get("excluded").and_then(|v| v.as_array()) {
            excluded = arr
                .iter()
                .map(|e| backup_pb::BackupExcludedTable {
                    schema: e
                        .get("schema")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    table: e
                        .get("table")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    reason: e
                        .get("reason")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                })
                .collect();
        }
    }

    Ok(Response::new(backup_pb::GetBackupResponse {
        backup: Some(run),
        tables,
        excluded,
        error: None,
    }))
}

pub(crate) async fn put_backup_policy(
    svc: &BackupServiceImpl,
    request: Request<backup_pb::PutBackupPolicyRequest>,
) -> Result<Response<backup_pb::PutBackupPolicyResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let policy_name = required_backup_field(
        "policy_name",
        &req.policy_name,
        "must be a non-empty policy name",
    )?;
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
    let mut context = project_scoped_native_service_context(&metadata, &tenant_id);
    let binding = svc.resolve_project_snapshot(&context.project_id)?;
    context.project_id = binding.project_id.clone();

    // Reuse the existing policy id on update so the upsert is in place.
    let existing = runtime
        .native_entity_read_for_service(
            "backup",
            &context,
            policy_read_by_name(&tenant_id, &binding.project_id, &policy_name),
        )
        .await?;
    let policy_id = existing
        .first()
        .map(|row| json_str(row_object(row), "policy_id"))
        .filter(|id| !id.trim().is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let now = Utc::now();
    let mut record = LogicalRecord::new();
    record.insert("policy_id".to_string(), logical_string(&policy_id));
    record.insert("tenant_id".to_string(), logical_string(&tenant_id));
    record.insert(
        "project_id".to_string(),
        logical_string(&binding.project_id),
    );
    record.insert("policy_name".to_string(), logical_string(&policy_name));
    record.insert(
        "schedule_cron".to_string(),
        logical_string(req.schedule_cron.trim()),
    );
    record.insert(
        "retention_days".to_string(),
        LogicalValue::Int(i64::from(req.retention_days.max(0))),
    );
    record.insert(
        "max_retained_backups".to_string(),
        LogicalValue::Int(i64::from(req.max_retained_backups.max(0))),
    );
    record.insert("enabled".to_string(), LogicalValue::Bool(req.enabled));
    record.insert(
        "object_backend".to_string(),
        logical_string(req.object_backend.trim()),
    );
    record.insert(
        "object_bucket".to_string(),
        logical_string(req.object_bucket.trim()),
    );
    record.insert("created_at".to_string(), LogicalValue::Timestamp(now));
    record.insert("updated_at".to_string(), LogicalValue::Timestamp(now));
    record.insert(
        "metadata_json".to_string(),
        logical_string(non_empty_json(&req.metadata_json)),
    );

    // Built before the write so a Postgres target commits both together.
    let event_extra = serde_json::json!({
        "tenant_id": tenant_id,
        "project_id": binding.project_id,
        "policy_id": policy_id,
        "policy_name": policy_name,
        "enabled": req.enabled,
        "retention_days": req.retention_days,
    });
    let event_op = event_transaction_op(
        svc,
        TOPIC_POLICY_UPSERTED,
        &tenant_id,
        &tenant_id,
        &context.project_id,
        &policy_name,
        event_extra.clone(),
    );
    let had_event = event_op.is_some();
    let co_committed = runtime
        .native_entity_write_co_commit_for_service(
            "backup",
            &context,
            BACKUP_POLICY_MSG,
            record,
            policy_conflict(),
            event_op,
        )
        .await?;
    if had_event && !co_committed {
        // Target is not Postgres, so the outbox row cannot join the
        // write's transaction. Keep the best-effort emit for that backend.
        emit_event(
            svc,
            TOPIC_POLICY_UPSERTED,
            &tenant_id,
            &tenant_id,
            &context.project_id,
            &policy_name,
            event_extra,
        )
        .await;
    }

    Ok(Response::new(backup_pb::PutBackupPolicyResponse {
        policy_id,
        message: "backup policy saved".to_string(),
        error: None,
    }))
}

pub(crate) async fn get_backup_policy(
    svc: &BackupServiceImpl,
    request: Request<backup_pb::GetBackupPolicyRequest>,
) -> Result<Response<backup_pb::GetBackupPolicyResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let policy_name = required_backup_field(
        "policy_name",
        &req.policy_name,
        "must be a non-empty policy name",
    )?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "backup",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let mut context = project_scoped_native_service_context(&metadata, &tenant_id);
    let binding = svc.resolve_project_snapshot(&context.project_id)?;
    context.project_id = binding.project_id.clone();
    let policy = runtime
        .native_entity_read_for_service(
            "backup",
            &context,
            policy_read_by_name(&tenant_id, &binding.project_id, &policy_name),
        )
        .await?
        .first()
        .map(policy_view_from_json)
        .ok_or_else(|| {
            backup_not_found_status(
                "get_backup_policy",
                "backup_policy_not_found",
                "backup policy not found",
            )
        })?;
    Ok(Response::new(backup_pb::GetBackupPolicyResponse {
        policy: Some(policy),
        error: None,
    }))
}

pub(crate) async fn list_backup_policies(
    svc: &BackupServiceImpl,
    request: Request<backup_pb::ListBackupPoliciesRequest>,
) -> Result<Response<backup_pb::ListBackupPoliciesResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "backup",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let mut context = project_scoped_native_service_context(&metadata, &tenant_id);
    let binding = svc.resolve_project_snapshot(&context.project_id)?;
    context.project_id = binding.project_id.clone();
    let limit = clamp_limit(req.page_size);
    let offset = parse_offset(&req.page_token);
    let rows = runtime
        .native_entity_read_for_service(
            "backup",
            &context,
            policies_list_read(&tenant_id, &binding.project_id, limit, offset),
        )
        .await?;
    let policies: Vec<backup_pb::BackupPolicyView> =
        rows.iter().map(policy_view_from_json).collect();
    let next = next_page_token(offset, limit, policies.len());
    Ok(Response::new(backup_pb::ListBackupPoliciesResponse {
        policies,
        next_page_token: next,
        error: None,
    }))
}

pub(crate) async fn delete_backup_policy(
    svc: &BackupServiceImpl,
    request: Request<backup_pb::DeleteBackupPolicyRequest>,
) -> Result<Response<backup_pb::DeleteBackupPolicyResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let policy_name = required_backup_field(
        "policy_name",
        &req.policy_name,
        "must be a non-empty policy name",
    )?;
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
    let mut context = project_scoped_native_service_context(&metadata, &tenant_id);
    let binding = svc.resolve_project_snapshot(&context.project_id)?;
    context.project_id = binding.project_id.clone();
    // Built before the delete so a Postgres target commits both together.
    let event_extra = serde_json::json!({
        "tenant_id": tenant_id,
        "project_id": binding.project_id,
        "policy_name": policy_name
    });
    let event_op = event_transaction_op(
        svc,
        TOPIC_POLICY_DELETED,
        &tenant_id,
        &tenant_id,
        &context.project_id,
        &policy_name,
        event_extra.clone(),
    );
    let had_event = event_op.is_some();
    let co_committed = runtime
        .native_entity_delete_co_commit_for_service(
            "backup",
            &context,
            LogicalDelete {
                message_type: BACKUP_POLICY_MSG.to_string(),
                filter: LogicalFilter::And(vec![
                    LogicalFilter::Comparison {
                        field: "tenant_id".to_string(),
                        op: ComparisonOp::Eq,
                        value: logical_string(&tenant_id),
                    },
                    LogicalFilter::Comparison {
                        field: "project_id".to_string(),
                        op: ComparisonOp::Eq,
                        value: logical_string(&binding.project_id),
                    },
                    LogicalFilter::Comparison {
                        field: "policy_name".to_string(),
                        op: ComparisonOp::Eq,
                        value: logical_string(&policy_name),
                    },
                ]),
                return_fields: Vec::new(),
            },
            event_op,
        )
        .await?;

    if had_event && !co_committed {
        // Target is not Postgres; keep the best-effort emit for that backend.
        emit_event(
            svc,
            TOPIC_POLICY_DELETED,
            &tenant_id,
            &tenant_id,
            &context.project_id,
            &policy_name,
            event_extra,
        )
        .await;
    }

    Ok(Response::new(backup_pb::DeleteBackupPolicyResponse {
        deleted: true,
        message: "backup policy deleted".to_string(),
        error: None,
    }))
}
