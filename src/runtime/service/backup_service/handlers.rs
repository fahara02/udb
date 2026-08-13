//! The inventory/policy RPC handlers for the native `BackupService`
//! (list/get backups, put/get/list/delete backup policies), extracted from the
//! trait impl as free `pub(crate) async fn`s taking `svc` where the trait method
//! took `&self`. `mod.rs` delegates one line to each. Bodies are verbatim — the
//! same cross-tenant guard, admission, native-entity reads/writes, and outbox
//! emission as the former god file.

use chrono::Utc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{ComparisonOp, LogicalDelete, LogicalFilter, LogicalRecord, LogicalValue};
use crate::proto::udb::core::backup::services::v1 as backup_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, non_empty_json, tenant_only_native_service_context,
    validate_request_tenant,
};
use super::BackupServiceImpl;
use super::config::{
    BACKUP_POLICY_MSG, MANIFEST_SUFFIX, TOPIC_POLICY_DELETED, TOPIC_POLICY_UPSERTED,
};
use super::errors::{backup_not_found_status, required_backup_field};
use super::events::emit_event;
use super::model::{json_str, policy_view_from_json, row_object, run_summary_from_json};
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
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
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
            runs_list_read(&tenant_id, kind, limit, offset),
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
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let run = runtime
        .native_entity_read_for_service("backup", &context, run_read_by_id(&tenant_id, &backup_id))
        .await?
        .first()
        .map(run_summary_from_json)
        .ok_or_else(|| {
            backup_not_found_status("get_backup", "backup_run_not_found", "backup run not found")
        })?;

    // Best-effort: read the run manifest for per-table detail. A missing
    // manifest (e.g. object store offline) still returns the journal summary.
    let (mut tables, mut excluded) = (Vec::new(), Vec::new());
    if !run.object_prefix.trim().is_empty() {
        let manifest_key = format!("{}{MANIFEST_SUFFIX}", run.object_prefix);
        let get_req = crate::runtime::core::setup_data::object_request_json(
            "get",
            &svc.object_bucket,
            &manifest_key,
            "",
        );
        if let Ok(bytes) = runtime
            .get_object_backend_target_for_project(
                &svc.object_backend,
                None,
                &context.project_id,
                &get_req,
            )
            .await
            && let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes)
        {
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
    let context = tenant_only_native_service_context(&metadata, &tenant_id);

    // Reuse the existing policy id on update so the upsert is in place.
    let existing = runtime
        .native_entity_read_for_service(
            "backup",
            &context,
            policy_read_by_name(&tenant_id, &policy_name),
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

    runtime
        .native_entity_write_for_service(
            "backup",
            &context,
            BACKUP_POLICY_MSG,
            record,
            policy_conflict(),
        )
        .await?;

    emit_event(
        svc,
        TOPIC_POLICY_UPSERTED,
        &tenant_id,
        &tenant_id,
        &context.project_id,
        &policy_name,
        serde_json::json!({
            "tenant_id": tenant_id,
            "policy_id": policy_id,
            "policy_name": policy_name,
            "enabled": req.enabled,
            "retention_days": req.retention_days,
        }),
    )
    .await;

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
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let policy = runtime
        .native_entity_read_for_service(
            "backup",
            &context,
            policy_read_by_name(&tenant_id, &policy_name),
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
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let limit = clamp_limit(req.page_size);
    let offset = parse_offset(&req.page_token);
    let rows = runtime
        .native_entity_read_for_service(
            "backup",
            &context,
            policies_list_read(&tenant_id, limit, offset),
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
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    runtime
        .native_entity_delete_for_service(
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
                        field: "policy_name".to_string(),
                        op: ComparisonOp::Eq,
                        value: logical_string(&policy_name),
                    },
                ]),
                return_fields: Vec::new(),
            },
        )
        .await?;

    emit_event(
        svc,
        TOPIC_POLICY_DELETED,
        &tenant_id,
        &tenant_id,
        &context.project_id,
        &policy_name,
        serde_json::json!({ "tenant_id": tenant_id, "policy_name": policy_name }),
    )
    .await;

    Ok(Response::new(backup_pb::DeleteBackupPolicyResponse {
        deleted: true,
        message: "backup policy deleted".to_string(),
        error: None,
    }))
}
