//! The five `WorkflowService` RPCs as free functions taking `&WorkflowServiceImpl`.
//! `mod.rs`'s `#[tonic::async_trait] impl WorkflowService` delegates one line each
//! into here. Tenant identity always comes from the VERIFIED claim (never the
//! request body); state is durable; every mutation and its outbox event commit in
//! ONE transaction (16.3.3).

use chrono::Utc;
use sqlx::Row;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::workflow::services::v1 as workflow_pb;
use crate::runtime::canonical_store::system_store::{CompensationStatus, SagaStatus, SagaStore};
use crate::runtime::channels::OperationChannel;
use crate::runtime::saga::{self, SagaKind};

use super::super::native_helpers::{
    admit_on as native_admit_on, metadata_project_id, native_next_page_token_for_total,
    native_offset_page_window, non_empty_json, parse_uuid, validate_request_scope,
};
use super::WorkflowServiceImpl;
use super::config::{
    MAX_COMPENSATIONS_BYTES, MAX_PAYLOAD_BYTES, STATUS_CANCELLED, STATUS_COMPENSATED,
    STATUS_COMPENSATING, TOPIC_CANCELLED, TOPIC_SIGNALED, TOPIC_STARTED,
};
use super::errors::{
    workflow_cancel_terminal_status, workflow_internal_status, workflow_not_found_status,
    workflow_required_field, workflow_signal_terminal_status, workflow_size_field,
};
use super::events::insert_rpc_outbox;
use super::model::{
    clamp_total_steps, is_terminal_status, non_empty_json_array, workflow_from_row, workflow_model,
    workflow_status_filter_to_db,
};
use super::store::{workflow_project_bind, workflow_scope_predicate, workflow_select_projection};

pub(crate) async fn start_workflow(
    svc: &WorkflowServiceImpl,
    request: Request<workflow_pb::StartWorkflowRequest>,
) -> Result<Response<workflow_pb::StartWorkflowResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
    if req.workflow_type.trim().is_empty() {
        return Err(workflow_required_field(
            "workflow_type",
            "must be a non-empty workflow type",
            "workflow_type is required",
        ));
    }
    if req.payload.len() > MAX_PAYLOAD_BYTES {
        return Err(workflow_size_field(
            "payload",
            MAX_PAYLOAD_BYTES,
            format!("payload exceeds {MAX_PAYLOAD_BYTES} bytes"),
        ));
    }
    if req.compensations.len() > MAX_COMPENSATIONS_BYTES {
        return Err(workflow_size_field(
            "compensations",
            MAX_COMPENSATIONS_BYTES,
            format!("compensations exceed {MAX_COMPENSATIONS_BYTES} bytes"),
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "workflow",
        OperationChannel::Admin,
        &req.tenant_id,
        Some(&req.project_id),
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let pool = svc.require_pool()?;
    let m = workflow_model();
    let rel = m.relation.clone();
    let workflow_id = Uuid::new_v4().to_string();
    let total_steps = clamp_total_steps(req.total_steps);
    let payload = non_empty_json(&req.payload);
    let compensations = non_empty_json_array(&req.compensations);
    let correlation_id = if req.correlation_id.trim().is_empty() {
        workflow_id.clone()
    } else {
        req.correlation_id.trim().to_string()
    };

    // REUSE the saga engine: record a durable saga (Pending) carrying this
    // workflow's reverse-order compensations so the EXISTING recovery worker can
    // compensate it on cancel. Best-effort — a slim deployment with no canonical
    // store still gets a durable workflow row (empty saga link).
    let saga_id = if let Some(store) = svc.system_stores() {
        let comps: serde_json::Value =
            serde_json::from_str(&compensations).unwrap_or_else(|_| serde_json::json!([]));
        match saga::start_workflow_saga(
            store.as_ref(),
            SagaKind::Workflow,
            &tenant_id,
            &correlation_id,
            req.workflow_type.trim(),
            comps,
        )
        .await
        {
            Ok(id) => id.to_string(),
            Err(err) => {
                tracing::warn!(error = %err, "workflow saga record failed; continuing without compensation linkage");
                String::new()
            }
        }
    } else {
        String::new()
    };

    // 16.3.3 — the instance INSERT and its `started` outbox event commit in ONE
    // transaction: no dual-write window where a durable workflow exists without
    // its event (or vice versa). The saga record above is cross-store and cannot
    // join this PG transaction; on any failure past it, the orphan saga is
    // settled terminal so the recovery queue never carries a saga whose
    // workflow row never became durable.
    let mut tx = pool.begin().await.map_err(|err| {
        workflow_internal_status(
            "start_workflow_begin",
            format!("start workflow begin failed: {err}"),
        )
    })?;
    if let Err(err) = sqlx::query(&format!(
        "INSERT INTO {rel} \
           ({workflow_id}, {tenant_id}, {project_id}, {workflow_type}, {status}, \
            {current_step}, {total_steps}, {payload}, {compensations}, {correlation_id}, \
            {saga_id}, {next_run_at}, {last_transition_at}) \
         VALUES ($1::UUID, $2::UUID, NULLIF($3, '')::UUID, $4, 'RUNNING', \
            0, $5, $6::JSONB, $7::JSONB, NULLIF($8, ''), \
            NULLIF($9, '')::UUID, NOW(), NOW())",
        workflow_id = m.q("workflow_id"),
        tenant_id = m.q("tenant_id"),
        project_id = m.q("project_id"),
        workflow_type = m.q("workflow_type"),
        status = m.q("status"),
        current_step = m.q("current_step"),
        total_steps = m.q("total_steps"),
        payload = m.q("payload"),
        compensations = m.q("compensations"),
        correlation_id = m.q("correlation_id"),
        saga_id = m.q("saga_id"),
        next_run_at = m.q("next_run_at"),
        last_transition_at = m.q("last_transition_at"),
    ))
    .bind(&workflow_id)
    .bind(&tenant_id)
    .bind(&req.project_id)
    .bind(req.workflow_type.trim())
    .bind(total_steps)
    .bind(&payload)
    .bind(&compensations)
    .bind(&correlation_id)
    .bind(&saga_id)
    .execute(&mut *tx)
    .await
    {
        svc.settle_orphan_saga(&saga_id).await;
        return Err(workflow_internal_status(
            "start_workflow",
            format!("start workflow failed: {err}"),
        ));
    }

    if let Err(status) = insert_rpc_outbox(
        &mut tx,
        svc.outbox_relation.as_deref(),
        TOPIC_STARTED,
        &tenant_id, // partition key = tenant_id (proto method_event_contract)
        &tenant_id,
        &req.project_id,
        &correlation_id,
        "started",
        &workflow_id,
        "start_workflow",
        serde_json::json!({
            "workflow_id": workflow_id.clone(),
            "tenant_id": tenant_id.clone(),
            "project_id": req.project_id.clone(),
            "workflow_type": req.workflow_type.clone(),
            "total_steps": total_steps,
            "saga_id": saga_id.clone(),
        }),
    )
    .await
    {
        svc.settle_orphan_saga(&saga_id).await;
        return Err(status);
    }

    if let Err(err) = tx.commit().await {
        svc.settle_orphan_saga(&saga_id).await;
        return Err(workflow_internal_status(
            "start_workflow_commit",
            format!("start workflow commit failed: {err}"),
        ));
    }

    Ok(Response::new(workflow_pb::StartWorkflowResponse {
        workflow_id,
        message: "workflow started".to_string(),
        error: None,
    }))
}

pub(crate) async fn get_workflow(
    svc: &WorkflowServiceImpl,
    request: Request<workflow_pb::GetWorkflowRequest>,
) -> Result<Response<workflow_pb::GetWorkflowResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // 16.3.1 — GetWorkflowRequest carries no project field (proto verified), so
    // the project scope is resolved from request metadata / the verified claim
    // and bound into both the scope validation and the row predicate.
    // Normalize the metadata/claim-resolved scope: the workflow schema stores
    // project_id as a UUID, so a non-UUID project CODE (e.g. "default") degrades to
    // tenant-wide instead of erroring "invalid input syntax for type uuid".
    let project_id = workflow_project_bind(&metadata_project_id(&metadata).unwrap_or_default());
    validate_request_scope(&metadata, &req.tenant_id, &project_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "workflow",
        OperationChannel::Read,
        &req.tenant_id,
        Some(project_id.as_str()),
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let workflow_id = parse_uuid("workflow_id", &req.workflow_id)?.to_string();
    let pool = svc.require_pool()?;
    let m = workflow_model();
    let rel = m.relation.clone();
    let projection = workflow_select_projection(&m);
    let row = sqlx::query(&format!(
        "SELECT {projection} FROM {rel} \
         WHERE {workflow_id} = $1::UUID AND {scope} AND {deleted} IS NULL",
        workflow_id = m.q("workflow_id"),
        scope = workflow_scope_predicate(&m, "$2", "$3"),
        deleted = m.q("deleted_at"),
    ))
    .bind(&workflow_id)
    .bind(&tenant_id)
    .bind(&project_id)
    .fetch_optional(pool)
    .await
    .map_err(|err| {
        workflow_internal_status("get_workflow", format!("get workflow failed: {err}"))
    })?;
    let workflow = row
        .as_ref()
        .map(workflow_from_row)
        .transpose()?
        .ok_or_else(|| workflow_not_found_status("get_workflow"))?;
    Ok(Response::new(workflow_pb::GetWorkflowResponse {
        workflow: Some(workflow),
        error: None,
    }))
}

pub(crate) async fn list_workflows(
    svc: &WorkflowServiceImpl,
    request: Request<workflow_pb::ListWorkflowsRequest>,
) -> Result<Response<workflow_pb::ListWorkflowsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // 16.3.1 — ListWorkflowsRequest carries no project field (proto verified);
    // resolve the project scope from metadata / the verified claim.
    // Normalize the metadata/claim-resolved scope: the workflow schema stores
    // project_id as a UUID, so a non-UUID project CODE (e.g. "default") degrades to
    // tenant-wide instead of erroring "invalid input syntax for type uuid".
    let project_id = workflow_project_bind(&metadata_project_id(&metadata).unwrap_or_default());
    validate_request_scope(&metadata, &req.tenant_id, &project_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "workflow",
        OperationChannel::Read,
        &req.tenant_id,
        Some(project_id.as_str()),
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let status_filter = workflow_status_filter_to_db(&req.status)?;
    let pool = svc.require_pool()?;
    let m = workflow_model();
    let rel = m.relation.clone();
    let projection = workflow_select_projection(&m);
    let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
    let where_clause = format!(
        "WHERE {scope} AND {deleted} IS NULL AND ($2 = '' OR {status} = $2)",
        scope = workflow_scope_predicate(&m, "$1", "$3"),
        deleted = m.q("deleted_at"),
        status = m.q("status"),
    );
    let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {rel} {where_clause}"))
        .bind(&tenant_id)
        .bind(&status_filter)
        .bind(&project_id)
        .fetch_one(pool)
        .await
        .map_err(|err| {
            workflow_internal_status(
                "list_workflows_count",
                format!("count workflows failed: {err}"),
            )
        })?;
    let rows = sqlx::query(&format!(
        "SELECT {projection} FROM {rel} {where_clause} \
         ORDER BY {next_run_at} DESC NULLS LAST, {workflow_id} LIMIT $4 OFFSET $5",
        next_run_at = m.q("next_run_at"),
        workflow_id = m.q("workflow_id"),
    ))
    .bind(&tenant_id)
    .bind(&status_filter)
    .bind(&project_id)
    .bind(page_window.limit_i64())
    .bind(page_window.offset_i64())
    .fetch_all(pool)
    .await
    .map_err(|err| {
        workflow_internal_status("list_workflows", format!("list workflows failed: {err}"))
    })?;
    let mut workflows = Vec::with_capacity(rows.len());
    for row in &rows {
        workflows.push(workflow_from_row(row)?);
    }
    Ok(Response::new(workflow_pb::ListWorkflowsResponse {
        workflows,
        total_count: total as i32,
        error: None,
        next_page_token: native_next_page_token_for_total(
            page_window.offset,
            page_window.limit,
            total,
        ),
    }))
}

pub(crate) async fn cancel_workflow(
    svc: &WorkflowServiceImpl,
    request: Request<workflow_pb::CancelWorkflowRequest>,
) -> Result<Response<workflow_pb::CancelWorkflowResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // 16.3.1 — CancelWorkflowRequest carries no project field (proto verified);
    // resolve the project scope from metadata / the verified claim and bind it
    // into both queries so a project-scoped caller cannot cancel across projects.
    // Normalize the metadata/claim-resolved scope: the workflow schema stores
    // project_id as a UUID, so a non-UUID project CODE (e.g. "default") degrades to
    // tenant-wide instead of erroring "invalid input syntax for type uuid".
    let project_id = workflow_project_bind(&metadata_project_id(&metadata).unwrap_or_default());
    validate_request_scope(&metadata, &req.tenant_id, &project_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "workflow",
        OperationChannel::Admin,
        &req.tenant_id,
        Some(project_id.as_str()),
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let workflow_id = parse_uuid("workflow_id", &req.workflow_id)?.to_string();
    let pool = svc.require_pool()?;
    let m = workflow_model();
    let rel = m.relation.clone();

    // Load current status + linked saga.
    let row = sqlx::query(&format!(
        "SELECT {status} AS status, COALESCE({saga_id}::TEXT, '') AS saga_id \
         FROM {rel} \
         WHERE {workflow_id} = $1::UUID AND {scope} AND {deleted} IS NULL",
        status = m.q("status"),
        saga_id = m.q("saga_id"),
        workflow_id = m.q("workflow_id"),
        scope = workflow_scope_predicate(&m, "$2", "$3"),
        deleted = m.q("deleted_at"),
    ))
    .bind(&workflow_id)
    .bind(&tenant_id)
    .bind(&project_id)
    .fetch_optional(pool)
    .await
    .map_err(|err| {
        workflow_internal_status(
            "cancel_workflow_load",
            format!("cancel workflow load failed: {err}"),
        )
    })?
    .ok_or_else(|| workflow_not_found_status("cancel_workflow"))?;
    let status: String = row.try_get("status").map_err(|e| {
        workflow_internal_status("cancel_workflow_decode", format!("decode status: {e}"))
    })?;
    let saga_id: String = row.try_get("saga_id").map_err(|e| {
        workflow_internal_status("cancel_workflow_decode", format!("decode saga_id: {e}"))
    })?;

    // Already terminal: COMPLETED/FAILED cannot be cancelled; an
    // already-cancelled/compensated workflow is an idempotent no-op.
    if is_terminal_status(&status) {
        return match status.as_str() {
            STATUS_CANCELLED | STATUS_COMPENSATED => {
                Ok(Response::new(workflow_pb::CancelWorkflowResponse {
                    message: "workflow already cancelled".to_string(),
                    error: None,
                }))
            }
            _ => Err(workflow_cancel_terminal_status()),
        };
    }

    // Keep marking the linked saga (its ROW lifecycle stays with the saga
    // engine: Indeterminate makes the SagaRecoveryWorker settle the saga
    // record). The INSTANCE moves to COMPENSATING and its terminal state is
    // owned by the workflow tick's compensation driver (16.3.2), which emits
    // the reverse-order `compensate.step` events and settles COMPENSATED.
    // With no linked saga there is nothing recorded to undo, so the instance
    // goes straight to CANCELLED.
    let mut new_status = STATUS_CANCELLED;
    if !saga_id.is_empty()
        && let Some(store) = svc.system_stores()
        && let Ok(saga_uuid) = saga_id.parse::<Uuid>()
    {
        match SagaStore::update_saga_status(
            store.as_ref(),
            saga_uuid,
            SagaStatus::Indeterminate,
            CompensationStatus::None,
        )
        .await
        {
            Ok(()) => new_status = STATUS_COMPENSATING,
            Err(err) => {
                tracing::warn!(error = %err, saga_id, "workflow cancel: saga handoff failed; marking cancelled");
            }
        }
    }

    // 16.3.3 — the status flip and its outbox event commit in ONE transaction
    // (no dual-write window between the durable state and the event stream).
    let mut tx = pool.begin().await.map_err(|err| {
        workflow_internal_status(
            "cancel_workflow_begin",
            format!("cancel workflow begin failed: {err}"),
        )
    })?;
    sqlx::query(&format!(
        "UPDATE {rel} SET {status} = $4, {next_run_at} = NULL, {last_error} = $5, \
            {last_transition_at} = NOW() \
         WHERE {workflow_id} = $1::UUID AND {scope} AND {deleted} IS NULL",
        status = m.q("status"),
        next_run_at = m.q("next_run_at"),
        last_error = m.q("last_error"),
        last_transition_at = m.q("last_transition_at"),
        workflow_id = m.q("workflow_id"),
        scope = workflow_scope_predicate(&m, "$2", "$3"),
        deleted = m.q("deleted_at"),
    ))
    .bind(&workflow_id)
    .bind(&tenant_id)
    .bind(&project_id)
    .bind(new_status)
    .bind(req.reason.trim())
    .execute(&mut *tx)
    .await
    .map_err(|err| {
        workflow_internal_status("cancel_workflow", format!("cancel workflow failed: {err}"))
    })?;

    insert_rpc_outbox(
        &mut tx,
        svc.outbox_relation.as_deref(),
        TOPIC_CANCELLED,
        &workflow_id, // partition key = workflow_id (proto method_event_contract)
        &tenant_id,
        &project_id,
        &workflow_id,
        "cancelled",
        &workflow_id,
        "cancel_workflow",
        serde_json::json!({
            "workflow_id": workflow_id.clone(),
            "tenant_id": tenant_id.clone(),
            "project_id": project_id.clone(),
            "status": new_status,
            "saga_id": saga_id.clone(),
            "reason": req.reason.clone(),
        }),
    )
    .await?;
    tx.commit().await.map_err(|err| {
        workflow_internal_status(
            "cancel_workflow_commit",
            format!("cancel workflow commit failed: {err}"),
        )
    })?;

    Ok(Response::new(workflow_pb::CancelWorkflowResponse {
        message: "workflow cancellation requested".to_string(),
        error: None,
    }))
}

pub(crate) async fn signal_workflow(
    svc: &WorkflowServiceImpl,
    request: Request<workflow_pb::SignalWorkflowRequest>,
) -> Result<Response<workflow_pb::SignalWorkflowResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // 16.3.1 — SignalWorkflowRequest carries no project field (proto verified);
    // resolve the project scope from metadata / the verified claim and bind it
    // into both queries so a project-scoped caller cannot signal across projects.
    // Normalize the metadata/claim-resolved scope: the workflow schema stores
    // project_id as a UUID, so a non-UUID project CODE (e.g. "default") degrades to
    // tenant-wide instead of erroring "invalid input syntax for type uuid".
    let project_id = workflow_project_bind(&metadata_project_id(&metadata).unwrap_or_default());
    validate_request_scope(&metadata, &req.tenant_id, &project_id)?;
    if req.signal_name.trim().is_empty() {
        return Err(workflow_required_field(
            "signal_name",
            "must be a non-empty workflow signal name",
            "signal_name is required",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "workflow",
        OperationChannel::Admin,
        &req.tenant_id,
        Some(project_id.as_str()),
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let workflow_id = parse_uuid("workflow_id", &req.workflow_id)?.to_string();
    let pool = svc.require_pool()?;
    let m = workflow_model();
    let rel = m.relation.clone();

    let row = sqlx::query(&format!(
        "SELECT {status} AS status, COALESCE({payload}::TEXT, '') AS payload \
         FROM {rel} \
         WHERE {workflow_id} = $1::UUID AND {scope} AND {deleted} IS NULL",
        status = m.q("status"),
        payload = m.q("payload"),
        workflow_id = m.q("workflow_id"),
        scope = workflow_scope_predicate(&m, "$2", "$3"),
        deleted = m.q("deleted_at"),
    ))
    .bind(&workflow_id)
    .bind(&tenant_id)
    .bind(&project_id)
    .fetch_optional(pool)
    .await
    .map_err(|err| {
        workflow_internal_status(
            "signal_workflow_load",
            format!("signal workflow load failed: {err}"),
        )
    })?
    .ok_or_else(|| workflow_not_found_status("signal_workflow"))?;
    let status: String = row.try_get("status").map_err(|e| {
        workflow_internal_status("signal_workflow_decode", format!("decode status: {e}"))
    })?;
    let payload_text: String = row.try_get("payload").map_err(|e| {
        workflow_internal_status("signal_workflow_decode", format!("decode payload: {e}"))
    })?;
    if is_terminal_status(&status) {
        return Err(workflow_signal_terminal_status());
    }

    // Durably record the delivered signal in the payload's `signals` array and
    // resume forward progress (a waiting step is unblocked on the next tick).
    let mut payload_json: serde_json::Value =
        serde_json::from_str(&payload_text).unwrap_or_else(|_| serde_json::json!({}));
    if !payload_json.is_object() {
        payload_json = serde_json::json!({});
    }
    let signal_payload: serde_json::Value = serde_json::from_str(req.signal_payload.trim())
        .unwrap_or_else(|_| serde_json::Value::String(req.signal_payload.clone()));
    let entry = serde_json::json!({
        "name": req.signal_name.trim(),
        "payload": signal_payload,
        "delivered_at": Utc::now().to_rfc3339(),
    });
    if let Some(obj) = payload_json.as_object_mut() {
        obj.entry("signals")
            .or_insert_with(|| serde_json::json!([]));
        if let Some(arr) = obj.get_mut("signals").and_then(|v| v.as_array_mut()) {
            arr.push(entry);
        }
    }
    let updated_payload = payload_json.to_string();

    // 16.3.3 — the signal writeback and its outbox event commit in ONE
    // transaction (no dual-write window).
    let mut tx = pool.begin().await.map_err(|err| {
        workflow_internal_status(
            "signal_workflow_begin",
            format!("signal workflow begin failed: {err}"),
        )
    })?;
    sqlx::query(&format!(
        "UPDATE {rel} SET {status} = 'RUNNING', {pending_signal} = NULL, \
            {payload} = $4::JSONB, {next_run_at} = NOW(), {last_transition_at} = NOW() \
         WHERE {workflow_id} = $1::UUID AND {scope} AND {deleted} IS NULL",
        status = m.q("status"),
        pending_signal = m.q("pending_signal"),
        payload = m.q("payload"),
        next_run_at = m.q("next_run_at"),
        last_transition_at = m.q("last_transition_at"),
        workflow_id = m.q("workflow_id"),
        scope = workflow_scope_predicate(&m, "$2", "$3"),
        deleted = m.q("deleted_at"),
    ))
    .bind(&workflow_id)
    .bind(&tenant_id)
    .bind(&project_id)
    .bind(&updated_payload)
    .execute(&mut *tx)
    .await
    .map_err(|err| {
        workflow_internal_status("signal_workflow", format!("signal workflow failed: {err}"))
    })?;

    insert_rpc_outbox(
        &mut tx,
        svc.outbox_relation.as_deref(),
        TOPIC_SIGNALED,
        &workflow_id, // partition key = workflow_id (proto method_event_contract)
        &tenant_id,
        &project_id,
        &workflow_id,
        "signaled",
        &workflow_id,
        "signal_workflow",
        serde_json::json!({
            "workflow_id": workflow_id.clone(),
            "tenant_id": tenant_id.clone(),
            "project_id": project_id.clone(),
            "signal_name": req.signal_name.clone(),
        }),
    )
    .await?;
    tx.commit().await.map_err(|err| {
        workflow_internal_status(
            "signal_workflow_commit",
            format!("signal workflow commit failed: {err}"),
        )
    })?;

    Ok(Response::new(workflow_pb::SignalWorkflowResponse {
        message: "signal delivered".to_string(),
        error: None,
    }))
}
