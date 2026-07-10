//! Native `WorkflowService` (master-plan 9.12) — durable multi-step operations with
//! compensation, exposed as a first-class native service.
//!
//! Mirrors `scheduler_service`: proto-driven Postgres CRUD over the UDB-owned
//! `udb_workflow.workflow_instances` table (column identifiers resolved from the
//! embedded proto manifest via [`NativeModel`]), plus a leader-elected tick that
//! claims DUE instances with `FOR UPDATE SKIP LOCKED` and advances them.
//!
//! Doctrine (9.12) — REUSE the saga engine, never a second orchestration loop:
//!   * `StartWorkflow` persists a durable instance, then records a saga via the
//!     EXISTING [`crate::runtime::saga::start_workflow_saga`] seam (which calls
//!     `SagaStore::record_saga`) tagged with [`SagaKind::Workflow`]. The instance
//!     is the durable state that survives a restart / leader change.
//!   * `CancelWorkflow` flips that linked saga into the recoverable state, so the
//!     EXISTING [`crate::runtime::saga::SagaRecoveryWorker`] performs the
//!     reverse-order compensation — this service never reimplements compensation.
//!   * `SignalWorkflow` delivers an external signal to a waiting step, resuming it.
//!   * The leader-elected tick fires transition events ONLY (it never executes a
//!     payload in-process), exactly like the scheduler tick.
//!
//! Every state transition emits one versioned dot-topic outbox event
//! (`udb.workflow.<state>.v1`). Tenant identity always comes from the VERIFIED
//! claim (never the request body); state is durable; the service fails closed on a
//! missing pool, runtime, or outbox relation.

use std::sync::Arc;

use chrono::Utc;
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::workflow::entity::v1 as wf_pb;
use crate::proto::udb::core::workflow::services::v1 as workflow_pb;
use crate::proto::udb::core::workflow::services::v1::workflow_service_server::WorkflowService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::canonical_store::SystemStores;
use crate::runtime::canonical_store::system_store::{CompensationStatus, SagaStatus, SagaStore};
use crate::runtime::channels::{ChannelManager, OperationChannel};
use crate::runtime::native_catalog::{NativeModel, native_model};
use crate::runtime::saga::{self, SagaKind};

pub use crate::proto::udb::core::workflow::services::v1::workflow_service_server::WorkflowServiceServer;

use super::DataBrokerService;
use super::auth_service::events::{ComplianceEnvelope, build_native_compliance_envelope};
use super::native_helpers::{
    MAX_LIST_ROWS, NativeEventContext, admit_on as native_admit_on,
    enqueue_outbox_event_with_context, native_next_page_token_for_total, native_offset_page_window,
    non_empty_json, parse_uuid, validate_request_scope,
};

const WORKFLOW_MSG: &str = "udb.core.workflow.entity.v1.WorkflowInstance";

// ── outbox topics (one per state transition) ───────────────────────────────────
const TOPIC_STARTED: &str = "udb.workflow.started.v1";
const TOPIC_STEP_ADVANCED: &str = "udb.workflow.step.advanced.v1";
const TOPIC_COMPLETED: &str = "udb.workflow.completed.v1";
const TOPIC_SIGNALED: &str = "udb.workflow.signaled.v1";
const TOPIC_CANCELLED: &str = "udb.workflow.cancelled.v1";

// ── stored status tokens (VARCHAR, short form) ─────────────────────────────────
// RUNNING/COMPLETED are written as SQL literals in the tick UPDATEs; only the two
// cancel-path targets need named constants here.
const STATUS_COMPENSATING: &str = "COMPENSATING";
const STATUS_CANCELLED: &str = "CANCELLED";

/// Upper bound on forward steps per workflow (master-plan 9.12: ≤20 steps). A
/// request beyond this is clamped down rather than rejected.
const MAX_WORKFLOW_STEPS: i32 = 20;
/// Upper bound on the opaque payload (master-plan 9.12: ≤8 KiB). Bounds the durable
/// row and the fired event so one workflow cannot bloat the outbox.
const MAX_PAYLOAD_BYTES: usize = 8 * 1024;
/// Same bound on the compensation list handed to the saga engine.
const MAX_COMPENSATIONS_BYTES: usize = 8 * 1024;

/// Default batch the tick advances per pass — a named constant (no per-request env
/// reads). Bounds how many DUE instances one tick advances so a backlog can't
/// starve the transaction. Referenced by the serve() leader-spawn block (follow-up).
#[allow(dead_code)]
pub(crate) const WORKFLOW_TICK_BATCH: i64 = 200;

/// Postgres-backed `WorkflowService` handler.
pub struct WorkflowServiceImpl {
    pg_pool: Option<PgPool>,
    /// Runtime handle for the default `SystemStores` (the saga engine the workflow
    /// is handed to). `None` only in bare unit-test construction.
    runtime: Option<Arc<DataBrokerRuntime>>,
    /// Transactional-outbox relation; mutations and tick transitions enqueue here.
    outbox_relation: Option<String>,
    /// Per-tenant fair-admission manager (same one the data plane uses). `None`
    /// only in bare unit-test construction (admit becomes a no-op).
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
}

fn workflow_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "workflow",
        operation,
        capability_required,
        message,
    )
}

fn workflow_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message)
}

fn workflow_not_found_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "workflow",
        operation,
        "workflow_not_found",
        "workflow not found",
    )
}

fn workflow_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("workflow", operation, message)
}

impl WorkflowServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Workflow CRUD is durable-only: fail closed when no Postgres pool exists.
    fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            workflow_capability_status(
                "postgres_store",
                "postgres_store",
                "workflow service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }

    /// The default `SystemStores` (the saga engine). When absent (slim deployment)
    /// a workflow is still durable in its own table but has no linked saga, so
    /// compensation on cancel is unavailable — `StartWorkflow` records that as an
    /// empty `saga_id` rather than failing the create.
    fn system_stores(&self) -> Option<Arc<dyn SystemStores>> {
        self.runtime.as_ref()?.default_system_stores()
    }
}

impl Default for WorkflowServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

fn workflow_model() -> NativeModel {
    native_model(
        WORKFLOW_MSG,
        &[
            "workflow_id",
            "tenant_id",
            "project_id",
            "workflow_type",
            "status",
            "current_step",
            "total_steps",
            "payload",
            "compensations",
            "correlation_id",
            "saga_id",
            "pending_signal",
            "last_error",
            "next_run_at",
            "last_transition_at",
            "deleted_at",
            "deleted_by",
        ],
    )
}

// ── enum <-> db ────────────────────────────────────────────────────────────────

fn workflow_status_from_db(value: &str) -> i32 {
    use wf_pb::WorkflowStatus as S;
    let v = match value {
        "PENDING" | "WORKFLOW_STATUS_PENDING" => S::Pending,
        "RUNNING" | "WORKFLOW_STATUS_RUNNING" => S::Running,
        "WAITING_SIGNAL" | "WORKFLOW_STATUS_WAITING_SIGNAL" => S::WaitingSignal,
        "COMPLETED" | "WORKFLOW_STATUS_COMPLETED" => S::Completed,
        "COMPENSATING" | "WORKFLOW_STATUS_COMPENSATING" => S::Compensating,
        "COMPENSATED" | "WORKFLOW_STATUS_COMPENSATED" => S::Compensated,
        "CANCELLED" | "WORKFLOW_STATUS_CANCELLED" => S::Cancelled,
        "FAILED" | "WORKFLOW_STATUS_FAILED" => S::Failed,
        _ => S::Unspecified,
    };
    v as i32
}

/// Normalize a status filter string to the canonical SHORT stored token. Empty →
/// empty (no filter). Unknown non-empty input is rejected (fail closed).
fn workflow_status_filter_to_db(value: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(String::new());
    }
    match v.to_ascii_uppercase().as_str() {
        "PENDING" | "WORKFLOW_STATUS_PENDING" => Ok("PENDING".to_string()),
        "RUNNING" | "WORKFLOW_STATUS_RUNNING" => Ok("RUNNING".to_string()),
        "WAITING_SIGNAL" | "WORKFLOW_STATUS_WAITING_SIGNAL" => Ok("WAITING_SIGNAL".to_string()),
        "COMPLETED" | "WORKFLOW_STATUS_COMPLETED" => Ok("COMPLETED".to_string()),
        "COMPENSATING" | "WORKFLOW_STATUS_COMPENSATING" => Ok("COMPENSATING".to_string()),
        "COMPENSATED" | "WORKFLOW_STATUS_COMPENSATED" => Ok("COMPENSATED".to_string()),
        "CANCELLED" | "WORKFLOW_STATUS_CANCELLED" => Ok("CANCELLED".to_string()),
        "FAILED" | "WORKFLOW_STATUS_FAILED" => Ok("FAILED".to_string()),
        other => Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!("unknown workflow status filter: {other}"),
            [("status_filter", "must be a known workflow status")],
        )),
    }
}

fn workflow_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

fn workflow_size_field(field: &'static str, limit: usize, message: String) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field, format!("must be no larger than {limit} bytes"))],
    )
}

fn workflow_cancel_terminal_status() -> Status {
    workflow_policy_status(
        "cancel_workflow",
        "workflow_terminal_state",
        "workflow is in a terminal state and cannot be cancelled",
    )
}

fn workflow_signal_terminal_status() -> Status {
    workflow_policy_status(
        "signal_workflow",
        "workflow_terminal_state",
        "workflow is in a terminal state and cannot be signalled",
    )
}

/// A terminal status is never re-advanced and cannot be cancelled again.
fn is_terminal_status(status: &str) -> bool {
    matches!(status, "COMPLETED" | "COMPENSATED" | "CANCELLED" | "FAILED")
}

/// The outbox topic a forward advance emits: the terminal step fires
/// `completed`, every intermediate step fires `step.advanced`. Pure — the tick
/// uses it so the "right event per transition" contract is unit-testable.
fn advance_event_topic(to_completed: bool) -> &'static str {
    if to_completed {
        TOPIC_COMPLETED
    } else {
        TOPIC_STEP_ADVANCED
    }
}

fn clamp_total_steps(requested: i32) -> i32 {
    requested.clamp(1, MAX_WORKFLOW_STEPS)
}

/// Default an empty compensation field to a JSON array (the saga engine expects a
/// JSON array of compensation payloads), unlike [`non_empty_json`] which defaults
/// to an object.
fn non_empty_json_array(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        "[]".to_string()
    } else {
        trimmed.to_string()
    }
}

fn epoch_to_ts(epoch: Option<i64>) -> Option<prost_types::Timestamp> {
    epoch.map(|seconds| prost_types::Timestamp { seconds, nanos: 0 })
}

// ── projection + row mapping ────────────────────────────────────────────────────

fn workflow_select_projection(m: &NativeModel) -> String {
    [
        m.text("workflow_id"),
        m.text("tenant_id"),
        m.text_or_empty("project_id"),
        m.select("workflow_type"),
        m.select("status"),
        m.select("current_step"),
        m.select("total_steps"),
        m.text_or_empty("payload"),
        m.text_or_empty("compensations"),
        m.text_or_empty("correlation_id"),
        m.text_or_empty("saga_id"),
        m.text_or_empty("pending_signal"),
        m.text_or_empty("last_error"),
        format!(
            "EXTRACT(EPOCH FROM {})::BIGINT AS next_run_at_epoch",
            m.q("next_run_at")
        ),
        format!(
            "EXTRACT(EPOCH FROM {})::BIGINT AS last_transition_at_epoch",
            m.q("last_transition_at")
        ),
    ]
    .join(", ")
}

fn workflow_from_row(row: &sqlx::postgres::PgRow) -> Result<wf_pb::WorkflowInstance, Status> {
    let map = |e: sqlx::Error| {
        workflow_internal_status(
            "decode_workflow_instance",
            format!("decode workflow instance failed: {e}"),
        )
    };
    Ok(wf_pb::WorkflowInstance {
        workflow_id: row.try_get("workflow_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        project_id: row.try_get("project_id").map_err(map)?,
        workflow_type: row.try_get("workflow_type").map_err(map)?,
        status: workflow_status_from_db(&row.try_get::<String, _>("status").map_err(map)?),
        current_step: row.try_get("current_step").map_err(map)?,
        total_steps: row.try_get("total_steps").map_err(map)?,
        payload: row.try_get("payload").map_err(map)?,
        compensations: row.try_get("compensations").map_err(map)?,
        correlation_id: row.try_get("correlation_id").map_err(map)?,
        saga_id: row.try_get("saga_id").map_err(map)?,
        pending_signal: row.try_get("pending_signal").map_err(map)?,
        last_error: row.try_get("last_error").map_err(map)?,
        next_run_at: epoch_to_ts(
            row.try_get::<Option<i64>, _>("next_run_at_epoch")
                .map_err(map)?,
        ),
        last_transition_at: epoch_to_ts(
            row.try_get::<Option<i64>, _>("last_transition_at_epoch")
                .map_err(map)?,
        ),
        ..Default::default()
    })
}

#[tonic::async_trait]
impl WorkflowService for WorkflowServiceImpl {
    async fn start_workflow(
        &self,
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
            self.channels.as_ref(),
            &self.metrics,
            "workflow",
            OperationChannel::Admin,
            &req.tenant_id,
            Some(&req.project_id),
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let pool = self.require_pool()?;
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
        let saga_id = if let Some(store) = self.system_stores() {
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

        sqlx::query(&format!(
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
        .execute(pool)
        .await
        .map_err(|err| {
            workflow_internal_status("start_workflow", format!("start workflow failed: {err}"))
        })?;

        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_STARTED,
            &tenant_id,
            &tenant_id,
            &req.project_id,
            serde_json::json!({
                "workflow_id": workflow_id.clone(),
                "tenant_id": tenant_id.clone(),
                "project_id": req.project_id.clone(),
                "workflow_type": req.workflow_type.clone(),
                "total_steps": total_steps,
                "saga_id": saga_id.clone(),
            }),
            NativeEventContext {
                target_resource: workflow_id.clone(),
                correlation_id: correlation_id.clone(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;

        Ok(Response::new(workflow_pb::StartWorkflowResponse {
            workflow_id,
            message: "workflow started".to_string(),
            error: None,
        }))
    }

    async fn get_workflow(
        &self,
        request: Request<workflow_pb::GetWorkflowRequest>,
    ) -> Result<Response<workflow_pb::GetWorkflowResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_scope(&metadata, &req.tenant_id, "")?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "workflow",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let workflow_id = parse_uuid("workflow_id", &req.workflow_id)?.to_string();
        let pool = self.require_pool()?;
        let m = workflow_model();
        let rel = m.relation.clone();
        let projection = workflow_select_projection(&m);
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} \
             WHERE {workflow_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL",
            workflow_id = m.q("workflow_id"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(&workflow_id)
        .bind(&tenant_id)
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

    async fn list_workflows(
        &self,
        request: Request<workflow_pb::ListWorkflowsRequest>,
    ) -> Result<Response<workflow_pb::ListWorkflowsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_scope(&metadata, &req.tenant_id, "")?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "workflow",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let status_filter = workflow_status_filter_to_db(&req.status)?;
        let pool = self.require_pool()?;
        let m = workflow_model();
        let rel = m.relation.clone();
        let projection = workflow_select_projection(&m);
        let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
        let where_clause = format!(
            "WHERE {tenant_id} = $1::UUID AND {deleted} IS NULL AND ($2 = '' OR {status} = $2)",
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
            status = m.q("status"),
        );
        let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {rel} {where_clause}"))
            .bind(&tenant_id)
            .bind(&status_filter)
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
             ORDER BY {next_run_at} DESC NULLS LAST, {workflow_id} LIMIT $3 OFFSET $4",
            next_run_at = m.q("next_run_at"),
            workflow_id = m.q("workflow_id"),
        ))
        .bind(&tenant_id)
        .bind(&status_filter)
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

    async fn cancel_workflow(
        &self,
        request: Request<workflow_pb::CancelWorkflowRequest>,
    ) -> Result<Response<workflow_pb::CancelWorkflowResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_scope(&metadata, &req.tenant_id, "")?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "workflow",
            OperationChannel::Admin,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let workflow_id = parse_uuid("workflow_id", &req.workflow_id)?.to_string();
        let pool = self.require_pool()?;
        let m = workflow_model();
        let rel = m.relation.clone();

        // Load current status + linked saga.
        let row = sqlx::query(&format!(
            "SELECT {status} AS status, COALESCE({saga_id}::TEXT, '') AS saga_id \
             FROM {rel} \
             WHERE {workflow_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL",
            status = m.q("status"),
            saga_id = m.q("saga_id"),
            workflow_id = m.q("workflow_id"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(&workflow_id)
        .bind(&tenant_id)
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
                STATUS_CANCELLED | "COMPENSATED" => {
                    Ok(Response::new(workflow_pb::CancelWorkflowResponse {
                        message: "workflow already cancelled".to_string(),
                        error: None,
                    }))
                }
                _ => Err(workflow_cancel_terminal_status()),
            };
        }

        // REUSE the saga compensation path: flip the linked saga to Indeterminate
        // so the EXISTING SagaRecoveryWorker runs its reverse-order compensations.
        // The instance moves to COMPENSATING; with no linked saga there is nothing
        // to compensate, so it goes straight to CANCELLED.
        let mut new_status = STATUS_CANCELLED;
        if !saga_id.is_empty()
            && let Some(store) = self.system_stores()
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

        sqlx::query(&format!(
            "UPDATE {rel} SET {status} = $3, {next_run_at} = NULL, {last_error} = $4, \
                {last_transition_at} = NOW() \
             WHERE {workflow_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL",
            status = m.q("status"),
            next_run_at = m.q("next_run_at"),
            last_error = m.q("last_error"),
            last_transition_at = m.q("last_transition_at"),
            workflow_id = m.q("workflow_id"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(&workflow_id)
        .bind(&tenant_id)
        .bind(new_status)
        .bind(req.reason.trim())
        .execute(pool)
        .await
        .map_err(|err| {
            workflow_internal_status("cancel_workflow", format!("cancel workflow failed: {err}"))
        })?;

        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_CANCELLED,
            &workflow_id,
            &tenant_id,
            "",
            serde_json::json!({
                "workflow_id": workflow_id.clone(),
                "tenant_id": tenant_id.clone(),
                "status": new_status,
                "saga_id": saga_id.clone(),
                "reason": req.reason.clone(),
            }),
            NativeEventContext {
                target_resource: workflow_id.clone(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;

        Ok(Response::new(workflow_pb::CancelWorkflowResponse {
            message: "workflow cancellation requested".to_string(),
            error: None,
        }))
    }

    async fn signal_workflow(
        &self,
        request: Request<workflow_pb::SignalWorkflowRequest>,
    ) -> Result<Response<workflow_pb::SignalWorkflowResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_scope(&metadata, &req.tenant_id, "")?;
        if req.signal_name.trim().is_empty() {
            return Err(workflow_required_field(
                "signal_name",
                "must be a non-empty workflow signal name",
                "signal_name is required",
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "workflow",
            OperationChannel::Admin,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let workflow_id = parse_uuid("workflow_id", &req.workflow_id)?.to_string();
        let pool = self.require_pool()?;
        let m = workflow_model();
        let rel = m.relation.clone();

        let row = sqlx::query(&format!(
            "SELECT {status} AS status, COALESCE({payload}::TEXT, '') AS payload \
             FROM {rel} \
             WHERE {workflow_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL",
            status = m.q("status"),
            payload = m.q("payload"),
            workflow_id = m.q("workflow_id"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(&workflow_id)
        .bind(&tenant_id)
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

        sqlx::query(&format!(
            "UPDATE {rel} SET {status} = 'RUNNING', {pending_signal} = NULL, \
                {payload} = $3::JSONB, {next_run_at} = NOW(), {last_transition_at} = NOW() \
             WHERE {workflow_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL",
            status = m.q("status"),
            pending_signal = m.q("pending_signal"),
            payload = m.q("payload"),
            next_run_at = m.q("next_run_at"),
            last_transition_at = m.q("last_transition_at"),
            workflow_id = m.q("workflow_id"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(&workflow_id)
        .bind(&tenant_id)
        .bind(&updated_payload)
        .execute(pool)
        .await
        .map_err(|err| {
            workflow_internal_status("signal_workflow", format!("signal workflow failed: {err}"))
        })?;

        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_SIGNALED,
            &workflow_id,
            &tenant_id,
            "",
            serde_json::json!({
                "workflow_id": workflow_id.clone(),
                "tenant_id": tenant_id.clone(),
                "signal_name": req.signal_name.clone(),
            }),
            NativeEventContext {
                target_resource: workflow_id.clone(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;

        Ok(Response::new(workflow_pb::SignalWorkflowResponse {
            message: "signal delivered".to_string(),
            error: None,
        }))
    }
}

// ── leader-elected tick ─────────────────────────────────────────────────────────

/// The `SELECT ... FOR UPDATE SKIP LOCKED` statement the tick uses to claim DUE
/// RUNNING workflow instances. Built from the manifest model so column identifiers
/// stay single-sourced. Exposed (and unit-tested) so the no-double-advance contract
/// is asserted on the rendered SQL.
pub(crate) fn due_workflows_claim_sql(m: &NativeModel) -> String {
    let rel = m.relation.clone();
    format!(
        "SELECT {workflow_id}::text AS workflow_id, {tenant_id}::text AS tenant_id, \
            COALESCE({project_id}::text, '') AS project_id, {workflow_type} AS workflow_type, \
            COALESCE({payload}::text, '') AS payload, COALESCE({saga_id}::text, '') AS saga_id, \
            {current_step} AS current_step, {total_steps} AS total_steps \
         FROM {rel} \
         WHERE {status} = 'RUNNING' AND {deleted} IS NULL \
           AND {next_run_at} IS NOT NULL AND {next_run_at} <= NOW() \
         ORDER BY {next_run_at} \
         LIMIT $1 \
         FOR UPDATE SKIP LOCKED",
        workflow_id = m.q("workflow_id"),
        tenant_id = m.q("tenant_id"),
        project_id = m.q("project_id"),
        workflow_type = m.q("workflow_type"),
        payload = m.q("payload"),
        saga_id = m.q("saga_id"),
        current_step = m.q("current_step"),
        total_steps = m.q("total_steps"),
        status = m.q("status"),
        deleted = m.q("deleted_at"),
        next_run_at = m.q("next_run_at"),
    )
}

/// One workflow-tick pass (leader-elected by the caller). Claims up to `batch_size`
/// DUE RUNNING instances with `FOR UPDATE SKIP LOCKED`, then — within the SAME
/// transaction — advances each instance's `current_step` and durably enqueues a
/// transition outbox row (`step.advanced`, or `completed` on the terminal step).
/// Because the advance and the outbox insert commit atomically, an instance is
/// never double-advanced and every transition is at-least-once via the outbox→CDC
/// pipeline. The tick FIRES EVENTS ONLY; it never runs a payload in-process — it is
/// the workflow counterpart of the scheduler tick and adds no second orchestration
/// loop (compensation remains the EXISTING SagaRecoveryWorker path).
///
/// After commit, the saga rows of completed workflows are marked `Committed`
/// (terminal) on the saga engine — best-effort, cross-store, so it cannot fail the
/// durable advance. Fail closed: a missing outbox relation yields `Ok(0)`.
pub(crate) async fn run_workflow_tick_once(
    pool: &PgPool,
    outbox_relation: Option<&str>,
    stores: Option<Arc<dyn SystemStores>>,
    batch_size: i64,
) -> Result<i64, Status> {
    let Some(outbox_rel) = outbox_relation else {
        tracing::warn!("workflow tick: no outbox relation configured; cannot advance workflows");
        return Ok(0);
    };
    let m = workflow_model();
    let wf_rel = m.relation.clone();
    let claim_sql = due_workflows_claim_sql(&m);
    let batch = batch_size.clamp(1, MAX_LIST_ROWS);

    let mut tx = pool.begin().await.map_err(|err| {
        workflow_internal_status(
            "workflow_tick_begin",
            format!("workflow tick begin failed: {err}"),
        )
    })?;
    let rows = sqlx::query(&claim_sql)
        .bind(batch)
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| {
            workflow_internal_status(
                "workflow_tick_claim",
                format!("workflow tick claim failed: {err}"),
            )
        })?;

    let now = Utc::now();
    let mut acted = 0i64;
    let mut completed_sagas: Vec<Uuid> = Vec::new();
    for row in &rows {
        let get = |c: &str| -> Result<String, Status> {
            row.try_get::<String, _>(c).map_err(|e| {
                workflow_internal_status(
                    "workflow_tick_decode",
                    format!("workflow tick decode {c} failed: {e}"),
                )
            })
        };
        let workflow_id = get("workflow_id")?;
        let tenant_id = get("tenant_id")?;
        let project_id = get("project_id")?;
        let workflow_type = get("workflow_type")?;
        let payload = get("payload")?;
        let saga_id = get("saga_id")?;
        let current_step: i32 = row.try_get("current_step").map_err(|e| {
            workflow_internal_status(
                "workflow_tick_decode",
                format!("workflow tick decode current_step: {e}"),
            )
        })?;
        let total_steps: i32 = row.try_get("total_steps").map_err(|e| {
            workflow_internal_status(
                "workflow_tick_decode",
                format!("workflow tick decode total_steps: {e}"),
            )
        })?;

        let new_step = current_step.saturating_add(1);
        let completed = new_step >= total_steps;
        let topic = advance_event_topic(completed);
        let payload_json: serde_json::Value =
            serde_json::from_str(&payload).unwrap_or(serde_json::Value::Null);
        let event_payload = serde_json::json!({
            "workflow_id": workflow_id.clone(),
            "tenant_id": tenant_id.clone(),
            "project_id": project_id.clone(),
            "workflow_type": workflow_type.clone(),
            "current_step": new_step,
            "total_steps": total_steps,
            "completed": completed,
            "payload": payload_json,
            "advanced_at": now.to_rfc3339(),
        });
        insert_tick_outbox(
            &mut tx,
            outbox_rel,
            topic,
            &tenant_id,
            &project_id,
            &workflow_id,
            event_payload,
            if completed { "completed" } else { "advanced" },
        )
        .await?;

        if completed {
            sqlx::query(&format!(
                "UPDATE {wf_rel} SET {status} = 'COMPLETED', {current_step} = $2, \
                    {next_run_at} = NULL, {last_transition_at} = NOW() WHERE {workflow_id} = $1::UUID",
                status = m.q("status"),
                current_step = m.q("current_step"),
                next_run_at = m.q("next_run_at"),
                last_transition_at = m.q("last_transition_at"),
                workflow_id = m.q("workflow_id"),
            ))
            .bind(&workflow_id)
            .bind(new_step)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                workflow_internal_status(
                    "workflow_tick_complete_update",
                    format!("workflow tick complete update failed: {e}"),
                )
            })?;
            if let Ok(saga_uuid) = saga_id.parse::<Uuid>() {
                completed_sagas.push(saga_uuid);
            }
        } else {
            sqlx::query(&format!(
                "UPDATE {wf_rel} SET {current_step} = $2, {next_run_at} = NOW(), \
                    {last_transition_at} = NOW() WHERE {workflow_id} = $1::UUID",
                current_step = m.q("current_step"),
                next_run_at = m.q("next_run_at"),
                last_transition_at = m.q("last_transition_at"),
                workflow_id = m.q("workflow_id"),
            ))
            .bind(&workflow_id)
            .bind(new_step)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                workflow_internal_status(
                    "workflow_tick_advance_update",
                    format!("workflow tick advance update failed: {e}"),
                )
            })?;
        }
        acted += 1;
    }

    tx.commit().await.map_err(|err| {
        workflow_internal_status(
            "workflow_tick_commit",
            format!("workflow tick commit failed: {err}"),
        )
    })?;

    // Best-effort, cross-store: settle the saga rows of completed workflows to the
    // terminal `Committed` state on the EXISTING saga engine so they are not left in
    // the `Pending` queue. A failure here never undoes the durable advance above.
    if let Some(store) = stores {
        for saga_uuid in completed_sagas {
            if let Err(err) = SagaStore::update_saga_status(
                store.as_ref(),
                saga_uuid,
                SagaStatus::Committed,
                CompensationStatus::None,
            )
            .await
            {
                tracing::debug!(error = %err, saga_id = %saga_uuid, "workflow tick: saga commit settle failed");
            }
        }
    }
    Ok(acted)
}

/// Insert ONE tick outbox row inside the tick transaction (transactional outbox),
/// using the SAME shared compliance envelope the auth/native lanes emit so the CDC
/// tailer decodes it identically. The actor is the workflow worker (a system
/// principal), not an end user.
async fn insert_tick_outbox(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    relation: &str,
    topic: &str,
    tenant_id: &str,
    project_id: &str,
    workflow_id: &str,
    payload: serde_json::Value,
    operation: &str,
) -> Result<(), Status> {
    let env = ComplianceEnvelope {
        actor: "udb:workflow".to_string(),
        operation: operation.to_string(),
        outcome: "success".to_string(),
        auth_method: "system".to_string(),
        ..ComplianceEnvelope::default()
    };
    let event_id = Uuid::new_v4();
    let envelope = build_native_compliance_envelope(
        &event_id.to_string(),
        topic,
        workflow_id, // partition key = workflow_id (per-workflow ordering)
        tenant_id,
        project_id,
        &env,
        workflow_id, // correlation id
        "none",
        1,
        &[],
        payload,
    );
    crate::runtime::cdc::insert_outbox_row(
        &mut **tx, relation, event_id, topic, tenant_id, &envelope,
    )
    .await
    .map_err(|e| {
        workflow_internal_status(
            "workflow_tick_outbox_insert",
            format!("workflow tick outbox insert failed: {e}"),
        )
    })
}

impl DataBrokerService {
    /// Build the native `WorkflowService`, wired to the broker's Postgres pool, the
    /// runtime (for the saga engine), and the transactional outbox. The
    /// leader-elected tick worker (`run_workflow_tick_once`) is spawned by `serve()`
    /// (not here) under `NativeWorkerHost::spawn_while_leader(WORKER_WORKFLOW_TICK,
    /// ...)`, so a non-leader replica never advances.
    pub(crate) fn build_workflow_service(&self) -> WorkflowServiceImpl {
        let runtime = self.runtime.load_full();
        let pg_pool = runtime
            .native_store_pool_for_service("workflow", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        WorkflowServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}

#[cfg(test)]
mod workflow_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::metadata::MetadataValue;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail metadata");
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

    fn assert_schema_not_found_detail(status: &Status, operation: &str) {
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), "workflow not found");
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "workflow");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, "workflow_not_found");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "workflow");
        assert_eq!(detail.operation, operation);
        assert!(detail.capability_required.is_empty());
        assert!(detail.policy_decision_id.is_empty());
        assert!(detail.field_violations.is_empty());
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    /// A caller scoped to tenant-a must not start/operate on tenant-b's workflow by
    /// putting a foreign tenant_id in the request BODY; the scope guard rejects this
    /// before any pool/DB access (no Postgres needed) — mirrors `scheduler_service`.
    #[tokio::test]
    async fn start_workflow_rejects_cross_tenant_body() {
        let svc = WorkflowServiceImpl::new(); // no pool/runtime/channels (admit no-op)
        let mut request = Request::new(workflow_pb::StartWorkflowRequest {
            tenant_id: "tenant-b".to_string(),
            workflow_type: "order_fulfillment".to_string(),
            total_steps: 3,
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .start_workflow(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn start_workflow_missing_type_carries_field_violation() {
        let svc = WorkflowServiceImpl::new(); // no pool/runtime; validation runs first
        let mut request = Request::new(workflow_pb::StartWorkflowRequest {
            tenant_id: "tenant-a".to_string(),
            workflow_type: " ".to_string(),
            total_steps: 3,
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

        let err = svc
            .start_workflow(request)
            .await
            .expect_err("missing workflow type must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "workflow_type is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "workflow_type");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty workflow type"
        );
    }

    #[tokio::test]
    async fn start_workflow_oversized_payload_carries_field_violation() {
        let svc = WorkflowServiceImpl::new(); // no pool/runtime; validation runs first
        let mut request = Request::new(workflow_pb::StartWorkflowRequest {
            tenant_id: "tenant-a".to_string(),
            workflow_type: "order_fulfillment".to_string(),
            total_steps: 3,
            payload: "x".repeat(MAX_PAYLOAD_BYTES + 1),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

        let err = svc
            .start_workflow(request)
            .await
            .expect_err("oversized payload must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "payload exceeds 8192 bytes");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "payload");
        assert_eq!(
            detail.field_violations[0].description,
            "must be no larger than 8192 bytes"
        );
    }

    #[tokio::test]
    async fn signal_workflow_missing_signal_name_carries_field_violation() {
        let svc = WorkflowServiceImpl::new(); // no pool/runtime; validation runs first
        let mut request = Request::new(workflow_pb::SignalWorkflowRequest {
            tenant_id: "tenant-a".to_string(),
            workflow_id: String::new(),
            signal_name: " ".to_string(),
            signal_payload: String::new(),
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

        let err = svc
            .signal_workflow(request)
            .await
            .expect_err("missing signal name must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "signal_name is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "signal_name");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty workflow signal name"
        );
    }

    #[test]
    fn workflow_status_filter_unknown_value_carries_field_violation() {
        let err = workflow_status_filter_to_db("stuck")
            .expect_err("unknown status filter must fail closed");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "unknown workflow status filter: STUCK");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "status_filter");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a known workflow status"
        );
    }

    #[test]
    fn workflow_missing_postgres_capability_carries_typed_detail() {
        let err = workflow_capability_status(
            "postgres_store",
            "postgres_store",
            "workflow service requires a Postgres-backed store (no PG pool configured)",
        );
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "workflow service requires a Postgres-backed store (no PG pool configured)"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "workflow");
        assert_eq!(detail.operation, "postgres_store");
        assert_eq!(detail.capability_required, "postgres_store");
        assert!(!detail.retryable);
    }

    #[test]
    fn workflow_terminal_cancel_and_signal_denials_carry_policy_detail() {
        let cancel = workflow_cancel_terminal_status();
        assert_eq!(cancel.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            cancel.message(),
            "workflow is in a terminal state and cannot be cancelled"
        );
        assert_policy_detail(&cancel, "cancel_workflow", "workflow_terminal_state");

        let signal = workflow_signal_terminal_status();
        assert_eq!(signal.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            signal.message(),
            "workflow is in a terminal state and cannot be signalled"
        );
        assert_policy_detail(&signal, "signal_workflow", "workflow_terminal_state");
    }

    #[test]
    fn workflow_not_found_statuses_carry_schema_detail() {
        for operation in ["get_workflow", "cancel_workflow", "signal_workflow"] {
            assert_schema_not_found_detail(&workflow_not_found_status(operation), operation);
        }
    }

    #[test]
    fn workflow_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &workflow_internal_status(
                "workflow_tick_claim",
                "workflow tick claim failed: database is unavailable",
            ),
            "workflow_tick_claim",
            "workflow tick claim failed: database is unavailable",
        );
    }

    /// The due-claim SQL MUST use `FOR UPDATE SKIP LOCKED` so two leaders can never
    /// double-advance the same workflow, and must filter to RUNNING, non-deleted,
    /// due rows.
    #[test]
    fn due_claim_sql_uses_skip_locked() {
        let sql = due_workflows_claim_sql(&workflow_model());
        assert!(
            sql.contains("FOR UPDATE SKIP LOCKED"),
            "claim must skip locked rows to avoid double-advance: {sql}"
        );
        assert!(
            sql.contains("'RUNNING'"),
            "claim must only take RUNNING instances"
        );
        assert!(
            sql.contains("IS NULL"),
            "claim must exclude soft-deleted rows"
        );
        assert!(
            sql.contains("<= NOW()"),
            "claim must only take instances whose next_run_at is due"
        );
    }

    /// A state transition emits the right event: an intermediate advance fires
    /// `step.advanced`, the terminal advance fires `completed`. Pure — no PG.
    #[test]
    fn transition_emits_correct_event_topic() {
        assert_eq!(advance_event_topic(false), TOPIC_STEP_ADVANCED);
        assert_eq!(advance_event_topic(true), TOPIC_COMPLETED);
        assert_eq!(TOPIC_STEP_ADVANCED, "udb.workflow.step.advanced.v1");
        assert_eq!(TOPIC_COMPLETED, "udb.workflow.completed.v1");
    }

    /// The workflow lane tags its sagas with `SagaKind::Workflow`, and the
    /// discriminator round-trips through the saga `operation` field while the
    /// DEFAULT (data-plane) kind is preserved verbatim — so existing sagas are
    /// unchanged.
    #[test]
    fn workflow_saga_kind_round_trips() {
        let tagged = SagaKind::Workflow.tag_operation("order_fulfillment");
        assert_eq!(SagaKind::from_operation(&tagged), SagaKind::Workflow);
        // Default is the identity function — pre-9.12 sagas are byte-for-byte same.
        assert_eq!(SagaKind::Default.tag_operation("upsert"), "upsert");
        assert_eq!(SagaKind::from_operation("upsert"), SagaKind::Default);
    }

    /// Status filter normalization rejects unknown tokens (fail closed) and accepts
    /// the canonical short form.
    #[test]
    fn status_filter_validates() {
        assert_eq!(workflow_status_filter_to_db("").unwrap(), "");
        assert_eq!(workflow_status_filter_to_db("running").unwrap(), "RUNNING");
        assert!(workflow_status_filter_to_db("bogus").is_err());
        assert!(is_terminal_status("COMPLETED"));
        assert!(!is_terminal_status("RUNNING"));
    }
}
