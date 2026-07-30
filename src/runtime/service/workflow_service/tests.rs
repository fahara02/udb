//! Unit fixtures for `WorkflowService`: tenant-isolation and typed-detail guards
//! that fire before any pool access, the no-double-advance / no-double-emit claim
//! SQL, the pure state-machine helpers (event-topic selection, reverse-order
//! compensation, exactly-once emission marker), and the shared scope predicate.

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::proto::udb::core::workflow::services::v1 as workflow_pb;
use crate::proto::udb::core::workflow::services::v1::workflow_service_server::WorkflowService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
use crate::runtime::saga::SagaKind;

use super::WorkflowServiceImpl;
use super::config::{
    COMPENSATE_EMITTED_KEY, MAX_PAYLOAD_BYTES, STATUS_COMPENSATING, STATUS_FAILED,
    TOPIC_COMPENSATE_STEP, TOPIC_COMPENSATED, TOPIC_COMPLETED, TOPIC_FAILED, TOPIC_STEP_ADVANCED,
    WORKFLOW_STEP_TIMEOUT_SECS, workflow_step_timeout_secs,
};
use super::errors::{
    workflow_cancel_terminal_status, workflow_capability_status, workflow_internal_status,
    workflow_not_found_status, workflow_signal_compensating_status,
    workflow_signal_terminal_status,
};
use super::model::{is_terminal_status, workflow_model, workflow_status_filter_to_db};
use super::store::workflow_scope_predicate;
use super::tick::{
    advance_event_topic, compensate_emitted_from_payload, compensating_workflows_claim_sql,
    compensation_steps_to_emit, due_workflows_claim_sql, pending_signal_for_step, signal_delivered,
    signal_resumes, timed_out_workflows_claim_sql, timeout_target_status,
};

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
    let err =
        workflow_status_filter_to_db("stuck").expect_err("unknown status filter must fail closed");

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

/// 16.3.2 — the compensation driver emits one event per completed step in
/// REVERSE order (last completed step first), naming steps from the recorded
/// compensations array when possible, positionally otherwise. Pure — no PG.
#[test]
fn compensation_steps_emit_in_reverse_order() {
    let comps = serde_json::json!([
        {"name": "reserve_inventory"},
        {"name": "charge_card"},
        {"name": "ship_order"},
    ]);
    assert_eq!(
        compensation_steps_to_emit(&comps, 3, 0),
        vec![
            (2, "ship_order".to_string()),
            (1, "charge_card".to_string()),
            (0, "reserve_inventory".to_string()),
        ]
    );
    // No names recorded → positional fallback, never the payload contents.
    assert_eq!(
        compensation_steps_to_emit(&serde_json::json!([]), 2, 0),
        vec![(1, "step_1".to_string()), (0, "step_0".to_string())]
    );
}

/// 16.3.2 — the payload emission marker makes re-ticks exactly-once: steps a
/// previous pass already enqueued are skipped, an over-stamped or complete
/// marker emits nothing, and zero completed steps mean nothing to undo.
#[test]
fn compensation_emission_marker_is_exactly_once() {
    let comps = serde_json::json!([]);
    assert_eq!(
        compensation_steps_to_emit(&comps, 3, 2),
        vec![(0, "step_0".to_string())]
    );
    assert!(compensation_steps_to_emit(&comps, 3, 3).is_empty());
    assert!(compensation_steps_to_emit(&comps, 3, 7).is_empty());
    assert!(compensation_steps_to_emit(&comps, 0, 0).is_empty());

    // Marker decode: absent / malformed / negative all read as 0.
    assert_eq!(COMPENSATE_EMITTED_KEY, "compensate_emitted_steps");
    assert_eq!(compensate_emitted_from_payload(&serde_json::json!({})), 0);
    assert_eq!(
        compensate_emitted_from_payload(&serde_json::json!({"compensate_emitted_steps": 2})),
        2
    );
    assert_eq!(
        compensate_emitted_from_payload(&serde_json::json!({"compensate_emitted_steps": "x"})),
        0
    );
    assert_eq!(
        compensate_emitted_from_payload(&serde_json::json!({"compensate_emitted_steps": -4})),
        0
    );
}

/// 16.3.2 — the compensation claim mirrors the forward claim's no-double-emit
/// contract: SKIP LOCKED, COMPENSATING-only, non-deleted rows.
#[test]
fn compensating_claim_sql_shape() {
    let sql = compensating_workflows_claim_sql(&workflow_model());
    assert!(
        sql.contains("FOR UPDATE SKIP LOCKED"),
        "compensation claim must skip locked rows: {sql}"
    );
    assert!(
        sql.contains("'COMPENSATING'"),
        "claim must only take COMPENSATING instances: {sql}"
    );
    assert!(
        sql.contains("IS NULL"),
        "claim must exclude soft-deleted rows: {sql}"
    );
}

/// 16.3.4 — the timeout sweep claims RUNNING rows whose last transition is
/// older than the bound timeout, with the same SKIP LOCKED discipline (mirrors
/// `due_claim_sql_uses_skip_locked`).
#[test]
fn timed_out_claim_sql_shape() {
    let sql = timed_out_workflows_claim_sql(&workflow_model());
    assert!(
        sql.contains("FOR UPDATE SKIP LOCKED"),
        "timeout claim must skip locked rows: {sql}"
    );
    assert!(
        sql.contains("'RUNNING'"),
        "sweep must only take RUNNING instances: {sql}"
    );
    assert!(
        sql.contains("IS NULL"),
        "sweep must exclude soft-deleted rows: {sql}"
    );
    assert!(
        sql.contains("make_interval(secs => $2::DOUBLE PRECISION)"),
        "sweep must bind the env-resolved timeout, never inline it: {sql}"
    );
    assert!(
        sql.contains("< NOW() -"),
        "sweep must compare the transition stamp against the timeout window: {sql}"
    );
}

/// The env-overridable timeout resolves to a positive number of seconds (the
/// named default when the env override is absent/invalid).
#[test]
fn workflow_step_timeout_resolves_once_positive() {
    let secs = workflow_step_timeout_secs();
    assert!(secs > 0);
    assert_eq!(WORKFLOW_STEP_TIMEOUT_SECS, 3600);
}

/// 16.3.1 — the shared scope predicate used by get/list/cancel/signal binds the
/// tenant AND a project arm: empty project stays tenant-wide (backward
/// compatible, the api-key list semantic), a non-empty project restricts to
/// exactly that project's rows.
#[test]
fn scope_predicate_binds_tenant_and_project() {
    let scope = workflow_scope_predicate(&workflow_model(), "$2", "$3");
    assert!(
        scope.contains("= $2::UUID"),
        "scope must bind the tenant: {scope}"
    );
    assert!(
        scope.contains("$3 = ''"),
        "empty project must stay tenant-wide (backward compatible): {scope}"
    );
    assert!(
        scope.contains("= NULLIF($3, '')::UUID"),
        "non-empty project must bind a project equality predicate: {scope}"
    );
}

/// The compensation/failure lifecycle topics are versioned dot-topics matching
/// the 16.3 contract.
#[test]
fn compensation_and_failure_topics_are_versioned() {
    assert_eq!(TOPIC_COMPENSATE_STEP, "udb.workflow.compensate.step.v1");
    assert_eq!(TOPIC_COMPENSATED, "udb.workflow.compensated.v1");
    assert_eq!(TOPIC_FAILED, "udb.workflow.failed.v1");
}

/// H7 — a COMPENSATING instance is non-signalable: the denial carries a truthful
/// `workflow_compensating_state` policy decision (NOT the terminal-state one), so
/// a signal can never revert an in-flight compensation to RUNNING.
#[test]
fn workflow_signal_compensating_denial_carries_policy_detail() {
    let denial = workflow_signal_compensating_status();
    assert_eq!(denial.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        denial.message(),
        "workflow is compensating and cannot be signalled"
    );
    assert_policy_detail(&denial, "signal_workflow", "workflow_compensating_state");
}

/// H6 — a timed-out instance that already completed forward steps moves to
/// COMPENSATING (its steps are undone before it settles), never straight to a
/// settled FAILED with completed steps standing; one that timed out before any
/// step completed settles FAILED (nothing to undo). Pure — no PG.
#[test]
fn timeout_target_compensates_only_with_completed_steps() {
    assert_eq!(timeout_target_status(1), STATUS_COMPENSATING);
    assert_eq!(timeout_target_status(5), STATUS_COMPENSATING);
    assert_eq!(timeout_target_status(0), STATUS_FAILED);
    assert_eq!(timeout_target_status(-3), STATUS_FAILED);
}

/// H9 — the forward pass honors step outcomes: a step declared (in the payload
/// `signal_waits` map) to block on a signal that has not been delivered gates the
/// advance (returns the pending signal name); once the signal is recorded in the
/// `signals` log the gate clears; a step with no declared wait never gates. Pure.
#[test]
fn pending_signal_gates_step_until_delivered() {
    let waiting = serde_json::json!({
        "signal_waits": { "2": "payment_confirmed" }
    });
    // Step 2 is gated on `payment_confirmed`, which has not been delivered.
    assert_eq!(
        pending_signal_for_step(&waiting, 2),
        Some("payment_confirmed".to_string())
    );
    // Steps without a declared wait advance freely.
    assert_eq!(pending_signal_for_step(&waiting, 1), None);
    assert_eq!(pending_signal_for_step(&waiting, 3), None);

    // Once the matching signal is recorded, the gate clears.
    let delivered = serde_json::json!({
        "signal_waits": { "2": "payment_confirmed" },
        "signals": [ { "name": "payment_confirmed" } ]
    });
    assert_eq!(pending_signal_for_step(&delivered, 2), None);
    assert!(signal_delivered(&delivered, "payment_confirmed"));
    assert!(!signal_delivered(&delivered, "other"));

    // A payload with no signal_waits map never gates.
    assert_eq!(pending_signal_for_step(&serde_json::json!({}), 2), None);
}

/// H9 — a WAITING_SIGNAL instance resumes ONLY on the exact signal it is blocked
/// on; an unrelated signal is recorded but does not resume it. A RUNNING instance
/// is always nudged forward. Pure — no PG.
#[test]
fn signal_resumes_only_on_exact_pending_match() {
    assert!(signal_resumes(
        "WAITING_SIGNAL",
        "payment_confirmed",
        "payment_confirmed"
    ));
    assert!(!signal_resumes(
        "WAITING_SIGNAL",
        "payment_confirmed",
        "shipment_ready"
    ));
    // A running (non-waiting) instance is simply nudged forward by any signal.
    assert!(signal_resumes("RUNNING", "", "anything"));
}
