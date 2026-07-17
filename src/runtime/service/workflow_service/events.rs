//! Transactional-outbox emission for the native `WorkflowService`: the tick's
//! system-actor writer and the RPC caller-supplied-tx writer, both using the SAME
//! shared compliance envelope the auth/native lanes emit so the CDC tailer decodes
//! them identically.

use uuid::Uuid;

use super::super::auth_service::events::{
    ComplianceEnvelope, build_native_compliance_envelope, enterprise_audit_mode,
    validate_native_compliance,
};
use super::errors::workflow_internal_status;
use tonic::Status;

/// Insert ONE tick outbox row inside the tick transaction (transactional outbox),
/// using the SAME shared compliance envelope the auth/native lanes emit so the CDC
/// tailer decodes it identically. The actor is the workflow worker (a system
/// principal), not an end user.
pub(crate) async fn insert_tick_outbox(
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

/// Insert ONE RPC outbox row inside the SAME transaction as the row mutation it
/// announces (16.3.3 — the generalized, caller-supplied-tx sibling of
/// [`insert_tick_outbox`]): both commit or neither does, closing the dual-write
/// window the pool-scoped `enqueue_outbox_event_with_context` path leaves open.
/// Envelope parity with that path: actor/auth_method/decision/policy/trace come
/// from the ambient method-security + OTel request context, and in enterprise
/// audit mode a non-compliant event FAILS the transaction (fail closed) instead
/// of being silently dropped. A missing outbox relation keeps the established
/// slim-deployment semantic: the mutation commits without an event (there is no
/// outbox to write), logged at warn level.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert_rpc_outbox(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    relation: Option<&str>,
    topic: &str,
    partition_key: &str,
    tenant_id: &str,
    project_id: &str,
    correlation_id: &str,
    operation: &str,
    target_resource: &str,
    rpc: &'static str,
    payload: serde_json::Value,
) -> Result<(), Status> {
    let Some(relation) = relation else {
        tracing::warn!(
            topic,
            rpc,
            "workflow: no outbox relation configured; committing mutation without event"
        );
        return Ok(());
    };
    let trace = crate::runtime::otel::current_trace_context();
    let correlation = if !correlation_id.trim().is_empty() {
        correlation_id.to_string()
    } else if !trace.correlation_id.trim().is_empty() {
        trace.correlation_id.clone()
    } else {
        partition_key.to_string()
    };
    let env = ComplianceEnvelope {
        actor: crate::runtime::otel::current_actor(),
        operation: operation.to_string(),
        outcome: "success".to_string(),
        auth_method: crate::runtime::otel::current_auth_method(),
        decision_id: crate::runtime::otel::current_decision_id(),
        policy_version: crate::runtime::otel::current_policy_revision(),
        trace_id: trace.trace_id.clone(),
        span_id: trace.span_id.clone(),
        target_resource: target_resource.to_string(),
        ..ComplianceEnvelope::default()
    };
    if enterprise_audit_mode()
        && let Err(missing) = validate_native_compliance(
            topic,
            tenant_id,
            &env.actor,
            &env.operation,
            &correlation,
            true,
        )
    {
        return Err(workflow_internal_status(
            rpc,
            format!("refusing non-compliant native event (enterprise audit mode): {missing}"),
        ));
    }
    let event_id = Uuid::new_v4();
    let envelope = build_native_compliance_envelope(
        &event_id.to_string(),
        topic,
        partition_key,
        tenant_id,
        project_id,
        &env,
        &correlation,
        "none",
        1,
        &[],
        payload,
    );
    crate::runtime::cdc::insert_outbox_row(
        &mut **tx,
        relation,
        event_id,
        topic,
        partition_key,
        &envelope,
    )
    .await
    .map_err(|e| workflow_internal_status(rpc, format!("workflow outbox insert failed: {e}")))
}
