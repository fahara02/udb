//! Strict transactional dot-topic outbox emission for the native
//! `NotificationService`. Initial sends use the typed native transaction and
//! retry/delivery outcomes use their existing Postgres transactions so durable
//! state and redaction-safe events cannot diverge.

use super::super::native_helpers::{
    NativeEventContext, enqueue_outbox_event_in_tx, native_transaction_outbox_op,
};
use super::config::{
    NOTIFICATION_SENT_TOPIC, NOTIFICATION_SUPPRESSED_TOPIC, delivery_event_topic,
    legacy_delivery_event_topic,
};
use super::model::{notification_sent_payload, notification_suppressed_payload};
use crate::proto::udb::core::notification::entity::v1 as notif_entity_pb;

/// Stored, non-secret context required to serialize both the canonical delivery
/// outcome and the older public FAILED/DELIVERED message contracts. Callers
/// populate this from the locked NotificationLog and durable attempt row; no
/// recipient address, rendered content, or provider credential is accepted.
pub(crate) struct NotificationDeliveryEvent<'a> {
    pub log_id: &'a str,
    pub template_id: &'a str,
    pub event_type: &'a str,
    pub tenant_id: &'a str,
    pub project_id: &'a str,
    pub correlation_id: &'a str,
    pub channel_db: &'a str,
    pub provider: &'a str,
    pub status_db: &'a str,
    pub provider_message_id: &'a str,
    pub error_detail: &'a str,
    /// Zero-based retry ordinal required by NotificationFailedEvent.
    pub retry_attempt: i32,
    pub will_retry: bool,
}

/// Produce the additive canonical payload and, where a public compatibility
/// topic exists, the exact v1 message payload for that topic. A single timestamp
/// is shared so the two rows describe one outcome even though the outbox assigns
/// each envelope its own event id.
pub(crate) fn delivery_event_payloads(
    event: &NotificationDeliveryEvent<'_>,
) -> (serde_json::Value, Option<serde_json::Value>) {
    let occurred_at = chrono::Utc::now().to_rfc3339();
    let canonical = serde_json::json!({
        "log_id": event.log_id,
        "template_id": event.template_id,
        "event_type": event.event_type,
        "tenant_id": event.tenant_id,
        "project_id": event.project_id,
        "correlation_id": event.correlation_id,
        "channel": event.channel_db,
        "provider": event.provider,
        "status": event.status_db,
        "provider_message_id": event.provider_message_id,
        "error_code": if event.status_db == "FAILED" { "DELIVERY_FAILED" } else { "" },
        "error_detail": event.error_detail,
        "retry_attempt": event.retry_attempt,
        "will_retry": event.will_retry,
        "occurred_at": occurred_at,
    });
    let legacy = match event.status_db {
        "FAILED" => Some(serde_json::json!({
            "log_id": event.log_id,
            "template_id": event.template_id,
            "event_type": event.event_type,
            "channel": event.channel_db,
            "tenant_id": event.tenant_id,
            "project_id": event.project_id,
            "error_code": "DELIVERY_FAILED",
            "error_detail": event.error_detail,
            "retry_attempt": event.retry_attempt,
            "will_retry": event.will_retry,
            "correlation_id": event.correlation_id,
            "occurred_at": occurred_at,
        })),
        "DELIVERED" => Some(serde_json::json!({
            "log_id": event.log_id,
            "channel": event.channel_db,
            "tenant_id": event.tenant_id,
            "project_id": event.project_id,
            "correlation_id": event.correlation_id,
            "occurred_at": occurred_at,
        })),
        _ => None,
    };
    (canonical, legacy)
}

fn sent_event_parts(
    log: &notif_entity_pb::NotificationLog,
    retry: bool,
) -> (serde_json::Value, NativeEventContext) {
    (
        notification_sent_payload(log, retry),
        NativeEventContext {
            operation: if retry {
                "notification.retry"
            } else {
                "notification.send"
            }
            .to_string(),
            outcome: "allow".to_string(),
            target_resource: log.log_id.clone(),
            ..NativeEventContext::default()
        },
    )
}

fn suppressed_event_parts(
    log: &notif_entity_pb::NotificationLog,
) -> (serde_json::Value, NativeEventContext) {
    (
        notification_suppressed_payload(log, "USER_OPT_OUT"),
        NativeEventContext {
            operation: "notification.suppress".to_string(),
            outcome: "deny".to_string(),
            target_resource: log.log_id.clone(),
            ..NativeEventContext::default()
        },
    )
}

/// Build the sent-event step that co-commits with the typed NotificationLog
/// batch. Envelope validation occurs before any database mutation.
pub(crate) fn sent_event_transaction_op(
    outbox_relation: Option<&str>,
    log: &notif_entity_pb::NotificationLog,
) -> Result<Option<crate::runtime::core::native_store::NativeEntityTransactionOp>, String> {
    let (payload, context) = sent_event_parts(log, false);
    native_transaction_outbox_op(
        outbox_relation,
        NOTIFICATION_SENT_TOPIC,
        &log.recipient_id,
        &log.tenant_id,
        &log.project_id,
        payload,
        context,
    )
}

/// Build the suppression-event step that co-commits with an initially
/// SUPPRESSED NotificationLog.
pub(crate) fn suppressed_event_transaction_op(
    outbox_relation: Option<&str>,
    log: &notif_entity_pb::NotificationLog,
) -> Result<Option<crate::runtime::core::native_store::NativeEntityTransactionOp>, String> {
    let (payload, context) = suppressed_event_parts(log);
    native_transaction_outbox_op(
        outbox_relation,
        NOTIFICATION_SUPPRESSED_TOPIC,
        &log.tenant_id,
        &log.tenant_id,
        &log.project_id,
        payload,
        context,
    )
}

/// Enqueue a retry sent event inside the caller's state-transition transaction.
pub(crate) async fn enqueue_sent_event_in_tx(
    executor: &mut sqlx::PgConnection,
    outbox_relation: Option<&str>,
    log: &notif_entity_pb::NotificationLog,
) -> Result<(), String> {
    let (payload, context) = sent_event_parts(log, true);
    enqueue_outbox_event_in_tx(
        &mut *executor,
        outbox_relation,
        NOTIFICATION_SENT_TOPIC,
        &log.recipient_id,
        &log.tenant_id,
        &log.project_id,
        payload,
        context,
    )
    .await
}

/// Enqueue an opt-out suppression event inside the caller's state transaction.
pub(crate) async fn enqueue_suppressed_event_in_tx(
    executor: &mut sqlx::PgConnection,
    outbox_relation: Option<&str>,
    log: &notif_entity_pb::NotificationLog,
) -> Result<(), String> {
    let (payload, context) = suppressed_event_parts(log);
    enqueue_outbox_event_in_tx(
        &mut *executor,
        outbox_relation,
        NOTIFICATION_SUPPRESSED_TOPIC,
        &log.tenant_id,
        &log.tenant_id,
        &log.project_id,
        payload,
        context,
    )
    .await
}

/// Enqueue a delivery outcome in the caller's transaction. The attempt row,
/// parent-log transition, and event are one durable state change: an outbox
/// failure rolls all of them back instead of acknowledging a partially recorded
/// delivery. The project is supplied by the stored NotificationLog, never from
/// an unverified request-header fallback.
pub(crate) async fn enqueue_delivery_event_in_tx(
    executor: &mut sqlx::PgConnection,
    outbox_relation: Option<&str>,
    event: &NotificationDeliveryEvent<'_>,
) -> Result<(), String> {
    let outcome = if event.status_db == "FAILED" {
        "failure"
    } else {
        "allow"
    };
    let (canonical_payload, legacy_payload) = delivery_event_payloads(event);
    let event_context = || NativeEventContext {
        operation: "notification.deliver".to_string(),
        outcome: outcome.to_string(),
        target_resource: event.log_id.to_string(),
        ..NativeEventContext::default()
    };
    enqueue_outbox_event_in_tx(
        &mut *executor,
        outbox_relation,
        &delivery_event_topic(event.status_db),
        event.log_id,
        event.tenant_id,
        event.project_id,
        canonical_payload,
        event_context(),
    )
    .await?;

    if let (Some(legacy_topic), Some(legacy_payload)) =
        (legacy_delivery_event_topic(event.status_db), legacy_payload)
    {
        // The legacy message contract declares tenant_id as its partition key;
        // preserve that ordering contract while the canonical delivery topic is
        // intentionally log-partitioned. Both rows share this transaction with
        // the attempt and parent state, so either both aliases commit or neither
        // does.
        enqueue_outbox_event_in_tx(
            &mut *executor,
            outbox_relation,
            legacy_topic,
            event.tenant_id,
            event.tenant_id,
            event.project_id,
            legacy_payload,
            event_context(),
        )
        .await?;
    }
    Ok(())
}
