//! Best-effort versioned dot-topic outbox emission for the native
//! `NotificationService`. Extracted verbatim from the former inherent
//! `emit_sent_event` / `emit_delivery_event` methods; they stay inherent methods on
//! [`NotificationServiceImpl`] (the RPC handlers call `svc.emit_*`), so the bodies
//! — including the redaction-safe delivery payload — are byte-for-byte identical.

use sqlx::PgPool;

use super::super::native_helpers::{
    NativeEventContext, enqueue_outbox_event, enqueue_outbox_event_with_context,
};
use super::NotificationServiceImpl;
use super::config::{NOTIFICATION_SENT_TOPIC, delivery_event_topic};
use super::model::notification_delivery_payload;

impl NotificationServiceImpl {
    /// Best-effort: enqueue a "notification sent" event into the shared native
    /// outbox envelope (top-level tenant/project for CDC routing). Never fails
    /// the RPC.
    pub(crate) async fn emit_sent_event(
        &self,
        pool: &PgPool,
        log_id: &str,
        event_type: &str,
        recipient_id: &str,
        tenant_id: &str,
        project_id: &str,
        channels: &[i32],
        retry: bool,
    ) {
        enqueue_outbox_event(
            pool,
            self.outbox_relation.as_deref(),
            NOTIFICATION_SENT_TOPIC,
            recipient_id,
            tenant_id,
            project_id,
            notification_delivery_payload(
                log_id,
                event_type,
                recipient_id,
                tenant_id,
                project_id,
                channels,
                retry,
            ),
            Some(&self.metrics),
        )
        .await;
    }

    /// Best-effort: publish a terminal delivery outcome (master-plan 9.13) to
    /// `udb.notification.delivery.<status>.v1` with the compliance envelope. The
    /// payload carries only routing/outcome metadata — never provider secrets.
    /// Never fails the RPC.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn emit_delivery_event(
        &self,
        pool: &PgPool,
        log_id: &str,
        tenant_id: &str,
        project_id: &str,
        channel_db: &str,
        provider: &str,
        status_db: &str,
        provider_message_id: &str,
    ) {
        let outcome = if status_db == "FAILED" {
            "failure"
        } else {
            "allow"
        };
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            &delivery_event_topic(status_db),
            log_id,
            tenant_id,
            project_id,
            serde_json::json!({
                "log_id": log_id,
                "tenant_id": tenant_id,
                "project_id": project_id,
                "channel": channel_db,
                "provider": provider,
                "status": status_db,
                "provider_message_id": provider_message_id,
            }),
            NativeEventContext {
                operation: "notification.deliver".to_string(),
                outcome: outcome.to_string(),
                target_resource: log_id.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }
}
