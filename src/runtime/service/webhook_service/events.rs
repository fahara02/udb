//! Best-effort versioned dot-topic outbox emission for the native
//! `WebhookService`. Extracted verbatim from the former inherent `emit_event`
//! method as a free `pub(crate)` fn taking `svc` where the method took `&self`.

use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_with_context};
use super::WebhookServiceImpl;

/// Best-effort versioned dot-topic outbox event (mirrors `lock_service`).
pub(crate) async fn emit_event(
    svc: &WebhookServiceImpl,
    topic: &str,
    partition_key: &str,
    tenant_id: &str,
    payload: serde_json::Value,
) {
    let Some(pool) = svc.pg_pool.as_ref() else {
        return;
    };
    enqueue_outbox_event_with_context(
        pool,
        svc.outbox_relation.as_deref(),
        topic,
        partition_key,
        tenant_id,
        "",
        payload,
        NativeEventContext {
            target_resource: partition_key.to_string(),
            ..NativeEventContext::default()
        },
        Some(&svc.metrics),
    )
    .await;
}
