//! Best-effort `analytics.events` outbox emission plus the pure event-payload
//! builder for the native `AnalyticsService`. Extracted verbatim;
//! `emit_analytics_event` takes `svc` where the trait method took `&self`.

use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_with_context};
use super::AnalyticsServiceImpl;
use super::config::TOPIC_ANALYTICS_EVENTS;

/// Emit one proto-declared `analytics.events` outbox event (best-effort; the
/// durable write already succeeded). Mirrors `config_service::emit_flag_changed`:
/// partition key = tenant (the contract's `partition_key_field`), actor = the
/// verified claim subject (auto-filled from the method-security principal when
/// no claim context is installed).
pub(crate) async fn emit_analytics_event(
    svc: &AnalyticsServiceImpl,
    event_type: &'static str,
    tenant_id: &str,
    project_id: &str,
    stage_name: &str,
    payload: serde_json::Value,
) {
    let Some(pool) = svc.pg_pool.as_ref() else {
        return;
    };
    enqueue_outbox_event_with_context(
        pool,
        svc.outbox_relation.as_deref(),
        TOPIC_ANALYTICS_EVENTS,
        tenant_id,
        tenant_id,
        project_id,
        payload,
        NativeEventContext {
            operation: event_type.to_string(),
            target_resource: stage_name.to_string(),
            ..NativeEventContext::default()
        },
        Some(&svc.metrics),
    )
    .await;
}

/// Pure payload builder for the `analytics.events` outbox events (unit-tested
/// SDK-visible shape). `snapshots_written` is present only on TriggerSnapshot.
pub(crate) fn analytics_event_payload(
    event_type: &str,
    stage_name: &str,
    tenant_id: &str,
    snapshots_written: Option<i64>,
) -> serde_json::Value {
    let mut payload = serde_json::json!({
        "event_type": event_type,
        "stage_name": stage_name,
        "tenant_id": tenant_id,
    });
    if let (Some(written), Some(map)) = (snapshots_written, payload.as_object_mut()) {
        map.insert("snapshots_written".to_string(), serde_json::json!(written));
    }
    payload
}
