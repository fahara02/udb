//! Best-effort versioned dot-topic outbox emission for `CacheService` (mirrors
//! `lock_service`). Gated on the `redis` feature, since cache events are only
//! produced by the Redis-backed handlers.

use super::super::native_helpers::{
    NativeEventContext, enqueue_outbox_event_with_context, project_scoped_native_service_context,
};
use super::CacheServiceImpl;

/// Best-effort versioned dot-topic outbox event (mirrors `lock_service`).
pub(crate) async fn emit_event(
    svc: &CacheServiceImpl,
    metadata: &tonic::metadata::MetadataMap,
    topic: &str,
    tenant_id: &str,
    namespace: &str,
    payload: serde_json::Value,
) {
    let Some(pool) = svc.pg_pool.as_ref() else {
        return;
    };
    let context = project_scoped_native_service_context(metadata, tenant_id);
    enqueue_outbox_event_with_context(
        pool,
        svc.outbox_relation.as_deref(),
        topic,
        namespace,
        tenant_id,
        &context.project_id,
        payload,
        NativeEventContext {
            target_resource: namespace.to_string(),
            ..NativeEventContext::default()
        },
        Some(&svc.metrics),
    )
    .await;
}
