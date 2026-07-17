//! Best-effort tenant lifecycle event emission into the shared transactional
//! outbox, plus the two no-secrets event payload builders. Extracted verbatim;
//! `emit_event` takes `svc` where the trait method took `&self`.

use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_with_context};
use super::TenantServiceImpl;

/// Best-effort tenant-event emission into the shared transactional outbox
/// (`→ CDC → Kafka`). `operation` is the contract-declared event type for the
/// emitting RPC (e.g. [`super::config::EVENT_TYPE_TENANT_CREATED`]); it lands in
/// the compliance envelope so the durable row is traceable to its RPC contract.
/// No-op when no PG pool / outbox relation is configured (mirrors the other
/// native services' best-effort emit posture).
pub(crate) async fn emit_event(
    svc: &TenantServiceImpl,
    topic: &str,
    operation: &str,
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
            operation: operation.to_string(),
            target_resource: tenant_id.to_string(),
            ..NativeEventContext::default()
        },
        Some(&svc.metrics),
    )
    .await;
}

/// Lifecycle event payload for tenant create/update: identifiers + status ONLY.
/// Deliberately excludes `config`/`branding` bodies and any credential material —
/// the outbox payload must never carry tenant secrets.
pub(crate) fn tenant_lifecycle_event_payload(
    tenant_id: &str,
    code: &str,
    status: &str,
) -> serde_json::Value {
    serde_json::json!({
        "tenant_id": tenant_id,
        "code": code,
        "status": status,
    })
}

/// Config-update event payload: tenant + config KEY only. The config VALUE is
/// deliberately omitted (it may carry secrets; same no-secrets rule as above).
pub(crate) fn tenant_config_event_payload(tenant_id: &str, config_key: &str) -> serde_json::Value {
    serde_json::json!({
        "tenant_id": tenant_id,
        "config_key": config_key,
    })
}
