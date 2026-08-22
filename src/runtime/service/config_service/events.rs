//! Change-event emission for `ConfigService`: the acting principal (from the
//! verified claim) and the per-mutation `udb.config.flag.changed.v1` outbox event.

use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_with_context};
use super::ConfigServiceImpl;
use super::config::TOPIC_FLAG_CHANGED;

/// The acting principal for the change event, from the verified claim (falls back
/// to `system` when no claim context is present, e.g. internal callers).
pub(crate) fn event_actor() -> String {
    let subject = crate::runtime::service::method_security::current_claim_context()
        .subject
        .trim()
        .to_string();
    if subject.is_empty() {
        "system".to_string()
    } else {
        subject
    }
}

/// Build the flag-changed event as a transaction step so it commits with the
/// write or delete that caused it.
///
/// `None` means nothing to co-commit (no outbox relation, or a rejected
/// envelope); the caller then falls back to [`emit_flag_changed`].
pub(crate) fn flag_changed_transaction_op(
    svc: &ConfigServiceImpl,
    tenant_id: &str,
    project_id: &str,
    flag_key: &str,
    actor: &str,
    revision: i64,
) -> Option<crate::runtime::core::native_store::NativeEntityTransactionOp> {
    match super::super::native_helpers::native_transaction_outbox_op(
        svc.outbox_relation.as_deref(),
        TOPIC_FLAG_CHANGED,
        flag_key,
        tenant_id,
        project_id,
        serde_json::json!({
            "key": flag_key,
            "actor": actor,
            "revision": revision,
            "tenant_id": tenant_id,
            "project_id": project_id,
        }),
        NativeEventContext {
            actor: actor.to_string(),
            target_resource: flag_key.to_string(),
            ..NativeEventContext::default()
        },
    ) {
        Ok(op) => op,
        Err(reject) => {
            tracing::warn!(
                topic = TOPIC_FLAG_CHANGED,
                flag_key,
                error = %reject,
                "refusing to enqueue non-compliant flag event; the flag write still stands"
            );
            svc.metrics.inc_outbox_enqueue_failures_total("native");
            None
        }
    }
}

/// Emit the per-mutation `udb.config.flag.changed.v1` event `{key, actor,
/// revision}` (best-effort; the durable write already succeeded).
pub(crate) async fn emit_flag_changed(
    svc: &ConfigServiceImpl,
    tenant_id: &str,
    project_id: &str,
    flag_key: &str,
    actor: &str,
    revision: i64,
) {
    let Some(pool) = svc.pg_pool.as_ref() else {
        return;
    };
    let payload = serde_json::json!({
        "key": flag_key,
        "actor": actor,
        "revision": revision,
        "tenant_id": tenant_id,
        "project_id": project_id,
    });
    enqueue_outbox_event_with_context(
        pool,
        svc.outbox_relation.as_deref(),
        TOPIC_FLAG_CHANGED,
        flag_key,
        tenant_id,
        project_id,
        payload,
        NativeEventContext {
            actor: actor.to_string(),
            target_resource: flag_key.to_string(),
            ..NativeEventContext::default()
        },
        Some(&svc.metrics),
    )
    .await;
}
