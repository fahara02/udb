//! The per-mutation outbox event emission for the native `BackupService`.
//! Extracted verbatim; `emit_event` takes `svc` where the trait method took
//! `&self`.

use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_with_context};
use super::BackupServiceImpl;

/// Emit a per-mutation versioned dot-topic outbox event (best-effort).
/// Build the backup event as a transaction step so it commits with the policy
/// write or delete that caused it.
///
/// `None` means nothing to co-commit; the caller falls back to [`emit_event`].
#[allow(clippy::too_many_arguments)]
pub(crate) fn event_transaction_op(
    svc: &BackupServiceImpl,
    topic: &str,
    partition_key: &str,
    tenant_id: &str,
    project_id: &str,
    target_resource: &str,
    payload: serde_json::Value,
) -> Option<crate::runtime::core::native_store::NativeEntityTransactionOp> {
    match super::super::native_helpers::native_transaction_outbox_op(
        svc.outbox_relation.as_deref(),
        topic,
        partition_key,
        tenant_id,
        project_id,
        payload,
        NativeEventContext {
            target_resource: target_resource.to_string(),
            ..NativeEventContext::default()
        },
    ) {
        Ok(op) => op,
        Err(reject) => {
            tracing::warn!(
                topic,
                error = %reject,
                "refusing to enqueue non-compliant backup event; the write still stands"
            );
            svc.metrics.inc_outbox_enqueue_failures_total("native");
            None
        }
    }
}

pub(crate) async fn emit_event(
    svc: &BackupServiceImpl,
    topic: &str,
    partition_key: &str,
    tenant_id: &str,
    project_id: &str,
    target_resource: &str,
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
        project_id,
        payload,
        NativeEventContext {
            target_resource: target_resource.to_string(),
            ..NativeEventContext::default()
        },
        Some(&svc.metrics),
    )
    .await;
}
