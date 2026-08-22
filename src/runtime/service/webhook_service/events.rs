//! Versioned dot-topic outbox emission for the native `WebhookService`.
//!
//! Every endpoint mutation here writes raw SQL against the same Postgres pool the
//! outbox lives in, so all of them can — and now do — commit the row and its
//! event in one transaction. There is no backend for which that is impossible,
//! which is why this module has no best-effort emit left.

use super::super::native_helpers::{
    NativeEventContext, OutboxEnvelopeReject, build_enriched_outbox_envelope,
};
use super::WebhookServiceImpl;

/// Insert the outbox row through the CALLER'S transaction, so the webhook row and
/// its event commit together.
///
/// This replaced a best-effort emit that ran after the write had already
/// committed, which left a window where a subscription existed that nothing
/// downstream was ever told about, and nothing re-derives it.
///
/// The two failure classes are handled differently on purpose. An
/// INFRASTRUCTURE failure is propagated so the caller's transaction rolls the
/// row back, which is the window being closed. An ENVELOPE REJECTION keeps the
/// pre-existing best-effort posture — warn, count, let the write stand — so this
/// never converts a silent event drop into a failed RPC.
pub(crate) async fn emit_event_in_tx(
    svc: &WebhookServiceImpl,
    executor: &mut sqlx::PgConnection,
    topic: &str,
    partition_key: &str,
    tenant_id: &str,
    payload: serde_json::Value,
) -> Result<(), String> {
    let Some(relation) = svc.outbox_relation.as_deref() else {
        return Ok(());
    };
    let context = NativeEventContext {
        target_resource: partition_key.to_string(),
        ..NativeEventContext::default()
    };
    let (event_id, envelope) = match build_enriched_outbox_envelope(
        topic,
        partition_key,
        tenant_id,
        "",
        context,
        payload,
    ) {
        Ok(built) => built,
        Err(reject) => {
            let bucket = match reject {
                OutboxEnvelopeReject::TenantScopeMissing => "native_tenant_scope",
                OutboxEnvelopeReject::Compliance(_) => "native_compliance",
            };
            tracing::warn!(
                topic,
                error = %reject,
                "refusing to enqueue non-compliant webhook event; the webhook write still stands"
            );
            svc.metrics.inc_outbox_enqueue_failures_total(bucket);
            return Ok(());
        }
    };
    crate::runtime::cdc::insert_outbox_row(
        &mut *executor,
        relation,
        event_id,
        topic,
        partition_key,
        &envelope,
    )
    .await
    .map_err(|err| format!("outbox insert failed: {err}"))
}
