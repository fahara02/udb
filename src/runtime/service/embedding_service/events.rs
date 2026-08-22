//! The per-mutation / per-row outbox event emission for the native
//! `EmbeddingService`, plus the pure no-credential work-event payload builder.
//! Extracted verbatim from the former god file — the best-effort
//! `enqueue_outbox_event_with_context` emits and the base/extra payload merges are
//! byte-for-byte identical. The four `emit_*` methods stay inherent on
//! `EmbeddingServiceImpl` (they use `self`), shared between the RPC handlers and
//! the leader-owned background passes.

use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_with_context};
use super::EmbeddingServiceImpl;

/// Bound an over-long embedding input to `max_chars`, preferring to cut at the
/// last whitespace within the bound so a word is not split. Char-based, so it
/// never panics on a multi-byte boundary; `<= max_chars` returns the text
/// unchanged. Pure.
#[cfg(test)]
pub(crate) fn bound_embedding_text(text: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let mut char_count = 0usize;
    let mut cut_byte = None;
    let mut last_ws_byte = None;
    for (index, ch) in text.char_indices() {
        if char_count == max_chars {
            cut_byte = Some(index);
            break;
        }
        if ch.is_whitespace() {
            last_ws_byte = Some(index);
        }
        char_count += 1;
    }
    let Some(cut) = cut_byte else {
        return text.to_string();
    };
    let end = last_ws_byte.unwrap_or(cut);
    text[..end].trim_end().to_string()
}

/// Build the `udb.embedding.work.v1` payload the sidecar pool consumes. Pure so
/// the no-credential invariant is unit-asserted: it carries ONLY the row pk +
/// text + non-secret routing. There is NO credential/API-key field here — model
/// credentials live exclusively in the sidecar (architecture guard 9.11). The
/// text is bounded to `max_chars` (see [`bound_embedding_text`]) so a
/// pathologically long row cannot exceed the embedding model's input limit.
#[cfg(test)]
pub(crate) fn build_work_event_payload(
    tenant_id: &str,
    source_name: &str,
    row_pk: &str,
    text: &str,
    model_id: &str,
    target_collection: &str,
    max_chars: usize,
) -> serde_json::Value {
    serde_json::json!({
        "tenant_id": tenant_id,
        "source": source_name,
        "row_pk": row_pk,
        "text": bound_embedding_text(text, max_chars),
        "model_id": model_id,
        "target_collection": target_collection,
    })
}

impl EmbeddingServiceImpl {
    /// Emit a per-mutation versioned dot-topic control event (best-effort).
    /// The base identifiers every source event carries, merged with the caller's
    /// extra fields. Shared by both emit postures so the payload is byte-identical
    /// whichever one a call site takes.
    fn source_event_payload(
        tenant_id: &str,
        project_id: &str,
        source_name: &str,
        extra: serde_json::Value,
    ) -> serde_json::Value {
        let mut payload = serde_json::json!({
            "tenant_id": tenant_id,
            "project_id": project_id,
            "source": source_name,
        });
        if let (Some(object), Some(extra)) = (payload.as_object_mut(), extra.as_object()) {
            for (key, value) in extra {
                object.insert(key.clone(), value.clone());
            }
        }
        payload
    }

    /// Build the source event as a transaction step so it commits with the write
    /// that caused it.
    ///
    /// `None` means nothing to co-commit; the caller falls back to
    /// [`Self::emit_source_event`].
    pub(crate) fn source_event_transaction_op(
        &self,
        topic: &str,
        tenant_id: &str,
        project_id: &str,
        source_name: &str,
        extra: serde_json::Value,
    ) -> Option<crate::runtime::core::native_store::NativeEntityTransactionOp> {
        match crate::runtime::service::native_helpers::native_transaction_outbox_op(
            self.outbox_relation.as_deref(),
            topic,
            source_name,
            tenant_id,
            project_id,
            Self::source_event_payload(tenant_id, project_id, source_name, extra),
            NativeEventContext {
                target_resource: source_name.to_string(),
                ..NativeEventContext::default()
            },
        ) {
            Ok(op) => op,
            Err(reject) => {
                tracing::warn!(
                    topic,
                    error = %reject,
                    "refusing to enqueue non-compliant embedding source event; the write still stands"
                );
                self.metrics.inc_outbox_enqueue_failures_total("native");
                None
            }
        }
    }

    /// Best-effort source event.
    ///
    /// The callers left on this path cannot do better, so do not convert them to
    /// [`Self::source_event_transaction_op`]:
    ///
    /// - the non-Postgres fallback of a co-committed site;
    /// - passes whose durable effect is a VECTOR ENGINE write rather than a
    ///   Postgres one, where no transaction exists for the event to join;
    /// - `ingest_one`'s document events, which announce the end of a MULTI-STEP
    ///   ingest (document row, then the chunk work batch, then job bookkeeping).
    ///   No single transaction spans that, and binding the event to the document
    ///   write would announce an ingest whose chunks are not persisted yet —
    ///   trading a lost event for a lying one.
    pub(crate) async fn emit_source_event(
        &self,
        topic: &str,
        tenant_id: &str,
        project_id: &str,
        source_name: &str,
        extra: serde_json::Value,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        let payload = Self::source_event_payload(tenant_id, project_id, source_name, extra);
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            topic,
            source_name,
            tenant_id,
            project_id,
            payload,
            NativeEventContext {
                target_resource: source_name.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }
}
