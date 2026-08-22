//! The per-mutation outbox event emission for the native `SearchService`.
//! Extracted verbatim from the former god file — the best-effort
//! `enqueue_outbox_event_with_context` emit and the base/extra payload merge are
//! byte-for-byte identical. `emit_index_event` stays an inherent method on
//! `SearchServiceImpl` (it uses `self`), shared between the RPC handlers and the
//! leader-owned background passes.

use super::super::native_helpers::{
    NativeEventContext, enqueue_outbox_event_with_context, native_transaction_outbox_op,
};
use super::SearchServiceImpl;

impl SearchServiceImpl {
    /// The base identifiers every index event carries, merged with the caller's
    /// extra fields. Shared by both emit postures so the payload is byte-identical
    /// whichever one a call site takes.
    fn index_event_payload(
        tenant_id: &str,
        project_id: &str,
        index_name: &str,
        extra: serde_json::Value,
    ) -> serde_json::Value {
        let mut payload = serde_json::json!({
            "tenant_id": tenant_id,
            "project_id": project_id,
            "index_name": index_name,
        });
        if let (Some(object), Some(extra)) = (payload.as_object_mut(), extra.as_object()) {
            for (key, value) in extra {
                object.insert(key.clone(), value.clone());
            }
        }
        payload
    }

    /// Build the index event as a transaction step, so a call site whose write
    /// goes through the dispatch layer can commit both together via
    /// [`crate::runtime::core::DataBrokerRuntime::native_entity_write_co_commit_for_service`].
    ///
    /// `None` means nothing to co-commit: no outbox relation, or a rejected
    /// envelope (warned and counted here, matching `emit_index_event`'s
    /// best-effort posture rather than failing the RPC).
    pub(crate) fn index_event_transaction_op(
        &self,
        topic: &str,
        tenant_id: &str,
        project_id: &str,
        index_name: &str,
        extra: serde_json::Value,
    ) -> Option<crate::runtime::core::native_store::NativeEntityTransactionOp> {
        match native_transaction_outbox_op(
            self.outbox_relation.as_deref(),
            topic,
            index_name,
            tenant_id,
            project_id,
            Self::index_event_payload(tenant_id, project_id, index_name, extra),
            NativeEventContext {
                target_resource: index_name.to_string(),
                ..NativeEventContext::default()
            },
        ) {
            Ok(op) => op,
            Err(reject) => {
                tracing::warn!(
                    topic,
                    error = %reject,
                    "refusing to enqueue non-compliant search index event; the index write still stands"
                );
                self.metrics.inc_outbox_enqueue_failures_total("native");
                None
            }
        }
    }

    /// Emit a per-mutation versioned dot-topic outbox event (best-effort).
    ///
    /// Two callers legitimately cannot do better, so do not "fix" them into
    /// [`Self::index_event_transaction_op`]:
    ///
    /// - the non-Postgres fallback of a co-committed site, where the outbox table
    ///   and the entity write are in different databases;
    /// - the freshness and teardown passes, whose durable effect is a VECTOR
    ///   ENGINE upsert/delete, not a Postgres write — there is no transaction for
    ///   the event to join. Both leave their job unmarked on failure, so the next
    ///   leader pass re-derives the work rather than losing it.
    pub(crate) async fn emit_index_event(
        &self,
        topic: &str,
        tenant_id: &str,
        project_id: &str,
        index_name: &str,
        extra: serde_json::Value,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        let payload = Self::index_event_payload(tenant_id, project_id, index_name, extra);
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            topic,
            index_name,
            tenant_id,
            project_id,
            payload,
            NativeEventContext {
                target_resource: index_name.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }
}
