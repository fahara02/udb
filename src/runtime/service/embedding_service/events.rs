//! The per-mutation / per-row outbox event emission for the native
//! `EmbeddingService`, plus the pure no-credential work-event payload builder.
//! Extracted verbatim from the former god file — the best-effort
//! `enqueue_outbox_event_with_context` emits and the base/extra payload merges are
//! byte-for-byte identical. The four `emit_*` methods stay inherent on
//! `EmbeddingServiceImpl` (they use `self`), shared between the RPC handlers and
//! the leader-owned background passes.

use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_with_context};
use super::EmbeddingServiceImpl;
use super::config::TOPIC_WORK;

/// Build the `udb.embedding.work.v1` payload the sidecar pool consumes. Pure so
/// the no-credential invariant is unit-asserted: it carries ONLY the row pk +
/// text + non-secret routing. There is NO credential/API-key field here — model
/// credentials live exclusively in the sidecar (architecture guard 9.11).
pub(crate) fn build_work_event_payload(
    tenant_id: &str,
    source_name: &str,
    row_pk: &str,
    text: &str,
    model_id: &str,
    target_collection: &str,
) -> serde_json::Value {
    serde_json::json!({
        "tenant_id": tenant_id,
        "source": source_name,
        "row_pk": row_pk,
        "text": text,
        "model_id": model_id,
        "target_collection": target_collection,
    })
}

impl EmbeddingServiceImpl {
    /// Emit a per-mutation versioned dot-topic control event (best-effort).
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

    /// Emit a `udb.embedding.work.v1` event for one source row (the seam the CDC
    /// change handler and the leader-spawned backfill worker both call). The
    /// payload is the no-credential [`build_work_event_payload`]; the partition key
    /// is the row pk so a sidecar pool fans out by row.
    #[allow(dead_code)] // called by the injected-event test seam `run_embedding_work_emitter`
    pub(crate) async fn emit_work_event(
        &self,
        tenant_id: &str,
        project_id: &str,
        source_name: &str,
        row_pk: &str,
        text: &str,
        model_id: &str,
        target_collection: &str,
    ) {
        self.emit_work_event_with_source_event(
            tenant_id,
            project_id,
            source_name,
            row_pk,
            text,
            model_id,
            target_collection,
            None,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn emit_backfill_work_event(
        &self,
        tenant_id: &str,
        project_id: &str,
        source_name: &str,
        row_pk: &str,
        text: &str,
        model_id: &str,
        target_collection: &str,
        backfill_event_id: &str,
        backfill_id: &str,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        let mut payload = build_work_event_payload(
            tenant_id,
            source_name,
            row_pk,
            text,
            model_id,
            target_collection,
        );
        if let Some(object) = payload.as_object_mut() {
            object.insert(
                "backfill_event_id".to_string(),
                serde_json::Value::String(backfill_event_id.to_string()),
            );
            object.insert(
                "backfill_id".to_string(),
                serde_json::Value::String(backfill_id.to_string()),
            );
        }
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_WORK,
            row_pk,
            tenant_id,
            project_id,
            payload,
            NativeEventContext {
                operation: "embedding.backfill.work.emit".to_string(),
                target_resource: source_name.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn emit_work_event_with_source_event(
        &self,
        tenant_id: &str,
        project_id: &str,
        source_name: &str,
        row_pk: &str,
        text: &str,
        model_id: &str,
        target_collection: &str,
        source_event_id: Option<&str>,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        let mut payload = build_work_event_payload(
            tenant_id,
            source_name,
            row_pk,
            text,
            model_id,
            target_collection,
        );
        if let (Some(event_id), Some(object)) = (source_event_id, payload.as_object_mut()) {
            object.insert(
                "source_event_id".to_string(),
                serde_json::Value::String(event_id.to_string()),
            );
        }
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_WORK,
            row_pk,
            tenant_id,
            project_id,
            payload,
            NativeEventContext {
                operation: "embedding.work.emit".to_string(),
                target_resource: source_name.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }
}
