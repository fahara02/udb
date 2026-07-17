//! The per-mutation outbox event emission for the native `SearchService`.
//! Extracted verbatim from the former god file — the best-effort
//! `enqueue_outbox_event_with_context` emit and the base/extra payload merge are
//! byte-for-byte identical. `emit_index_event` stays an inherent method on
//! `SearchServiceImpl` (it uses `self`), shared between the RPC handlers and the
//! leader-owned background passes.

use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_with_context};
use super::SearchServiceImpl;

impl SearchServiceImpl {
    /// Emit a per-mutation versioned dot-topic outbox event (best-effort).
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
