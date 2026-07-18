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

/// Bound an over-long embedding input to `max_chars`, preferring to cut at the
/// last whitespace within the bound so a word is not split. Char-based, so it
/// never panics on a multi-byte boundary; `<= max_chars` returns the text
/// unchanged. Pure.
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

/// Build the `udb.embedding.work.v1` payload for ONE chunk of a source row. The
/// event `row_pk` is the chunk's point id (bare `parent_pk` for a single-chunk
/// row, `parent_pk#chunk:seq` otherwise) so the chunk-agnostic sidecar echoes it
/// back and each chunk lands as its own point; chunk provenance (`parent_pk`,
/// `chunk_seq`, `chunk_count`, `char_start`) rides alongside for observability
/// and downstream stamping. Reuses [`build_work_event_payload`] so the
/// no-credential invariant holds for chunk events too. Returns the payload and
/// the chunk point id (the outbox partition key). Pure.
pub(crate) fn build_chunk_work_event_payload(
    tenant_id: &str,
    source_name: &str,
    parent_pk: &str,
    chunk: &super::chunking::Chunk,
    chunk_count: usize,
    model_id: &str,
    target_collection: &str,
    max_chars: usize,
) -> (String, serde_json::Value) {
    let point_id = super::chunking::chunk_point_id(parent_pk, chunk.seq, chunk_count);
    let mut payload = build_work_event_payload(
        tenant_id,
        source_name,
        &point_id,
        &chunk.text,
        model_id,
        target_collection,
        max_chars,
    );
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "parent_pk".to_string(),
            serde_json::Value::String(parent_pk.to_string()),
        );
        object.insert("chunk_seq".to_string(), serde_json::json!(chunk.seq));
        object.insert("chunk_count".to_string(), serde_json::json!(chunk_count));
        object.insert(
            "char_start".to_string(),
            serde_json::json!(chunk.char_start),
        );
    }
    (point_id, payload)
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
        // Fan out one work event per chunk (a short row → one bare-id event, so
        // behavior is unchanged for it). Each chunk carries the backfill lineage.
        let max_chars = super::config::max_embedding_text_chars();
        let chunks = super::chunking::chunk_source_text(text);
        for chunk in &chunks {
            let (point_id, mut payload) = build_chunk_work_event_payload(
                tenant_id,
                source_name,
                row_pk,
                chunk,
                chunks.len(),
                model_id,
                target_collection,
                max_chars,
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
                &point_id,
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
        // Fan out one work event per chunk. A row within one window yields a
        // single bare-`row_pk` event (unchanged behavior); a long row yields N
        // composite-id chunk events. Each chunk carries the source event id so
        // the leader pass dedups the whole row's fan-out by `source_event_id`.
        let max_chars = super::config::max_embedding_text_chars();
        let chunks = super::chunking::chunk_source_text(text);
        for chunk in &chunks {
            let (point_id, mut payload) = build_chunk_work_event_payload(
                tenant_id,
                source_name,
                row_pk,
                chunk,
                chunks.len(),
                model_id,
                target_collection,
                max_chars,
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
                &point_id,
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
}
