//! Leader-owned background passes (change-driven work emit / backfill / vector
//! teardown) for the native `EmbeddingService`, plus the CDC-envelope decoders
//! and the shared vector-delete seam. Extracted verbatim from the former god
//! file.
//!
//! The passes follow the house journal-consumer pattern (cache invalidation,
//! search freshness): read published events from the durable CDC journal, join
//! them to this service's own registrations, dedup by an output event carrying
//! the source journal event id, and act ONLY through the mediated runtime seams.
//! The `run_embedding_work_emitter_once` entry point is re-exported from `mod.rs`
//! for `serve()`, which spawns it under leader election. The two `self`-using
//! vector-delete helpers stay inherent methods on `EmbeddingServiceImpl` so their
//! bodies are byte-identical.

use std::sync::Arc;

use sqlx::{PgPool, Row};

use crate::proto::{SelectRequest, Sort};

use super::EmbeddingServiceImpl;
use super::config::{
    DEFAULT_VECTOR_COLLECTION, EMBEDDING_BACKFILL_PAGE_LIMIT, EMBEDDING_TEARDOWN_DELETE_BATCH,
    STATUS_ACTIVE, TOPIC_BACKFILL_COMPLETED, TOPIC_BACKFILL_REQUESTED,
    TOPIC_SOURCE_CHANGE_COMPLETED, TOPIC_SOURCE_DELETED, TOPIC_SOURCE_TEARDOWN_COMPLETED,
    TOPIC_WORK, TOPIC_WORK_DEAD_LETTER, embedding_retry_sweep_limit,
};
use super::model::{StoredSource, stored_model_from_json};
use super::queue::{
    WorkBatch, complete_job_enumeration, dead_letter_exhausted_work, load_retryable_work,
    reemit_work_item, update_job_emission,
};
use super::store::{embedding_source_model, model_read_by_id};
use super::vector_store::VectorStore as _;

struct EmbeddingWorkJob {
    source: StoredSource,
    event_id: String,
    tenant_id: String,
    project_id: String,
    payload: serde_json::Value,
}

struct EmbeddingBackfillJob {
    source: StoredSource,
    event_id: String,
    tenant_id: String,
    project_id: String,
    backfill_id: String,
    mode: String,
}

/// One `udb.embedding.source.deleted.v1` journal event still awaiting vector
/// teardown (no `teardown.completed` marker keyed by its event id yet).
struct EmbeddingTeardownJob {
    event_id: String,
    tenant_id: String,
    project_id: String,
    source_name: String,
    /// The collection recorded on the source-deleted event (may be empty for a
    /// legacy row; the processor falls back to [`DEFAULT_VECTOR_COLLECTION`]).
    target_collection: String,
    vector_backend: String,
    vector_instance: String,
    /// CURRENT status of the (tenant, source_name) source row, if any — a source
    /// re-registered ACTIVE after the delete supersedes the teardown (deleting
    /// then would wipe fresh vectors).
    source_status: String,
}

/// SQL for one change-driven work-emitter journal pass. Every journal/outbox
/// payload key is read with the `COALESCE(payload->>k, payload->'payload'->>k)`
/// nesting fallback because the CDC journal ENVELOPE-nests emitted payloads
/// under `payload.payload` — the same lesson already applied to the backfill
/// loader; a top-level-only read matches zero real rows. Pure so the SQL shape
/// is unit-asserted.
pub(crate) fn embedding_work_jobs_sql(journal_relation: &str, outbox_relation: &str) -> String {
    let source = embedding_source_model();
    let source_rel = source.relation.clone();
    format!(
        "SELECT \
            s.{source_id}::TEXT AS source_id, \
            s.{tenant_id}::TEXT AS source_tenant_id, \
            s.{source_name}::TEXT AS source_name, \
            s.{source_message_type}::TEXT AS source_message_type, \
            s.{text_fields_json}::TEXT AS text_fields_json, \
            s.{target_collection}::TEXT AS target_collection, \
            s.{model_id}::TEXT AS model_id, \
            s.{tenant_column}::TEXT AS tenant_column, \
            s.{source_cdc_topic}::TEXT AS source_cdc_topic, \
            s.{status}::TEXT AS status, \
            j.event_id::TEXT AS event_id, \
            COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') AS event_tenant_id, \
            COALESCE(j.payload->>'project_id', j.payload->'payload'->>'project_id', '') AS project_id, \
            j.payload::TEXT AS payload_json \
         FROM {journal_relation} j \
         JOIN {source_rel} s \
           ON s.{tenant_id}::TEXT = COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') \
          AND s.{status} = $1 \
          AND COALESCE(s.{source_cdc_topic}::TEXT, '') <> '' \
          AND s.{source_cdc_topic} = j.topic \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') <> '' \
           AND j.topic <> $2 \
           AND NOT EXISTS ( \
               SELECT 1 FROM {outbox_relation} o \
               WHERE o.topic = $2 \
                 AND COALESCE(o.payload->>'source_event_id', o.payload->'payload'->>'source_event_id') = j.event_id::TEXT \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM {journal_relation} done \
               WHERE done.topic = $2 \
                 AND COALESCE(done.payload->>'source_event_id', done.payload->'payload'->>'source_event_id') = j.event_id::TEXT \
           ) \
         ORDER BY j.published_at ASC, j.event_id ASC \
         LIMIT $3",
        source_id = source.q("source_id"),
        tenant_id = source.q("tenant_id"),
        source_name = source.q("source_name"),
        source_message_type = source.q("source_message_type"),
        text_fields_json = source.q("text_fields_json"),
        target_collection = source.q("target_collection"),
        model_id = source.q("model_id"),
        tenant_column = source.q("tenant_column"),
        source_cdc_topic = source.q("source_cdc_topic"),
        status = source.q("status"),
    )
}

async fn load_embedding_work_jobs(
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    batch: i64,
) -> Result<Vec<EmbeddingWorkJob>, String> {
    let limit = batch.max(1);
    let rows = sqlx::query(&embedding_work_jobs_sql(journal_relation, outbox_relation))
        .bind(STATUS_ACTIVE)
        .bind(TOPIC_SOURCE_CHANGE_COMPLETED)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|err| format!("load embedding work jobs failed: {err}"))?;

    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        let payload_json: String = row
            .try_get("payload_json")
            .map_err(|err| format!("decode embedding source payload failed: {err}"))?;
        let payload: serde_json::Value = serde_json::from_str(&payload_json)
            .map_err(|err| format!("decode embedding source payload JSON failed: {err}"))?;
        jobs.push(EmbeddingWorkJob {
            source: StoredSource {
                source_id: row
                    .try_get("source_id")
                    .map_err(|err| format!("decode embedding source id failed: {err}"))?,
                tenant_id: row
                    .try_get("source_tenant_id")
                    .map_err(|err| format!("decode embedding source tenant failed: {err}"))?,
                source_name: row
                    .try_get("source_name")
                    .map_err(|err| format!("decode embedding source name failed: {err}"))?,
                source_message_type: row
                    .try_get("source_message_type")
                    .map_err(|err| format!("decode embedding source message type failed: {err}"))?,
                text_fields_json: row
                    .try_get("text_fields_json")
                    .map_err(|err| format!("decode embedding text fields failed: {err}"))?,
                target_collection: row
                    .try_get("target_collection")
                    .map_err(|err| format!("decode embedding collection failed: {err}"))?,
                model_id: row
                    .try_get("model_id")
                    .map_err(|err| format!("decode embedding model id failed: {err}"))?,
                tenant_column: row
                    .try_get("tenant_column")
                    .map_err(|err| format!("decode embedding tenant column failed: {err}"))?,
                source_cdc_topic: row
                    .try_get("source_cdc_topic")
                    .map_err(|err| format!("decode embedding source cdc topic failed: {err}"))?,
                status: row
                    .try_get("status")
                    .map_err(|err| format!("decode embedding source status failed: {err}"))?,
            },
            event_id: row
                .try_get("event_id")
                .map_err(|err| format!("decode embedding event id failed: {err}"))?,
            tenant_id: row
                .try_get("event_tenant_id")
                .map_err(|err| format!("decode embedding event tenant failed: {err}"))?,
            project_id: row
                .try_get("project_id")
                .map_err(|err| format!("decode embedding event project failed: {err}"))?,
            payload,
        });
    }
    Ok(jobs)
}

async fn load_embedding_backfill_jobs(
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    batch: i64,
) -> Result<Vec<EmbeddingBackfillJob>, String> {
    let source = embedding_source_model();
    let source_rel = source.relation.clone();
    let limit = batch.max(1);
    let rows = sqlx::query(&format!(
        "SELECT \
            s.{source_id}::TEXT AS source_id, \
            s.{tenant_id}::TEXT AS source_tenant_id, \
            s.{source_name}::TEXT AS source_name, \
            s.{source_message_type}::TEXT AS source_message_type, \
            s.{text_fields_json}::TEXT AS text_fields_json, \
            s.{target_collection}::TEXT AS target_collection, \
            s.{model_id}::TEXT AS model_id, \
            s.{tenant_column}::TEXT AS tenant_column, \
            s.{source_cdc_topic}::TEXT AS source_cdc_topic, \
            s.{status}::TEXT AS status, \
            j.event_id::TEXT AS event_id, \
            COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') AS event_tenant_id, \
            COALESCE(j.payload->>'project_id', j.payload->'payload'->>'project_id', '') AS project_id, \
            COALESCE(j.payload->>'backfill_id', j.payload->'payload'->>'backfill_id', '') AS backfill_id, \
            COALESCE(j.payload->>'mode', j.payload->'payload'->>'mode', 'INCREMENTAL') AS mode \
         FROM {journal_relation} j \
         JOIN {source_rel} s \
           ON s.{tenant_id}::TEXT = COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') \
          AND s.{source_name}::TEXT = COALESCE(j.payload->>'source', j.payload->'payload'->>'source', '') \
          AND s.{status} = $1 \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND j.topic = $2 \
           AND COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') <> '' \
           AND COALESCE(j.payload->>'backfill_id', j.payload->'payload'->>'backfill_id', '') <> '' \
           AND NOT EXISTS ( \
               SELECT 1 FROM {outbox_relation} pending_done \
               WHERE pending_done.topic = $3 \
                 AND COALESCE(pending_done.payload->>'backfill_event_id', pending_done.payload->'payload'->>'backfill_event_id') = j.event_id::TEXT \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM {journal_relation} done \
               WHERE done.topic = $3 \
                 AND COALESCE(done.payload->>'backfill_event_id', done.payload->'payload'->>'backfill_event_id') = j.event_id::TEXT \
           ) \
         ORDER BY j.published_at ASC, j.event_id ASC \
         LIMIT $4",
        source_id = source.q("source_id"),
        tenant_id = source.q("tenant_id"),
        source_name = source.q("source_name"),
        source_message_type = source.q("source_message_type"),
        text_fields_json = source.q("text_fields_json"),
        target_collection = source.q("target_collection"),
        model_id = source.q("model_id"),
        tenant_column = source.q("tenant_column"),
        source_cdc_topic = source.q("source_cdc_topic"),
        status = source.q("status"),
    ))
    .bind(STATUS_ACTIVE)
    .bind(TOPIC_BACKFILL_REQUESTED)
    .bind(TOPIC_BACKFILL_COMPLETED)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load embedding backfill jobs failed: {err}"))?;

    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        jobs.push(EmbeddingBackfillJob {
            source: StoredSource {
                source_id: row
                    .try_get("source_id")
                    .map_err(|err| format!("decode embedding source id failed: {err}"))?,
                tenant_id: row
                    .try_get("source_tenant_id")
                    .map_err(|err| format!("decode embedding source tenant failed: {err}"))?,
                source_name: row
                    .try_get("source_name")
                    .map_err(|err| format!("decode embedding source name failed: {err}"))?,
                source_message_type: row
                    .try_get("source_message_type")
                    .map_err(|err| format!("decode embedding source message type failed: {err}"))?,
                text_fields_json: row
                    .try_get("text_fields_json")
                    .map_err(|err| format!("decode embedding text fields failed: {err}"))?,
                target_collection: row
                    .try_get("target_collection")
                    .map_err(|err| format!("decode embedding collection failed: {err}"))?,
                model_id: row
                    .try_get("model_id")
                    .map_err(|err| format!("decode embedding model id failed: {err}"))?,
                tenant_column: row
                    .try_get("tenant_column")
                    .map_err(|err| format!("decode embedding tenant column failed: {err}"))?,
                source_cdc_topic: row
                    .try_get("source_cdc_topic")
                    .map_err(|err| format!("decode embedding source cdc topic failed: {err}"))?,
                status: row
                    .try_get("status")
                    .map_err(|err| format!("decode embedding source status failed: {err}"))?,
            },
            event_id: row
                .try_get("event_id")
                .map_err(|err| format!("decode embedding backfill event id failed: {err}"))?,
            tenant_id: row
                .try_get("event_tenant_id")
                .map_err(|err| format!("decode embedding backfill tenant failed: {err}"))?,
            project_id: row
                .try_get("project_id")
                .map_err(|err| format!("decode embedding backfill project failed: {err}"))?,
            backfill_id: row
                .try_get("backfill_id")
                .map_err(|err| format!("decode embedding backfill id failed: {err}"))?,
            mode: row
                .try_get("mode")
                .unwrap_or_else(|_| "INCREMENTAL".to_string()),
        });
    }
    Ok(jobs)
}

/// SQL loading `udb.embedding.source.deleted.v1` journal events that have not
/// yet emitted a teardown-completed marker (checked in BOTH the pending outbox
/// and the replay journal, keyed by `teardown_event_id` — the dedup shape the
/// backfill loader uses). Payload keys use the nested-envelope COALESCE
/// fallback (see [`embedding_work_jobs_sql`]). The LEFT JOIN surfaces the
/// source row's CURRENT status so a re-registered (ACTIVE-again) source is
/// never torn down. Pure so the SQL shape is unit-asserted.
pub(crate) fn embedding_teardown_jobs_sql(journal_relation: &str, outbox_relation: &str) -> String {
    let source = embedding_source_model();
    let source_rel = source.relation.clone();
    format!(
        "SELECT \
            j.event_id::TEXT AS event_id, \
            COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') AS event_tenant_id, \
            COALESCE(j.payload->>'project_id', j.payload->'payload'->>'project_id', '') AS project_id, \
            COALESCE(j.payload->>'source', j.payload->'payload'->>'source', '') AS source_name, \
            COALESCE(j.payload->>'target_collection', j.payload->'payload'->>'target_collection', '') AS target_collection, \
            COALESCE(j.payload->>'vector_backend', j.payload->'payload'->>'vector_backend', 'qdrant') AS vector_backend, \
            COALESCE(j.payload->>'vector_instance', j.payload->'payload'->>'vector_instance', 'default') AS vector_instance, \
            COALESCE(s.{status}::TEXT, '') AS source_status \
         FROM {journal_relation} j \
         LEFT JOIN {source_rel} s \
           ON s.{tenant_id}::TEXT = COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') \
          AND s.{source_name}::TEXT = COALESCE(j.payload->>'source', j.payload->'payload'->>'source', '') \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND j.topic = $1 \
           AND COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') <> '' \
           AND COALESCE(j.payload->>'source', j.payload->'payload'->>'source', '') <> '' \
           AND NOT EXISTS ( \
               SELECT 1 FROM {outbox_relation} o \
               WHERE o.topic = $2 \
                 AND COALESCE(o.payload->>'teardown_event_id', o.payload->'payload'->>'teardown_event_id') = j.event_id::TEXT \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM {journal_relation} done \
               WHERE done.topic = $2 \
                 AND COALESCE(done.payload->>'teardown_event_id', done.payload->'payload'->>'teardown_event_id') = j.event_id::TEXT \
           ) \
         ORDER BY j.published_at ASC, j.event_id ASC \
         LIMIT $3",
        status = source.q("status"),
        tenant_id = source.q("tenant_id"),
        source_name = source.q("source_name"),
    )
}

/// SQL enumerating a deleted source's emitted point ids: DISTINCT
/// `(target_collection, row_pk)` pairs from `udb.embedding.work.v1` events in
/// the replay journal UNION the pending outbox, tenant+source scoped, with the
/// nested-envelope COALESCE fallback on every key, keyset-paginated on the pair
/// so each pass is bounded. Pure so the SQL shape is unit-asserted.
pub(crate) fn embedding_teardown_point_ids_sql(
    journal_relation: &str,
    outbox_relation: &str,
) -> String {
    format!(
        "SELECT ids.target_collection, ids.row_pk FROM ( \
            SELECT \
                COALESCE(j.payload->>'target_collection', j.payload->'payload'->>'target_collection', '') AS target_collection, \
                COALESCE(j.payload->>'row_pk', j.payload->'payload'->>'row_pk', '') AS row_pk \
            FROM {journal_relation} j \
            WHERE j.topic = $1 \
              AND COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') = $2 \
              AND COALESCE(j.payload->>'source', j.payload->'payload'->>'source', '') = $3 \
            UNION \
            SELECT \
                COALESCE(o.payload->>'target_collection', o.payload->'payload'->>'target_collection', ''), \
                COALESCE(o.payload->>'row_pk', o.payload->'payload'->>'row_pk', '') \
            FROM {outbox_relation} o \
            WHERE o.topic = $1 \
              AND COALESCE(o.payload->>'tenant_id', o.payload->'payload'->>'tenant_id', '') = $2 \
              AND COALESCE(o.payload->>'source', o.payload->'payload'->>'source', '') = $3 \
         ) ids \
         WHERE ids.row_pk <> '' \
           AND (ids.target_collection, ids.row_pk) > ($4, $5) \
         ORDER BY ids.target_collection ASC, ids.row_pk ASC \
         LIMIT $6"
    )
}

async fn load_embedding_source_teardown_jobs(
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    batch: i64,
) -> Result<Vec<EmbeddingTeardownJob>, String> {
    let limit = batch.max(1);
    let rows = sqlx::query(&embedding_teardown_jobs_sql(
        journal_relation,
        outbox_relation,
    ))
    .bind(TOPIC_SOURCE_DELETED)
    .bind(TOPIC_SOURCE_TEARDOWN_COMPLETED)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load embedding teardown jobs failed: {err}"))?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        jobs.push(EmbeddingTeardownJob {
            event_id: row
                .try_get("event_id")
                .map_err(|err| format!("decode embedding teardown event id failed: {err}"))?,
            tenant_id: row
                .try_get("event_tenant_id")
                .map_err(|err| format!("decode embedding teardown tenant failed: {err}"))?,
            project_id: row
                .try_get("project_id")
                .map_err(|err| format!("decode embedding teardown project failed: {err}"))?,
            source_name: row
                .try_get("source_name")
                .map_err(|err| format!("decode embedding teardown source failed: {err}"))?,
            target_collection: row
                .try_get("target_collection")
                .map_err(|err| format!("decode embedding teardown collection failed: {err}"))?,
            vector_backend: row
                .try_get("vector_backend")
                .map_err(|err| format!("decode embedding teardown backend failed: {err}"))?,
            vector_instance: row
                .try_get("vector_instance")
                .map_err(|err| format!("decode embedding teardown instance failed: {err}"))?,
            source_status: row
                .try_get("source_status")
                .map_err(|err| format!("decode embedding teardown source status failed: {err}"))?,
        });
    }
    Ok(jobs)
}

/// Tear down a deleted source's vectors. Point ids are enumerated from the
/// source's emitted `udb.embedding.work.v1` events (replay journal + pending
/// outbox), grouped per target collection, and deleted through the SHARED
/// vector seam in [`EMBEDDING_TEARDOWN_DELETE_BATCH`]-bounded batches; only
/// after every batch succeeds is the `teardown.completed` marker emitted (a
/// delete failure leaves the job pending so the next leader pass retries —
/// fail closed, no false completion). A source re-registered ACTIVE after the
/// delete supersedes the teardown: it is marked completed WITHOUT deleting.
///
/// Erasure is retention-INDEPENDENT: a filter-delete on the `{_tenant_id,
/// _source}` payload tags (`delete_embedding_points_by_source`) runs FIRST and
/// removes every tagged point regardless of journal retention, then the point-id
/// enumeration runs as a fallback for legacy points written before `_source`
/// tagging. Points a sidecar reported for a row that never produced a work event
/// are still tagged `_source` at upsert, so the filter-delete covers them too.
async fn process_embedding_teardown_job(
    service: &EmbeddingServiceImpl,
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    job: &EmbeddingTeardownJob,
) -> Result<i64, String> {
    if job.source_status == STATUS_ACTIVE {
        // Superseded by a re-registration: mark the delete event handled without
        // touching the store (deleting now would wipe the fresh source's vectors).
        service
            .emit_source_event(
                TOPIC_SOURCE_TEARDOWN_COMPLETED,
                &job.tenant_id,
                &job.project_id,
                &job.source_name,
                serde_json::json!({
                    "teardown_event_id": job.event_id,
                    "deleted": 0,
                    "superseded": true,
                }),
            )
            .await;
        return Ok(0);
    }
    let fallback_collection = if job.target_collection.trim().is_empty() {
        DEFAULT_VECTOR_COLLECTION.to_string()
    } else {
        job.target_collection.clone()
    };
    let store = service
        .vector_store_for_routing(&job.project_id, &job.vector_backend, &job.vector_instance)
        .map_err(|error| format!("embedding teardown vector store unavailable: {error}"))?;
    // B.1.5 — authoritative, retention-INDEPENDENT erasure FIRST: drop every
    // point tagged `{_tenant_id, _source}` in the source's collection by payload
    // filter. Unlike the point-id enumeration below (which can only see points
    // whose `udb.embedding.work.v1` events survive journal retention), this
    // erases a deleted source completely even after its work events are purged —
    // closing the GDPR right-to-erasure hole. A failure returns (job stays
    // pending → retried next leader pass), never a false completion. The journal
    // enumeration still runs, as the fallback for legacy points written before
    // `_source` tagging (and points left in a since-changed target collection).
    let source_filter = super::model::source_teardown_filter(&job.tenant_id, &job.source_name)
        .ok_or_else(|| "embedding teardown requires a tenant and source scope".to_string())?;
    store
        .delete_by_filter(&fallback_collection, source_filter)
        .await
        .map_err(|err| {
            format!(
                "embedding teardown filter-delete failed (collection \
                 {fallback_collection}): {err}"
            )
        })?;
    let mut deleted = 0i64;
    let mut cursor_collection = String::new();
    let mut cursor_pk = String::new();
    loop {
        let rows = sqlx::query(&embedding_teardown_point_ids_sql(
            journal_relation,
            outbox_relation,
        ))
        .bind(TOPIC_WORK)
        .bind(&job.tenant_id)
        .bind(&job.source_name)
        .bind(&cursor_collection)
        .bind(&cursor_pk)
        .bind(EMBEDDING_TEARDOWN_DELETE_BATCH)
        .fetch_all(pool)
        .await
        .map_err(|err| format!("enumerate embedding teardown points failed: {err}"))?;
        if rows.is_empty() {
            break;
        }
        let mut points = Vec::with_capacity(rows.len());
        for row in &rows {
            let collection: String = row.try_get("target_collection").map_err(|err| {
                format!("decode embedding teardown point collection failed: {err}")
            })?;
            let row_pk: String = row
                .try_get("row_pk")
                .map_err(|err| format!("decode embedding teardown point pk failed: {err}"))?;
            points.push((collection, row_pk));
        }
        if let Some((last_collection, last_pk)) = points.last() {
            cursor_collection = last_collection.clone();
            cursor_pk = last_pk.clone();
        }
        // Rows arrive ordered by collection, so consecutive runs form one
        // bounded seam call per collection.
        let mut index = 0usize;
        while index < points.len() {
            let group_collection = points[index].0.clone();
            let mut ids = Vec::new();
            while index < points.len() && points[index].0 == group_collection {
                ids.push(points[index].1.clone());
                index += 1;
            }
            let effective_collection = if group_collection.trim().is_empty() {
                fallback_collection.clone()
            } else {
                group_collection
            };
            let count = ids.len() as i64;
            store
                .delete_points(&effective_collection, ids)
                .await
                .map_err(|err| {
                    format!(
                        "embedding teardown vector delete failed (collection \
                         {effective_collection}): {err}"
                    )
                })?;
            deleted = deleted.saturating_add(count);
        }
        if points.len() < EMBEDDING_TEARDOWN_DELETE_BATCH as usize {
            break;
        }
    }
    service
        .emit_source_event(
            TOPIC_SOURCE_TEARDOWN_COMPLETED,
            &job.tenant_id,
            &job.project_id,
            &job.source_name,
            serde_json::json!({
                "teardown_event_id": job.event_id,
                "target_collection": job.target_collection,
                "deleted": deleted,
            }),
        )
        .await;
    Ok(deleted)
}

pub(crate) fn backfill_read_context(tenant_id: &str, project_id: &str) -> crate::RequestContext {
    crate::RequestContext {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        purpose: "embedding_backfill".to_string(),
        scopes: vec!["udb:read".to_string()],
        service_identity: "udb.embedding.backfill".to_string(),
        ..crate::RequestContext::default()
    }
}

pub(crate) fn backfill_select_request(
    source: &StoredSource,
    primary_key: &str,
    text_fields: &[String],
    after_pk: Option<&str>,
    project_column: Option<&str>,
    project_id: &str,
) -> SelectRequest {
    let mut fields = Vec::with_capacity(text_fields.len().saturating_add(1));
    fields.push(primary_key.to_string());
    let effective_text_fields = if text_fields.is_empty() {
        vec!["text".to_string()]
    } else {
        text_fields.to_vec()
    };
    for field in &effective_text_fields {
        if field != primary_key && !fields.contains(field) {
            fields.push(field.clone());
        }
    }
    let mut tenant_filter = serde_json::Map::new();
    tenant_filter.insert(
        source.tenant_column.clone(),
        serde_json::Value::String(source.tenant_id.clone()),
    );
    let mut filters = vec![serde_json::Value::Object(tenant_filter)];
    // Project isolation: the data-plane planner rejects a select over a
    // project-scoped source table unless the filter carries the project column
    // (`project isolation requires filter on <col>`). Backfill enumerates through
    // that same served SELECT path, so it must add the source's project column =
    // the backfill job's project_id (empty in single-project mode, which matches
    // the empty project_id stored on single-project rows).
    if let Some(project_column) = project_column.filter(|col| !col.trim().is_empty()) {
        let mut project_filter = serde_json::Map::new();
        project_filter.insert(
            project_column.to_string(),
            serde_json::Value::String(project_id.to_string()),
        );
        filters.push(serde_json::Value::Object(project_filter));
    }
    if let Some(after_pk) = after_pk.filter(|value| !value.trim().is_empty()) {
        let mut cursor_op = serde_json::Map::new();
        cursor_op.insert(
            "$gt".to_string(),
            serde_json::Value::String(after_pk.to_string()),
        );
        let mut cursor_filter = serde_json::Map::new();
        cursor_filter.insert(
            primary_key.to_string(),
            serde_json::Value::Object(cursor_op),
        );
        filters.push(serde_json::Value::Object(cursor_filter));
    }
    SelectRequest {
        message_type: source.source_message_type.clone(),
        filter: crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
            "$and": filters,
        })),
        fields,
        limit: EMBEDDING_BACKFILL_PAGE_LIMIT,
        sort: vec![Sort {
            field: primary_key.to_string(),
            descending: false,
        }],
        ..SelectRequest::default()
    }
}

fn json_row_pk(row: &serde_json::Value, primary_key: &str) -> String {
    json_string_field(row, primary_key).unwrap_or_default()
}

async fn load_active_model(
    service: &EmbeddingServiceImpl,
    tenant_id: &str,
    project_id: &str,
    model_id: &str,
) -> Result<super::model::StoredModel, String> {
    let runtime = service
        .runtime
        .as_ref()
        .ok_or_else(|| "embedding model lookup requires runtime dispatch".to_string())?;
    let context = super::super::native_helpers::native_service_context(
        &tonic::metadata::MetadataMap::new(),
        tenant_id,
        project_id,
    );
    runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            model_read_by_id(tenant_id, model_id),
        )
        .await
        .map_err(|error| format!("embedding model lookup failed: {error}"))?
        .first()
        .map(stored_model_from_json)
        .filter(|model| model.status == STATUS_ACTIVE && model.tenant_state == STATUS_ACTIVE)
        .ok_or_else(|| format!("active embedding model '{model_id}' not found for tenant"))
}

async fn process_embedding_backfill_job(
    service: &EmbeddingServiceImpl,
    job: &EmbeddingBackfillJob,
) -> Result<i64, String> {
    if job.source.status != STATUS_ACTIVE
        || job.source.tenant_id.trim().is_empty()
        || job.source.tenant_id.trim() != job.tenant_id.trim()
    {
        return Ok(0);
    }
    let runtime = service
        .runtime
        .as_ref()
        .ok_or_else(|| "embedding backfill requires runtime dispatch".to_string())?;
    let catalog = service
        .catalog
        .as_ref()
        .ok_or_else(|| "embedding backfill requires active catalog".to_string())?;
    let state = catalog.active_for(&job.project_id);
    let table = crate::broker::table_for_message(&state.manifest, &job.source.source_message_type)
        .ok_or_else(|| {
            format!(
                "embedding backfill source entity '{}' is not present in active catalog",
                job.source.source_message_type
            )
        })?;
    let primary_key = table.primary_key.first().cloned().ok_or_else(|| {
        format!(
            "embedding backfill source entity '{}' has no primary key",
            job.source.source_message_type
        )
    })?;
    let text_fields = parse_source_text_fields(&job.source.text_fields_json);
    // The source table's project column (if project-scoped) — the backfill SELECT
    // must filter on it or the planner rejects it for project isolation.
    let project_column = crate::generation::sql::resolve_project_column(table);
    let context = backfill_read_context(&job.tenant_id, &job.project_id);
    let model = load_active_model(
        service,
        &job.tenant_id,
        &job.project_id,
        &job.source.model_id,
    )
    .await?;
    if job.source.target_collection != model.active_collection {
        return Err(
            "embedding source/model collection binding changed before backfill".to_string(),
        );
    }
    let mut emitted = 0i64;
    let mut enumerated = 0i64;
    let mut after_pk: Option<String> = None;
    loop {
        let request = backfill_select_request(
            &job.source,
            &primary_key,
            &text_fields,
            after_pk.as_deref(),
            project_column,
            &job.project_id,
        );
        let (record_set, _) = runtime
            .select(&state.manifest, request, context.clone())
            .await
            .map_err(|err| format!("embedding backfill source select failed: {err}"))?;
        if record_set.records_json.is_empty() {
            break;
        }
        let mut last_pk = String::new();
        for raw in &record_set.records_json {
            let row: serde_json::Value = serde_json::from_slice(raw)
                .map_err(|err| format!("decode embedding backfill source row failed: {err}"))?;
            let row_pk = json_row_pk(&row, &primary_key);
            if row_pk.trim().is_empty() {
                continue;
            }
            last_pk = row_pk.clone();
            let text = extract_backfill_source_text(&row, &text_fields, table);
            if text.trim().is_empty() {
                continue;
            }
            let chunks = super::chunking::chunk_source_text_for_model(&text, &model);
            let result = service
                .persist_and_emit_work_batch(WorkBatch {
                    tenant_id: &job.tenant_id,
                    project_id: &job.project_id,
                    job_id: &job.backfill_id,
                    source_name: &job.source.source_name,
                    parent_pk: &row_pk,
                    document_id: "",
                    doc_version: "1",
                    target_collection: &model.active_collection,
                    model: &model,
                    chunks: &chunks,
                    parent_text: &text,
                    force: job.mode.eq_ignore_ascii_case("FULL"),
                })
                .await
                .map_err(|error| format!("persist embedding backfill work failed: {error}"))?;
            if !result.stale_point_ids.is_empty() {
                service
                    .vector_store_for_model(&job.project_id, &model)
                    .map_err(|error| error.to_string())?
                    .delete_points(&model.active_collection, result.stale_point_ids)
                    .await
                    .map_err(|error| format!("delete stale embedding chunks failed: {error}"))?;
            }
            emitted = emitted.saturating_add(result.emitted);
            enumerated = enumerated.saturating_add(1);
        }
        if record_set.records_json.len() < EMBEDDING_BACKFILL_PAGE_LIMIT as usize
            || last_pk.is_empty()
        {
            break;
        }
        after_pk = Some(last_pk);
    }
    if let Some(pool) = service.pg_pool.as_ref() {
        update_job_emission(pool, &job.tenant_id, &job.backfill_id, enumerated, emitted)
            .await
            .map_err(|error| error.to_string())?;
        complete_job_enumeration(pool, &job.tenant_id, &job.backfill_id)
            .await
            .map_err(|error| error.to_string())?;
    }
    service
        .emit_source_event(
            TOPIC_BACKFILL_COMPLETED,
            &job.tenant_id,
            &job.project_id,
            &job.source.source_name,
            serde_json::json!({
                "backfill_event_id": job.event_id,
                "backfill_id": job.backfill_id,
                "rows_enumerated": enumerated,
                "chunks_emitted": emitted,
            }),
        )
        .await;
    Ok(emitted)
}

fn json_string_field(payload: &serde_json::Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(crate) fn source_change_row(payload: &serde_json::Value) -> Option<&serde_json::Value> {
    payload
        .get("after")
        .or_else(|| payload.get("payload").and_then(|inner| inner.get("after")))
        .or_else(|| payload.get("payload"))
}

pub(crate) fn source_change_row_pk(payload: &serde_json::Value) -> String {
    for key in ["document_id", "row_pk", "id"] {
        if let Some(value) = json_string_field(payload, key) {
            return value;
        }
    }
    if let Some(row) = source_change_row(payload) {
        for key in ["row_pk", "id"] {
            if let Some(value) = json_string_field(row, key) {
                return value;
            }
        }
    }
    String::new()
}

fn source_change_operation(payload: &serde_json::Value) -> String {
    json_string_field(payload, "operation")
        .or_else(|| json_string_field(payload, "op"))
        .or_else(|| {
            payload
                .get("payload")
                .and_then(|inner| json_string_field(inner, "op"))
        })
        .unwrap_or_default()
}

pub(crate) fn source_change_is_delete(payload: &serde_json::Value) -> bool {
    let op = source_change_operation(payload);
    op.eq_ignore_ascii_case("delete") || op.eq_ignore_ascii_case("d")
}

pub(crate) fn parse_source_text_fields(raw: &str) -> Vec<String> {
    if let Ok(fields) = serde_json::from_str::<Vec<String>>(raw) {
        return fields;
    }
    if let Ok(inner) = serde_json::from_str::<String>(raw) {
        if let Ok(fields) = serde_json::from_str::<Vec<String>>(&inner) {
            return fields;
        }
    }
    Vec::new()
}

async fn process_embedding_work_job(
    service: &EmbeddingServiceImpl,
    job: &EmbeddingWorkJob,
) -> bool {
    let source_tenant = job.source.tenant_id.trim();
    let event_tenant = job.tenant_id.trim();
    if source_tenant.is_empty() || event_tenant != source_tenant {
        return false;
    }
    if job.source.status != STATUS_ACTIVE || job.source.source_cdc_topic.trim().is_empty() {
        return false;
    }
    let row_pk = source_change_row_pk(&job.payload);
    if row_pk.is_empty() {
        return false;
    }
    let collection = job.source.collection();
    let model = match load_active_model(
        service,
        event_tenant,
        &job.project_id,
        &job.source.model_id,
    )
    .await
    {
        Ok(model) => model,
        Err(error) => {
            tracing::warn!(source = %job.source.source_name, %error, "embedding source change model lookup failed");
            return false;
        }
    };
    if collection != model.active_collection {
        tracing::warn!(source = %job.source.source_name, "embedding source/model collection binding is inconsistent");
        return false;
    }
    let store = match service.vector_store_for_model(&job.project_id, &model) {
        Ok(store) => store,
        Err(error) => {
            tracing::warn!(%error, "embedding vector store unavailable");
            return false;
        }
    };
    if source_change_is_delete(&job.payload) {
        if let Some(filter) =
            super::model::row_teardown_filter(event_tenant, &job.source.source_name, &row_pk)
            && let Err(error) = store.delete_by_filter(&collection, filter).await
        {
            tracing::warn!(%error, "embedding row filtered delete failed");
            return false;
        }
        if let Err(error) = store.delete_points(&collection, vec![row_pk.clone()]).await {
            tracing::warn!(%error, "embedding row legacy point delete failed");
            return false;
        }
        service
            .emit_source_event(
                TOPIC_SOURCE_CHANGE_COMPLETED,
                event_tenant,
                &job.project_id,
                &job.source.source_name,
                serde_json::json!({"source_event_id": job.event_id, "deleted": true}),
            )
            .await;
        return true;
    }

    let text_fields = parse_source_text_fields(&job.source.text_fields_json);
    let row = source_change_row(&job.payload).unwrap_or(&job.payload);
    let text = extract_source_text(row, &text_fields);
    if text.trim().is_empty() {
        if let Some(filter) =
            super::model::row_teardown_filter(event_tenant, &job.source.source_name, &row_pk)
            && let Err(error) = store.delete_by_filter(&collection, filter).await
        {
            tracing::warn!(%error, "embedding empty-row filtered delete failed");
            return false;
        }
        if let Err(error) = store.delete_points(&collection, vec![row_pk.clone()]).await {
            tracing::warn!(%error, "embedding empty-row legacy point delete failed");
            return false;
        }
        service
            .emit_source_event(
                TOPIC_SOURCE_CHANGE_COMPLETED,
                event_tenant,
                &job.project_id,
                &job.source.source_name,
                serde_json::json!({"source_event_id": job.event_id, "empty": true}),
            )
            .await;
        return true;
    }
    let chunks = super::chunking::chunk_source_text_for_model(&text, &model);
    let result = match service
        .persist_and_emit_work_batch(WorkBatch {
            tenant_id: event_tenant,
            project_id: &job.project_id,
            job_id: "",
            source_name: &job.source.source_name,
            parent_pk: &row_pk,
            document_id: "",
            doc_version: "1",
            target_collection: &collection,
            model: &model,
            chunks: &chunks,
            parent_text: &text,
            force: false,
        })
        .await
    {
        Ok(result) => result,
        Err(error) => {
            tracing::warn!(%error, "embedding source work persistence failed");
            return false;
        }
    };
    if !result.stale_point_ids.is_empty() {
        if let Err(error) = store
            .delete_points(&collection, result.stale_point_ids)
            .await
        {
            tracing::warn!(%error, "embedding stale tail chunk delete failed");
            return false;
        }
    }
    service.emit_source_event(
        TOPIC_SOURCE_CHANGE_COMPLETED, event_tenant, &job.project_id,
        &job.source.source_name,
        serde_json::json!({"source_event_id": job.event_id, "chunks_emitted": result.emitted, "chunks_unchanged": result.unchanged}),
    ).await;
    true
}

/// Run one leader-owned pass over the durable CDC journal, in three stages:
/// (1) active embedding sources are joined by `(tenant_id, source_cdc_topic)`
/// to published source changes — source events that already produced
/// `udb.embedding.work.v1` are skipped by `source_event_id` in both the outbox
/// and replay journal; (2) requested backfills enumerate their source rows via
/// the served SELECT path and emit per-row work; (3) `source.deleted` events
/// tear down the deleted source's vectors through the shared vector seam,
/// deduplicated by a `teardown.completed` marker.
pub(crate) async fn run_embedding_work_emitter_once(
    service: Arc<EmbeddingServiceImpl>,
    journal_relation: &str,
    batch: i64,
) -> Result<i64, String> {
    let pool = service
        .pg_pool
        .as_ref()
        .ok_or_else(|| "embedding work emitter requires native Postgres store".to_string())?;
    let outbox_relation = service
        .outbox_relation
        .as_deref()
        .ok_or_else(|| "embedding work emitter requires transactional outbox".to_string())?;
    let jobs = load_embedding_work_jobs(pool, journal_relation, outbox_relation, batch).await?;
    let mut acted = 0i64;
    for job in &jobs {
        if process_embedding_work_job(&service, job).await {
            acted = acted.saturating_add(1);
        }
    }
    let backfills =
        load_embedding_backfill_jobs(pool, journal_relation, outbox_relation, batch).await?;
    for job in &backfills {
        acted = acted.saturating_add(process_embedding_backfill_job(&service, job).await?);
    }
    let teardowns =
        load_embedding_source_teardown_jobs(pool, journal_relation, outbox_relation, batch).await?;
    for job in &teardowns {
        acted = acted.saturating_add(
            process_embedding_teardown_job(&service, pool, journal_relation, outbox_relation, job)
                .await?,
        );
    }
    let retry_items = load_retryable_work(pool, embedding_retry_sweep_limit())
        .await
        .map_err(|error| error.to_string())?;
    // One model load per distinct (tenant, project, model) across the sweep —
    // a retry batch is typically many items of ONE model, so per-item loads
    // were pure N+1.
    let mut sweep_models: std::collections::HashMap<(String, String, String), _> =
        std::collections::HashMap::new();
    for item in &retry_items {
        let model_key = (
            item.tenant_id.clone(),
            item.project_id.clone(),
            item.model_id.clone(),
        );
        if !sweep_models.contains_key(&model_key) {
            let loaded =
                load_active_model(&service, &item.tenant_id, &item.project_id, &item.model_id)
                    .await?;
            sweep_models.insert(model_key.clone(), loaded);
        }
        let model = sweep_models
            .get(&model_key)
            .expect("model cached by the branch above");
        reemit_work_item(&service, item, model)
            .await
            .map_err(|error| error.to_string())?;
        service.metrics.inc_embedding_work("retried");
        acted = acted.saturating_add(1);
    }
    let exhausted = dead_letter_exhausted_work(pool, embedding_retry_sweep_limit())
        .await
        .map_err(|error| error.to_string())?;
    for item in &exhausted {
        service
            .emit_source_event(
                TOPIC_WORK_DEAD_LETTER,
                &item.tenant_id,
                &item.project_id,
                &item.source_name,
                serde_json::json!({
                    "work_item_id": item.work_item_id,
                    "attempt_count": item.attempt_count,
                    "error": "embedding work visibility timeout exhausted",
                }),
            )
            .await;
        acted = acted.saturating_add(1);
    }
    let backlog: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM udb_embedding.embedding_work_items WHERE status = 'PENDING'",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("load embedding backlog failed: {error}"))?;
    service.metrics.set_embedding_backlog(backlog);
    Ok(acted)
}

/// Concatenate the configured source text field(s) from a change row. When no
/// fields are configured, falls back to a `text` field if present. Pure.
pub(crate) fn extract_source_text(row: &serde_json::Value, fields: &[String]) -> String {
    if fields.is_empty() {
        return row
            .get("text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
    }
    fields
        .iter()
        .filter_map(|field| row.get(field).and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>()
        .join(" ")
}

fn extract_backfill_source_text(
    row: &serde_json::Value,
    fields: &[String],
    table: &crate::generation::ManifestTable,
) -> String {
    if fields.is_empty() {
        return extract_source_text(row, fields);
    }
    fields
        .iter()
        .filter_map(|field| {
            row.get(field)
                .and_then(serde_json::Value::as_str)
                .or_else(|| {
                    table
                        .columns
                        .iter()
                        .find(|column| column.field_name == *field)
                        .and_then(|column| row.get(&column.column_name))
                        .and_then(serde_json::Value::as_str)
                })
        })
        .collect::<Vec<_>>()
        .join(" ")
}
