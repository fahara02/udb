use std::collections::BTreeSet;

use sqlx::{PgPool, Row};
use tonic::Status;
use uuid::Uuid;

use super::EmbeddingServiceImpl;
use super::chunking::{Chunk, chunk_content_hash, chunk_point_id, deterministic_work_item_id};
use super::config::{
    DEFAULT_WORK_MAX_ATTEMPTS, EMBEDDING_WORK_ITEM_MSG, TOPIC_WORK, WORK_ACKED, WORK_DEAD,
    WORK_PENDING, embedding_work_visibility_timeout,
};
use super::errors::{embedding_field_violation, embedding_policy_status_with_code};
use super::model::{StoredModel, json_i32, json_i64, json_str, native_json_object};
use super::store::{work_item_conflict, work_item_read_by_id, work_item_record};

#[derive(Clone, Debug)]
pub(crate) struct DurableWorkItem {
    pub(crate) work_item_id: String,
    pub(crate) tenant_id: String,
    pub(crate) project_id: String,
    pub(crate) job_id: String,
    pub(crate) source_name: String,
    pub(crate) parent_pk: String,
    pub(crate) point_id: String,
    pub(crate) document_id: String,
    pub(crate) doc_version: String,
    pub(crate) chunk_seq: i32,
    pub(crate) chunk_count: i32,
    pub(crate) chunk_hash: String,
    pub(crate) chunk_text: String,
    pub(crate) parent_text: String,
    pub(crate) char_start: i64,
    pub(crate) char_end: i64,
    pub(crate) token_start: i64,
    pub(crate) token_end: i64,
    pub(crate) model_id: String,
    pub(crate) target_collection: String,
    pub(crate) status: String,
    pub(crate) attempt_count: i32,
    pub(crate) max_attempts: i32,
    pub(crate) token_count: i64,
}

impl DurableWorkItem {
    pub(crate) fn from_native_json(row: &serde_json::Value) -> Self {
        let map = native_json_object(row);
        Self {
            work_item_id: json_str(map, "work_item_id"),
            tenant_id: json_str(map, "tenant_id"),
            project_id: json_str(map, "project_id"),
            job_id: json_str(map, "job_id"),
            source_name: json_str(map, "source_name"),
            parent_pk: json_str(map, "parent_pk"),
            point_id: json_str(map, "point_id"),
            document_id: json_str(map, "document_id"),
            doc_version: json_str(map, "doc_version"),
            chunk_seq: json_i32(map, "chunk_seq"),
            chunk_count: json_i32(map, "chunk_count"),
            chunk_hash: json_str(map, "chunk_hash"),
            chunk_text: json_str(map, "chunk_text"),
            parent_text: json_str(map, "parent_text"),
            char_start: json_i64(map, "char_start"),
            char_end: json_i64(map, "char_end"),
            token_start: json_i64(map, "token_start"),
            token_end: json_i64(map, "token_end"),
            model_id: json_str(map, "model_id"),
            target_collection: json_str(map, "target_collection"),
            status: json_str(map, "status"),
            attempt_count: json_i32(map, "attempt_count"),
            max_attempts: json_i32(map, "max_attempts"),
            token_count: json_i64(map, "token_count"),
        }
    }

    fn work_payload(&self, model: &StoredModel) -> serde_json::Value {
        serde_json::json!({
            "work_item_id": self.work_item_id,
            "job_id": self.job_id,
            "tenant_id": self.tenant_id,
            "project_id": self.project_id,
            "source": self.source_name,
            "parent_pk": self.parent_pk,
            "row_pk": self.point_id,
            "document_id": self.document_id,
            "doc_version": self.doc_version,
            "chunk_seq": self.chunk_seq,
            "chunk_count": self.chunk_count,
            "chunk_hash": self.chunk_hash,
            "token_count": self.token_count,
            "text": self.chunk_text,
            "parent_text": self.parent_text,
            "char_start": self.char_start,
            "char_end": self.char_end,
            "token_start": self.token_start,
            "token_end": self.token_end,
            "model_id": self.model_id,
            "target_collection": self.target_collection,
            "provider": model.provider,
            "model_name": model.model_name,
            "model_version": model.version,
            "dimensions": model.dimensions,
            "distance_metric": model.distance_metric,
            "normalize": model.normalize,
            "output_dtype": model.output_dtype,
            "rescore": model.rescore,
            "task_type": model.task_type,
            "asymmetric": model.asymmetric,
            // Always the model's FULL dimensionality: the active collection's
            // geometry is created and verified at `model.dimensions`, so a
            // smaller Matryoshka cut here would fail the dims-of-record gate.
            // `matryoshka_dims` is registry-validated capability metadata for a
            // FUTURE alias cutover into a truncated-dim collection; it is not
            // consulted on the live emit path yet.
            "truncate_dim": model.dimensions,
            "tokenizer": model.tokenizer,
            "provider_endpoint_ref": model.provider_endpoint_ref,
            "contextual_retrieval": model.contextual_retrieval,
            "late_chunking": model.late_chunking,
            "attempt": self.attempt_count + 1,
        })
    }
}

pub(crate) struct WorkBatch<'a> {
    pub(crate) tenant_id: &'a str,
    pub(crate) project_id: &'a str,
    pub(crate) job_id: &'a str,
    pub(crate) source_name: &'a str,
    pub(crate) parent_pk: &'a str,
    pub(crate) document_id: &'a str,
    pub(crate) doc_version: &'a str,
    pub(crate) target_collection: &'a str,
    pub(crate) model: &'a StoredModel,
    pub(crate) chunks: &'a [Chunk],
    pub(crate) parent_text: &'a str,
    pub(crate) force: bool,
}

#[derive(Default)]
pub(crate) struct WorkBatchResult {
    pub(crate) emitted: i64,
    pub(crate) unchanged: i64,
    pub(crate) stale_point_ids: Vec<String>,
}

fn queue_status(operation: &'static str, error: impl std::fmt::Display) -> Status {
    embedding_policy_status_with_code(
        tonic::Code::Internal,
        operation,
        "embedding_durable_queue",
        format!("embedding durable queue failed: {error}"),
    )
}

async fn insert_work_outbox(
    pool: &PgPool,
    relation: Option<&str>,
    item: &DurableWorkItem,
    payload: serde_json::Value,
) -> Result<(), Status> {
    let relation = relation.ok_or_else(|| {
        embedding_policy_status_with_code(
            tonic::Code::FailedPrecondition,
            "embedding_work_enqueue",
            "embedding_outbox_required",
            "embedding work requires a configured durable outbox",
        )
    })?;
    let event_id = Uuid::new_v4();
    let actor = crate::runtime::otel::current_actor();
    let auth_method = crate::runtime::otel::current_auth_method();
    let trace = crate::runtime::otel::current_trace_context();
    let env = super::super::auth_service::events::ComplianceEnvelope {
        actor: if actor.is_empty() {
            "udb:embedding".to_string()
        } else {
            actor
        },
        operation: "embedding.work.emit".to_string(),
        outcome: "success".to_string(),
        auth_method: if auth_method.is_empty() {
            "system".to_string()
        } else {
            auth_method
        },
        decision_id: crate::runtime::otel::current_decision_id(),
        policy_version: crate::runtime::otel::current_policy_revision(),
        trace_id: trace.trace_id,
        span_id: trace.span_id,
        target_resource: item.source_name.clone(),
        ..Default::default()
    };
    let envelope = super::super::auth_service::events::build_native_compliance_envelope(
        &event_id.to_string(),
        TOPIC_WORK,
        &item.point_id,
        &item.tenant_id,
        &item.project_id,
        &env,
        &item.work_item_id,
        "storage_only_text",
        1,
        &["text".to_string(), "parent_text".to_string()],
        payload,
    );
    crate::runtime::cdc::insert_outbox_row(
        pool,
        relation,
        event_id,
        TOPIC_WORK,
        &item.point_id,
        &envelope,
    )
    .await
    .map_err(|error| queue_status("embedding_work_enqueue", error))
}

impl EmbeddingServiceImpl {
    pub(crate) async fn persist_and_emit_work_batch(
        &self,
        batch: WorkBatch<'_>,
    ) -> Result<WorkBatchResult, Status> {
        let runtime = self.require_runtime()?;
        let pool = self.pg_pool.as_ref().ok_or_else(|| {
            embedding_policy_status_with_code(
                tonic::Code::FailedPrecondition,
                "embedding_work_persist",
                "embedding_postgres_required",
                "embedding durable work requires Postgres",
            )
        })?;
        let context = super::super::native_helpers::native_service_context(
            &tonic::metadata::MetadataMap::new(),
            batch.tenant_id,
            batch.project_id,
        );
        let mut result = WorkBatchResult::default();
        if batch.model.late_chunking
            && batch.parent_text.split_whitespace().count()
                > batch.model.max_input_tokens.max(1) as usize
        {
            return Err(embedding_field_violation(
                "parent_text",
                "must fit within the registered model max_input_tokens for late chunking",
                "late chunking parent text exceeds the model token envelope",
            ));
        }
        let advanced_parent_text = if batch.model.contextual_retrieval || batch.model.late_chunking
        {
            batch.parent_text
        } else {
            ""
        };
        let mut current_work_ids = Vec::with_capacity(batch.chunks.len());
        let mut current_point_ids = BTreeSet::new();

        for chunk in batch.chunks {
            let chunk_hash = chunk_content_hash(&chunk.text);
            let point_id = chunk_point_id(batch.parent_pk, chunk.seq, batch.chunks.len());
            let work_item_id = deterministic_work_item_id(
                batch.tenant_id,
                batch.source_name,
                batch.parent_pk,
                chunk.seq,
                batch.doc_version,
                &chunk_hash,
            );
            current_work_ids.push(work_item_id.clone());
            current_point_ids.insert(point_id.clone());
            let existing = runtime
                .native_entity_read_for_service(
                    "embedding",
                    &context,
                    work_item_read_by_id(batch.tenant_id, &work_item_id),
                )
                .await?
                .first()
                .map(DurableWorkItem::from_native_json);
            if !batch.force
                && existing
                    .as_ref()
                    .is_some_and(|item| item.status == WORK_ACKED)
            {
                result.unchanged += 1;
                continue;
            }
            let item = DurableWorkItem {
                work_item_id: work_item_id.clone(),
                tenant_id: batch.tenant_id.to_string(),
                project_id: batch.project_id.to_string(),
                job_id: batch.job_id.to_string(),
                source_name: batch.source_name.to_string(),
                parent_pk: batch.parent_pk.to_string(),
                point_id,
                document_id: batch.document_id.to_string(),
                doc_version: batch.doc_version.to_string(),
                chunk_seq: chunk.seq as i32,
                chunk_count: batch.chunks.len() as i32,
                chunk_hash,
                chunk_text: chunk.text.clone(),
                parent_text: advanced_parent_text.to_string(),
                char_start: chunk.char_start as i64,
                char_end: chunk.char_end as i64,
                token_start: chunk.token_start as i64,
                token_end: chunk.token_start.saturating_add(chunk.token_count) as i64,
                model_id: batch.model.model_id.clone(),
                target_collection: batch.target_collection.to_string(),
                status: WORK_PENDING.to_string(),
                attempt_count: existing.as_ref().map_or(0, |value| value.attempt_count),
                max_attempts: existing
                    .as_ref()
                    .map_or(DEFAULT_WORK_MAX_ATTEMPTS, |value| value.max_attempts.max(1)),
                token_count: chunk.token_count as i64,
            };
            runtime
                .native_entity_write_for_service(
                    "embedding",
                    &context,
                    EMBEDDING_WORK_ITEM_MSG,
                    work_item_record(
                        &item.work_item_id,
                        &item.tenant_id,
                        &item.project_id,
                        &item.job_id,
                        &item.source_name,
                        &item.parent_pk,
                        &item.point_id,
                        &item.document_id,
                        &item.doc_version,
                        item.chunk_seq,
                        item.chunk_count,
                        &item.chunk_hash,
                        &item.chunk_text,
                        &item.model_id,
                        &item.target_collection,
                        WORK_PENDING,
                        item.attempt_count,
                        item.max_attempts,
                        "",
                        true,
                        item.token_count,
                        &item.parent_text,
                        item.char_start,
                        item.char_end,
                        item.token_start,
                        item.token_end,
                    ),
                    work_item_conflict(),
                )
                .await?;
            insert_work_outbox(
                pool,
                self.outbox_relation.as_deref(),
                &item,
                item.work_payload(batch.model),
            )
            .await?;
            mark_emitted(pool, &item.work_item_id, &item.tenant_id).await?;
            self.metrics.inc_embedding_work("emitted");
            result.emitted += 1;
        }

        if !current_work_ids.is_empty() {
            let rows = sqlx::query(
                "UPDATE udb_embedding.embedding_work_items SET status = 'SUPERSEDED', updated_at = CURRENT_TIMESTAMP \
                 WHERE tenant_id = $1 AND source_name = $2 AND parent_pk = $3 \
                   AND NOT (work_item_id = ANY($4)) AND status <> 'SUPERSEDED' RETURNING point_id",
            )
            .bind(batch.tenant_id).bind(batch.source_name).bind(batch.parent_pk)
            .bind(&current_work_ids).fetch_all(pool).await
            .map_err(|error| queue_status("embedding_work_supersede", error))?;
            let mut stale = BTreeSet::new();
            for row in rows {
                let point_id: String = row.try_get("point_id").unwrap_or_default();
                if !point_id.is_empty() && !current_point_ids.contains(&point_id) {
                    stale.insert(point_id);
                }
            }
            result.stale_point_ids = stale.into_iter().collect();
        }
        Ok(result)
    }
}

async fn mark_emitted(pool: &PgPool, work_item_id: &str, tenant_id: &str) -> Result<(), Status> {
    let delay = i64::try_from(embedding_work_visibility_timeout().as_secs()).unwrap_or(i64::MAX);
    sqlx::query(
        "UPDATE udb_embedding.embedding_work_items SET attempt_count = attempt_count + 1, \
         last_emitted_at = CURRENT_TIMESTAMP, next_attempt_at = CURRENT_TIMESTAMP + make_interval(secs => $3::double precision), \
         updated_at = CURRENT_TIMESTAMP WHERE work_item_id = $1 AND tenant_id = $2",
    )
    .bind(work_item_id).bind(tenant_id).bind(delay).execute(pool).await
    .map_err(|error| queue_status("embedding_work_mark_emitted", error))?;
    Ok(())
}

pub(crate) async fn load_retryable_work(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<DurableWorkItem>, Status> {
    let rows = sqlx::query(
        "SELECT w.work_item_id, w.tenant_id, w.project_id, w.job_id::text AS job_id, \
         w.source_name, w.parent_pk, w.point_id, COALESCE(w.document_id, '') AS document_id, \
         w.doc_version, w.chunk_seq, w.chunk_count, w.chunk_hash, w.chunk_text, \
         COALESCE(w.parent_text, '') AS parent_text, w.char_start, w.char_end, w.token_start, w.token_end, w.model_id, \
         w.target_collection, w.status, w.attempt_count, w.max_attempts, w.token_count \
         FROM udb_embedding.embedding_work_items w \
         WHERE w.status = 'PENDING' AND w.retryable = TRUE AND w.next_attempt_at <= CURRENT_TIMESTAMP \
           AND w.attempt_count < w.max_attempts ORDER BY w.next_attempt_at ASC LIMIT $1",
    )
    .bind(limit).fetch_all(pool).await
    .map_err(|error| queue_status("embedding_retry_load", error))?;
    Ok(rows
        .into_iter()
        .map(|row| DurableWorkItem {
            work_item_id: row.try_get("work_item_id").unwrap_or_default(),
            tenant_id: row.try_get("tenant_id").unwrap_or_default(),
            project_id: row.try_get("project_id").unwrap_or_default(),
            job_id: row.try_get("job_id").unwrap_or_default(),
            source_name: row.try_get("source_name").unwrap_or_default(),
            parent_pk: row.try_get("parent_pk").unwrap_or_default(),
            point_id: row.try_get("point_id").unwrap_or_default(),
            document_id: row.try_get("document_id").unwrap_or_default(),
            doc_version: row.try_get("doc_version").unwrap_or_default(),
            chunk_seq: row.try_get("chunk_seq").unwrap_or_default(),
            chunk_count: row.try_get("chunk_count").unwrap_or_default(),
            chunk_hash: row.try_get("chunk_hash").unwrap_or_default(),
            chunk_text: row.try_get("chunk_text").unwrap_or_default(),
            parent_text: row.try_get("parent_text").unwrap_or_default(),
            char_start: row.try_get("char_start").unwrap_or_default(),
            char_end: row.try_get("char_end").unwrap_or_default(),
            token_start: row.try_get("token_start").unwrap_or_default(),
            token_end: row.try_get("token_end").unwrap_or_default(),
            model_id: row.try_get("model_id").unwrap_or_default(),
            target_collection: row.try_get("target_collection").unwrap_or_default(),
            status: row.try_get("status").unwrap_or_default(),
            attempt_count: row.try_get("attempt_count").unwrap_or_default(),
            max_attempts: row
                .try_get("max_attempts")
                .unwrap_or(DEFAULT_WORK_MAX_ATTEMPTS),
            token_count: row.try_get("token_count").unwrap_or_default(),
        })
        .collect())
}

pub(crate) async fn reemit_work_item(
    svc: &EmbeddingServiceImpl,
    item: &DurableWorkItem,
    model: &StoredModel,
) -> Result<(), Status> {
    let pool = svc
        .pg_pool
        .as_ref()
        .ok_or_else(|| queue_status("embedding_retry_emit", "Postgres unavailable"))?;
    insert_work_outbox(
        pool,
        svc.outbox_relation.as_deref(),
        item,
        item.work_payload(model),
    )
    .await?;
    mark_emitted(pool, &item.work_item_id, &item.tenant_id).await
}

/// An accounting event to enqueue ATOMICALLY with a fresh work-item ack.
/// Exactly-once by construction: the metering outbox row commits in the same
/// transaction that flips the item to ACKED, and a replayed report (item
/// already ACKED) skips both the flip and the event — so billing can neither
/// silently drop (insert failure rolls the ack back for a clean retry) nor
/// double-count.
pub(crate) struct AckMeteringEvent {
    pub(crate) outbox_relation: Option<String>,
    pub(crate) topic: &'static str,
    pub(crate) project_id: String,
    pub(crate) source_name: String,
    pub(crate) payload: serde_json::Value,
}

pub(crate) async fn ack_work_item(
    pool: &PgPool,
    tenant_id: &str,
    work_item_id: &str,
    token_count: i64,
    metering: Option<AckMeteringEvent>,
) -> Result<bool, Status> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| queue_status("embedding_ack", error))?;
    let row = sqlx::query(
        "UPDATE udb_embedding.embedding_work_items SET status = 'ACKED', acked_at = CURRENT_TIMESTAMP, \
         token_count = GREATEST(token_count, $3), updated_at = CURRENT_TIMESTAMP \
         WHERE work_item_id = $1 AND tenant_id = $2 AND status <> 'ACKED' RETURNING job_id",
    ).bind(work_item_id).bind(tenant_id).bind(token_count).fetch_optional(&mut *tx).await
    .map_err(|error| queue_status("embedding_ack", error))?;
    if let Some(row) = row {
        let job_id: Option<Uuid> = row.try_get("job_id").ok();
        if let Some(job_id) = job_id {
            sqlx::query(
                "UPDATE udb_embedding.embedding_jobs SET vectors_stored = vectors_stored + 1, \
                 status = CASE WHEN chunks_emitted <= vectors_stored + failed + 1 THEN 'COMPLETED' ELSE status END, \
                 finished_at = CASE WHEN chunks_emitted <= vectors_stored + failed + 1 THEN CURRENT_TIMESTAMP ELSE finished_at END, \
                 updated_at = CURRENT_TIMESTAMP WHERE job_id = $1 AND tenant_id = $2",
            ).bind(job_id).bind(tenant_id).execute(&mut *tx).await
            .map_err(|error| queue_status("embedding_ack_job", error))?;
        }
        if let Some(metering) = metering {
            crate::runtime::service::native_helpers::enqueue_outbox_event_in_tx(
                &mut *tx,
                metering.outbox_relation.as_deref(),
                metering.topic,
                &metering.source_name,
                tenant_id,
                &metering.project_id,
                metering.payload,
                crate::runtime::service::native_helpers::NativeEventContext {
                    target_resource: metering.source_name.clone(),
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| queue_status("embedding_ack_metering", error))?;
        }
        tx.commit()
            .await
            .map_err(|error| queue_status("embedding_ack", error))?;
        return Ok(true);
    }
    tx.commit()
        .await
        .map_err(|error| queue_status("embedding_ack", error))?;
    Ok(false)
}

pub(crate) struct FailureResult {
    pub(crate) dead: bool,
    pub(crate) attempt_count: i32,
}

pub(crate) struct DeadWorkItem {
    pub(crate) work_item_id: String,
    pub(crate) tenant_id: String,
    pub(crate) project_id: String,
    pub(crate) source_name: String,
    pub(crate) attempt_count: i32,
}

pub(crate) async fn dead_letter_exhausted_work(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<DeadWorkItem>, Status> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| queue_status("embedding_dead_sweep", error))?;
    let rows = sqlx::query(
        "WITH exhausted AS (SELECT work_item_id FROM udb_embedding.embedding_work_items \
         WHERE status = 'PENDING' AND attempt_count >= max_attempts ORDER BY next_attempt_at ASC \
         LIMIT $1 FOR UPDATE SKIP LOCKED) \
         UPDATE udb_embedding.embedding_work_items w SET status = 'DEAD', retryable = FALSE, \
         last_error = CASE WHEN last_error = '' THEN 'visibility timeout exhausted' ELSE last_error END, \
         updated_at = CURRENT_TIMESTAMP FROM exhausted e WHERE w.work_item_id = e.work_item_id \
         RETURNING w.work_item_id, w.tenant_id, w.project_id, w.source_name, w.attempt_count, w.job_id",
    ).bind(limit).fetch_all(&mut *tx).await
    .map_err(|error| queue_status("embedding_dead_sweep", error))?;
    let mut dead = Vec::with_capacity(rows.len());
    for row in rows {
        let tenant_id: String = row.try_get("tenant_id").unwrap_or_default();
        let job_id: Option<Uuid> = row.try_get("job_id").ok();
        if let Some(job_id) = job_id {
            sqlx::query(
                "UPDATE udb_embedding.embedding_jobs SET failed = failed + 1, status = 'FAILED', \
                 error = 'embedding work visibility timeout exhausted', finished_at = CURRENT_TIMESTAMP, \
                 updated_at = CURRENT_TIMESTAMP WHERE job_id = $1 AND tenant_id = $2",
            ).bind(job_id).bind(&tenant_id).execute(&mut *tx).await
            .map_err(|error| queue_status("embedding_dead_sweep_job", error))?;
        }
        dead.push(DeadWorkItem {
            work_item_id: row.try_get("work_item_id").unwrap_or_default(),
            tenant_id,
            project_id: row.try_get("project_id").unwrap_or_default(),
            source_name: row.try_get("source_name").unwrap_or_default(),
            attempt_count: row.try_get("attempt_count").unwrap_or_default(),
        });
    }
    tx.commit()
        .await
        .map_err(|error| queue_status("embedding_dead_sweep", error))?;
    Ok(dead)
}

pub(crate) async fn fail_work_item(
    pool: &PgPool,
    tenant_id: &str,
    work_item_id: &str,
    error_message: &str,
    retryable: bool,
) -> Result<FailureResult, Status> {
    if error_message.trim().is_empty() {
        return Err(embedding_field_violation(
            "error",
            "must describe the sidecar failure",
            "error is required",
        ));
    }
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| queue_status("embedding_nack", error))?;
    let row = sqlx::query(
        "UPDATE udb_embedding.embedding_work_items SET last_error = $3, retryable = $4, \
         status = CASE WHEN NOT $4 OR attempt_count >= max_attempts THEN 'DEAD' ELSE 'PENDING' END, \
         next_attempt_at = CURRENT_TIMESTAMP + make_interval(secs => LEAST(3600, CAST(power(2, LEAST(attempt_count, 10)) AS INTEGER))), \
         updated_at = CURRENT_TIMESTAMP WHERE work_item_id = $1 AND tenant_id = $2 \
         RETURNING status, attempt_count, job_id",
    ).bind(work_item_id).bind(tenant_id).bind(error_message).bind(retryable)
    .fetch_optional(&mut *tx).await.map_err(|error| queue_status("embedding_nack", error))?
    .ok_or_else(|| embedding_policy_status_with_code(tonic::Code::NotFound, "embedding_nack", "embedding_work_item_not_found", "embedding work item not found"))?;
    let status: String = row.try_get("status").unwrap_or_default();
    let attempt_count: i32 = row.try_get("attempt_count").unwrap_or_default();
    let dead = status == WORK_DEAD;
    if dead {
        let job_id: Option<Uuid> = row.try_get("job_id").ok();
        if let Some(job_id) = job_id {
            sqlx::query(
                "UPDATE udb_embedding.embedding_jobs SET failed = failed + 1, status = 'FAILED', \
                 error = $3, finished_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP \
                 WHERE job_id = $1 AND tenant_id = $2",
            )
            .bind(job_id)
            .bind(tenant_id)
            .bind(error_message)
            .execute(&mut *tx)
            .await
            .map_err(|error| queue_status("embedding_nack_job", error))?;
        }
    }
    tx.commit()
        .await
        .map_err(|error| queue_status("embedding_nack", error))?;
    Ok(FailureResult {
        dead,
        attempt_count,
    })
}

pub(crate) async fn update_job_emission(
    pool: &PgPool,
    tenant_id: &str,
    job_id: &str,
    rows: i64,
    chunks: i64,
) -> Result<(), Status> {
    if job_id.trim().is_empty() {
        return Ok(());
    }
    sqlx::query(
        "UPDATE udb_embedding.embedding_jobs SET status = 'RUNNING', started_at = COALESCE(started_at, CURRENT_TIMESTAMP), \
         rows_enumerated = rows_enumerated + $3, chunks_emitted = chunks_emitted + $4, \
         updated_at = CURRENT_TIMESTAMP WHERE job_id = $1::uuid AND tenant_id = $2",
    ).bind(job_id).bind(tenant_id).bind(rows).bind(chunks).execute(pool).await
    .map_err(|error| queue_status("embedding_job_progress", error))?;
    Ok(())
}

pub(crate) async fn complete_job_enumeration(
    pool: &PgPool,
    tenant_id: &str,
    job_id: &str,
) -> Result<(), Status> {
    if job_id.trim().is_empty() {
        return Ok(());
    }
    sqlx::query(
        "UPDATE udb_embedding.embedding_jobs SET \
         status = CASE WHEN chunks_emitted <= vectors_stored + failed THEN 'COMPLETED' ELSE 'RUNNING' END, \
         finished_at = CASE WHEN chunks_emitted <= vectors_stored + failed THEN CURRENT_TIMESTAMP ELSE finished_at END, \
         updated_at = CURRENT_TIMESTAMP WHERE job_id = $1::uuid AND tenant_id = $2",
    ).bind(job_id).bind(tenant_id).execute(pool).await
    .map_err(|error| queue_status("embedding_job_complete_enumeration", error))?;
    Ok(())
}
