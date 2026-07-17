//! Leader-owned background passes (freshness / reindex / teardown) for the
//! native `SearchService`, extracted verbatim from the former god file.
//!
//! All three passes follow the house journal-consumer pattern (cache
//! invalidation, embedding work emitter): read published events from the durable
//! CDC journal, join them to this service's own registrations, dedup by an
//! output event carrying the source journal event id, and act ONLY through the
//! mediated runtime seams. They are spawned by the leader via
//! `NativeWorkerHost::spawn_while_leader` (never a bare per-replica loop) — the
//! `run_index_freshness_consumer` / `run_search_reindex_once` entry points are
//! re-exported from `mod.rs` for `serve()`.

use std::sync::Arc;

use sqlx::{PgPool, Row};

use crate::proto::{SelectRequest, Sort, VectorPointMutation, VectorUpsertRequest};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::native_catalog::NativeModel;

use super::SearchServiceImpl;
use super::config::{
    SEARCH_INDEX_MSG, SEARCH_REINDEX_PAGE_LIMIT, STATUS_ACTIVE, STATUS_DELETED, TOPIC_DELETED,
    TOPIC_FRESHNESS_APPLIED, TOPIC_REINDEX, TOPIC_REINDEX_COMPLETED, TOPIC_TEARDOWN_COMPLETED,
};
use super::model::{StoredIndex, json_str, stored_index_from_pg_row};
use super::store::{index_conflict, index_record, search_index_model};

/// Fail-closed tenant scope check for a CDC freshness event, mirroring the
/// tenant-scoped branch of `cdc::engine_tail::payload_value_matches_stream_scope`
/// (#8): a tenant-less or foreign-`tenant_id` payload is dropped, never indexed.
/// Reuses the public [`crate::runtime::cdc::tenant_scoped_topic`] seam to assert
/// the source stream is a tenant topic. Pure — unit-tested.
pub(crate) fn event_in_index_scope(
    topic: &str,
    payload: &serde_json::Value,
    tenant_scope: &str,
) -> bool {
    let tenant_scope = tenant_scope.trim();
    // An index is always tenant-bound; a missing scope can match nothing.
    if tenant_scope.is_empty() {
        return false;
    }
    let event_tenant = payload
        .get("tenant_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim();
    // Fail closed: a tenant-less payload is never indexed into a tenant index,
    // even on a topic that is not itself tenant-scoped by prefix.
    if event_tenant.is_empty() {
        let _ = crate::runtime::cdc::tenant_scoped_topic(topic);
        return false;
    }
    event_tenant == tenant_scope
}

/// The `s.<col> AS <name>` select list shared by all three job loaders.
fn stored_index_columns_sql(model: &NativeModel) -> String {
    format!(
        "s.{index_id}::TEXT AS index_id, \
         s.{index_name}::TEXT AS index_name, \
         s.{source_message_type}::TEXT AS source_message_type, \
         s.{backend}::TEXT AS backend, \
         s.{resource_name}::TEXT AS resource_name, \
         s.{vector_dims} AS vector_dims, \
         s.{tenant_column}::TEXT AS tenant_column, \
         s.{source_cdc_topic}::TEXT AS source_cdc_topic, \
         s.{status}::TEXT AS status",
        index_id = model.q("index_id"),
        index_name = model.q("index_name"),
        source_message_type = model.q("source_message_type"),
        backend = model.q("backend"),
        resource_name = model.q("resource_name"),
        vector_dims = model.q("vector_dims"),
        tenant_column = model.q("tenant_column"),
        source_cdc_topic = model.q("source_cdc_topic"),
        status = model.q("status"),
    )
}

/// A journal CDC event joined to the ACTIVE index registered over its topic.
struct SearchFreshnessJob {
    index: StoredIndex,
    event_id: String,
    tenant_id: String,
    project_id: String,
    payload: serde_json::Value,
}

/// A `reindex.requested` journal event joined to its (non-deleted) index row.
struct SearchReindexJob {
    index: StoredIndex,
    event_id: String,
    tenant_id: String,
    project_id: String,
    reindex_id: String,
}

/// An `index.deleted` journal event joined to its DELETED index row.
struct SearchTeardownJob {
    index: StoredIndex,
    event_id: String,
    tenant_id: String,
    project_id: String,
}

/// Freshness candidates: published source-topic CDC events for ACTIVE indexes
/// that carry a dense embedding, minus events already marked applied. The
/// journal may ENVELOPE-NEST the emitted payload under `payload.payload`, so
/// every payload key is read at BOTH levels (`COALESCE(top, nested)`).
/// Binds: `$1` = active status, `$2` = applied-marker topic, `$3` = limit.
pub(crate) fn freshness_jobs_sql(
    model: &NativeModel,
    journal_relation: &str,
    outbox_relation: &str,
) -> String {
    format!(
        "SELECT {cols}, \
            j.event_id::TEXT AS event_id, \
            COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') AS event_tenant_id, \
            COALESCE(j.payload->>'project_id', j.payload->'payload'->>'project_id', '') AS project_id, \
            j.payload::TEXT AS payload_json \
         FROM {journal_relation} j \
         JOIN {relation} s \
           ON s.{tenant_id}::TEXT = COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') \
          AND s.{status} = $1 \
          AND COALESCE(s.{source_cdc_topic}::TEXT, '') <> '' \
          AND s.{source_cdc_topic} = j.topic \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') <> '' \
           AND (jsonb_typeof(j.payload->'vector') = 'array' \
                OR jsonb_typeof(j.payload->'payload'->'vector') = 'array') \
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
        cols = stored_index_columns_sql(model),
        relation = model.relation,
        tenant_id = model.q("tenant_id"),
        status = model.q("status"),
        source_cdc_topic = model.q("source_cdc_topic"),
    )
}

/// Reindex jobs: `reindex.requested` events joined to their non-deleted index
/// row, minus requests that already produced a `reindex.completed` marker
/// (keyed `reindex_event_id`). Binds: `$1` = deleted status (excluded),
/// `$2` = requested topic, `$3` = completed topic, `$4` = limit.
pub(crate) fn reindex_jobs_sql(
    model: &NativeModel,
    journal_relation: &str,
    outbox_relation: &str,
) -> String {
    format!(
        "SELECT {cols}, \
            j.event_id::TEXT AS event_id, \
            COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') AS event_tenant_id, \
            COALESCE(j.payload->>'project_id', j.payload->'payload'->>'project_id', '') AS project_id, \
            COALESCE(j.payload->>'reindex_id', j.payload->'payload'->>'reindex_id', '') AS reindex_id \
         FROM {journal_relation} j \
         JOIN {relation} s \
           ON s.{tenant_id}::TEXT = COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') \
          AND s.{index_name}::TEXT = COALESCE(j.payload->>'index_name', j.payload->'payload'->>'index_name', '') \
          AND s.{status} <> $1 \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND j.topic = $2 \
           AND COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') <> '' \
           AND COALESCE(j.payload->>'reindex_id', j.payload->'payload'->>'reindex_id', '') <> '' \
           AND NOT EXISTS ( \
               SELECT 1 FROM {outbox_relation} o \
               WHERE o.topic = $3 \
                 AND COALESCE(o.payload->>'reindex_event_id', o.payload->'payload'->>'reindex_event_id') = j.event_id::TEXT \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM {journal_relation} done \
               WHERE done.topic = $3 \
                 AND COALESCE(done.payload->>'reindex_event_id', done.payload->'payload'->>'reindex_event_id') = j.event_id::TEXT \
           ) \
         ORDER BY j.published_at ASC, j.event_id ASC \
         LIMIT $4",
        cols = stored_index_columns_sql(model),
        relation = model.relation,
        tenant_id = model.q("tenant_id"),
        index_name = model.q("index_name"),
        status = model.q("status"),
    )
}

/// Teardown jobs: `index.deleted` events joined to their DELETED index row,
/// minus deletions that already produced a `teardown.completed` marker (keyed
/// `teardown_event_id`). Binds: `$1` = deleted status, `$2` = deleted topic,
/// `$3` = completed topic, `$4` = limit.
pub(crate) fn teardown_jobs_sql(
    model: &NativeModel,
    journal_relation: &str,
    outbox_relation: &str,
) -> String {
    format!(
        "SELECT {cols}, \
            j.event_id::TEXT AS event_id, \
            COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') AS event_tenant_id, \
            COALESCE(j.payload->>'project_id', j.payload->'payload'->>'project_id', '') AS project_id \
         FROM {journal_relation} j \
         JOIN {relation} s \
           ON s.{tenant_id}::TEXT = COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') \
          AND s.{index_name}::TEXT = COALESCE(j.payload->>'index_name', j.payload->'payload'->>'index_name', '') \
          AND s.{status} = $1 \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND j.topic = $2 \
           AND COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') <> '' \
           AND NOT EXISTS ( \
               SELECT 1 FROM {outbox_relation} o \
               WHERE o.topic = $3 \
                 AND COALESCE(o.payload->>'teardown_event_id', o.payload->'payload'->>'teardown_event_id') = j.event_id::TEXT \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM {journal_relation} done \
               WHERE done.topic = $3 \
                 AND COALESCE(done.payload->>'teardown_event_id', done.payload->'payload'->>'teardown_event_id') = j.event_id::TEXT \
           ) \
         ORDER BY j.published_at ASC, j.event_id ASC \
         LIMIT $4",
        cols = stored_index_columns_sql(model),
        relation = model.relation,
        tenant_id = model.q("tenant_id"),
        index_name = model.q("index_name"),
        status = model.q("status"),
    )
}

async fn load_search_freshness_jobs(
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    batch: i64,
) -> Result<Vec<SearchFreshnessJob>, String> {
    let model = search_index_model();
    let rows = sqlx::query(&freshness_jobs_sql(
        &model,
        journal_relation,
        outbox_relation,
    ))
    .bind(STATUS_ACTIVE)
    .bind(TOPIC_FRESHNESS_APPLIED)
    .bind(batch.max(1))
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load search freshness jobs failed: {err}"))?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        let payload_json: String = row
            .try_get("payload_json")
            .map_err(|err| format!("decode search freshness payload failed: {err}"))?;
        let payload: serde_json::Value = serde_json::from_str(&payload_json)
            .map_err(|err| format!("decode search freshness payload JSON failed: {err}"))?;
        jobs.push(SearchFreshnessJob {
            index: stored_index_from_pg_row(&row)?,
            event_id: row
                .try_get("event_id")
                .map_err(|err| format!("decode search freshness event id failed: {err}"))?,
            tenant_id: row
                .try_get("event_tenant_id")
                .map_err(|err| format!("decode search freshness tenant failed: {err}"))?,
            project_id: row
                .try_get("project_id")
                .map_err(|err| format!("decode search freshness project failed: {err}"))?,
            payload,
        });
    }
    Ok(jobs)
}

async fn load_search_reindex_jobs(
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    batch: i64,
) -> Result<Vec<SearchReindexJob>, String> {
    let model = search_index_model();
    let rows = sqlx::query(&reindex_jobs_sql(&model, journal_relation, outbox_relation))
        .bind(STATUS_DELETED)
        .bind(TOPIC_REINDEX)
        .bind(TOPIC_REINDEX_COMPLETED)
        .bind(batch.max(1))
        .fetch_all(pool)
        .await
        .map_err(|err| format!("load search reindex jobs failed: {err}"))?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        jobs.push(SearchReindexJob {
            index: stored_index_from_pg_row(&row)?,
            event_id: row
                .try_get("event_id")
                .map_err(|err| format!("decode search reindex event id failed: {err}"))?,
            tenant_id: row
                .try_get("event_tenant_id")
                .map_err(|err| format!("decode search reindex tenant failed: {err}"))?,
            project_id: row
                .try_get("project_id")
                .map_err(|err| format!("decode search reindex project failed: {err}"))?,
            reindex_id: row
                .try_get("reindex_id")
                .map_err(|err| format!("decode search reindex id failed: {err}"))?,
        });
    }
    Ok(jobs)
}

async fn load_search_teardown_jobs(
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    batch: i64,
) -> Result<Vec<SearchTeardownJob>, String> {
    let model = search_index_model();
    let rows = sqlx::query(&teardown_jobs_sql(
        &model,
        journal_relation,
        outbox_relation,
    ))
    .bind(STATUS_DELETED)
    .bind(TOPIC_DELETED)
    .bind(TOPIC_TEARDOWN_COMPLETED)
    .bind(batch.max(1))
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load search teardown jobs failed: {err}"))?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        jobs.push(SearchTeardownJob {
            index: stored_index_from_pg_row(&row)?,
            event_id: row
                .try_get("event_id")
                .map_err(|err| format!("decode search teardown event id failed: {err}"))?,
            tenant_id: row
                .try_get("event_tenant_id")
                .map_err(|err| format!("decode search teardown tenant failed: {err}"))?,
            project_id: row
                .try_get("project_id")
                .map_err(|err| format!("decode search teardown project failed: {err}"))?,
        });
    }
    Ok(jobs)
}

/// Worker-side request context for mediated reads/writes performed by the
/// leader passes (mirrors the embedding backfill's `backfill_read_context`).
fn search_worker_context(
    tenant_id: &str,
    project_id: &str,
    purpose: &str,
) -> crate::RequestContext {
    crate::RequestContext {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        purpose: purpose.to_string(),
        scopes: vec!["udb:read".to_string()],
        service_identity: "udb.search.worker".to_string(),
        ..crate::RequestContext::default()
    }
}

/// Journal rows may ENVELOPE-NEST the emitted payload under `payload.payload`
/// (the CDC journal envelope); read the nested level when the top level lacks
/// the freshness keys. Pure — unit-tested.
pub(crate) fn freshness_event_body(payload: &serde_json::Value) -> &serde_json::Value {
    if payload.get("vector").is_some() || payload.get("id").is_some() {
        return payload;
    }
    payload.get("payload").unwrap_or(payload)
}

/// Extract a dense embedding from a freshness event body or a source row. Only
/// a change that already carries its embedding (`vector`, or an `embedding`
/// column on a source row — e.g. reported by an embedding sidecar, master-plan
/// 9.11) is upserted; pure-text freshness for an ES-backed index is
/// engine-side. Pure — unit-tested.
pub(crate) fn payload_vector(body: &serde_json::Value) -> Option<Vec<f32>> {
    ["vector", "embedding"].iter().find_map(|key| {
        body.get(*key)
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value| value.as_f64().map(|number| number as f32))
                    .collect::<Vec<f32>>()
            })
            .filter(|vector| !vector.is_empty())
    })
}

/// Pure status/metadata writeback for a finished reindex pass. BOTH outcomes
/// restore `ACTIVE` — an index is NEVER stranded in `REINDEXING` (the
/// active-only Search/List filter would make it vanish). The SearchIndex entity
/// has no dedicated error column, so a failure records `last_error` in the
/// mutable `metadata_json` column (which the Reindex RPC already resets).
pub(crate) fn reindex_writeback(error: Option<&str>) -> (&'static str, String) {
    match error {
        None => (STATUS_ACTIVE, "{}".to_string()),
        Some(error) => (
            STATUS_ACTIVE,
            serde_json::json!({ "last_error": error }).to_string(),
        ),
    }
}

/// The engine point payload for a re-upserted source row: the full row minus
/// its dense-vector column(s). The mediated `vector_upsert` stamps
/// [`super::config::TENANT_SCOPE_PAYLOAD_KEY`] itself at write time.
fn reindex_point_payload(row: &serde_json::Value) -> Option<prost_types::Struct> {
    let mut object = row.as_object().cloned().unwrap_or_default();
    for key in ["vector", "embedding"] {
        object.remove(key);
    }
    crate::runtime::executor_utils::json_to_struct(&serde_json::Value::Object(object))
}

/// Mediated tenant-scoped SELECT over an index's source entity, cursor-paged on
/// the primary key (mirrors the embedding backfill's `backfill_select_request`,
/// including the project-isolation predicate the served planner requires).
pub(crate) fn source_rows_select_request(
    index: &StoredIndex,
    tenant_id: &str,
    primary_key: &str,
    fields: Vec<String>,
    after_pk: Option<&str>,
    project_column: Option<&str>,
    project_id: &str,
) -> SelectRequest {
    let mut tenant_filter = serde_json::Map::new();
    tenant_filter.insert(
        index.tenant_column.clone(),
        serde_json::Value::String(tenant_id.to_string()),
    );
    let mut filters = vec![serde_json::Value::Object(tenant_filter)];
    // Project isolation: the data-plane planner rejects a select over a
    // project-scoped source table unless the filter carries the project column.
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
        message_type: index.source_message_type.clone(),
        filter: crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
            "$and": filters,
        })),
        fields,
        limit: SEARCH_REINDEX_PAGE_LIMIT,
        sort: vec![Sort {
            field: primary_key.to_string(),
            descending: false,
        }],
        ..SelectRequest::default()
    }
}

/// Resolve the source entity's manifest table + primary key for a worker job.
fn source_table_and_pk<'a>(
    manifest: &'a crate::generation::CatalogManifest,
    source_message_type: &str,
) -> Result<(&'a crate::generation::ManifestTable, String), String> {
    let table =
        crate::broker::table_for_message(manifest, source_message_type).ok_or_else(|| {
            format!("search source entity '{source_message_type}' is not present in active catalog")
        })?;
    let primary_key = table.primary_key.first().cloned().ok_or_else(|| {
        format!("search source entity '{source_message_type}' has no primary key")
    })?;
    Ok((table, primary_key))
}

/// Apply one in-scope CDC freshness event through the runtime's MEDIATED
/// `vector_upsert` — the same mediated vector dispatch family `Search` queries
/// — never a raw engine write. Returns `Some(true)` when a point was upserted,
/// `Some(false)` for a terminal skip (out-of-scope / no id / no vector —
/// retrying cannot change it, so the applied marker is still written), and
/// `None` for a transient failure (no marker: the next leader pass retries).
async fn apply_search_freshness_job(
    service: &SearchServiceImpl,
    job: &SearchFreshnessJob,
) -> Option<bool> {
    let (Some(runtime), Some(catalog)) = (service.runtime.as_deref(), service.catalog.as_deref())
    else {
        // Capability missing: leave the event unmarked so a later, fully wired
        // pass picks it up (fail closed, never silently drop).
        return None;
    };
    let body = freshness_event_body(&job.payload);
    // Fail closed: a missing/foreign tenant_id payload is dropped, not indexed.
    if !event_in_index_scope(&job.index.source_cdc_topic, body, &job.tenant_id) {
        return Some(false);
    }
    let Some(vector) = payload_vector(body) else {
        return Some(false);
    };
    let id = body
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    if id.is_empty() {
        return Some(false);
    }
    let state = catalog.active_for(&job.project_id);
    let context = search_worker_context(&job.tenant_id, &job.project_id, "search_freshness");
    let request = VectorUpsertRequest {
        context: None,
        collection: job.index.collection(),
        points: vec![VectorPointMutation {
            id,
            vector,
            payload: body
                .get("payload")
                .and_then(|value| crate::runtime::executor_utils::json_to_struct(value)),
        }],
        idempotency_key: String::new(),
    };
    if let Err(err) = runtime
        .vector_upsert(&state.manifest, request, context)
        .await
    {
        tracing::warn!(
            index = %job.index.index_name,
            error = %err,
            "search index freshness upsert failed; will retry on next pass"
        );
        return None;
    }
    Some(true)
}

/// Per-index CDC freshness consumer (master-plan 9.5) — ONE leader-owned pass
/// over the durable CDC journal. Published events on an ACTIVE index's source
/// topic are joined tenant-scoped, dropped when outside the index's tenant
/// scope ([`event_in_index_scope`], fail closed), and an in-scope
/// embedding-carrying change is upserted through the runtime's MEDIATED vector
/// dispatch. Each consumed event is marked with a
/// [`TOPIC_FRESHNESS_APPLIED`] outbox event keyed by `source_event_id`, so a
/// pass never re-consumes the journal. The leader spawns this via
/// `NativeWorkerHost::spawn_while_leader` (the `run_cache_invalidation_worker_once`
/// shape). Returns the number of points upserted.
pub(crate) async fn run_index_freshness_consumer(
    service: Arc<SearchServiceImpl>,
    journal_relation: &str,
    batch: i64,
) -> Result<i64, String> {
    let pool = service
        .pg_pool
        .as_ref()
        .ok_or_else(|| "search freshness consumer requires native Postgres store".to_string())?;
    let outbox_relation = service
        .outbox_relation
        .as_deref()
        .ok_or_else(|| "search freshness consumer requires transactional outbox".to_string())?;
    let jobs = load_search_freshness_jobs(pool, journal_relation, outbox_relation, batch).await?;
    let mut applied = 0i64;
    for job in &jobs {
        let Some(was_applied) = apply_search_freshness_job(&service, job).await else {
            // Transient failure: no applied marker, retried on the next pass.
            continue;
        };
        if was_applied {
            applied = applied.saturating_add(1);
        }
        service
            .emit_index_event(
                TOPIC_FRESHNESS_APPLIED,
                &job.tenant_id,
                &job.project_id,
                &job.index.index_name,
                serde_json::json!({
                    "source_event_id": job.event_id,
                    "applied": was_applied,
                }),
            )
            .await;
    }
    Ok(applied)
}

/// Page every tenant-scoped source row through the served mediated SELECT and
/// re-upsert each dense-vector row into the engine through the SAME mediated
/// `vector_upsert` dispatch family `Search` queries — never a raw engine write.
/// Rows without a `vector`/`embedding` column are skipped (a full-text-only
/// index has no dense vector to rebuild here; its content is engine-side).
async fn reindex_source_rows(
    runtime: &DataBrokerRuntime,
    manifest: &crate::generation::CatalogManifest,
    job: &SearchReindexJob,
) -> Result<u64, String> {
    let (table, primary_key) = source_table_and_pk(manifest, &job.index.source_message_type)?;
    let project_column = crate::generation::sql::resolve_project_column(table);
    let context = search_worker_context(&job.tenant_id, &job.project_id, "search_reindex");
    let collection = job.index.collection();
    let mut upserted = 0u64;
    let mut after_pk: Option<String> = None;
    loop {
        let request = source_rows_select_request(
            &job.index,
            &job.tenant_id,
            &primary_key,
            // All columns: the row's dense vector AND its payload fields.
            Vec::new(),
            after_pk.as_deref(),
            project_column,
            &job.project_id,
        );
        let (record_set, _) = runtime
            .select(manifest, request, context.clone())
            .await
            .map_err(|err| format!("search reindex source select failed: {err}"))?;
        if record_set.records_json.is_empty() {
            break;
        }
        let fetched = record_set.records_json.len();
        let mut last_pk = String::new();
        let mut points = Vec::new();
        for raw in &record_set.records_json {
            let row: serde_json::Value = serde_json::from_slice(raw)
                .map_err(|err| format!("decode search reindex source row failed: {err}"))?;
            let row_pk = row
                .as_object()
                .map(|map| json_str(map, &primary_key))
                .unwrap_or_default();
            if row_pk.trim().is_empty() {
                continue;
            }
            last_pk = row_pk.clone();
            let Some(vector) = payload_vector(&row) else {
                continue;
            };
            points.push(VectorPointMutation {
                id: row_pk,
                vector,
                payload: reindex_point_payload(&row),
            });
        }
        let page_points = points.len() as u64;
        if !points.is_empty() {
            runtime
                .vector_upsert(
                    manifest,
                    VectorUpsertRequest {
                        context: None,
                        collection: collection.clone(),
                        points,
                        idempotency_key: String::new(),
                    },
                    context.clone(),
                )
                .await
                .map_err(|err| format!("search reindex vector upsert failed: {err}"))?;
            upserted = upserted.saturating_add(page_points);
        }
        if fetched < SEARCH_REINDEX_PAGE_LIMIT as usize || last_pk.is_empty() {
            break;
        }
        after_pk = Some(last_pk);
    }
    Ok(upserted)
}

/// Execute one reindex job end-to-end: mediated source backfill, then the
/// `REINDEXING → ACTIVE` writeback and the completion event. On a work failure
/// the index is restored to `ACTIVE` with `last_error` recorded — NEVER
/// stranded in `REINDEXING` — and the completion event carries
/// `success: false`. Only a failed writeback bubbles (no completion marker is
/// written, so the next leader pass retries the still-REINDEXING job).
async fn process_search_reindex_job(
    service: &SearchServiceImpl,
    job: &SearchReindexJob,
) -> Result<i64, String> {
    let runtime = service
        .runtime
        .as_deref()
        .ok_or_else(|| "search reindex requires runtime dispatch".to_string())?;
    let catalog = service
        .catalog
        .as_deref()
        .ok_or_else(|| "search reindex requires active catalog".to_string())?;
    if job.tenant_id.trim().is_empty() {
        return Ok(0);
    }
    let state = catalog.active_for(&job.project_id);
    let outcome = reindex_source_rows(runtime, &state.manifest, job).await;
    let (upserted, error) = match &outcome {
        Ok(count) => (*count, None),
        Err(err) => {
            tracing::warn!(
                index = %job.index.index_name,
                error = %err,
                "search reindex failed; restoring index to ACTIVE with last_error"
            );
            (0, Some(err.as_str()))
        }
    };
    let (status, metadata_json) = reindex_writeback(error);
    let context = search_worker_context(&job.tenant_id, &job.project_id, "search_reindex");
    runtime
        .native_entity_write_for_service(
            "search",
            &context,
            SEARCH_INDEX_MSG,
            index_record(
                &job.index.index_id,
                &job.tenant_id,
                &job.index.index_name,
                &job.index.source_message_type,
                &job.index.backend,
                &job.index.resource_name,
                job.index.vector_dims,
                &job.index.tenant_column,
                &job.index.source_cdc_topic,
                status,
                &metadata_json,
            ),
            index_conflict(),
        )
        .await
        .map_err(|err| format!("search reindex status writeback failed: {err}"))?;
    service
        .emit_index_event(
            TOPIC_REINDEX_COMPLETED,
            &job.tenant_id,
            &job.project_id,
            &job.index.index_name,
            serde_json::json!({
                "reindex_event_id": job.event_id,
                "reindex_id": job.reindex_id,
                "upserted": upserted,
                "success": error.is_none(),
                "error": error.unwrap_or_default(),
            }),
        )
        .await;
    Ok(i64::try_from(upserted).unwrap_or(i64::MAX))
}

/// Purge a deleted index's engine points for its tenant through the EXISTING
/// per-point delete seam (`vector_delete_backend_target` — the same shared path
/// the embedding/asset services delete through; never a parallel engine
/// client). Point ids are enumerated from the source entity via the served
/// mediated SELECT (tenant-scoped): the runtime exposes no engine-scroll seam,
/// and dropping the whole collection would destroy other tenants' points in a
/// shared collection. HONESTY: points whose source rows were deleted after
/// indexing are not enumerable here and may remain as orphans until an operator
/// drops the collection.
async fn teardown_index_points(
    runtime: &DataBrokerRuntime,
    manifest: &crate::generation::CatalogManifest,
    job: &SearchTeardownJob,
) -> Result<u64, String> {
    let (table, primary_key) = source_table_and_pk(manifest, &job.index.source_message_type)?;
    let project_column = crate::generation::sql::resolve_project_column(table);
    let context = search_worker_context(&job.tenant_id, &job.project_id, "search_teardown");
    let collection = job.index.collection();
    let mut deleted = 0u64;
    let mut after_pk: Option<String> = None;
    loop {
        let request = source_rows_select_request(
            &job.index,
            &job.tenant_id,
            &primary_key,
            vec![primary_key.clone()],
            after_pk.as_deref(),
            project_column,
            &job.project_id,
        );
        let (record_set, _) = runtime
            .select(manifest, request, context.clone())
            .await
            .map_err(|err| format!("search teardown source select failed: {err}"))?;
        if record_set.records_json.is_empty() {
            break;
        }
        let fetched = record_set.records_json.len();
        let mut last_pk = String::new();
        let mut page_ids = Vec::new();
        for raw in &record_set.records_json {
            let row: serde_json::Value = serde_json::from_slice(raw)
                .map_err(|err| format!("decode search teardown source row failed: {err}"))?;
            let row_pk = row
                .as_object()
                .map(|map| json_str(map, &primary_key))
                .unwrap_or_default();
            if row_pk.trim().is_empty() {
                continue;
            }
            last_pk = row_pk.clone();
            page_ids.push(row_pk);
        }
        let page_count = page_ids.len() as u64;
        if !page_ids.is_empty() {
            runtime
                .vector_delete_backend_target(None, &job.project_id, &collection, page_ids)
                .await
                .map_err(|err| format!("search teardown vector delete failed: {err}"))?;
            deleted = deleted.saturating_add(page_count);
        }
        if fetched < SEARCH_REINDEX_PAGE_LIMIT as usize || last_pk.is_empty() {
            break;
        }
        after_pk = Some(last_pk);
    }
    Ok(deleted)
}

/// Execute one teardown job. Success emits the [`TOPIC_TEARDOWN_COMPLETED`]
/// marker; a failure logs and leaves the job unmarked so the next leader pass
/// retries it (a deleted index's engine data is never silently abandoned).
async fn process_search_teardown_job(
    service: &SearchServiceImpl,
    job: &SearchTeardownJob,
) -> Result<i64, String> {
    let runtime = service
        .runtime
        .as_deref()
        .ok_or_else(|| "search teardown requires runtime dispatch".to_string())?;
    let catalog = service
        .catalog
        .as_deref()
        .ok_or_else(|| "search teardown requires active catalog".to_string())?;
    if job.tenant_id.trim().is_empty() {
        return Ok(0);
    }
    let state = catalog.active_for(&job.project_id);
    match teardown_index_points(runtime, &state.manifest, job).await {
        Ok(deleted) => {
            service
                .emit_index_event(
                    TOPIC_TEARDOWN_COMPLETED,
                    &job.tenant_id,
                    &job.project_id,
                    &job.index.index_name,
                    serde_json::json!({
                        "teardown_event_id": job.event_id,
                        "points_deleted": deleted,
                    }),
                )
                .await;
            Ok(i64::try_from(deleted).unwrap_or(i64::MAX))
        }
        Err(err) => {
            tracing::warn!(
                index = %job.index.index_name,
                error = %err,
                "search index teardown failed; will retry on next pass"
            );
            Ok(0)
        }
    }
}

/// ONE leader-owned reindex/teardown pass (master-plan 16.2.2/16.2.3).
/// Consumes the `reindex.requested` jobs the Reindex RPC emits (mediated source
/// backfill → engine re-upsert → `REINDEXING → ACTIVE` writeback →
/// `reindex.completed`) and the `index.deleted` teardown jobs DeleteIndex
/// enqueues (per-point engine purge → `teardown.completed`). The leader spawns
/// this via `NativeWorkerHost::spawn_while_leader` (the
/// `run_cache_invalidation_worker_once` shape). Returns rows acted on.
pub(crate) async fn run_search_reindex_once(
    service: Arc<SearchServiceImpl>,
    journal_relation: &str,
    batch: i64,
) -> Result<i64, String> {
    let pool = service
        .pg_pool
        .as_ref()
        .ok_or_else(|| "search reindex worker requires native Postgres store".to_string())?;
    let outbox_relation = service
        .outbox_relation
        .as_deref()
        .ok_or_else(|| "search reindex worker requires transactional outbox".to_string())?;
    let jobs = load_search_reindex_jobs(pool, journal_relation, outbox_relation, batch).await?;
    let mut acted = 0i64;
    for job in &jobs {
        acted = acted.saturating_add(process_search_reindex_job(&service, job).await?);
    }
    let teardowns =
        load_search_teardown_jobs(pool, journal_relation, outbox_relation, batch).await?;
    for job in &teardowns {
        acted = acted.saturating_add(process_search_teardown_job(&service, job).await?);
    }
    Ok(acted)
}
