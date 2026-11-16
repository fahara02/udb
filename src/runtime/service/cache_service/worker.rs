//! CDC-driven self-invalidation worker (leader-elected). Maps a source-table
//! change event to a namespace sweep and emits `udb.cache.invalidated.v1`. The
//! tenant is derived ONLY from the event payload; tenant-less events are skipped,
//! mirroring the CDC stream-scope fail-closed rule.

#[cfg(feature = "redis")]
use std::sync::Arc;

#[cfg(feature = "redis")]
use sqlx::{PgPool, Row};

#[cfg(feature = "redis")]
use crate::metrics::MetricsRecorder;

#[cfg(feature = "redis")]
use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_with_context};
#[cfg(feature = "redis")]
use super::config::TOPIC_INVALIDATED;
#[cfg(feature = "redis")]
use super::redis_engine;

/// A single source-table change event the invalidation worker consumes. The
/// tenant is taken from the CDC payload, never from a request body.
#[derive(Debug, Clone)]
pub(crate) struct CacheInvalidationEvent {
    /// Source CDC event id. Empty for direct/test callers; worker-loaded events
    /// carry this and stamp it into the invalidation event for durable dedupe.
    pub event_id: String,
    /// The CDC topic the event arrived on (carried through for tracing).
    pub topic: String,
    /// The changed source table; maps to the cache namespace to invalidate.
    pub source_table: String,
    /// The full CDC payload — inspected for `tenant_id` (fail-closed scoping).
    pub payload: serde_json::Value,
}

/// Fail-closed tenant scoping for an invalidation event: return the trimmed
/// payload `tenant_id` only when non-empty. A tenant-less event yields `None` and
/// is SKIPPED — it never triggers a cross-tenant namespace sweep. This mirrors the
/// CDC stream-scope rule in `engine_tail::payload_value_matches_stream_scope`
/// (#8): a payload that cannot prove which tenant it belongs to is not delivered.
pub(crate) fn invalidation_tenant_scope(payload: &serde_json::Value) -> Option<String> {
    let tenant = payload
        .get("tenant_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim();
    (!tenant.is_empty()).then(|| tenant.to_string())
}

/// Map a CDC source table to the cache namespace it invalidates. Convention: the
/// namespace shares the table's name. `None` for a table that carries no name.
pub(crate) fn namespace_for_source_table(source_table: &str) -> Option<String> {
    let table = source_table.trim();
    (!table.is_empty()).then(|| table.to_string())
}

pub(crate) fn invalidation_source_from_payload(payload: &serde_json::Value) -> String {
    for key in ["source_table", "table_name", "message_type"] {
        if let Some(value) = payload.get(key).and_then(serde_json::Value::as_str) {
            let value = value.trim();
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    String::new()
}

/// The invalidator's row in the journal reader cursors.
#[cfg(feature = "redis")]
const CACHE_JOURNAL_READER: &str = "cache.invalidate";
/// Journal positions one invalidation tick scans at most.
#[cfg(feature = "redis")]
const CACHE_JOURNAL_WINDOW: i64 = 5_000;

/// One window of candidate events, plus the last journal position the query
/// returned (before any row is skipped in memory).
#[cfg(feature = "redis")]
struct CacheInvalidationPage {
    events: Vec<(i64, CacheInvalidationEvent)>,
    rows: usize,
    last_position: Option<i64>,
}

/// Bind order: `$1` the invalidation topic, `$2` the batch, `$3`/`$4` the
/// journal position window `(from, to]` of the invalidator's reader cursor, so a
/// tick never re-reads the whole retained journal.
#[cfg(feature = "redis")]
async fn load_cache_invalidation_events(
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    batch: i64,
    window: (i64, i64),
) -> Result<CacheInvalidationPage, String> {
    let limit = batch.max(1);
    let rows = sqlx::query(&format!(
        "SELECT j.event_id::TEXT AS event_id, j.topic AS topic, j.payload::TEXT AS payload_json, \
                j.journal_position AS journal_position \
         FROM {journal_relation} j \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND j.journal_position > $3 \
           AND j.journal_position <= $4 \
           AND j.topic <> $1 \
           AND COALESCE(j.payload->>'tenant_id', '') <> '' \
           AND ( \
               COALESCE(j.payload->>'source_table', '') <> '' \
               OR COALESCE(j.payload->>'table_name', '') <> '' \
               OR COALESCE(j.payload->>'message_type', '') <> '' \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM {outbox_relation} o \
               WHERE o.topic = $1 \
                 AND o.payload->>'source_event_id' = j.event_id::TEXT \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM {journal_relation} done \
               WHERE done.topic = $1 \
                 AND done.payload->>'source_event_id' = j.event_id::TEXT \
           ) \
         ORDER BY j.journal_position ASC \
         LIMIT $2"
    ))
    .bind(TOPIC_INVALIDATED)
    .bind(limit)
    .bind(window.0)
    .bind(window.1)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load cache invalidation events failed: {err}"))?;

    let mut events = Vec::with_capacity(rows.len());
    let row_count = rows.len();
    let mut last_position = None;
    for row in rows {
        let position: i64 = row
            .try_get("journal_position")
            .map_err(|err| format!("decode cache invalidation position failed: {err}"))?;
        last_position = Some(position);
        let payload_json: String = row
            .try_get("payload_json")
            .map_err(|err| format!("decode cache invalidation payload failed: {err}"))?;
        let payload: serde_json::Value = serde_json::from_str(&payload_json)
            .map_err(|err| format!("decode cache invalidation payload JSON failed: {err}"))?;
        let source_table = invalidation_source_from_payload(&payload);
        if source_table.trim().is_empty() {
            continue;
        }
        events.push((
            position,
            CacheInvalidationEvent {
                event_id: row
                    .try_get("event_id")
                    .map_err(|err| format!("decode cache invalidation event id failed: {err}"))?,
                topic: row
                    .try_get("topic")
                    .map_err(|err| format!("decode cache invalidation topic failed: {err}"))?,
                source_table,
                payload,
            },
        ));
    }
    Ok(CacheInvalidationPage {
        events,
        rows: row_count,
        last_position,
    })
}

/// Run ONE leader pass of CDC-driven cache invalidation (the consumer shape the
/// leader spawns under `run_while_leader(WORKER_CACHE_INVALIDATOR)`). For each
/// source-table change event it: derives the tenant fail-closed from the payload
/// (skipping tenant-less events), `SCAN`+`DEL`-sweeps the matching namespace for
/// that tenant, and emits `udb.cache.invalidated.v1`. Returns the total number of
/// cache keys invalidated and the index of the first event whose sweep failed.
/// Best-effort per event: a single failing sweep is logged and skipped rather
/// than aborting the whole pass; the caller retries from it on the next tick.
#[cfg_attr(test, allow(dead_code))]
#[allow(dead_code)]
#[cfg(feature = "redis")]
pub(crate) async fn run_cache_invalidation_once(
    redis: &redis::Client,
    outbox_pool: &PgPool,
    outbox_relation: Option<&str>,
    events: &[CacheInvalidationEvent],
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) -> Result<(u64, Option<usize>), String> {
    let mut total: u64 = 0;
    let mut first_failed = None;
    for (index, event) in events.iter().enumerate() {
        // Fail-closed: a tenant-less event never sweeps another tenant's keyspace.
        let Some(tenant) = invalidation_tenant_scope(&event.payload) else {
            tracing::debug!(
                topic = %event.topic,
                "cache invalidation: skipping tenant-less event"
            );
            continue;
        };
        let Some(namespace) = namespace_for_source_table(&event.source_table) else {
            continue;
        };
        // A change to a shared (global) table carries the reserved shared marker
        // instead of a tenant: every tenant may have cached its rows, so every
        // tenant holding this namespace is swept, not only the platform writer.
        let tenants: Vec<String> = if crate::broker::is_shared_tenant_marker(&tenant) {
            match tenants_holding_namespace(redis, &namespace).await {
                Ok(mut found) => {
                    found.insert(tenant.clone());
                    found.into_iter().collect()
                }
                Err(err) => {
                    tracing::warn!(
                        namespace = %namespace,
                        error = %err,
                        "cache invalidation tenant discovery failed; skipping event"
                    );
                    first_failed.get_or_insert(index);
                    continue;
                }
            }
        } else {
            vec![tenant]
        };
        // Sweep every tenant first and emit only when all sweeps succeeded: the
        // emitted event marks the source event done, so emitting after a partial
        // sweep would leave the remaining tenants stale for good. A retry
        // re-sweeps all of them, which is harmless.
        let mut swept = Vec::with_capacity(tenants.len());
        let mut failed = false;
        for tenant in tenants {
            match redis_engine::flush_namespace(redis, &tenant, &namespace).await {
                Ok(deleted) => swept.push((tenant, deleted)),
                Err(err) => {
                    tracing::warn!(
                        tenant = %tenant,
                        namespace = %namespace,
                        error = %err,
                        "cache invalidation sweep failed; skipping event"
                    );
                    failed = true;
                    break;
                }
            }
        }
        if failed {
            first_failed.get_or_insert(index);
            continue;
        }
        for (tenant, deleted) in swept {
            total = total.saturating_add(deleted);
            // Emit the invalidation event so downstream consumers (and live-query/
            // read-fence machinery) observe the namespace turn over.
            // Best-effort by nature: the durable effect of this pass is the cache
            // sweep itself (Redis/memcached), not a Postgres write, so there is no
            // transaction for the event to join.
            enqueue_outbox_event_with_context(
                outbox_pool,
                outbox_relation,
                TOPIC_INVALIDATED,
                &namespace,
                &tenant,
                "",
                serde_json::json!({
                    "tenant_id": tenant,
                    "namespace": namespace,
                    "keys_invalidated": deleted,
                    "source_event_id": event.event_id,
                    "source_table": event.source_table,
                    "reason": "cdc_source_change",
                }),
                NativeEventContext {
                    operation: "cache.invalidate".to_string(),
                    target_resource: namespace.clone(),
                    ..NativeEventContext::default()
                },
                metrics,
            )
            .await;
        }
    }
    Ok((total, first_failed))
}

/// The Redis `SCAN MATCH` pattern for `namespace` under ANY tenant. Glob
/// metacharacters in the namespace are escaped so it matches literally.
#[cfg(any(test, feature = "redis"))]
pub(crate) fn any_tenant_namespace_pattern(namespace: &str) -> String {
    format!(
        "{}:*:{}:*",
        super::config::KEY_ROOT,
        super::keys::glob_literal(namespace)
    )
}

/// The tenants that hold `namespace`, recovered from full cache keys. A key is
/// `udb:cache:<tenant>:<namespace>:<rest>`; a tenant id may itself contain `:`,
/// so every split point is tried and kept only when the key is exactly one of
/// that (tenant, namespace)'s data, byte-counter or meta keys. A key of another
/// namespace that merely contains `:<namespace>:` is never attributed.
#[cfg(any(test, feature = "redis"))]
pub(crate) fn namespace_key_tenants(
    keys: &[String],
    namespace: &str,
) -> std::collections::BTreeSet<String> {
    use super::keys::{bytes_counter_key, meta_key, strip_data_prefix};
    let root = format!("{}:", super::config::KEY_ROOT);
    let needle = format!(":{namespace}:");
    let mut tenants = std::collections::BTreeSet::new();
    for key in keys {
        let Some(rest) = key.strip_prefix(&root) else {
            continue;
        };
        let mut from = 0;
        while let Some(found) = rest[from..].find(&needle) {
            let at = from + found;
            let tenant = &rest[..at];
            if !tenant.trim().is_empty()
                && (strip_data_prefix(key, tenant, namespace).is_some()
                    || *key == bytes_counter_key(tenant, namespace)
                    || *key == meta_key(tenant, namespace))
            {
                tenants.insert(tenant.to_string());
            }
            from = at + 1;
        }
    }
    tenants
}

/// Every tenant currently holding keys under `namespace` (a bounded `SCAN`,
/// never `KEYS`). Used only for changes to shared tables.
#[cfg(feature = "redis")]
async fn tenants_holding_namespace(
    redis: &redis::Client,
    namespace: &str,
) -> Result<std::collections::BTreeSet<String>, String> {
    use super::config::{SWEEP_COMMAND, SWEEP_COUNT};
    let mut conn = crate::runtime::redis_shared::connection(redis)
        .await
        .map_err(|err| format!("redis connection failed: {err}"))?;
    let pattern = any_tenant_namespace_pattern(namespace);
    let mut cursor: u64 = 0;
    let mut tenants = std::collections::BTreeSet::new();
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd(SWEEP_COMMAND)
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(SWEEP_COUNT)
            .query_async(&mut conn)
            .await
            .map_err(|err| format!("redis SCAN failed: {err}"))?;
        tenants.extend(namespace_key_tenants(&keys, namespace));
        if next == 0 {
            break;
        }
        cursor = next;
    }
    Ok(tenants)
}

#[cfg(test)]
mod shared_invalidation_tests {
    use super::super::config::KEY_ROOT;
    use super::super::keys::{bytes_counter_key, data_key, meta_key};
    use super::{any_tenant_namespace_pattern, namespace_key_tenants};

    #[test]
    fn shared_sweep_finds_every_tenant_holding_the_namespace() {
        let keys = vec![
            data_key("tenant-a", "concepts", "c1"),
            bytes_counter_key("tenant-b", "concepts"),
            meta_key("org:tenant-c", "concepts"),
            // Another namespace whose local key happens to contain the name.
            data_key("tenant-d", "orders", "x:concepts:y"),
            "unrelated:key".to_string(),
        ];
        let tenants: Vec<String> = namespace_key_tenants(&keys, "concepts")
            .into_iter()
            .collect();
        assert_eq!(tenants, vec!["org:tenant-c", "tenant-a", "tenant-b"]);
    }

    #[test]
    fn any_tenant_pattern_escapes_glob_characters() {
        assert_eq!(
            any_tenant_namespace_pattern("con*cepts"),
            format!("{KEY_ROOT}:*:con\\*cepts:*")
        );
    }
}

/// One full worker tick: CDC-driven namespace invalidation, then a bounded
/// byte-counter reconciliation pass.
///
/// Reconciliation exists because Redis TTL expiry deletes data keys WITHOUT
/// running the `DECRBY` bookkeeping that `delete` performs, so `__bytes__` only
/// ever drifts UPWARD until a namespace hits false `resource_exhausted`. Each
/// tick recomputes a rotating subset of namespaces from ground truth
/// (SCAN + STRLEN) and SETs the counter to the recomputed absolute sum — see
/// `redis_engine::reconcile_bytes_counters_once` for the bounds. Best-effort:
/// a failed reconciliation never aborts the invalidation result, because a
/// stale counter only over-counts, which fails CLOSED toward refusing writes.
#[cfg(feature = "redis")]
pub(crate) async fn run_cache_invalidation_worker_once(
    redis: &redis::Client,
    outbox_pool: &PgPool,
    outbox_relation: &str,
    journal_relation: &str,
    batch: i64,
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) -> Result<i64, String> {
    use crate::runtime::cdc::journal;
    let catalog = crate::runtime::system::SystemCatalogConfig::current();
    let head = journal::head(outbox_pool, &catalog)
        .await
        .map_err(|err| format!("read CDC journal head failed: {err}"))?;
    let window = journal::reader_window(
        outbox_pool,
        &catalog,
        CACHE_JOURNAL_READER,
        head,
        CACHE_JOURNAL_WINDOW,
    )
    .await
    .map_err(|err| format!("read cache journal reader failed: {err}"))?;
    let page = load_cache_invalidation_events(
        outbox_pool,
        journal_relation,
        outbox_relation,
        batch,
        window,
    )
    .await?;
    let (positions, events): (Vec<i64>, Vec<CacheInvalidationEvent>) =
        page.events.into_iter().unzip();
    let (invalidated, first_failed) =
        run_cache_invalidation_once(redis, outbox_pool, Some(outbox_relation), &events, metrics)
            .await?;
    // Stop before a failed sweep so the next tick retries it; events already
    // invalidated after it are excluded by the loader's dedupe.
    let next_position = match first_failed {
        Some(index) => positions[index] - 1,
        None if (page.rows as i64) < batch.max(1) => window.1,
        None => page.last_position.unwrap_or(window.1),
    };
    journal::advance_reader(outbox_pool, &catalog, CACHE_JOURNAL_READER, next_position)
        .await
        .map_err(|err| format!("advance cache journal reader failed: {err}"))?;
    match redis_engine::reconcile_bytes_counters_once(redis).await {
        Ok(reconciled) if reconciled > 0 => {
            tracing::debug!(
                namespaces = reconciled,
                "cache byte-counter reconciliation pass completed"
            );
        }
        Ok(_) => {}
        Err(err) => {
            tracing::warn!(
                error = %err,
                "cache byte-counter reconciliation pass failed; counters stay \
                 eventually consistent until the next pass"
            );
        }
    }
    Ok(i64::try_from(invalidated).unwrap_or(i64::MAX))
}
