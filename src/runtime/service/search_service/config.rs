//! Static configuration for the native `SearchService`: the entity message name,
//! the versioned outbox topics, the durable status tokens, the supported engine
//! backends, the top-k/quota bounds, the leader-pass batch/cadence knobs, and the
//! tenant-scope/vector-distance keys. Extracted verbatim from the former god
//! file; every value is byte-stable for downstream audit/CDC consumers.

use std::sync::OnceLock;
use std::time::Duration;

pub(crate) const SEARCH_INDEX_MSG: &str = "udb.core.search.entity.v1.SearchIndex";

pub(crate) const TOPIC_CREATED: &str = "udb.search.index.created.v1";
pub(crate) const TOPIC_DELETED: &str = "udb.search.index.deleted.v1";
pub(crate) const TOPIC_REINDEX: &str = "udb.search.index.reindex.requested.v1";
/// Completion marker for one executed reindex request, keyed by
/// `reindex_event_id` (the journal event id of the reindex.requested event) so
/// the leader pass never re-runs a finished reindex (mirrors the embedding
/// backfill requested/completed pairing).
pub(crate) const TOPIC_REINDEX_COMPLETED: &str = "udb.search.index.reindex.completed.v1";
/// Completion marker for a deleted index's engine teardown, keyed by
/// `teardown_event_id` (the journal event id of the index.deleted event the
/// teardown job consumes).
pub(crate) const TOPIC_TEARDOWN_COMPLETED: &str = "udb.search.index.teardown.completed.v1";
/// Applied marker for one consumed CDC freshness event, keyed by
/// `source_event_id`, so the leader freshness pass never re-consumes a journal
/// event (the same output-event-as-dedup-marker pattern as cache invalidation
/// and the embedding work emitter).
pub(crate) const TOPIC_FRESHNESS_APPLIED: &str = "udb.search.index.freshness.applied.v1";

pub(crate) const STATUS_ACTIVE: &str = "ACTIVE";
pub(crate) const STATUS_REINDEXING: &str = "REINDEXING";
pub(crate) const STATUS_DELETED: &str = "DELETED";

pub(crate) const BACKEND_QDRANT: &str = "qdrant";
pub(crate) const BACKEND_ELASTICSEARCH: &str = "elasticsearch";

/// Default number of hits returned when the caller does not specify `top_k`.
pub(crate) const DEFAULT_TOP_K: i32 = 10;
/// Upper bound on `top_k` so one query cannot pull an unbounded result set.
pub(crate) const MAX_TOP_K: i32 = 200;
/// Per-tenant registered-index budget. Bounds the durable table so one tenant
/// cannot exhaust the shared store; a new index beyond this fails closed.
pub(crate) const MAX_INDEXES_PER_TENANT: usize = 128;

/// Journal events consumed per leader freshness pass (mirrors
/// `EMBEDDING_WORK_EMITTER_BATCH` / `CACHE_INVALIDATION_BATCH`).
pub(crate) const SEARCH_FRESHNESS_BATCH: i64 = 200;
/// Reindex/teardown jobs consumed per leader pass. Each job pages through its
/// ENTIRE source table, so the job batch is deliberately smaller than the
/// per-event journal batch.
pub(crate) const SEARCH_REINDEX_BATCH: i64 = 25;
/// Page size for enumerating source rows during a reindex/teardown pass
/// (mirrors `EMBEDDING_BACKFILL_PAGE_LIMIT`).
pub(crate) const SEARCH_REINDEX_PAGE_LIMIT: i32 = 200;
const DEFAULT_SEARCH_FRESHNESS_INTERVAL_SECS: u64 = 30;
const SEARCH_FRESHNESS_INTERVAL_ENV: &str = "UDB_SEARCH_FRESHNESS_INTERVAL_SECS";
const DEFAULT_SEARCH_REINDEX_INTERVAL_SECS: u64 = 30;
const SEARCH_REINDEX_INTERVAL_ENV: &str = "UDB_SEARCH_REINDEX_INTERVAL_SECS";

/// The tenant-scope payload key the vector IR compiler stamps at write time.
/// Every Search both filters on it server-side and strips it from returned hit
/// payloads (it is broker bookkeeping, never application data).
pub(crate) const TENANT_SCOPE_PAYLOAD_KEY: &str = "_tenant_id";
/// Vector distance metric provisioned for a new engine collection (cosine —
/// the same default the shared `vector_upsert_backend_target` seam ensures).
pub(crate) const ENGINE_VECTOR_DISTANCE: &str = "cosine";

/// Leader poll interval for the CDC freshness pass (env resolved ONCE).
pub(crate) fn search_freshness_interval() -> Duration {
    static INTERVAL: OnceLock<Duration> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        Duration::from_secs(
            std::env::var(SEARCH_FRESHNESS_INTERVAL_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_SEARCH_FRESHNESS_INTERVAL_SECS),
        )
    })
}

/// Leader poll interval for the reindex/teardown pass (env resolved ONCE).
pub(crate) fn search_reindex_interval() -> Duration {
    static INTERVAL: OnceLock<Duration> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        Duration::from_secs(
            std::env::var(SEARCH_REINDEX_INTERVAL_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_SEARCH_REINDEX_INTERVAL_SECS),
        )
    })
}

/// Clamp a requested `top_k` into `[1, MAX_TOP_K]`; non-positive → default.
pub(crate) fn resolve_top_k(requested: i32) -> i32 {
    if requested <= 0 {
        DEFAULT_TOP_K
    } else {
        requested.min(MAX_TOP_K)
    }
}
