//! Static configuration for the native `EmbeddingService`: the entity message
//! name, the versioned outbox/work topics, the durable status tokens, the
//! top-k/quota bounds, the leader-pass batch knobs, the work-emitter cadence, and
//! the operator-tunable `Retrieve` score-threshold / fusion-weight knobs.
//! Extracted verbatim from the former god file; every value is byte-stable for
//! downstream audit/CDC consumers.

use std::sync::OnceLock;
use std::time::Duration;

pub(crate) const EMBEDDING_SOURCE_MSG: &str = "udb.core.embedding.entity.v1.EmbeddingSource";

/// The change-driven embedding WORK topic the sidecar pool consumes. Its payload
/// carries ONLY the row pk + text + non-secret routing — never any credential.
pub(crate) const TOPIC_WORK: &str = "udb.embedding.work.v1";
pub(crate) const TOPIC_SOURCE_REGISTERED: &str = "udb.embedding.source.registered.v1";
pub(crate) const TOPIC_SOURCE_DELETED: &str = "udb.embedding.source.deleted.v1";
pub(crate) const TOPIC_BACKFILL_REQUESTED: &str = "udb.embedding.backfill.requested.v1";
pub(crate) const TOPIC_BACKFILL_COMPLETED: &str = "udb.embedding.backfill.completed.v1";
/// Completion marker for a deleted source's vector teardown, keyed by
/// `teardown_event_id` (the journal event id of the source-deleted event) so the
/// leader pass never re-runs a finished teardown (mirrors the backfill
/// requested/completed pairing).
pub(crate) const TOPIC_SOURCE_TEARDOWN_COMPLETED: &str =
    "udb.embedding.source.teardown.completed.v1";

pub(crate) const STATUS_ACTIVE: &str = "ACTIVE";
pub(crate) const STATUS_DELETED: &str = "DELETED";
pub(crate) const EMBEDDING_WORK_EMITTER_BATCH: i64 = 200;
/// Page size for enumerating (and deleting) a deleted source's point ids during
/// vector teardown — bounds each journal scan and each vector-seam delete call.
pub(crate) const EMBEDDING_TEARDOWN_DELETE_BATCH: i64 = 200;
pub(crate) const EMBEDDING_BACKFILL_PAGE_LIMIT: i32 = 200;
const DEFAULT_EMBEDDING_WORK_EMITTER_INTERVAL_SECS: u64 = 30;
const EMBEDDING_WORK_EMITTER_INTERVAL_ENV: &str = "UDB_EMBEDDING_WORK_EMITTER_INTERVAL_SECS";

/// Minimum similarity score a vector hit must clear to be returned by `Retrieve`.
/// Operator-tunable (was a hardcoded `0.0`); resolved once via a `OnceLock`. `0.0`
/// keeps the historical "return everything the engine ranks" behavior by default.
/// A `RetrieveRequest.score_threshold` proto field (Part B.2.3) will later let a
/// caller raise this per query; until then this is the server-side floor.
const DEFAULT_EMBEDDING_RETRIEVE_SCORE_THRESHOLD: f32 = 0.0;
const EMBEDDING_RETRIEVE_SCORE_THRESHOLD_ENV: &str = "UDB_EMBEDDING_RETRIEVE_SCORE_THRESHOLD";
/// Comma-separated `lexical,vector` weights for hybrid-search fusion, e.g.
/// `"0.4,0.6"`. Empty (the default) preserves the delegated engine's built-in
/// fusion weighting (was a hardcoded empty vec).
const EMBEDDING_RETRIEVE_FUSION_WEIGHTS_ENV: &str = "UDB_EMBEDDING_RETRIEVE_FUSION_WEIGHTS";

/// Maximum characters of source text carried in a single `udb.embedding.work.v1`
/// event. Embedding models cap their input (roughly a few thousand tokens); an
/// unbounded row would make the sidecar's provider call fail, and — since a
/// failed embedding is never reported back — the row would silently stay
/// un-embedded. Bounding the text here keeps the request within a safe envelope.
/// Operator-tunable; a `<= 0`/malformed override falls back to the default.
const DEFAULT_EMBEDDING_MAX_TEXT_CHARS: usize = 8000;
const EMBEDDING_MAX_TEXT_CHARS_ENV: &str = "UDB_EMBEDDING_MAX_TEXT_CHARS";

/// Chunking knobs (Part B.2.2). A source row's text is split into overlapping
/// `chunk_size`-character windows (word-boundary-aware), `chunk_overlap`
/// characters shared between neighbors, capped at `max_chunks_per_row` points per
/// row. Defaults suit a general RAG corpus; all operator-tunable, resolved once.
/// A row whose text fits in one window keeps its bare `row_pk` point id (no
/// behavior change for short rows).
const DEFAULT_EMBEDDING_CHUNK_SIZE: usize = 1000;
const EMBEDDING_CHUNK_SIZE_ENV: &str = "UDB_EMBEDDING_CHUNK_SIZE";
const DEFAULT_EMBEDDING_CHUNK_OVERLAP: usize = 150;
const EMBEDDING_CHUNK_OVERLAP_ENV: &str = "UDB_EMBEDDING_CHUNK_OVERLAP";
const DEFAULT_EMBEDDING_MAX_CHUNKS_PER_ROW: usize = 256;
const EMBEDDING_MAX_CHUNKS_PER_ROW_ENV: &str = "UDB_EMBEDDING_MAX_CHUNKS_PER_ROW";

/// Fallback vector collection when a source row somehow carries no target (mirrors
/// `asset_service::DEFAULT_VECTOR_COLLECTION`; a source normally always specifies
/// its own `target_collection`, validated non-empty at register time).
pub(crate) const DEFAULT_VECTOR_COLLECTION: &str = "udb_asset_embeddings";

/// Default number of hits returned when the caller does not specify `top_k`.
const DEFAULT_TOP_K: i32 = 10;
/// Upper bound on `top_k` so one query cannot pull an unbounded result set.
const MAX_TOP_K: i32 = 200;
/// Per-tenant registered-source budget. Bounds the durable table so one tenant
/// cannot exhaust the shared store; a new source beyond this fails closed.
pub(crate) const MAX_SOURCES_PER_TENANT: usize = 128;

/// Clamp a requested `top_k` into `[1, MAX_TOP_K]`; non-positive → default.
pub(crate) fn resolve_top_k(requested: i32) -> i32 {
    if requested <= 0 {
        DEFAULT_TOP_K
    } else {
        requested.min(MAX_TOP_K)
    }
}

pub(crate) fn embedding_work_emitter_interval() -> Duration {
    static INTERVAL: OnceLock<Duration> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        Duration::from_secs(
            std::env::var(EMBEDDING_WORK_EMITTER_INTERVAL_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_EMBEDDING_WORK_EMITTER_INTERVAL_SECS),
        )
    })
}

/// Server-side minimum-score floor applied to every mediated `Retrieve`. Resolved
/// once (no per-request env read); a non-finite or negative override is ignored so
/// the floor is always a real, sane bound.
pub(crate) fn retrieve_score_threshold() -> f32 {
    static THRESHOLD: OnceLock<f32> = OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        std::env::var(EMBEDDING_RETRIEVE_SCORE_THRESHOLD_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<f32>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(DEFAULT_EMBEDDING_RETRIEVE_SCORE_THRESHOLD)
    })
}

/// Hybrid-search fusion weights, parsed once from a comma-separated list. Empty
/// (the default, and the fallback for any malformed/negative entry) hands fusion
/// weighting back to the delegated engine — identical to the previous hardcoded
/// `Vec::new()`.
pub(crate) fn retrieve_fusion_weights() -> Vec<f32> {
    static WEIGHTS: OnceLock<Vec<f32>> = OnceLock::new();
    WEIGHTS
        .get_or_init(|| {
            let Ok(raw) = std::env::var(EMBEDDING_RETRIEVE_FUSION_WEIGHTS_ENV) else {
                return Vec::new();
            };
            let parsed: Option<Vec<f32>> = raw
                .split(',')
                .map(|part| {
                    part.trim()
                        .parse::<f32>()
                        .ok()
                        .filter(|v| v.is_finite() && *v >= 0.0)
                })
                .collect();
            // ALL entries must parse to a valid non-negative weight, else fall back
            // to engine-default fusion rather than apply a half-parsed weighting.
            parsed
                .filter(|weights| !weights.is_empty())
                .unwrap_or_default()
        })
        .clone()
}

/// Maximum source-text characters per work event, resolved once. A `<= 0` or
/// malformed override is ignored so the bound is always a real, sane limit.
pub(crate) fn max_embedding_text_chars() -> usize {
    static MAX_CHARS: OnceLock<usize> = OnceLock::new();
    *MAX_CHARS.get_or_init(|| {
        std::env::var(EMBEDDING_MAX_TEXT_CHARS_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_EMBEDDING_MAX_TEXT_CHARS)
    })
}

fn usize_env_once(cell: &'static OnceLock<usize>, var: &str, default: usize) -> usize {
    *cell.get_or_init(|| {
        std::env::var(var)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default)
    })
}

/// Chunk window size in characters (resolved once). Bounded to
/// `max_embedding_text_chars` so a single chunk can never exceed the work-event
/// text envelope.
pub(crate) fn embedding_chunk_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    usize_env_once(
        &SIZE,
        EMBEDDING_CHUNK_SIZE_ENV,
        DEFAULT_EMBEDDING_CHUNK_SIZE,
    )
    .min(max_embedding_text_chars())
    .max(1)
}

/// Characters shared between neighboring chunks (resolved once). The chunker
/// clamps this below the window size to guarantee forward progress.
pub(crate) fn embedding_chunk_overlap() -> usize {
    static OVERLAP: OnceLock<usize> = OnceLock::new();
    // A `0` override is legitimate (no overlap), so this one accepts zero.
    *OVERLAP.get_or_init(|| {
        std::env::var(EMBEDDING_CHUNK_OVERLAP_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_EMBEDDING_CHUNK_OVERLAP)
    })
}

/// Safety cap on chunks emitted per source row (resolved once), so a
/// pathologically large row cannot fan out an unbounded number of points.
pub(crate) fn embedding_max_chunks_per_row() -> usize {
    static MAX_CHUNKS: OnceLock<usize> = OnceLock::new();
    usize_env_once(
        &MAX_CHUNKS,
        EMBEDDING_MAX_CHUNKS_PER_ROW_ENV,
        DEFAULT_EMBEDDING_MAX_CHUNKS_PER_ROW,
    )
}
