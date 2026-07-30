//! Static configuration for the native `EmbeddingService`: the entity message
//! name, the versioned outbox/work topics, the durable status tokens, the
//! top-k/quota bounds, the leader-pass batch knobs, the work-emitter cadence, and
//! the operator-tunable `Retrieve` score-threshold / fusion-weight knobs.
//! Extracted verbatim from the former god file; every value is byte-stable for
//! downstream audit/CDC consumers.

use std::sync::OnceLock;
use std::time::Duration;

pub(crate) const EMBEDDING_SOURCE_MSG: &str = "udb.core.embedding.entity.v1.EmbeddingSource";
pub(crate) const EMBEDDING_MODEL_MSG: &str = "udb.core.embedding.entity.v1.EmbeddingModel";
pub(crate) const EMBEDDING_JOB_MSG: &str = "udb.core.embedding.entity.v1.EmbeddingJob";
pub(crate) const EMBEDDING_WORK_ITEM_MSG: &str = "udb.core.embedding.entity.v1.EmbeddingWorkItem";
pub(crate) const EMBEDDING_DOCUMENT_MSG: &str = "udb.core.embedding.entity.v1.EmbeddingDocument";

/// The change-driven embedding WORK topic the sidecar pool consumes. Its payload
/// carries ONLY the row pk + text + non-secret routing — never any credential.
pub(crate) const TOPIC_WORK: &str = "udb.embedding.work.v1";
pub(crate) const TOPIC_SOURCE_REGISTERED: &str = "udb.embedding.source.registered.v1";
pub(crate) const TOPIC_SOURCE_DELETED: &str = "udb.embedding.source.deleted.v1";
pub(crate) const TOPIC_BACKFILL_REQUESTED: &str = "udb.embedding.backfill.requested.v1";
pub(crate) const TOPIC_BACKFILL_COMPLETED: &str = "udb.embedding.backfill.completed.v1";
pub(crate) const TOPIC_SOURCE_CHANGE_COMPLETED: &str = "udb.embedding.source.change.completed.v1";
pub(crate) const TOPIC_MODEL_REGISTERED: &str = "udb.embedding.model.registered.v1";
pub(crate) const TOPIC_MODEL_STATUS_CHANGED: &str = "udb.embedding.model.status.changed.v1";
pub(crate) const TOPIC_MODEL_ALIAS_CUTOVER: &str = "udb.embedding.model.alias.cutover.v1";
pub(crate) const TOPIC_DOCUMENT_PARSE: &str = "udb.embedding.document.parse.v1";
pub(crate) const TOPIC_DOCUMENT_INGESTED: &str = "udb.embedding.document.ingested.v1";
pub(crate) const TOPIC_WORK_DEAD_LETTER: &str = "udb.embedding.work.dead.v1";
pub(crate) const TOPIC_METERED: &str = "udb.embedding.metered.v1";
pub(crate) const TOPIC_RETRIEVAL_SAMPLED: &str = "udb.embedding.retrieval.sampled.v1";
pub(crate) const TOPIC_RETRIEVAL_EVALUATED: &str = "udb.embedding.retrieval.evaluated.v1";
/// Completion marker for a deleted source's vector teardown, keyed by
/// `teardown_event_id` (the journal event id of the source-deleted event) so the
/// leader pass never re-runs a finished teardown (mirrors the backfill
/// requested/completed pairing).
pub(crate) const TOPIC_SOURCE_TEARDOWN_COMPLETED: &str =
    "udb.embedding.source.teardown.completed.v1";

pub(crate) const STATUS_ACTIVE: &str = "ACTIVE";
pub(crate) const STATUS_DELETED: &str = "DELETED";
pub(crate) const STATUS_DEPRECATED: &str = "DEPRECATED";
pub(crate) const STATUS_RETIRED: &str = "RETIRED";
pub(crate) const WORK_PENDING: &str = "PENDING";
pub(crate) const WORK_ACKED: &str = "ACKED";
pub(crate) const WORK_DEAD: &str = "DEAD";
pub(crate) const JOB_PENDING: &str = "PENDING";
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

/// Optional sidecar reranker endpoint. Empty (the default) disables reranking
/// unless a request explicitly asks for it (then a strict config gets a typed
/// capability error). Resolved ONCE via a `OnceLock` — was read per-request via
/// a raw `std::env::var`, unlike every other embedding knob.
const EMBEDDING_RERANK_URL_ENV: &str = "UDB_EMBEDDING_RERANK_URL";
/// Upper bound on the candidate set a single `Retrieve` pulls from the store
/// before MMR/rerank/top-k trimming, so one query cannot pull an unbounded set
/// (was a hardcoded `.min(800)`).
const DEFAULT_EMBEDDING_MAX_CANDIDATES: usize = 800;
const EMBEDDING_MAX_CANDIDATES_ENV: &str = "UDB_EMBEDDING_MAX_CANDIDATES";
/// Multiplier applied to `top_k` to over-fetch candidates when MMR diversity
/// re-ranking is requested (MMR needs a larger pool to select from). Was a
/// hardcoded `top_k * 4`.
const DEFAULT_EMBEDDING_MMR_OVERFETCH: usize = 4;
const EMBEDDING_MMR_OVERFETCH_ENV: &str = "UDB_EMBEDDING_MMR_OVERFETCH";
/// Per-request timeout for the sidecar reranker HTTP call (was a hardcoded
/// `Duration::from_secs(10)`).
const DEFAULT_EMBEDDING_RERANK_TIMEOUT_SECS: u64 = 10;
const EMBEDDING_RERANK_TIMEOUT_ENV: &str = "UDB_EMBEDDING_RERANK_TIMEOUT_SECS";

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
pub(crate) const MAX_MODELS_PER_TENANT: usize = 128;
pub(crate) const MAX_EMBEDDING_REPORT_BATCH: usize = 256;
pub(crate) const MAX_DOCUMENT_INGEST_BATCH: usize = 100;
pub(crate) const DEFAULT_WORK_MAX_ATTEMPTS: i32 = 5;
const DEFAULT_WORK_VISIBILITY_TIMEOUT_SECS: u64 = 120;
const WORK_VISIBILITY_TIMEOUT_ENV: &str = "UDB_EMBEDDING_WORK_VISIBILITY_TIMEOUT_SECS";
const DEFAULT_RETRY_SWEEP_LIMIT: i64 = 200;
const RETRY_SWEEP_LIMIT_ENV: &str = "UDB_EMBEDDING_RETRY_SWEEP_LIMIT";
const RETRIEVAL_EVAL_SAMPLE_RATE_ENV: &str = "UDB_EMBEDDING_EVAL_SAMPLE_RATE";
const FRESH_BUFFER_TTL_ENV: &str = "UDB_EMBEDDING_FRESH_BUFFER_TTL_SECONDS";
const FRESH_BUFFER_CAPACITY_ENV: &str = "UDB_EMBEDDING_FRESH_BUFFER_CAPACITY";
const DEFAULT_FRESH_BUFFER_TTL_SECS: u64 = 30;
const DEFAULT_FRESH_BUFFER_CAPACITY: usize = 10_000;

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

/// Sidecar reranker endpoint, resolved once (trimmed). Empty ⇒ reranking is
/// unconfigured. Returns a `&'static str` so callers never re-read the env.
pub(crate) fn embedding_rerank_url() -> &'static str {
    static URL: OnceLock<String> = OnceLock::new();
    URL.get_or_init(|| {
        std::env::var(EMBEDDING_RERANK_URL_ENV)
            .map(|value| value.trim().to_string())
            .unwrap_or_default()
    })
    .as_str()
}

/// Candidate-set cap for a single `Retrieve` (resolved once). A `<= 0`/malformed
/// override falls back to the default so the bound is always sane.
pub(crate) fn embedding_max_candidates() -> usize {
    static MAX: OnceLock<usize> = OnceLock::new();
    usize_env_once(
        &MAX,
        EMBEDDING_MAX_CANDIDATES_ENV,
        DEFAULT_EMBEDDING_MAX_CANDIDATES,
    )
}

/// MMR over-fetch multiplier applied to `top_k` (resolved once). A `<= 0`/
/// malformed override falls back to the default.
pub(crate) fn embedding_mmr_overfetch() -> usize {
    static OVERFETCH: OnceLock<usize> = OnceLock::new();
    usize_env_once(
        &OVERFETCH,
        EMBEDDING_MMR_OVERFETCH_ENV,
        DEFAULT_EMBEDDING_MMR_OVERFETCH,
    )
}

/// Sidecar reranker HTTP timeout (resolved once). A `<= 0`/malformed override
/// falls back to the default so the timeout is always a real, sane bound.
pub(crate) fn embedding_rerank_timeout() -> Duration {
    static TIMEOUT: OnceLock<Duration> = OnceLock::new();
    *TIMEOUT.get_or_init(|| {
        Duration::from_secs(
            std::env::var(EMBEDDING_RERANK_TIMEOUT_ENV)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_EMBEDDING_RERANK_TIMEOUT_SECS),
        )
    })
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

/// A positive `usize` knob bounded to `[1, 100]` (a percentage-style margin);
/// a `0`/out-of-range/malformed override falls back to the default.
fn pct_env_once(cell: &'static OnceLock<usize>, var: &str, default: usize) -> usize {
    *cell.get_or_init(|| {
        std::env::var(var)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|v| (1..=100).contains(v))
            .unwrap_or(default)
    })
}

/// A trimmed, non-empty string knob resolved once. Returns a `&'static str` so
/// callers never re-read the env (same pattern as [`embedding_rerank_url`]).
fn str_env_once(cell: &'static OnceLock<String>, var: &str, default: &str) -> &'static str {
    cell.get_or_init(|| {
        std::env::var(var)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| default.to_string())
    })
    .as_str()
}

// ── RegisterModel defaults (Part B.2 straggler centralization) ───────────────
// Historically hardcoded in `registry.rs`; exposed as knobs with byte-stable
// defaults so the write path AND the re-register identity check resolve the SAME
// value (they must agree or a benign re-register would be rejected as a geometry
// change).
const DEFAULT_EMBEDDING_REGISTER_CHUNK_TOKENS: i32 = 512;
const EMBEDDING_REGISTER_CHUNK_TOKENS_ENV: &str = "UDB_EMBEDDING_DEFAULT_CHUNK_TOKENS";
const DEFAULT_EMBEDDING_REGISTER_OVERLAP_PCT: i32 = 15;
const EMBEDDING_REGISTER_OVERLAP_PCT_ENV: &str = "UDB_EMBEDDING_DEFAULT_CHUNK_OVERLAP_PCT";
const DEFAULT_EMBEDDING_VECTOR_BACKEND: &str = "qdrant";
const EMBEDDING_VECTOR_BACKEND_ENV: &str = "UDB_EMBEDDING_DEFAULT_VECTOR_BACKEND";
const DEFAULT_EMBEDDING_DISTANCE_METRIC: &str = "COSINE";
const EMBEDDING_DISTANCE_METRIC_ENV: &str = "UDB_EMBEDDING_DEFAULT_DISTANCE_METRIC";
const DEFAULT_EMBEDDING_OUTPUT_DTYPE: &str = "FLOAT32";
const EMBEDDING_OUTPUT_DTYPE_ENV: &str = "UDB_EMBEDDING_DEFAULT_OUTPUT_DTYPE";
const DEFAULT_EMBEDDING_TASK_TYPE: &str = "DOCUMENT";
const EMBEDDING_TASK_TYPE_ENV: &str = "UDB_EMBEDDING_DEFAULT_TASK_TYPE";
const DEFAULT_EMBEDDING_CHUNKING_STRATEGY: &str = "TOKEN_RECURSIVE";
const EMBEDDING_CHUNKING_STRATEGY_ENV: &str = "UDB_EMBEDDING_DEFAULT_CHUNKING_STRATEGY";

/// Default token-window size applied when a `RegisterModelRequest` omits
/// `chunk_tokens` (was a hardcoded `512`, still clamped to `max_input_tokens` by
/// the caller). Must be positive.
pub(crate) fn embedding_default_chunk_tokens() -> i32 {
    static TOKENS: OnceLock<i32> = OnceLock::new();
    *TOKENS.get_or_init(|| {
        std::env::var(EMBEDDING_REGISTER_CHUNK_TOKENS_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<i32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_EMBEDDING_REGISTER_CHUNK_TOKENS)
    })
}

/// Default chunk-overlap percentage applied when a request omits
/// `chunk_overlap_tokens` (was a hardcoded `15%`). `0` is a legitimate override
/// (no overlap); values are clamped to `[0, 100]`.
pub(crate) fn embedding_default_overlap_pct() -> i32 {
    static PCT: OnceLock<i32> = OnceLock::new();
    *PCT.get_or_init(|| {
        std::env::var(EMBEDDING_REGISTER_OVERLAP_PCT_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<i32>().ok())
            .filter(|v| (0..=100).contains(v))
            .unwrap_or(DEFAULT_EMBEDDING_REGISTER_OVERLAP_PCT)
    })
}

/// Default vector backend when a request omits `vector_backend` (was `"qdrant"`).
pub(crate) fn embedding_default_vector_backend() -> &'static str {
    static BACKEND: OnceLock<String> = OnceLock::new();
    str_env_once(
        &BACKEND,
        EMBEDDING_VECTOR_BACKEND_ENV,
        DEFAULT_EMBEDDING_VECTOR_BACKEND,
    )
}

/// Default distance metric when a request omits `distance_metric` (was `"COSINE"`).
pub(crate) fn embedding_default_distance_metric() -> &'static str {
    static METRIC: OnceLock<String> = OnceLock::new();
    str_env_once(
        &METRIC,
        EMBEDDING_DISTANCE_METRIC_ENV,
        DEFAULT_EMBEDDING_DISTANCE_METRIC,
    )
}

/// Default output dtype when a request omits `output_dtype` (was `"FLOAT32"`).
pub(crate) fn embedding_default_output_dtype() -> &'static str {
    static DTYPE: OnceLock<String> = OnceLock::new();
    str_env_once(
        &DTYPE,
        EMBEDDING_OUTPUT_DTYPE_ENV,
        DEFAULT_EMBEDDING_OUTPUT_DTYPE,
    )
}

/// Default task type when a request omits `task_type` (was `"DOCUMENT"`).
pub(crate) fn embedding_default_task_type() -> &'static str {
    static TASK: OnceLock<String> = OnceLock::new();
    str_env_once(&TASK, EMBEDDING_TASK_TYPE_ENV, DEFAULT_EMBEDDING_TASK_TYPE)
}

/// Default chunking strategy when a request omits `chunking_strategy`
/// (was `"TOKEN_RECURSIVE"`).
pub(crate) fn embedding_default_chunking_strategy() -> &'static str {
    static STRATEGY: OnceLock<String> = OnceLock::new();
    str_env_once(
        &STRATEGY,
        EMBEDDING_CHUNKING_STRATEGY_ENV,
        DEFAULT_EMBEDDING_CHUNKING_STRATEGY,
    )
}

// ── Chunking heuristics (Part B.2 straggler centralization) ──────────────────
const DEFAULT_EMBEDDING_PROVIDER_TOKEN_MARGIN_PCT: usize = 85;
const EMBEDDING_PROVIDER_TOKEN_MARGIN_PCT_ENV: &str = "UDB_EMBEDDING_PROVIDER_TOKEN_MARGIN_PCT";
const DEFAULT_EMBEDDING_CHUNK_BOUNDARY_MIN_PCT: usize = 80;
const EMBEDDING_CHUNK_BOUNDARY_MIN_PCT_ENV: &str = "UDB_EMBEDDING_CHUNK_BOUNDARY_MIN_PCT";

/// Percentage of a provider's `max_input_tokens` a token window may fill before
/// the safety margin kicks in (was a hardcoded `85%`). The broker counts
/// whitespace tokens, not provider-exact BPE, so it stays under the ceiling.
pub(crate) fn embedding_provider_token_margin_pct() -> usize {
    static PCT: OnceLock<usize> = OnceLock::new();
    pct_env_once(
        &PCT,
        EMBEDDING_PROVIDER_TOKEN_MARGIN_PCT_ENV,
        DEFAULT_EMBEDDING_PROVIDER_TOKEN_MARGIN_PCT,
    )
}

/// Minimum percentage of a token window that must be filled before the chunker
/// will cut early at a paragraph/sentence boundary (was a hardcoded `window*4/5`
/// i.e. 80%). Keeps chunks from collapsing to tiny fragments at the first
/// boundary.
pub(crate) fn embedding_chunk_boundary_min_pct() -> usize {
    static PCT: OnceLock<usize> = OnceLock::new();
    pct_env_once(
        &PCT,
        EMBEDDING_CHUNK_BOUNDARY_MIN_PCT_ENV,
        DEFAULT_EMBEDDING_CHUNK_BOUNDARY_MIN_PCT,
    )
}

// ── Durable-queue retry backoff (Part B.2 straggler centralization) ──────────
const DEFAULT_EMBEDDING_RETRY_BACKOFF_CAP_SECS: i64 = 3600;
const EMBEDDING_RETRY_BACKOFF_CAP_ENV: &str = "UDB_EMBEDDING_RETRY_BACKOFF_CAP_SECS";
const DEFAULT_EMBEDDING_RETRY_BACKOFF_BASE: i64 = 2;
const EMBEDDING_RETRY_BACKOFF_BASE_ENV: &str = "UDB_EMBEDDING_RETRY_BACKOFF_BASE";

/// Upper bound (seconds) on the exponential nack backoff `next_attempt_at` gap
/// (was a hardcoded `LEAST(3600, ...)`). Must be positive.
pub(crate) fn embedding_retry_backoff_cap_secs() -> i64 {
    static CAP: OnceLock<i64> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var(EMBEDDING_RETRY_BACKOFF_CAP_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_EMBEDDING_RETRY_BACKOFF_CAP_SECS)
    })
}

/// Exponential base for the nack backoff `power(base, attempt)` (was a hardcoded
/// `2`). Clamped to `>= 2` so retries always grow.
pub(crate) fn embedding_retry_backoff_base() -> i64 {
    static BASE: OnceLock<i64> = OnceLock::new();
    *BASE.get_or_init(|| {
        std::env::var(EMBEDDING_RETRY_BACKOFF_BASE_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| *v >= 2)
            .unwrap_or(DEFAULT_EMBEDDING_RETRY_BACKOFF_BASE)
    })
}

// ── Matryoshka truncated-dim cutover (Part B.2.3) ────────────────────────────
const DEFAULT_EMBEDDING_MATRYOSHKA_STRATEGY: &str = "largest";
const EMBEDDING_MATRYOSHKA_STRATEGY_ENV: &str = "UDB_EMBEDDING_MATRYOSHKA_STRATEGY";

/// Which configured Matryoshka truncation a model serves at when
/// `matryoshka_dims` is non-empty: `"largest"` (default — the highest-accuracy
/// cut that still truncates) or `"smallest"` (max storage/compute savings).
pub(crate) fn embedding_matryoshka_strategy() -> &'static str {
    static STRATEGY: OnceLock<String> = OnceLock::new();
    str_env_once(
        &STRATEGY,
        EMBEDDING_MATRYOSHKA_STRATEGY_ENV,
        DEFAULT_EMBEDDING_MATRYOSHKA_STRATEGY,
    )
}

/// The dimensionality a model actually serves at. `matryoshka_dims_json` is the
/// registry-stored JSON array of valid truncations; when it carries a positive
/// value `<= dimensions`, the `strategy` picks one (largest/smallest) and that
/// becomes the collection geometry + the sidecar `truncate_dim` + the Retrieve
/// query cut. An empty/absent/all-invalid list ⇒ full `dimensions` (the historic
/// behavior — zero change for non-Matryoshka models). Pure — unit-tested.
pub(crate) fn select_matryoshka_dim(
    dimensions: i32,
    matryoshka_dims_json: &str,
    strategy: &str,
) -> i32 {
    if dimensions <= 0 {
        return dimensions;
    }
    let dims: Vec<i32> = serde_json::from_str(matryoshka_dims_json.trim()).unwrap_or_default();
    let valid = dims
        .into_iter()
        .filter(|dim| *dim > 0 && *dim <= dimensions);
    let chosen = match strategy.trim().to_ascii_lowercase().as_str() {
        "smallest" => valid.min(),
        // "largest" (and any unrecognized strategy) → the most-accurate cut.
        _ => valid.max(),
    };
    chosen.unwrap_or(dimensions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matryoshka_empty_list_keeps_full_dimensions() {
        assert_eq!(select_matryoshka_dim(1024, "[]", "largest"), 1024);
        assert_eq!(select_matryoshka_dim(1024, "", "largest"), 1024);
        assert_eq!(select_matryoshka_dim(1024, "not json", "largest"), 1024);
    }

    #[test]
    fn matryoshka_largest_picks_highest_valid_cut() {
        assert_eq!(select_matryoshka_dim(1024, "[256,512,768]", "largest"), 768);
        // Values above the full dimensionality are ignored (never up-scale).
        assert_eq!(select_matryoshka_dim(768, "[256,768,4096]", "largest"), 768);
    }

    #[test]
    fn matryoshka_smallest_picks_lowest_valid_cut() {
        assert_eq!(
            select_matryoshka_dim(1024, "[256,512,768]", "smallest"),
            256
        );
        // Non-positive entries are rejected before selection.
        assert_eq!(select_matryoshka_dim(1024, "[0,-8,384]", "smallest"), 384);
    }

    #[test]
    fn matryoshka_unknown_strategy_defaults_to_largest() {
        assert_eq!(
            select_matryoshka_dim(1024, "[256,512]", "banana"),
            512,
            "an unrecognized strategy must fall back to the largest cut"
        );
    }

    #[test]
    fn matryoshka_all_invalid_falls_back_to_full() {
        assert_eq!(select_matryoshka_dim(512, "[0,-1,9999]", "largest"), 512);
    }
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

pub(crate) fn embedding_work_visibility_timeout() -> Duration {
    static TIMEOUT: OnceLock<Duration> = OnceLock::new();
    *TIMEOUT.get_or_init(|| {
        Duration::from_secs(
            std::env::var(WORK_VISIBILITY_TIMEOUT_ENV)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_WORK_VISIBILITY_TIMEOUT_SECS),
        )
    })
}

pub(crate) fn embedding_retry_sweep_limit() -> i64 {
    static LIMIT: OnceLock<i64> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var(RETRY_SWEEP_LIMIT_ENV)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_RETRY_SWEEP_LIMIT)
    })
}

pub(crate) fn retrieval_eval_sample_rate() -> f64 {
    static RATE: OnceLock<f64> = OnceLock::new();
    *RATE.get_or_init(|| {
        std::env::var(RETRIEVAL_EVAL_SAMPLE_RATE_ENV)
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
            .unwrap_or(0.0)
    })
}

pub(crate) fn embedding_fresh_buffer_ttl() -> Duration {
    static TTL: OnceLock<Duration> = OnceLock::new();
    *TTL.get_or_init(|| {
        Duration::from_secs(
            std::env::var(FRESH_BUFFER_TTL_ENV)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_FRESH_BUFFER_TTL_SECS)
                .min(300),
        )
    })
}

pub(crate) fn embedding_fresh_buffer_capacity() -> usize {
    static CAPACITY: OnceLock<usize> = OnceLock::new();
    *CAPACITY.get_or_init(|| {
        std::env::var(FRESH_BUFFER_CAPACITY_ENV)
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_FRESH_BUFFER_CAPACITY)
            .min(100_000)
    })
}
