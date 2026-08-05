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
/// Reserved point-payload key carrying the RAW source primary key. The stored
/// Qdrant point id is a SHA-256 hash of the tenant-scoped `"{tenant}:{pk}"` id
/// (Qdrant point ids must be UUID/int, so `qdrant_point_id` hashes anything
/// else), which makes the raw pk UNRECOVERABLE from the returned id. The search
/// READ path returns this payload value as the hit id, and strips it from the
/// returned payload just like the tenant-scope key.
pub(crate) const SOURCE_PK_PAYLOAD_KEY: &str = "_source_pk";
/// Vector distance metric provisioned for a new engine collection (cosine —
/// the same default the shared `vector_upsert_backend_target` seam ensures).
/// Now only the *fallback* when a per-index `metadata_json.distance` is absent
/// ([`index_distance_metric`]).
pub(crate) const ENGINE_VECTOR_DISTANCE: &str = "cosine";

/// Canonical reciprocal-rank-fusion `k` from the original RRF paper (Cormack et
/// al.); larger values flatten the contribution of top ranks. Now the *default*
/// for the env-tunable [`search_rrf_k`].
const DEFAULT_SEARCH_RRF_K: f64 = 60.0;
const SEARCH_RRF_K_ENV: &str = "UDB_SEARCH_RRF_K";
const SEARCH_FUSION_WEIGHTS_ENV: &str = "UDB_SEARCH_FUSION_WEIGHTS";

/// Operator-tunable reciprocal-rank-fusion `k` (env `UDB_SEARCH_RRF_K`), resolved
/// ONCE. A non-positive/non-finite/unparsable override keeps the canonical
/// [`DEFAULT_SEARCH_RRF_K`] (60) so the constant stays byte-stable by default.
pub(crate) fn search_rrf_k() -> f64 {
    static V: OnceLock<f64> = OnceLock::new();
    *V.get_or_init(|| parse_rrf_k(std::env::var(SEARCH_RRF_K_ENV).ok().as_deref()))
}

/// Pure parse of the `UDB_SEARCH_RRF_K` value: a finite, strictly-positive `f64`
/// or the canonical default. Split out so the knob is unit-tested without the
/// process-wide `OnceLock`/env.
fn parse_rrf_k(raw: Option<&str>) -> f64 {
    raw.and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(DEFAULT_SEARCH_RRF_K)
}

/// Per-index / per-modality fusion weights (env `UDB_SEARCH_FUSION_WEIGHTS`, a
/// comma-separated list), resolved ONCE. Mirrors
/// `embedding_service::config::retrieve_fusion_weights`: empty (the default, and
/// the fallback for any malformed/negative entry) hands weighting back to the
/// engine-default fusion — identical to the previous hardcoded `Vec::new()`.
pub(crate) fn search_fusion_weights() -> Vec<f32> {
    static WEIGHTS: OnceLock<Vec<f32>> = OnceLock::new();
    WEIGHTS
        .get_or_init(|| {
            parse_fusion_weights(std::env::var(SEARCH_FUSION_WEIGHTS_ENV).ok().as_deref())
        })
        .clone()
}

/// Pure parse of the `UDB_SEARCH_FUSION_WEIGHTS` value. ALL entries must parse to
/// a finite, non-negative weight, else fall back to engine-default fusion (empty)
/// rather than apply a half-parsed weighting. Split out for unit tests.
fn parse_fusion_weights(raw: Option<&str>) -> Vec<f32> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    let parsed: Option<Vec<f32>> = raw
        .split(',')
        .map(|part| {
            part.trim()
                .parse::<f32>()
                .ok()
                .filter(|value| value.is_finite() && *value >= 0.0)
        })
        .collect();
    parsed
        .filter(|weights| !weights.is_empty())
        .unwrap_or_default()
}

/// Resolve a per-index vector **distance metric** from the stored index
/// `metadata_json` (`{"distance": "..."}`), falling back to
/// [`ENGINE_VECTOR_DISTANCE`] (cosine) when the key is absent/empty/unknown. The
/// returned token is normalized to the canonical set the shared vector seam
/// understands (`normalize_qdrant_distance` accepts these case-insensitively);
/// pure so provisioning is unit-tested without a runtime.
pub(crate) fn index_distance_metric(metadata_json: &str) -> &'static str {
    let raw = serde_json::from_str::<serde_json::Value>(metadata_json)
        .ok()
        .as_ref()
        .and_then(|value| value.get("distance"))
        .and_then(serde_json::Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    match raw.as_str() {
        "cosine" => "cosine",
        "dot" | "dotproduct" | "dot_product" | "ip" => "dot",
        "euclid" | "euclidean" | "l2" => "euclid",
        "manhattan" | "l1" => "manhattan",
        // Unknown/empty → the shared default (cosine).
        _ => ENGINE_VECTOR_DISTANCE,
    }
}

/// Resolve an optional per-index text **analyzer** from the stored index
/// `metadata_json` (`{"analyzer": "..."}`). `None` when absent or blank. Pure so
/// the metadata contract is unit-tested; the actual ES-mapping consumption lives
/// in the setup_data index-PUT dispatch (see the `TODO(leader)` in
/// `handlers::create_index`).
pub(crate) fn index_analyzer(metadata_json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(metadata_json)
        .ok()
        .as_ref()
        .and_then(|value| value.get("analyzer"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod knob_tests {
    use super::{
        DEFAULT_SEARCH_RRF_K, ENGINE_VECTOR_DISTANCE, index_analyzer, index_distance_metric,
        parse_fusion_weights, parse_rrf_k,
    };

    #[test]
    fn rrf_k_uses_default_when_absent_or_invalid() {
        assert_eq!(parse_rrf_k(None), DEFAULT_SEARCH_RRF_K);
        assert_eq!(parse_rrf_k(Some("")), DEFAULT_SEARCH_RRF_K);
        assert_eq!(parse_rrf_k(Some("not-a-number")), DEFAULT_SEARCH_RRF_K);
        assert_eq!(parse_rrf_k(Some("0")), DEFAULT_SEARCH_RRF_K);
        assert_eq!(parse_rrf_k(Some("-5")), DEFAULT_SEARCH_RRF_K);
        assert_eq!(parse_rrf_k(Some("nan")), DEFAULT_SEARCH_RRF_K);
    }

    #[test]
    fn rrf_k_honors_a_valid_override() {
        assert!((parse_rrf_k(Some("10")) - 10.0).abs() < 1e-12);
        assert!((parse_rrf_k(Some(" 42.5 ")) - 42.5).abs() < 1e-12);
    }

    #[test]
    fn fusion_weights_empty_by_default_or_on_any_bad_entry() {
        assert!(parse_fusion_weights(None).is_empty());
        assert!(parse_fusion_weights(Some("")).is_empty());
        // A single bad entry voids the whole weighting (no half-parsed vector).
        assert!(parse_fusion_weights(Some("0.7, oops")).is_empty());
        assert!(parse_fusion_weights(Some("0.7,-0.3")).is_empty());
    }

    #[test]
    fn fusion_weights_parses_a_valid_list() {
        assert_eq!(parse_fusion_weights(Some("0.7, 0.3")), vec![0.7f32, 0.3f32]);
        assert_eq!(
            parse_fusion_weights(Some("1,2,3")),
            vec![1.0f32, 2.0f32, 3.0f32]
        );
        assert_eq!(parse_fusion_weights(Some("0")), vec![0.0f32]);
    }

    #[test]
    fn distance_metric_defaults_to_cosine_when_absent_or_unknown() {
        assert_eq!(index_distance_metric(""), ENGINE_VECTOR_DISTANCE);
        assert_eq!(index_distance_metric("{}"), ENGINE_VECTOR_DISTANCE);
        assert_eq!(index_distance_metric("not json"), ENGINE_VECTOR_DISTANCE);
        assert_eq!(
            index_distance_metric(r#"{"distance":"weird"}"#),
            ENGINE_VECTOR_DISTANCE
        );
    }

    #[test]
    fn distance_metric_reads_and_normalizes_known_values() {
        assert_eq!(index_distance_metric(r#"{"distance":"Cosine"}"#), "cosine");
        assert_eq!(
            index_distance_metric(r#"{"distance":"EUCLIDEAN"}"#),
            "euclid"
        );
        assert_eq!(index_distance_metric(r#"{"distance":" l2 "}"#), "euclid");
        assert_eq!(
            index_distance_metric(r#"{"distance":"dot_product"}"#),
            "dot"
        );
        assert_eq!(
            index_distance_metric(r#"{"distance":"manhattan"}"#),
            "manhattan"
        );
    }

    #[test]
    fn analyzer_is_none_when_absent_and_some_when_present() {
        assert_eq!(index_analyzer(""), None);
        assert_eq!(index_analyzer("{}"), None);
        assert_eq!(index_analyzer(r#"{"analyzer":"  "}"#), None);
        assert_eq!(
            index_analyzer(r#"{"analyzer":"english"}"#),
            Some("english".to_string())
        );
        assert_eq!(
            index_analyzer(r#"{"analyzer":" standard "}"#),
            Some("standard".to_string())
        );
    }
}

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

/// Clamp a requested `top_k` into `[1, max_top_k()]`; non-positive → default.
pub(crate) fn resolve_top_k(requested: i32) -> i32 {
    if requested <= 0 {
        DEFAULT_TOP_K
    } else {
        requested.min(max_top_k())
    }
}

/// Resolve the max `top_k` / result-page bound from `UDB_SEARCH_MAX_TOP_K`,
/// falling back to [`MAX_TOP_K`]. Resolved once; a non-positive/unparsable value
/// keeps the default so the bound is always finite.
pub(crate) fn max_top_k() -> i32 {
    static V: OnceLock<i32> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("UDB_SEARCH_MAX_TOP_K")
            .ok()
            .and_then(|value| value.trim().parse::<i32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(MAX_TOP_K)
    })
}

/// Resolve the per-tenant index quota from `UDB_SEARCH_MAX_INDEXES_PER_TENANT`,
/// falling back to [`MAX_INDEXES_PER_TENANT`]. Resolved once.
pub(crate) fn max_indexes_per_tenant() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("UDB_SEARCH_MAX_INDEXES_PER_TENANT")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(MAX_INDEXES_PER_TENANT)
    })
}
