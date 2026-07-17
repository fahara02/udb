//! Pure reciprocal-rank fusion for the native `SearchService` — the isolated
//! cross-index rank-fusion function ("one search box over everything") and its
//! canonical constant. Extracted verbatim from the former god file; the
//! `score = Σ 1/(60 + rank)` definition and the deterministic tie-break are
//! byte-for-byte identical and unit-tested.

/// Reciprocal-rank-fusion constant. The canonical `k` from the original RRF
/// paper (Cormack et al.); larger values flatten the contribution of top ranks.
const RRF_K: f64 = 60.0;

/// Pure reciprocal-rank fusion: `score(doc) = Σ_lists 1/(RRF_K + rank)`, where
/// `rank` is the doc's 0-based position in each ranked list it appears in. Used
/// to fuse the per-index ranked id lists when a Search spans several of the
/// tenant's indexes ("one search box over everything"). Returns `(id, score)`
/// sorted by score descending then id ascending for determinism. Reuses the same
/// `1/(60 + rank)` definition Qdrant applies internally for single-collection
/// hybrid search, so cross-index fusion is consistent with intra-index fusion.
pub(crate) fn reciprocal_rank_fusion(ranked_lists: &[Vec<String>]) -> Vec<(String, f64)> {
    use std::collections::HashMap;
    let mut scores: HashMap<String, f64> = HashMap::new();
    for list in ranked_lists {
        for (rank, id) in list.iter().enumerate() {
            *scores.entry(id.clone()).or_insert(0.0) += 1.0 / (RRF_K + rank as f64);
        }
    }
    let mut fused: Vec<(String, f64)> = scores.into_iter().collect();
    fused.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    fused
}
