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

/// Fuse per-index ranked lists that carry the engine's own relevance score.
///
/// - **Single index** (the common case): the engine's cosine/relevance scores and
///   order are preserved verbatim. Previously every result — even a single-index
///   search — had its score overwritten by the positional RRF value
///   `1/(60+rank)`, so `SearchHit.score` was never the real similarity a client
///   could threshold or display.
/// - **Multiple indexes:** raw scores from different indexes/metrics are not
///   comparable, so fall back to reciprocal-rank fusion over positions.
pub(crate) fn fuse_ranked_lists(ranked_lists: &[Vec<(String, f64)>]) -> Vec<(String, f64)> {
    if ranked_lists.len() == 1 {
        return ranked_lists[0].clone();
    }
    let id_lists: Vec<Vec<String>> = ranked_lists
        .iter()
        .map(|list| list.iter().map(|(id, _score)| id.clone()).collect())
        .collect();
    reciprocal_rank_fusion(&id_lists)
}

#[cfg(test)]
mod fuse_tests {
    use super::fuse_ranked_lists;

    #[test]
    fn single_index_preserves_engine_scores_and_order() {
        let lists = vec![vec![("a".to_string(), 0.91), ("b".to_string(), 0.42)]];
        assert_eq!(
            fuse_ranked_lists(&lists),
            vec![("a".to_string(), 0.91), ("b".to_string(), 0.42)],
            "a single index must keep the engine's cosine scores, not RRF positions"
        );
    }

    #[test]
    fn multi_index_falls_back_to_rrf() {
        // Two lists → positional RRF; a doc ranked #0 in both scores highest.
        let lists = vec![
            vec![("a".to_string(), 0.9), ("b".to_string(), 0.1)],
            vec![("a".to_string(), 0.5), ("c".to_string(), 0.4)],
        ];
        let fused = fuse_ranked_lists(&lists);
        assert_eq!(fused[0].0, "a");
        // Score is the RRF value 1/(60+0) + 1/(60+0), not an engine score.
        assert!((fused[0].1 - (2.0 / 60.0)).abs() < 1e-9);
    }
}
