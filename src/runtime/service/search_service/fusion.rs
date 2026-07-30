//! Pure reciprocal-rank fusion for the native `SearchService` — the isolated
//! cross-index rank-fusion functions ("one search box over everything"). The
//! `score = Σ w_i · 1/(k + rank)` definition and the deterministic tie-break are
//! unit-tested; `k` is the env-tunable [`super::config::search_rrf_k`] (default
//! 60, the canonical Cormack et al. value) and the optional per-list weights come
//! from the `UDB_SEARCH_FUSION_WEIGHTS` knob (empty → uniform, unchanged).

use super::config::search_rrf_k;

/// Pure reciprocal-rank fusion: `score(doc) = Σ_lists 1/(k + rank)`, where `k` is
/// the env-tunable [`search_rrf_k`] (default 60) and `rank` is the doc's 0-based
/// position in each ranked list it appears in. Used to fuse the per-index ranked
/// id lists when a Search spans several of the tenant's indexes ("one search box
/// over everything"). Returns `(id, score)` sorted by score descending then id
/// ascending for determinism. Reuses the same `1/(k + rank)` definition Qdrant
/// applies internally for single-collection hybrid search, so cross-index fusion
/// is consistent with intra-index fusion.
pub(crate) fn reciprocal_rank_fusion(ranked_lists: &[Vec<String>]) -> Vec<(String, f64)> {
    // Uniform weights: identical to the weighted fusion with every list at 1.0.
    weighted_reciprocal_rank_fusion(ranked_lists, &[])
}

/// Weighted reciprocal-rank fusion: `score(doc) = Σ_lists w_i · 1/(k + rank)`,
/// where `w_i` is `weights[i]` (per-index / per-modality) or `1.0` when the list
/// has no configured weight — so an empty `weights` reproduces the uniform
/// [`reciprocal_rank_fusion`] byte-for-byte. A non-finite/negative weight also
/// falls back to `1.0`. Deterministic tie-break (score desc, id asc) is preserved.
fn weighted_reciprocal_rank_fusion(
    ranked_lists: &[Vec<String>],
    weights: &[f32],
) -> Vec<(String, f64)> {
    use std::collections::HashMap;
    let k = search_rrf_k();
    let mut scores: HashMap<String, f64> = HashMap::new();
    for (list_idx, list) in ranked_lists.iter().enumerate() {
        let weight = weights
            .get(list_idx)
            .copied()
            .map(f64::from)
            .filter(|w| w.is_finite() && *w >= 0.0)
            .unwrap_or(1.0);
        for (rank, id) in list.iter().enumerate() {
            *scores.entry(id.clone()).or_insert(0.0) += weight * (1.0 / (k + rank as f64));
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

/// Fuse per-index ranked lists that carry the engine's own relevance score, with
/// optional per-index fusion weights (`weights[i]`, or `1.0` when unset), driven
/// by the operator `UDB_SEARCH_FUSION_WEIGHTS` knob.
///
/// - **Single index** (the common case): the engine's cosine/relevance scores and
///   order are preserved verbatim, regardless of any configured weights — one
///   index has no cross-index rank to fuse. Previously every result — even a
///   single-index search — had its score overwritten by the positional RRF value
///   `1/(60+rank)`, so `SearchHit.score` was never the real similarity a client
///   could threshold or display.
/// - **Multiple indexes:** raw scores from different indexes/metrics are not
///   comparable, so fall back to (weighted) reciprocal-rank fusion over positions.
///   An empty `weights` reproduces the uniform [`reciprocal_rank_fusion`].
pub(crate) fn fuse_ranked_lists_weighted(
    ranked_lists: &[Vec<(String, f64)>],
    weights: &[f32],
) -> Vec<(String, f64)> {
    if ranked_lists.len() == 1 {
        return ranked_lists[0].clone();
    }
    let id_lists: Vec<Vec<String>> = ranked_lists
        .iter()
        .map(|list| list.iter().map(|(id, _score)| id.clone()).collect())
        .collect();
    if weights.is_empty() {
        reciprocal_rank_fusion(&id_lists)
    } else {
        weighted_reciprocal_rank_fusion(&id_lists, weights)
    }
}

#[cfg(test)]
mod fuse_tests {
    use super::{fuse_ranked_lists_weighted, reciprocal_rank_fusion};

    #[test]
    fn single_index_preserves_engine_scores_and_order() {
        let lists = vec![vec![("a".to_string(), 0.91), ("b".to_string(), 0.42)]];
        assert_eq!(
            fuse_ranked_lists_weighted(&lists, &[]),
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
        let fused = fuse_ranked_lists_weighted(&lists, &[]);
        assert_eq!(fused[0].0, "a");
        // Score is the RRF value 1/(60+0) + 1/(60+0), not an engine score.
        assert!((fused[0].1 - (2.0 / 60.0)).abs() < 1e-9);
    }

    #[test]
    fn empty_weights_equal_uniform_fusion() {
        // A weighted fusion with no configured weights routes through the uniform
        // `reciprocal_rank_fusion` (every list at 1.0) — same fused ranking.
        let id_lists = vec![
            vec!["a".to_string(), "b".to_string()],
            vec!["b".to_string(), "c".to_string()],
        ];
        let scored = vec![
            vec![("a".to_string(), 0.9), ("b".to_string(), 0.1)],
            vec![("b".to_string(), 0.5), ("c".to_string(), 0.4)],
        ];
        assert_eq!(
            fuse_ranked_lists_weighted(&scored, &[]),
            reciprocal_rank_fusion(&id_lists)
        );
    }

    #[test]
    fn single_index_ignores_weights_and_keeps_engine_scores() {
        let lists = vec![vec![("a".to_string(), 0.91), ("b".to_string(), 0.42)]];
        assert_eq!(
            fuse_ranked_lists_weighted(&lists, &[5.0]),
            vec![("a".to_string(), 0.91), ("b".to_string(), 0.42)],
            "a single index keeps engine scores regardless of configured weights"
        );
    }

    #[test]
    fn weights_scale_per_list_rrf_contribution() {
        // "a" is rank 0 in list 0 only; "b" is rank 0 in list 1 only. With list 0
        // weighted 3.0 and list 1 weighted 1.0, a's contribution (3/60) beats b's
        // (1/60) even though both are top-ranked in their own list.
        let lists = vec![vec![("a".to_string(), 0.9)], vec![("b".to_string(), 0.9)]];
        let fused = fuse_ranked_lists_weighted(&lists, &[3.0, 1.0]);
        assert_eq!(fused[0].0, "a", "the heavier list's top doc must win");
        assert!((fused[0].1 - (3.0 / 60.0)).abs() < 1e-9);
        assert_eq!(fused[1].0, "b");
        assert!((fused[1].1 - (1.0 / 60.0)).abs() < 1e-9);
    }

    #[test]
    fn missing_or_bad_weight_falls_back_to_one() {
        // Only the first list has a weight; the second (unweighted) defaults to
        // 1.0 — a NEGATIVE weight also falls back to 1.0.
        let lists = vec![vec![("a".to_string(), 0.9)], vec![("b".to_string(), 0.9)]];
        let fused = fuse_ranked_lists_weighted(&lists, &[-2.0]);
        // Both fall back to 1.0 → tied score, id-ascending tie-break → a, b.
        assert!((fused[0].1 - (1.0 / 60.0)).abs() < 1e-9);
        assert!((fused[1].1 - (1.0 / 60.0)).abs() < 1e-9);
        assert_eq!(fused[0].0, "a");
        assert_eq!(fused[1].0, "b");
    }
}
