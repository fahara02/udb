use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::proto::VectorPoint;

struct FreshEntry {
    source_name: String,
    model_id: String,
    inserted_at: Instant,
    point: VectorPoint,
}

/// Bounded process-local read-your-writes buffer. The vector database remains
/// authoritative; this only bridges its indexing lag and expires aggressively.
///
/// Sharded PER TENANT (F6 fairness): a write-heavy tenant can only evict its
/// OWN oldest entries, never another tenant's read-your-writes rows. Each shard
/// is independently bounded to `capacity` and TTL-pruned; an emptied shard is
/// dropped so idle tenants cost nothing.
pub(crate) struct FreshVectorBuffer {
    shards: Mutex<HashMap<String, VecDeque<FreshEntry>>>,
    ttl: Duration,
    capacity: usize,
}

impl FreshVectorBuffer {
    pub(crate) fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            shards: Mutex::new(HashMap::new()),
            ttl,
            capacity: capacity.max(1),
        }
    }

    pub(crate) fn insert(
        &self,
        tenant_id: &str,
        source_name: &str,
        model_id: &str,
        point: VectorPoint,
    ) {
        let Ok(mut shards) = self.shards.lock() else {
            return;
        };
        let now = Instant::now();
        let shard = shards.entry(tenant_id.to_string()).or_default();
        prune(shard, now, self.ttl);
        shard.retain(|entry| {
            !(entry.source_name == source_name
                && entry.model_id == model_id
                && entry.point.id == point.id
                && entry.point.vector_name == point.vector_name)
        });
        shard.push_back(FreshEntry {
            source_name: source_name.to_string(),
            model_id: model_id.to_string(),
            inserted_at: now,
            point,
        });
        // Bound THIS tenant's shard only — never another tenant's entries.
        while shard.len() > self.capacity {
            shard.pop_front();
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn search(
        &self,
        tenant_id: &str,
        source_name: &str,
        model_id: &str,
        vector_name: &str,
        query: &[f32],
        distance_metric: &str,
        limit: usize,
    ) -> Vec<VectorPoint> {
        let Ok(mut shards) = self.shards.lock() else {
            return Vec::new();
        };
        let Some(shard) = shards.get_mut(tenant_id) else {
            return Vec::new();
        };
        prune(shard, Instant::now(), self.ttl);
        let mut seen = BTreeSet::new();
        let mut points = shard
            .iter()
            .rev()
            .filter(|entry| {
                entry.source_name == source_name
                    && entry.model_id == model_id
                    && entry.point.vector_name == vector_name
                    && entry.point.vector.len() == query.len()
                    && seen.insert(entry.point.id.clone())
            })
            .map(|entry| {
                let mut point = entry.point.clone();
                point.score = similarity(distance_metric, query, &point.vector);
                point
            })
            .collect::<Vec<_>>();
        // Drop an emptied shard (all entries TTL-expired) so idle tenants do not
        // accumulate stale map keys. `is_empty()` is the last use of the shard
        // borrow, so removing the key afterwards is sound.
        let drop_shard = shard.is_empty();
        if drop_shard {
            shards.remove(tenant_id);
        }
        points.sort_by(|left, right| right.score.total_cmp(&left.score));
        points.truncate(limit);
        points
    }
}

impl Default for FreshVectorBuffer {
    fn default() -> Self {
        Self::new(
            super::config::embedding_fresh_buffer_ttl(),
            super::config::embedding_fresh_buffer_capacity(),
        )
    }
}

fn prune(entries: &mut VecDeque<FreshEntry>, now: Instant, ttl: Duration) {
    while entries
        .front()
        .is_some_and(|entry| now.duration_since(entry.inserted_at) >= ttl)
    {
        entries.pop_front();
    }
}

/// The ONE metric-aware similarity used across the embedding service (the fresh
/// read-your-writes buffer here and the MMR diversity re-ranker in
/// `retrieval.rs`), so the two never drift apart (they previously carried two
/// separate cosine implementations).
///
/// A HIGHER value ALWAYS means "more similar", for EVERY supported metric:
/// - `COSINE` (default / unknown): cosine similarity in `[-1, 1]`.
/// - `DOT`: raw dot product.
/// - `EUCLID`: mapped to `1 / (1 + distance)` in `(0, 1]`, so a SMALLER L2
///   distance yields a LARGER score. Keeping EUCLID higher-better is what lets
///   the fresh-buffer scores merge and sort DESC consistently with COSINE/DOT
///   in `retrieval.rs` (this is the F3 EUCLID fresh-buffer ranking fix). See the
///   `retrieval.rs` merge-site NOTE for the residual engine-convention question.
///
/// Empty / mismatched-length inputs score `0.0` (defensive; callers already
/// length-match, but the guard keeps the helper total).
pub(crate) fn similarity(metric: &str, a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    if metric.eq_ignore_ascii_case("DOT") {
        return a.iter().zip(b).map(|(left, right)| left * right).sum();
    }
    if metric.eq_ignore_ascii_case("EUCLID") {
        let distance = a
            .iter()
            .zip(b)
            .map(|(left, right)| (left - right).powi(2))
            .sum::<f32>()
            .sqrt();
        return 1.0 / (1.0 + distance);
    }
    let dot = a
        .iter()
        .zip(b)
        .map(|(left, right)| left * right)
        .sum::<f32>();
    let a_norm = a.iter().map(|value| value * value).sum::<f32>().sqrt();
    let b_norm = b.iter().map(|value| value * value).sum::<f32>().sqrt();
    if a_norm == 0.0 || b_norm == 0.0 {
        0.0
    } else {
        dot / (a_norm * b_norm)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(id: &str, vector: Vec<f32>) -> VectorPoint {
        VectorPoint {
            id: id.to_string(),
            score: 0.0,
            payload: None,
            vector,
            vector_name: String::new(),
        }
    }

    #[test]
    fn fresh_search_is_tenant_model_scoped_and_deduplicated() {
        let buffer = FreshVectorBuffer::new(Duration::from_secs(30), 8);
        buffer.insert("t1", "docs", "m1", point("a", vec![1.0, 0.0]));
        buffer.insert("t2", "docs", "m1", point("foreign", vec![1.0, 0.0]));
        buffer.insert("t1", "docs", "m1", point("a", vec![0.0, 1.0]));
        let hits = buffer.search("t1", "docs", "m1", "", &[0.0, 1.0], "COSINE", 10);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "a");
        assert_eq!(hits[0].score, 1.0);
    }

    /// F6 fairness regression: one tenant flooding the buffer must NOT evict a
    /// different tenant's read-your-writes entry, and each shard stays bounded.
    #[test]
    fn per_tenant_shard_isolates_eviction() {
        // Capacity 2 PER TENANT. t1 writes one entry, then t2 floods 50 writes.
        let buffer = FreshVectorBuffer::new(Duration::from_secs(30), 2);
        buffer.insert("t1", "docs", "m1", point("keep", vec![1.0, 0.0]));
        for index in 0..50 {
            buffer.insert(
                "t2",
                "docs",
                "m1",
                point(&format!("flood-{index}"), vec![0.0, 1.0]),
            );
        }
        // t1's entry survives the t2 flood (no cross-tenant eviction).
        let t1 = buffer.search("t1", "docs", "m1", "", &[1.0, 0.0], "COSINE", 10);
        assert_eq!(t1.len(), 1);
        assert_eq!(t1[0].id, "keep");
        // t2's shard is capped at 2 and holds ONLY t2's own (newest) entries.
        let t2 = buffer.search("t2", "docs", "m1", "", &[0.0, 1.0], "COSINE", 10);
        assert_eq!(t2.len(), 2);
        assert!(t2.iter().all(|hit| hit.id.starts_with("flood-")));
    }

    #[test]
    fn similarity_is_higher_better_for_every_metric() {
        // COSINE (and unknown/empty metric ⇒ cosine default): identical
        // direction ⇒ 1.0, orthogonal ⇒ 0.0.
        assert!((similarity("COSINE", &[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-6);
        assert!(similarity("COSINE", &[1.0, 0.0], &[0.0, 1.0]).abs() < 1e-6);
        assert!((similarity("", &[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-6);

        // DOT: raw dot product; a larger-magnitude aligned vector scores higher.
        assert!((similarity("DOT", &[2.0, 3.0], &[4.0, 5.0]) - 23.0).abs() < 1e-6);
        assert!(
            similarity("dot", &[1.0, 1.0], &[2.0, 2.0])
                > similarity("dot", &[1.0, 1.0], &[1.0, 1.0])
        );

        // EUCLID: mapped higher-better — zero distance ⇒ 1.0, and a SMALLER
        // distance ranks ABOVE a larger one (the F3 fix).
        assert!((similarity("EUCLID", &[1.0, 2.0], &[1.0, 2.0]) - 1.0).abs() < 1e-6);
        let near = similarity("EUCLID", &[0.0, 0.0], &[1.0, 0.0]); // dist 1 ⇒ 0.5
        let far = similarity("EUCLID", &[0.0, 0.0], &[3.0, 0.0]); // dist 3 ⇒ 0.25
        assert!(
            near > far,
            "EUCLID must be higher-better (nearer ranks first)"
        );
        assert!((near - 0.5).abs() < 1e-6);

        // Defensive guard: empty / length-mismatched inputs ⇒ 0.0.
        assert_eq!(similarity("COSINE", &[], &[]), 0.0);
        assert_eq!(similarity("DOT", &[1.0], &[1.0, 2.0]), 0.0);
        assert_eq!(similarity("EUCLID", &[1.0, 2.0], &[1.0]), 0.0);
    }
}
