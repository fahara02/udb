use std::collections::{BTreeSet, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::proto::VectorPoint;

struct FreshEntry {
    tenant_id: String,
    source_name: String,
    model_id: String,
    inserted_at: Instant,
    point: VectorPoint,
}

/// Bounded process-local read-your-writes buffer. The vector database remains
/// authoritative; this only bridges its indexing lag and expires aggressively.
pub(crate) struct FreshVectorBuffer {
    entries: Mutex<VecDeque<FreshEntry>>,
    ttl: Duration,
    capacity: usize,
}

impl FreshVectorBuffer {
    pub(crate) fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            entries: Mutex::new(VecDeque::with_capacity(capacity.min(1024))),
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
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        let now = Instant::now();
        prune(&mut entries, now, self.ttl);
        entries.retain(|entry| {
            !(entry.tenant_id == tenant_id
                && entry.source_name == source_name
                && entry.model_id == model_id
                && entry.point.id == point.id
                && entry.point.vector_name == point.vector_name)
        });
        entries.push_back(FreshEntry {
            tenant_id: tenant_id.to_string(),
            source_name: source_name.to_string(),
            model_id: model_id.to_string(),
            inserted_at: now,
            point,
        });
        while entries.len() > self.capacity {
            entries.pop_front();
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
        let Ok(mut entries) = self.entries.lock() else {
            return Vec::new();
        };
        prune(&mut entries, Instant::now(), self.ttl);
        let mut seen = BTreeSet::new();
        let mut points = entries
            .iter()
            .rev()
            .filter(|entry| {
                entry.tenant_id == tenant_id
                    && entry.source_name == source_name
                    && entry.model_id == model_id
                    && entry.point.vector_name == vector_name
                    && entry.point.vector.len() == query.len()
                    && seen.insert(entry.point.id.clone())
            })
            .map(|entry| {
                let mut point = entry.point.clone();
                point.score = similarity(query, &point.vector, distance_metric);
                point
            })
            .collect::<Vec<_>>();
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

fn similarity(query: &[f32], vector: &[f32], distance_metric: &str) -> f32 {
    if distance_metric.eq_ignore_ascii_case("DOT") {
        return query
            .iter()
            .zip(vector)
            .map(|(left, right)| left * right)
            .sum();
    }
    if distance_metric.eq_ignore_ascii_case("EUCLID") {
        let distance = query
            .iter()
            .zip(vector)
            .map(|(left, right)| (left - right).powi(2))
            .sum::<f32>()
            .sqrt();
        return 1.0 / (1.0 + distance);
    }
    let dot = query
        .iter()
        .zip(vector)
        .map(|(left, right)| left * right)
        .sum::<f32>();
    let query_norm = query.iter().map(|value| value * value).sum::<f32>().sqrt();
    let vector_norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if query_norm == 0.0 || vector_norm == 0.0 {
        0.0
    } else {
        dot / (query_norm * vector_norm)
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
}
