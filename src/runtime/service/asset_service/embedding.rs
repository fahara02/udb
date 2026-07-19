//! The shared vector-point helpers for the native `AssetService`: the best-effort
//! EMBED-step upsert and the failure-path delete, both riding the SAME mediated
//! `vector_upsert_backend_target` / `vector_delete_backend_target` seam the
//! `EmbeddingService` uses (never a hand-built engine call). Extracted verbatim
//! from the former god file; they stay inherent methods on `AssetServiceImpl`
//! (they use `self.runtime` + `self.vector_collection`).

use super::AssetServiceImpl;

/// Where a completed EMBED step's vector landed, so a later failure can delete it
/// from the exact same backend instance + project it was upserted into.
#[derive(Debug, Clone)]
pub(crate) struct VectorEmbeddingTarget {
    pub(crate) project_id: String,
    pub(crate) instance: String,
}

impl AssetServiceImpl {
    /// Best-effort: push a completed EMBED step's vector into the vector backend.
    /// `point_id` is the asset id; the embedding + dim come from the step result.
    /// Never fails the pipeline — a vector-backend outage just logs.
    ///
    /// The point payload carries `_tenant_id`/`_project_id` stamps: the shared
    /// Qdrant seam enforces tenant-partitioned HNSW (`m=0` + a `_tenant_id`
    /// tenant index), so an unstamped point would belong to no tenant partition
    /// and be invisible to the tenant-filtered search path (which ANDs the same
    /// stamps into must-filters).
    pub(crate) async fn upsert_embedding(
        &self,
        tenant_id: &str,
        project_id: &str,
        point_id: &str,
        result: &serde_json::Value,
    ) -> Option<VectorEmbeddingTarget> {
        let Some(runtime) = self.runtime.as_ref() else {
            return None;
        };
        let Some(arr) = result.get("embedding").and_then(|e| e.as_array()) else {
            return None;
        };
        let vector: Vec<f32> = arr
            .iter()
            .filter_map(|v| v.as_f64().map(|f| f as f32))
            .collect();
        if vector.is_empty() {
            return None;
        }
        let dim = result
            .get("dim")
            .and_then(|d| d.as_i64())
            .unwrap_or(vector.len() as i64) as i32;
        let mut stamps = serde_json::Map::new();
        if !tenant_id.trim().is_empty() {
            stamps.insert(
                "_tenant_id".to_string(),
                serde_json::Value::String(tenant_id.trim().to_string()),
            );
        }
        if !project_id.trim().is_empty() {
            stamps.insert(
                "_project_id".to_string(),
                serde_json::Value::String(project_id.trim().to_string()),
            );
        }
        let payload =
            crate::runtime::executor_utils::json_to_struct(&serde_json::Value::Object(stamps));
        let point = crate::proto::VectorPointMutation {
            id: point_id.to_string(),
            vector,
            payload,
            vector_name: String::new(),
        };
        let vector_instance = runtime
            .choose_instance_name_for_project("qdrant", true, project_id)
            .map(str::to_string)
            .unwrap_or_else(|| "default".to_string());
        if let Err(err) = runtime
            .vector_upsert_backend_target(
                Some(&vector_instance),
                project_id,
                &self.vector_collection,
                dim,
                vec![point],
            )
            .await
        {
            tracing::warn!(error = %err, collection = %self.vector_collection, point_id, "asset embedding vector upsert failed");
            None
        } else {
            Some(VectorEmbeddingTarget {
                project_id: project_id.to_string(),
                instance: vector_instance,
            })
        }
    }

    /// Best-effort: remove an asset's embedding (point id = asset_id) from the
    /// vector backend. Called on pipeline failure so a failed run leaves no orphan
    /// vector. Never fails the caller.
    pub(crate) async fn delete_embedding(
        &self,
        project_id: &str,
        vector_instance: Option<&str>,
        point_id: &str,
    ) {
        let Some(runtime) = self.runtime.as_ref() else {
            return;
        };
        if point_id.trim().is_empty() {
            return;
        }
        if let Err(err) = runtime
            .vector_delete_backend_target(
                vector_instance,
                project_id,
                &self.vector_collection,
                vec![point_id.to_string()],
            )
            .await
        {
            tracing::warn!(error = %err, collection = %self.vector_collection, point_id, "asset embedding vector delete failed");
        }
    }
}
