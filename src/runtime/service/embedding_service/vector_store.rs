use std::sync::Arc;

use crate::proto::{
    VectorHybridSearchRequest, VectorPointMutation, VectorSearchRequest, VectorSet,
};
use crate::runtime::DataBrokerRuntime;

use super::model::StoredModel;

#[async_trait::async_trait]
pub(crate) trait VectorStore: Send + Sync {
    async fn ensure_collection(
        &self,
        collection: &str,
        dimensions: i32,
        distance: &str,
        output_dtype: &str,
        vector_names: &[String],
    ) -> Result<(), tonic::Status>;
    async fn upsert(
        &self,
        collection: &str,
        dimensions: i32,
        distance: &str,
        output_dtype: &str,
        points: Vec<VectorPointMutation>,
    ) -> Result<(), tonic::Status>;
    async fn search(&self, request: &VectorSearchRequest) -> Result<VectorSet, tonic::Status>;
    async fn hybrid_search(
        &self,
        request: &VectorHybridSearchRequest,
    ) -> Result<VectorSet, tonic::Status>;
    async fn delete_points(
        &self,
        collection: &str,
        point_ids: Vec<String>,
    ) -> Result<(), tonic::Status>;
    async fn delete_by_filter(
        &self,
        collection: &str,
        filter: serde_json::Value,
    ) -> Result<(), tonic::Status>;
    async fn swap_alias(&self, alias: &str, collection: &str) -> Result<(), tonic::Status>;
}

pub(crate) struct RuntimeVectorStore {
    runtime: Arc<DataBrokerRuntime>,
    backend: String,
    instance: String,
    project_id: String,
}

impl RuntimeVectorStore {
    pub(crate) fn for_routing(
        runtime: Arc<DataBrokerRuntime>,
        project_id: &str,
        backend: &str,
        instance: &str,
    ) -> Self {
        Self {
            runtime,
            backend: backend.trim().to_ascii_lowercase(),
            instance: instance.trim().to_string(),
            project_id: project_id.to_string(),
        }
    }

    pub(crate) fn for_model(
        runtime: Arc<DataBrokerRuntime>,
        project_id: &str,
        model: &StoredModel,
    ) -> Self {
        Self::for_routing(
            runtime,
            project_id,
            &model.vector_backend,
            &model.vector_instance,
        )
    }

    fn instance(&self) -> Option<&str> {
        (!self.instance.is_empty()).then_some(self.instance.as_str())
    }

    fn qdrant_only(&self, operation: &'static str) -> Result<(), tonic::Status> {
        if self.backend == "qdrant" {
            return Ok(());
        }
        Err(crate::runtime::executor_utils::capability_status(
            "embedding",
            operation,
            "vector_backend_filtered_delete",
            format!(
                "vector backend '{}' does not expose the required filtered-delete/alias capability",
                self.backend
            ),
        ))
    }
}

#[async_trait::async_trait]
impl VectorStore for RuntimeVectorStore {
    async fn ensure_collection(
        &self,
        collection: &str,
        dimensions: i32,
        distance: &str,
        output_dtype: &str,
        vector_names: &[String],
    ) -> Result<(), tonic::Status> {
        self.runtime
            .vector_ensure_backend_kind_target(
                &self.backend,
                self.instance(),
                &self.project_id,
                collection,
                dimensions,
                distance,
                output_dtype,
                vector_names,
            )
            .await
    }

    async fn upsert(
        &self,
        collection: &str,
        dimensions: i32,
        distance: &str,
        output_dtype: &str,
        points: Vec<VectorPointMutation>,
    ) -> Result<(), tonic::Status> {
        let mut vector_names = points
            .iter()
            .map(|point| point.vector_name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        vector_names.sort_unstable();
        vector_names.dedup();
        if !vector_names.is_empty() {
            self.ensure_collection(
                collection,
                dimensions,
                distance,
                output_dtype,
                &vector_names,
            )
            .await?;
        }
        self.runtime
            .vector_upsert_existing_backend_kind_target(
                &self.backend,
                self.instance(),
                &self.project_id,
                collection,
                points,
            )
            .await
    }

    async fn search(&self, request: &VectorSearchRequest) -> Result<VectorSet, tonic::Status> {
        self.runtime
            .vector_search_backend_kind_target(
                &self.backend,
                self.instance(),
                &self.project_id,
                request,
            )
            .await
    }

    async fn hybrid_search(
        &self,
        request: &VectorHybridSearchRequest,
    ) -> Result<VectorSet, tonic::Status> {
        self.runtime
            .vector_hybrid_backend_kind_target(
                &self.backend,
                self.instance(),
                &self.project_id,
                request,
            )
            .await
    }

    async fn delete_points(
        &self,
        collection: &str,
        point_ids: Vec<String>,
    ) -> Result<(), tonic::Status> {
        self.qdrant_only("embedding_vector_delete")?;
        self.runtime
            .vector_delete_backend_target(self.instance(), &self.project_id, collection, point_ids)
            .await
    }

    async fn delete_by_filter(
        &self,
        collection: &str,
        filter: serde_json::Value,
    ) -> Result<(), tonic::Status> {
        self.qdrant_only("embedding_vector_delete_by_filter")?;
        self.runtime
            .vector_delete_by_filter_backend_target(
                self.instance(),
                &self.project_id,
                collection,
                filter,
            )
            .await
    }

    async fn swap_alias(&self, alias: &str, collection: &str) -> Result<(), tonic::Status> {
        self.qdrant_only("embedding_vector_alias_cutover")?;
        self.runtime
            .vector_swap_alias_backend_target(self.instance(), &self.project_id, alias, collection)
            .await
    }
}
