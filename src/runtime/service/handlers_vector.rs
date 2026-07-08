//! service.rs split — vector RPC handlers (Phase G).
use super::*;

fn vector_hybrid_search_unsupported_status() -> Status {
    crate::runtime::executor_utils::capability_status(
        "qdrant",
        "VectorHybridSearch",
        "hybrid_search",
        "backend qdrant does not support hybrid_search",
    )
}

impl DataBrokerService {
    pub(crate) async fn vector_search_inner(
        &self,
        request: Request<VectorSearchRequest>,
    ) -> Result<Response<VectorSet>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("VectorSearch", started, Err(e)),
        };
        let request = request.into_inner();
        if let Err(err) = self
            .authorize(&security, &request.collection, "VectorSearch")
            .await
        {
            return self.record_grpc("VectorSearch", started, Err(err));
        }
        self.metrics.inc_vector_op(&request.collection, "search");
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context();
        let execution_context = metadata_context.clone();
        let response_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Vector,
                Some(&metadata_context),
                Some("qdrant"),
                || async move {
                    runtime
                        .vector_search(manifest, request, execution_context)
                        .await
                },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "VectorSearch",
                started,
                Ok(self.with_catalog_response_headers(Response::new(res), &response_context)),
            ),
            Err(err) => self.record_grpc("VectorSearch", started, Err(err)),
        }
    }

    pub(crate) async fn vector_hybrid_search_inner(
        &self,
        request: Request<VectorHybridSearchRequest>,
    ) -> Result<Response<VectorSet>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("VectorHybridSearch", started, Err(e)),
        };
        let request = request.into_inner();
        if let Err(err) = self
            .authorize(&security, &request.collection, "VectorHybridSearch")
            .await
        {
            return self.record_grpc("VectorHybridSearch", started, Err(err));
        }
        // Capability guard: hybrid search requires sparse + dense index support.
        {
            use crate::planning::backend::BackendKind;
            let cap = BackendKind::Qdrant.capabilities();
            if !cap.supports_hybrid_search {
                return self.record_grpc(
                    "VectorHybridSearch",
                    started,
                    Err(vector_hybrid_search_unsupported_status()),
                );
            }
        }
        self.metrics
            .inc_vector_op(&request.collection, "hybrid_search");
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context();
        let execution_context = metadata_context.clone();
        let response_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Vector,
                Some(&metadata_context),
                Some("qdrant"),
                || async move {
                    runtime
                        .vector_hybrid_search(manifest, request, execution_context)
                        .await
                },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "VectorHybridSearch",
                started,
                Ok(self.with_catalog_response_headers(Response::new(res), &response_context)),
            ),
            Err(err) => self.record_grpc("VectorHybridSearch", started, Err(err)),
        }
    }

    pub(crate) async fn vector_upsert_inner(
        &self,
        request: Request<VectorUpsertRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("VectorUpsert", started, Err(e)),
        };
        let request = request.into_inner();
        if let Err(err) = self
            .authorize(&security, &request.collection, "VectorUpsert")
            .await
        {
            return self.record_grpc("VectorUpsert", started, Err(err));
        }
        self.metrics.inc_vector_op(&request.collection, "upsert");
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context();
        let execution_context = metadata_context.clone();
        let response_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Vector,
                Some(&metadata_context),
                Some("qdrant"),
                || async move {
                    runtime
                        .vector_upsert(manifest, request, execution_context)
                        .await
                },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "VectorUpsert",
                started,
                Ok(self
                    .with_mutation_response_headers(res, &response_context)
                    .await),
            ),
            Err(err) => self.record_grpc("VectorUpsert", started, Err(err)),
        }
    }

    pub(crate) async fn vector_batch_upsert_inner(
        &self,
        request: Request<tonic::Streaming<VectorUpsertRequest>>,
    ) -> Result<Response<ResponseStream<MutationResponse>>, Status> {
        let (started, security) = authorized_call!(self, request, "VectorBatchUpsert");
        let metadata_context = security.request_context();
        let response_context = metadata_context.clone();
        // Arc::clone the active manifest into the stream instead of deep-copying
        // the whole CatalogManifest (matches the data batch handlers) — #137.
        let manifest = self
            .catalog
            .active_for(&security.project_id)
            .manifest
            .clone();
        let runtime = self.runtime_snapshot().clone();
        let mut stream = request.into_inner();
        let metrics = self.metrics.clone();
        let channels = self.runtime_snapshot().channels().clone();
        let security_for_stream = security.clone();
        // #112: per-item authorization — the batch grant covered only
        // "VectorBatchUpsert", not each streamed item's target collection.
        // Casbin-only per-item authz (see batch_select_inner): capture the
        // cloneable snapshot and decide each item via `casbin_authorize`.
        let abac_snapshot = self.current_abac_snapshot();
        let out = async_stream::try_stream! {
            while let Some(item) = stream.message().await? {
                metrics.inc_vector_op(&item.collection, "batch_upsert");
                // #112: authorize THIS item's collection + stamp its decision id.
                let item_decision_id = DataBrokerService::authorize_message_item(
                    &abac_snapshot,
                    &security_for_stream,
                    &item.collection,
                    "VectorUpsert",
                )
                .await?;
                let item_context =
                    security_for_stream.request_context_with_decision(&item_decision_id);
                // FIX-77: admission + inflight gauge + per-item deadline +
                // latency + timeout mapping all live in the shared helper.
                let val = super::native_helpers::execute_stream_batch_item(
                    &channels,
                    &metrics,
                    &metadata_context,
                    crate::runtime::channels::OperationChannel::Vector,
                    "qdrant",
                    runtime.vector_upsert(&manifest, item, item_context),
                )
                .await?;
                yield val;
            }
        };
        self.record_grpc(
            "VectorBatchUpsert",
            started,
            Ok(self.with_catalog_response_headers(
                Response::new(Box::pin(out) as ResponseStream<MutationResponse>),
                &response_context,
            )),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use prost::Message as _;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    #[test]
    fn vector_hybrid_search_unsupported_carries_capability_detail() {
        let err = vector_hybrid_search_unsupported_status();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "backend qdrant does not support hybrid_search"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "qdrant");
        assert_eq!(detail.operation, "VectorHybridSearch");
        assert_eq!(detail.capability_required, "hybrid_search");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }
}
