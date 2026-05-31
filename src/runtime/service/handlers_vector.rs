//! service.rs split — vector RPC handlers (Phase G).
use super::*;

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
                    Err(Status::failed_precondition(
                        "backend qdrant does not support hybrid_search",
                    )),
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
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("VectorBatchUpsert", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "VectorBatchUpsert").await {
            return self.record_grpc("VectorBatchUpsert", started, Err(err));
        }
        let metadata_context = security.request_context();
        let response_context = metadata_context.clone();
        let manifest = (*self.catalog.active_for(&security.project_id).manifest).clone();
        let runtime = self.runtime_snapshot().clone();
        let mut stream = request.into_inner();
        let metrics = self.metrics.clone();
        let channels = self.runtime_snapshot().channels().clone();
        let out = async_stream::try_stream! {
            while let Some(item) = stream.message().await? {
                metrics.inc_vector_op(&item.collection, "batch_upsert");
                let op = crate::runtime::channels::OperationChannel::Vector;
                let project = non_empty(&metadata_context.project_id).unwrap_or("default");
                let tenant_hash = tenant_hash_label(&metadata_context.tenant_id);
                let instance = non_empty(&metadata_context.target_instance).unwrap_or("default");
                let _permit = match channels.acquire_fair_with_backpressure(
                    op,
                    Some(&metadata_context.tenant_id),
                    Some(&metadata_context.project_id),
                    Some("qdrant"),
                    Some(&metadata_context.target_instance),
                    op.default_cost(),
                ).await {
                    Ok(permit) => {
                        metrics.record_fair_admission(project, &tenant_hash, "qdrant", instance, op.as_str(), "accepted");
                        metrics.add_fair_cost(project, &tenant_hash, "qdrant", instance, op.as_str(), f64::from(op.default_cost()));
                        permit
                    },
                    Err(err) => {
                        metrics.inc_channel_rejected("vector");
                        metrics.record_fair_admission(project, &tenant_hash, "qdrant", instance, op.as_str(), "rejected");
                        Err(err)?
                    }
                };
                metrics.inc_channel_inflight("vector");
                let start = Instant::now();

                let res = tokio::time::timeout(
                    Duration::from_secs(channels.deadline_secs(crate::runtime::channels::OperationChannel::Vector, Some("qdrant"))),
                    runtime.vector_upsert(&manifest, item, metadata_context.clone())
                ).await;

                metrics.dec_channel_inflight("vector");
                metrics.observe_channel_latency("vector", start.elapsed().as_secs_f64());

                match res {
                    Ok(Ok(val)) => yield val,
                    Ok(Err(e)) => Err(e)?,
                    Err(_) => {
                        metrics.inc_channel_timeout("vector");
                        Err(Status::deadline_exceeded("vector channel timeout"))?
                    }
                }
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
