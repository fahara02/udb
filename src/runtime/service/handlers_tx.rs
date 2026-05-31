//! service.rs split — tx RPC handlers (Phase G).
use super::*;

impl DataBrokerService {
    pub(crate) async fn begin_tx_inner(
        &self,
        request: Request<tonic::Streaming<Mutation>>,
    ) -> Result<Response<ResponseStream<TxStatus>>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("BeginTx", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "BeginTx").await {
            return self.record_grpc("BeginTx", started, Err(err));
        }
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context();
        let response_context = metadata_context.clone();
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Transaction,
                || async move {
                    Ok(runtime
                        .begin_tx(manifest, request.into_inner(), metadata_context)
                        .await)
                },
            )
            .await;

        match result {
            Ok(statuses) => self.record_grpc(
                "BeginTx",
                started,
                Ok(self.with_catalog_response_headers(
                    Response::new(
                        Box::pin(tokio_stream::iter(statuses)) as ResponseStream<TxStatus>
                    ),
                    &response_context,
                )),
            ),
            Err(err) => self.record_grpc("BeginTx", started, Err(err)),
        }
    }

    pub(crate) async fn publish_cdc_inner(
        &self,
        request: Request<CdcSubscriptionRequest>,
    ) -> Result<Response<ResponseStream<CdcEnvelope>>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("PublishCDC", started, Err(e)),
        };
        let request = request.into_inner();
        if let Err(err) = self
            .authorize(&security, &request.topic_pattern, "PublishCDC")
            .await
        {
            return self.record_grpc("PublishCDC", started, Err(err));
        }
        let Some(cdc_engine) = &self.cdc_engine else {
            return self.record_grpc(
                "PublishCDC",
                started,
                Err(Status::unavailable(
                    "CDC tailer is not configured; set UDB_KAFKA_BROKERS to enable PublishCDC",
                )),
            );
        };
        let topic_pattern = if request.topic_pattern.trim().is_empty() {
            "*".to_string()
        } else {
            request.topic_pattern
        };
        let since_event_id = if request.since_event_id.trim().is_empty() {
            None
        } else {
            Some(request.since_event_id)
        };
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Cdc,
                || async move {
                    cdc_engine
                        .stream_cdc(security.scopes.clone(), topic_pattern, since_event_id)
                        .await
                },
            )
            .await;

        match result {
            Ok(stream) => {
                let mapped_stream = stream.map(|item| item.map(proto_cdc_envelope));
                self.record_grpc(
                    "PublishCDC",
                    started,
                    Ok(Response::new(
                        Box::pin(mapped_stream) as ResponseStream<CdcEnvelope>
                    )),
                )
            }
            Err(err) => self.record_grpc("PublishCDC", started, Err(err)),
        }
    }

    pub(crate) async fn create_materialized_view_inner(
        &self,
        request: Request<ViewDefinition>,
    ) -> Result<Response<MutationResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("CreateMaterializedView", started, Err(e)),
        };
        let request = request.into_inner();
        if let Err(err) = self
            .authorize(&security, &request.name, "CreateMaterializedView")
            .await
        {
            return self.record_grpc("CreateMaterializedView", started, Err(err));
        }
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context();
        let response_context = metadata_context.clone();
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Admin,
                || async move {
                    runtime
                        .create_materialized_view(manifest, request, metadata_context)
                        .await
                },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "CreateMaterializedView",
                started,
                Ok(self.with_catalog_response_headers(Response::new(res), &response_context)),
            ),
            Err(err) => self.record_grpc("CreateMaterializedView", started, Err(err)),
        }
    }

    pub(crate) async fn enqueue_outbox_event_inner(
        &self,
        request: Request<EnqueueOutboxEventRequest>,
    ) -> Result<Response<EnqueueOutboxEventResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("EnqueueOutboxEvent", started, Err(e)),
        };
        let request = request.into_inner();
        if let Err(err) = self
            .authorize(&security, &request.topic, "EnqueueOutboxEvent")
            .await
        {
            return self.record_grpc("EnqueueOutboxEvent", started, Err(err));
        }
        let mut payload = request
            .payload
            .as_ref()
            .map(crate::runtime::executor_utils::struct_to_json)
            .unwrap_or(serde_json::Value::Null);
        let cdc_config = self.runtime_snapshot().config().cdc.clone();
        let schema_uri = if request.schema_uri.is_empty() {
            None
        } else {
            Some(request.schema_uri.as_str())
        };
        let idempotency_key = if request.idempotency_key.is_empty() {
            None
        } else {
            Some(request.idempotency_key.as_str())
        };
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context();
        let response_context = metadata_context.clone();
        let topic = request.topic.clone();
        let partition_key = request.partition_key.clone();
        let valid_topics = cdc_config.valid_topics.clone();
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        payload = crate::runtime::cdc::apply_manifest_cdc_redaction(
            manifest,
            "",
            &topic,
            schema_uri,
            payload,
            cdc_config.redaction_mode,
            cdc_config.redaction_version,
        );

        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Cdc,
                || async move {
                    runtime
                        .enqueue_outbox_event(
                            &topic,
                            &partition_key,
                            payload,
                            schema_uri,
                            idempotency_key,
                            &valid_topics,
                            &metadata_context,
                        )
                        .await
                },
            )
            .await;

        match result {
            Ok(result) => self.record_grpc(
                "EnqueueOutboxEvent",
                started,
                Ok(self.with_catalog_response_headers(
                    Response::new(EnqueueOutboxEventResponse {
                        event_id: result.event_id,
                        enqueued: result.enqueued,
                        was_duplicate: result.was_duplicate,
                    }),
                    &response_context,
                )),
            ),
            Err(err) => self.record_grpc("EnqueueOutboxEvent", started, Err(err)),
        }
    }
}
