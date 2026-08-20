//! service.rs split — tx RPC handlers (Phase G).
use super::*;

type CdcEngineResponseStream = Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<crate::runtime::cdc::CdcEnvelope, Status>>
            + Send
            + 'static,
    >,
>;

/// Maximum interval during which an already-open CDC stream may continue using
/// a credential/policy decision before the canonical authorities are consulted
/// again. The existing channel timeout remains a bounded reconnect backstop.
const CDC_AUTHORIZATION_RECHECK_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CdcStreamDeadlineReason {
    CredentialExpired,
    ReauthenticationRequired,
}

fn unix_now_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

fn cdc_stream_budget(
    channel_timeout_secs: u64,
    credential_expires_at_unix: i64,
    now_unix: i64,
) -> Result<(Duration, CdcStreamDeadlineReason), Status> {
    let channel_timeout_secs = channel_timeout_secs.max(1);
    if credential_expires_at_unix <= 0 {
        return Ok((
            Duration::from_secs(channel_timeout_secs),
            CdcStreamDeadlineReason::ReauthenticationRequired,
        ));
    }
    let remaining = credential_expires_at_unix.saturating_sub(now_unix);
    if remaining <= 0 {
        return Err(crate::runtime::executor_utils::unauthenticated_status(
            "cdc_credential_expired",
            "CDC subscription credential has expired; authenticate again",
        ));
    }
    let remaining = u64::try_from(remaining).unwrap_or(u64::MAX);
    if remaining <= channel_timeout_secs {
        Ok((
            Duration::from_secs(remaining.max(1)),
            CdcStreamDeadlineReason::CredentialExpired,
        ))
    } else {
        Ok((
            Duration::from_secs(channel_timeout_secs),
            CdcStreamDeadlineReason::ReauthenticationRequired,
        ))
    }
}

struct CdcStreamLifetimeGuard {
    _permit: Option<crate::runtime::channels::ChannelPermit>,
    metrics: Arc<dyn MetricsRecorder>,
    started: Instant,
}

impl CdcStreamLifetimeGuard {
    fn new(
        permit: Option<crate::runtime::channels::ChannelPermit>,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        metrics.inc_channel_inflight("cdc");
        Self {
            _permit: permit,
            metrics,
            started: Instant::now(),
        }
    }
}

impl Drop for CdcStreamLifetimeGuard {
    fn drop(&mut self) {
        self.metrics.dec_channel_inflight("cdc");
        self.metrics
            .observe_channel_latency("cdc", self.started.elapsed().as_secs_f64());
    }
}

fn cdc_stream_deadline_status(reason: CdcStreamDeadlineReason) -> Status {
    match reason {
        CdcStreamDeadlineReason::CredentialExpired => {
            crate::runtime::executor_utils::unauthenticated_status(
                "cdc_credential_expired",
                "CDC subscription credential expired; authenticate and reconnect",
            )
        }
        CdcStreamDeadlineReason::ReauthenticationRequired => {
            crate::runtime::executor_utils::unauthenticated_status(
                "cdc_reauthentication_required",
                "CDC subscription reached its maximum authorization age; authenticate and reconnect",
            )
        }
    }
}

fn guard_cdc_response_stream(
    stream: CdcEngineResponseStream,
    guard: CdcStreamLifetimeGuard,
    deadline: tokio::time::Instant,
    deadline_reason: CdcStreamDeadlineReason,
    credential_revalidator: Option<crate::runtime::credential_layer::CredentialRevalidator>,
    mut security: SecurityContext,
    authz_snapshot: Arc<arc_swap::ArcSwap<AuthzSnapshot>>,
    topic_pattern: String,
) -> CdcEngineResponseStream {
    let timeout_metrics = guard.metrics.clone();
    Box::pin(async_stream::stream! {
        let _guard = guard;
        let timeout = tokio::time::sleep_until(deadline);
        tokio::pin!(timeout);
        let mut authorization_recheck = tokio::time::interval_at(
            tokio::time::Instant::now() + CDC_AUTHORIZATION_RECHECK_INTERVAL,
            CDC_AUTHORIZATION_RECHECK_INTERVAL,
        );
        authorization_recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut stream = stream;
        loop {
            tokio::select! {
                biased;
                _ = &mut timeout => {
                    timeout_metrics.inc_channel_timeout("cdc");
                    yield Err(cdc_stream_deadline_status(deadline_reason));
                    break;
                }
                _ = authorization_recheck.tick() => {
                    if let Some(revalidator) = credential_revalidator.as_ref() {
                        match revalidator.revalidate_security_context(&security).await {
                            Ok(refreshed) => security = refreshed,
                            Err(status) => {
                                yield Err(status);
                                break;
                            }
                        }
                    }
                    if let Err(status) =
                        crate::runtime::cdc::CdcEngine::ensure_stream_read_scope(&security.scopes)
                    {
                        yield Err(status);
                        break;
                    }
                    if let Err(status) =
                        super::tenant_service::tenant_status_gate(&security.tenant_id)
                    {
                        yield Err(status);
                        break;
                    }
                    let snapshot = authz_snapshot.load_full();
                    if let Err(status) = DataBrokerService::authorize_message_item(
                        snapshot.as_ref(),
                        &security,
                        &topic_pattern,
                        "PublishCDC",
                    )
                    .await
                    {
                        yield Err(status);
                        break;
                    }
                }
                item = stream.next() => match item {
                    Some(item) => yield item,
                    None => break,
                }
            }
        }
    })
}

impl DataBrokerService {
    pub(crate) async fn begin_tx_inner(
        &self,
        request: Request<tonic::Streaming<Mutation>>,
    ) -> Result<Response<ResponseStream<TxStatus>>, Status> {
        let (started, security) = authorized_call!(self, request, "BeginTx");
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

    #[tracing::instrument(skip_all, name = "cdc.publish")]
    pub(crate) async fn publish_cdc_inner(
        &self,
        request: Request<CdcSubscriptionRequest>,
    ) -> Result<Response<ResponseStream<CdcEnvelope>>, Status> {
        let started = Instant::now();
        let credential_revalidator = request
            .extensions()
            .get::<crate::runtime::credential_layer::PreresolvedCredentials>()
            .and_then(|credentials| credentials.revalidator.clone());
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("PublishCDC", started, Err(e)),
        };
        if let Err(status) = super::tenant_service::tenant_status_gate(&security.tenant_id) {
            return self.record_grpc("PublishCDC", started, Err(status));
        }
        if security.credential_type != 0 && credential_revalidator.is_none() {
            return self.record_grpc(
                "PublishCDC",
                started,
                Err(crate::runtime::executor_utils::unauthenticated_status(
                    "cdc_credential_revalidator_missing",
                    "PublishCDC requires the served credential-resolution layer so authorization can be revalidated during the stream",
                )),
            );
        }
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
                Err(crate::runtime::executor_utils::capability_status(
                    "cdc",
                    "PublishCDC",
                    "cdc_tailer",
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
        let cdc_ctx = security.request_context();
        let runtime = self.runtime_snapshot();
        let channels = runtime.channels().clone();
        let op = crate::runtime::channels::OperationChannel::Cdc;
        let (stream_budget, deadline_reason) = match cdc_stream_budget(
            channels.deadline_secs(op, None),
            security.expires_at_unix,
            unix_now_i64(),
        ) {
            Ok(value) => value,
            Err(err) => return self.record_grpc("PublishCDC", started, Err(err)),
        };
        // Admission belongs to the whole lazy response stream. `admit_on` uses
        // the same tenant/project semaphore hierarchy and metrics as native
        // services, and the guard below retains the permit until completion,
        // error, server deadline, or client cancellation.
        let permit = match super::native_helpers::admit_on(
            Some(&channels),
            &self.metrics,
            "cdc",
            op,
            &cdc_ctx.tenant_id,
            Some(&cdc_ctx.project_id),
        )
        .await
        {
            Ok(permit) => permit,
            Err(err) => return self.record_grpc("PublishCDC", started, Err(err)),
        };
        let guard = CdcStreamLifetimeGuard::new(permit, self.metrics.clone());
        let deadline = tokio::time::Instant::now() + stream_budget;
        let authz_snapshot = self.authz_snapshot();
        let recheck_topic_pattern = topic_pattern.clone();
        let result = tokio::time::timeout_at(
            deadline,
            cdc_engine.stream_cdc(
                security.scopes.clone(),
                topic_pattern,
                since_event_id,
                Some(security.tenant_id.clone()),
                Some(security.project_id.clone()),
            ),
        )
        .await;

        match result {
            Ok(Ok(stream)) => {
                let guarded_stream = guard_cdc_response_stream(
                    stream,
                    guard,
                    deadline,
                    deadline_reason,
                    credential_revalidator,
                    security,
                    authz_snapshot,
                    recheck_topic_pattern,
                );
                let mapped_stream = guarded_stream.map(|item| item.map(proto_cdc_envelope));
                self.record_grpc(
                    "PublishCDC",
                    started,
                    Ok(Response::new(
                        Box::pin(mapped_stream) as ResponseStream<CdcEnvelope>
                    )),
                )
            }
            Ok(Err(err)) => self.record_grpc("PublishCDC", started, Err(err)),
            Err(_) => {
                self.metrics.inc_channel_timeout("cdc");
                self.record_grpc(
                    "PublishCDC",
                    started,
                    Err(crate::runtime::executor_utils::deadline_exceeded_status(
                        "cdc",
                        "cdc stream setup",
                        crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                        "CDC stream setup exceeded the configured authorization lifetime",
                    )),
                )
            }
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
        // The envelope's tenant/project come from the caller-supplied PAYLOAD, and
        // `prepare_outbox_envelope` only checks that tenant_id is non-empty - it
        // takes no request context, so it cannot tell the payload's tenant from the
        // caller's. A principal allowed to publish on a topic could therefore emit
        // a CDC envelope attributed to ANY tenant, and every downstream consumer
        // that trusts the envelope would believe it.
        //
        // Bind them to the verified claim here, where the context IS available. A
        // genuine cross-tenant admin may still publish on another tenant's behalf;
        // everyone else is held to their own scope. An absent payload tenant is
        // left alone for `prepare_outbox_envelope` to reject with its own message.
        if let Some(obj) = payload.as_object() {
            let payload_tenant = obj
                .get("tenant_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let payload_project = obj
                .get("project_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            if crate::runtime::service::method_security::claim_context_present() {
                let ctx = crate::runtime::service::method_security::current_claim_context();
                if let Err(err) =
                    crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
                        &ctx,
                        payload_tenant,
                        payload_project,
                    )
                {
                    return self.record_grpc("EnqueueOutboxEvent", started, Err(err));
                }
            }
        }
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

        // urgent_fix #2: scope the CDC channel permit to the caller's tenant/project
        // (per-tenant rate limiting) instead of the shared `anonymous` bucket.
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Cdc,
                Some(&response_context),
                None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdc_stream_budget_clamps_to_verified_credential_expiry() {
        let (budget, reason) =
            cdc_stream_budget(30, 1_010, 1_000).expect("live credential has a stream budget");
        assert_eq!(budget, Duration::from_secs(10));
        assert_eq!(reason, CdcStreamDeadlineReason::CredentialExpired);
    }

    #[test]
    fn cdc_stream_budget_forces_periodic_reauthentication() {
        let (budget, reason) = cdc_stream_budget(30, 0, 1_000)
            .expect("non-expiring credential still has a bounded authorization age");
        assert_eq!(budget, Duration::from_secs(30));
        assert_eq!(reason, CdcStreamDeadlineReason::ReauthenticationRequired);
    }

    #[test]
    fn cdc_stream_budget_rejects_already_expired_credentials() {
        let status = cdc_stream_budget(30, 1_000, 1_000)
            .expect_err("expired credential must not open a stream");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }
}
