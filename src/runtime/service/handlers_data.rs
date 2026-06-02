//! service.rs split — data RPC handlers (Phase G).
use super::*;

impl DataBrokerService {
    pub(crate) async fn select_inner(
        &self,
        request: Request<SelectRequest>,
    ) -> Result<Response<RecordSet>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("Select", started, Err(e)),
        };
        let request = request.into_inner();
        let decision_id = match self
            .authorize(&security, &request.message_type, "Select")
            .await
        {
            Ok(id) => id,
            Err(err) => return self.record_grpc("Select", started, Err(err)),
        };
        if let Err(err) = enforce_select_export_controls(
            &self.catalog.active_for(&security.project_id).manifest,
            &security,
            &request.message_type,
            &request.fields,
        ) {
            return self.record_grpc("Select", started, Err(err));
        }
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context_with_decision(&decision_id);
        // One clone for the moved closure; the original is borrowed for admission
        // (before) and the response headers (after) — was two clones (#100).
        let exec_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Read,
                Some(&metadata_context),
                Some("postgres"),
                || async move { runtime.select(manifest, request, exec_context).await },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "Select",
                started,
                Ok(self.with_catalog_response_headers(Response::new(res), &metadata_context)),
            ),
            Err(err) => self.record_grpc("Select", started, Err(err)),
        }
    }

    /// Additive typed columnar read (A.4). Runs the exact same authorize /
    /// export-control / channel-scoped select path as `Select`, then re-encodes
    /// the resulting (already masked) `RecordSet` as a `RecordBatchV2` and emits
    /// it as a single server-streamed batch. True per-batch streaming over
    /// `query_stream` is layered on later (A.6); the V1 `Select` path is
    /// untouched.
    pub(crate) async fn select_v2_inner(
        &self,
        request: Request<SelectRequest>,
    ) -> Result<Response<ResponseStream<crate::proto::RecordBatchV2>>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("SelectV2", started, Err(e)),
        };
        let request = request.into_inner();
        let decision_id = match self
            .authorize(&security, &request.message_type, "Select")
            .await
        {
            Ok(id) => id,
            Err(err) => return self.record_grpc("SelectV2", started, Err(err)),
        };
        if let Err(err) = enforce_select_export_controls(
            &self.catalog.active_for(&security.project_id).manifest,
            &security,
            &request.message_type,
            &request.fields,
        ) {
            return self.record_grpc("SelectV2", started, Err(err));
        }
        // Active catalog version stamps the batch's schema_version.
        let schema_version = self
            .catalog
            .active_for(&security.project_id)
            .metadata
            .version
            .clone();
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context_with_decision(&decision_id);
        let exec_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Read,
                Some(&metadata_context),
                Some("postgres"),
                || async move { runtime.select(manifest, request, exec_context).await },
            )
            .await;

        match result {
            Ok(res) => {
                let batch = crate::runtime::executor_utils::record_batch_v2_from_record_set(
                    &res,
                    &schema_version,
                );
                let stream: ResponseStream<crate::proto::RecordBatchV2> =
                    Box::pin(tokio_stream::once(Ok(batch)));
                self.record_grpc(
                    "SelectV2",
                    started,
                    Ok(self
                        .with_catalog_response_headers(Response::new(stream), &metadata_context)),
                )
            }
            Err(err) => self.record_grpc("SelectV2", started, Err(err)),
        }
    }

    pub(crate) async fn batch_select_inner(
        &self,
        request: Request<tonic::Streaming<SelectRequest>>,
    ) -> Result<Response<ResponseStream<RecordSet>>, Status> {
        let (started, security) = authorized_call!(self, request, "BatchSelect");
        let metadata_context = security.request_context();
        let response_context = metadata_context.clone();
        // Arc::clone — share the active manifest into the stream instead of a deep
        // copy of the whole CatalogManifest per batch (#99).
        let manifest = self
            .catalog
            .active_for(&security.project_id)
            .manifest
            .clone();
        let runtime = self.runtime_snapshot().clone();
        let security_for_stream = security.clone();
        let mut stream = request.into_inner();
        let metrics = self.metrics.clone();
        let channels = runtime.channels().clone();
        // #112: capture the (cloneable) ABAC authz inputs so each streamed item's
        // own `message_type` is authorized inside the stream — the batch RPC's
        // `authorized_call!` only authorized `BatchSelect`, not the per-item types.
        let abac_v2 = self
            .abac_v2_override
            .unwrap_or_else(super::authz_v2_enabled);
        let abac_snapshot = self.current_abac_snapshot();
        let abac_policies = self.abac_policies.clone();
        let abac_default_allow = self.abac_default_allow;
        // #155: batch items are admitted and executed ONE AT A TIME on purpose.
        // The fair-admission permit is acquired PER ITEM (it drops at the end of
        // each loop iteration, not held across the whole batch) so every item
        // pays its own backpressure/cost, and each execute is bounded by a
        // per-item `deadline_secs` timeout below — a slow item can't stall the
        // channel indefinitely. This is bounded-concurrency admission for a
        // streaming RPC, not a permit-held-across-batch serialization bug.
        let out = async_stream::try_stream! {
            while let Some(item) = stream.message().await? {
                enforce_select_export_controls(&manifest, &security_for_stream, &item.message_type, &item.fields)?;
                // #112: authorize THIS item's message_type and stamp its own
                // decision id (the batch-level grant covered only "BatchSelect").
                let item_decision_id = DataBrokerService::authorize_message_item(
                    abac_v2,
                    &abac_snapshot,
                    &abac_policies,
                    abac_default_allow,
                    &security_for_stream,
                    &item.message_type,
                    "Select",
                )?;
                let item_context =
                    security_for_stream.request_context_with_decision(&item_decision_id);
                let op = crate::runtime::channels::OperationChannel::Read;
                let project = non_empty(&metadata_context.project_id).unwrap_or("default");
                let tenant_hash = tenant_hash_label(&metadata_context.tenant_id);
                let instance = non_empty(&metadata_context.target_instance).unwrap_or("default");

                let _permit = match channels.acquire_fair_with_backpressure(
                    op,
                    Some(&metadata_context.tenant_id),
                    Some(&metadata_context.project_id),
                    Some("postgres"),
                    Some(&metadata_context.target_instance),
                    op.default_cost(),
                ).await {
                    Ok(permit) => {
                        metrics.record_fair_admission(project, &tenant_hash, "postgres", instance, op.as_str(), "accepted");
                        metrics.add_fair_cost(project, &tenant_hash, "postgres", instance, op.as_str(), f64::from(op.default_cost()));
                        permit
                    },
                    Err(err) => {
                        metrics.inc_channel_rejected("read");
                        metrics.record_fair_admission(project, &tenant_hash, "postgres", instance, op.as_str(), "rejected");
                        Err(err)?
                    }
                };
                metrics.inc_channel_inflight("read");
                let start = Instant::now();

                let res = tokio::time::timeout(
                    Duration::from_secs(channels.deadline_secs(crate::runtime::channels::OperationChannel::Read, Some("postgres"))),
                    runtime.select(&manifest, item, item_context)
                ).await;

                metrics.dec_channel_inflight("read");
                metrics.observe_channel_latency("read", start.elapsed().as_secs_f64());

                match res {
                    Ok(Ok(val)) => yield val,
                    Ok(Err(e)) => Err(e)?,
                    Err(_) => {
                        metrics.inc_channel_timeout("read");
                        Err(Status::deadline_exceeded("read channel timeout"))?
                    }
                }
            }
        };
        self.record_grpc(
            "BatchSelect",
            started,
            Ok(self.with_catalog_response_headers(
                Response::new(Box::pin(out) as ResponseStream<RecordSet>),
                &response_context,
            )),
        )
    }

    pub(crate) async fn upsert_inner(
        &self,
        request: Request<UpsertRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("Upsert", started, Err(e)),
        };
        let request = request.into_inner();
        let decision_id = match self
            .authorize(&security, &request.message_type, "Upsert")
            .await
        {
            Ok(id) => id,
            Err(err) => return self.record_grpc("Upsert", started, Err(err)),
        };
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context_with_decision(&decision_id);
        // One clone for the moved closure; the original is borrowed for admission
        // and the response headers — was two clones (#100).
        let exec_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Write,
                Some(&metadata_context),
                Some("postgres"),
                || async move { runtime.upsert(manifest, request, exec_context).await },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "Upsert",
                started,
                Ok(self
                    .with_mutation_response_headers(res, &metadata_context)
                    .await),
            ),
            Err(err) => self.record_grpc("Upsert", started, Err(err)),
        }
    }

    pub(crate) async fn batch_upsert_inner(
        &self,
        request: Request<tonic::Streaming<UpsertRequest>>,
    ) -> Result<Response<ResponseStream<MutationResponse>>, Status> {
        let (started, security) = authorized_call!(self, request, "BatchUpsert");
        let metadata_context = security.request_context();
        let response_context = metadata_context.clone();
        // Arc::clone — share the active manifest into the stream instead of a deep
        // copy of the whole CatalogManifest per batch (#99).
        let manifest = self
            .catalog
            .active_for(&security.project_id)
            .manifest
            .clone();
        let runtime = self.runtime_snapshot().clone();
        let mut stream = request.into_inner();
        let metrics = self.metrics.clone();
        let channels = runtime.channels().clone();
        let security_for_stream = security.clone();
        // #112: capture ABAC authz inputs so each streamed item's own
        // `message_type` is authorized (batch grant covered only "BatchUpsert").
        let abac_v2 = self
            .abac_v2_override
            .unwrap_or_else(super::authz_v2_enabled);
        let abac_snapshot = self.current_abac_snapshot();
        let abac_policies = self.abac_policies.clone();
        let abac_default_allow = self.abac_default_allow;
        // #155: per-item fair-admission permit (dropped each iteration) + per-item
        // timeout below — intentional bounded-concurrency admission for the
        // streaming RPC, not a permit-held-across-batch serialization bug.
        let out = async_stream::try_stream! {
            while let Some(item) = stream.message().await? {
                // #112: authorize THIS item's message_type + stamp its decision id.
                let item_decision_id = DataBrokerService::authorize_message_item(
                    abac_v2,
                    &abac_snapshot,
                    &abac_policies,
                    abac_default_allow,
                    &security_for_stream,
                    &item.message_type,
                    "Upsert",
                )?;
                let item_context =
                    security_for_stream.request_context_with_decision(&item_decision_id);
                let op = crate::runtime::channels::OperationChannel::Write;
                let project = non_empty(&metadata_context.project_id).unwrap_or("default");
                let tenant_hash = tenant_hash_label(&metadata_context.tenant_id);
                let instance = non_empty(&metadata_context.target_instance).unwrap_or("default");
                let _permit = match channels.acquire_fair_with_backpressure(
                    op,
                    Some(&metadata_context.tenant_id),
                    Some(&metadata_context.project_id),
                    Some("postgres"),
                    Some(&metadata_context.target_instance),
                    op.default_cost(),
                ).await {
                    Ok(permit) => {
                        metrics.record_fair_admission(project, &tenant_hash, "postgres", instance, op.as_str(), "accepted");
                        metrics.add_fair_cost(project, &tenant_hash, "postgres", instance, op.as_str(), f64::from(op.default_cost()));
                        permit
                    },
                    Err(err) => {
                        metrics.inc_channel_rejected("write");
                        metrics.record_fair_admission(project, &tenant_hash, "postgres", instance, op.as_str(), "rejected");
                        Err(err)?
                    }
                };
                metrics.inc_channel_inflight("write");
                let start = Instant::now();

                let res = tokio::time::timeout(
                    Duration::from_secs(channels.deadline_secs(crate::runtime::channels::OperationChannel::Write, Some("postgres"))),
                    runtime.upsert(&manifest, item, item_context)
                ).await;

                metrics.dec_channel_inflight("write");
                metrics.observe_channel_latency("write", start.elapsed().as_secs_f64());

                match res {
                    Ok(Ok(val)) => yield val,
                    Ok(Err(e)) => Err(e)?,
                    Err(_) => {
                        metrics.inc_channel_timeout("write");
                        Err(Status::deadline_exceeded("write channel timeout"))?
                    }
                }
            }
        };
        self.record_grpc(
            "BatchUpsert",
            started,
            Ok(Response::new(
                Box::pin(out) as ResponseStream<MutationResponse>
            ))
            .map(|response| self.with_catalog_response_headers(response, &response_context)),
        )
    }

    pub(crate) async fn delete_inner(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("Delete", started, Err(e)),
        };
        let req = request.into_inner();
        let message_type = req.message_type.clone();
        // Authorize against the concrete target table (not "*"), so per-table
        // ABAC Allow/Deny policies actually match — matching Select/Upsert.
        let decision_id = match self.authorize(&security, &message_type, "Delete").await {
            Ok(id) => id,
            Err(err) => return self.record_grpc("Delete", started, Err(err)),
        };
        let context = security.request_context_with_decision(&decision_id);
        let response_context = context.clone();
        let filter = req
            .filter
            .as_ref()
            .map(crate::runtime::executor_utils::struct_to_json)
            .unwrap_or(serde_json::Value::Null);
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Write,
                || async move {
                    runtime
                        .delete(manifest, &message_type, filter, context)
                        .await
                },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "Delete",
                started,
                Ok(self
                    .with_mutation_response_headers(res, &response_context)
                    .await),
            ),
            Err(err) => self.record_grpc("Delete", started, Err(err)),
        }
    }

    pub(crate) async fn generic_dispatch_inner(
        &self,
        request: Request<GenericDispatchRequest>,
    ) -> Result<Response<GenericDispatchResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "GenericDispatch");
        if !security
            .scopes
            .iter()
            .any(|s| s == "udb:dispatch" || s == "udb:admin" || s == "*")
        {
            return self.record_grpc(
                "GenericDispatch",
                started,
                Err(Status::permission_denied(
                    "scope udb:dispatch or udb:admin is required",
                )),
            );
        }
        let req = request.into_inner();
        let metadata_context = security.request_context();
        let response_context = metadata_context.clone();
        // ── Capability enforcement ────────────────────────────────────────────
        if let Err(err) = check_generic_dispatch_operation(&req.backend, &req.operation) {
            return self.record_grpc("GenericDispatch", started, Err(err));
        }
        if req.dry_run {
            let runtime = self.runtime_snapshot();
            let resolved_backend = match runtime
                .resolve_backend_selector_for_project(&req.backend, &metadata_context.project_id)
            {
                Ok(resolved) => resolved,
                Err(err) => return self.record_grpc("GenericDispatch", started, Err(err)),
            };
            return self.record_grpc(
                "GenericDispatch",
                started,
                Ok(Response::new(GenericDispatchResponse {
                    backend: req.backend.clone(),
                    operation: req.operation.clone(),
                    resource_uri: req.resource_uri.clone(),
                    result_json: serde_json::json!({
                        "dry_run": true,
                        "execution_plan": {
                            "pipeline": [
                                "extract_security_context",
                                "authorize",
                                "check_capability",
                                "resolve_backend_selector",
                                "admit_generic_dispatch_channel",
                                "resolve_dispatch_executor",
                                "execute"
                            ],
                            "backend": resolved_backend.backend,
                            "instance": resolved_backend.instance,
                            "operation": req.operation,
                            "resource_kind": req.resource_kind,
                            "resource_name": req.resource_name,
                            "resource_uri": req.resource_uri,
                            "write_like": matches!(
                                req.operation.as_str(),
                                "ensure_resource" | "drop_resource" | "mutate" | "transaction" | "put_object"
                            ),
                            "requires_scope": "udb:dispatch or udb:admin",
                            "channel": "generic_dispatch"
                        },
                        "resource_name": req.resource_name,
                        "idempotency_key": req.idempotency_key,
                    })
                    .to_string(),
                    ..Default::default()
                })),
            );
        }
        // Run through the single shared backend-dispatch core (also used by the
        // typed cache/document/graph/time-series/analytical RPCs).
        // Route write-like operations to a write instance (mirrors the dry-run
        // `write_like` above); read-only ops stay on read instances.
        let write_like = matches!(
            req.operation.as_str(),
            "ensure_resource" | "drop_resource" | "mutate" | "transaction" | "put_object"
        );
        let result: Result<String, tonic::Status> = self
            .execute_backend_operation(
                &metadata_context,
                &req.backend,
                write_like,
                req.operation.clone(),
                req.resource_name.clone(),
                req.spec_json.clone(),
            )
            .await;
        let response = match result {
            Ok(result_json) => GenericDispatchResponse {
                backend: req.backend,
                operation: req.operation,
                result_json,
                ..Default::default()
            },
            Err(err) => return self.record_grpc("GenericDispatch", started, Err(err)),
        };
        self.record_grpc("GenericDispatch", started, Ok(Response::new(response)))
            .map(|response| self.with_catalog_response_headers(response, &response_context))
    }

    /// Single shared backend-dispatch core. Resolves the executor for `backend`
    /// in the request's project, then runs `operation` (`query`/`mutate`/
    /// `search`/`ensure_resource`/… with `resource_name`/`spec_json` as needed)
    /// through it, scoped to the GenericDispatch channel + circuit breaker, and
    /// returns the executor's raw JSON result.
    ///
    /// `generic_dispatch_inner` and the typed cache/document/graph/time-series/
    /// analytical RPC handlers all funnel through this — there is no second
    /// dispatch implementation. Callers do their own auth/scope checks and map
    /// the returned JSON into their response type.
    pub(crate) async fn execute_backend_operation(
        &self,
        context: &crate::RequestContext,
        backend: &str,
        write: bool,
        operation: String,
        resource_name: String,
        spec_json: String,
    ) -> Result<String, tonic::Status> {
        let runtime = self.runtime_snapshot();
        let resolved_backend =
            runtime.resolve_backend_selector_for_project(backend, &context.project_id)?;
        let target = runtime.backend_executor_for_project(
            &resolved_backend.backend,
            resolved_backend.instance.as_deref(),
            &context.project_id,
        )?;
        let breaker_backend = target.backend.clone();
        let breaker_instance = target.instance.clone();
        let mut admission_context = context.clone();
        admission_context.target_backend = target.backend.clone();
        admission_context.target_instance = target.instance.clone().unwrap_or_default();
        let executor = runtime.resolve_dispatch_executor(
            &target.backend,
            target.instance.as_deref(),
            write,
            tonic::Code::FailedPrecondition,
            Some(&admission_context),
        )?;
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::GenericDispatch,
                Some(&admission_context),
                Some(&breaker_backend),
                || async move {
                    use crate::runtime::executors::{
                        BackendExecutor, BackendHealth, MutationExecutor, ObjectExecutor,
                        QueryExecutor, ResourceAdminExecutor, SearchExecutor,
                    };
                    match operation.as_str() {
                        "ping" => executor
                            .ping()
                            .await
                            .map(|_| r#"{"status":"ok"}"#.to_string())
                            .map_err(tonic::Status::unavailable),
                        "probe" => match executor.probe().await {
                            Ok(probe) => serde_json::to_string(&probe)
                                .map_err(|e| tonic::Status::internal(e.to_string())),
                            Err(status) => Err(status),
                        },
                        "ensure_resource" => executor
                            .ensure_resource(&resource_name, &spec_json)
                            .await
                            .map(|_| r#"{"status":"ok"}"#.to_string()),
                        "drop_resource" => executor
                            .drop_resource(&resource_name)
                            .await
                            .map(|_| r#"{"status":"ok"}"#.to_string()),
                        "list_resources" => match executor.list_resources().await {
                            Ok(list) => serde_json::to_string(&list)
                                .map_err(|e| tonic::Status::internal(e.to_string())),
                            Err(status) => Err(status),
                        },
                        "query" => executor.query(&spec_json).await,
                        "mutate" => executor.mutate(&spec_json).await,
                        "transaction" => executor.transaction(&spec_json).await,
                        "search" => executor.search(&spec_json).await,
                        "get_object" => executor.get_object(&spec_json).await.map(|bytes| {
                            use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
                            serde_json::json!({
                                "data_base64": B64.encode(&bytes),
                                "bytes": bytes.len()
                            })
                            .to_string()
                        }),
                        "put_object" => {
                            let spec_value: serde_json::Value =
                                serde_json::from_str(&spec_json).map_err(|err| {
                                    tonic::Status::invalid_argument(format!(
                                        "put_object spec_json must be valid JSON: {err}"
                                    ))
                                })?;
                            let bytes =
                                crate::runtime::executor_utils::object_bytes_from_json(&spec_value)?;
                            executor.put_object(&spec_json, bytes).await
                        }
                        other => Err(tonic::Status::invalid_argument(format!(
                            "unknown operation '{other}'; allowed: ping, probe, ensure_resource, drop_resource, list_resources, query, mutate, transaction, search, get_object, put_object"
                        ))),
                    }
                },
            )
            .await;
        runtime.record_backend_result(
            &breaker_backend,
            breaker_instance.as_deref(),
            result.is_ok(),
        );
        result
    }
}
