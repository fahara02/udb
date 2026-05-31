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
        if let Err(err) = self
            .authorize(&security, &request.message_type, "Select")
            .await
        {
            return self.record_grpc("Select", started, Err(err));
        }
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
        let metadata_context = security.request_context();
        let admission_context = metadata_context.clone();
        let response_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Read,
                Some(&admission_context),
                Some("postgres"),
                || async move { runtime.select(manifest, request, metadata_context).await },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "Select",
                started,
                Ok(self.with_catalog_response_headers(Response::new(res), &response_context)),
            ),
            Err(err) => self.record_grpc("Select", started, Err(err)),
        }
    }

    pub(crate) async fn batch_select_inner(
        &self,
        request: Request<tonic::Streaming<SelectRequest>>,
    ) -> Result<Response<ResponseStream<RecordSet>>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("BatchSelect", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "BatchSelect").await {
            return self.record_grpc("BatchSelect", started, Err(err));
        }
        let metadata_context = security.request_context();
        let response_context = metadata_context.clone();
        let manifest = (*self.catalog.active_for(&security.project_id).manifest).clone();
        let runtime = self.runtime_snapshot().clone();
        let security_for_stream = security.clone();
        let mut stream = request.into_inner();
        let metrics = self.metrics.clone();
        let channels = self.runtime_snapshot().channels().clone();
        let out = async_stream::try_stream! {
            while let Some(item) = stream.message().await? {
                enforce_select_export_controls(&manifest, &security_for_stream, &item.message_type, &item.fields)?;
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
                    runtime.select(&manifest, item, metadata_context.clone())
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
        if let Err(err) = self
            .authorize(&security, &request.message_type, "Upsert")
            .await
        {
            return self.record_grpc("Upsert", started, Err(err));
        }
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context();
        let admission_context = metadata_context.clone();
        let response_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Write,
                Some(&admission_context),
                Some("postgres"),
                || async move { runtime.upsert(manifest, request, metadata_context).await },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "Upsert",
                started,
                Ok(self
                    .with_mutation_response_headers(res, &response_context)
                    .await),
            ),
            Err(err) => self.record_grpc("Upsert", started, Err(err)),
        }
    }

    pub(crate) async fn batch_upsert_inner(
        &self,
        request: Request<tonic::Streaming<UpsertRequest>>,
    ) -> Result<Response<ResponseStream<MutationResponse>>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("BatchUpsert", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "BatchUpsert").await {
            return self.record_grpc("BatchUpsert", started, Err(err));
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
                    runtime.upsert(&manifest, item, metadata_context.clone())
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
        if let Err(err) = self.authorize(&security, "*", "Delete").await {
            return self.record_grpc("Delete", started, Err(err));
        }
        let context = security.request_context();
        let response_context = context.clone();
        let req = request.into_inner();
        let message_type = req.message_type.clone();
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
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("GenericDispatch", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "GenericDispatch").await {
            return self.record_grpc("GenericDispatch", started, Err(err));
        }
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
        let runtime = self.runtime_snapshot();
        let backend = req.backend.clone();
        let resolved_backend = match runtime
            .resolve_backend_selector_for_project(&backend, &metadata_context.project_id)
        {
            Ok(resolved) => resolved,
            Err(err) => return self.record_grpc("GenericDispatch", started, Err(err)),
        };
        let breaker_backend = resolved_backend.backend.clone();
        let resource_name = req.resource_name.clone();
        let spec_json = req.spec_json.clone();
        let operation = req.operation.clone();
        if req.dry_run {
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
                            "operation": operation,
                            "resource_kind": req.resource_kind,
                            "resource_name": req.resource_name,
                            "resource_uri": req.resource_uri,
                            "write_like": matches!(
                                operation.as_str(),
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
        // U2 step 6: resolve metadata (registry/connectivity/circuit-breaker)
        // first, then build the live `DispatchExecutor` via the plugin-keyed
        // resolver. Replaces the former `DefaultBackendExecutor` adapter.
        let target = match runtime.backend_executor_for_project(
            &resolved_backend.backend,
            resolved_backend.instance.as_deref(),
            &metadata_context.project_id,
        ) {
            Ok(target) => target,
            Err(err) => return self.record_grpc("GenericDispatch", started, Err(err)),
        };
        let breaker_instance = target.instance.clone();
        // U7: build the request context BEFORE resolving the executor so the
        // Postgres factory can bake it into the dispatch executor and
        // `set_request_local_settings` runs inside the request-scoped
        // transaction. RLS now sees tenant/project/purpose on the generic
        // path, not just the typed Select/Upsert RPCs.
        let mut admission_context = metadata_context.clone();
        admission_context.target_backend = target.backend.clone();
        admission_context.target_instance = target.instance.clone().unwrap_or_default();
        let executor = match runtime.resolve_dispatch_executor(
            &target.backend,
            target.instance.as_deref(),
            /* write */ false,
            tonic::Code::FailedPrecondition,
            Some(&admission_context),
        ) {
            Ok(executor) => executor,
            Err(err) => return self.record_grpc("GenericDispatch", started, Err(err)),
        };

        let result: Result<String, tonic::Status> = self
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
                        "put_object" => executor.put_object(&spec_json, vec![]).await,
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
}
