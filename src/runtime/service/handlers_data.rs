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
        // Casbin-only per-item authz: capture the cloneable `Arc<AuthzSnapshot>`
        // so each streamed item is decided through the SAME `casbin_authorize`
        // path as the single-item gate — deny-by-default, no legacy `evaluate_abac`,
        // no v2 flag. The stream body is async, so it `.await`s the decision.
        let abac_snapshot = self.current_abac_snapshot();
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
                    &abac_snapshot,
                    &security_for_stream,
                    &item.message_type,
                    "Select",
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
                    crate::runtime::channels::OperationChannel::Read,
                    "postgres",
                    runtime.select(&manifest, item, item_context),
                )
                .await?;
                yield val;
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
        // Casbin-only per-item authz (see batch_select_inner): capture the
        // cloneable snapshot and decide each item via `casbin_authorize`.
        let abac_snapshot = self.current_abac_snapshot();
        // #155: per-item fair-admission permit (dropped each iteration) + per-item
        // timeout below — intentional bounded-concurrency admission for the
        // streaming RPC, not a permit-held-across-batch serialization bug.
        let out = async_stream::try_stream! {
            while let Some(item) = stream.message().await? {
                // #112: authorize THIS item's message_type + stamp its decision id.
                let item_decision_id = DataBrokerService::authorize_message_item(
                    &abac_snapshot,
                    &security_for_stream,
                    &item.message_type,
                    "Upsert",
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
                    crate::runtime::channels::OperationChannel::Write,
                    "postgres",
                    runtime.upsert(&manifest, item, item_context),
                )
                .await?;
                yield val;
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
        guard_rls_bypass_operation(&operation, &spec_json)?;
        let runtime = self.runtime_snapshot();
        let resolved_backend =
            runtime.resolve_backend_selector_for_project(backend, &context.project_id)?;
        let active_catalog = self.catalog.active_for(&context.project_id);
        let compiled_dispatch = compile_neutral_ir_dispatch(
            &resolved_backend.backend,
            resolved_backend.instance.as_deref(),
            context,
            &active_catalog.manifest,
            &operation,
            &spec_json,
        )?;
        let operation = compiled_dispatch
            .as_ref()
            .map(|compiled| compiled.operation.clone())
            .unwrap_or(operation);
        let spec_json = compiled_dispatch
            .as_ref()
            .map(|compiled| compiled.spec_json.clone())
            .unwrap_or(spec_json);
        let write = if compiled_dispatch.is_some() {
            matches!(
                operation.as_str(),
                "ensure_resource" | "drop_resource" | "mutate" | "transaction" | "put_object"
            )
        } else {
            write
        };
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
                    use futures::FutureExt as _;
                    // bug_report.md H: a single backend executor RPC must NEVER abort
                    // the whole broker. Run the dispatch inside catch_unwind so a Rust
                    // panic in any executor (e.g. the ClickHouse insert/HTTP path under
                    // concurrent load) fails ONLY this request as `Internal` — the
                    // process and both listeners stay up. The panic hook (C1) still
                    // logs+flushes the panic location before catch_unwind converts it.
                    let dispatch = async move {
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
                    };
                    std::panic::AssertUnwindSafe(dispatch)
                        .catch_unwind()
                        .await
                        .unwrap_or_else(|_| {
                            Err(tonic::Status::internal(
                                "backend operation panicked; request failed (broker stayed up)",
                            ))
                        })
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

#[derive(Debug, Clone)]
pub(crate) struct CompiledDispatchRequest {
    pub(crate) operation: String,
    pub(crate) spec_json: String,
}

fn compile_neutral_ir_dispatch(
    backend: &str,
    instance: Option<&str>,
    context: &crate::RequestContext,
    manifest: &CatalogManifest,
    requested_operation: &str,
    spec_json: &str,
) -> Result<Option<CompiledDispatchRequest>, Status> {
    let spec: serde_json::Value = serde_json::from_str(spec_json)
        .map_err(|err| Status::invalid_argument(format!("invalid spec_json: {err}")))?;
    let Some(ir) = spec
        .get("ir")
        .or_else(|| spec.get("neutral_ir"))
        .or_else(|| spec.get("logical_operation"))
    else {
        return Ok(None);
    };
    let Some(kind) = crate::backend::BackendKind::from_token(backend) else {
        return Err(Status::invalid_argument(format!(
            "backend '{backend}' has no neutral-IR compiler"
        )));
    };
    let ir_op = ir
        .get("op")
        .or_else(|| ir.get("operation"))
        .or_else(|| spec.get("ir_op"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| Status::invalid_argument("neutral IR dispatch requires `ir.op`"))?;
    let payload = ir_payload(ir)?;
    let mut ctx = crate::ir::compile::CompileContext::new(manifest)
        .with_tenant(&context.tenant_id)
        .with_project(&context.project_id);
    if let Some(instance) = instance.filter(|value| !value.trim().is_empty()) {
        ctx = ctx.with_instance(instance);
    }
    let (rendering, family) = compile_ir_payload(&kind, ir_op, payload, &ctx)?;
    let compiled = compiled_rendering_to_dispatch(&rendering, family)?;
    if !requested_operation.trim().is_empty() && requested_operation != compiled.operation {
        tracing::debug!(
            backend = %backend,
            requested_operation = %requested_operation,
            compiled_operation = %compiled.operation,
            ir_op = %ir_op,
            "neutral IR dispatch operation rewritten after compilation"
        );
    }
    Ok(Some(compiled))
}

fn ir_payload(ir: &serde_json::Value) -> Result<serde_json::Value, Status> {
    if let Some(payload) = ir.get("request").or_else(|| ir.get("body")) {
        return Ok(payload.clone());
    }
    let serde_json::Value::Object(map) = ir else {
        return Err(Status::invalid_argument(
            "neutral IR dispatch `ir` must be an object",
        ));
    };
    let mut payload = map.clone();
    payload.remove("op");
    payload.remove("operation");
    Ok(serde_json::Value::Object(payload))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogicalOpFamily {
    Read,
    Write,
    Update,
    Delete,
    Search,
    ResourceOp,
    Aggregate,
}

fn compile_ir_payload(
    kind: &crate::backend::BackendKind,
    ir_op: &str,
    payload: serde_json::Value,
    ctx: &crate::ir::compile::CompileContext<'_>,
) -> Result<(crate::ir::compile::CompiledRendering, LogicalOpFamily), Status> {
    use crate::ir::compile::{CompileOperation, compile_for_backend};
    let compile = |op: CompileOperation<'_>| {
        compile_for_backend(kind, op, ctx)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "backend '{}' has no neutral-IR compiler in this build",
                    kind.as_str()
                ))
            })?
            .map_err(|err| {
                Status::invalid_argument(format!(
                    "neutral IR compile failed [{}]: {err}",
                    err.code()
                ))
            })
    };
    match ir_op.trim().to_ascii_lowercase().as_str() {
        "read" | "query" => {
            let op: crate::ir::LogicalRead = serde_json::from_value(payload)
                .map_err(|err| Status::invalid_argument(format!("invalid LogicalRead: {err}")))?;
            Ok((compile(CompileOperation::Read(&op))?, LogicalOpFamily::Read))
        }
        "write" | "upsert" | "mutate" => {
            let op: crate::ir::LogicalWrite = serde_json::from_value(payload)
                .map_err(|err| Status::invalid_argument(format!("invalid LogicalWrite: {err}")))?;
            Ok((
                compile(CompileOperation::Write(&op))?,
                LogicalOpFamily::Write,
            ))
        }
        "update" => {
            let op: crate::ir::LogicalUpdate = serde_json::from_value(payload)
                .map_err(|err| Status::invalid_argument(format!("invalid LogicalUpdate: {err}")))?;
            Ok((
                compile(CompileOperation::Update(&op))?,
                LogicalOpFamily::Update,
            ))
        }
        "delete" => {
            let op: crate::ir::LogicalDelete = serde_json::from_value(payload)
                .map_err(|err| Status::invalid_argument(format!("invalid LogicalDelete: {err}")))?;
            Ok((
                compile(CompileOperation::Delete(&op))?,
                LogicalOpFamily::Delete,
            ))
        }
        "search" => {
            let op: crate::ir::LogicalSearch = serde_json::from_value(payload)
                .map_err(|err| Status::invalid_argument(format!("invalid LogicalSearch: {err}")))?;
            Ok((
                compile(CompileOperation::Search(&op))?,
                LogicalOpFamily::Search,
            ))
        }
        "resource_op" | "resource" => {
            let op: crate::ir::LogicalResourceOp =
                serde_json::from_value(payload).map_err(|err| {
                    Status::invalid_argument(format!("invalid LogicalResourceOp: {err}"))
                })?;
            Ok((
                compile(CompileOperation::ResourceOp(&op))?,
                LogicalOpFamily::ResourceOp,
            ))
        }
        "aggregate" => {
            let op: crate::ir::LogicalAggregate =
                serde_json::from_value(payload).map_err(|err| {
                    Status::invalid_argument(format!("invalid LogicalAggregate: {err}"))
                })?;
            Ok((
                compile(CompileOperation::Aggregate(&op))?,
                LogicalOpFamily::Aggregate,
            ))
        }
        other => Err(Status::invalid_argument(format!(
            "unsupported neutral IR op '{other}'"
        ))),
    }
}

pub(crate) fn compile_logical_read_dispatch(
    kind: &crate::backend::BackendKind,
    op: &crate::ir::LogicalRead,
    ctx: &crate::ir::compile::CompileContext<'_>,
) -> Result<CompiledDispatchRequest, Status> {
    use crate::ir::compile::{CompileOperation, compile_for_backend};
    let rendering = compile_for_backend(kind, CompileOperation::Read(op), ctx)
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "backend '{}' has no neutral-IR compiler in this build",
                kind.as_str()
            ))
        })?
        .map_err(|err| {
            Status::invalid_argument(format!("neutral IR compile failed [{}]: {err}", err.code()))
        })?;
    compiled_rendering_to_dispatch(&rendering, LogicalOpFamily::Read)
}

pub(crate) fn compile_logical_write_dispatch(
    kind: &crate::backend::BackendKind,
    op: &crate::ir::LogicalWrite,
    ctx: &crate::ir::compile::CompileContext<'_>,
) -> Result<CompiledDispatchRequest, Status> {
    use crate::ir::compile::{CompileOperation, compile_for_backend};
    let rendering = compile_for_backend(kind, CompileOperation::Write(op), ctx)
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "backend '{}' has no neutral-IR compiler in this build",
                kind.as_str()
            ))
        })?
        .map_err(|err| {
            Status::invalid_argument(format!("neutral IR compile failed [{}]: {err}", err.code()))
        })?;
    compiled_rendering_to_dispatch(&rendering, LogicalOpFamily::Write)
}

#[allow(dead_code)]
pub(crate) fn compile_logical_update_dispatch(
    kind: &crate::backend::BackendKind,
    op: &crate::ir::LogicalUpdate,
    ctx: &crate::ir::compile::CompileContext<'_>,
) -> Result<CompiledDispatchRequest, Status> {
    use crate::ir::compile::{CompileOperation, compile_for_backend};
    let rendering = compile_for_backend(kind, CompileOperation::Update(op), ctx)
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "backend '{}' has no neutral-IR compiler in this build",
                kind.as_str()
            ))
        })?
        .map_err(|err| {
            Status::invalid_argument(format!("neutral IR compile failed [{}]: {err}", err.code()))
        })?;
    compiled_rendering_to_dispatch(&rendering, LogicalOpFamily::Update)
}

pub(crate) fn compile_logical_aggregate_dispatch(
    kind: &crate::backend::BackendKind,
    op: &crate::ir::LogicalAggregate,
    ctx: &crate::ir::compile::CompileContext<'_>,
) -> Result<CompiledDispatchRequest, Status> {
    use crate::ir::compile::{CompileOperation, compile_for_backend};
    let rendering = compile_for_backend(kind, CompileOperation::Aggregate(op), ctx)
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "backend '{}' has no neutral-IR compiler in this build",
                kind.as_str()
            ))
        })?
        .map_err(|err| {
            Status::invalid_argument(format!("neutral IR compile failed [{}]: {err}", err.code()))
        })?;
    compiled_rendering_to_dispatch(&rendering, LogicalOpFamily::Aggregate)
}

#[allow(dead_code)]
pub(crate) fn compile_logical_delete_dispatch(
    kind: &crate::backend::BackendKind,
    op: &crate::ir::LogicalDelete,
    ctx: &crate::ir::compile::CompileContext<'_>,
) -> Result<CompiledDispatchRequest, Status> {
    use crate::ir::compile::{CompileOperation, compile_for_backend};
    let rendering = compile_for_backend(kind, CompileOperation::Delete(op), ctx)
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "backend '{}' has no neutral-IR compiler in this build",
                kind.as_str()
            ))
        })?
        .map_err(|err| {
            Status::invalid_argument(format!("neutral IR compile failed [{}]: {err}", err.code()))
        })?;
    compiled_rendering_to_dispatch(&rendering, LogicalOpFamily::Delete)
}

fn compiled_rendering_to_dispatch(
    rendering: &crate::ir::compile::CompiledRendering,
    family: LogicalOpFamily,
) -> Result<CompiledDispatchRequest, Status> {
    use crate::backend::BackendKind;
    use crate::ir::compile::{CompiledRendering, KeyValueOp, ObjectOp};
    match rendering {
        CompiledRendering::Sql {
            backend,
            statement,
            params,
        } => {
            let sql = if matches!(backend, BackendKind::Clickhouse) {
                inline_sql_params(statement, params)?
            } else {
                statement.clone()
            };
            let params_json = if matches!(backend, BackendKind::Clickhouse) {
                Vec::new()
            } else {
                params.iter().map(logical_value_to_json).collect::<Vec<_>>()
            };
            let param_types = if matches!(backend, BackendKind::Postgres) {
                postgres_param_types(statement, params)
            } else {
                Vec::new()
            };
            let operation = if matches!(
                family,
                LogicalOpFamily::Read | LogicalOpFamily::Search | LogicalOpFamily::Aggregate
            ) {
                "query"
            } else {
                "mutate"
            };
            let mut spec = serde_json::json!({
                "sql": sql,
                "params": params_json,
                "compiler_mediated": true,
            });
            if !param_types.is_empty() {
                spec["param_types"] = serde_json::json!(param_types);
            }
            Ok(CompiledDispatchRequest {
                operation: operation.to_string(),
                spec_json: spec.to_string(),
            })
        }
        CompiledRendering::Json {
            backend,
            method,
            path,
            body,
        } => json_rendering_to_dispatch(backend, method, path, body, family),
        CompiledRendering::KeyValue {
            op,
            key_template,
            value,
            ttl_seconds,
            ..
        } => {
            let (operation, request_op) = match op {
                KeyValueOp::Get => ("query", "get"),
                KeyValueOp::Exists => ("query", "exists"),
                KeyValueOp::Scan => ("query", "scan"),
                KeyValueOp::Set => ("mutate", "set"),
                KeyValueOp::Delete => ("mutate", "delete"),
            };
            let mut spec = serde_json::json!({
                "operation": request_op,
                "key": key_template,
                "compiler_mediated": true,
            });
            if let Some(value) = value {
                spec["value"] = serde_json::Value::String(
                    String::from_utf8(value.clone()).unwrap_or_else(|_| {
                        use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
                        format!("base64:{}", B64.encode(value))
                    }),
                );
            }
            if let Some(ttl) = ttl_seconds {
                spec["ttl"] = serde_json::json!(ttl);
            }
            Ok(CompiledDispatchRequest {
                operation: operation.to_string(),
                spec_json: spec.to_string(),
            })
        }
        CompiledRendering::Object {
            op,
            bucket,
            key,
            content_type,
            ..
        } => {
            let operation = match op {
                ObjectOp::GetObject | ObjectOp::HeadObject => "get_object",
                ObjectOp::PutObject => "put_object",
                ObjectOp::DeleteObject | ObjectOp::ListObjects | ObjectOp::GeneratePresigned => {
                    return Err(Status::failed_precondition(format!(
                        "compiled object op '{op:?}' is not exposed by GenericDispatch"
                    )));
                }
            };
            let mut spec = serde_json::json!({
                "bucket": bucket,
                "key": key,
                "object_key": key,
                "compiler_mediated": true,
            });
            if let Some(content_type) = content_type {
                spec["content_type"] = serde_json::json!(content_type);
            }
            Ok(CompiledDispatchRequest {
                operation: operation.to_string(),
                spec_json: spec.to_string(),
            })
        }
    }
}

fn json_rendering_to_dispatch(
    backend: &crate::backend::BackendKind,
    method: &crate::ir::compile::HttpMethod,
    path: &str,
    body: &serde_json::Value,
    family: LogicalOpFamily,
) -> Result<CompiledDispatchRequest, Status> {
    use crate::backend::BackendKind;
    let operation = match family {
        LogicalOpFamily::Write | LogicalOpFamily::Update | LogicalOpFamily::Delete => "mutate",
        LogicalOpFamily::Search => "search",
        LogicalOpFamily::ResourceOp => "mutate",
        LogicalOpFamily::Read | LogicalOpFamily::Aggregate => "query",
    };
    let spec = match backend {
        BackendKind::Mongodb => mongodb_rendering_body(path, body, family)?,
        BackendKind::Qdrant => qdrant_rendering_body(path, body, family)?,
        BackendKind::Elasticsearch | BackendKind::Pinecone | BackendKind::Weaviate => {
            serde_json::json!({
                "method": http_method_token(method),
                "path": path,
                "body": body,
                "compiler_mediated": true,
            })
        }
        _ => serde_json::json!({
            "method": http_method_token(method),
            "path": path,
            "body": body,
            "compiler_mediated": true,
        }),
    };
    Ok(CompiledDispatchRequest {
        operation: operation.to_string(),
        spec_json: spec.to_string(),
    })
}

fn mongodb_rendering_body(
    path: &str,
    body: &serde_json::Value,
    family: LogicalOpFamily,
) -> Result<serde_json::Value, Status> {
    let mut spec = body.clone();
    let serde_json::Value::Object(map) = &mut spec else {
        return Err(Status::invalid_argument(
            "MongoDB compiled rendering body must be an object",
        ));
    };
    match family {
        LogicalOpFamily::Read => {}
        LogicalOpFamily::Aggregate | LogicalOpFamily::Search => {
            map.insert("operation".into(), serde_json::json!("aggregate"));
        }
        LogicalOpFamily::Write | LogicalOpFamily::Update => {
            if path.ends_with("insertMany") || map.contains_key("documents") {
                map.insert("operation".into(), serde_json::json!("insert_many"));
            } else if path.ends_with("insertOne") || map.contains_key("document") {
                map.insert("operation".into(), serde_json::json!("insert"));
            } else if let Some(replacement) = map.remove("replacement") {
                map.insert("operation".into(), serde_json::json!("upsert"));
                map.insert("document".into(), replacement);
            } else {
                map.insert("operation".into(), serde_json::json!("upsert"));
            }
        }
        LogicalOpFamily::Delete => {
            map.insert("operation".into(), serde_json::json!("delete_many"));
        }
        LogicalOpFamily::ResourceOp => {}
    }
    map.insert("compiler_mediated".into(), serde_json::json!(true));
    Ok(spec)
}

fn qdrant_rendering_body(
    path: &str,
    body: &serde_json::Value,
    family: LogicalOpFamily,
) -> Result<serde_json::Value, Status> {
    let mut spec = body.clone();
    let serde_json::Value::Object(map) = &mut spec else {
        return Err(Status::invalid_argument(
            "Qdrant compiled rendering body must be an object",
        ));
    };
    if let Some(collection) = qdrant_collection_from_path(path) {
        map.insert("collection".into(), serde_json::json!(collection));
    }
    match family {
        LogicalOpFamily::Write | LogicalOpFamily::Update => {
            map.insert("operation".into(), serde_json::json!("upsert"));
        }
        LogicalOpFamily::Delete => {
            map.insert("operation".into(), serde_json::json!("delete"));
        }
        _ => {}
    }
    map.insert("compiler_mediated".into(), serde_json::json!(true));
    Ok(spec)
}

fn qdrant_collection_from_path(path: &str) -> Option<String> {
    let mut parts = path.split('/').filter(|part| !part.is_empty());
    while let Some(part) = parts.next() {
        if part == "collections" {
            return parts.next().map(str::to_string);
        }
    }
    None
}

fn http_method_token(method: &crate::ir::compile::HttpMethod) -> &'static str {
    match method {
        crate::ir::compile::HttpMethod::Get => "GET",
        crate::ir::compile::HttpMethod::Post => "POST",
        crate::ir::compile::HttpMethod::Put => "PUT",
        crate::ir::compile::HttpMethod::Patch => "PATCH",
        crate::ir::compile::HttpMethod::Delete => "DELETE",
    }
}

fn logical_value_to_json(value: &crate::ir::value::LogicalValue) -> serde_json::Value {
    use crate::ir::value::LogicalValue;
    match value {
        LogicalValue::Null => serde_json::Value::Null,
        LogicalValue::Bool(value) => serde_json::Value::Bool(*value),
        LogicalValue::Int(value) => serde_json::json!(value),
        LogicalValue::Float(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        LogicalValue::String(value) => serde_json::Value::String(value.clone()),
        LogicalValue::Bytes(value) => {
            use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
            serde_json::Value::String(format!("base64:{}", B64.encode(value)))
        }
        LogicalValue::Timestamp(value) => serde_json::Value::String(value.to_rfc3339()),
        LogicalValue::Json(value) => value.clone(),
        LogicalValue::Array(values) => {
            serde_json::Value::Array(values.iter().map(logical_value_to_json).collect())
        }
    }
}

fn logical_value_param_type(value: &crate::ir::value::LogicalValue) -> &'static str {
    use crate::ir::value::LogicalValue;
    match value {
        LogicalValue::Json(_) => "json",
        // A `Timestamp` renders to an RFC-3339 *string*; without this hint the
        // executor would bind it as `text`, which Postgres refuses to coerce
        // into a `timestamptz` column on INSERT ("column … is of type timestamp
        // with time zone but expression is of type text"). Type it so the bind
        // path parses it back to a real `DateTime<Utc>`.
        LogicalValue::Timestamp(_) => "timestamptz",
        LogicalValue::Array(values) => match values
            .iter()
            .find(|value| !matches!(value, LogicalValue::Null))
        {
            Some(LogicalValue::String(_)) => "array_string",
            Some(LogicalValue::Int(_)) => "array_int",
            Some(LogicalValue::Float(_)) => "array_float",
            Some(LogicalValue::Bool(_)) => "array_bool",
            _ => "json",
        },
        _ => "",
    }
}

fn postgres_param_types(
    statement: &str,
    params: &[crate::ir::value::LogicalValue],
) -> Vec<&'static str> {
    params
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            let value_type = logical_value_param_type(value);
            if value_type.is_empty() {
                postgres_placeholder_cast_type(statement, idx + 1).unwrap_or("")
            } else {
                value_type
            }
        })
        .collect()
}

fn postgres_placeholder_cast_type(statement: &str, position: usize) -> Option<&'static str> {
    let marker = format!("${position}::");
    let (_, tail) = statement.split_once(&marker)?;
    let lower = tail.trim_start().to_ascii_lowercase();
    if lower.starts_with("timestamp with time zone") || lower.starts_with("timestamptz") {
        Some("timestamptz")
    } else if lower.starts_with("uuid") {
        Some("uuid")
    } else {
        None
    }
}

fn inline_sql_params(
    statement: &str,
    params: &[crate::ir::value::LogicalValue],
) -> Result<String, Status> {
    let mut out = String::with_capacity(statement.len() + params.len() * 8);
    let mut params = params.iter();
    for ch in statement.chars() {
        if ch == '?' {
            let value = params.next().ok_or_else(|| {
                Status::invalid_argument("compiled SQL has more placeholders than params")
            })?;
            out.push_str(&clickhouse_literal(value));
        } else {
            out.push(ch);
        }
    }
    if params.next().is_some() {
        return Err(Status::invalid_argument(
            "compiled SQL has more params than placeholders",
        ));
    }
    Ok(out)
}

fn clickhouse_literal(value: &crate::ir::value::LogicalValue) -> String {
    use crate::ir::value::LogicalValue;
    match value {
        LogicalValue::Null => "NULL".to_string(),
        LogicalValue::Bool(value) => {
            if *value {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        LogicalValue::Int(value) => value.to_string(),
        LogicalValue::Float(value) => value.to_string(),
        LogicalValue::String(value) => format!("'{}'", value.replace('\'', "''")),
        LogicalValue::Bytes(value) => {
            use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
            format!("'{}'", B64.encode(value))
        }
        LogicalValue::Timestamp(value) => format!("'{}'", value.to_rfc3339()),
        LogicalValue::Json(value) => format!("'{}'", value.to_string().replace('\'', "''")),
        LogicalValue::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(clickhouse_literal)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{ManifestColumn, ManifestTable};

    fn fixture_manifest() -> CatalogManifest {
        CatalogManifest {
            tables: vec![ManifestTable {
                message_name: "acme.billing.v1.Customer".to_string(),
                schema: "public".to_string(),
                table: "customers".to_string(),
                primary_key: vec!["id".to_string()],
                columns: vec![
                    ManifestColumn {
                        field_name: "id".into(),
                        column_name: "id".into(),
                        proto_type: "string".into(),
                        sql_type: "uuid".into(),
                        is_primary: true,
                        not_null: true,
                        ..Default::default()
                    },
                    ManifestColumn {
                        field_name: "email".into(),
                        column_name: "email".into(),
                        proto_type: "string".into(),
                        sql_type: "text".into(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn neutral_ir_dispatch_compiles_to_executor_ready_postgres_query() {
        let manifest = fixture_manifest();
        let context = crate::RequestContext {
            tenant_id: "tenant-a".into(),
            project_id: "billing".into(),
            ..Default::default()
        };
        let spec = serde_json::json!({
            "ir": {
                "op": "read",
                "message_type": "acme.billing.v1.Customer",
                "filter": {
                    "Comparison": {
                        "field": "email",
                        "op": "eq",
                        "value": { "String": "a@b.com" }
                    }
                },
                "pagination": { "limit": 5 }
            }
        });

        let compiled = compile_neutral_ir_dispatch(
            "postgres",
            None,
            &context,
            &manifest,
            "query",
            &spec.to_string(),
        )
        .expect("compile dispatch")
        .expect("compiled request");
        assert_eq!(compiled.operation, "query");

        let dispatch: serde_json::Value =
            serde_json::from_str(&compiled.spec_json).expect("dispatch json");
        assert_eq!(dispatch["compiler_mediated"], true);
        assert!(
            dispatch["sql"]
                .as_str()
                .unwrap()
                .contains("FROM \"public\".\"customers\"")
        );
        assert_eq!(dispatch["params"], serde_json::json!(["a@b.com"]));
    }

    #[test]
    fn postgres_param_types_use_placeholder_casts_for_nulls() {
        use crate::ir::value::LogicalValue;

        let statement = r#"INSERT INTO "udb_authn"."users" ("id", "email_verified_at", "tenant_id")
               VALUES ($1::UUID, $2::TIMESTAMPTZ, $3)"#;
        let params = vec![
            LogicalValue::Null,
            LogicalValue::Null,
            LogicalValue::String("tenant-a".into()),
        ];

        assert_eq!(
            postgres_param_types(statement, &params),
            vec!["uuid", "timestamptz", ""]
        );
    }
}
