//! service.rs split — data RPC handlers (Phase G).
use super::*;
use crate::runtime::core::{logical_value_to_json, postgres_param_types};

fn handlers_data_invalid_field(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field.into(), description.into())],
    )
}

fn neutral_ir_compile_failed_status(err: crate::ir::compile::CompileError) -> Status {
    handlers_data_invalid_field(
        "ir",
        "neutral IR must compile for the selected backend",
        format!("neutral IR compile failed [{}]: {err}", err.code()),
    )
}

fn raw_dispatch_disabled_status(kind: &crate::backend::BackendKind) -> Status {
    let backend = kind.as_str();
    let upper = backend.to_ascii_uppercase();
    crate::runtime::executor_utils::policy_status(
        "generic_dispatch_raw_dispatch",
        "raw_dispatch_requires_ir_envelope",
        format!(
            "raw dispatch disabled for mediated backend {backend}; supply an `ir` \
             envelope or set {}{upper}=1",
            crate::runtime::config::RAW_DISPATCH_OPT_OUT_PREFIX
        ),
    )
}

fn generic_dispatch_scope_status() -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        "GenericDispatch",
        "dispatch_scope_required",
        "scope udb:dispatch or udb:admin is required",
    )
}

fn neutral_ir_compiler_unavailable_status(
    kind: &crate::backend::BackendKind,
    operation: &str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        kind.as_str(),
        operation,
        "neutral_ir_compiler",
        format!(
            "backend '{}' has no neutral-IR compiler in this build",
            kind.as_str()
        ),
    )
}

fn generic_dispatch_compiled_capability_status(
    backend: &str,
    operation: &str,
    capability_required: &str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        backend,
        operation,
        capability_required,
        message,
    )
}

fn generic_dispatch_internal_status(
    backend: &str,
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status(backend, operation, message)
}

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
            Ok((record_set, stale_warning)) => {
                let mut response = self
                    .with_catalog_response_headers(Response::new(record_set), &metadata_context);
                // 03.2.1.4: surface the typed stale-read warning the runtime
                // served on the soft path (Eventual/BoundedStaleness, or
                // ProjectionOk non-own-write) as a response header. Reuses the
                // same `insert_ascii_header` path as the other consistency
                // headers; absent when the fence cleared or hard-failed.
                if let Some(warning) = stale_warning
                    && let Ok(json) = serde_json::to_string(&warning)
                {
                    insert_ascii_header(response.metadata_mut(), "x-udb-stale-read-warning", &json);
                }
                self.record_grpc("Select", started, Ok(response))
            }
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
            // 03.2.1.4: the V2 columnar stream has no per-batch warning channel;
            // the typed stale-read warning is surfaced only on the V1 `Select`
            // handler, so discard it here (hard-fail modes already errored).
            Ok((record_set, _stale_warning)) => {
                let batch = crate::runtime::executor_utils::record_batch_v2_from_record_set(
                    &record_set,
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
        let abac_snapshot = self.current_authz_snapshot();
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
                    // 03.2.1.4: drop the typed stale-warning side-channel — the
                    // batch stream yields bare `RecordSet`s (hard-fail modes
                    // already errored inside `select`).
                    async { runtime.select(&manifest, item, item_context).await.map(|(rs, _)| rs) },
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
        let abac_snapshot = self.current_authz_snapshot();
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
        // KEYSTONE (lane 05): capture the idempotency_key before the moved closure
        // so it threads into setup_data::delete for durable keyed dedup. Empty key
        // = keyless delete (no dedup, hot path unaffected).
        let idempotency_key = req.idempotency_key.clone();
        // G-2: capture the optional compare-and-swap precondition before the moved
        // closure, mirroring idempotency_key.
        let expected = req.expected.clone();
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
                        .delete(
                            manifest,
                            &message_type,
                            filter,
                            context,
                            idempotency_key,
                            expected,
                        )
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

    /// W7: partial update — SET named columns / apply atomic increments on the
    /// matched rows. Mirrors delete_inner's authorize/context/channel shape and
    /// threads CAS + keyed idempotency into setup_data::update.
    pub(crate) async fn update_inner(
        &self,
        request: Request<UpdateRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("Update", started, Err(e)),
        };
        let req = request.into_inner();
        let message_type = req.message_type.clone();
        let idempotency_key = req.idempotency_key.clone();
        let expected = req.expected.clone();
        let return_record = req.return_record;
        let decision_id = match self.authorize(&security, &message_type, "Update").await {
            Ok(id) => id,
            Err(err) => return self.record_grpc("Update", started, Err(err)),
        };
        let context = security.request_context_with_decision(&decision_id);
        let response_context = context.clone();
        let filter = req
            .filter
            .as_ref()
            .map(crate::runtime::executor_utils::struct_to_json)
            .unwrap_or(serde_json::Value::Null);
        let changes = req
            .changes
            .as_ref()
            .map(crate::runtime::executor_utils::struct_to_json)
            .unwrap_or(serde_json::Value::Null);
        let increments: Vec<(String, f64)> = req
            .increments
            .iter()
            .map(|increment| (increment.column.clone(), increment.delta))
            .collect();
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Write,
                || async move {
                    runtime
                        .update(
                            manifest,
                            &message_type,
                            filter,
                            changes,
                            increments,
                            context,
                            idempotency_key,
                            expected,
                            return_record,
                        )
                        .await
                },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "Update",
                started,
                Ok(self
                    .with_mutation_response_headers(res, &response_context)
                    .await),
            ),
            Err(err) => self.record_grpc("Update", started, Err(err)),
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
                Err(generic_dispatch_scope_status()),
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
                                "ensure_resource" | "drop_resource" | "mutate" | "transaction" | "put_object" | "delete_object"
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
            "ensure_resource"
                | "drop_resource"
                | "mutate"
                | "transaction"
                | "put_object"
                | "delete_object"
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
        // item 2.1 — IR mediated-by-default. When the request carried no `ir`
        // envelope, `compiled_dispatch` is None and the raw `spec_json` would flow
        // straight to the executor with no tenant/predicate injection. For backends
        // that HAVE a neutral-IR compiler this raw fall-through is now the gated
        // exception: blocked (fail-closed) in production, counted in dev.
        if compiled_dispatch.is_none() {
            enforce_raw_dispatch_gate(&resolved_backend.backend, self.metrics.as_ref())?;
        }
        let operation = compiled_dispatch
            .as_ref()
            .map(|compiled| compiled.operation.clone())
            .unwrap_or(operation);
        let resource_name = compiled_dispatch
            .as_ref()
            .and_then(|compiled| compiled.resource_name.clone())
            .unwrap_or(resource_name);
        let spec_json = compiled_dispatch
            .as_ref()
            .map(|compiled| compiled.spec_json.clone())
            .unwrap_or(spec_json);
        let write = if compiled_dispatch.is_some() {
            matches!(
                operation.as_str(),
                "ensure_resource"
                    | "drop_resource"
                    | "mutate"
                    | "transaction"
                    | "put_object"
                    | "delete_object"
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
        // 03.3.5.1: honour the read fence on the generic non-PG read chokepoint.
        // Gate on `!write` so mutations (mutate/put_object/ensure_resource/...)
        // never pay the wait, and restrict to the read operations. Empty fences
        // short-circuit (keyless reads stay free); a hard-fail mode errors; a
        // soft stale warning is discarded (raw-JSON dispatch has no warning
        // channel).
        if !write
            && matches!(
                operation.as_str(),
                "query" | "search" | "get_object" | "list_resources"
            )
        {
            let _ = runtime
                .enforce_read_fence(
                    &admission_context,
                    &target.backend,
                    target.instance.as_deref().unwrap_or("selected"),
                )
                .await?;
        }
        let executor = runtime.resolve_dispatch_executor(
            &target.backend,
            target.instance.as_deref(),
            write,
            tonic::Code::FailedPrecondition,
            Some(&admission_context),
        )?;
        let dispatch_backend = breaker_backend.clone();
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
                    let panic_backend = dispatch_backend.clone();
                    let panic_operation = operation.clone();
                    let dispatch = async move {
                    match operation.as_str() {
                        "ping" => executor
                            .ping()
                            .await
                            .map(|_| r#"{"status":"ok"}"#.to_string())
                            .map_err(|err| {
                                crate::runtime::executor_utils::backend_transport_status(
                                    &dispatch_backend,
                                    "ping",
                                    err,
                                )
                            }),
                        "probe" => match executor.probe().await {
                            Ok(probe) => serde_json::to_string(&probe)
                                .map_err(|e| {
                                    generic_dispatch_internal_status(
                                        &dispatch_backend,
                                        "probe",
                                        e.to_string(),
                                    )
                                }),
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
                                .map_err(|e| {
                                    generic_dispatch_internal_status(
                                        &dispatch_backend,
                                        "list_resources",
                                        e.to_string(),
                                    )
                                }),
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
                                    handlers_data_invalid_field(
                                        "spec_json",
                                        "put_object spec_json must be valid JSON",
                                        format!("put_object spec_json must be valid JSON: {err}"),
                                    )
                                })?;
                            let bytes =
                                crate::runtime::executor_utils::object_bytes_from_json(&spec_value)?;
                            executor.put_object(&spec_json, bytes).await
                        }
                        "delete_object" => executor
                            .delete_object(&spec_json)
                            .await
                            .map(|_| r#"{"status":"ok"}"#.to_string()),
                        other => Err(handlers_data_invalid_field(
                            "operation",
                            "must be one of ping, probe, ensure_resource, drop_resource, list_resources, query, mutate, transaction, search, get_object, put_object, or delete_object",
                            format!(
                                "unknown operation '{other}'; allowed: ping, probe, ensure_resource, drop_resource, list_resources, query, mutate, transaction, search, get_object, put_object, delete_object"
                            ),
                        )),
                    }
                    };
                    std::panic::AssertUnwindSafe(dispatch)
                        .catch_unwind()
                        .await
                        .unwrap_or_else(|_| {
                            Err(generic_dispatch_internal_status(
                                &panic_backend,
                                panic_operation,
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
    pub(crate) resource_name: Option<String>,
}

// The per-backend raw-dispatch opt-out env resolution lives in
// `crate::runtime::config::raw_dispatch_opt_out` (the startup/config boundary) so
// this hot-path handler file carries NO env reads — the `connection_manager`
// hot-path + env-confinement guards forbid `std::env` access in `handlers_*.rs`.

/// item 2.1 — gate the raw (un-mediated) generic-dispatch fall-through.
///
/// `backend` is the resolved canonical backend token. Behaviour:
/// * Non-mediated backends (KV / object stores with no neutral-IR compiler):
///   unchanged — raw dispatch is the only path, so it is always allowed.
/// * Mediated backends in **production** (`SecurityConfig::is_production()`):
///   raw dispatch is BLOCKED with `failed_precondition` unless the operator set
///   `UDB_DISPATCH_ALLOW_RAW_<BACKEND>` truthy.
/// * Mediated backends in **dev** (non-production): allowed, but increments
///   `udb_raw_dispatch_total{backend}` so drift off the mediated path is visible.
fn enforce_raw_dispatch_gate(
    backend: &str,
    metrics: &dyn crate::metrics::MetricsRecorder,
) -> Result<(), Status> {
    let Some(kind) = crate::backend::BackendKind::from_token(backend) else {
        // No recognised kind ⇒ no compiler ⇒ nothing to mediate around.
        return Ok(());
    };
    raw_dispatch_decision(
        &kind,
        crate::runtime::security::SecurityConfig::current().is_production(),
        crate::runtime::config::raw_dispatch_opt_out(backend),
        metrics,
    )
}

/// Pure decision core for the raw-dispatch gate, separated from the global env /
/// `SecurityConfig` reads so the policy is unit-testable without touching process
/// state. See [`enforce_raw_dispatch_gate`] for the resolved-from-globals wrapper.
fn raw_dispatch_decision(
    kind: &crate::backend::BackendKind,
    is_production: bool,
    opt_out: bool,
    metrics: &dyn crate::metrics::MetricsRecorder,
) -> Result<(), Status> {
    // Only gate backends that are compiler-mediated on the DATA-PLANE path
    // (`compiler_mediated_runtime_path_wired` = the single source from 2.3, which
    // already excludes KV/object stores like redis/memcached/s3 where raw IS the
    // legitimate path). Using the bare `is_mediated_backend` here would wrongly
    // gate KV backends that merely have a compiler arm.
    if !crate::backend::plugin::compiler_mediated_runtime_path_wired(kind) {
        return Ok(());
    }
    if opt_out {
        return Ok(());
    }
    if is_production {
        return Err(raw_dispatch_disabled_status(kind));
    }
    // Dev mode: permit the raw path but record the drift.
    metrics.inc_raw_dispatch_total(kind.as_str());
    Ok(())
}

fn compile_neutral_ir_dispatch(
    backend: &str,
    instance: Option<&str>,
    context: &crate::RequestContext,
    manifest: &CatalogManifest,
    requested_operation: &str,
    spec_json: &str,
) -> Result<Option<CompiledDispatchRequest>, Status> {
    let spec: serde_json::Value = serde_json::from_str(spec_json).map_err(|err| {
        handlers_data_invalid_field(
            "spec_json",
            "must be valid dispatch JSON",
            format!("invalid spec_json: {err}"),
        )
    })?;
    let Some(ir) = spec
        .get("ir")
        .or_else(|| spec.get("neutral_ir"))
        .or_else(|| spec.get("logical_operation"))
    else {
        return Ok(None);
    };
    let Some(kind) = crate::backend::BackendKind::from_token(backend) else {
        return Err(handlers_data_invalid_field(
            "backend",
            "must identify a backend with a neutral-IR compiler",
            format!("backend '{backend}' has no neutral-IR compiler"),
        ));
    };
    let ir_op = ir
        .get("op")
        .or_else(|| ir.get("operation"))
        .or_else(|| spec.get("ir_op"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            handlers_data_invalid_field(
                "ir.op",
                "neutral IR dispatch requires an operation",
                "neutral IR dispatch requires `ir.op`",
            )
        })?;
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
        return Err(handlers_data_invalid_field(
            "ir",
            "neutral IR dispatch body must be an object",
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
            .ok_or_else(|| neutral_ir_compiler_unavailable_status(kind, "neutral_ir_dispatch"))?
            .map_err(neutral_ir_compile_failed_status)
    };
    match ir_op.trim().to_ascii_lowercase().as_str() {
        "read" | "query" => {
            let op: crate::ir::LogicalRead = serde_json::from_value(payload).map_err(|err| {
                handlers_data_invalid_field(
                    "ir",
                    "must be a valid LogicalRead payload",
                    format!("invalid LogicalRead: {err}"),
                )
            })?;
            Ok((compile(CompileOperation::Read(&op))?, LogicalOpFamily::Read))
        }
        "write" | "upsert" | "mutate" => {
            let op: crate::ir::LogicalWrite = serde_json::from_value(payload).map_err(|err| {
                handlers_data_invalid_field(
                    "ir",
                    "must be a valid LogicalWrite payload",
                    format!("invalid LogicalWrite: {err}"),
                )
            })?;
            Ok((
                compile(CompileOperation::Write(&op))?,
                LogicalOpFamily::Write,
            ))
        }
        "update" => {
            let op: crate::ir::LogicalUpdate = serde_json::from_value(payload).map_err(|err| {
                handlers_data_invalid_field(
                    "ir",
                    "must be a valid LogicalUpdate payload",
                    format!("invalid LogicalUpdate: {err}"),
                )
            })?;
            Ok((
                compile(CompileOperation::Update(&op))?,
                LogicalOpFamily::Update,
            ))
        }
        "delete" => {
            let op: crate::ir::LogicalDelete = serde_json::from_value(payload).map_err(|err| {
                handlers_data_invalid_field(
                    "ir",
                    "must be a valid LogicalDelete payload",
                    format!("invalid LogicalDelete: {err}"),
                )
            })?;
            Ok((
                compile(CompileOperation::Delete(&op))?,
                LogicalOpFamily::Delete,
            ))
        }
        "search" => {
            let op: crate::ir::LogicalSearch = serde_json::from_value(payload).map_err(|err| {
                handlers_data_invalid_field(
                    "ir",
                    "must be a valid LogicalSearch payload",
                    format!("invalid LogicalSearch: {err}"),
                )
            })?;
            Ok((
                compile(CompileOperation::Search(&op))?,
                LogicalOpFamily::Search,
            ))
        }
        "resource_op" | "resource" => {
            let op: crate::ir::LogicalResourceOp =
                serde_json::from_value(payload).map_err(|err| {
                    handlers_data_invalid_field(
                        "ir",
                        "must be a valid LogicalResourceOp payload",
                        format!("invalid LogicalResourceOp: {err}"),
                    )
                })?;
            Ok((
                compile(CompileOperation::ResourceOp(&op))?,
                LogicalOpFamily::ResourceOp,
            ))
        }
        "aggregate" => {
            let op: crate::ir::LogicalAggregate =
                serde_json::from_value(payload).map_err(|err| {
                    handlers_data_invalid_field(
                        "ir",
                        "must be a valid LogicalAggregate payload",
                        format!("invalid LogicalAggregate: {err}"),
                    )
                })?;
            Ok((
                compile(CompileOperation::Aggregate(&op))?,
                LogicalOpFamily::Aggregate,
            ))
        }
        other => Err(handlers_data_invalid_field(
            "ir.op",
            "must be a supported neutral IR operation",
            format!("unsupported neutral IR op '{other}'"),
        )),
    }
}

pub(crate) fn compile_logical_read_dispatch(
    kind: &crate::backend::BackendKind,
    op: &crate::ir::LogicalRead,
    ctx: &crate::ir::compile::CompileContext<'_>,
) -> Result<CompiledDispatchRequest, Status> {
    use crate::ir::compile::{CompileOperation, compile_for_backend};
    let rendering = compile_for_backend(kind, CompileOperation::Read(op), ctx)
        .ok_or_else(|| neutral_ir_compiler_unavailable_status(kind, "compile_logical_read"))?
        .map_err(neutral_ir_compile_failed_status)?;
    compiled_rendering_to_dispatch(&rendering, LogicalOpFamily::Read)
}

pub(crate) fn compile_logical_write_dispatch(
    kind: &crate::backend::BackendKind,
    op: &crate::ir::LogicalWrite,
    ctx: &crate::ir::compile::CompileContext<'_>,
) -> Result<CompiledDispatchRequest, Status> {
    use crate::ir::compile::{CompileOperation, compile_for_backend};
    let rendering = compile_for_backend(kind, CompileOperation::Write(op), ctx)
        .ok_or_else(|| neutral_ir_compiler_unavailable_status(kind, "compile_logical_write"))?
        .map_err(neutral_ir_compile_failed_status)?;
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
        .ok_or_else(|| neutral_ir_compiler_unavailable_status(kind, "compile_logical_update"))?
        .map_err(neutral_ir_compile_failed_status)?;
    compiled_rendering_to_dispatch(&rendering, LogicalOpFamily::Update)
}

pub(crate) fn compile_logical_aggregate_dispatch(
    kind: &crate::backend::BackendKind,
    op: &crate::ir::LogicalAggregate,
    ctx: &crate::ir::compile::CompileContext<'_>,
) -> Result<CompiledDispatchRequest, Status> {
    use crate::ir::compile::{CompileOperation, compile_for_backend};
    let rendering = compile_for_backend(kind, CompileOperation::Aggregate(op), ctx)
        .ok_or_else(|| neutral_ir_compiler_unavailable_status(kind, "compile_logical_aggregate"))?
        .map_err(neutral_ir_compile_failed_status)?;
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
        .ok_or_else(|| neutral_ir_compiler_unavailable_status(kind, "compile_logical_delete"))?
        .map_err(neutral_ir_compile_failed_status)?;
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
                resource_name: None,
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
                resource_name: None,
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
                ObjectOp::DeleteObject => "delete_object",
                ObjectOp::ListObjects | ObjectOp::GeneratePresigned => {
                    return Err(generic_dispatch_compiled_capability_status(
                        "object",
                        "compiled_object_rendering",
                        "generic_dispatch_object_resource_op",
                        format!("compiled object op '{op:?}' is not exposed by GenericDispatch"),
                    ));
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
                resource_name: None,
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
    if matches!(backend, BackendKind::Qdrant) && matches!(family, LogicalOpFamily::ResourceOp) {
        return qdrant_resource_rendering_to_dispatch(method, path, body);
    }
    if matches!(backend, BackendKind::Mongodb) {
        return mongodb_rendering_to_dispatch(path, body, family);
    }
    if matches!(backend, BackendKind::Neo4j) {
        return neo4j_rendering_to_dispatch(body, family);
    }
    let operation = match family {
        LogicalOpFamily::Write | LogicalOpFamily::Update | LogicalOpFamily::Delete => "mutate",
        LogicalOpFamily::Search => "search",
        LogicalOpFamily::ResourceOp => "mutate",
        LogicalOpFamily::Read | LogicalOpFamily::Aggregate => "query",
    };
    let spec = match backend {
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
        resource_name: None,
    })
}

fn neo4j_rendering_to_dispatch(
    body: &serde_json::Value,
    family: LogicalOpFamily,
) -> Result<CompiledDispatchRequest, Status> {
    let statement = body
        .get("statements")
        .and_then(serde_json::Value::as_array)
        .and_then(|statements| statements.first())
        .ok_or_else(|| {
            handlers_data_invalid_field(
                "statements",
                "compiled Neo4j rendering must contain at least one statement",
                "compiled Neo4j rendering missing statement",
            )
        })?;
    let cypher = statement
        .get("statement")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            handlers_data_invalid_field(
                "statements.statement",
                "compiled Neo4j statement text is required",
                "compiled Neo4j statement missing text",
            )
        })?;
    let parameters = statement
        .get("parameters")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let operation = match family {
        LogicalOpFamily::Read | LogicalOpFamily::Aggregate | LogicalOpFamily::Search => "query",
        LogicalOpFamily::Write
        | LogicalOpFamily::Update
        | LogicalOpFamily::Delete
        | LogicalOpFamily::ResourceOp => "mutate",
    };
    let spec = if operation == "query" {
        serde_json::json!({
            "cypher": cypher,
            "parameters": parameters,
            "compiler_mediated": true,
        })
    } else {
        serde_json::json!({
            "operation": "cypher",
            "cypher": cypher,
            "parameters": parameters,
            "compiler_mediated": true,
        })
    };
    Ok(CompiledDispatchRequest {
        operation: operation.to_string(),
        spec_json: spec.to_string(),
        resource_name: None,
    })
}

fn mongodb_rendering_to_dispatch(
    path: &str,
    body: &serde_json::Value,
    family: LogicalOpFamily,
) -> Result<CompiledDispatchRequest, Status> {
    let (operation, spec, resource_name) = match family {
        LogicalOpFamily::Read => ("query", mongodb_rendering_body(path, body, family)?, None),
        LogicalOpFamily::Aggregate | LogicalOpFamily::Search => {
            ("query", mongodb_rendering_body(path, body, family)?, None)
        }
        LogicalOpFamily::Write | LogicalOpFamily::Update | LogicalOpFamily::Delete => {
            ("mutate", mongodb_rendering_body(path, body, family)?, None)
        }
        LogicalOpFamily::ResourceOp => mongodb_resource_rendering_to_dispatch_parts(path, body)?,
    };
    Ok(CompiledDispatchRequest {
        operation: operation.to_string(),
        spec_json: spec.to_string(),
        resource_name,
    })
}

fn qdrant_resource_rendering_to_dispatch(
    method: &crate::ir::compile::HttpMethod,
    path: &str,
    body: &serde_json::Value,
) -> Result<CompiledDispatchRequest, Status> {
    use crate::ir::compile::HttpMethod;
    let collection = qdrant_collection_from_path(path);
    let operation = match method {
        HttpMethod::Put => "ensure_resource",
        HttpMethod::Delete => "drop_resource",
        HttpMethod::Get => "list_resources",
        _ => {
            return Err(generic_dispatch_compiled_capability_status(
                "qdrant",
                "compiled_resource_rendering",
                "qdrant_resource_http_method",
                format!(
                    "compiled Qdrant resource op uses unsupported HTTP method {}",
                    http_method_token(method)
                ),
            ));
        }
    };
    if !matches!(method, HttpMethod::Get) && collection.as_deref().unwrap_or("").is_empty() {
        return Err(handlers_data_invalid_field(
            "collection",
            "compiled Qdrant resource path must include a collection name",
            "compiled Qdrant resource op missing collection name in path",
        ));
    }
    Ok(CompiledDispatchRequest {
        operation: operation.to_string(),
        spec_json: body.to_string(),
        resource_name: collection,
    })
}

fn mongodb_resource_rendering_to_dispatch_parts(
    path: &str,
    body: &serde_json::Value,
) -> Result<(&'static str, serde_json::Value, Option<String>), Status> {
    let collection = body
        .get("collection")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty());
    if path.ends_with("createCollection") {
        let collection = collection.ok_or_else(|| {
            handlers_data_invalid_field(
                "collection",
                "compiled MongoDB createCollection requires collection",
                "compiled MongoDB createCollection missing collection",
            )
        })?;
        return Ok((
            "ensure_resource",
            body.get("options")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({})),
            Some(collection.to_string()),
        ));
    }
    if path.ends_with("dropCollection") {
        let collection = collection.ok_or_else(|| {
            handlers_data_invalid_field(
                "collection",
                "compiled MongoDB dropCollection requires collection",
                "compiled MongoDB dropCollection missing collection",
            )
        })?;
        return Ok((
            "drop_resource",
            serde_json::json!({}),
            Some(collection.to_string()),
        ));
    }
    if path.ends_with("listCollections") {
        return Ok(("list_resources", serde_json::json!({}), None));
    }
    if path.ends_with("createIndex") {
        let collection = collection.ok_or_else(|| {
            handlers_data_invalid_field(
                "collection",
                "compiled MongoDB createIndex requires collection",
                "compiled MongoDB createIndex missing collection",
            )
        })?;
        let mut index = serde_json::Map::new();
        if let Some(keys) = body.get("keys").or_else(|| body.get("key")) {
            index.insert("key".to_string(), keys.clone());
        }
        if let Some(name) = body.get("name") {
            index.insert("name".to_string(), name.clone());
        }
        if let Some(unique) = body.get("unique") {
            index.insert("unique".to_string(), unique.clone());
        }
        if let Some(expire) = body
            .get("expire_after_seconds")
            .or_else(|| body.get("expireAfterSeconds"))
        {
            index.insert("expire_after_seconds".to_string(), expire.clone());
        }
        return Ok((
            "mutate",
            serde_json::json!({
                "collection": collection,
                "operation": "create_indexes",
                "indexes": [serde_json::Value::Object(index)],
                "compiler_mediated": true,
            }),
            None,
        ));
    }
    if path.ends_with("listIndexes") {
        let collection = collection.ok_or_else(|| {
            handlers_data_invalid_field(
                "collection",
                "compiled MongoDB listIndexes requires collection",
                "compiled MongoDB listIndexes missing collection",
            )
        })?;
        return Ok((
            "query",
            serde_json::json!({
                "collection": collection,
                "operation": "list_indexes",
                "compiler_mediated": true,
            }),
            None,
        ));
    }
    if path.ends_with("dropIndex") {
        let collection = collection.ok_or_else(|| {
            handlers_data_invalid_field(
                "collection",
                "compiled MongoDB dropIndex requires collection",
                "compiled MongoDB dropIndex missing collection",
            )
        })?;
        let name = body
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                handlers_data_invalid_field(
                    "name",
                    "compiled MongoDB dropIndex requires name",
                    "compiled MongoDB dropIndex missing name",
                )
            })?;
        return Ok((
            "mutate",
            serde_json::json!({
                "collection": collection,
                "operation": "drop_index",
                "name": name,
                "compiler_mediated": true,
            }),
            None,
        ));
    }
    Err(generic_dispatch_compiled_capability_status(
        "mongodb",
        "compiled_resource_rendering",
        "mongodb_resource_path",
        format!("compiled MongoDB resource op uses unsupported path '{path}'"),
    ))
}

fn mongodb_rendering_body(
    path: &str,
    body: &serde_json::Value,
    family: LogicalOpFamily,
) -> Result<serde_json::Value, Status> {
    let mut spec = body.clone();
    let serde_json::Value::Object(map) = &mut spec else {
        return Err(handlers_data_invalid_field(
            "body",
            "MongoDB compiled rendering body must be an object",
            "MongoDB compiled rendering body must be an object",
        ));
    };
    match family {
        LogicalOpFamily::Read => {}
        LogicalOpFamily::Aggregate => {
            map.insert("operation".into(), serde_json::json!("aggregate"));
        }
        LogicalOpFamily::Search if path.ends_with("aggregate") => {
            map.insert("operation".into(), serde_json::json!("aggregate"));
        }
        LogicalOpFamily::Search => {}
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
        return Err(handlers_data_invalid_field(
            "body",
            "Qdrant compiled rendering body must be an object",
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

fn inline_sql_params(
    statement: &str,
    params: &[crate::ir::value::LogicalValue],
) -> Result<String, Status> {
    let mut out = String::with_capacity(statement.len() + params.len() * 8);
    let mut params = params.iter();
    for ch in statement.chars() {
        if ch == '?' {
            let value = params.next().ok_or_else(|| {
                handlers_data_invalid_field(
                    "params",
                    "compiled SQL placeholders must match params length",
                    "compiled SQL has more placeholders than params",
                )
            })?;
            out.push_str(&clickhouse_literal(value));
        } else {
            out.push(ch);
        }
    }
    if params.next().is_some() {
        return Err(handlers_data_invalid_field(
            "params",
            "compiled SQL params length must match placeholders",
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
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

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

    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed error detail trailer");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_single_field_violation(status: &tonic::Status, field: &str, description: &str) {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert!(!detail.retryable);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_capability_detail(
        status: &tonic::Status,
        backend: &str,
        operation: &str,
        capability_required: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, backend);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability_required);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_policy_detail(status: &tonic::Status, operation: &str, policy_decision_id: &str) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_internal_detail(status: &tonic::Status, backend: &str, operation: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, backend);
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn generic_dispatch_internal_status_carries_typed_detail() {
        let err = generic_dispatch_internal_status(
            "clickhouse",
            "mutate",
            "backend operation panicked; request failed (broker stayed up)",
        );
        assert_eq!(
            err.message(),
            "backend operation panicked; request failed (broker stayed up)"
        );
        assert_internal_detail(&err, "clickhouse", "mutate");
    }

    #[test]
    fn generic_dispatch_scope_denial_carries_policy_detail() {
        let err = generic_dispatch_scope_status();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_eq!(err.message(), "scope udb:dispatch or udb:admin is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, "GenericDispatch");
        assert_eq!(detail.policy_decision_id, "dispatch_scope_required");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn neutral_ir_validation_carries_field_violations() {
        let manifest = fixture_manifest();
        let context = crate::RequestContext::default();

        let err = compile_neutral_ir_dispatch("postgres", None, &context, &manifest, "query", "{")
            .expect_err("invalid spec_json must fail");
        assert_single_field_violation(&err, "spec_json", "must be valid dispatch JSON");

        let spec = serde_json::json!({"ir": {"op": "read"}});
        let err = compile_neutral_ir_dispatch(
            "bogus",
            None,
            &context,
            &manifest,
            "query",
            &spec.to_string(),
        )
        .expect_err("unknown compiler backend must fail");
        assert_single_field_violation(
            &err,
            "backend",
            "must identify a backend with a neutral-IR compiler",
        );

        let spec = serde_json::json!({"ir": {"message_type": "acme.billing.v1.Customer"}});
        let err = compile_neutral_ir_dispatch(
            "postgres",
            None,
            &context,
            &manifest,
            "query",
            &spec.to_string(),
        )
        .expect_err("missing ir.op must fail");
        assert_single_field_violation(&err, "ir.op", "neutral IR dispatch requires an operation");

        let err = ir_payload(&serde_json::json!("bad")).expect_err("scalar ir body must fail");
        assert_single_field_violation(&err, "ir", "neutral IR dispatch body must be an object");

        let err = compile_ir_payload(
            &crate::backend::BackendKind::Postgres,
            "unknown",
            serde_json::json!({}),
            &crate::ir::compile::CompileContext::new(&manifest),
        )
        .expect_err("unsupported ir op must fail");
        assert_single_field_violation(&err, "ir.op", "must be a supported neutral IR operation");
    }

    #[test]
    fn compiled_rendering_validation_carries_field_violations() {
        use crate::ir::compile::HttpMethod;
        use crate::ir::value::LogicalValue;

        let err = neo4j_rendering_to_dispatch(&serde_json::json!({}), LogicalOpFamily::Search)
            .expect_err("missing Neo4j statement must fail");
        assert_single_field_violation(
            &err,
            "statements",
            "compiled Neo4j rendering must contain at least one statement",
        );

        let err = qdrant_resource_rendering_to_dispatch(
            &HttpMethod::Put,
            "/collections",
            &serde_json::json!({}),
        )
        .expect_err("missing Qdrant collection must fail");
        assert_single_field_violation(
            &err,
            "collection",
            "compiled Qdrant resource path must include a collection name",
        );

        let err = mongodb_resource_rendering_to_dispatch_parts(
            "/action/createCollection",
            &serde_json::json!({}),
        )
        .expect_err("missing MongoDB collection must fail");
        assert_single_field_violation(
            &err,
            "collection",
            "compiled MongoDB createCollection requires collection",
        );

        let err = mongodb_rendering_body("", &serde_json::json!("bad"), LogicalOpFamily::Read)
            .expect_err("MongoDB body must be object");
        assert_single_field_violation(
            &err,
            "body",
            "MongoDB compiled rendering body must be an object",
        );

        let err = qdrant_rendering_body("", &serde_json::json!("bad"), LogicalOpFamily::Read)
            .expect_err("Qdrant body must be object");
        assert_single_field_violation(
            &err,
            "body",
            "Qdrant compiled rendering body must be an object",
        );

        let err = inline_sql_params("SELECT ? ?", &[LogicalValue::Int(1)])
            .expect_err("missing inline param must fail");
        assert_single_field_violation(
            &err,
            "params",
            "compiled SQL placeholders must match params length",
        );

        let err = inline_sql_params("SELECT ?", &[LogicalValue::Int(1), LogicalValue::Int(2)])
            .expect_err("extra inline param must fail");
        assert_single_field_violation(
            &err,
            "params",
            "compiled SQL params length must match placeholders",
        );
    }

    #[test]
    fn compiled_rendering_capability_denials_carry_error_detail() {
        use crate::ir::compile::HttpMethod;

        let err = qdrant_resource_rendering_to_dispatch(
            &HttpMethod::Post,
            "/collections/customers_vec",
            &serde_json::json!({}),
        )
        .expect_err("unsupported Qdrant resource method must fail");
        assert_eq!(
            err.message(),
            "compiled Qdrant resource op uses unsupported HTTP method POST"
        );
        assert_capability_detail(
            &err,
            "qdrant",
            "compiled_resource_rendering",
            "qdrant_resource_http_method",
        );

        let err = mongodb_resource_rendering_to_dispatch_parts(
            "/action/renameCollection",
            &serde_json::json!({}),
        )
        .expect_err("unsupported MongoDB resource path must fail");
        assert_eq!(
            err.message(),
            "compiled MongoDB resource op uses unsupported path '/action/renameCollection'"
        );
        assert_capability_detail(
            &err,
            "mongodb",
            "compiled_resource_rendering",
            "mongodb_resource_path",
        );
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
    fn qdrant_compiled_resource_op_routes_to_resource_admin_executor() {
        use crate::ir::compile::HttpMethod;

        let ensure = qdrant_resource_rendering_to_dispatch(
            &HttpMethod::Put,
            "/collections/customers_vec",
            &serde_json::json!({"vectors": {"size": 3, "distance": "Cosine"}}),
        )
        .expect("qdrant ensure resource dispatch");
        assert_eq!(ensure.operation, "ensure_resource");
        assert_eq!(ensure.resource_name.as_deref(), Some("customers_vec"));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&ensure.spec_json).expect("ensure spec json")
                ["vectors"]["size"],
            3
        );

        let drop = qdrant_resource_rendering_to_dispatch(
            &HttpMethod::Delete,
            "/collections/customers_vec",
            &serde_json::json!({}),
        )
        .expect("qdrant drop resource dispatch");
        assert_eq!(drop.operation, "drop_resource");
        assert_eq!(drop.resource_name.as_deref(), Some("customers_vec"));

        let list = qdrant_resource_rendering_to_dispatch(
            &HttpMethod::Get,
            "/collections",
            &serde_json::json!({}),
        )
        .expect("qdrant list resource dispatch");
        assert_eq!(list.operation, "list_resources");
        assert_eq!(list.resource_name, None);
    }

    #[test]
    fn mongodb_compiled_text_search_routes_to_query_find() {
        let compiled = mongodb_rendering_to_dispatch(
            "/action/find",
            &serde_json::json!({
                "collection": "customers",
                "filter": {
                    "$text": { "$search": "Alice" },
                    "tenant_id": "tenant-a"
                },
                "projection": {
                    "id": 1,
                    "name": 1,
                    "_score": { "$meta": "textScore" }
                },
                "limit": 10
            }),
            LogicalOpFamily::Search,
        )
        .expect("MongoDB text search dispatch");
        assert_eq!(compiled.operation, "query");
        assert_eq!(compiled.resource_name, None);
        let spec: serde_json::Value =
            serde_json::from_str(&compiled.spec_json).expect("MongoDB search spec");
        assert_eq!(spec["compiler_mediated"], true);
        assert!(spec.get("operation").is_none());
        assert_eq!(spec["filter"]["tenant_id"], "tenant-a");
    }

    #[test]
    fn mongodb_compiled_index_ensure_routes_to_create_indexes_mutation() {
        let compiled = mongodb_rendering_to_dispatch(
            "/action/createIndex",
            &serde_json::json!({
                "collection": "customers",
                "name": "idx_customers_name_text",
                "keys": { "name": "text", "email": "text" }
            }),
            LogicalOpFamily::ResourceOp,
        )
        .expect("MongoDB index ensure dispatch");
        assert_eq!(compiled.operation, "mutate");
        let spec: serde_json::Value =
            serde_json::from_str(&compiled.spec_json).expect("MongoDB index spec");
        assert_eq!(spec["compiler_mediated"], true);
        assert_eq!(spec["operation"], "create_indexes");
        assert_eq!(spec["collection"], "customers");
        assert_eq!(spec["indexes"][0]["name"], "idx_customers_name_text");
        assert_eq!(spec["indexes"][0]["key"]["name"], "text");
    }

    #[test]
    fn neo4j_compiled_search_routes_to_query_cypher() {
        let compiled = neo4j_rendering_to_dispatch(
            &serde_json::json!({
                "statements": [{
                    "statement": "CALL db.index.fulltext.queryNodes('Customer_fulltext', $p0) YIELD node, score WITH node AS n, score AS _score WHERE n.`_tenant_id` = $p1 RETURN n, _score",
                    "parameters": { "p0": "Alice", "p1": "tenant-a" }
                }]
            }),
            LogicalOpFamily::Search,
        )
        .expect("Neo4j search dispatch");
        assert_eq!(compiled.operation, "query");
        let spec: serde_json::Value =
            serde_json::from_str(&compiled.spec_json).expect("Neo4j query spec");
        assert_eq!(spec["compiler_mediated"], true);
        assert_eq!(spec["parameters"]["p1"], "tenant-a");
        assert!(spec.get("operation").is_none());
    }

    #[test]
    fn neo4j_compiled_resource_op_routes_to_cypher_mutation() {
        let compiled = neo4j_rendering_to_dispatch(
            &serde_json::json!({
                "statements": [{
                    "statement": "CREATE FULLTEXT INDEX `Customer_fulltext` IF NOT EXISTS FOR (n:`Customer`) ON EACH [n.`name`, n.`email`]",
                    "parameters": {}
                }]
            }),
            LogicalOpFamily::ResourceOp,
        )
        .expect("Neo4j resource dispatch");
        assert_eq!(compiled.operation, "mutate");
        let spec: serde_json::Value =
            serde_json::from_str(&compiled.spec_json).expect("Neo4j mutation spec");
        assert_eq!(spec["compiler_mediated"], true);
        assert_eq!(spec["operation"], "cypher");
        assert!(
            spec["cypher"]
                .as_str()
                .expect("cypher text")
                .contains("CREATE FULLTEXT INDEX")
        );
    }

    #[test]
    fn raw_dispatch_gate_blocks_mediated_backend_in_production() {
        use crate::backend::BackendKind;
        use crate::metrics::{NoopMetrics, PrometheusMetrics};

        let pg = BackendKind::Postgres; // mediated (always compiled in)
        let noop = NoopMetrics;

        // Production + no opt-out ⇒ fail-closed with failed_precondition.
        let blocked = raw_dispatch_decision(&pg, true, false, &noop)
            .expect_err("production raw dispatch must be blocked");
        assert_eq!(blocked.code(), tonic::Code::FailedPrecondition);
        assert!(
            blocked
                .message()
                .contains("UDB_DISPATCH_ALLOW_RAW_POSTGRES"),
            "error must name the per-backend opt-out env: {}",
            blocked.message()
        );
        assert_policy_detail(
            &blocked,
            "generic_dispatch_raw_dispatch",
            "raw_dispatch_requires_ir_envelope",
        );

        // Production + opt-out env truthy ⇒ allowed.
        raw_dispatch_decision(&pg, true, true, &noop)
            .expect("opt-out must permit raw dispatch even in production");

        // Dev mode ⇒ allowed AND the drift counter is incremented.
        let prom = PrometheusMetrics::new().expect("build PrometheusMetrics");
        raw_dispatch_decision(&pg, false, false, &prom)
            .expect("dev-mode raw dispatch must be allowed");
        let text = prom.gather_text("");
        assert!(
            text.contains("udb_raw_dispatch_total{backend=\"postgres\"} 1"),
            "dev-mode raw dispatch must increment the drift counter:\n{text}"
        );

        // KV backend (Redis) is unaffected even in production — raw dispatch is its
        // legitimate path. Redis has a compiler arm (`is_mediated_backend` is true),
        // but it is NOT compiler-mediated on the data-plane path, so the gate must
        // skip it. This is exactly why the gate keys on
        // `compiler_mediated_runtime_path_wired`, not the bare `is_mediated_backend`.
        let redis = BackendKind::Redis;
        assert!(!crate::backend::plugin::compiler_mediated_runtime_path_wired(&redis));
        raw_dispatch_decision(&redis, true, false, &noop)
            .expect("non-data-plane-mediated backend must never be gated");
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
