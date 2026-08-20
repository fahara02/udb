//! service.rs split — resource RPC handlers (Phase G).
use super::*;

impl DataBrokerService {
    pub(crate) async fn ensure_resource_inner(
        &self,
        request: Request<ResourceAdminRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "EnsureResource");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("EnsureResource", started, Err(err));
        }
        let req = request.into_inner();
        let metadata_context = security.request_context();
        // Capability guard: the target backend must support resource lifecycle management.
        if let Err(err) = check_backend_capability(&req.backend, "ensure_resource", |c| {
            c.supports_resource_lifecycle
        }) {
            return self.record_grpc("EnsureResource", started, Err(err));
        }
        let runtime = self.runtime_snapshot();
        let targets = match runtime.resolve_backend_targets_for_project(
            &req.backend,
            &req.spec_json,
            &metadata_context.project_id,
        ) {
            Ok(resolved) => resolved,
            Err(err) => return self.record_grpc("EnsureResource", started, Err(err)),
        };
        // A dry run answers "would this succeed?", so it must run the checks that
        // decide that: the backend-capability guard above and the project target
        // resolution. Returning before them reported success for a backend that
        // does not support resource lifecycle at all, or a project with no
        // resolvable target — the two things a dry run exists to catch. Only the
        // MUTATION is skipped.
        if req.dry_run {
            return self.record_grpc(
                "EnsureResource",
                started,
                Ok(Response::new(MutationResponse {
                    mutation_id: req.idempotency_key.clone(),
                    resource_uri: format!("{}/{}", req.backend, req.resource_name),
                    affected_rows: 0,
                    ..Default::default()
                })),
            );
        }
        let resolved_targets = targets.clone();
        // Phase 8 (§9): observe scatter-gather fan-out width per backend kind.
        self.metrics
            .inc_backend_fanout(&req.backend, targets.len() as u64);
        let resource_name = req.resource_name.clone();
        let dispatch_resource_name = resource_name.clone();
        let spec_json = req.spec_json.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::GenericDispatch,
                Some(&metadata_context),
                Some(&req.backend),
                || async move {
                    for target in targets {
                        runtime
                            .ensure_resource_backend_target(
                                &target.backend,
                                target.instance.as_deref(),
                                &dispatch_resource_name,
                                &spec_json,
                            )
                            .await?;
                    }
                    Ok(())
                },
            )
            .await;
        if result.is_ok() {
            for target in &resolved_targets {
                if crate::backend::BackendKind::from_store_kind("", &target.backend).is_some_and(
                    |kind| {
                        let cap = kind.capabilities();
                        cap.supports_vector_search || cap.supports_hybrid_search
                    },
                ) {
                    self.runtime_snapshot().record_vector_resource_backend(
                        &metadata_context.project_id,
                        &resource_name,
                        &target.backend,
                        target.instance.as_deref(),
                    );
                }
            }
            let _ = self
                .runtime_snapshot()
                .write_audit_log(
                    &security.service_identity,
                    "EnsureResource",
                    &format!("{}/{}", req.backend, req.resource_name),
                    &serde_json::json!({
                        "backend": req.backend.clone(),
                        "resource_name": req.resource_name.clone(),
                        "dry_run": false,
                        "idempotency_key": req.idempotency_key.clone(),
                    }),
                    "ok",
                    &security.tenant_id,
                    "",
                    &security.correlation_id,
                )
                .await;
        }
        match result {
            Ok(()) => {
                let response = MutationResponse {
                    mutation_id: uuid::Uuid::new_v4().to_string(),
                    resource_uri: format!("{}/{}", req.backend, req.resource_name),
                    affected_rows: 1,
                    ..Default::default()
                };
                self.record_grpc(
                    "EnsureResource",
                    started,
                    Ok(self
                        .with_mutation_response_headers(response, &metadata_context)
                        .await),
                )
            }
            Err(err) => self.record_grpc("EnsureResource", started, Err(err)),
        }
    }

    pub(crate) async fn drop_resource_inner(
        &self,
        request: Request<ResourceAdminRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "DropResource");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("DropResource", started, Err(err));
        }
        let req = request.into_inner();
        let metadata_context = security.request_context();
        if let Err(err) = guard_rls_bypass_operation("drop_resource", &req.spec_json) {
            return self.record_grpc("DropResource", started, Err(err));
        }
        // Capability guard.
        if let Err(err) = check_backend_capability(&req.backend, "drop_resource", |c| {
            c.supports_resource_lifecycle
        }) {
            return self.record_grpc("DropResource", started, Err(err));
        }
        let runtime = self.runtime_snapshot();
        let targets = match runtime.resolve_backend_targets_for_project(
            &req.backend,
            &req.spec_json,
            &metadata_context.project_id,
        ) {
            Ok(resolved) => resolved,
            Err(err) => return self.record_grpc("DropResource", started, Err(err)),
        };
        // Same as EnsureResource: a dry run must clear the RLS-bypass guard, the
        // capability guard and target resolution before it can claim the drop
        // would succeed. This one skipped `guard_rls_bypass_operation` too, so a
        // spec that would be REFUSED for attempting an RLS bypass was reported as
        // a clean dry run.
        if req.dry_run {
            return self.record_grpc(
                "DropResource",
                started,
                Ok(Response::new(MutationResponse {
                    mutation_id: req.idempotency_key.clone(),
                    resource_uri: format!("{}/{}", req.backend, req.resource_name),
                    affected_rows: 0,
                    ..Default::default()
                })),
            );
        }
        self.metrics
            .inc_backend_fanout(&req.backend, targets.len() as u64);
        let resource_name = req.resource_name.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::GenericDispatch,
                Some(&metadata_context),
                Some(&req.backend),
                || async move {
                    for target in targets {
                        runtime
                            .drop_resource_backend_target(
                                &target.backend,
                                target.instance.as_deref(),
                                &resource_name,
                            )
                            .await?;
                    }
                    Ok(())
                },
            )
            .await;
        if result.is_ok() {
            let _ = self
                .runtime_snapshot()
                .write_audit_log(
                    &security.service_identity,
                    "DropResource",
                    &format!("{}/{}", req.backend, req.resource_name),
                    &serde_json::json!({
                        "backend": req.backend.clone(),
                        "resource_name": req.resource_name.clone(),
                        "dry_run": false,
                        "idempotency_key": req.idempotency_key.clone(),
                    }),
                    "ok",
                    &security.tenant_id,
                    "",
                    &security.correlation_id,
                )
                .await;
        }
        match result {
            Ok(()) => {
                let response = MutationResponse {
                    mutation_id: uuid::Uuid::new_v4().to_string(),
                    resource_uri: format!("{}/{}", req.backend, req.resource_name),
                    affected_rows: 1,
                    ..Default::default()
                };
                self.record_grpc(
                    "DropResource",
                    started,
                    Ok(self
                        .with_mutation_response_headers(response, &metadata_context)
                        .await),
                )
            }
            Err(err) => self.record_grpc("DropResource", started, Err(err)),
        }
    }

    pub(crate) async fn list_resources_inner(
        &self,
        request: Request<ResourceAdminRequest>,
    ) -> Result<Response<ResourceListResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "ListResources");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("ListResources", started, Err(err));
        }
        let req = request.into_inner();
        let metadata_context = security.request_context();
        let runtime = self.runtime_snapshot();
        let targets = match runtime.resolve_backend_targets_for_project(
            &req.backend,
            &req.spec_json,
            &metadata_context.project_id,
        ) {
            Ok(resolved) => resolved,
            Err(err) => return self.record_grpc("ListResources", started, Err(err)),
        };
        self.metrics
            .inc_backend_fanout(&req.backend, targets.len() as u64);
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::GenericDispatch,
                Some(&metadata_context),
                Some(&req.backend),
                || async move {
                    let mut resources = Vec::new();
                    for target in targets {
                        let target_label = target
                            .instance
                            .as_ref()
                            .map(|instance| format!("{}:{instance}", target.backend))
                            .unwrap_or_else(|| target.backend.clone());
                        for resource in runtime
                            .list_resources_backend_target(
                                &target.backend,
                                target.instance.as_deref(),
                            )
                            .await?
                        {
                            resources.push(format!("{target_label}/{resource}"));
                        }
                    }
                    Ok(resources)
                },
            )
            .await;
        self.record_grpc(
            "ListResources",
            started,
            result.map(|resources| {
                Response::new(ResourceListResponse {
                    backend: req.backend,
                    resources,
                })
            }),
        )
    }
}
