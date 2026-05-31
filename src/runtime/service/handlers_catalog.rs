//! service.rs split — catalog RPC handlers (Phase G).
use super::*;

impl DataBrokerService {
    pub(crate) async fn get_catalog_manifest_inner(
        &self,
        request: Request<CatalogManifestRequest>,
    ) -> Result<Response<CatalogManifestResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("GetCatalogManifest", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "GetCatalogManifest").await {
            return self.record_grpc("GetCatalogManifest", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "GetCatalogManifest",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let request = request.into_inner();
        let manifest_value = self
            .runtime_snapshot()
            .catalog_manifest_json(&self.catalog.active().manifest, request.redact);
        let manifest_json = match serde_json::to_string_pretty(&manifest_value) {
            Ok(json) => json.into_bytes(),
            Err(e) => {
                return self.record_grpc(
                    "GetCatalogManifest",
                    started,
                    Err(Status::internal(format!(
                        "failed to serialize catalog manifest: {e}"
                    ))),
                );
            }
        };
        self.record_grpc(
            "GetCatalogManifest",
            started,
            Ok(Response::new(CatalogManifestResponse { manifest_json })),
        )
    }

    pub(crate) async fn stage_catalog_inner(
        &self,
        request: Request<StageCatalogRequest>,
    ) -> Result<Response<CatalogVersionResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("StageCatalog", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "StageCatalog").await {
            return self.record_grpc("StageCatalog", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "StageCatalog",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let req = request.into_inner();
        let actor = security.service_identity.clone();
        let manifest = match parse_catalog_manifest_payload(&req.manifest_json) {
            Ok(manifest) => manifest,
            Err(err) => return self.record_grpc("StageCatalog", started, Err(err)),
        };
        let version = catalog_payload_version(&req.manifest_json, &manifest);
        let compatibility_level = self
            .runtime_snapshot()
            .config()
            .service
            .catalog_compatibility_level
            .clone();
        let runtime = self.runtime_snapshot();
        let project_id_for_stage = req.project_id.clone();
        let version_for_stage = version.clone();
        let manifest_json = req.manifest_json.clone();
        let reason = req.reason.clone();
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Admin,
                || async move {
                    runtime
                        .stage_catalog(
                            &project_id_for_stage,
                            &version_for_stage,
                            &manifest_json,
                            &reason,
                        )
                        .await
                },
            )
            .await;
        let response = match result {
            Ok(catalog_id) => {
                let staged_checksum = self
                    .catalog
                    .stage_catalog(
                        manifest,
                        req.project_id.clone(),
                        version.clone(),
                        compatibility_level,
                    )
                    .await
                    .unwrap_or_default();
                let _ = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &actor,
                        "StageCatalog",
                        &catalog_id,
                        &serde_json::json!({"project_id": req.project_id, "version": version}),
                        "ok",
                        "",
                        &req.project_id,
                        "",
                    )
                    .await;
                CatalogVersionResponse {
                    catalog_id,
                    project_id: req.project_id,
                    version,
                    status: "STAGED".into(),
                    checksum_sha256: staged_checksum,
                    ..Default::default()
                }
            }
            Err(err) => return self.record_grpc("StageCatalog", started, Err(err)),
        };
        self.record_grpc("StageCatalog", started, Ok(Response::new(response)))
    }

    pub(crate) async fn activate_catalog_inner(
        &self,
        request: Request<CatalogVersionRequest>,
    ) -> Result<Response<CatalogVersionResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("ActivateCatalog", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "ActivateCatalog").await {
            return self.record_grpc("ActivateCatalog", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "ActivateCatalog",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let req = request.into_inner();
        let actor = security.service_identity.clone();
        let runtime = self.runtime_snapshot();
        let project_id_for_activate = req.project_id.clone();
        let version_for_activate = req.version.clone();
        let reason_for_activate = req.reason.clone();
        let actor_for_activate = actor.clone();
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Admin,
                || async move {
                    runtime
                        .activate_catalog(
                            &project_id_for_activate,
                            &version_for_activate,
                            &reason_for_activate,
                            &actor_for_activate,
                        )
                        .await
                },
            )
            .await;
        let response = match result {
            Ok(()) => {
                if let Ok(Some(manifest)) = self.runtime_snapshot().load_last_manifest().await {
                    let checksum = manifest.checksum_sha256.clone();
                    let active_version = if req.version.trim().is_empty() {
                        catalog_payload_version(&[], &manifest)
                    } else {
                        req.version.clone()
                    };
                    // Just in case it hasn't been staged in this exact node:
                    let _ = self
                        .catalog
                        .stage_catalog(
                            manifest,
                            req.project_id.clone(),
                            active_version,
                            "backward".to_string(),
                        )
                        .await;
                    let _ = self.catalog.activate_catalog(&checksum).await;
                }

                let _ = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &actor,
                        "ActivateCatalog",
                        &req.version,
                        &serde_json::json!({"project_id": req.project_id}),
                        "ok",
                        "",
                        &req.project_id,
                        "",
                    )
                    .await;
                CatalogVersionResponse {
                    catalog_id: req.version.clone(),
                    project_id: req.project_id,
                    version: req.version,
                    status: "ACTIVE".into(),
                    ..Default::default()
                }
            }
            Err(err) => return self.record_grpc("ActivateCatalog", started, Err(err)),
        };
        self.record_grpc("ActivateCatalog", started, Ok(Response::new(response)))
    }

    pub(crate) async fn rollback_catalog_inner(
        &self,
        request: Request<CatalogVersionRequest>,
    ) -> Result<Response<CatalogVersionResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("RollbackCatalog", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "RollbackCatalog").await {
            return self.record_grpc("RollbackCatalog", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "RollbackCatalog",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let req = request.into_inner();
        let actor = security.service_identity.clone();
        let runtime = self.runtime_snapshot();
        let project_id_for_rollback = req.project_id.clone();
        let version_for_rollback = req.version.clone();
        let reason_for_rollback = req.reason.clone();
        let actor_for_rollback = actor.clone();
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Admin,
                || async move {
                    runtime
                        .rollback_catalog(
                            &project_id_for_rollback,
                            &version_for_rollback,
                            &reason_for_rollback,
                            &actor_for_rollback,
                        )
                        .await
                },
            )
            .await;
        let response =
            match result {
                Ok(()) => {
                    if let Ok(Some(manifest)) = self.runtime_snapshot().load_last_manifest().await {
                        let checksum = manifest.checksum_sha256.clone();
                        let active_version = if req.version.trim().is_empty() {
                            catalog_payload_version(&[], &manifest)
                        } else {
                            req.version.clone()
                        };
                        let _ = self
                            .catalog
                            .stage_catalog(
                                manifest,
                                req.project_id.clone(),
                                active_version,
                                "backward".to_string(),
                            )
                            .await;
                        let _ = self.catalog.activate_catalog(&checksum).await;
                    }

                    let _ = self.runtime_snapshot().write_audit_log(
                    &actor, "RollbackCatalog", &req.version,
                    &serde_json::json!({"project_id": req.project_id, "reason": req.reason}),
                    "ok", &security.tenant_id, &req.project_id, &security.correlation_id,
                ).await;
                    CatalogVersionResponse {
                        catalog_id: req.version.clone(),
                        project_id: req.project_id,
                        version: req.version,
                        status: "ACTIVE".into(),
                        ..Default::default()
                    }
                }
                Err(err) => return self.record_grpc("RollbackCatalog", started, Err(err)),
            };
        self.record_grpc("RollbackCatalog", started, Ok(Response::new(response)))
    }

    pub(crate) async fn validate_catalog_inner(
        &self,
        request: Request<StageCatalogRequest>,
    ) -> Result<Response<CatalogValidationResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("ValidateCatalog", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "ValidateCatalog").await {
            return self.record_grpc("ValidateCatalog", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "ValidateCatalog",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let req = request.into_inner();
        let manifest = match parse_catalog_manifest_payload(&req.manifest_json) {
            Ok(manifest) => manifest,
            Err(err) => {
                return self.record_grpc(
                    "ValidateCatalog",
                    started,
                    Ok(Response::new(CatalogValidationResponse {
                        valid: false,
                        errors: vec![err.message().to_string()],
                        ..Default::default()
                    })),
                );
            }
        };
        let lint = crate::generation::lint_catalog(&manifest);
        self.record_grpc(
            "ValidateCatalog",
            started,
            Ok(Response::new(CatalogValidationResponse {
                valid: lint.passed,
                checksum_sha256: manifest.checksum_sha256,
                errors: lint
                    .items
                    .iter()
                    .filter(|item| matches!(item.severity, crate::generation::LintSeverity::Error))
                    .map(|item| item.description.clone())
                    .collect(),
                warnings: lint
                    .items
                    .iter()
                    .filter(|item| {
                        matches!(item.severity, crate::generation::LintSeverity::Warning)
                    })
                    .map(|item| item.description.clone())
                    .collect(),
            })),
        )
    }

    pub(crate) async fn get_catalog_versions_inner(
        &self,
        request: Request<CatalogManifestRequest>,
    ) -> Result<Response<CatalogVersionListResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("GetCatalogVersions", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "GetCatalogVersions").await {
            return self.record_grpc("GetCatalogVersions", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "GetCatalogVersions",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let _req = request.into_inner();
        let project_id = security.project_id.clone();
        let runtime = self.runtime_snapshot();
        let project_id_for_query = project_id.clone();
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Admin,
                || async move { runtime.get_catalog_versions(&project_id_for_query).await },
            )
            .await;
        match result {
            Ok(versions) => {
                let active_version = versions
                    .iter()
                    .find(|v| v["status"].as_str() == Some("ACTIVE"))
                    .and_then(|v| v["version"].as_str())
                    .unwrap_or_default()
                    .to_string();
                let proto_versions: Vec<CatalogVersionResponse> = versions
                    .iter()
                    .map(|v| CatalogVersionResponse {
                        catalog_id: v["catalog_id"].as_str().unwrap_or_default().into(),
                        project_id: project_id.clone(),
                        version: v["version"].as_str().unwrap_or_default().into(),
                        status: v["status"].as_str().unwrap_or_default().into(),
                        checksum_sha256: v["checksum_sha256"].as_str().unwrap_or_default().into(),
                        created_at_unix: v["created_at_unix"].as_i64().unwrap_or_default(),
                        warnings: Vec::new(),
                        errors: Vec::new(),
                    })
                    .collect();
                self.record_grpc(
                    "GetCatalogVersions",
                    started,
                    Ok(Response::new(CatalogVersionListResponse {
                        project_id,
                        versions: proto_versions,
                        active_version,
                    })),
                )
            }
            Err(err) => self.record_grpc("GetCatalogVersions", started, Err(err)),
        }
    }

    pub(crate) async fn get_catalog_version_inner(
        &self,
        request: Request<CatalogVersionRequest>,
    ) -> Result<Response<CatalogVersionResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("GetCatalogVersion", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "GetCatalogVersion").await {
            return self.record_grpc("GetCatalogVersion", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "GetCatalogVersion",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let req = request.into_inner();
        let project_id = if req.project_id.trim().is_empty() {
            security.project_id.clone()
        } else {
            req.project_id.clone()
        };
        let selector = req.version.trim().to_string();
        let runtime = self.runtime_snapshot();
        let project_id_for_query = project_id.clone();
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Admin,
                || async move { runtime.get_catalog_versions(&project_id_for_query).await },
            )
            .await;
        let versions = match result {
            Ok(versions) => versions,
            Err(err) => return self.record_grpc("GetCatalogVersion", started, Err(err)),
        };
        let selected = versions.iter().find(|v| {
            let catalog_id = v["catalog_id"].as_str().unwrap_or_default();
            let version = v["version"].as_str().unwrap_or_default();
            let status = v["status"].as_str().unwrap_or_default();
            if selector.is_empty() {
                status == "ACTIVE"
            } else {
                selector == catalog_id || selector == version
            }
        });
        let Some(v) = selected else {
            return self.record_grpc(
                "GetCatalogVersion",
                started,
                Err(Status::not_found("catalog version not found")),
            );
        };
        self.record_grpc(
            "GetCatalogVersion",
            started,
            Ok(Response::new(CatalogVersionResponse {
                catalog_id: v["catalog_id"].as_str().unwrap_or_default().into(),
                project_id,
                version: v["version"].as_str().unwrap_or_default().into(),
                status: v["status"].as_str().unwrap_or_default().into(),
                checksum_sha256: v["checksum_sha256"].as_str().unwrap_or_default().into(),
                created_at_unix: v["created_at_unix"].as_i64().unwrap_or_default(),
                ..Default::default()
            })),
        )
    }

    pub(crate) async fn plan_migration_inner(
        &self,
        request: Request<MigrationPlanRequest>,
    ) -> Result<Response<MigrationPlanResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("PlanMigration", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "PlanMigration").await {
            return self.record_grpc("PlanMigration", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "PlanMigration",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let req = request.into_inner();
        let runtime = self.runtime_snapshot();
        let project_id = req.project_id.clone();
        let dry_run = req.dry_run;
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Migration,
                || async move { runtime.plan_migration(&project_id, dry_run).await },
            )
            .await;
        match result {
            Ok(run_id) => self.record_grpc(
                "PlanMigration",
                started,
                Ok(Response::new(MigrationPlanResponse {
                    run_id,
                    project_id: req.project_id,
                    state: if req.dry_run {
                        "DRY_RUN".into()
                    } else {
                        "PREFLIGHT".into()
                    },
                    ..Default::default()
                })),
            ),
            Err(err) => self.record_grpc("PlanMigration", started, Err(err)),
        }
    }

    pub(crate) async fn apply_migration_inner(
        &self,
        request: Request<MigrationApplyRequest>,
    ) -> Result<Response<MigrationStatusResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("ApplyMigration", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "ApplyMigration").await {
            return self.record_grpc("ApplyMigration", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "ApplyMigration",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let req = request.into_inner();
        let actor = security.service_identity.clone();
        let runtime = self.runtime_snapshot();
        let project_id = req.project_id.clone();
        let run_id = req.run_id.clone();
        let approval_token = req.approval_token.clone();

        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Migration,
                || async move {
                    Ok(runtime
                        .apply_migration(&project_id, &run_id, &approval_token)
                        .await)
                },
            )
            .await;

        let result = match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(e),
        };
        match result {
            Ok(()) => {
                let _ = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &actor,
                        "ApplyMigration",
                        &req.run_id,
                        &serde_json::json!({"project_id": req.project_id, "run_id": req.run_id}),
                        "ok",
                        &security.tenant_id,
                        &req.project_id,
                        &security.correlation_id,
                    )
                    .await;
                self.record_grpc(
                    "ApplyMigration",
                    started,
                    Ok(Response::new(MigrationStatusResponse {
                        run_id: req.run_id,
                        project_id: req.project_id,
                        state: "COMPLETED".into(),
                        ..Default::default()
                    })),
                )
            }
            Err(err) => self.record_grpc("ApplyMigration", started, Err(err)),
        }
    }

    pub(crate) async fn get_migration_status_inner(
        &self,
        request: Request<MigrationRunRequest>,
    ) -> Result<Response<MigrationStatusResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("GetMigrationStatus", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "GetMigrationStatus").await {
            return self.record_grpc("GetMigrationStatus", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "GetMigrationStatus",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let req = request.into_inner();
        let runtime = self.runtime_snapshot();
        let project_id = req.project_id.clone();
        let run_id = req.run_id.clone();
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Migration,
                || async move { runtime.get_migration_status(&project_id, &run_id).await },
            )
            .await;
        match result {
            Ok(v) => self.record_grpc(
                "GetMigrationStatus",
                started,
                Ok(Response::new(MigrationStatusResponse {
                    run_id: v["run_id"].as_str().unwrap_or_default().into(),
                    project_id: v["project_id"].as_str().unwrap_or_default().into(),
                    state: v["state"].as_str().unwrap_or_default().into(),
                    error: v["error"].as_str().unwrap_or_default().into(),
                    ..Default::default()
                })),
            ),
            Err(err) => self.record_grpc("GetMigrationStatus", started, Err(err)),
        }
    }

    pub(crate) async fn list_migration_runs_inner(
        &self,
        request: Request<MigrationRunListRequest>,
    ) -> Result<Response<MigrationRunListResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("ListMigrationRuns", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "ListMigrationRuns").await {
            return self.record_grpc("ListMigrationRuns", started, Err(err));
        }
        if let Err(err) = self.require_portal_permission(&security, "ListMigrationRuns", false) {
            return self.record_grpc("ListMigrationRuns", started, Err(err));
        }
        let req = request.into_inner();
        let limit = bounded_list_limit(req.limit);
        let offset = page_offset(&req.page_token);
        let result = self
            .runtime_snapshot()
            .list_migration_runs(
                &req.project_id,
                &req.state_filter,
                limit as i64,
                offset as i64,
            )
            .await;
        match result {
            Ok(rows) => {
                let runs = rows
                    .iter()
                    .map(|v| MigrationStatusResponse {
                        run_id: v["run_id"].as_str().unwrap_or_default().into(),
                        project_id: v["project_id"].as_str().unwrap_or_default().into(),
                        catalog_version: v["catalog_version"].as_str().unwrap_or_default().into(),
                        state: v["state"].as_str().unwrap_or_default().into(),
                        started_at: v["started_at_unix"]
                            .as_i64()
                            .unwrap_or_default()
                            .to_string(),
                        finished_at: v["finished_at_unix"]
                            .as_i64()
                            .unwrap_or_default()
                            .to_string(),
                        error: v["error"].as_str().unwrap_or_default().into(),
                        ..Default::default()
                    })
                    .collect::<Vec<_>>();
                let total_count = runs.len() as i32;
                self.record_grpc(
                    "ListMigrationRuns",
                    started,
                    Ok(Response::new(MigrationRunListResponse {
                        runs,
                        next_page_token: next_page_token(offset, limit, total_count),
                        total_count,
                    })),
                )
            }
            Err(err) => self.record_grpc("ListMigrationRuns", started, Err(err)),
        }
    }

    pub(crate) async fn approve_migration_plan_inner(
        &self,
        request: Request<MigrationRunRequest>,
    ) -> Result<Response<MigrationStatusResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("ApproveMigrationPlan", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "ApproveMigrationPlan").await {
            return self.record_grpc("ApproveMigrationPlan", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "ApproveMigrationPlan",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let req = request.into_inner();
        let token = uuid::Uuid::new_v4().to_string();
        let runtime = self.runtime_snapshot();
        let project_id = req.project_id.clone();
        let run_id = req.run_id.clone();
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Migration,
                || async move { runtime.apply_migration(&project_id, &run_id, &token).await },
            )
            .await;
        match result {
            Ok(()) => {
                let _ = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &security.service_identity,
                        "ApproveMigrationPlan",
                        &req.run_id,
                        &serde_json::json!({
                            "project_id": req.project_id.clone(),
                            "run_id": req.run_id.clone(),
                            "idempotency_key": req.idempotency_key.clone(),
                        }),
                        "ok",
                        &security.tenant_id,
                        &req.project_id,
                        &security.correlation_id,
                    )
                    .await;
                self.record_grpc(
                    "ApproveMigrationPlan",
                    started,
                    Ok(Response::new(MigrationStatusResponse {
                        run_id: req.run_id,
                        project_id: req.project_id,
                        state: "COMPLETED".into(),
                        ..Default::default()
                    })),
                )
            }
            Err(err) => self.record_grpc("ApproveMigrationPlan", started, Err(err)),
        }
    }
}
