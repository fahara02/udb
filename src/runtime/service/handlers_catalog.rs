//! service.rs split — catalog RPC handlers (Phase G).
use super::*;
use crate::proto::{MigrationOperationStatus, ReviewedCatalogTransitionEvidence};

fn reviewed_catalog_actor(
    security: &SecurityContext,
    principal: Option<&crate::runtime::credential_layer::VerifiedPrincipal>,
    context: Option<&crate::proto::RequestContext>,
    operation: &'static str,
) -> Result<String, Status> {
    let principal = principal.ok_or_else(|| {
        crate::runtime::executor_utils::unauthenticated_status(
            "catalog_reviewed_credential_required",
            "reviewed catalog transitions require an actual verified credential",
        )
    })?;
    if principal.subject.trim().is_empty() {
        return Err(crate::runtime::executor_utils::unauthenticated_status(
            "catalog_reviewed_actor_required",
            "reviewed catalog transitions require a verified credential subject",
        ));
    }
    if principal.tenant_id.trim().is_empty()
        || principal.tenant_id != security.tenant_id
        || principal.project_id != security.project_id
    {
        return Err(service_policy_denied(
            operation,
            "catalog_reviewed_principal_scope_mismatch",
            "reviewed catalog authority must retain the verified tenant and project identity",
        ));
    }
    if let Some(context) = context {
        if !context.tenant_id.trim().is_empty() && context.tenant_id != principal.tenant_id {
            return Err(service_policy_denied(
                operation,
                "catalog_reviewed_body_tenant_mismatch",
                "reviewed catalog body tenant must match the verified credential tenant",
            ));
        }
        resolve_catalog_mutation_project(security, &context.project_id, operation)?;
    }
    Ok(principal.subject.clone())
}

fn reviewed_catalog_transition_response(
    value: &serde_json::Value,
) -> Option<ReviewedCatalogTransitionEvidence> {
    let evidence = value.get("reviewed_catalog_transition")?.as_object()?;
    let string = |field: &str| {
        evidence
            .get(field)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let unix = |field: &str| {
        evidence
            .get(field)
            .and_then(serde_json::Value::as_i64)
            .unwrap_or_default()
    };
    Some(ReviewedCatalogTransitionEvidence {
        run_id: string("run_id"),
        tenant_id: string("tenant_id"),
        project_id: string("project_id"),
        expected_active_catalog_id: string("expected_active_catalog_id"),
        expected_active_manifest_integrity_sha256: string(
            "expected_active_manifest_integrity_sha256",
        ),
        target_manifest_integrity_sha256: string("target_manifest_integrity_sha256"),
        target_schema_checksum_sha256: string("target_schema_checksum_sha256"),
        operations_hash: string("operations_hash"),
        reviewed_operation_fingerprints: evidence
            .get("reviewed_operation_fingerprints")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .map(str::to_string)
            .collect(),
        approved_by: string("approved_by"),
        approved_at_unix: unix("approved_at_unix"),
        application_state: string("application_state"),
        applied_operations_hash: string("applied_operations_hash"),
        applied_at_unix: unix("applied_at_unix"),
        application_evidence_sha256: string("application_evidence_sha256"),
    })
}

fn migration_status_response(value: &serde_json::Value) -> MigrationStatusResponse {
    let string = |field: &str| value[field].as_str().unwrap_or_default().to_string();
    MigrationStatusResponse {
        run_id: string("run_id"),
        project_id: string("project_id"),
        catalog_version: string("catalog_version"),
        state: string("state"),
        started_at: string("started_at"),
        finished_at: string("finished_at"),
        error: string("error"),
        operations: value["operations"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|operation| MigrationOperationStatus {
                index: operation["index"]
                    .as_i64()
                    .and_then(|index| i32::try_from(index).ok())
                    .unwrap_or_default(),
                backend: operation["backend"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                resource_uri: operation["resource_uri"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                operation_kind: operation["operation_kind"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                status: operation["status"].as_str().unwrap_or_default().to_string(),
                error: operation["error"].as_str().unwrap_or_default().to_string(),
            })
            .collect(),
        reviewed_catalog_transition: reviewed_catalog_transition_response(value),
        ..Default::default()
    }
}

// Perf: `GetCatalogManifest` re-serialised the whole manifest (serde_json
// to_string_pretty over every table/column) on every call — the dominant cost of
// the RPC. The manifest changes ONLY on catalog activation, so cache the serialised
// bytes keyed by (manifest content checksum, redact flag). A content change yields a
// fresh checksum key; a handful of keys means activation churn, so we clear rather
// than grow.
#[allow(clippy::type_complexity)]
fn catalog_manifest_json_cache()
-> &'static std::sync::Mutex<std::collections::HashMap<(String, bool), std::sync::Arc<Vec<u8>>>> {
    static CACHE: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<(String, bool), std::sync::Arc<Vec<u8>>>>,
    > = std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

fn active_catalog_version_response(
    catalog: &crate::runtime::catalog::CatalogManager,
    project_id: &str,
    selector: &str,
) -> Option<CatalogVersionResponse> {
    let active = catalog.active_exact_for(project_id)?;
    let metadata = &active.metadata;
    let version = metadata.version.trim();
    let checksum = metadata.checksum.trim();
    let selector = selector.trim();
    if version.is_empty() && checksum.is_empty() {
        return None;
    }
    if !selector.is_empty() && selector != version && selector != checksum {
        return None;
    }
    let response_project_id = if project_id.trim().is_empty() {
        metadata.project_id.clone()
    } else {
        project_id.trim().to_string()
    };
    Some(CatalogVersionResponse {
        catalog_id: if checksum.is_empty() {
            version.to_string()
        } else {
            checksum.to_string()
        },
        project_id: response_project_id,
        version: version.to_string(),
        status: "ACTIVE".into(),
        checksum_sha256: checksum.to_string(),
        created_at_unix: metadata.applied_at_unix,
        ..Default::default()
    })
}

fn catalog_version_not_found_status() -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "catalog",
        "GetCatalogVersion",
        "catalog_version_not_found",
        "catalog version not found",
    )
}

fn catalog_handler_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("catalog", operation, message)
}

fn catalog_record_response(
    project_id: &str,
    record: crate::runtime::core::ProjectCatalogRecord,
    replayed: bool,
) -> CatalogVersionResponse {
    CatalogVersionResponse {
        catalog_id: record.catalog_id,
        project_id: project_id.to_string(),
        version: record.version,
        status: record.status,
        checksum_sha256: record.checksum_sha256,
        manifest_integrity_sha256: record.manifest_integrity_sha256,
        created_at_unix: record.created_at_unix,
        warnings: replayed
            .then(|| "idempotent replay returned the originally committed result".to_string())
            .into_iter()
            .collect(),
        ..Default::default()
    }
}

fn explicit_catalog_platform_authority() -> bool {
    if !crate::runtime::service::method_security::claim_context_present() {
        return false;
    }
    let claim = crate::runtime::service::method_security::current_claim_context();
    claim.roles.iter().any(|role| {
        matches!(
            role.trim().to_ascii_lowercase().as_str(),
            "platform_admin" | "udb:platform_admin" | "superadmin" | "super_admin"
        )
    }) || claim
        .scopes
        .iter()
        .any(|scope| scope.trim() == "udb:platform_admin")
}

fn request_has_explicit_catalog_platform_authority(security: &SecurityContext) -> bool {
    if crate::runtime::service::method_security::claim_context_present() {
        explicit_catalog_platform_authority()
    } else {
        // Trusted in-process callers do not have a verified-claim task-local.
        // Preserve their explicit platform seam without treating broad tenant
        // admin scopes (`udb:*` / `udb:admin`) as cross-project authority.
        security
            .scopes
            .iter()
            .any(|scope| scope.trim() == "udb:platform_admin")
    }
}

pub(crate) fn require_catalog_platform_authority(
    security: &SecurityContext,
    operation: &'static str,
) -> Result<(), Status> {
    // The data listener supplies verified SecurityContext without the native
    // listener's task-local claim carrier; absence cannot grant global access.
    if !request_has_explicit_catalog_platform_authority(security) {
        return Err(service_policy_denied(
            operation,
            "catalog_platform_authority_required",
            "explicit platform authority is required to enumerate projects across tenants",
        ));
    }
    Ok(())
}

pub(crate) fn resolve_catalog_mutation_project(
    security: &SecurityContext,
    requested_project_id: &str,
    operation: &'static str,
) -> Result<String, Status> {
    let security_project = if security.project_id.trim().is_empty() {
        crate::runtime::catalog::DEFAULT_PROJECT_ID
    } else {
        security.project_id.trim()
    };
    let project_id = if requested_project_id.trim().is_empty() {
        security_project
    } else {
        requested_project_id.trim()
    };
    let has_platform_authority = request_has_explicit_catalog_platform_authority(security);

    if crate::runtime::service::method_security::claim_context_present() {
        let claim = crate::runtime::service::method_security::current_claim_context();
        let claim_project = if claim.project_id.trim().is_empty() {
            crate::runtime::catalog::DEFAULT_PROJECT_ID
        } else {
            claim.project_id.trim()
        };
        if claim_project != project_id && !has_platform_authority {
            return Err(service_policy_denied(
                operation,
                "catalog_project_scope_mismatch",
                "catalog project_id must match the authenticated project unless explicit platform authority is present",
            ));
        }
    } else if security_project != project_id && !has_platform_authority {
        return Err(service_policy_denied(
            operation,
            "catalog_project_scope_mismatch",
            "catalog project_id must match the authenticated project unless explicit platform authority is present",
        ));
    }
    Ok(project_id.to_string())
}

fn catalog_transition_superseded_status(operation: &'static str, project_id: &str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::Aborted,
        "catalog",
        operation,
        "catalog_transition_superseded",
        format!(
            "catalog transition for project '{project_id}' was committed but a newer transition is now ACTIVE"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    #[test]
    fn reviewed_catalog_actor_requires_verified_credential_and_body_scope() {
        let security = SecurityContext {
            tenant_id: "tenant-a".into(),
            project_id: "project-a".into(),
            service_identity: "audit-display-is-not-the-actor".into(),
            scopes: vec!["udb:admin".into()],
            ..Default::default()
        };
        let absent = reviewed_catalog_actor(&security, None, None, "PlanMigration")
            .expect_err("header authority cannot manufacture an approval actor");
        assert_eq!(absent.code(), tonic::Code::Unauthenticated);
        assert_eq!(
            decode_detail(&absent).policy_decision_id,
            "catalog_reviewed_credential_required"
        );
        let principal = crate::runtime::credential_layer::VerifiedPrincipal {
            subject: "verified-person".into(),
            tenant_id: "tenant-a".into(),
            project_id: "project-a".into(),
            ..Default::default()
        };
        assert_eq!(
            reviewed_catalog_actor(&security, Some(&principal), None, "PlanMigration")
                .expect("a verified human subject has no service display dependency"),
            "verified-person"
        );
        let foreign = crate::proto::RequestContext {
            tenant_id: "tenant-b".into(),
            project_id: "project-a".into(),
            ..Default::default()
        };
        let denied = reviewed_catalog_actor(
            &security,
            Some(&principal),
            Some(&foreign),
            "ApproveMigrationPlan",
        )
        .expect_err("a body tenant cannot replace verified ownership");
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);
        assert_eq!(
            decode_detail(&denied).policy_decision_id,
            "catalog_reviewed_body_tenant_mismatch"
        );
    }

    #[test]
    fn reviewed_catalog_status_preserves_native_verified_operation_evidence() {
        let status = serde_json::json!({
            "run_id": "durable-run", "project_id": "project-a", "state": "COMPLETED",
            "operations": [{"index": 4, "backend": "postgres", "resource_uri": "postgres://target/table",
                "operation_kind": "verified_preapplied", "status": "VERIFIED", "error": ""}],
            "reviewed_catalog_transition": {"run_id": "durable-run", "tenant_id": "tenant-a",
                "project_id": "project-a", "operations_hash": "immutable-plan",
                "reviewed_operation_fingerprints": ["reviewed-index"], "approved_by": "verified-person",
                "application_state": "COMPLETED", "application_evidence_sha256": "native-target-proof"}
        });
        let response = migration_status_response(&status);
        assert_eq!(response.state, "COMPLETED");
        let operation = &response.operations[0];
        assert_eq!(operation.index, 4);
        assert_eq!(operation.operation_kind, "verified_preapplied");
        assert_eq!(operation.status, "VERIFIED");
        let evidence = response
            .reviewed_catalog_transition
            .expect("durable evidence");
        assert_eq!(evidence.application_state, "COMPLETED");
        assert_eq!(evidence.application_evidence_sha256, "native-target-proof");
        assert_eq!(evidence.approved_by, "verified-person");
        assert_eq!(evidence.reviewed_operation_fingerprints, ["reviewed-index"]);
        assert!(
            response.approval_token.is_none(),
            "status does not return bearer approval authority"
        );
    }

    #[test]
    fn catalog_project_resolution_defaults_and_denies_cross_project_without_platform_scope() {
        let default_security = SecurityContext {
            scopes: vec!["udb:admin".to_string()],
            ..SecurityContext::default()
        };
        assert_eq!(
            resolve_catalog_mutation_project(&default_security, "", "GetCapabilities")
                .expect("blank project uses the documented default"),
            crate::runtime::catalog::DEFAULT_PROJECT_ID
        );

        let bound_security = SecurityContext {
            project_id: "project-a".to_string(),
            scopes: vec!["udb:admin".to_string()],
            ..SecurityContext::default()
        };
        let denied =
            resolve_catalog_mutation_project(&bound_security, "project-b", "LookupMessageSchema")
                .expect_err("tenant admin cannot cross project authority");
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);
        let denied_detail = decode_detail(&denied);
        assert_eq!(denied_detail.kind, ErrorKind::Policy as i32);
        assert_eq!(
            denied_detail.policy_decision_id,
            "catalog_project_scope_mismatch"
        );

        let platform_security = SecurityContext {
            scopes: vec!["udb:platform_admin".to_string()],
            ..bound_security
        };
        assert_eq!(
            resolve_catalog_mutation_project(
                &platform_security,
                "project-b",
                "LookupMessageSchema",
            )
            .expect("explicit platform scope permits the requested project"),
            "project-b"
        );
    }

    #[test]
    fn active_catalog_version_response_serves_in_memory_active_catalog() {
        let catalog = crate::runtime::catalog::CatalogManager::new(CatalogManifest {
            checksum_sha256: "boot-checksum".to_string(),
            ..CatalogManifest::default()
        });

        let response = active_catalog_version_response(&catalog, "default", "")
            .expect("default active catalog should be visible before persisted versions exist");

        assert_eq!(response.project_id, "default");
        assert_eq!(response.version, "1.0.0");
        assert_eq!(response.status, "ACTIVE");
        assert_eq!(response.checksum_sha256, "boot-checksum");
        assert_eq!(response.catalog_id, "boot-checksum");
        assert!(response.manifest_integrity_sha256.is_empty());
    }

    #[test]
    fn active_catalog_version_response_honors_selector() {
        let catalog = crate::runtime::catalog::CatalogManager::new(CatalogManifest {
            checksum_sha256: "boot-checksum".to_string(),
            ..CatalogManifest::default()
        });

        assert!(
            active_catalog_version_response(&catalog, "default", "missing").is_none(),
            "non-matching selectors must still return not found"
        );
        assert!(
            active_catalog_version_response(&catalog, "default", "1.0.0").is_some(),
            "active version selector should match the in-memory active catalog"
        );
        assert!(
            active_catalog_version_response(&catalog, "default", "boot-checksum").is_some(),
            "active checksum selector should match the in-memory active catalog"
        );
        assert!(
            active_catalog_version_response(&catalog, "customer-project", "").is_none(),
            "a missing customer project must not inherit the default active catalog"
        );
    }

    #[test]
    fn catalog_version_not_found_carries_schema_detail() {
        let err = catalog_version_not_found_status();
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert_eq!(err.message(), "catalog version not found");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "catalog");
        assert_eq!(detail.operation, "GetCatalogVersion");
        assert_eq!(detail.capability_required, "catalog_version_not_found");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "catalog");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    #[test]
    fn catalog_handler_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &catalog_handler_internal_status(
                "GetCatalogManifest",
                "failed to serialize catalog manifest: invalid value",
            ),
            "GetCatalogManifest",
            "failed to serialize catalog manifest: invalid value",
        );
    }
}

impl DataBrokerService {
    // Preserve the exact verified credential for reviewed authority. The usual
    // authorization macro intentionally retains only the audit SecurityContext;
    // its display service identity is not an approval actor.
    async fn authorize_catalog_request<T: Sync>(
        &self,
        request: &Request<T>,
        method: &'static str,
    ) -> Result<
        (
            SecurityContext,
            Option<crate::runtime::credential_layer::VerifiedPrincipal>,
        ),
        Status,
    > {
        let (security, principal) =
            crate::security::security_and_verified_principal_from_request(request)?;
        self.authorize_control(&security, method).await?;
        Ok((security, principal))
    }

    /// Publish one authoritative durable ACTIVE snapshot into this node. The
    /// mutex covers both the database read and atomic map replacement, so two
    /// local handlers cannot apply snapshots out of commit order.
    pub(crate) async fn reconcile_durable_active_project_catalogs(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>, Status> {
        let _guard = self.catalog_reconcile_lock.lock().await;
        self.catalog.set_authority_fresh(false);
        let runtime = self.runtime_snapshot();
        for _ in 0..3 {
            let generation_before = runtime.catalog_reload_generation().await?;
            let records = runtime.load_all_active_project_catalogs().await?;
            let generation_after = runtime.catalog_reload_generation().await?;
            if generation_before != generation_after {
                continue;
            }
            let mut active_ids = std::collections::BTreeMap::new();
            let catalogs = records
                .into_iter()
                .map(|(project_id, record)| {
                    active_ids.insert(project_id.clone(), record.catalog_id.clone());
                    (
                        project_id,
                        record.manifest,
                        record.version,
                        record.checksum_sha256,
                        record.compatibility_level,
                        record.created_at_unix,
                    )
                })
                .collect();
            self.catalog.replace_durable_active_catalogs(catalogs);
            self.catalog_generation
                .store(generation_after, AtomicOrdering::Release);
            self.catalog.set_authority_fresh(true);
            return Ok(active_ids);
        }
        Err(crate::runtime::executor_utils::retryable_status(
            "catalog",
            "catalog_reconcile_generation_changed",
            100,
            "catalog authority changed during reconciliation; request refused until a stable durable snapshot is loaded",
        ))
    }

    pub(crate) async fn reconcile_durable_active_project_catalogs_if_changed(
        &self,
    ) -> Result<(), Status> {
        let generation = self.runtime_snapshot().catalog_reload_generation().await?;
        if generation == self.catalog_generation.load(AtomicOrdering::Acquire) {
            self.catalog.set_authority_fresh(true);
            return Ok(());
        }
        self.reconcile_durable_active_project_catalogs().await?;
        Ok(())
    }

    pub(crate) async fn get_catalog_manifest_inner(
        &self,
        request: Request<CatalogManifestRequest>,
    ) -> Result<Response<CatalogManifestResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "GetCatalogManifest");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("GetCatalogManifest", started, Err(err));
        }
        let request = request.into_inner();
        let active = match self.catalog.active_exact_for(&security.project_id) {
            Some(active) => active,
            None => {
                return self.record_grpc(
                    "GetCatalogManifest",
                    started,
                    Err(crate::runtime::executor_utils::schema_status(
                        tonic::Code::FailedPrecondition,
                        "catalog",
                        "GetCatalogManifest",
                        "catalog_project_not_active",
                        format!(
                            "project '{}' has no ACTIVE catalog, and falling back to the default project is refused because it would return another project's manifest. Stage and activate a catalog for it (StageCatalog then ActivateCatalog).",
                            security.project_id.trim()
                        ),
                    )),
                );
            }
        };
        let checksum = active.metadata.checksum.clone();
        let cache_key = (checksum.clone(), request.redact);
        // Fast path: serve the already-serialised bytes for this manifest version.
        if !checksum.is_empty() {
            if let Ok(cache) = catalog_manifest_json_cache().lock() {
                if let Some(bytes) = cache.get(&cache_key) {
                    return self.record_grpc(
                        "GetCatalogManifest",
                        started,
                        Ok(Response::new(CatalogManifestResponse {
                            manifest_json: bytes.as_ref().clone(),
                        })),
                    );
                }
            }
        }
        let manifest_value = self
            .runtime_snapshot()
            .catalog_manifest_json(&active.manifest, request.redact);
        let manifest_json = match serde_json::to_string_pretty(&manifest_value) {
            Ok(json) => json.into_bytes(),
            Err(e) => {
                return self.record_grpc(
                    "GetCatalogManifest",
                    started,
                    Err(catalog_handler_internal_status(
                        "GetCatalogManifest",
                        format!("failed to serialize catalog manifest: {e}"),
                    )),
                );
            }
        };
        if !checksum.is_empty() {
            if let Ok(mut cache) = catalog_manifest_json_cache().lock() {
                if cache.len() > 8 {
                    cache.clear();
                }
                cache.insert(cache_key, std::sync::Arc::new(manifest_json.clone()));
            }
        }
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
        let (security, principal) = match self
            .authorize_catalog_request(&request, "StageCatalog")
            .await
        {
            Ok(caller) => caller,
            Err(err) => return self.record_grpc("StageCatalog", started, Err(err)),
        };
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("StageCatalog", started, Err(err));
        }
        let req = request.into_inner();
        let project_id =
            match resolve_catalog_mutation_project(&security, &req.project_id, "StageCatalog") {
                Ok(project_id) => project_id,
                Err(err) => return self.record_grpc("StageCatalog", started, Err(err)),
            };
        let actor = if req.reviewed_migration_run_id.trim().is_empty() {
            security.service_identity.clone()
        } else {
            match reviewed_catalog_actor(
                &security,
                principal.as_ref(),
                req.context.as_ref(),
                "StageCatalog",
            ) {
                Ok(actor) => actor,
                Err(err) => return self.record_grpc("StageCatalog", started, Err(err)),
            }
        };
        let manifest = match parse_catalog_manifest_payload(&req.manifest_json) {
            Ok(manifest) => manifest,
            Err(err) => return self.record_grpc("StageCatalog", started, Err(err)),
        };
        // ValidateCatalog lints the very same payload and reports `valid: false`,
        // but StageCatalog only parsed it — so a catalog ValidateCatalog rejects
        // could still be staged, and then activated. Staging is the point where a
        // bad catalog stops being the caller's problem and becomes the broker's,
        // so it fails closed on the same lint.
        let lint = crate::generation::lint_catalog(&manifest);
        if !lint.passed {
            let details: Vec<String> = lint
                .items
                .iter()
                .filter(|item| matches!(item.severity, crate::generation::LintSeverity::Error))
                .map(|item| item.description.clone())
                .collect();
            return self.record_grpc(
                "StageCatalog",
                started,
                Err(crate::runtime::executor_utils::invalid_argument_fields(
                    format!(
                        "catalog failed validation with {} error(s); run ValidateCatalog for the full report: {}",
                        details.len(),
                        details.join("; ")
                    ),
                    [("manifest_json", "must pass catalog lint before it can be staged")],
                )),
            );
        }
        let fallback_version = self
            .catalog
            .active_exact_for(&project_id)
            .map(|active| active.metadata.version.clone())
            .unwrap_or_default();
        let version = catalog_payload_version(&req.manifest_json, &manifest, &fallback_version);
        let compatibility_level = self
            .runtime_snapshot()
            .config()
            .service
            .catalog_compatibility_level
            .clone();
        let runtime = self.runtime_snapshot();
        let project_id_for_stage = project_id.clone();
        let version_for_stage = version.clone();
        let manifest_json = req.manifest_json.clone();
        let reason = req.reason.clone();
        let actor_for_stage = actor.clone();
        let idempotency_key = req.idempotency_key.clone();
        let reviewed_run_id = req.reviewed_migration_run_id.clone();
        let tenant_id = security.tenant_id.clone();
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Admin,
                || async move {
                    if reviewed_run_id.trim().is_empty() {
                        runtime
                            .stage_catalog(
                                &project_id_for_stage,
                                &version_for_stage,
                                &manifest_json,
                                &reason,
                                &actor_for_stage,
                                &compatibility_level,
                                &idempotency_key,
                            )
                            .await
                    } else {
                        runtime
                            .stage_catalog_reviewed(
                                &project_id_for_stage,
                                &version_for_stage,
                                &manifest_json,
                                &reason,
                                &actor_for_stage,
                                &compatibility_level,
                                &idempotency_key,
                                &tenant_id,
                                &reviewed_run_id,
                            )
                            .await
                    }
                },
            )
            .await;
        let response = match result {
            Ok(result) => {
                if !result.replayed {
                    let _ = self
                        .runtime_snapshot()
                        .write_audit_log(
                            &actor,
                            "StageCatalog",
                            &result.catalog.catalog_id,
                            &serde_json::json!({"project_id": project_id, "version": version}),
                            "ok",
                            "",
                            &project_id,
                            "",
                        )
                        .await;
                }
                catalog_record_response(&project_id, result.catalog, result.replayed)
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
        let (security, principal) = match self
            .authorize_catalog_request(&request, "ActivateCatalog")
            .await
        {
            Ok(caller) => caller,
            Err(err) => return self.record_grpc("ActivateCatalog", started, Err(err)),
        };
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("ActivateCatalog", started, Err(err));
        }
        let req = request.into_inner();
        let project_id =
            match resolve_catalog_mutation_project(&security, &req.project_id, "ActivateCatalog") {
                Ok(project_id) => project_id,
                Err(err) => return self.record_grpc("ActivateCatalog", started, Err(err)),
            };
        let actor = if req.reviewed_migration_run_id.trim().is_empty() {
            security.service_identity.clone()
        } else {
            match reviewed_catalog_actor(
                &security,
                principal.as_ref(),
                req.context.as_ref(),
                "ActivateCatalog",
            ) {
                Ok(actor) => actor,
                Err(err) => return self.record_grpc("ActivateCatalog", started, Err(err)),
            }
        };
        let runtime = self.runtime_snapshot();
        let project_id_for_activate = project_id.clone();
        let version_for_activate = req.version.clone();
        let reason_for_activate = req.reason.clone();
        let actor_for_activate = actor.clone();
        let idempotency_key = req.idempotency_key.clone();
        let reviewed_run_id = req.reviewed_migration_run_id.clone();
        let tenant_id = security.tenant_id.clone();
        self.catalog.set_authority_fresh(false);
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Admin,
                || async move {
                    if reviewed_run_id.trim().is_empty() {
                        runtime
                            .activate_catalog(
                                &project_id_for_activate,
                                &version_for_activate,
                                &reason_for_activate,
                                &actor_for_activate,
                                &idempotency_key,
                            )
                            .await
                    } else {
                        runtime
                            .activate_catalog_reviewed(
                                &project_id_for_activate,
                                &version_for_activate,
                                &reason_for_activate,
                                &actor_for_activate,
                                &idempotency_key,
                                &tenant_id,
                                &reviewed_run_id,
                            )
                            .await
                    }
                },
            )
            .await;
        let response = match result {
            Ok(result) => {
                let active_ids = match self.reconcile_durable_active_project_catalogs().await {
                    Ok(active_ids) => active_ids,
                    Err(err) => return self.record_grpc("ActivateCatalog", started, Err(err)),
                };
                if !result.replayed
                    && active_ids.get(&project_id) != Some(&result.catalog.catalog_id)
                {
                    return self.record_grpc(
                        "ActivateCatalog",
                        started,
                        Err(catalog_transition_superseded_status(
                            "ActivateCatalog",
                            &project_id,
                        )),
                    );
                }
                if !result.replayed {
                    if let Err(err) = self
                        .runtime_snapshot()
                        .write_audit_log(
                            &actor,
                            "ActivateCatalog",
                            &req.version,
                            &serde_json::json!({"project_id": project_id}),
                            "ok",
                            "",
                            &project_id,
                            "",
                        )
                        .await
                    {
                        tracing::warn!(
                            error = %err,
                            project_id = %project_id,
                            "secondary ActivateCatalog audit mirror failed; durable catalog reload/activation logs remain authoritative"
                        );
                    }
                }
                catalog_record_response(&project_id, result.catalog, result.replayed)
            }
            Err(err) => {
                if let Err(reconcile_err) = self.reconcile_durable_active_project_catalogs().await {
                    return self.record_grpc("ActivateCatalog", started, Err(reconcile_err));
                }
                return self.record_grpc("ActivateCatalog", started, Err(err));
            }
        };
        self.record_grpc("ActivateCatalog", started, Ok(Response::new(response)))
    }

    pub(crate) async fn rollback_catalog_inner(
        &self,
        request: Request<CatalogVersionRequest>,
    ) -> Result<Response<CatalogVersionResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "RollbackCatalog");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("RollbackCatalog", started, Err(err));
        }
        let req = request.into_inner();
        let project_id =
            match resolve_catalog_mutation_project(&security, &req.project_id, "RollbackCatalog") {
                Ok(project_id) => project_id,
                Err(err) => return self.record_grpc("RollbackCatalog", started, Err(err)),
            };
        if req.version.trim().is_empty() {
            return self.record_grpc(
                "RollbackCatalog",
                started,
                Err(crate::runtime::executor_utils::invalid_argument_fields(
                    "rollback catalog version is required",
                    [("version", "must identify an explicit prior catalog version")],
                )),
            );
        }
        let actor = security.service_identity.clone();
        let runtime = self.runtime_snapshot();
        let project_id_for_rollback = project_id.clone();
        let version_for_rollback = req.version.clone();
        let reason_for_rollback = req.reason.clone();
        let actor_for_rollback = actor.clone();
        let idempotency_key = req.idempotency_key.clone();
        self.catalog.set_authority_fresh(false);
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
                            &idempotency_key,
                        )
                        .await
                },
            )
            .await;
        let response = match result {
            Ok(result) => {
                let active_ids = match self.reconcile_durable_active_project_catalogs().await {
                    Ok(active_ids) => active_ids,
                    Err(err) => return self.record_grpc("RollbackCatalog", started, Err(err)),
                };
                if !result.replayed
                    && active_ids.get(&project_id) != Some(&result.catalog.catalog_id)
                {
                    return self.record_grpc(
                        "RollbackCatalog",
                        started,
                        Err(catalog_transition_superseded_status(
                            "RollbackCatalog",
                            &project_id,
                        )),
                    );
                }
                if !result.replayed {
                    if let Err(err) = self
                        .runtime_snapshot()
                        .write_audit_log(
                            &actor,
                            "RollbackCatalog",
                            &req.version,
                            &serde_json::json!({"project_id": project_id, "reason": req.reason}),
                            "ok",
                            &security.tenant_id,
                            &project_id,
                            &security.correlation_id,
                        )
                        .await
                    {
                        tracing::warn!(
                            error = %err,
                            project_id = %project_id,
                            "secondary RollbackCatalog audit mirror failed; durable catalog reload/activation logs remain authoritative"
                        );
                    }
                }
                catalog_record_response(&project_id, result.catalog, result.replayed)
            }
            Err(err) => {
                if let Err(reconcile_err) = self.reconcile_durable_active_project_catalogs().await {
                    return self.record_grpc("RollbackCatalog", started, Err(reconcile_err));
                }
                return self.record_grpc("RollbackCatalog", started, Err(err));
            }
        };
        self.record_grpc("RollbackCatalog", started, Ok(Response::new(response)))
    }

    pub(crate) async fn validate_catalog_inner(
        &self,
        request: Request<StageCatalogRequest>,
    ) -> Result<Response<CatalogValidationResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "ValidateCatalog");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("ValidateCatalog", started, Err(err));
        }
        let req = request.into_inner();
        if let Err(err) =
            resolve_catalog_mutation_project(&security, &req.project_id, "ValidateCatalog")
        {
            return self.record_grpc("ValidateCatalog", started, Err(err));
        }
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
        let (started, security) = authorized_call!(self, request, "GetCatalogVersions");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("GetCatalogVersions", started, Err(err));
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
                let has_persisted_active = versions
                    .iter()
                    .any(|v| v["status"].as_str() == Some("ACTIVE"));
                let mut proto_versions: Vec<CatalogVersionResponse> = versions
                    .iter()
                    .map(|v| CatalogVersionResponse {
                        catalog_id: v["catalog_id"].as_str().unwrap_or_default().into(),
                        project_id: project_id.clone(),
                        version: v["version"].as_str().unwrap_or_default().into(),
                        status: v["status"].as_str().unwrap_or_default().into(),
                        checksum_sha256: v["checksum_sha256"].as_str().unwrap_or_default().into(),
                        manifest_integrity_sha256: v["manifest_integrity_sha256"]
                            .as_str()
                            .unwrap_or_default()
                            .into(),
                        created_at_unix: v["created_at_unix"].as_i64().unwrap_or_default(),
                        warnings: Vec::new(),
                        errors: Vec::new(),
                    })
                    .collect();
                if !has_persisted_active {
                    if let Some(active) =
                        active_catalog_version_response(&self.catalog, &project_id, "")
                    {
                        proto_versions.push(active);
                    }
                }
                let active_version = proto_versions
                    .iter()
                    .find(|v| v.status == "ACTIVE")
                    .map(|v| v.version.clone())
                    .unwrap_or_default();
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
        let (started, security) = authorized_call!(self, request, "GetCatalogVersion");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("GetCatalogVersion", started, Err(err));
        }
        let req = request.into_inner();
        let project_id =
            match resolve_catalog_mutation_project(&security, &req.project_id, "GetCatalogVersion")
            {
                Ok(project_id) => project_id,
                Err(err) => return self.record_grpc("GetCatalogVersion", started, Err(err)),
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
            let checksum = v["checksum_sha256"].as_str().unwrap_or_default();
            let status = v["status"].as_str().unwrap_or_default();
            if selector.is_empty() {
                status == "ACTIVE"
            } else {
                selector == catalog_id || selector == version || selector == checksum
            }
        });
        let Some(v) = selected else {
            if let Some(response) =
                active_catalog_version_response(&self.catalog, &project_id, &selector)
            {
                return self.record_grpc("GetCatalogVersion", started, Ok(Response::new(response)));
            }
            return self.record_grpc(
                "GetCatalogVersion",
                started,
                Err(catalog_version_not_found_status()),
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
                manifest_integrity_sha256: v["manifest_integrity_sha256"]
                    .as_str()
                    .unwrap_or_default()
                    .into(),
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
        let (security, principal) = match self
            .authorize_catalog_request(&request, "PlanMigration")
            .await
        {
            Ok(caller) => caller,
            Err(err) => return self.record_grpc("PlanMigration", started, Err(err)),
        };
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("PlanMigration", started, Err(err));
        }
        let req = request.into_inner();
        let project_id =
            match resolve_catalog_mutation_project(&security, &req.project_id, "PlanMigration") {
                Ok(project_id) => project_id,
                Err(err) => return self.record_grpc("PlanMigration", started, Err(err)),
            };
        let reviewed_request = if req.candidate_manifest_json.is_empty() {
            if !req.expected_active_catalog_id.trim().is_empty()
                || !req
                    .expected_active_manifest_integrity_sha256
                    .trim()
                    .is_empty()
                || !req.idempotency_key.trim().is_empty()
            {
                return self.record_grpc(
                    "PlanMigration",
                    started,
                    Err(crate::runtime::executor_utils::invalid_argument_fields(
                        "candidate planning options require an unstaged candidate manifest",
                        [(
                            "candidate_manifest_json",
                            "must be nonempty with candidate planning options",
                        )],
                    )),
                );
            }
            None
        } else {
            let actor = match reviewed_catalog_actor(
                &security,
                principal.as_ref(),
                req.context.as_ref(),
                "PlanMigration",
            ) {
                Ok(actor) => actor,
                Err(err) => return self.record_grpc("PlanMigration", started, Err(err)),
            };
            Some(crate::runtime::core::ReviewedCatalogPlanRequest {
                tenant_id: security.tenant_id.clone(),
                candidate_manifest_json: req.candidate_manifest_json.clone(),
                expected_active_catalog_id: req.expected_active_catalog_id.clone(),
                expected_active_manifest_integrity_sha256: req
                    .expected_active_manifest_integrity_sha256
                    .clone(),
                idempotency_key: req.idempotency_key.clone(),
                actor,
            })
        };
        let runtime = self.runtime_snapshot();
        let project_id_for_plan = project_id.clone();
        let dry_run = req.dry_run;
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Migration,
                || async move {
                    match reviewed_request {
                        Some(reviewed_request) => {
                            let run_id = runtime
                                .plan_reviewed_catalog_transition(
                                    &project_id_for_plan,
                                    &reviewed_request,
                                    dry_run,
                                )
                                .await?;
                            let status = runtime
                                .get_migration_status_for_caller(
                                    &project_id_for_plan,
                                    &run_id,
                                    &reviewed_request.tenant_id,
                                )
                                .await?;
                            if reviewed_catalog_transition_response(&status).is_none() {
                                return Err(catalog_handler_internal_status(
                                    "PlanMigration",
                                    "committed candidate plan has no durable transition evidence",
                                ));
                            }
                            Ok((run_id, Some(status)))
                        }
                        None => runtime
                            .plan_migration(&project_id_for_plan, dry_run)
                            .await
                            .map(|run_id| (run_id, None)),
                    }
                },
            )
            .await;
        match result {
            Ok((run_id, status)) => {
                let reviewed_catalog_transition = status
                    .as_ref()
                    .and_then(reviewed_catalog_transition_response);
                let operations_hash = reviewed_catalog_transition
                    .as_ref()
                    .map(|evidence| evidence.operations_hash.clone())
                    .unwrap_or_default();
                let requires_review = reviewed_catalog_transition
                    .as_ref()
                    .map(|evidence| evidence.reviewed_operation_fingerprints.clone())
                    .unwrap_or_default();
                let operations = status
                    .as_ref()
                    .and_then(|status| status["operations"].as_array())
                    .into_iter()
                    .flatten()
                    .map(|operation| {
                        operation
                            .as_str()
                            .map(str::to_string)
                            .unwrap_or_else(|| operation.to_string())
                    })
                    .collect();
                self.record_grpc(
                    "PlanMigration",
                    started,
                    Ok(Response::new(MigrationPlanResponse {
                        run_id,
                        project_id,
                        catalog_version: status
                            .as_ref()
                            .and_then(|status| status["catalog_version"].as_str())
                            .unwrap_or_default()
                            .to_string(),
                        state: if let Some(status) = &status {
                            status["state"].as_str().unwrap_or_default().to_string()
                        } else if req.dry_run {
                            "DRY_RUN".into()
                        } else {
                            "PREFLIGHT".into()
                        },
                        operations,
                        requires_review,
                        operations_hash,
                        reviewed_catalog_transition,
                        ..Default::default()
                    })),
                )
            }
            Err(err) => self.record_grpc("PlanMigration", started, Err(err)),
        }
    }

    pub(crate) async fn apply_migration_inner(
        &self,
        request: Request<MigrationApplyRequest>,
    ) -> Result<Response<MigrationStatusResponse>, Status> {
        let started = Instant::now();
        let (security, principal) = match self
            .authorize_catalog_request(&request, "ApplyMigration")
            .await
        {
            Ok(caller) => caller,
            Err(err) => return self.record_grpc("ApplyMigration", started, Err(err)),
        };
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("ApplyMigration", started, Err(err));
        }
        let req = request.into_inner();
        let project_id =
            match resolve_catalog_mutation_project(&security, &req.project_id, "ApplyMigration") {
                Ok(project_id) => project_id,
                Err(err) => return self.record_grpc("ApplyMigration", started, Err(err)),
            };
        // Only a genuine credential subject reaches the reviewed core path.
        // Empty leaves ordinary runs unchanged and refuses candidate apply.
        let verified_actor = principal
            .as_ref()
            .map(|principal| principal.subject.clone())
            .unwrap_or_default();
        let actor = if verified_actor.is_empty() {
            security.service_identity.clone()
        } else {
            verified_actor.clone()
        };
        let runtime = self.runtime_snapshot();
        let project_id_for_apply = project_id.clone();
        let run_id = req.run_id.clone();
        let approval_token = req.approval_token.clone();
        let tenant_id = security.tenant_id.clone();
        let security_for_apply = security.clone();
        let context_for_apply = req.context.clone();

        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Migration,
                || async move {
                    let current = runtime
                        .get_migration_status_for_caller(&project_id_for_apply, &run_id, &tenant_id)
                        .await?;
                    if current.get("reviewed_catalog_transition").is_some() {
                        reviewed_catalog_actor(
                            &security_for_apply,
                            principal.as_ref(),
                            context_for_apply.as_ref(),
                            "ApplyMigration",
                        )?;
                    }
                    // This read only selects request validation. The common
                    // apply body independently checks durable candidate scope,
                    // verified actor, token and base under its authority lock.
                    runtime
                        .apply_migration_for_caller(
                            &project_id_for_apply,
                            &run_id,
                            &approval_token,
                            &tenant_id,
                            &verified_actor,
                        )
                        .await?;
                    runtime
                        .get_migration_status_for_caller(&project_id_for_apply, &run_id, &tenant_id)
                        .await
                },
            )
            .await;

        match result {
            Ok(status) => {
                let _ = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &actor,
                        "ApplyMigration",
                        &req.run_id,
                        &serde_json::json!({"project_id": project_id, "run_id": req.run_id}),
                        "ok",
                        &security.tenant_id,
                        &project_id,
                        &security.correlation_id,
                    )
                    .await;
                self.record_grpc(
                    "ApplyMigration",
                    started,
                    Ok(Response::new(migration_status_response(&status))),
                )
            }
            Err(err) => self.record_grpc("ApplyMigration", started, Err(err)),
        }
    }

    pub(crate) async fn get_migration_status_inner(
        &self,
        request: Request<MigrationRunRequest>,
    ) -> Result<Response<MigrationStatusResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "GetMigrationStatus");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("GetMigrationStatus", started, Err(err));
        }
        let req = request.into_inner();
        let project_id = match resolve_catalog_mutation_project(
            &security,
            &req.project_id,
            "GetMigrationStatus",
        ) {
            Ok(project_id) => project_id,
            Err(err) => return self.record_grpc("GetMigrationStatus", started, Err(err)),
        };
        let runtime = self.runtime_snapshot();
        let project_id_for_query = project_id.clone();
        let run_id = req.run_id.clone();
        let tenant_id = security.tenant_id.clone();
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Migration,
                || async move {
                    runtime
                        .get_migration_status_for_caller(&project_id_for_query, &run_id, &tenant_id)
                        .await
                },
            )
            .await;
        match result {
            Ok(v) => self.record_grpc(
                "GetMigrationStatus",
                started,
                Ok(Response::new(migration_status_response(&v))),
            ),
            Err(err) => self.record_grpc("GetMigrationStatus", started, Err(err)),
        }
    }

    pub(crate) async fn list_migration_runs_inner(
        &self,
        request: Request<MigrationRunListRequest>,
    ) -> Result<Response<MigrationRunListResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "ListMigrationRuns");
        if let Err(err) = self.require_portal_permission(&security, "ListMigrationRuns", false) {
            return self.record_grpc("ListMigrationRuns", started, Err(err));
        }
        let req = request.into_inner();
        let project_id =
            match resolve_catalog_mutation_project(&security, &req.project_id, "ListMigrationRuns")
            {
                Ok(project_id) => project_id,
                Err(err) => return self.record_grpc("ListMigrationRuns", started, Err(err)),
            };
        let limit = bounded_list_limit(req.limit);
        let offset = page_offset(&req.page_token);
        let result = self
            .runtime_snapshot()
            .list_migration_runs(&project_id, &req.state_filter, limit as i64, offset as i64)
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
        let (security, principal) = match self
            .authorize_catalog_request(&request, "ApproveMigrationPlan")
            .await
        {
            Ok(caller) => caller,
            Err(err) => return self.record_grpc("ApproveMigrationPlan", started, Err(err)),
        };
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("ApproveMigrationPlan", started, Err(err));
        }
        let req = request.into_inner();
        let project_id = match resolve_catalog_mutation_project(
            &security,
            &req.project_id,
            "ApproveMigrationPlan",
        ) {
            Ok(project_id) => project_id,
            Err(err) => return self.record_grpc("ApproveMigrationPlan", started, Err(err)),
        };
        let reviewed = !req.expected_operations_hash.trim().is_empty()
            || !req.reviewed_operation_fingerprints.is_empty();
        let actor = if reviewed {
            match reviewed_catalog_actor(
                &security,
                principal.as_ref(),
                req.context.as_ref(),
                "ApproveMigrationPlan",
            ) {
                Ok(actor) => actor,
                Err(err) => return self.record_grpc("ApproveMigrationPlan", started, Err(err)),
            }
        } else {
            security.service_identity.clone()
        };
        let token = uuid::Uuid::new_v4().to_string();
        let runtime = self.runtime_snapshot();
        let project_id_for_approve = project_id.clone();
        let run_id = req.run_id.clone();
        let token_for_store = token.clone();
        let tenant_id = security.tenant_id.clone();
        let actor_for_approve = actor.clone();
        let expected_operations_hash = req.expected_operations_hash.clone();
        let reviewed_fingerprints = req.reviewed_operation_fingerprints.clone();
        let result = self
            .execute_with_channel(
                crate::runtime::channels::OperationChannel::Migration,
                || async move {
                    if reviewed {
                        let stored_token = runtime
                            .approve_reviewed_catalog_transition(
                                &project_id_for_approve,
                                &run_id,
                                &token_for_store,
                                &tenant_id,
                                &actor_for_approve,
                                &expected_operations_hash,
                                &reviewed_fingerprints,
                            )
                            .await?;
                        let status = runtime
                            .get_migration_status_for_caller(
                                &project_id_for_approve,
                                &run_id,
                                &tenant_id,
                            )
                            .await?;
                        Ok((stored_token, Some(status)))
                    } else {
                        runtime
                            .approve_migration_plan(
                                &project_id_for_approve,
                                &run_id,
                                &token_for_store,
                            )
                            .await
                            .map(|()| (token_for_store, None))
                    }
                },
            )
            .await;
        match result {
            Ok((token, status)) => {
                let _ = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &actor,
                        "ApproveMigrationPlan",
                        &req.run_id,
                        &serde_json::json!({
                            "project_id": project_id.clone(),
                            "run_id": req.run_id.clone(),
                            "idempotency_key": req.idempotency_key.clone(),
                            "approval_token_issued": true,
                        }),
                        "ok",
                        &security.tenant_id,
                        &project_id,
                        &security.correlation_id,
                    )
                    .await;
                let mut body = match status {
                    Some(status) => migration_status_response(&status),
                    None => MigrationStatusResponse {
                        run_id: req.run_id,
                        project_id,
                        state: "APPROVED".into(),
                        ..Default::default()
                    },
                };
                body.approval_token = Some(token.clone());
                body.applyable = Some(matches!(
                    body.state.as_str(),
                    "APPROVED" | "APPLYING" | "VERIFYING"
                ));
                let mut response = Response::new(body);
                response.metadata_mut().insert(
                    "x-udb-approval-token",
                    tonic::metadata::MetadataValue::try_from(token.as_str()).map_err(|err| {
                        catalog_handler_internal_status(
                            "ApproveMigrationPlan",
                            format!("approval token metadata failed: {err}"),
                        )
                    })?,
                );
                self.record_grpc("ApproveMigrationPlan", started, Ok(response))
            }
            Err(err) => self.record_grpc("ApproveMigrationPlan", started, Err(err)),
        }
    }
}

#[cfg(test)]
mod catalog_platform_authority_tests {
    use super::*;

    #[test]
    fn data_listener_tenant_admin_cannot_enumerate_global_projects() {
        assert!(!method_security::claim_context_present());
        for scope in ["", "*", "udb:*", "udb:admin", "udb:auth:admin"] {
            let security = SecurityContext {
                tenant_id: "tenant-one".into(),
                project_id: "project-one".into(),
                scopes: vec![scope.into()],
                ..Default::default()
            };
            assert_eq!(
                require_catalog_platform_authority(&security, "ListProjects")
                    .expect_err("tenant action scopes cannot grant global enumeration")
                    .code(),
                tonic::Code::PermissionDenied
            );
        }
        let platform = SecurityContext {
            scopes: vec!["udb:platform_admin".into()],
            ..Default::default()
        };
        require_catalog_platform_authority(&platform, "ListProjects")
            .expect("verified explicit platform scope admits global enumeration");
    }

    #[tokio::test]
    async fn native_listener_verified_claim_controls_platform_authority() {
        let fallback = SecurityContext {
            scopes: vec!["udb:platform_admin".into()],
            ..Default::default()
        };
        let tenant = method_security::VerifiedClaimContext {
            authenticated: true,
            tenant_id: "tenant-one".into(),
            project_id: "project-one".into(),
            scopes: vec!["udb:admin".into()],
            ..Default::default()
        };
        method_security::scope_claim_context_for_test(tenant, async {
            assert_eq!(
                require_catalog_platform_authority(&fallback, "ListProjects")
                    .expect_err("a present verified tenant claim overrides fallback scopes")
                    .code(),
                tonic::Code::PermissionDenied
            );
        })
        .await;
        let platform = method_security::VerifiedClaimContext {
            authenticated: true,
            roles: vec!["platform_admin".into()],
            ..Default::default()
        };
        method_security::scope_claim_context_for_test(platform, async {
            require_catalog_platform_authority(&SecurityContext::default(), "ListProjects")
                .expect("actual verified platform role admits enumeration");
        })
        .await;
    }
}
