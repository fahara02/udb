//! service.rs split — policy RPC handlers (Phase G).
use super::*;

type BackendSummaryRow = (
    String,
    String,
    bool,
    String,
    String,
    String,
    std::collections::HashMap<String, String>,
    String,
);

fn policy_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

fn validate_ensure_project_id(project_id: &str) -> Result<(), Status> {
    if project_id.trim().is_empty() {
        return Err(policy_required_field(
            "project_id",
            "must be a non-empty project id",
            "project_id is required",
        ));
    }
    Ok(())
}

impl DataBrokerService {
    pub(crate) async fn list_policies_inner(
        &self,
        request: Request<PolicyListRequest>,
    ) -> Result<Response<PolicyListResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "ListPolicies");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("ListPolicies", started, Err(err));
        }
        let req = request.into_inner();
        let limit = bounded_list_limit(req.limit);
        let offset = page_offset(&req.page_token);
        let result = self
            .runtime_snapshot()
            .list_policies_page(!req.include_disabled, limit as i64, offset as i64)
            .await;
        match result {
            Ok((entries, total_count)) => {
                let policies: Vec<PolicyRecord> = entries
                    .iter()
                    .map(|e| PolicyRecord {
                        policy_id: e["policy_id"].as_i64().unwrap_or_default(),
                        effect: e["effect"].as_str().unwrap_or_default().into(),
                        service_identity: e["service_identity"].as_str().unwrap_or_default().into(),
                        tenant_id: e["tenant_id"].as_str().unwrap_or_default().into(),
                        purpose: e["purpose"].as_str().unwrap_or_default().into(),
                        message_type: e["message_type"].as_str().unwrap_or_default().into(),
                        operation: e["operation"].as_str().unwrap_or_default().into(),
                        required_scope: e["required_scope"].as_str().unwrap_or_default().into(),
                        priority: e["priority"].as_i64().unwrap_or_default() as i32,
                        enabled: e["enabled"].as_bool().unwrap_or(true),
                    })
                    .collect();
                let returned = policies.len() as i32;
                self.record_grpc(
                    "ListPolicies",
                    started,
                    Ok(Response::new(PolicyListResponse {
                        policies,
                        next_page_token: next_page_token(offset, limit, returned),
                        total_count: total_count as i32,
                    })),
                )
            }
            Err(err) => self.record_grpc("ListPolicies", started, Err(err)),
        }
    }

    pub(crate) async fn put_policy_inner(
        &self,
        request: Request<PutPolicyRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "PutPolicy");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("PutPolicy", started, Err(err));
        }
        let req = request.into_inner();
        let p = req.policy.unwrap_or_default();
        let result = self
            .runtime_snapshot()
            .put_policy(
                &p.effect,
                &p.service_identity,
                &p.tenant_id,
                &p.purpose,
                &p.message_type,
                &p.operation,
                &p.required_scope,
                p.priority,
                p.enabled,
                if p.policy_id > 0 {
                    Some(p.policy_id)
                } else {
                    None
                },
            )
            .await;
        match result {
            Ok(id) => {
                let _ = self.runtime_snapshot().write_audit_log(
                    &security.service_identity, "PutPolicy", &format!("policy/{id}"),
                    &serde_json::json!({"effect": p.effect, "operation": p.operation, "tenant_id": p.tenant_id}),
                    "ok", &security.tenant_id, "", &security.correlation_id,
                ).await;
                self.record_grpc(
                    "PutPolicy",
                    started,
                    Ok(Response::new(MutationResponse {
                        mutation_id: uuid::Uuid::new_v4().to_string(),
                        resource_uri: format!("policy/{id}"),
                        affected_rows: 1,
                        ..Default::default()
                    })),
                )
            }
            Err(err) => self.record_grpc("PutPolicy", started, Err(err)),
        }
    }

    pub(crate) async fn delete_policy_inner(
        &self,
        request: Request<PolicyRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "DeletePolicy");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("DeletePolicy", started, Err(err));
        }
        let req = request.into_inner();
        let result = self.runtime_snapshot().delete_policy(req.policy_id).await;
        match result {
            Ok(_) => {
                let _ = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &security.service_identity,
                        "DeletePolicy",
                        &format!("policy/{}", req.policy_id),
                        &serde_json::json!({"policy_id": req.policy_id}),
                        "ok",
                        &security.tenant_id,
                        "",
                        &security.correlation_id,
                    )
                    .await;
                self.record_grpc(
                    "DeletePolicy",
                    started,
                    Ok(Response::new(MutationResponse {
                        mutation_id: uuid::Uuid::new_v4().to_string(),
                        resource_uri: format!("policy/{}", req.policy_id),
                        affected_rows: 1,
                        ..Default::default()
                    })),
                )
            }
            Err(err) => self.record_grpc("DeletePolicy", started, Err(err)),
        }
    }

    pub(crate) async fn reload_policies_inner(
        &self,
        request: Request<CapabilitiesRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "ReloadPolicies");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("ReloadPolicies", started, Err(err));
        }
        // Authorization policy is sourced exclusively from the Casbin governance
        // table (`udb_authz.policy_rules`) via the AuthzService, and the shared authz
        // snapshot auto-refreshes on the authz revision fence: every policy mutation
        // invalidates the cache and a background warmer reloads it, so subsequent
        // authorize() calls already see the latest active policy set without a manual
        // reload. This RPC remains as an explicit, audited acknowledgement that the
        // live policy set tracks the governance table.
        let count = self.current_authz_snapshot().policies.len();
        let _ = self
            .runtime_snapshot()
            .write_audit_log(
                &security.service_identity,
                "ReloadPolicies",
                "policies",
                &serde_json::json!({"reloaded": count}),
                "ok",
                &security.tenant_id,
                "",
                &security.correlation_id,
            )
            .await;
        self.record_grpc(
            "ReloadPolicies",
            started,
            Ok(Response::new(MutationResponse {
                mutation_id: uuid::Uuid::new_v4().to_string(),
                resource_uri: "policies".into(),
                affected_rows: count as i64,
                ..Default::default()
            })),
        )
    }

    pub(crate) async fn lint_policies_inner(
        &self,
        request: Request<CapabilitiesRequest>,
    ) -> Result<Response<PolicyLintResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "LintPolicies");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("LintPolicies", started, Err(err));
        }
        let result = self.runtime_snapshot().lint_policies().await;
        match result {
            Ok(findings) => {
                let passed = findings.iter().any(|f| f.contains("lint passed"));
                self.record_grpc(
                    "LintPolicies",
                    started,
                    Ok(Response::new(PolicyLintResponse { passed, findings })),
                )
            }
            Err(err) => self.record_grpc("LintPolicies", started, Err(err)),
        }
    }

    pub(crate) async fn ensure_project_inner(
        &self,
        request: Request<EnsureProjectRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "EnsureProject");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("EnsureProject", started, Err(err));
        }
        let req = request.into_inner();
        let project_id = match super::handlers_catalog::resolve_catalog_mutation_project(
            &security,
            &req.project_id,
            "EnsureProject",
        ) {
            Ok(project_id) => project_id,
            Err(err) => return self.record_grpc("EnsureProject", started, Err(err)),
        };
        if let Err(err) = validate_ensure_project_id(&project_id) {
            return self.record_grpc("EnsureProject", started, Err(err));
        }
        match self
            .runtime_snapshot()
            .ensure_project(&project_id, &req.name, &req.cdc_topic_prefix)
            .await
        {
            Ok(()) => {
                let _ = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &security.service_identity,
                        "EnsureProject",
                        &project_id,
                        &serde_json::json!({"project_id": project_id.clone(), "name": req.name}),
                        "ok",
                        &security.tenant_id,
                        &project_id,
                        &security.correlation_id,
                    )
                    .await;
                self.record_grpc(
                    "EnsureProject",
                    started,
                    Ok(Response::new(MutationResponse {
                        mutation_id: project_id.clone(),
                        resource_uri: format!("udb:project:{project_id}"),
                        affected_rows: 1,
                        ..MutationResponse::default()
                    })),
                )
            }
            Err(err) => self.record_grpc("EnsureProject", started, Err(err)),
        }
    }

    pub(crate) async fn list_projects_inner(
        &self,
        request: Request<ProjectListRequest>,
    ) -> Result<Response<ProjectListResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "ListProjects");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("ListProjects", started, Err(err));
        }
        if let Err(err) =
            super::handlers_catalog::require_catalog_platform_authority("ListProjects")
        {
            return self.record_grpc("ListProjects", started, Err(err));
        }
        let req = request.into_inner();
        let limit = bounded_list_limit(req.limit);
        let offset = page_offset(&req.page_token);
        match self.runtime_snapshot().list_projects().await {
            Ok(rows) => {
                let projects: Vec<ProjectRecord> = rows
                    .iter()
                    .skip(offset as usize)
                    .take(limit as usize)
                    .map(|r| ProjectRecord {
                        project_id: r["project_id"].as_str().unwrap_or_default().to_string(),
                        name: r["name"].as_str().unwrap_or_default().to_string(),
                        cdc_topic_prefix: r["cdc_topic_prefix"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        active_catalog_version: r["active_catalog_version"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        created_at_unix: r["created_at_unix"].as_i64().unwrap_or_default(),
                    })
                    .collect();
                let returned = projects.len() as i32;
                self.record_grpc(
                    "ListProjects",
                    started,
                    Ok(Response::new(ProjectListResponse {
                        projects,
                        next_page_token: next_page_token(offset, limit, returned),
                        total_count: rows.len() as i32,
                    })),
                )
            }
            Err(err) => self.record_grpc("ListProjects", started, Err(err)),
        }
    }

    pub(crate) async fn get_admin_summary_inner(
        &self,
        request: Request<AdminSummaryRequest>,
    ) -> Result<Response<AdminSummaryResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "GetAdminSummary");
        if let Err(err) = self.require_portal_permission(&security, "GetAdminSummary", false) {
            return self.record_grpc("GetAdminSummary", started, Err(err));
        }
        let req = request.into_inner();
        let mut warnings: Vec<String> = Vec::new();
        let project_id = match super::handlers_catalog::resolve_catalog_mutation_project(
            &security,
            &req.project_id,
            "GetAdminSummary",
        ) {
            Ok(project_id) => project_id,
            Err(err) => return self.record_grpc("GetAdminSummary", started, Err(err)),
        };
        let active_catalog = match self.catalog.active_exact_for(&project_id) {
            Some(active) => active,
            None => {
                return self.record_grpc(
                    "GetAdminSummary",
                    started,
                    Err(crate::runtime::executor_utils::schema_status(
                        tonic::Code::FailedPrecondition,
                        "catalog",
                        "GetAdminSummary",
                        "catalog_project_not_active",
                        "admin summary requires an exact ACTIVE project catalog",
                    )),
                );
            }
        };

        // ── Catalog summary ───────────────────────────────────────────────────
        let catalog = {
            vec![AdminCatalogSummary {
                project_id: project_id.clone(),
                active_version: active_catalog.metadata.version.clone(),
                active_checksum: active_catalog.metadata.checksum.clone(),
                active_since: active_catalog.metadata.applied_at_unix.to_string(),
                table_count: active_catalog.manifest.tables.len() as i32,
                store_count: active_catalog.manifest.stores.len() as i32,
                pending_migration_state: String::new(),
            }]
        };

        // ── CDC summary ───────────────────────────────────────────────────────
        let cdc = {
            let slot_name = self.runtime_snapshot().config().cdc.slot_name.clone();
            let cdc_json = self
                .runtime_snapshot()
                .get_cdc_status(&slot_name, &security.tenant_id, &project_id)
                .await
                .ok();
            let paused = cdc_json
                .as_ref()
                .and_then(|v| v["paused"].as_bool())
                .unwrap_or(false);
            let is_leader = self.cdc_engine.is_some();
            let (lag, depth) = self
                .runtime_snapshot()
                .cdc_outbox_metrics()
                .await
                .unwrap_or((0.0, 0));
            // Count open DLQ events.
            let dlq_open_count = self
                .runtime_snapshot()
                .list_dlq_events("", "open", 1000, "0", &security.tenant_id, &project_id)
                .await
                .map(|rows| rows.len() as i64)
                .unwrap_or(0);
            AdminCdcSummary {
                is_leader,
                paused,
                slot_name,
                last_event_id: String::new(),
                lag_seconds: lag,
                outbox_depth: depth,
                dlq_open_count,
            }
        };

        // ── Saga summary (aggregate from DB, best-effort) ─────────────────────
        let sagas = {
            let mut summary = AdminSagaSummary::default();
            for status in &[
                "in_progress",
                "compensated",
                "failed_compensation",
                "manual_review",
                "indeterminate",
            ] {
                let count = self
                    .runtime_snapshot()
                    .list_sagas_admin(&security.tenant_id, status, "", "", 1000, 0)
                    .await
                    .map(|rows| rows.len() as i64)
                    .unwrap_or(0);
                match *status {
                    "in_progress" => summary.active = count,
                    "compensated" => summary.compensated = count,
                    "failed_compensation" => summary.failed_compensation = count,
                    "manual_review" => summary.manual_review = count,
                    "indeterminate" => summary.indeterminate = count,
                    _ => {}
                }
            }
            summary
        };

        // ── Backend summary ───────────────────────────────────────────────────
        let init = self.runtime_snapshot().init_report().clone();
        let transport_for_backend = |backend: &str| {
            crate::backend::BackendKind::from_token(backend)
                .map(|kind| self.runtime_snapshot().backend_transport_label(kind))
                .unwrap_or("unknown")
        };
        // Project-filtered like every other field in this response (CDC status,
        // DLQ events). `GetAdminSummary` resolves one project and is not gated on
        // platform authority, so listing instances an operator labelled for a
        // different project would disclose another tenant's topology — the exact
        // case `backend_instances_for_project` exists to prevent.
        let mut configured: Vec<BackendSummaryRow> = self
            .runtime_snapshot()
            .backend_instances_for_project(&project_id)
            .into_iter()
            .filter(|instance| instance.enabled)
            .map(|instance| {
                let display = format!("{}:{}", instance.backend, instance.name);
                let routing_status = if instance.connected {
                    "available"
                } else if instance.configured {
                    "degraded"
                } else {
                    "unconfigured"
                };
                (
                    display,
                    instance.backend.clone(),
                    instance.connected,
                    transport_for_backend(&instance.backend).to_string(),
                    instance.name.clone(),
                    instance.role.clone(),
                    instance.labels.clone(),
                    routing_status.to_string(),
                )
            })
            .collect();
        if configured.is_empty() {
            configured = vec![
                (
                    "postgres".to_string(),
                    "postgres".to_string(),
                    init.postgres_configured,
                    transport_for_backend("postgres").to_string(),
                    "primary".to_string(),
                    "read_write".to_string(),
                    std::collections::HashMap::new(),
                    if init.postgres_configured {
                        "available"
                    } else {
                        "unconfigured"
                    }
                    .to_string(),
                ),
                (
                    "redis".to_string(),
                    "redis".to_string(),
                    init.redis_configured,
                    transport_for_backend("redis").to_string(),
                    "default".to_string(),
                    "read_write".to_string(),
                    std::collections::HashMap::new(),
                    if init.redis_configured {
                        "available"
                    } else {
                        "unconfigured"
                    }
                    .to_string(),
                ),
                (
                    "qdrant".to_string(),
                    "qdrant".to_string(),
                    init.qdrant_configured,
                    transport_for_backend("qdrant").to_string(),
                    "default".to_string(),
                    "read_write".to_string(),
                    std::collections::HashMap::new(),
                    if init.qdrant_configured {
                        "available"
                    } else {
                        "unconfigured"
                    }
                    .to_string(),
                ),
                (
                    "s3".to_string(),
                    "s3".to_string(),
                    init.s3_configured,
                    transport_for_backend("s3").to_string(),
                    "default".to_string(),
                    "read_write".to_string(),
                    std::collections::HashMap::new(),
                    if init.s3_configured {
                        "available"
                    } else {
                        "unconfigured"
                    }
                    .to_string(),
                ),
                (
                    "mongodb".to_string(),
                    "mongodb".to_string(),
                    init.mongodb_configured,
                    transport_for_backend("mongodb").to_string(),
                    "default".to_string(),
                    "read_write".to_string(),
                    std::collections::HashMap::new(),
                    if init.mongodb_configured {
                        "available"
                    } else {
                        "unconfigured"
                    }
                    .to_string(),
                ),
                (
                    "neo4j".to_string(),
                    "neo4j".to_string(),
                    init.neo4j_configured,
                    transport_for_backend("neo4j").to_string(),
                    "default".to_string(),
                    "read_write".to_string(),
                    std::collections::HashMap::new(),
                    if init.neo4j_configured {
                        "available"
                    } else {
                        "unconfigured"
                    }
                    .to_string(),
                ),
                (
                    "clickhouse".to_string(),
                    "clickhouse".to_string(),
                    init.clickhouse_configured,
                    transport_for_backend("clickhouse").to_string(),
                    "default".to_string(),
                    "read".to_string(),
                    std::collections::HashMap::new(),
                    if init.clickhouse_configured {
                        "available"
                    } else {
                        "unconfigured"
                    }
                    .to_string(),
                ),
            ];
        }

        let mut backends = Vec::new();
        for (
            display_name,
            backend_name,
            is_configured,
            transport,
            instance_name,
            role,
            labels,
            routing_status,
        ) in configured
        {
            if !is_configured {
                continue;
            }
            let kind = crate::planning::backend::BackendKind::from_store_kind("", &backend_name);
            let cap = kind.as_ref().map(|k| k.capabilities()).unwrap_or_default();

            let (probe_ok, probe_latency_ms) = if req.with_probes {
                let result = match kind.as_ref() {
                    Some(kind) if kind.has_runtime_probe() => {
                        self.runtime_snapshot().probe_backend(kind.clone()).await
                    }
                    _ => crate::runtime::core::BackendProbeResult {
                        backend: backend_name.to_string(),
                        ok: true,
                        latency_ms: 0,
                        error: None,
                    },
                };
                (result.ok, result.latency_ms as i64)
            } else {
                (true, 0)
            };

            backends.push(AdminBackendSummary {
                backend: display_name,
                status: if probe_ok {
                    "ok".into()
                } else {
                    "degraded".into()
                },
                transport,
                consistency_model: cap.consistency_model.clone(),
                supports_transactions: cap.supports_transactions,
                supports_schema_migration: cap.supports_schema_migration,
                supports_vector_search: cap.supports_vector_search,
                supports_hybrid_search: cap.supports_hybrid_search,
                max_payload_bytes: cap.max_payload_bytes as i64,
                probe_ok,
                probe_latency_ms,
                instance_name,
                role,
                labels,
                routing_status,
            });
        }

        // ── MongoDB transport note ────────────────────────────────────────────
        if init.mongodb_configured {
            warnings.push(
                "mongodb: using Atlas Data API (HTTP); native wire-protocol not yet supported"
                    .to_string(),
            );
        }
        let replica_snapshots = self.runtime_snapshot().pg_replica_snapshots();
        if !replica_snapshots.is_empty() {
            let healthy = replica_snapshots
                .iter()
                .filter(|snapshot| snapshot.healthy)
                .count();
            warnings.push(format!(
                "postgres replicas: {}/{} healthy, strategy={}",
                healthy,
                replica_snapshots.len(),
                self.runtime_snapshot().pg_replica_strategy()
            ));
            for snapshot in replica_snapshots
                .iter()
                .filter(|snapshot| !snapshot.healthy)
                .take(3)
            {
                warnings.push(format!(
                    "postgres replica {} degraded: {}",
                    snapshot.label,
                    snapshot
                        .last_error
                        .as_deref()
                        .unwrap_or("health check failed")
                ));
            }
        }

        // ── Policy count ──────────────────────────────────────────────────────
        // The live active policy set is the shared Casbin snapshot (PG-warmed from
        // `udb_authz.policy_rules`), not a separate env-JSON ABAC vector.
        let active_policy_count = self.current_authz_snapshot().policies.len() as i32;

        let snapshot_at_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        self.record_grpc(
            "GetAdminSummary",
            started,
            Ok(Response::new(AdminSummaryResponse {
                catalog,
                cdc: Some(cdc),
                sagas: Some(sagas),
                backends,
                active_policy_count,
                snapshot_at_unix_ms,
                warnings,
            })),
        )
    }

    pub(crate) async fn list_admin_audit_logs_inner(
        &self,
        request: Request<AdminAuditLogRequest>,
    ) -> Result<Response<AdminAuditLogResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "ListAdminAuditLogs");
        if let Err(err) = self.require_portal_permission(&security, "ListAdminAuditLogs", false) {
            return self.record_grpc("ListAdminAuditLogs", started, Err(err));
        }
        let req = request.into_inner();
        let limit = bounded_list_limit(req.limit);
        let offset = page_offset(&req.page_token);
        let result = self
            .runtime_snapshot()
            .list_admin_audit_logs(
                &req.operation_filter,
                &req.actor_filter,
                &req.tenant_id_filter,
                &req.project_id_filter,
                limit as i64,
                offset as i64,
                req.redact,
            )
            .await;
        match result {
            Ok(rows) => {
                let logs = rows
                    .iter()
                    .map(|row| AdminAuditLogRecord {
                        audit_id: row["audit_id"].as_str().unwrap_or_default().to_string(),
                        actor: row["actor"].as_str().unwrap_or_default().to_string(),
                        operation: row["operation"].as_str().unwrap_or_default().to_string(),
                        target: row["target"].as_str().unwrap_or_default().to_string(),
                        request_json: admin_audit_request_json_for_response(
                            row["request_json"].clone(),
                            req.redact,
                        )
                        .to_string()
                        .into_bytes(),
                        result: row["result"].as_str().unwrap_or_default().to_string(),
                        tenant_id: row["tenant_id"].as_str().unwrap_or_default().to_string(),
                        project_id: row["project_id"].as_str().unwrap_or_default().to_string(),
                        correlation_id: row["correlation_id"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        created_at_unix: row["created_at_unix"].as_i64().unwrap_or_default(),
                        previous_hash: row["previous_hash"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        current_hash: row["current_hash"].as_str().unwrap_or_default().to_string(),
                        signer_key_id: row["signer_key_id"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        external_anchor: row["external_anchor"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                    })
                    .collect::<Vec<_>>();
                let total_count = logs.len() as i32;
                self.record_grpc(
                    "ListAdminAuditLogs",
                    started,
                    Ok(Response::new(AdminAuditLogResponse {
                        logs,
                        next_page_token: next_page_token(offset, limit, total_count),
                        total_count,
                    })),
                )
            }
            Err(err) => self.record_grpc("ListAdminAuditLogs", started, Err(err)),
        }
    }

    pub(crate) async fn verify_admin_audit_log_inner(
        &self,
        request: Request<AdminAuditVerifyRequest>,
    ) -> Result<Response<AdminAuditVerifyResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "VerifyAdminAuditLog");
        if let Err(err) = self.require_portal_permission(&security, "VerifyAdminAuditLog", false) {
            return self.record_grpc("VerifyAdminAuditLog", started, Err(err));
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .verify_admin_audit_log_chain(req.limit as i64)
            .await;
        match result {
            Ok(row) => self.record_grpc(
                "VerifyAdminAuditLog",
                started,
                Ok(Response::new(AdminAuditVerifyResponse {
                    passed: row["passed"].as_bool().unwrap_or(false),
                    checked_count: row["checked_count"].as_i64().unwrap_or_default() as i32,
                    first_broken_audit_id: row["first_broken_audit_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    reason: row["reason"].as_str().unwrap_or_default().to_string(),
                    expected_previous_hash: row["expected_previous_hash"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    actual_previous_hash: row["actual_previous_hash"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    expected_current_hash: row["expected_current_hash"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    actual_current_hash: row["actual_current_hash"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    last_hash: row["last_hash"].as_str().unwrap_or_default().to_string(),
                })),
            ),
            Err(err) => self.record_grpc("VerifyAdminAuditLog", started, Err(err)),
        }
    }
}

fn admin_audit_request_json_for_response(
    value: serde_json::Value,
    redacted: bool,
) -> serde_json::Value {
    if redacted {
        return value;
    }
    match crate::runtime::cdc::CdcConfig::current().encryption_key_resolver {
        Some(ref resolver) => crate::runtime::cdc::decrypt_encrypted_json_fields(
            value,
            resolver,
            crate::runtime::cdc::encryption::DecryptScope::Audit,
        ),
        None => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::{Code, Status};

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    #[test]
    fn ensure_project_missing_project_id_carries_field_violation() {
        let err = validate_ensure_project_id(" ")
            .expect_err("missing project_id must fail before project persistence");

        assert_eq!(err.code(), Code::InvalidArgument);
        assert_eq!(err.message(), "project_id is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "project_id");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty project id"
        );
    }
}
