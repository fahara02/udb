//! service.rs split — meta RPC handlers (Phase G).
use super::*;
use crate::protocol::BackendCapabilityDescriptor;
use crate::runtime::schema_registry::{LookupError, NegotiationOutcome, SchemaRegistry};

// Perf: `GetHealthReport` with `with_probes=true` fans out a serial round-trip to
// every configured backend (~13) plus a per-native-service store self-check — the
// dominant ~143ms cost of the RPC. The two awaited loops are now run concurrently
// (see `get_health_report_inner`), and the fully-assembled response is cached for a
// SHORT TTL keyed by (project_scope, with_probes). The TTL is deliberately tiny so a
// newly-failing required fact (bad signing key, missing PG) can't be masked for more
// than a few seconds — health must stay responsive (directive: no capability lies).
const HEALTH_REPORT_CACHE_TTL_SECS: u64 = 3;

fn catalog_version_incompatible_status(operation: &'static str, reason: String) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::FailedPrecondition,
        "catalog",
        operation,
        "catalog_version_incompatible",
        reason,
    )
}

fn message_schema_not_found_status(message_type: &str, project_id: &str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "catalog",
        "LookupMessageSchema",
        "message_schema_not_found",
        format!("message schema '{message_type}' not found for project '{project_id}'"),
    )
}

fn project_scope_mismatch_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        operation,
        "project_scope_mismatch",
        "requested project_id does not match authenticated project",
    )
}

#[allow(clippy::type_complexity)]
fn health_report_cache() -> &'static std::sync::Mutex<
    std::collections::HashMap<(String, bool), (std::time::Instant, HealthReportResponse)>,
> {
    static CACHE: std::sync::OnceLock<
        std::sync::Mutex<
            std::collections::HashMap<(String, bool), (std::time::Instant, HealthReportResponse)>,
        >,
    > = std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

impl DataBrokerService {
    pub(crate) async fn get_capabilities_inner(
        &self,
        request: Request<CapabilitiesRequest>,
    ) -> Result<Response<CapabilitiesResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "GetCapabilities");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("GetCapabilities", started, Err(err));
        }
        let mut enabled_backends = self.runtime_snapshot().enabled_backend_names();
        let enabled_set: std::collections::HashSet<String> =
            enabled_backends.iter().cloned().collect();
        let mut degraded_backends: Vec<String> = crate::backend::all_plugins()
            .into_iter()
            .map(|plugin| plugin.kind().as_str().to_string())
            .filter(|backend| !enabled_set.contains(backend))
            .collect();
        enabled_backends.sort();
        enabled_backends.dedup();
        degraded_backends.sort();
        degraded_backends.dedup();
        let manifest_checksum = if !self.catalog.active().manifest.checksum_sha256.is_empty() {
            self.catalog.active().manifest.checksum_sha256.clone()
        } else {
            // Fallback for older manifests built without a stored checksum: hash the
            // full manifest instead of only message names so schema changes are visible.
            use sha2::Digest;
            let mut hasher = sha2::Sha256::new();
            if let Ok(bytes) = serde_json::to_vec(&self.catalog.active().manifest) {
                hasher.update(bytes);
            }
            format!("{:x}", hasher.finalize())
        };
        let req = request.into_inner();
        let sys_cfg = crate::runtime::system::SystemCatalogConfig::default();
        let sys_schema = &sys_cfg.cdc.system_schema;
        let qi = |s: &str| format!("\"{s}\"");
        let qrel = |schema: &str, table: &str| format!("{}.{}", qi(schema), qi(table));
        let mut system_catalog_relations: Vec<String> = vec![
            qrel(sys_schema, &sys_cfg.cdc.outbox_table),
            qrel(sys_schema, &sys_cfg.cdc.offsets_table),
            qrel(sys_schema, &sys_cfg.cdc.lock_log_table),
            qrel(sys_schema, &sys_cfg.saga_table),
            qrel(&sys_cfg.abac_schema, &sys_cfg.abac_table),
        ];
        // When a project_id is provided, include project-specific catalog information.
        let project_scope = req.project_id.trim().to_string();
        if !project_scope.is_empty()
            && let Ok(versions) = self
                .runtime_snapshot()
                .get_catalog_versions(&project_scope)
                .await
        {
            for v in &versions {
                let ver = v["version"].as_str().unwrap_or("unknown");
                system_catalog_relations.push(format!("project:{project_scope}:catalog:{ver}"));
            }
        }
        let supported_rpcs: Vec<String> = SUPPORTED_RPC_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect();

        // ── V2 protocol negotiation (additive fields 9/10) ──────────────────
        // Derive the negotiation block from the same constants the V1 fields
        // use, so a V2-aware client can feature-detect without parsing the V1
        // surface. This block never alters fields 1-8.
        //
        // Compression: the DataBroker server does not currently configure
        // tonic transport compression negotiation (no accept_compressed /
        // send_compressed on the server builder), so we advertise none. If
        // compression is wired later, populate this from that config.
        //
        // Max message bytes: the server relies on tonic's transport defaults
        // (no max_decoding_message_size / max_encoding_message_size override),
        // so we report 0 ("not advertised / use transport default").
        // urgent_fix #37: annotate the compile-time capability matrix with which
        // backends are actually CONFIGURED on this node (have a live instance/DSN),
        // so `GetCapabilities` advertises real deployment capability, not merely
        // "the binary was compiled with this backend".
        let configured_backends: std::collections::HashSet<String> = {
            let snap = self.runtime_snapshot();
            snap.backend_instances()
                .iter()
                .map(|inst| inst.backend.clone())
                .collect()
        };
        let backend_caps = crate::backend::capability_matrix_configured(&configured_backends);
        let backend_protocol_support: Vec<crate::proto::BackendProtocolSupport> = backend_caps
            .iter()
            .map(|entry| {
                let kind = crate::backend::BackendKind::from_token(&entry.backend);
                let v2 = kind.map(|kind| kind.capabilities_v2());
                let supports_streaming_reads =
                    v2.as_ref().is_some_and(|cap| cap.supports_streaming);
                let supports_object_streaming = v2.as_ref().is_some_and(|cap| cap.is_object_store);
                crate::proto::BackendProtocolSupport {
                    backend: entry.backend.clone(),
                    supports_streaming_reads,
                    supports_object_streaming,
                    // Empty => inherits server-wide ProtocolSupport.encodings.
                    encodings: Vec::new(),
                }
            })
            .collect();
        let protocol_support = Some(crate::proto::ProtocolSupport {
            // Single-version server today: min == max == UDB_PROTOCOL_VERSION.
            min_protocol_version: UDB_PROTOCOL_VERSION.to_string(),
            max_protocol_version: UDB_PROTOCOL_VERSION.to_string(),
            // V1 row encoding plus the additive typed columnar encoding served
            // by the SelectV2 RPC (A.4). Clients pick V2 only when they support
            // it and otherwise fall back to record_set_v1.
            encodings: vec!["record_set_v1".to_string(), "record_batch_v2".to_string()],
            // No transport compression negotiation configured (see comment).
            compression: Vec::new(),
            // The runtime provides default streaming impls for query reads and
            // object payloads (see executors::QueryExecutor::query_stream /
            // ObjectExecutor::get_object_stream).
            supports_streaming_reads: true,
            supports_object_streaming: true,
            // 0 == transport default (no explicit server-side override).
            max_recv_message_bytes: 0,
            max_send_message_bytes: 0,
            supported_rpcs: supported_rpcs.clone(),
        });
        let native_services: Vec<crate::proto::NativeServiceStatus> =
            crate::runtime::service::native_registry::resolved_native_service_statuses(
                self.runtime_snapshot().config(),
            )
            .into_iter()
            .map(crate::runtime::service::native_registry::status_to_proto)
            .collect();

        let startup_summary = format!(
            "[UDB] capabilities: {} table(s), {} store(s), {} backend(s) enabled, {} degraded",
            self.catalog.active().manifest.tables.len(),
            self.catalog.active().manifest.stores.len(),
            enabled_backends.len(),
            degraded_backends.len()
        );
        tracing::info!("{startup_summary}");

        self.record_grpc(
            "GetCapabilities",
            started,
            Ok(Response::new(CapabilitiesResponse {
                schema_checksum: manifest_checksum,
                protocol_version: UDB_PROTOCOL_VERSION.to_string(),
                enabled_backends,
                degraded_backends,
                system_catalog_relations,
                supported_rpcs,
                backend_instances: self
                    .runtime_snapshot()
                    .backend_instances()
                    .iter()
                    .map(backend_instance_status)
                    .collect(),
                backend_capabilities: backend_caps
                    .into_iter()
                    .map(|entry| BackendCapabilityDescriptor {
                        backend: entry.backend,
                        tier: entry.tier,
                        operations: entry.operations,
                        unsupported_error_code: entry.unsupported_error_code,
                        consistency_model: entry.consistency_model,
                        max_payload_bytes: entry.max_payload_bytes as i64,
                        supports_xa: entry.supports_xa,
                        supports_two_phase_commit: entry.supports_two_phase_commit,
                    })
                    .collect(),
                protocol_support,
                backend_protocol_support,
                native_services,
                // master-plan 3.5: surface the operator-declared deployment tier
                // resolved ONCE at startup (the same OnceLock the startup floor
                // gate uses). Empty string when no tier was declared.
                deployment_tier: crate::runtime::core::declared_deployment_tier()
                    .map(|tier| tier.as_str().to_string())
                    .unwrap_or_default(),
            })),
        )
    }

    pub(crate) async fn get_health_report_inner(
        &self,
        request: Request<HealthReportRequest>,
    ) -> Result<Response<HealthReportResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "GetHealthReport");
        // Admin scope is enforced on EVERY call, BEFORE any cache return, so cached
        // health data is never served to an unauthorized caller.
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("GetHealthReport", started, Err(err));
        }
        let request = request.into_inner();
        let project_scope = request.project_id.trim().to_string();

        // Fast path: serve a recently-assembled report within the short TTL. Keyed by
        // (project_scope, with_probes) so scoped/probe variants don't collide.
        let cache_key = (project_scope.clone(), request.with_probes);
        let ttl = std::time::Duration::from_secs(HEALTH_REPORT_CACHE_TTL_SECS);
        if let Ok(cache) = health_report_cache().lock() {
            if let Some((stored_at, cached)) = cache.get(&cache_key) {
                if stored_at.elapsed() < ttl {
                    return self.record_grpc(
                        "GetHealthReport",
                        started,
                        Ok(Response::new(cached.clone())),
                    );
                }
            }
        }

        let init = self.runtime_snapshot().init_report().clone();
        let mut errors = Vec::new();
        let mut warnings = init.warnings.clone();

        // Privilege check
        let priv_report = if init.postgres_configured {
            let pr = self.runtime_snapshot().check_postgres_privileges().await;
            if !pr.create_publication {
                warnings.push("PG role lacks CREATE PUBLICATION privilege".into());
            }
            if !pr.replication_slot {
                warnings.push("PG role lacks replication role for CDC".into());
            }
            Some(pr)
        } else {
            errors.push("PostgreSQL is required: UDB_PG_DSN / DATABASE_URL is not set".into());
            None
        };

        // Native-service statuses — single source, reused by the readiness probe
        // loop below and the unified readiness contract further down.
        let native_statuses =
            crate::runtime::service::native_registry::resolved_native_service_statuses(
                self.runtime_snapshot().config(),
            );

        // Live probes
        let mut probes = Vec::new();
        if request.with_probes {
            // Perf: each `probe_backend` / `native_store_self_check` is an independent
            // network round-trip. Awaiting them in series made this RPC's dominant cost.
            // Collect the futures and drive them concurrently with
            // `futures::future::join_all` (matches the `futures` idiom used elsewhere in
            // src/runtime, e.g. core/catalog_sql.rs). `configured_probe_backends` returns
            // a deterministic order; `join_all` preserves input order in its output, so
            // the probe order in the report stays deterministic.
            let snapshot = self.runtime_snapshot();
            let backend_probe_futs: Vec<_> = snapshot
                .configured_probe_backends(false)
                .into_iter()
                .map(|backend| snapshot.probe_backend(backend))
                .collect();
            probes.extend(futures::future::join_all(backend_probe_futs).await);
            #[cfg(feature = "kafka")]
            probes.push(self.runtime_snapshot().probe_kafka_metadata());

            // Native-store writability (P6.2): probe EVERY mounted native service
            // that declares a canonical persistence backing — a real KV round-trip
            // on the backend its proto `native_service` binding resolves to, not a
            // capability claim (directive #4). Previously only "storage" was probed,
            // so the other mounted native services' persistence was never verified.
            // Degraded → per-service warning, never fatal.
            //
            // These self-checks are likewise independent round-trips: collect them
            // (keeping each service_id alongside its future) and join concurrently,
            // then fold the Ok/Err results into `warnings` with the SAME message
            // format strings, in the deterministic native-status iteration order.
            let self_check_futs: Vec<_> = native_statuses
                .iter()
                .filter(|s| {
                    s.mounted
                        && crate::runtime::service::native_store_binding::native_service_store_backend(
                            &s.service_id,
                        )
                        .is_some()
                })
                .map(|status| {
                    let service_id = status.service_id.clone();
                    let snapshot = self.runtime_snapshot();
                    let project_scope = project_scope.clone();
                    async move {
                        let outcome = snapshot
                            .native_store_self_check(&service_id, &project_scope)
                            .await;
                        (service_id, outcome)
                    }
                })
                .collect();
            for (service_id, outcome) in futures::future::join_all(self_check_futs).await {
                match outcome {
                    Ok(backend) => warnings.push(format!(
                        "native store [{service_id}]: persistence verified on '{backend}'"
                    )),
                    Err(err) => warnings.push(format!(
                        "native store [{service_id}]: persistence self-check failed: {}",
                        err.message()
                    )),
                }
            }
        }

        // Annotate MongoDB transport in warnings.
        if let Some(transport) = self.runtime_snapshot().mongodb_transport_kind() {
            warnings.push(format!(
                "mongodb: transport={transport}; native wire-protocol not supported"
            ));
        }

        // When a project scope is requested, surface catalog version info.
        if !project_scope.is_empty() {
            match self
                .runtime_snapshot()
                .get_catalog_versions(&project_scope)
                .await
            {
                Ok(versions) if versions.is_empty() => {
                    warnings.push(format!(
                        "project '{project_scope}': no catalog versions found"
                    ));
                }
                Ok(versions) => {
                    let active: Vec<_> = versions
                        .iter()
                        .filter(|v| v["status"].as_str().unwrap_or("") == "ACTIVE")
                        .filter_map(|v| v["version"].as_str())
                        .collect();
                    warnings.push(format!(
                        "project '{project_scope}': {} catalog version(s), active=[{}]",
                        versions.len(),
                        active.join(", ")
                    ));
                }
                Err(_) => {
                    warnings.push(format!(
                        "project '{project_scope}': catalog version query failed"
                    ));
                }
            }
        }

        let privileges_json = priv_report
            .as_ref()
            .and_then(|p| serde_json::to_vec(p).ok())
            .unwrap_or_default();
        let probes_json = serde_json::to_vec(&probes).unwrap_or_default();

        // ── Phase 10: unified readiness contract ──────────────────────────────
        // GetHealthReport, `udb native doctor`, the gRPC health service, and the
        // auth-plane readiness checks all derive their facts from the same
        // `slo::ReadinessFacts` shape so the four surfaces agree (the acceptance
        // criterion). Reuse the existing auth-plane checks rather than
        // re-deriving signing-key / casbin / JWKS posture here.
        let auth_report = crate::runtime::service::auth_service::readiness::check_auth_readiness(
            &crate::runtime::security::SecurityConfig::current(),
        )
        .await;
        let auth_triples: Vec<(String, bool, String)> = auth_report
            .checks
            .iter()
            .map(|c| (c.name.clone(), c.ok, c.detail.clone()))
            .collect();
        let readiness =
            crate::runtime::slo::build_readiness_facts(&init, &native_statuses, &auth_triples);
        // Fold the shared facts into the report's error/warning channels so a
        // failing required fact (e.g. a bad signing key, missing PG) fails the
        // health report exactly as it fails doctor and flips gRPC health.
        for err in readiness.errors() {
            if !errors.contains(&err) {
                errors.push(err);
            }
        }
        for warn in readiness.warnings() {
            if !warnings.contains(&warn) {
                warnings.push(warn);
            }
        }

        let native_services: Vec<crate::proto::NativeServiceStatus> = native_statuses
            .into_iter()
            .map(crate::runtime::service::native_registry::status_to_proto)
            .collect();

        let response = HealthReportResponse {
            passed: errors.is_empty(),
            postgres_configured: init.postgres_configured,
            redis_configured: init.redis_configured,
            qdrant_configured: init.qdrant_configured,
            s3_configured: init.s3_configured,
            errors,
            warnings,
            privileges_json,
            probes_json,
            backend_instances: self
                .runtime_snapshot()
                .backend_instances()
                .iter()
                .map(backend_instance_status)
                .collect(),
            native_services,
        };

        // Cache the freshly-assembled report for the short TTL. Clear the map if it
        // grows (project-scope churn) rather than letting it accumulate — same bounded
        // strategy as the catalog-manifest cache.
        if let Ok(mut cache) = health_report_cache().lock() {
            if cache.len() > 8 {
                cache.clear();
            }
            cache.insert(cache_key, (std::time::Instant::now(), response.clone()));
        }

        self.record_grpc("GetHealthReport", started, Ok(Response::new(response)))
    }

    pub(crate) async fn lookup_message_schema_inner(
        &self,
        request: Request<MessageSchemaLookupRequest>,
    ) -> Result<Response<MessageSchemaLookupResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "LookupMessageSchema");
        let req = request.into_inner();
        if let (Some(requested), Some(bound)) =
            (non_empty(&req.project_id), non_empty(&security.project_id))
        {
            if requested != bound && !security.has_scope("udb:admin") {
                return self.record_grpc(
                    "LookupMessageSchema",
                    started,
                    Err(project_scope_mismatch_status("LookupMessageSchema")),
                );
            }
        }
        let project_id = non_empty(&req.project_id)
            .or_else(|| non_empty(&security.project_id))
            .unwrap_or("default")
            .to_string();
        let client_version = non_empty(&req.client_catalog_version)
            .unwrap_or(&security.client_catalog_version)
            .to_string();
        let registry = SchemaRegistry::new(self.catalog.clone());
        let descriptor =
            match registry.lookup_message(&project_id, &req.message_type, &client_version) {
                Ok(descriptor) => descriptor,
                Err(LookupError::MessageNotFound { message_type, .. }) => {
                    return self.record_grpc(
                        "LookupMessageSchema",
                        started,
                        Err(message_schema_not_found_status(&message_type, &project_id)),
                    );
                }
                Err(LookupError::MessageAmbiguous {
                    message_type,
                    candidates,
                    ..
                }) => {
                    return self.record_grpc(
                        "LookupMessageSchema",
                        started,
                        Err(crate::runtime::executor_utils::invalid_argument_fields(
                            format!(
                                "ambiguous message type '{message_type}': [{}]",
                                candidates.join(", ")
                            ),
                            [(
                                "message_type",
                                "must use a fully-qualified protobuf name or schema.table",
                            )],
                        )),
                    );
                }
                Err(LookupError::Incompatible { reason, .. }) => {
                    return self.record_grpc(
                        "LookupMessageSchema",
                        started,
                        Err(catalog_version_incompatible_status(
                            "LookupMessageSchema",
                            reason,
                        )),
                    );
                }
            };

        self.record_grpc(
            "LookupMessageSchema",
            started,
            Ok(Response::new(MessageSchemaLookupResponse {
                schema: Some(message_descriptor_to_proto(descriptor)),
            })),
        )
    }

    pub(crate) async fn list_message_schemas_inner(
        &self,
        request: Request<MessageSchemaListRequest>,
    ) -> Result<Response<MessageSchemaListResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "ListMessageSchemas");
        let req = request.into_inner();
        if let (Some(requested), Some(bound)) =
            (non_empty(&req.project_id), non_empty(&security.project_id))
        {
            if requested != bound && !security.has_scope("udb:admin") {
                return self.record_grpc(
                    "ListMessageSchemas",
                    started,
                    Err(project_scope_mismatch_status("ListMessageSchemas")),
                );
            }
        }
        let project_id = non_empty(&req.project_id)
            .or_else(|| non_empty(&security.project_id))
            .unwrap_or("default")
            .to_string();
        let client_version = non_empty(&req.client_catalog_version)
            .unwrap_or(&security.client_catalog_version)
            .to_string();
        let registry = SchemaRegistry::new(self.catalog.clone());
        let outcome = registry.negotiate_version(&project_id, &client_version);
        if let NegotiationOutcome::Incompatible { reason, .. } = outcome {
            return self.record_grpc(
                "ListMessageSchemas",
                started,
                Err(catalog_version_incompatible_status(
                    "ListMessageSchemas",
                    reason,
                )),
            );
        }
        let active = self.catalog.active_for(&project_id);
        self.record_grpc(
            "ListMessageSchemas",
            started,
            Ok(Response::new(MessageSchemaListResponse {
                project_id,
                catalog_version: active.metadata.version.clone(),
                manifest_checksum: active.metadata.checksum.clone(),
                message_types: registry.list_messages(&active.metadata.project_id),
            })),
        )
    }
}

/// Which listener a health service is being built for. Each UDB listener gets
/// its own `tonic_health` service so a health probe on one port reports only the
/// gRPC services actually mounted on that port (per the gRPC health spec, the
/// empty service name `""` is overall-server health for that listener).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthPlane {
    /// The public DataBroker listener.
    DataBroker,
    /// The internal native control-plane listener (`UDB_AUTH_GRPC_ADDR`):
    /// authn/authz/apikey/idp/tenant/notification/analytics/storage/asset and
    /// the WebRTC *control* services.
    NativeControlPlane,
    /// The peer-facing WebRTC listener (`UDB_WEBRTC_GRPC_ADDR`).
    WebRtcPeer,
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
    fn catalog_version_incompatible_carries_schema_detail() {
        let err = catalog_version_incompatible_status(
            "LookupMessageSchema",
            "client catalog version 1 is incompatible with active version 2".to_string(),
        );
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "client catalog version 1 is incompatible with active version 2"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "catalog");
        assert_eq!(detail.operation, "LookupMessageSchema");
        assert_eq!(detail.capability_required, "catalog_version_incompatible");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    #[test]
    fn message_schema_not_found_carries_schema_detail() {
        let err = message_schema_not_found_status("app.Invoice", "billing");
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert_eq!(
            err.message(),
            "message schema 'app.Invoice' not found for project 'billing'"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "catalog");
        assert_eq!(detail.operation, "LookupMessageSchema");
        assert_eq!(detail.capability_required, "message_schema_not_found");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    #[test]
    fn project_scope_mismatch_carries_policy_detail() {
        for operation in ["LookupMessageSchema", "ListMessageSchemas"] {
            let err = project_scope_mismatch_status(operation);
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
            assert_eq!(
                err.message(),
                "requested project_id does not match authenticated project"
            );
            let detail = decode_detail(&err);
            assert_eq!(detail.kind, ErrorKind::Policy as i32);
            assert_eq!(detail.backend, "");
            assert_eq!(detail.operation, operation);
            assert_eq!(detail.policy_decision_id, "project_scope_mismatch");
            assert!(!detail.retryable);
            assert_eq!(detail.retry_after_ms, 0);
        }
    }
}

/// Build a `tonic_health` service for a single UDB listener, with each gRPC
/// service marked `Serving`/`NotServing` to reflect real readiness.
///
/// The returned `HealthServer` is added to that listener's tonic server by the
/// caller in `serve()` (the parent owns server wiring). The reporter is consumed
/// internally — all status marking happens here so the parent only has to mount
/// the returned service.
///
/// Marking rules:
/// - `DataBroker`: the `DataBrokerServer` is marked `Serving` iff the shared
///   `slo::ReadinessFacts` contract passes.
/// - `NativeControlPlane` / `WebRtcPeer`: each native gRPC service mounted on
///   that listener is marked `Serving` when its `NativeServiceStatus.mounted` is
///   true and the shared readiness facts pass, and `NotServing` otherwise.
///
/// The overall-server entry (`""`) is set to `Serving` iff at least one service
/// on the listener is serving, so a load balancer's empty-name health check
/// fails closed when every native service on the listener is degraded.
///
/// Parent (`service/mod.rs::serve`) usage — one call per listener, mount the
/// returned `health_service` on that listener's `Server`:
/// ```ignore
/// let native_health =
///     build_listener_health_service(HealthPlane::NativeControlPlane, &runtime_config, Some(&runtime)).await;
/// auth_server.add_service(native_health) /* ...other services... */;
///
/// let webrtc_health =
///     build_listener_health_service(HealthPlane::WebRtcPeer, &runtime_config, Some(&runtime)).await;
/// webrtc_peer_server.add_service(webrtc_health) /* ...other services... */;
/// ```
pub async fn build_listener_health_service(
    plane: HealthPlane,
    config: &crate::runtime::config::UdbConfig,
    runtime: Option<&crate::runtime::DataBrokerRuntime>,
) -> tonic_health::pb::health_server::HealthServer<impl tonic_health::pb::health_server::Health> {
    let (mut reporter, health_service) = tonic_health::server::health_reporter();
    let statuses =
        crate::runtime::service::native_registry::resolved_native_service_statuses(config);
    let readiness_passed = if let Some(runtime) = runtime {
        let auth_triples = crate::runtime::service::auth_readiness_triples(
            &crate::runtime::security::SecurityConfig::current(),
        )
        .await;
        crate::runtime::slo::build_readiness_facts(runtime.init_report(), &statuses, &auth_triples)
            .passed()
    } else {
        true
    };

    mark_listener_health(&mut reporter, plane, &statuses, readiness_passed).await;

    // A8 (UDB-DB-READINESS-001): keep readiness LIVE, not boot-only. Spawn a
    // per-listener task that re-probes PostgreSQL reachability and flips the gRPC
    // serving status at runtime, so a load balancer stops routing to a broker
    // that has lost its database (the boot gate covers only startup). Per-node —
    // each broker reports its OWN readiness, so no leader election. Conservative:
    // a listener already degraded at boot for a non-DB reason
    // (`readiness_passed == false`) is never marked serving; a lost database only
    // DOWNGRADES, and recovery restores exactly the boot verdict.
    if runtime.is_some() {
        let boot_ready = readiness_passed;
        tokio::spawn(async move {
            let mut consecutive_failures = 0u32;
            let mut last_marked = boot_ready;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(
                    READINESS_REFRESH_INTERVAL_SECS,
                ))
                .await;
                // `None` → the auth-plane deps are not installed on this listener;
                // do not spuriously downgrade — leave the boot status in place.
                let Some(pg_ok) = crate::runtime::credential_layer::auth_plane_pg_reachable().await
                else {
                    continue;
                };
                if pg_ok {
                    consecutive_failures = 0;
                } else {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                }
                // Downgrade only after a short run of failures (do not flap on one
                // transient blip); recover immediately once the probe succeeds.
                let live_ok = pg_ok || consecutive_failures < READINESS_FAILURE_GRACE;
                let want_ready = boot_ready && live_ok;
                if want_ready != last_marked {
                    mark_listener_health(&mut reporter, plane, &statuses, want_ready).await;
                    last_marked = want_ready;
                    tracing::warn!(
                        plane = ?plane,
                        serving = want_ready,
                        "listener readiness changed from live PostgreSQL probe",
                    );
                }
            }
        });
    }

    health_service
}

/// How often the live readiness-refresh task (A8) re-probes PostgreSQL.
const READINESS_REFRESH_INTERVAL_SECS: u64 = 5;
/// Consecutive failed probes tolerated before a listener is marked NotServing —
/// so a single transient blip does not flap the serving status.
const READINESS_FAILURE_GRACE: u32 = 2;

/// Mark every gRPC service on a listener `Serving`/`NotServing` from a single
/// `readiness_passed` verdict. Shared by the boot marking and the live
/// readiness-refresh task (A8) so both apply identical rules. The overall-server
/// entry (`""`) is `Serving` iff at least one service on the listener is serving,
/// so an LB empty-name probe fails closed when the listener is fully degraded.
async fn mark_listener_health(
    reporter: &mut tonic_health::server::HealthReporter,
    plane: HealthPlane,
    statuses: &[crate::runtime::service::native_registry::NativeServiceStatus],
    readiness_passed: bool,
) {
    use crate::runtime::service::native_registry::NativeListenerKind;
    use tonic_health::ServingStatus;

    let mut any_serving = false;
    match plane {
        HealthPlane::DataBroker => {
            if readiness_passed {
                reporter
                    .set_serving::<DataBrokerServer<DataBrokerService>>()
                    .await;
                any_serving = true;
            } else {
                reporter
                    .set_not_serving::<DataBrokerServer<DataBrokerService>>()
                    .await;
            }
        }
        HealthPlane::NativeControlPlane | HealthPlane::WebRtcPeer => {
            let want_kind = match plane {
                HealthPlane::NativeControlPlane => NativeListenerKind::ControlPlane.as_str(),
                HealthPlane::WebRtcPeer => NativeListenerKind::WebRtcPeer.as_str(),
                HealthPlane::DataBroker => unreachable!(),
            };
            for status in statuses {
                if !status.enabled || status.listener_kind != want_kind {
                    continue;
                }
                let serving = if status.mounted && readiness_passed {
                    any_serving = true;
                    ServingStatus::Serving
                } else {
                    ServingStatus::NotServing
                };
                // One UDB native service can ship several proto gRPC services
                // (e.g. WebRTC = Room/Peer/Track/Turn/Signaling); mark each by
                // its full proto service name so a `Check{service}` probe for any
                // of them resolves.
                for proto_service in &status.proto_services {
                    reporter
                        .set_service_status(proto_service.as_str(), serving)
                        .await;
                }
            }
        }
    }

    // Overall-server health (`""`) fails closed when nothing on the listener is
    // serving, so an LB empty-name probe does not route to a dead listener.
    reporter
        .set_service_status(
            "",
            if any_serving {
                ServingStatus::Serving
            } else {
                ServingStatus::NotServing
            },
        )
        .await;
}

fn message_descriptor_to_proto(
    descriptor: crate::runtime::schema_registry::MessageDescriptor,
) -> MessageSchemaDescriptor {
    MessageSchemaDescriptor {
        message_type: descriptor.message_type,
        project_id: descriptor.project_id,
        catalog_version: descriptor.catalog_version,
        manifest_checksum: descriptor.manifest_checksum,
        schema: descriptor.schema,
        table: descriptor.table,
        primary_key: descriptor.primary_key,
        fields: descriptor
            .fields
            .into_iter()
            .map(|field| MessageFieldDescriptor {
                name: field.name,
                column_name: field.column_name,
                proto_type: field.proto_type,
                sql_type: field.sql_type,
                not_null: field.not_null,
                is_primary: field.is_primary,
                is_array: field.is_array,
            })
            .collect(),
    }
}

// ── B2: GetHealthReport cache contract ────────────────────────────────────────
#[cfg(test)]
mod health_cache_tests {
    use super::*;

    /// B2 (capability-lie guard): the report cache TTL must stay tiny so a
    /// newly-failing required fact (bad signing key, missing PG/store) cannot be
    /// masked by a stale cached `passed=true` for more than a few seconds.
    #[test]
    fn health_cache_ttl_is_short_enough_to_not_mask_failures() {
        assert!(
            HEALTH_REPORT_CACHE_TTL_SECS <= 5,
            "health cache TTL must stay tiny (capability-lie guard): {HEALTH_REPORT_CACHE_TTL_SECS}s"
        );
    }

    /// B2 (cache keying): the key is `(project_scope, with_probes)`, so a
    /// probe-less request, a different probe mode, or a different project must
    /// NOT read another variant's cached report.
    #[test]
    fn health_cache_key_separates_project_scope_and_probe_mode() {
        let now = std::time::Instant::now();
        let yes = HealthReportResponse {
            passed: true,
            ..Default::default()
        };
        let no = HealthReportResponse {
            passed: false,
            ..Default::default()
        };
        let k_probe = ("b2-cache-proj".to_string(), true);
        let k_noprobe = ("b2-cache-proj".to_string(), false);
        {
            let mut cache = health_report_cache().lock().unwrap();
            cache.insert(k_probe.clone(), (now, yes));
            cache.insert(k_noprobe.clone(), (now, no));
        }
        {
            let cache = health_report_cache().lock().unwrap();
            assert!(
                cache.get(&k_probe).unwrap().1.passed,
                "with_probes=true variant"
            );
            assert!(
                !cache.get(&k_noprobe).unwrap().1.passed,
                "with_probes=false must be a distinct cache entry"
            );
            assert!(
                cache.get(&("b2-other-proj".to_string(), true)).is_none(),
                "a different project scope must not collide"
            );
        }
        // Don't pollute the process-global cache for the live handler.
        let mut cache = health_report_cache().lock().unwrap();
        cache.remove(&k_probe);
        cache.remove(&k_noprobe);
    }
}
