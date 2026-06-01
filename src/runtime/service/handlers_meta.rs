//! service.rs split — meta RPC handlers (Phase G).
use super::*;
use crate::protocol::BackendCapabilityDescriptor;
use crate::runtime::schema_registry::{LookupError, NegotiationOutcome, SchemaRegistry};

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
        let supported_rpcs = SUPPORTED_RPC_NAMES
            .iter()
            .map(|name| (*name).to_string())
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
                backend_capabilities: crate::backend::capability_matrix()
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
            })),
        )
    }

    pub(crate) async fn get_health_report_inner(
        &self,
        request: Request<HealthReportRequest>,
    ) -> Result<Response<HealthReportResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "GetHealthReport");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("GetHealthReport", started, Err(err));
        }
        let request = request.into_inner();
        let project_scope = request.project_id.trim().to_string();
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

        // Live probes
        let mut probes = Vec::new();
        if request.with_probes {
            #[cfg(feature = "redis")]
            if init.redis_configured {
                probes.push(self.runtime_snapshot().probe_redis_ping().await);
            }
            if init.qdrant_configured {
                probes.push(self.runtime_snapshot().probe_qdrant_collections().await);
            }
            #[cfg(feature = "s3")]
            if init.s3_configured {
                probes.push(self.runtime_snapshot().probe_s3_access().await);
            }
            if init.mongodb_configured {
                probes.push(self.runtime_snapshot().probe_mongodb_ping().await);
            }
            if init.neo4j_configured {
                probes.push(self.runtime_snapshot().probe_neo4j_ping().await);
            }
            if init.clickhouse_configured {
                probes.push(self.runtime_snapshot().probe_clickhouse_ping().await);
            }
            #[cfg(feature = "kafka")]
            probes.push(self.runtime_snapshot().probe_kafka_metadata());
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

        self.record_grpc(
            "GetHealthReport",
            started,
            Ok(Response::new(HealthReportResponse {
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
            })),
        )
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
                    Err(Status::permission_denied(
                        "requested project_id does not match authenticated project",
                    )),
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
                        Err(Status::not_found(format!(
                            "message schema '{message_type}' not found for project '{project_id}'"
                        ))),
                    );
                }
                Err(LookupError::Incompatible { reason, .. }) => {
                    return self.record_grpc(
                        "LookupMessageSchema",
                        started,
                        Err(Status::failed_precondition(reason)),
                    );
                }
            };

        self.record_grpc(
            "LookupMessageSchema",
            started,
            Ok(Response::new(MessageSchemaLookupResponse {
                descriptor: Some(message_descriptor_to_proto(descriptor)),
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
                    Err(Status::permission_denied(
                        "requested project_id does not match authenticated project",
                    )),
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
                Err(Status::failed_precondition(reason)),
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
