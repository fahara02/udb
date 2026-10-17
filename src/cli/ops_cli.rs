//! File-driven operations: `udb policy diff|apply`, `udb identity diff|apply`,
//! `udb data seed`. Each reads a declared state, shows how the live state
//! differs, and (with `apply`) reconciles it; applying twice is a no-op.

use super::identity_ops::{DeclaredAccount, IdentityFile, identity_command};
use super::*;

pub(crate) fn run_ops_command(command: OpsCommand) -> i32 {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("udb: failed to create tokio runtime: {err}");
            return 1;
        }
    };
    // `udb up` reports per step and has its own exit codes (3: stopped at a
    // step that needs a person).
    if let OpsCommand::Up(options) = command {
        return runtime.block_on(super::up::run(options));
    }
    match runtime.block_on(run_ops_command_async(command)) {
        Ok(value) => {
            output_json(&value, "command result");
            0
        }
        Err(err) => {
            eprintln!("udb: {err}");
            1
        }
    }
}

async fn run_ops_command_async(command: OpsCommand) -> Result<serde_json::Value, String> {
    match command {
        OpsCommand::Policy {
            apply,
            file,
            overlays,
            tenant,
            dsn,
            changed_by,
        } => {
            if file.trim().is_empty() {
                return Err("provide -f <policies.json|yaml>".to_string());
            }
            let tenant = if tenant.trim().is_empty() {
                env::var("UDB_TENANT_ID").unwrap_or_default()
            } else {
                tenant
            };
            let mut declared = load_policy_file(&file)?;
            for overlay in &overlays {
                declared.extend(load_policy_file(overlay)?);
            }
            let declared = udb::runtime::service::normalize_declared_policies(&tenant, declared)?;
            let dsn = if dsn.trim().is_empty() {
                env::var("UDB_PG_DSN")
                    .or_else(|_| env::var("DATABASE_URL"))
                    .map_err(|_| {
                        "set UDB_PG_DSN (or DATABASE_URL), or pass --dsn <postgres-dsn>".to_string()
                    })?
            } else {
                dsn.trim().to_string()
            };
            let changed_by = if changed_by.trim().is_empty() {
                "udb-cli".to_string()
            } else {
                changed_by.trim().to_string()
            };
            let report = udb::runtime::service::reconcile_authz_policies_offline(
                &dsn,
                &tenant,
                declared,
                apply,
                &changed_by,
            )
            .await?;
            serde_json::to_value(&report).map_err(|err| format!("render report failed: {err}"))
        }
        OpsCommand::Identity {
            apply,
            file,
            tenant,
            allow_transfer,
        } => identity_command(apply, &file, &tenant, allow_transfer).await,
        OpsCommand::DataSeed {
            file,
            tenant,
            project,
            dry_run,
        } => data_seed_command(&file, &tenant, &project, dry_run).await,
        OpsCommand::Projection {
            backfill,
            message_type,
            project,
            limit,
        } => projection_command(backfill, &message_type, &project, limit).await,
        OpsCommand::EventsTail {
            topic_pattern,
            since,
            decode,
            max,
        } => events_tail_command(&topic_pattern, &since, decode, max).await,
        OpsCommand::ResourcesList { backend } => resources_list_command(&backend).await,
        OpsCommand::SubjectPurge {
            subject,
            reason,
            dry_run,
            receipt_out,
        } => subject_purge_command(&subject, &reason, dry_run, &receipt_out).await,
        OpsCommand::MigrateStorage {
            bucket,
            tenant,
            prefix,
            limit,
            dry_run,
        } => migrate_storage_command(&bucket, &tenant, &prefix, limit, dry_run).await,
        OpsCommand::Up(_) => unreachable!("udb up is dispatched by run_ops_command"),
    }
}

/// Read a policy file: a JSON or YAML list of policies, or an object with a
/// `policies` list (and optionally `tenant`, applied to entries without one).
pub(super) fn load_policy_file(
    path: &str,
) -> Result<Vec<udb::runtime::authz::AuthzPolicy>, String> {
    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum PolicyFile {
        List(Vec<udb::runtime::authz::AuthzPolicy>),
        Document {
            #[serde(default)]
            tenant: String,
            policies: Vec<udb::runtime::authz::AuthzPolicy>,
        },
    }
    let parsed: PolicyFile = read_structured(path)?;
    Ok(match parsed {
        PolicyFile::List(policies) => policies,
        PolicyFile::Document { tenant, policies } => policies
            .into_iter()
            .map(|mut policy| {
                if policy.tenant.trim().is_empty() {
                    policy.tenant = tenant.clone();
                }
                policy
            })
            .collect(),
    })
}

// ── udb data seed ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Deserialize)]
struct SeedEntity {
    message_type: String,
    #[serde(default)]
    conflict_fields: Vec<String>,
    records: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct SeedFile {
    #[serde(default)]
    tenant: String,
    #[serde(default)]
    project: String,
    entities: Vec<SeedEntity>,
}

fn data_target() -> String {
    let raw = env::var("UDB_GRPC_TARGET")
        .or_else(|_| env::var("UDB_GRPC_ADDR"))
        .map(|addr| super::authz_cli::client_target_addr(&addr))
        .unwrap_or_else(|_| DEFAULT_GRPC_TARGET_ADDR.to_string());
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw
    } else {
        format!("http://{raw}")
    }
}

/// A setting's value: the flag, else the declaration file, else `env_name`.
pub(super) fn pick(flag: &str, from_file: &str, env_name: &str) -> String {
    [flag, from_file]
        .into_iter()
        .map(str::trim)
        .find(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| env::var(env_name).unwrap_or_default().trim().to_string())
}

async fn data_seed_command(
    file: &str,
    tenant: &str,
    project: &str,
    dry_run: bool,
) -> Result<serde_json::Value, String> {
    use udb::proto::data_broker_client::DataBrokerClient;
    use udb::proto::udb::entity::v1 as entity_pb;

    if file.trim().is_empty() {
        return Err("provide -f <seed.yaml>".to_string());
    }
    let parsed: SeedFile = read_structured(file)?;
    let tenant = pick(tenant, &parsed.tenant, "UDB_TENANT_ID");
    let project = pick(project, &parsed.project, "UDB_PROJECT_ID");
    let total: usize = parsed.entities.iter().map(|e| e.records.len()).sum();
    for entity in &parsed.entities {
        if entity.message_type.trim().is_empty() {
            return Err("every entity needs a message_type".to_string());
        }
        if let Some(bad) = entity.records.iter().position(|r| !r.is_object()) {
            return Err(format!(
                "{} record #{bad} is not an object",
                entity.message_type
            ));
        }
    }
    if dry_run {
        return Ok(serde_json::json!({
            "dry_run": true,
            "tenant_id": tenant,
            "project_id": project,
            "records": total,
            "entities": parsed.entities.iter().map(|e| serde_json::json!({
                "message_type": e.message_type, "records": e.records.len()
            })).collect::<Vec<_>>(),
        }));
    }
    let mut client = DataBrokerClient::connect(data_target())
        .await
        .map_err(|err| format!("failed to connect to the broker: {err}"))?;
    let purpose = env::var("UDB_PURPOSE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "seed".to_string());
    let mut written = Vec::new();
    for entity in &parsed.entities {
        for (index, record) in entity.records.iter().enumerate() {
            let mut request = super::authz_cli::with_metadata(entity_pb::UpsertRequest {
                context: Some(entity_pb::RequestContext {
                    tenant_id: tenant.clone(),
                    purpose: purpose.clone(),
                    ..Default::default()
                }),
                message_type: entity.message_type.trim().to_string(),
                record_json: serde_json::to_vec(record).unwrap_or_default(),
                conflict_fields: entity.conflict_fields.clone(),
                ..Default::default()
            });
            for (key, value) in [
                ("x-tenant-id", tenant.as_str()),
                ("x-udb-project-id", project.as_str()),
                ("x-purpose", purpose.as_str()),
            ] {
                if !value.trim().is_empty()
                    && let Ok(value) = value.trim().parse()
                {
                    request.metadata_mut().insert(key, value);
                }
            }
            client.upsert(request).await.map_err(|err| {
                format!(
                    "{} record #{index} failed after {} written: {err}",
                    entity.message_type,
                    written.len()
                )
            })?;
            written.push((entity.message_type.clone(), index));
        }
    }
    Ok(serde_json::json!({
        "tenant_id": tenant,
        "project_id": project,
        "written": written.len(),
    }))
}

// ── udb projection status|backfill ─────────────────────────────────────────

/// `status`: sample every projection target of the entity against the
/// canonical rows and report divergence. `backfill`: scan up to `limit` rows
/// (default 10000) and enqueue a repair task for every missing or divergent
/// row. Both run ScanProjectionDrift on the data plane and need an admin
/// bearer (UDB_AUTH_TOKEN).
async fn projection_command(
    backfill: bool,
    message_type: &str,
    project: &str,
    limit: i32,
) -> Result<serde_json::Value, String> {
    use udb::proto::data_broker_client::DataBrokerClient;
    use udb::proto::udb::entity::v1 as entity_pb;

    if message_type.trim().is_empty() {
        return Err(
            "name the entity: udb projection status|backfill <message type, e.g. acme.notes.v1.Note>"
                .to_string(),
        );
    }
    let project = if project.trim().is_empty() {
        env::var("UDB_PROJECT_ID").unwrap_or_default()
    } else {
        project.trim().to_string()
    };
    let mut client = DataBrokerClient::connect(data_target())
        .await
        .map_err(|err| format!("failed to connect to the broker: {err}"))?;
    let request = entity_pb::ProjectionDriftScanRequest {
        project_id: project.clone(),
        message_type: message_type.trim().to_string(),
        scan_mode: if backfill { "full" } else { "sample" }.to_string(),
        rows_per_target: 100,
        repair: backfill,
        limit: if backfill && limit <= 0 {
            10_000
        } else {
            limit
        },
        ..Default::default()
    };
    let response = client
        .scan_projection_drift(super::authz_cli::with_metadata(request))
        .await
        .map_err(|err| format!("projection scan failed: {err}"))?
        .into_inner();
    let targets: Vec<serde_json::Value> = response
        .reports
        .iter()
        .map(|report| {
            serde_json::json!({
                "target": format!("{}:{}/{}", report.target_backend, report.target_instance, report.target_resource),
                "rows_scanned": report.source_rows_scanned,
                "divergent": report.divergent_rows.len(),
                "rows_to_repair": report.rows_to_repair,
                "repair_tasks_enqueued": report.repair_tasks_enqueued,
            })
        })
        .collect();
    let in_sync = response
        .reports
        .iter()
        .all(|report| report.divergent_rows.is_empty() && report.rows_to_repair == 0);
    Ok(serde_json::json!({
        "message_type": response.message_type,
        "project_id": response.project_id,
        "scan_mode": response.scan_mode,
        "source_rows_loaded": response.source_rows_loaded,
        "in_sync": in_sync,
        "targets": targets,
        "warnings": response.warnings,
    }))
}

// ── udb.yaml + udb up ───────────────────────────────────────────────────────

/// A project's declared operational state in one file. Every section is
/// optional; each one is reconciled by the same code as its standalone command
/// (`udb identity apply`, `udb policy apply`, `udb data seed`), and sections may
/// point at a separate file instead of inlining it.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct ProjectFile {
    pub(super) tenant: String,
    pub(super) project: String,
    /// The proto directory and package filter `udb up` builds the catalog
    /// from (else UDB_PROTO_ROOT / UDB_PROTO_NAMESPACE, else `proto`).
    pub(super) proto_root: String,
    pub(super) proto_namespace: String,
    /// The serving broker, for the reviewed catalog transition.
    pub(super) catalog: CatalogSection,
    service_accounts: Vec<DeclaredAccount>,
    service_accounts_file: String,
    policies: Vec<udb::runtime::authz::AuthzPolicy>,
    policies_file: String,
    policy_overlays: Vec<String>,
    seed: Option<serde_json::Value>,
    seed_file: String,
    pub(super) embedding: Option<EmbeddingSection>,
    /// Pipeline units run by the broker: point UDB_PIPELINES_FILE at this
    /// file. `udb up` only validates them.
    pipelines: Vec<serde_yaml::Value>,
    /// The native-service selection `udb native add|remove|generate` and
    /// `udb app init` maintain (it replaced `.udb/native-services.json` in
    /// 0.6.0). `udb up` does not reconcile it; it is parsed here so a
    /// malformed section fails `udb up` instead of being skipped.
    #[allow(dead_code)]
    native_services: Option<super::native_app::NativeServicesManifest>,
}

/// `catalog:` in udb.yaml.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct CatalogSection {
    /// The broker's gRPC address (else UDB_GRPC_TARGET).
    pub(super) target: String,
}

/// Declared embedding models and sources (P5). Models and sources are per
/// tenant in udb, and a source must name its model's versioned collection, so
/// they are declared here and registered by `udb up` rather than inferred.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct EmbeddingSection {
    models: Vec<DeclaredModel>,
    sources: Vec<DeclaredSource>,
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
struct DeclaredModel {
    model_id: String,
    provider: String,
    model_name: String,
    version: String,
    dimensions: i32,
    distance_metric: String,
    normalize: bool,
    task_type: String,
    provider_endpoint_ref: String,
    vector_backend: String,
    active_collection: String,
    collection_alias: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
struct DeclaredSource {
    source_name: String,
    source_message_type: String,
    text_fields: Vec<String>,
    model_id: String,
    /// Defaults to the model's active_collection.
    target_collection: String,
    /// Backfill existing rows when the source is first registered.
    backfill: bool,
}

/// Paths in udb.yaml are relative to the file itself.
pub(super) fn relative_to(base: &str, path: &str) -> String {
    let candidate = std::path::Path::new(path);
    if candidate.is_absolute() {
        return path.to_string();
    }
    std::path::Path::new(base)
        .parent()
        .map(|dir| dir.join(candidate).display().to_string())
        .unwrap_or_else(|| path.to_string())
}

/// Read udb.yaml for `udb up`. An empty `file` means `udb.yaml` when it exists
/// and an empty declaration when it does not (a project can be brought up from
/// flags alone); a named file must exist. Returns the path the file's relative
/// references resolve against.
pub(super) fn load_project_file(file: &str) -> Result<(String, ProjectFile), String> {
    if file.trim().is_empty() {
        let default = "udb.yaml";
        if !std::path::Path::new(default).exists() {
            return Ok((default.to_string(), ProjectFile::default()));
        }
        return Ok((default.to_string(), read_structured(default)?));
    }
    Ok((file.to_string(), read_structured(file)?))
}

impl ProjectFile {
    /// Every declared policy: inline, then `policies_file`, then the overlays.
    pub(super) fn declared_policies(
        &self,
        file: &str,
    ) -> Result<Vec<udb::runtime::authz::AuthzPolicy>, String> {
        let mut policies = self.policies.clone();
        if !self.policies_file.trim().is_empty() {
            policies.extend(load_policy_file(&relative_to(
                file,
                self.policies_file.trim(),
            ))?);
        }
        for overlay in &self.policy_overlays {
            policies.extend(load_policy_file(&relative_to(file, overlay))?);
        }
        Ok(policies)
    }

    /// The file `udb check --policies` reads, when the policies live in one.
    pub(super) fn policies_path(&self, file: &str) -> Option<String> {
        (!self.policies_file.trim().is_empty())
            .then(|| relative_to(file, self.policies_file.trim()))
    }

    /// Every declared service account, inline and from `service_accounts_file`,
    /// with file references resolved.
    pub(super) fn declared_accounts(
        &self,
        file: &str,
        tenant: &str,
    ) -> Result<Vec<DeclaredAccount>, String> {
        // Resolve references before serializing the combined declarations.
        // Inline declarations belong to udb.yaml; external declarations belong
        // to their own source file, even when the bridge writes a temporary JSON
        // document.
        let source_base = |path: &str| {
            fs::canonicalize(
                std::path::Path::new(path)
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or(std::path::Path::new(".")),
            )
            .map_err(|_| "identity declaration directory is unavailable".to_string())
        };
        let mut accounts = self.service_accounts.clone();
        for account in &mut accounts {
            account.resolve_file_references(&source_base(file)?)?;
        }
        if !self.service_accounts_file.trim().is_empty() {
            let nested_path = relative_to(file, self.service_accounts_file.trim());
            let mut nested: IdentityFile = read_structured(&nested_path)?;
            if !nested.tenant.is_empty() && nested.tenant != tenant {
                return Err("external identity declaration tenant differs from udb.yaml".into());
            }
            for account in &mut nested.service_accounts {
                account.resolve_file_references(&source_base(&nested_path)?)?;
            }
            accounts.extend(nested.service_accounts);
        }
        Ok(accounts)
    }

    /// The declared seed document, inline or from `seed_file`.
    pub(super) fn declared_seed(&self, file: &str) -> Result<Option<serde_json::Value>, String> {
        Ok(match (&self.seed, self.seed_file.trim()) {
            (Some(inline), _) => Some(inline.clone()),
            (None, "") => None,
            (None, path) => Some(read_structured::<serde_json::Value>(&relative_to(
                file, path,
            ))?),
        })
    }

    /// The names of the declared pipeline units. The broker runs them (with
    /// UDB_PIPELINES_FILE pointing at this file); `udb up` only validates them.
    pub(super) fn declared_pipelines(&self, file: &str) -> Result<Vec<String>, String> {
        if self.pipelines.is_empty() {
            return Ok(Vec::new());
        }
        let text = fs::read_to_string(file).map_err(|err| format!("read {file}: {err}"))?;
        let specs = udb::runtime::core::pipelines::parse_pipeline_specs(&text)?;
        Ok(specs.iter().map(|spec| spec.name.clone()).collect())
    }
}

/// Reconcile declared service accounts with the `udb identity apply|diff` code.
pub(super) async fn reconcile_identities(
    accounts: &[DeclaredAccount],
    tenant: &str,
    apply: bool,
    allow_transfer: bool,
) -> Result<serde_json::Value, String> {
    let staged =
        std::env::temp_dir().join(format!("udb-up-identities-{}.json", uuid::Uuid::new_v4()));
    let body = serde_json::json!({ "tenant": tenant, "service_accounts": accounts });
    fs::write(&staged, body.to_string())
        .map_err(|err| format!("stage identities failed: {err}"))?;
    let result =
        identity_command(apply, &staged.display().to_string(), tenant, allow_transfer).await;
    let _ = fs::remove_file(&staged);
    result
}

/// Write a seed document with the `udb data seed` code.
pub(super) async fn seed_records(
    seed: &serde_json::Value,
    tenant: &str,
    project: &str,
    dry_run: bool,
) -> Result<serde_json::Value, String> {
    let staged = std::env::temp_dir().join(format!("udb-up-seed-{}.json", uuid::Uuid::new_v4()));
    fs::write(&staged, seed.to_string()).map_err(|err| format!("stage seed failed: {err}"))?;
    let result = data_seed_command(&staged.display().to_string(), tenant, project, dry_run).await;
    let _ = fs::remove_file(&staged);
    result
}

// ── udb up: embedding models and sources ──────────────────────────────────

/// Register declared models and sources that the tenant does not have yet.
/// A model's vector identity (name, dimensions, collection) is immutable: a
/// declared model whose stored registration differs is reported, never
/// changed — declare a new model_id instead.
pub(super) async fn embedding_up(
    tenant: &str,
    section: &EmbeddingSection,
    dry_run: bool,
) -> Result<serde_json::Value, String> {
    use udb::proto::udb::core::embedding::services::v1 as embedding_pb;
    use udb::proto::udb::core::embedding::services::v1::embedding_service_client::EmbeddingServiceClient;

    let mut client = EmbeddingServiceClient::connect(super::authz_cli::auth_target())
        .await
        .map_err(|err| format!("failed to connect to the embedding service: {err}"))?;

    let mut existing_models = Vec::new();
    let mut token = String::new();
    loop {
        let page = client
            .list_models(super::authz_cli::with_metadata(
                embedding_pb::ListModelsRequest {
                    tenant_id: tenant.to_string(),
                    page_size: 200,
                    page_token: token.clone(),
                    ..Default::default()
                },
            ))
            .await
            .map_err(|err| format!("list embedding models failed: {err}"))?
            .into_inner();
        existing_models.extend(page.models);
        if page.next_page_token.is_empty() {
            break;
        }
        token = page.next_page_token;
    }
    let mut existing_sources = Vec::new();
    let mut token = String::new();
    loop {
        let page = client
            .list_sources(super::authz_cli::with_metadata(
                embedding_pb::ListSourcesRequest {
                    tenant_id: tenant.to_string(),
                    page_size: 200,
                    page_token: token.clone(),
                },
            ))
            .await
            .map_err(|err| format!("list embedding sources failed: {err}"))?
            .into_inner();
        existing_sources.extend(page.sources);
        if page.next_page_token.is_empty() {
            break;
        }
        token = page.next_page_token;
    }

    let mut models_report = Vec::new();
    for model in &section.models {
        if model.model_id.trim().is_empty() || model.active_collection.trim().is_empty() {
            return Err("every embedding model needs model_id and active_collection".to_string());
        }
        let action = match existing_models
            .iter()
            .find(|m| m.model_id == model.model_id)
        {
            Some(stored)
                if stored.model_name == model.model_name
                    && stored.dimensions == model.dimensions
                    && stored.active_collection == model.active_collection =>
            {
                "unchanged".to_string()
            }
            Some(stored) => format!(
                "differs (stored {} / {} dims / {}); a model's vector identity cannot change, declare a new model_id",
                stored.model_name, stored.dimensions, stored.active_collection
            ),
            None if dry_run => "would register".to_string(),
            None => {
                client
                    .register_model(super::authz_cli::with_metadata(
                        embedding_pb::RegisterModelRequest {
                            tenant_id: tenant.to_string(),
                            model_id: model.model_id.clone(),
                            provider: model.provider.clone(),
                            model_name: model.model_name.clone(),
                            version: model.version.clone(),
                            dimensions: model.dimensions,
                            distance_metric: model.distance_metric.clone(),
                            normalize: model.normalize,
                            task_type: model.task_type.clone(),
                            provider_endpoint_ref: model.provider_endpoint_ref.clone(),
                            vector_backend: model.vector_backend.clone(),
                            active_collection: model.active_collection.clone(),
                            collection_alias: model.collection_alias.clone(),
                            ..Default::default()
                        },
                    ))
                    .await
                    .map_err(|err| format!("register model {} failed: {err}", model.model_id))?;
                "registered".to_string()
            }
        };
        models_report.push(serde_json::json!({ "model_id": model.model_id, "action": action }));
    }

    let mut sources_report = Vec::new();
    for source in &section.sources {
        let target_collection = if source.target_collection.trim().is_empty() {
            section
                .models
                .iter()
                .find(|m| m.model_id == source.model_id)
                .map(|m| m.active_collection.clone())
                .or_else(|| {
                    existing_models
                        .iter()
                        .find(|m| m.model_id == source.model_id)
                        .map(|m| m.active_collection.clone())
                })
                .ok_or_else(|| {
                    format!(
                        "source {}: model {} is neither declared nor registered",
                        source.source_name, source.model_id
                    )
                })?
        } else {
            source.target_collection.clone()
        };
        let stored = existing_sources
            .iter()
            .find(|s| s.source_name == source.source_name);
        let unchanged = stored.is_some_and(|s| {
            s.source_message_type == source.source_message_type
                && s.model_id == source.model_id
                && s.target_collection == target_collection
        });
        let action = if unchanged {
            "unchanged".to_string()
        } else if dry_run {
            if stored.is_some() {
                "would update"
            } else {
                "would register"
            }
            .to_string()
        } else {
            client
                .register_source(super::authz_cli::with_metadata(
                    embedding_pb::RegisterSourceRequest {
                        tenant_id: tenant.to_string(),
                        source_name: source.source_name.clone(),
                        source_message_type: source.source_message_type.clone(),
                        text_fields: source.text_fields.clone(),
                        target_collection: target_collection.clone(),
                        model_id: source.model_id.clone(),
                        ..Default::default()
                    },
                ))
                .await
                .map_err(|err| format!("register source {} failed: {err}", source.source_name))?;
            if stored.is_none() && source.backfill {
                client
                    .backfill(super::authz_cli::with_metadata(
                        embedding_pb::BackfillRequest {
                            tenant_id: tenant.to_string(),
                            source_name: source.source_name.clone(),
                            mode: String::new(),
                        },
                    ))
                    .await
                    .map_err(|err| format!("backfill {} failed: {err}", source.source_name))?;
                "registered and backfill queued".to_string()
            } else if stored.is_some() {
                "updated".to_string()
            } else {
                "registered".to_string()
            }
        };
        sources_report.push(serde_json::json!({
            "source_name": source.source_name,
            "target_collection": target_collection,
            "action": action,
        }));
    }
    Ok(serde_json::json!({ "models": models_report, "sources": sources_report }))
}

// ── udb subject purge ───────────────────────────────────────────────────────

/// Erase a data subject in the caller's tenant (PurgeSubject, admin bearer)
/// and keep the signed receipt.
async fn subject_purge_command(
    subject: &str,
    reason: &str,
    dry_run: bool,
    receipt_out: &str,
) -> Result<serde_json::Value, String> {
    use udb::proto::data_broker_client::DataBrokerClient;
    use udb::proto::udb::entity::v1 as entity_pb;

    if subject.trim().is_empty() {
        return Err("name the subject: udb subject purge <subject id>".to_string());
    }
    let mut client = DataBrokerClient::connect(data_target())
        .await
        .map_err(|err| format!("failed to connect to the broker: {err}"))?;
    let response = client
        .purge_subject(super::authz_cli::with_metadata(
            entity_pb::PurgeSubjectRequest {
                subject_id: subject.trim().to_string(),
                confirm_subject_id: subject.trim().to_string(),
                reason: reason.to_string(),
                dry_run,
                ..Default::default()
            },
        ))
        .await
        .map_err(|err| format!("purge failed: {err}"))?
        .into_inner();
    if !receipt_out.trim().is_empty() && !response.receipt_jws.is_empty() {
        fs::write(receipt_out.trim(), &response.receipt_jws)
            .map_err(|err| format!("write receipt to {receipt_out} failed: {err}"))?;
    }
    Ok(serde_json::json!({
        "dry_run": response.dry_run,
        "total_deleted": response.total_deleted,
        "tables": response.tables.iter().map(|t| serde_json::json!({
            "message_type": t.message_type, "column": t.column, "deleted": t.deleted
        })).collect::<Vec<_>>(),
        "not_covered": response.not_covered,
        "receipt_signed": !response.receipt_jws.is_empty(),
        "receipt": serde_json::from_str::<serde_json::Value>(&response.receipt_json).unwrap_or_default(),
    }))
}

// ── udb migrate storage ─────────────────────────────────────────────────────

/// Copy objects written before tenant-namespaced keys under the named tenant
/// (sources are kept). Uses the broker's own S3/MinIO configuration from the
/// environment.
async fn migrate_storage_command(
    bucket: &str,
    tenant: &str,
    prefix: &str,
    limit: usize,
    dry_run: bool,
) -> Result<serde_json::Value, String> {
    #[cfg(feature = "s3")]
    {
        let bucket = if bucket.trim().is_empty() {
            env::var("UDB_STORAGE_BUCKET")
                .map_err(|_| "pass --bucket <name> (or set UDB_STORAGE_BUCKET)".to_string())?
        } else {
            bucket.trim().to_string()
        };
        let runtime = udb::runtime::DataBrokerRuntime::from_env().await;
        let report = runtime
            .migrate_legacy_object_keys(&bucket, tenant, prefix, limit, dry_run)
            .await
            .map_err(|err| format!("storage migration failed: {err}"))?;
        serde_json::to_value(&report).map_err(|err| format!("render report failed: {err}"))
    }
    #[cfg(not(feature = "s3"))]
    {
        let _ = (bucket, tenant, prefix, limit, dry_run);
        Err("this udb build has no S3/MinIO support (feature `s3`)".to_string())
    }
}

// ── udb events tail / udb resources list ──────────────────────────────────

/// Unwrap the envelope a CDC stream delivers into the domain event: the outbox
/// payload sits under "payload", and a journal row may wrap the envelope once
/// more.
fn decode_event_payload(payload_json: &str) -> serde_json::Value {
    let mut value: serde_json::Value =
        serde_json::from_str(payload_json).unwrap_or(serde_json::Value::Null);
    if let Some(inner) = value.get("payload").cloned()
        && inner.get("event_type").is_some()
        && inner.get("payload").is_some()
    {
        value = inner;
    }
    value
}

/// Print each event on `topic_pattern` as one JSON line, newest first after
/// `since`, until `max` events (0 = until interrupted). Uses PublishCDC on the
/// data plane, so the caller's grants apply.
async fn events_tail_command(
    topic_pattern: &str,
    since: &str,
    decode: bool,
    max: u64,
) -> Result<serde_json::Value, String> {
    use udb::proto::data_broker_client::DataBrokerClient;
    use udb::proto::udb::entity::v1 as entity_pb;

    if topic_pattern.trim().is_empty() {
        return Err(
            "name a topic: udb events tail <topic or pattern, e.g. acme.notes.*>".to_string(),
        );
    }
    let mut client = DataBrokerClient::connect(data_target())
        .await
        .map_err(|err| format!("failed to connect to the broker: {err}"))?;
    let mut stream = client
        .publish_cdc(super::authz_cli::with_metadata(
            entity_pb::CdcSubscriptionRequest {
                topic_pattern: topic_pattern.trim().to_string(),
                since_event_id: since.trim().to_string(),
                ..Default::default()
            },
        ))
        .await
        .map_err(|err| format!("subscribe failed: {err}"))?
        .into_inner();
    let mut seen = 0u64;
    while let Some(envelope) = stream
        .message()
        .await
        .map_err(|err| format!("stream failed after {seen} event(s): {err}"))?
    {
        let payload = if decode {
            decode_event_payload(&envelope.payload_json)
        } else {
            serde_json::Value::String(envelope.payload_json.clone())
        };
        println!(
            "{}",
            serde_json::json!({
                "event_id": envelope.event_id,
                "topic": envelope.topic,
                "partition_key": envelope.partition_key,
                "published_at": envelope.published_at.map(|ts| ts.seconds),
                "payload": payload,
            })
        );
        seen += 1;
        if max > 0 && seen >= max {
            break;
        }
    }
    Ok(serde_json::json!({ "events": seen }))
}

/// List the resources (collections, graphs, indexes, buckets) a backend holds
/// for the caller's scope.
async fn resources_list_command(backend: &str) -> Result<serde_json::Value, String> {
    use udb::proto::data_broker_client::DataBrokerClient;
    use udb::proto::udb::entity::v1 as entity_pb;

    if backend.trim().is_empty() {
        return Err("pass --backend <qdrant|neo4j|elasticsearch|s3|…>".to_string());
    }
    let mut client = DataBrokerClient::connect(data_target())
        .await
        .map_err(|err| format!("failed to connect to the broker: {err}"))?;
    let response = client
        .list_resources(super::authz_cli::with_metadata(
            entity_pb::ResourceAdminRequest {
                backend: backend.trim().to_string(),
                ..Default::default()
            },
        ))
        .await
        .map_err(|err| format!("list resources failed: {err}"))?
        .into_inner();
    Ok(serde_json::json!({
        "backend": response.backend,
        "resources": response.resources,
    }))
}

/// Read a YAML or JSON file into `T` (by extension; YAML otherwise).
fn read_structured<T: serde::de::DeserializeOwned>(path: &str) -> Result<T, String> {
    let raw = fs::read_to_string(path).map_err(|err| format!("read {path} failed: {err}"))?;
    if path.ends_with(".json") {
        serde_json::from_str(&raw).map_err(|err| format!("parse {path} failed: {err}"))
    } else {
        serde_yaml::from_str(&raw).map_err(|err| format!("parse {path} failed: {err}"))
    }
}

/// Read the same identity declaration used by `identity diff|apply`; the code
/// checker never invents grant scopes or replaces the declared account/tenant.
pub(super) fn code_scan_principal(
    file: &str,
    identity: &str,
    role: &str,
    subject: &str,
) -> Result<udb::runtime::authz::Principal, String> {
    if file.trim().is_empty() || identity.trim().is_empty() {
        return Err("--code requires --identities <identities.yaml> and --identity <service identity or account>".into());
    }
    let declared: IdentityFile = read_structured(file)?;
    uuid::Uuid::parse_str(declared.tenant.trim())
        .map_err(|_| "identities file must declare its canonical tenant UUID".to_string())?;
    let matches: Vec<_> = declared
        .service_accounts
        .iter()
        .filter(|account| {
            account.identity.trim() == identity.trim() || account.account.trim() == identity.trim()
        })
        .collect();
    if matches.len() != 1 {
        return Err(format!(
            "identity '{identity}' must resolve to exactly one declared account, found {}",
            matches.len()
        ));
    }
    let account = matches[0];
    uuid::Uuid::parse_str(account.account.trim())
        .map_err(|_| format!("identity '{identity}' has an invalid account UUID"))?;
    if account.identity.trim().is_empty() {
        return Err(format!(
            "account '{}' has no declared service identity",
            account.account
        ));
    }
    if !subject.trim().is_empty()
        && subject.trim() != account.account.trim()
        && subject.trim() != account.identity.trim()
    {
        return Err(
            "--subject must match the selected declared account or service identity".into(),
        );
    }
    Ok(udb::runtime::authz::Principal {
        principal_id: account.account.trim().into(),
        subject: account.account.trim().into(),
        service_identity: account.identity.trim().into(),
        tenant_id: declared.tenant.trim().into(),
        project_id: account.project.trim().into(),
        scopes: sorted_scopes(&account.scopes),
        roles: if role.trim().is_empty() {
            vec![]
        } else {
            vec![role.trim().into()]
        },
        ..Default::default()
    })
}

fn sorted_scopes(scopes: &[String]) -> Vec<String> {
    let mut out: Vec<String> = scopes
        .iter()
        .map(|scope| scope.trim().to_string())
        .filter(|scope| !scope.is_empty())
        .collect();
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod code_scan_tests {
    #[test]
    fn code_scan_uses_declared_grant_facts_and_refuses_identity_substitution() {
        let path =
            std::env::temp_dir().join(format!("udb-code-identity-{}.json", uuid::Uuid::new_v4()));
        let account = "00000000-0000-4000-8000-000000000001";
        std::fs::write(&path, serde_json::json!({
            "tenant": "00000000-0000-4000-8000-000000000002",
            "service_accounts": [{"account": account, "identity": "billing", "project": "app", "scopes": ["udb:data:read"]}]
        }).to_string()).unwrap();
        let file = path.to_str().unwrap();
        let caller = super::code_scan_principal(file, "billing", "reader", "").unwrap();
        assert_eq!(caller.subject, account);
        assert_eq!(caller.service_identity, "billing");
        assert_eq!(caller.project_id, "app");
        assert_eq!(caller.scopes, vec!["udb:data:read"]);
        assert!(super::code_scan_principal(file, "billing", "", "other-account").is_err());
        assert!(super::code_scan_principal(file, "unknown", "", "").is_err());
        assert!(super::code_scan_principal("", "billing", "", "").is_err());
        std::fs::remove_file(path).unwrap();
    }
}
