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
        OpsCommand::Up {
            file,
            dry_run,
            allow_transfer,
            dsn,
        } => up_command(&file, dry_run, allow_transfer, &dsn).await,
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
    let pick = |flag: &str, from_file: &str, env_name: &str| -> String {
        [flag, from_file]
            .into_iter()
            .map(str::trim)
            .find(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| env::var(env_name).unwrap_or_default())
    };
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
struct ProjectFile {
    tenant: String,
    project: String,
    service_accounts: Vec<DeclaredAccount>,
    service_accounts_file: String,
    policies: Vec<udb::runtime::authz::AuthzPolicy>,
    policies_file: String,
    policy_overlays: Vec<String>,
    seed: Option<serde_json::Value>,
    seed_file: String,
}

/// Paths in udb.yaml are relative to the file itself.
fn relative_to(base: &str, path: &str) -> String {
    let candidate = std::path::Path::new(path);
    if candidate.is_absolute() {
        return path.to_string();
    }
    std::path::Path::new(base)
        .parent()
        .map(|dir| dir.join(candidate).display().to_string())
        .unwrap_or_else(|| path.to_string())
}

/// `udb up`: policies, then identities, then seed data; `--dry-run` shows the
/// differences only. Stops at the first section that fails, reporting what was
/// already applied.
async fn up_command(
    file: &str,
    dry_run: bool,
    allow_transfer: bool,
    dsn: &str,
) -> Result<serde_json::Value, String> {
    let project: ProjectFile = read_structured(file)?;
    if project.tenant.trim().is_empty() {
        return Err(format!("{file}: set `tenant:` (the canonical tenant UUID)"));
    }
    let tenant = project.tenant.trim().to_string();
    // Resolve references before serializing the combined declarations. Inline
    // declarations belong to udb.yaml; external declarations belong to their
    // own source file, even when the bridge writes a temporary JSON document.
    let source_base = |path: &str| {
        fs::canonicalize(
            std::path::Path::new(path)
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or(std::path::Path::new(".")),
        )
        .map_err(|_| "identity declaration directory is unavailable".to_string())
    };
    let mut accounts = project.service_accounts.clone();
    for account in &mut accounts {
        account.resolve_file_references(&source_base(file)?)?;
    }
    if !project.service_accounts_file.trim().is_empty() {
        let nested_path = relative_to(file, project.service_accounts_file.trim());
        let mut nested: IdentityFile = read_structured(&nested_path)?;
        if !nested.tenant.is_empty() && nested.tenant != tenant {
            return Err("external identity declaration tenant differs from udb.yaml".into());
        }
        for account in &mut nested.service_accounts {
            account.resolve_file_references(&source_base(&nested_path)?)?;
        }
        accounts.extend(nested.service_accounts);
    }
    let mut report = serde_json::Map::new();
    report.insert(
        "tenant_id".into(),
        serde_json::Value::String(tenant.clone()),
    );
    report.insert("dry_run".into(), serde_json::Value::Bool(dry_run));

    // Policies first: identities created next must not run before their grants
    // have rules to match.
    let mut policies = project.policies.clone();
    if !project.policies_file.trim().is_empty() {
        policies.extend(load_policy_file(&relative_to(
            file,
            project.policies_file.trim(),
        ))?);
    }
    for overlay in &project.policy_overlays {
        policies.extend(load_policy_file(&relative_to(file, overlay))?);
    }
    if !policies.is_empty() {
        let declared = udb::runtime::service::normalize_declared_policies(&tenant, policies)?;
        let dsn = if dsn.trim().is_empty() {
            env::var("UDB_PG_DSN")
                .or_else(|_| env::var("DATABASE_URL"))
                .map_err(|_| "policies need UDB_PG_DSN (or DATABASE_URL) or --dsn".to_string())?
        } else {
            dsn.trim().to_string()
        };
        let policy_report = udb::runtime::service::reconcile_authz_policies_offline(
            &dsn, &tenant, declared, !dry_run, "udb-up",
        )
        .await?;
        report.insert(
            "policies".into(),
            serde_json::to_value(&policy_report).unwrap_or_default(),
        );
    }

    if !accounts.is_empty() {
        let staged =
            std::env::temp_dir().join(format!("udb-up-identities-{}.json", uuid::Uuid::new_v4()));
        let body = serde_json::json!({ "tenant": tenant, "service_accounts": accounts });
        fs::write(&staged, body.to_string())
            .map_err(|err| format!("stage identities failed: {err}"))?;
        let result = identity_command(
            !dry_run,
            &staged.display().to_string(),
            &tenant,
            allow_transfer,
        )
        .await;
        let _ = fs::remove_file(&staged);
        report.insert("identities".into(), result?);
    }

    let seed = match (&project.seed, project.seed_file.trim()) {
        (Some(inline), _) => Some(inline.clone()),
        (None, "") => None,
        (None, path) => Some(read_structured::<serde_json::Value>(&relative_to(
            file, path,
        ))?),
    };
    if let Some(seed) = seed {
        let staged =
            std::env::temp_dir().join(format!("udb-up-seed-{}.json", uuid::Uuid::new_v4()));
        fs::write(&staged, seed.to_string()).map_err(|err| format!("stage seed failed: {err}"))?;
        let result = data_seed_command(
            &staged.display().to_string(),
            &tenant,
            project.project.trim(),
            dry_run,
        )
        .await;
        let _ = fs::remove_file(&staged);
        report.insert("seed".into(), result?);
    }
    Ok(serde_json::Value::Object(report))
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
