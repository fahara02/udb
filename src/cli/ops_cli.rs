//! File-driven operations: `udb policy diff|apply`, `udb identity diff|apply`,
//! `udb data seed`. Each reads a declared state, shows how the live state
//! differs, and (with `apply`) reconciles it; applying twice is a no-op.

use super::*;
use udb::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use udb::proto::udb::core::authn::services::v1 as authn_pb;
use udb::proto::udb::core::authn::services::v1::authn_service_client::AuthnServiceClient;

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

// ── udb identity diff|apply ─────────────────────────────────────────────────

/// One declared service account in an identities file.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct DeclaredAccount {
    /// The service-account principal id (its UUID).
    account: String,
    /// The service identity the grant binds (unique across the deployment).
    identity: String,
    #[serde(default)]
    project: String,
    scopes: Vec<String>,
    #[serde(default)]
    reason: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct IdentityFile {
    #[serde(default)]
    tenant: String,
    service_accounts: Vec<DeclaredAccount>,
}

/// The live grant facts the planner compares against.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveGrant {
    account: String,
    identity: String,
    project: String,
    scopes: Vec<String>,
    active: bool,
    revision: i64,
}

/// What reconciling one declared account takes. Steps run in order; each
/// later step uses the revision the previous one returned.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "step", rename_all = "snake_case")]
enum IdentityStep {
    Unchanged,
    Create,
    /// Move the identity's grant from another account (only with
    /// `--allow-transfer`).
    Transfer {
        from: String,
        expected_revision: i64,
    },
    RotateIdentity {
        expected_revision: i64,
    },
    ReplaceScopes {
        expected_revision: i64,
    },
    Refused {
        reason: String,
        detail: String,
    },
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

/// Plan the steps for one declared account against the tenant's live grants.
/// A grant is never moved unless `allow_transfer`; nothing is ever revoked.
fn plan_identity(
    declared: &DeclaredAccount,
    live: &[LiveGrant],
    allow_transfer: bool,
) -> Vec<IdentityStep> {
    let own = live.iter().find(|grant| grant.account == declared.account);
    let holder = live
        .iter()
        .find(|grant| grant.identity == declared.identity && grant.account != declared.account);
    let want_scopes = sorted_scopes(&declared.scopes);
    let scopes_differ = |grant: &LiveGrant| {
        sorted_scopes(&grant.scopes) != want_scopes
            || (!declared.project.trim().is_empty() && grant.project != declared.project.trim())
    };
    match (own, holder) {
        (Some(own), _) if !own.active => vec![IdentityStep::Refused {
            reason: "grant_revoked".to_string(),
            detail: format!(
                "account {} holds a revoked grant; a revoked grant is final, use a new service account",
                declared.account
            ),
        }],
        (Some(_), Some(holder)) => vec![IdentityStep::Refused {
            reason: "UDB_GRANT_OWNED_BY_OTHER".to_string(),
            detail: format!(
                "identity '{}' belongs to account {} and account {} already holds another grant; revoke one of them first",
                declared.identity, holder.account, declared.account
            ),
        }],
        (Some(own), None) => {
            let mut steps = Vec::new();
            if own.identity != declared.identity {
                steps.push(IdentityStep::RotateIdentity {
                    expected_revision: own.revision,
                });
            }
            if scopes_differ(own) {
                steps.push(IdentityStep::ReplaceScopes {
                    expected_revision: own.revision,
                });
            }
            if steps.is_empty() {
                steps.push(IdentityStep::Unchanged);
            }
            steps
        }
        (None, Some(holder)) if !allow_transfer => vec![IdentityStep::Refused {
            reason: "UDB_GRANT_OWNED_BY_OTHER".to_string(),
            detail: format!(
                "identity '{}' is granted to account {}; pass --allow-transfer to move it to {} on purpose",
                declared.identity, holder.account, declared.account
            ),
        }],
        (None, Some(holder)) => {
            let mut steps = vec![IdentityStep::Transfer {
                from: holder.account.clone(),
                expected_revision: holder.revision,
            }];
            if scopes_differ(holder) {
                steps.push(IdentityStep::ReplaceScopes {
                    expected_revision: holder.revision + 1,
                });
            }
            steps
        }
        (None, None) => vec![IdentityStep::Create],
    }
}

fn live_grant(grant: &authn_entity_pb::ServiceAccountGrant) -> LiveGrant {
    LiveGrant {
        account: grant.user_id.clone(),
        identity: grant.service_identity.clone(),
        project: grant.project_id.clone(),
        scopes: serde_json::from_str::<Vec<String>>(&grant.approved_scopes_json)
            .unwrap_or_default(),
        active: grant.status.eq_ignore_ascii_case("ACTIVE"),
        revision: grant.revision,
    }
}

async fn identity_command(
    apply: bool,
    file: &str,
    tenant: &str,
    allow_transfer: bool,
) -> Result<serde_json::Value, String> {
    if file.trim().is_empty() {
        return Err("provide -f <identities.yaml>".to_string());
    }
    let parsed: IdentityFile = read_structured(file)?;
    let tenant = [tenant, parsed.tenant.as_str()]
        .into_iter()
        .map(str::trim)
        .find(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            env::var("UDB_TENANT_ID")
                .ok()
                .filter(|v| !v.trim().is_empty())
        })
        .ok_or_else(|| "pass --tenant <uuid> or set `tenant:` in the file".to_string())?;
    if !parsed.tenant.trim().is_empty() && parsed.tenant.trim() != tenant {
        return Err(format!(
            "the file is for tenant '{}' but --tenant is '{tenant}'",
            parsed.tenant.trim()
        ));
    }
    let mut client = AuthnServiceClient::connect(super::authz_cli::auth_target())
        .await
        .map_err(|err| format!("failed to connect to authn service: {err}"))?;
    let mut live = Vec::new();
    let mut page_token = String::new();
    loop {
        let page = client
            .list_service_account_grants(super::authz_cli::with_metadata(
                authn_pb::ListServiceAccountGrantsRequest {
                    tenant_id: tenant.clone(),
                    page_size: 200,
                    page_token: page_token.clone(),
                },
            ))
            .await
            .map_err(|err| format!("list grants failed: {err}"))?
            .into_inner();
        live.extend(page.grants.iter().map(live_grant));
        if page.next_page_token.is_empty() {
            break;
        }
        page_token = page.next_page_token;
    }

    let mut plan = Vec::new();
    let mut refused = 0usize;
    for declared in &parsed.service_accounts {
        let steps = plan_identity(declared, &live, allow_transfer);
        refused += steps
            .iter()
            .filter(|step| matches!(step, IdentityStep::Refused { .. }))
            .count();
        plan.push((declared.clone(), steps));
    }
    let declared_accounts: std::collections::HashSet<&str> = parsed
        .service_accounts
        .iter()
        .map(|account| account.account.as_str())
        .collect();
    let undeclared: Vec<&LiveGrant> = live
        .iter()
        .filter(|grant| grant.active && !declared_accounts.contains(grant.account.as_str()))
        .collect();

    let mut results = Vec::new();
    if apply {
        if refused > 0 {
            return Err(format!(
                "{refused} account(s) cannot be reconciled; run `udb identity diff` to see why. Nothing was changed"
            ));
        }
        for (declared, steps) in &plan {
            let mut revision = 0i64;
            for step in steps {
                let reason = if declared.reason.trim().is_empty() {
                    "udb identity apply".to_string()
                } else {
                    declared.reason.clone()
                };
                let grant = match step {
                    IdentityStep::Unchanged | IdentityStep::Refused { .. } => continue,
                    IdentityStep::Create => {
                        client
                            .create_service_account_grant(super::authz_cli::with_metadata(
                                authn_pb::CreateServiceAccountGrantRequest {
                                    tenant_id: tenant.clone(),
                                    user_id: declared.account.clone(),
                                    service_identity: declared.identity.clone(),
                                    project_id: declared.project.clone(),
                                    approved_scopes: sorted_scopes(&declared.scopes),
                                    reason,
                                },
                            ))
                            .await
                            .map_err(|err| {
                                format!("create grant for {} failed: {err}", declared.account)
                            })?
                            .into_inner()
                            .grant
                    }
                    IdentityStep::Transfer {
                        from,
                        expected_revision,
                    } => {
                        client
                            .transfer_service_account_grant(super::authz_cli::with_metadata(
                                authn_pb::TransferServiceAccountGrantRequest {
                                    tenant_id: tenant.clone(),
                                    from_user_id: from.clone(),
                                    to_user_id: declared.account.clone(),
                                    expected_revision: *expected_revision,
                                    reason,
                                },
                            ))
                            .await
                            .map_err(|err| {
                                format!("transfer grant to {} failed: {err}", declared.account)
                            })?
                            .into_inner()
                            .grant
                    }
                    IdentityStep::RotateIdentity { expected_revision } => {
                        client
                            .rotate_service_account_identity(super::authz_cli::with_metadata(
                                authn_pb::RotateServiceAccountIdentityRequest {
                                    tenant_id: tenant.clone(),
                                    user_id: declared.account.clone(),
                                    new_service_identity: declared.identity.clone(),
                                    expected_revision: revision.max(*expected_revision),
                                    reason,
                                },
                            ))
                            .await
                            .map_err(|err| {
                                format!("rotate identity for {} failed: {err}", declared.account)
                            })?
                            .into_inner()
                            .grant
                    }
                    IdentityStep::ReplaceScopes { expected_revision } => {
                        client
                            .replace_service_account_grant(super::authz_cli::with_metadata(
                                authn_pb::ReplaceServiceAccountGrantRequest {
                                    tenant_id: tenant.clone(),
                                    user_id: declared.account.clone(),
                                    approved_scopes: sorted_scopes(&declared.scopes),
                                    project_id: declared.project.clone(),
                                    reason,
                                    expected_revision: revision.max(*expected_revision),
                                },
                            ))
                            .await
                            .map_err(|err| {
                                format!("replace scopes for {} failed: {err}", declared.account)
                            })?
                            .into_inner()
                            .grant
                    }
                };
                if let Some(grant) = grant {
                    revision = grant.revision;
                }
            }
            results.push(serde_json::json!({
                "account": declared.account,
                "identity": declared.identity,
                "revision": revision,
            }));
        }
    }
    Ok(serde_json::json!({
        "tenant_id": tenant,
        "applied": apply,
        "plan": plan
            .iter()
            .map(|(declared, steps)| serde_json::json!({
                "account": declared.account,
                "identity": declared.identity,
                "steps": steps,
            }))
            .collect::<Vec<_>>(),
        "refused": refused,
        "results": results,
        // Reported, never revoked: removing access is an explicit
        // `udb auth grant revoke`.
        "undeclared_active_grants": undeclared
            .iter()
            .map(|grant| serde_json::json!({"account": grant.account, "identity": grant.identity}))
            .collect::<Vec<_>>(),
    }))
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

    let mut accounts = project.service_accounts.clone();
    if !project.service_accounts_file.trim().is_empty() {
        let nested: IdentityFile =
            read_structured(&relative_to(file, project.service_accounts_file.trim()))?;
        accounts.extend(nested.service_accounts);
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

#[cfg(test)]
mod ops_cli_tests {
    use super::{DeclaredAccount, IdentityStep, LiveGrant, plan_identity};

    fn declared(account: &str, identity: &str, scopes: &[&str]) -> DeclaredAccount {
        DeclaredAccount {
            account: account.into(),
            identity: identity.into(),
            project: String::new(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            reason: String::new(),
        }
    }

    fn grant(account: &str, identity: &str, scopes: &[&str], revision: i64) -> LiveGrant {
        LiveGrant {
            account: account.into(),
            identity: identity.into(),
            project: String::new(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            active: true,
            revision,
        }
    }

    /// Applying the same file twice is a no-op: a matching grant is unchanged
    /// (scope order and duplicates do not matter).
    #[test]
    fn matching_grant_is_unchanged() {
        let live = [grant(
            "a",
            "billing",
            &["udb:data:read", "udb:data:write"],
            3,
        )];
        assert_eq!(
            plan_identity(
                &declared(
                    "a",
                    "billing",
                    &["udb:data:write", "udb:data:read", "udb:data:read"]
                ),
                &live,
                false
            ),
            vec![IdentityStep::Unchanged]
        );
    }

    /// A declared identity held by another account is never moved implicitly
    /// (a bootstrap once took over a production grant this way); --allow-transfer
    /// moves it explicitly.
    #[test]
    fn identity_held_by_another_account_needs_allow_transfer() {
        let live = [grant("prod", "billing", &["udb:data:read"], 7)];
        let refused = plan_identity(
            &declared("dev", "billing", &["udb:data:read"]),
            &live,
            false,
        );
        assert!(
            matches!(&refused[..], [IdentityStep::Refused { reason, .. }] if reason == "UDB_GRANT_OWNED_BY_OTHER")
        );
        assert_eq!(
            plan_identity(&declared("dev", "billing", &["udb:data:read"]), &live, true),
            vec![IdentityStep::Transfer {
                from: "prod".into(),
                expected_revision: 7
            }]
        );
    }

    /// New accounts are created; drifted ones rotate and/or replace scopes.
    #[test]
    fn create_rotate_and_replace() {
        assert_eq!(
            plan_identity(&declared("n", "new", &["udb:data:read"]), &[], false),
            vec![IdentityStep::Create]
        );
        let live = [grant("a", "old", &["udb:data:read"], 2)];
        assert_eq!(
            plan_identity(&declared("a", "new", &["udb:data:write"]), &live, false),
            vec![
                IdentityStep::RotateIdentity {
                    expected_revision: 2
                },
                IdentityStep::ReplaceScopes {
                    expected_revision: 2
                },
            ]
        );
    }
}
