//! Authenticated reviewed catalog transitions through the ordinary broker RPCs.
//!
//! Files preserve server responses and an opaque approval token for the next
//! command. They never establish approval or application authority themselves.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};
use tonic::Request;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use udb::proto::data_broker_client::DataBrokerClient;
use udb::proto::{
    CatalogManifestRequest, CatalogVersionRequest, CatalogVersionResponse, MigrationApplyRequest,
    MigrationPlanRequest, MigrationRunRequest, MigrationStatusResponse, RequestContext,
    ReviewedCatalogTransitionEvidence, StageCatalogRequest,
};

const MAX_INPUT_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TransitionAction {
    Plan,
    Approve,
    Apply,
    Stage,
    Activate,
    Status,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransitionCommand {
    pub action: TransitionAction,
    pub project: String,
    pub tenant: Option<String>,
    pub target: Option<String>,
    pub manifest: Option<String>,
    pub plan: Option<String>,
    pub approval: Option<String>,
    pub out: Option<String>,
    pub run_id: Option<String>,
    pub catalog_id: Option<String>,
    pub expected_active_catalog_id: Option<String>,
    pub expected_active_manifest_integrity_sha256: Option<String>,
    pub idempotency_key: Option<String>,
    pub reason: String,
    pub timeout_seconds: u64,
}

const DEFAULT_REASON: &str = "reviewed catalog transition";
const DEFAULT_TIMEOUT_SECONDS: u64 = 300;

impl TransitionCommand {
    /// The command `parse` builds for `action` with only the connection flags
    /// set, for callers that drive a transition programmatically (`udb up`).
    /// The caller fills the action's own fields before `run_async`.
    pub(crate) fn new(
        action: TransitionAction,
        project: &str,
        tenant: Option<String>,
        target: Option<String>,
    ) -> Self {
        Self {
            action,
            project: project.to_string(),
            tenant,
            target,
            manifest: None,
            plan: None,
            approval: None,
            out: None,
            run_id: None,
            catalog_id: None,
            expected_active_catalog_id: None,
            expected_active_manifest_integrity_sha256: None,
            idempotency_key: None,
            reason: DEFAULT_REASON.to_string(),
            timeout_seconds: DEFAULT_TIMEOUT_SECONDS,
        }
    }
}

pub(crate) fn parse(args: &[String]) -> Result<TransitionCommand, String> {
    let action = match args.get(2).map(String::as_str) {
        Some("plan") => TransitionAction::Plan,
        Some("approve") => TransitionAction::Approve,
        Some("apply") => TransitionAction::Apply,
        Some("stage") => TransitionAction::Stage,
        Some("activate") => TransitionAction::Activate,
        Some("status") => TransitionAction::Status,
        _ => {
            return Err(
                "catalog transition requires plan|approve|apply|stage|activate|status".into(),
            );
        }
    };
    let specific: &[&str] = match action {
        TransitionAction::Plan => &[
            "--manifest",
            "--expected-active-catalog-id",
            "--expected-active-manifest-integrity-sha256",
            "--out",
            "--idempotency-key",
        ],
        TransitionAction::Approve => &["--plan", "--out", "--idempotency-key"],
        TransitionAction::Apply => &["--approval", "--idempotency-key"],
        TransitionAction::Stage => &["--manifest", "--run-id", "--idempotency-key", "--reason"],
        TransitionAction::Activate => {
            &["--catalog-id", "--run-id", "--idempotency-key", "--reason"]
        }
        TransitionAction::Status => &["--run-id", "--out"],
    };
    let mut values = BTreeMap::new();
    let mut index = 3;
    while index < args.len() {
        let name = args[index].as_str();
        if !["--project", "--tenant", "--target", "--timeout-secs"].contains(&name)
            && !specific.contains(&name)
        {
            return Err(format!("unsupported catalog transition argument '{name}'"));
        }
        let value = args
            .get(index + 1)
            .filter(|value| !value.trim().is_empty() && !value.starts_with("--"))
            .ok_or_else(|| format!("{name} requires a nonempty value"))?;
        if values.insert(name, value.clone()).is_some() {
            return Err(format!("{name} must be supplied exactly once"));
        }
        index += 2;
    }
    let required: &[&str] = match action {
        TransitionAction::Plan => &[
            "--project",
            "--manifest",
            "--expected-active-catalog-id",
            "--expected-active-manifest-integrity-sha256",
            "--out",
            "--idempotency-key",
        ],
        TransitionAction::Approve => &["--project", "--plan", "--out", "--idempotency-key"],
        TransitionAction::Apply => &["--project", "--approval", "--idempotency-key"],
        TransitionAction::Stage => &["--project", "--manifest", "--run-id", "--idempotency-key"],
        TransitionAction::Activate => {
            &["--project", "--catalog-id", "--run-id", "--idempotency-key"]
        }
        TransitionAction::Status => &["--project"],
    };
    for name in required {
        if !values.contains_key(name) {
            return Err(format!("{name} is required for catalog transition"));
        }
    }
    let timeout_seconds =
        values
            .get("--timeout-secs")
            .map_or(Ok(DEFAULT_TIMEOUT_SECONDS), |raw| {
                raw.parse::<u64>()
                    .map_err(|_| "--timeout-secs must be an integer".to_string())
            })?;
    if !(1..=3600).contains(&timeout_seconds) {
        return Err("--timeout-secs must be between 1 and 3600".into());
    }
    let get = |key: &str| values.get(key).cloned();
    Ok(TransitionCommand {
        action,
        project: get("--project").unwrap(),
        tenant: get("--tenant"),
        target: get("--target"),
        manifest: get("--manifest"),
        plan: get("--plan"),
        approval: get("--approval"),
        out: get("--out"),
        run_id: get("--run-id"),
        catalog_id: get("--catalog-id"),
        expected_active_catalog_id: get("--expected-active-catalog-id"),
        expected_active_manifest_integrity_sha256: get(
            "--expected-active-manifest-integrity-sha256",
        ),
        idempotency_key: get("--idempotency-key"),
        reason: get("--reason").unwrap_or_else(|| DEFAULT_REASON.into()),
        timeout_seconds,
    })
}

struct Caller {
    bearer: String,
    tenant: String,
    project: String,
    timeout: Duration,
}

impl Caller {
    fn from_env(command: &TransitionCommand) -> Result<Self, String> {
        let bearer = std::env::var("UDB_AUTH_TOKEN")
            .or_else(|_| std::env::var("UDB_BEARER_TOKEN"))
            .map_err(
                |_| "set UDB_AUTH_TOKEN or UDB_BEARER_TOKEN to an authorized operator bearer",
            )?;
        let bearer = bearer.trim().to_string();
        if bearer.is_empty() || bearer.bytes().any(|byte| byte.is_ascii_whitespace()) {
            return Err(
                "UDB_AUTH_TOKEN/UDB_BEARER_TOKEN must contain one nonempty bearer token".into(),
            );
        }
        let tenant = command
            .tenant
            .clone()
            .or_else(|| std::env::var("UDB_TENANT_ID").ok())
            .filter(|value| !value.trim().is_empty())
            .ok_or("--tenant or UDB_TENANT_ID is required")?;
        Ok(Self {
            bearer,
            tenant,
            project: command.project.clone(),
            timeout: Duration::from_secs(command.timeout_seconds),
        })
    }

    fn context(&self) -> RequestContext {
        RequestContext {
            tenant_id: self.tenant.clone(),
            project_id: self.project.clone(),
            purpose: "catalog.reviewed-transition".into(),
            correlation_id: format!("udb-catalog-{}", uuid::Uuid::new_v4()),
            ..Default::default()
        }
    }

    fn request<T>(&self, body: T) -> Result<Request<T>, String> {
        let mut request = Request::new(body);
        request.set_timeout(self.timeout);
        let mut authorization = format!("Bearer {}", self.bearer)
            .parse::<tonic::metadata::MetadataValue<_>>()
            .map_err(|_| "operator bearer is not valid gRPC metadata")?;
        authorization.set_sensitive(true);
        request
            .metadata_mut()
            .insert("authorization", authorization);
        for (name, value) in [
            ("x-tenant-id", self.tenant.clone()),
            ("x-udb-project-id", self.project.clone()),
            ("x-purpose", "catalog.reviewed-transition".to_string()),
            ("x-request-id", uuid::Uuid::new_v4().to_string()),
        ] {
            request.metadata_mut().insert(
                name,
                value
                    .parse()
                    .map_err(|_| format!("{name} is not valid gRPC metadata"))?,
            );
        }
        // The broker resolves user, service identity and scopes from the bearer.
        Ok(request)
    }
}

async fn connect(command: &TransitionCommand) -> Result<DataBrokerClient<Channel>, String> {
    let raw = command
        .target
        .clone()
        .or_else(|| std::env::var("UDB_GRPC_TARGET").ok())
        .or_else(|| std::env::var("UDB_GRPC_ADDR").ok())
        .ok_or("--target or UDB_GRPC_TARGET is required")?;
    let raw = raw.trim();
    if raw.is_empty() || raw.contains('@') || raw.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(
            "broker target must be a nonempty address without credentials or whitespace".into(),
        );
    }
    let target = if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.to_string()
    } else {
        format!("http://{raw}")
    };
    let mut endpoint = Endpoint::from_shared(target.clone())
        .map_err(|_| "broker target is not a valid endpoint")?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(command.timeout_seconds));
    if target.starts_with("https://") {
        let ca_path = std::env::var("UDB_TLS_CA_FILE")
            .map_err(|_| "HTTPS catalog transitions require UDB_TLS_CA_FILE")?;
        let ca = read_bounded(&ca_path)?;
        endpoint = endpoint
            .tls_config(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca)))
            .map_err(|_| "catalog transition TLS configuration is invalid")?;
    }
    let channel = endpoint
        .connect()
        .await
        .map_err(|_| "could not connect to the catalog broker")?;
    Ok(DataBrokerClient::new(channel).max_encoding_message_size(MAX_INPUT_BYTES as usize))
}

fn read_bounded(path: &str) -> Result<Vec<u8>, String> {
    let file = File::open(path).map_err(|_| "could not open the transition input file")?;
    let mut bytes = Vec::new();
    file.take(MAX_INPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "could not read the transition input file")?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_INPUT_BYTES {
        return Err("transition input must be nonempty and at most 16 MiB".into());
    }
    Ok(bytes)
}

pub(crate) fn read_json(path: &str) -> Result<Value, String> {
    serde_json::from_slice(&read_bounded(path)?)
        .map_err(|_| "transition input is not valid JSON".into())
}

fn text(value: &Value, field: &str) -> Result<String, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| format!("transition response file requires {field}"))
}

fn file_binding(value: &Value, project: &str) -> Result<String, String> {
    if value.get("schema_version").and_then(Value::as_u64) != Some(1) {
        return Err("transition response file requires schema_version=1".into());
    }
    if text(value, "project_id")? != project {
        return Err("transition response file project_id does not match --project".into());
    }
    text(value, "run_id")
}

pub(crate) fn write_new_json(path: &str, value: &Value) -> Result<(), String> {
    let bytes =
        serde_json::to_vec_pretty(value).map_err(|_| "could not encode transition response")?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(Path::new(path)).map_err(
        |_| "transition --out must be a new writable file; existing files are never overwritten",
    )?;
    file.write_all(&bytes)
        .and_then(|()| file.write_all(b"\n"))
        .and_then(|()| file.sync_all())
        .map_err(|_| "could not persist the transition response file".to_string())
}

fn evidence_json(evidence: &ReviewedCatalogTransitionEvidence) -> Value {
    json!({
        "run_id": evidence.run_id, "tenant_id": evidence.tenant_id, "project_id": evidence.project_id,
        "expected_active_catalog_id": evidence.expected_active_catalog_id,
        "expected_active_manifest_integrity_sha256": evidence.expected_active_manifest_integrity_sha256,
        "target_manifest_integrity_sha256": evidence.target_manifest_integrity_sha256,
        "target_schema_checksum_sha256": evidence.target_schema_checksum_sha256,
        "operations_hash": evidence.operations_hash,
        "reviewed_operation_fingerprints": evidence.reviewed_operation_fingerprints,
        "approved_by": evidence.approved_by, "approved_at_unix": evidence.approved_at_unix,
        "application_state": evidence.application_state, "applied_operations_hash": evidence.applied_operations_hash,
        "applied_at_unix": evidence.applied_at_unix, "application_evidence_sha256": evidence.application_evidence_sha256,
    })
}

fn status_json(status: &MigrationStatusResponse) -> Value {
    json!({
        "schema_version": 1, "run_id": status.run_id, "project_id": status.project_id,
        "catalog_version": status.catalog_version, "state": status.state,
        "started_at": status.started_at, "finished_at": status.finished_at,
        "error": status.error, "applyable": status.applyable,
        "operations": status.operations.iter().map(|operation| json!({
            "index": operation.index, "backend": operation.backend, "resource_uri": operation.resource_uri,
            "operation_kind": operation.operation_kind, "status": operation.status, "error": operation.error,
        })).collect::<Vec<_>>(),
        "reviewed_catalog_transition": status.reviewed_catalog_transition.as_ref().map(evidence_json),
    })
}

fn validate_status_binding(
    status: &MigrationStatusResponse,
    caller: &Caller,
    run: &str,
) -> Result<(), String> {
    let evidence = status
        .reviewed_catalog_transition
        .as_ref()
        .ok_or("broker returned no native reviewed transition evidence")?;
    if status.project_id != caller.project
        || status.run_id != run
        || evidence.project_id != caller.project
        || evidence.tenant_id != caller.tenant
        || evidence.run_id != run
    {
        return Err("broker status does not match the requested tenant/project/run".into());
    }
    Ok(())
}

fn catalog_json(catalog: &CatalogVersionResponse, run: &str) -> Value {
    json!({ "schema_version": 1, "reviewed_migration_run_id": run, "catalog_id": catalog.catalog_id,
        "project_id": catalog.project_id, "version": catalog.version, "status": catalog.status,
        "checksum_sha256": catalog.checksum_sha256, "manifest_integrity_sha256": catalog.manifest_integrity_sha256,
        "errors": catalog.errors, "warnings": catalog.warnings })
}

fn active_catalog_json(
    project: &str,
    versions: &[CatalogVersionResponse],
) -> Result<Value, String> {
    let mut active = versions.iter().filter(|catalog| catalog.status == "ACTIVE");
    let catalog = active
        .next()
        .ok_or("broker returned no durable ACTIVE catalog")?;
    if active.next().is_some()
        || catalog.project_id != project
        || catalog.catalog_id.parse::<uuid::Uuid>().is_err()
        || catalog.manifest_integrity_sha256.len() != 64
        || !catalog
            .manifest_integrity_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(
            "broker returned no unambiguous durable ACTIVE catalog id/manifest integrity".into(),
        );
    }
    Ok(catalog_json(catalog, ""))
}

fn rpc_error(action: &str, error: tonic::Status) -> String {
    // The broker's refusal message names the cause and the next step (it never
    // carries credentials or approval tokens); response bodies are not printed.
    let message: String = error.message().chars().take(4000).collect();
    if message.trim().is_empty() {
        format!(
            "catalog transition {action} refused: code={:?}",
            error.code()
        )
    } else {
        format!(
            "catalog transition {action} refused: code={:?}: {message}",
            error.code()
        )
    }
}

/// The flags that address the same broker and tenant as this invocation.
pub(crate) fn connection_flags(command: &TransitionCommand, tenant: &str) -> String {
    let mut flags = format!("--project {}", command.project);
    if !tenant.is_empty() {
        flags.push_str(&format!(" --tenant {tenant}"));
    }
    if let Some(target) = command.target.as_deref() {
        flags.push_str(&format!(" --target {target}"));
    }
    flags
}

/// The exact `plan` command for advancing the ACTIVE catalog `active`
/// (a `catalog_json` value), with the expected id and outer manifest integrity
/// filled in and a fresh idempotency key.
fn next_plan_command(command: &TransitionCommand, tenant: &str, active: &Value) -> String {
    let id = active["catalog_id"].as_str().unwrap_or_default();
    let integrity = active["manifest_integrity_sha256"]
        .as_str()
        .unwrap_or_default();
    let short: String = id.chars().take(8).collect();
    format!(
        "udb manifest-export && udb catalog transition plan {} --manifest udb_catalog_manifest.json \
         --expected-active-catalog-id {id} --expected-active-manifest-integrity-sha256 {integrity} \
         --out plan-{short}.json --idempotency-key plan-{}",
        connection_flags(command, tenant),
        uuid::Uuid::new_v4()
    )
}

/// The next command for a reviewed run in `state` (from `status --run-id`).
pub(crate) fn next_run_command(
    command: &TransitionCommand,
    tenant: &str,
    run: &str,
    state: &str,
) -> String {
    let flags = connection_flags(command, tenant);
    let key = uuid::Uuid::new_v4();
    let short: String = run.chars().take(8).collect();
    match state {
        "PREFLIGHT" => format!(
            "review the plan file's operations, then: udb catalog transition approve {flags} \
             --plan plan-<id>.json --out approval-{short}.json --idempotency-key approve-{key}"
        ),
        "APPROVED" | "APPLYING" | "VERIFYING" => format!(
            "udb catalog transition apply {flags} --approval approval-{short}.json \
             --idempotency-key apply-{key}"
        ),
        "COMPLETED" => format!(
            "udb catalog transition stage {flags} --manifest udb_catalog_manifest.json \
             --run-id {run} --idempotency-key stage-{key}; then activate the staged catalog id it \
             prints: udb catalog transition activate {flags} --catalog-id <staged-id> --run-id {run} \
             --idempotency-key activate-{key}"
        ),
        "ERROR" => format!(
            "read the run's error, fix the cause, and plan again (`udb catalog transition status \
             {flags}` prints the plan command)"
        ),
        other => format!(
            "run state {other}: rerun `udb catalog transition status {flags} --run-id {run}`"
        ),
    }
}

pub(crate) fn run(command: TransitionCommand) -> i32 {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(_) => {
            eprintln!("catalog transition: could not create the async runtime");
            return 1;
        }
    };
    match runtime.block_on(run_async(command)) {
        Ok(value) => {
            super::output_json(&value, "catalog transition");
            0
        }
        Err(error) => {
            eprintln!("{error}");
            1
        }
    }
}

pub(crate) async fn run_async(command: TransitionCommand) -> Result<Value, String> {
    let caller = Caller::from_env(&command)?;
    // Validate response files before network mutation. The server independently
    // validates their echoes against the durable native run under its lock.
    let input = match command.action {
        TransitionAction::Approve => Some(read_json(command.plan.as_deref().unwrap())?),
        TransitionAction::Apply => Some(read_json(command.approval.as_deref().unwrap())?),
        _ => None,
    };
    let input_run = input
        .as_ref()
        .map(|value| file_binding(value, &command.project))
        .transpose()?;
    let mut client = connect(&command).await?;
    match command.action {
        TransitionAction::Plan => {
            let response = client
                .plan_migration(
                    caller.request(MigrationPlanRequest {
                        context: Some(caller.context()),
                        project_id: command.project.clone(),
                        dry_run: false,
                        candidate_manifest_json: read_bounded(
                            command.manifest.as_deref().unwrap(),
                        )?,
                        expected_active_catalog_id: command
                            .expected_active_catalog_id
                            .clone()
                            .unwrap(),
                        expected_active_manifest_integrity_sha256: command
                            .expected_active_manifest_integrity_sha256
                            .clone()
                            .unwrap(),
                        idempotency_key: command.idempotency_key.unwrap(),
                    })?,
                )
                .await
                .map_err(|error| rpc_error("plan", error))?
                .into_inner();
            let evidence = response
                .reviewed_catalog_transition
                .as_ref()
                .ok_or("broker returned no reviewed transition evidence")?;
            if response.project_id != command.project
                || evidence.project_id != command.project
                || evidence.tenant_id != caller.tenant
                || evidence.run_id != response.run_id
                || Some(evidence.expected_active_catalog_id.as_str())
                    != command.expected_active_catalog_id.as_deref()
                || Some(evidence.expected_active_manifest_integrity_sha256.as_str())
                    != command.expected_active_manifest_integrity_sha256.as_deref()
                || response.operations_hash != evidence.operations_hash
            {
                return Err(
                    "broker plan response does not match the requested tenant/project/run".into(),
                );
            }
            let value = json!({ "schema_version": 1, "run_id": response.run_id, "project_id": response.project_id,
                "catalog_version": response.catalog_version, "state": response.state,
                "operations": response.operations, "requires_review": response.requires_review, "blocked": response.blocked,
                "operations_hash": response.operations_hash, "reviewed_catalog_transition": evidence_json(evidence) });
            write_new_json(command.out.as_deref().unwrap(), &value)?;
            Ok(value)
        }
        TransitionAction::Approve => {
            let input = input.as_ref().unwrap();
            let evidence = input
                .get("reviewed_catalog_transition")
                .ok_or("plan requires native reviewed_catalog_transition evidence")?;
            let run = input_run.unwrap();
            if text(evidence, "run_id")? != run
                || text(evidence, "project_id")? != command.project
                || text(evidence, "tenant_id")? != caller.tenant
            {
                return Err(
                    "plan reviewed evidence does not match the requested tenant/project/run".into(),
                );
            }
            let fingerprints = evidence
                .get("reviewed_operation_fingerprints")
                .and_then(Value::as_array)
                .ok_or("plan requires reviewed_operation_fingerprints")?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .filter(|value| !value.is_empty())
                        .map(ToString::to_string)
                        .ok_or("plan review fingerprints must be nonempty strings".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let response = client
                .approve_migration_plan(caller.request(MigrationRunRequest {
                    context: Some(caller.context()),
                    run_id: run.clone(),
                    project_id: command.project.clone(),
                    idempotency_key: command.idempotency_key.unwrap(),
                    expected_operations_hash: text(evidence, "operations_hash")?,
                    reviewed_operation_fingerprints: fingerprints,
                })?)
                .await
                .map_err(|error| rpc_error("approve", error))?
                .into_inner();
            validate_status_binding(&response, &caller, &run)?;
            let token = response
                .approval_token
                .as_deref()
                .filter(|token| !token.is_empty())
                .ok_or("broker returned no approval token")?;
            let mut private = status_json(&response);
            private["approval_token"] = Value::String(token.to_string());
            write_new_json(command.out.as_deref().unwrap(), &private)?;
            let mut public = status_json(&response);
            public["approval_token_saved"] = Value::Bool(true);
            Ok(public)
        }
        TransitionAction::Apply => {
            let run = input_run.unwrap();
            let response = client
                .apply_migration(caller.request(MigrationApplyRequest {
                    context: Some(caller.context()),
                    run_id: run.clone(),
                    project_id: command.project,
                    approval_token: text(input.as_ref().unwrap(), "approval_token")?,
                    idempotency_key: command.idempotency_key.unwrap(),
                })?)
                .await
                .map_err(|error| rpc_error("apply", error))?
                .into_inner();
            validate_status_binding(&response, &caller, &run)?;
            if response.state != "COMPLETED" {
                return Err("broker migration application did not reach COMPLETED".into());
            }
            Ok(status_json(&response))
        }
        TransitionAction::Stage => {
            let run = command.run_id.unwrap();
            let response = client
                .stage_catalog(caller.request(StageCatalogRequest {
                    context: Some(caller.context()),
                    manifest_json: read_bounded(command.manifest.as_deref().unwrap())?,
                    project_id: command.project.clone(),
                    reason: command.reason,
                    idempotency_key: command.idempotency_key.unwrap(),
                    reviewed_migration_run_id: run.clone(),
                })?)
                .await
                .map_err(|error| rpc_error("stage", error))?
                .into_inner();
            if response.project_id != command.project
                || response.status != "STAGED"
                || response.manifest_integrity_sha256.is_empty()
            {
                return Err("broker did not return the requested durable STAGED catalog".into());
            }
            Ok(catalog_json(&response, &run))
        }
        TransitionAction::Activate => {
            let run = command.run_id.unwrap();
            let response = client
                .activate_catalog(caller.request(CatalogVersionRequest {
                    context: Some(caller.context()),
                    project_id: command.project.clone(),
                    version: command.catalog_id.clone().unwrap(),
                    reason: command.reason,
                    idempotency_key: command.idempotency_key.unwrap(),
                    reviewed_migration_run_id: run.clone(),
                })?)
                .await
                .map_err(|error| rpc_error("activate", error))?
                .into_inner();
            if response.project_id != command.project
                || response.status != "ACTIVE"
                || Some(response.catalog_id.as_str()) != command.catalog_id.as_deref()
            {
                return Err("broker did not return the requested durable ACTIVE catalog".into());
            }
            Ok(catalog_json(&response, &run))
        }
        TransitionAction::Status => {
            if command.run_id.is_none() {
                let response = client
                    .get_catalog_versions(caller.request(CatalogManifestRequest {
                        context: Some(caller.context()),
                        ..Default::default()
                    })?)
                    .await
                    .map_err(|error| rpc_error("status", error))?
                    .into_inner();
                if response.project_id != command.project {
                    return Err("broker catalog discovery returned a different project".into());
                }
                let mut value = active_catalog_json(&command.project, &response.versions)?;
                value["next_command"] =
                    Value::String(next_plan_command(&command, &caller.tenant, &value));
                value["next_command_needs"] = Value::String(
                    "UDB_AUTH_TOKEN (or UDB_BEARER_TOKEN) set to an authorized operator bearer"
                        .to_string(),
                );
                if let Some(out) = command.out {
                    write_new_json(&out, &value)?;
                }
                return Ok(value);
            }
            let run = command.run_id.clone().unwrap();
            let response = client
                .get_migration_status(caller.request(MigrationRunRequest {
                    context: Some(caller.context()),
                    project_id: command.project.clone(),
                    run_id: run.clone(),
                    ..Default::default()
                })?)
                .await
                .map_err(|error| rpc_error("status", error))?
                .into_inner();
            validate_status_binding(&response, &caller, &run)?;
            let mut value = status_json(&response);
            value["next_command"] = Value::String(next_run_command(
                &command,
                &caller.tenant,
                &run,
                &response.state,
            ));
            if let Some(out) = command.out.clone() {
                write_new_json(&out, &value)?;
            }
            Ok(value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transition_active_discovery_requires_durable_outer_integrity() {
        let mut catalog = CatalogVersionResponse {
            catalog_id: uuid::Uuid::new_v4().to_string(),
            project_id: "owned".into(),
            status: "ACTIVE".into(),
            checksum_sha256: "inner-selector".into(),
            ..Default::default()
        };
        assert!(active_catalog_json("owned", &[catalog.clone()]).is_err());
        catalog.manifest_integrity_sha256 = "a".repeat(64);
        let discovered = active_catalog_json("owned", &[catalog.clone()]).unwrap();
        assert_eq!(discovered["checksum_sha256"], "inner-selector");
        assert_eq!(discovered["manifest_integrity_sha256"], "a".repeat(64));
        assert!(active_catalog_json("other", &[catalog.clone()]).is_err());
        assert!(active_catalog_json("owned", &[catalog.clone(), catalog]).is_err());
    }

    fn status_command() -> TransitionCommand {
        parse(
            &["catalog", "transition", "status", "--project", "virtued"]
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        )
        .expect("status parses")
    }

    #[test]
    fn status_prints_the_exact_next_plan_command_with_the_active_identity() {
        let command = status_command();
        let active = json!({
            "catalog_id": "3f1c0d2e-0000-4000-8000-000000000001",
            "manifest_integrity_sha256": "a".repeat(64),
        });
        let next = next_plan_command(&command, "t-1", &active);
        assert!(
            next.contains("udb catalog transition plan --project virtued --tenant t-1"),
            "{next}"
        );
        assert!(
            next.contains("--expected-active-catalog-id 3f1c0d2e-0000-4000-8000-000000000001"),
            "{next}"
        );
        assert!(
            next.contains(&format!(
                "--expected-active-manifest-integrity-sha256 {}",
                "a".repeat(64)
            )),
            "{next}"
        );
        assert!(next.contains("--idempotency-key plan-"), "{next}");
        assert!(next.contains("udb manifest-export"), "{next}");
        // The command it prints parses as a complete plan invocation.
        let plan_part = next.split("&& ").nth(1).expect("plan part");
        let argv: Vec<String> = plan_part
            .split_whitespace()
            .skip(1)
            .map(str::to_string)
            .collect();
        let parsed = parse(&argv).expect("the printed plan command is complete");
        assert_eq!(parsed.action, TransitionAction::Plan);

        let after_apply = next_run_command(&command, "t-1", "run-123", "COMPLETED");
        assert!(
            after_apply.contains("transition stage --project virtued"),
            "{after_apply}"
        );
        assert!(after_apply.contains("--run-id run-123"), "{after_apply}");
    }

    #[test]
    fn rpc_errors_carry_the_brokers_refusal_message() {
        let message = rpc_error(
            "stage",
            tonic::Status::unknown("candidate catalog version 2.0.0 is not compatible"),
        );
        assert!(message.contains("Unknown"), "{message}");
        assert!(message.contains("is not compatible"), "{message}");
    }

    #[test]
    fn transition_approval_file_never_becomes_native_authority() {
        let forged = json!({"schema_version":1,"project_id":"other","run_id":"run"});
        assert!(
            file_binding(&forged, "owned")
                .unwrap_err()
                .contains("project_id")
        );
        let status = MigrationStatusResponse {
            run_id: "run".into(),
            project_id: "owned".into(),
            approval_token: Some("private-token".into()),
            ..Default::default()
        };
        assert!(status_json(&status).get("approval_token").is_none());
    }

    #[test]
    fn transition_response_file_preserves_prior_review_and_restricts_new_token() {
        let path =
            std::env::temp_dir().join(format!("udb-reviewed-token-{}.json", uuid::Uuid::new_v4()));
        let path = path.to_str().unwrap();
        let original = json!({"approval_token":"opaque-native-token"});
        write_new_json(path, &original).expect("persist a new response");
        assert!(write_new_json(path, &json!({"approval_token":"replacement"})).is_err());
        assert_eq!(read_json(path).unwrap(), original);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        std::fs::remove_file(path).unwrap();
    }
}
