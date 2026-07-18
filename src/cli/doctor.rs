//! main.rs split — doctor (Phase H).
use super::*;
use std::collections::HashSet;
use udb::runtime::preflight::PreflightFinding;

/// A remediation derived from a doctor finding, classified by whether `--fix`
/// may APPLY it. `--fix` only ever touches LOCAL FILES (the `.env` in the cwd);
/// it never writes to a backend/DB, never makes a network call, and never
/// applies a finding whose correct edit is ambiguous or would loosen posture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Remediation {
    /// AUTO-FIXABLE (local file): ensure `key=value` exists in `.env`. Reserved
    /// for documented, NON-SECRET safe defaults that can only enable a required
    /// feature or tighten posture — never a secret, never a value that loosens
    /// security, never remote state. An already-defined key is left untouched.
    SetEnvDefault {
        finding: String,
        key: &'static str,
        value: &'static str,
        rationale: &'static str,
    },
    /// AUTO-FIXABLE (local file): rewrite the local `.env`'s CRLF (and lone CR)
    /// line endings to LF. A CRLF in `.env` makes downstream parsers read a
    /// trailing `\r` into values (e.g. reqwest "builder error" on a DSN).
    NormalizeEnvCrlf { detail: String },
    /// ADVISORY ONLY — `--fix` NEVER applies this. The operator must run/set it
    /// themselves: a real secret, a reachable endpoint, an interface choice, or
    /// any change that loosens posture or touches remote/DB/network state.
    Advisory { finding: String, command: String },
}

impl Remediation {
    /// Whether `--fix` is allowed to apply this remediation to a local file.
    pub(crate) fn is_auto_fixable(&self) -> bool {
        matches!(
            self,
            Remediation::SetEnvDefault { .. } | Remediation::NormalizeEnvCrlf { .. }
        )
    }

    /// One-line human description for the report.
    pub(crate) fn describe(&self) -> String {
        match self {
            Remediation::SetEnvDefault {
                finding,
                key,
                value,
                rationale,
            } => format!("[{finding}] auto-fix (.env): set {key}={value} — {rationale}"),
            Remediation::NormalizeEnvCrlf { detail } => {
                format!("[env-crlf] auto-fix (.env): {detail}")
            }
            Remediation::Advisory { finding, command } => {
                format!("[{finding}] advisory (run yourself): {command}")
            }
        }
    }
}

/// Map an enterprise preflight finding to its remediation. PURE: no IO.
///
/// Only `sessions` is auto-fixable: `UDB_SESSION_ENABLED=true` is a documented,
/// non-secret toggle that merely ENABLES the required login path (it can never
/// loosen security). Its companion hash secret has no safe default and surfaces
/// as the separate advisory `password-hash-secret` finding. Everything else —
/// encryption/hash secrets (must be unique secrets), Redis/auth-plane endpoints
/// (operator-specific), and `authz-default-deny` (whose documented fix would
/// LOOSEN authz to default-allow) — stays advisory and is never auto-applied.
pub(crate) fn remediation_for_preflight(finding: &PreflightFinding) -> Remediation {
    match finding.name {
        "sessions" => Remediation::SetEnvDefault {
            finding: finding.name.to_string(),
            key: "UDB_SESSION_ENABLED",
            value: "true",
            rationale: "login (Authenticate) requires server-side sessions enabled",
        },
        _ => Remediation::Advisory {
            finding: finding.name.to_string(),
            command: finding.fix.to_string(),
        },
    }
}

/// Derive a remediation from a single TLS-path check (master-plan 7.4's canonical
/// example). Returns `None` when the check PASSES — the path is not required, or
/// it is required and the file exists — and an ADVISORY shell `export` when the
/// path is required-but-missing (unset, or set to a non-existent file).
///
/// PURE (no IO). The suggestion is PARAMETRIZED from the check itself — the env
/// var name, a placeholder filename, and the file's role — never a hardcoded
/// block. It is advisory-only: a TLS cert/key/CA path is operator-specific real
/// material, so `--fix` must NEVER invent or write one; the operator runs the
/// `export` (or sets the `.env` line) themselves with their actual path.
pub(crate) fn tls_path_remediation(
    required: bool,
    exists: bool,
    finding: &'static str,
    env_key: &'static str,
    current: &str,
    placeholder: &'static str,
    role: &'static str,
) -> Option<Remediation> {
    if !required || exists {
        return None;
    }
    let state = if current.trim().is_empty() {
        "currently unset".to_string()
    } else {
        format!("currently '{current}', which does not exist")
    };
    Some(Remediation::Advisory {
        finding: finding.to_string(),
        command: format!(
            "export {env_key}=/path/to/{placeholder} # set this to your {role} ({state})"
        ),
    })
}

/// PURE: append `key=value` to `.env` content if `key` is not already defined.
/// Returns the (possibly unchanged) content and whether a line was added. Only
/// ADDS a missing default — an existing definition (even commented-out keys are
/// ignored) is never rewritten, so a deliberate operator value always wins.
pub(crate) fn env_with_default(content: &str, key: &str, value: &str) -> (String, bool) {
    let already_defined = content.lines().any(|line| {
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') {
            return false;
        }
        trimmed
            .split_once('=')
            .map(|(k, _)| k.trim() == key)
            .unwrap_or(false)
    });
    if already_defined {
        return (content.to_string(), false);
    }
    let mut out = content.to_string();
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(key);
    out.push('=');
    out.push_str(value);
    out.push('\n');
    (out, true)
}

/// PURE: normalize CRLF and lone-CR line endings to LF. Returns the (possibly
/// unchanged) content and whether anything changed.
pub(crate) fn normalize_crlf(content: &str) -> (String, bool) {
    if !content.contains('\r') {
        return (content.to_string(), false);
    }
    (content.replace("\r\n", "\n").replace('\r', "\n"), true)
}

/// Build a CRLF remediation for a local `.env` ONLY when the file exists and
/// actually contains a CR. Returns `None` otherwise (the common case), so the
/// remediation list stays empty unless there is a real, local hygiene fix.
fn crlf_remediation_for_env(path: &std::path::Path) -> Option<Remediation> {
    let content = std::fs::read_to_string(path).ok()?;
    if content.contains('\r') {
        Some(Remediation::NormalizeEnvCrlf {
            detail: "normalize CRLF/CR line endings to LF (a trailing \\r corrupts env values)"
                .to_string(),
        })
    } else {
        None
    }
}

/// Apply the AUTO-FIXABLE remediations to the local `.env` file ONLY. Returns a
/// human log of every change made (empty when nothing was applied).
///
/// The ONLY side effect is reading and rewriting `./.env`. There is no path here
/// to a database, a backend, or the network — advisory remediations are skipped
/// entirely. Safe defaults are added before line-ending normalization so a
/// freshly-appended line is also normalized in the same pass.
fn apply_local_fixes(remediations: &[Remediation]) -> Vec<String> {
    let mut log = Vec::new();
    if !remediations.iter().any(|r| r.is_auto_fixable()) {
        return log;
    }
    let env_path = std::path::Path::new(".env");
    let mut content = std::fs::read_to_string(env_path).unwrap_or_default();
    let mut changed = false;
    for remediation in remediations {
        match remediation {
            Remediation::SetEnvDefault { key, value, .. } => {
                let (next, did) = env_with_default(&content, key, value);
                if did {
                    content = next;
                    changed = true;
                    log.push(format!("appended {key}={value} to .env"));
                }
            }
            Remediation::NormalizeEnvCrlf { .. } => {
                let (next, did) = normalize_crlf(&content);
                if did {
                    content = next;
                    changed = true;
                    log.push("normalized .env line endings (CRLF/CR → LF)".to_string());
                }
            }
            Remediation::Advisory { .. } => {}
        }
    }
    if changed && let Err(err) = std::fs::write(env_path, content) {
        log.push(format!("failed to write .env: {err}"));
    }
    log
}

#[derive(Debug, Serialize)]
pub(crate) struct DoctorReport {
    pub(crate) passed: bool,
    postgres_configured: bool,
    redis_configured: bool,
    qdrant_configured: bool,
    s3_configured: bool,
    mongodb_configured: bool,
    neo4j_configured: bool,
    clickhouse_configured: bool,
    encryption_configured: bool,
    tls_configured: bool,
    tls_cert_exists: bool,
    tls_key_exists: bool,
    tls_ca_exists: bool,
    system_catalog: Option<SystemCatalogInspection>,
    postgres_privileges: Option<PostgresPrivilegeReport>,
    backend_probes: Vec<BackendProbeResult>,
    backend_capabilities: Vec<BackendCapabilityMatrixEntry>,
    native_services: Vec<udb::runtime::service::native_registry::NativeServiceStatus>,
    /// Operator-declared deployment tier (`UDB_DEPLOYMENT_TIER`), resolved once
    /// at startup (master-plan 3.5). `None` = no tier declared (permissive dev
    /// default — the startup tier floor is not enforced).
    deployment_tier: Option<String>,
    errors: Vec<String>,
    warnings: Vec<String>,
    /// Remediations derived from the findings above. Each is either an
    /// auto-fixable LOCAL-FILE edit or an advisory command for the operator.
    remediations: Vec<Remediation>,
    /// Local-file changes `--fix` actually applied this run (empty unless
    /// `--fix` was passed and an auto-fixable remediation was present).
    applied_fixes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DoctorStatus {
    Clean,
    Warnings,
    Failed,
}

impl DoctorStatus {
    pub(crate) fn exit_code(self) -> i32 {
        match self {
            Self::Clean => 0,
            Self::Warnings => 2,
            Self::Failed => 1,
        }
    }
}

pub(crate) fn doctor_status(report: &DoctorReport) -> DoctorStatus {
    if !report.passed {
        DoctorStatus::Failed
    } else if !report.warnings.is_empty() {
        DoctorStatus::Warnings
    } else {
        DoctorStatus::Clean
    }
}

fn capability_matrix_for_configured_backends(
    configured_backend_tokens: &HashSet<String>,
) -> Vec<BackendCapabilityMatrixEntry> {
    udb::backend::capability_matrix_configured(configured_backend_tokens)
}

/// Best-effort load of the project manifest for manifest-aware doctor checks.
/// Returns `None` (not an error) when no parseable proto root is present, so
/// `udb doctor` still runs the generic checks outside a project directory.
fn load_manifest_best_effort(
    proto_root: &std::path::Path,
    namespace: &str,
) -> Option<udb::CatalogManifest> {
    let config = udb::ParserConfig::new(namespace);
    let report = udb::parse_directory_report(proto_root, &config).ok()?;
    udb::CatalogManifest::from_schemas(&report.schemas).ok()
}

/// Whether a manifest-required backend is configured, reflecting the runtime's
/// ACTUAL view for the key backends (so a file-configured Qdrant/S3/Redis is
/// recognised, not just an env var) and falling back to env presence otherwise.
fn backend_configured(runtime: &DataBrokerRuntime, req: &udb::BackendRequirement) -> bool {
    match req.backend.as_str() {
        "qdrant" => runtime.qdrant_configured(),
        "s3" | "minio" => runtime.s3_configured(),
        "redis" => runtime.config().has_redis(),
        _ => req.env_keys.iter().any(|k| {
            std::env::var(k)
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false)
        }),
    }
}

pub(crate) async fn run_doctor(
    with_probes: bool,
    enterprise: bool,
    fix: bool,
    proto_root: &std::path::Path,
    namespace: &str,
) -> DoctorReport {
    let runtime = DataBrokerRuntime::from_env().await;
    let init = runtime.init_report();
    let mut errors = Vec::new();
    let mut warnings = init.warnings.clone();

    // --enterprise: run the same one-shot prerequisite preflight as startup
    // (UDB_FRICTION §2). Assume a PUBLIC bind for the auth-plane-exposure check
    // (the common enterprise case); Fail findings fail the report, Warn advise.
    // The structured findings are reused below to derive remediations, so the
    // `--fix` path shares this single diagnostic engine (no second one).
    let preflight_findings: Vec<PreflightFinding> = if enterprise {
        let public_addr = "0.0.0.0:50051".parse().expect("static addr parses");
        udb::runtime::preflight::evaluate(runtime.config(), public_addr)
    } else {
        Vec::new()
    };
    if enterprise {
        use udb::runtime::preflight::PreflightSeverity;
        for finding in &preflight_findings {
            let line = format!(
                "enterprise[{}] {}: {} → {}",
                finding.severity.label(),
                finding.name,
                finding.detail,
                finding.fix
            );
            match finding.severity {
                PreflightSeverity::Fail => {
                    if !errors.contains(&line) {
                        errors.push(line);
                    }
                }
                PreflightSeverity::Warn => {
                    if !warnings.contains(&line) {
                        warnings.push(line);
                    }
                }
            }
        }

        // Manifest-aware backend checks: the true prerequisites of a
        // proto-driven broker depend on the manifest (vector/object/cache
        // backends), not just the generic enterprise subset. Load the same
        // manifest `serve` will load (best-effort, from the project dir) and
        // report any required backend that is not configured — the SAME
        // condition that would later stop `udb serve`.
        match load_manifest_best_effort(proto_root, namespace) {
            Some(manifest) => {
                for req in udb::required_backends(&manifest) {
                    if backend_configured(&runtime, &req) {
                        continue;
                    }
                    let line = format!(
                        "enterprise[{}] backend-{}: manifest requires {} '{}' (owner {}) but {} is not configured → set {}",
                        if req.fatal { "FAIL" } else { "WARN" },
                        req.backend,
                        req.resource_kind,
                        req.resource_name,
                        req.owner,
                        req.backend,
                        if req.env_keys.is_empty() {
                            "its backend config".to_string()
                        } else {
                            req.env_keys.join(" (or ")
                        },
                    );
                    if req.fatal {
                        if !errors.contains(&line) {
                            errors.push(line);
                        }
                    } else if !warnings.contains(&line) {
                        warnings.push(line);
                    }
                }
            }
            None => warnings.push(
                "enterprise: could not load a proto manifest from the current directory — \
                 run `udb requirements` from the project root for manifest-aware backend checks"
                    .to_string(),
            ),
        }
    }
    let mut system_catalog = None;
    let mut postgres_privileges = None;
    let mut backend_probes = Vec::new();
    let native_services =
        udb::runtime::service::native_registry::resolved_native_service_statuses(runtime.config());

    // ── mTLS diagnostics ──────────────────────────────────────────────────────
    let tls_cert = env::var("UDB_TLS_CERT_PATH").unwrap_or_default();
    let tls_key = env::var("UDB_TLS_KEY_PATH").unwrap_or_default();
    let tls_ca = env::var("UDB_TLS_CA_CERT_PATH").unwrap_or_default();
    let tls_configured = !tls_cert.is_empty() && !tls_key.is_empty();
    let tls_cert_exists = !tls_cert.is_empty() && std::path::Path::new(&tls_cert).exists();
    let tls_key_exists = !tls_key.is_empty() && std::path::Path::new(&tls_key).exists();
    let tls_ca_exists = !tls_ca.is_empty() && std::path::Path::new(&tls_ca).exists();
    if tls_configured && !tls_cert_exists {
        warnings.push(format!(
            "UDB_TLS_CERT_PATH is set to '{tls_cert}' but the file does not exist"
        ));
    }
    if tls_configured && !tls_key_exists {
        warnings.push(format!(
            "UDB_TLS_KEY_PATH is set to '{tls_key}' but the file does not exist"
        ));
    }
    if !tls_ca.is_empty() && !tls_ca_exists {
        warnings.push(format!(
            "UDB_TLS_CA_CERT_PATH is set to '{tls_ca}' but the file does not exist"
        ));
    }
    // master-plan 7.4 canonical example: a required-but-missing TLS path yields a
    // parametrized `export UDB_TLS_*=...` advisory (operator runs it with their
    // real path — `--fix` never invents cert material). Tied 1:1 to the warnings
    // above so a remediation is only emitted for a check that actually failed.
    let tls_remediations: Vec<Remediation> = [
        tls_path_remediation(
            tls_configured,
            tls_cert_exists,
            "tls-cert",
            "UDB_TLS_CERT_PATH",
            &tls_cert,
            "cert.pem",
            "server certificate (PEM)",
        ),
        tls_path_remediation(
            tls_configured,
            tls_key_exists,
            "tls-key",
            "UDB_TLS_KEY_PATH",
            &tls_key,
            "key.pem",
            "server private key (PEM)",
        ),
        tls_path_remediation(
            !tls_ca.is_empty(),
            tls_ca_exists,
            "tls-ca",
            "UDB_TLS_CA_CERT_PATH",
            &tls_ca,
            "ca.pem",
            "client-CA bundle for mTLS (PEM)",
        ),
    ]
    .into_iter()
    .flatten()
    .collect();

    if !init.postgres_configured {
        errors.push("PostgreSQL is required: set UDB_PG_DSN or DATABASE_URL".to_string());
    } else {
        match runtime.inspect_system_catalog().await {
            Ok(inspection) => {
                if !inspection.ok {
                    errors.push(format!(
                        "UDB system catalog is incomplete; missing {} relation(s)",
                        inspection.missing.len()
                    ));
                }
                system_catalog = Some(inspection);
            }
            Err(err) => errors.push(format!("failed to inspect UDB system catalog: {err}")),
        }

        // PostgreSQL privilege checks.
        let priv_report = runtime.check_postgres_privileges().await;
        if !priv_report.create_schema {
            warnings.push(
                "PG role lacks CREATE privilege on the database (needed for CREATE SCHEMA)".into(),
            );
        }
        if !priv_report.create_table {
            let sys_schema = SystemCatalogConfig::default().cdc.system_schema;
            warnings.push(format!(
                "PG role lacks CREATE privilege on {sys_schema} or public schema (CREATE TABLE)"
            ));
        }
        if !priv_report.create_publication {
            warnings.push(
                "PG role lacks superuser/replication role (needed for CREATE PUBLICATION)".into(),
            );
        }
        if !priv_report.replication_slot {
            warnings.push(
                "PG role lacks replication role (needed for logical replication slots)".into(),
            );
        }
        if !priv_report.advisory_lock {
            warnings.push(
                "PG role cannot acquire advisory locks (CDC leader election may fail)".into(),
            );
        }
        for err in &priv_report.errors {
            warnings.push(format!("privilege check error: {err}"));
        }
        postgres_privileges = Some(priv_report);
    }

    if !init.redis_configured {
        warnings.push(
            "Redis is not configured; read-through cache and CDC idempotency degrade".to_string(),
        );
    }
    if !init.qdrant_configured {
        warnings.push("Qdrant is not configured; vector RPCs will be unavailable".to_string());
    }
    if !init.s3_configured {
        warnings.push("S3/MinIO is not configured; object RPCs will be unavailable".to_string());
    }
    if !init.mongodb_configured {
        warnings.push("MongoDB is not configured; document RPCs will be unavailable".to_string());
    }
    if !init.neo4j_configured {
        warnings.push("Neo4j is not configured; graph RPCs will be unavailable".to_string());
    }
    if !init.clickhouse_configured {
        warnings.push(
            "ClickHouse is not configured; analytics/column RPCs will be unavailable".to_string(),
        );
    }
    for status in &native_services {
        if status.enabled && status.degraded {
            warnings.push(format!(
                "native service {} is enabled but degraded: {}",
                status.service_id, status.disabled_reason
            ));
        }
        if !status.enabled && status.configured {
            warnings.push(format!(
                "native service {} is selected/configured but disabled: {}",
                status.service_id, status.disabled_reason
            ));
        }
    }

    // ── B.13/B.14 canonical-feasibility honesty warnings ──────────────────────
    // Prerequisite wording is read from the live feasibility profiles so doctor
    // never drifts from the configured capability matrix. Join with "; ".
    let configured_backends: HashSet<String> = runtime
        .backend_instances()
        .iter()
        .map(|inst| inst.backend.clone())
        .collect();
    let capability_matrix = capability_matrix_for_configured_backends(&configured_backends);
    let prereqs_for = |backend: &str| -> Option<String> {
        capability_matrix
            .iter()
            .find(|e| e.backend == backend)
            .and_then(|e| e.canonical_feasibility.as_ref())
            .filter(|p| !p.durability_prerequisites.is_empty())
            .map(|p| p.durability_prerequisites.join("; "))
    };
    if init.s3_configured
        && let Some(prereqs) = prereqs_for("s3")
    {
        warnings.push(format!(
            "B.13: object stores (S3/MinIO) are canonical CANDIDATES only and cannot host system state until these prerequisites hold: {prereqs}"
        ));
    }
    if init.redis_configured
        && let Some(prereqs) = prereqs_for("redis")
    {
        warnings.push(format!(
            "B.14: Redis canonical promotion requires the durable AOF profile ({prereqs}); without it Redis stays a projection cache"
        ));
    }

    // Optional live backend probes (--probe flag or when all backends are configured).
    if with_probes {
        for backend in runtime.configured_probe_backends(false) {
            backend_probes.push(runtime.probe_backend(backend).await);
        }
        #[cfg(feature = "kafka")]
        backend_probes.push(runtime.probe_kafka_metadata());
        for probe in &backend_probes {
            if !probe.ok
                && let Some(ref err) = probe.error
            {
                warnings.push(format!("{} probe: {}", probe.backend, err));
            }
        }
    }

    // ── Phase 10: unified readiness contract ──────────────────────────────────
    // doctor, GetHealthReport, gRPC health, and the auth-plane readiness checks
    // all derive from the same `slo::ReadinessFacts` shape so the surfaces report
    // the same facts. We reuse the live auth-plane checks (signing keys / casbin
    // / JWKS / audit-sink / token-issuance posture) instead of re-deriving them.
    // `auth_readiness_triples` is re-exported from `udb::runtime::service`
    // (parent `service/mod.rs`: `pub use auth_service::auth_readiness_triples;`).
    let auth_triples = udb::runtime::service::auth_readiness_triples(
        &udb::runtime::security::SecurityConfig::current(),
    )
    .await;
    let readiness = udb::runtime::slo::build_readiness_facts(init, &native_services, &auth_triples);
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

    // ── Remediations (master-plan 7.4) ────────────────────────────────────────
    // Derive remediations from the SAME findings doctor already produced — no
    // second diagnostic engine. Enterprise preflight findings map to a concrete
    // env remediation (only `sessions` is an auto-fixable local default); a
    // required-but-missing TLS path maps to a parametrized advisory `export`; CRLF
    // in a local `.env` is an auto-fixable file-hygiene remediation. `--fix`
    // applies ONLY the auto-fixable, LOCAL-FILE ones; everything else stays
    // advisory (the operator runs it — never remote or destructive state).
    let mut remediations: Vec<Remediation> = preflight_findings
        .iter()
        .map(remediation_for_preflight)
        .collect();
    remediations.extend(tls_remediations);
    if let Some(crlf) = crlf_remediation_for_env(std::path::Path::new(".env")) {
        remediations.push(crlf);
    }
    let applied_fixes = if fix {
        apply_local_fixes(&remediations)
    } else {
        Vec::new()
    };

    DoctorReport {
        passed: errors.is_empty(),
        postgres_configured: init.postgres_configured,
        redis_configured: init.redis_configured,
        qdrant_configured: init.qdrant_configured,
        s3_configured: init.s3_configured,
        mongodb_configured: init.mongodb_configured,
        neo4j_configured: init.neo4j_configured,
        clickhouse_configured: init.clickhouse_configured,
        encryption_configured: init.encryption_configured,
        tls_configured,
        tls_cert_exists,
        tls_key_exists,
        tls_ca_exists,
        system_catalog,
        postgres_privileges,
        backend_probes,
        backend_capabilities: capability_matrix,
        native_services,
        // master-plan 3.5: the deployment tier resolved ONCE at startup (the same
        // OnceLock the startup floor gate uses; `from_env` above already ran the
        // gate, so this only reads the cached value).
        deployment_tier: udb::runtime::core::declared_deployment_tier()
            .map(|tier| tier.as_str().to_string()),
        errors,
        warnings,
        remediations,
        applied_fixes,
    }
}

#[derive(serde::Serialize)]
pub(crate) struct CompatEntry {
    pub(crate) option_name: &'static str,
    pub(crate) option_kind: &'static str,
    pub(crate) option_type: &'static str,
    pub(crate) target: &'static str,
    pub(crate) required: bool,
    pub(crate) since_version: &'static str,
    pub(crate) description: &'static str,
    pub(crate) example: &'static str,
    pub(crate) accepted_keys: &'static [&'static str],
}

pub(crate) fn build_compat_matrix() -> Vec<CompatEntry> {
    udb::parser::documented_option_metadata()
        .iter()
        .map(|option| CompatEntry {
            option_name: option.option_name,
            option_kind: option.kind,
            option_type: option.option_type,
            target: option.target,
            required: option.required,
            since_version: option.since_version,
            description: option.description,
            example: option.example,
            accepted_keys: option.accepted_keys,
        })
        .collect()
}

/// Emit a human-readable ASCII summary of a DoctorReport.
pub(crate) fn print_doctor_human(report: &DoctorReport) {
    println!("UDB Doctor Report");
    println!("{}", "=".repeat(50));
    let status = if report.passed { "PASS" } else { "FAIL" };
    println!("Overall: {status}");
    println!();
    println!("Backends:");
    println!("  PostgreSQL : {}", bool_icon(report.postgres_configured));
    println!("  Redis      : {}", bool_icon(report.redis_configured));
    println!("  Qdrant     : {}", bool_icon(report.qdrant_configured));
    println!("  S3/MinIO   : {}", bool_icon(report.s3_configured));
    println!("  MongoDB    : {}", bool_icon(report.mongodb_configured));
    println!("  Neo4j      : {}", bool_icon(report.neo4j_configured));
    println!("  ClickHouse : {}", bool_icon(report.clickhouse_configured));
    println!("  Encryption : {}", bool_icon(report.encryption_configured));
    println!();
    println!(
        "Deployment Tier (UDB_DEPLOYMENT_TIER): {}",
        report
            .deployment_tier
            .as_deref()
            .unwrap_or("none (permissive dev default — startup tier floor not enforced)")
    );
    println!();
    println!("mTLS:");
    println!("  Configured : {}", bool_icon(report.tls_configured));
    println!("  Cert file  : {}", bool_icon(report.tls_cert_exists));
    println!("  Key file   : {}", bool_icon(report.tls_key_exists));
    println!("  CA file    : {}", bool_icon(report.tls_ca_exists));
    if let Some(ref priv_report) = report.postgres_privileges {
        println!();
        println!("PostgreSQL Privileges:");
        println!(
            "  CREATE SCHEMA      : {}",
            bool_icon(priv_report.create_schema)
        );
        println!(
            "  CREATE TABLE       : {}",
            bool_icon(priv_report.create_table)
        );
        println!(
            "  CREATE PUBLICATION : {}",
            bool_icon(priv_report.create_publication)
        );
        println!(
            "  Replication Slot   : {}",
            bool_icon(priv_report.replication_slot)
        );
        println!(
            "  Advisory Lock      : {}",
            bool_icon(priv_report.advisory_lock)
        );
    }
    if let Some(ref catalog) = report.system_catalog {
        println!();
        println!("System Catalog ({}):", catalog.schema);
        println!("  OK      : {}", bool_icon(catalog.ok));
        println!("  Missing : {}", catalog.missing.len());
        for rel in &catalog.missing {
            println!("    - {rel}");
        }
    }
    if !report.backend_probes.is_empty() {
        println!();
        println!("Live Backend Probes:");
        for probe in &report.backend_probes {
            let label = if probe.ok {
                format!("OK   ({}ms)", probe.latency_ms)
            } else {
                format!("FAIL — {}", probe.error.as_deref().unwrap_or("unknown"))
            };
            println!("  {:10} : {label}", probe.backend);
        }
    }
    if !report.backend_capabilities.is_empty() {
        println!();
        println!("Backend Capability Matrix:");
        for entry in &report.backend_capabilities {
            println!(
                "  {:12} {:10} ops={} consistency={} max_payload={} xa={} two_phase={}",
                entry.backend,
                entry.tier,
                entry.operations.join(","),
                entry.consistency_model,
                entry.max_payload_bytes,
                entry.supports_xa,
                entry.supports_two_phase_commit
            );
        }
    }
    if !report.native_services.is_empty() {
        println!();
        println!("Native Services:");
        for status in &report.native_services {
            let state = if status.healthy {
                "healthy"
            } else if status.degraded {
                "degraded"
            } else if status.enabled {
                "enabled"
            } else {
                "disabled"
            };
            println!(
                "  {:18} {:9} surface={} listener={} migrate={} workers={} deps=[{}]{}",
                status.service_id,
                state,
                status.surface,
                status.listener_kind,
                status.migration_status,
                if status.background_worker_enabled {
                    status.background_workers.join(",")
                } else if status.owns_background_workers {
                    "disabled".to_string()
                } else {
                    "none".to_string()
                },
                status.required_backends.join(","),
                if status.disabled_reason.is_empty() {
                    String::new()
                } else {
                    format!(" reason={}", status.disabled_reason)
                }
            );
        }
    }
    let canonical_feasibility: Vec<&BackendCapabilityMatrixEntry> = report
        .backend_capabilities
        .iter()
        .filter(|e| {
            e.canonical_feasibility
                .as_ref()
                .is_some_and(|p| p.family == "object" || p.family == "cache")
        })
        .collect();
    if !canonical_feasibility.is_empty() {
        println!();
        println!("Canonical Feasibility (object & cache promotion roadmap):");
        for entry in &canonical_feasibility {
            let profile = entry
                .canonical_feasibility
                .as_ref()
                .expect("filtered to entries with a feasibility profile");
            println!(
                "  {} [{}] candidate={} role={:?} implemented={}",
                entry.backend,
                profile.family,
                profile.candidate.as_str(),
                entry.role,
                profile.implemented
            );
            println!("     atomic-claim     : {}", profile.atomic_claim_strategy);
            println!(
                "     ordered-progress : {}",
                profile.ordered_progress_strategy
            );
            println!(
                "     tenant-isolation : {}",
                profile.tenant_isolation_strategy
            );
            println!("     read-fence       : {}", profile.read_fence_strategy);
            println!("     read-fence-support: {}", profile.read_fence_supported);
            println!(
                "     consistency-modes: {}",
                profile.supported_consistency_modes.join(", ")
            );
            println!("     prerequisites:");
            if profile.durability_prerequisites.is_empty() {
                println!("        (none)");
            } else {
                for prereq in profile.durability_prerequisites {
                    println!("        - {prereq}");
                }
            }
            println!("     blocking gaps:");
            if profile.blocking_gaps.is_empty() {
                println!("        (none)");
            } else {
                for gap in profile.blocking_gaps {
                    println!("        - {gap}");
                }
            }
            println!(
                "     live gate        : {}",
                profile.live_conformance_env.unwrap_or("(none)")
            );
        }
    }
    if !report.errors.is_empty() {
        println!();
        println!("Errors:");
        for e in &report.errors {
            println!("  [!] {e}");
        }
    }
    if !report.warnings.is_empty() {
        println!();
        println!("Warnings:");
        for w in &report.warnings {
            println!("  [w] {w}");
        }
    }
    if !report.remediations.is_empty() {
        println!();
        println!("Remediations:");
        for remediation in &report.remediations {
            let tag = if remediation.is_auto_fixable() {
                "fixable"
            } else {
                "advise "
            };
            println!("  [{tag}] {}", remediation.describe());
        }
        if report.applied_fixes.is_empty() {
            println!(
                "  (pass `--fix` to apply the [fixable] local-file remediations; advisory items \
                 must be run by an operator)"
            );
        }
    }
    if !report.applied_fixes.is_empty() {
        println!();
        println!("Applied local fixes (--fix, .env only):");
        for change in &report.applied_fixes {
            println!("  [+] {change}");
        }
    }
}

pub(crate) fn bool_icon(v: bool) -> &'static str {
    if v { "ok" } else { "MISSING" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doctor_capability_matrix_marks_configured_tokens_only() {
        let configured = HashSet::from(["postgres".to_string(), "redis".to_string()]);
        let matrix = capability_matrix_for_configured_backends(&configured);

        let postgres = matrix
            .iter()
            .find(|entry| entry.backend == "postgres")
            .expect("postgres capability exists");
        let redis = matrix
            .iter()
            .find(|entry| entry.backend == "redis")
            .expect("redis capability exists");
        let qdrant = matrix
            .iter()
            .find(|entry| entry.backend == "qdrant")
            .expect("qdrant capability exists");

        assert!(postgres.configured);
        assert!(redis.configured);
        assert!(!qdrant.configured);
    }

    use udb::runtime::preflight::{PreflightFinding, PreflightSeverity};

    fn finding(name: &'static str, fix: &'static str) -> PreflightFinding {
        PreflightFinding {
            name,
            severity: PreflightSeverity::Warn,
            detail: String::new(),
            fix,
        }
    }

    #[test]
    fn sessions_finding_is_auto_fixable_env_default() {
        let rem = remediation_for_preflight(&finding(
            "sessions",
            "set UDB_SESSION_ENABLED=true and UDB_SESSION_HASH_SECRET",
        ));
        assert!(
            rem.is_auto_fixable(),
            "sessions toggle is a safe local default"
        );
        match rem {
            Remediation::SetEnvDefault { key, value, .. } => {
                assert_eq!(key, "UDB_SESSION_ENABLED");
                assert_eq!(value, "true");
            }
            other => panic!("expected SetEnvDefault, got {other:?}"),
        }
    }

    #[test]
    fn secret_and_endpoint_findings_are_advisory_only() {
        // Secrets and reachable-endpoint findings have no safe local default and
        // must NEVER be auto-applied — they stay advisory.
        for name in [
            "encryption-key",
            "password-hash-secret",
            "redis",
            "auth-plane-exposure",
        ] {
            let rem = remediation_for_preflight(&finding(name, "set something"));
            assert!(!rem.is_auto_fixable(), "{name} must be advisory-only");
            assert!(matches!(rem, Remediation::Advisory { .. }));
        }
    }

    #[test]
    fn authz_default_deny_is_never_auto_fixed() {
        // Critical: the documented fix (UDB_ABAC_DEFAULT_ALLOW=true) LOOSENS
        // authz, so `--fix` must never apply it. It must be advisory.
        let rem = remediation_for_preflight(&finding(
            "authz-default-deny",
            "configure policies via the AuthzService (policy_rules), or set UDB_ABAC_DEFAULT_ALLOW=true for dev/bootstrap",
        ));
        assert!(!rem.is_auto_fixable());
        assert!(matches!(rem, Remediation::Advisory { .. }));
    }

    #[test]
    fn env_default_appends_missing_key_only() {
        let (out, changed) =
            env_with_default("UDB_PG_DSN=postgres://x\n", "UDB_SESSION_ENABLED", "true");
        assert!(changed);
        assert!(out.contains("UDB_SESSION_ENABLED=true\n"));
        assert!(
            out.contains("UDB_PG_DSN=postgres://x\n"),
            "existing keys preserved"
        );
    }

    #[test]
    fn env_default_adds_trailing_newline_when_missing() {
        // No trailing newline on the last line — must not glue onto it.
        let (out, changed) = env_with_default("FOO=bar", "UDB_SESSION_ENABLED", "true");
        assert!(changed);
        assert_eq!(out, "FOO=bar\nUDB_SESSION_ENABLED=true\n");
    }

    #[test]
    fn env_default_respects_existing_definition() {
        let (out, changed) =
            env_with_default("UDB_SESSION_ENABLED=false\n", "UDB_SESSION_ENABLED", "true");
        assert!(!changed, "an operator-set value must win");
        assert_eq!(out, "UDB_SESSION_ENABLED=false\n");
    }

    #[test]
    fn env_default_ignores_commented_key() {
        let (out, changed) = env_with_default(
            "# UDB_SESSION_ENABLED=true\n",
            "UDB_SESSION_ENABLED",
            "true",
        );
        assert!(changed, "a commented-out key does not count as defined");
        assert!(out.contains("\nUDB_SESSION_ENABLED=true\n"));
    }

    #[test]
    fn tls_missing_cert_emits_parametrized_export_advisory() {
        // A required-but-missing TLS cert (env set to a non-existent path) must
        // yield an advisory `export UDB_TLS_CERT_PATH=...` derived from the check.
        let rem = tls_path_remediation(
            true,
            false,
            "tls-cert",
            "UDB_TLS_CERT_PATH",
            "/etc/udb/missing.pem",
            "cert.pem",
            "server certificate (PEM)",
        )
        .expect("a required-but-missing TLS path yields a remediation");
        assert!(
            !rem.is_auto_fixable(),
            "TLS material is operator-specific, never auto-applied"
        );
        match &rem {
            Remediation::Advisory { command, .. } => {
                assert!(
                    command.contains("UDB_TLS_CERT_PATH"),
                    "names the right env var"
                );
                assert!(
                    command.starts_with("export "),
                    "is a safe local shell export"
                );
                assert!(
                    command.contains("/etc/udb/missing.pem"),
                    "parametrized from the failing path"
                );
            }
            other => panic!("expected Advisory export, got {other:?}"),
        }
    }

    #[test]
    fn tls_unset_cert_reports_currently_unset() {
        // Env unset (empty current) is still a failing check → advisory, and the
        // suggestion is built from the env var name (no hardcoded path block).
        let rem = tls_path_remediation(
            true,
            false,
            "tls-key",
            "UDB_TLS_KEY_PATH",
            "",
            "key.pem",
            "server private key (PEM)",
        )
        .expect("unset-but-required yields a remediation");
        match &rem {
            Remediation::Advisory { command, .. } => {
                assert!(command.contains("UDB_TLS_KEY_PATH"));
                assert!(command.contains("currently unset"));
            }
            other => panic!("expected Advisory, got {other:?}"),
        }
    }

    #[test]
    fn tls_passing_check_emits_no_remediation() {
        // File exists → check passes → no remediation.
        assert!(
            tls_path_remediation(
                true,
                true,
                "tls-cert",
                "UDB_TLS_CERT_PATH",
                "/real/cert.pem",
                "cert.pem",
                "server certificate (PEM)",
            )
            .is_none(),
            "a passing check emits no remediation"
        );
        // Not required (e.g. CA unset and optional) → no remediation either.
        assert!(
            tls_path_remediation(
                false,
                false,
                "tls-ca",
                "UDB_TLS_CA_CERT_PATH",
                "",
                "ca.pem",
                "client-CA bundle for mTLS (PEM)",
            )
            .is_none(),
            "an unused optional check emits no remediation"
        );
    }

    #[test]
    fn tls_remediation_is_never_remote_or_destructive() {
        // 7.4 guard: `--fix` only ever emits safe LOCAL .env/shell exports. The
        // TLS remediation must never contain a remote or destructive action.
        for current in ["", "/etc/udb/missing.pem"] {
            let rem = tls_path_remediation(
                true,
                false,
                "tls-cert",
                "UDB_TLS_CERT_PATH",
                current,
                "cert.pem",
                "server certificate (PEM)",
            )
            .expect("failing check yields a remediation");
            let Remediation::Advisory { command, .. } = &rem else {
                panic!("expected Advisory");
            };
            assert!(!rem.is_auto_fixable());
            let lowered = command.to_ascii_lowercase();
            for forbidden in [
                "rm ", "rm -", "curl", "wget", "ssh", "scp", "drop ", "delete ", "sudo ", "| sh",
                "&&",
            ] {
                assert!(
                    !lowered.contains(forbidden),
                    "remediation `{command}` must not contain `{forbidden}` (no remote/destructive action)"
                );
            }
        }
    }

    #[test]
    fn crlf_normalizes_to_lf() {
        let (out, changed) = normalize_crlf("A=1\r\nB=2\r\n");
        assert!(changed);
        assert_eq!(out, "A=1\nB=2\n");

        let (out2, changed2) = normalize_crlf("A=1\nB=2\n");
        assert!(!changed2, "already-LF content is unchanged");
        assert_eq!(out2, "A=1\nB=2\n");
    }
}

/// Emit a human-readable lint report to stdout.
pub(crate) fn print_lint_human(report: &LintReport) {
    let status = if report.passed { "PASS" } else { "FAIL" };
    println!("UDB Lint Report  [{status}]");
    println!("{}", "─".repeat(60));
    println!(
        "  Tables: {}  Stores: {}  Errors: {}  Warnings: {}  Info: {}",
        report.table_count,
        report.store_count,
        report.error_count,
        report.warning_count,
        report.info_count
    );
    if report.items.is_empty() {
        println!();
        println!("  No findings — schema is clean.");
        return;
    }
    println!();
    for item in &report.items {
        let sev = match item.severity {
            LintSeverity::Error => "ERROR  ",
            LintSeverity::Warning => "WARN   ",
            LintSeverity::Info => "INFO   ",
        };
        let location = if item.column.is_empty() {
            format!("{}.{}", item.schema, item.table)
        } else {
            format!("{}.{}.{}", item.schema, item.table, item.column)
        };
        let loc_str = if location == "." {
            "(global)".to_string()
        } else {
            location
        };
        println!("[{sev}] {loc_str}");
        println!("         kind        : {}", item.kind);
        println!("         description : {}", item.description);
        if !item.suggestion.is_empty() {
            println!("         suggestion  : {}", item.suggestion);
        }
        println!();
    }
}
