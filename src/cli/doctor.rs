//! main.rs split — doctor (Phase H).
use super::*;
use std::collections::HashSet;

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
    errors: Vec<String>,
    warnings: Vec<String>,
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
    if enterprise {
        use udb::runtime::preflight::PreflightSeverity;
        let public_addr = "0.0.0.0:50051".parse().expect("static addr parses");
        for finding in udb::runtime::preflight::evaluate(runtime.config(), public_addr) {
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
        errors,
        warnings,
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
