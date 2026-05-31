//! main.rs split — doctor (Phase H).
use super::*;

#[derive(Debug, Serialize)]
pub(crate) struct DoctorReport {
    pub(crate) passed: bool,
    postgres_configured: bool,
    redis_configured: bool,
    qdrant_configured: bool,
    s3_configured: bool,
    encryption_configured: bool,
    tls_configured: bool,
    tls_cert_exists: bool,
    tls_key_exists: bool,
    tls_ca_exists: bool,
    system_catalog: Option<SystemCatalogInspection>,
    postgres_privileges: Option<PostgresPrivilegeReport>,
    backend_probes: Vec<BackendProbeResult>,
    backend_capabilities: Vec<BackendCapabilityMatrixEntry>,
    errors: Vec<String>,
    warnings: Vec<String>,
}

pub(crate) async fn run_doctor(with_probes: bool) -> DoctorReport {
    let runtime = DataBrokerRuntime::from_env().await;
    let init = runtime.init_report();
    let mut errors = Vec::new();
    let mut warnings = init.warnings.clone();
    let mut system_catalog = None;
    let mut postgres_privileges = None;
    let mut backend_probes = Vec::new();

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

    // Optional live backend probes (--probe flag or when all backends are configured).
    if with_probes {
        #[cfg(feature = "redis")]
        if init.redis_configured {
            backend_probes.push(runtime.probe_redis_ping().await);
        }
        if init.qdrant_configured {
            backend_probes.push(runtime.probe_qdrant_collections().await);
        }
        #[cfg(feature = "s3")]
        if init.s3_configured {
            backend_probes.push(runtime.probe_s3_access().await);
        }
        if init.mongodb_configured {
            backend_probes.push(runtime.probe_mongodb_ping().await);
        }
        if init.neo4j_configured {
            backend_probes.push(runtime.probe_neo4j_ping().await);
        }
        if init.clickhouse_configured {
            backend_probes.push(runtime.probe_clickhouse_ping().await);
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

    DoctorReport {
        passed: errors.is_empty(),
        postgres_configured: init.postgres_configured,
        redis_configured: init.redis_configured,
        qdrant_configured: init.qdrant_configured,
        s3_configured: init.s3_configured,
        encryption_configured: init.encryption_configured,
        tls_configured,
        tls_cert_exists,
        tls_key_exists,
        tls_ca_exists,
        system_catalog,
        postgres_privileges,
        backend_probes,
        backend_capabilities: udb::backend::capability_matrix(),
        errors,
        warnings,
    }
}

#[derive(serde::Serialize)]
pub(crate) struct CompatEntry {
    option_name: &'static str,
    option_type: &'static str,
    target: &'static str,
    required: bool,
    since_version: &'static str,
    description: &'static str,
    example: &'static str,
}

pub(crate) fn build_compat_matrix() -> Vec<CompatEntry> {
    vec![
        CompatEntry {
            option_name: "db.table",
            option_type: "MessageOptions",
            target: "PostgreSQL / Qdrant / S3 / Redis / Neo4j",
            required: true,
            since_version: "0.1.0",
            description: "Marks a proto message as a mapped UDB table.",
            example: r#"option (db.table) = { name: "users" schema: "app" primary_key: "id" };"#,
        },
        CompatEntry {
            option_name: "db.column",
            option_type: "FieldOptions",
            target: "PostgreSQL",
            required: false,
            since_version: "0.1.0",
            description: "Maps a proto field to a SQL column.",
            example: r#"string email = 2 [(db.column).name = "email", (db.column).type = "TEXT"];"#,
        },
        CompatEntry {
            option_name: "db.vector_column",
            option_type: "FieldOptions",
            target: "Qdrant",
            required: false,
            since_version: "0.2.0",
            description: "Declares a field as a vector embedding column for Qdrant.",
            example: r#"repeated float embedding = 5 [(db.vector_column).dimension = 1536];"#,
        },
        CompatEntry {
            option_name: "db.object_store",
            option_type: "MessageOptions",
            target: "S3 / MinIO",
            required: false,
            since_version: "0.2.0",
            description: "Routes a message type to an S3-compatible object store bucket.",
            example: r#"option (db.object_store) = { bucket: "artifacts" prefix: "docs/" };"#,
        },
        CompatEntry {
            option_name: "db.cache",
            option_type: "MessageOptions",
            target: "Redis",
            required: false,
            since_version: "0.3.0",
            description: "Enables Redis caching for a message type.",
            example: r#"option (db.cache) = { ttl_seconds: 300 key_prefix: "user:" };"#,
        },
        CompatEntry {
            option_name: "db.index",
            option_type: "FieldOptions",
            target: "PostgreSQL",
            required: false,
            since_version: "0.1.0",
            description: "Creates a secondary index on the mapped column.",
            example: r#"string email = 2 [(db.column).name = "email", (db.index).unique = true];"#,
        },
        CompatEntry {
            option_name: "db.foreign_key",
            option_type: "FieldOptions",
            target: "PostgreSQL",
            required: false,
            since_version: "0.1.0",
            description: "Declares a foreign key reference to another table.",
            example: r#"string user_id = 3 [(db.foreign_key) = { ref_table: "users" ref_column: "id" }];"#,
        },
        CompatEntry {
            option_name: "db.cdc",
            option_type: "MessageOptions",
            target: "PostgreSQL (outbox) → Kafka",
            required: false,
            since_version: "0.4.0",
            description: "Enables change-data-capture outbox publishing for a table.",
            example: r#"option (db.cdc) = { topic: "app.events.users" format: "json" };"#,
        },
        CompatEntry {
            option_name: "db.abac",
            option_type: "MessageOptions",
            target: "UDB security layer",
            required: false,
            since_version: "0.3.0",
            description: "Attaches ABAC access-control metadata to a message type.",
            example: r#"option (db.abac) = { required_scope: "users:read" purpose: "identity" };"#,
        },
        CompatEntry {
            option_name: "db.field_mask",
            option_type: "FieldOptions",
            target: "UDB security layer",
            required: false,
            since_version: "0.3.0",
            description: "Marks a field as masked unless the caller presents the required scope.",
            example: r#"string ssn = 8 [(db.field_mask).required_scope = "pii:read"];"#,
        },
    ]
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
