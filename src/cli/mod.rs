//! CLI entry point internals (Phase H split of main.rs).
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process;
use std::process::Command as ProcessCommand;

use serde::Serialize;
use serde_yaml::Value as YamlValue;
use udb::{
    AbacPolicy, BackendCapabilityMatrixEntry, BackendProbeResult, BackendSyncTarget,
    CatalogManifest, DDL_ANALYTICS_EVENTS_DAILY, DEFAULT_LEDGER_SCHEMA, DataBrokerRuntime,
    DbOpsSyncConfig, DsnGenerationConfig, FsmState, LintReport, LintSeverity, MigrationOptions,
    MigrationPlanConfig, MultiPgConfig, ParserConfig, PgInstance, PolicyLintFinding,
    PostgresPrivilegeReport, ProtoCatalog, SqlGenerationConfig, StartupLifecycleReport,
    SystemCatalogConfig, SystemCatalogInspection, build_drift_report, build_migration_plan,
    default_system_catalog_ddl, generate_bootstrap_sql, generate_unified_dsn_catalog,
    init_observability, lint_catalog, lint_policies, parse_directory_report, run_startup_lifecycle,
    schema_checksum, serve, sync_all_backends, sync_db_ops,
};

mod args;
mod auth;
mod doctor;
mod env_setup;
mod output;
mod scaffold;
pub(crate) use args::*;
pub(crate) use auth::*;
pub(crate) use doctor::*;
pub(crate) use env_setup::*;
pub(crate) use output::*;
pub(crate) use scaffold::*;
#[cfg(test)]
mod tests;

const DEFAULT_SERVE_THREAD_STACK_SIZE: usize = 64 * 1024 * 1024;

fn serve_thread_stack_size() -> usize {
    env::var("UDB_THREAD_STACK_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SERVE_THREAD_STACK_SIZE)
}

fn admin_reset_sql(ledger_schema: &str) -> String {
    let ledger_schema = if ledger_schema.trim().is_empty() {
        DEFAULT_LEDGER_SCHEMA
    } else {
        ledger_schema.trim()
    };
    let escaped = ledger_schema.replace('"', "\"\"");
    format!(
        r#"
DO $$
DECLARE
    _schema TEXT;
    _dropped_schemas TEXT[] := '{{}}';
BEGIN
    FOR _schema IN
        SELECT nspname
        FROM pg_namespace
        WHERE nspname NOT IN ({ledger_literal}, 'information_schema')
          AND nspname NOT LIKE 'pg_%'
        ORDER BY nspname
    LOOP
        EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', _schema);
        _dropped_schemas := _dropped_schemas || _schema;
        RAISE NOTICE 'dropped schema %', _schema;
    END LOOP;

    DROP TABLE IF EXISTS "{escaped}".migration_error_log      CASCADE;
    DROP TABLE IF EXISTS "{escaped}".migration_runtime_state  CASCADE;
    DROP TABLE IF EXISTS "{escaped}".schema_migrations        CASCADE;
    DROP TABLE IF EXISTS "{escaped}".proto_schema_versions    CASCADE;

    RAISE NOTICE 'dropped UDB ledger tables from schema {escaped}';
    RAISE NOTICE 'reset complete — dropped schemas: %',
        CASE WHEN array_length(_dropped_schemas, 1) IS NULL
             THEN '(none)'
             ELSE array_to_string(_dropped_schemas, ', ')
        END;
END;
$$;
"#,
        ledger_literal = sql_literal(ledger_schema),
    )
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn run() {
    // Load project root env file.
    //
    // Resolution order (first file found in cwd → parent chain wins):
    //   1. .env.<APP_ENV>   e.g. .env.local  or  .env.prod
    //   2. .env.local
    //   3. .env.prod
    //   4. .env             (dotenvy default — fallback)
    let args: Vec<String> = env::args().skip(1).collect();
    load_project_dotenv();
    load_udb_config_overlay(&args);
    ensure_cli_correlation_id();
    init_observability();
    let (command, proto_root, namespace, serve_addr) = parse_args(&args);
    let proto_root = resolve_existing_project_path(&proto_root);

    // Commands that do not need proto parsing — emit output and exit immediately.
    match command {
        Command::TrackerDdl => {
            let ledger_schema =
                env::var("UDB_LEDGER_SCHEMA").unwrap_or_else(|_| DEFAULT_LEDGER_SCHEMA.to_string());
            print!("{}", udb::all_tracker_ddl_sql_for_schema(&ledger_schema));
            process::exit(0);
        }
        Command::SystemDdl => {
            print!("{}", default_system_catalog_ddl());
            process::exit(0);
        }
        Command::StatusSchema => {
            print!("{}", DDL_ANALYTICS_EVENTS_DAILY);
            process::exit(0);
        }
        Command::FsmStates => {
            #[derive(Serialize)]
            struct StateInfo {
                state: String,
                transitions: Vec<String>,
            }
            let info: Vec<StateInfo> = FsmState::ALL
                .iter()
                .map(|s| StateInfo {
                    state: s.as_str().to_string(),
                    transitions: s
                        .valid_transitions()
                        .iter()
                        .map(|t| t.as_str().to_string())
                        .collect(),
                })
                .collect();
            output_json(&info, "FSM states");
            process::exit(0);
        }
        Command::ConfigSkeleton => {
            let opts = MigrationOptions::default();
            output_json(&opts, "MigrationOptions skeleton");
            process::exit(0);
        }
        Command::Doctor {
            output_mode,
            with_probes,
        } => {
            let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|err| {
                eprintln!("failed to create tokio runtime: {err}");
                process::exit(1);
            });
            let report = runtime.block_on(run_doctor(with_probes));
            let exit_code = doctor_status(&report).exit_code();
            match output_mode {
                DoctorOutputMode::Json => output_json(&report, "doctor report"),
                DoctorOutputMode::Human => print_doctor_human(&report),
            }
            process::exit(exit_code);
        }
        Command::HealthCheck => {
            // Lightweight Docker HEALTHCHECK: connect to PG and verify system catalog.
            let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|err| {
                eprintln!("failed to create tokio runtime: {err}");
                process::exit(1);
            });
            let healthy = runtime.block_on(async {
                let rt = DataBrokerRuntime::from_env().await;
                if !rt.init_report().postgres_configured {
                    return false;
                }
                rt.inspect_system_catalog()
                    .await
                    .map(|r| r.ok)
                    .unwrap_or(false)
            });
            if healthy {
                println!("healthy");
                process::exit(0);
            } else {
                eprintln!("unhealthy: PostgreSQL system catalog check failed");
                process::exit(1);
            }
        }
        Command::InitProject => {
            emit_init_project_scaffold();
            process::exit(0);
        }
        Command::Dev {
            action,
            service,
            confirmed,
        } => {
            process::exit(run_dev_sandbox(action, service.as_deref(), confirmed));
        }
        Command::Auth(auth_command) => {
            process::exit(run_auth_command(auth_command));
        }
        Command::AdminReleaseLock => {
            let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|err| {
                eprintln!("failed to create tokio runtime: {err}");
                process::exit(1);
            });
            let exit_code = runtime.block_on(async {
                let rt = DataBrokerRuntime::from_env().await;
                let Some(pool) = rt.pg_pool_clone() else {
                    eprintln!("release-lock: PostgreSQL not configured");
                    return 1i32;
                };
                use udb::engine::PG_ADVISORY_LOCK_KEY;
                let classid = (PG_ADVISORY_LOCK_KEY >> 32) as i32;
                let objid = (PG_ADVISORY_LOCK_KEY & 0xFFFF_FFFF) as i32;
                match sqlx::query_scalar::<_, Option<bool>>(
                    "SELECT pg_terminate_backend(pid) FROM pg_locks \
                     WHERE locktype = 'advisory' AND classid = $1 AND objid = $2 AND granted = true"
                )
                .bind(classid)
                .bind(objid)
                .fetch_optional(&pool)
                .await {
                    Ok(Some(Some(true))) => {
                        eprintln!("release-lock: terminated backend holding advisory lock {:#x}", PG_ADVISORY_LOCK_KEY);
                        0
                    }
                    Ok(_) => {
                        eprintln!("release-lock: no backend found holding advisory lock {:#x} (already released?)", PG_ADVISORY_LOCK_KEY);
                        0
                    }
                    Err(err) => {
                        eprintln!("release-lock: failed to terminate backend: {err}");
                        1
                    }
                }
            });
            process::exit(exit_code);
        }
        Command::AdminVerifyAudit { limit } => {
            let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|err| {
                eprintln!("failed to create tokio runtime: {err}");
                process::exit(1);
            });
            let exit_code = runtime.block_on(async {
                let rt = DataBrokerRuntime::from_env().await;
                match rt.verify_admin_audit_log_chain(limit).await {
                    Ok(report) => {
                        let passed = report["passed"].as_bool().unwrap_or(false);
                        output_json(&report, "admin audit verification");
                        if passed { 0 } else { 2 }
                    }
                    Err(err) => {
                        eprintln!("admin verify-audit: failed — {err}");
                        1
                    }
                }
            });
            process::exit(exit_code);
        }
        Command::AdminResetDb { confirmed } => {
            if !confirmed {
                eprintln!(
                    "admin reset-db: destructive operation — pass --yes to confirm.\n\
                     This will drop all UDB-managed schemas and ledger tables."
                );
                process::exit(1);
            }

            // Drop all UDB-managed schemas and ledger tables.
            // Reads from pg_namespace directly — works even when the UDB ledger was
            // already dropped (previous partial reset, manual cleanup, etc.).
            // vision_db is a UDB-owned database; all non-system schemas are UDB schemas.
            let reset_sql = admin_reset_sql(
                &env::var("UDB_LEDGER_SCHEMA")
                    .unwrap_or_else(|_| DEFAULT_LEDGER_SCHEMA.to_string()),
            );
            let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|err| {
                eprintln!("failed to create tokio runtime: {err}");
                process::exit(1);
            });
            runtime.block_on(async {
                let rt = DataBrokerRuntime::from_env().await;
                match rt.execute_raw_sql(&reset_sql, "admin reset-db").await {
                    Ok(()) => {
                        eprintln!("admin reset-db: complete — all UDB-managed schemas and ledger tables dropped");
                        process::exit(0);
                    }
                    Err(err) => {
                        eprintln!("admin reset-db: failed — {err}");
                        process::exit(1);
                    }
                }
            });
        }
        Command::PolicyLint => {
            // Load policies from UDB_ABAC_POLICY_FILE, or use empty set with a warning.
            let policies = load_abac_policies_for_lint();
            let findings = lint_policies(&policies);
            #[derive(Serialize)]
            struct LintResult<'a> {
                policy_count: usize,
                finding_count: usize,
                passed: bool,
                findings: &'a [PolicyLintFinding],
            }
            let has_errors = findings.iter().any(|f| f.severity == "error");
            let result = LintResult {
                policy_count: policies.len(),
                finding_count: findings.len(),
                passed: !has_errors,
                findings: &findings,
            };
            let exit_code = if has_errors { 1 } else { 0 };
            output_json(&result, "policy lint result");
            process::exit(exit_code);
        }
        Command::PolicySeed => {
            let policies = load_abac_policies_for_lint();
            if policies.is_empty() {
                eprintln!(
                    "No policies loaded. Set UDB_ABAC_POLICY_FILE to a JSON array of AbacPolicy objects."
                );
                process::exit(1);
            }
            let sys_cfg = SystemCatalogConfig::default();
            let table = pg_relation(&sys_cfg.abac_schema, &sys_cfg.abac_table);
            let source = env::var("UDB_ABAC_POLICY_FILE").unwrap_or_default();
            println!("-- UDB ABAC policy seed SQL");
            println!("-- Source: {source}");
            println!("-- Table : {table}");
            println!("-- Generated: {} policies", policies.len());
            println!();
            println!("BEGIN;");
            println!("DELETE FROM {table}; -- clear existing policies before re-seeding");
            println!();
            for (i, p) in policies.iter().enumerate() {
                let effect = match p.effect {
                    udb::PolicyEffect::Allow => "allow",
                    udb::PolicyEffect::Deny => "deny",
                };
                println!(
                    "INSERT INTO {table} \
                     (effect, service_identity, tenant_id, purpose, message_type, operation, required_scope) VALUES \
                     ({eff}, {svc}, {ten}, {pur}, {msg}, {op}, {sco}); -- #{i}",
                    eff = sql_literal(effect),
                    svc = sql_literal(&p.service_identity),
                    ten = sql_literal(&p.tenant_id),
                    pur = sql_literal(&p.purpose),
                    msg = sql_literal(&p.message_type),
                    op = sql_literal(&p.operation),
                    sco = sql_literal(&p.required_scope),
                );
            }
            println!();
            println!("COMMIT;");
            process::exit(0);
        }
        Command::FieldMaskPreview => {
            // FieldMaskPreview needs proto parsing — defer to below.
        }
        Command::ManifestExport => {
            // ManifestExport needs proto parsing — defer to below.
        }
        Command::SyncMigrations { .. } => {
            // SyncMigrations needs proto parsing — defer to below.
        }
        Command::CompatMatrix => {
            output_json(&build_compat_matrix(), "compatibility matrix");
            process::exit(0);
        }
        _ => {}
    }

    let config = ParserConfig::new(namespace);
    let parse_report = match parse_directory_report(&proto_root, &config) {
        Ok(report) => report,
        Err(err) => {
            eprintln!("{err}");
            process::exit(1);
        }
    };
    for diagnostic in &parse_report.diagnostics {
        eprintln!("diagnostic: {diagnostic}");
    }
    let schemas = parse_report.schemas;
    let checksum = match schema_checksum(&schemas) {
        Ok(checksum) => checksum,
        Err(err) => {
            eprintln!("failed to serialize schema checksum input: {err}");
            process::exit(1);
        }
    };

    eprintln!("schemas: {}", schemas.len());
    eprintln!("checksum_sha256: {checksum}");

    match command {
        Command::Catalog => output_json(&ProtoCatalog { schemas }, "catalog"),
        Command::Dsn => {
            let catalog = generate_unified_dsn_catalog(&schemas, &DsnGenerationConfig::default())
                .unwrap_or_else(|err| fatal_json("failed to build DSN catalog", err));
            output_json(&catalog, "DSN catalog");
        }
        Command::Sql => {
            let artifacts = generate_bootstrap_sql(&schemas, &SqlGenerationConfig::default())
                .unwrap_or_else(|err| fatal_json("failed to generate SQL artifacts", err));
            output_json(&artifacts, "SQL artifacts");
        }
        Command::Plan => {
            // Load prior manifest from --prior <path> or UDB_PRIOR_MANIFEST_PATH,
            // mirroring the Drift handler. Hardcoding None always emitted a
            // full-create diff, contradicting the documented --prior behaviour.
            let prior = load_prior_manifest_from_args(&args);
            let plan =
                build_migration_plan(prior.as_ref(), &schemas, &MigrationPlanConfig::default())
                    .unwrap_or_else(|err| fatal_json("failed to build migration plan", err));
            output_json(&plan, "migration plan");
        }
        Command::Lint => {
            let manifest = CatalogManifest::from_schemas(&schemas)
                .unwrap_or_else(|err| fatal_json("failed to build catalog manifest", err));
            let report = lint_catalog(&manifest);
            let exit_code = if report.passed { 0 } else { 1 };
            let use_human = args.iter().any(|a| a == "--human")
                || env::var("UDB_LINT_HUMAN")
                    .map(|v| v == "1" || v == "true")
                    .unwrap_or(false);
            if use_human {
                print_lint_human(&report);
            } else {
                if !report.passed {
                    eprintln!(
                        "lint: {} error(s), {} warning(s) — migration blocked",
                        report.error_count, report.warning_count
                    );
                } else {
                    eprintln!(
                        "lint: passed ({} tables, {} stores, {} warning(s))",
                        report.table_count, report.store_count, report.warning_count
                    );
                }
                output_json(&report, "lint report");
            }
            process::exit(exit_code);
        }
        Command::Drift => {
            let manifest = CatalogManifest::from_schemas(&schemas)
                .unwrap_or_else(|err| fatal_json("failed to build catalog manifest", err));
            // Load prior manifest from --prior <path> or UDB_PRIOR_MANIFEST_PATH.
            let prior = load_prior_manifest_from_args(&args);
            let report = build_drift_report(prior.as_ref(), &manifest);
            let exit_code = if report.blocked_count > 0 { 1 } else { 0 };
            if prior.is_some() {
                eprintln!(
                    "drift (with prior manifest): {} auto-safe, {} requires-review, {} blocked",
                    report.auto_safe_count, report.requires_review_count, report.blocked_count
                );
            } else if report.blocked_count > 0 {
                eprintln!(
                    "drift: {} blocked operation(s) — migration cannot proceed automatically",
                    report.blocked_count
                );
            } else {
                eprintln!(
                    "drift: {} auto-safe, {} requires-review, {} blocked",
                    report.auto_safe_count, report.requires_review_count, report.blocked_count
                );
            }
            output_json(&report, "drift report");
            process::exit(exit_code);
        }
        Command::Serve => {
            let manifest = CatalogManifest::from_schemas(&schemas)
                .unwrap_or_else(|err| fatal_json("failed to build catalog manifest", err));
            let addr = serve_addr.parse().unwrap_or_else(|err| {
                eprintln!("invalid serve address '{serve_addr}': {err}");
                process::exit(1);
            });
            eprintln!("udb DataBroker listening on {addr}");
            let stack_size = serve_thread_stack_size();
            let serve_thread = std::thread::Builder::new()
                .name("udb-serve".to_string())
                .stack_size(stack_size)
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .thread_name("udb-runtime")
                        .thread_stack_size(stack_size)
                        .build()
                        .map_err(|err| format!("failed to create tokio runtime: {err}"))?;
                    runtime
                        .block_on(serve(manifest, schemas, addr))
                        .map_err(|err| format!("udb DataBroker stopped with error: {err}"))
                })
                .unwrap_or_else(|err| {
                    eprintln!("failed to spawn serve thread: {err}");
                    process::exit(1);
                });
            match serve_thread.join() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    eprintln!("{err}");
                    process::exit(1);
                }
                Err(_) => {
                    eprintln!("udb DataBroker serve thread panicked");
                    process::exit(1);
                }
            }
        }
        Command::AdminForceSync => {
            let manifest = CatalogManifest::from_schemas(&schemas)
                .unwrap_or_else(|err| fatal_json("failed to build catalog manifest", err));
            eprintln!(
                "force-sync: manifest built ({} tables, {} stores)",
                manifest.tables.len(),
                manifest.stores.len()
            );
            let multi_pg = MultiPgConfig::from_env();
            let active = multi_pg.active();
            if active.is_empty() {
                eprintln!("force-sync: no active PostgreSQL instances configured");
                process::exit(1);
            }
            eprintln!(
                "force-sync: {} instance(s): {}",
                active.len(),
                multi_pg.active_summary()
            );
            let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|err| {
                eprintln!("failed to create tokio runtime: {err}");
                process::exit(1);
            });
            let mut all_ok = true;
            for instance in &active {
                run_force_sync_for_instance(instance, &manifest, &schemas, &runtime, &mut all_ok);
            }
            if !all_ok {
                process::exit(1);
            }
        }
        // GAP 8: Dry-run — generate SQL plan, print it, exit without executing anything.
        Command::AdminDryRun => {
            let manifest = CatalogManifest::from_schemas(&schemas)
                .unwrap_or_else(|err| fatal_json("failed to build catalog manifest", err));
            let multi_pg = MultiPgConfig::from_env();
            let active = multi_pg.active();
            if active.is_empty() {
                eprintln!("dry-run: no active PostgreSQL instances configured");
                process::exit(1);
            }
            eprintln!(
                "dry-run: {} instance(s): {}",
                active.len(),
                multi_pg.active_summary()
            );
            let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|err| {
                eprintln!("failed to create tokio runtime: {err}");
                process::exit(1);
            });
            let mut all_ok = true;
            for instance in &active {
                run_dry_run_for_instance(instance, &manifest, &schemas, &runtime, &mut all_ok);
            }
            if !all_ok {
                process::exit(1);
            }
        }
        Command::TrackerDdl
        | Command::SystemDdl
        | Command::StatusSchema
        | Command::FsmStates
        | Command::ConfigSkeleton
        | Command::Doctor { .. }
        | Command::HealthCheck
        | Command::InitProject
        | Command::Dev { .. }
        | Command::Auth(_)
        | Command::AdminReleaseLock
        | Command::AdminVerifyAudit { .. }
        | Command::AdminResetDb { .. }
        | Command::PolicyLint
        | Command::PolicySeed
        | Command::CompatMatrix => {
            // Already handled before proto parsing above; unreachable.
        }
        Command::Explain => {
            let message_type = schemas
                .first()
                .map(|s| s.message_name.as_str())
                .unwrap_or("(none)");
            let manifest = CatalogManifest::from_schemas(&schemas)
                .unwrap_or_else(|err| fatal_json("failed to build catalog manifest", err));
            #[derive(Serialize)]
            struct ExplainReport<'a> {
                message_type: &'a str,
                table_count: usize,
                store_count: usize,
                tables: Vec<serde_json::Value>,
            }
            let tables: Vec<serde_json::Value> = manifest
                .tables
                .iter()
                .map(|t| {
                    serde_json::json!({
                        "schema": t.schema,
                        "table": t.table,
                        "message_type": t.message_name,
                        "columns": t.columns.iter().map(|c| serde_json::json!({
                            "name": c.column_name,
                            "type": c.sql_type,
                            "pii": c.security.is_pii,
                        })).collect::<Vec<_>>(),
                    })
                })
                .collect();
            let report = ExplainReport {
                message_type,
                table_count: manifest.tables.len(),
                store_count: manifest.stores.len(),
                tables,
            };
            output_json(&report, "explain report");
        }
        Command::ManifestExport => {
            let manifest = CatalogManifest::from_schemas(&schemas)
                .unwrap_or_else(|err| fatal_json("failed to build catalog manifest", err));
            let path = env::var("UDB_MANIFEST_EXPORT_PATH")
                .unwrap_or_else(|_| "udb_catalog_manifest.json".to_string());
            let json = serde_json::to_string_pretty(&manifest)
                .unwrap_or_else(|err| fatal_json("failed to serialize manifest", err));
            fs::write(&path, json).unwrap_or_else(|err| {
                eprintln!("failed to write manifest to {path}: {err}");
                process::exit(1);
            });
            eprintln!("manifest exported to {path}");
        }
        Command::FieldMaskPreview => {
            let manifest = CatalogManifest::from_schemas(&schemas)
                .unwrap_or_else(|err| fatal_json("failed to build catalog manifest", err));
            let target_scope = env::var("UDB_FIELD_MASK_SCOPE").unwrap_or_default();
            let scope_display = if target_scope.is_empty() {
                "(all)".to_string()
            } else {
                target_scope.clone()
            };
            #[derive(Serialize)]
            struct FieldMaskEntry<'a> {
                message_type: &'a str,
                column: &'a str,
                sql_type: &'a str,
                is_pii: bool,
                is_encrypted: bool,
                masked_for_scope: bool,
            }
            let entries: Vec<FieldMaskEntry<'_>> = manifest
                .tables
                .iter()
                .flat_map(|t| {
                    t.columns.iter().map(|c| {
                        let masked = c.security.is_pii || c.security.is_encrypted || c.encrypted;
                        let masked_for_scope = if target_scope.is_empty() {
                            masked
                        } else {
                            masked && target_scope != "udb:admin" && target_scope != "*"
                        };
                        FieldMaskEntry {
                            message_type: &t.message_name,
                            column: &c.column_name,
                            sql_type: &c.sql_type,
                            is_pii: c.security.is_pii,
                            is_encrypted: c.security.is_encrypted || c.encrypted,
                            masked_for_scope,
                        }
                    })
                })
                .collect();
            #[derive(Serialize)]
            struct FieldMaskReport<'a> {
                scope: &'a str,
                table_count: usize,
                column_count: usize,
                masked_count: usize,
                columns: Vec<FieldMaskEntry<'a>>,
            }
            let masked_count = entries.iter().filter(|e| e.masked_for_scope).count();
            let report = FieldMaskReport {
                scope: &scope_display,
                table_count: manifest.tables.len(),
                column_count: entries.len(),
                masked_count,
                columns: entries,
            };
            output_json(&report, "field mask preview");
        }
        Command::SyncMigrations {
            force_bootstrap,
            backend,
        } => {
            let mut cfg = DbOpsSyncConfig::from_env();
            if force_bootstrap {
                cfg.force_bootstrap = true;
            }
            // --backend flag overrides env var
            if let Some(ref b) = backend {
                cfg.backend =
                    BackendSyncTarget::from_token(b).unwrap_or(BackendSyncTarget::Postgres);
            }
            let is_multi = !matches!(cfg.backend, BackendSyncTarget::Postgres);
            if is_multi {
                match sync_all_backends(&schemas, &cfg) {
                    Ok(multi) => {
                        let all_clean = multi.backends.values().all(|r| r.clean);
                        let exit_code = if all_clean { 0 } else { 1 };
                        eprintln!(
                            "sync-migrations: {} backend(s) synced — clean={}",
                            multi.backends.len(),
                            all_clean
                        );
                        output_json(&multi, "sync-migrations multi-backend report");
                        process::exit(exit_code);
                    }
                    Err(err) => {
                        eprintln!("sync-migrations failed: {err}");
                        process::exit(1);
                    }
                }
            } else {
                match sync_db_ops(&schemas, &cfg) {
                    Ok(report) => {
                        let exit_code = if report.clean { 0 } else { 1 };
                        if report.clean {
                            eprintln!(
                                "sync-migrations: clean — {} file(s) verified, {} written",
                                report.verified, report.written
                            );
                        } else {
                            eprintln!(
                                "sync-migrations: {} stale file(s), {} no-header, \
                                 {} bootstrap artifact(s) written — review db_ops/bootstrap",
                                report.stale, report.no_header, report.bootstrap_written
                            );
                        }
                        output_json(&report, "sync-migrations report");
                        process::exit(exit_code);
                    }
                    Err(err) => {
                        eprintln!("sync-migrations failed: {err}");
                        process::exit(1);
                    }
                }
            }
        }
    }
}

fn ensure_cli_correlation_id() -> String {
    if let Ok(existing) = env::var("UDB_CORRELATION_ID")
        && !existing.trim().is_empty()
    {
        return existing;
    }
    let generated = uuid::Uuid::new_v4().to_string();
    // This CLI helper runs during command startup, before UDB spawns worker
    // threads that read process environment. Keep the mutation here so callers
    // that still consult UDB_CORRELATION_ID see the generated id consistently.
    unsafe {
        env::set_var("UDB_CORRELATION_ID", &generated);
    }
    generated
}

fn run_dev_sandbox(action: DevAction, service: Option<&str>, confirmed: bool) -> i32 {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let compose_file = manifest_dir.join("docker-compose.playground.yml");
    if !compose_file.exists() {
        eprintln!(
            "dev: playground compose file not found at {}",
            compose_file.display()
        );
        return 1;
    }

    let run_compose = |args: &[&str]| -> Result<(), i32> {
        let status = ProcessCommand::new("docker")
            .arg("compose")
            .arg("-f")
            .arg(&compose_file)
            .args(args)
            .status()
            .map_err(|err| {
                eprintln!("dev: failed to execute docker compose: {err}");
                1
            })?;
        if status.success() {
            Ok(())
        } else {
            eprintln!("dev: docker compose exited with {status}");
            Err(status.code().unwrap_or(1))
        }
    };

    match action {
        DevAction::Up => {
            if let Err(code) = run_compose(&["up", "-d", "--build"]) {
                return code;
            }
            let _ = run_compose(&["ps"]);
            eprintln!("dev: UDB playground is starting");
            eprintln!("dev: gRPC localhost:50051, metrics localhost:50052");
            0
        }
        DevAction::Down => run_compose(&["down"]).err().unwrap_or(0),
        DevAction::Logs => {
            let service = service.unwrap_or("udb");
            run_compose(&["logs", "-f", service]).err().unwrap_or(0)
        }
        DevAction::Status => run_compose(&["ps"]).err().unwrap_or(0),
        DevAction::Reset => {
            if !confirmed {
                eprintln!("dev reset: destructive operation - pass --yes to remove volumes");
                return 1;
            }
            if let Err(code) = run_compose(&["down", "-v"]) {
                return code;
            }
            run_compose(&["up", "-d", "--build"]).err().unwrap_or(0)
        }
        DevAction::Smoke => {
            let script = if cfg!(windows) {
                manifest_dir.join("scripts").join("smoke_test.ps1")
            } else {
                manifest_dir.join("scripts").join("smoke_test.sh")
            };
            let status = if cfg!(windows) {
                ProcessCommand::new("powershell")
                    .arg("-ExecutionPolicy")
                    .arg("Bypass")
                    .arg("-File")
                    .arg(&script)
                    .status()
            } else {
                ProcessCommand::new("bash").arg(&script).status()
            };
            match status {
                Ok(status) if status.success() => 0,
                Ok(status) => status.code().unwrap_or(1),
                Err(err) => {
                    eprintln!("dev smoke: failed to execute {}: {err}", script.display());
                    1
                }
            }
        }
    }
}
