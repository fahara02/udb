//! CLI entry point internals (Phase H split of main.rs).
use std::env;
use std::fs;
use std::io::Write;
use std::panic;
use std::path::PathBuf;
use std::process;
use std::process::Command as ProcessCommand;
use std::sync::Once;

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
mod init;
mod init_prompt;
mod native_app;
pub(crate) mod native_lint;
mod output;
mod proto_export;
mod proto_fmt;
mod scaffold;
mod sdk_gen;
pub(crate) use args::*;
pub(crate) use auth::*;
pub(crate) use doctor::*;
pub(crate) use env_setup::*;
pub(crate) use output::*;
pub(crate) use scaffold::*;

/// D.9: platform-appropriate smoke-test script filename. Extracted so the
/// Windows vs Unix choice is explicit and unit-tested (both scripts must ship).
fn smoke_script_filename() -> &'static str {
    if cfg!(windows) {
        "smoke_test.ps1"
    } else {
        "smoke_test.sh"
    }
}

#[cfg(test)]
mod platform_tests {
    use super::smoke_script_filename;
    use std::path::PathBuf;

    #[test]
    fn smoke_script_selected_per_platform_and_both_ship() {
        assert_eq!(
            smoke_script_filename(),
            if cfg!(windows) {
                "smoke_test.ps1"
            } else {
                "smoke_test.sh"
            }
        );
        // Windows support is explicit, not accidental: both scripts must exist.
        let scripts = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts");
        assert!(
            scripts.join("smoke_test.ps1").is_file(),
            "Windows smoke script missing"
        );
        assert!(
            scripts.join("smoke_test.sh").is_file(),
            "Unix smoke script missing"
        );
    }
}
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

fn install_serve_panic_hook() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unnamed");
            let payload = info
                .payload()
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
                .unwrap_or("<non-string panic payload>");
            let location = info
                .location()
                .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
                .unwrap_or_else(|| "<unknown>".to_string());
            eprintln!(
                "udb serve panic: thread={thread_name} location={location} payload={payload}"
            );
            let _ = std::io::stderr().flush();
            previous(info);
        }));
    });
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
    // Phase 10: install the OTLP trace exporter (feature- and env-gated) BEFORE
    // the fmt subscriber so spans export when `otel` is compiled + enabled.
    udb::runtime::otel::init_otel();
    init_observability();
    let (command, proto_root, namespace, serve_addr) = parse_args(&args);
    if let Command::InvalidUsage { message } = &command {
        eprintln!("{message}");
        process::exit(2);
    }
    let proto_root = resolve_existing_project_path(&proto_root);

    // Commands that do not need proto parsing — emit output and exit immediately.
    match command {
        Command::InvalidUsage { .. } => unreachable!("handled before path resolution"),
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
        Command::Init(args) => match init::run(&args) {
            Ok(code) => process::exit(code),
            Err(err) => {
                eprintln!("init failed: {err}");
                process::exit(1);
            }
        },
        Command::ProtoExport {
            out_dir,
            manage_buf_yaml,
            format_proto,
        } => {
            process::exit(proto_export::run(&out_dir, manage_buf_yaml, format_proto));
        }
        Command::ProtoFmt { root, check } => {
            process::exit(proto_fmt::run(&root, check));
        }
        Command::Sdk {
            action,
            lang,
            templates_dir,
            out_dir,
            selector,
        } => {
            process::exit(sdk_gen::run(
                action,
                &lang,
                &templates_dir,
                &out_dir,
                &selector,
            ));
        }
        Command::Native {
            action,
            services,
            out_dir,
            lang,
            framework,
            json,
            confirmed,
            baseline,
        } => {
            process::exit(run_native_contract(
                action,
                &services,
                &out_dir,
                lang.as_deref(),
                &framework,
                json,
                confirmed,
                &baseline,
            ));
        }
        Command::AppInit {
            lang,
            framework,
            services,
            tenant,
            project,
            auth,
            out_dir,
            package_manager,
            confirmed,
        } => {
            process::exit(native_app::run_app_init(native_app::AppInitArgs {
                lang,
                framework,
                services,
                tenant,
                project,
                auth,
                out_dir,
                package_manager,
                confirmed,
            }));
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
            let (result, exit_code) = build_policy_lint_cli_result(load_abac_policies_for_lint());
            if let Some(error) = result.error.as_ref() {
                eprintln!("{error}");
            }
            output_json(&result, "policy lint result");
            process::exit(exit_code);
        }
        Command::PolicySeed => {
            let policies = match load_abac_policies_for_lint() {
                Ok(policies) => policies,
                Err(err) => {
                    eprintln!("{err}");
                    process::exit(1);
                }
            };
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
            install_serve_panic_hook();
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
        | Command::InvalidUsage { .. }
        | Command::SystemDdl
        | Command::StatusSchema
        | Command::FsmStates
        | Command::ConfigSkeleton
        | Command::Doctor { .. }
        | Command::HealthCheck
        | Command::InitProject
        | Command::Init(_)
        | Command::Dev { .. }
        | Command::Auth(_)
        | Command::AdminReleaseLock
        | Command::AdminVerifyAudit { .. }
        | Command::AdminResetDb { .. }
        | Command::PolicyLint
        | Command::PolicySeed
        | Command::ProtoExport { .. }
        | Command::ProtoFmt { .. }
        | Command::Sdk { .. }
        | Command::Native { .. }
        | Command::AppInit { .. }
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

#[allow(clippy::too_many_arguments)]
fn run_native_contract(
    action: NativeAction,
    services: &[String],
    out_dir: &str,
    lang: Option<&str>,
    framework: &str,
    json: bool,
    _confirmed: bool,
    baseline: &str,
) -> i32 {
    // `contract-baseline` emits raw descriptor bytes and must not route through the
    // parsed-manifest path. It writes binary, so prefer a direct file write to the
    // `--baseline` path (robust on every platform); fall back to raw stdout when no
    // path is given (e.g. POSIX shell redirection).
    if matches!(action, NativeAction::ContractBaseline) {
        let bytes = udb::runtime::native_catalog::embedded_file_descriptor_set();
        if baseline.trim().is_empty() {
            use std::io::Write as _;
            if let Err(err) = std::io::stdout().write_all(bytes) {
                eprintln!("native contract-baseline: failed to write descriptor bytes: {err}");
                return 1;
            }
        } else if let Err(err) = fs::write(baseline, bytes) {
            eprintln!("native contract-baseline: failed to write '{baseline}': {err}");
            return 1;
        }
        return 0;
    }

    let manifest = match udb::runtime::descriptor_manifest::try_descriptor_contract_manifest() {
        Ok(manifest) => manifest,
        Err(err) => {
            eprintln!("native contract: {err}");
            return 1;
        }
    };
    match action {
        NativeAction::ContractBaseline => unreachable!("handled above"),
        NativeAction::ContractDiff => run_contract_diff(&manifest, baseline),
        NativeAction::Docs => {
            print!("{}", native_docs_markdown(&manifest));
            0
        }
        NativeAction::Manifest => {
            output_json(&native_manifest_json(&manifest), "native contract manifest");
            0
        }
        NativeAction::List => native_app::run_list(&manifest, json),
        NativeAction::Add => native_app::run_add(services, out_dir),
        NativeAction::Remove => native_app::run_remove(services, out_dir),
        NativeAction::Generate => native_app::run_generate(out_dir, lang, framework),
        NativeAction::Doctor => native_app::run_doctor(out_dir),
        NativeAction::Smoke => native_app::run_smoke(out_dir),
        NativeAction::Lint => {
            let findings = native_contract_findings(&manifest);
            if findings.is_empty() {
                println!("native contract lint passed");
                0
            } else {
                let has_error = findings.iter().any(|finding| {
                    finding.get("severity").and_then(|v| v.as_str()) == Some("error")
                });
                output_json(&findings, "native contract lint findings");
                if has_error { 1 } else { 0 }
            }
        }
    }
}

/// Phase 3 acceptance gate: diff the live descriptor contract against a committed
/// baseline `FileDescriptorSet` and fail (exit 1) on any contract-breaking change
/// — auth/db/sdk/event-breaking or removed surface. The baseline is a deliberate
/// reference (e.g. last release); intentional breaks bump
/// `NATIVE_CONTRACT_VERSION` and regenerate the baseline.
fn run_contract_diff(
    live: &udb::runtime::descriptor_manifest::DescriptorContractManifest,
    baseline: &str,
) -> i32 {
    if baseline.trim().is_empty() {
        eprintln!("native contract-diff: --baseline <path> is required");
        return 1;
    }
    let bytes = match fs::read(baseline) {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("native contract-diff: cannot read baseline '{baseline}': {err}");
            return 1;
        }
    };
    let old =
        match udb::runtime::descriptor_manifest::descriptor_contract_manifest_from_bytes(&bytes) {
            Ok(manifest) => manifest,
            Err(err) => {
                eprintln!("native contract-diff: baseline '{baseline}' failed to decode: {err}");
                return 1;
            }
        };
    let changes = udb::runtime::descriptor_diff::diff_manifests(&old, live);
    let summary = udb::runtime::descriptor_diff::summarize(&changes);
    output_json(&summary, "native contract diff");
    let breaking = [
        "auth_breaking",
        "db_breaking",
        "sdk_breaking",
        "event_breaking",
        "removed",
    ]
    .iter()
    .map(|key| summary.get(*key).and_then(|v| v.as_u64()).unwrap_or(0))
    .sum::<u64>();
    if breaking > 0 {
        eprintln!(
            "::error::native contract-diff: {breaking} contract-breaking change(s) vs baseline \
             '{baseline}'. If intentional, bump NATIVE_CONTRACT_VERSION and regenerate the \
             baseline (`udb native contract-baseline --baseline {baseline}`)."
        );
        1
    } else {
        0
    }
}

/// Render a Markdown native-service table from the descriptor manifest, so docs
/// are descriptor-generated (Phase 3 "docs and inventories") rather than
/// hand-maintained and drift-prone.
fn native_docs_markdown(
    manifest: &udb::runtime::descriptor_manifest::DescriptorContractManifest,
) -> String {
    let mut rows: Vec<(String, String)> = Vec::new();
    for service in &manifest.services {
        let Some(native) = service.native_service.as_ref() else {
            continue;
        };
        let deps = native_dependency_tokens(native);
        let deps = if deps.is_empty() {
            "—".to_string()
        } else {
            deps.join(", ")
        };
        let listeners = [
            ("control-plane", native.control_plane_listener_allowed),
            ("public", native.public_listener_allowed),
            ("peer", native.peer_listener_allowed),
        ]
        .into_iter()
        .filter(|(_, allowed)| *allowed)
        .map(|(name, _)| name)
        .collect::<Vec<_>>()
        .join(", ");
        let id = udb::runtime::service::native_registry::canonical_service_id(&native.service_id);
        rows.push((
            id.clone(),
            format!(
                "| `{}` | {} | {} | {} | {} | {} | {} |",
                id,
                native.display_name,
                native.category,
                if native.default_enabled { "yes" } else { "no" },
                deps,
                service.methods.len(),
                if listeners.is_empty() {
                    "—".to_string()
                } else {
                    listeners
                },
            ),
        ));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out = String::new();
    out.push_str("# UDB Native Services\n\n");
    out.push_str(
        "<!-- Generated by `udb native docs` from the embedded descriptor. Do not edit by hand. -->\n\n",
    );
    out.push_str(&format!(
        "Contract version `{}` · {} native services.\n\n",
        udb::runtime::descriptor_diff::NATIVE_CONTRACT_VERSION,
        rows.len(),
    ));
    out.push_str(
        "| Service | Display name | Category | Default | Dependencies | RPCs | Listeners |\n",
    );
    out.push_str("| --- | --- | --- | --- | --- | --- | --- |\n");
    for (_, row) in &rows {
        out.push_str(row);
        out.push('\n');
    }
    out
}

fn native_manifest_json(
    manifest: &udb::runtime::descriptor_manifest::DescriptorContractManifest,
) -> serde_json::Value {
    let services: Vec<serde_json::Value> = manifest
        .services
        .iter()
        .map(|service| {
            let native = service.native_service.as_ref();
            let rpcs: Vec<serde_json::Value> = service
                .methods
                .iter()
                .map(|rpc| {
                    let security = rpc.endpoint_security.as_ref();
                    serde_json::json!({
                        "method": rpc.method,
                        "path": rpc.grpc_path(),
                        "kind": rpc.kind(),
                        "input": rpc.input_type,
                        "output": rpc.output_type,
                        "auth_mode": security.map(|s| s.auth_mode_name()).unwrap_or("unspecified"),
                        "scopes": security.map(|s| s.scopes.clone()).unwrap_or_default(),
                        "roles": security.map(|s| s.roles.clone()).unwrap_or_default(),
                        "policy_ref": security.map(|s| s.policy_ref.clone()).unwrap_or_default(),
                        "tenant_required": security.map(|s| s.tenant_required).unwrap_or(false),
                        "tenant_field": security.map(|s| s.tenant_field.clone()).unwrap_or_default(),
                        "project_field": security.map(|s| s.project_field.clone()).unwrap_or_default(),
                        "endpoint_security": security.map(endpoint_security_json),
                        "event_contract": rpc.event_contract.as_ref().map(event_contract_json),
                        "emits": rpc.emits.iter().map(emitted_event_json).collect::<Vec<_>>(),
                        "sdk_surface": rpc.sdk_surface.as_ref().map(sdk_surface_json),
                        "dependency_contract": rpc.dependency_contract.as_ref().map(dependency_contract_json),
                        "http": rpc.http_rule.as_ref().map(|http| serde_json::json!({
                            "verb": http.verb,
                            "path": http.path,
                            "body": http.body,
                            "response_body": http.response_body,
                        })),
                    })
                })
                .collect();
            serde_json::json!({
                "service": service.full_name(),
                "file": service.file_path,
                "native_service": native.map(|n| serde_json::json!({
                    "service_id": n.service_id,
                    "logical_service_id": n.logical_service_id,
                    "proto_service_id": n.proto_service_id,
                    "display_name": n.display_name,
                    "category": n.category,
                    "default_enabled": n.default_enabled,
                    "dependencies": native_dependency_tokens(n),
                    "sdk_facade_name": n.sdk_facade_name,
                    "cli_scaffold_group": n.cli_scaffold_group,
                    "public_listener_allowed": n.public_listener_allowed,
                    "control_plane_listener_allowed": n.control_plane_listener_allowed,
                    "peer_listener_allowed": n.peer_listener_allowed,
                    "owns_background_workers": n.owns_background_workers,
                })),
                "sdk_surface": service.sdk_surface.as_ref().map(sdk_surface_json),
                "cli_scaffold": service.cli_scaffold.as_ref().map(cli_scaffold_json),
                "dependency_contract": service.dependency_contract.as_ref().map(dependency_contract_json),
                "rpc_count": rpcs.len(),
                "rpcs": rpcs,
            })
        })
        .collect();

    // Persisted "tables": messages carrying db_table_security, with their fields
    // and per-column security. This is the canonical DB-contract surface (the live
    // descriptor manifest drives drift checks; this JSON is the committed,
    // diffable projection of the same truth).
    let tables: Vec<serde_json::Value> = manifest
        .messages
        .iter()
        .filter(|m| m.db_table_security.is_some())
        .map(|message| {
            let fields: Vec<serde_json::Value> = message
                .fields
                .iter()
                .map(|field| {
                    serde_json::json!({
                        "name": field.name,
                        "number": field.number,
                        "type": field.type_name,
                        "column_security": field.db_column_security.as_ref().map(column_security_json),
                        "scalar_security": scalar_security_json(&field.scalar_security),
                    })
                })
                .collect();
            serde_json::json!({
                "message": message.full_name,
                "file": message.file_path,
                "table_security": message.db_table_security.as_ref().map(table_security_json),
                "field_count": fields.len(),
                "fields": fields,
            })
        })
        .collect();

    let messages: Vec<serde_json::Value> = manifest
        .messages
        .iter()
        .map(|message| {
            let fields: Vec<serde_json::Value> = message
                .fields
                .iter()
                .map(|field| {
                    serde_json::json!({
                        "name": field.name,
                        "number": field.number,
                        "type": field.type_name,
                        "column_security": field.db_column_security.as_ref().map(column_security_json),
                        "scalar_security": scalar_security_json(&field.scalar_security),
                    })
                })
                .collect();
            serde_json::json!({
                "message": message.full_name,
                "file": message.file_path,
                "sdk_surface": message.sdk_surface.as_ref().map(sdk_surface_json),
                "event_contract": message.event_contract.as_ref().map(event_contract_json),
                "dependency_contract": message.dependency_contract.as_ref().map(dependency_contract_json),
                "table_security": message.db_table_security.as_ref().map(table_security_json),
                "field_count": fields.len(),
                "fields": fields,
            })
        })
        .collect();

    // Event contracts declared anywhere (messages or RPCs), keyed by source so CDC
    // topic drift is reviewable from the committed contract.
    let mut events: Vec<serde_json::Value> = Vec::new();
    for message in &manifest.messages {
        if let Some(event) = message.event_contract.as_ref() {
            let mut value = event_contract_json(event);
            value["source"] = serde_json::json!(message.full_name);
            events.push(value);
        }
    }
    for service in &manifest.services {
        for rpc in &service.methods {
            if let Some(event) = rpc.event_contract.as_ref() {
                let mut value = event_contract_json(event);
                value["source"] = serde_json::json!(rpc.grpc_path());
                events.push(value);
            }
        }
    }
    events.sort_by(|a, b| {
        a["source"]
            .as_str()
            .unwrap_or_default()
            .cmp(b["source"].as_str().unwrap_or_default())
    });

    serde_json::json!({
        "contract_version": udb::runtime::descriptor_diff::NATIVE_CONTRACT_VERSION,
        "udb_version": env!("CARGO_PKG_VERSION"),
        "protocol_version": udb::runtime::native_catalog::protocol_version(),
        "service_count": services.len(),
        "services": services,
        "message_count": messages.len(),
        "messages": messages,
        "table_count": tables.len(),
        "tables": tables,
        "event_count": events.len(),
        "events": events,
    })
}

fn dependency_contract_json(
    dependency: &udb::runtime::descriptor_manifest::DependencyContract,
) -> serde_json::Value {
    serde_json::json!({
        "required_native_services": dependency.required_native_services,
        "optional_native_services": dependency.optional_native_services,
        "required_backends": dependency.required_backends,
        "optional_backends": dependency.optional_backends,
        "required_features": dependency.required_features,
        "required_env": dependency.required_env,
        "degraded_when_missing": dependency.degraded_when_missing,
    })
}

fn endpoint_security_json(
    security: &udb::runtime::descriptor_manifest::EndpointSecurityContract,
) -> serde_json::Value {
    serde_json::json!({
        "auth_mode": security.auth_mode_name(),
        "roles": security.roles,
        "scopes": security.scopes,
        "policy_ref": security.policy_ref,
        "tenant_required": security.tenant_required,
        "csrf_required": security.csrf_required,
        "internal_grpc_only": security.internal_grpc_only,
        "required_assurance_level": security.required_assurance_level,
        "allowed_credential_types": security.allowed_credential_types,
        "rate_limit_policy_ref": security.rate_limit_policy_ref,
        "abuse_policy_ref": security.abuse_policy_ref,
        "audit_event_type": security.audit_event_type,
        "decision_resource": security.decision_resource,
        "owner_field": security.owner_field,
        "tenant_field": security.tenant_field,
        "project_field": security.project_field,
        "idempotency_required": security.idempotency_required,
        "request_context_required": security.request_context_required,
    })
}

fn event_contract_json(
    event: &udb::runtime::descriptor_manifest::EventContract,
) -> serde_json::Value {
    serde_json::json!({
        "event_type": event.event_type,
        "outbox_topic": event.outbox_topic,
        "partition_key_field": event.partition_key_field,
        "payload_redaction_profile": event.payload_redaction_profile,
        "delivery_guarantee": event.delivery_guarantee,
        "replay_compatibility": event.replay_compatibility,
    })
}

fn emitted_event_json(emit: &udb::runtime::descriptor_manifest::EmittedEvent) -> serde_json::Value {
    serde_json::json!({
        "topic": emit.topic,
        "partition_key_field": emit.partition_key_field,
        "delivery_guarantee": emit.delivery_guarantee,
        "payload_redaction_profile": emit.payload_redaction_profile,
        "conditional": emit.conditional,
    })
}

fn sdk_surface_json(
    surface: &udb::runtime::descriptor_manifest::SdkSurfaceContract,
) -> serde_json::Value {
    serde_json::json!({
        "include_in_facade": surface.include_in_facade,
        "method_alias": surface.method_alias,
        "required_credential_provider": surface.required_credential_provider,
        "streaming_helper_type": surface.streaming_helper_type,
        "default_deadline_ms": surface.default_deadline_ms,
        "default_max_attempts": surface.default_max_attempts,
        "browser_safe": surface.browser_safe,
        "server_only": surface.server_only,
        "boilerplate_recipe_tags": surface.boilerplate_recipe_tags,
        "generate_minimal_example": surface.generate_minimal_example,
    })
}

fn cli_scaffold_json(
    scaffold: &udb::runtime::descriptor_manifest::CliScaffoldContract,
) -> serde_json::Value {
    serde_json::json!({
        "scaffold_package": scaffold.scaffold_package,
        "import_path": scaffold.import_path,
        "required_env": scaffold.required_env,
        "generated_files": scaffold.generated_files,
        "route_name": scaffold.route_name,
        "middleware_name": scaffold.middleware_name,
        "required_native_services": scaffold.required_native_services,
        "optional_native_services": scaffold.optional_native_services,
        "secret_placeholders": scaffold.secret_placeholders,
        "post_generation_commands": scaffold.post_generation_commands,
        "smoke_test_command": scaffold.smoke_test_command,
    })
}

fn table_security_json(
    table: &udb::runtime::descriptor_manifest::DbTableSecurityContract,
) -> serde_json::Value {
    serde_json::json!({
        "tenant_isolation_mode": table.tenant_isolation_mode,
        "project_isolation_mode": table.project_isolation_mode,
        "tenant_column": table.tenant_column,
        "project_column": table.project_column,
        "rls_policy_template": table.rls_policy_template,
        "soft_delete_mode": table.soft_delete_mode,
        "retention_class": table.retention_class,
        "retention_days": table.retention_days,
        "audit_mode": table.audit_mode,
        "encryption_profile": table.encryption_profile,
        "pii_profile": table.pii_profile,
        "break_glass_visible": table.break_glass_visible,
        "export_eligible": table.export_eligible,
        "data_residency_policy_ref": table.data_residency_policy_ref,
    })
}

fn column_security_json(
    column: &udb::runtime::descriptor_manifest::DbColumnSecurityContract,
) -> serde_json::Value {
    serde_json::json!({
        "secret_classification": column.secret_classification,
        "output_view": column.output_view,
        "redaction_strategy": column.redaction_strategy,
        "tokenization_strategy": column.tokenization_strategy,
        "hashing_strategy": column.hashing_strategy,
        "hashing_algorithm": column.hashing_algorithm,
        "encryption_key_class": column.encryption_key_class,
        "searchable_encrypted": column.searchable_encrypted,
        "uniqueness_scope": column.uniqueness_scope,
        "owner_field": column.owner_field,
        "tenant_field": column.tenant_field,
        "project_field": column.project_field,
    })
}

fn scalar_security_json(
    scalar: &udb::runtime::descriptor_manifest::ScalarFieldSecurity,
) -> serde_json::Value {
    serde_json::json!({
        "pii": scalar.pii,
        "encrypted_security": scalar.encrypted_security,
        "log_masked": scalar.log_masked,
        "log_redacted": scalar.log_redacted,
        "sensitive": scalar.sensitive,
        "requires_consent": scalar.requires_consent,
        "data_purpose": scalar.data_purpose,
        "retention_days": scalar.retention_days,
        "tokenized": scalar.tokenized,
        "security_classification": scalar.security_classification,
        "data_category": scalar.data_category,
    })
}

pub(crate) fn native_dependency_tokens(
    native: &udb::runtime::descriptor_manifest::NativeServiceContract,
) -> Vec<&'static str> {
    let mut deps = Vec::new();
    if native.requires_postgres {
        deps.push("postgres");
    }
    if native.requires_redis {
        deps.push("redis");
    }
    if native.requires_object_store {
        deps.push("object_store");
    }
    if native.requires_kafka {
        deps.push("kafka");
    }
    deps
}

fn native_contract_findings(
    manifest: &udb::runtime::descriptor_manifest::DescriptorContractManifest,
) -> Vec<serde_json::Value> {
    let mut findings = Vec::new();
    for service in &manifest.services {
        let requires_endpoint_security =
            service.package.starts_with("udb.core.") && service.package.contains(".services.");
        if service.package.starts_with("udb.core.") && service.native_service.is_none() {
            findings.push(serde_json::json!({
                "severity": "error",
                "kind": "native_service_missing",
                "service": service.full_name(),
            }));
        }
        if service.native_service.is_some() {
            if service.sdk_surface.is_none() {
                findings.push(serde_json::json!({
                    "severity": "warning",
                    "kind": "service_sdk_surface_missing",
                    "service": service.full_name(),
                }));
            }
            if service.cli_scaffold.is_none() {
                findings.push(serde_json::json!({
                    "severity": "warning",
                    "kind": "service_cli_scaffold_missing",
                    "service": service.full_name(),
                }));
            }
            if service.dependency_contract.is_none() {
                findings.push(serde_json::json!({
                    "severity": "warning",
                    "kind": "service_dependency_contract_missing",
                    "service": service.full_name(),
                }));
            }
        }
        for rpc in &service.methods {
            let Some(security) = rpc.endpoint_security.as_ref() else {
                if requires_endpoint_security {
                    findings.push(serde_json::json!({
                        "severity": "error",
                        "kind": "endpoint_security_missing",
                        "rpc": rpc.grpc_path(),
                    }));
                }
                continue;
            };
            if security.auth_mode_name() != "public"
                && security.scopes.is_empty()
                && security.roles.is_empty()
                && security.policy_ref.trim().is_empty()
            {
                findings.push(serde_json::json!({
                    "severity": "error",
                    "kind": "non_public_rpc_without_policy",
                    "rpc": rpc.grpc_path(),
                }));
            }
            if service.native_service.is_some() {
                if rpc.sdk_surface.is_none() {
                    findings.push(serde_json::json!({
                        "severity": "warning",
                        "kind": "method_sdk_surface_missing",
                        "rpc": rpc.grpc_path(),
                    }));
                }
                if rpc.cli_scaffold.is_none() {
                    findings.push(serde_json::json!({
                        "severity": "warning",
                        "kind": "method_cli_scaffold_missing",
                        "rpc": rpc.grpc_path(),
                    }));
                }
                if rpc.event_contract.is_none()
                    && !rpc.method.starts_with("Get")
                    && !rpc.method.starts_with("List")
                    && !rpc.method.starts_with("Check")
                    && !rpc.method.starts_with("Validate")
                    && !rpc.method.starts_with("Introspect")
                {
                    findings.push(serde_json::json!({
                        "severity": "warning",
                        "kind": "method_event_contract_missing",
                        "rpc": rpc.grpc_path(),
                    }));
                }
                if rpc.dependency_contract.is_none() {
                    findings.push(serde_json::json!({
                        "severity": "warning",
                        "kind": "method_dependency_contract_missing",
                        "rpc": rpc.grpc_path(),
                    }));
                }
            }
        }
    }
    for message in &manifest.messages {
        let is_entity = message.file_path.contains("/entity/")
            || message.file_path.contains("\\entity\\")
            || message.file_path.contains("/entities/")
            || message.file_path.contains("\\entities\\");
        if is_entity && message.db_table_security.is_none() {
            findings.push(serde_json::json!({
                "severity": "warning",
                "kind": "db_table_security_missing",
                "message": message.full_name,
                "file": message.file_path,
            }));
        }
        let is_event =
            message.file_path.contains("/events/") || message.file_path.contains("\\events\\");
        if is_event && message.event_contract.is_none() {
            findings.push(serde_json::json!({
                "severity": "warning",
                "kind": "message_event_contract_missing",
                "message": message.full_name,
                "file": message.file_path,
            }));
        }
    }
    // F5/F13: append the enterprise descriptor lints (public-RPC abuse-policy ref,
    // tenant_required tenant source, tenant-scoped table RLS gap, scalar-option
    // loss, SDK RPC-count drift) from the dedicated lint module.
    findings.extend(native_lint::descriptor_lint_findings(manifest));
    findings
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

#[derive(Serialize)]
struct PolicyLintCliResult {
    policy_count: usize,
    finding_count: usize,
    passed: bool,
    findings: Vec<PolicyLintFinding>,
    error: Option<String>,
}

fn build_policy_lint_cli_result(
    policies: Result<Vec<AbacPolicy>, String>,
) -> (PolicyLintCliResult, i32) {
    match policies {
        Ok(policies) => {
            let findings = lint_policies(&policies);
            let has_errors = findings.iter().any(|finding| finding.severity == "error");
            (
                PolicyLintCliResult {
                    policy_count: policies.len(),
                    finding_count: findings.len(),
                    passed: !has_errors,
                    findings,
                    error: None,
                },
                if has_errors { 1 } else { 0 },
            )
        }
        Err(error) => (
            PolicyLintCliResult {
                policy_count: 0,
                finding_count: 0,
                passed: false,
                findings: Vec::new(),
                error: Some(error),
            },
            1,
        ),
    }
}

fn find_existing_project_file(components: &[&str]) -> Option<PathBuf> {
    let mut relative = PathBuf::new();
    for component in components {
        relative.push(component);
    }
    find_existing_relative_path_from(env::current_dir().ok()?, &[relative])
}

fn find_existing_relative_path_from(start: PathBuf, candidates: &[PathBuf]) -> Option<PathBuf> {
    let mut dir = start;
    loop {
        for relative in candidates {
            let path = dir.join(relative);
            if path.is_file() {
                return Some(path);
            }
        }
        match dir.parent() {
            Some(parent) => dir = parent.to_path_buf(),
            None => return None,
        }
    }
}

fn run_dev_sandbox(action: DevAction, service: Option<&str>, confirmed: bool) -> i32 {
    let compose_file = match find_existing_project_file(&["docker-compose.playground.yml"]) {
        Some(path) => path,
        None => {
            eprintln!(
                "dev: docker-compose.playground.yml not found in the current directory or any parent; run from inside a UDB checkout"
            );
            return 1;
        }
    };

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
            let script = match find_existing_project_file(&["scripts", smoke_script_filename()]) {
                Some(path) => path,
                None => {
                    eprintln!(
                        "dev smoke: {} not found under scripts/ in the current directory or any parent; run from inside a UDB checkout",
                        smoke_script_filename()
                    );
                    return 1;
                }
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
