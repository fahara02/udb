//! main.rs split — args (Phase H).
use super::*;

pub(crate) enum Command {
    Catalog,
    Dsn,
    Sql,
    Plan,
    Lint,
    Drift,
    /// Check runtime environment and backend readiness.
    Doctor {
        output_mode: DoctorOutputMode,
        with_probes: bool,
    },
    /// Lightweight Docker HEALTHCHECK — exit 0 if healthy, 1 otherwise.
    HealthCheck,
    /// Start the tonic DataBroker skeleton.
    Serve,
    /// Force the startup lifecycle from CLI and exit with a JSON report.
    AdminForceSync,
    /// GAP 8: Dry-run — generate all SQL that would be applied, print it as JSON, and exit.
    /// Nothing is executed against any backend. Safe to run against production at any time.
    AdminDryRun,
    /// Emit tracker table DDL to stdout (for psql bootstrap scripts).
    TrackerDdl,
    /// Emit UDB-owned system catalog DDL to stdout.
    SystemDdl,
    /// Emit the analytics events DDL (signal DB schema).
    StatusSchema,
    /// List all FSM states and valid transitions as JSON.
    FsmStates,
    /// Emit a default `MigrationOptions` skeleton as JSON (config reference).
    ConfigSkeleton,
    /// Scaffold a new UDB project (sample proto, config template, DDL bootstrap, docker-compose).
    InitProject,
    /// Start, stop, inspect, or test the local multi-backend UDB sandbox.
    Dev {
        action: DevAction,
        service: Option<String>,
        confirmed: bool,
    },
    /// Explain the generated DDL, DSN, and policies for a single message type.
    Explain,
    /// Export the current CatalogManifest to a JSON file (for CI plan approval workflows).
    ManifestExport,
    /// Lint a set of ABAC policies loaded from UDB_ABAC_POLICY_FILE or stdin (JSON array).
    PolicyLint,
    /// Generate INSERT SQL to seed ABAC policies into the configured UDB ABAC table.
    PolicySeed,
    /// Preview field masking for a given message type and scope set.
    FieldMaskPreview,
    /// Print the compatibility matrix of supported proto option shapes as JSON.
    CompatMatrix,
    /// Terminate the PostgreSQL backend session(s) that hold the startup advisory lock.
    /// Use this to clear a stale lock left by a process that was killed before releasing it.
    AdminReleaseLock,
    /// Verify the tamper-evident admin audit-log hash chain.
    AdminVerifyAudit {
        /// Optional maximum rows to scan. <= 0 scans the full local chain.
        limit: i64,
    },
    /// Drop all UDB-managed schemas and ledger tables.
    ///
    /// Schema names are discovered dynamically from `public.schema_migrations.source_schema`
    /// — UDB never hardcodes any proto-specific schema name here.
    /// Requires explicit `--yes` flag to guard against accidental runs.
    AdminResetDb {
        /// Must be true (pass `--yes`) or the command exits 1 without touching the DB.
        confirmed: bool,
    },
    /// Sync db_ops/migrations (and db_ops/bootstrap) with the current proto AST.
    ///
    /// - Creates `db_ops/{migrations,seeds,bootstrap}` at sibling level if absent.
    /// - If migrations is empty: writes baseline SQL (proto is source of truth).
    /// - If migrations is non-empty: double-checksum verification; stale files
    ///   trigger proto-priority refresh artifacts in `db_ops/bootstrap`.
    SyncMigrations {
        /// When true, refresh db_ops/bootstrap even when all files verify clean.
        force_bootstrap: bool,
        /// Target backend: postgres, qdrant, minio, redis, mongodb, neo4j, clickhouse, all.
        /// Defaults to the `UDB_DB_OPS_BACKEND` env var, or "postgres" if unset.
        backend: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DevAction {
    Up,
    Down,
    Logs,
    Status,
    Reset,
    Smoke,
}

impl DevAction {
    pub(crate) fn parse(value: Option<&str>) -> Self {
        match value.unwrap_or("up") {
            "up" | "start" => Self::Up,
            "down" | "stop" => Self::Down,
            "logs" => Self::Logs,
            "status" | "ps" => Self::Status,
            "reset" => Self::Reset,
            "smoke" | "test" => Self::Smoke,
            _ => Self::Up,
        }
    }
}

pub(crate) fn parse_args(args: &[String]) -> (Command, String, String, String) {
    let mut offset = 0usize;

    // Parse global flags that apply to specific subcommands.
    let has_flag = |flag: &str| -> bool { args.iter().any(|a| a == flag) };
    let flag_value = |flag: &str| -> Option<String> {
        args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
    };
    let doctor_output_mode = if has_flag("--human") {
        DoctorOutputMode::Human
    } else {
        DoctorOutputMode::Json
    };
    let with_probes = has_flag("--probe");
    // --prior <path> : load a prior CatalogManifest JSON for drift/plan diffs.
    let prior_manifest_path: Option<String> = flag_value("--prior");
    let _ = prior_manifest_path; // used by Drift/Plan handlers via env fallback below

    let command = match args.first().map(|value| value.as_str()) {
        Some("catalog") => {
            offset = 1;
            Command::Catalog
        }
        Some("dsn") => {
            offset = 1;
            Command::Dsn
        }
        Some("sql") => {
            offset = 1;
            Command::Sql
        }
        Some("plan") => {
            offset = 1;
            Command::Plan
        }
        Some("lint") => {
            offset = 1;
            Command::Lint
        }
        Some("drift") => {
            offset = 1;
            Command::Drift
        }
        Some("doctor") => {
            offset = 1;
            Command::Doctor {
                output_mode: doctor_output_mode,
                with_probes,
            }
        }
        Some("health-check") | Some("healthcheck") => {
            offset = 1;
            Command::HealthCheck
        }
        Some("init-project") => {
            offset = 1;
            Command::InitProject
        }
        Some("dev") => {
            offset = 1;
            Command::Dev {
                action: DevAction::parse(args.get(1).map(String::as_str)),
                service: args
                    .get(2)
                    .filter(|value| !value.starts_with("--"))
                    .cloned(),
                confirmed: has_flag("--yes"),
            }
        }
        Some("explain") => {
            offset = 1;
            Command::Explain
        }
        Some("manifest-export") => {
            offset = 1;
            Command::ManifestExport
        }
        Some("policy-lint") => {
            offset = 1;
            Command::PolicyLint
        }
        Some("policy-seed") => {
            offset = 1;
            Command::PolicySeed
        }
        Some("field-mask-preview") => {
            offset = 1;
            Command::FieldMaskPreview
        }
        Some("compat-matrix") => {
            offset = 1;
            Command::CompatMatrix
        }
        Some("sync-migrations") => {
            offset = 1;
            Command::SyncMigrations {
                force_bootstrap: has_flag("--force-bootstrap"),
                backend: flag_value("--backend"),
            }
        }
        Some("serve") => {
            offset = 1;
            Command::Serve
        }
        Some("admin") if args.get(1).map(|value| value.as_str()) == Some("force-sync") => {
            offset = 2;
            Command::AdminForceSync
        }
        Some("admin") if args.get(1).map(|value| value.as_str()) == Some("release-lock") => {
            offset = 2;
            Command::AdminReleaseLock
        }
        Some("admin")
            if matches!(
                args.get(1).map(|value| value.as_str()),
                Some("verify-audit" | "verify-audit-log")
            ) =>
        {
            offset = 2;
            Command::AdminVerifyAudit {
                limit: flag_value("--limit")
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(0),
            }
        }
        // GAP 8: `udb-proto-parser admin dry-run` — generate SQL plan, exit without applying.
        Some("admin") if args.get(1).map(|value| value.as_str()) == Some("dry-run") => {
            offset = 2;
            Command::AdminDryRun
        }
        // `udb-proto-parser admin reset-db [--yes]`
        // Drops all UDB-managed schemas (discovered from the ledger) and the ledger itself.
        // Requires --yes to prevent accidental data loss.
        Some("admin") if args.get(1).map(|value| value.as_str()) == Some("reset-db") => {
            offset = 2;
            Command::AdminResetDb {
                confirmed: has_flag("--yes"),
            }
        }
        Some("tracker-ddl") => {
            offset = 1;
            Command::TrackerDdl
        }
        Some("system-ddl") => {
            offset = 1;
            Command::SystemDdl
        }
        Some("status-schema") => {
            offset = 1;
            Command::StatusSchema
        }
        Some("fsm-states") => {
            offset = 1;
            Command::FsmStates
        }
        Some("config-skeleton") => {
            offset = 1;
            Command::ConfigSkeleton
        }
        _ => Command::Catalog,
    };
    let positional_args: Vec<String> = args
        .iter()
        .enumerate()
        .skip(offset)
        .filter_map(|(index, arg)| {
            if index > offset
                && matches!(
                    args[index - 1].as_str(),
                    "--prior" | "--backend" | "--config"
                )
            {
                return None;
            }

            if matches!(
                arg.as_str(),
                "--human"
                    | "--probe"
                    | "--force-bootstrap"
                    | "--yes"
                    | "--prior"
                    | "--backend"
                    | "--config"
            ) {
                None
            } else {
                Some(arg.clone())
            }
        })
        .collect();

    let proto_root = positional_args
        .first()
        .cloned()
        .or_else(|| env::var("UDB_PROTO_ROOT").ok())
        .or_else(|| env::var("UDB_PROTO_DIR").ok())
        .unwrap_or_else(|| "proto".to_string());
    let namespace = positional_args
        .get(1)
        .cloned()
        .or_else(|| env::var("UDB_PROTO_NAMESPACE").ok())
        .unwrap_or_default();
    let serve_addr = positional_args
        .get(2)
        .cloned()
        .or_else(|| env::var("UDB_GRPC_ADDR").ok())
        .unwrap_or_else(|| "0.0.0.0:50051".to_string());
    (command, proto_root, namespace, serve_addr)
}
