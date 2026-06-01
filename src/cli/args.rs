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
    /// Schemas are discovered from the live catalog (`pg_namespace`): every
    /// non-system schema (excluding `public`, `information_schema`, and `pg_*`)
    /// is dropped, then the UDB ledger tables in `public` are removed. This is
    /// authoritative even when the UDB ledger was already dropped, and assumes
    /// the single-DB-per-UDB ownership model (every non-system schema is UDB's).
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
    /// Native auth control-plane CLI over generated authn/authz/apikey RPCs.
    Auth(AuthCommand),
}

pub(crate) enum AuthCommand {
    PrincipalList {
        tenant_id: String,
    },
    IdentityLink {
        user_id: String,
        provider_id: String,
        subject: String,
    },
    SessionRevoke {
        session_id: String,
        principal_id: String,
        all_for_principal: bool,
    },
    ApiKeyCreate {
        owner_id: String,
        name: String,
        scopes: Vec<String>,
    },
    RoleBind {
        user_id: String,
        role_id: String,
        domain: String,
        assigned_by: String,
    },
    RelationPut {
        subject: String,
        relation: String,
        object: String,
        tenant: String,
        project: String,
    },
    PolicyPut {
        subject: String,
        role: String,
        action: String,
        resource: String,
        effect: String,
        tenant: String,
        project: String,
    },
    PolicyLint,
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

/// Parse the `auth …` subcommand grammar.
///
/// Returns the parsed [`AuthCommand`] together with the number of leading
/// positional tokens consumed (always 3: `auth <noun> <verb>`), so the caller
/// can advance its `offset` exactly as the inline match used to. Returns `None`
/// for any unrecognized `auth …` shape, letting the caller fall through to the
/// default command.
fn parse_auth_subcommand(args: &[String]) -> Option<(AuthCommand, usize)> {
    let has_flag = |flag: &str| -> bool { args.iter().any(|a| a == flag) };
    let flag_value = |flag: &str| -> Option<String> {
        args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
    };
    let flag_values = |flag: &str| -> Vec<String> {
        args.windows(2)
            .filter(|w| w[0] == flag)
            .map(|w| w[1].clone())
            .collect()
    };

    let noun = args.get(1).map(|value| value.as_str());
    let verb = args.get(2).map(|value| value.as_str());
    let command = match (noun, verb) {
        (Some("principal"), Some("list")) => AuthCommand::PrincipalList {
            tenant_id: flag_value("--tenant").unwrap_or_default(),
        },
        (Some("identity"), Some("link")) => AuthCommand::IdentityLink {
            user_id: flag_value("--user").unwrap_or_default(),
            provider_id: flag_value("--provider").unwrap_or_default(),
            subject: flag_value("--subject").unwrap_or_default(),
        },
        (Some("session"), Some("revoke")) => AuthCommand::SessionRevoke {
            session_id: flag_value("--session").unwrap_or_default(),
            principal_id: flag_value("--principal").unwrap_or_default(),
            all_for_principal: has_flag("--all-for-principal"),
        },
        (Some("api-key"), Some("create")) => AuthCommand::ApiKeyCreate {
            owner_id: flag_value("--owner").unwrap_or_default(),
            name: flag_value("--name").unwrap_or_default(),
            scopes: flag_values("--scope"),
        },
        (Some("role"), Some("bind")) => AuthCommand::RoleBind {
            user_id: flag_value("--user").unwrap_or_default(),
            role_id: flag_value("--role").unwrap_or_default(),
            domain: flag_value("--domain").unwrap_or_default(),
            assigned_by: flag_value("--by").unwrap_or_default(),
        },
        (Some("relation"), Some("put")) => AuthCommand::RelationPut {
            subject: flag_value("--subject").unwrap_or_default(),
            relation: flag_value("--relation").unwrap_or_default(),
            object: flag_value("--object").unwrap_or_default(),
            tenant: flag_value("--tenant").unwrap_or_default(),
            project: flag_value("--project").unwrap_or_default(),
        },
        (Some("policy"), Some("put")) => AuthCommand::PolicyPut {
            subject: flag_value("--subject").unwrap_or_default(),
            role: flag_value("--role").unwrap_or_default(),
            action: flag_value("--action").unwrap_or_default(),
            resource: flag_value("--resource").unwrap_or_default(),
            effect: flag_value("--effect").unwrap_or_else(|| "ALLOW".to_string()),
            tenant: flag_value("--tenant").unwrap_or_default(),
            project: flag_value("--project").unwrap_or_default(),
        },
        (Some("policy"), Some("lint")) => AuthCommand::PolicyLint,
        _ => return None,
    };
    Some((command, 3))
}

pub(crate) fn parse_args(args: &[String]) -> (Command, String, String, String) {
    let mut offset = 0usize;

    // Parse global flags that apply to specific subcommands.
    let has_flag = |flag: &str| -> bool { args.iter().any(|a| a == flag) };
    let flag_value = |flag: &str| -> Option<String> {
        args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
    };
    let flag_values = |flag: &str| -> Vec<String> {
        args.windows(2)
            .filter(|w| w[0] == flag)
            .map(|w| w[1].clone())
            .collect()
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
        // The 8-variant `auth …` subcommand grammar lives in
        // `parse_auth_subcommand`. The guard ensures an unrecognized `auth …`
        // shape falls through to `_ => Command::Catalog` exactly as before.
        Some("auth") if parse_auth_subcommand(args).is_some() => {
            let (auth_command, consumed) =
                parse_auth_subcommand(args).expect("guard guarantees Some");
            offset = consumed;
            Command::Auth(auth_command)
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
                    "--prior"
                        | "--backend"
                        | "--config"
                        | "--tenant"
                        | "--project"
                        | "--user"
                        | "--provider"
                        | "--subject"
                        | "--session"
                        | "--principal"
                        | "--owner"
                        | "--name"
                        | "--scope"
                        | "--role"
                        | "--domain"
                        | "--by"
                        | "--relation"
                        | "--object"
                        | "--action"
                        | "--resource"
                        | "--effect"
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
                    | "--all-for-principal"
                    | "--prior"
                    | "--backend"
                    | "--tenant"
                    | "--project"
                    | "--user"
                    | "--provider"
                    | "--subject"
                    | "--session"
                    | "--principal"
                    | "--owner"
                    | "--name"
                    | "--scope"
                    | "--role"
                    | "--domain"
                    | "--by"
                    | "--relation"
                    | "--object"
                    | "--action"
                    | "--resource"
                    | "--effect"
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
        .or_else(|| env::var("UDB_GRPC_BIND_ADDR").ok())
        .or_else(|| env::var("UDB_GRPC_ADDR").ok())
        .unwrap_or_else(|| DEFAULT_GRPC_BIND_ADDR.to_string());
    (command, proto_root, namespace, serve_addr)
}
