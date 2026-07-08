//! main.rs split — args (Phase H).
use super::*;

pub(crate) enum Command {
    InvalidUsage {
        message: String,
    },
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
        /// `--enterprise`: also run the enterprise prerequisite preflight
        /// (encryption key, auth secrets, sessions, auth-plane exposure, redis,
        /// authz default-deny) — UDB_FRICTION §2.
        enterprise: bool,
        /// `--fix`: APPLY the auto-fixable remediations doctor already emits, but
        /// ONLY to local files (e.g. append a missing `UDB_*` with a documented
        /// safe default to `.env`, normalize CRLF line endings). NEVER touches
        /// remote/DB/network state and never applies a finding whose correct edit
        /// is ambiguous or would loosen posture — those stay advisory-only.
        fix: bool,
    },
    /// Emit the resolved runtime backend contract for the current project's
    /// manifest (required backends, env vars, current status) so application
    /// teams can satisfy it BEFORE the first startup attempt.
    Requirements {
        json: bool,
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
    /// Project-aware UDB scaffold planner/executor.
    Init(InitArgs),
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
    /// Lint a set of ABAC policies loaded from UDB_ABAC_POLICY_FILE (JSON array).
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
    /// Authz governance CLI over the generated AuthzService RPCs (policy
    /// simulation / "what would change if this bundle shipped").
    Authz(AuthzCommand),
    /// Compliance evidence automation (master-plan 4.4). Drains the durable
    /// audit window into a chain-hashed evidence bundle written through the
    /// storage-service object helpers and emits a machine-readable manifest.
    Compliance(ComplianceCommand),
    /// Export UDB's embedded proto contract (annotations + broker/service protos)
    /// into a local proto tree, so a project can `import "udb/core/common/v1/
    /// db.proto"` without cloning the repo or depending on a registry. The files
    /// are compiled into the binary, so they always match the running version.
    ProtoExport {
        /// Destination proto root (files land under `<dir>/udb/core/…`).
        /// Defaults to an existing `proto/` dir, or creates `proto/`.
        out_dir: String,
        /// Create-or-merge `buf.yaml` at the project root. On by default;
        /// pass `--no-buf-yaml` to skip touching buf.yaml.
        manage_buf_yaml: bool,
        /// Run UDB proto formatting after export so long field annotations stay
        /// on one physical line.
        format_proto: bool,
    },
    /// Format exported proto files so long UDB field annotations stay on one
    /// physical line. This is intentionally narrower than `buf format`.
    ProtoFmt {
        /// Proto file or directory to format. Defaults to `proto`.
        root: String,
        /// Do not write files; exit 2 when formatting would change output.
        check: bool,
    },
    /// Generate the per-language SDK client/robustness layer from the embedded
    /// proto RPC manifest (descriptor set = source of truth) and the editable
    /// `sdk-templates/<lang>/` templates. `manifest` dumps the RPC surface as
    /// JSON; `list-langs` prints which template dirs are available.
    Sdk {
        action: SdkAction,
        /// Language to generate for, or `all`. Defaults to `all`.
        lang: String,
        /// Template root. Defaults to `sdk-templates`.
        templates_dir: String,
        /// SDK output root. Defaults to `sdk`.
        out_dir: String,
        /// RPC-manifest selectors applied before rendering (`generate` only).
        selector: SdkSelector,
    },
    /// ORM ergonomics (master-plan 10.5): generate typed entity/repository
    /// models from the embedded proto descriptor set. This REUSES the SDK
    /// generation machinery (`sdk_gen::run_orm_scaffold` → the same FSM/template
    /// pipeline as `udb sdk generate`) — it is NOT a second generator — and
    /// surfaces the `udb plan` / `udb sync-migrations` → model-generation path.
    Orm {
        action: OrmAction,
        /// Language to generate models for, or `all`. Defaults to `all`.
        lang: String,
        /// Optional entity FQN (`pkg.Message`) or short message name to scope
        /// generation to a single entity. `None` generates every entity.
        entity: Option<String>,
        /// Template root. Defaults to `sdk-templates`.
        templates_dir: String,
        /// Output root. Defaults to `sdk`.
        out_dir: String,
    },
    /// Inspect and lint the descriptor-derived native service contract, and
    /// drive the native-service app lifecycle (add/remove/generate/doctor/smoke).
    Native {
        action: NativeAction,
        /// Service IDs / aliases passed positionally (lifecycle actions).
        services: Vec<String>,
        /// Output directory for the `.udb/` manifest + generated files.
        out_dir: String,
        /// Target SDK language for generated client-factory snippets.
        lang: Option<String>,
        /// Target framework hint for generated snippets.
        framework: String,
        /// Emit machine-readable JSON where supported (e.g. `native list --json`).
        json: bool,
        /// Skip interactive guards / proceed non-interactively.
        confirmed: bool,
        /// Path to the committed contract baseline (`--baseline`) for
        /// `contract-diff` / `contract-baseline`.
        baseline: String,
    },
    /// Scaffold a minimal app integration wiring the `UdbProject` facade for the
    /// selected native services into `--out`.
    AppInit {
        lang: String,
        framework: String,
        services: Vec<String>,
        tenant: String,
        project: String,
        auth: String,
        out_dir: String,
        package_manager: String,
        confirmed: bool,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct InitArgs {
    pub tui: bool,
    pub no_tui: bool,
    pub confirmed: bool,
    pub force: bool,
    pub revert: bool,
    pub config: Option<String>,
    pub json_plan: bool,
    pub dry_run: bool,
    pub profile: Option<String>,
    pub framework: Option<String>,
    pub backends: Vec<String>,
    pub native_services: Vec<String>,
    pub features: Vec<String>,
    pub proto_strategy: Option<String>,
}

/// RPC-manifest selectors for `udb sdk generate`. Default (all `None`/`false`)
/// preserves the historical "render every RPC" behavior exactly.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SdkSelector {
    /// `--surface <public|control_plane|peer>`.
    pub surface: Option<String>,
    /// `--service <id,...>` — comma list of native/logical service IDs.
    pub services: Vec<String>,
    /// `--native-services` — restrict to RPCs that carry a `native_service_id`.
    pub native_only: bool,
    /// `--include-deps` — also pull in services a selected service depends on.
    pub include_deps: bool,
    /// `--strict-server-capabilities` — drop RPCs lacking required backends.
    pub strict_server_capabilities: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SdkAction {
    Generate,
    Manifest,
    ListLangs,
    Init,
}

/// Sub-action of `udb orm`. Currently only `scaffold` (model generation), kept
/// as an enum so future ORM ergonomics verbs slot in without changing the
/// dispatch shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrmAction {
    Scaffold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeAction {
    Manifest,
    List,
    Lint,
    Add,
    Remove,
    Generate,
    Doctor,
    Smoke,
    /// Write the embedded `FileDescriptorSet` bytes to stdout (the contract
    /// baseline that `contract-diff` compares against).
    ContractBaseline,
    /// Diff the live descriptor contract against a committed baseline and exit
    /// non-zero on auth/db/sdk/event-breaking or removed changes.
    ContractDiff,
    /// Emit a Markdown native-service table generated from the descriptor (docs
    /// are descriptor-driven, not hand-maintained).
    Docs,
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
    /// urgent_fix #20: OFFLINE root bootstrap. Creates an initial verified user
    /// directly against the database (constructing the authn service in-process,
    /// bypassing the PEP-fronted internal listener — the same path the live tests
    /// use), so a fresh deployment has a first credential to `Authenticate` with.
    /// This is the only way to mint the first principal without an existing one.
    Bootstrap {
        username: String,
        email: String,
        password: String,
        tenant: String,
        project: String,
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

/// `udb authz …` governance subcommands. Currently a single verb — policy
/// simulation — which calls the server-side `AuthzService.SimulatePolicy` model.
pub(crate) enum AuthzCommand {
    /// `udb authz simulate --bundle <path> [--draft <id>] [--tenant <t>]
    ///   [--project <p>] [--persist] [--policy-version <id>]`
    ///
    /// Loads a candidate policy bundle (policies/bindings/tuples + simulation
    /// `cases`) and/or a stored draft id, asks the server to evaluate every case
    /// against BOTH the active snapshot and the candidate, and renders the
    /// allow/deny diff. The server runs its own policy engine and never mutates
    /// durable state unless `--persist` is given.
    Simulate {
        bundle: String,
        draft_id: String,
        tenant: String,
        project: String,
        persist: bool,
        policy_version_id: String,
    },
}

/// `udb compliance …` subcommands. Currently a single verb — `evidence` —
/// which exports the durable auth-audit window (the relation owned by the
/// auth_service `PostgresAuditLogSink`) into a chain-hashed JSONL evidence
/// bundle, writes it through the storage-service object helpers, and renders a
/// machine-readable manifest.
pub(crate) enum ComplianceCommand {
    /// `udb compliance evidence [--since <rfc3339>] [--until <rfc3339>]
    ///   [--tenant <id>] [--backend <s3|minio>] [--bucket <name>]
    ///   [--prefix <key-prefix>] [--project <id>] [--limit <n>]
    ///   [--out <local-manifest-path>] [--dry-run]`
    Evidence {
        since: String,
        until: String,
        tenant: String,
        backend: String,
        bucket: String,
        prefix: String,
        project: String,
        limit: i64,
        out: String,
        dry_run: bool,
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
    pub(crate) fn parse(value: Option<&str>) -> Result<Self, String> {
        match value.unwrap_or("up") {
            "up" | "start" => Ok(Self::Up),
            "down" | "stop" => Ok(Self::Down),
            "logs" => Ok(Self::Logs),
            "status" | "ps" => Ok(Self::Status),
            "reset" => Ok(Self::Reset),
            "smoke" | "test" => Ok(Self::Smoke),
            other => Err(format!(
                "unknown dev action '{other}'{}; known: {}",
                did_you_mean(other, DEV_ACTIONS),
                DEV_ACTIONS.join(", ")
            )),
        }
    }
}

const KNOWN_COMMANDS: &[&str] = &[
    "catalog",
    "dsn",
    "sql",
    "plan",
    "lint",
    "drift",
    "doctor",
    "health-check",
    "init",
    "init-project",
    "scaffold",
    "dev",
    "explain",
    "manifest-export",
    "policy-lint",
    "policy-seed",
    "field-mask-preview",
    "compat-matrix",
    "sync-migrations",
    "dbops sync",
    "auth",
    "authz simulate",
    "compliance evidence",
    "serve",
    "proto export",
    "proto fmt",
    "sdk",
    "native",
    "app init",
    "admin force-sync",
    "admin release-lock",
    "admin verify-audit",
    "admin dry-run",
    "admin reset-db",
    "tracker-ddl",
    "system-ddl",
    "status-schema",
    "fsm-states",
    "config-skeleton",
];

const DEV_ACTIONS: &[&str] = &[
    "up/start",
    "down/stop",
    "logs",
    "status/ps",
    "reset",
    "smoke/test",
];

const SDK_ACTIONS: &[&str] = &[
    "generate",
    "init/doctor/preflight",
    "manifest",
    "list-langs/languages",
];

const ORM_ACTIONS: &[&str] = &["scaffold"];

const NATIVE_ACTIONS: &[&str] = &[
    "manifest",
    "list",
    "lint",
    "add",
    "remove/rm",
    "generate/gen",
    "doctor",
    "smoke",
    "contract-baseline",
    "contract-diff",
    "docs",
];

fn invalid_usage(message: impl Into<String>) -> Command {
    Command::InvalidUsage {
        message: message.into(),
    }
}

/// Levenshtein edit distance over chars. Inputs are CLI tokens (a few chars),
/// so the simple two-row DP is more than fast enough.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut curr = vec![0usize; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        curr[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b.len()]
}

/// Append a `; did you mean '<token>'?` hint when `input` is a near-miss of a
/// known token. `candidates` are the display forms used in the `known:` list
/// (e.g. `"remove/rm"`, `"proto export"`); they are split on `/` and spaces into
/// atomic tokens so the suggestion is a single word. Only suggests when the best
/// match is within edit distance 1..=2 (so an exact match — the auth fall-through
/// case where `input` already equals a known token — yields no spurious hint).
fn did_you_mean(input: &str, candidates: &[&str]) -> String {
    let best = candidates
        .iter()
        .flat_map(|c| c.split(['/', ' ']))
        .filter(|atom| !atom.is_empty())
        .map(|atom| (atom, edit_distance(input, atom)))
        .filter(|(_, d)| (1..=2).contains(d))
        .min_by_key(|(_, d)| *d)
        .map(|(atom, _)| atom);
    match best {
        Some(atom) => format!("; did you mean '{atom}'?"),
        None => String::new(),
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
        (Some("bootstrap"), Some("user")) => AuthCommand::Bootstrap {
            username: flag_value("--username").unwrap_or_else(|| "admin".to_string()),
            email: flag_value("--email").unwrap_or_else(|| "admin@example.com".to_string()),
            password: flag_value("--password").unwrap_or_default(),
            tenant: flag_value("--tenant").unwrap_or_else(|| "acme".to_string()),
            project: flag_value("--project").unwrap_or_else(|| "default".to_string()),
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

/// Parse the `authz …` subcommand grammar (`authz <verb> [flags]`). Returns the
/// parsed [`AuthzCommand`] and the number of leading positional tokens consumed
/// (always 2: `authz <verb>`). Returns `None` for any unrecognized shape so the
/// caller falls through to InvalidUsage.
fn parse_authz_subcommand(args: &[String]) -> Option<(AuthzCommand, usize)> {
    let has_flag = |flag: &str| -> bool { args.iter().any(|a| a == flag) };
    let flag_value = |flag: &str| -> Option<String> {
        args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
    };
    let command = match args.get(1).map(|value| value.as_str()) {
        Some("simulate") | Some("sim") => AuthzCommand::Simulate {
            bundle: flag_value("--bundle").unwrap_or_default(),
            draft_id: flag_value("--draft").unwrap_or_default(),
            tenant: flag_value("--tenant").unwrap_or_default(),
            project: flag_value("--project").unwrap_or_default(),
            persist: has_flag("--persist"),
            policy_version_id: flag_value("--policy-version").unwrap_or_default(),
        },
        _ => return None,
    };
    Some((command, 2))
}

/// Parse the `compliance …` subcommand grammar (`compliance <verb> [flags]`).
/// Returns the parsed [`ComplianceCommand`] and the number of leading positional
/// tokens consumed (always 2: `compliance <verb>`). Returns `None` for any
/// unrecognized shape so the caller falls through to InvalidUsage.
fn parse_compliance_subcommand(args: &[String]) -> Option<(ComplianceCommand, usize)> {
    let has_flag = |flag: &str| -> bool { args.iter().any(|a| a == flag) };
    let flag_value = |flag: &str| -> Option<String> {
        args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
    };
    let command = match args.get(1).map(|value| value.as_str()) {
        Some("evidence") | Some("export") => ComplianceCommand::Evidence {
            since: flag_value("--since").unwrap_or_default(),
            until: flag_value("--until").unwrap_or_default(),
            tenant: flag_value("--tenant").unwrap_or_default(),
            backend: flag_value("--backend").unwrap_or_default(),
            bucket: flag_value("--bucket").unwrap_or_default(),
            prefix: flag_value("--prefix").unwrap_or_default(),
            project: flag_value("--project").unwrap_or_default(),
            limit: flag_value("--limit")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0),
            out: flag_value("--out").unwrap_or_default(),
            dry_run: has_flag("--dry-run"),
        },
        _ => return None,
    };
    Some((command, 2))
}

/// Split a comma-separated flag value into trimmed, non-empty tokens.
pub(crate) fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())
        .collect()
}

/// Flags that consume the following argv token as their value. The positional
/// collector and `collect_positional_services` use this to skip flag values.
const VALUE_FLAGS: &[&str] = &[
    "--prior",
    "--backend",
    "--out",
    "--lang",
    "--entity",
    "--templates",
    "--config",
    "--tenant",
    "--project",
    "--user",
    "--provider",
    "--subject",
    "--session",
    "--principal",
    "--owner",
    "--name",
    "--scope",
    "--role",
    "--domain",
    "--by",
    "--relation",
    "--object",
    "--action",
    "--resource",
    "--effect",
    "--root",
    "--limit",
    "--surface",
    "--service",
    "--services",
    "--framework",
    "--auth",
    "--package-manager",
    "--baseline",
    "--profile",
    "--native-service",
    "--feature",
    "--proto-strategy",
    "--bundle",
    "--draft",
    "--policy-version",
    "--since",
    "--until",
    "--bucket",
    "--prefix",
];

/// Collect positional (non-flag) tokens after `start`, skipping any token that
/// is itself a flag or the value consumed by a value-taking flag. Used by the
/// native lifecycle grammar (`udb native add authn authz ...`).
fn collect_positional_services(args: &[String], start: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = start;
    while i < args.len() {
        let arg = args[i].as_str();
        if VALUE_FLAGS.contains(&arg) {
            i += 2; // skip flag and its value
            continue;
        }
        if arg.starts_with("--") {
            i += 1;
            continue;
        }
        out.push(args[i].clone());
        i += 1;
    }
    out
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
    let doctor_enterprise = has_flag("--enterprise");
    let doctor_fix = has_flag("--fix");
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
                enterprise: doctor_enterprise,
                fix: doctor_fix,
            }
        }
        Some("requirements") | Some("requires") => {
            offset = 1;
            Command::Requirements {
                json: has_flag("--json"),
            }
        }
        Some("health-check") | Some("healthcheck") => {
            offset = 1;
            Command::HealthCheck
        }
        Some("init") => {
            offset = 1;
            Command::Init(InitArgs {
                tui: has_flag("--tui"),
                no_tui: has_flag("--no-tui"),
                confirmed: has_flag("--yes"),
                force: has_flag("--force"),
                revert: has_flag("--revert"),
                config: flag_value("--config"),
                json_plan: has_flag("--json-plan"),
                dry_run: has_flag("--dry-run"),
                profile: flag_value("--profile"),
                framework: flag_value("--framework"),
                backends: flag_values("--backend")
                    .into_iter()
                    .flat_map(|value| split_csv(&value))
                    .collect(),
                native_services: flag_values("--native-service")
                    .into_iter()
                    .chain(flag_values("--services"))
                    .flat_map(|value| split_csv(&value))
                    .collect(),
                features: flag_values("--feature")
                    .into_iter()
                    .flat_map(|value| split_csv(&value))
                    .collect(),
                proto_strategy: flag_value("--proto-strategy"),
            })
        }
        // `scaffold` is the canonical name for the six-language example emitter
        // (used by the scaffold-compiles CI gate); `init-project` is the legacy
        // alias. Both dispatch to the same scaffold_files() emitter.
        Some("init-project") | Some("scaffold") => {
            offset = 1;
            Command::InitProject
        }
        Some("dev") => {
            offset = 1;
            match DevAction::parse(
                args.get(1)
                    .map(String::as_str)
                    .filter(|value| !value.starts_with("--")),
            ) {
                Ok(action) => Command::Dev {
                    action,
                    service: args
                        .get(2)
                        .filter(|value| !value.starts_with("--"))
                        .cloned(),
                    confirmed: has_flag("--yes"),
                },
                Err(message) => invalid_usage(message),
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
        Some("dbops") if args.get(1).map(|value| value.as_str()) == Some("sync") => {
            offset = 2;
            Command::SyncMigrations {
                force_bootstrap: has_flag("--force-bootstrap"),
                backend: flag_value("--backend"),
            }
        }
        // The 8-variant `auth …` subcommand grammar lives in
        // `parse_auth_subcommand`. Unrecognized `auth …` shapes now fall
        // through to InvalidUsage instead of silently running catalog.
        Some("auth") if parse_auth_subcommand(args).is_some() => {
            let (auth_command, consumed) =
                parse_auth_subcommand(args).expect("guard guarantees Some");
            offset = consumed;
            Command::Auth(auth_command)
        }
        // `udb authz simulate --bundle <path> …` — policy governance simulation.
        // Unrecognized `authz …` shapes fall through to InvalidUsage.
        Some("authz") if parse_authz_subcommand(args).is_some() => {
            let (authz_command, consumed) =
                parse_authz_subcommand(args).expect("guard guarantees Some");
            offset = consumed;
            Command::Authz(authz_command)
        }
        // `udb compliance evidence [--since … --until … --bucket … --prefix …]`
        // — master-plan 4.4 compliance-evidence export. Unrecognized
        // `compliance …` shapes fall through to InvalidUsage.
        Some("compliance") if parse_compliance_subcommand(args).is_some() => {
            let (compliance_command, consumed) =
                parse_compliance_subcommand(args).expect("guard guarantees Some");
            offset = consumed;
            Command::Compliance(compliance_command)
        }
        Some("serve") => {
            offset = 1;
            Command::Serve
        }
        // `udb proto export [--out <dir>] [--buf-yaml] [--fmt]`
        Some("proto") if args.get(1).map(|value| value.as_str()) == Some("export") => {
            offset = 2;
            Command::ProtoExport {
                out_dir: flag_value("--out").unwrap_or_default(),
                manage_buf_yaml: !has_flag("--no-buf-yaml"),
                format_proto: has_flag("--fmt") || has_flag("--format"),
            }
        }
        // `udb proto fmt [<dir>] [--check]`
        Some("proto") if args.get(1).map(|value| value.as_str()) == Some("fmt") => {
            offset = 2;
            Command::ProtoFmt {
                root: flag_value("--out")
                    .or_else(|| flag_value("--root"))
                    .or_else(|| {
                        args.get(2)
                            .filter(|value| !value.starts_with("--"))
                            .cloned()
                    })
                    .unwrap_or_default(),
                check: has_flag("--check"),
            }
        }
        // `udb sdk <init|generate|manifest|list-langs> [--lang <name>] [--templates <dir>] [--out <dir>]
        //    [--surface <s>] [--service <id,...>] [--native-services] [--include-deps]
        //    [--strict-server-capabilities]`
        Some("sdk") => {
            offset = 2;
            let action = match args.get(1).map(|value| value.as_str()) {
                None => SdkAction::Generate,
                Some(value) if value.starts_with("--") => SdkAction::Generate,
                Some("generate") | Some("gen") => SdkAction::Generate,
                Some("init") | Some("doctor") | Some("preflight") => SdkAction::Init,
                Some("manifest") => SdkAction::Manifest,
                Some("list-langs") | Some("languages") => SdkAction::ListLangs,
                Some(other) => {
                    return (
                        invalid_usage(format!(
                            "unknown sdk action '{other}'{}; known: {}",
                            did_you_mean(other, SDK_ACTIONS),
                            SDK_ACTIONS.join(", ")
                        )),
                        "proto".to_string(),
                        String::new(),
                        DEFAULT_GRPC_BIND_ADDR.to_string(),
                    );
                }
            };
            let services: Vec<String> = flag_value("--service")
                .map(|value| split_csv(&value))
                .unwrap_or_default();
            Command::Sdk {
                action,
                lang: flag_value("--lang").unwrap_or_else(|| "all".to_string()),
                templates_dir: flag_value("--templates")
                    .unwrap_or_else(|| "sdk-templates".to_string()),
                out_dir: flag_value("--out").unwrap_or_else(|| "sdk".to_string()),
                selector: SdkSelector {
                    surface: flag_value("--surface"),
                    services,
                    native_only: has_flag("--native-services"),
                    include_deps: has_flag("--include-deps"),
                    strict_server_capabilities: has_flag("--strict-server-capabilities"),
                },
            }
        }
        // `udb orm scaffold [--lang <name>|all] [--entity <pkg.Message>]
        //    [--templates <dir>] [--out <dir>]` — master-plan 10.5. Reuses the
        // SDK generation machinery to emit typed entity/repository models.
        Some("orm") => {
            offset = 2;
            let action = match args.get(1).map(|value| value.as_str()) {
                None => OrmAction::Scaffold,
                Some(value) if value.starts_with("--") => OrmAction::Scaffold,
                Some("scaffold") | Some("models") | Some("generate") => OrmAction::Scaffold,
                Some(other) => {
                    return (
                        invalid_usage(format!(
                            "unknown orm action '{other}'{}; known: {}",
                            did_you_mean(other, ORM_ACTIONS),
                            ORM_ACTIONS.join(", ")
                        )),
                        "proto".to_string(),
                        String::new(),
                        DEFAULT_GRPC_BIND_ADDR.to_string(),
                    );
                }
            };
            Command::Orm {
                action,
                lang: flag_value("--lang").unwrap_or_else(|| "all".to_string()),
                entity: flag_value("--entity"),
                templates_dir: flag_value("--templates")
                    .unwrap_or_else(|| "sdk-templates".to_string()),
                out_dir: flag_value("--out").unwrap_or_else(|| "sdk".to_string()),
            }
        }
        // `udb native <manifest|list|lint|add|remove|generate|doctor|smoke> [services...]
        //    [--out <dir>] [--lang <l>] [--framework <f>] [--json] [--yes]`
        Some("native") => {
            offset = 2;
            let action = match args.get(1).map(|value| value.as_str()) {
                None => NativeAction::Manifest,
                Some(value) if value.starts_with("--") => NativeAction::Manifest,
                Some("manifest") => NativeAction::Manifest,
                Some("list") => NativeAction::List,
                Some("lint") => NativeAction::Lint,
                Some("add") => NativeAction::Add,
                Some("remove") | Some("rm") => NativeAction::Remove,
                Some("generate") | Some("gen") => NativeAction::Generate,
                Some("doctor") => NativeAction::Doctor,
                Some("smoke") => NativeAction::Smoke,
                Some("contract-baseline") => NativeAction::ContractBaseline,
                Some("contract-diff") => NativeAction::ContractDiff,
                Some("docs") => NativeAction::Docs,
                Some(other) => {
                    return (
                        invalid_usage(format!(
                            "unknown native action '{other}'{}; known: {}",
                            did_you_mean(other, NATIVE_ACTIONS),
                            NATIVE_ACTIONS.join(", ")
                        )),
                        "proto".to_string(),
                        String::new(),
                        DEFAULT_GRPC_BIND_ADDR.to_string(),
                    );
                }
            };
            let services = collect_positional_services(args, 2);
            Command::Native {
                action,
                services,
                out_dir: flag_value("--out").unwrap_or_else(|| ".".to_string()),
                lang: flag_value("--lang"),
                framework: flag_value("--framework").unwrap_or_default(),
                json: has_flag("--json"),
                confirmed: has_flag("--yes"),
                baseline: flag_value("--baseline").unwrap_or_default(),
            }
        }
        // `udb app init [--lang --framework --services --auth --tenant --project
        //    --out --package-manager --yes]`
        Some("app") if args.get(1).map(|value| value.as_str()) == Some("init") => {
            offset = 2;
            let services: Vec<String> = flag_value("--services")
                .map(|value| split_csv(&value))
                .unwrap_or_default();
            Command::AppInit {
                lang: flag_value("--lang").unwrap_or_else(|| "typescript".to_string()),
                framework: flag_value("--framework").unwrap_or_default(),
                services,
                tenant: flag_value("--tenant").unwrap_or_default(),
                project: flag_value("--project").unwrap_or_default(),
                auth: flag_value("--auth").unwrap_or_default(),
                out_dir: flag_value("--out").unwrap_or_else(|| ".".to_string()),
                package_manager: flag_value("--package-manager").unwrap_or_default(),
                confirmed: has_flag("--yes"),
            }
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
        // GAP 8: `udb admin dry-run` — generate SQL plan, exit without applying.
        Some("admin") if args.get(1).map(|value| value.as_str()) == Some("dry-run") => {
            offset = 2;
            Command::AdminDryRun
        }
        // `udb admin reset-db [--yes]`
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
        Some(other) => invalid_usage(format!(
            "unknown command '{other}'{}; known: {}",
            did_you_mean(other, KNOWN_COMMANDS),
            KNOWN_COMMANDS.join(", ")
        )),
        None => Command::Catalog,
    };
    let positional_args: Vec<String> = args
        .iter()
        .enumerate()
        .skip(offset)
        .filter_map(|(index, arg)| {
            // Drop the value token that follows any value-taking flag.
            if index > offset && VALUE_FLAGS.contains(&args[index - 1].as_str()) {
                return None;
            }

            // Drop boolean flags and value-flag names themselves.
            if VALUE_FLAGS.contains(&arg.as_str())
                || matches!(
                    arg.as_str(),
                    "--human"
                        | "--probe"
                        | "--fix"
                        | "--enterprise"
                        | "--force-bootstrap"
                        | "--yes"
                        | "--force"
                        | "--revert"
                        | "--all-for-principal"
                        | "--buf-yaml"
                        | "--no-buf-yaml"
                        | "--fmt"
                        | "--format"
                        | "--check"
                        | "--json"
                        | "--native-services"
                        | "--include-deps"
                        | "--strict-server-capabilities"
                        | "--tui"
                        | "--no-tui"
                        | "--json-plan"
                        | "--dry-run"
                        | "--persist"
                )
            {
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
