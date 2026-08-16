//! CLI discoverability layer (stopgap pending the full clap migration in
//! `CLI_UPGRADE_PLAN.md`). The hand-rolled `parse_args` has no `--help`/`-h`/
//! `--version`; this module answers them from a static registry so `udb` is
//! explorable without reading source. It does NOT parse — it prints and exits
//! before `parse_args` runs, so existing parsing/back-compat is untouched.

use std::process;

/// One command's help entry. `usage`/`details` are empty for commands that only
/// need a one-line summary in the top-level list.
struct CmdHelp {
    /// Invocation token(s), e.g. "serve" or "auth bootstrap user".
    name: &'static str,
    group: &'static str,
    summary: &'static str,
    usage: &'static str,
    details: &'static str,
}

const GROUPS: &[&str] = &[
    "Core",
    "Schema & SQL",
    "Auth & policy",
    "SDK & native",
    "Scaffold",
    "Admin",
    "Diagnostics",
];

const COMMANDS: &[CmdHelp] = &[
    CmdHelp {
        name: "serve",
        group: "Core",
        summary: "Start the broker (syncs schema from protos; control plane on data-port+10).",
        usage: "udb serve [<proto-root>] [<namespace>] [<data-addr>]",
        details: "\
  <proto-root>  proto dir to load (default: ./proto; \"\" keeps the default)
  <namespace>   UDB_PROTO_NAMESPACE filter (\"\" = none; an over-eager filter
                that loads 0 custom schemas is warned at startup)
  <data-addr>   data-plane bind (default 0.0.0.0:50051); auth plane = port+10
  Wait for the \"UDB DataBroker is ready: data=… auth=…\" line before clients.
  Run `udb requirements` and `udb doctor --enterprise` FIRST.
  Example: udb serve proto \"\" 0.0.0.0:50051",
    },
    CmdHelp {
        name: "requirements",
        group: "Core",
        summary: "Print the backend contract this project's manifest declares (run before first start).",
        usage: "udb requirements [--json]",
        details: "\
  Lists each required backend (Postgres/Qdrant/object-store/Redis), its env
  vars, whether it's configured, and fatal-vs-degraded. Exits non-zero if a
  fatal backend is unset.",
    },
    CmdHelp {
        name: "doctor",
        group: "Core",
        summary: "Env + backend readiness; --enterprise adds a manifest-aware prerequisite preflight.",
        usage: "udb doctor [--enterprise] [--probe] [--human]",
        details: "\
  --enterprise  also check encryption/password/session/auth-plane/redis/ABAC
                AND report any required backend your protos declare but you
                haven't configured (the same condition that stops `serve`).
  --probe       actively probe backend connectivity.  --human  text output.
  Exit: 0 clean, 2 warnings, 1 fail.",
    },
    CmdHelp {
        name: "lint",
        group: "Schema & SQL",
        summary: "Lint the catalog built from your annotated protos (CI-safe; nonzero on errors).",
        usage: "udb lint [--human]",
        details: "",
    },
    CmdHelp {
        name: "drift",
        group: "Schema & SQL",
        summary: "Diff the manifest against a prior one; flags destructive/blocked changes.",
        usage: "udb drift [--prior <manifest.json>]",
        details: "",
    },
    CmdHelp {
        name: "sql",
        group: "Schema & SQL",
        summary: "Emit the generated bootstrap SQL artifacts as JSON (no DB touched).",
        usage: "udb sql",
        details: "",
    },
    CmdHelp {
        name: "plan",
        group: "Schema & SQL",
        summary: "Build a migration plan (optionally vs a prior manifest) as JSON.",
        usage: "udb plan [--prior <manifest.json>] [--emit-approval-plan <path>]",
        details: "--emit-approval-plan writes the exact approval-plan file serve accepts \
                  (same canonical change set + operations_hash), so migration.require_approval_plan \
                  can point at it directly — no failed startup needed to discover the hash.",
    },
    CmdHelp {
        name: "catalog",
        group: "Schema & SQL",
        summary: "Dump the parsed proto catalog as JSON.",
        usage: "udb catalog",
        details: "",
    },
    CmdHelp {
        name: "dsn",
        group: "Schema & SQL",
        summary: "Print the unified DSN catalog derived from the protos.",
        usage: "udb dsn",
        details: "",
    },
    CmdHelp {
        name: "manifest-export",
        group: "Schema & SQL",
        summary: "Export the current CatalogManifest to JSON (for CI plan-approval).",
        usage: "udb manifest-export",
        details: "",
    },
    CmdHelp {
        name: "auth migrate-grants",
        group: "Auth & policy",
        summary: "Migrate legacy profile-attribute service grants into the typed service_account_grants table.",
        usage: "udb auth migrate-grants [--dry-run]",
        details: "\
  Needs UDB_PG_DSN. Deterministic: scans ACTIVE service accounts, validates each
  profile grant (admin/wildcard scopes and duplicate identities are REJECTED and
  reported, never partially written), and creates one typed grant per account.
  --dry-run reports without writing. After migration the typed grant is the
  authoritative source for password login, API keys, and mTLS bindings.",
    },
    CmdHelp {
        name: "auth bootstrap user",
        group: "Auth & policy",
        summary: "Mint the FIRST admin OFFLINE (no running broker needed); prints the canonical tenant UUID.",
        usage: "udb auth bootstrap user --username <u> --email <e> --password <p> --tenant <code> --project <p> [--platform-admin]",
        details: "\
  Needs UDB_PG_DSN + UDB_PASSWORD_HASH_SECRET. Defaults: --username admin,
  --tenant acme, --project default. CAPTURE the printed tenant_id (UUID) —
  it, not the human code, goes in the login JWT and tenant-scoped filters.
  --platform-admin is a direct-Postgres, offline-only operator action that binds
  the principal to the reserved active system/global platform role. It is
  rejected for served bootstrap and is intended for separate control identities.
  After this you MUST seed ABAC (default-deny) before any data CRUD works.",
    },
    CmdHelp {
        name: "auth api-key create",
        group: "Auth & policy",
        summary: "Mint an API key (tenant-scoped).",
        usage: "udb auth api-key create --owner <id> --name <n> --scope <s> [--scope <s>…]",
        details: "",
    },
    CmdHelp {
        name: "auth api-key list/revoke",
        group: "Auth & policy",
        summary: "List an owner's API keys or revoke one OFFLINE (UDB-AUTH-009 rotation/reconciliation).",
        usage: "udb auth api-key list --owner <id>  |  udb auth api-key revoke --key <key_prefix>",
        details: "  Needs UDB_PG_DSN (offline, same operator trust model as `auth bootstrap user`).
  list prints every key for the owner (prefix/name/tenant/scopes/revoked) so a
  provisioner can reconcile instead of minting duplicates; revoke deactivates by
  key prefix. Create now rejects a duplicate ACTIVE name for the same owner.",
    },
    CmdHelp {
        name: "auth grant",
        group: "Auth & policy",
        summary: "Manage typed service-account grants through the authenticated native API.",
        usage: "udb auth grant <create|get|list|replace|rotate-identity|revoke> --tenant <tenant-id> [flags]",
        details: "\
  Needs UDB_AUTH_TARGET (or UDB_GRPC_TARGET) and UDB_AUTH_TOKEN.
  create:  --tenant <tenant-id> --user <uuid> --identity <svc-id> --scope <s> [--scope <s>…]
           [--project <p>] [--reason <r>]
  get:     --tenant <tenant-id> --user <uuid>
  list:    --tenant <tenant-id>
  replace: --tenant <tenant-id> --user <uuid> --scope <s> [--scope <s>…]
           --expected-revision <n> [--project <p>] [--reason <r>]
  rotate-identity: --tenant <tenant-id> --user <uuid> --identity <new-id>
           --expected-revision <n> [--reason <r>]
  revoke:  --tenant <tenant-id> --user <uuid> [--reason <r>]
  Wildcard/admin/owner scopes are always rejected. Replace and identity rotation
  bump the revision, invalidating dependent keys and bindings until re-issued.",
    },
    CmdHelp {
        name: "auth cert-binding",
        group: "Auth & policy",
        summary: "Manage mTLS certificate bindings through the authenticated native API.",
        usage: "udb auth cert-binding <create|list|revoke> --tenant <tenant-id> [flags]",
        details: "\
  Needs UDB_AUTH_TARGET (or UDB_GRPC_TARGET) and UDB_AUTH_TOKEN.
  create: --tenant <tenant-id> --user <uuid> --selector-kind <k> --selector-value <v>
          [--scope <s>…] [--not-before-unix <seconds>] [--not-after-unix <seconds>]
          [--reason <r>]
  list:   --tenant <tenant-id>
  revoke: --tenant <tenant-id> --binding <id> [--reason <r>]
  Selector kinds: SPIFFE_URI, DNS_SAN, SUBJECT_CN, FINGERPRINT_SHA256. The
  account must hold an ACTIVE grant; --scope attenuates it (empty = full grant).
  Re-creating a REVOKED selector supersedes the old row in place (same id).",
    },
    CmdHelp {
        name: "auth policy put",
        group: "Auth & policy",
        summary: "Write a control-plane Casbin governance rule (NOT the data-plane ABAC gate).",
        usage: "udb auth policy put --subject <s> --action <a> --resource <r> --effect <ALLOW|DENY> --tenant <t> --project <p>",
        details: "",
    },
    CmdHelp {
        name: "policy-lint",
        group: "Auth & policy",
        summary: "Lint ABAC policy files from UDB_ABAC_POLICY_FILE (nonzero on broken files).",
        usage: "udb policy-lint",
        details: "",
    },
    CmdHelp {
        name: "policy-seed",
        group: "Auth & policy",
        summary: "Generate INSERT SQL to seed ABAC policies into the UDB ABAC table.",
        usage: "udb policy-seed",
        details: "",
    },
    CmdHelp {
        name: "authz seed",
        group: "Auth & policy",
        summary: "Seed the STANDARD data-plane authorization for a project (idempotent, offline).",
        usage: "udb authz seed --tenant <uuid> [--role app_rw] [--entity <fqn> …] [--action <verb> …] [--project <id>] [--dsn <dsn>] [--emit <path>]",
        details: "\
  The straightforward way to stop fighting `PERMISSION_DENIED` on CRUD. Writes one
  role-gated ALLOW policy per (entity, action) into `udb_authz.policy_rules` — the
  table the data plane actually enforces — using the REAL action tokens the broker
  submits (`Select`/`Upsert`/`Delete`/`Update`/`BulkCas`, NOT a `data.*` alias) and
  the canonical tenant UUID. Postgres-direct (needs UDB_PG_DSN/DATABASE_URL or
  --dsn); run it right after `udb auth bootstrap user`. Idempotent (safe to re-run)
  and atomic (all rows in one tx, so an open `UDB_ABAC_DEFAULT_ALLOW` window never
  half-closes).\n\
  Defaults: `--role app_rw`, all data actions, object `*` (the whole catalog).
  `--entity <fqn>` (repeatable) narrows to specific message types; `--action <verb>`
  (repeatable) narrows the verbs; `--emit <path>` also writes the equivalent
  offline policy JSON for version control.\n\
  Then bind principals (users AND service accounts) to the role so the policy
  applies: `udb auth role bind --principal <id> --role <role> --tenant <uuid>`.\n\
  Example: udb authz seed --tenant 00000000-0000-0000-0000-0000000d0001 --role app_rw",
    },
    CmdHelp {
        name: "proto export",
        group: "SDK & native",
        summary: "Vendor UDB's annotation protos so app protos can import udb/core/common/v1/db.proto.",
        usage: "udb proto export --out <dir> [--no-buf-yaml] [--fmt]",
        details: "\
  Sibling verb: `udb proto fmt [<dir>] [--check]` re-wraps long UDB field
  annotations onto one physical line (narrower than `buf format`).",
    },
    CmdHelp {
        name: "sdk generate",
        group: "SDK & native",
        summary: "Generate/refresh a language SDK from the embedded RPC manifest + templates.",
        usage: "udb sdk generate --lang <ts|python|go|java|csharp|php|all> [--out <dir>] [--surface <…>]",
        details: "\
  Sibling verbs: `udb sdk manifest` (dump the RPC surface as JSON),
  `udb sdk list-langs` (available template dirs).",
    },
    CmdHelp {
        name: "native list",
        group: "SDK & native",
        summary: "Inspect the descriptor-derived native-service contract.",
        usage: "udb native <list|manifest|docs|lint|contract-diff|contract-baseline> [--json]",
        details: "",
    },
    CmdHelp {
        name: "init",
        group: "Scaffold",
        summary: "Project-aware scaffold planner/executor.",
        usage: "udb init [--profile <p>] [--backend <b>…] [--native-service <s>…] [--yes] [--dry-run]",
        details: "",
    },
    CmdHelp {
        name: "init-project",
        group: "Scaffold",
        summary: "Scaffold a minimal project (sample proto, config, DDL, docker-compose).",
        usage: "udb init-project",
        details: "",
    },
    CmdHelp {
        name: "app init",
        group: "Scaffold",
        summary: "Scaffold an app integration wiring the UdbProject facade.",
        usage: "udb app init --lang <l> --services <s,…> --tenant <t> --project <p> --out <dir>",
        details: "",
    },
    CmdHelp {
        name: "dev up",
        group: "Scaffold",
        summary: "Start/stop/test the local multi-backend sandbox (from a repo checkout).",
        usage: "udb dev <up|down|smoke> [<service>] [--yes]",
        details: "",
    },
    CmdHelp {
        name: "admin force-sync",
        group: "Admin",
        summary: "Force the startup lifecycle from the CLI and exit with a JSON report.",
        usage: "udb admin force-sync",
        details: "",
    },
    CmdHelp {
        name: "admin release-lock",
        group: "Admin",
        summary: "Terminate the PG session(s) holding the startup advisory lock (clear a stale lock).",
        usage: "udb admin release-lock",
        details: "Run against the DIRECT DSN, not a pooler.",
    },
    CmdHelp {
        name: "admin reset-db",
        group: "Admin",
        summary: "Drop all UDB-managed schemas + ledger tables (DESTRUCTIVE; needs --yes).",
        usage: "udb admin reset-db --yes",
        details: "",
    },
    CmdHelp {
        name: "admin dry-run",
        group: "Admin",
        summary: "Generate the SQL plan and exit WITHOUT applying (safe on production).",
        usage: "udb admin dry-run",
        details: "",
    },
    CmdHelp {
        name: "admin verify-audit",
        group: "Admin",
        summary: "Verify the tamper-evident admin audit-log hash chain.",
        usage: "udb admin verify-audit [--limit <n>]",
        details: "",
    },
    CmdHelp {
        name: "sync-migrations",
        group: "Admin",
        summary: "Sync db_ops/migrations with the current proto AST.",
        usage: "udb sync-migrations [--force-bootstrap] [--backend <b>]",
        details: "",
    },
    CmdHelp {
        name: "compat-matrix",
        group: "Diagnostics",
        summary: "Print the authoritative supported proto-annotation matrix as JSON.",
        usage: "udb compat-matrix",
        details: "",
    },
    CmdHelp {
        name: "explain",
        group: "Diagnostics",
        summary: "Explain the generated DDL/DSN/policies for a message type.",
        usage: "udb explain",
        details: "",
    },
    CmdHelp {
        name: "health-check",
        group: "Diagnostics",
        summary: "Lightweight Docker HEALTHCHECK — exit 0 if healthy.",
        usage: "udb health-check",
        details: "",
    },
    CmdHelp {
        name: "tracker-ddl",
        group: "Diagnostics",
        summary: "Emit the migration-ledger table DDL to stdout.",
        usage: "udb tracker-ddl",
        details: "",
    },
    CmdHelp {
        name: "config-skeleton",
        group: "Diagnostics",
        summary: "Emit a default MigrationOptions config skeleton as JSON.",
        usage: "udb config-skeleton",
        details: "",
    },
];

/// Answer `--help`/`-h`/`help [cmd]`/no-args and `--version`/`-V`, then exit.
/// Returns normally only when the args are a real command to parse.
pub(crate) fn handle_help_or_version(args: &[String]) {
    if args.iter().any(|a| a == "--version" || a == "-V") {
        println!("udb {}", env!("CARGO_PKG_VERSION"));
        process::exit(0);
    }

    let first = args.first().map(String::as_str);

    // `udb help [cmd...]`
    if first == Some("help") {
        let topic = args[1..].join(" ");
        if topic.is_empty() {
            print_top_level();
        } else {
            print_command_help(&topic);
        }
        process::exit(0);
    }

    // bare `udb`, or `udb --help` / `udb -h`
    if args.is_empty() || matches!(first, Some("--help") | Some("-h")) {
        print_top_level();
        process::exit(0);
    }

    // `udb <cmd> [sub...] --help|-h` — show that command's help.
    if args.iter().skip(1).any(|a| a == "--help" || a == "-h") {
        // Match the longest command prefix from the args (so "auth bootstrap
        // user --help" resolves to the 3-token command, "serve --help" to one).
        let non_flag: Vec<&str> = args
            .iter()
            .map(String::as_str)
            .take_while(|a| !a.starts_with('-'))
            .collect();
        let topic = non_flag.join(" ");
        print_command_help(&topic);
        process::exit(0);
    }
}

fn print_top_level() {
    println!(
        "udb {} — proto-driven multi-database broker\n",
        env!("CARGO_PKG_VERSION")
    );
    println!(
        "USAGE:\n  udb <command> [args] [flags]\n  udb help <command>      show a command's flags + example\n  udb <command> --help    same\n  udb --version\n"
    );
    println!("COMMANDS:");
    for group in GROUPS {
        let mut printed_group = false;
        for cmd in COMMANDS.iter().filter(|c| &c.group == group) {
            if !printed_group {
                println!("\n  {group}:");
                printed_group = true;
            }
            println!("    {:<22} {}", cmd.name, cmd.summary);
        }
    }
    println!(
        "\nNew project? The bootstrap runbook (proto → bootstrap admin → seed ABAC →\n\
         login → CRUD) is in docs/enterprise-deployment.md and examples/ts_enterprise.\n\
         Ground truth for RPCs/annotations: `udb sdk manifest`, `udb native list`, `udb compat-matrix`."
    );
}

fn print_command_help(topic: &str) {
    let topic = topic.trim();
    // Exact match, else longest-prefix match (so "auth bootstrap user xyz" still
    // finds "auth bootstrap user"), else a near-name suggestion.
    let found = COMMANDS.iter().find(|c| c.name == topic).or_else(|| {
        COMMANDS
            .iter()
            .filter(|c| topic.starts_with(c.name) || c.name.starts_with(topic))
            .max_by_key(|c| c.name.len())
    });
    match found {
        Some(cmd) => {
            println!("udb {} — {}\n", cmd.name, cmd.summary);
            if !cmd.usage.is_empty() {
                println!("USAGE:\n  {}\n", cmd.usage);
            }
            if !cmd.details.is_empty() {
                println!("{}", cmd.details);
            }
        }
        None => {
            println!("No help for '{topic}'.\n");
            print_top_level();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_command_has_a_known_group() {
        for cmd in COMMANDS {
            assert!(
                GROUPS.contains(&cmd.group),
                "command {} has unlisted group {}",
                cmd.name,
                cmd.group
            );
        }
    }

    #[test]
    fn core_commands_are_documented() {
        for want in [
            "serve",
            "doctor",
            "requirements",
            "auth bootstrap user",
            "sdk generate",
        ] {
            assert!(
                COMMANDS.iter().any(|c| c.name == want),
                "missing help entry for {want}"
            );
        }
    }
}
