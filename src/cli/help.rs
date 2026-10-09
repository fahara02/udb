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

/// Top-level command words, derived from the help registry so the
/// unknown-command suggestion never drifts from what `udb help` documents.
pub(super) fn top_level_commands() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = COMMANDS
        .iter()
        .map(|command| {
            command
                .name
                .split_whitespace()
                .next()
                .unwrap_or(command.name)
        })
        .collect();
    names.sort_unstable();
    names.dedup();
    names
}

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
        name: "catalog bootstrap",
        group: "Core",
        summary: "Give a project an ACTIVE catalog so an upgraded deployment can serve.",
        usage: "udb catalog bootstrap --project <id> [--dsn <dsn>]",
        details: "  From 0.5.9 the data plane refuses any principal whose project has no ACTIVE
  catalog. A deployment created before projects had catalogs has no catalog rows
  at all, so after upgrading, every service authenticating under a named project
  fails its first call. This stages the current manifest for that project and
  activates it in one step.

  Idempotent: if the project already has an ACTIVE catalog it reports that id and
  changes nothing, so it is safe to re-run and safe on a healthy deployment.

  Run once per project that services authenticate under.",
    },
    CmdHelp {
        name: "catalog stage",
        group: "Core",
        summary: "Stage the catalog built from your protos for a project, without activating it.",
        usage: "udb catalog stage --project <id> [--dsn <dsn>]",
        details: "  Adds an entity without rebuilding the broker: build the manifest from the
  protos, stage it, check it, then `udb catalog activate`. Prints the staged
  catalog id as JSON on stdout. Staging the same manifest again replays the
  earlier stage instead of adding a row.",
    },
    CmdHelp {
        name: "catalog activate",
        group: "Core",
        summary: "Make a staged catalog the project's ACTIVE one; running brokers reload it.",
        usage: "udb catalog activate --project <id> --catalog-id <id> [--dsn <dsn>]",
        details: "  Running brokers pick the new catalog up through the catalog reload
  notification, so no restart is needed. Re-running is a no-op.",
    },
    CmdHelp {
        name: "catalog transition",
        group: "Core",
        summary: "Review, apply, stage and activate an exact candidate through authorized broker RPCs.",
        usage: "udb catalog transition <plan|approve|apply|stage|activate|status> --project <id> [--tenant <id>] [--target <broker>]",
        details: "  Set UDB_AUTH_TOKEN (or UDB_BEARER_TOKEN) to an authorized operator bearer.\n\
  --tenant / UDB_TENANT_ID and --target / UDB_GRPC_TARGET identify the serving\n\
  broker. HTTPS requires UDB_TLS_CA_FILE; identity and scopes are verified there.\n\
  Every mutation requires --idempotency-key. Each RPC has a bounded deadline\n\
  (--timeout-secs 1..3600, default 300). Ordinary backward checks stay enabled.\n\n\
  plan: --manifest <exact-manifest.json> --expected-active-catalog-id <id>\n\
        --expected-active-manifest-integrity-sha256 <outer-integrity> --out <new-plan.json>\n\
        Use the stored catalog integrity, not the inner schema checksum.\n\
  approve: --plan <reviewed-plan.json> --out <new-approval.json>\n\
        Review the native operations/fingerprints first. The broker records\n\
        the verified approver and returns an opaque token; stdout omits it.\n\
  apply: --approval <approval.json>\n\
        The broker applies/verifies the actual target and records evidence.\n\
  stage: --manifest <same-manifest.json> --run-id <native-run-id> [--reason <text>]\n\
  activate: --catalog-id <staged-id> --run-id <same-native-run-id> [--reason <text>]\n\
  status: [--run-id <native-run-id>] [--out <new-status.json>]\n\
    Without --run-id, discover the exact durable ACTIVE catalog id and outer manifest integrity.\n\n\
  Files are response caches and review inputs, never canonical authority.\n\
  Output files are new-only (0600 on Unix); do not commit approval tokens.\n\
  Missing/foreign/mismatched/unfinished evidence, a changed ACTIVE base, and\n\
  blocked or destructive changes are refused by the broker. Restart/retry\n\
  uses the same durable run and idempotency keys.",
    },
    CmdHelp {
        name: "catalog status",
        group: "Core",
        summary: "Show the project's ACTIVE catalog (exit 3 when it has none).",
        usage: "udb catalog status --project <id> [--dsn <dsn>]",
        details: "  Prints the active catalog id, version, checksum and entity count as JSON.
  Exit code 3 means the project has no ACTIVE catalog, so every service in it
  is refused until one is bootstrapped or activated.",
    },
    CmdHelp {
        name: "verify",
        group: "Core",
        summary: "Compare the proto manifest against a LIVE database, read-only, before applying anything.",
        usage: "udb verify --live [--dsn <dsn>] [--json]",
        details: "  Runs the SAME comparison `serve` runs during startup verification, but
  read-only and before any DDL. `udb drift --prior` compares protos against a
  prior MANIFEST; this compares them against the actual database, which is the
  class of divergence that otherwise surfaces only at startup — after the
  migration has already been applied.

  Prints each finding with its schema, table and column. Exits 1 when any
  finding exists, so it can gate a deploy.

  Typical use before upgrading a long-lived deployment:
    udb verify --live --dsn \"$UDB_PG_DSN\"",
    },
    CmdHelp {
        name: "env",
        group: "Core",
        summary: "Generate a working .env for a deployment posture, with real generated secrets.",
        usage: "udb env [--profile dev|enterprise] [--out .env] [--dsn <dsn>] [--force]",
        details: "  Writes the variables that actually decide whether a broker boots, each with a
  one-line reason, instead of making you assemble them from the ~270 in
  .env.example. Secrets (UDB_ENCRYPTION_KEY, UDB_APPROVAL_SIGNING_KEY) are
  generated fresh per file from the OS CSPRNG, so a generated file is never a
  shared placeholder.

  --profile enterprise (default) writes production posture: mandatory mTLS and
  UDB_MIGRATE_ENABLED=false, so a restart never migrates a live database.
  --profile dev writes loopback posture with mTLS commented out.

  Prints to stdout unless --out is given, and refuses to overwrite an existing
  file without --force, because replacing a file that holds an encryption key
  can lock you out of encrypted data.

  Then: udb requirements (backends this manifest needs) and udb doctor.",
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
        usage: "udb manifest-export [<proto-root>]",
        details: "  Writes udb_catalog_manifest.json in the working directory by default. Set
  UDB_MANIFEST_EXPORT_PATH to choose the file, or to `-` to stream the JSON to
  stdout (use this in the container image, whose working directory is not
  writable by the runtime user).",
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
        usage: "udb auth grant <create|get|list|replace|rotate-identity|transfer|revoke> --tenant <tenant-id> [flags]",
        details: "\
  Needs UDB_AUTH_TARGET (or UDB_GRPC_TARGET) and UDB_AUTH_TOKEN.
  create:  --tenant <tenant-id> --user <uuid> --identity <svc-id> --scope <s> [--scope <s>…]
           [--project <p>] [--reason <r>]
  get:     --tenant <tenant-id> --user <uuid>
  list:    --tenant <tenant-id>
  replace: --tenant <tenant-id> --user <uuid> --scope <s> [--scope <s>…]
           --expected-revision <n> [--project <p>] [--reason <r>]
  transfer: --tenant <tenant-id> --from <uuid> --to <uuid> --expected-revision <n> [--reason <r>]
           (the only way to move a grant and its service identity to another account)
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
        name: "auth role",
        group: "Auth & policy",
        summary: "Bind users and service accounts to data-plane roles; create and list roles.",
        usage: "udb auth role <bind|unbind|create|list|assignments> [flags]",
        details: "\
  udb auth role bind --principal <id> --role <code> --tenant <uuid> [--project <p>] [--expires-at-unix <n>]
    PutRoleBinding — the binding the data-plane enforcer matches `udb authz seed`
    policies against. <id> is the bare user_id (for a service account, its
    user_id / API-key owner; its service identity also matches). <code> is the
    bare role code (`app_rw`, `svc_billing`), NOT `role:…`. <uuid> is the
    canonical tenant UUID; omit --project to bind across all projects. Takes
    effect within the enforcer snapshot TTL (5s by default).
  udb auth role unbind --principal <id> --role <code> --tenant <uuid> [--project <p>]
    Re-puts the same binding already expired, so the enforcer drops it.
  udb auth role create --code <code> [--name <n>] [--description <d>] [--tenant <uuid>] [--project <p>]
  udb auth role list [--domain <d>] [--all]
  udb auth role assignments --user <id> [--domain <d>]
    Lists AssignRole (`user_roles`) assignments, not `bind --principal` bindings.
  Legacy: udb auth role bind --user <id> --role <role-UUID> (AssignRole; needs a
  Role row from `role create`; tenant/project from UDB_TENANT_ID/UDB_PROJECT_ID).\n\
  All verbs call the native authz API: set UDB_AUTH_TOKEN to an admin bearer
  (an `auth bootstrap user` organization owner is enough). PutRoleBinding is
  refused in governed mode — use the policy draft flow there.\n\
  Example: udb auth role bind --principal 3c1f…-sa --role svc_billing --tenant 00000000-0000-0000-0000-0000000d0001",
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
        name: "check",
        group: "Schema & SQL",
        summary: "One verdict over catalog lint, policy lint and entity/policy coverage.",
        usage: "udb check [--policies <policies.yaml|json>]",
        details: "  Runs the catalog lint (including projection options: unknown keys, and
  payload_fields / fts_columns naming missing, encrypted or PII columns), the
  policy lint and the entity coverage check, prints one JSON report and exits 1
  when any part has an error. Policies come from --policies, else
  UDB_ABAC_POLICY_FILE; without either the policy checks are skipped.",
    },
    CmdHelp {
        name: "gen edge",
        group: "Scaffold",
        summary: "Print the proto for an edge table projected as a graph relationship.",
        usage: "udb gen edge <source table> -[REL{prop:type,...}]-> <target table> [--package <pkg>] [--message <Name>]",
        details: "  Emits a message with a UUID key, the tenant column, a foreign key to each
  endpoint and a graph_store declaring the relationship type (REL) and the
  endpoint fields; the listed properties (string, int64, int32, double, bool,
  timestamp) become relationship properties. Example:
    udb gen edge notes -[RELATED{weight:double}]-> notes --package acme.notes.v1",
    },
    CmdHelp {
        name: "upgrade",
        group: "Admin",
        summary: "List the breaking changes between two versions that touch this project.",
        usage: "udb upgrade --check --from <x.y.z> [--to <x.y.z>] [--repo <dir>] [--dsn <dsn>]",
        details: "  Reads the `Breaking for callers` entries of every release after --from up to
  --to (default: this binary), searches --repo (default .) for each entry's code
  detectors and runs its read-only SQL probes against --dsn (or UDB_PG_DSN).
  Prints each change with where it hits and its fix; exits 1 when any applies.",
    },
    CmdHelp {
        name: "self verify",
        group: "Admin",
        summary: "Check a udb binary against its release's published sha256.",
        usage: "udb self verify [--version <x.y.z>] [--tier full] [--file <path>] [--manifest <manifest.json>]",
        details: "  Downloads manifest.json (checked against manifest.json.sha256) for the version
  (default: this binary's) and compares size and sha256 of --file (default: this
  executable). UDB_RELEASE_BASE_URL points at a mirror; --manifest works offline.",
    },
    CmdHelp {
        name: "self install",
        group: "Admin",
        summary: "Download a release binary and install it only if its sha256 matches.",
        usage: "udb self install --version <x.y.z> [--tier full] [--to <path>]",
        details: "  Picks the asset for this OS/arch and tier from the release manifest, verifies
  size and sha256, then writes it to --to (default ./udb or ./udb.exe) through a
  staging file and rename, so a failed download never replaces a working binary.",
    },
    CmdHelp {
        name: "policy diff",
        group: "Auth & policy",
        summary: "Show how a tenant's live policies differ from a policy file.",
        usage: "udb policy diff -f <policies.yaml|json> --tenant <uuid> [--overlay <file>]… [--dsn <dsn>]",
        details: "  Reads a list of policies (or {tenant, policies}) and compares the whole rule,
  purpose, conditions and scopes included, against the tenant's active rows in
  udb_authz.policy_rules. A policy naming another tenant is refused. Postgres-direct
  (UDB_PG_DSN / DATABASE_URL / --dsn).",
    },
    CmdHelp {
        name: "policy apply",
        group: "Auth & policy",
        summary: "Reconcile a tenant's policies to a policy file (one transaction).",
        usage: "udb policy apply -f <policies.yaml|json> --tenant <uuid> [--overlay <file>]… [--dsn <dsn>] [--by <name>]",
        details: "  Adds the declared rules that are missing and soft-deletes active rules the
  file no longer declares, for that tenant only, then appends an authz revision
  so every replica reloads. Applying the same file twice changes nothing.",
    },
    CmdHelp {
        name: "identity diff",
        group: "Auth & policy",
        summary: "Show how service-account grants differ from an identities file.",
        usage: "udb identity diff -f <identities.yaml> [--tenant <uuid>] [--allow-transfer]",
        details: "  File: {tenant, service_accounts: [{account, identity, project, scopes, reason}]}.
  Plans create / rotate-identity / replace-scopes per account. An identity held
  by another account is refused (UDB_GRANT_OWNED_BY_OTHER) unless
  --allow-transfer. Grants the file does not declare are reported, never revoked.",
    },
    CmdHelp {
        name: "identity apply",
        group: "Auth & policy",
        summary: "Reconcile service-account grants to an identities file.",
        usage: "udb identity apply -f <identities.yaml> [--tenant <uuid>] [--allow-transfer]",
        details: "  Runs the `identity diff` plan through the authn API (UDB_AUTH_TARGET,
  UDB_AUTH_TOKEN). Refuses to change anything if any account is refused.",
    },
    CmdHelp {
        name: "projection status",
        group: "Admin",
        summary: "Check an entity's projections (vector, search, graph, cache) against its rows.",
        usage: "udb projection status <message type> [--project <id>]",
        details: "  Samples every projection target of the entity and reports divergent and
  missing rows per target (ScanProjectionDrift on UDB_GRPC_TARGET; admin bearer
  in UDB_AUTH_TOKEN).",
    },
    CmdHelp {
        name: "projection backfill",
        group: "Admin",
        summary: "Repair an entity's projections: enqueue a task for every missing or divergent row.",
        usage: "udb projection backfill <message type> [--project <id>] [--limit <rows, default 10000>]",
        details: "  Scans up to --limit canonical rows and enqueues projection repair tasks, which
  the projection workers apply. Run `udb projection status` afterwards to confirm.",
    },
    CmdHelp {
        name: "events tail",
        group: "Diagnostics",
        summary: "Print the events on a topic as JSON lines (optionally decoded).",
        usage: "udb events tail <topic pattern> [--since <event id>] [--decode] [--max <n>]",
        details: "  Subscribes with PublishCDC on UDB_GRPC_TARGET (bearer in UDB_AUTH_TOKEN) and
  prints one JSON line per event. --decode unwraps the outbox envelope into the
  domain event; --since resumes after an event id; --max stops after n events.",
    },
    CmdHelp {
        name: "resources list",
        group: "Diagnostics",
        summary: "List the collections, graphs, indexes or buckets a backend holds.",
        usage: "udb resources list --backend <qdrant|neo4j|elasticsearch|s3|…>",
        details: "  Calls ListResources on the data plane; use it to inspect vector collections
  (--backend qdrant) and graphs (--backend neo4j) the projections created.",
    },
    CmdHelp {
        name: "up",
        group: "Admin",
        summary: "Reconcile a project's udb.yaml: policies, service accounts, seed data.",
        usage: "udb up [-f udb.yaml] [--dry-run] [--allow-transfer] [--dsn <dsn>]",
        details: "  udb.yaml: {tenant, project, policies | policies_file, policy_overlays,
  service_accounts | service_accounts_file, seed | seed_file}. Runs the same
  reconcilers as `udb policy apply`, `udb identity apply` and `udb data seed`, in
  that order; --dry-run prints the differences only. Paths are relative to the
  file. Applying the same file twice changes nothing.",
    },
    CmdHelp {
        name: "data seed",
        group: "Admin",
        summary: "Upsert seed rows from a file through the broker (idempotent).",
        usage: "udb data seed -f <seed.yaml|json> [--tenant <uuid>] [--project <id>] [--dry-run]",
        details: "  File: {tenant, project, entities: [{message_type, conflict_fields, records: [...]}]}.
  Each record is an Upsert on the data plane (UDB_GRPC_TARGET, bearer in
  UDB_AUTH_TOKEN, purpose UDB_PURPOSE or \"seed\"), so policies, tenancy and
  events apply exactly as for an application write.",
    },
    CmdHelp {
        name: "authz check",
        group: "Auth & policy",
        summary: "Ask the live policy engine whether a principal may do an action, and why not.",
        usage: "udb authz check --user <id> --object <message type> --action <Select|Upsert|…> [--tenant <uuid>] [--purpose <p>]",
        details: "  Calls AuthzService.CheckAccess against the running broker (UDB_AUTH_TARGET,
  with the bearer in UDB_AUTH_TOKEN) and prints the decision as JSON. A denial
  includes the broker's diagnosis: the closest rule for that action and object
  in the tenant and the attribute it fails on (purpose, project, scopes,
  subject/role, conditions). `udb authz explain` is the same command.",
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
  Defaults: `--role app_rw`, the CRUD verbs (Select/Upsert/Delete/Update/BulkCas),
  object `*` (the whole catalog). `--entity <fqn>` (repeatable) narrows to specific
  message types (or a topic / bucket / collection for PublishCDC, object and
  vector verbs); `--action <verb>` (repeatable) picks other verbs, e.g. PublishCDC,
  VectorSearch, GetObject or a typed store token (cache.get, document.find, ...);
  `--emit <path>` also writes the equivalent offline policy JSON for version
  control (written before seeding; `--emit -` returns it in the output).\n\
  Then bind principals (users AND service accounts) to the role so the policy
  applies: `udb auth role bind --principal <id> --role <role> --tenant <uuid>`.\n\
  Example: udb authz seed --tenant 00000000-0000-0000-0000-0000000d0001 --role app_rw",
    },
    CmdHelp {
        name: "proto export",
        group: "SDK & native",
        summary: "Vendor UDB's annotation protos so app protos can import udb/core/common/v1/db.proto.",
        usage: "udb proto export --out <dir> [--no-buf-yaml] [--yes] [--fmt]",
        details: "\
  Sibling verb: `udb proto fmt [<dir>] [--check]` re-wraps long UDB field
  annotations onto one physical line (narrower than `buf format`).",
    },
    CmdHelp {
        name: "sdk generate",
        group: "SDK & native",
        summary: "Generate/refresh a language SDK from the embedded RPC manifest + templates.",
        usage: "udb sdk generate --lang <ts|python|go|java|csharp|php|all> [--out <dir>] [--surface <…>] [--check]",
        details: "\
  --check renders the requested output and exits 1 if any generated file is
  missing or stale, without changing the output tree. Use the same selectors,
  --templates, --project-proto and --go-package as normal generation.
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
            "env",
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
