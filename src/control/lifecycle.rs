use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

use crate::ast::ProtoSchema;
use crate::control::auto_alter::{LintInput, plan_repairs};
use crate::control::plan_approval::{
    ApprovalConfig, ApprovedPlan, ExportedPlan, current_unix_ms, plan_matches_current_diff,
};
use crate::db_ops_sync::{discover_db_ops_root, resolve_seeders_dir};
use crate::engine::{Engine, FsmState};
use crate::generation::{
    CatalogManifest, DsnGenerationConfig, GeneratedArtifact, LintItem, LintSeverity,
    SqlGenerationConfig, generate_bootstrap_sql, generate_delta_sql, generate_review_delta_sql,
    generate_unified_dsn_catalog,
};
use crate::migration::diff::{ChangeKind, ChangeOperation, ChangeSafety, diff_manifests};
use crate::provisioning::try_build_provisioning_plan;
use crate::runtime::DataBrokerRuntime;
use crate::tracker::all_tracker_ddl_sql_for_schema;
use crate::{lint_catalog, schema_checksum};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StartupLifecycleReport {
    pub run_id: String,
    pub state: String,
    pub completed: bool,
    pub force_sync: bool,
    /// GAP 8: When true, SQL is generated but never executed. The report lists
    /// every artifact that *would* have been applied so operators can preview
    /// migrations before running against production.
    pub dry_run: bool,
    pub applied_sql_artifacts: usize,
    pub verified_tables: usize,
    pub verified_vector_collections: usize,
    pub verified_object_buckets: usize,
    pub steps: Vec<String>,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    /// SQL artifact bodies included when dry_run = true.
    pub dry_run_plan: Vec<String>,
    pub migration_metric_operations: Vec<MigrationMetricOperation>,
    pub pending_migration_files: i64,
    /// UDB-CAT-002: the complete STRUCTURED combined-catalog lint findings
    /// (severity, kind, schema/table/column, source file, description,
    /// remediation suggestion) — populated whenever lint produced any finding
    /// (blocking failure, force-sync bypass, or clean-with-warnings), so the
    /// startup FSM JSON is machine-readable and not only flattened lines.
    /// serde-defaulted for backward compatibility with older report consumers.
    #[serde(default)]
    pub lint_items: Vec<crate::generation::lint::LintItem>,
    #[serde(default)]
    pub lint_error_count: usize,
    #[serde(default)]
    pub lint_warning_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MigrationMetricOperation {
    pub kind: String,
    pub schema: String,
    pub safety: String,
}

impl StartupLifecycleReport {
    fn step(&mut self, state: FsmState, message: impl Into<String>) {
        self.state = state.as_str().to_string();
        self.steps
            .push(format!("{}: {}", state.as_str(), message.into()));
    }
}

fn record_change_metric_operations(
    report: &mut StartupLifecycleReport,
    changes: &[ChangeOperation],
) {
    for change in changes {
        report
            .migration_metric_operations
            .push(MigrationMetricOperation {
                kind: format!("{:?}", change.kind).to_ascii_lowercase(),
                schema: if change.schema.trim().is_empty() {
                    "default".to_string()
                } else {
                    change.schema.clone()
                },
                safety: match change.safety {
                    ChangeSafety::SafeAuto => "auto",
                    ChangeSafety::RequiresReview => "requires_review",
                    ChangeSafety::Blocked => "blocked",
                }
                .to_string(),
            });
    }
}

/// Select the PostgreSQL delta generator for the serving startup lifecycle.
///
/// `approved_via_plan` is set only after the canonical change set passes the
/// configured approval policy. Keeping this decision in one function lets the
/// lifecycle and its regression tests exercise the same authorization boundary.
fn generate_startup_delta(
    manifest: &CatalogManifest,
    changes: &[ChangeOperation],
    approved_via_plan: bool,
    dry_run: bool,
) -> Vec<GeneratedArtifact> {
    let config = SqlGenerationConfig::default();
    if approved_via_plan || dry_run {
        // Dry-run cannot execute artifacts, so it includes review work to make
        // the preview complete. A mutating run reaches this branch only after
        // the exact canonical change set passed the approval gate.
        generate_review_delta_sql(manifest, changes, &config)
    } else {
        generate_delta_sql(manifest, changes, &config)
    }
}

/// Operator assertion that a backend change UDB cannot apply itself has been
/// reconciled by hand, so startup may record the new manifest as current.
pub(crate) const ACK_MANUAL_BACKEND_RECONCILIATION_ENV: &str =
    "UDB_ACK_MANUAL_BACKEND_RECONCILIATION";

/// Whether the operator has acknowledged manual reconciliation of backend
/// changes with no executor. Deliberately opt-in and explicit: it lets the
/// manifest advance past a change UDB did not perform, so it must never be
/// implied by a broader "degraded startup" switch.
fn manual_backend_reconciliation_acknowledged() -> bool {
    matches!(
        std::env::var(ACK_MANUAL_BACKEND_RECONCILIATION_ENV)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

const REVIEW_REQUIRED_SQL_ARTIFACT_MARKER: &str = "UDB:sql_artifact_requires_review=true";

fn sql_artifact_requires_review(content: &str) -> bool {
    content.lines().any(|line| {
        let line = line.trim();
        line.contains(REVIEW_REQUIRED_SQL_ARTIFACT_MARKER) || line.contains("requires_review=true")
    })
}

fn review_required_sql_artifacts(artifacts: &[GeneratedArtifact]) -> Vec<String> {
    artifacts
        .iter()
        .filter(|artifact| sql_artifact_requires_review(&artifact.content))
        .map(|artifact| {
            if artifact.schema.trim().is_empty() || artifact.table.trim().is_empty() {
                artifact.rel_path.clone()
            } else {
                format!(
                    "{} ({}.{}): review-required SQL artifact",
                    artifact.rel_path, artifact.schema, artifact.table
                )
            }
        })
        .collect()
}

fn reject_review_required_sql_artifacts(
    runtime: &DataBrokerRuntime,
    report: &mut StartupLifecycleReport,
    artifacts: &[GeneratedArtifact],
) -> Result<(), String> {
    let held = review_required_sql_artifacts(artifacts);
    if held.is_empty() {
        return Ok(());
    }
    Err(fail(
        runtime,
        report,
        "review_required_sql_artifact",
        format!(
            "{} review-required bootstrap SQL artifact(s) require manual approval and were not applied: {}",
            held.len(),
            held.join("; ")
        ),
    ))
}

#[derive(Debug)]
enum ApprovalPlanFile {
    Sealed(ApprovedPlan),
    Legacy(ExportedPlan),
}

fn parse_approval_plan_file(
    raw: &str,
    require_signed_plan: bool,
) -> Result<ApprovalPlanFile, String> {
    match serde_json::from_str::<ApprovedPlan>(raw) {
        Ok(plan) if !plan.signatures.is_empty() => Ok(ApprovalPlanFile::Sealed(plan)),
        Ok(_) if require_signed_plan => Err(
            "approval policy requires a signed ApprovedPlan, but the file has no signatures"
                .to_string(),
        ),
        Err(err) if require_signed_plan => Err(format!(
            "approval policy requires a signed ApprovedPlan, but the file did not parse as one: {err}"
        )),
        _ => serde_json::from_str::<ExportedPlan>(raw)
            .map(ApprovalPlanFile::Legacy)
            .map_err(|err| format!("approved plan is not valid JSON: {err}")),
    }
}

fn approval_policy_requires_signed_plan(
    config: &ApprovalConfig,
    signing_key_env_set: bool,
) -> bool {
    signing_key_env_set || config.requires_signed_plan()
}

fn require_approved_plan_for_changes(
    runtime: &DataBrokerRuntime,
    report: &mut StartupLifecycleReport,
    manifest: &CatalogManifest,
    changes: &[ChangeOperation],
    gate_reason: &str,
    accepted_message: impl FnOnce(&str) -> String,
) -> Result<(), String> {
    let approval_plan_path = runtime
        .config()
        .migration
        .require_approval_plan
        .trim()
        .to_string();
    if approval_plan_path.is_empty() {
        return Err(fail(
            runtime,
            report,
            gate_reason,
            "review-required migration work needs migration.require_approval_plan before it can run"
                .to_string(),
        ));
    }

    let raw = fs::read_to_string(&approval_plan_path).map_err(|err| {
        fail(
            runtime,
            report,
            "load_approved_plan",
            format!("cannot read approved plan {approval_plan_path}: {err}"),
        )
    })?;
    let approval_config = ApprovalConfig::from_env();
    let require_signed_plan = approval_policy_requires_signed_plan(
        &approval_config,
        std::env::var_os("UDB_APPROVAL_SIGNING_KEY").is_some(),
    );
    let approval_plan = parse_approval_plan_file(&raw, require_signed_plan).map_err(|err| {
        fail(
            runtime,
            report,
            "load_approved_plan",
            format!("approved plan {approval_plan_path} rejected: {err}"),
        )
    })?;
    let verdict = match approval_plan {
        ApprovalPlanFile::Sealed(sealed) => sealed
            .ready_to_apply(&approval_config, manifest, changes, current_unix_ms())
            .map_err(|err| {
                fail(
                    runtime,
                    report,
                    "approval_plan_rejected",
                    format!("sealed approval plan {approval_plan_path} rejected: {err:?}"),
                )
            })?,
        ApprovalPlanFile::Legacy(approved) => {
            if require_signed_plan {
                return Err(fail(
                    runtime,
                    report,
                    "approval_plan_rejected",
                    format!(
                        "approval policy requires signed ApprovedPlan {approval_plan_path}; unsigned ExportedPlan fallback is disabled"
                    ),
                ));
            }
            plan_matches_current_diff(&approved, manifest, changes)
        }
    };
    if !verdict.is_match() {
        // The verdict alone reports only counts/hashes, so an operator cannot
        // derive the plan `serve` will accept. Name the exact operation set serve
        // computed — reproducible with `udb plan --prior <ledger manifest>`, which
        // now shares `canonical_change_set` with this gate.
        let expected: Vec<String> = changes
            .iter()
            .map(|c| {
                format!(
                    "{:?} {:?} {}.{}.{}",
                    c.safety, c.kind, c.schema, c.table, c.column
                )
            })
            .collect();
        tracing::error!(
            target: "udb.migration",
            approval_plan = %approval_plan_path,
            verdict = ?verdict,
            expected_operation_count = expected.len(),
            expected_operations = %expected.join(" | "),
            "approval plan mismatch; regenerate with `udb plan --prior <ledger manifest>` (same canonical change set)"
        );
        let preview = if expected.len() > 30 {
            format!(
                "{}; …({} more)",
                expected[..30].join(" | "),
                expected.len() - 30
            )
        } else {
            expected.join(" | ")
        };
        return Err(fail(
            runtime,
            report,
            "approval_plan_mismatch",
            format!(
                "current diff does not match approved plan {approval_plan_path}: {verdict:?}; \
                 serve computed {} operation(s) [{preview}] — regenerate the plan with \
                 `udb plan --prior <ledger manifest>` (the CLI and serve now share one \
                 canonical change set)",
                expected.len(),
            ),
        ));
    }
    report.step(FsmState::Applying, accepted_message(&approval_plan_path));
    Ok(())
}

/// The ONE canonical migration change set the startup approval gate validates and
/// both apply phases (bootstrap SQL + schema delta) authorize from:
///
/// - `Some(prior)` with a CHANGED proto checksum → the real prior→current delta;
/// - `None` (first bootstrap) → the complete from-empty set;
/// - `Some(prior)` with an UNCHANGED checksum → empty (no migration work — a
///   `force_sync` re-run still applies idempotently under the ledger, but demands
///   no re-approval).
///
/// It delegates to [`crate::migration::plan::canonical_change_set`] — the SAME
/// function `udb plan` (`build_migration_plan`) uses — so the producer and
/// consumer of an approved plan can never disagree on the operation set. Two
/// defects made this necessary: (a) serve previously ran two independent gates
/// that diffed the plan against DIFFERENT change sets (a from-`None` artifact
/// subset vs the full prior→current diff), deadlocking upgrades; and (b) serve's
/// diff covered only `diff_manifests` (Postgres DDL) while `udb plan` also emits
/// `diff_all_backends`, so any backend delta made the producer's plan
/// un-approvable by the consumer (`CountMismatch`).
fn canonical_migration_changes(
    prior_manifest: Option<&CatalogManifest>,
    manifest: &CatalogManifest,
) -> Vec<ChangeOperation> {
    match prior_manifest {
        Some(prior) if prior.checksum_sha256 != manifest.checksum_sha256 => {
            crate::migration::plan::canonical_change_set(Some(prior), manifest)
        }
        None => crate::migration::plan::canonical_change_set(None, manifest),
        _ => Vec::new(),
    }
}

/// Run the startup migration lifecycle and POST the configured migration
/// notification webhook (completed / failed) on the way out — wiring the
/// previously dead `control::notification` surface the legacy Go service used to
/// send. Best-effort: the webhook never affects the lifecycle result (#138).
pub async fn run_startup_lifecycle(
    runtime: &DataBrokerRuntime,
    manifest: &CatalogManifest,
    schemas: &[ProtoSchema],
    force_sync: bool,
    dry_run: bool,
) -> Result<StartupLifecycleReport, String> {
    let result = run_startup_lifecycle_core(runtime, manifest, schemas, force_sync, dry_run).await;
    #[cfg(feature = "http-client")]
    send_lifecycle_webhook(runtime, &result).await;
    result
}

/// Best-effort migration completion webhook: builds a [`NotificationConfig`]
/// from `MigrationOptions.notification_url`/`notification_on` and POSTs the
/// completed/failed payload when the operator opted into that event (#138).
#[cfg(feature = "http-client")]
async fn send_lifecycle_webhook(
    runtime: &DataBrokerRuntime,
    result: &Result<StartupLifecycleReport, String>,
) {
    use crate::control::notification::{NotificationConfig, NotificationEvent};
    let migration = &runtime.config().migration;
    let cfg = NotificationConfig::new(
        migration.notification_url.clone(),
        &migration.notification_on,
    );
    if cfg.url.trim().is_empty() {
        return;
    }
    let payload = match result {
        Ok(report) => {
            if !cfg.wants_event(&NotificationEvent::Completed) {
                return;
            }
            cfg.completed_payload(&report.run_id)
        }
        Err(err) => {
            if !cfg.wants_event(&NotificationEvent::Failed) {
                return;
            }
            // The error may be a serialized report (carries run_id) or a plain
            // message; recover the run_id when possible.
            let run_id = serde_json::from_str::<StartupLifecycleReport>(err)
                .map(|r| r.run_id)
                .unwrap_or_else(|_| "unknown".to_string());
            cfg.failed_payload(&run_id, err, "")
        }
    };
    let client = reqwest::Client::new();
    if let Err(e) = client
        .post(&cfg.url)
        .json(&payload)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    {
        tracing::warn!(url = %cfg.url, error = %e, "migration completion webhook POST failed (best-effort)");
    }
}

/// Served input schemas in UDB SYSTEM namespaces (`udb`, `udb_*`) that
/// CONFLICT with the embedded system catalog: same message identity
/// (`proto_package` + `message_name`) but a different semantic projection
/// (schema, table, sorted `(field_name, field_number, proto_type)`). This is
/// the vendored-`udb proto export` corruption class — the input silently
/// SHADOWS the embedded copy in the merge and later fails as cryptic
/// field-number reuse. Never matched on file paths, so self-hosting the UDB
/// repo's own proto tree passes. A `udb_*` table the embedded catalog does
/// not know at all is deliberately ALLOWED: the repo itself carries
/// auxiliary system-namespace tables outside the embedded core catalog
/// (e.g. the live-SDK harness's `udb_sdk_live.sdk_live_records`), and an
/// additive unknown table cannot shadow anything.
fn stale_system_schema_inputs(schemas: &[ProtoSchema]) -> Vec<String> {
    type Projection = (String, String, Vec<(String, i32, String)>);
    fn projection(schema: &ProtoSchema) -> Projection {
        let mut columns: Vec<(String, i32, String)> = schema
            .columns
            .iter()
            .map(|c| (c.field_name.clone(), c.field_number, c.proto_type.clone()))
            .collect();
        columns.sort();
        (
            schema.schema_name.clone(),
            schema.table_name.clone(),
            columns,
        )
    }
    let native: std::collections::HashMap<(String, String), Projection> =
        crate::runtime::native_catalog::native_schemas()
            .iter()
            .map(|schema| {
                (
                    (schema.proto_package.clone(), schema.message_name.clone()),
                    projection(schema),
                )
            })
            .collect();
    schemas
        .iter()
        .filter(|schema| schema.schema_name == "udb" || schema.schema_name.starts_with("udb_"))
        .filter(|schema| {
            match native.get(&(schema.proto_package.clone(), schema.message_name.clone())) {
                Some(embedded) => *embedded != projection(schema),
                // Unknown identity: additive, cannot shadow an embedded schema —
                // allowed (see doc comment; proven necessary by the repo's own
                // smoke, which serves auxiliary udb_* tables).
                None => false,
            }
        })
        .map(|schema| format!("{}.{}", schema.schema_name, schema.table_name))
        .collect()
}

async fn run_startup_lifecycle_core(
    runtime: &DataBrokerRuntime,
    manifest: &CatalogManifest,
    schemas: &[ProtoSchema],
    force_sync: bool,
    // GAP 8: dry_run=true generates all SQL but does NOT execute it; safe for
    // pre-flight review in production environments.
    dry_run: bool,
) -> Result<StartupLifecycleReport, String> {
    // Native services (auth, …) migrate through this same diff/apply engine:
    // merge their proto-derived schemas into the catalog so their tables are
    // created/altered exactly like user tables. Proto is the single source of
    // truth — there is no separate hand-written DDL path. No-op when native
    // services are disabled or the merge fails.
    // Capture the project (custom) schema count BEFORE the native
    // merge so we can report the custom-vs-native split and warn when a project
    // loaded zero custom schemas (e.g. an over-eager UDB_PROTO_NAMESPACE filter
    // silently produced a UDB-only broker).
    let custom_schema_count = schemas.len();
    // Served input declaring tables in UDB SYSTEM schemas (`udb`, `udb_*`) is
    // legal ONLY when it matches the embedded system catalog (the self-hosting
    // case: serving the UDB repo's own proto tree). A vendored `udb proto
    // export` from an OLDER release silently SHADOWS the embedded copy in the
    // merge and then fails later in cryptic ways (e.g. field-number reuse
    // between an old export's synthetic audit columns and the current system
    // protos). Detect the mismatch at intake and fail with the actual fix.
    let system_schema_tables = stale_system_schema_inputs(schemas);
    let (merged_manifest, merged_schemas) =
        crate::runtime::native_catalog::merge_native(manifest, schemas);
    let manifest: &CatalogManifest = &merged_manifest;
    let schemas: &[ProtoSchema] = &merged_schemas;

    // The progress-log prefix MUST reflect the actual run mode.
    // Only an explicit force-sync may print "udb force-sync:" — a normal serve
    // bootstrap prints "udb migrate:" so operators don't think the destructive
    // command is running.
    let mode_label = if force_sync { "force-sync" } else { "migrate" };

    let native_schema_count = merged_schemas.len().saturating_sub(custom_schema_count);
    tracing::info!(
        custom_schemas = custom_schema_count,
        native_schemas = native_schema_count,
        total_schemas = merged_schemas.len(),
        "UDB manifest loaded: {custom_schema_count} custom schema(s) + {native_schema_count} UDB-native schema(s)"
    );
    if custom_schema_count == 0 {
        tracing::warn!(
            "no custom (project) schemas were loaded — the broker will serve only \
             UDB-native tables. If you expected your project schemas, check the proto \
             root and remove/relax UDB_PROTO_NAMESPACE."
        );
    }

    let mut engine = Engine::new_auto_id();
    let mut report = StartupLifecycleReport {
        run_id: engine.run_id.clone(),
        state: engine.state.as_str().to_string(),
        force_sync,
        dry_run,
        ..StartupLifecycleReport::default()
    };

    if !system_schema_tables.is_empty() {
        let message = format!(
            "served proto input declares {} table(s) in UDB system schemas that do not match \
             this broker's embedded system catalog: {}. The broker embeds its own system \
             catalog — do NOT serve `udb proto export` output (a vendored export from another \
             UDB version shadows the embedded copy and corrupts the catalog). Remove the \
             vendored udb/** protos from the served proto root; the export exists only for \
             imports and codegen.",
            system_schema_tables.len(),
            system_schema_tables
                .iter()
                .take(5)
                .cloned()
                .collect::<Vec<_>>()
                .join(", "),
        );
        runtime.emit_drift_metric("system_schema_in_input");
        report.errors.push(message.clone());
        return Err(report_failure_json(&report, message));
    }

    transition(
        &mut engine,
        &mut report,
        FsmState::Initialising,
        "bootstrapping migration ledger",
    )?;
    if !runtime.postgres_configured() {
        let message = "PostgreSQL is required before UDB can accept gRPC traffic".to_string();
        runtime.emit_drift_metric("postgres_unavailable");
        report
            .warnings
            .extend(runtime.init_report().warnings.iter().cloned());
        report.errors.push(message.clone());
        return Err(report_failure_json(&report, message));
    }
    // GAP 33: Acquire a PostgreSQL advisory lock before any schema modification
    // to prevent concurrent UDB instances racing during startup.
    //
    // We use pg_try_advisory_xact_lock (transaction-level) scoped narrowly:
    // the lock is acquired, the tracker DDL and system catalog bootstrap run
    // within the same transaction, and the transaction is COMMITted before any
    // SQL artifact execution begins.  This is intentional — the advisory lock
    // only needs to protect the ledger bootstrap phase, not the artifact apply
    // phase.  Holding an open xact (with AccessExclusiveLock from ALTER TABLE
    // on schema_migrations) across the apply phase would cause pool connections
    // in apply_sql_artifact to block indefinitely waiting for the row-level
    // update lock on the same table, producing a deadlock on the 2nd run.
    //
    // The session-level variant pg_try_advisory_lock is used here because:
    //   1. We release it explicitly with pg_advisory_unlock after the bootstrap
    //      transaction commits, so the PgBouncer session-leak risk is avoided.
    //   2. A session-level lock held on a dedicated connection is more robust
    //      than an xact-level lock when the surrounding transaction touches
    //      DDL-heavy tables like schema_migrations.
    if dry_run {
        report.step(
            FsmState::Initialising,
            "dry-run mode — skipping ledger and system catalog bootstrap",
        );
    } else if let Some(pool) = runtime.pg_pool_clone() {
        use crate::engine::PG_ADVISORY_LOCK_KEY;
        // UDB_FRICTION §6: the startup lock below is a PostgreSQL *session-level*
        // advisory lock. Over a transaction pooler (PgBouncer / Neon `-pooler`)
        // a "session" is not pinned to one server backend, so a crashed instance
        // can strand the lock on a pooled backend → every subsequent start sees
        // "another UDB instance holds the startup advisory lock" and crash-loops
        // forever. Warn loudly when the DSN looks pooled; point at the direct
        // endpoint + the `udb admin release-lock` recovery command.
        {
            let primary = &runtime.config().primary;
            let dsn = if !primary.direct_dsn.trim().is_empty() {
                primary.direct_dsn.clone()
            } else {
                primary.pooler_dsn.clone()
            };
            if looks_like_pooled_dsn(&dsn, primary.port) {
                let warning = "PostgreSQL DSN looks like a transaction-pooled endpoint \
                     (pgbouncer / Neon `-pooler` / port 6432). The startup advisory \
                     SESSION-lock is unsafe over a transaction pooler: a crashed instance \
                     can strand the lock and cause a permanent startup crash-loop. Point \
                     the broker at a direct/session endpoint, or clear a stale lock with \
                     `udb admin release-lock` (run it against the DIRECT DSN)."
                    .to_string();
                tracing::warn!("{warning}");
                report.warnings.push(warning);
            }
        }
        let mut conn = pool
            .acquire()
            .await
            .map_err(|err| fail(runtime, &mut report, "advisory_lock_conn", err.to_string()))?;
        // When force_sync is active (admin override), retry pg_try_advisory_lock
        // in a tight loop for up to 10 seconds. This handles stale session-level
        // locks left by crashed/orphaned UDB processes whose server-side PgBouncer
        // session is still alive but being recycled:
        //   - pg_advisory_lock (blocking) is not safe with PgBouncer transaction
        //     pooling because SET lock_timeout is transaction-scoped and advisory
        //     locks are session-scoped, leading to unpredictable behaviour.
        //   - A retry loop with pg_try_advisory_lock is portable across all PG
        //     versions and pooling modes.
        //
        // For normal (non-force_sync) startup we keep a single pg_try_advisory_lock
        // so two concurrent service instances fail fast without waiting.
        let lock_acquired: bool = if force_sync {
            // NW-universal: deadline + poll interval are operator-tunable
            // via `UDB_FORCE_SYNC_LOCK_TIMEOUT_SECS` (default 10, range
            // 1-3600) and `UDB_FORCE_SYNC_LOCK_POLL_MS` (default 500,
            // range 50-30_000). Pre-fix these were hardcoded — slow
            // networks or genuinely long-running migrations had no way
            // to extend the wait without recompiling.
            let timeout_secs = force_sync_lock_timeout_secs();
            let poll_ms = force_sync_lock_poll_ms();
            // We distinguish three outcomes from each poll:
            //   Ok(true)  → lock acquired, proceed
            //   Ok(false) → lock held by another session, retry after sleep
            //   Err(_)    → DB-level failure (network, auth, etc.), abort immediately
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
            let mut acquired = false;
            'retry: loop {
                if std::time::Instant::now() >= deadline {
                    break 'retry;
                }
                match sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
                    .bind(PG_ADVISORY_LOCK_KEY)
                    .fetch_one(&mut *conn)
                    .await
                {
                    Ok(true) => {
                        acquired = true;
                        report.step(
                            FsmState::Initialising,
                            format!(
                                "force_sync: acquired advisory lock ({:#x})",
                                PG_ADVISORY_LOCK_KEY
                            ),
                        );
                        break 'retry;
                    }
                    Ok(false) => {
                        // Lock held by another session — wait and retry.
                        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
                    }
                    Err(err) => {
                        // Distinct from "lock held" — this is a real DB error.
                        let message = format!(
                            "force_sync: database error while polling advisory lock ({:#x}): {err}",
                            PG_ADVISORY_LOCK_KEY
                        );
                        report.errors.push(message.clone());
                        return Err(report_failure_json(&report, message));
                    }
                }
            }
            if !acquired {
                let message = format!(
                    "force_sync: timed out ({timeout_secs}s) waiting for advisory lock ({:#x}) — \
                     a UDB instance is actively running. Stop it first, or extend the wait \
                     via UDB_FORCE_SYNC_LOCK_TIMEOUT_SECS.",
                    PG_ADVISORY_LOCK_KEY
                );
                report.errors.push(message.clone());
                return Err(report_failure_json(&report, message));
            }
            acquired
        } else {
            // Non-force_sync: fail fast — don't wait if another instance is running.
            match sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
                .bind(PG_ADVISORY_LOCK_KEY)
                .fetch_one(&mut *conn)
                .await
            {
                Ok(got) => got,
                Err(err) => {
                    let message = format!(
                        "database error acquiring startup advisory lock ({:#x}): {err}",
                        PG_ADVISORY_LOCK_KEY
                    );
                    report.errors.push(message.clone());
                    return Err(report_failure_json(&report, message));
                }
            }
        };
        if !lock_acquired {
            let message = format!(
                "another UDB instance holds the startup advisory lock \
                 (key={:#x}); this instance exits to avoid concurrent schema modification",
                PG_ADVISORY_LOCK_KEY
            );
            report.errors.push(message.clone());
            return Err(report_failure_json(&report, message));
        }
        report.step(
            FsmState::Initialising,
            format!(
                "acquired startup advisory session-lock ({:#x})",
                PG_ADVISORY_LOCK_KEY
            ),
        );
        // Run tracker DDL and system catalog bootstrap while holding the advisory
        // lock. All three operations (DDL, system catalog, unlock) MUST run on
        // the same `conn` that acquired the lock — session-level advisory locks
        // are bound to the backend PID, so using a different pool connection for
        // the unlock produces the PostgreSQL notice:
        //   "you don't own a lock of type ExclusiveLock"
        // execute_raw_sql() pulls a separate pool connection internally, so we
        // execute the DDL directly on `conn` here instead.
        // Execute tracker DDL on the same connection that holds the advisory lock.
        // We split on ";\n" and run each statement individually since sqlx does
        // not support executing multi-statement strings on a PoolConnection directly.
        for stmt in all_tracker_ddl_sql_for_schema(&runtime.config().migration.ledger_schema)
            .split(";\n")
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            if let Err(err) = sqlx::query(stmt).execute(&mut *conn).await {
                let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(PG_ADVISORY_LOCK_KEY)
                    .execute(&mut *conn)
                    .await;
                return Err(fail(runtime, &mut report, "tracker_ddl", err.to_string()));
            }
        }
        // ensure_system_catalog also needs to run on the same connection.
        // Run each system catalog statement individually on `conn`.
        for stmt in crate::runtime::system::system_catalog_statements_public() {
            if let Err(err) = sqlx::query(&stmt).execute(&mut *conn).await {
                let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(PG_ADVISORY_LOCK_KEY)
                    .execute(&mut *conn)
                    .await;
                return Err(fail(
                    runtime,
                    &mut report,
                    "system_catalog_ddl",
                    err.to_string(),
                ));
            }
        }
        // Release the advisory lock on the same connection that holds it.
        // A failed unlock is self-healing (PostgreSQL drops session-level
        // advisory locks when the backend disconnects), but record it so an
        // operator can see it rather than having it vanish silently.
        if let Err(err) = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(PG_ADVISORY_LOCK_KEY)
            .execute(&mut *conn)
            .await
        {
            report.warnings.push(format!(
                "failed to release startup advisory session-lock ({:#x}): {err} \
                 (PostgreSQL will free it on backend disconnect)",
                PG_ADVISORY_LOCK_KEY
            ));
        }
        report.step(
            FsmState::Initialising,
            format!(
                "released startup advisory session-lock ({:#x})",
                PG_ADVISORY_LOCK_KEY
            ),
        );
    }

    transition(
        &mut engine,
        &mut report,
        FsmState::LoadProtoState,
        "loaded proto AST from startup input",
    )?;
    let checksum = schema_checksum(schemas)
        .map_err(|err| fail(runtime, &mut report, "schema_checksum", err.to_string()))?;
    report.step(
        FsmState::LoadProtoState,
        format!("schema checksum {checksum}"),
    );

    transition(
        &mut engine,
        &mut report,
        FsmState::ProtoChecksumLint,
        "validating catalog checksum and annotations",
    )?;
    let lint = lint_catalog(manifest);
    // UDB-CAT-002: retain the structured findings on the report itself (every
    // outcome — blocking, bypassed, or clean-with-warnings) so the FSM JSON
    // carries machine-readable identity + remediation, not only rendered lines.
    if !lint.items.is_empty() {
        report.lint_items = lint.items.clone();
        report.lint_error_count = lint.error_count;
        report.lint_warning_count = lint.warning_count;
    }
    if !lint.passed && !force_sync {
        runtime.emit_drift_metric("catalog_lint_failed");
        // Surface EVERY individual finding (kind, FQN/schema.table, source,
        // description) — not just the aggregate counts. Without this an operator
        // running `udb serve` / `udb admin dry-run` on a merged (embedded +
        // consumer) catalog saw only "N error(s), M warning(s)" and could not
        // identify which schemas blocked startup without patching UDB or forcing
        // a run. Each pushed line is also echoed per-error via tracing by
        // report_failure_json below.
        for item in &lint.items {
            // The flattened string arrays are DERIVED from the structured
            // findings (single source of truth: `report.lint_items`); the
            // structured tracing event carries each identity/remediation field
            // separately so log pipelines need not parse the rendered line.
            let line = item.display_line();
            tracing::error!(
                severity = ?item.severity,
                kind = %item.kind,
                schema = %item.schema,
                table = %item.table,
                column = %item.column,
                source_file = %item.source_file,
                description = %item.description,
                suggestion = %item.suggestion,
                "catalog lint finding"
            );
            match item.severity {
                LintSeverity::Error => report.errors.push(line),
                _ => report.warnings.push(line),
            }
        }
        let detail = lint
            .items
            .iter()
            .filter(|item| item.severity == LintSeverity::Error)
            .map(LintItem::display_line)
            .collect::<Vec<_>>()
            .join("\n");
        let message = format!(
            "catalog lint failed with {} error(s), {} warning(s):\n{detail}",
            lint.error_count, lint.warning_count
        );
        return Err(report_failure_json(&report, message));
    }
    // GAP 34: When force_sync bypasses lint errors, record each bypassed error
    // as a warning in the lifecycle report so there is a forensic audit trail.
    if !lint.passed && force_sync {
        runtime.emit_drift_metric("catalog_lint_bypassed");
        for item in lint
            .items
            .iter()
            .filter(|i| i.severity == LintSeverity::Error)
        {
            // Medium (review): the bypass audit trail is structured too, not
            // only a flattened warning line.
            tracing::warn!(
                severity = ?item.severity,
                kind = %item.kind,
                schema = %item.schema,
                table = %item.table,
                column = %item.column,
                source_file = %item.source_file,
                description = %item.description,
                suggestion = %item.suggestion,
                "catalog lint error BYPASSED by force_sync"
            );
            report
                .warnings
                .push(format!("[force_sync bypass] {}", item.display_line()));
        }
    }
    if lint.warning_count > 0 {
        report.warnings.push(format!(
            "catalog lint emitted {} warning(s)",
            lint.warning_count
        ));
        // Surface each warning individually (not just the count) so a dry-run /
        // serve that lints clean-but-noisy still tells the operator exactly which
        // schemas/tables produced the warnings.
        for item in lint
            .items
            .iter()
            .filter(|item| item.severity == LintSeverity::Warning)
        {
            tracing::warn!(
                severity = ?item.severity,
                kind = %item.kind,
                schema = %item.schema,
                table = %item.table,
                column = %item.column,
                source_file = %item.source_file,
                description = %item.description,
                suggestion = %item.suggestion,
                "catalog lint finding"
            );
            report.warnings.push(item.display_line());
        }
    }

    transition(
        &mut engine,
        &mut report,
        FsmState::PlanProtoDiff,
        "building startup migration plan",
    )?;
    let dsn_catalog = generate_unified_dsn_catalog(schemas, &DsnGenerationConfig::default())
        .map_err(|err| fail(runtime, &mut report, "dsn_catalog", err.to_string()))?;
    let provisioning_plan = try_build_provisioning_plan(manifest, &dsn_catalog.entries)
        .map_err(|err| fail(runtime, &mut report, "provisioning_plan", err))?;
    // Resolve the explicit recovery override once per lifecycle. Its presence
    // disables fast-start so a normal serve can reject misuse fail-closed.
    let replay_prior_checksum = migration_replay_prior_checksum();

    // ── §4 fast start (opt-in: UDB_STARTUP_SKIP_IF_UNCHANGED) ──────────────────
    // When the persisted manifest checksum already equals the current one, skip
    // the ENTIRE expensive generate/apply/provision/verify suite — otherwise the
    // broker re-runs hundreds of idempotent "… already exists, skipping" steps
    // plus live drift introspection on EVERY restart (~2 min over a remote DB).
    // The cheap advisory-lock + tracker-DDL + system-catalog bootstrap above
    // already ran. Reads ONLY the checksum (no multi-MB manifest_json fetch).
    // Overrides always take the full path. Tradeoff (same as skip_unchanged_verify):
    // external schema/store drift is not re-verified — `udb admin force-sync` is
    // the escape hatch.
    if runtime.config().migration.skip_if_unchanged
        && !force_sync
        && !dry_run
        && !runtime.config().migration.force_reseed
        && !runtime.config().migration.emergency_auto_alter
        && replay_prior_checksum.is_none()
    {
        match runtime.load_last_manifest_checksum_if_exists().await {
            Ok(Some(stored)) if stored == manifest.checksum_sha256 => {
                // Drive the FSM through its legal terminal path with skip notes.
                for (state, note) in [
                    (
                        FsmState::GenerateSql,
                        "fast start: schema checksum unchanged — skipping SQL generation",
                    ),
                    (FsmState::ChecksumLint, "fast start: skipping checksum lint"),
                    (
                        FsmState::Applying,
                        "fast start: skipping apply + backend provisioning",
                    ),
                    (
                        FsmState::Verifying,
                        "fast start: skipping live drift verification",
                    ),
                    (
                        FsmState::Completed,
                        "startup lifecycle completed (fast start; manifest checksum unchanged)",
                    ),
                ] {
                    transition(&mut engine, &mut report, state, note)?;
                }
                report.verified_tables = manifest.tables.len();
                report.completed = true;
                tracing::info!(
                    schema_checksum = %stored,
                    "UDB fast start: proto manifest checksum unchanged — skipped \
                     generate/apply/provision/verify (UDB_STARTUP_SKIP_IF_UNCHANGED)"
                );
                return Ok(report);
            }
            // Changed checksum or no prior ledger row → full startup path.
            Ok(_) => {}
            Err(err) => {
                report.warnings.push(format!(
                    "fast-start checksum probe failed ({}); running full startup lifecycle",
                    err.message()
                ));
            }
        }
    }

    // ── Manifest-derived backend preflight (BEFORE SQL apply) ──────────────────
    // The manifest knows which non-Postgres backends it requires (vector
    // collections → Qdrant, object buckets → S3/MinIO). Previously a missing
    // backend was only discovered AFTER the full SQL migration pass (minutes
    // against a remote DB), and surfaced as ambiguous "drift". Check the whole
    // required set up front and fail fast with ONE consolidated error so the
    // operator fixes infra before paying the slow SQL cost.
    {
        let mut missing: Vec<String> = Vec::new();
        for action in &provisioning_plan.actions {
            match action.resource_kind.as_str() {
                "vector_collection" if !runtime.qdrant_configured() => missing.push(format!(
                    "vector collection '{}' requires Qdrant — set UDB_QDRANT_URL (or QDRANT_URL)",
                    action.resource_name
                )),
                "bucket" if !runtime.s3_configured() => missing.push(format!(
                    "object bucket '{}' requires S3/MinIO — set UDB_MINIO_ENDPOINT (or S3_ENDPOINT)",
                    action.resource_name
                )),
                _ => {}
            }
        }
        if !missing.is_empty() {
            let summary = format!(
                "manifest requires {} backend resource(s) that are not configured:\n  - {}",
                missing.len(),
                missing.join("\n  - ")
            );
            if dry_run || allow_degraded_backend_startup(runtime) {
                report.warnings.push(summary);
            } else {
                // category=backend_missing (NOT schema drift).
                return Err(fail(runtime, &mut report, "backend_missing", summary));
            }
        }
    }

    // Load the prior manifest here — before SQL generation — so we can skip the
    // bootstrap apply entirely when the proto checksum is unchanged.  Previously
    // this was loaded a second time after the apply block, which meant every run
    // unconditionally re-executed hundreds of idempotent CREATE TABLE/INDEX
    // statements even when nothing had changed.
    let prior_selection = if dry_run {
        load_prior_manifest_for_dry_run(runtime, manifest, &mut report, replay_prior_checksum)
            .await?
    } else {
        load_prior_manifest_for_apply(
            runtime,
            manifest,
            &mut report,
            force_sync,
            replay_prior_checksum,
        )
        .await?
    };
    let PriorManifestSelection {
        manifest: prior_manifest,
        latest_checksum: expected_latest_checksum,
    } = prior_selection;

    // Name the LEDGER SOURCE whenever a prior manifest is in play. Operators
    // with multiple DSN env vars (UDB_PG_DSN vs DATABASE_URL) have inspected
    // the WRONG database for hours because nothing said which database the
    // prior manifest was read from.
    let ledger_identity = if let Some(prior) = prior_manifest.as_ref() {
        let identity = runtime.manifest_ledger_identity().await;
        report.step(
            FsmState::PlanProtoDiff,
            format!(
                "prior manifest ledger: {identity} checksum={}",
                prior.checksum_sha256
            ),
        );
        identity
    } else {
        String::new()
    };

    // A run whose INPUT contains zero custom schemas while the prior manifest
    // still records custom tables would plan a DropTable for every previously
    // known app table. That is virtually always a misconfiguration (an
    // over-eager namespace filter or a wrong proto root), not an intentional
    // teardown — abort BEFORE planning instead of producing a drop-everything
    // plan. An operator who really means it sets UDB_ALLOW_EMPTY_CUSTOM_INPUT=1.
    if custom_schema_count == 0
        && std::env::var("UDB_ALLOW_EMPTY_CUSTOM_INPUT")
            .ok()
            .as_deref()
            != Some("1")
        && let Some(prior) = prior_manifest.as_ref()
    {
        let prior_custom: Vec<String> = prior
            .tables
            .iter()
            .filter(|t| !(t.schema == "udb" || t.schema.starts_with("udb_")))
            .map(|t| format!("{}.{}", t.schema, t.table))
            .collect();
        if !prior_custom.is_empty() {
            return Err(fail(
                runtime,
                &mut report,
                "empty_custom_input_with_prior_tables",
                format!(
                    "the served proto input contains 0 custom schemas but the prior manifest \
                     ({ledger_identity}) records {} custom table(s) (e.g. {}). Planning would \
                     drop every previously-known app table. Check the proto root and \
                     UDB_PROTO_NAMESPACE; set UDB_ALLOW_EMPTY_CUSTOM_INPUT=1 only if removing \
                     all custom tables is intentional.",
                    prior_custom.len(),
                    prior_custom
                        .iter()
                        .take(5)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", "),
                ),
            ));
        }
    }

    let checksum_unchanged = prior_manifest
        .as_ref()
        .map(|p| p.checksum_sha256 == manifest.checksum_sha256)
        .unwrap_or(false);

    transition(
        &mut engine,
        &mut report,
        FsmState::GenerateSql,
        "generating bootstrap SQL artifacts",
    )?;

    // Backend capability checks are performed inline at each execution point below.
    // Non-Postgres backends (Redis keyspaces, MinIO buckets, Qdrant collections)
    // are handled by their own provisioning arms and silently skipped when inactive.

    transition(
        &mut engine,
        &mut report,
        FsmState::ChecksumLint,
        "SQL artifact checksums accepted",
    )?;

    transition(
        &mut engine,
        &mut report,
        FsmState::Applying,
        "applying SQL and backend provisioning actions",
    )?;

    // ── Single canonical migration approval (one plan, one decision) ──────────
    // Startup previously ran TWO independent approval gates in the same pass: the
    // bootstrap SQL-artifact gate (which diffed the proto catalog against `None`,
    // an artifact-only subset) and the schema-diff gate (the full prior→current
    // diff), both reading the SAME `migration.require_approval_plan` and both
    // demanding EXACT operation counts. Those counts differ, so no single plan
    // could satisfy both and any upgrade carrying both bootstrap artifacts and a
    // non-empty schema diff crash-looped. It was also an atomicity hole: bootstrap
    // SQL and backend provisioning could mutate project state before the full plan
    // ever reached its gate.
    //
    // Compute the canonical change set ONCE (from the prior manifest when one
    // exists, else from empty), reject Blocked ops, and take ONE approval decision
    // HERE — before the pre-migrate hook, bootstrap SQL, provisioning, or delta —
    // then authorize BOTH apply phases from it. The SQL executor's per-artifact
    // checksum ledger still governs what is physically (re)applied, so an
    // already-recorded artifact is skipped rather than re-approved.
    let canonical_changes = canonical_migration_changes(prior_manifest.as_ref(), manifest);
    let mut migration_authorized_via_plan = false;
    if !dry_run && !checksum_unchanged {
        let blocked: Vec<String> = canonical_changes
            .iter()
            .filter(|c| c.safety == ChangeSafety::Blocked)
            .map(|c| {
                format!(
                    "{:?} {}.{}({}) [Blocked: {}]",
                    c.kind, c.schema, c.table, c.column, c.blocked_reason
                )
            })
            .collect();
        if !blocked.is_empty() {
            return Err(fail(
                runtime,
                &mut report,
                "blocked_schema_change",
                format!(
                    "{} migration change(s) are blocked and cannot be applied even with an approved plan: {}; \
                     prior manifest: {ledger_identity} checksum={}",
                    blocked.len(),
                    blocked.join("; "),
                    prior_manifest
                        .as_ref()
                        .map(|p| p.checksum_sha256.as_str())
                        .unwrap_or("<none>"),
                ),
            ));
        }
        let review_count = canonical_changes
            .iter()
            .filter(|c| c.safety == ChangeSafety::RequiresReview)
            .count();
        let approval_plan_configured = !runtime
            .config()
            .migration
            .require_approval_plan
            .trim()
            .is_empty();
        // Validate once when there is review-required work OR a plan is configured
        // for an otherwise auto-safe migration (preserving the explicit-plan gate).
        // Fails closed on a missing plan for review work, or on any count / checksum
        // / operations-hash mismatch, before a single row is mutated.
        if review_count > 0 || approval_plan_configured {
            require_approved_plan_for_changes(
                runtime,
                &mut report,
                manifest,
                &canonical_changes,
                "migration_approval",
                |path| {
                    if review_count > 0 {
                        format!(
                            "approved plan {path} accepted for the canonical migration ({review_count} review-required change(s))"
                        )
                    } else {
                        format!("approved plan {path} accepted (canonical diff matches)")
                    }
                },
            )?;
            migration_authorized_via_plan = true;
        }
    }

    // Skip bootstrap SQL generation and application when the proto checksum
    // matches the last recorded run.  All the CREATE TABLE/INDEX/POLICY
    // statements are idempotent, but sending them to a cloud database (Neon,
    // Supabase, RDS) on every run wastes 5-30 s and produces noisy slow-query
    // warnings.
    //
    // force_sync and force_reseed both bypass the checksum shortcut. force_sync
    // is an operator assertion that live state may need reconciliation even when
    // the proto checksum is unchanged; the schema_migrations ledger's
    // per-artifact applied_set still protects partial-failure recovery.
    // Pre-migrate hook: execute the operator's SQL file before any migration
    // SQL is applied. A failure here aborts the run (fail-closed).
    let pre_hook = runtime
        .config()
        .migration
        .pre_migrate_sql
        .trim()
        .to_string();
    if !pre_hook.is_empty() && !dry_run {
        match fs::read_to_string(&pre_hook) {
            Ok(sql) => {
                runtime
                    .execute_raw_sql(&sql, "pre_migrate_sql hook")
                    .await
                    .map_err(|err| {
                        fail(
                            runtime,
                            &mut report,
                            "pre_migrate_hook",
                            format!("pre-migrate hook {pre_hook} failed: {err}"),
                        )
                    })?;
                report.step(
                    FsmState::Applying,
                    format!("ran pre-migrate hook {pre_hook}"),
                );
            }
            Err(err) => {
                return Err(fail(
                    runtime,
                    &mut report,
                    "pre_migrate_hook",
                    format!("cannot read pre-migrate hook {pre_hook}: {err}"),
                ));
            }
        }
    }

    let force_reseed = runtime.config().migration.force_reseed;
    if checksum_unchanged && !force_reseed && !force_sync {
        report.step(
            FsmState::Applying,
            if dry_run {
                format!(
                    "proto checksum {} unchanged — DRY RUN would skip bootstrap SQL apply (set migration.force_reseed=true or force_sync=true to override)",
                    &manifest.checksum_sha256[..8.min(manifest.checksum_sha256.len())]
                )
            } else {
                format!(
                    "proto checksum {} unchanged — skipping bootstrap SQL apply (set migration.force_reseed=true or force_sync=true to override)",
                    &manifest.checksum_sha256[..8.min(manifest.checksum_sha256.len())]
                )
            },
        );
        report.applied_sql_artifacts = 0;
    } else {
        let sql_artifacts = generate_bootstrap_sql(schemas, &SqlGenerationConfig::default())
            .map_err(|err| fail(runtime, &mut report, "generate_sql", err.to_string()))?;
        report.pending_migration_files = if dry_run {
            sql_artifacts.len() as i64
        } else {
            0
        };
        for artifact in &sql_artifacts {
            report
                .migration_metric_operations
                .push(MigrationMetricOperation {
                    kind: if artifact.kind.trim().is_empty() {
                        "bootstrap_sql".to_string()
                    } else {
                        artifact.kind.clone()
                    },
                    schema: artifact.schema.clone(),
                    safety: if sql_artifact_requires_review(&artifact.content) {
                        "requires_review".to_string()
                    } else {
                        "auto".to_string()
                    },
                });
        }

        // GAP 8: In dry-run mode, collect artifact SQL for the plan report instead of executing.
        if dry_run {
            let held = review_required_sql_artifacts(&sql_artifacts);
            if !held.is_empty() {
                report.warnings.push(format!(
                    "{} review-required bootstrap SQL artifact(s) would be held for manual approval: {}",
                    held.len(),
                    held.join("; ")
                ));
            }
            report.step(
                FsmState::Applying,
                format!(
                    "{} artifact(s) — DRY RUN (not applied)",
                    sql_artifacts.len()
                ),
            );
            for artifact in &sql_artifacts {
                report.dry_run_plan.push(format!(
                    "-- {}: {}\n{}",
                    artifact.rel_path, artifact.schema, artifact.content
                ));
            }
        } else {
            let held = review_required_sql_artifacts(&sql_artifacts);
            // The canonical approval decision above already authorized (or
            // rejected, before any mutation) every review-required change in this
            // migration. Fail closed here ONLY when the canonical change set still
            // has PENDING review-required SQL-artifact work that the single gate
            // did not authorize. A generator marker for an artifact already
            // recorded in the ledger — unchanged since the prior manifest, or
            // re-emitted under force_sync — is neither re-approved nor re-applied
            // (`execute_sql_artifacts` skips it by content checksum).
            if !held.is_empty()
                && !migration_authorized_via_plan
                && canonical_changes.iter().any(|c| {
                    c.kind == ChangeKind::ApplySqlArtifact
                        && c.safety == ChangeSafety::RequiresReview
                })
            {
                reject_review_required_sql_artifacts(runtime, &mut report, &sql_artifacts)?;
            }
            runtime
                .execute_sql_artifacts(&sql_artifacts, mode_label)
                .await
                .map_err(|err| fail(runtime, &mut report, "apply_sql", err.to_string()))?;
        }
        report.applied_sql_artifacts = sql_artifacts.len();
    }

    if dry_run {
        report.step(
            FsmState::Applying,
            format!(
                "{} backend provisioning action(s) — DRY RUN (not applied)",
                provisioning_plan.actions.len()
            ),
        );
    } else {
        for action in &provisioning_plan.actions {
            match action.resource_kind.as_str() {
                "table" => {}
                "vector_collection" => {
                    if !runtime.qdrant_configured() {
                        if allow_degraded_backend_startup(runtime) {
                            report.warnings.push(format!(
                                "skipped vector collection '{}' because qdrant is not configured",
                                action.resource_name
                            ));
                            continue;
                        }
                        return Err(fail(
                            runtime,
                            &mut report,
                            "qdrant_required",
                            format!(
                                "manifest requires vector collection '{}' but qdrant is not configured",
                                action.resource_name
                            ),
                        ));
                    }
                    let store = manifest
                        .stores
                        .iter()
                        .find(|store| store.resource_name == action.resource_name)
                        .ok_or_else(|| {
                            fail(
                                runtime,
                                &mut report,
                                "qdrant_store_lookup",
                                format!("missing manifest store {}", action.resource_name),
                            )
                        })?;
                    if let Err(err) = runtime.ensure_qdrant_store(store).await {
                        // C3: a per-backend apply DRIFT (qdrant unreachable / collection
                        // mismatch) must NOT exit the whole broker under STARTUP_FORCE_SYNC.
                        // When degraded-backend startup is allowed, emit the drift metric,
                        // warn, and keep serving the other backends; otherwise stay
                        // fail-closed (explicit flag, not force-sync). bug_report.md C3.
                        if allow_degraded_backend_startup(runtime) {
                            runtime.emit_drift_metric("qdrant_apply");
                            report.warnings.push(format!(
                                "degraded: vector collection '{}' apply failed; qdrant serving degraded: {err}",
                                action.resource_name
                            ));
                        } else {
                            return Err(fail(
                                runtime,
                                &mut report,
                                "qdrant_apply",
                                err.to_string(),
                            ));
                        }
                    }
                }
                "bucket" => {
                    if !runtime.s3_configured() {
                        if allow_degraded_backend_startup(runtime) {
                            report.warnings.push(format!(
                                "skipped object bucket '{}' because s3/minio is not configured",
                                action.resource_name
                            ));
                            continue;
                        }
                        return Err(fail(
                            runtime,
                            &mut report,
                            "s3_required",
                            format!(
                                "manifest requires object bucket '{}' but s3/minio is not configured",
                                action.resource_name
                            ),
                        ));
                    }
                    let store = manifest
                        .stores
                        .iter()
                        .find(|store| store.resource_name == action.resource_name)
                        .ok_or_else(|| {
                            fail(
                                runtime,
                                &mut report,
                                "s3_store_lookup",
                                format!("missing manifest store {}", action.resource_name),
                            )
                        })?;
                    if let Err(err) = runtime.ensure_s3_bucket(store).await {
                        // C3: object-store apply drift degrades the object backend
                        // instead of crashing the broker, when allowed. bug_report.md C3.
                        if allow_degraded_backend_startup(runtime) {
                            runtime.emit_drift_metric("s3_apply");
                            report.warnings.push(format!(
                                "degraded: object bucket '{}' apply failed; s3/minio serving degraded: {err}",
                                action.resource_name
                            ));
                        } else {
                            return Err(fail(runtime, &mut report, "s3_apply", err.to_string()));
                        }
                    }
                }
                "collection" | "graph" | "column_table" | "measurement" | "experiment" => {
                    let store = manifest
                        .stores
                        .iter()
                        .find(|store| store.resource_name == action.resource_name)
                        .ok_or_else(|| {
                            fail(
                                runtime,
                                &mut report,
                                "backend_store_lookup",
                                format!("missing manifest store {}", action.resource_name),
                            )
                        })?;
                    let spec_json = serde_json::to_string(store).unwrap_or_default();
                    match runtime
                        .ensure_resource_backend(&action.backend, &action.resource_name, &spec_json)
                        .await
                    {
                        Ok(()) => report.step(
                            FsmState::Applying,
                            format!(
                                "ensured {} resource {}",
                                action.backend, action.resource_name
                            ),
                        ),
                        Err(err) if err.code() == tonic::Code::FailedPrecondition => {
                            report.warnings.push(format!(
                                "skipped {} provisioning for inactive backend resource {}: {}",
                                action.backend,
                                action.resource_name,
                                err.message()
                            ));
                        }
                        Err(err) => {
                            // C3: a configured backend's resource-apply drift degrades
                            // that backend instead of exiting the whole broker, when
                            // degraded startup is allowed. bug_report.md C3.
                            if allow_degraded_backend_startup(runtime) {
                                runtime.emit_drift_metric("backend_apply");
                                report.warnings.push(format!(
                                    "degraded: {} resource '{}' apply failed; backend serving degraded: {}",
                                    action.backend,
                                    action.resource_name,
                                    err
                                ));
                            } else {
                                return Err(fail(
                                    runtime,
                                    &mut report,
                                    "backend_apply",
                                    err.to_string(),
                                ));
                            }
                        }
                    }
                }
                "keyspace" => {
                    // Redis keyspaces have no DDL to apply — schema is expressed
                    // via generated redis artifacts. Nothing to do here.
                }
                _ => {}
            }
        }
    }

    // ── Proto-diff auto-alter ─────────────────────────────────────────────────
    // Use the prior manifest loaded earlier (before the apply block) to compute
    // the diff.  If the proto changed since the last run, generate ALTER TABLE
    // (and equivalent) statements and apply them before the verify step runs.
    match &prior_manifest {
        Some(prior) if prior.checksum_sha256 != manifest.checksum_sha256 => {
            // Derive the SAME canonical set the approval gate validated
            // (relational + per-backend), not `diff_manifests` alone. The old
            // comment below claimed the two were identical; they are not
            // whenever the manifest carries a Qdrant/Mongo/Neo4j/ClickHouse/S3
            // delta, and the difference was silently dropped here after being
            // counted, approved, and hashed upstream.
            let changes = canonical_migration_changes(Some(prior), manifest);
            record_change_metric_operations(&mut report, &changes);

            // Of those backend operations only the CREATE kinds have an apply
            // path — the desired-state ENSURE provisioning above brings a new
            // collection/bucket/constraint into being. The UPDATE and DROP kinds
            // have no renderer and no executor anywhere, so continuing would
            // record the new manifest as applied while the store still holds the
            // old geometry, and the next boot would fast-start straight over the
            // divergence. Fail closed instead of diverging silently.
            let unappliable: Vec<String> = changes
                .iter()
                .filter(|change| {
                    matches!(
                        change.kind,
                        ChangeKind::UpdateCollection
                            | ChangeKind::DropCollection
                            | ChangeKind::UpdateValidator
                            | ChangeKind::UpdateConstraint
                            | ChangeKind::DropConstraint
                            | ChangeKind::ChangeTableEngine
                            | ChangeKind::UpdateLifecyclePolicy
                            | ChangeKind::DropBucket
                            | ChangeKind::UpdateStore
                            | ChangeKind::DropStore
                    )
                })
                .map(|change| format!("{:?} {}.{}", change.kind, change.schema, change.table))
                .collect();
            if !unappliable.is_empty() {
                // RECOVERY PATH. The diff is derived prior-manifest → current
                // manifest, and the stored manifest only advances after a
                // SUCCESSFUL startup — so reconciling the store by hand does not
                // clear this condition on its own, and refusing unconditionally
                // would deadlock the broker until the proto change was reverted.
                // The operator asserts "I have reconciled the backend myself" by
                // setting the ack variable; startup then proceeds, the manifest
                // advances, and the condition clears permanently. The assertion
                // is recorded as a warning so it shows up in the startup report
                // rather than being an invisible override.
                if manual_backend_reconciliation_acknowledged() {
                    report.warnings.push(format!(
                        "operator acknowledged manual backend reconciliation for changes UDB \
                         cannot apply ({}); recording the new manifest as current. Verify the \
                         store geometry matches the proto — nothing re-checks it after this.",
                        unappliable.join(", ")
                    ));
                } else {
                    return Err(fail(
                        runtime,
                        &mut report,
                        "backend_delta_unappliable",
                        format!(
                            "the manifest requires backend changes UDB cannot apply automatically \
                             ({}). Applying the rest would record this manifest as current while \
                             the store keeps its previous geometry, and the next boot would \
                             fast-start over the divergence. Either revert the proto change, or \
                             reconcile the store by hand and set {}=true to acknowledge it — \
                             hand-reconciliation alone does NOT clear this, because the diff is \
                             computed against the stored manifest, which only advances on a \
                             successful start.",
                            unappliable.join(", "),
                            ACK_MANUAL_BACKEND_RECONCILIATION_ENV
                        ),
                    ));
                }
            }

            // The approval decision and the Blocked-op rejection were made ONCE,
            // upfront, against the canonical change set (identical to this diff),
            // before any project mutation. Reuse that single decision instead of
            // re-comparing the same plan against a second change set — the
            // dual-gate count mismatch that used to deadlock any upgrade carrying
            // both bootstrap artifacts and a schema diff. `approved_via_plan` lets
            // the delta apply below skip the artifact-marker re-gate.
            let approved_via_plan = migration_authorized_via_plan;

            // The ordinary generator is intentionally unattended-safe and
            // excludes RequiresReview operations. Once the exact canonical
            // change set has passed the approval gate above, use the explicit
            // approved generator so the reviewed operations are not silently
            // dropped while the new manifest is recorded as applied.
            let delta = generate_startup_delta(manifest, &changes, approved_via_plan, dry_run);
            report.pending_migration_files = if dry_run { delta.len() as i64 } else { 0 };
            report.step(
                FsmState::Applying,
                format!(
                    "proto changed ({prior_ck} → {new_ck}): {n} delta artifact(s)",
                    prior_ck = &prior.checksum_sha256[..8.min(prior.checksum_sha256.len())],
                    new_ck = &manifest.checksum_sha256[..8.min(manifest.checksum_sha256.len())],
                    n = delta.len(),
                ),
            );
            if !delta.is_empty() && !dry_run {
                // Same shared review gate as the first-bootstrap branch: a
                // changed review-required artifact must abort BEFORE the delta
                // execute list runs — UNLESS an approved plan already authorized
                // this diff above, in which case re-blocking its own delta would
                // defeat the four-eyes apply. Without approval, the change-level
                // gate already rejected any RequiresReview diff; this keeps the
                // artifact-marker gate fail-closed with one source of truth.
                if !approved_via_plan {
                    reject_review_required_sql_artifacts(runtime, &mut report, &delta)?;
                }
                // Apply the delta (ALTER TABLE, ADD COLUMN, …) against the live DB.
                runtime
                    .execute_sql_artifacts(&delta, mode_label)
                    .await
                    .map_err(|err| {
                        fail(runtime, &mut report, "apply_delta_sql", err.to_string())
                    })?;
                report.applied_sql_artifacts += delta.len();

                // Write the delta artifacts to db_ops/postgres/bootstrap/ so operators
                // have a clear, committed audit trail of every schema change.
                let db_ops_root = resolve_db_ops_root(runtime);
                let bootstrap_dir = db_ops_root.join("postgres").join("bootstrap");
                match std::fs::create_dir_all(&bootstrap_dir) {
                    Err(err) => {
                        return Err(fail(
                            runtime,
                            &mut report,
                            "write_bootstrap_artifacts",
                            format!(
                                "could not create bootstrap dir {}: {err}",
                                bootstrap_dir.display()
                            ),
                        ));
                    }
                    Ok(()) => {
                        for artifact in &delta {
                            let dest = bootstrap_dir.join(&artifact.rel_path);
                            if let Some(parent) = dest.parent() {
                                std::fs::create_dir_all(parent).map_err(|err| {
                                    fail(
                                        runtime,
                                        &mut report,
                                        "write_bootstrap_artifacts",
                                        format!(
                                            "could not create bootstrap artifact dir {}: {err}",
                                            parent.display()
                                        ),
                                    )
                                })?;
                            }
                            match std::fs::write(&dest, artifact.content.as_bytes()) {
                                Err(err) => {
                                    return Err(fail(
                                        runtime,
                                        &mut report,
                                        "write_bootstrap_artifacts",
                                        format!(
                                            "could not write bootstrap artifact {}: {err}",
                                            artifact.rel_path
                                        ),
                                    ));
                                }
                                Ok(()) => report.step(
                                    FsmState::Applying,
                                    format!("wrote bootstrap/{}", artifact.rel_path),
                                ),
                            }
                        }
                    }
                }
            }
        }
        Some(_) => {
            report.step(
                FsmState::Applying,
                "proto checksum unchanged — no delta required",
            );
        }
        None => {
            report.step(
                FsmState::Applying,
                "no prior manifest found — baseline apply is the initial migration",
            );
        }
    }

    let db_ops_root = resolve_db_ops_root(runtime);
    let seeders_dir = resolve_seeders_dir(&db_ops_root);
    let seed_artifacts = load_seed_artifacts_from_dir(&seeders_dir)
        .map_err(|err| fail(runtime, &mut report, "load_seed_artifacts", err))?;
    if seed_artifacts.is_empty() {
        report.step(
            FsmState::Applying,
            format!("no seed artifacts found in {}", seeders_dir.display()),
        );
    } else if dry_run {
        report.step(
            FsmState::Applying,
            format!(
                "{} seed artifact(s) — DRY RUN (not applied)",
                seed_artifacts.len()
            ),
        );
        for artifact in &seed_artifacts {
            report
                .dry_run_plan
                .push(format!("-- seed artifact: {}", artifact.rel_path));
        }
    } else {
        runtime
            .execute_sql_artifacts_serial(&seed_artifacts, mode_label)
            .await
            .map_err(|err| fail(runtime, &mut report, "apply_seed_sql", err.to_string()))?;
        report.step(
            FsmState::Applying,
            format!(
                "seed routine checked {} artifact(s) in {}",
                seed_artifacts.len(),
                seeders_dir.display()
            ),
        );
        report.applied_sql_artifacts += seed_artifacts.len();
    }

    // Post-migrate hook: execute the operator's SQL file after all migrations
    // and seeds. A failure here is non-fatal (logged as a warning).
    let post_hook = runtime
        .config()
        .migration
        .post_migrate_sql
        .trim()
        .to_string();
    if !post_hook.is_empty() && !dry_run {
        match fs::read_to_string(&post_hook) {
            Ok(sql) => match runtime.execute_raw_sql(&sql, "post_migrate_sql hook").await {
                Ok(()) => report.step(
                    FsmState::Applying,
                    format!("ran post-migrate hook {post_hook}"),
                ),
                Err(err) => report
                    .warnings
                    .push(format!("post-migrate hook {post_hook} failed: {err}")),
            },
            Err(err) => report
                .warnings
                .push(format!("cannot read post-migrate hook {post_hook}: {err}")),
        }
    }

    transition(
        &mut engine,
        &mut report,
        FsmState::Verifying,
        "verifying live backend topology",
    )?;
    // Verify by default even when the proto checksum is unchanged. External
    // schema drift can happen after a successful run, and a proto checksum alone
    // cannot prove the live database still matches it. Operators who explicitly
    // accept that risk can set migration.skip_unchanged_verify=true.
    //
    // #133: live DB-vs-proto drift the verifier surfaces. When emergency
    // auto-alter is enabled, this is carried into the auto-alter block below and
    // fed to the repair planner instead of fail-closing the startup — lint of
    // the proto manifest alone can never produce missing-table/column drift.
    let emergency_auto_alter = runtime.config().migration.emergency_auto_alter;
    let skip_unchanged_verify = runtime.config().migration.skip_unchanged_verify;
    let mut pg_drift: Vec<crate::runtime::core::ManifestDrift> = Vec::new();
    if dry_run {
        report.step(
            FsmState::Verifying,
            "dry-run mode — skipping pg_catalog verification (SQL not applied)".to_string(),
        );
        report.verified_tables = manifest.tables.len();
    } else if checksum_unchanged && !force_reseed && !force_sync && skip_unchanged_verify {
        report.step(
            FsmState::Verifying,
            format!(
                "proto checksum {} unchanged — skipping pg_catalog verification because migration.skip_unchanged_verify=true",
                &manifest.checksum_sha256[..8.min(manifest.checksum_sha256.len())]
            ),
        );
        report.verified_tables = manifest.tables.len();
    } else {
        let drift = runtime
            .verify_postgres_manifest_drift(manifest)
            .await
            .map_err(|err| fail(runtime, &mut report, "postgres_verify", err.to_string()))?;
        report.verified_tables = manifest.tables.len();
        if !drift.is_empty() {
            runtime.emit_drift_metric("postgres_manifest_mismatch");
            if emergency_auto_alter {
                // #133: defer to the emergency auto-alter block, which feeds this
                // live drift into the repair planner and applies the safe repairs.
                report.step(
                    FsmState::Verifying,
                    format!(
                        "detected {} schema drift finding(s); deferring to emergency auto-alter",
                        drift.len()
                    ),
                );
                pg_drift = drift;
            } else {
                // Every drift finding names its schema/table/column. Route them
                // through `report_failure_json` like every other failure path so
                // each one is emitted as its own `tracing::error!` line —
                // serializing the report inline here skipped that, leaving the
                // operator with a headline that named no differing object and a
                // compact JSON blob. On a 221-schema deployment "something
                // differs" is not actionable.
                let finding_count = drift.len();
                report.errors.extend(drift.into_iter().map(|d| d.message));
                // State the consequence explicitly: the DDL already landed, and
                // the new manifest is deliberately NOT recorded, so a restart
                // re-verifies the same way rather than silently converging.
                report.errors.push(format!(
                    "{finding_count} live-schema drift finding(s) above; the migration's DDL has already been applied                      but the new manifest was NOT recorded in proto_schema_versions, so restarting repeats this                      verification against the same live schema"
                ));
                // Name the documented repair path. `emergency_auto_alter` feeds these
                // exact findings to the repair planner instead of fail-closing, and it
                // is otherwise discoverable only by reading this file.
                report.errors.push(
                    "remedy: run `udb verify --live --dsn <dsn>` to review these findings against the database                      before applying anything, or set migration.emergency_auto_alter=true                      (UDB_MIGRATION_EMERGENCY_AUTO_ALTER=true) to let startup feed them to the repair planner                      and apply the safe repairs"
                        .to_string(),
                );
                return Err(report_failure_json(
                    &report,
                    "PostgreSQL drift detected".to_string(),
                ));
            }
        }
    }

    if dry_run {
        report.step(
            FsmState::Verifying,
            "dry-run mode — skipping live backend resource verification (backend provisioning not applied)",
        );
    } else {
        for store in manifest
            .stores
            .iter()
            .filter(|store| store.store_kind == "vector")
        {
            if !runtime.qdrant_configured() {
                if allow_degraded_backend_startup(runtime) {
                    report.warnings.push(format!(
                        "skipped vector store verification '{}' because qdrant is not configured",
                        store.resource_name
                    ));
                    continue;
                }
                return Err(fail(
                    runtime,
                    &mut report,
                    "qdrant_required",
                    format!(
                        "manifest requires vector store '{}' but qdrant is not configured",
                        store.resource_name
                    ),
                ));
            }
            {
                runtime
                    .verify_qdrant_store(store)
                    .await
                    .map_err(|err| fail(runtime, &mut report, "qdrant_verify", err.to_string()))?;
                report.verified_vector_collections += 1;
            }
        }
        #[cfg(feature = "s3")]
        for store in manifest
            .stores
            .iter()
            .filter(|store| matches!(store.store_kind.as_str(), "object" | "blob" | "storage"))
        {
            if !runtime.s3_configured() {
                if allow_degraded_backend_startup(runtime) {
                    report.warnings.push(format!(
                        "skipped object store verification '{}' because s3/minio is not configured",
                        store.resource_name
                    ));
                    continue;
                }
                return Err(fail(
                    runtime,
                    &mut report,
                    "s3_required",
                    format!(
                        "manifest requires object store '{}' but s3/minio is not configured",
                        store.resource_name
                    ),
                ));
            }
            {
                runtime
                    .verify_s3_bucket(store)
                    .await
                    .map_err(|err| fail(runtime, &mut report, "s3_verify", err.to_string()))?;
                report.verified_object_buckets += 1;
            }
        }
    }

    // Record the current manifest only after live topology verification passes.
    // In dry-run mode, skip the save so the DB state is not mutated.
    if !dry_run {
        runtime
            .save_manifest_if_latest(manifest, expected_latest_checksum.as_deref())
            .await
            .map_err(|err| fail(runtime, &mut report, "save_manifest", err.to_string()))?;
    }
    // ── End proto-diff auto-alter ─────────────────────────────────────────────

    // ── Lint-driven auto-alter (emergency_auto_alter) ─────────────────────────
    // After topology verification, when emergency_auto_alter is enabled, lint the
    // live schema and apply the SAFE automatic repairs the planner derives
    // (CREATE SCHEMA / ADD COLUMN / SET DEFAULT / ENABLE RLS / …). Findings that
    // require manual review are surfaced as warnings, never auto-applied. The FSM
    // path Verifying → AutoAltering → Completed is valid; when disabled the run
    // stays Verifying → Completed.
    if emergency_auto_alter && !dry_run {
        transition(
            &mut engine,
            &mut report,
            FsmState::AutoAltering,
            "linting live schema and applying safe auto-repairs",
        )?;
        // #133: feed the LIVE DB-vs-proto drift captured during verification into
        // the repair planner. `lint_catalog(manifest)` lints the proto manifest in
        // isolation and can never surface missing-table/column drift (it has no view
        // of the live DB); the real drift comes from `verify_postgres_manifest_drift`
        // above (`pg_drift`), carrying the manifest column `sql_type`/`default_value`
        // so ADD COLUMN / SET DEFAULT repairs generate correct DDL.
        const SAFE_REPAIR_KINDS: [&str; 6] = [
            "missing_schema",
            "missing_table",
            "missing_column",
            "default_mismatch",
            "nullability_mismatch",
            "rls_enabled_no_policies",
        ];
        let mut repair_inputs: Vec<LintInput> = pg_drift
            .iter()
            .filter(|d| SAFE_REPAIR_KINDS.contains(&d.kind.as_str()))
            .map(|d| LintInput {
                lint_kind: d.kind.clone(),
                schema: d.schema.clone(),
                table: d.table.clone(),
                column: d.column.clone(),
                sql_type: d.sql_type.clone(),
                default_value: d.default_value.clone(),
                not_null: d.not_null,
            })
            .collect();
        // Also fold in proto-manifest lint findings (e.g. declared RLS without a
        // policy) that the live introspection does not cover, de-duplicating
        // against the live drift already collected.
        let live_lint = lint_catalog(manifest);
        for item in live_lint
            .items
            .iter()
            .filter(|item| SAFE_REPAIR_KINDS.contains(&item.kind.as_str()))
        {
            let already = repair_inputs.iter().any(|r| {
                r.lint_kind == item.kind
                    && r.schema == item.schema
                    && r.table == item.table
                    && r.column == item.column
            });
            if !already {
                repair_inputs.push(LintInput {
                    lint_kind: item.kind.clone(),
                    schema: item.schema.clone(),
                    table: item.table.clone(),
                    column: item.column.clone(),
                    sql_type: String::new(),
                    default_value: String::new(),
                    not_null: false,
                });
            }
        }
        if !repair_inputs.is_empty() {
            let repair_plan = plan_repairs(&repair_inputs);
            for decision in &repair_plan.decisions {
                report
                    .migration_metric_operations
                    .push(MigrationMetricOperation {
                        kind: decision.kind.as_str().to_string(),
                        schema: if decision.schema.trim().is_empty() {
                            "public".to_string()
                        } else {
                            decision.schema.clone()
                        },
                        safety: if decision.is_auto_safe {
                            "auto".to_string()
                        } else {
                            "requires_review".to_string()
                        },
                    });
            }
            report.step(
                FsmState::AutoAltering,
                format!(
                    "discovered {} repair(s): {} auto-safe, {} require review",
                    repair_plan.decisions.len(),
                    repair_plan.auto_safe_count,
                    repair_plan.requires_review_count
                ),
            );
            // Apply only auto-safe repairs that produced a concrete DDL statement.
            let artifacts: Vec<GeneratedArtifact> = repair_plan
                .decisions
                .iter()
                .filter(|d| d.is_auto_safe && !d.ddl.trim().is_empty())
                .enumerate()
                .map(|(i, d)| GeneratedArtifact {
                    rel_path: format!("auto_alter/{:03}_{}.sql", i, d.kind.as_str()),
                    kind: "auto_alter".to_string(),
                    schema: if d.schema.is_empty() {
                        "public".to_string()
                    } else {
                        d.schema.clone()
                    },
                    table: d.table.clone(),
                    content: d.ddl.clone(),
                })
                .collect();
            if !artifacts.is_empty() {
                runtime
                    .execute_sql_artifacts(&artifacts, mode_label)
                    .await
                    .map_err(|err| {
                        fail(runtime, &mut report, "auto_alter_apply", err.to_string())
                    })?;
                report.applied_sql_artifacts += artifacts.len();
                report.step(
                    FsmState::AutoAltering,
                    format!("applied {} auto-repair(s)", artifacts.len()),
                );
            }
            // Surface review-required repairs as warnings (never auto-applied).
            for d in repair_plan.decisions.iter().filter(|d| !d.is_auto_safe) {
                report.warnings.push(format!(
                    "[auto-alter requires review] {} {}.{}: {}",
                    d.kind.as_str(),
                    d.schema,
                    d.table,
                    d.reason
                ));
            }
        } else {
            report.step(FsmState::AutoAltering, "no auto-repair opportunities found");
        }
    }

    transition(
        &mut engine,
        &mut report,
        FsmState::Completed,
        "startup lifecycle completed",
    )?;
    report.completed = true;
    Ok(report)
}

const MIGRATION_REPLAY_PRIOR_CHECKSUM_ENV: &str = "UDB_MIGRATION_REPLAY_PRIOR_CHECKSUM";

#[derive(Debug)]
struct PriorManifestSelection {
    manifest: Option<CatalogManifest>,
    /// The actual latest checksum observed before planning. This, rather than
    /// an explicitly replayed historical manifest, anchors the final ledger CAS.
    latest_checksum: Option<String>,
}

fn migration_replay_prior_checksum() -> Option<String> {
    std::env::var(MIGRATION_REPLAY_PRIOR_CHECKSUM_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn validate_migration_replay_request(
    force_sync: bool,
    dry_run: bool,
    latest_checksum: Option<&str>,
    current_checksum: &str,
    replay_checksum: &str,
) -> Result<(), String> {
    if !force_sync && !dry_run {
        return Err(format!(
            "{MIGRATION_REPLAY_PRIOR_CHECKSUM_ENV} is a recovery override and is accepted only by admin dry-run or admin force-sync"
        ));
    }
    let Some(latest_checksum) = latest_checksum else {
        return Err(format!(
            "{MIGRATION_REPLAY_PRIOR_CHECKSUM_ENV} cannot be used because the manifest ledger is empty"
        ));
    };
    if latest_checksum != current_checksum {
        return Err(format!(
            "{MIGRATION_REPLAY_PRIOR_CHECKSUM_ENV} requires the latest ledger checksum to equal the current proto checksum; latest={latest_checksum} current={current_checksum}"
        ));
    }
    if replay_checksum == current_checksum {
        return Err(format!(
            "{MIGRATION_REPLAY_PRIOR_CHECKSUM_ENV} must name an older manifest, not the current checksum"
        ));
    }
    Ok(())
}

async fn load_selected_prior_manifest(
    runtime: &DataBrokerRuntime,
    manifest: &CatalogManifest,
    report: &mut StartupLifecycleReport,
    latest_checksum: Option<String>,
    replay_checksum: Option<String>,
    force_sync: bool,
    dry_run: bool,
) -> Result<PriorManifestSelection, String> {
    let selected_checksum = match replay_checksum {
        Some(replay_checksum) => {
            validate_migration_replay_request(
                force_sync,
                dry_run,
                latest_checksum.as_deref(),
                &manifest.checksum_sha256,
                &replay_checksum,
            )
            .map_err(|message| fail(runtime, report, "migration_replay_prior_invalid", message))?;
            report.step(
                FsmState::PlanProtoDiff,
                format!(
                    "explicit migration recovery replay selected historical checksum {replay_checksum}; latest ledger checksum remains {}",
                    latest_checksum.as_deref().unwrap_or_default()
                ),
            );
            Some(replay_checksum)
        }
        None => latest_checksum.clone(),
    };

    let Some(selected_checksum) = selected_checksum else {
        return Ok(PriorManifestSelection {
            manifest: None,
            latest_checksum,
        });
    };

    let selected = if dry_run {
        runtime
            .load_manifest_by_checksum_with_statement_timeout(
                &selected_checksum,
                dry_run_manifest_fetch_timeout(),
            )
            .await
    } else {
        runtime.load_manifest_by_checksum(&selected_checksum).await
    }
    .map_err(|err| {
        fail(
            runtime,
            report,
            "load_manifest_by_checksum",
            err.to_string(),
        )
    })?;
    if selected.is_none() {
        return Err(fail(
            runtime,
            report,
            "migration_replay_prior_missing",
            format!(
                "selected prior manifest checksum {selected_checksum} is not present in the manifest ledger"
            ),
        ));
    }

    Ok(PriorManifestSelection {
        manifest: selected,
        latest_checksum,
    })
}

async fn load_prior_manifest_for_dry_run(
    runtime: &DataBrokerRuntime,
    manifest: &CatalogManifest,
    report: &mut StartupLifecycleReport,
    replay_checksum: Option<String>,
) -> Result<PriorManifestSelection, String> {
    let prior_checksum = match runtime.load_last_manifest_checksum_if_exists().await {
        Ok(value) => value,
        Err(err) => {
            report.warnings.push(format!(
                "dry-run could not read prior manifest checksum: {}; planning bootstrap SQL only",
                err.message()
            ));
            return Ok(PriorManifestSelection {
                manifest: None,
                latest_checksum: None,
            });
        }
    };

    if replay_checksum.is_some() {
        return load_selected_prior_manifest(
            runtime,
            manifest,
            report,
            prior_checksum,
            replay_checksum,
            false,
            true,
        )
        .await;
    }

    let Some(prior_checksum) = prior_checksum.clone() else {
        report.step(
            FsmState::PlanProtoDiff,
            "dry-run found no prior proto manifest ledger; planning bootstrap SQL",
        );
        return Ok(PriorManifestSelection {
            manifest: None,
            latest_checksum: None,
        });
    };

    if prior_checksum == manifest.checksum_sha256 {
        report.step(
            FsmState::PlanProtoDiff,
            "dry-run prior checksum matches startup manifest; loading full prior manifest_json",
        );
    }

    let timeout = dry_run_manifest_fetch_timeout();
    match runtime
        .load_manifest_by_checksum_with_statement_timeout(&prior_checksum, timeout)
        .await
    {
        Ok(value) => Ok(PriorManifestSelection {
            manifest: value,
            latest_checksum: Some(prior_checksum),
        }),
        Err(err) => {
            report.warnings.push(format!(
                "dry-run could not load prior manifest_json within {}s: {}; \
                 planning bootstrap SQL only",
                timeout.as_secs().max(1),
                err.message()
            ));
            Ok(PriorManifestSelection {
                manifest: None,
                latest_checksum: Some(prior_checksum),
            })
        }
    }
}

fn dry_run_manifest_fetch_timeout() -> std::time::Duration {
    // NW-universal: bumped the upper bound from 120s → 3600s. Large
    // manifests on slow networks legitimately need more than 2
    // minutes; pre-fix any value above 120 was silently clamped and
    // the operator had no way to know why their long-fetch deployment
    // was timing out.
    let seconds = std::env::var("UDB_DRY_RUN_MANIFEST_FETCH_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(10)
        .clamp(1, 3600);
    std::time::Duration::from_secs(seconds)
}

/// NW-universal: operator-tunable timeout for the `force_sync`
/// advisory-lock retry loop. Default 10s matches the pre-fix
/// hardcoded value; range 1–3600s accommodates both fast CI checks
/// and slow networks where the previous holder's migration is
/// genuinely long-running.
fn force_sync_lock_timeout_secs() -> u64 {
    std::env::var("UDB_FORCE_SYNC_LOCK_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(10)
        .clamp(1, 3600)
}

/// NW-universal: operator-tunable poll interval between
/// `pg_try_advisory_lock` attempts. Default 500ms matches the
/// pre-fix hardcoded value; range 50–30_000ms lets operators trade
/// off database load against acquisition latency.
fn force_sync_lock_poll_ms() -> u64 {
    std::env::var("UDB_FORCE_SYNC_LOCK_POLL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(500)
        .clamp(50, 30_000)
}

async fn load_prior_manifest_for_apply(
    runtime: &DataBrokerRuntime,
    manifest: &CatalogManifest,
    report: &mut StartupLifecycleReport,
    force_sync: bool,
    replay_checksum: Option<String>,
) -> Result<PriorManifestSelection, String> {
    let prior_checksum = runtime
        .load_last_manifest_checksum_if_exists()
        .await
        .map_err(|err| {
            fail(
                runtime,
                report,
                "load_last_manifest_checksum",
                err.to_string(),
            )
        })?;

    if replay_checksum.is_some() {
        return load_selected_prior_manifest(
            runtime,
            manifest,
            report,
            prior_checksum,
            replay_checksum,
            force_sync,
            false,
        )
        .await;
    }

    let Some(prior_checksum) = prior_checksum else {
        return Ok(PriorManifestSelection {
            manifest: None,
            latest_checksum: None,
        });
    };

    if prior_checksum == manifest.checksum_sha256 {
        report.step(
            FsmState::PlanProtoDiff,
            "prior checksum matches startup manifest; loading full prior manifest_json",
        );
    }

    let prior = runtime
        .load_manifest_by_checksum(&prior_checksum)
        .await
        .map_err(|err| {
            fail(
                runtime,
                report,
                "load_manifest_by_checksum",
                err.to_string(),
            )
        })?;
    Ok(PriorManifestSelection {
        manifest: prior,
        latest_checksum: Some(prior_checksum),
    })
}

fn allow_degraded_backend_startup(runtime: &DataBrokerRuntime) -> bool {
    runtime.config().service.allow_degraded_backends
}

fn transition(
    engine: &mut Engine,
    report: &mut StartupLifecycleReport,
    state: FsmState,
    message: impl Into<String>,
) -> Result<(), String> {
    engine.transition(state.clone())?;
    report.step(state, message);
    Ok(())
}

fn fail(
    runtime: &DataBrokerRuntime,
    report: &mut StartupLifecycleReport,
    reason: &str,
    message: String,
) -> String {
    runtime.emit_drift_metric(reason);
    report.errors.push(message.clone());
    report_failure_json(report, message)
}

/// Serialize the lifecycle report to its machine-parseable single-line JSON (kept
/// for callers that parse it), but ALSO emit each accumulated error as its own
/// `tracing::error!` line. The compact JSON blob `{"run_id":…,"steps":[…],"errors":[…]}`
/// is unreadable in `docker logs`; the per-error lines make a failed startup
/// diagnosable at a glance (UDB_FRICTION §3).
fn report_failure_json(report: &StartupLifecycleReport, fallback: String) -> String {
    for err in &report.errors {
        tracing::error!(
            run_id = %report.run_id,
            "udb startup lifecycle error: {err}"
        );
    }
    serde_json::to_string(report).unwrap_or(fallback)
}

/// Heuristic: does this PostgreSQL DSN (plus its parsed port) look like a
/// transaction-pooled endpoint, where a session-level advisory lock is unsafe?
/// Matches the common poolers: PgBouncer (`pgbouncer`, default port 6432),
/// Neon pooled (`-pooler`), and Supabase pooled (`pooler.`).
fn looks_like_pooled_dsn(dsn: &str, port: u16) -> bool {
    let lowered = dsn.to_ascii_lowercase();
    port == 6432
        || lowered.contains(":6432")
        || lowered.contains("-pooler")
        || lowered.contains("pooler.")
        || lowered.contains("pgbouncer")
}

fn resolve_db_ops_root(runtime: &DataBrokerRuntime) -> PathBuf {
    let configured = runtime.config().migration.db_ops_root.trim();
    if !configured.is_empty() {
        return PathBuf::from(configured);
    }
    discover_db_ops_root().unwrap_or_else(|_| PathBuf::from("../db_ops"))
}

fn load_seed_artifacts_from_dir(
    seeders_dir: &Path,
) -> Result<Vec<crate::generation::GeneratedArtifact>, String> {
    let seed_files = ordered_seed_files(seeders_dir)?;
    seed_files
        .into_iter()
        .map(|seed_path| build_seed_artifact(seeders_dir, &seed_path))
        .collect()
}

fn ordered_seed_files(seeders_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let runner = seeders_dir.join("999_seed_all.sql");
    if runner.is_file() {
        return parse_seed_runner(&runner);
    }

    let entries = match fs::read_dir(seeders_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(format!(
                "failed to read seeders dir {}: {err}",
                seeders_dir.display()
            ));
        }
    };

    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| {
            format!(
                "failed to scan seeders dir {}: {err}",
                seeders_dir.display()
            )
        })?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("sql") {
            continue;
        }
        if path.file_name().and_then(|name| name.to_str()) == Some("999_seed_all.sql") {
            continue;
        }
        files.push(path);
    }
    files.sort();
    Ok(files)
}

fn parse_seed_runner(runner_path: &Path) -> Result<Vec<PathBuf>, String> {
    let runner = fs::read_to_string(runner_path).map_err(|err| {
        format!(
            "failed to read seed runner {}: {err}",
            runner_path.display()
        )
    })?;
    let base_dir = runner_path.parent().unwrap_or_else(|| Path::new("."));
    let mut files = Vec::new();
    for line in runner.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }
        let include_path = trimmed
            .strip_prefix("\\ir")
            .or_else(|| trimmed.strip_prefix("\\i"))
            .map(str::trim);
        let Some(include_path) = include_path else {
            continue;
        };
        if include_path.is_empty() {
            continue;
        }
        files.push(base_dir.join(include_path));
    }
    Ok(files)
}

fn build_seed_artifact(
    seeders_dir: &Path,
    seed_path: &Path,
) -> Result<crate::generation::GeneratedArtifact, String> {
    let content = fs::read_to_string(seed_path)
        .map_err(|err| format!("failed to read seed file {}: {err}", seed_path.display()))?;
    let relative_path = seed_path
        .strip_prefix(seeders_dir)
        .unwrap_or(seed_path)
        .to_string_lossy()
        .replace('\\', "/");
    let file_name = seed_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let (schema, table) = parse_seed_identity(file_name);

    Ok(crate::generation::GeneratedArtifact {
        rel_path: format!("seeds/{relative_path}"),
        kind: "seed".to_string(),
        schema,
        table,
        content,
    })
}

fn parse_seed_identity(file_name: &str) -> (String, String) {
    let stem = file_name.strip_suffix(".sql").unwrap_or(file_name);
    let logical_name = stem.split_once('_').map(|(_, rest)| rest).unwrap_or(stem);
    match logical_name.split_once('_') {
        Some((schema, table)) => (schema.to_string(), table.to_string()),
        None => (String::new(), logical_name.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_replay_requires_explicit_admin_mode_and_current_latest_ledger() {
        let current = "current";
        let prior = "prior";

        assert!(
            validate_migration_replay_request(true, false, Some(current), current, prior).is_ok()
        );
        assert!(
            validate_migration_replay_request(false, true, Some(current), current, prior).is_ok()
        );

        let normal_start =
            validate_migration_replay_request(false, false, Some(current), current, prior)
                .expect_err("normal serve must reject a replay override");
        assert!(normal_start.contains("only by admin dry-run or admin force-sync"));

        let empty_ledger = validate_migration_replay_request(true, false, None, current, prior)
            .expect_err("replay requires ledger history");
        assert!(empty_ledger.contains("manifest ledger is empty"));

        let advanced = validate_migration_replay_request(
            true,
            false,
            Some("different-latest"),
            current,
            prior,
        )
        .expect_err("replay must not race a newer manifest");
        assert!(advanced.contains("requires the latest ledger checksum to equal"));

        let current_as_prior =
            validate_migration_replay_request(true, false, Some(current), current, current)
                .expect_err("replay must select a historical manifest");
        assert!(current_as_prior.contains("must name an older manifest"));
    }

    #[test]
    fn startup_delta_emits_reviewed_drop_only_after_plan_authorization() {
        let reviewed_drop = ChangeOperation {
            kind: ChangeKind::DropForeignKey,
            safety: ChangeSafety::RequiresReview,
            schema: "fleet".to_string(),
            table: "driver_sessions".to_string(),
            object_name: "fk_driver_sessions_profile_old".to_string(),
            ..Default::default()
        };
        let blocked_drop = ChangeOperation {
            kind: ChangeKind::DropForeignKey,
            safety: ChangeSafety::Blocked,
            schema: "fleet".to_string(),
            table: "driver_sessions".to_string(),
            object_name: "fk_driver_sessions_profile_blocked".to_string(),
            ..Default::default()
        };
        let changes = vec![reviewed_drop, blocked_drop];
        let manifest = CatalogManifest::default();

        assert!(
            generate_startup_delta(&manifest, &changes, false, false).is_empty(),
            "unattended startup must not render review-required work"
        );

        let preview = generate_startup_delta(&manifest, &changes, false, true);
        assert_eq!(preview.len(), 1, "dry-run must preview reviewed work");

        let approved = generate_startup_delta(&manifest, &changes, true, false);
        assert_eq!(approved.len(), 1);
        assert!(
            approved[0]
                .content
                .contains("DROP CONSTRAINT IF EXISTS \"fk_driver_sessions_profile_old\"")
        );
        assert!(
            !approved[0]
                .content
                .contains("fk_driver_sessions_profile_blocked")
        );
    }

    // Regression for the dual approval-gate deadlock: the ONE canonical change set
    // an upgrade approves must be the prior→current delta, NOT the from-empty
    // subset the old bootstrap-artifact gate diffed against — that mismatch made
    // the two startup gates demand incompatible operation counts and crash-loop
    // any upgrade carrying both bootstrap artifacts and a schema diff.
    #[test]
    fn canonical_migration_changes_uses_prior_delta_not_from_empty() {
        use crate::generation::manifest::{ManifestColumn, ManifestTable};

        fn col(name: &str, sql_type: &str) -> ManifestColumn {
            ManifestColumn {
                column_name: name.to_string(),
                field_name: name.to_string(),
                sql_type: sql_type.to_string(),
                ..ManifestColumn::default()
            }
        }
        fn manifest(cols: Vec<ManifestColumn>, ck: &str, table_ck: &str) -> CatalogManifest {
            CatalogManifest {
                checksum_sha256: ck.to_string(),
                tables: vec![ManifestTable {
                    schema: "public".to_string(),
                    table: "invoices".to_string(),
                    columns: cols,
                    checksum_sha256: table_ck.to_string(),
                    ..ManifestTable::default()
                }],
                ..CatalogManifest::default()
            }
        }

        // Prior deployment already has the table; the upgrade ADDS one column.
        let prior = manifest(vec![col("id", "UUID")], "ck-prior", "t-prior");
        let current = manifest(
            vec![col("id", "UUID"), col("note", "TEXT")],
            "ck-current",
            "t-current",
        );

        let from_empty = diff_manifests(None, &current);
        let canonical = canonical_migration_changes(Some(&prior), &current);

        // The fix: an upgrade approves the prior→current delta, which is a
        // DIFFERENT (smaller) set than creating the whole table from empty. Before
        // the fix, the bootstrap gate approved the from-empty artifact subset while
        // the schema gate approved this delta — the incompatible-count deadlock.
        assert!(
            !canonical.is_empty(),
            "adding a column must yield a non-empty canonical delta"
        );
        assert_ne!(
            canonical, from_empty,
            "the canonical upgrade set must NOT be the from-empty set that deadlocked the two gates"
        );

        // First bootstrap (no prior) → the complete from-empty set.
        assert_eq!(canonical_migration_changes(None, &current), from_empty);

        // Unchanged proto checksum → no migration work, so no approval is demanded.
        assert!(
            canonical_migration_changes(Some(&current), &current).is_empty(),
            "an unchanged checksum must yield an empty canonical change set"
        );
    }

    // The system-schema intake guard must pass the SELF-HOSTING case (input
    // identical to the embedded catalog, as when the UDB repo serves its own
    // proto tree) and fail vendored-export drift: a changed field number on a
    // udb_* table, and a udb_* table the embedded catalog does not know.
    #[test]
    fn stale_system_schema_inputs_discriminates_selfhost_from_vendored_export() {
        // Non-udb schemas are never reported.
        let mut app = ProtoSchema {
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            message_name: "Order".to_string(),
            proto_package: "acme.v1".to_string(),
            ..ProtoSchema::default()
        };
        assert!(stale_system_schema_inputs(&[app.clone()]).is_empty());
        // A udb_* table UNKNOWN to the embedded catalog is additive — it cannot
        // shadow an embedded schema, and the repo's own smoke serves auxiliary
        // system-namespace tables (udb_sdk_live.*). Allowed.
        app.schema_name = "udb_myapp".to_string();
        assert!(
            stale_system_schema_inputs(&[app]).is_empty(),
            "unknown-identity udb_* tables must pass (additive, non-shadowing)"
        );

        // Self-hosting: an EXACT copy of an embedded native schema passes,
        // even from a different file path.
        let native = crate::runtime::native_catalog::native_schemas()
            .iter()
            .find(|schema| schema.schema_name.starts_with("udb_"))
            .expect("embedded catalog has udb_* schemas")
            .clone();
        let mut selfhost = native.clone();
        selfhost.file = "/consumer/checkout/some/other/path.proto".to_string();
        assert!(
            stale_system_schema_inputs(&[selfhost]).is_empty(),
            "identical system schema from a different path must pass"
        );

        // Vendored old export: same identity, drifted field number → reported.
        let mut vendored = native.clone();
        if let Some(column) = vendored.columns.first_mut() {
            column.field_number += 100;
        }
        assert_eq!(
            stale_system_schema_inputs(&[vendored]),
            [format!("{}.{}", native.schema_name, native.table_name)]
        );
    }

    #[test]
    fn review_required_sql_artifacts_are_detected_from_render_marker() {
        let artifacts = vec![
            GeneratedArtifact {
                rel_path: "public/001_safe.sql".to_string(),
                content: "-- UDB:sql_artifact_requires_review=false\nSELECT 1;\n".to_string(),
                ..GeneratedArtifact::default()
            },
            GeneratedArtifact {
                rel_path: "public/002_hold.sql".to_string(),
                schema: "public".to_string(),
                table: "orders".to_string(),
                content: "-- UDB:sql_artifact_requires_review=true\nSELECT dangerous();\n"
                    .to_string(),
                ..GeneratedArtifact::default()
            },
        ];

        let held = review_required_sql_artifacts(&artifacts);
        assert_eq!(held.len(), 1);
        assert!(held[0].contains("public/002_hold.sql"));
        assert!(held[0].contains("public.orders"));
    }

    fn safe_bootstrap_artifact() -> GeneratedArtifact {
        GeneratedArtifact {
            rel_path: "public/001_safe.sql".to_string(),
            schema: "public".to_string(),
            table: "widgets".to_string(),
            content: "-- UDB:sql_artifact_requires_review=false\nCREATE TABLE IF NOT EXISTS public.widgets (id TEXT PRIMARY KEY);\n".to_string(),
            ..GeneratedArtifact::default()
        }
    }

    fn review_required_bootstrap_artifact() -> GeneratedArtifact {
        GeneratedArtifact {
            rel_path: "public/002_hold.sql".to_string(),
            schema: "public".to_string(),
            table: "orders".to_string(),
            content: "-- UDB:sql_artifact_requires_review=true\nALTER TABLE public.orders DROP COLUMN total;\n".to_string(),
            ..GeneratedArtifact::default()
        }
    }

    /// Audit item 11: a changed requires_review artifact in unattended mode
    /// must hit the wired gate helper and abort BEFORE any execute list could
    /// include it — exercised against the REAL serving-path gate
    /// (`reject_review_required_sql_artifacts`) and the real partitioning
    /// function (`review_required_sql_artifacts`), not a re-implementation.
    #[test]
    fn unattended_review_required_artifact_aborts_via_wired_gate_before_execute() {
        let runtime = DataBrokerRuntime::planning_only();
        let mut report = StartupLifecycleReport::default();
        let artifacts = vec![
            safe_bootstrap_artifact(),
            review_required_bootstrap_artifact(),
        ];

        // Partitioning first: the review-required artifact lands in the held
        // set; the safe artifact does not.
        let held = review_required_sql_artifacts(&artifacts);
        assert_eq!(held.len(), 1);
        assert!(held[0].contains("public/002_hold.sql"));
        assert!(!held.iter().any(|entry| entry.contains("001_safe.sql")));

        // The gate aborts the run (Err) so no execute list is ever built; the
        // serialized report names the held artifact and records the error.
        let err = reject_review_required_sql_artifacts(&runtime, &mut report, &artifacts)
            .expect_err("review-required artifact must abort unattended bootstrap");
        assert!(err.contains("public/002_hold.sql"));
        assert!(err.contains("require manual approval and were not applied"));
        assert!(
            report
                .errors
                .iter()
                .any(|message| message.contains("public/002_hold.sql"))
        );
    }

    #[test]
    fn gate_passes_when_no_artifact_requires_review() {
        let runtime = DataBrokerRuntime::planning_only();
        let mut report = StartupLifecycleReport::default();
        let artifacts = vec![safe_bootstrap_artifact()];

        reject_review_required_sql_artifacts(&runtime, &mut report, &artifacts)
            .expect("safe artifacts must pass the review gate");
        assert!(report.errors.is_empty());
    }

    fn legacy_exported_plan_json() -> String {
        serde_json::to_string(&ExportedPlan {
            generated_at: "2026-01-01T00:00:00Z".to_string(),
            manifest_checksum: "checksum".to_string(),
            auto_count: 0,
            blocked_count: 0,
            hint_warnings: 0,
            operations: Vec::new(),
            blocked: Vec::new(),
            hints: Vec::new(),
            operations_hash: "sha256:empty".to_string(),
        })
        .unwrap()
    }

    #[test]
    fn approval_plan_parser_preserves_legacy_exported_plan_without_signed_policy() {
        let parsed = parse_approval_plan_file(&legacy_exported_plan_json(), false).unwrap();
        assert!(matches!(parsed, ApprovalPlanFile::Legacy(_)));
    }

    #[test]
    fn approval_policy_requires_signed_plan_when_signing_key_env_is_present() {
        let config = ApprovalConfig {
            quorum_size: 1,
            allowed_roles: Vec::new(),
            expiry: std::time::Duration::from_secs(3600),
            signing_key: Vec::new(),
        };

        assert!(!approval_policy_requires_signed_plan(&config, false));
        assert!(approval_policy_requires_signed_plan(&config, true));
    }

    #[test]
    fn approval_plan_parser_rejects_legacy_exported_plan_when_signed_policy_required() {
        let err = parse_approval_plan_file(&legacy_exported_plan_json(), true).unwrap_err();
        assert!(err.contains("requires a signed ApprovedPlan"));
    }

    #[test]
    fn approval_plan_parser_rejects_unsigned_approved_plan_when_signed_policy_required() {
        let plan: ExportedPlan = serde_json::from_str(&legacy_exported_plan_json()).unwrap();
        let raw = serde_json::to_string(&ApprovedPlan {
            plan,
            signatures: Vec::new(),
            expires_at_unix_ms: 1_000,
            seal: "sha256:empty".to_string(),
        })
        .unwrap();

        let err = parse_approval_plan_file(&raw, true).unwrap_err();
        assert!(err.contains("no signatures"));
    }

    #[test]
    fn load_seed_artifacts_from_runner_preserves_runner_order() {
        let tmp = std::env::temp_dir().join(format!(
            "udb_test_seed_loader_runner_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&tmp).unwrap();
        fs::write(tmp.join("010_beta.sql"), "SELECT 10;\n").unwrap();
        fs::write(tmp.join("001_alpha.sql"), "SELECT 1;\n").unwrap();
        fs::write(
            tmp.join("999_seed_all.sql"),
            "\\ir 010_beta.sql\n\\ir 001_alpha.sql\n",
        )
        .unwrap();

        let artifacts = load_seed_artifacts_from_dir(&tmp).expect("seed artifacts");
        let rel_paths = artifacts
            .iter()
            .map(|artifact| artifact.rel_path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(rel_paths, vec!["seeds/010_beta.sql", "seeds/001_alpha.sql"]);
        assert_eq!(artifacts[0].table, "beta");
        assert_eq!(artifacts[1].table, "alpha");

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn load_seed_artifacts_without_runner_sorts_sql_files() {
        let tmp = std::env::temp_dir().join(format!(
            "udb_test_seed_loader_fallback_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&tmp).unwrap();
        fs::write(tmp.join("010_beta.sql"), "SELECT 10;\n").unwrap();
        fs::write(tmp.join("001_alpha.sql"), "SELECT 1;\n").unwrap();
        fs::write(tmp.join("README.md"), "ignore\n").unwrap();

        let artifacts = load_seed_artifacts_from_dir(&tmp).expect("seed artifacts");
        let rel_paths = artifacts
            .iter()
            .map(|artifact| artifact.rel_path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(rel_paths, vec!["seeds/001_alpha.sql", "seeds/010_beta.sql"]);

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn legacy_lifecycle_json_defaults_structured_lint_fields() {
        let mut legacy = serde_json::to_value(StartupLifecycleReport::default())
            .expect("serialize lifecycle fixture");
        let object = legacy
            .as_object_mut()
            .expect("lifecycle report is an object");
        object.remove("lint_items");
        object.remove("lint_error_count");
        object.remove("lint_warning_count");
        let report: StartupLifecycleReport =
            serde_json::from_value(legacy).expect("legacy lifecycle report remains readable");
        assert!(report.lint_items.is_empty());
        assert_eq!(report.lint_error_count, 0);
        assert_eq!(report.lint_warning_count, 0);
    }

    #[test]
    fn failure_json_preserves_complete_structured_lint_finding_and_remediation() {
        let finding = LintItem {
            severity: LintSeverity::Error,
            kind: "ambiguous_catalog_identity".to_string(),
            schema: "authn".to_string(),
            table: "users".to_string(),
            column: "tenant_id".to_string(),
            description: "short message name User is ambiguous".to_string(),
            suggestion: "use acme.authn.v1.User".to_string(),
            source_file: "proto/acme/authn.proto".to_string(),
        };
        let report = StartupLifecycleReport {
            run_id: "lint-failure".to_string(),
            errors: vec![finding.display_line()],
            lint_items: vec![finding.clone()],
            lint_error_count: 1,
            lint_warning_count: 0,
            ..StartupLifecycleReport::default()
        };

        let encoded = report_failure_json(&report, "fallback".to_string());
        let decoded: StartupLifecycleReport =
            serde_json::from_str(&encoded).expect("failure result is report JSON");
        assert_eq!(decoded.lint_items, vec![finding]);
        assert_eq!(decoded.lint_error_count, 1);
        assert!(decoded.errors[0].contains("fix: use acme.authn.v1.User"));
    }
}
