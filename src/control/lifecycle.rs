use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

use crate::ast::ProtoSchema;
use crate::control::auto_alter::{LintInput, plan_repairs};
use crate::control::plan_approval::{
    ApprovalConfig, ApprovedPlan, ExportedPlan, plan_matches_current_diff,
};
use crate::db_ops_sync::{discover_db_ops_root, resolve_seeders_dir};
use crate::engine::{Engine, FsmState};
use crate::generation::{
    CatalogManifest, DsnGenerationConfig, GeneratedArtifact, LintSeverity, SqlGenerationConfig,
    generate_bootstrap_sql, generate_delta_sql, generate_unified_dsn_catalog,
};
use crate::migration::diff::{ChangeOperation, ChangeSafety, diff_manifests};
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
    let (merged_manifest, merged_schemas) =
        crate::runtime::native_catalog::merge_native(manifest, schemas);
    let manifest: &CatalogManifest = &merged_manifest;
    let schemas: &[ProtoSchema] = &merged_schemas;

    let mut engine = Engine::new_auto_id();
    let mut report = StartupLifecycleReport {
        run_id: engine.run_id.clone(),
        state: engine.state.as_str().to_string(),
        force_sync,
        dry_run,
        ..StartupLifecycleReport::default()
    };

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
        return Err(serde_json::to_string(&report).unwrap_or(message));
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
                        return Err(serde_json::to_string(&report).unwrap_or(message));
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
                return Err(serde_json::to_string(&report).unwrap_or(message));
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
                    return Err(serde_json::to_string(&report).unwrap_or(message));
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
            return Err(serde_json::to_string(&report).unwrap_or(message));
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
    if !lint.passed && !force_sync {
        let message = format!(
            "catalog lint failed with {} error(s), {} warning(s)",
            lint.error_count, lint.warning_count
        );
        runtime.emit_drift_metric("catalog_lint_failed");
        report.errors.push(message.clone());
        return Err(serde_json::to_string(&report).unwrap_or(message));
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
            report.warnings.push(format!(
                "[force_sync bypass] lint error ignored: {} — {}",
                item.kind, item.description
            ));
        }
    }
    if lint.warning_count > 0 {
        report.warnings.push(format!(
            "catalog lint emitted {} warning(s)",
            lint.warning_count
        ));
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

    // Load the prior manifest here — before SQL generation — so we can skip the
    // bootstrap apply entirely when the proto checksum is unchanged.  Previously
    // this was loaded a second time after the apply block, which meant every run
    // unconditionally re-executed hundreds of idempotent CREATE TABLE/INDEX
    // statements even when nothing had changed.
    let prior_manifest = if dry_run {
        load_prior_manifest_for_dry_run(runtime, manifest, &mut report).await?
    } else {
        load_prior_manifest_for_apply(runtime, manifest, &mut report).await?
    };

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
                    safety: "auto".to_string(),
                });
        }

        // GAP 8: In dry-run mode, collect artifact SQL for the plan report instead of executing.
        if dry_run {
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
            runtime
                .execute_sql_artifacts(&sql_artifacts)
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
                    runtime.ensure_qdrant_store(store).await.map_err(|err| {
                        fail(runtime, &mut report, "qdrant_apply", err.to_string())
                    })?;
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
                    runtime
                        .ensure_s3_bucket(store)
                        .await
                        .map_err(|err| fail(runtime, &mut report, "s3_apply", err.to_string()))?;
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
                            return Err(fail(
                                runtime,
                                &mut report,
                                "backend_apply",
                                err.to_string(),
                            ));
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
            let changes = diff_manifests(Some(prior), manifest);
            record_change_metric_operations(&mut report, &changes);

            // Fail-closed: a RequiresReview/Blocked change must never be silently
            // dropped while the manifest checksum advances — that would mark the
            // migration "done" and never retry it. Surface them and abort so an
            // operator handles the change explicitly (apply manually / approve).
            if !dry_run {
                let needs_review: Vec<String> = changes
                    .iter()
                    .filter(|c| c.safety != ChangeSafety::SafeAuto)
                    .map(|c| {
                        format!(
                            "{:?} {}.{}({}) [{:?}: {}]",
                            c.kind, c.schema, c.table, c.column, c.safety, c.blocked_reason
                        )
                    })
                    .collect();
                if !needs_review.is_empty() {
                    return Err(fail(
                        runtime,
                        &mut report,
                        "blocked_schema_change",
                        format!(
                            "{} schema change(s) require manual review and cannot be auto-applied: {}",
                            needs_review.len(),
                            needs_review.join("; ")
                        ),
                    ));
                }
            }

            // Approval gate: when an approved-plan file is configured the current
            // diff must exactly match it before any DDL is applied (four-eyes).
            let approval_plan_path = runtime
                .config()
                .migration
                .require_approval_plan
                .trim()
                .to_string();
            if !approval_plan_path.is_empty() && !dry_run {
                let raw = fs::read_to_string(&approval_plan_path).map_err(|err| {
                    fail(
                        runtime,
                        &mut report,
                        "load_approved_plan",
                        format!("cannot read approved plan {approval_plan_path}: {err}"),
                    )
                })?;
                // Prefer the SEALED quorum format (ApprovedPlan = ExportedPlan +
                // HMAC signatures + expiry + seal). When the file is a sealed
                // plan carrying at least one signature, enforce the full
                // four-eyes check (signatures + freshness + diff match) via
                // ready_to_apply using the UDB_APPROVAL_* config. Otherwise fall
                // back to a bare ExportedPlan + count/hash diff match (the
                // single-signer / Git-PR model).
                let sealed: Option<ApprovedPlan> = serde_json::from_str::<ApprovedPlan>(&raw)
                    .ok()
                    .filter(|p| !p.signatures.is_empty());
                let verdict = if let Some(sealed) = sealed {
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    sealed
                        .ready_to_apply(&ApprovalConfig::from_env(), manifest, &changes, now_ms)
                        .map_err(|err| {
                            fail(
                                runtime,
                                &mut report,
                                "approval_plan_rejected",
                                format!(
                                    "sealed approval plan {approval_plan_path} rejected: {err:?}"
                                ),
                            )
                        })?
                } else {
                    let approved: ExportedPlan = serde_json::from_str(&raw).map_err(|err| {
                        fail(
                            runtime,
                            &mut report,
                            "load_approved_plan",
                            format!("approved plan {approval_plan_path} is not valid JSON: {err}"),
                        )
                    })?;
                    plan_matches_current_diff(&approved, manifest, &changes)
                };
                if !verdict.is_match() {
                    return Err(fail(
                        runtime,
                        &mut report,
                        "approval_plan_mismatch",
                        format!(
                            "current diff does not match approved plan {approval_plan_path}: {verdict:?}"
                        ),
                    ));
                }
                report.step(
                    FsmState::Applying,
                    format!("approved plan {approval_plan_path} accepted (diff matches)"),
                );
            }

            let delta = generate_delta_sql(manifest, &changes, &SqlGenerationConfig::default());
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
                // Apply the delta (ALTER TABLE, ADD COLUMN, …) against the live DB.
                runtime.execute_sql_artifacts(&delta).await.map_err(|err| {
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
            .execute_sql_artifacts_serial(&seed_artifacts)
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
                report.errors.extend(drift.into_iter().map(|d| d.message));
                return Err(serde_json::to_string(&report)
                    .unwrap_or_else(|_| "PostgreSQL drift detected".to_string()));
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
            .save_manifest_if_latest(
                manifest,
                prior_manifest
                    .as_ref()
                    .map(|prior| prior.checksum_sha256.as_str()),
            )
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
                    .execute_sql_artifacts(&artifacts)
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

async fn load_prior_manifest_for_dry_run(
    runtime: &DataBrokerRuntime,
    manifest: &CatalogManifest,
    report: &mut StartupLifecycleReport,
) -> Result<Option<CatalogManifest>, String> {
    let prior_checksum = match runtime.load_last_manifest_checksum_if_exists().await {
        Ok(value) => value,
        Err(err) => {
            report.warnings.push(format!(
                "dry-run could not read prior manifest checksum: {}; planning bootstrap SQL only",
                err.message()
            ));
            return Ok(None);
        }
    };

    let Some(prior_checksum) = prior_checksum else {
        report.step(
            FsmState::PlanProtoDiff,
            "dry-run found no prior proto manifest ledger; planning bootstrap SQL",
        );
        return Ok(None);
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
        Ok(value) => Ok(value),
        Err(err) => {
            report.warnings.push(format!(
                "dry-run could not load prior manifest_json within {}s: {}; \
                 planning bootstrap SQL only",
                timeout.as_secs().max(1),
                err.message()
            ));
            Ok(None)
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
) -> Result<Option<CatalogManifest>, String> {
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

    let Some(prior_checksum) = prior_checksum else {
        return Ok(None);
    };

    if prior_checksum == manifest.checksum_sha256 {
        report.step(
            FsmState::PlanProtoDiff,
            "prior checksum matches startup manifest; loading full prior manifest_json",
        );
    }

    runtime
        .load_manifest_by_checksum(&prior_checksum)
        .await
        .map_err(|err| {
            fail(
                runtime,
                report,
                "load_manifest_by_checksum",
                err.to_string(),
            )
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
    serde_json::to_string(report).unwrap_or(message)
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
}
