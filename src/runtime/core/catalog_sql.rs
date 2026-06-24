//! Continuation `impl DataBrokerRuntime` block (Phase F split of core.rs).
use super::*;
use crate::control::tracker::DEFAULT_LEDGER_SCHEMA;

/// Structured DB-vs-proto drift finding produced by the pg_catalog verifier.
///
/// `kind` uses the same snake_case identifiers the repair planner consumes
/// (`missing_table`, `missing_column`, `missing_index`, `missing_trigger`,
/// `rls_enabled_no_policies`) so emergency auto-alter can route *real* live
/// drift into `plan_repairs` instead of re-linting the proto manifest in
/// isolation (which can never surface missing-table/column drift) — #133.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestDrift {
    pub kind: String,
    pub schema: String,
    pub table: String,
    /// Empty when the finding is table-level.
    pub column: String,
    /// SQL type from the manifest column (for ADD COLUMN repairs). Empty for
    /// non-column findings.
    pub sql_type: String,
    /// Default-value expression from the manifest column. Empty when none.
    pub default_value: String,
    /// Human-readable description (identical to the legacy findings string).
    pub message: String,
}

impl DataBrokerRuntime {
    /// GAP 7: Execute SQL artifacts with bounded-parallel schema-level execution.
    ///
    /// Per-schema bootstrap artifacts are run with bounded concurrency using
    /// `buffer_unordered` to reduce total migration time while avoiding PostgreSQL
    /// system-catalog "tuple concurrently updated" errors that arise when many
    /// DDL statements race in the same catalog tables simultaneously.
    ///
    /// Concurrency is controlled by the `UDB_DDL_CONCURRENCY` env var (default 4).
    /// Set to 1 to force fully-serial DDL (safest for serverless / shared PG).
    /// The single cross-schema FK artifact ("zzz_foreign_keys.sql") always runs
    /// last, serially, after all per-schema work has completed.
    /// `mode_label` is the migration-mode prefix used in progress logs:
    /// pass `"migrate"` for a normal `udb serve` startup and
    /// `"force-sync"` ONLY when an explicit force-sync is active — never
    /// hard-code "force-sync" here or a normal serve looks like the destructive
    /// command is running.
    pub async fn execute_sql_artifacts(
        &self,
        artifacts: &[GeneratedArtifact],
        mode_label: &str,
    ) -> Result<(), tonic::Status> {
        self.execute_sql_artifacts_internal(artifacts, true, mode_label)
            .await
    }

    pub async fn execute_sql_artifacts_serial(
        &self,
        artifacts: &[GeneratedArtifact],
        mode_label: &str,
    ) -> Result<(), tonic::Status> {
        self.execute_sql_artifacts_internal(artifacts, false, mode_label)
            .await
    }

    pub(crate) async fn execute_sql_artifacts_internal(
        &self,
        artifacts: &[GeneratedArtifact],
        allow_parallel: bool,
        mode_label: &str,
    ) -> Result<(), tonic::Status> {
        let pool = self.pg_pool()?;
        let ledger_schema = self.config.migration.ledger_schema.as_str();
        let schema_migrations = ledger_relation(ledger_schema, "schema_migrations");

        // GAP 35 / GAP 44: Load already-applied artifact filename+checksum pairs
        // from the migration ledger. A stable rel_path alone is not enough: the
        // artifact may have changed while keeping the same file name.
        // On the next startup after a partial failure, artifacts that committed
        // successfully with the same content are skipped rather than re-applied.
        let applied_sql =
            format!("SELECT filename, checksum FROM {schema_migrations} WHERE state = 'applied'");
        let applied_set: std::collections::HashSet<(String, String)> = match sqlx::query_as(
            &applied_sql,
        )
        .fetch_all(pool)
        .await
        {
            Ok(rows) => rows.into_iter().collect(),
            Err(err) => {
                tracing::warn!(error = %err, "failed to load applied artifact set; treating all as pending");
                std::collections::HashSet::new()
            }
        };

        if !applied_set.is_empty() {
            tracing::info!(
                already_applied = applied_set.len(),
                "skipping already-applied artifacts from previous interrupted run"
            );
        }

        let pending = artifacts
            .iter()
            .filter(|artifact| {
                let checksum = artifact_content_checksum(&artifact.content);
                let force_reseed_seed =
                    artifact.kind == "seed" && self.config.migration.force_reseed;
                let is_applied =
                    applied_set.contains(&(artifact.rel_path.clone(), checksum.clone()));
                if force_reseed_seed {
                    eprintln!(
                        "udb {mode_label}: force-reseed seed {} | computed={}",
                        artifact.rel_path,
                        &checksum[..16.min(checksum.len())],
                    );
                } else if !is_applied {
                    // Show both the computed checksum and what the DB has for this filename,
                    // so we can diagnose checksum mismatches between runs.
                    let db_checksum = applied_set
                        .iter()
                        .find(|(f, _)| f == &artifact.rel_path)
                        .map(|(_, c)| c.as_str())
                        .unwrap_or("(not in applied_set)");
                    eprintln!(
                        "udb {mode_label}: pending {} | computed={} | db={}",
                        artifact.rel_path,
                        &checksum[..16.min(checksum.len())],
                        &db_checksum[..16.min(db_checksum.len())]
                    );
                }
                force_reseed_seed || !is_applied
            })
            .collect::<Vec<_>>();
        eprintln!(
            "udb {mode_label}: {} pending SQL artifact(s)",
            pending.len()
        );

        // Emit a throttled progress heartbeat during apply so a
        // long bootstrap against a remote DB does not look hung, and mirror
        // progress into the queryable `migration_runtime_state` singleton row.
        let total = pending.len();
        let started = std::time::Instant::now();
        let applied = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let step = (total / 20).max(1);
        let mode = mode_label.to_string();

        if !allow_parallel {
            for artifact in &pending {
                tracing::debug!(mode = %mode, artifact = %artifact.rel_path, "applying artifact");
                Self::apply_sql_artifact(
                    pool,
                    artifact,
                    self.config.migration.force_reseed,
                    ledger_schema,
                )
                .await?;
                let n = applied.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                if n == total || n % step == 0 {
                    migration_heartbeat(
                        pool,
                        ledger_schema,
                        &mode,
                        &artifact.rel_path,
                        n,
                        total,
                        started,
                    )
                    .await;
                }
            }
            return Ok(());
        }

        // Separate extension artifacts (must run first), cross-schema artifacts
        // (must run last), and per-schema artifacts (safe for bounded parallelism).
        let (serial_first, rest): (Vec<_>, Vec<_>) = pending
            .iter()
            .copied()
            .partition(|a| a.rel_path == "000_extensions.sql" || a.rel_path.starts_with("000_"));
        let (serial_last, parallel): (Vec<_>, Vec<_>) = rest
            .into_iter()
            .partition(|a| a.rel_path.starts_with("zzz_") || a.schema.is_empty());

        for artifact in &serial_first {
            tracing::debug!(mode = %mode, artifact = %artifact.rel_path, "applying artifact");
            Self::apply_sql_artifact(
                pool,
                artifact,
                self.config.migration.force_reseed,
                ledger_schema,
            )
            .await?;
            let n = applied.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            if n == total || n % step == 0 {
                migration_heartbeat(
                    pool,
                    ledger_schema,
                    &mode,
                    &artifact.rel_path,
                    n,
                    total,
                    started,
                )
                .await;
            }
        }

        // Run per-schema artifacts with bounded concurrency using a semaphore to
        // avoid PostgreSQL system-catalog "tuple concurrently updated" races that
        // occur when many DDL statements run simultaneously on Neon/PgBouncer.
        // Configured DDL concurrency defaults to 4; set 1 for serial execution.
        let concurrency: usize = self.config.ddl_concurrency.max(1);
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency));

        let mut futs: futures::stream::FuturesUnordered<_> = parallel
            .into_iter()
            .map(|artifact| {
                let pool = pool.clone();
                let sem = sem.clone();
                let force_reseed = self.config.migration.force_reseed;
                let ledger_schema = ledger_schema.to_string();
                let mode = mode.clone();
                let applied = applied.clone();
                // `started`/`total`/`step` are Copy — bind owned locals so the
                // `async move` block captures copies, not borrows of the loop env.
                let started = started;
                let total = total;
                let step = step;
                async move {
                    // Hold the semaphore permit for the full duration of this DDL.
                    let _permit = sem
                        .acquire_owned()
                        .await
                        .expect("DDL semaphore unexpectedly closed");

                    tracing::debug!(mode = %mode, artifact = %artifact.rel_path, "applying artifact");
                    let result =
                        Self::apply_sql_artifact(&pool, artifact, force_reseed, &ledger_schema)
                            .await;
                    if result.is_ok() {
                        let n =
                            applied.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                        if n == total || n % step == 0 {
                            migration_heartbeat(
                                &pool,
                                &ledger_schema,
                                &mode,
                                &artifact.rel_path,
                                n,
                                total,
                                started,
                            )
                            .await;
                        }
                    }
                    result
                }
            })
            .collect();

        while let Some(result) = futures::StreamExt::next(&mut futs).await {
            result?;
        }

        // Serial pass: cross-schema FK constraints and other final artifacts.
        for artifact in &serial_last {
            tracing::debug!(mode = %mode, artifact = %artifact.rel_path, "applying artifact");
            Self::apply_sql_artifact(
                pool,
                artifact,
                self.config.migration.force_reseed,
                ledger_schema,
            )
            .await?;
            let n = applied.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            if n == total || n % step == 0 {
                migration_heartbeat(
                    pool,
                    ledger_schema,
                    &mode,
                    &artifact.rel_path,
                    n,
                    total,
                    started,
                )
                .await;
            }
        }
        Ok(())
    }

    pub(crate) async fn apply_sql_artifact(
        pool: &PgPool,
        artifact: &GeneratedArtifact,
        force_reseed: bool,
        ledger_schema: &str,
    ) -> Result<(), tonic::Status> {
        let checksum = artifact_content_checksum(&artifact.content);
        let schema_migrations = ledger_relation(ledger_schema, "schema_migrations");
        const CHUNK_THRESHOLD: usize = 1024 * 1024; // 1 MiB
        let is_seed = artifact.kind == "seed";
        let is_large = artifact.content.len() > CHUNK_THRESHOLD;

        // FAST PATH: a small, BEGIN/COMMIT-wrapped DDL artifact folds its ledger
        // 'applied' record INTO its own transaction, so the DDL and the ledger
        // commit atomically in ONE round-trip — instead of three (a separate
        // 'in_progress' write before + 'applied' write after). On a large remote
        // manifest (hundreds of artifacts) those two extra round-trips per
        // artifact dominate; folding removes them. On any failure the whole
        // transaction rolls back and nothing is recorded, so the artifact
        // correctly re-applies on the next run (the content checksum is computed
        // on the UNFOLDED `artifact.content`, so the skip ledger is unaffected).
        let trimmed = artifact.content.trim_end();
        if !is_seed && !is_large && trimmed.ends_with("COMMIT;") {
            let body = trimmed.strip_suffix("COMMIT;").unwrap_or(trimmed);
            let folded = format!(
                "{body}\n{ledger}\nCOMMIT;\n",
                ledger = applied_ledger_upsert_sql(&schema_migrations, artifact, &checksum)
            );
            return pool
                .execute(folded.as_str())
                .await
                .map(|_| ())
                .map_err(|err| {
                    tracing::error!(
                        artifact = %artifact.rel_path,
                        schema = %artifact.schema,
                        table = %artifact.table,
                        kind = %artifact.kind,
                        error = %err,
                        "SQL artifact apply failed"
                    );
                    tonic::Status::internal(format!(
                        "failed to apply SQL artifact {}: {err}",
                        artifact.rel_path
                    ))
                });
        }

        // Use INSERT ... ON CONFLICT DO UPDATE only when the checksum has
        // changed (i.e. the artifact content differs from the previously
        // recorded version).  When the checksum is identical, DO NOTHING
        // preserves the existing 'applied' state and avoids taking a
        // RowExclusiveLock on the row that would block concurrent DDL.
        // This prevents the 2nd-run re-apply problem: execute_sql_artifacts
        // already filters out (filename, checksum) pairs that are 'applied',
        // so reaching here with an unchanged checksum means force_sync is
        // active — in that case we still want to re-apply the DDL but we
        // must not reset a cleanly-applied row back to 'in_progress' until
        // we are actually about to execute it.
        // Ledger write is best-effort: a transient failure must not abort
        // startup, but it must not be silent either — a swallowed failure here
        // leaves the artifact unrecorded and re-applied on the next run.
        let in_progress_sql = format!(
            "INSERT INTO {schema_migrations} \
             (filename, checksum, state, migration_kind, proto_manifest_checksum, source_schema, source_table, operation_kind) \
             VALUES ($1, $2, 'in_progress', $3, $4, $5, $6, $7) \
             ON CONFLICT (filename) DO UPDATE SET \
                checksum = EXCLUDED.checksum, \
                state = 'in_progress', \
                migration_kind = EXCLUDED.migration_kind, \
                proto_manifest_checksum = EXCLUDED.proto_manifest_checksum, \
                source_schema = EXCLUDED.source_schema, \
                source_table = EXCLUDED.source_table, \
                operation_kind = EXCLUDED.operation_kind \
             WHERE {schema_migrations}.checksum IS DISTINCT FROM EXCLUDED.checksum \
                OR {schema_migrations}.state IS DISTINCT FROM 'applied'"
        );
        if let Err(err) = sqlx::query(&in_progress_sql)
            .bind(&artifact.rel_path)
            .bind(&checksum)
            .bind(&artifact.kind)
            .bind(extract_manifest_checksum(&artifact.content))
            .bind(&artifact.schema)
            .bind(&artifact.table)
            .bind(&artifact.kind)
            .execute(pool)
            .await
        {
            tracing::warn!(
                artifact = %artifact.rel_path,
                error = %err,
                "failed to record schema_migrations 'in_progress' ledger entry; artifact will re-apply next run",
            );
        }

        // Large seed files (> 1 MiB) are split into individual ;-terminated
        // statements and executed one at a time.  Sending a 39 MB INSERT as a
        // single pool.execute() call takes ~300 s, after which Windows TCP
        // (OS error 10053 / WSAECONNABORTED) drops the idle-seeming connection
        // before the schema_migrations ledger entry can be written.  Chunked
        // execution keeps each round-trip short and the TCP connection alive.
        // Seeds are always executed statement-by-statement so that individual
        // rows with integrity constraint violations (FK, NOT NULL, etc.) from
        // the MySQL source can be skipped without aborting the whole artifact.
        if is_large || is_seed {
            let stmts = Self::split_sql_statements(&artifact.content);
            tracing::info!(
                artifact = %artifact.rel_path,
                total_bytes = artifact.content.len(),
                statement_count = stmts.len(),
                "large artifact: executing as {} individual statements",
                stmts.len(),
            );
            // Resume checkpoint: skip statements already applied in a previous
            // interrupted run (e.g. Windows os error 10053 / WSAECONNABORTED).
            // (start_stmt_idx, start_row_idx): resume position. start_row_idx is
            // only meaningful for the statement at start_stmt_idx — rows below it
            // were committed in a prior interrupted run and must not be replayed.
            let (start_stmt_idx, start_row_idx): (usize, usize) = if is_seed {
                Self::ensure_seed_progress_table(pool, ledger_schema).await;
                if force_reseed {
                    Self::clear_seed_checkpoint(pool, ledger_schema, &artifact.rel_path).await;
                    (0, 0)
                } else {
                    Self::load_seed_checkpoint(pool, ledger_schema, &artifact.rel_path).await
                }
            } else {
                (0, 0)
            };
            if start_stmt_idx > 0 || start_row_idx > 0 {
                tracing::info!(
                    artifact = %artifact.rel_path,
                    resuming_from_statement = start_stmt_idx,
                    resuming_from_row = start_row_idx,
                    "seed resume: skipping {} statement(s); skipping {} row(s) of the resume statement",
                    start_stmt_idx,
                    start_row_idx,
                );
            }
            for (idx, stmt) in stmts.iter().enumerate() {
                // Skip statements already applied in a previous interrupted run.
                if idx < start_stmt_idx {
                    continue;
                }
                let mut transient_attempts = 0u32;
                'stmt: loop {
                    match pool.execute(stmt.as_str()).await {
                        Ok(_) => {
                            if is_seed {
                                Self::save_seed_checkpoint(
                                    pool,
                                    ledger_schema,
                                    &artifact.rel_path,
                                    idx,
                                )
                                .await;
                            }
                            break 'stmt;
                        }
                        Err(ref err) if is_seed && Self::is_skippable_seed_error(err) => {
                            // Bulk INSERT failed with a constraint/data violation.
                            // Optimistic fallback: split the multi-row VALUES list
                            // into individual single-row INSERTs and retry so that
                            // only the offending row(s) are skipped while all other
                            // valid rows in the same statement are still inserted.
                            let row_stmts = Self::split_insert_rows(stmt);
                            if row_stmts.len() > 1 {
                                tracing::warn!(
                                    artifact = %artifact.rel_path,
                                    statement_index = idx,
                                    row_count = row_stmts.len(),
                                    original_error = %err,
                                    "seed batch failed — retrying as {} individual row inserts",
                                    row_stmts.len(),
                                );
                                // Run per-row INSERTs concurrently in chunks of
                                // ROW_CHUNK to stay within the pool's max_conn
                                // limit and avoid PoolTimedOut on large seeds.
                                {
                                    use futures::stream::FuturesUnordered;
                                    const ROW_CHUNK: usize = 8;
                                    let mut first_fatal: Option<(usize, sqlx::Error)> = None;
                                    // On a mid-statement resume, skip rows already
                                    // committed in the prior run (row-level resume).
                                    let row_skip = if idx == start_stmt_idx {
                                        start_row_idx
                                    } else {
                                        0
                                    };
                                    'chunks: for (chunk_i, chunk) in
                                        row_stmts.chunks(ROW_CHUNK).enumerate()
                                    {
                                        let chunk_start = chunk_i * ROW_CHUNK;
                                        let mut futs: FuturesUnordered<_> = chunk
                                            .iter()
                                            .cloned()
                                            .enumerate()
                                            .filter(|(local_idx, _)| {
                                                chunk_start + local_idx >= row_skip
                                            })
                                            .map(|(local_idx, row_stmt)| {
                                                let p = pool.clone();
                                                let row_idx = chunk_start + local_idx;
                                                async move {
                                                    (row_idx, p.execute(row_stmt.as_str()).await)
                                                }
                                            })
                                            .collect();
                                        while let Some((row_idx, result)) = futs.next().await {
                                            match result {
                                                Ok(_) => {}
                                                Err(ref row_err)
                                                    if Self::is_skippable_seed_error(row_err) =>
                                                {
                                                    tracing::warn!(
                                                        artifact = %artifact.rel_path,
                                                        statement_index = idx,
                                                        row_index = row_idx,
                                                        error = %row_err,
                                                        "seed row skipped: dirty MySQL source data"
                                                    );
                                                }
                                                Err(row_err) => {
                                                    if first_fatal.is_none() {
                                                        first_fatal = Some((row_idx, row_err));
                                                    }
                                                }
                                            }
                                        }
                                        if first_fatal.is_some() {
                                            break 'chunks;
                                        }
                                        // Chunk fully applied — every row up to
                                        // chunk_last is committed or skipped. Chunks
                                        // run in order, so this is a safe contiguous
                                        // checkpoint. Guard against regressing the
                                        // checkpoint when an early chunk was entirely
                                        // below row_skip (already done in a prior run).
                                        let chunk_last = chunk_start + chunk.len() - 1;
                                        if chunk_last >= row_skip {
                                            Self::save_seed_row_checkpoint(
                                                pool,
                                                ledger_schema,
                                                &artifact.rel_path,
                                                idx,
                                                chunk_last,
                                            )
                                            .await;
                                        }
                                    }
                                    if let Some((row_idx, row_err)) = first_fatal {
                                        // Only hard-fail on actual database-level errors
                                        // (wrong data, schema mismatches, etc.).  Any
                                        // other sqlx error variant (Io, PoolTimedOut,
                                        // PoolClosed, WorkerCrashed, TlsHandshake, …)
                                        // is a transient infrastructure problem — retry
                                        // the whole statement with exponential back-off.
                                        let is_db_err = matches!(row_err, sqlx::Error::Database(_));
                                        if !is_db_err && transient_attempts < 3 {
                                            transient_attempts += 1;
                                            let delay_secs = 1u64 << transient_attempts;
                                            tracing::warn!(
                                                artifact = %artifact.rel_path,
                                                statement_index = idx,
                                                row_index = row_idx,
                                                attempt = transient_attempts,
                                                delay_secs,
                                                error = %row_err,
                                                "transient row error — retrying statement after back-off"
                                            );
                                            tokio::time::sleep(std::time::Duration::from_secs(
                                                delay_secs,
                                            ))
                                            .await;
                                            continue 'stmt;
                                        }
                                        // Use eprintln! (unbuffered stderr) so the
                                        // error is visible even if the process exits
                                        // before the tracing subscriber flushes.
                                        eprintln!(
                                            "FATAL row error: artifact={} stmt={} row={} error={:?}",
                                            artifact.rel_path, idx, row_idx, row_err
                                        );
                                        tracing::error!(
                                            artifact = %artifact.rel_path,
                                            statement_index = idx,
                                            row_index = row_idx,
                                            error = %row_err,
                                            "SQL artifact apply failed (row insert)"
                                        );
                                        return Err(tonic::Status::internal(format!(
                                            "failed to apply SQL artifact {} (stmt {} row {}): {row_err}",
                                            artifact.rel_path, idx, row_idx
                                        )));
                                    }
                                }
                            } else {
                                // Single-row or non-INSERT statement — skip it.
                                tracing::warn!(
                                    artifact = %artifact.rel_path,
                                    statement_index = idx,
                                    error = %err,
                                    "seed chunk skipped: dirty MySQL source data"
                                );
                            }
                            // Save checkpoint after per-row retry block completes.
                            if is_seed {
                                Self::save_seed_checkpoint(
                                    pool,
                                    ledger_schema,
                                    &artifact.rel_path,
                                    idx,
                                )
                                .await;
                            }
                            break 'stmt;
                        }
                        Err(ref err)
                            if transient_attempts < 3
                                && Self::is_transient_connection_error(err) =>
                        {
                            transient_attempts += 1;
                            tracing::warn!(
                                artifact = %artifact.rel_path,
                                statement_index = idx,
                                attempt = transient_attempts,
                                error = %err,
                                "transient connection error — retrying statement ({}/3)",
                                transient_attempts,
                            );
                            tokio::time::sleep(Duration::from_secs(2u64.pow(transient_attempts)))
                                .await;
                            continue 'stmt;
                        }
                        Err(err) => {
                            tracing::error!(
                                artifact = %artifact.rel_path,
                                statement_index = idx,
                                error = %err,
                                "SQL artifact apply failed (chunked)"
                            );
                            return Err(tonic::Status::internal(format!(
                                "failed to apply SQL artifact {} (stmt {}): {err}",
                                artifact.rel_path, idx
                            )));
                        }
                    }
                } // end 'stmt retry loop
            }
            // Clear the statement-level checkpoint once the entire artifact succeeds.
            if is_seed {
                Self::clear_seed_checkpoint(pool, ledger_schema, &artifact.rel_path).await;
            }
        } else {
            pool.execute(artifact.content.as_str())
                .await
                .map_err(|err| {
                    tracing::error!(
                        artifact = %artifact.rel_path,
                        schema   = %artifact.schema,
                        table    = %artifact.table,
                        kind     = %artifact.kind,
                        error    = %err,
                        "SQL artifact apply failed"
                    );
                    tonic::Status::internal(format!(
                        "failed to apply SQL artifact {}: {err}",
                        artifact.rel_path
                    ))
                })?;
        }

        let applied_sql = format!(
            "UPDATE {schema_migrations} \
             SET state = 'applied', applied_at = NOW(), checksum = $2 \
             WHERE filename = $1"
        );
        if let Err(err) = sqlx::query(&applied_sql)
            .bind(&artifact.rel_path)
            .bind(&checksum)
            .execute(pool)
            .await
        {
            tracing::warn!(
                artifact = %artifact.rel_path,
                error = %err,
                "failed to mark schema_migrations 'applied'; artifact will re-apply next run",
            );
        }
        Ok(())
    }

    /// Split a SQL script into individual `;`-terminated statements.
    ///
    /// Scans line-by-line: whenever a line's trimmed content ends with `';'`
    /// that marks the end of a complete SQL statement.  This handles the
    /// multi-row `INSERT ... VALUES (...) ON CONFLICT DO NOTHING;` format
    /// produced by the seed exporter, as well as single-line DDL statements.
    ///
    /// Strings that happen to contain semicolons are safe because they appear
    /// as quoted values inside a `VALUES` clause — those lines end with `','`
    /// or `'),'`, never with a bare `';'`.
    /// Split a SQL script into individual `;`-terminated statements.
    ///
    /// Tracks single-quoted string literals (including `''` escape sequences),
    /// double-quoted identifiers, and `--` line comments so that a `;` that
    /// appears inside a string value (e.g. Bengali address data containing `';'`)
    /// is never mistaken for a statement terminator.
    pub(crate) fn split_sql_statements(content: &str) -> Vec<String> {
        let mut statements: Vec<String> = Vec::new();
        let mut buf = String::new();
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_line_comment = false;
        let chars: Vec<char> = content.chars().collect();
        let len = chars.len();
        let mut i = 0;

        while i < len {
            let ch = chars[i];

            // Newline always ends a line comment.
            if ch == '\n' {
                in_line_comment = false;
                buf.push(ch);
                i += 1;
                continue;
            }

            if in_line_comment {
                buf.push(ch);
                i += 1;
                continue;
            }

            if in_single_quote {
                buf.push(ch);
                if ch == '\\' {
                    // MySQL-style backslash escape: `\'` keeps us inside the string.
                    // Consume the next character verbatim without interpreting it.
                    if i + 1 < len {
                        buf.push(chars[i + 1]);
                        i += 2;
                        continue;
                    }
                } else if ch == '\'' {
                    // `''` is the standard SQL escape for a literal single quote.
                    if i + 1 < len && chars[i + 1] == '\'' {
                        buf.push(chars[i + 1]);
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                i += 1;
                continue;
            }

            if in_double_quote {
                buf.push(ch);
                if ch == '"' {
                    if i + 1 < len && chars[i + 1] == '"' {
                        buf.push(chars[i + 1]);
                        i += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
                i += 1;
                continue;
            }

            // Outside any string/comment — check for special tokens.
            match ch {
                '\'' => {
                    in_single_quote = true;
                    buf.push(ch);
                }
                '"' => {
                    in_double_quote = true;
                    buf.push(ch);
                }
                '-' if i + 1 < len && chars[i + 1] == '-' => {
                    in_line_comment = true;
                    buf.push(ch);
                }
                ';' => {
                    buf.push(ch);
                    let trimmed = buf.trim().to_owned();
                    if !trimmed.is_empty() {
                        statements.push(trimmed);
                    }
                    buf.clear();
                }
                _ => {
                    buf.push(ch);
                }
            }
            i += 1;
        }

        // Flush any trailing content without a terminating semicolon.
        let trailing = buf.trim().to_owned();
        if !trailing.is_empty() {
            statements.push(trailing);
        }
        statements
    }

    /// Splits a multi-row `INSERT INTO t (...) VALUES (r1), (r2), ...` into
    /// individual single-row `INSERT INTO t (...) VALUES (rN)` statements so
    /// that a FK/constraint violation on one row does not abort insertion of
    /// all other valid rows.
    ///
    /// Any trailing clause (e.g. `ON CONFLICT DO NOTHING`) is preserved on
    /// every generated statement.  Non-INSERT statements or single-row INSERTs
    /// are returned unchanged (wrapped in a single-element Vec).
    pub(crate) fn split_insert_rows(stmt: &str) -> Vec<String> {
        // Quick bail-out: must contain VALUES keyword
        let upper = stmt.to_ascii_uppercase();
        let insert_pos = match upper.find("INSERT") {
            Some(p) => p,
            None => return vec![stmt.to_owned()],
        };
        let rel = match upper[insert_pos..].find("VALUES") {
            Some(p) => p,
            None => return vec![stmt.to_owned()],
        };
        let values_end = insert_pos + rel + 6; // byte index just after "VALUES"

        // Everything after "VALUES " — must start with '(' to be a VALUES list.
        let rest = stmt[values_end..].trim_start();
        if !rest.starts_with('(') {
            return vec![stmt.to_owned()];
        }

        // The INSERT header: "INSERT INTO schema.table (col, ...) VALUES"
        let header = stmt[..values_end].trim_end().to_owned();

        // Walk `rest` char-by-char, collecting top-level (…) groups as rows.
        let chars: Vec<char> = rest.chars().collect();
        let n = chars.len();
        let mut rows: Vec<String> = Vec::new();
        let mut depth: i32 = 0;
        let mut in_single = false;
        let mut in_double = false;
        let mut row_start: usize = 0;
        let mut last_row_end: usize = 0;
        let mut i = 0;

        while i < n {
            let ch = chars[i];

            if in_single {
                if ch == '\'' {
                    if i + 1 < n && chars[i + 1] == '\'' {
                        i += 2;
                        continue;
                    }
                    in_single = false;
                }
                i += 1;
                continue;
            }
            if in_double {
                if ch == '"' {
                    if i + 1 < n && chars[i + 1] == '"' {
                        i += 2;
                        continue;
                    }
                    in_double = false;
                }
                i += 1;
                continue;
            }

            match ch {
                '\'' => in_single = true,
                '"' => in_double = true,
                '(' => {
                    if depth == 0 {
                        row_start = i;
                    }
                    depth += 1;
                }
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        let row: String = chars[row_start..=i].iter().collect();
                        rows.push(row);
                        last_row_end = i;
                    }
                }
                _ => {}
            }
            i += 1;
        }

        // Nothing to split or only one row — return as-is.
        if rows.len() <= 1 {
            return vec![stmt.to_owned()];
        }

        // Anything after the last ')' is the trailing clause
        // (e.g. " ON CONFLICT DO NOTHING" or just ";" / empty).
        let suffix: String = chars[last_row_end + 1..].iter().collect();
        let suffix = suffix.trim().trim_end_matches(';').trim().to_owned();

        // Build one INSERT per row, reattaching the trailing clause.
        rows.into_iter()
            .map(|row| {
                if suffix.is_empty() {
                    format!("{} {};", header, row)
                } else {
                    format!("{} {} {};", header, row, suffix)
                }
            })
            .collect()
    }

    /// Returns `true` when the sqlx error is a transient network/pool error
    /// that may succeed on retry (TCP drop, connection pool exhaustion, etc.).
    pub(crate) fn is_transient_connection_error(err: &sqlx::Error) -> bool {
        matches!(
            err,
            sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed
        )
    }

    /// Ensure the `udb_seed_progress` checkpoint table exists.
    /// Created once per force-sync run; errors are silently ignored.
    pub(crate) async fn ensure_seed_progress_table(pool: &PgPool, ledger_schema: &str) {
        let seed_progress = ledger_relation(ledger_schema, "udb_seed_progress");
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {seed_progress} (\
                    artifact_path TEXT PRIMARY KEY, \
                    statement_index INTEGER NOT NULL, \
                    row_index INTEGER NOT NULL DEFAULT -1, \
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\
                )"
        );
        let _ = pool.execute(create_sql.as_str()).await;
        // Backfill row_index for checkpoint tables created before row-level
        // resume existed. -1 means "statement fully applied" (no partial rows).
        let alter_sql = format!(
            "ALTER TABLE {seed_progress} \
             ADD COLUMN IF NOT EXISTS row_index INTEGER NOT NULL DEFAULT -1"
        );
        let _ = pool.execute(alter_sql.as_str()).await;
    }

    /// Load the resume position for `artifact_path` as `(stmt_idx, row_idx)`:
    /// skip statements below `stmt_idx`, and within the statement *at* `stmt_idx`
    /// skip rows below `row_idx`. Returns `(0, 0)` when no checkpoint exists.
    ///
    /// `row_index = -1` in storage means the recorded statement was fully
    /// applied, so we resume at the next statement (`stmt + 1`, row 0). A
    /// non-negative `row_index = R` means the statement was only partially
    /// applied (interrupted mid per-row split), so we re-enter that statement
    /// and skip rows `<= R`.
    pub(crate) async fn load_seed_checkpoint(
        pool: &PgPool,
        ledger_schema: &str,
        artifact_path: &str,
    ) -> (usize, usize) {
        let seed_progress = ledger_relation(ledger_schema, "udb_seed_progress");
        let sql = format!(
            "SELECT statement_index, row_index \
             FROM {seed_progress} \
             WHERE artifact_path = $1"
        );
        match sqlx::query_as::<_, (i32, i32)>(&sql)
            .bind(artifact_path)
            .fetch_optional(pool)
            .await
        {
            Ok(Some((stmt, row))) => {
                if row < 0 {
                    ((stmt as usize).saturating_add(1), 0)
                } else {
                    (stmt as usize, (row as usize).saturating_add(1))
                }
            }
            _ => (0, 0),
        }
    }

    /// Persist that statement `stmt_idx` was applied **in full** (row_index = -1)
    /// so a subsequent interrupted run resumes at the next statement.
    pub(crate) async fn save_seed_checkpoint(
        pool: &PgPool,
        ledger_schema: &str,
        artifact_path: &str,
        stmt_idx: usize,
    ) {
        let seed_progress = ledger_relation(ledger_schema, "udb_seed_progress");
        let sql = format!(
            "INSERT INTO {seed_progress} \
                 (artifact_path, statement_index, row_index, updated_at) \
             VALUES ($1, $2, -1, NOW()) \
             ON CONFLICT (artifact_path) DO UPDATE SET \
                 statement_index = EXCLUDED.statement_index, \
                 row_index = -1, \
                 updated_at = NOW()"
        );
        let _ = sqlx::query(&sql)
            .bind(artifact_path)
            .bind(stmt_idx as i32)
            .execute(pool)
            .await;
    }

    /// Persist a **partial** checkpoint: every row up to and including `row_idx`
    /// of statement `stmt_idx` has been committed or skipped. Used between the
    /// sequential row-chunks of a split multi-row INSERT so a mid-statement
    /// interruption resumes without replaying already-committed rows.
    pub(crate) async fn save_seed_row_checkpoint(
        pool: &PgPool,
        ledger_schema: &str,
        artifact_path: &str,
        stmt_idx: usize,
        row_idx: usize,
    ) {
        let seed_progress = ledger_relation(ledger_schema, "udb_seed_progress");
        let sql = format!(
            "INSERT INTO {seed_progress} \
                 (artifact_path, statement_index, row_index, updated_at) \
             VALUES ($1, $2, $3, NOW()) \
             ON CONFLICT (artifact_path) DO UPDATE SET \
                 statement_index = EXCLUDED.statement_index, \
                 row_index = EXCLUDED.row_index, \
                 updated_at = NOW()"
        );
        let _ = sqlx::query(&sql)
            .bind(artifact_path)
            .bind(stmt_idx as i32)
            .bind(row_idx as i32)
            .execute(pool)
            .await;
    }

    /// Remove the checkpoint for `artifact_path` after it has been fully
    /// and successfully applied so stale data cannot affect future runs.
    pub(crate) async fn clear_seed_checkpoint(
        pool: &PgPool,
        ledger_schema: &str,
        artifact_path: &str,
    ) {
        let seed_progress = ledger_relation(ledger_schema, "udb_seed_progress");
        let sql = format!("DELETE FROM {seed_progress} WHERE artifact_path = $1");
        let _ = sqlx::query(&sql).bind(artifact_path).execute(pool).await;
    }

    /// Returns `true` when the sqlx error represents dirty MySQL export data
    /// that should be skipped during seed execution rather than aborting the
    /// entire artifact.  Covers two SQLSTATE classes:
    ///
    /// * Class 22 — Data Exception (numeric overflow, string truncation,
    ///   invalid text representation, etc.) — values from MySQL that don't
    ///   fit the PostgreSQL column type/precision.
    /// * Class 23 — Integrity Constraint Violation (FK, NOT NULL, unique,
    ///   check, exclusion) — orphaned references that were never enforced
    ///   on the MySQL side.
    pub(crate) fn is_skippable_seed_error(err: &sqlx::Error) -> bool {
        if let sqlx::Error::Database(db_err) = err {
            db_err
                .code()
                .as_deref()
                .map(|c| {
                    c.starts_with("22") // data exception
                    || c.starts_with("23") // integrity constraint violation
                    || c == "42703" // undefined_column — schema mismatch in seed row
                    || c == "57014" // query_canceled (statement_timeout) — row hung, skip
                })
                .unwrap_or(false)
        } else {
            false
        }
    }

    pub async fn execute_raw_sql(&self, sql: &str, label: &str) -> Result<(), tonic::Status> {
        self.pg_pool()?
            .execute(sql)
            .await
            .map_err(|err| tonic::Status::internal(format!("failed to execute {label}: {err}")))?;
        Ok(())
    }

    /// Load the most recently applied [`CatalogManifest`] from `proto_schema_versions`.
    ///
    /// Returns `None` when no manifest has been recorded yet (first-ever migration run).
    ///
    /// Uses a two-phase fetch to avoid transferring the multi-MB manifest_json JSONB
    /// blob on every startup. Phase 1 fetches only the checksum (tiny, index-only scan).
    /// Phase 2 fetches the full JSON only when needed (exact primary-key lookup).
    /// This eliminates the 3-5s slow-query warning on cloud/Neon PostgreSQL instances.
    pub async fn load_last_manifest(&self) -> Result<Option<CatalogManifest>, tonic::Status> {
        let pool = self.pg_pool()?;
        let proto_schema_versions = ledger_relation(
            &self.config.migration.ledger_schema,
            "proto_schema_versions",
        );
        // Pin both queries to one connection so that:
        //  a) Neon cold-start latency is paid once (the first SELECT warms the compute).
        //  b) Phase 2 re-uses the already-warm backend — avoids the 5-9 s cold-start
        //     that produced the "slow statement" warning on `manifest_json::TEXT`.
        let mut conn = pool.acquire().await.map_err(|err| {
            tonic::Status::internal(format!("load_last_manifest: acquire failed: {err}"))
        })?;
        // Phase 1: fetch only the checksum — covered by the composite index on
        // (applied_at DESC NULLS LAST, id DESC), returning just a TEXT column.
        let checksum_sql = format!(
            "SELECT manifest_checksum \
             FROM {proto_schema_versions} \
             ORDER BY applied_at DESC NULLS LAST, id DESC \
             LIMIT 1"
        );
        let checksum_row: Option<(String,)> = sqlx::query_as(&checksum_sql)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!(
                    "proto_schema_versions checksum query failed: {err}"
                ))
            })?;
        let checksum = match checksum_row {
            None => return Ok(None),
            Some((c,)) => c,
        };
        // Phase 2: fetch full JSON by primary key — always a fast index lookup.
        // Re-uses the same warm connection from phase 1, avoiding a second cold-start.
        let json_sql = format!(
            "SELECT manifest_json::TEXT \
             FROM {proto_schema_versions} \
             WHERE manifest_checksum = $1"
        );
        let json_row: Option<(String,)> = sqlx::query_as(&json_sql)
            .bind(&checksum)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!("proto_schema_versions json query failed: {err}"))
            })?;
        decode_catalog_manifest_row(json_row)
    }

    /// Return the latest manifest checksum without reading the large JSONB body.
    ///
    /// Dry-run planning uses this to avoid the expensive `manifest_json::TEXT`
    /// fetch when the live checksum already matches the startup manifest.
    pub async fn load_last_manifest_checksum_if_exists(
        &self,
    ) -> Result<Option<String>, tonic::Status> {
        let pool = self.pg_pool()?;
        let proto_schema_versions = ledger_relation(
            &self.config.migration.ledger_schema,
            "proto_schema_versions",
        );
        let proto_schema_versions_regclass = ledger_regclass_name(
            &self.config.migration.ledger_schema,
            "proto_schema_versions",
        );
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(proto_schema_versions_regclass)
            .fetch_one(pool)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!(
                    "proto_schema_versions existence query failed: {err}"
                ))
            })?;
        if !exists {
            return Ok(None);
        }

        let checksum_sql = format!(
            "SELECT manifest_checksum \
             FROM {proto_schema_versions} \
             ORDER BY applied_at DESC NULLS LAST, id DESC \
             LIMIT 1"
        );
        sqlx::query_scalar(&checksum_sql)
            .fetch_optional(pool)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!(
                    "proto_schema_versions checksum query failed: {err}"
                ))
            })
    }

    /// Load a specific manifest by checksum.
    pub async fn load_manifest_by_checksum(
        &self,
        checksum: &str,
    ) -> Result<Option<CatalogManifest>, tonic::Status> {
        let pool = self.pg_pool()?;
        let proto_schema_versions = ledger_relation(
            &self.config.migration.ledger_schema,
            "proto_schema_versions",
        );
        let json_sql = format!(
            "SELECT manifest_json::TEXT \
             FROM {proto_schema_versions} \
             WHERE manifest_checksum = $1"
        );
        let json_row: Option<(String,)> = sqlx::query_as(&json_sql)
            .bind(checksum)
            .fetch_optional(pool)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!("proto_schema_versions json query failed: {err}"))
            })?;
        decode_catalog_manifest_row(json_row)
    }

    /// Load a specific manifest with a transaction-local statement timeout.
    ///
    /// This is intended for dry-run planning, where a stale/cold cloud database
    /// should not make a preview command wait minutes on a large JSONB manifest.
    pub async fn load_manifest_by_checksum_with_statement_timeout(
        &self,
        checksum: &str,
        timeout: Duration,
    ) -> Result<Option<CatalogManifest>, tonic::Status> {
        let pool = self.pg_pool()?;
        let proto_schema_versions = ledger_relation(
            &self.config.migration.ledger_schema,
            "proto_schema_versions",
        );
        let mut tx = pool.begin().await.map_err(|err| {
            tonic::Status::internal(format!(
                "proto_schema_versions json transaction failed: {err}"
            ))
        })?;
        let timeout_ms = timeout.as_millis().max(1);
        let timeout_value = format!("{timeout_ms}ms");
        sqlx::query("SELECT set_config('statement_timeout', $1, true)")
            .bind(timeout_value)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!(
                    "proto_schema_versions statement_timeout setup failed: {err}"
                ))
            })?;

        let json_sql = format!(
            "SELECT manifest_json::TEXT \
             FROM {proto_schema_versions} \
             WHERE manifest_checksum = $1"
        );
        let json_row: Option<(String,)> = sqlx::query_as(&json_sql)
            .bind(checksum)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!("proto_schema_versions json query failed: {err}"))
            })?;

        tx.commit().await.map_err(|err| {
            tonic::Status::internal(format!(
                "proto_schema_versions json transaction commit failed: {err}"
            ))
        })?;
        decode_catalog_manifest_row(json_row)
    }

    /// Upsert the current [`CatalogManifest`] into `proto_schema_versions` and mark it applied.
    ///
    /// Called at the end of every successful migration run so that the next run can
    /// compute a precise diff and generate only the delta ALTER statements needed.
    pub async fn save_manifest(&self, manifest: &CatalogManifest) -> Result<(), tonic::Status> {
        self.save_manifest_inner(manifest, None).await
    }

    /// Save the manifest only if the applied-manifest ledger still matches the
    /// checksum observed before the migration run began.
    ///
    /// This closes the apply→verify→save TOCTOU window: if another UDB instance
    /// advances `proto_schema_versions` while this instance is applying or
    /// verifying, the save aborts instead of overwriting the newer manifest's
    /// `applied_at` ordering.
    pub async fn save_manifest_if_latest(
        &self,
        manifest: &CatalogManifest,
        expected_latest_checksum: Option<&str>,
    ) -> Result<(), tonic::Status> {
        self.save_manifest_inner(manifest, Some(expected_latest_checksum))
            .await
    }

    async fn save_manifest_inner(
        &self,
        manifest: &CatalogManifest,
        expected_latest_checksum: Option<Option<&str>>,
    ) -> Result<(), tonic::Status> {
        let pool = self.pg_pool()?;
        let proto_schema_versions = ledger_relation(
            &self.config.migration.ledger_schema,
            "proto_schema_versions",
        );

        // Acquire one connection and hold it for all operations so that the
        // ping and the INSERT (or UPDATE) share the same backend.  This
        // prevents sqlx from giving us a different, potentially dead
        // connection from the pool for the heavy write after the ping.
        let mut tx = pool.begin().await.map_err(|err| {
            tonic::Status::internal(format!("save_manifest: transaction begin failed: {err}"))
        })?;

        // Serialize the latest-check and save against other lifecycle writers.
        let lock_sql = format!("LOCK TABLE {proto_schema_versions} IN SHARE ROW EXCLUSIVE MODE");
        sqlx::query(&lock_sql)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!("proto_schema_versions lock failed: {err}"))
            })?;

        let latest_sql = format!(
            "SELECT manifest_checksum \
             FROM {proto_schema_versions} \
             ORDER BY applied_at DESC NULLS LAST, id DESC \
             LIMIT 1"
        );
        let latest_checksum: Option<String> = sqlx::query_scalar(&latest_sql)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!(
                    "proto_schema_versions latest checksum query failed: {err}"
                ))
            })?;
        if let Some(expected_latest_checksum) = expected_latest_checksum
            && latest_checksum.as_deref() != expected_latest_checksum
        {
            return Err(tonic::Status::aborted(format!(
                "manifest ledger advanced during migration lifecycle: expected latest checksum {:?}, found {:?}",
                expected_latest_checksum, latest_checksum
            )));
        }

        // If the manifest checksum is already recorded, only touch `applied_at`.
        // The manifest JSON for a given checksum is immutable, so re-inserting
        // the full multi-MB JSONB document on every run is wasteful and is the
        // dominant cause of the slow-statement warning on Neon.
        let exists_sql = format!(
            "SELECT EXISTS(SELECT 1 FROM {proto_schema_versions} WHERE manifest_checksum = $1)"
        );
        let already_exists: bool = sqlx::query_scalar(&exists_sql)
            .bind(&manifest.checksum_sha256)
            .fetch_one(&mut *tx)
            .await
            .unwrap_or(false);

        if already_exists {
            let update_sql = format!(
                "UPDATE {proto_schema_versions}
                 SET applied_at = NOW()
                 WHERE manifest_checksum = $1"
            );
            sqlx::query(&update_sql)
                .bind(&manifest.checksum_sha256)
                .execute(&mut *tx)
                .await
                .map_err(|err| {
                    tonic::Status::internal(format!("proto_schema_versions touch failed: {err}"))
                })?;
            tx.commit().await.map_err(|err| {
                tonic::Status::internal(format!("proto_schema_versions touch commit failed: {err}"))
            })?;
            return Ok(());
        }

        let json = serde_json::to_string(manifest)
            .map_err(|err| tonic::Status::internal(format!("manifest serialise failed: {err}")))?;
        let insert_sql = format!(
            "INSERT INTO {proto_schema_versions}
                 (manifest_checksum, manifest_json, generator_version, applied_at)
             VALUES ($1, $2::jsonb, $3, NOW())
             ON CONFLICT (manifest_checksum) DO UPDATE
                 SET applied_at = NOW()"
        );
        sqlx::query(&insert_sql)
            .bind(&manifest.checksum_sha256)
            .bind(&json)
            .bind(&manifest.generator_version)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!("proto_schema_versions upsert failed: {err}"))
            })?;
        tx.commit().await.map_err(|err| {
            tonic::Status::internal(format!("proto_schema_versions upsert commit failed: {err}"))
        })?;
        Ok(())
    }

    /// Verify the live PostgreSQL schema against the manifest, returning
    /// human-readable drift findings. Thin string wrapper over
    /// [`verify_postgres_manifest_drift`] preserving the historical output shape
    /// used by the startup fail-closed path and its tests.
    pub async fn verify_postgres_manifest(
        &self,
        manifest: &CatalogManifest,
    ) -> Result<Vec<String>, tonic::Status> {
        Ok(self
            .verify_postgres_manifest_drift(manifest)
            .await?
            .into_iter()
            .map(|d| d.message)
            .collect())
    }

    /// Verify the live PostgreSQL schema against the manifest, returning
    /// **structured** drift findings keyed by repair `kind`. Used by emergency
    /// auto-alter (#133) to feed real DB-vs-proto drift into `plan_repairs`.
    pub async fn verify_postgres_manifest_drift(
        &self,
        manifest: &CatalogManifest,
    ) -> Result<Vec<ManifestDrift>, tonic::Status> {
        let pool = self.pg_pool()?;
        let mut drift = Vec::new();

        // Collect all expected schema names so we can scope the bulk queries.
        let schemas: Vec<&str> = manifest
            .tables
            .iter()
            .map(|t| t.schema.as_str())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        // Collect the exact table names we need to verify so the query
        // only returns rows for manifest tables, not every object in those
        // schemas.  This is the key optimisation: on a large database the
        // schemas contain far more tables than the manifest declares, so
        // filtering on both schema AND table cuts the row count by ~8×.
        let table_names: Vec<&str> = manifest
            .tables
            .iter()
            .map(|t| t.table.as_str())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        // ── Single round-trip: fetch tables, columns, indexes, triggers, and RLS ──
        //
        // Previously this was 5 separate queries against information_schema views,
        // which do per-row privilege checks and are extremely slow on large databases
        // (>10s for 2000+ columns).  A single UNION ALL over pg_catalog avoids all
        // permission-check joins and costs only one Neon/network round-trip.
        // $1 = schema names, $2 = table names (both from the manifest).
        //
        // Row shape: (kind, schema_name, table_name, extra)
        //   't' → (schema, table, "")          — regular table exists
        //   'c' → (schema, table, column_name) — column exists
        //   'i' → (schema, table, index_name)  — index exists
        //   'g' → (schema, table, trigger_name)— trigger exists
        //   'r' → (schema, table, "rls")       — RLS enabled on table
        // Run introspection inside a transaction with SET LOCAL so the elevated
        // statement_timeout is scoped to this connection and auto-resets on
        // commit. A plain pool-level SET + reset could leak the timeout to the
        // next pool borrower, because the reset may land on a different
        // connection than the one that ran the query.
        let mut introspection_tx = pool.begin().await.map_err(|err| {
            tonic::Status::internal(format!(
                "pg_catalog introspection: failed to begin transaction: {err}"
            ))
        })?;
        if let Err(err) = sqlx::query("SET LOCAL statement_timeout = '60s'")
            .execute(&mut *introspection_tx)
            .await
        {
            tracing::warn!(error = %err, "failed to set statement_timeout for pg_catalog introspection");
        }
        let introspection_rows: Vec<(String, String, String, String)> =
            sqlx::query_as::<_, (String, String, String, String)>(
                r#"
SELECT kind, schema_name, table_name, extra FROM (
    SELECT 't'        AS kind,
           n.nspname  AS schema_name,
           c.relname  AS table_name,
           ''         AS extra
    FROM   pg_catalog.pg_class     c
    JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE  c.relkind IN ('r', 'v', 'm', 'f', 'p')
      AND  n.nspname = ANY($1)
      AND  c.relname = ANY($2)

    UNION ALL

    SELECT 'c',
           n.nspname,
           c.relname,
           a.attname
    FROM   pg_catalog.pg_attribute  a
    JOIN   pg_catalog.pg_class      c ON c.oid = a.attrelid
    JOIN   pg_catalog.pg_namespace  n ON n.oid = c.relnamespace
    WHERE  c.relkind IN ('r', 'v', 'm', 'f', 'p')
      AND  a.attnum > 0
      AND  NOT a.attisdropped
      AND  n.nspname = ANY($1)
      AND  c.relname = ANY($2)

    UNION ALL

    SELECT 'i',
           schemaname,
           tablename,
           indexname
    FROM   pg_catalog.pg_indexes
    WHERE  schemaname = ANY($1)
      AND  tablename  = ANY($2)

    UNION ALL

    SELECT 'g',
           n.nspname,
           c.relname,
           t.tgname
    FROM   pg_catalog.pg_trigger    t
    JOIN   pg_catalog.pg_class      c ON c.oid = t.tgrelid
    JOIN   pg_catalog.pg_namespace  n ON n.oid = c.relnamespace
    WHERE  NOT t.tgisinternal
      AND  n.nspname = ANY($1)
      AND  c.relname = ANY($2)

    UNION ALL

    SELECT 'r',
           n.nspname,
           c.relname,
           'rls'
    FROM   pg_catalog.pg_class     c
    JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE  c.relrowsecurity = true
      AND  n.nspname = ANY($1)
      AND  c.relname = ANY($2)
) q
                "#,
            )
            .bind(&schemas)
            .bind(&table_names)
            .fetch_all(&mut *introspection_tx)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!("pg_catalog schema introspection failed: {err}"))
            })?;
        introspection_tx.commit().await.map_err(|err| {
            tonic::Status::internal(format!(
                "pg_catalog introspection: failed to commit transaction: {err}"
            ))
        })?;

        let mut existing_tables: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();
        let mut existing_columns: std::collections::HashSet<(String, String, String)> =
            std::collections::HashSet::new();
        let mut existing_indexes: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut existing_triggers: std::collections::HashSet<(String, String, String)> =
            std::collections::HashSet::new();
        let mut rls_enabled_tables: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();

        for (kind, s, t, x) in introspection_rows {
            match kind.as_str() {
                "t" => {
                    existing_tables.insert((s, t));
                }
                "c" => {
                    existing_columns.insert((s, t, x));
                }
                "i" => {
                    existing_indexes.insert(x);
                }
                "g" => {
                    existing_triggers.insert((s, t, x));
                }
                "r" => {
                    rls_enabled_tables.insert((s, t));
                }
                _ => {}
            }
        }

        // ── 4. Diff in memory ────────────────────────────────────────────────
        for table in &manifest.tables {
            if !existing_tables.contains(&(table.schema.clone(), table.table.clone())) {
                drift.push(ManifestDrift {
                    kind: "missing_table".to_string(),
                    schema: table.schema.clone(),
                    table: table.table.clone(),
                    column: String::new(),
                    sql_type: String::new(),
                    default_value: String::new(),
                    message: format!("missing PostgreSQL table {}.{}", table.schema, table.table),
                });
                continue;
            }
            for column in &table.columns {
                if !existing_columns.contains(&(
                    table.schema.clone(),
                    table.table.clone(),
                    column.column_name.clone(),
                )) {
                    drift.push(ManifestDrift {
                        kind: "missing_column".to_string(),
                        schema: table.schema.clone(),
                        table: table.table.clone(),
                        column: column.column_name.clone(),
                        sql_type: column.sql_type.clone(),
                        default_value: column.default_value.clone(),
                        message: format!(
                            "missing PostgreSQL column {}.{}.{}",
                            table.schema, table.table, column.column_name
                        ),
                    });
                }
            }

            // GAP 11: Verify declared indexes actually exist.
            for index in &table.indexes {
                let index_name = if index.name.trim().is_empty() {
                    format!(
                        "idx_{}_{}_{}",
                        table.schema,
                        table.table,
                        index.columns.join("_")
                    )
                } else {
                    index.name.clone()
                };
                if !existing_indexes.contains(&index_name) {
                    drift.push(ManifestDrift {
                        kind: "missing_index".to_string(),
                        schema: table.schema.clone(),
                        table: table.table.clone(),
                        column: index.columns.join(","),
                        sql_type: String::new(),
                        default_value: String::new(),
                        message: format!(
                            "missing index {} on {}.{}",
                            index_name, table.schema, table.table
                        ),
                    });
                }
            }

            // GAP 11: Verify declared triggers actually exist.
            for trigger in &table.triggers {
                if !existing_triggers.contains(&(
                    table.schema.clone(),
                    table.table.clone(),
                    trigger.name.clone(),
                )) {
                    drift.push(ManifestDrift {
                        kind: "missing_trigger".to_string(),
                        schema: table.schema.clone(),
                        table: table.table.clone(),
                        column: String::new(),
                        sql_type: String::new(),
                        default_value: String::new(),
                        message: format!(
                            "missing trigger {} on {}.{}",
                            trigger.name, table.schema, table.table
                        ),
                    });
                }
            }

            // GAP 11: Verify RLS is enabled when the manifest declares RLS policies.
            if !table.rls_policies.is_empty()
                && !rls_enabled_tables.contains(&(table.schema.clone(), table.table.clone()))
            {
                drift.push(ManifestDrift {
                    kind: "rls_enabled_no_policies".to_string(),
                    schema: table.schema.clone(),
                    table: table.table.clone(),
                    column: String::new(),
                    sql_type: String::new(),
                    default_value: String::new(),
                    message: format!(
                        "RLS not enabled on {}.{} but {} polic(ies) declared",
                        table.schema,
                        table.table,
                        table.rls_policies.len()
                    ),
                });
            }
        }
        Ok(drift)
    }

    pub async fn cdc_outbox_metrics(&self) -> Result<(f64, i64), tonic::Status> {
        let pool = self.pg_pool()?;
        let cdc_config = self.config.cdc.clone();
        let sql = format!(
            "SELECT
                 COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at))), 0)::DOUBLE PRECISION AS lag_seconds,
                 COUNT(*)::BIGINT AS depth
             FROM {}",
            cdc_config.outbox_relation()
        );
        let row = sqlx::query(&sql).fetch_one(pool).await.map_err(|err| {
            tonic::Status::internal(format!("CDC outbox metrics query failed: {err}"))
        })?;
        Ok((
            row.try_get::<f64, _>("lag_seconds").unwrap_or_default(),
            row.try_get::<i64, _>("depth").unwrap_or_default(),
        ))
    }

    pub async fn ensure_system_catalog(&self) -> Result<SystemCatalogReport, tonic::Status> {
        ensure_system_catalog(self.pg_pool()?).await
    }

    pub async fn inspect_system_catalog(&self) -> Result<SystemCatalogInspection, tonic::Status> {
        inspect_system_catalog(self.pg_pool()?).await
    }

    pub async fn ensure_qdrant_store(&self, store: &ManifestStore) -> Result<(), tonic::Status> {
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = store;
            return Err(tonic::Status::failed_precondition(
                "qdrant/vector feature is not enabled",
            ));
        }
        #[cfg(feature = "qdrant")]
        {
            let qdrant = self.qdrant()?;
            qdrant.ensure_collection(store).await
        }
    }

    pub async fn verify_qdrant_store(&self, store: &ManifestStore) -> Result<(), tonic::Status> {
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = store;
            return Err(tonic::Status::failed_precondition(
                "qdrant/vector feature is not enabled",
            ));
        }
        #[cfg(feature = "qdrant")]
        {
            let qdrant = self.qdrant()?;
            qdrant.collection_exists(&store.resource_name).await
        }
    }

    pub async fn ensure_s3_bucket(&self, store: &ManifestStore) -> Result<(), tonic::Status> {
        #[cfg(not(feature = "s3"))]
        {
            let _ = store;
            return Err(tonic::Status::failed_precondition(
                "s3/object-store feature is not enabled",
            ));
        }
        #[cfg(feature = "s3")]
        {
            let s3 = self.s3()?;
            let exists = s3
                .head_bucket()
                .bucket(&store.resource_name)
                .send()
                .await
                .is_ok();
            if !exists {
                s3.create_bucket()
                    .bucket(&store.resource_name)
                    .send()
                    .await
                    .map_err(|err| {
                        tonic::Status::unavailable(format!(
                            "failed to create S3/MinIO bucket {}: {err}",
                            store.resource_name
                        ))
                    })?;
            }
            Ok(())
        }
    }

    pub async fn verify_s3_bucket(&self, store: &ManifestStore) -> Result<(), tonic::Status> {
        #[cfg(not(feature = "s3"))]
        {
            let _ = store;
            return Err(tonic::Status::failed_precondition(
                "s3/object-store feature is not enabled",
            ));
        }
        #[cfg(feature = "s3")]
        {
            self.s3()?
                .head_bucket()
                .bucket(&store.resource_name)
                .send()
                .await
                .map_err(|err| {
                    tonic::Status::unavailable(format!(
                        "S3/MinIO bucket {} verification failed: {err}",
                        store.resource_name
                    ))
                })?;
            Ok(())
        }
    }

    pub fn emit_drift_metric(&self, reason: &str) {
        // Do NOT label every startup failure "drift". The word
        // "drift" tells an operator the relational schema diverged from the
        // manifest — sending them to inspect tables/partitions/migrations. But
        // the same path also fires for a backend that simply has no config
        // (qdrant/s3 not set) or a configured backend that rejected
        // provisioning/verification — none of which are schema drift. Classify
        // the reason into a coarse category and emit a category-accurate
        // headline so the audit trail and dashboards can tell them apart.
        let (category, headline) = startup_failure_category(reason);
        tracing::error!(
            metric = "udb.migration.drift_detected",
            reason = reason,
            category = category,
            "UDB startup {category}: {headline}"
        );
    }
}

/// Classify a startup-failure `reason` into a coarse, operator-meaningful
/// category + headline.
///
/// - `schema_drift` — the live relational schema differs from the manifest.
/// - `backend_missing` — a required non-Postgres backend has no configuration.
/// - `backend_provision_failed` — a configured backend rejected provisioning.
/// - `backend_verify_failed` — a configured resource exists but failed checks.
/// - `startup_error` — anything else (lint/approval/hook), so we never imply
///   schema drift for an unrelated failure.
fn startup_failure_category(reason: &str) -> (&'static str, &'static str) {
    match reason {
        "postgres_manifest_mismatch" | "schema_drift" => (
            "schema_drift",
            "live relational schema differs from the proto manifest",
        ),
        "postgres_unavailable" | "qdrant_required" | "s3_required" | "backend_missing" => {
            ("backend_missing", "a required backend is not configured")
        }
        "qdrant_apply" | "s3_apply" | "backend_apply" | "backend_provision_failed" => (
            "backend_provision_failed",
            "a configured backend rejected provisioning",
        ),
        "qdrant_verify" | "s3_verify" | "backend_verify" | "backend_verify_failed" => (
            "backend_verify_failed",
            "a configured backend resource failed verification",
        ),
        _ => ("startup_error", "startup step failed"),
    }
}

/// Build the `schema_migrations` 'applied' upsert as a literal SQL statement
/// (inlined, single-quote-escaped values) so it can be appended INSIDE an
/// artifact's own `BEGIN/COMMIT` batch — letting the DDL and its ledger record
/// commit atomically in one round-trip. Mirrors the bound-parameter upsert used
/// on the slow path; `proto_manifest_checksum` is stored as text ('' when the
/// header carries none, matching the legacy bound-param behaviour).
fn applied_ledger_upsert_sql(
    schema_migrations: &str,
    artifact: &GeneratedArtifact,
    checksum: &str,
) -> String {
    fn lit(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }
    format!(
        "INSERT INTO {sm} \
         (filename, checksum, state, migration_kind, proto_manifest_checksum, source_schema, source_table, operation_kind) \
         VALUES ({f}, {c}, 'applied', {k}, {pmc}, {ss}, {st}, {k}) \
         ON CONFLICT (filename) DO UPDATE SET \
            checksum = EXCLUDED.checksum, \
            state = 'applied', \
            applied_at = NOW(), \
            migration_kind = EXCLUDED.migration_kind, \
            proto_manifest_checksum = EXCLUDED.proto_manifest_checksum, \
            source_schema = EXCLUDED.source_schema, \
            source_table = EXCLUDED.source_table, \
            operation_kind = EXCLUDED.operation_kind;",
        sm = schema_migrations,
        f = lit(&artifact.rel_path),
        c = lit(checksum),
        k = lit(&artifact.kind),
        pmc = lit(&extract_manifest_checksum(&artifact.content)),
        ss = lit(&artifact.schema),
        st = lit(&artifact.table),
    )
}

/// Emit one throttled migration-progress heartbeat: a structured
/// `tracing::info!` line AND a best-effort update of the queryable
/// `migration_runtime_state` singleton row, so an operator can tell the broker
/// is doing useful work even when a single SQL statement is slow. The DB write
/// is best-effort — a failure here must never abort the migration.
async fn migration_heartbeat(
    pool: &PgPool,
    ledger_schema: &str,
    mode: &str,
    current: &str,
    applied: usize,
    total: usize,
    started: std::time::Instant,
) {
    let elapsed = started.elapsed().as_secs_f64();
    tracing::info!(
        applied,
        total,
        current,
        elapsed_secs = elapsed,
        "udb {mode} progress: {applied}/{total} artifacts applied, current={current}, elapsed={elapsed:.1}s"
    );
    let rel = ledger_relation(ledger_schema, "migration_runtime_state");
    let note = format!("{mode}: applied {applied}/{total} (current={current})");
    let sql = format!(
        "UPDATE {rel} SET active_file = $1, last_note = $2, updated_at = NOW() WHERE singleton = TRUE"
    );
    if let Err(err) = sqlx::query(&sql)
        .bind(current)
        .bind(&note)
        .execute(pool)
        .await
    {
        tracing::debug!(error = %err, "migration progress heartbeat DB update failed (best-effort)");
    }
}

fn ledger_relation(schema: &str, table: &str) -> String {
    format!(
        "{}.{}",
        pg_ident(normalize_ledger_schema(schema)),
        pg_ident(table)
    )
}

fn ledger_regclass_name(schema: &str, table: &str) -> String {
    format!(
        "{}.{}",
        pg_ident(normalize_ledger_schema(schema)),
        pg_ident(table)
    )
}

fn normalize_ledger_schema(schema: &str) -> &str {
    let schema = schema.trim();
    if schema.is_empty() {
        DEFAULT_LEDGER_SCHEMA
    } else {
        schema
    }
}

fn pg_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
