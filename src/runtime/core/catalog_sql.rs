//! Continuation `impl DataBrokerRuntime` block (Phase F split of core.rs).
use super::*;

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
    pub async fn execute_sql_artifacts(
        &self,
        artifacts: &[GeneratedArtifact],
    ) -> Result<(), tonic::Status> {
        self.execute_sql_artifacts_internal(artifacts, true).await
    }

    pub async fn execute_sql_artifacts_serial(
        &self,
        artifacts: &[GeneratedArtifact],
    ) -> Result<(), tonic::Status> {
        self.execute_sql_artifacts_internal(artifacts, false).await
    }

    pub(crate) async fn execute_sql_artifacts_internal(
        &self,
        artifacts: &[GeneratedArtifact],
        allow_parallel: bool,
    ) -> Result<(), tonic::Status> {
        let pool = self.pg_pool()?;

        // GAP 35 / GAP 44: Load already-applied artifact filename+checksum pairs
        // from the migration ledger. A stable rel_path alone is not enough: the
        // artifact may have changed while keeping the same file name.
        // On the next startup after a partial failure, artifacts that committed
        // successfully with the same content are skipped rather than re-applied.
        let applied_set: std::collections::HashSet<(String, String)> = match sqlx::query_as(
            "SELECT filename, checksum FROM public.schema_migrations WHERE state = 'applied'",
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
                        "udb force-sync: force-reseed seed {} | computed={}",
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
                        "udb force-sync: pending {} | computed={} | db={}",
                        artifact.rel_path,
                        &checksum[..16.min(checksum.len())],
                        &db_checksum[..16.min(db_checksum.len())]
                    );
                }
                force_reseed_seed || !is_applied
            })
            .collect::<Vec<_>>();
        eprintln!("udb force-sync: {} pending SQL artifact(s)", pending.len());

        if !allow_parallel {
            for artifact in &pending {
                eprintln!("udb force-sync: applying {}", artifact.rel_path);
                Self::apply_sql_artifact(pool, artifact, self.config.migration.force_reseed)
                    .await?;
                eprintln!("udb force-sync: applied {}", artifact.rel_path);
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
            eprintln!("udb force-sync: applying {}", artifact.rel_path);
            Self::apply_sql_artifact(pool, artifact, self.config.migration.force_reseed).await?;
            eprintln!("udb force-sync: applied {}", artifact.rel_path);
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
                async move {
                    // Hold the semaphore permit for the full duration of this DDL.
                    let _permit = sem
                        .acquire_owned()
                        .await
                        .expect("DDL semaphore unexpectedly closed");

                    eprintln!("udb force-sync: applying {}", artifact.rel_path);
                    let result = Self::apply_sql_artifact(&pool, artifact, force_reseed).await;
                    if result.is_ok() {
                        eprintln!("udb force-sync: applied {}", artifact.rel_path);
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
            eprintln!("udb force-sync: applying {}", artifact.rel_path);
            Self::apply_sql_artifact(pool, artifact, self.config.migration.force_reseed).await?;
            eprintln!("udb force-sync: applied {}", artifact.rel_path);
        }
        Ok(())
    }

    pub(crate) async fn apply_sql_artifact(
        pool: &PgPool,
        artifact: &GeneratedArtifact,
        force_reseed: bool,
    ) -> Result<(), tonic::Status> {
        let checksum = artifact_content_checksum(&artifact.content);
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
        let _ = sqlx::query(
            "INSERT INTO public.schema_migrations \
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
             WHERE public.schema_migrations.checksum IS DISTINCT FROM EXCLUDED.checksum \
                OR public.schema_migrations.state IS DISTINCT FROM 'applied'",
        )
        .bind(&artifact.rel_path)
        .bind(&checksum)
        .bind(&artifact.kind)
        .bind(extract_manifest_checksum(&artifact.content))
        .bind(&artifact.schema)
        .bind(&artifact.table)
        .bind(&artifact.kind)
        .execute(pool)
        .await;

        // Large seed files (> 1 MiB) are split into individual ;-terminated
        // statements and executed one at a time.  Sending a 39 MB INSERT as a
        // single pool.execute() call takes ~300 s, after which Windows TCP
        // (OS error 10053 / WSAECONNABORTED) drops the idle-seeming connection
        // before the schema_migrations ledger entry can be written.  Chunked
        // execution keeps each round-trip short and the TCP connection alive.
        const CHUNK_THRESHOLD: usize = 1024 * 1024; // 1 MiB
        // Seeds are always executed statement-by-statement so that individual
        // rows with integrity constraint violations (FK, NOT NULL, etc.) from
        // the MySQL source can be skipped without aborting the whole artifact.
        let is_seed = artifact.kind == "seed";
        if artifact.content.len() > CHUNK_THRESHOLD || is_seed {
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
            let start_stmt_idx: usize = if is_seed {
                Self::ensure_seed_progress_table(pool).await;
                if force_reseed {
                    Self::clear_seed_checkpoint(pool, &artifact.rel_path).await;
                    0
                } else {
                    Self::load_seed_checkpoint(pool, &artifact.rel_path).await
                }
            } else {
                0
            };
            if start_stmt_idx > 0 {
                tracing::info!(
                    artifact = %artifact.rel_path,
                    resuming_from = start_stmt_idx,
                    "seed resume: skipping {} already-applied statement(s)",
                    start_stmt_idx,
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
                                Self::save_seed_checkpoint(pool, &artifact.rel_path, idx).await;
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
                                    'chunks: for (chunk_i, chunk) in
                                        row_stmts.chunks(ROW_CHUNK).enumerate()
                                    {
                                        let chunk_start = chunk_i * ROW_CHUNK;
                                        let mut futs: FuturesUnordered<_> = chunk
                                            .iter()
                                            .cloned()
                                            .enumerate()
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
                                Self::save_seed_checkpoint(pool, &artifact.rel_path, idx).await;
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
                Self::clear_seed_checkpoint(pool, &artifact.rel_path).await;
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

        let _ = sqlx::query(
            "UPDATE public.schema_migrations \
             SET state = 'applied', applied_at = NOW(), checksum = $2 \
             WHERE filename = $1",
        )
        .bind(&artifact.rel_path)
        .bind(&checksum)
        .execute(pool)
        .await;
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
    pub(crate) async fn ensure_seed_progress_table(pool: &PgPool) {
        let _ = pool
            .execute(
                "CREATE TABLE IF NOT EXISTS public.udb_seed_progress (\
                    artifact_path TEXT PRIMARY KEY, \
                    statement_index INTEGER NOT NULL, \
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\
                )",
            )
            .await;
    }

    /// Load the last successfully applied statement index for `artifact_path`.
    /// Returns the index of the **next** statement to run (checkpoint + 1),
    /// or 0 if no checkpoint exists.
    pub(crate) async fn load_seed_checkpoint(pool: &PgPool, artifact_path: &str) -> usize {
        match sqlx::query_as::<_, (i32,)>(
            "SELECT statement_index \
             FROM public.udb_seed_progress \
             WHERE artifact_path = $1",
        )
        .bind(artifact_path)
        .fetch_optional(pool)
        .await
        {
            Ok(Some((idx,))) => (idx as usize).saturating_add(1),
            _ => 0,
        }
    }

    /// Persist the index of the last successfully applied statement so a
    /// subsequent interrupted run can resume from the right position.
    pub(crate) async fn save_seed_checkpoint(pool: &PgPool, artifact_path: &str, stmt_idx: usize) {
        let _ = sqlx::query(
            "INSERT INTO public.udb_seed_progress \
                 (artifact_path, statement_index, updated_at) \
             VALUES ($1, $2, NOW()) \
             ON CONFLICT (artifact_path) DO UPDATE SET \
                 statement_index = EXCLUDED.statement_index, \
                 updated_at = NOW()",
        )
        .bind(artifact_path)
        .bind(stmt_idx as i32)
        .execute(pool)
        .await;
    }

    /// Remove the checkpoint for `artifact_path` after it has been fully
    /// and successfully applied so stale data cannot affect future runs.
    pub(crate) async fn clear_seed_checkpoint(pool: &PgPool, artifact_path: &str) {
        let _ = sqlx::query("DELETE FROM public.udb_seed_progress WHERE artifact_path = $1")
            .bind(artifact_path)
            .execute(pool)
            .await;
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
        // Pin both queries to one connection so that:
        //  a) Neon cold-start latency is paid once (the first SELECT warms the compute).
        //  b) Phase 2 re-uses the already-warm backend — avoids the 5-9 s cold-start
        //     that produced the "slow statement" warning on `manifest_json::TEXT`.
        let mut conn = pool.acquire().await.map_err(|err| {
            tonic::Status::internal(format!("load_last_manifest: acquire failed: {err}"))
        })?;
        // Phase 1: fetch only the checksum — covered by the composite index on
        // (applied_at DESC NULLS LAST, id DESC), returning just a TEXT column.
        let checksum_row: Option<(String,)> = sqlx::query_as(
            "SELECT manifest_checksum \
             FROM public.proto_schema_versions \
             ORDER BY applied_at DESC NULLS LAST, id DESC \
             LIMIT 1",
        )
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
        let json_row: Option<(String,)> = sqlx::query_as(
            "SELECT manifest_json::TEXT \
             FROM public.proto_schema_versions \
             WHERE manifest_checksum = $1",
        )
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
        let exists: bool =
            sqlx::query_scalar("SELECT to_regclass('public.proto_schema_versions') IS NOT NULL")
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

        sqlx::query_scalar(
            "SELECT manifest_checksum \
             FROM public.proto_schema_versions \
             ORDER BY applied_at DESC NULLS LAST, id DESC \
             LIMIT 1",
        )
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
        let json_row: Option<(String,)> = sqlx::query_as(
            "SELECT manifest_json::TEXT \
             FROM public.proto_schema_versions \
             WHERE manifest_checksum = $1",
        )
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

        let json_row: Option<(String,)> = sqlx::query_as(
            "SELECT manifest_json::TEXT \
             FROM public.proto_schema_versions \
             WHERE manifest_checksum = $1",
        )
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
        let pool = self.pg_pool()?;

        // Acquire one connection and hold it for all operations so that the
        // ping and the INSERT (or UPDATE) share the same backend.  This
        // prevents sqlx from giving us a different, potentially dead
        // connection from the pool for the heavy write after the ping.
        let mut conn = pool.acquire().await.map_err(|err| {
            tonic::Status::internal(format!("save_manifest: acquire failed: {err}"))
        })?;

        // Warm up this specific connection before the write.  Neon serverless
        // can silently kill the server-side backend while the pool holds the
        // socket open; a cheap ping forces reconnection at a predictable cost.
        let _ = sqlx::query("SELECT 1").execute(&mut *conn).await;

        // If the manifest checksum is already recorded, only touch `applied_at`.
        // The manifest JSON for a given checksum is immutable, so re-inserting
        // the full multi-MB JSONB document on every run is wasteful and is the
        // dominant cause of the slow-statement warning on Neon.
        let already_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM public.proto_schema_versions WHERE manifest_checksum = $1)",
        )
        .bind(&manifest.checksum_sha256)
        .fetch_one(&mut *conn)
        .await
        .unwrap_or(false);

        if already_exists {
            sqlx::query(
                "UPDATE public.proto_schema_versions
                 SET applied_at = NOW()
                 WHERE manifest_checksum = $1",
            )
            .bind(&manifest.checksum_sha256)
            .execute(&mut *conn)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!("proto_schema_versions touch failed: {err}"))
            })?;
            return Ok(());
        }

        let json = serde_json::to_string(manifest)
            .map_err(|err| tonic::Status::internal(format!("manifest serialise failed: {err}")))?;
        sqlx::query(
            "INSERT INTO public.proto_schema_versions
                 (manifest_checksum, manifest_json, generator_version, applied_at)
             VALUES ($1, $2::jsonb, $3, NOW())
             ON CONFLICT (manifest_checksum) DO UPDATE
                 SET applied_at = NOW()",
        )
        .bind(&manifest.checksum_sha256)
        .bind(&json)
        .bind(&manifest.generator_version)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tonic::Status::internal(format!("proto_schema_versions upsert failed: {err}"))
        })?;
        Ok(())
    }

    pub async fn verify_postgres_manifest(
        &self,
        manifest: &CatalogManifest,
    ) -> Result<Vec<String>, tonic::Status> {
        let pool = self.pg_pool()?;
        let mut findings = Vec::new();

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
        // Set statement_timeout for this query only. SET LOCAL only takes effect
        // inside a transaction; outside one, use SET + reset after to avoid
        // leaking the timeout to the next pool borrower.
        // We use a plain SET here and reset after the query completes.
        let _ = sqlx::query("SET statement_timeout = '60s'")
            .execute(pool)
            .await;
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
            .fetch_all(pool)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!("pg_catalog schema introspection failed: {err}"))
            })?;
        // Reset statement_timeout to default so this pool connection is safe to reuse.
        let _ = sqlx::query("SET statement_timeout = DEFAULT")
            .execute(pool)
            .await;

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
                findings.push(format!(
                    "missing PostgreSQL table {}.{}",
                    table.schema, table.table
                ));
                continue;
            }
            for column in &table.columns {
                if !existing_columns.contains(&(
                    table.schema.clone(),
                    table.table.clone(),
                    column.column_name.clone(),
                )) {
                    findings.push(format!(
                        "missing PostgreSQL column {}.{}.{}",
                        table.schema, table.table, column.column_name
                    ));
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
                    findings.push(format!(
                        "missing index {} on {}.{}",
                        index_name, table.schema, table.table
                    ));
                }
            }

            // GAP 11: Verify declared triggers actually exist.
            for trigger in &table.triggers {
                if !existing_triggers.contains(&(
                    table.schema.clone(),
                    table.table.clone(),
                    trigger.name.clone(),
                )) {
                    findings.push(format!(
                        "missing trigger {} on {}.{}",
                        trigger.name, table.schema, table.table
                    ));
                }
            }

            // GAP 11: Verify RLS is enabled when the manifest declares RLS policies.
            if !table.rls_policies.is_empty()
                && !rls_enabled_tables.contains(&(table.schema.clone(), table.table.clone()))
            {
                findings.push(format!(
                    "RLS not enabled on {}.{} but {} polic(ies) declared",
                    table.schema,
                    table.table,
                    table.rls_policies.len()
                ));
            }
        }
        Ok(findings)
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
        tracing::error!(
            metric = "udb.migration.drift_detected",
            reason = reason,
            "UDB startup drift detected"
        );
    }
}
