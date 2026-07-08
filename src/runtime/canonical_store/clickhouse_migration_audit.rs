//! ClickHouse implementation of [`MigrationAuditStore`] (B.10c PHASE 2).
//!
//! Semantics mirror the Postgres impl (`postgres_migration_audit.rs`) exactly so
//! the cross-backend conformance contract passes byte-for-byte:
//!
//! - Run rows live in a `ReplacingMergeTree(version) ORDER BY run_id` so
//!   `finish_migration_run` can update state/error/finished_at by INSERTing a
//!   version+1 row; EVERY read uses `SELECT … FINAL` so superseded versions
//!   never leak.
//! - Op-ledger rows are immutable → an append-only `MergeTree`. Their monotone
//!   `id` is materialised from a `ReplacingMergeTree(version)` counter
//!   (`udb_migration_op_seq`) via the SAME read-insert-reread CAS emulation
//!   phase 1 uses for the outbox sequence (the ClickHouse analogue of PG's
//!   `BIGSERIAL`).
//! - `applied_at` is set only when the op status is `APPLIED`; `finished_at` is
//!   set on `finish_migration_run`. Both are OPTIONAL epoch-millis `Int64`
//!   columns using the sentinel `0` (= absent) so they round-trip
//!   `None`-when-absent.
//! - State / status values stored as their EXACT PG canonical strings.
//!
//! The run-row rewrites and migration-op sequence allocation run under a
//! KeeperMap-backed migration-audit mutation lease, so ClickHouse's
//! read-insert-reread counter emulation is not exposed to concurrent writers.
//!
//! ## Schema
//!
//! - `udb_migration_runs` — `ReplacingMergeTree(version) ORDER BY run_id`.
//! - `udb_migration_op_ledger` — append-only `MergeTree ORDER BY (run_id,
//!   operation_index, id)` so `list_migration_ops` reads a run's ops in
//!   `operation_index` order directly off the sort key.
//! - `udb_migration_op_seq` — `ReplacingMergeTree(version) ORDER BY id`, the
//!   monotone ledger-id counter (read-insert-reread CAS).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value as Json;
use uuid::Uuid;

use super::clickhouse::{ClickHouseCanonicalStore, sql_lit};
use super::clickhouse_projection::{
    ch_dt, ch_err, ch_i32, ch_i64, ch_json, ch_opt_dt, ch_str, ch_u64, ch_uuid, opt_dt_lit,
};
use super::system_store::{
    MigrationAuditStore, MigrationOpInsert, MigrationOpRow, MigrationRunInsert, MigrationRunRow,
    MigrationRunState, MigrationRunsFilter, OpLedgerStatus, SystemStoreError, SystemStoreResult,
};

/// Well-known counter row id for the migration-op ledger sequence.
const OP_SEQ_ID: &str = "migration_op";
const CLICKHOUSE_MIGRATION_AUDIT_MUTATION_LOCK: &str = "__udb_clickhouse_migration_audit";

const RUN_COLS: &str = "run_id, project_id, catalog_version, state, operations_hash, \
     approval_token, started_at, finished_at, error";
const OP_COLS: &str = "id, run_id, operation_index, backend, resource_uri, operation_kind, \
     status, payload_json, error, applied_at";

fn row_to_run(row: &Json) -> SystemStoreResult<MigrationRunRow> {
    let run_id = ch_uuid(row, "run_id")?;
    let state_str = ch_str(row, "state");
    let state = MigrationRunState::parse(&state_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown migration run state '{state_str}' in clickhouse row"
        ))
    })?;
    Ok(MigrationRunRow {
        run_id,
        project_id: ch_str(row, "project_id"),
        catalog_version: ch_str(row, "catalog_version"),
        state,
        operations_hash: ch_str(row, "operations_hash"),
        approval_token: ch_str(row, "approval_token"),
        started_at: ch_dt(row, "started_at"),
        finished_at: ch_opt_dt(row, "finished_at"),
        error: ch_str(row, "error"),
    })
}

fn row_to_op(row: &Json) -> SystemStoreResult<MigrationOpRow> {
    let run_id = ch_uuid(row, "run_id")?;
    let status_str = ch_str(row, "status");
    let status = OpLedgerStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown op ledger status '{status_str}' in clickhouse row"
        ))
    })?;
    Ok(MigrationOpRow {
        id: ch_i64(row, "id"),
        run_id,
        operation_index: ch_i32(row, "operation_index"),
        backend: ch_str(row, "backend"),
        resource_uri: ch_str(row, "resource_uri"),
        operation_kind: ch_str(row, "operation_kind"),
        status,
        payload_json: ch_json(row, "payload_json", Json::Object(Default::default())),
        error: ch_str(row, "error"),
        applied_at: ch_opt_dt(row, "applied_at"),
    })
}

impl ClickHouseCanonicalStore {
    fn runs_table(&self) -> SystemStoreResult<String> {
        self.qualified("udb_migration_runs")
            .map_err(|e| ch_err("runs_table", e))
    }
    fn ledger_table(&self) -> SystemStoreResult<String> {
        self.qualified("udb_migration_op_ledger")
            .map_err(|e| ch_err("ledger_table", e))
    }
    fn op_seq_table(&self) -> SystemStoreResult<String> {
        self.qualified("udb_migration_op_seq")
            .map_err(|e| ch_err("op_seq_table", e))
    }

    async fn acquire_migration_audit_mutation_lock(&self, op: &str) -> SystemStoreResult<String> {
        let owner = format!("{op}:{}", Uuid::new_v4());
        self.acquire_system_mutation_lease(CLICKHOUSE_MIGRATION_AUDIT_MUTATION_LOCK, &owner, op)
            .await
            .map_err(|err| ch_err(op, err))?;
        Ok(owner)
    }

    async fn release_migration_audit_mutation_lock(
        &self,
        owner: &str,
        op: &str,
    ) -> SystemStoreResult<()> {
        self.release_system_mutation_lease(CLICKHOUSE_MIGRATION_AUDIT_MUTATION_LOCK, owner, op)
            .await
            .map_err(|err| ch_err(op, err))
    }

    async fn has_ledger_column(&self, column: &str) -> SystemStoreResult<bool> {
        let sql = format!(
            "SELECT count() AS n FROM system.columns \
             WHERE database = {database} AND table = 'udb_migration_op_ledger' AND name = {column}",
            database = sql_lit(&self.database),
            column = sql_lit(column),
        );
        let rows = self
            .executor()
            .select_rows(&sql)
            .await
            .map_err(|e| ch_err("has_ledger_column", e))?;
        Ok(rows.first().map(|r| ch_i64(r, "n")).unwrap_or(0) > 0)
    }

    /// Read one run's FINAL row + its version. `FINAL` collapses superseded
    /// ReplacingMergeTree parts so only the highest version is observed.
    async fn read_run_versioned(
        &self,
        run_id: Uuid,
    ) -> SystemStoreResult<Option<(MigrationRunRow, u64)>> {
        let tbl = self.runs_table()?;
        let sql = format!(
            "SELECT {RUN_COLS}, version FROM {tbl} FINAL WHERE run_id = {id}",
            id = sql_lit(&run_id.to_string()),
        );
        let rows = self
            .executor()
            .select_rows(&sql)
            .await
            .map_err(|e| ch_err("get_migration_run", e))?;
        match rows.first() {
            Some(r) => Ok(Some((row_to_run(r)?, ch_u64(r, "version")))),
            None => Ok(None),
        }
    }

    /// INSERT a full run row at `version`.
    async fn insert_run_version(
        &self,
        run: &MigrationRunRow,
        version: u64,
    ) -> SystemStoreResult<()> {
        let tbl = self.runs_table()?;
        let sql = format!(
            "INSERT INTO {tbl} ({RUN_COLS}, version) VALUES (\
             {run_id}, {project}, {catalog}, {state}, {ops_hash}, {token}, \
             {started}, {finished}, {error}, {version})",
            run_id = sql_lit(&run.run_id.to_string()),
            project = sql_lit(&run.project_id),
            catalog = sql_lit(&run.catalog_version),
            state = sql_lit(run.state.as_str()),
            ops_hash = sql_lit(&run.operations_hash),
            token = sql_lit(&run.approval_token),
            started = run.started_at.timestamp_millis(),
            finished = opt_dt_lit(run.finished_at),
            error = sql_lit(&run.error),
        );
        self.executor()
            .execute_ddl(&sql)
            .await
            .map_err(|e| ch_err("insert_run_version", e))
    }

    /// Allocate the next monotone ledger id via the ReplacingMergeTree counter,
    /// emulating CAS: read current seq with FINAL → INSERT version+1 row → re-read
    /// FINAL to confirm. Callers hold the migration-audit mutation lease, so this
    /// read-insert-reread allocation has one writer across broker processes.
    async fn next_op_id(&self) -> SystemStoreResult<i64> {
        let seq_table = self.op_seq_table()?;
        let read_sql = format!(
            "SELECT seq FROM {seq_table} FINAL WHERE id = {id}",
            id = sql_lit(OP_SEQ_ID),
        );
        let current = self
            .executor()
            .select_rows(&read_sql)
            .await
            .map_err(|e| ch_err("next_op_id read", e))?
            .first()
            .map(|r| ch_i64(r, "seq"))
            .unwrap_or(0);
        let next = current + 1;
        // version == seq: both monotone from 1, so the highest seq is always the
        // highest version — FINAL therefore always surfaces the newest allocation.
        let insert = format!(
            "INSERT INTO {seq_table} (id, seq, version) VALUES ({id}, {next}, {next})",
            id = sql_lit(OP_SEQ_ID),
        );
        self.executor()
            .execute_ddl(&insert)
            .await
            .map_err(|e| ch_err("next_op_id insert", e))?;
        let confirmed = self
            .executor()
            .select_rows(&read_sql)
            .await
            .map_err(|e| ch_err("next_op_id confirm", e))?
            .first()
            .map(|r| ch_i64(r, "seq"))
            .unwrap_or(0);
        if confirmed < next {
            return Err(ch_err(
                "next_op_id",
                format!(
                    "ledger-id allocation lost a race: inserted {next}, re-read {confirmed} \
                     (migration-audit mutation lease invariant violated)"
                ),
            ));
        }
        Ok(next)
    }
}

#[async_trait]
impl MigrationAuditStore for ClickHouseCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "clickhouse"
    }

    async fn ensure_migration_audit_tables(&self) -> SystemStoreResult<()> {
        let runs = self.runs_table()?;
        // Runs: ReplacingMergeTree by run_id so finish_migration_run updates via
        // version+1. finished_at is a nullable epoch-millis Int64 (sentinel 0).
        let runs_ddl = format!(
            "CREATE TABLE IF NOT EXISTS {runs} (\
             run_id String, \
             project_id String, \
             catalog_version String, \
             state String, \
             operations_hash String, \
             approval_token String, \
             started_at Int64, \
             finished_at Int64, \
             error String, \
             version UInt64\
             ) ENGINE = ReplacingMergeTree(version) ORDER BY run_id"
        );
        self.executor()
            .execute_ddl(&runs_ddl)
            .await
            .map_err(|e| ch_err("ensure_migration_audit_tables runs", e))?;
        // Ledger: append-only MergeTree ordered by (run_id, operation_index, id)
        // so list reads in order. applied_at is a nullable epoch-millis Int64.
        let ledger = self.ledger_table()?;
        let ledger_ddl = format!(
            "CREATE TABLE IF NOT EXISTS {ledger} (\
             id Int64, \
             run_id String, \
             operation_index Int32, \
             backend String, \
             resource_uri String, \
             operation_kind String, \
             status String, \
             payload_json String, \
             error String, \
             applied_at Int64\
             ) ENGINE = MergeTree ORDER BY (run_id, operation_index, id)"
        );
        self.executor()
            .execute_ddl(&ledger_ddl)
            .await
            .map_err(|e| ch_err("ensure_migration_audit_tables ledger", e))?;
        let payload_col = format!(
            "ALTER TABLE {ledger} ADD COLUMN IF NOT EXISTS payload_json String DEFAULT '{{}}'"
        );
        self.executor()
            .execute_ddl(&payload_col)
            .await
            .map_err(|e| ch_err("ensure_migration_audit_tables payload_json", e))?;
        if self.has_ledger_column("rollback_json").await? {
            let backfill = format!(
                "ALTER TABLE {ledger} UPDATE payload_json = rollback_json \
                 WHERE payload_json = '{{}}' AND rollback_json != '{{}}'"
            );
            self.executor()
                .execute_ddl(&backfill)
                .await
                .map_err(|e| ch_err("ensure_migration_audit_tables payload backfill", e))?;
        }
        // Op-seq: ReplacingMergeTree counter (read-insert-reread CAS).
        let seq = self.op_seq_table()?;
        let seq_ddl = format!(
            "CREATE TABLE IF NOT EXISTS {seq} (\
             id String, \
             seq Int64, \
             version UInt64\
             ) ENGINE = ReplacingMergeTree(version) ORDER BY id"
        );
        self.executor()
            .execute_ddl(&seq_ddl)
            .await
            .map_err(|e| ch_err("ensure_migration_audit_tables seq", e))?;
        Ok(())
    }

    async fn start_migration_run(&self, run: &MigrationRunInsert) -> SystemStoreResult<Uuid> {
        let owner = self
            .acquire_migration_audit_mutation_lock("start_migration_run")
            .await?;
        let result = async {
            let run_id = Uuid::new_v4();
            let now = Self::now_unix_ms();
            let row = MigrationRunRow {
                run_id,
                project_id: run.project_id.clone(),
                catalog_version: run.catalog_version.clone(),
                state: run.state,
                operations_hash: run.operations_hash.clone(),
                approval_token: run.approval_token.clone(),
                started_at: DateTime::<Utc>::from_timestamp_millis(now).unwrap_or_else(Utc::now),
                // finished_at absent → sentinel 0 → round-trips None.
                finished_at: None,
                error: String::new(),
            };
            self.insert_run_version(&row, 1).await?;
            Ok(run_id)
        }
        .await;
        self.release_migration_audit_mutation_lock(&owner, "start_migration_run")
            .await?;
        result
    }

    async fn record_migration_op(&self, op: &MigrationOpInsert) -> SystemStoreResult<i64> {
        let owner = self
            .acquire_migration_audit_mutation_lock("record_migration_op")
            .await?;
        let result = async {
            let id = self.next_op_id().await?;
            let ledger = self.ledger_table()?;
            // applied_at set only when status == APPLIED (PG's CASE); otherwise sentinel
            // 0 so it round-trips as None.
            let applied_at = if op.status == OpLedgerStatus::Applied {
                opt_dt_lit(Some(
                    DateTime::<Utc>::from_timestamp_millis(Self::now_unix_ms())
                        .unwrap_or_else(Utc::now),
                ))
            } else {
                0
            };
            let sql = format!(
                "INSERT INTO {ledger} ({OP_COLS}) VALUES (\
                 {id}, {run_id}, {op_index}, {backend}, {resource}, {kind}, {status}, \
                 {payload}, {error}, {applied})",
                id = id,
                run_id = sql_lit(&op.run_id.to_string()),
                op_index = op.operation_index,
                backend = sql_lit(&op.backend),
                resource = sql_lit(&op.resource_uri),
                kind = sql_lit(&op.operation_kind),
                status = sql_lit(op.status.as_str()),
                payload = sql_lit(&op.payload_json.to_string()),
                error = sql_lit(&op.error),
                applied = applied_at,
            );
            self.executor()
                .execute_ddl(&sql)
                .await
                .map_err(|e| ch_err("record_migration_op insert", e))?;
            Ok(id)
        }
        .await;
        self.release_migration_audit_mutation_lock(&owner, "record_migration_op")
            .await?;
        result
    }

    async fn finish_migration_run(
        &self,
        run_id: Uuid,
        new_state: MigrationRunState,
        error: &str,
    ) -> SystemStoreResult<()> {
        let owner = self
            .acquire_migration_audit_mutation_lock("finish_migration_run")
            .await?;
        let result = async {
            // finished_at MUST persist + round-trip as Some. version+1 INSERT. A
            // missing run is an error (PG's rows_affected == 0 → InvalidInput).
            let Some((mut run, version)) = self.read_run_versioned(run_id).await? else {
                return Err(SystemStoreError::InvalidInput(format!(
                    "migration run {run_id} not found for finish_migration_run"
                )));
            };
            run.state = new_state;
            run.error = error.to_string();
            run.finished_at = Some(
                DateTime::<Utc>::from_timestamp_millis(Self::now_unix_ms())
                    .unwrap_or_else(Utc::now),
            );
            self.insert_run_version(&run, version.saturating_add(1).max(1))
                .await
        }
        .await;
        self.release_migration_audit_mutation_lock(&owner, "finish_migration_run")
            .await?;
        result
    }

    async fn get_migration_run(&self, run_id: Uuid) -> SystemStoreResult<Option<MigrationRunRow>> {
        Ok(self.read_run_versioned(run_id).await?.map(|(row, _)| row))
    }

    async fn list_migration_ops(&self, run_id: Uuid) -> SystemStoreResult<Vec<MigrationOpRow>> {
        let ledger = self.ledger_table()?;
        // Append-only ledger ordered by (run_id, operation_index, id) — read this
        // run's ops in operation_index order directly off the sort key.
        let sql = format!(
            "SELECT {OP_COLS} FROM {ledger} WHERE run_id = {id} \
             ORDER BY operation_index ASC, id ASC",
            id = sql_lit(&run_id.to_string()),
        );
        let rows = self
            .executor()
            .select_rows(&sql)
            .await
            .map_err(|e| ch_err("list_migration_ops", e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(row_to_op(row)?);
        }
        Ok(out)
    }

    async fn list_migration_runs(
        &self,
        filter: &MigrationRunsFilter,
    ) -> SystemStoreResult<Vec<MigrationRunRow>> {
        let runs = self.runs_table()?;
        // FINAL scan + Rust filter / DESC sort / page. FINAL collapses superseded
        // versions so each run lists once at its latest state.
        let scan = format!("SELECT {RUN_COLS}, version FROM {runs} FINAL");
        let rows = self
            .executor()
            .select_rows(&scan)
            .await
            .map_err(|e| ch_err("list_migration_runs scan", e))?;
        let mut out: Vec<MigrationRunRow> = Vec::new();
        for row in &rows {
            let run = row_to_run(row)?;
            if let Some(p) = &filter.project_id {
                if &run.project_id != p {
                    continue;
                }
            }
            if let Some(s) = filter.state {
                if run.state != s {
                    continue;
                }
            }
            if let Some(v) = &filter.catalog_version {
                if &run.catalog_version != v {
                    continue;
                }
            }
            out.push(run);
        }
        out.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        let limit = if filter.limit <= 0 { 100 } else { filter.limit } as usize;
        let offset = filter.offset.max(0) as usize;
        Ok(out.into_iter().skip(offset).take(limit).collect())
    }
}
