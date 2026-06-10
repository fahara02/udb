//! Cassandra / ScyllaDB implementation of [`MigrationAuditStore`] (B.10a
//! PHASE 2).
//!
//! Semantics mirror the Postgres impl (`postgres_migration_audit.rs`) exactly
//! so the cross-backend conformance contract passes byte-for-byte:
//!
//! - Run rows keyed by `run_id text` (uuid as text).
//! - Op-ledger rows carry a monotone `id` materialised from a counter row
//!   (`udb_migration_op_seq`) via the SAME LWT-CAS pattern phase 1 uses for the
//!   outbox sequence — the Cassandra analogue of PG's `BIGSERIAL` (a CQL
//!   `COUNTER` can't take part in LWTs, so a plain `bigint` + Paxos CAS is the
//!   correct atomic read-modify-write).
//! - `applied_at` is set only when the op status is `APPLIED`; `finished_at` is
//!   set on `finish_migration_run`. Both are OPTIONAL timestamps that
//!   round-trip `None`-when-absent (the bug class the MSSQL impl flagged).
//! - State / status values stored as their EXACT PG canonical strings.
//!
//! ## Schema
//!
//! - `udb_migration_runs` — `run_id text PRIMARY KEY`.
//! - `udb_migration_op_ledger` — `PRIMARY KEY (run_id, operation_index, id)`
//!   clustered ASC so `list_migration_ops` reads a run's ops in
//!   `operation_index` order off a single-partition scan.
//! - `udb_migration_op_seq (id text PRIMARY KEY, seq bigint)` — the monotone
//!   ledger-id counter (LWT-CAS allocated).
//!
//! ## `ALLOW FILTERING`
//!
//! `list_migration_runs` filters on non-key columns and so scans with ALLOW
//! FILTERING + a Rust fold; acceptable for the conformance contract's tiny
//! data. `list_migration_ops` and `get_migration_run` are point/partition
//! reads (no filtering).

use async_trait::async_trait;
use scylla::frame::response::result::Row;
use scylla::statement::SerialConsistency;
use uuid::Uuid;

use super::cassandra::{CassandraCanonicalStore, now_unix_ms};
use super::cassandra_projection::{
    cass_err, cql_ts, get_dt, get_i32, get_i64, get_json, get_opt_dt, get_text, get_uuid,
};
use super::system_store::{
    MigrationAuditStore, MigrationOpInsert, MigrationOpRow, MigrationRunInsert, MigrationRunRow,
    MigrationRunState, MigrationRunsFilter, OpLedgerStatus, SystemStoreError, SystemStoreResult,
};

/// Well-known counter row id for the migration-op ledger sequence.
const OP_SEQ_ID: &str = "migration_op";
/// Cap on the ledger-id CAS retry loop (one Paxos round per iteration).
const OP_SEQ_CAS_MAX_ATTEMPTS: u32 = 64;

const RUN_COLS: &str = "run_id, project_id, catalog_version, state, operations_hash, \
     approval_token, started_at, finished_at, error";
const OP_COLS: &str = "id, run_id, operation_index, backend, resource_uri, operation_kind, \
     status, payload_json, error, applied_at";

fn row_to_run(row: &Row) -> SystemStoreResult<MigrationRunRow> {
    let run_id = get_uuid(row, 0)?;
    let state_str = get_text(row, 3);
    let state = MigrationRunState::parse(&state_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown migration run state '{state_str}' in cassandra row"
        ))
    })?;
    Ok(MigrationRunRow {
        run_id,
        project_id: get_text(row, 1),
        catalog_version: get_text(row, 2),
        state,
        operations_hash: get_text(row, 4),
        approval_token: get_text(row, 5),
        started_at: get_dt(row, 6),
        finished_at: get_opt_dt(row, 7),
        error: get_text(row, 8),
    })
}

fn row_to_op(row: &Row) -> SystemStoreResult<MigrationOpRow> {
    let run_id = get_uuid(row, 1)?;
    let status_str = get_text(row, 6);
    let status = OpLedgerStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown op ledger status '{status_str}' in cassandra row"
        ))
    })?;
    Ok(MigrationOpRow {
        id: get_i64(row, 0),
        run_id,
        operation_index: get_i32(row, 2),
        backend: get_text(row, 3),
        resource_uri: get_text(row, 4),
        operation_kind: get_text(row, 5),
        status,
        payload_json: get_json(row, 7, serde_json::Value::Object(Default::default())),
        error: get_text(row, 8),
        applied_at: get_opt_dt(row, 9),
    })
}

impl CassandraCanonicalStore {
    fn runs_table(&self) -> String {
        self.qualified("udb_migration_runs")
    }
    fn ledger_table(&self) -> String {
        self.qualified("udb_migration_op_ledger")
    }
    fn op_seq_table(&self) -> String {
        self.qualified("udb_migration_op_seq")
    }

    async fn has_ledger_column(&self, column: &str) -> SystemStoreResult<bool> {
        let rows = self
            .client()
            .cql_query_rows(
                "SELECT column_name FROM system_schema.columns \
                 WHERE keyspace_name = ? AND table_name = ? AND column_name = ?",
                (self.keyspace.as_str(), "udb_migration_op_ledger", column),
            )
            .await
            .map_err(|e| cass_err("has_ledger_column", e))?;
        Ok(!rows.is_empty())
    }

    async fn backfill_payload_json_from_rollback_json(&self) -> SystemStoreResult<()> {
        if !self.has_ledger_column("rollback_json").await? {
            return Ok(());
        }
        let scan = format!(
            "SELECT run_id, operation_index, id, rollback_json FROM {tbl}",
            tbl = self.ledger_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&scan, ())
            .await
            .map_err(|e| cass_err("payload_json backfill scan", e))?;
        let update = format!(
            "UPDATE {tbl} SET payload_json = ? \
             WHERE run_id = ? AND operation_index = ? AND id = ?",
            tbl = self.ledger_table(),
        );
        for row in &rows {
            let payload = get_text(row, 3);
            if payload.is_empty() {
                continue;
            }
            let run_id = get_text(row, 0);
            let operation_index = get_i32(row, 1);
            let id = get_i64(row, 2);
            self.client()
                .cql_execute(
                    &update,
                    (payload.as_str(), run_id.as_str(), operation_index, id),
                )
                .await
                .map_err(|e| cass_err("payload_json backfill update", e))?;
        }
        Ok(())
    }

    /// Allocate the next monotone ledger id via the LWT-CAS counter (same
    /// pattern as the phase-1 outbox seq).
    async fn next_op_id(&self) -> SystemStoreResult<i64> {
        let seq_table = self.op_seq_table();
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            if attempt > OP_SEQ_CAS_MAX_ATTEMPTS {
                return Err(cass_err(
                    "record_migration_op",
                    "ledger-id CAS did not converge",
                ));
            }
            let current = self
                .client()
                .cql_query_first_i64(
                    &format!("SELECT seq FROM {seq_table} WHERE id = '{OP_SEQ_ID}'"),
                    (),
                )
                .await
                .map_err(|e| cass_err("record_migration_op read seq", e))?;
            match current {
                None => {
                    let seed = format!(
                        "INSERT INTO {seq_table} (id, seq) VALUES ('{OP_SEQ_ID}', ?) IF NOT EXISTS"
                    );
                    let applied = self
                        .client()
                        .cql_lwt_applied(&seed, (1i64,), SerialConsistency::Serial)
                        .await
                        .map_err(|e| cass_err("record_migration_op seed seq", e))?;
                    if applied {
                        return Ok(1);
                    }
                }
                Some(cur) => {
                    let next = cur + 1;
                    let cas = format!(
                        "UPDATE {seq_table} SET seq = ? WHERE id = '{OP_SEQ_ID}' IF seq = ?"
                    );
                    let applied = self
                        .client()
                        .cql_lwt_applied(&cas, (next, cur), SerialConsistency::Serial)
                        .await
                        .map_err(|e| cass_err("record_migration_op cas seq", e))?;
                    if applied {
                        return Ok(next);
                    }
                }
            }
            // Lost the race — re-read and retry.
        }
    }
}

#[async_trait]
impl MigrationAuditStore for CassandraCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "cassandra"
    }

    async fn ensure_migration_audit_tables(&self) -> SystemStoreResult<()> {
        self.ensure_keyspace()
            .await
            .map_err(|e| cass_err("ensure_migration_audit_tables keyspace", e))?;
        let runs_ddl = format!(
            "CREATE TABLE IF NOT EXISTS {tbl} ( \
                run_id text PRIMARY KEY, \
                project_id text, \
                catalog_version text, \
                state text, \
                operations_hash text, \
                approval_token text, \
                started_at timestamp, \
                finished_at timestamp, \
                error text \
             )",
            tbl = self.runs_table(),
        );
        self.client()
            .cql_execute(&runs_ddl, ())
            .await
            .map_err(|e| cass_err("ensure_migration_audit_tables runs", e))?;
        // Ledger clustered by operation_index so list reads in order.
        let ledger_ddl = format!(
            "CREATE TABLE IF NOT EXISTS {tbl} ( \
                run_id text, \
                operation_index int, \
                id bigint, \
                backend text, \
                resource_uri text, \
                operation_kind text, \
                status text, \
                payload_json text, \
                error text, \
                applied_at timestamp, \
                PRIMARY KEY (run_id, operation_index, id) \
             ) WITH CLUSTERING ORDER BY (operation_index ASC, id ASC)",
            tbl = self.ledger_table(),
        );
        self.client()
            .cql_execute(&ledger_ddl, ())
            .await
            .map_err(|e| cass_err("ensure_migration_audit_tables ledger", e))?;
        if !self.has_ledger_column("payload_json").await? {
            let payload_col = format!(
                "ALTER TABLE {tbl} ADD payload_json text",
                tbl = self.ledger_table(),
            );
            self.client()
                .cql_execute(&payload_col, ())
                .await
                .map_err(|e| cass_err("ensure_migration_audit_tables payload_json", e))?;
        }
        self.backfill_payload_json_from_rollback_json().await?;
        let seq_ddl = format!(
            "CREATE TABLE IF NOT EXISTS {tbl} ( id text PRIMARY KEY, seq bigint )",
            tbl = self.op_seq_table(),
        );
        self.client()
            .cql_execute(&seq_ddl, ())
            .await
            .map_err(|e| cass_err("ensure_migration_audit_tables seq", e))?;
        Ok(())
    }

    async fn start_migration_run(&self, run: &MigrationRunInsert) -> SystemStoreResult<Uuid> {
        let run_id = Uuid::new_v4();
        let now = now_unix_ms();
        // finished_at deliberately omitted → CQL leaves it unset, round-trips None.
        let sql = format!(
            "INSERT INTO {tbl} ( \
                run_id, project_id, catalog_version, state, operations_hash, \
                approval_token, started_at, error \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            tbl = self.runs_table(),
        );
        self.client()
            .cql_execute(
                &sql,
                (
                    run_id.to_string(),
                    run.project_id.as_str(),
                    run.catalog_version.as_str(),
                    run.state.as_str(),
                    run.operations_hash.as_str(),
                    run.approval_token.as_str(),
                    cql_ts(now),
                    "",
                ),
            )
            .await
            .map_err(|e| cass_err("start_migration_run", e))?;
        Ok(run_id)
    }

    async fn record_migration_op(&self, op: &MigrationOpInsert) -> SystemStoreResult<i64> {
        let id = self.next_op_id().await?;
        // applied_at set only when status == APPLIED (PG's CASE); otherwise left
        // unset so it round-trips as None.
        let applied_at = if op.status == OpLedgerStatus::Applied {
            Some(cql_ts(now_unix_ms()))
        } else {
            None
        };
        let sql = format!(
            "INSERT INTO {tbl} ( \
                run_id, operation_index, id, backend, resource_uri, operation_kind, \
                status, payload_json, error, applied_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tbl = self.ledger_table(),
        );
        self.client()
            .cql_execute(
                &sql,
                (
                    op.run_id.to_string(),
                    op.operation_index,
                    id,
                    op.backend.as_str(),
                    op.resource_uri.as_str(),
                    op.operation_kind.as_str(),
                    op.status.as_str(),
                    op.payload_json.to_string(),
                    op.error.as_str(),
                    applied_at,
                ),
            )
            .await
            .map_err(|e| cass_err("record_migration_op insert", e))?;
        Ok(id)
    }

    async fn finish_migration_run(
        &self,
        run_id: Uuid,
        new_state: MigrationRunState,
        error: &str,
    ) -> SystemStoreResult<()> {
        // finished_at MUST persist + round-trip as Some. IF EXISTS so a missing
        // run surfaces as an error (PG's rows_affected == 0 → InvalidInput).
        let now = now_unix_ms();
        let sql = format!(
            "UPDATE {tbl} SET state = ?, error = ?, finished_at = ? WHERE run_id = ? IF EXISTS",
            tbl = self.runs_table(),
        );
        let applied = self
            .client()
            .cql_lwt_applied(
                &sql,
                (new_state.as_str(), error, cql_ts(now), run_id.to_string()),
                SerialConsistency::Serial,
            )
            .await
            .map_err(|e| cass_err("finish_migration_run", e))?;
        if !applied {
            return Err(SystemStoreError::InvalidInput(format!(
                "migration run {run_id} not found for finish_migration_run"
            )));
        }
        Ok(())
    }

    async fn get_migration_run(&self, run_id: Uuid) -> SystemStoreResult<Option<MigrationRunRow>> {
        let sql = format!(
            "SELECT {RUN_COLS} FROM {tbl} WHERE run_id = ?",
            tbl = self.runs_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&sql, (run_id.to_string(),))
            .await
            .map_err(|e| cass_err("get_migration_run", e))?;
        match rows.first() {
            Some(r) => Ok(Some(row_to_run(r)?)),
            None => Ok(None),
        }
    }

    async fn list_migration_ops(&self, run_id: Uuid) -> SystemStoreResult<Vec<MigrationOpRow>> {
        // Single-partition read returns rows already clustered by
        // operation_index ASC — no Rust-side sort needed.
        let sql = format!(
            "SELECT {OP_COLS} FROM {tbl} WHERE run_id = ?",
            tbl = self.ledger_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&sql, (run_id.to_string(),))
            .await
            .map_err(|e| cass_err("list_migration_ops", e))?;
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
        // Scan + Rust filter / DESC sort / page. ALLOW FILTERING: tiny data.
        let scan = format!(
            "SELECT {RUN_COLS} FROM {tbl} ALLOW FILTERING",
            tbl = self.runs_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&scan, ())
            .await
            .map_err(|e| cass_err("list_migration_runs scan", e))?;
        let mut runs: Vec<MigrationRunRow> = Vec::new();
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
            runs.push(run);
        }
        // started_at DESC, like PG.
        runs.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        let limit = if filter.limit <= 0 { 100 } else { filter.limit } as usize;
        let offset = filter.offset.max(0) as usize;
        Ok(runs.into_iter().skip(offset).take(limit).collect())
    }
}
