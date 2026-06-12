//! B.8 phase 2 — SQL Server (Tiberius) implementation of
//! [`MigrationAuditStore`].
//!
//! Mirrors the Postgres impl's semantics in T-SQL. The runs table is
//! created before the op-ledger (the ledger's FK targets it). uuids /
//! timestamps are cast to text in SELECTs; `id` is a `BIGINT IDENTITY`
//! returned via `OUTPUT inserted.id`. `applied_at` is set when the op
//! status is `APPLIED`, matching the PG `CASE WHEN … THEN NOW()`.

use async_trait::async_trait;
use tiberius::Row;
use uuid::Uuid;

use super::dialect::normalize_limit_offset;
use super::mssql::MssqlCanonicalStore;
use super::mssql_projection::{
    opt_str_col, parse_opt_ts_cell, parse_ts_cell, parse_uuid_cell, str_col,
};
use super::system_store::{
    MigrationAuditStore, MigrationOpInsert, MigrationOpRow, MigrationRunInsert, MigrationRunRow,
    MigrationRunState, MigrationRunsFilter, OpLedgerStatus, SystemStoreError, SystemStoreResult,
};
use crate::runtime::executors::mssql::SqlParam;

const DEFAULT_RUNS: &str = "udb_migration_runs";
const DEFAULT_LEDGER: &str = "udb_migration_op_ledger";

impl MssqlCanonicalStore {
    /// Override the migration relations. Default to `udb_migration_runs`
    /// and `udb_migration_op_ledger`.
    pub fn with_migration_relations(
        mut self,
        runs: impl Into<String>,
        ledger: impl Into<String>,
    ) -> Self {
        self.migration_runs_relation = Some(runs.into());
        self.migration_ledger_relation = Some(ledger.into());
        self
    }

    pub(super) fn migration_runs_rel(&self) -> &str {
        self.migration_runs_relation
            .as_deref()
            .unwrap_or(DEFAULT_RUNS)
    }

    pub(super) fn migration_ledger_rel(&self) -> &str {
        self.migration_ledger_relation
            .as_deref()
            .unwrap_or(DEFAULT_LEDGER)
    }
}

const RUN_SELECT_COLS: &str = "\
    CONVERT(NVARCHAR(36), run_id) AS run_id, project_id, catalog_version, state, \
    operations_hash, approval_token, \
    CONVERT(NVARCHAR(33), started_at, 127) AS started_at, \
    CONVERT(NVARCHAR(33), finished_at, 127) AS finished_at, error";

const OP_SELECT_COLS: &str = "\
    id, CONVERT(NVARCHAR(36), run_id) AS run_id, operation_index, backend, resource_uri, \
    operation_kind, status, payload_json, error, \
    CONVERT(NVARCHAR(33), applied_at, 127) AS applied_at";

fn i32_col(row: &Row, name: &str) -> i32 {
    row.try_get::<i32, _>(name).ok().flatten().unwrap_or(0)
}

fn row_to_run(row: &Row) -> SystemStoreResult<MigrationRunRow> {
    let run_id_str = opt_str_col(row, "run_id")
        .ok_or_else(|| SystemStoreError::query("mssql", "SELECT run_id", "run_id was NULL"))?;
    let run_id = parse_uuid_cell(&run_id_str)?;
    let state_str = str_col(row, "state");
    let state = MigrationRunState::parse(&state_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown migration run state '{state_str}' in mssql row"
        ))
    })?;
    Ok(MigrationRunRow {
        run_id,
        project_id: str_col(row, "project_id"),
        catalog_version: str_col(row, "catalog_version"),
        state,
        operations_hash: str_col(row, "operations_hash"),
        approval_token: str_col(row, "approval_token"),
        started_at: parse_ts_cell(opt_str_col(row, "started_at").as_deref()),
        finished_at: parse_opt_ts_cell(opt_str_col(row, "finished_at").as_deref()),
        error: str_col(row, "error"),
    })
}

fn row_to_op(row: &Row) -> SystemStoreResult<MigrationOpRow> {
    let run_id_str = opt_str_col(row, "run_id")
        .ok_or_else(|| SystemStoreError::query("mssql", "SELECT run_id", "run_id was NULL"))?;
    let run_id = parse_uuid_cell(&run_id_str)?;
    let status_str = str_col(row, "status");
    let status = OpLedgerStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown op ledger status '{status_str}' in mssql row"
        ))
    })?;
    let payload_json = match opt_str_col(row, "payload_json") {
        Some(text) => {
            serde_json::from_str(&text).unwrap_or(serde_json::Value::Object(Default::default()))
        }
        None => serde_json::Value::Object(Default::default()),
    };
    Ok(MigrationOpRow {
        id: row.try_get::<i64, _>("id").ok().flatten().unwrap_or(0),
        run_id,
        operation_index: i32_col(row, "operation_index"),
        backend: str_col(row, "backend"),
        resource_uri: str_col(row, "resource_uri"),
        operation_kind: str_col(row, "operation_kind"),
        status,
        payload_json: payload_json,
        error: str_col(row, "error"),
        applied_at: parse_opt_ts_cell(opt_str_col(row, "applied_at").as_deref()),
    })
}

#[async_trait]
impl MigrationAuditStore for MssqlCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mssql"
    }

    async fn ensure_migration_audit_tables(&self) -> SystemStoreResult<()> {
        let runs_rel = Self::safe_object_name(self.migration_runs_rel())
            .map_err(SystemStoreError::InvalidInput)?;
        let ledger_rel = Self::safe_object_name(self.migration_ledger_rel())
            .map_err(SystemStoreError::InvalidInput)?;
        let (runs_ddl, ledger_ddl) =
            super::sql_schema::mssql_migration_audit_ddl(runs_rel, ledger_rel);
        // Runs first (the ledger FK references it), then the ledger.
        self.client()
            .simple_batch(&runs_ddl)
            .await
            .map_err(|e| SystemStoreError::query("mssql", runs_ddl.clone(), e))?;
        self.client()
            .simple_batch(&ledger_ddl)
            .await
            .map_err(|e| SystemStoreError::query("mssql", ledger_ddl.clone(), e))?;
        let payload_col = format!(
            "IF COL_LENGTH('{ledger_rel}', 'payload_json') IS NULL \
             ALTER TABLE {ledger_rel} ADD payload_json NVARCHAR(MAX) NULL"
        );
        self.client()
            .simple_batch(&payload_col)
            .await
            .map_err(|e| SystemStoreError::query("mssql", payload_col.clone(), e))?;
        // Only tables created by an OLD schema have the legacy `rollback_json`
        // column; one created by the current DDL never does, so an
        // unconditional UPDATE referencing it fails with
        // `Invalid column name 'rollback_json'` (code 207) and aborts
        // canonical-store registration. Guard the backfill on the column's
        // existence (COL_LENGTH, the same idiom used for payload_json above).
        let backfill = format!(
            "IF COL_LENGTH('{ledger_rel}', 'rollback_json') IS NOT NULL \
             UPDATE {ledger_rel} \
             SET payload_json = rollback_json \
             WHERE payload_json IS NULL AND rollback_json IS NOT NULL"
        );
        self.client()
            .simple_batch(&backfill)
            .await
            .map_err(|e| SystemStoreError::query("mssql", backfill.clone(), e))?;
        Ok(())
    }

    async fn start_migration_run(&self, run: &MigrationRunInsert) -> SystemStoreResult<Uuid> {
        let rel = Self::safe_object_name(self.migration_runs_rel())
            .map_err(SystemStoreError::InvalidInput)?;
        let run_id = Uuid::new_v4();
        let sql = format!(
            "INSERT INTO {rel} ( \
                run_id, project_id, catalog_version, state, \
                operations_hash, approval_token, started_at \
             ) VALUES ( \
                CONVERT(UNIQUEIDENTIFIER, @P1), @P2, @P3, @P4, @P5, @P6, SYSUTCDATETIME() \
             )"
        );
        let params = [
            SqlParam::Str(run_id.to_string()),
            SqlParam::Str(run.project_id.clone()),
            SqlParam::Str(run.catalog_version.clone()),
            SqlParam::Str(run.state.as_str().to_string()),
            SqlParam::Str(run.operations_hash.clone()),
            SqlParam::Str(run.approval_token.clone()),
        ];
        self.client()
            .execute_sql(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        Ok(run_id)
    }

    async fn record_migration_op(&self, op: &MigrationOpInsert) -> SystemStoreResult<i64> {
        let rel = Self::safe_object_name(self.migration_ledger_rel())
            .map_err(SystemStoreError::InvalidInput)?;
        // applied_at is set only when the status is APPLIED (mirrors PG's
        // CASE WHEN $6 = 'APPLIED' THEN NOW() ELSE NULL END). OUTPUT
        // inserted.id returns the IDENTITY-assigned bigint.
        let sql = format!(
            "INSERT INTO {rel} ( \
                run_id, operation_index, backend, resource_uri, \
                operation_kind, status, payload_json, error, applied_at \
             ) \
             OUTPUT inserted.id AS id \
             VALUES ( \
                CONVERT(UNIQUEIDENTIFIER, @P1), @P2, @P3, @P4, @P5, @P6, @P7, @P8, \
                CASE WHEN @P6 = 'APPLIED' THEN SYSUTCDATETIME() ELSE NULL END \
             )"
        );
        let params = [
            SqlParam::Str(op.run_id.to_string()),
            SqlParam::Int(i64::from(op.operation_index)),
            SqlParam::Str(op.backend.clone()),
            SqlParam::Str(op.resource_uri.clone()),
            SqlParam::Str(op.operation_kind.clone()),
            SqlParam::Str(op.status.as_str().to_string()),
            SqlParam::Str(op.payload_json.to_string()),
            SqlParam::Str(op.error.clone()),
        ];
        let rows = self
            .client()
            .fetch_rows(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        let id = rows
            .first()
            .and_then(|r| r.try_get::<i64, _>("id").ok().flatten())
            .ok_or_else(|| {
                SystemStoreError::query("mssql", sql.clone(), "record_migration_op returned no id")
            })?;
        Ok(id)
    }

    async fn finish_migration_run(
        &self,
        run_id: Uuid,
        new_state: MigrationRunState,
        error: &str,
    ) -> SystemStoreResult<()> {
        let rel = Self::safe_object_name(self.migration_runs_rel())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!(
            "UPDATE {rel} \
             SET state = @P2, error = @P3, finished_at = SYSUTCDATETIME() \
             WHERE run_id = CONVERT(UNIQUEIDENTIFIER, @P1)"
        );
        let params = [
            SqlParam::Str(run_id.to_string()),
            SqlParam::Str(new_state.as_str().to_string()),
            SqlParam::Str(error.to_string()),
        ];
        let n = self
            .client()
            .execute_sql(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        if n == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "migration run {run_id} not found for finish_migration_run"
            )));
        }
        Ok(())
    }

    async fn get_migration_run(&self, run_id: Uuid) -> SystemStoreResult<Option<MigrationRunRow>> {
        let rel = Self::safe_object_name(self.migration_runs_rel())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!(
            "SELECT {RUN_SELECT_COLS} FROM {rel} WHERE run_id = CONVERT(UNIQUEIDENTIFIER, @P1)"
        );
        let params = [SqlParam::Str(run_id.to_string())];
        let rows = self
            .client()
            .fetch_rows(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        match rows.first() {
            Some(r) => Ok(Some(row_to_run(r)?)),
            None => Ok(None),
        }
    }

    async fn list_migration_ops(&self, run_id: Uuid) -> SystemStoreResult<Vec<MigrationOpRow>> {
        let rel = Self::safe_object_name(self.migration_ledger_rel())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!(
            "SELECT {OP_SELECT_COLS} FROM {rel} \
             WHERE run_id = CONVERT(UNIQUEIDENTIFIER, @P1) \
             ORDER BY operation_index ASC"
        );
        let params = [SqlParam::Str(run_id.to_string())];
        let rows = self
            .client()
            .fetch_rows(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            out.push(row_to_op(r)?);
        }
        Ok(out)
    }

    async fn list_migration_runs(
        &self,
        filter: &MigrationRunsFilter,
    ) -> SystemStoreResult<Vec<MigrationRunRow>> {
        let rel = Self::safe_object_name(self.migration_runs_rel())
            .map_err(SystemStoreError::InvalidInput)?;
        let mut clauses: Vec<String> = Vec::new();
        let mut params: Vec<SqlParam> = Vec::new();
        let mut n = 1usize;
        if let Some(p) = &filter.project_id {
            clauses.push(format!("project_id = @P{n}"));
            params.push(SqlParam::Str(p.clone()));
            n += 1;
        }
        if let Some(s) = filter.state {
            clauses.push(format!("state = @P{n}"));
            params.push(SqlParam::Str(s.as_str().to_string()));
            n += 1;
        }
        if let Some(v) = &filter.catalog_version {
            clauses.push(format!("catalog_version = @P{n}"));
            params.push(SqlParam::Str(v.clone()));
            n += 1;
        }
        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        let (limit, offset) = normalize_limit_offset(filter.limit, filter.offset);
        let offset_ph = format!("@P{n}");
        let limit_ph = format!("@P{}", n + 1);
        params.push(SqlParam::Int(offset));
        params.push(SqlParam::Int(limit));
        let sql = format!(
            "SELECT {RUN_SELECT_COLS} FROM {rel} \
             {where_sql} \
             ORDER BY started_at DESC \
             OFFSET {offset_ph} ROWS FETCH NEXT {limit_ph} ROWS ONLY"
        );
        let rows = self
            .client()
            .fetch_rows(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            out.push(row_to_run(r)?);
        }
        Ok(out)
    }
}
