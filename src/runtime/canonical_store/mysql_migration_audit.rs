//! MySQL implementation of [`MigrationAuditStore`].
//!
//! ## Dialect choices
//!
//! - `run_id` is `CHAR(36)` UUID, generated Rust-side.
//! - `id` is `BIGINT AUTO_INCREMENT`.
//! - `rollback_json` is native `JSON`.
//! - Timestamps use `TIMESTAMP(6)` microsecond precision.
//! - CHECK constraints on `state` and `status` (MySQL 8.0.16+).
//! - FK with `ON DELETE CASCADE` referencing
//!   `udb_migration_runs(run_id)`.
//! - MySQL has no `INSERT ... RETURNING id`; we use
//!   `LAST_INSERT_ID()` for the ledger insert and a follow-up
//!   `SELECT` is not needed because we generated the run UUID
//!   Rust-side.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::dialect::{SqlDialect, build_eq_where, normalize_limit_offset};
use super::mysql::MysqlCanonicalStore;
use super::system_store::{
    MigrationAuditStore, MigrationOpInsert, MigrationOpRow, MigrationRunInsert, MigrationRunRow,
    MigrationRunState, MigrationRunsFilter, OpLedgerStatus, SystemStoreError, SystemStoreResult,
};

const RUNS_TABLE: &str = "udb_migration_runs";
const LEDGER_TABLE: &str = "udb_migration_op_ledger";

fn row_to_run(row: sqlx::mysql::MySqlRow) -> SystemStoreResult<MigrationRunRow> {
    let run_id_str: String = row
        .try_get("run_id")
        .map_err(|e| SystemStoreError::query("mysql", "SELECT run_id", e))?;
    let run_id = Uuid::parse_str(&run_id_str).map_err(|e| {
        SystemStoreError::InvalidInput(format!("run_id '{run_id_str}' is not a valid UUID: {e}"))
    })?;
    let state_str: String = row
        .try_get("state")
        .map_err(|e| SystemStoreError::query("mysql", "SELECT state", e))?;
    let state = MigrationRunState::parse(&state_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown migration run state '{state_str}' in MySQL row"
        ))
    })?;
    Ok(MigrationRunRow {
        run_id,
        project_id: row.try_get("project_id").unwrap_or_default(),
        catalog_version: row.try_get("catalog_version").unwrap_or_default(),
        state,
        operations_hash: row.try_get("operations_hash").unwrap_or_default(),
        approval_token: row.try_get("approval_token").unwrap_or_default(),
        started_at: row
            .try_get::<DateTime<Utc>, _>("started_at")
            .unwrap_or_else(|_| Utc::now()),
        finished_at: row
            .try_get::<Option<DateTime<Utc>>, _>("finished_at")
            .ok()
            .flatten(),
        error: row.try_get("error").unwrap_or_default(),
    })
}

fn row_to_op(row: sqlx::mysql::MySqlRow) -> SystemStoreResult<MigrationOpRow> {
    let run_id_str: String = row
        .try_get("run_id")
        .map_err(|e| SystemStoreError::query("mysql", "SELECT run_id", e))?;
    let run_id = Uuid::parse_str(&run_id_str).map_err(|e| {
        SystemStoreError::InvalidInput(format!("run_id '{run_id_str}' is not a valid UUID: {e}"))
    })?;
    let status_str: String = row
        .try_get("status")
        .map_err(|e| SystemStoreError::query("mysql", "SELECT status", e))?;
    let status = OpLedgerStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown op ledger status '{status_str}' in MySQL row"
        ))
    })?;
    Ok(MigrationOpRow {
        id: row.try_get("id").unwrap_or(0),
        run_id,
        operation_index: row.try_get("operation_index").unwrap_or(0),
        backend: row.try_get("backend").unwrap_or_default(),
        resource_uri: row.try_get("resource_uri").unwrap_or_default(),
        operation_kind: row.try_get("operation_kind").unwrap_or_default(),
        status,
        rollback_json: row
            .try_get("rollback_json")
            .unwrap_or(serde_json::Value::Object(Default::default())),
        error: row.try_get("error").unwrap_or_default(),
        applied_at: row
            .try_get::<Option<DateTime<Utc>>, _>("applied_at")
            .ok()
            .flatten(),
    })
}

#[async_trait]
impl MigrationAuditStore for MysqlCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mysql"
    }

    async fn ensure_migration_audit_tables(&self) -> SystemStoreResult<()> {
        let runs_ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
                run_id            CHAR(36) NOT NULL PRIMARY KEY,
                project_id        VARCHAR(255) NOT NULL DEFAULT '',
                catalog_version   VARCHAR(255) NOT NULL DEFAULT '',
                state             VARCHAR(32) NOT NULL DEFAULT 'DRY_RUN',
                operations_hash   VARCHAR(255) NOT NULL DEFAULT '',
                approval_token    VARCHAR(255) NOT NULL DEFAULT '',
                started_at        TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                finished_at       TIMESTAMP(6) NULL,
                error             TEXT NOT NULL,
                CONSTRAINT chk_{RUNS_TABLE}_state CHECK (state IN ('DRY_RUN','PREFLIGHT','APPLYING','VERIFYING','COMPLETED','ERROR','DEAD_LETTER'))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        );
        let runs_idx = format!(
            "CREATE INDEX idx_{RUNS_TABLE}_project_state ON {RUNS_TABLE} (project_id, state, started_at)"
        );
        let ledger_ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
                id                BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                run_id            CHAR(36) NOT NULL,
                operation_index   INT NOT NULL,
                backend           VARCHAR(64) NOT NULL DEFAULT 'postgres',
                resource_uri      TEXT NOT NULL,
                operation_kind    VARCHAR(64) NOT NULL DEFAULT '',
                status            VARCHAR(32) NOT NULL DEFAULT 'PENDING',
                rollback_json     JSON NOT NULL,
                error             TEXT NOT NULL,
                applied_at        TIMESTAMP(6) NULL,
                CONSTRAINT chk_{LEDGER_TABLE}_status CHECK (status IN ('PENDING','APPLIED','VERIFIED','SKIPPED','FAILED','ROLLED_BACK')),
                CONSTRAINT fk_{LEDGER_TABLE}_run FOREIGN KEY (run_id) REFERENCES {RUNS_TABLE}(run_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        );
        let ledger_idx = format!(
            "CREATE INDEX idx_{LEDGER_TABLE}_run_idx ON {LEDGER_TABLE} (run_id, operation_index)"
        );
        sqlx::query(&runs_ddl)
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", runs_ddl.clone(), e))?;
        // Index creation idempotent across versions.
        if let Err(e) = sqlx::query(&runs_idx).execute(self.mysql_pool()).await {
            let msg = e.to_string();
            if !msg.contains("Duplicate key name") {
                return Err(SystemStoreError::query("mysql", runs_idx.clone(), e));
            }
        }
        sqlx::query(&ledger_ddl)
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", ledger_ddl.clone(), e))?;
        if let Err(e) = sqlx::query(&ledger_idx).execute(self.mysql_pool()).await {
            let msg = e.to_string();
            if !msg.contains("Duplicate key name") {
                return Err(SystemStoreError::query("mysql", ledger_idx.clone(), e));
            }
        }
        Ok(())
    }

    async fn start_migration_run(&self, run: &MigrationRunInsert) -> SystemStoreResult<Uuid> {
        let run_id = Uuid::new_v4();
        let sql = format!(
            "INSERT INTO {RUNS_TABLE} (
                run_id, project_id, catalog_version, state,
                operations_hash, approval_token, error
            ) VALUES (?, ?, ?, ?, ?, ?, '')"
        );
        sqlx::query(&sql)
            .bind(run_id.to_string())
            .bind(&run.project_id)
            .bind(&run.catalog_version)
            .bind(run.state.as_str())
            .bind(&run.operations_hash)
            .bind(&run.approval_token)
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        Ok(run_id)
    }

    async fn record_migration_op(&self, op: &MigrationOpInsert) -> SystemStoreResult<i64> {
        let rollback_text = serde_json::to_string(&op.rollback_json)
            .map_err(|e| SystemStoreError::InvalidInput(format!("rollback_json: {e}")))?;
        // applied_at: set to NOW(6) when status is APPLIED. MySQL
        // doesn't allow conditional expressions in DEFAULT, so we
        // branch the SQL.
        let applied_at_expr = if matches!(op.status, OpLedgerStatus::Applied) {
            "NOW(6)"
        } else {
            "NULL"
        };
        let sql = format!(
            "INSERT INTO {LEDGER_TABLE} (
                run_id, operation_index, backend, resource_uri,
                operation_kind, status, rollback_json, error, applied_at
            ) VALUES (?, ?, ?, ?, ?, ?, CAST(? AS JSON), ?, {applied_at_expr})"
        );
        let result = sqlx::query(&sql)
            .bind(op.run_id.to_string())
            .bind(op.operation_index)
            .bind(&op.backend)
            .bind(&op.resource_uri)
            .bind(&op.operation_kind)
            .bind(op.status.as_str())
            .bind(&rollback_text)
            .bind(&op.error)
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        Ok(result.last_insert_id() as i64)
    }

    async fn finish_migration_run(
        &self,
        run_id: Uuid,
        new_state: MigrationRunState,
        error: &str,
    ) -> SystemStoreResult<()> {
        let sql = format!(
            "UPDATE {RUNS_TABLE}
             SET state = ?, error = ?, finished_at = NOW(6)
             WHERE run_id = ?"
        );
        let result = sqlx::query(&sql)
            .bind(new_state.as_str())
            .bind(error)
            .bind(run_id.to_string())
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        if result.rows_affected() == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "migration run {run_id} not found for finish_migration_run"
            )));
        }
        Ok(())
    }

    async fn get_migration_run(&self, run_id: Uuid) -> SystemStoreResult<Option<MigrationRunRow>> {
        let sql = format!(
            "SELECT run_id, project_id, catalog_version, state,
                    operations_hash, approval_token,
                    started_at, finished_at, error
             FROM {RUNS_TABLE}
             WHERE run_id = ?"
        );
        let row = sqlx::query(&sql)
            .bind(run_id.to_string())
            .fetch_optional(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        match row {
            Some(r) => Ok(Some(row_to_run(r)?)),
            None => Ok(None),
        }
    }

    async fn list_migration_ops(&self, run_id: Uuid) -> SystemStoreResult<Vec<MigrationOpRow>> {
        let sql = format!(
            "SELECT id, run_id, operation_index, backend, resource_uri,
                    operation_kind, status, rollback_json, error, applied_at
             FROM {LEDGER_TABLE}
             WHERE run_id = ?
             ORDER BY operation_index ASC"
        );
        let rows = sqlx::query(&sql)
            .bind(run_id.to_string())
            .fetch_all(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(row_to_op(r)?);
        }
        Ok(out)
    }

    async fn list_migration_runs(
        &self,
        filter: &MigrationRunsFilter,
    ) -> SystemStoreResult<Vec<MigrationRunRow>> {
        let w = build_eq_where(
            SqlDialect::MYSQL,
            &[
                ("project_id", filter.project_id.is_some()),
                ("state", filter.state.is_some()),
                ("catalog_version", filter.catalog_version.is_some()),
            ],
        );
        let where_sql = &w.where_sql;
        let limit_placeholder = &w.limit_placeholder;
        let offset_placeholder = &w.offset_placeholder;
        let (limit, offset) = normalize_limit_offset(filter.limit, filter.offset);
        let sql = format!(
            "SELECT run_id, project_id, catalog_version, state,
                    operations_hash, approval_token,
                    started_at, finished_at, error
             FROM {RUNS_TABLE}
             {where_sql}
             ORDER BY started_at DESC
             LIMIT {limit_placeholder} OFFSET {offset_placeholder}"
        );
        let mut q = sqlx::query(&sql);
        if let Some(p) = &filter.project_id {
            q = q.bind(p.clone());
        }
        if let Some(s) = filter.state {
            q = q.bind(s.as_str());
        }
        if let Some(v) = &filter.catalog_version {
            q = q.bind(v.clone());
        }
        q = q.bind(limit).bind(offset);
        let rows = q
            .fetch_all(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(row_to_run(r)?);
        }
        Ok(out)
    }
}
