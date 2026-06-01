//! PostgreSQL implementation of [`MigrationAuditStore`].
//!
//! Schema mirrors the existing `runtime/system.rs` DDL: UUID PK on
//! `udb_migration_runs`, BIGSERIAL on `udb_migration_op_ledger`,
//! JSONB rollback, ON DELETE CASCADE FK, plus the existing indexes
//! `(project_id, state, started_at DESC)` and `(run_id, operation_index)`.
//!
//! SQL is byte-equivalent to what `PostgresMigrationAuditSink`
//! issues today so NW1 step 3 collapses the sink onto this trait
//! with zero behaviour change.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::dialect::{SqlDialect, build_eq_where, normalize_limit_offset};
use super::postgres::PostgresCanonicalStore;
use super::system_store::{
    MigrationAuditStore, MigrationOpInsert, MigrationOpRow, MigrationRunInsert, MigrationRunRow,
    MigrationRunState, MigrationRunsFilter, OpLedgerStatus, SystemStoreError, SystemStoreResult,
};

const DEFAULT_RUNS: &str = r#""udb_system"."udb_migration_runs""#;
const DEFAULT_LEDGER: &str = r#""udb_system"."udb_migration_op_ledger""#;

impl PostgresCanonicalStore {
    pub fn with_migration_relations(
        mut self,
        runs: impl Into<String>,
        ledger: impl Into<String>,
    ) -> Self {
        self.migration_runs_relation = Some(runs.into());
        self.migration_ledger_relation = Some(ledger.into());
        self
    }

    fn runs_rel(&self) -> &str {
        self.migration_runs_relation
            .as_deref()
            .unwrap_or(DEFAULT_RUNS)
    }
    fn ledger_rel(&self) -> &str {
        self.migration_ledger_relation
            .as_deref()
            .unwrap_or(DEFAULT_LEDGER)
    }
}

fn row_to_run(row: sqlx::postgres::PgRow) -> SystemStoreResult<MigrationRunRow> {
    let run_id: Uuid = row
        .try_get("run_id")
        .map_err(|e| SystemStoreError::query("postgres", "SELECT run_id", e))?;
    let state_str: String = row
        .try_get("state")
        .map_err(|e| SystemStoreError::query("postgres", "SELECT state", e))?;
    let state = MigrationRunState::parse(&state_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown migration run state '{state_str}' in PG row"
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

fn row_to_op(row: sqlx::postgres::PgRow) -> SystemStoreResult<MigrationOpRow> {
    let run_id: Uuid = row
        .try_get("run_id")
        .map_err(|e| SystemStoreError::query("postgres", "SELECT run_id", e))?;
    let status_str: String = row
        .try_get("status")
        .map_err(|e| SystemStoreError::query("postgres", "SELECT status", e))?;
    let status = OpLedgerStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!("unknown op ledger status '{status_str}' in PG row"))
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
impl MigrationAuditStore for PostgresCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "postgres"
    }

    async fn ensure_migration_audit_tables(&self) -> SystemStoreResult<()> {
        let runs_rel = self.runs_rel();
        let ledger_rel = self.ledger_rel();
        let stmts = [
            format!(
                r#"
                CREATE TABLE IF NOT EXISTS {runs_rel} (
                    run_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    project_id        TEXT NOT NULL DEFAULT '',
                    catalog_version   TEXT NOT NULL DEFAULT '',
                    state             TEXT NOT NULL DEFAULT 'DRY_RUN'
                                      CHECK (state IN ('DRY_RUN','PREFLIGHT','APPLYING','VERIFYING','COMPLETED','ERROR','DEAD_LETTER')),
                    operations_hash   TEXT NOT NULL DEFAULT '',
                    approval_token    TEXT NOT NULL DEFAULT '',
                    started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at       TIMESTAMPTZ,
                    error             TEXT NOT NULL DEFAULT ''
                )
                "#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_migration_runs_project_state"
                     ON {runs_rel} (project_id, state, started_at DESC)"#
            ),
            format!(
                r#"
                CREATE TABLE IF NOT EXISTS {ledger_rel} (
                    id                BIGSERIAL PRIMARY KEY,
                    run_id            UUID NOT NULL REFERENCES {runs_rel}(run_id) ON DELETE CASCADE,
                    operation_index   INTEGER NOT NULL,
                    backend           TEXT NOT NULL DEFAULT 'postgres',
                    resource_uri      TEXT NOT NULL DEFAULT '',
                    operation_kind    TEXT NOT NULL DEFAULT '',
                    status            TEXT NOT NULL DEFAULT 'PENDING'
                                      CHECK (status IN ('PENDING','APPLIED','VERIFIED','SKIPPED','FAILED','ROLLED_BACK')),
                    rollback_json     JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                    error             TEXT NOT NULL DEFAULT '',
                    applied_at        TIMESTAMPTZ
                )
                "#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_migration_op_ledger_run_idx"
                     ON {ledger_rel} (run_id, operation_index)"#
            ),
        ];
        for sql in stmts.iter() {
            sqlx::query(sql)
                .execute(self.pg_pool())
                .await
                .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        }
        Ok(())
    }

    async fn start_migration_run(&self, run: &MigrationRunInsert) -> SystemStoreResult<Uuid> {
        let rel = self.runs_rel();
        let sql = format!(
            r#"INSERT INTO {rel} (
                project_id, catalog_version, state,
                operations_hash, approval_token, started_at
            ) VALUES ($1, $2, $3, $4, $5, NOW())
            RETURNING run_id"#
        );
        let run_id: Uuid = sqlx::query_scalar(&sql)
            .bind(&run.project_id)
            .bind(&run.catalog_version)
            .bind(run.state.as_str())
            .bind(&run.operations_hash)
            .bind(&run.approval_token)
            .fetch_one(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        Ok(run_id)
    }

    async fn record_migration_op(&self, op: &MigrationOpInsert) -> SystemStoreResult<i64> {
        let rel = self.ledger_rel();
        // applied_at is set when the status is APPLIED, mirroring
        // the existing PostgresMigrationAuditSink.
        let sql = format!(
            r#"INSERT INTO {rel} (
                run_id, operation_index, backend, resource_uri,
                operation_kind, status, rollback_json, error, applied_at
            ) VALUES (
                $1::UUID, $2, $3, $4, $5, $6, $7::jsonb, $8,
                CASE WHEN $6 = 'APPLIED' THEN NOW() ELSE NULL END
            )
            RETURNING id::BIGINT"#
        );
        let id: i64 = sqlx::query_scalar(&sql)
            .bind(op.run_id)
            .bind(op.operation_index)
            .bind(&op.backend)
            .bind(&op.resource_uri)
            .bind(&op.operation_kind)
            .bind(op.status.as_str())
            .bind(op.rollback_json.to_string())
            .bind(&op.error)
            .fetch_one(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        Ok(id)
    }

    async fn finish_migration_run(
        &self,
        run_id: Uuid,
        new_state: MigrationRunState,
        error: &str,
    ) -> SystemStoreResult<()> {
        let rel = self.runs_rel();
        let sql = format!(
            r#"UPDATE {rel}
               SET state = $2, error = $3, finished_at = NOW()
               WHERE run_id = $1"#
        );
        let result = sqlx::query(&sql)
            .bind(run_id)
            .bind(new_state.as_str())
            .bind(error)
            .execute(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        if result.rows_affected() == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "migration run {run_id} not found for finish_migration_run"
            )));
        }
        Ok(())
    }

    async fn get_migration_run(&self, run_id: Uuid) -> SystemStoreResult<Option<MigrationRunRow>> {
        let rel = self.runs_rel();
        let sql = format!(
            r#"SELECT run_id, project_id, catalog_version, state,
                      operations_hash, approval_token,
                      started_at, finished_at, error
               FROM {rel}
               WHERE run_id = $1"#
        );
        let row = sqlx::query(&sql)
            .bind(run_id)
            .fetch_optional(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        match row {
            Some(r) => Ok(Some(row_to_run(r)?)),
            None => Ok(None),
        }
    }

    async fn list_migration_ops(&self, run_id: Uuid) -> SystemStoreResult<Vec<MigrationOpRow>> {
        let rel = self.ledger_rel();
        let sql = format!(
            r#"SELECT id, run_id, operation_index, backend, resource_uri,
                      operation_kind, status, rollback_json, error, applied_at
               FROM {rel}
               WHERE run_id = $1
               ORDER BY operation_index ASC"#
        );
        let rows = sqlx::query(&sql)
            .bind(run_id)
            .fetch_all(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
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
        let rel = self.runs_rel();
        let w = build_eq_where(
            SqlDialect::POSTGRES,
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
            r#"SELECT run_id, project_id, catalog_version, state,
                      operations_hash, approval_token,
                      started_at, finished_at, error
               FROM {rel}
               {where_sql}
               ORDER BY started_at DESC
               LIMIT {limit_placeholder} OFFSET {offset_placeholder}"#
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
            .fetch_all(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(row_to_run(r)?);
        }
        Ok(out)
    }
}
