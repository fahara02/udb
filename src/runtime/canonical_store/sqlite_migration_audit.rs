//! SQLite implementation of [`MigrationAuditStore`].
//!
//! ## Dialect choices
//!
//! - `run_id` is TEXT UUID, generated Rust-side.
//! - `id` is `INTEGER PRIMARY KEY AUTOINCREMENT` (auto-incremented).
//! - `rollback_json` is TEXT (parsed via `serde_json` on read).
//! - `started_at` / `finished_at` / `applied_at` are RFC-3339 TEXT.
//! - CHECK constraints on both `state` and `status` mirror PG.
//! - Foreign key with `ON DELETE CASCADE` (requires `PRAGMA foreign_keys = ON`;
//!   `ensure_migration_audit_tables` issues that pragma).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::dialect::{SqlDialect, build_eq_where, normalize_limit_offset};
use super::sqlite::SqliteCanonicalStore;
use super::system_store::{
    MigrationAuditStore, MigrationOpInsert, MigrationOpRow, MigrationRunInsert, MigrationRunRow,
    MigrationRunState, MigrationRunsFilter, OpLedgerStatus, SystemStoreError, SystemStoreResult,
};

const RUNS_TABLE: &str = "udb_migration_runs";
const LEDGER_TABLE: &str = "udb_migration_op_ledger";

fn parse_iso(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

fn parse_iso_opt(s: Option<String>) -> Option<DateTime<Utc>> {
    s.filter(|s| !s.is_empty()).map(|s| parse_iso(&s))
}

fn row_to_run(row: sqlx::sqlite::SqliteRow) -> SystemStoreResult<MigrationRunRow> {
    let run_id_str: String = row
        .try_get("run_id")
        .map_err(|e| SystemStoreError::query("sqlite", "SELECT run_id", e))?;
    let run_id = Uuid::parse_str(&run_id_str).map_err(|e| {
        SystemStoreError::InvalidInput(format!("run_id '{run_id_str}' is not a valid UUID: {e}"))
    })?;
    let state_str: String = row
        .try_get("state")
        .map_err(|e| SystemStoreError::query("sqlite", "SELECT state", e))?;
    let state = MigrationRunState::parse(&state_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown migration run state '{state_str}' in SQLite row"
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
            .try_get::<String, _>("started_at")
            .map(|s| parse_iso(&s))
            .unwrap_or_else(|_| Utc::now()),
        finished_at: parse_iso_opt(
            row.try_get::<Option<String>, _>("finished_at")
                .ok()
                .flatten(),
        ),
        error: row.try_get("error").unwrap_or_default(),
    })
}

fn row_to_op(row: sqlx::sqlite::SqliteRow) -> SystemStoreResult<MigrationOpRow> {
    let run_id_str: String = row
        .try_get("run_id")
        .map_err(|e| SystemStoreError::query("sqlite", "SELECT run_id", e))?;
    let run_id = Uuid::parse_str(&run_id_str).map_err(|e| {
        SystemStoreError::InvalidInput(format!("run_id '{run_id_str}' is not a valid UUID: {e}"))
    })?;
    let status_str: String = row
        .try_get("status")
        .map_err(|e| SystemStoreError::query("sqlite", "SELECT status", e))?;
    let status = OpLedgerStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown op ledger status '{status_str}' in SQLite row"
        ))
    })?;
    let rollback_text: String = row.try_get("rollback_json").unwrap_or_default();
    let rollback_json = if rollback_text.is_empty() {
        serde_json::Value::Object(Default::default())
    } else {
        serde_json::from_str(&rollback_text).map_err(|e| {
            SystemStoreError::InvalidInput(format!(
                "rollback_json is not valid JSON: {e} (raw: '{rollback_text}')"
            ))
        })?
    };
    Ok(MigrationOpRow {
        id: row.try_get("id").unwrap_or(0),
        run_id,
        operation_index: row.try_get("operation_index").unwrap_or(0),
        backend: row.try_get("backend").unwrap_or_default(),
        resource_uri: row.try_get("resource_uri").unwrap_or_default(),
        operation_kind: row.try_get("operation_kind").unwrap_or_default(),
        status,
        rollback_json,
        error: row.try_get("error").unwrap_or_default(),
        applied_at: parse_iso_opt(
            row.try_get::<Option<String>, _>("applied_at")
                .ok()
                .flatten(),
        ),
    })
}

#[async_trait]
impl MigrationAuditStore for SqliteCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "sqlite"
    }

    async fn ensure_migration_audit_tables(&self) -> SystemStoreResult<()> {
        // SQLite requires foreign_keys pragma to enforce FK clauses.
        // It's per-connection but the sqlx pool typically caches it.
        let pragma = "PRAGMA foreign_keys = ON";
        sqlx::query(pragma)
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", pragma, e))?;

        let runs_ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
                run_id            TEXT PRIMARY KEY,
                project_id        TEXT NOT NULL DEFAULT '',
                catalog_version   TEXT NOT NULL DEFAULT '',
                state             TEXT NOT NULL DEFAULT 'DRY_RUN'
                                  CHECK (state IN ('DRY_RUN','PREFLIGHT','APPLYING','VERIFYING','COMPLETED','ERROR','DEAD_LETTER')),
                operations_hash   TEXT NOT NULL DEFAULT '',
                approval_token    TEXT NOT NULL DEFAULT '',
                started_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                finished_at       TEXT,
                error             TEXT NOT NULL DEFAULT ''
            )
            "#
        );
        let runs_idx = format!(
            "CREATE INDEX IF NOT EXISTS idx_{RUNS_TABLE}_project_state \
             ON {RUNS_TABLE} (project_id, state, started_at DESC)"
        );
        let ledger_ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id            TEXT NOT NULL,
                operation_index   INTEGER NOT NULL,
                backend           TEXT NOT NULL DEFAULT 'postgres',
                resource_uri      TEXT NOT NULL DEFAULT '',
                operation_kind    TEXT NOT NULL DEFAULT '',
                status            TEXT NOT NULL DEFAULT 'PENDING'
                                  CHECK (status IN ('PENDING','APPLIED','VERIFIED','SKIPPED','FAILED','ROLLED_BACK')),
                rollback_json     TEXT NOT NULL DEFAULT '{{}}',
                error             TEXT NOT NULL DEFAULT '',
                applied_at        TEXT,
                FOREIGN KEY (run_id) REFERENCES {RUNS_TABLE}(run_id) ON DELETE CASCADE
            )
            "#
        );
        let ledger_idx = format!(
            "CREATE INDEX IF NOT EXISTS idx_{LEDGER_TABLE}_run_idx \
             ON {LEDGER_TABLE} (run_id, operation_index)"
        );
        for sql in [runs_ddl, runs_idx, ledger_ddl, ledger_idx] {
            sqlx::query(&sql)
                .execute(self.pool_ref())
                .await
                .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        }
        Ok(())
    }

    async fn start_migration_run(&self, run: &MigrationRunInsert) -> SystemStoreResult<Uuid> {
        let run_id = Uuid::new_v4();
        let sql = format!(
            "INSERT INTO {RUNS_TABLE} (
                run_id, project_id, catalog_version, state,
                operations_hash, approval_token
            ) VALUES (?, ?, ?, ?, ?, ?)"
        );
        sqlx::query(&sql)
            .bind(run_id.to_string())
            .bind(&run.project_id)
            .bind(&run.catalog_version)
            .bind(run.state.as_str())
            .bind(&run.operations_hash)
            .bind(&run.approval_token)
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        Ok(run_id)
    }

    async fn record_migration_op(&self, op: &MigrationOpInsert) -> SystemStoreResult<i64> {
        let rollback_text = serde_json::to_string(&op.rollback_json)
            .map_err(|e| SystemStoreError::InvalidInput(format!("rollback_json: {e}")))?;
        // applied_at is set to now() when the new status is APPLIED;
        // NULL otherwise. This matches the existing PG behavior in
        // PostgresMigrationAuditSink::record_op.
        let applied_at_expr = if matches!(op.status, OpLedgerStatus::Applied) {
            "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
        } else {
            "NULL"
        };
        let sql = format!(
            "INSERT INTO {LEDGER_TABLE} (
                run_id, operation_index, backend, resource_uri,
                operation_kind, status, rollback_json, error, applied_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, {applied_at_expr})"
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
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        Ok(result.last_insert_rowid())
    }

    async fn finish_migration_run(
        &self,
        run_id: Uuid,
        new_state: MigrationRunState,
        error: &str,
    ) -> SystemStoreResult<()> {
        let sql = format!(
            "UPDATE {RUNS_TABLE}
             SET state = ?, error = ?, finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE run_id = ?"
        );
        let result = sqlx::query(&sql)
            .bind(new_state.as_str())
            .bind(error)
            .bind(run_id.to_string())
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
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
            .fetch_optional(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
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
            .fetch_all(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
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
            SqlDialect::SQLITE,
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
            .fetch_all(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(row_to_run(r)?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn fresh_store() -> SqliteCanonicalStore {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite");
        let store = SqliteCanonicalStore::new(pool, "test", "udb_outbox_events");
        MigrationAuditStore::ensure_migration_audit_tables(&store)
            .await
            .expect("DDL");
        store
    }

    fn sample_run(project: &str) -> MigrationRunInsert {
        MigrationRunInsert {
            project_id: project.to_string(),
            catalog_version: "v1".to_string(),
            operations_hash: "sha256:abc".to_string(),
            approval_token: "tok-123".to_string(),
            state: MigrationRunState::Applying,
        }
    }

    fn sample_op(run_id: Uuid, idx: i32, status: OpLedgerStatus) -> MigrationOpInsert {
        MigrationOpInsert {
            run_id,
            operation_index: idx,
            backend: "postgres".to_string(),
            resource_uri: format!("udb://postgres/op-{idx}.sql"),
            operation_kind: "apply".to_string(),
            status,
            rollback_json: serde_json::json!({"undo": format!("DROP TABLE op_{idx}")}),
            error: String::new(),
        }
    }

    /// Pin: start_run + record_op + finish_run + get_run + list_ops
    /// round-trip.
    #[tokio::test]
    async fn full_run_round_trip() {
        let store = fresh_store().await;
        let id = store
            .start_migration_run(&sample_run("alpha"))
            .await
            .expect("start");
        assert_ne!(id, Uuid::nil());

        for i in 0..3 {
            let _ = store
                .record_migration_op(&sample_op(id, i, OpLedgerStatus::Applied))
                .await
                .expect("record op");
        }
        store
            .finish_migration_run(id, MigrationRunState::Completed, "")
            .await
            .expect("finish");

        let row = store
            .get_migration_run(id)
            .await
            .expect("get")
            .expect("Some");
        assert_eq!(row.run_id, id);
        assert_eq!(row.state, MigrationRunState::Completed);
        assert_eq!(row.project_id, "alpha");
        assert_eq!(row.catalog_version, "v1");
        assert!(row.finished_at.is_some());
        assert_eq!(row.error, "");

        let ops = store.list_migration_ops(id).await.expect("list ops");
        assert_eq!(ops.len(), 3);
        for (i, op) in ops.iter().enumerate() {
            assert_eq!(op.operation_index, i as i32);
            assert_eq!(op.status, OpLedgerStatus::Applied);
            assert_eq!(op.backend, "postgres");
            assert!(op.applied_at.is_some(), "applied status sets applied_at");
            assert!(
                op.rollback_json["undo"]
                    .as_str()
                    .map(|s| s.starts_with("DROP TABLE op_"))
                    .unwrap_or(false)
            );
        }
    }

    /// Pin: applied_at is NULL for non-APPLIED statuses.
    #[tokio::test]
    async fn applied_at_only_set_on_applied_status() {
        let store = fresh_store().await;
        let id = store.start_migration_run(&sample_run("p")).await.unwrap();
        store
            .record_migration_op(&sample_op(id, 0, OpLedgerStatus::Pending))
            .await
            .unwrap();
        store
            .record_migration_op(&sample_op(id, 1, OpLedgerStatus::Skipped))
            .await
            .unwrap();
        store
            .record_migration_op(&sample_op(id, 2, OpLedgerStatus::Failed))
            .await
            .unwrap();
        let ops = store.list_migration_ops(id).await.unwrap();
        for op in ops {
            assert!(
                op.applied_at.is_none(),
                "non-APPLIED status must not set applied_at (got {:?} for status {:?})",
                op.applied_at,
                op.status
            );
        }
    }

    /// Pin: finish_run on missing run errors. Defends against
    /// caller writing a finish for a run that was rolled back.
    #[tokio::test]
    async fn finish_run_on_missing_errors() {
        let store = fresh_store().await;
        let phantom = Uuid::new_v4();
        let err = store
            .finish_migration_run(phantom, MigrationRunState::Completed, "")
            .await
            .expect_err("must error");
        match err {
            SystemStoreError::InvalidInput(msg) => assert!(msg.contains("not found")),
            other => panic!("expected InvalidInput, got: {other}"),
        }
    }

    /// Pin: get_run on missing returns Ok(None).
    #[tokio::test]
    async fn get_run_returns_none_when_missing() {
        let store = fresh_store().await;
        let row = store.get_migration_run(Uuid::new_v4()).await.unwrap();
        assert!(row.is_none());
    }

    /// Pin: list_migration_runs filters by project_id, state,
    /// catalog_version.
    #[tokio::test]
    async fn list_runs_honours_filters() {
        let store = fresh_store().await;
        let _ = store
            .start_migration_run(&sample_run("alpha"))
            .await
            .unwrap();
        let mut beta_run = sample_run("beta");
        beta_run.state = MigrationRunState::Completed;
        let _ = store.start_migration_run(&beta_run).await.unwrap();
        let mut alpha2 = sample_run("alpha");
        alpha2.catalog_version = "v2".to_string();
        let _ = store.start_migration_run(&alpha2).await.unwrap();

        // Filter by project.
        let alpha = store
            .list_migration_runs(&MigrationRunsFilter {
                project_id: Some("alpha".to_string()),
                limit: 100,
                ..MigrationRunsFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(alpha.len(), 2);

        // Filter by state.
        let completed = store
            .list_migration_runs(&MigrationRunsFilter {
                state: Some(MigrationRunState::Completed),
                limit: 100,
                ..MigrationRunsFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].project_id, "beta");

        // Filter by catalog_version.
        let v2 = store
            .list_migration_runs(&MigrationRunsFilter {
                catalog_version: Some("v2".to_string()),
                limit: 100,
                ..MigrationRunsFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(v2.len(), 1);
    }

    /// Pin: FK cascade — deleting a run also deletes its op rows.
    /// The schema declared ON DELETE CASCADE; verify it actually
    /// fires (SQLite needs PRAGMA foreign_keys=ON, which our DDL
    /// sets).
    #[tokio::test]
    async fn run_delete_cascades_to_op_ledger() {
        let store = fresh_store().await;
        let id = store.start_migration_run(&sample_run("p")).await.unwrap();
        for i in 0..3 {
            store
                .record_migration_op(&sample_op(id, i, OpLedgerStatus::Applied))
                .await
                .unwrap();
        }
        assert_eq!(store.list_migration_ops(id).await.unwrap().len(), 3);

        // Delete the run row directly.
        sqlx::query(&format!("DELETE FROM {RUNS_TABLE} WHERE run_id = ?"))
            .bind(id.to_string())
            .execute(store.pool_ref())
            .await
            .unwrap();

        assert_eq!(
            store.list_migration_ops(id).await.unwrap().len(),
            0,
            "FK ON DELETE CASCADE should have removed the op rows"
        );
    }
}
