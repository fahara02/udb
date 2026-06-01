//! MySQL implementation of [`AdminAuditStore`].
//!
//! ## Dialect choices
//!
//! - `CHAR(36)` UUID, native `JSON` column, `TIMESTAMP(6)` for
//!   microsecond precision.
//! - **Atomic chain append**: `GET_LOCK('udb_admin_audit_chain', 30)`
//!   acquires a named user-level lock for up to 30 seconds. The lock
//!   is held by the connection (not the transaction), so we explicitly
//!   `RELEASE_LOCK` after `COMMIT`. Two concurrent appends serialize
//!   on this lock.
//! - Indexes on `(operation, created_at DESC)` and `(current_hash)`,
//!   tolerated as Duplicate-key for cross-version idempotent DDL.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{Connection, Row, mysql::MySqlConnection};
use uuid::Uuid;

use super::dialect::{SqlDialect, build_eq_where, normalize_limit_offset};
use super::mysql::MysqlCanonicalStore;
use super::system_store::{
    AdminAuditChainReport, AdminAuditInsert, AdminAuditListFilter, AdminAuditRow, AdminAuditStore,
    SystemStoreError, SystemStoreResult, compute_admin_audit_hash, verify_admin_audit_chain_step,
};

const TABLE: &str = "udb_admin_audit_log";
const CHAIN_LOCK_NAME: &str = "udb_admin_audit_chain";
fn row_to_audit(row: sqlx::mysql::MySqlRow) -> SystemStoreResult<AdminAuditRow> {
    let audit_id_str: String = row
        .try_get("audit_id")
        .map_err(|e| SystemStoreError::query("mysql", "SELECT audit_id", e))?;
    let audit_id = Uuid::parse_str(&audit_id_str).map_err(|e| {
        SystemStoreError::InvalidInput(format!(
            "audit_id '{audit_id_str}' is not a valid UUID: {e}"
        ))
    })?;
    Ok(AdminAuditRow {
        audit_id,
        actor: row.try_get("actor").unwrap_or_default(),
        operation: row.try_get("operation").unwrap_or_default(),
        target: row.try_get("target").unwrap_or_default(),
        request_json: row
            .try_get("request_json")
            .unwrap_or(serde_json::Value::Null),
        result: row.try_get("result").unwrap_or_default(),
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        project_id: row.try_get("project_id").unwrap_or_default(),
        correlation_id: row.try_get("correlation_id").unwrap_or_default(),
        previous_hash: row.try_get("previous_hash").unwrap_or_default(),
        current_hash: row.try_get("current_hash").unwrap_or_default(),
        signer_key_id: row.try_get("signer_key_id").unwrap_or_default(),
        external_anchor: row.try_get("external_anchor").unwrap_or_default(),
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .unwrap_or_else(|_| Utc::now()),
    })
}

#[async_trait]
impl AdminAuditStore for MysqlCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mysql"
    }

    async fn ensure_admin_audit_tables(&self) -> SystemStoreResult<()> {
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {TABLE} (
                audit_id         CHAR(36) NOT NULL PRIMARY KEY,
                actor            VARCHAR(255) NOT NULL DEFAULT '',
                operation        VARCHAR(255) NOT NULL,
                target           VARCHAR(255) NOT NULL DEFAULT '',
                request_json     JSON NOT NULL,
                result           VARCHAR(32) NOT NULL DEFAULT 'ok',
                tenant_id        VARCHAR(255) NOT NULL DEFAULT '',
                project_id       VARCHAR(255) NOT NULL DEFAULT '',
                correlation_id   VARCHAR(255) NOT NULL DEFAULT '',
                previous_hash    VARCHAR(128) NOT NULL DEFAULT '',
                current_hash     VARCHAR(128) NOT NULL DEFAULT '',
                signer_key_id    VARCHAR(255) NOT NULL DEFAULT '',
                external_anchor  TEXT NOT NULL,
                created_at       TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        );
        let idx_op = format!("CREATE INDEX idx_{TABLE}_op ON {TABLE} (operation, created_at)");
        let idx_hash = format!("CREATE INDEX idx_{TABLE}_hash ON {TABLE} (current_hash)");
        sqlx::query(&create_table)
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", create_table.clone(), e))?;
        for sql in [&idx_op, &idx_hash] {
            if let Err(e) = sqlx::query(sql).execute(self.mysql_pool()).await {
                let msg = e.to_string();
                if !msg.contains("Duplicate key name") && !msg.contains("already exists") {
                    return Err(SystemStoreError::query("mysql", sql.clone(), e));
                }
            }
        }
        Ok(())
    }

    async fn latest_admin_audit_hash(&self) -> SystemStoreResult<String> {
        let sql = format!(
            "SELECT current_hash FROM {TABLE} \
             WHERE current_hash <> '' \
             ORDER BY created_at DESC, audit_id DESC LIMIT 1"
        );
        let hash: Option<String> = sqlx::query_scalar(&sql)
            .fetch_optional(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        Ok(hash.unwrap_or_default())
    }

    async fn append_admin_audit(&self, entry: &AdminAuditInsert) -> SystemStoreResult<Uuid> {
        // MySQL named locks are CONNECTION-scoped. GET_LOCK, the read+insert
        // transaction, and RELEASE_LOCK must therefore all run on the SAME
        // pinned connection — otherwise the lock serializes nothing (the tx
        // runs on a different pooled connection) and RELEASE_LOCK on yet
        // another connection is a silent no-op. Acquire one connection and
        // hold it for the entire critical section.
        let mut conn = self
            .mysql_pool()
            .acquire()
            .await
            .map_err(|e| SystemStoreError::io("mysql", e))?;

        let lock_acquired: Option<(i64,)> = sqlx::query_as("SELECT GET_LOCK(?, ?)")
            .bind(CHAIN_LOCK_NAME)
            .bind(30i64)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| SystemStoreError::query("mysql", "GET_LOCK", e))?;
        // `GET_LOCK` returns 1 on success, 0 on timeout, NULL on error.
        let acquired = matches!(lock_acquired, Some((1,)));
        if !acquired {
            return Err(SystemStoreError::Io {
                backend: "mysql",
                source: format!("could not acquire chain lock '{CHAIN_LOCK_NAME}' within 30s"),
            });
        }
        // From here on we MUST release the lock. Run the tx on the same conn,
        // then release on the same conn (success or failure).
        let result = self.append_admin_audit_locked(&mut conn, entry).await;
        let _ = sqlx::query("SELECT RELEASE_LOCK(?)")
            .bind(CHAIN_LOCK_NAME)
            .execute(&mut *conn)
            .await;
        result
    }

    async fn list_admin_audit(
        &self,
        filter: &AdminAuditListFilter,
    ) -> SystemStoreResult<Vec<AdminAuditRow>> {
        let w = build_eq_where(
            SqlDialect::MYSQL,
            &[
                ("operation", filter.operation.is_some()),
                ("actor", filter.actor.is_some()),
                ("tenant_id", filter.tenant_id.is_some()),
                ("project_id", filter.project_id.is_some()),
            ],
        );
        let where_sql = &w.where_sql;
        let limit_placeholder = &w.limit_placeholder;
        let offset_placeholder = &w.offset_placeholder;
        let (limit, offset) = normalize_limit_offset(filter.limit, filter.offset);
        let sql = format!(
            "SELECT audit_id, actor, operation, target, request_json, result,
                    tenant_id, project_id, correlation_id,
                    previous_hash, current_hash, signer_key_id, external_anchor,
                    created_at
             FROM {TABLE}
             {where_sql}
             ORDER BY created_at DESC
             LIMIT {limit_placeholder} OFFSET {offset_placeholder}"
        );
        let mut q = sqlx::query(&sql);
        if let Some(o) = &filter.operation {
            q = q.bind(o.clone());
        }
        if let Some(a) = &filter.actor {
            q = q.bind(a.clone());
        }
        if let Some(t) = &filter.tenant_id {
            q = q.bind(t.clone());
        }
        if let Some(p) = &filter.project_id {
            q = q.bind(p.clone());
        }
        q = q.bind(limit).bind(offset);
        let rows = q
            .fetch_all(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let mut row = row_to_audit(r)?;
            if filter.redact_request_json {
                row.request_json = serde_json::json!({"redacted": true});
            }
            out.push(row);
        }
        Ok(out)
    }

    async fn verify_admin_audit_chain(
        &self,
        limit: Option<i64>,
    ) -> SystemStoreResult<AdminAuditChainReport> {
        let mut conn = self
            .mysql_pool()
            .acquire()
            .await
            .map_err(|e| SystemStoreError::io("mysql", e))?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                SystemStoreError::query("mysql", "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", e)
            })?;
        sqlx::query("START TRANSACTION")
            .execute(&mut *conn)
            .await
            .map_err(|e| SystemStoreError::query("mysql", "START TRANSACTION", e))?;
        let mut previous_hash = String::new();
        let mut checked: i64 = 0;
        let mut offset: i64 = 0;
        let final_report = loop {
            let remaining = match limit {
                Some(n) if n > 0 => (n - checked).max(0),
                _ => i64::MAX,
            };
            if remaining == 0 {
                break AdminAuditChainReport::Passed {
                    checked_count: checked,
                    last_hash: previous_hash.clone(),
                };
            }
            let page = remaining.min(super::dialect::admin_audit_verify_page_size());
            let sql = format!(
                "SELECT audit_id, actor, operation, target, request_json, result,
                        tenant_id, project_id, correlation_id,
                        previous_hash, current_hash, signer_key_id, external_anchor,
                        created_at
                 FROM {TABLE}
                 ORDER BY created_at ASC, audit_id ASC
                 LIMIT ? OFFSET ?"
            );
            let rows = match sqlx::query(&sql)
                .bind(page)
                .bind(offset)
                .fetch_all(&mut *conn)
                .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                    return Err(SystemStoreError::query("mysql", sql.clone(), e));
                }
            };
            if rows.is_empty() {
                break AdminAuditChainReport::Passed {
                    checked_count: checked,
                    last_hash: previous_hash.clone(),
                };
            }
            let n_rows = rows.len() as i64;
            let mut tamper: Option<AdminAuditChainReport> = None;
            for r in rows {
                let row = match row_to_audit(r) {
                    Ok(row) => row,
                    Err(err) => {
                        let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                        return Err(err);
                    }
                };
                match verify_admin_audit_chain_step(&row, &previous_hash, checked) {
                    Ok(next) => {
                        previous_hash = next;
                        checked += 1;
                    }
                    Err(report) => {
                        tamper = Some(report);
                        break;
                    }
                }
            }
            if let Some(report) = tamper {
                break report;
            }
            offset += n_rows;
            if n_rows < page {
                break AdminAuditChainReport::Passed {
                    checked_count: checked,
                    last_hash: previous_hash.clone(),
                };
            }
        };
        sqlx::query("COMMIT")
            .execute(&mut *conn)
            .await
            .map_err(|e| SystemStoreError::query("mysql", "COMMIT", e))?;
        Ok(final_report)
    }
}

impl MysqlCanonicalStore {
    /// Inner — caller MUST have acquired the chain lock; this runs
    /// the read-latest + INSERT inside a transaction. The named lock
    /// gives us serialization across concurrent calls.
    async fn append_admin_audit_locked(
        &self,
        conn: &mut MySqlConnection,
        entry: &AdminAuditInsert,
    ) -> SystemStoreResult<Uuid> {
        let mut tx = conn
            .begin()
            .await
            .map_err(|e| SystemStoreError::io("mysql", e))?;
        let latest_sql = format!(
            "SELECT current_hash FROM {TABLE} \
             WHERE current_hash <> '' \
             ORDER BY created_at DESC, audit_id DESC LIMIT 1"
        );
        let previous_hash: Option<String> = sqlx::query_scalar::<_, Option<String>>(&latest_sql)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| SystemStoreError::query("mysql", latest_sql.clone(), e))?
            .flatten();
        let previous_hash = previous_hash.unwrap_or_default();
        let current_hash = compute_admin_audit_hash(
            &previous_hash,
            &entry.actor,
            &entry.operation,
            &entry.target,
            &entry.request_json,
            &entry.result,
            &entry.tenant_id,
            &entry.project_id,
            &entry.correlation_id,
            &entry.signer_key_id,
            &entry.external_anchor,
        );
        let audit_id = Uuid::new_v4();
        let request_json_text = serde_json::to_string(&entry.request_json)
            .map_err(|e| SystemStoreError::InvalidInput(format!("request_json: {e}")))?;
        let insert_sql = format!(
            "INSERT INTO {TABLE} (
                audit_id, actor, operation, target, request_json, result,
                tenant_id, project_id, correlation_id,
                previous_hash, current_hash, signer_key_id, external_anchor
            ) VALUES (?, ?, ?, ?, CAST(? AS JSON), ?, ?, ?, ?, ?, ?, ?, ?)"
        );
        sqlx::query(&insert_sql)
            .bind(audit_id.to_string())
            .bind(&entry.actor)
            .bind(&entry.operation)
            .bind(&entry.target)
            .bind(&request_json_text)
            .bind(&entry.result)
            .bind(&entry.tenant_id)
            .bind(&entry.project_id)
            .bind(&entry.correlation_id)
            .bind(&previous_hash)
            .bind(&current_hash)
            .bind(&entry.signer_key_id)
            .bind(&entry.external_anchor)
            .execute(&mut *tx)
            .await
            .map_err(|e| SystemStoreError::query("mysql", insert_sql.clone(), e))?;
        tx.commit()
            .await
            .map_err(|e| SystemStoreError::io("mysql", e))?;
        Ok(audit_id)
    }
}
