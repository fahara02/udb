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
use sqlx::Row;
use uuid::Uuid;

use super::mysql::MysqlCanonicalStore;
use super::system_store::{
    AdminAuditChainReport, AdminAuditInsert, AdminAuditListFilter, AdminAuditRow, AdminAuditStore,
    SystemStoreError, SystemStoreResult, compute_admin_audit_hash, verify_admin_audit_chain_step,
};

const TABLE: &str = "udb_admin_audit_log";
const CHAIN_LOCK_NAME: &str = "udb_admin_audit_chain";
const VERIFY_PAGE_SIZE: i64 = 1000;

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
        // MySQL pattern: acquire the named lock, run the read+insert
        // in a transaction (the lock is connection-scoped, not
        // tx-scoped, so a clean release after commit), release.
        //
        // We grab the lock *outside* the transaction so the lock is
        // held across the entire tx + commit. The wait is 30s; if it
        // doesn't acquire, we bail with an error.
        let lock_acquired: Option<(i64,)> = sqlx::query_as("SELECT GET_LOCK(?, ?)")
            .bind(CHAIN_LOCK_NAME)
            .bind(30i64)
            .fetch_optional(self.mysql_pool())
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
        // From here on we MUST release the lock — wrap the rest in a
        // helper closure so we can release in both success and
        // failure paths.
        let result = self.append_admin_audit_locked(entry).await;
        // Release in any case (we know we hold it).
        let _ = sqlx::query("SELECT RELEASE_LOCK(?)")
            .bind(CHAIN_LOCK_NAME)
            .execute(self.mysql_pool())
            .await;
        result
    }

    async fn list_admin_audit(
        &self,
        filter: &AdminAuditListFilter,
    ) -> SystemStoreResult<Vec<AdminAuditRow>> {
        let mut clauses: Vec<&str> = Vec::new();
        if filter.operation.is_some() {
            clauses.push("operation = ?");
        }
        if filter.actor.is_some() {
            clauses.push("actor = ?");
        }
        if filter.tenant_id.is_some() {
            clauses.push("tenant_id = ?");
        }
        if filter.project_id.is_some() {
            clauses.push("project_id = ?");
        }
        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        let limit = if filter.limit <= 0 { 100 } else { filter.limit };
        let offset = filter.offset.max(0);
        let sql = format!(
            "SELECT audit_id, actor, operation, target, request_json, result,
                    tenant_id, project_id, correlation_id,
                    previous_hash, current_hash, signer_key_id, external_anchor,
                    created_at
             FROM {TABLE}
             {where_sql}
             ORDER BY created_at DESC
             LIMIT ? OFFSET ?"
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
        let mut previous_hash = String::new();
        let mut checked: i64 = 0;
        let mut offset: i64 = 0;
        loop {
            let remaining = match limit {
                Some(n) if n > 0 => (n - checked).max(0),
                _ => i64::MAX,
            };
            if remaining == 0 {
                break;
            }
            let page = remaining.min(VERIFY_PAGE_SIZE);
            let sql = format!(
                "SELECT audit_id, actor, operation, target, request_json, result,
                        tenant_id, project_id, correlation_id,
                        previous_hash, current_hash, signer_key_id, external_anchor,
                        created_at
                 FROM {TABLE}
                 ORDER BY created_at ASC, audit_id ASC
                 LIMIT ? OFFSET ?"
            );
            let rows = sqlx::query(&sql)
                .bind(page)
                .bind(offset)
                .fetch_all(self.mysql_pool())
                .await
                .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
            if rows.is_empty() {
                break;
            }
            let n_rows = rows.len() as i64;
            for r in rows {
                let row = row_to_audit(r)?;
                match verify_admin_audit_chain_step(&row, &previous_hash, checked) {
                    Ok(next) => {
                        previous_hash = next;
                        checked += 1;
                    }
                    Err(report) => return Ok(report),
                }
            }
            offset += n_rows;
            if n_rows < page {
                break;
            }
        }
        Ok(AdminAuditChainReport::Passed {
            checked_count: checked,
            last_hash: previous_hash,
        })
    }
}

impl MysqlCanonicalStore {
    /// Inner — caller MUST have acquired the chain lock; this runs
    /// the read-latest + INSERT inside a transaction. The named lock
    /// gives us serialization across concurrent calls.
    async fn append_admin_audit_locked(&self, entry: &AdminAuditInsert) -> SystemStoreResult<Uuid> {
        let mut tx = self
            .mysql_pool()
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
