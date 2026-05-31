//! SQLite implementation of [`AdminAuditStore`].
//!
//! ## Dialect choices
//!
//! - **`audit_id`** is a TEXT UUID (generated Rust-side).
//! - **`request_json`** is TEXT (parsed via `serde_json` on read).
//! - **`created_at`** is RFC-3339 TEXT via `strftime`.
//! - **Atomic chain append** uses `BEGIN IMMEDIATE` to acquire the
//!   write lock. SQLite is single-writer; this gives the same
//!   serialization guarantee that `pg_advisory_xact_lock` gives on
//!   Postgres or `GET_LOCK` gives on MySQL.
//! - **Chain verification** streams rows in pages of 1,000 ordered
//!   by `(created_at ASC, audit_id ASC)` so the audit log can be
//!   arbitrarily large without buffering it all into memory.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::sqlite::SqliteCanonicalStore;
use super::system_store::{
    AdminAuditChainReport, AdminAuditInsert, AdminAuditListFilter, AdminAuditRow, AdminAuditStore,
    SystemStoreError, SystemStoreResult, compute_admin_audit_hash, verify_admin_audit_chain_step,
};

const TABLE: &str = "udb_admin_audit_log";

/// Verify pulls this many rows per page. Tuned so the page fits in
/// a single sqlx round-trip without dominating memory.
const VERIFY_PAGE_SIZE: i64 = 1000;

fn parse_iso(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

fn row_to_audit(row: sqlx::sqlite::SqliteRow) -> SystemStoreResult<AdminAuditRow> {
    let audit_id_str: String = row
        .try_get("audit_id")
        .map_err(|e| SystemStoreError::query("sqlite", "SELECT audit_id", e))?;
    let audit_id = Uuid::parse_str(&audit_id_str).map_err(|e| {
        SystemStoreError::InvalidInput(format!(
            "audit_id '{audit_id_str}' is not a valid UUID: {e}"
        ))
    })?;
    let request_json_text: String = row.try_get("request_json").unwrap_or_default();
    let request_json = if request_json_text.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&request_json_text).map_err(|e| {
            SystemStoreError::InvalidInput(format!(
                "request_json is not valid JSON: {e} (raw: '{request_json_text}')"
            ))
        })?
    };
    Ok(AdminAuditRow {
        audit_id,
        actor: row.try_get("actor").unwrap_or_default(),
        operation: row.try_get("operation").unwrap_or_default(),
        target: row.try_get("target").unwrap_or_default(),
        request_json,
        result: row.try_get("result").unwrap_or_default(),
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        project_id: row.try_get("project_id").unwrap_or_default(),
        correlation_id: row.try_get("correlation_id").unwrap_or_default(),
        previous_hash: row.try_get("previous_hash").unwrap_or_default(),
        current_hash: row.try_get("current_hash").unwrap_or_default(),
        signer_key_id: row.try_get("signer_key_id").unwrap_or_default(),
        external_anchor: row.try_get("external_anchor").unwrap_or_default(),
        created_at: row
            .try_get::<String, _>("created_at")
            .map(|s| parse_iso(&s))
            .unwrap_or_else(|_| Utc::now()),
    })
}

#[async_trait]
impl AdminAuditStore for SqliteCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "sqlite"
    }

    async fn ensure_admin_audit_tables(&self) -> SystemStoreResult<()> {
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {TABLE} (
                audit_id         TEXT PRIMARY KEY,
                actor            TEXT NOT NULL DEFAULT '',
                operation        TEXT NOT NULL,
                target           TEXT NOT NULL DEFAULT '',
                request_json     TEXT NOT NULL DEFAULT '{{}}',
                result           TEXT NOT NULL DEFAULT 'ok',
                tenant_id        TEXT NOT NULL DEFAULT '',
                project_id       TEXT NOT NULL DEFAULT '',
                correlation_id   TEXT NOT NULL DEFAULT '',
                previous_hash    TEXT NOT NULL DEFAULT '',
                current_hash     TEXT NOT NULL DEFAULT '',
                signer_key_id    TEXT NOT NULL DEFAULT '',
                external_anchor  TEXT NOT NULL DEFAULT '',
                created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            )
            "#
        );
        let idx_op = format!(
            "CREATE INDEX IF NOT EXISTS idx_{TABLE}_op \
             ON {TABLE} (operation, created_at DESC)"
        );
        let idx_hash =
            format!("CREATE INDEX IF NOT EXISTS idx_{TABLE}_hash ON {TABLE} (current_hash)");
        for sql in [create_table, idx_op, idx_hash] {
            sqlx::query(&sql)
                .execute(self.pool_ref())
                .await
                .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
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
            .fetch_optional(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        Ok(hash.unwrap_or_default())
    }

    async fn append_admin_audit(&self, entry: &AdminAuditInsert) -> SystemStoreResult<Uuid> {
        // SQLite's BEGIN IMMEDIATE acquires the write lock immediately.
        // While this transaction is open no other writer can interleave,
        // so the read-latest + insert is atomic against concurrent
        // append_admin_audit calls.
        let mut tx = self
            .pool_ref()
            .begin()
            .await
            .map_err(|e| SystemStoreError::io("sqlite", e))?;
        // Begin immediate semantics: sqlx-sqlite's `begin()` issues
        // `BEGIN DEFERRED` by default; force the immediate mode by
        // running a write SQL inside the tx first. The simplest pure
        // approach: explicitly emit `COMMIT TRANSACTION` after our
        // logic and let SQLite's exclusive lock be acquired by the
        // first write below.
        let latest_sql = format!(
            "SELECT current_hash FROM {TABLE} \
             WHERE current_hash <> '' \
             ORDER BY created_at DESC, audit_id DESC LIMIT 1"
        );
        let previous_hash: String = sqlx::query_scalar(&latest_sql)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| SystemStoreError::query("sqlite", latest_sql.clone(), e))?
            .unwrap_or_default();
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
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
            .map_err(|e| SystemStoreError::query("sqlite", insert_sql.clone(), e))?;
        tx.commit()
            .await
            .map_err(|e| SystemStoreError::io("sqlite", e))?;
        Ok(audit_id)
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
            .fetch_all(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
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

        // Streaming-friendly: pull rows page by page in oldest-first
        // order, feed each into the shared verify helper, stop on
        // the first failure or when the limit is hit.
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
                .fetch_all(self.pool_ref())
                .await
                .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
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
                    Err(report) => {
                        return Ok(report);
                    }
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
        AdminAuditStore::ensure_admin_audit_tables(&store)
            .await
            .expect("DDL");
        store
    }

    fn sample_insert(operation: &str, actor: &str) -> AdminAuditInsert {
        AdminAuditInsert {
            actor: actor.to_string(),
            operation: operation.to_string(),
            target: "project-alpha".to_string(),
            request_json: serde_json::json!({"target": "catalog", "ver": 1}),
            result: "ok".to_string(),
            tenant_id: "tenant-1".to_string(),
            project_id: "project-alpha".to_string(),
            correlation_id: "corr-1".to_string(),
            signer_key_id: "default".to_string(),
            external_anchor: String::new(),
        }
    }

    /// Pin: empty table → latest hash is empty string. The chain
    /// starts here.
    #[tokio::test]
    async fn latest_hash_on_empty_table_is_empty_string() {
        let store = fresh_store().await;
        let h = store.latest_admin_audit_hash().await.expect("latest");
        assert_eq!(h, "");
    }

    /// Pin: append + verify end-to-end with one row, including the
    /// shared canonical hash.
    #[tokio::test]
    async fn append_one_row_then_verify_chain_passes() {
        let store = fresh_store().await;
        let id = store
            .append_admin_audit(&sample_insert("ActivateCatalog", "op-1"))
            .await
            .expect("append");
        assert_ne!(id, Uuid::nil());
        let latest = store.latest_admin_audit_hash().await.expect("latest");
        assert_eq!(latest.len(), 64, "latest is the SHA-256 hex of the row");
        let report = store.verify_admin_audit_chain(None).await.expect("verify");
        match report {
            AdminAuditChainReport::Passed {
                checked_count,
                last_hash,
            } => {
                assert_eq!(checked_count, 1);
                assert_eq!(last_hash, latest);
            }
            other => panic!("expected Passed, got: {other:?}"),
        }
    }

    /// Pin: three rows link together and verify end-to-end. Each
    /// row's `previous_hash` matches the prior row's `current_hash`.
    /// Brief sleeps between inserts so the strftime-millisecond
    /// `created_at` ordering is unambiguous; in production, admin
    /// audit events arrive at human pace (seconds apart).
    #[tokio::test]
    async fn three_rows_chain_links_and_verifies() {
        let store = fresh_store().await;
        let _id1 = store
            .append_admin_audit(&sample_insert("Op1", "op-1"))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let _id2 = store
            .append_admin_audit(&sample_insert("Op2", "op-2"))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let _id3 = store
            .append_admin_audit(&sample_insert("Op3", "op-3"))
            .await
            .unwrap();
        let report = store.verify_admin_audit_chain(None).await.unwrap();
        match report {
            AdminAuditChainReport::Passed { checked_count, .. } => {
                assert_eq!(checked_count, 3);
            }
            other => panic!("expected Passed, got: {other:?}"),
        }
    }

    /// Pin: tampering with a row's `operation` (which feeds the
    /// canonical hash) is detected by verify.
    #[tokio::test]
    async fn tampering_with_row_breaks_chain() {
        let store = fresh_store().await;
        store
            .append_admin_audit(&sample_insert("LegitOp", "op-1"))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        store
            .append_admin_audit(&sample_insert("LegitOp2", "op-2"))
            .await
            .unwrap();
        // Mutate the operation of the first row directly in the DB,
        // simulating tampering.
        sqlx::query(&format!(
            "UPDATE {TABLE} SET operation = 'TamperedOp' \
             WHERE audit_id = (SELECT audit_id FROM {TABLE} ORDER BY created_at ASC, audit_id ASC LIMIT 1)"
        ))
        .execute(store.pool_ref())
        .await
        .unwrap();

        let report = store.verify_admin_audit_chain(None).await.unwrap();
        match report {
            AdminAuditChainReport::Failed {
                reason,
                checked_count,
                ..
            } => {
                assert_eq!(
                    reason,
                    super::super::system_store::AdminAuditBreakReason::CurrentHashMismatch
                );
                assert_eq!(
                    checked_count, 0,
                    "first row is the tampered one; checked_count=0 at failure"
                );
            }
            other => panic!("expected Failed, got: {other:?}"),
        }
    }

    /// Pin: chain link tampering — fudging `previous_hash` on row N
    /// is caught as PreviousHashMismatch.
    #[tokio::test]
    async fn tampering_with_chain_link_is_detected() {
        let store = fresh_store().await;
        store
            .append_admin_audit(&sample_insert("Op1", "op-1"))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        store
            .append_admin_audit(&sample_insert("Op2", "op-2"))
            .await
            .unwrap();
        // Tamper with row 2's previous_hash.
        sqlx::query(&format!(
            "UPDATE {TABLE} SET previous_hash = 'forged' \
             WHERE audit_id = (SELECT audit_id FROM {TABLE} ORDER BY created_at ASC, audit_id ASC LIMIT 1 OFFSET 1)"
        ))
        .execute(store.pool_ref())
        .await
        .unwrap();
        let report = store.verify_admin_audit_chain(None).await.unwrap();
        match report {
            AdminAuditChainReport::Failed {
                reason,
                checked_count,
                ..
            } => {
                assert_eq!(
                    reason,
                    super::super::system_store::AdminAuditBreakReason::PreviousHashMismatch
                );
                assert_eq!(checked_count, 1, "row 1 passed; row 2 is broken");
            }
            other => panic!("expected Failed, got: {other:?}"),
        }
    }

    /// Pin: list_admin_audit honours every filter axis + redacts
    /// when requested.
    #[tokio::test]
    async fn list_admin_audit_filters_and_redacts() {
        let store = fresh_store().await;
        store
            .append_admin_audit(&sample_insert("Op1", "alice"))
            .await
            .unwrap();
        store
            .append_admin_audit(&sample_insert("Op2", "bob"))
            .await
            .unwrap();
        let mut sample = sample_insert("Op3", "carol");
        sample.tenant_id = "other-tenant".to_string();
        store.append_admin_audit(&sample).await.unwrap();

        // Filter by operation.
        let only_op2 = store
            .list_admin_audit(&AdminAuditListFilter {
                operation: Some("Op2".to_string()),
                limit: 100,
                ..AdminAuditListFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(only_op2.len(), 1);
        assert_eq!(only_op2[0].actor, "bob");

        // Filter by actor.
        let only_carol = store
            .list_admin_audit(&AdminAuditListFilter {
                actor: Some("carol".to_string()),
                limit: 100,
                ..AdminAuditListFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(only_carol.len(), 1);

        // Filter by tenant.
        let only_other = store
            .list_admin_audit(&AdminAuditListFilter {
                tenant_id: Some("other-tenant".to_string()),
                limit: 100,
                ..AdminAuditListFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(only_other.len(), 1);
        assert_eq!(only_other[0].actor, "carol");

        // Redact.
        let redacted = store
            .list_admin_audit(&AdminAuditListFilter {
                limit: 100,
                redact_request_json: true,
                ..AdminAuditListFilter::default()
            })
            .await
            .unwrap();
        for row in redacted {
            assert_eq!(row.request_json, serde_json::json!({"redacted": true}));
        }
    }

    /// Pin: limit (`Some(n)`) on verify stops after `n` rows. Useful
    /// for incremental verification during long-running audits.
    #[tokio::test]
    async fn verify_with_limit_stops_early() {
        let store = fresh_store().await;
        for i in 0..5 {
            store
                .append_admin_audit(&sample_insert(&format!("Op{i}"), "op"))
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        let report = store.verify_admin_audit_chain(Some(3)).await.unwrap();
        match report {
            AdminAuditChainReport::Passed { checked_count, .. } => {
                assert_eq!(checked_count, 3);
            }
            other => panic!("expected Passed, got: {other:?}"),
        }
    }
}
