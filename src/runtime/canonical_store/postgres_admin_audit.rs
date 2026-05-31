//! PostgreSQL implementation of [`AdminAuditStore`].
//!
//! Schema mirrors the existing `udb_system.udb_admin_audit_log`
//! table from `runtime/system.rs` exactly: `UUID` PK with
//! `gen_random_uuid()` default, `JSONB` for `request_json`,
//! `TIMESTAMPTZ` for `created_at`, the existing `(operation,
//! created_at DESC)` and `(current_hash)` indexes.
//!
//! ## Atomicity
//!
//! `pg_advisory_xact_lock(0x7564625F6175_6469)` is acquired inside
//! the append transaction. The constant ("udb_audi" in ASCII) is the
//! 64-bit advisory-lock key dedicated to this chain. Two appends
//! issuing concurrently against the same chain will serialize on
//! this lock — the second waits for the first to commit, then sees
//! the new `current_hash` and links correctly.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::postgres::PostgresCanonicalStore;
use super::system_store::{
    AdminAuditChainReport, AdminAuditInsert, AdminAuditListFilter, AdminAuditRow, AdminAuditStore,
    SystemStoreError, SystemStoreResult, compute_admin_audit_hash, verify_admin_audit_chain_step,
};

const DEFAULT_REL: &str = r#""udb_system"."udb_admin_audit_log""#;

/// Postgres advisory-lock key dedicated to the admin audit chain.
/// `0x7564625F6175_6469` is "udb_audi" in ASCII — encoded as the
/// 64-bit integer the `pg_advisory_xact_lock()` builtin takes.
const ADMIN_AUDIT_CHAIN_LOCK_KEY: i64 = 0x7564_625F_6175_6469;

/// Pages of this many rows are pulled during streaming verify.
const VERIFY_PAGE_SIZE: i64 = 1000;

impl PostgresCanonicalStore {
    pub fn with_admin_audit_relation(mut self, relation: impl Into<String>) -> Self {
        self.admin_audit_relation = Some(relation.into());
        self
    }

    fn admin_audit_relation_ref(&self) -> &str {
        self.admin_audit_relation.as_deref().unwrap_or(DEFAULT_REL)
    }
}

fn row_to_audit(row: sqlx::postgres::PgRow) -> SystemStoreResult<AdminAuditRow> {
    let audit_id: Uuid = row
        .try_get("audit_id")
        .map_err(|e| SystemStoreError::query("postgres", "SELECT audit_id", e))?;
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
impl AdminAuditStore for PostgresCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "postgres"
    }

    async fn ensure_admin_audit_tables(&self) -> SystemStoreResult<()> {
        let rel = self.admin_audit_relation_ref();
        let stmts = [
            format!(
                r#"
                CREATE TABLE IF NOT EXISTS {rel} (
                    audit_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    actor            TEXT NOT NULL DEFAULT '',
                    operation        TEXT NOT NULL,
                    target           TEXT NOT NULL DEFAULT '',
                    request_json     JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                    result           TEXT NOT NULL DEFAULT 'ok',
                    tenant_id        TEXT NOT NULL DEFAULT '',
                    project_id       TEXT NOT NULL DEFAULT '',
                    correlation_id   TEXT NOT NULL DEFAULT '',
                    previous_hash    TEXT NOT NULL DEFAULT '',
                    current_hash     TEXT NOT NULL DEFAULT '',
                    signer_key_id    TEXT NOT NULL DEFAULT '',
                    external_anchor  TEXT NOT NULL DEFAULT '',
                    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_admin_audit_log_op"
                     ON {rel} (operation, created_at DESC)"#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_admin_audit_log_hash"
                     ON {rel} (current_hash)"#
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

    async fn latest_admin_audit_hash(&self) -> SystemStoreResult<String> {
        let rel = self.admin_audit_relation_ref();
        let sql = format!(
            r#"SELECT current_hash FROM {rel}
               WHERE current_hash <> ''
               ORDER BY created_at DESC, audit_id DESC
               LIMIT 1"#
        );
        let hash: Option<String> = sqlx::query_scalar(&sql)
            .fetch_optional(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        Ok(hash.unwrap_or_default())
    }

    async fn append_admin_audit(&self, entry: &AdminAuditInsert) -> SystemStoreResult<Uuid> {
        let rel = self.admin_audit_relation_ref();
        // Begin a tx, acquire the per-chain advisory lock,
        // SELECT latest hash, compute current hash, INSERT, COMMIT.
        // pg_advisory_xact_lock auto-releases on tx end.
        let mut tx = self
            .pg_pool()
            .begin()
            .await
            .map_err(|e| SystemStoreError::io("postgres", e))?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(ADMIN_AUDIT_CHAIN_LOCK_KEY)
            .execute(&mut *tx)
            .await
            .map_err(|e| SystemStoreError::query("postgres", "pg_advisory_xact_lock", e))?;
        let latest_sql = format!(
            r#"SELECT current_hash FROM {rel}
               WHERE current_hash <> ''
               ORDER BY created_at DESC, audit_id DESC
               LIMIT 1"#
        );
        let previous_hash: String = sqlx::query_scalar::<_, Option<String>>(&latest_sql)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| SystemStoreError::query("postgres", latest_sql.clone(), e))?
            .flatten()
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
        let insert_sql = format!(
            r#"INSERT INTO {rel} (
                audit_id, actor, operation, target, request_json, result,
                tenant_id, project_id, correlation_id,
                previous_hash, current_hash, signer_key_id, external_anchor
            ) VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12,$13)"#
        );
        sqlx::query(&insert_sql)
            .bind(audit_id)
            .bind(&entry.actor)
            .bind(&entry.operation)
            .bind(&entry.target)
            .bind(entry.request_json.to_string())
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
            .map_err(|e| SystemStoreError::query("postgres", insert_sql.clone(), e))?;
        tx.commit()
            .await
            .map_err(|e| SystemStoreError::io("postgres", e))?;
        Ok(audit_id)
    }

    async fn list_admin_audit(
        &self,
        filter: &AdminAuditListFilter,
    ) -> SystemStoreResult<Vec<AdminAuditRow>> {
        let rel = self.admin_audit_relation_ref();
        let mut clauses: Vec<String> = Vec::new();
        let mut bind_index: usize = 0;
        for (column, value) in [
            ("operation", filter.operation.as_ref()),
            ("actor", filter.actor.as_ref()),
            ("tenant_id", filter.tenant_id.as_ref()),
            ("project_id", filter.project_id.as_ref()),
        ] {
            if value.is_some() {
                bind_index += 1;
                clauses.push(format!("{column} = ${bind_index}"));
            }
        }
        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        let limit_p = bind_index + 1;
        let offset_p = bind_index + 2;
        let limit = if filter.limit <= 0 { 100 } else { filter.limit };
        let offset = filter.offset.max(0);
        let sql = format!(
            r#"SELECT audit_id, actor, operation, target, request_json, result,
                      tenant_id, project_id, correlation_id,
                      previous_hash, current_hash, signer_key_id, external_anchor,
                      created_at
               FROM {rel}
               {where_sql}
               ORDER BY created_at DESC
               LIMIT ${limit_p} OFFSET ${offset_p}"#
        );
        let mut q = sqlx::query(&sql);
        for value in [
            filter.operation.as_ref(),
            filter.actor.as_ref(),
            filter.tenant_id.as_ref(),
            filter.project_id.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            q = q.bind(value.clone());
        }
        q = q.bind(limit).bind(offset);
        let rows = q
            .fetch_all(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
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
        let rel = self.admin_audit_relation_ref();
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
                r#"SELECT audit_id, actor, operation, target, request_json, result,
                          tenant_id, project_id, correlation_id,
                          previous_hash, current_hash, signer_key_id, external_anchor,
                          created_at
                   FROM {rel}
                   ORDER BY created_at ASC, audit_id ASC
                   LIMIT $1 OFFSET $2"#
            );
            let rows = sqlx::query(&sql)
                .bind(page)
                .bind(offset)
                .fetch_all(self.pg_pool())
                .await
                .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
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
