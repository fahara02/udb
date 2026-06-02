//! B.8 phase 2 — SQL Server (Tiberius) implementation of
//! [`AdminAuditStore`] (tamper-evident hash chain).
//!
//! ## Atomicity (the single most important correctness point)
//!
//! The chain append's *lock + read-latest-hash + insert + commit* MUST all
//! run on ONE connection inside ONE transaction. If they didn't, the
//! per-`fetch_rows`/`execute_sql` round-robin pool would scatter the
//! statements across different connections: the application lock would
//! serialise nothing and two concurrent appends could both read the same
//! `previous_hash` and fork the chain.
//!
//! We therefore drive the whole critical section through
//! [`MssqlClient::with_pinned_client`], which hands us one
//! `&mut TiberiusClient` for the closure. Inside it we:
//!
//! 1. `SET XACT_ABORT ON; BEGIN TRANSACTION;`
//! 2. `EXEC sp_getapplock @Resource='udb_admin_audit_chain',
//!    @LockMode='Exclusive', @LockOwner='Transaction', @LockTimeout=30000;`
//!    (`@LockOwner='Transaction'` auto-releases the lock on COMMIT/ROLLBACK).
//! 3. SELECT the latest `current_hash`.
//! 4. Compute the new `current_hash` in **Rust** via the shared
//!    [`compute_admin_audit_hash`] — never in SQL, so the chain hashes
//!    byte-identically across every backend.
//! 5. INSERT the row (`OUTPUT inserted.audit_id`).
//! 6. `COMMIT TRANSACTION;` (releases the applock).
//!
//! `verify_admin_audit_chain` pins one connection too and reads under
//! `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` + an explicit transaction
//! so the `OFFSET/FETCH` paging sees one stable snapshot (a concurrent
//! append with an earlier `created_at` can't shift the window and hide a
//! tamper).

use async_trait::async_trait;
use tiberius::Row;
use uuid::Uuid;

use super::dialect::normalize_limit_offset;
use super::mssql::MssqlCanonicalStore;
use super::mssql_projection::{opt_str_col, parse_ts_cell, parse_uuid_cell, str_col};
use super::system_store::{
    AdminAuditChainReport, AdminAuditInsert, AdminAuditListFilter, AdminAuditRow, AdminAuditStore,
    SystemStoreError, SystemStoreResult, compute_admin_audit_hash, verify_admin_audit_chain_step,
};
use crate::runtime::executors::mssql::{MssqlClient, SqlParam};

const DEFAULT_REL: &str = "udb_admin_audit_log";
const CHAIN_LOCK_NAME: &str = "udb_admin_audit_chain";

impl MssqlCanonicalStore {
    /// Override the admin-audit relation. Defaults to `udb_admin_audit_log`.
    pub fn with_admin_audit_relation(mut self, relation: impl Into<String>) -> Self {
        self.admin_audit_relation = Some(relation.into());
        self
    }

    pub(super) fn admin_audit_relation_ref(&self) -> &str {
        self.admin_audit_relation.as_deref().unwrap_or(DEFAULT_REL)
    }
}

/// JSON column decode that defaults to NULL.
fn json_col(row: &Row, name: &str) -> serde_json::Value {
    match opt_str_col(row, name) {
        Some(text) => serde_json::from_str(&text).unwrap_or(serde_json::Value::Null),
        None => serde_json::Value::Null,
    }
}

const AUDIT_SELECT_COLS: &str = "\
    CONVERT(NVARCHAR(36), audit_id) AS audit_id, actor, operation, target, request_json, result, \
    tenant_id, project_id, correlation_id, previous_hash, current_hash, signer_key_id, external_anchor, \
    CONVERT(NVARCHAR(33), created_at, 127) AS created_at";

fn row_to_audit(row: &Row) -> SystemStoreResult<AdminAuditRow> {
    let audit_id_str = opt_str_col(row, "audit_id")
        .ok_or_else(|| SystemStoreError::query("mssql", "SELECT audit_id", "audit_id was NULL"))?;
    let audit_id = parse_uuid_cell(&audit_id_str)?;
    Ok(AdminAuditRow {
        audit_id,
        actor: str_col(row, "actor"),
        operation: str_col(row, "operation"),
        target: str_col(row, "target"),
        request_json: json_col(row, "request_json"),
        result: str_col(row, "result"),
        tenant_id: str_col(row, "tenant_id"),
        project_id: str_col(row, "project_id"),
        correlation_id: str_col(row, "correlation_id"),
        previous_hash: str_col(row, "previous_hash"),
        current_hash: str_col(row, "current_hash"),
        signer_key_id: str_col(row, "signer_key_id"),
        external_anchor: str_col(row, "external_anchor"),
        created_at: parse_ts_cell(opt_str_col(row, "created_at").as_deref()),
    })
}

#[async_trait]
impl AdminAuditStore for MssqlCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mssql"
    }

    async fn ensure_admin_audit_tables(&self) -> SystemStoreResult<()> {
        let rel = Self::safe_object_name(self.admin_audit_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = super::sql_schema::mssql_admin_audit_ddl(rel);
        self.client()
            .simple_batch(&sql)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))
    }

    async fn latest_admin_audit_hash(&self) -> SystemStoreResult<String> {
        let rel = Self::safe_object_name(self.admin_audit_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!(
            "SELECT TOP 1 current_hash FROM {rel} \
             WHERE current_hash <> '' \
             ORDER BY created_at DESC, audit_id DESC"
        );
        let rows = self
            .client()
            .fetch_rows(&sql, &[])
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        Ok(rows
            .first()
            .map(|r| str_col(r, "current_hash"))
            .unwrap_or_default())
    }

    async fn append_admin_audit(&self, entry: &AdminAuditInsert) -> SystemStoreResult<Uuid> {
        let rel = Self::safe_object_name(self.admin_audit_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?
            .to_string();
        let audit_id = Uuid::new_v4();
        // Snapshot the entry's borrowed fields into owned values the closure
        // can move + reference (the closure is `AsyncFnOnce`).
        let entry = entry.clone();

        let latest_sql = format!(
            "SELECT TOP 1 current_hash FROM {rel} \
             WHERE current_hash <> '' \
             ORDER BY created_at DESC, audit_id DESC"
        );
        let insert_sql = format!(
            "INSERT INTO {rel} ( \
                audit_id, actor, operation, target, request_json, result, \
                tenant_id, project_id, correlation_id, \
                previous_hash, current_hash, signer_key_id, external_anchor \
             ) VALUES ( \
                CONVERT(UNIQUEIDENTIFIER, @P1), @P2, @P3, @P4, @P5, @P6, \
                @P7, @P8, @P9, @P10, @P11, @P12, @P13)"
        );

        self.client()
            .with_pinned_client(async move |client| {
                // 1. Open the serialising transaction. XACT_ABORT ON means any
                //    statement error rolls the whole tx back (and releases the
                //    transaction-scoped applock) instead of leaving it open.
                client
                    .simple_query(
                        "SET XACT_ABORT ON; \
                         BEGIN TRANSACTION; \
                         EXEC sp_getapplock @Resource = N'udb_admin_audit_chain', \
                            @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = 30000;",
                    )
                    .await?;

                // 2. Read the latest hash under the lock (on the same conn).
                let no_params: [&dyn tiberius::ToSql; 0] = [];
                let latest_rows = client
                    .query(&latest_sql, &no_params)
                    .await?
                    .into_first_result()
                    .await?;
                let previous_hash = latest_rows
                    .first()
                    .and_then(|r| r.try_get::<&str, _>(0).ok().flatten())
                    .map(|s| s.to_string())
                    .unwrap_or_default();

                // 3. Compute the new hash in Rust (shared helper — identical
                //    across every backend).
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

                // 4. INSERT the row on the same connection / transaction.
                let params = [
                    SqlParam::Str(audit_id.to_string()),
                    SqlParam::Str(entry.actor.clone()),
                    SqlParam::Str(entry.operation.clone()),
                    SqlParam::Str(entry.target.clone()),
                    SqlParam::Str(entry.request_json.to_string()),
                    SqlParam::Str(entry.result.clone()),
                    SqlParam::Str(entry.tenant_id.clone()),
                    SqlParam::Str(entry.project_id.clone()),
                    SqlParam::Str(entry.correlation_id.clone()),
                    SqlParam::Str(previous_hash.clone()),
                    SqlParam::Str(current_hash.clone()),
                    SqlParam::Str(entry.signer_key_id.clone()),
                    SqlParam::Str(entry.external_anchor.clone()),
                ];
                let refs = MssqlClient::bind_param_refs(&params);
                client.execute(&insert_sql, &refs).await?;

                // 5. Commit (releases the transaction-scoped applock).
                client.simple_query("COMMIT TRANSACTION;").await?;
                Ok::<_, tiberius::error::Error>(())
            })
            .await
            .map_err(|e| SystemStoreError::query("mssql", "append_admin_audit", e))?;
        Ok(audit_id)
    }

    async fn list_admin_audit(
        &self,
        filter: &AdminAuditListFilter,
    ) -> SystemStoreResult<Vec<AdminAuditRow>> {
        let rel = Self::safe_object_name(self.admin_audit_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let mut clauses: Vec<String> = Vec::new();
        let mut params: Vec<SqlParam> = Vec::new();
        let mut n = 1usize;
        if let Some(o) = &filter.operation {
            clauses.push(format!("operation = @P{n}"));
            params.push(SqlParam::Str(o.clone()));
            n += 1;
        }
        if let Some(a) = &filter.actor {
            clauses.push(format!("actor = @P{n}"));
            params.push(SqlParam::Str(a.clone()));
            n += 1;
        }
        if let Some(t) = &filter.tenant_id {
            clauses.push(format!("tenant_id = @P{n}"));
            params.push(SqlParam::Str(t.clone()));
            n += 1;
        }
        if let Some(p) = &filter.project_id {
            clauses.push(format!("project_id = @P{n}"));
            params.push(SqlParam::Str(p.clone()));
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
            "SELECT {AUDIT_SELECT_COLS} FROM {rel} \
             {where_sql} \
             ORDER BY created_at DESC \
             OFFSET {offset_ph} ROWS FETCH NEXT {limit_ph} ROWS ONLY"
        );
        let rows = self
            .client()
            .fetch_rows(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
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
        let rel = Self::safe_object_name(self.admin_audit_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?
            .to_string();
        let page_size = super::dialect::admin_audit_verify_page_size();

        // Drive the entire scan on ONE pinned connection under a single
        // SERIALIZABLE transaction so the OFFSET/FETCH window is stable for
        // the whole verification (a concurrent append can't shift it and hide
        // tamper). COMMIT is issued on every exit path.
        self.client()
            .with_pinned_client(async move |client| {
                client
                    .simple_query(
                        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;",
                    )
                    .await?;

                let mut previous_hash = String::new();
                let mut checked: i64 = 0;
                let mut offset: i64 = 0;
                let outcome = loop {
                    let remaining = match limit {
                        Some(nn) if nn > 0 => (nn - checked).max(0),
                        _ => i64::MAX,
                    };
                    if remaining == 0 {
                        break Ok(AdminAuditChainReport::Passed {
                            checked_count: checked,
                            last_hash: previous_hash.clone(),
                        });
                    }
                    let page = remaining.min(page_size);
                    let sql = format!(
                        "SELECT {AUDIT_SELECT_COLS} FROM {rel} \
                         ORDER BY created_at ASC, audit_id ASC \
                         OFFSET @P1 ROWS FETCH NEXT @P2 ROWS ONLY"
                    );
                    let params = [SqlParam::Int(offset), SqlParam::Int(page)];
                    let refs = MssqlClient::bind_param_refs(&params);
                    let rows = client.query(&sql, &refs).await?.into_first_result().await?;
                    if rows.is_empty() {
                        break Ok(AdminAuditChainReport::Passed {
                            checked_count: checked,
                            last_hash: previous_hash.clone(),
                        });
                    }
                    let n_rows = rows.len() as i64;
                    let mut tamper: Option<AdminAuditChainReport> = None;
                    let mut decode_err: Option<SystemStoreError> = None;
                    for r in &rows {
                        let row = match row_to_audit(r) {
                            Ok(row) => row,
                            Err(e) => {
                                decode_err = Some(e);
                                break;
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
                    if let Some(e) = decode_err {
                        break Err(e);
                    }
                    if let Some(report) = tamper {
                        break Ok(report);
                    }
                    offset += n_rows;
                    if n_rows < page {
                        break Ok(AdminAuditChainReport::Passed {
                            checked_count: checked,
                            last_hash: previous_hash.clone(),
                        });
                    }
                };

                // Always close the read transaction.
                let _ = client.simple_query("COMMIT TRANSACTION;").await;
                // Surface a decode error as a tiberius-compatible error string;
                // it is mapped back to a SystemStoreError by the caller below.
                match outcome {
                    Ok(report) => Ok(report),
                    Err(e) => Err(tiberius::error::Error::Conversion(
                        format!("__verify_decode__:{e}").into(),
                    )),
                }
            })
            .await
            .map_err(|e| {
                // A decode error tunnelled through the closure (corrupt
                // `audit_id`): re-surface it as InvalidInput. Everything else is
                // a genuine query/transport failure.
                if let Some(idx) = e.find("__verify_decode__:") {
                    SystemStoreError::InvalidInput(
                        e[idx + "__verify_decode__:".len()..].to_string(),
                    )
                } else {
                    SystemStoreError::query("mssql", "verify_admin_audit_chain", e)
                }
            })
    }
}
