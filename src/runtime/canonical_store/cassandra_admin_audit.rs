//! Cassandra / ScyllaDB implementation of [`AdminAuditStore`] (B.10a PHASE 2).
//!
//! Semantics mirror the Postgres impl (`postgres_admin_audit.rs`) exactly so
//! the cross-backend conformance contract passes byte-for-byte. The hash chain
//! is computed in Rust via the SHARED [`compute_admin_audit_hash`] /
//! [`verify_admin_audit_chain_step`] helpers, so PG / MySQL / SQLite / MSSQL /
//! MongoDB / Cassandra chains are byte-identical.
//!
//! ## Atomicity — chain-head LWT
//!
//! Cassandra has no `pg_advisory_xact_lock` and no multi-row transaction, so
//! `append_admin_audit` serialises the chain through a single **chain-head**
//! row in `udb_admin_audit_chain (id='chain', head_hash)`:
//!
//! 1. Read the current `head_hash` (empty when the chain is fresh).
//! 2. Compute the new row's `current_hash` via the shared hasher.
//! 3. CAS the head: `UPDATE … SET head_hash=<new> WHERE id='chain' IF
//!    head_hash=<observed>` (or `INSERT … IF NOT EXISTS` on the very first
//!    append). On a not-applied CAS another appender advanced the head; re-read
//!    and retry. This is single-writer chain serialisation without a multi-row
//!    transaction.
//! 4. INSERT the audit row (plain write — the head CAS already linearised it).
//!
//! ## Schema
//!
//! - `udb_admin_audit_log` — partition `chain text` (always `'main'`),
//!   clustering `(created_at timestamp, audit_id text)` ASC so verify reads the
//!   chain in `(created_at, audit_id)` order directly off the clustering order.
//! - `udb_admin_audit_chain` — `id text PRIMARY KEY, head_hash text` (the LWT
//!   serialisation point).
//!
//! ## `ALLOW FILTERING`
//!
//! `list_admin_audit` filters on non-key columns (operation/actor/tenant/
//! project) and so scans with ALLOW FILTERING + a Rust fold; verify reads the
//! whole single partition in clustering order and feeds rows one at a time to
//! the shared step. Acceptable for the conformance contract's tiny data.

use async_trait::async_trait;
use scylla::frame::response::result::Row;
use scylla::statement::SerialConsistency;
use uuid::Uuid;

use super::cassandra::{CassandraCanonicalStore, now_unix_ms};
use super::cassandra_projection::{cass_err, cql_ts, get_dt, get_json, get_text, get_uuid};
use super::system_store::{
    AdminAuditChainReport, AdminAuditInsert, AdminAuditListFilter, AdminAuditRow, AdminAuditStore,
    SystemStoreResult, compute_admin_audit_hash, verify_admin_audit_chain_step,
};

/// Single partition value for the audit-log table — every row shares it so the
/// clustering order spans the whole chain.
const CHAIN_PARTITION: &str = "main";
/// Well-known PK of the chain-head row.
const CHAIN_HEAD_ID: &str = "chain";
/// Cap on the chain-head CAS retry loop (one Paxos round per iteration).
const CHAIN_CAS_MAX_ATTEMPTS: u32 = 64;

/// Canonical audit SELECT column order. Pinned next to the mapper.
const AUDIT_COLS: &str = "audit_id, actor, operation, target, request_json, result, \
     tenant_id, project_id, correlation_id, previous_hash, current_hash, \
     signer_key_id, external_anchor, created_at";

fn row_to_audit(row: &Row) -> SystemStoreResult<AdminAuditRow> {
    let audit_id = get_uuid(row, 0)?;
    Ok(AdminAuditRow {
        audit_id,
        actor: get_text(row, 1),
        operation: get_text(row, 2),
        target: get_text(row, 3),
        request_json: get_json(row, 4, serde_json::Value::Null),
        result: get_text(row, 5),
        tenant_id: get_text(row, 6),
        project_id: get_text(row, 7),
        correlation_id: get_text(row, 8),
        previous_hash: get_text(row, 9),
        current_hash: get_text(row, 10),
        signer_key_id: get_text(row, 11),
        external_anchor: get_text(row, 12),
        created_at: get_dt(row, 13),
    })
}

impl CassandraCanonicalStore {
    fn audit_table(&self) -> String {
        self.qualified("udb_admin_audit_log")
    }
    fn audit_chain_table(&self) -> String {
        self.qualified("udb_admin_audit_chain")
    }
}

#[async_trait]
impl AdminAuditStore for CassandraCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "cassandra"
    }

    async fn ensure_admin_audit_tables(&self) -> SystemStoreResult<()> {
        self.ensure_keyspace()
            .await
            .map_err(|e| cass_err("ensure_admin_audit_tables keyspace", e))?;
        // Audit log: single partition, clustered ASC by (created_at, audit_id)
        // so verify reads the chain in order directly.
        let log_ddl = format!(
            "CREATE TABLE IF NOT EXISTS {tbl} ( \
                chain text, \
                created_at timestamp, \
                audit_id text, \
                actor text, \
                operation text, \
                target text, \
                request_json text, \
                result text, \
                tenant_id text, \
                project_id text, \
                correlation_id text, \
                previous_hash text, \
                current_hash text, \
                signer_key_id text, \
                external_anchor text, \
                PRIMARY KEY (chain, created_at, audit_id) \
             ) WITH CLUSTERING ORDER BY (created_at ASC, audit_id ASC)",
            tbl = self.audit_table(),
        );
        self.client()
            .cql_execute(&log_ddl, ())
            .await
            .map_err(|e| cass_err("ensure_admin_audit_tables log", e))?;
        let chain_ddl = format!(
            "CREATE TABLE IF NOT EXISTS {tbl} ( id text PRIMARY KEY, head_hash text )",
            tbl = self.audit_chain_table(),
        );
        self.client()
            .cql_execute(&chain_ddl, ())
            .await
            .map_err(|e| cass_err("ensure_admin_audit_tables chain", e))?;
        Ok(())
    }

    async fn latest_admin_audit_hash(&self) -> SystemStoreResult<String> {
        // The chain-head row is the authoritative latest hash (CAS-advanced
        // before each audit-row insert), so a single point read suffices.
        let sql = format!(
            "SELECT head_hash FROM {tbl} WHERE id = ?",
            tbl = self.audit_chain_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&sql, (CHAIN_HEAD_ID,))
            .await
            .map_err(|e| cass_err("latest_admin_audit_hash", e))?;
        Ok(rows.first().map(|r| get_text(r, 0)).unwrap_or_default())
    }

    async fn append_admin_audit(&self, entry: &AdminAuditInsert) -> SystemStoreResult<Uuid> {
        let audit_id = Uuid::new_v4();
        let head_sql = format!(
            "SELECT head_hash FROM {tbl} WHERE id = ?",
            tbl = self.audit_chain_table(),
        );
        let chain_tbl = self.audit_chain_table();

        // ── Chain-head CAS loop ─────────────────────────────────────────────
        // Linearise the chain on the single head row: read head, compute next,
        // CAS. On a lost CAS another appender advanced the head; re-read + retry.
        let mut attempt = 0u32;
        let (previous_hash, current_hash) = loop {
            attempt += 1;
            if attempt > CHAIN_CAS_MAX_ATTEMPTS {
                return Err(cass_err(
                    "append_admin_audit",
                    "chain-head CAS did not converge",
                ));
            }
            let head_rows = self
                .client()
                .cql_query_rows(&head_sql, (CHAIN_HEAD_ID,))
                .await
                .map_err(|e| cass_err("append_admin_audit read head", e))?;
            let previous_hash = head_rows
                .first()
                .map(|r| get_text(r, 0))
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
            let applied = if head_rows.is_empty() {
                // Fresh chain — seed the head with `INSERT … IF NOT EXISTS`.
                let seed =
                    format!("INSERT INTO {chain_tbl} (id, head_hash) VALUES (?, ?) IF NOT EXISTS");
                self.client()
                    .cql_lwt_applied(
                        &seed,
                        (CHAIN_HEAD_ID, current_hash.as_str()),
                        SerialConsistency::Serial,
                    )
                    .await
                    .map_err(|e| cass_err("append_admin_audit seed head", e))?
            } else {
                // CAS: advance head only if it is still the value we read.
                let cas =
                    format!("UPDATE {chain_tbl} SET head_hash = ? WHERE id = ? IF head_hash = ?");
                self.client()
                    .cql_lwt_applied(
                        &cas,
                        (current_hash.as_str(), CHAIN_HEAD_ID, previous_hash.as_str()),
                        SerialConsistency::Serial,
                    )
                    .await
                    .map_err(|e| cass_err("append_admin_audit cas head", e))?
            };
            if applied {
                break (previous_hash, current_hash);
            }
            // Lost the CAS — another appender linked first; re-read and retry.
        };

        // ── Insert the audit row (plain write — head CAS already linearised) ─
        let now = now_unix_ms();
        let insert = format!(
            "INSERT INTO {tbl} ( \
                chain, created_at, audit_id, actor, operation, target, request_json, result, \
                tenant_id, project_id, correlation_id, previous_hash, current_hash, \
                signer_key_id, external_anchor \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tbl = self.audit_table(),
        );
        self.client()
            .cql_execute(
                &insert,
                (
                    CHAIN_PARTITION,
                    cql_ts(now),
                    audit_id.to_string(),
                    entry.actor.as_str(),
                    entry.operation.as_str(),
                    entry.target.as_str(),
                    entry.request_json.to_string(),
                    entry.result.as_str(),
                    entry.tenant_id.as_str(),
                    entry.project_id.as_str(),
                    entry.correlation_id.as_str(),
                    previous_hash.as_str(),
                    current_hash.as_str(),
                    entry.signer_key_id.as_str(),
                    entry.external_anchor.as_str(),
                ),
            )
            .await
            .map_err(|e| cass_err("append_admin_audit insert", e))?;
        Ok(audit_id)
    }

    async fn list_admin_audit(
        &self,
        filter: &AdminAuditListFilter,
    ) -> SystemStoreResult<Vec<AdminAuditRow>> {
        // Scan the single partition + Rust-side filter / DESC sort / page.
        // ALLOW FILTERING because the equality filters are on non-key columns.
        let scan = format!(
            "SELECT {AUDIT_COLS} FROM {tbl} ALLOW FILTERING",
            tbl = self.audit_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&scan, ())
            .await
            .map_err(|e| cass_err("list_admin_audit scan", e))?;
        let mut audits: Vec<AdminAuditRow> = Vec::new();
        for row in &rows {
            let audit = row_to_audit(row)?;
            if let Some(op) = &filter.operation {
                if &audit.operation != op {
                    continue;
                }
            }
            if let Some(actor) = &filter.actor {
                if &audit.actor != actor {
                    continue;
                }
            }
            if let Some(t) = &filter.tenant_id {
                if &audit.tenant_id != t {
                    continue;
                }
            }
            if let Some(p) = &filter.project_id {
                if &audit.project_id != p {
                    continue;
                }
            }
            audits.push(audit);
        }
        // created_at DESC, like PG.
        audits.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        let limit = if filter.limit <= 0 { 100 } else { filter.limit } as usize;
        let offset = filter.offset.max(0) as usize;
        let mut out: Vec<AdminAuditRow> = audits.into_iter().skip(offset).take(limit).collect();
        if filter.redact_request_json {
            for row in &mut out {
                row.request_json = serde_json::json!({"redacted": true});
            }
        }
        Ok(out)
    }

    async fn verify_admin_audit_chain(
        &self,
        limit: Option<i64>,
    ) -> SystemStoreResult<AdminAuditChainReport> {
        // Read the single partition in clustering order (created_at ASC,
        // audit_id ASC) and feed rows one at a time to the SHARED verify step.
        // The partition read returns rows already in chain order, so no
        // Rust-side sort is needed; we honour `limit` by stopping early.
        let sql = format!(
            "SELECT {AUDIT_COLS} FROM {tbl} WHERE chain = ?",
            tbl = self.audit_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&sql, (CHAIN_PARTITION,))
            .await
            .map_err(|e| cass_err("verify_admin_audit_chain", e))?;
        let max = match limit {
            Some(n) if n > 0 => Some(n),
            _ => None,
        };
        let mut previous_hash = String::new();
        let mut checked: i64 = 0;
        for row in &rows {
            if let Some(n) = max {
                if checked >= n {
                    break;
                }
            }
            let audit = row_to_audit(row)?;
            match verify_admin_audit_chain_step(&audit, &previous_hash, checked) {
                Ok(next) => {
                    previous_hash = next;
                    checked += 1;
                }
                Err(report) => return Ok(report),
            }
        }
        Ok(AdminAuditChainReport::Passed {
            checked_count: checked,
            last_hash: previous_hash,
        })
    }
}
