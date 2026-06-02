//! ClickHouse implementation of [`AdminAuditStore`] (B.10c PHASE 2).
//!
//! Semantics mirror the Postgres impl (`postgres_admin_audit.rs`) exactly so the
//! cross-backend conformance contract passes byte-for-byte. The hash chain is
//! computed in Rust via the SHARED [`compute_admin_audit_hash`] /
//! [`verify_admin_audit_chain_step`] helpers, so PG / MySQL / SQLite / MSSQL /
//! MongoDB / Cassandra / Neo4j / ClickHouse chains are byte-identical.
//!
//! ## Engine — append-only MergeTree
//!
//! Audit rows are immutable, so the natural fit is a plain `MergeTree` (no
//! versioning, no FINAL): `append_admin_audit` reads the latest hash, computes
//! the next via the shared hasher, and INSERTs the row.
//!
//! ## Single-writer chain caveat
//!
//! ClickHouse has no `pg_advisory_xact_lock` and no atomic CAS, so unlike the
//! Cassandra impl there is no chain-head CAS to serialise concurrent appenders.
//! `append_admin_audit` reads the current latest hash and appends; two concurrent
//! appenders could read the same latest hash and write two rows with the same
//! `previous_hash`, forking the chain. SINGLE-WRITER CAVEAT (see `clickhouse.rs`
//! module docs): the conformance run is sequential so the chain stays linear; a
//! hardened multi-writer path would need a `Keeper`-backed lock or an external
//! sequencer.
//!
//! ## Schema
//!
//! - `udb_admin_audit_log` — append-only `MergeTree ORDER BY (created_at,
//!   audit_id)` so verify reads the chain in `(created_at, audit_id)` order
//!   directly. `created_at` is epoch-millis `Int64`; `audit_id` / hashes / JSON
//!   are `String`.

use async_trait::async_trait;
use serde_json::Value as Json;
use uuid::Uuid;

use super::clickhouse::{ClickHouseCanonicalStore, sql_lit};
use super::clickhouse_projection::{ch_dt, ch_err, ch_json, ch_str, ch_uuid};
use super::system_store::{
    AdminAuditChainReport, AdminAuditInsert, AdminAuditListFilter, AdminAuditRow, AdminAuditStore,
    SystemStoreResult, compute_admin_audit_hash, verify_admin_audit_chain_step,
};

/// Canonical audit SELECT column order. Pinned next to the mapper.
const AUDIT_COLS: &str = "audit_id, actor, operation, target, request_json, result, \
     tenant_id, project_id, correlation_id, previous_hash, current_hash, \
     signer_key_id, external_anchor, created_at";

fn row_to_audit(row: &Json) -> SystemStoreResult<AdminAuditRow> {
    let audit_id = ch_uuid(row, "audit_id")?;
    Ok(AdminAuditRow {
        audit_id,
        actor: ch_str(row, "actor"),
        operation: ch_str(row, "operation"),
        target: ch_str(row, "target"),
        request_json: ch_json(row, "request_json", Json::Null),
        result: ch_str(row, "result"),
        tenant_id: ch_str(row, "tenant_id"),
        project_id: ch_str(row, "project_id"),
        correlation_id: ch_str(row, "correlation_id"),
        previous_hash: ch_str(row, "previous_hash"),
        current_hash: ch_str(row, "current_hash"),
        signer_key_id: ch_str(row, "signer_key_id"),
        external_anchor: ch_str(row, "external_anchor"),
        created_at: ch_dt(row, "created_at"),
    })
}

impl ClickHouseCanonicalStore {
    fn audit_table(&self) -> SystemStoreResult<String> {
        self.qualified("udb_admin_audit_log")
            .map_err(|e| ch_err("audit_table", e))
    }
}

#[async_trait]
impl AdminAuditStore for ClickHouseCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "clickhouse"
    }

    async fn ensure_admin_audit_tables(&self) -> SystemStoreResult<()> {
        let tbl = self.audit_table()?;
        // Append-only MergeTree ordered by (created_at, audit_id) so verify reads
        // the chain in order directly off the sort key.
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {tbl} (\
             audit_id String, \
             actor String, \
             operation String, \
             target String, \
             request_json String, \
             result String, \
             tenant_id String, \
             project_id String, \
             correlation_id String, \
             previous_hash String, \
             current_hash String, \
             signer_key_id String, \
             external_anchor String, \
             created_at Int64\
             ) ENGINE = MergeTree ORDER BY (created_at, audit_id)"
        );
        self.executor()
            .execute_ddl(&ddl)
            .await
            .map_err(|e| ch_err("ensure_admin_audit_tables", e))?;
        Ok(())
    }

    async fn latest_admin_audit_hash(&self) -> SystemStoreResult<String> {
        let tbl = self.audit_table()?;
        // Newest row's current_hash by (created_at, audit_id) DESC.
        let sql = format!(
            "SELECT current_hash FROM {tbl} ORDER BY created_at DESC, audit_id DESC LIMIT 1"
        );
        let rows = self
            .executor()
            .select_rows(&sql)
            .await
            .map_err(|e| ch_err("latest_admin_audit_hash", e))?;
        Ok(rows
            .first()
            .map(|r| ch_str(r, "current_hash"))
            .unwrap_or_default())
    }

    async fn append_admin_audit(&self, entry: &AdminAuditInsert) -> SystemStoreResult<Uuid> {
        let tbl = self.audit_table()?;
        let audit_id = Uuid::new_v4();
        // 1. Read the latest hash (empty when the chain is fresh).
        let previous_hash = self.latest_admin_audit_hash().await?;
        // 2. Compute the new row's current_hash via the SHARED hasher (byte-
        //    identical to every other backend).
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
        // 3. INSERT the row (append). SINGLE-WRITER CAVEAT (see module docs): no
        //    chain-head CAS, so two concurrent appenders could fork the chain;
        //    the conformance run is sequential so the chain stays linear.
        let now = Self::now_unix_ms();
        let sql = format!(
            "INSERT INTO {tbl} ({AUDIT_COLS}) VALUES (\
             {audit_id}, {actor}, {operation}, {target}, {request_json}, {result}, \
             {tenant}, {project}, {corr}, {prev}, {cur}, {signer}, {anchor}, {created})",
            audit_id = sql_lit(&audit_id.to_string()),
            actor = sql_lit(&entry.actor),
            operation = sql_lit(&entry.operation),
            target = sql_lit(&entry.target),
            request_json = sql_lit(&entry.request_json.to_string()),
            result = sql_lit(&entry.result),
            tenant = sql_lit(&entry.tenant_id),
            project = sql_lit(&entry.project_id),
            corr = sql_lit(&entry.correlation_id),
            prev = sql_lit(&previous_hash),
            cur = sql_lit(&current_hash),
            signer = sql_lit(&entry.signer_key_id),
            anchor = sql_lit(&entry.external_anchor),
            created = now,
        );
        self.executor()
            .execute_ddl(&sql)
            .await
            .map_err(|e| ch_err("append_admin_audit insert", e))?;
        Ok(audit_id)
    }

    async fn list_admin_audit(
        &self,
        filter: &AdminAuditListFilter,
    ) -> SystemStoreResult<Vec<AdminAuditRow>> {
        let tbl = self.audit_table()?;
        // Scan + Rust-side filter / DESC sort / page (the equality filters are on
        // non-key columns). Append-only, no FINAL needed.
        let scan = format!("SELECT {AUDIT_COLS} FROM {tbl}");
        let rows = self
            .executor()
            .select_rows(&scan)
            .await
            .map_err(|e| ch_err("list_admin_audit scan", e))?;
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
        let tbl = self.audit_table()?;
        // Read oldest-to-newest by (created_at, audit_id) ASC and feed rows one at
        // a time to the SHARED verify step. Honour `limit` by stopping early.
        let sql = format!("SELECT {AUDIT_COLS} FROM {tbl} ORDER BY created_at ASC, audit_id ASC");
        let rows = self
            .executor()
            .select_rows(&sql)
            .await
            .map_err(|e| ch_err("verify_admin_audit_chain", e))?;
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
