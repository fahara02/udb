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
//! 3. INSERT the audit row FIRST. The head must never point at a hash whose
//!    row does not exist: advancing the head and then failing (or crashing)
//!    before the row write would leave a permanent gap that breaks every later
//!    verify, i.e. an append that "failed" would corrupt the chain.
//! 4. CAS the head: `UPDATE … SET head_hash=<new> WHERE id='chain' IF
//!    head_hash=<observed>` (or `INSERT … IF NOT EXISTS` on the very first
//!    append). On a not-applied CAS another appender advanced the head: delete
//!    the just-written (unlinked) row, re-read and retry. This is single-writer
//!    chain serialisation without a multi-row transaction.
//!
//! A crash between steps 3 and 4 leaves an UNLINKED row: its hash is neither
//! the head nor any row's `previous_hash`. Verify skips such dead-end rows, but
//! only while the walk still ends exactly at the head; any other divergence
//! (deleted, edited or re-linked rows) falls back to the strict walk and is
//! reported exactly as before.
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
        let head_sql = format!(
            "SELECT head_hash FROM {tbl} WHERE id = ?",
            tbl = self.audit_chain_table(),
        );
        let chain_tbl = self.audit_chain_table();

        let insert = format!(
            "INSERT INTO {tbl} ( \
                chain, created_at, audit_id, actor, operation, target, request_json, result, \
                tenant_id, project_id, correlation_id, previous_hash, current_hash, \
                signer_key_id, external_anchor \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tbl = self.audit_table(),
        );
        let delete_unlinked = format!(
            "DELETE FROM {tbl} WHERE chain = ? AND created_at = ? AND audit_id = ?",
            tbl = self.audit_table(),
        );

        // ── Chain-head CAS loop ─────────────────────────────────────────────
        // Linearise the chain on the single head row: read head, compute next,
        // write the row, CAS the head. On a lost CAS another appender advanced
        // the head; drop our unlinked row, re-read + retry.
        let mut attempt = 0u32;
        let audit_id = loop {
            attempt += 1;
            if attempt > CHAIN_CAS_MAX_ATTEMPTS {
                return Err(cass_err(
                    "append_admin_audit",
                    "chain-head CAS did not converge",
                ));
            }
            // Fresh id per attempt, so an undeletable unlinked row from a lost
            // CAS can never share an id with the row that finally links.
            let audit_id = Uuid::new_v4();
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
            // ── Row first: the head may only ever name a persisted row ──────
            let created_at_ms = now_unix_ms();
            self.client()
                .cql_execute(
                    &insert,
                    (
                        CHAIN_PARTITION,
                        cql_ts(created_at_ms),
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
            // ── Then link it: CAS the head onto the persisted row ───────────
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
                break audit_id;
            }
            // Lost the CAS — another appender linked first. Our row links to a
            // stale head: remove it (best-effort; a leftover is an unlinked
            // dead end that verify tolerates) and retry on the new head.
            if let Err(e) = self
                .client()
                .cql_execute(
                    &delete_unlinked,
                    (CHAIN_PARTITION, cql_ts(created_at_ms), audit_id.to_string()),
                )
                .await
            {
                tracing::warn!(
                    audit_id = %audit_id,
                    error = %cass_err("append_admin_audit delete unlinked row", e),
                    "admin audit: could not delete an unlinked row after a lost head CAS"
                );
            }
        };
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
        let audits = rows
            .iter()
            .map(row_to_audit)
            .collect::<SystemStoreResult<Vec<_>>>()?;
        let head = self.latest_admin_audit_hash().await?;
        Ok(verify_chain_tolerating_unlinked(&audits, &head, limit))
    }
}

/// Strict forward walk (the shared semantics every backend uses).
fn verify_chain_strict(audits: &[AdminAuditRow], limit: Option<i64>) -> AdminAuditChainReport {
    walk_chain(audits.iter(), limit)
}

fn walk_chain<'a>(
    audits: impl Iterator<Item = &'a AdminAuditRow>,
    limit: Option<i64>,
) -> AdminAuditChainReport {
    let max = match limit {
        Some(n) if n > 0 => Some(n),
        _ => None,
    };
    let mut previous_hash = String::new();
    let mut checked: i64 = 0;
    for audit in audits {
        if let Some(n) = max {
            if checked >= n {
                break;
            }
        }
        match verify_admin_audit_chain_step(audit, &previous_hash, checked) {
            Ok(next) => {
                previous_hash = next;
                checked += 1;
            }
            Err(report) => return report,
        }
    }
    AdminAuditChainReport::Passed {
        checked_count: checked,
        last_hash: previous_hash,
    }
}

/// Verify the chain, skipping rows left UNLINKED by an append that wrote its
/// row but never won the head CAS (crash, or an undeletable loser row): such a
/// row's hash is neither the head nor any row's `previous_hash`.
///
/// The tolerant walk is accepted ONLY when it passes and (for a full walk)
/// ends exactly at the chain head. Any other outcome — a deleted, edited or
/// re-linked row — returns the strict walk's report, so tamper detection and
/// the reported break are unchanged from the strict semantics.
fn verify_chain_tolerating_unlinked(
    audits: &[AdminAuditRow],
    head: &str,
    limit: Option<i64>,
) -> AdminAuditChainReport {
    if head.is_empty() {
        return verify_chain_strict(audits, limit);
    }
    let referenced: std::collections::HashSet<&str> =
        audits.iter().map(|a| a.previous_hash.as_str()).collect();
    let linked = audits
        .iter()
        .filter(|a| a.current_hash == head || referenced.contains(a.current_hash.as_str()));
    let tolerant = walk_chain(linked, limit);
    let limited = matches!(limit, Some(n) if n > 0);
    match &tolerant {
        AdminAuditChainReport::Passed { last_hash, .. } if limited || last_hash == head => tolerant,
        _ => verify_chain_strict(audits, limit),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(previous_hash: &str, actor: &str) -> AdminAuditRow {
        let request_json = serde_json::json!({"k": actor});
        let current_hash = compute_admin_audit_hash(
            previous_hash,
            actor,
            "op",
            "target",
            &request_json,
            "ok",
            "tenant",
            "project",
            "corr",
            "",
            "",
        );
        AdminAuditRow {
            audit_id: Uuid::new_v4(),
            actor: actor.to_string(),
            operation: "op".to_string(),
            target: "target".to_string(),
            request_json,
            result: "ok".to_string(),
            tenant_id: "tenant".to_string(),
            project_id: "project".to_string(),
            correlation_id: "corr".to_string(),
            previous_hash: previous_hash.to_string(),
            current_hash,
            signer_key_id: String::new(),
            external_anchor: String::new(),
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn unlinked_row_from_a_crashed_append_does_not_break_verify() {
        let a = row("", "a");
        // Crashed append: row written on head=a, head never advanced to it.
        let orphan = row(&a.current_hash, "orphan");
        // The next append linked on the same head.
        let b = row(&a.current_hash, "b");
        let head = b.current_hash.clone();
        let audits = vec![a.clone(), orphan, b.clone()];
        // The strict walk alone would report a break at `b`.
        assert!(!verify_chain_strict(&audits, None).is_passed());
        let report = verify_chain_tolerating_unlinked(&audits, &head, None);
        assert!(report.is_passed(), "{report:?}");
        assert_eq!(report.checked_count(), 2);
    }

    #[test]
    fn trailing_unlinked_row_is_not_reported_as_the_tip() {
        let a = row("", "a");
        let orphan = row(&a.current_hash, "orphan");
        let head = a.current_hash.clone();
        let report = verify_chain_tolerating_unlinked(&[a, orphan], &head, None);
        match report {
            AdminAuditChainReport::Passed {
                last_hash,
                checked_count,
            } => {
                assert_eq!(last_hash, head);
                assert_eq!(checked_count, 1);
            }
            other => panic!("expected pass, got {other:?}"),
        }
    }

    #[test]
    fn tampering_still_fails_with_strict_semantics() {
        let a = row("", "a");
        let b = row(&a.current_hash, "b");
        let c = row(&b.current_hash, "c");
        let head = c.current_hash.clone();
        // Deleted middle row: never tolerated.
        let gap = vec![a.clone(), c.clone()];
        assert_eq!(
            verify_chain_tolerating_unlinked(&gap, &head, None),
            verify_chain_strict(&gap, None)
        );
        assert!(!verify_chain_tolerating_unlinked(&gap, &head, None).is_passed());
        // Edited tip hash: the walk no longer ends at the head → strict report.
        let mut forged = c.clone();
        forged.current_hash = "f".repeat(64);
        let edited = vec![a, b, forged];
        let report = verify_chain_tolerating_unlinked(&edited, &head, None);
        assert!(!report.is_passed(), "{report:?}");
        assert_eq!(report, verify_chain_strict(&edited, None));
    }
}
