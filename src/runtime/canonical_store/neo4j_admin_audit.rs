//! Neo4j implementation of [`AdminAuditStore`] (B.10b PHASE 2).
//!
//! Cypher over the HTTP transactional API, mirroring the Postgres impl
//! (`postgres_admin_audit.rs`) SEMANTICS exactly so the cross-backend
//! conformance contract passes byte-for-byte. The tamper-evident hash chain is
//! computed in Rust via the SHARED [`compute_admin_audit_hash`] /
//! [`verify_admin_audit_chain_step`] helpers, so PG / MySQL / SQLite / MSSQL /
//! MongoDB / Cassandra / Neo4j chains are byte-identical.
//!
//! Every row is a `(:UdbAuditLog)` node carrying `run_tag` (test isolation on
//! Community single-DB) + `audit_id` as a property; every query is scoped by
//! `{run_tag:$tag}`.
//!
//! ## Atomicity — chain-serialised append in ONE HTTP transaction
//!
//! PG uses `pg_advisory_xact_lock` to serialise the chain; Neo4j has no
//! advisory lock. Instead `append_admin_audit` runs read-head-then-create as a
//! LIST of statements through [`cypher_tx_rows`], which the HTTP
//! `/db/<db>/tx/commit` endpoint runs as ONE ACID transaction. The first
//! statement MATCHes the latest-hash node, taking a read lock that the
//! transaction holds to commit; we then compute the next `current_hash` in Rust
//! and the second statement CREATEs the audit node carrying that hash. Two
//! concurrent appends serialise on the contended nodes, so the chain never
//! forks with two rows sharing one `previous_hash`.
//!
//! ## Modeling choices (mirror the logical PG `udb_admin_audit_log`)
//!
//! - `request_json` JSON stored as a Cypher string property (serialised/parsed
//!   in Rust — Neo4j node properties cannot hold nested maps).
//! - `created_at` stored as a client-computed epoch-millis integer so the
//!   verify scan can `ORDER BY created_at, audit_id` exactly like PG.

use async_trait::async_trait;
use serde_json::{Value as Json, json};
use uuid::Uuid;

use super::neo4j::{
    LABEL_AUDIT_LOG, Neo4jCanonicalStore, neo_err, now_unix_ms, prop_dt, prop_json, prop_str,
    prop_uuid,
};
use super::system_store::{
    AdminAuditChainReport, AdminAuditInsert, AdminAuditListFilter, AdminAuditRow, AdminAuditStore,
    SystemStoreError, SystemStoreResult, compute_admin_audit_hash, verify_admin_audit_chain_step,
};

/// Build an `AdminAuditRow` from a `node{.*}` map projection.
fn node_to_audit(node: &Json) -> SystemStoreResult<AdminAuditRow> {
    let audit_id = prop_uuid(node, "audit_id")?;
    Ok(AdminAuditRow {
        audit_id,
        actor: prop_str(node, "actor"),
        operation: prop_str(node, "operation"),
        target: prop_str(node, "target"),
        request_json: prop_json(node, "request_json", Json::Null),
        result: prop_str(node, "result"),
        tenant_id: prop_str(node, "tenant_id"),
        project_id: prop_str(node, "project_id"),
        correlation_id: prop_str(node, "correlation_id"),
        previous_hash: prop_str(node, "previous_hash"),
        current_hash: prop_str(node, "current_hash"),
        signer_key_id: prop_str(node, "signer_key_id"),
        external_anchor: prop_str(node, "external_anchor"),
        created_at: prop_dt(node, "created_at"),
    })
}

impl Neo4jCanonicalStore {
    /// The run-tag bound param value (mirrors phase-1's `tag()`).
    fn audit_tag(&self) -> Json {
        json!(self.run_tag())
    }
}

#[async_trait]
impl AdminAuditStore for Neo4jCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "neo4j"
    }

    async fn ensure_admin_audit_tables(&self) -> SystemStoreResult<()> {
        // Composite RANGE indexes (Community-compatible — see phase-1
        // `ensure_system_tables`). `created_at` is in a composite index so the
        // verify scan's `ORDER BY created_at, audit_id` is cheap.
        let indexes = [
            format!(
                "CREATE INDEX udb_audit_id_idx IF NOT EXISTS \
                 FOR (a:{LABEL_AUDIT_LOG}) ON (a.run_tag, a.audit_id)"
            ),
            format!(
                "CREATE INDEX udb_audit_created_idx IF NOT EXISTS \
                 FOR (a:{LABEL_AUDIT_LOG}) ON (a.run_tag, a.created_at)"
            ),
        ];
        for ddl in indexes {
            self.executor()
                .cypher_rows(&ddl, json!({}))
                .await
                .map_err(|e| neo_err("ensure_admin_audit_tables", e))?;
        }
        Ok(())
    }

    async fn latest_admin_audit_hash(&self) -> SystemStoreResult<String> {
        // Newest non-empty current_hash by (created_at DESC, audit_id DESC) —
        // identical ordering to PG so a tie on created_at breaks the same way.
        let cypher = format!(
            "MATCH (a:{LABEL_AUDIT_LOG} {{run_tag:$tag}}) WHERE a.current_hash <> '' \
             RETURN a.current_hash AS h ORDER BY a.created_at DESC, a.audit_id DESC LIMIT 1"
        );
        let rows = self
            .executor()
            .cypher_rows(&cypher, json!({ "tag": self.audit_tag() }))
            .await
            .map_err(|e| neo_err("latest_admin_audit_hash", e))?;
        Ok(rows
            .first()
            .and_then(|r| r.get("h"))
            .and_then(Json::as_str)
            .map(|s| s.to_string())
            .unwrap_or_default())
    }

    async fn append_admin_audit(&self, entry: &AdminAuditInsert) -> SystemStoreResult<Uuid> {
        // ── Chain-serialised append via cypher_tx_rows (read head, then create) ─
        // We read the latest-hash node, compute the next `current_hash` in Rust
        // (via the SHARED hasher so the chain is byte-identical to every other
        // backend's), then run read-head + guarded-create as ONE HTTP
        // transaction through `cypher_tx_rows` (the /tx/commit endpoint commits
        // the statement list atomically). The create re-reads the head INSIDE
        // that tx and gates `WHERE observed = $previous_hash`, making the link a
        // compare-and-set: if a concurrent append advanced the head between our
        // first read and this tx, the guard fails, CREATE does not run, and the
        // create statement RETURNs `created = 0` — so the chain never forks and
        // a lost CAS surfaces (as a retryable Io error) rather than silently
        // double-linking.
        let read_head = format!(
            "MATCH (a:{LABEL_AUDIT_LOG} {{run_tag:$tag}}) WHERE a.current_hash <> '' \
             RETURN a.current_hash AS h ORDER BY a.created_at DESC, a.audit_id DESC LIMIT 1"
        );
        let head_params = json!({ "tag": self.audit_tag() });
        let head_rows = self
            .executor()
            .cypher_rows(&read_head, head_params.clone())
            .await
            .map_err(|e| neo_err("append_admin_audit read head", e))?;
        let previous_hash = head_rows
            .first()
            .and_then(|r| r.get("h"))
            .and_then(Json::as_str)
            .map(|s| s.to_string())
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
        let now = now_unix_ms();
        // Re-read the head inside the tx as `observed` (folding the empty chain
        // to '' via collect()), gate `WHERE observed = $previous_hash`, CREATE,
        // and RETURN the created-node count so the CAS outcome is observable.
        let create = format!(
            "OPTIONAL MATCH (a:{LABEL_AUDIT_LOG} {{run_tag:$tag}}) WHERE a.current_hash <> '' \
             WITH a.current_hash AS observed, a.created_at AS ca, a.audit_id AS ai \
             ORDER BY ca DESC, ai DESC \
             WITH collect(observed) AS observed_list \
             WITH CASE WHEN size(observed_list) = 0 THEN '' ELSE observed_list[0] END AS observed \
             WHERE observed = $previous_hash \
             CREATE (:{LABEL_AUDIT_LOG} {{run_tag:$tag, audit_id:$audit_id, actor:$actor, \
                 operation:$operation, target:$target, request_json:$request_json, result:$result, \
                 tenant_id:$tenant_id, project_id:$project_id, correlation_id:$correlation_id, \
                 previous_hash:$previous_hash, current_hash:$current_hash, \
                 signer_key_id:$signer_key_id, external_anchor:$external_anchor, created_at:$now}}) \
             RETURN count(*) AS created"
        );
        let create_params = json!({
            "tag": self.audit_tag(),
            "audit_id": audit_id.to_string(),
            "actor": entry.actor,
            "operation": entry.operation,
            "target": entry.target,
            "request_json": entry.request_json.to_string(),
            "result": entry.result,
            "tenant_id": entry.tenant_id,
            "project_id": entry.project_id,
            "correlation_id": entry.correlation_id,
            "previous_hash": previous_hash,
            "current_hash": current_hash,
            "signer_key_id": entry.signer_key_id,
            "external_anchor": entry.external_anchor,
            "now": now,
        });
        let statements: &[(&str, Json)] = &[
            (read_head.as_str(), head_params),
            (create.as_str(), create_params),
        ];
        let results = self
            .executor()
            .cypher_tx_rows(statements)
            .await
            .map_err(|e| neo_err("append_admin_audit tx", e))?;
        // `cypher_tx_rows` returns the rows of EACH statement in order; the
        // create is statement index 1, RETURNing `created` (1 on a winning CAS,
        // 0 on a lost one).
        let created = results
            .get(1)
            .and_then(|rows| rows.first())
            .and_then(|r| r.get("created"))
            .and_then(Json::as_i64)
            .unwrap_or(0);
        if created == 0 {
            // Guard missed (a concurrent append linked first). Surface as Io so
            // the caller can retry the append against the new head.
            return Err(neo_err(
                "append_admin_audit",
                "chain head advanced concurrently; retry append",
            ));
        }
        Ok(audit_id)
    }

    async fn list_admin_audit(
        &self,
        filter: &AdminAuditListFilter,
    ) -> SystemStoreResult<Vec<AdminAuditRow>> {
        // MATCH + optional equality filters + ORDER BY created_at DESC +
        // SKIP/LIMIT (PG's WHERE … ORDER BY created_at DESC LIMIT/OFFSET).
        let mut filters = String::new();
        if filter.operation.is_some() {
            filters.push_str(" AND a.operation = $operation");
        }
        if filter.actor.is_some() {
            filters.push_str(" AND a.actor = $actor");
        }
        if filter.tenant_id.is_some() {
            filters.push_str(" AND a.tenant_id = $tenant_id");
        }
        if filter.project_id.is_some() {
            filters.push_str(" AND a.project_id = $project_id");
        }
        let limit = if filter.limit <= 0 { 100 } else { filter.limit };
        let offset = filter.offset.max(0);
        let cypher = format!(
            "MATCH (a:{LABEL_AUDIT_LOG} {{run_tag:$tag}}) \
             WHERE true{filters} \
             WITH a ORDER BY a.created_at DESC SKIP $offset LIMIT $limit \
             RETURN a{{.*}} AS a"
        );
        let mut params = json!({ "tag": self.audit_tag(), "offset": offset, "limit": limit });
        let obj = params.as_object_mut().expect("params is an object");
        if let Some(op) = &filter.operation {
            obj.insert("operation".to_string(), json!(op));
        }
        if let Some(actor) = &filter.actor {
            obj.insert("actor".to_string(), json!(actor));
        }
        if let Some(t) = &filter.tenant_id {
            obj.insert("tenant_id".to_string(), json!(t));
        }
        if let Some(p) = &filter.project_id {
            obj.insert("project_id".to_string(), json!(p));
        }
        let rows = self
            .executor()
            .cypher_rows(&cypher, params)
            .await
            .map_err(|e| neo_err("list_admin_audit", e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let node = row
                .get("a")
                .ok_or_else(|| neo_err("list_admin_audit", "row missing 'a' projection"))?;
            let mut audit = node_to_audit(node)?;
            if filter.redact_request_json {
                audit.request_json = json!({"redacted": true});
            }
            out.push(audit);
        }
        Ok(out)
    }

    async fn verify_admin_audit_chain(
        &self,
        limit: Option<i64>,
    ) -> SystemStoreResult<AdminAuditChainReport> {
        // Read the chain oldest-to-newest (created_at ASC, audit_id ASC — same
        // total order PG verifies under) and feed rows one at a time to the
        // SHARED verify step, stopping at the first break or once `limit` rows
        // are checked.
        let max = match limit {
            Some(n) if n > 0 => Some(n),
            _ => None,
        };
        let mut cypher = format!(
            "MATCH (a:{LABEL_AUDIT_LOG} {{run_tag:$tag}}) \
             WITH a ORDER BY a.created_at ASC, a.audit_id ASC"
        );
        if let Some(n) = max {
            cypher.push_str(&format!(" LIMIT {n}"));
        }
        cypher.push_str(" RETURN a{.*} AS a");
        let rows = self
            .executor()
            .cypher_rows(&cypher, json!({ "tag": self.audit_tag() }))
            .await
            .map_err(|e| neo_err("verify_admin_audit_chain", e))?;
        let mut previous_hash = String::new();
        let mut checked: i64 = 0;
        for row in &rows {
            if let Some(n) = max {
                if checked >= n {
                    break;
                }
            }
            let node = row
                .get("a")
                .ok_or_else(|| neo_err("verify_admin_audit_chain", "row missing 'a' projection"))?;
            let audit = node_to_audit(node)?;
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
