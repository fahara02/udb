//! Neo4j implementation of [`MigrationAuditStore`] (B.10b PHASE 2).
//!
//! Cypher over the HTTP transactional API, mirroring the Postgres impl
//! (`postgres_migration_audit.rs`) SEMANTICS exactly so the cross-backend
//! conformance contract passes byte-for-byte:
//!
//! - Run rows are `(:UdbMigrationRun)` nodes keyed by `run_id`; op-ledger rows
//!   are `(:UdbMigrationOp)` nodes carrying `run_id` + `operation_index`.
//! - The op-ledger `id` is a monotone integer allocated from a
//!   `(:UdbCounter {run_tag, id:'migration_op_seq'})` node via the SAME
//!   MERGE+increment pattern phase-1 uses for the outbox sequence — the Neo4j
//!   analogue of PG's `BIGSERIAL` (Neo4j has no auto-increment). The bump and
//!   the op CREATE are split, but the counter node's write lock serialises
//!   concurrent allocations so every id is distinct + monotone.
//! - `applied_at` is set only when the op status is `APPLIED`; `finished_at` is
//!   set on `finish_migration_run`. Both are OPTIONAL timestamps that
//!   round-trip `None`-when-absent (the bug class the MSSQL impl flagged): an
//!   absent property reads back as `None`, never `Some(now)`.
//! - state / status values stored as their EXACT PG canonical strings.
//!
//! Every node carries `run_tag`; every query is scoped by `{run_tag:$tag}`.

use async_trait::async_trait;
use serde_json::{Value as Json, json};
use uuid::Uuid;

use super::neo4j::{
    LABEL_MIGRATION_OP, LABEL_MIGRATION_RUN, Neo4jCanonicalStore, neo_err, now_unix_ms, prop_dt,
    prop_i32, prop_i64, prop_json, prop_opt_dt, prop_str, prop_uuid,
};
use super::system_store::{
    MigrationAuditStore, MigrationOpInsert, MigrationOpRow, MigrationRunInsert, MigrationRunRow,
    MigrationRunState, MigrationRunsFilter, OpLedgerStatus, SystemStoreError, SystemStoreResult,
};

/// Well-known counter-node id for the migration-op ledger sequence (the Neo4j
/// `BIGSERIAL` analogue, a sibling of phase-1's `outbox_seq`).
const OP_SEQ_ID: &str = "migration_op_seq";

/// Build a `MigrationRunRow` from a `node{.*}` map projection.
fn node_to_run(node: &Json) -> SystemStoreResult<MigrationRunRow> {
    let run_id = prop_uuid(node, "run_id")?;
    let state_str = prop_str(node, "state");
    let state = MigrationRunState::parse(&state_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown migration run state '{state_str}' in neo4j row"
        ))
    })?;
    Ok(MigrationRunRow {
        run_id,
        project_id: prop_str(node, "project_id"),
        catalog_version: prop_str(node, "catalog_version"),
        state,
        operations_hash: prop_str(node, "operations_hash"),
        approval_token: prop_str(node, "approval_token"),
        started_at: prop_dt(node, "started_at"),
        finished_at: prop_opt_dt(node, "finished_at"),
        error: prop_str(node, "error"),
    })
}

/// Build a `MigrationOpRow` from a `node{.*}` map projection.
fn node_to_op(node: &Json) -> SystemStoreResult<MigrationOpRow> {
    let run_id = prop_uuid(node, "run_id")?;
    let status_str = prop_str(node, "status");
    let status = OpLedgerStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown op ledger status '{status_str}' in neo4j row"
        ))
    })?;
    Ok(MigrationOpRow {
        id: prop_i64(node, "id"),
        run_id,
        operation_index: prop_i32(node, "operation_index"),
        backend: prop_str(node, "backend"),
        resource_uri: prop_str(node, "resource_uri"),
        operation_kind: prop_str(node, "operation_kind"),
        status,
        payload_json: prop_json(
            node,
            "payload_json",
            prop_json(node, "rollback_json", Json::Object(Default::default())),
        ),
        error: prop_str(node, "error"),
        applied_at: prop_opt_dt(node, "applied_at"),
    })
}

impl Neo4jCanonicalStore {
    /// The run-tag bound param value (mirrors phase-1's `tag()`).
    fn mig_tag(&self) -> Json {
        json!(self.run_tag())
    }

    /// Allocate the next monotone ledger id from the `(:UdbCounter)` seq node,
    /// MERGE+bumping it in one statement (same pattern as the phase-1 outbox
    /// seq; the counter node's write lock serialises concurrent allocations).
    async fn next_migration_op_id(&self) -> SystemStoreResult<i64> {
        let cypher = "MERGE (c:UdbCounter {run_tag:$tag, id:$id}) \
             ON CREATE SET c.seq = 1 \
             ON MATCH SET c.seq = c.seq + 1 \
             RETURN c.seq AS seq";
        let rows = self
            .executor()
            .cypher_rows(cypher, json!({ "tag": self.mig_tag(), "id": OP_SEQ_ID }))
            .await
            .map_err(|e| neo_err("record_migration_op seq", e))?;
        rows.first()
            .and_then(|r| r.get("seq"))
            .and_then(Json::as_i64)
            .ok_or_else(|| neo_err("record_migration_op seq", "counter returned no sequence"))
    }
}

#[async_trait]
impl MigrationAuditStore for Neo4jCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "neo4j"
    }

    async fn ensure_migration_audit_tables(&self) -> SystemStoreResult<()> {
        // Composite RANGE indexes (Community-compatible — see phase-1
        // `ensure_system_tables`). The op index carries operation_index so
        // `list_migration_ops`' ORDER BY is cheap.
        let indexes = [
            format!(
                "CREATE INDEX udb_migration_run_id_idx IF NOT EXISTS \
                 FOR (r:{LABEL_MIGRATION_RUN}) ON (r.run_tag, r.run_id)"
            ),
            format!(
                "CREATE INDEX udb_migration_run_project_idx IF NOT EXISTS \
                 FOR (r:{LABEL_MIGRATION_RUN}) ON (r.run_tag, r.project_id)"
            ),
            format!(
                "CREATE INDEX udb_migration_op_run_idx IF NOT EXISTS \
                 FOR (o:{LABEL_MIGRATION_OP}) ON (o.run_tag, o.run_id, o.operation_index)"
            ),
        ];
        for ddl in indexes {
            self.executor()
                .cypher_rows(&ddl, json!({}))
                .await
                .map_err(|e| neo_err("ensure_migration_audit_tables", e))?;
        }
        Ok(())
    }

    async fn start_migration_run(&self, run: &MigrationRunInsert) -> SystemStoreResult<Uuid> {
        // CREATE a run node, _id = a fresh run_id. finished_at + error are
        // deliberately left ABSENT/empty so finished_at round-trips None until
        // finish_migration_run sets it.
        let run_id = Uuid::new_v4();
        let now = now_unix_ms();
        let cypher = format!(
            "CREATE (r:{LABEL_MIGRATION_RUN} {{run_tag:$tag, run_id:$run_id, \
                 project_id:$project_id, catalog_version:$catalog_version, state:$state, \
                 operations_hash:$operations_hash, approval_token:$approval_token, \
                 started_at:$now, error:''}})"
        );
        self.executor()
            .cypher_rows(
                &cypher,
                json!({
                    "tag": self.mig_tag(),
                    "run_id": run_id.to_string(),
                    "project_id": run.project_id,
                    "catalog_version": run.catalog_version,
                    "state": run.state.as_str(),
                    "operations_hash": run.operations_hash,
                    "approval_token": run.approval_token,
                    "now": now,
                }),
            )
            .await
            .map_err(|e| neo_err("start_migration_run", e))?;
        Ok(run_id)
    }

    async fn record_migration_op(&self, op: &MigrationOpInsert) -> SystemStoreResult<i64> {
        // Monotone id from the seq counter node; CREATE the op node carrying
        // id + run_id + operation_index. applied_at is set only when the status
        // is APPLIED (PG's CASE), otherwise left ABSENT so it round-trips None.
        let id = self.next_migration_op_id().await?;
        let mut props = json!({
            "tag": self.mig_tag(),
            "id": id,
            "run_id": op.run_id.to_string(),
            "operation_index": op.operation_index,
            "backend": op.backend,
            "resource_uri": op.resource_uri,
            "operation_kind": op.operation_kind,
            "status": op.status.as_str(),
            "payload_json": op.payload_json.to_string(),
            "error": op.error,
        });
        // applied_at only when APPLIED — the SET clause is conditional so the
        // property is absent (→ None) otherwise.
        let applied_set = if op.status == OpLedgerStatus::Applied {
            props
                .as_object_mut()
                .expect("params is an object")
                .insert("applied_at".to_string(), json!(now_unix_ms()));
            ", applied_at:$applied_at"
        } else {
            ""
        };
        let cypher = format!(
            "CREATE (o:{LABEL_MIGRATION_OP} {{run_tag:$tag, id:$id, run_id:$run_id, \
                 operation_index:$operation_index, backend:$backend, resource_uri:$resource_uri, \
                 operation_kind:$operation_kind, status:$status, payload_json:$payload_json, \
                 error:$error{applied_set}}})"
        );
        self.executor()
            .cypher_rows(&cypher, props)
            .await
            .map_err(|e| neo_err("record_migration_op insert", e))?;
        Ok(id)
    }

    async fn finish_migration_run(
        &self,
        run_id: Uuid,
        new_state: MigrationRunState,
        error: &str,
    ) -> SystemStoreResult<()> {
        // SET state + error + finished_at (which MUST persist + round-trip
        // Some). `count(r)` reports rows-affected so a missing run surfaces as
        // InvalidInput (PG's rows_affected == 0).
        let now = now_unix_ms();
        let cypher = format!(
            "MATCH (r:{LABEL_MIGRATION_RUN} {{run_tag:$tag, run_id:$run_id}}) \
             SET r.state=$state, r.error=$error, r.finished_at=$now \
             RETURN count(r) AS n"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({
                    "tag": self.mig_tag(),
                    "run_id": run_id.to_string(),
                    "state": new_state.as_str(),
                    "error": error,
                    "now": now,
                }),
            )
            .await
            .map_err(|e| neo_err("finish_migration_run", e))?;
        let n = rows
            .first()
            .and_then(|r| r.get("n"))
            .and_then(Json::as_i64)
            .unwrap_or(0);
        if n == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "migration run {run_id} not found for finish_migration_run"
            )));
        }
        Ok(())
    }

    async fn get_migration_run(&self, run_id: Uuid) -> SystemStoreResult<Option<MigrationRunRow>> {
        let cypher = format!(
            "MATCH (r:{LABEL_MIGRATION_RUN} {{run_tag:$tag, run_id:$run_id}}) RETURN r{{.*}} AS r"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({ "tag": self.mig_tag(), "run_id": run_id.to_string() }),
            )
            .await
            .map_err(|e| neo_err("get_migration_run", e))?;
        match rows.first().and_then(|r| r.get("r")) {
            Some(node) => Ok(Some(node_to_run(node)?)),
            None => Ok(None),
        }
    }

    async fn list_migration_ops(&self, run_id: Uuid) -> SystemStoreResult<Vec<MigrationOpRow>> {
        let cypher = format!(
            "MATCH (o:{LABEL_MIGRATION_OP} {{run_tag:$tag, run_id:$run_id}}) \
             WITH o ORDER BY o.operation_index ASC, o.id ASC \
             RETURN o{{.*}} AS o"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({ "tag": self.mig_tag(), "run_id": run_id.to_string() }),
            )
            .await
            .map_err(|e| neo_err("list_migration_ops", e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let node = row
                .get("o")
                .ok_or_else(|| neo_err("list_migration_ops", "row missing 'o' projection"))?;
            out.push(node_to_op(node)?);
        }
        Ok(out)
    }

    async fn list_migration_runs(
        &self,
        filter: &MigrationRunsFilter,
    ) -> SystemStoreResult<Vec<MigrationRunRow>> {
        // MATCH + optional equality filters + ORDER BY started_at DESC +
        // SKIP/LIMIT (PG's WHERE … ORDER BY started_at DESC LIMIT/OFFSET).
        let mut filters = String::new();
        if filter.project_id.is_some() {
            filters.push_str(" AND r.project_id = $project_id");
        }
        if filter.state.is_some() {
            filters.push_str(" AND r.state = $state");
        }
        if filter.catalog_version.is_some() {
            filters.push_str(" AND r.catalog_version = $catalog_version");
        }
        let limit = if filter.limit <= 0 { 100 } else { filter.limit };
        let offset = filter.offset.max(0);
        let cypher = format!(
            "MATCH (r:{LABEL_MIGRATION_RUN} {{run_tag:$tag}}) \
             WHERE true{filters} \
             WITH r ORDER BY r.started_at DESC SKIP $offset LIMIT $limit \
             RETURN r{{.*}} AS r"
        );
        let mut params = json!({ "tag": self.mig_tag(), "offset": offset, "limit": limit });
        let obj = params.as_object_mut().expect("params is an object");
        if let Some(p) = &filter.project_id {
            obj.insert("project_id".to_string(), json!(p));
        }
        if let Some(s) = filter.state {
            obj.insert("state".to_string(), json!(s.as_str()));
        }
        if let Some(v) = &filter.catalog_version {
            obj.insert("catalog_version".to_string(), json!(v));
        }
        let rows = self
            .executor()
            .cypher_rows(&cypher, params)
            .await
            .map_err(|e| neo_err("list_migration_runs", e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let node = row
                .get("r")
                .ok_or_else(|| neo_err("list_migration_runs", "row missing 'r' projection"))?;
            out.push(node_to_run(node)?);
        }
        Ok(out)
    }
}
