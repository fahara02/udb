//! Neo4j implementation of [`SagaStore`] (B.10b PHASE 2).
//!
//! Cypher over the HTTP transactional API, mirroring the Postgres impl
//! (`postgres_saga.rs`) SEMANTICS exactly so the cross-backend conformance
//! contract passes byte-for-byte. Every saga is a `(:UdbSaga)` node carrying
//! `run_tag` (test isolation on Community single-DB) + `saga_id` as a property;
//! every query is scoped by `{run_tag:$tag}`.
//!
//! ## Why one Cypher statement is enough where PG needs row locks
//!
//! Neo4j's auto-commit endpoint runs each statement as one ACID transaction, so
//! a `MATCH … WITH … LIMIT … SET …` (the recovery claim) runs entirely inside
//! that transaction and takes write locks on the matched nodes. Concurrent
//! claimers serialise on those node write locks the way PG serialises on the
//! row locks — no candidate CTE + second UPDATE round-trip is needed.
//!
//! ## Modeling choices (mirror the logical PG `udb_sagas`)
//!
//! - status / compensation_status stored as the EXACT PG canonical strings
//!   ([`SagaStatus::as_str`] / [`CompensationStatus::as_str`]).
//! - `steps` / `compensations` JSON stored as Cypher string properties
//!   (serialised/parsed in Rust — Neo4j node properties cannot hold nested
//!   maps).
//! - timestamps stored as client-computed epoch-millis integers (identical to
//!   the phase-1 lease math), so `created_at`/`updated_at` ordering matches the
//!   SQL stores.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value as Json, json};
use uuid::Uuid;

use super::neo4j::{
    LABEL_SAGA, Neo4jCanonicalStore, neo_err, now_unix_ms, prop_dt, prop_i32, prop_json, prop_str,
    prop_uuid,
};
use super::system_store::{
    CompensationStatus, SagaInsert, SagaListFilter, SagaRow, SagaStatus, SagaStore, SagaSummary,
    SystemStoreError, SystemStoreResult,
};

/// Build a `SagaRow` from a `node{.*}` map projection.
fn node_to_saga(node: &Json) -> SystemStoreResult<SagaRow> {
    let saga_id = prop_uuid(node, "saga_id")?;
    let status_str = prop_str(node, "status");
    let status = SagaStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!("unknown saga status '{status_str}' in neo4j row"))
    })?;
    let comp_status_str = prop_str(node, "compensation_status");
    let compensation_status = CompensationStatus::parse(&comp_status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown compensation_status '{comp_status_str}' in neo4j row"
        ))
    })?;
    Ok(SagaRow {
        saga_id,
        tx_id: prop_str(node, "tx_id"),
        tenant_id: prop_str(node, "tenant_id"),
        correlation_id: prop_str(node, "correlation_id"),
        status,
        backend_instance: prop_str(node, "backend_instance"),
        operation: prop_str(node, "operation"),
        current_step: prop_i32(node, "current_step"),
        retry_count: prop_i32(node, "retry_count"),
        recovery_attempts: prop_i32(node, "recovery_attempts"),
        compensation_status,
        steps: prop_json(node, "steps", Json::Array(vec![])),
        compensations: prop_json(node, "compensations", Json::Array(vec![])),
        last_error: prop_str(node, "last_error"),
        created_at: prop_dt(node, "created_at"),
        updated_at: prop_dt(node, "updated_at"),
    })
}

impl Neo4jCanonicalStore {
    /// The run-tag bound param value (mirrors phase-1's `tag()`).
    fn saga_tag(&self) -> Json {
        json!(self.run_tag())
    }
}

#[async_trait]
impl SagaStore for Neo4jCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "neo4j"
    }

    async fn ensure_saga_tables(&self) -> SystemStoreResult<()> {
        // Composite RANGE indexes (Community-compatible — see phase-1
        // `ensure_system_tables` for why NOT uniqueness constraints; the logical
        // singleton per (run_tag, saga_id) is enforced by the `CREATE` with a
        // fresh UUID, the index only makes lookups cheap). `CREATE INDEX IF NOT
        // EXISTS` is idempotent (Neo4j 5 syntax) and Community-safe.
        let indexes = [
            format!(
                "CREATE INDEX udb_saga_id_idx IF NOT EXISTS \
                 FOR (s:{LABEL_SAGA}) ON (s.run_tag, s.saga_id)"
            ),
            format!(
                "CREATE INDEX udb_saga_status_idx IF NOT EXISTS \
                 FOR (s:{LABEL_SAGA}) ON (s.run_tag, s.status)"
            ),
            format!(
                "CREATE INDEX udb_saga_tenant_idx IF NOT EXISTS \
                 FOR (s:{LABEL_SAGA}) ON (s.run_tag, s.tenant_id)"
            ),
        ];
        for ddl in indexes {
            self.executor()
                .cypher_rows(&ddl, json!({}))
                .await
                .map_err(|e| neo_err("ensure_saga_tables", e))?;
        }
        Ok(())
    }

    async fn record_saga(&self, saga: &SagaInsert) -> SystemStoreResult<Uuid> {
        // CREATE a fresh saga node, stamping the assigned saga_id + the full
        // payload + zeroed counters + compensation_status='none' (PG default) +
        // created/updated. JSON columns serialise to string properties.
        let saga_id = Uuid::new_v4();
        let now = now_unix_ms();
        let cypher = format!(
            "CREATE (s:{LABEL_SAGA} {{run_tag:$tag, saga_id:$saga_id, tx_id:$tx_id, \
                 tenant_id:$tenant_id, correlation_id:$correlation_id, status:$status, \
                 backend_instance:$backend_instance, operation:$operation, current_step:0, \
                 retry_count:0, recovery_attempts:0, compensation_status:$comp, \
                 steps:$steps, compensations:$compensations, last_error:'', \
                 created_at:$now, updated_at:$now}})"
        );
        self.executor()
            .cypher_rows(
                &cypher,
                json!({
                    "tag": self.saga_tag(),
                    "saga_id": saga_id.to_string(),
                    "tx_id": saga.tx_id,
                    "tenant_id": saga.tenant_id,
                    "correlation_id": saga.correlation_id,
                    "status": saga.status.as_str(),
                    "backend_instance": saga.backend_instance,
                    "operation": saga.operation,
                    "comp": CompensationStatus::None.as_str(),
                    "steps": saga.steps.to_string(),
                    "compensations": saga.compensations.to_string(),
                    "now": now,
                }),
            )
            .await
            .map_err(|e| neo_err("record_saga", e))?;
        Ok(saga_id)
    }

    async fn get_saga(&self, saga_id: Uuid) -> SystemStoreResult<Option<SagaRow>> {
        let cypher = format!(
            "MATCH (s:{LABEL_SAGA} {{run_tag:$tag, saga_id:$saga_id}}) RETURN s{{.*}} AS s"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({ "tag": self.saga_tag(), "saga_id": saga_id.to_string() }),
            )
            .await
            .map_err(|e| neo_err("get_saga", e))?;
        match rows.first().and_then(|r| r.get("s")) {
            Some(node) => Ok(Some(node_to_saga(node)?)),
            None => Ok(None),
        }
    }

    async fn list_sagas(&self, filter: &SagaListFilter) -> SystemStoreResult<Vec<SagaRow>> {
        // MATCH + optional equality filters + ORDER BY updated_at DESC +
        // SKIP/LIMIT (PG's WHERE … ORDER BY updated_at DESC LIMIT/OFFSET).
        let mut filters = String::new();
        if filter.tenant_id.is_some() {
            filters.push_str(" AND s.tenant_id = $tenant_id");
        }
        if filter.status.is_some() {
            filters.push_str(" AND s.status = $status");
        }
        if filter.tx_id.is_some() {
            filters.push_str(" AND s.tx_id = $tx_id");
        }
        if filter.correlation_id.is_some() {
            filters.push_str(" AND s.correlation_id = $correlation_id");
        }
        let limit = if filter.limit <= 0 { 100 } else { filter.limit };
        let offset = filter.offset.max(0);
        let cypher = format!(
            "MATCH (s:{LABEL_SAGA} {{run_tag:$tag}}) \
             WHERE true{filters} \
             WITH s ORDER BY s.updated_at DESC SKIP $offset LIMIT $limit \
             RETURN s{{.*}} AS s"
        );
        let mut params = json!({ "tag": self.saga_tag(), "offset": offset, "limit": limit });
        let obj = params.as_object_mut().expect("params is an object");
        if let Some(t) = &filter.tenant_id {
            obj.insert("tenant_id".to_string(), json!(t));
        }
        if let Some(s) = filter.status {
            obj.insert("status".to_string(), json!(s.as_str()));
        }
        if let Some(t) = &filter.tx_id {
            obj.insert("tx_id".to_string(), json!(t));
        }
        if let Some(c) = &filter.correlation_id {
            obj.insert("correlation_id".to_string(), json!(c));
        }
        let rows = self
            .executor()
            .cypher_rows(&cypher, params)
            .await
            .map_err(|e| neo_err("list_sagas", e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let node = row
                .get("s")
                .ok_or_else(|| neo_err("list_sagas", "row missing 's' projection"))?;
            out.push(node_to_saga(node)?);
        }
        Ok(out)
    }

    async fn update_saga_status(
        &self,
        saga_id: Uuid,
        status: SagaStatus,
        compensation_status: CompensationStatus,
    ) -> SystemStoreResult<()> {
        // SET status + compensation_status + updated_at; `count(s)` reports
        // rows-affected so a missing saga surfaces as InvalidInput (PG's
        // rows_affected == 0).
        let now = now_unix_ms();
        let cypher = format!(
            "MATCH (s:{LABEL_SAGA} {{run_tag:$tag, saga_id:$saga_id}}) \
             SET s.status=$status, s.compensation_status=$comp, s.updated_at=$now \
             RETURN count(s) AS n"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({
                    "tag": self.saga_tag(),
                    "saga_id": saga_id.to_string(),
                    "status": status.as_str(),
                    "comp": compensation_status.as_str(),
                    "now": now,
                }),
            )
            .await
            .map_err(|e| neo_err("update_saga_status", e))?;
        let n = rows
            .first()
            .and_then(|r| r.get("n"))
            .and_then(Json::as_i64)
            .unwrap_or(0);
        if n == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found for update_saga_status"
            )));
        }
        Ok(())
    }

    async fn update_saga_statuses_batch(
        &self,
        saga_ids: &[Uuid],
        status: SagaStatus,
        compensation_status: CompensationStatus,
    ) -> SystemStoreResult<()> {
        if saga_ids.is_empty() {
            return Ok(());
        }
        // Set-based variant (PG's `WHERE saga_id = ANY($3)`): bind the id list
        // and `WHERE s.saga_id IN $ids` in one statement.
        let now = now_unix_ms();
        let ids: Vec<String> = saga_ids.iter().map(|id| id.to_string()).collect();
        let cypher = format!(
            "MATCH (s:{LABEL_SAGA} {{run_tag:$tag}}) WHERE s.saga_id IN $ids \
             SET s.status=$status, s.compensation_status=$comp, s.updated_at=$now"
        );
        self.executor()
            .cypher_rows(
                &cypher,
                json!({
                    "tag": self.saga_tag(),
                    "ids": ids,
                    "status": status.as_str(),
                    "comp": compensation_status.as_str(),
                    "now": now,
                }),
            )
            .await
            .map_err(|e| neo_err("update_saga_statuses_batch", e))?;
        Ok(())
    }

    async fn mark_saga_manual_review(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        let now = now_unix_ms();
        let cypher = format!(
            "MATCH (s:{LABEL_SAGA} {{run_tag:$tag, saga_id:$saga_id}}) \
             SET s.status=$status, s.updated_at=$now \
             RETURN count(s) AS n"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({
                    "tag": self.saga_tag(),
                    "saga_id": saga_id.to_string(),
                    "status": SagaStatus::ManualReview.as_str(),
                    "now": now,
                }),
            )
            .await
            .map_err(|e| neo_err("mark_saga_manual_review", e))?;
        let n = rows
            .first()
            .and_then(|r| r.get("n"))
            .and_then(Json::as_i64)
            .unwrap_or(0);
        if n == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found"
            )));
        }
        Ok(())
    }

    async fn request_saga_recompensation(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        // PG: conditional UPDATE WHERE status IN ('failed_compensation',
        // 'manual_review') bumping retry_count. The single-statement gate
        // `WHERE s.status IN [...]` filters out non-retryable sagas, so a
        // matched 0 (count == 0) maps to the same InvalidInput PG raises.
        // `retry_count = retry_count + 1` is the read-modify in-statement (the
        // node write lock serialises concurrent bumps).
        let now = now_unix_ms();
        let cypher = format!(
            "MATCH (s:{LABEL_SAGA} {{run_tag:$tag, saga_id:$saga_id}}) \
             WHERE s.status IN ['failed_compensation','manual_review'] \
             SET s.status=$status, s.last_error='', s.retry_count=s.retry_count + 1, \
                 s.compensation_status=$comp, s.updated_at=$now \
             RETURN count(s) AS n"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({
                    "tag": self.saga_tag(),
                    "saga_id": saga_id.to_string(),
                    "status": SagaStatus::Indeterminate.as_str(),
                    "comp": CompensationStatus::RetryRequested.as_str(),
                    "now": now,
                }),
            )
            .await
            .map_err(|e| neo_err("request_saga_recompensation", e))?;
        let n = rows
            .first()
            .and_then(|r| r.get("n"))
            .and_then(Json::as_i64)
            .unwrap_or(0);
        if n == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} is not in a retryable state (must be failed_compensation or manual_review)"
            )));
        }
        Ok(())
    }

    async fn increment_recovery_attempts(
        &self,
        saga_id: Uuid,
        error: &str,
    ) -> SystemStoreResult<i64> {
        // PG: `UPDATE … SET recovery_attempts = recovery_attempts + 1 …
        // RETURNING`. One statement increments in place (node write lock
        // serialises concurrent bumps) and RETURNs the post-increment value.
        let now = now_unix_ms();
        let cypher = format!(
            "MATCH (s:{LABEL_SAGA} {{run_tag:$tag, saga_id:$saga_id}}) \
             SET s.recovery_attempts=s.recovery_attempts + 1, s.last_error=$err, s.updated_at=$now \
             RETURN s.recovery_attempts AS attempts"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({
                    "tag": self.saga_tag(),
                    "saga_id": saga_id.to_string(),
                    "err": error,
                    "now": now,
                }),
            )
            .await
            .map_err(|e| neo_err("increment_recovery_attempts", e))?;
        rows.first()
            .and_then(|r| r.get("attempts"))
            .and_then(Json::as_i64)
            .ok_or_else(|| {
                SystemStoreError::InvalidInput(format!(
                    "saga {saga_id} not found for increment_recovery_attempts"
                ))
            })
    }

    async fn claim_recoverable_sagas(
        &self,
        stale_after: Duration,
        limit: i64,
    ) -> SystemStoreResult<Vec<SagaRow>> {
        // The recoverable queue mirrors PG's SELECT: rows that are
        // 'indeterminate'/'in_doubt' ALWAYS, plus 'in_progress' rows whose
        // updated_at is older than the client-computed cutoff. PG only reads
        // here, returning each row with its CURRENT status, so the contract sees
        // an indeterminate row come back as indeterminate. The Cassandra impl's
        // "flip stale in_progress to indeterminate on claim" is its own recovery
        // optimisation, NOT part of the read contract — so here we likewise read
        // the candidate set and return rows with their original status, flipping
        // nothing (the worker's subsequent update_saga_status owns the
        // transition). oldest-first, LIMIT, in one auto-commit read tx.
        let cutoff = now_unix_ms() - (stale_after.as_millis() as i64);
        let cypher = format!(
            "MATCH (s:{LABEL_SAGA} {{run_tag:$tag}}) \
             WHERE s.status IN ['indeterminate','in_doubt'] \
                OR (s.status='in_progress' AND s.updated_at < $cutoff) \
             WITH s ORDER BY s.updated_at ASC LIMIT $limit \
             RETURN s{{.*}} AS s"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({
                    "tag": self.saga_tag(),
                    "cutoff": cutoff,
                    "limit": limit.max(1),
                }),
            )
            .await
            .map_err(|e| neo_err("claim_recoverable_sagas", e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let node = row
                .get("s")
                .ok_or_else(|| neo_err("claim_recoverable_sagas", "row missing 's' projection"))?;
            out.push(node_to_saga(node)?);
        }
        Ok(out)
    }

    async fn mark_stale_in_progress_indeterminate(
        &self,
        stale_after: Duration,
    ) -> SystemStoreResult<i64> {
        let now = now_unix_ms();
        let cutoff = now - (stale_after.as_millis() as i64);
        let cypher = format!(
            "MATCH (s:{LABEL_SAGA} {{run_tag:$tag}}) \
             WHERE s.status='in_progress' AND s.updated_at < $cutoff \
             SET s.status='indeterminate', \
                 s.last_error='stale in-progress reconciled at startup', s.updated_at=$now \
             RETURN count(s) AS n"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({ "tag": self.saga_tag(), "cutoff": cutoff, "now": now }),
            )
            .await
            .map_err(|e| neo_err("mark_stale_in_progress_indeterminate", e))?;
        Ok(rows
            .first()
            .and_then(|r| r.get("n"))
            .and_then(Json::as_i64)
            .unwrap_or(0))
    }

    async fn saga_summary(&self) -> SystemStoreResult<SagaSummary> {
        // Count by status with Cypher aggregation; fold into the typed summary
        // in Rust (an unknown status string is a hard error, like the SQL/Cass
        // impls).
        let cypher = format!(
            "MATCH (s:{LABEL_SAGA} {{run_tag:$tag}}) RETURN s.status AS status, count(s) AS n"
        );
        let rows = self
            .executor()
            .cypher_rows(&cypher, json!({ "tag": self.saga_tag() }))
            .await
            .map_err(|e| neo_err("saga_summary", e))?;
        let mut s = SagaSummary::default();
        for row in &rows {
            let status = prop_str(row, "status");
            let n = row.get("n").and_then(Json::as_i64).unwrap_or(0);
            match SagaStatus::parse(&status) {
                Some(SagaStatus::Indeterminate) => s.indeterminate += n,
                Some(SagaStatus::InProgress) => s.in_progress += n,
                Some(SagaStatus::Pending) => s.pending += n,
                Some(SagaStatus::Committed) => s.committed += n,
                Some(SagaStatus::Compensated) => s.compensated += n,
                Some(SagaStatus::Failed) => s.failed += n,
                Some(SagaStatus::InDoubt) => s.in_doubt += n,
                Some(SagaStatus::FailedCompensation) => s.failed_compensation += n,
                Some(SagaStatus::ManualReview) => s.manual_review += n,
                None => {
                    return Err(SystemStoreError::InvalidInput(format!(
                        "unknown saga status '{status}' in neo4j summary"
                    )));
                }
            }
        }
        Ok(s)
    }
}
