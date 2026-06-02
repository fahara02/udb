//! MongoDB implementation of [`SagaStore`] (B.9 phase 2).
//!
//! Each saga is one BSON document keyed by `_id = <saga_id uuid as string>`.
//! Semantics mirror the Postgres impl (`postgres_saga.rs`) exactly so the
//! cross-backend conformance contract passes byte-for-byte:
//!
//! - Status / compensation_status stored as their canonical Postgres strings.
//! - `steps` / `compensations` stored as embedded BSON (the JSON round-trips).
//! - Timestamps stored as `bson::DateTime` (millisecond ordering) and parsed
//!   back to `DateTime<Utc>`.
//! - `claim_recoverable_sagas` runs inside a session transaction — the Mongo
//!   analogue of PG's `SELECT … ORDER BY updated_at ASC` over the recoverable
//!   set, reading candidates and flipping `in_progress` staleness in one
//!   atomic pass so concurrent recovery workers never double-claim.
//!
//! The bson↔typed helpers live in `mongodb_projection.rs`; this file reuses
//! them (`mongo_err`, `bdt_to_chrono`, `now_bdt`, `parse_uuid_id`, …).

use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use futures::TryStreamExt;
use mongodb_driver::bson::{self, Bson, Document, doc};
use mongodb_driver::options::IndexOptions;
use mongodb_driver::{Collection, IndexModel};
use uuid::Uuid;

use super::mongodb::{MongoDbCanonicalStore, SAGA_COLLECTION};
use super::mongodb_projection::{
    get_dt, get_i32, get_json, get_str, json_to_bson, mongo_err, now_bdt, parse_uuid_id,
};
use super::system_store::{
    CompensationStatus, SagaInsert, SagaListFilter, SagaRow, SagaStatus, SagaStore, SagaSummary,
    SystemStoreError, SystemStoreResult,
};

// ── Row mapping ───────────────────────────────────────────────────────────────

fn doc_to_saga(doc: &Document) -> SystemStoreResult<SagaRow> {
    let saga_id = parse_uuid_id(doc, "_id")?;
    let status_str = get_str(doc, "status");
    let status = SagaStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!("unknown saga status '{status_str}' in mongodb row"))
    })?;
    let comp_status_str = get_str(doc, "compensation_status");
    let compensation_status = CompensationStatus::parse(&comp_status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown compensation_status '{comp_status_str}' in mongodb row"
        ))
    })?;
    Ok(SagaRow {
        saga_id,
        tx_id: get_str(doc, "tx_id"),
        tenant_id: get_str(doc, "tenant_id"),
        correlation_id: get_str(doc, "correlation_id"),
        status,
        backend_instance: get_str(doc, "backend_instance"),
        operation: get_str(doc, "operation"),
        current_step: get_i32(doc, "current_step"),
        retry_count: get_i32(doc, "retry_count"),
        recovery_attempts: get_i32(doc, "recovery_attempts"),
        compensation_status,
        steps: get_json(doc, "steps", serde_json::Value::Array(vec![])),
        compensations: get_json(doc, "compensations", serde_json::Value::Array(vec![])),
        last_error: get_str(doc, "last_error"),
        created_at: get_dt(doc, "created_at"),
        updated_at: get_dt(doc, "updated_at"),
    })
}

impl MongoDbCanonicalStore {
    pub(super) fn sagas(&self) -> Collection<Document> {
        self.db().collection::<Document>(SAGA_COLLECTION)
    }
}

#[async_trait]
impl SagaStore for MongoDbCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mongodb"
    }

    async fn ensure_saga_tables(&self) -> SystemStoreResult<()> {
        match self.db().create_collection(SAGA_COLLECTION).await {
            Ok(_) => {}
            Err(err) if Self::is_namespace_exists(&err) => {}
            Err(err) => return Err(mongo_err("ensure_saga_tables create", err)),
        }
        // Mirror the PG `(tenant_id, status, updated_at DESC)` index.
        let by_tenant = IndexModel::builder()
            .keys(doc! { "tenant_id": 1, "status": 1, "updated_at": -1 })
            .options(IndexOptions::builder().build())
            .build();
        self.sagas()
            .create_index(by_tenant)
            .await
            .map_err(|e| mongo_err("ensure_saga_tables tenant index", e))?;
        Ok(())
    }

    async fn record_saga(&self, saga: &SagaInsert) -> SystemStoreResult<Uuid> {
        let saga_id = Uuid::new_v4();
        let now = now_bdt();
        let row = doc! {
            "_id": saga_id.to_string(),
            "tx_id": &saga.tx_id,
            "tenant_id": &saga.tenant_id,
            "correlation_id": &saga.correlation_id,
            "status": saga.status.as_str(),
            "backend_instance": &saga.backend_instance,
            "operation": &saga.operation,
            "current_step": 0_i32,
            "retry_count": 0_i32,
            "recovery_attempts": 0_i32,
            "compensation_status": CompensationStatus::None.as_str(),
            "steps": json_to_bson(&saga.steps),
            "compensations": json_to_bson(&saga.compensations),
            "last_error": "",
            "created_at": now,
            "updated_at": now,
        };
        self.sagas()
            .insert_one(row)
            .await
            .map_err(|e| mongo_err("record_saga insert", e))?;
        Ok(saga_id)
    }

    async fn get_saga(&self, saga_id: Uuid) -> SystemStoreResult<Option<SagaRow>> {
        let row = self
            .sagas()
            .find_one(doc! { "_id": saga_id.to_string() })
            .await
            .map_err(|e| mongo_err("get_saga find", e))?;
        match row {
            Some(d) => Ok(Some(doc_to_saga(&d)?)),
            None => Ok(None),
        }
    }

    async fn list_sagas(&self, filter: &SagaListFilter) -> SystemStoreResult<Vec<SagaRow>> {
        let mut query = Document::new();
        if let Some(t) = &filter.tenant_id {
            query.insert("tenant_id", t);
        }
        if let Some(s) = filter.status {
            query.insert("status", s.as_str());
        }
        if let Some(t) = &filter.tx_id {
            query.insert("tx_id", t);
        }
        if let Some(c) = &filter.correlation_id {
            query.insert("correlation_id", c);
        }
        let limit = if filter.limit <= 0 { 100 } else { filter.limit };
        let skip = filter.offset.max(0) as u64;
        let mut cursor = self
            .sagas()
            .find(query)
            .sort(doc! { "updated_at": -1 })
            .skip(skip)
            .limit(limit)
            .await
            .map_err(|e| mongo_err("list_sagas find", e))?;
        let mut out = Vec::new();
        while let Some(d) = cursor
            .try_next()
            .await
            .map_err(|e| mongo_err("list_sagas cursor", e))?
        {
            out.push(doc_to_saga(&d)?);
        }
        Ok(out)
    }

    async fn update_saga_status(
        &self,
        saga_id: Uuid,
        status: SagaStatus,
        compensation_status: CompensationStatus,
    ) -> SystemStoreResult<()> {
        let now = now_bdt();
        let result = self
            .sagas()
            .update_one(
                doc! { "_id": saga_id.to_string() },
                doc! { "$set": {
                    "status": status.as_str(),
                    "compensation_status": compensation_status.as_str(),
                    "updated_at": now,
                } },
            )
            .await
            .map_err(|e| mongo_err("update_saga_status", e))?;
        if result.matched_count == 0 {
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
        let now = now_bdt();
        let ids: Vec<Bson> = saga_ids
            .iter()
            .map(|id| Bson::String(id.to_string()))
            .collect();
        self.sagas()
            .update_many(
                doc! { "_id": { "$in": ids } },
                doc! { "$set": {
                    "status": status.as_str(),
                    "compensation_status": compensation_status.as_str(),
                    "updated_at": now,
                } },
            )
            .await
            .map_err(|e| mongo_err("update_saga_statuses_batch", e))?;
        Ok(())
    }

    async fn mark_saga_manual_review(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        let now = now_bdt();
        let result = self
            .sagas()
            .update_one(
                doc! { "_id": saga_id.to_string() },
                doc! { "$set": {
                    "status": SagaStatus::ManualReview.as_str(),
                    "updated_at": now,
                } },
            )
            .await
            .map_err(|e| mongo_err("mark_saga_manual_review", e))?;
        if result.matched_count == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found"
            )));
        }
        Ok(())
    }

    async fn request_saga_recompensation(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        let now = now_bdt();
        // Conditional update: only sagas in `failed_compensation` or
        // `manual_review` may be re-queued — the Mongo analogue of PG's
        // `WHERE saga_id = $1 AND status IN (...)`.
        let result = self
            .sagas()
            .update_one(
                doc! {
                    "_id": saga_id.to_string(),
                    "status": { "$in": [
                        SagaStatus::FailedCompensation.as_str(),
                        SagaStatus::ManualReview.as_str(),
                    ] },
                },
                doc! {
                    "$set": {
                        "status": SagaStatus::Indeterminate.as_str(),
                        "last_error": "",
                        "compensation_status": CompensationStatus::RetryRequested.as_str(),
                        "updated_at": now,
                    },
                    "$inc": { "retry_count": 1_i32 },
                },
            )
            .await
            .map_err(|e| mongo_err("request_saga_recompensation", e))?;
        if result.matched_count == 0 {
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
        let now = now_bdt();
        // `find_one_and_update` with `$inc` and `ReturnDocument::After` yields
        // the post-increment counter — PG's `RETURNING recovery_attempts`.
        let updated = self
            .sagas()
            .find_one_and_update(
                doc! { "_id": saga_id.to_string() },
                doc! {
                    "$inc": { "recovery_attempts": 1_i32 },
                    "$set": { "last_error": error, "updated_at": now },
                },
            )
            .return_document(mongodb_driver::options::ReturnDocument::After)
            .await
            .map_err(|e| mongo_err("increment_recovery_attempts", e))?
            .ok_or_else(|| {
                SystemStoreError::InvalidInput(format!(
                    "saga {saga_id} not found for increment_recovery_attempts"
                ))
            })?;
        Ok(get_i32(&updated, "recovery_attempts") as i64)
    }

    async fn claim_recoverable_sagas(
        &self,
        stale_after: Duration,
        limit: i64,
    ) -> SystemStoreResult<Vec<SagaRow>> {
        let now = Utc::now();
        // Stale cutoff: `in_progress` rows older than `stale_after` are
        // recoverable; `indeterminate` / `in_doubt` always are.
        let cutoff = bson::DateTime::from_millis(
            (now - chrono::Duration::from_std(stale_after).unwrap_or_default()).timestamp_millis(),
        );
        let candidate_filter = doc! {
            "$or": [
                { "status": { "$in": [
                    SagaStatus::Indeterminate.as_str(),
                    SagaStatus::InDoubt.as_str(),
                ] } },
                {
                    "status": SagaStatus::InProgress.as_str(),
                    "updated_at": { "$lt": cutoff },
                },
            ],
        };

        // Atomic claim inside a session transaction: read the oldest-first
        // recoverable candidates, then flip the stale `in_progress` ones to
        // `indeterminate` so a second concurrent recovery worker committing the
        // same id set conflicts and retries (the Mongo analogue of claiming the
        // recoverable queue under one snapshot).
        let client = self.db().client();
        let mut session = client
            .start_session()
            .await
            .map_err(|e| mongo_err("claim_recoverable_sagas start_session", e))?;
        session
            .start_transaction()
            .await
            .map_err(|e| mongo_err("claim_recoverable_sagas start_transaction", e))?;

        let claim_result: SystemStoreResult<Vec<SagaRow>> = async {
            let mut cursor = self
                .sagas()
                .find(candidate_filter)
                .sort(doc! { "updated_at": 1 })
                .limit(limit.max(1))
                .session(&mut session)
                .await
                .map_err(|e| mongo_err("claim_recoverable_sagas find", e))?;
            let mut candidates: Vec<Document> = Vec::new();
            while let Some(next) = cursor.next(&mut session).await {
                let d = next.map_err(|e| mongo_err("claim_recoverable_sagas cursor", e))?;
                candidates.push(d);
            }
            if candidates.is_empty() {
                return Ok(Vec::new());
            }
            // Flip stale `in_progress` rows to `indeterminate` so the recovery
            // worker treats them uniformly (PG resets them via
            // `mark_stale_in_progress_indeterminate`; here we claim+flip in one
            // pass). Rows already indeterminate/in_doubt are returned as-is.
            let stale_ids: Vec<Bson> = candidates
                .iter()
                .filter(|d| get_str(d, "status") == SagaStatus::InProgress.as_str())
                .filter_map(|d| d.get_str("_id").ok().map(|s| Bson::String(s.to_string())))
                .collect();
            if !stale_ids.is_empty() {
                let flip_now = now_bdt();
                self.sagas()
                    .update_many(
                        doc! { "_id": { "$in": stale_ids } },
                        doc! { "$set": {
                            "status": SagaStatus::Indeterminate.as_str(),
                            "last_error": "stale in-progress reconciled at recovery claim",
                            "updated_at": flip_now,
                        } },
                    )
                    .session(&mut session)
                    .await
                    .map_err(|e| mongo_err("claim_recoverable_sagas update_many", e))?;
                // Reflect the post-claim status in the returned rows.
                for d in candidates.iter_mut() {
                    if get_str(d, "status") == SagaStatus::InProgress.as_str() {
                        d.insert("status", SagaStatus::Indeterminate.as_str());
                        d.insert("updated_at", flip_now);
                    }
                }
            }
            let mut out = Vec::with_capacity(candidates.len());
            for d in &candidates {
                out.push(doc_to_saga(d)?);
            }
            Ok(out)
        }
        .await;

        match claim_result {
            Ok(rows) => {
                session
                    .commit_transaction()
                    .await
                    .map_err(|e| mongo_err("claim_recoverable_sagas commit", e))?;
                Ok(rows)
            }
            Err(err) => {
                let _ = session.abort_transaction().await;
                Err(err)
            }
        }
    }

    async fn mark_stale_in_progress_indeterminate(
        &self,
        stale_after: Duration,
    ) -> SystemStoreResult<i64> {
        let cutoff = bson::DateTime::from_millis(
            (Utc::now() - chrono::Duration::from_std(stale_after).unwrap_or_default())
                .timestamp_millis(),
        );
        let now = now_bdt();
        let result = self
            .sagas()
            .update_many(
                doc! {
                    "status": SagaStatus::InProgress.as_str(),
                    "updated_at": { "$lt": cutoff },
                },
                doc! { "$set": {
                    "status": SagaStatus::Indeterminate.as_str(),
                    "last_error": "stale in-progress reconciled at startup",
                    "updated_at": now,
                } },
            )
            .await
            .map_err(|e| mongo_err("mark_stale_in_progress_indeterminate", e))?;
        Ok(result.modified_count as i64)
    }

    async fn saga_summary(&self) -> SystemStoreResult<SagaSummary> {
        let pipeline = vec![doc! { "$group": { "_id": "$status", "n": { "$sum": 1_i64 } } }];
        let mut cursor = self
            .sagas()
            .aggregate(pipeline)
            .await
            .map_err(|e| mongo_err("saga_summary aggregate", e))?;
        let mut s = SagaSummary::default();
        while let Some(d) = cursor
            .try_next()
            .await
            .map_err(|e| mongo_err("saga_summary cursor", e))?
        {
            let status = d.get_str("_id").unwrap_or_default();
            let n = d.get_i64("n").unwrap_or(0);
            match SagaStatus::parse(status) {
                Some(SagaStatus::Indeterminate) => s.indeterminate = n,
                Some(SagaStatus::InProgress) => s.in_progress = n,
                Some(SagaStatus::Pending) => s.pending = n,
                Some(SagaStatus::Committed) => s.committed = n,
                Some(SagaStatus::Compensated) => s.compensated = n,
                Some(SagaStatus::Failed) => s.failed = n,
                Some(SagaStatus::InDoubt) => s.in_doubt = n,
                Some(SagaStatus::FailedCompensation) => s.failed_compensation = n,
                Some(SagaStatus::ManualReview) => s.manual_review = n,
                None => {
                    return Err(SystemStoreError::InvalidInput(format!(
                        "unknown saga status '{status}' in mongodb summary"
                    )));
                }
            }
        }
        Ok(s)
    }
}
