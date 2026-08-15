//! MongoDB implementation of [`ProjectionTaskStore`] (B.9 phase 2).
//!
//! Each projection task is one BSON document keyed by `_id = <task_id uuid as
//! string>`. The semantics mirror the Postgres impl
//! (`postgres_projection.rs`) exactly so the cross-backend conformance
//! contract passes byte-for-byte:
//!
//! - **Idempotent enqueue** via a UNIQUE index on `idempotency_key` plus a
//!   `find_one_and_update(..., $setOnInsert, upsert, ReturnDocument::After)`,
//!   returning the assigned-or-existing `_id`.
//! - **Atomic batch claim** in a session transaction: read `PENDING|FAILED`
//!   candidates (oldest-first, `retry_count < max`, `next_retry_at` due),
//!   flip them to `IN_PROGRESS` with one `update_many`, commit, return the
//!   claimed rows.
//! - Status / operation enums stored as their canonical Postgres strings.
//! - Timestamps stored as `bson::DateTime` (millisecond ordering) and parsed
//!   back to `DateTime<Utc>`; `Option` timestamps round-trip as absent/present.
//!
//! This file also hosts the `pub(super)` bson→typed-row helpers reused by the
//! sibling system-store impls (`parse_uuid_id`, `bdt_to_chrono`, …).

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use mongodb_driver::bson::{self, Bson, Document, doc};
use mongodb_driver::options::{IndexOptions, ReturnDocument};
use mongodb_driver::{Collection, IndexModel};
use uuid::Uuid;

use super::mongodb::{MongoDbCanonicalStore, PROJECTION_COLLECTION};
use super::system_store::{
    DeadLetterGroup, PendingTaskMetric, ProjectionClaimFilter, ProjectionOperation,
    ProjectionTaskInsert, ProjectionTaskRow, ProjectionTaskStatus, ProjectionTaskStore,
    ProjectionTaskSummary, SystemStoreError, SystemStoreResult,
};

// ── Shared bson↔typed helpers (reused by the sibling system-store impls) ──────

/// Map any MongoDB driver error to a typed `SystemStoreError::Io` for the
/// `"mongodb"` backend. `op` names the failed operation for the message.
pub(super) fn mongo_err(op: &str, err: impl std::fmt::Display) -> SystemStoreError {
    SystemStoreError::Io {
        backend: "mongodb",
        source: format!("{op}: {err}"),
    }
}

/// chrono `DateTime<Utc>` from a `bson::DateTime`.
pub(super) fn bdt_to_chrono(bdt: bson::DateTime) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(bdt.timestamp_millis()).unwrap_or_else(Utc::now)
}

/// Now, as a `bson::DateTime` at millisecond precision.
pub(super) fn now_bdt() -> bson::DateTime {
    bson::DateTime::from_millis(Utc::now().timestamp_millis())
}

/// Read a required `DateTime<Utc>` field. Falls back to `Utc::now()` when the
/// field is absent or not a BSON datetime (matching the SQL impls' tolerant
/// `unwrap_or_else(|_| Utc::now())`).
pub(super) fn get_dt(doc: &Document, key: &str) -> DateTime<Utc> {
    doc.get_datetime(key)
        .map(|bdt| bdt_to_chrono(*bdt))
        .unwrap_or_else(|_| Utc::now())
}

/// Read an OPTIONAL `DateTime<Utc>` field: `None` when absent/null, `Some` when
/// a BSON datetime is present. This is the round-trip discipline the MSSQL bug
/// flagged — a missing field must parse back to `None`.
pub(super) fn get_opt_dt(doc: &Document, key: &str) -> Option<DateTime<Utc>> {
    match doc.get(key) {
        Some(Bson::DateTime(bdt)) => Some(bdt_to_chrono(*bdt)),
        _ => None,
    }
}

/// Read a string field, defaulting to empty.
pub(super) fn get_str(doc: &Document, key: &str) -> String {
    doc.get_str(key).unwrap_or_default().to_string()
}

/// Read an i32 field, defaulting to `0`. Tolerates an i64-encoded value.
pub(super) fn get_i32(doc: &Document, key: &str) -> i32 {
    doc.get_i32(key)
        .ok()
        .or_else(|| doc.get_i64(key).ok().map(|v| v as i32))
        .unwrap_or(0)
}

/// Parse a `Uuid` from a string `_id` (or any string-keyed uuid field).
pub(super) fn parse_uuid_id(doc: &Document, key: &str) -> SystemStoreResult<Uuid> {
    let raw = doc.get_str(key).map_err(|e| {
        SystemStoreError::InvalidInput(format!("mongodb row missing string '{key}': {e}"))
    })?;
    Uuid::parse_str(raw).map_err(|e| {
        SystemStoreError::InvalidInput(format!("mongodb row '{key}' is not a uuid '{raw}': {e}"))
    })
}

/// Read a JSON-shaped field (object/array) back into `serde_json::Value`.
/// Absent → `default`.
pub(super) fn get_json(doc: &Document, key: &str, default: serde_json::Value) -> serde_json::Value {
    match doc.get(key) {
        Some(b) => serde_json::to_value(b.clone()).unwrap_or(default),
        None => default,
    }
}

/// Encode a `serde_json::Value` as BSON for storage. JSON objects/arrays/scalars
/// all round-trip; on failure stores BSON null.
pub(super) fn json_to_bson(value: &serde_json::Value) -> Bson {
    bson::to_bson(value).unwrap_or(Bson::Null)
}

// ── Row mapping ───────────────────────────────────────────────────────────────

fn doc_to_projection_task(doc: &Document) -> SystemStoreResult<ProjectionTaskRow> {
    let task_id = parse_uuid_id(doc, "_id")?;
    let operation_str = get_str(doc, "operation");
    let operation = ProjectionOperation::parse(&operation_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection operation '{operation_str}' in mongodb row"
        ))
    })?;
    let status_str = get_str(doc, "status");
    let status = ProjectionTaskStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection status '{status_str}' in mongodb row"
        ))
    })?;
    Ok(ProjectionTaskRow {
        task_id,
        idempotency_key: get_str(doc, "idempotency_key"),
        project_id: get_str(doc, "project_id"),
        manifest_checksum: get_str(doc, "manifest_checksum"),
        target_backend: get_str(doc, "target_backend"),
        target_instance: get_str(doc, "target_instance"),
        projection_kind: get_str(doc, "projection_kind"),
        resource_name: get_str(doc, "resource_name"),
        operation,
        source_row_key: get_json(doc, "source_row_key", serde_json::Value::Null),
        target_options: get_json(doc, "target_options", serde_json::Value::Null),
        source_payload: get_json(doc, "source_payload", serde_json::Value::Null),
        source_checksum: get_str(doc, "source_checksum"),
        status,
        retry_count: get_i32(doc, "retry_count"),
        last_error: get_str(doc, "last_error"),
        created_at: get_dt(doc, "created_at"),
        updated_at: get_dt(doc, "updated_at"),
        next_retry_at: get_opt_dt(doc, "next_retry_at"),
        completed_at: get_opt_dt(doc, "completed_at"),
    })
}

impl MongoDbCanonicalStore {
    pub(super) fn projection(&self) -> Collection<Document> {
        self.db().collection::<Document>(PROJECTION_COLLECTION)
    }
}

#[async_trait]
impl ProjectionTaskStore for MongoDbCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mongodb"
    }

    async fn ensure_projection_tables(&self) -> SystemStoreResult<()> {
        match self.db().create_collection(PROJECTION_COLLECTION).await {
            Ok(_) => {}
            Err(err) if Self::is_namespace_exists(&err) => {}
            Err(err) => return Err(mongo_err("ensure_projection_tables create", err)),
        }
        // UNIQUE on idempotency_key mirrors the SQL UNIQUE constraint that
        // backs idempotent enqueue.
        let unique = IndexModel::builder()
            .keys(doc! { "idempotency_key": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build();
        // Helpful (non-unique) indexes matching the SQL secondary indexes.
        let by_status = IndexModel::builder()
            .keys(doc! { "status": 1, "created_at": 1 })
            .build();
        self.projection()
            .create_index(unique)
            .await
            .map_err(|e| mongo_err("ensure_projection_tables idempotency index", e))?;
        self.projection()
            .create_index(by_status)
            .await
            .map_err(|e| mongo_err("ensure_projection_tables status index", e))?;
        Ok(())
    }

    async fn enqueue_projection_task(
        &self,
        task: &ProjectionTaskInsert,
    ) -> SystemStoreResult<Uuid> {
        let now = now_bdt();
        let new_id = Uuid::new_v4().to_string();
        // Idempotent upsert keyed on idempotency_key: on FIRST insert
        // `$setOnInsert` materialises the full row with the freshly-minted
        // `_id`; on a repeat the existing doc is matched and only `updated_at`
        // touches. `ReturnDocument::After` yields the surviving `_id` either way
        // — exactly the Postgres CTE's returned-id semantics.
        let filter = doc! { "idempotency_key": &task.idempotency_key };
        let update = doc! {
            "$setOnInsert": {
                "_id": &new_id,
                "idempotency_key": &task.idempotency_key,
                "project_id": &task.project_id,
                "manifest_checksum": &task.manifest_checksum,
                "message_type": &task.message_type,
                "source_schema": &task.source_schema,
                "source_table": &task.source_table,
                "source_row_key": json_to_bson(&task.source_row_key),
                "operation": task.operation.as_str(),
                "target_backend": &task.target_backend,
                "target_instance": &task.target_instance,
                "projection_kind": &task.projection_kind,
                "resource_name": &task.resource_name,
                "target_options": json_to_bson(&task.target_options),
                "source_payload": json_to_bson(&task.source_payload),
                "source_checksum": &task.source_checksum,
                "status": ProjectionTaskStatus::Pending.as_str(),
                "retry_count": 0_i32,
                "last_error": "",
                "created_at": now,
            },
            "$set": { "updated_at": now },
        };
        let surviving = self
            .projection()
            .find_one_and_update(filter, update)
            .upsert(true)
            .return_document(ReturnDocument::After)
            .await
            .map_err(|e| mongo_err("enqueue_projection_task upsert", e))?
            .ok_or_else(|| mongo_err("enqueue_projection_task", "upsert returned no document"))?;
        parse_uuid_id(&surviving, "_id")
    }

    async fn claim_projection_tasks(
        &self,
        filter: &ProjectionClaimFilter,
    ) -> SystemStoreResult<Vec<ProjectionTaskRow>> {
        if filter.batch_size <= 0 {
            return Ok(Vec::new());
        }
        let now = now_bdt();
        // Common predicate fragments mirroring the PG candidate CTEs.
        let mut base = doc! { "retry_count": { "$lt": filter.max_retries } };
        if let Some(p) = &filter.project_id {
            base.insert("project_id", p);
        }
        if let Some(b) = &filter.target_backend {
            base.insert("target_backend", b);
        }
        if let Some(i) = &filter.target_instance {
            base.insert("target_instance", i);
        }
        // PENDING is always claimable; FAILED only once next_retry_at is due.
        let mut pending = base.clone();
        pending.insert("status", ProjectionTaskStatus::Pending.as_str());
        let mut failed = base.clone();
        failed.insert("status", ProjectionTaskStatus::Failed.as_str());
        failed.insert(
            "$or",
            bson::to_bson(&vec![
                doc! { "next_retry_at": { "$exists": false } },
                doc! { "next_retry_at": Bson::Null },
                doc! { "next_retry_at": { "$lte": now } },
            ])
            .unwrap_or(Bson::Array(vec![])),
        );
        let candidate_filter = doc! { "$or": [pending, failed] };

        // Atomic batch claim inside a session transaction: SELECT the
        // oldest-first candidates, then flip exactly those to IN_PROGRESS. The
        // transaction is the Mongo analogue of PG's `FOR UPDATE SKIP LOCKED` —
        // two concurrent workers committing the same id set conflict and one
        // retries, so no task is double-claimed.
        let client = self.db().client();
        let mut session = client
            .start_session()
            .await
            .map_err(|e| mongo_err("claim_projection_tasks start_session", e))?;
        session
            .start_transaction()
            .await
            .map_err(|e| mongo_err("claim_projection_tasks start_transaction", e))?;

        let claim_result: SystemStoreResult<Vec<ProjectionTaskRow>> = async {
            let mut cursor = self
                .projection()
                .find(candidate_filter)
                .sort(doc! { "created_at": 1 })
                .limit(filter.batch_size)
                .session(&mut session)
                .await
                .map_err(|e| mongo_err("claim_projection_tasks find", e))?;
            let mut candidates: Vec<Document> = Vec::new();
            while let Some(next) = cursor.next(&mut session).await {
                let d = next.map_err(|e| mongo_err("claim_projection_tasks cursor", e))?;
                candidates.push(d);
            }
            if candidates.is_empty() {
                return Ok(Vec::new());
            }
            let ids: Vec<Bson> = candidates
                .iter()
                .filter_map(|d| d.get_str("_id").ok().map(|s| Bson::String(s.to_string())))
                .collect();
            self.projection()
                .update_many(
                    doc! { "_id": { "$in": ids.clone() } },
                    doc! { "$set": {
                        "status": ProjectionTaskStatus::InProgress.as_str(),
                        "updated_at": now,
                    } },
                )
                .session(&mut session)
                .await
                .map_err(|e| mongo_err("claim_projection_tasks update_many", e))?;
            // Reflect the post-claim status in the returned rows (PG's UPDATE …
            // RETURNING shows IN_PROGRESS); the docs were read pre-flip.
            let mut out = Vec::with_capacity(candidates.len());
            for mut d in candidates {
                d.insert("status", ProjectionTaskStatus::InProgress.as_str());
                d.insert("updated_at", now);
                out.push(doc_to_projection_task(&d)?);
            }
            Ok(out)
        }
        .await;

        match claim_result {
            Ok(rows) => {
                session
                    .commit_transaction()
                    .await
                    .map_err(|e| mongo_err("claim_projection_tasks commit", e))?;
                Ok(rows)
            }
            Err(err) => {
                let _ = session.abort_transaction().await;
                Err(err)
            }
        }
    }

    async fn mark_projection_task_completed(&self, task_id: Uuid) -> SystemStoreResult<()> {
        let now = now_bdt();
        self.projection()
            .update_one(
                doc! { "_id": task_id.to_string() },
                doc! { "$set": {
                    "status": ProjectionTaskStatus::Completed.as_str(),
                    "completed_at": now,
                    "updated_at": now,
                }, "$unset": { "next_retry_at": "" } },
            )
            .await
            .map_err(|e| mongo_err("mark_projection_task_completed", e))?;
        Ok(())
    }

    async fn mark_projection_task_failed(
        &self,
        task_id: Uuid,
        new_retry_count: i32,
        new_status: ProjectionTaskStatus,
        error: &str,
    ) -> SystemStoreResult<()> {
        if !matches!(
            new_status,
            ProjectionTaskStatus::Failed | ProjectionTaskStatus::DeadLetter
        ) {
            return Err(SystemStoreError::InvalidInput(format!(
                "mark_projection_task_failed only accepts FAILED or DEAD_LETTER, got {}",
                new_status.as_str()
            )));
        }
        let now = now_bdt();
        self.projection()
            .update_one(
                doc! { "_id": task_id.to_string() },
                doc! { "$set": {
                    "status": new_status.as_str(),
                    "retry_count": new_retry_count,
                    "last_error": error,
                    "updated_at": now,
                }, "$unset": { "next_retry_at": "" } },
            )
            .await
            .map_err(|e| mongo_err("mark_projection_task_failed", e))?;
        Ok(())
    }

    async fn requeue_dead_letter_tasks(
        &self,
        target_backend: Option<&str>,
    ) -> SystemStoreResult<i64> {
        let now = now_bdt();
        let mut filter = doc! { "status": ProjectionTaskStatus::DeadLetter.as_str() };
        if let Some(b) = target_backend {
            filter.insert("target_backend", b);
        }
        let result = self
            .projection()
            .update_many(
                filter,
                doc! { "$set": {
                    "status": ProjectionTaskStatus::Pending.as_str(),
                    "retry_count": 0_i32,
                    "last_error": "",
                    "updated_at": now,
                }, "$unset": { "next_retry_at": "" } },
            )
            .await
            .map_err(|e| mongo_err("requeue_dead_letter_tasks", e))?;
        Ok(result.modified_count as i64)
    }

    async fn reset_stale_in_progress_tasks(&self, stale_after: Duration) -> SystemStoreResult<i64> {
        let cutoff = bson::DateTime::from_millis(
            (Utc::now() - chrono::Duration::from_std(stale_after).unwrap_or_default())
                .timestamp_millis(),
        );
        let now = now_bdt();
        let result = self
            .projection()
            .update_many(
                doc! {
                    "status": ProjectionTaskStatus::InProgress.as_str(),
                    "updated_at": { "$lt": cutoff },
                },
                doc! { "$set": {
                    "status": ProjectionTaskStatus::Pending.as_str(),
                    "last_error": "stale in-progress reconciliation",
                    "updated_at": now,
                } },
            )
            .await
            .map_err(|e| mongo_err("reset_stale_in_progress_tasks", e))?;
        Ok(result.modified_count as i64)
    }

    async fn pending_task_metrics(&self, limit: i64) -> SystemStoreResult<Vec<PendingTaskMetric>> {
        // Aggregate PENDING+FAILED counts + oldest created_at per group; the age
        // (NOW - oldest) is computed in Rust to mirror PG's EXTRACT(EPOCH …).
        let pipeline = vec![
            doc! { "$match": { "status": { "$in": [
                ProjectionTaskStatus::Pending.as_str(),
                ProjectionTaskStatus::Failed.as_str(),
            ] } } },
            doc! { "$group": {
                "_id": {
                    "project_id": "$project_id",
                    "target_backend": "$target_backend",
                    "target_instance": "$target_instance",
                    "projection_kind": "$projection_kind",
                },
                "pending": { "$sum": 1_i64 },
                "oldest": { "$min": "$created_at" },
            } },
            doc! { "$limit": limit.max(1) },
        ];
        let mut cursor = self
            .projection()
            .aggregate(pipeline)
            .await
            .map_err(|e| mongo_err("pending_task_metrics aggregate", e))?;
        let now = Utc::now();
        let mut out = Vec::new();
        while let Some(d) = cursor
            .try_next()
            .await
            .map_err(|e| mongo_err("pending_task_metrics cursor", e))?
        {
            let group = d.get_document("_id").cloned().unwrap_or_default();
            let oldest = get_opt_dt(&d, "oldest").unwrap_or(now);
            let oldest_age_seconds = (now - oldest).num_milliseconds() as f64 / 1000.0;
            out.push(PendingTaskMetric {
                project_id: get_str(&group, "project_id"),
                target_backend: get_str(&group, "target_backend"),
                target_instance: get_str(&group, "target_instance"),
                projection_kind: get_str(&group, "projection_kind"),
                pending: d.get_i64("pending").unwrap_or(0),
                oldest_age_seconds: oldest_age_seconds.max(0.0),
            });
        }
        Ok(out)
    }

    async fn dead_letter_groups(&self, limit: i64) -> SystemStoreResult<Vec<DeadLetterGroup>> {
        let pipeline = vec![
            doc! { "$match": {
                "status": ProjectionTaskStatus::DeadLetter.as_str(),
                "last_error": { "$not": { "$regex": "^projection authority rejected:" } },
            } },
            doc! { "$group": {
                "_id": {
                    "project_id": "$project_id",
                    "source_table": "$source_table",
                    "target_backend": "$target_backend",
                    "target_instance": "$target_instance",
                },
                "dead_count": { "$sum": 1_i64 },
            } },
            doc! { "$limit": limit.max(1) },
        ];
        let mut cursor = self
            .projection()
            .aggregate(pipeline)
            .await
            .map_err(|e| mongo_err("dead_letter_groups aggregate", e))?;
        let mut out = Vec::new();
        while let Some(d) = cursor
            .try_next()
            .await
            .map_err(|e| mongo_err("dead_letter_groups cursor", e))?
        {
            let group = d.get_document("_id").cloned().unwrap_or_default();
            out.push(DeadLetterGroup {
                project_id: get_str(&group, "project_id"),
                source_table: get_str(&group, "source_table"),
                target_backend: get_str(&group, "target_backend"),
                target_instance: get_str(&group, "target_instance"),
                dead_count: d.get_i64("dead_count").unwrap_or(0),
            });
        }
        Ok(out)
    }

    async fn requeue_dead_letter_by_source(
        &self,
        project_id: &str,
        source_table: &str,
        target_backend: &str,
        target_instance: &str,
    ) -> SystemStoreResult<i64> {
        let now = now_bdt();
        let result = self
            .projection()
            .update_many(
                doc! {
                    "status": ProjectionTaskStatus::DeadLetter.as_str(),
                    "last_error": { "$not": { "$regex": "^projection authority rejected:" } },
                    "project_id": project_id,
                    "source_table": source_table,
                    "target_backend": target_backend,
                    "target_instance": target_instance,
                },
                doc! { "$set": {
                    "status": ProjectionTaskStatus::Pending.as_str(),
                    "retry_count": 0_i32,
                    "last_error": "reconciliation repair",
                    "updated_at": now,
                } },
            )
            .await
            .map_err(|e| mongo_err("requeue_dead_letter_by_source", e))?;
        Ok(result.modified_count as i64)
    }

    async fn pending_projection_task_count(
        &self,
        idempotency_keys: &[String],
    ) -> SystemStoreResult<i64> {
        if idempotency_keys.is_empty() {
            return Ok(0);
        }
        let keys: Vec<Bson> = idempotency_keys
            .iter()
            .map(|k| Bson::String(k.clone()))
            .collect();
        let n = self
            .projection()
            .count_documents(doc! {
                "idempotency_key": { "$in": keys },
                // P2-1 NF-1/NF-2: only COMPLETED clears the fence; FAILED/DEAD_LETTER
                // are not projected yet so they still count as pending.
                "status": { "$ne": ProjectionTaskStatus::Completed.as_str() },
            })
            .await
            .map_err(|e| mongo_err("pending_projection_task_count", e))?;
        Ok(n as i64)
    }

    async fn projection_task_summary(&self) -> SystemStoreResult<ProjectionTaskSummary> {
        let pipeline = vec![doc! { "$group": { "_id": "$status", "n": { "$sum": 1_i64 } } }];
        let mut cursor = self
            .projection()
            .aggregate(pipeline)
            .await
            .map_err(|e| mongo_err("projection_task_summary aggregate", e))?;
        let mut s = ProjectionTaskSummary::default();
        while let Some(d) = cursor
            .try_next()
            .await
            .map_err(|e| mongo_err("projection_task_summary cursor", e))?
        {
            let status = d.get_str("_id").unwrap_or_default();
            let n = d.get_i64("n").unwrap_or(0);
            match ProjectionTaskStatus::parse(status) {
                Some(ProjectionTaskStatus::Pending) => s.pending = n,
                Some(ProjectionTaskStatus::InProgress) => s.in_progress = n,
                Some(ProjectionTaskStatus::Completed) => s.completed = n,
                Some(ProjectionTaskStatus::Failed) => s.failed = n,
                Some(ProjectionTaskStatus::DeadLetter) => s.dead_letter = n,
                None => {
                    return Err(SystemStoreError::InvalidInput(format!(
                        "unknown projection status '{status}' in mongodb summary"
                    )));
                }
            }
        }
        Ok(s)
    }
}
