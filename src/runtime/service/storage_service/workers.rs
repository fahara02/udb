//! The periodic orphan reaper for the native `StorageService`: the bounded,
//! oldest-first hard-delete of `PENDING` files that were registered but never
//! finalized, plus their abandoned object bytes. The batch delete re-asserts the
//! orphan predicate and uses RETURNING, so a file finalized mid-sweep is never
//! reaped. Spawned under leader election by `build_storage_service`.

use tonic::Status;

use crate::ir::{
    ComparisonOp, LogicalDelete, LogicalFilter, LogicalPagination, LogicalRead, LogicalSort,
    LogicalValue, NullOrder, SortDirection,
};
use crate::proto::udb::core::storage::entity::v1 as storage_entity_pb;

use super::StorageServiceImpl;
use super::config::FILE_MSG;
use super::model::file_from_json;
use super::store::{file_eq, file_projection, logical_string};

/// The orphan reaper's DELETE for a previously-read batch: by primary key AND
/// still `status = 'PENDING'` AND still older than `cutoff`, returning the ids of
/// the rows it actually removed. Re-asserting the orphan predicate in the delete
/// itself is what makes the read-then-delete safe against a concurrent finalize
/// (a file that became ACTIVE in between no longer matches and survives).
pub(crate) fn orphan_reap_delete<'a>(
    file_ids: impl IntoIterator<Item = &'a str>,
    cutoff: chrono::DateTime<chrono::Utc>,
) -> LogicalDelete {
    LogicalDelete {
        message_type: FILE_MSG.to_string(),
        filter: LogicalFilter::And(vec![
            LogicalFilter::InList {
                field: "file_id".to_string(),
                values: file_ids.into_iter().map(logical_string).collect(),
            },
            file_eq("status", "PENDING"),
            LogicalFilter::Comparison {
                field: "created_at".to_string(),
                op: ComparisonOp::Lt,
                value: LogicalValue::Timestamp(cutoff),
            },
        ]),
        return_fields: vec!["file_id".to_string()],
    }
}

impl StorageServiceImpl {
    /// Hard-DELETE orphaned `PENDING` files older than `older_than_minutes`
    /// (uploads that were registered but never finalized) and remove their
    /// abandoned object bytes. Uses the auto-injected `created_at` audit column
    /// (`audit_fields: true` on the File table). Returns the number deleted.
    ///
    /// Fully on the typed path (no raw SQL). This is a CROSS-TENANT maintenance
    /// sweep, so it runs under a SYSTEM context (empty tenant) and supplies NO
    /// tenant filter — it reaps every tenant's orphans. That is sound because the
    /// broker's platform-admin pool bypasses the File table's `force_rls`
    /// (per-tenant isolation for normal RPCs comes from each handler's tenant
    /// filter, which this maintenance path deliberately omits). The reap is bounded
    /// oldest-first via a `LogicalRead` (`ORDER BY created_at … LIMIT`), then exactly
    /// that batch is hard-deleted by primary key (`file_id IN (…)`). The cutoff is
    /// computed in Rust and bound as a timestamp, so no backend-specific `INTERVAL`
    /// arithmetic leaks into the neutral IR.
    pub(crate) async fn reap_orphans(
        &self,
        older_than_minutes: i64,
        batch_size: i64,
    ) -> Result<u64, Status> {
        let runtime = self.require_runtime()?;
        let batch_size = batch_size.clamp(1, 10_000);
        let context = crate::RequestContext {
            correlation_id: "storage-orphan-reaper".to_string(),
            ..crate::RequestContext::default()
        };
        let cutoff = chrono::Utc::now() - chrono::Duration::minutes(older_than_minutes.max(0));
        // 1) Bounded, oldest-first batch of PENDING orphans across all tenants.
        let read = LogicalRead {
            message_type: FILE_MSG.to_string(),
            filter: Some(LogicalFilter::And(vec![
                file_eq("status", "PENDING"),
                LogicalFilter::Comparison {
                    field: "created_at".to_string(),
                    op: ComparisonOp::Lt,
                    value: LogicalValue::Timestamp(cutoff),
                },
            ])),
            projection: Some(file_projection()),
            sort: vec![LogicalSort {
                field: "created_at".to_string(),
                direction: SortDirection::Asc,
                nulls: NullOrder::Default,
            }],
            include: Vec::new(),
            pagination: Some(LogicalPagination::limit(batch_size as u32)),
        };
        let doomed: Vec<storage_entity_pb::File> = runtime
            .native_entity_read_for_service("storage", &context, read)
            .await?
            .iter()
            .map(file_from_json)
            .collect();
        if doomed.is_empty() {
            return Ok(0);
        }
        // 2) Hard-DELETE that batch by primary key, RE-ASSERTING the orphan
        //    predicate (`status = 'PENDING'` AND older than the cutoff) in the SAME
        //    statement. Between the read above and this delete a client may finalize
        //    one of these files (PENDING -> ACTIVE); a bare `file_id IN (...)`
        //    delete would then hard-delete a live, just-finalized file and its
        //    bytes. RETURNING tells us exactly which rows were still orphans.
        let deleted_rows = runtime
            .native_entity_delete_rows_for_service(
                "storage",
                &context,
                orphan_reap_delete(doomed.iter().map(|f| f.file_id.as_str()), cutoff),
            )
            .await?;
        let deleted_ids: std::collections::HashSet<String> = deleted_rows
            .iter()
            .filter_map(|row| row.get("file_id").and_then(serde_json::Value::as_str))
            .map(|id| id.to_ascii_lowercase())
            .collect();
        // 3) Remove the object bytes ONLY for rows this statement actually deleted.
        //    A failed byte delete records a durable GC intent (the sweep converges
        //    it) instead of being logged and forgotten.
        let mut reaped = 0u64;
        for file in doomed
            .iter()
            .filter(|f| deleted_ids.contains(&f.file_id.to_ascii_lowercase()))
        {
            reaped += 1;
            let (backend, bucket) = self.file_object_location(file);
            if let Err(err) = self
                .try_delete_object_bytes(
                    &backend,
                    &bucket,
                    &file.tenant_id,
                    &file.project_id,
                    &file.object_key,
                )
                .await
            {
                if let Err(intent_err) = self
                    .insert_gc_intent(
                        &file.tenant_id,
                        &file.file_id,
                        &file.project_id,
                        &backend,
                        &bucket,
                        &file.object_key,
                        "REAP",
                        "orphaned pending upload",
                        err.message(),
                    )
                    .await
                {
                    tracing::warn!(
                        error = %err,
                        intent_error = %intent_err,
                        file_id = %file.file_id,
                        "storage orphan reaper: byte delete failed and no GC intent could be recorded; bytes orphaned"
                    );
                }
            }
        }
        Ok(reaped)
    }

    /// Drive PENDING durable object-GC intents (recorded by HARD `DeleteFile`) to
    /// convergence: for each intent in a bounded oldest-first batch, re-attempt the
    /// object-byte delete (idempotent) and either mark the intent DONE (immutable
    /// success outcome) or record the failed attempt — dead-lettering it
    /// (`status = 'FAILED'`) once the attempt cap is hit. Returns the number of
    /// intents converged (marked DONE) on this pass.
    ///
    /// Cross-tenant by design (the broker's own GC maintenance, same posture as the
    /// orphan reaper): each intent carries its tenant's project/backend/bucket, so a
    /// delete is dispatched against the object the intent recorded. Spawned under a
    /// dedicated leader-election lease (`WORKER_STORAGE_GC_SWEEP`) by
    /// `build_storage_service`.
    pub(crate) async fn sweep_gc_intents(&self, batch_size: i64) -> Result<u64, Status> {
        // Ensure the ledger exists even if no HARD delete has run on this leader yet.
        self.ensure_gc_intents_table().await?;
        let max_attempts = Self::gc_max_attempts();
        let pending = self.select_pending_gc_intents(batch_size).await?;
        let mut converged = 0u64;
        for intent in &pending {
            match self
                .try_delete_object_bytes(
                    &intent.backend,
                    &intent.bucket,
                    &intent.tenant_id,
                    &intent.project_id,
                    &intent.object_key,
                )
                .await
            {
                Ok(()) => {
                    self.mark_gc_intent_done(&intent.intent_id).await?;
                    converged += 1;
                }
                Err(err) => {
                    self.record_gc_intent_failure(&intent.intent_id, err.message(), max_attempts)
                        .await?;
                }
            }
        }
        Ok(converged)
    }
}
