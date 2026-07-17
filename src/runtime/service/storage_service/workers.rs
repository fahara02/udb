//! The periodic orphan reaper for the native `StorageService`: the bounded,
//! oldest-first hard-delete of `PENDING` files that were registered but never
//! finalized, plus their abandoned object bytes. Extracted verbatim from the
//! former god file — the cross-tenant system-context sweep and the by-primary-key
//! batch delete are byte-for-byte identical. Spawned under leader election by
//! `build_storage_service`.

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
        // 2) Hard-DELETE exactly that batch by primary key (UUID `file_id`).
        let delete = LogicalDelete {
            message_type: FILE_MSG.to_string(),
            filter: LogicalFilter::InList {
                field: "file_id".to_string(),
                values: doomed
                    .iter()
                    .map(|f| logical_string(f.file_id.as_str()))
                    .collect(),
            },
            return_fields: Vec::new(),
        };
        runtime
            .native_entity_delete_for_service("storage", &context, delete)
            .await?;
        // 3) Best-effort: remove the now-orphaned object bytes too.
        for file in &doomed {
            self.delete_object_bytes(&file.project_id, &file.object_key)
                .await;
        }
        Ok(doomed.len() as u64)
    }
}
