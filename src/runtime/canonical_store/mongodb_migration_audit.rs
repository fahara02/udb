//! MongoDB implementation of [`MigrationAuditStore`] (B.9 phase 2).
//!
//! Two collections mirror the PG `udb_migration_runs` + `udb_migration_op_ledger`
//! tables. Semantics mirror the Postgres impl (`postgres_migration_audit.rs`)
//! exactly so the cross-backend conformance contract passes byte-for-byte:
//!
//! - Run rows keyed by `_id = <run_id uuid as string>`.
//! - Op-ledger rows carry a monotone `id` materialised from a counter document
//!   (`{_id: MIGRATION_OP_SEQ_ID, seq}`) — the Mongo analogue of PG's
//!   `BIGSERIAL`.
//! - `applied_at` is set only when the op status is `APPLIED`; `finished_at`
//!   is set on `finish_migration_run`. Both are OPTIONAL timestamps that
//!   round-trip `None`-when-absent (the bug class the MSSQL impl flagged).
//! - State / status values stored as their canonical Postgres strings.

use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb_driver::bson::{Document, doc};
use mongodb_driver::options::IndexOptions;
use mongodb_driver::options::ReturnDocument;
use mongodb_driver::{Collection, IndexModel};
use uuid::Uuid;

use super::mongodb::{
    MIGRATION_LEDGER_COLLECTION, MIGRATION_OP_SEQ_ID, MIGRATION_RUNS_COLLECTION,
    MongoDbCanonicalStore,
};
use super::mongodb_projection::{
    get_dt, get_i32, get_json, get_opt_dt, get_str, json_to_bson, mongo_err, now_bdt, parse_uuid_id,
};
use super::system_store::{
    MigrationAuditStore, MigrationOpInsert, MigrationOpRow, MigrationRunInsert, MigrationRunRow,
    MigrationRunState, MigrationRunsFilter, OpLedgerStatus, SystemStoreError, SystemStoreResult,
};

// ── Row mapping ───────────────────────────────────────────────────────────────

fn doc_to_run(doc: &Document) -> SystemStoreResult<MigrationRunRow> {
    let run_id = parse_uuid_id(doc, "_id")?;
    let state_str = get_str(doc, "state");
    let state = MigrationRunState::parse(&state_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown migration run state '{state_str}' in mongodb row"
        ))
    })?;
    Ok(MigrationRunRow {
        run_id,
        project_id: get_str(doc, "project_id"),
        catalog_version: get_str(doc, "catalog_version"),
        state,
        operations_hash: get_str(doc, "operations_hash"),
        approval_token: get_str(doc, "approval_token"),
        started_at: get_dt(doc, "started_at"),
        finished_at: get_opt_dt(doc, "finished_at"),
        error: get_str(doc, "error"),
    })
}

fn doc_to_op(doc: &Document) -> SystemStoreResult<MigrationOpRow> {
    let run_id = parse_uuid_id(doc, "run_id")?;
    let status_str = get_str(doc, "status");
    let status = OpLedgerStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown op ledger status '{status_str}' in mongodb row"
        ))
    })?;
    Ok(MigrationOpRow {
        id: doc.get_i64("id").unwrap_or(0),
        run_id,
        operation_index: get_i32(doc, "operation_index"),
        backend: get_str(doc, "backend"),
        resource_uri: get_str(doc, "resource_uri"),
        operation_kind: get_str(doc, "operation_kind"),
        status,
        rollback_json: get_json(
            doc,
            "rollback_json",
            serde_json::Value::Object(Default::default()),
        ),
        error: get_str(doc, "error"),
        applied_at: get_opt_dt(doc, "applied_at"),
    })
}

impl MongoDbCanonicalStore {
    pub(super) fn migration_runs(&self) -> Collection<Document> {
        self.db().collection::<Document>(MIGRATION_RUNS_COLLECTION)
    }

    pub(super) fn migration_ledger(&self) -> Collection<Document> {
        self.db()
            .collection::<Document>(MIGRATION_LEDGER_COLLECTION)
    }
}

#[async_trait]
impl MigrationAuditStore for MongoDbCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mongodb"
    }

    async fn ensure_migration_audit_tables(&self) -> SystemStoreResult<()> {
        for name in [MIGRATION_RUNS_COLLECTION, MIGRATION_LEDGER_COLLECTION] {
            match self.db().create_collection(name).await {
                Ok(_) => {}
                Err(err) if Self::is_namespace_exists(&err) => {}
                Err(err) => return Err(mongo_err("ensure_migration_audit_tables create", err)),
            }
        }
        // Mirror the PG `(project_id, state, started_at DESC)` runs index and the
        // `(run_id, operation_index)` ledger index.
        let runs_idx = IndexModel::builder()
            .keys(doc! { "project_id": 1, "state": 1, "started_at": -1 })
            .options(IndexOptions::builder().build())
            .build();
        self.migration_runs()
            .create_index(runs_idx)
            .await
            .map_err(|e| mongo_err("ensure_migration_audit_tables runs index", e))?;
        let ledger_idx = IndexModel::builder()
            .keys(doc! { "run_id": 1, "operation_index": 1 })
            .options(IndexOptions::builder().build())
            .build();
        self.migration_ledger()
            .create_index(ledger_idx)
            .await
            .map_err(|e| mongo_err("ensure_migration_audit_tables ledger index", e))?;
        Ok(())
    }

    async fn start_migration_run(&self, run: &MigrationRunInsert) -> SystemStoreResult<Uuid> {
        let run_id = Uuid::new_v4();
        let row = doc! {
            "_id": run_id.to_string(),
            "project_id": &run.project_id,
            "catalog_version": &run.catalog_version,
            "state": run.state.as_str(),
            "operations_hash": &run.operations_hash,
            "approval_token": &run.approval_token,
            "started_at": now_bdt(),
            "error": "",
            // finished_at deliberately absent → round-trips as None.
        };
        self.migration_runs()
            .insert_one(row)
            .await
            .map_err(|e| mongo_err("start_migration_run insert", e))?;
        Ok(run_id)
    }

    async fn record_migration_op(&self, op: &MigrationOpInsert) -> SystemStoreResult<i64> {
        // Materialise the monotone ledger id from a counter doc — PG's BIGSERIAL.
        let counter = self
            .migration_ledger()
            .find_one_and_update(
                doc! { "_id": MIGRATION_OP_SEQ_ID },
                doc! { "$inc": { "seq": 1_i64 } },
            )
            .upsert(true)
            .return_document(ReturnDocument::After)
            .await
            .map_err(|e| mongo_err("record_migration_op counter", e))?
            .ok_or_else(|| mongo_err("record_migration_op", "counter returned no document"))?;
        let id = counter.get_i64("seq").unwrap_or(0);

        // applied_at is set when the status is APPLIED (PG's CASE), else absent
        // so it round-trips as None.
        let mut row = doc! {
            "id": id,
            "run_id": op.run_id.to_string(),
            "operation_index": op.operation_index,
            "backend": &op.backend,
            "resource_uri": &op.resource_uri,
            "operation_kind": &op.operation_kind,
            "status": op.status.as_str(),
            "rollback_json": json_to_bson(&op.rollback_json),
            "error": &op.error,
        };
        if op.status == OpLedgerStatus::Applied {
            row.insert("applied_at", now_bdt());
        }
        self.migration_ledger()
            .insert_one(row)
            .await
            .map_err(|e| mongo_err("record_migration_op insert", e))?;
        Ok(id)
    }

    async fn finish_migration_run(
        &self,
        run_id: Uuid,
        new_state: MigrationRunState,
        error: &str,
    ) -> SystemStoreResult<()> {
        // finished_at MUST persist + round-trip as Some.
        let result = self
            .migration_runs()
            .update_one(
                doc! { "_id": run_id.to_string() },
                doc! { "$set": {
                    "state": new_state.as_str(),
                    "error": error,
                    "finished_at": now_bdt(),
                } },
            )
            .await
            .map_err(|e| mongo_err("finish_migration_run", e))?;
        if result.matched_count == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "migration run {run_id} not found for finish_migration_run"
            )));
        }
        Ok(())
    }

    async fn get_migration_run(&self, run_id: Uuid) -> SystemStoreResult<Option<MigrationRunRow>> {
        let row = self
            .migration_runs()
            .find_one(doc! { "_id": run_id.to_string() })
            .await
            .map_err(|e| mongo_err("get_migration_run", e))?;
        match row {
            Some(d) => Ok(Some(doc_to_run(&d)?)),
            None => Ok(None),
        }
    }

    async fn list_migration_ops(&self, run_id: Uuid) -> SystemStoreResult<Vec<MigrationOpRow>> {
        let mut cursor = self
            .migration_ledger()
            .find(doc! { "run_id": run_id.to_string() })
            .sort(doc! { "operation_index": 1 })
            .await
            .map_err(|e| mongo_err("list_migration_ops find", e))?;
        let mut out = Vec::new();
        while let Some(d) = cursor
            .try_next()
            .await
            .map_err(|e| mongo_err("list_migration_ops cursor", e))?
        {
            out.push(doc_to_op(&d)?);
        }
        Ok(out)
    }

    async fn list_migration_runs(
        &self,
        filter: &MigrationRunsFilter,
    ) -> SystemStoreResult<Vec<MigrationRunRow>> {
        let mut query = Document::new();
        if let Some(p) = &filter.project_id {
            query.insert("project_id", p);
        }
        if let Some(s) = filter.state {
            query.insert("state", s.as_str());
        }
        if let Some(v) = &filter.catalog_version {
            query.insert("catalog_version", v);
        }
        let limit = if filter.limit <= 0 { 100 } else { filter.limit };
        let skip = filter.offset.max(0) as u64;
        let mut cursor = self
            .migration_runs()
            .find(query)
            .sort(doc! { "started_at": -1 })
            .skip(skip)
            .limit(limit)
            .await
            .map_err(|e| mongo_err("list_migration_runs find", e))?;
        let mut out = Vec::new();
        while let Some(d) = cursor
            .try_next()
            .await
            .map_err(|e| mongo_err("list_migration_runs cursor", e))?
        {
            out.push(doc_to_run(&d)?);
        }
        Ok(out)
    }
}
