//! MongoDB implementation of [`AdminAuditStore`] (B.9 phase 2).
//!
//! Each audit row is one BSON document keyed by `_id = <audit_id uuid as
//! string>`. Semantics mirror the Postgres impl (`postgres_admin_audit.rs`)
//! exactly so the cross-backend conformance contract passes byte-for-byte:
//!
//! - The hash chain is computed in Rust via the SHARED
//!   [`compute_admin_audit_hash`] / [`verify_admin_audit_chain_step`] helpers,
//!   so PG / MySQL / SQLite / MSSQL / MongoDB chains are byte-identical.
//! - `created_at` stored as `bson::DateTime` (millisecond ordering); rows are
//!   ordered `(created_at, _id)` for both verify and latest-hash lookups.
//!
//! ## Atomicity
//!
//! `append_admin_audit` serializes concurrent appenders on a single lock
//! document (`{_id:"lock"}` in `ADMIN_AUDIT_LOCK_COLLECTION`) which is upserted
//! INSIDE the append transaction — the Mongo analogue of PG's
//! `pg_advisory_xact_lock`. Two appends issuing concurrently against the same
//! chain conflict on that document write; the second retries, re-reads the new
//! `current_hash`, and links correctly. `verify_admin_audit_chain` streams the
//! chain one document at a time (never collecting the whole collection).

use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb_driver::bson::{Document, doc};
use mongodb_driver::options::IndexOptions;
use mongodb_driver::{Collection, IndexModel};
use uuid::Uuid;

use super::mongodb::{ADMIN_AUDIT_COLLECTION, ADMIN_AUDIT_LOCK_COLLECTION, MongoDbCanonicalStore};
use super::mongodb_projection::{
    get_dt, get_json, get_str, json_to_bson, mongo_err, now_bdt, parse_uuid_id,
};
use super::system_store::{
    AdminAuditChainReport, AdminAuditInsert, AdminAuditListFilter, AdminAuditRow, AdminAuditStore,
    SystemStoreError, SystemStoreResult, compute_admin_audit_hash, verify_admin_audit_chain_step,
};

/// The single lock document `_id` held during an append transaction.
const ADMIN_AUDIT_LOCK_ID: &str = "lock";

// ── Row mapping ───────────────────────────────────────────────────────────────

fn doc_to_audit(doc: &Document) -> SystemStoreResult<AdminAuditRow> {
    let audit_id = parse_uuid_id(doc, "_id")?;
    Ok(AdminAuditRow {
        audit_id,
        actor: get_str(doc, "actor"),
        operation: get_str(doc, "operation"),
        target: get_str(doc, "target"),
        request_json: get_json(doc, "request_json", serde_json::Value::Null),
        result: get_str(doc, "result"),
        tenant_id: get_str(doc, "tenant_id"),
        project_id: get_str(doc, "project_id"),
        correlation_id: get_str(doc, "correlation_id"),
        previous_hash: get_str(doc, "previous_hash"),
        current_hash: get_str(doc, "current_hash"),
        signer_key_id: get_str(doc, "signer_key_id"),
        external_anchor: get_str(doc, "external_anchor"),
        created_at: get_dt(doc, "created_at"),
    })
}

impl MongoDbCanonicalStore {
    pub(super) fn admin_audit(&self) -> Collection<Document> {
        self.db().collection::<Document>(ADMIN_AUDIT_COLLECTION)
    }

    fn admin_audit_lock(&self) -> Collection<Document> {
        self.db()
            .collection::<Document>(ADMIN_AUDIT_LOCK_COLLECTION)
    }
}

#[async_trait]
impl AdminAuditStore for MongoDbCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mongodb"
    }

    async fn ensure_admin_audit_tables(&self) -> SystemStoreResult<()> {
        for name in [ADMIN_AUDIT_COLLECTION, ADMIN_AUDIT_LOCK_COLLECTION] {
            match self.db().create_collection(name).await {
                Ok(_) => {}
                Err(err) if Self::is_namespace_exists(&err) => {}
                Err(err) => return Err(mongo_err("ensure_admin_audit_tables create", err)),
            }
        }
        // Mirror the PG `(operation, created_at DESC)` index.
        let by_created = IndexModel::builder()
            .keys(doc! { "operation": 1, "created_at": -1 })
            .options(IndexOptions::builder().build())
            .build();
        self.admin_audit()
            .create_index(by_created)
            .await
            .map_err(|e| mongo_err("ensure_admin_audit_tables index", e))?;
        Ok(())
    }

    async fn latest_admin_audit_hash(&self) -> SystemStoreResult<String> {
        let row = self
            .admin_audit()
            .find_one(doc! { "current_hash": { "$ne": "" } })
            .sort(doc! { "created_at": -1, "_id": -1 })
            .await
            .map_err(|e| mongo_err("latest_admin_audit_hash", e))?;
        Ok(row.map(|d| get_str(&d, "current_hash")).unwrap_or_default())
    }

    async fn append_admin_audit(&self, entry: &AdminAuditInsert) -> SystemStoreResult<Uuid> {
        let audit_id = Uuid::new_v4();
        let client = self.db().client();
        let mut session = client
            .start_session()
            .await
            .map_err(|e| mongo_err("append_admin_audit start_session", e))?;
        session
            .start_transaction()
            .await
            .map_err(|e| mongo_err("append_admin_audit start_transaction", e))?;

        let append_result: SystemStoreResult<()> = async {
            // 1. Acquire the chain lock by upserting the single `{_id:"lock"}`
            //    document inside the transaction. Concurrent appenders contend
            //    on this write; the loser retries and re-reads the new hash.
            self.admin_audit_lock()
                .update_one(
                    doc! { "_id": ADMIN_AUDIT_LOCK_ID },
                    doc! { "$set": { "held_at": now_bdt() } },
                )
                .upsert(true)
                .session(&mut session)
                .await
                .map_err(|e| mongo_err("append_admin_audit lock", e))?;

            // 2. Read the latest non-empty current_hash under the lock.
            let previous_hash = self
                .admin_audit()
                .find_one(doc! { "current_hash": { "$ne": "" } })
                .sort(doc! { "created_at": -1, "_id": -1 })
                .session(&mut session)
                .await
                .map_err(|e| mongo_err("append_admin_audit latest", e))?
                .map(|d| get_str(&d, "current_hash"))
                .unwrap_or_default();

            // 3. Compute the new row's current_hash via the SHARED helper.
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

            // 4. Insert the row carrying the assigned UUID.
            let row = doc! {
                "_id": audit_id.to_string(),
                "actor": &entry.actor,
                "operation": &entry.operation,
                "target": &entry.target,
                "request_json": json_to_bson(&entry.request_json),
                "result": &entry.result,
                "tenant_id": &entry.tenant_id,
                "project_id": &entry.project_id,
                "correlation_id": &entry.correlation_id,
                "previous_hash": &previous_hash,
                "current_hash": &current_hash,
                "signer_key_id": &entry.signer_key_id,
                "external_anchor": &entry.external_anchor,
                "created_at": now_bdt(),
            };
            self.admin_audit()
                .insert_one(row)
                .session(&mut session)
                .await
                .map_err(|e| mongo_err("append_admin_audit insert", e))?;
            Ok(())
        }
        .await;

        match append_result {
            Ok(()) => {
                session
                    .commit_transaction()
                    .await
                    .map_err(|e| mongo_err("append_admin_audit commit", e))?;
                Ok(audit_id)
            }
            Err(err) => {
                let _ = session.abort_transaction().await;
                Err(err)
            }
        }
    }

    async fn list_admin_audit(
        &self,
        filter: &AdminAuditListFilter,
    ) -> SystemStoreResult<Vec<AdminAuditRow>> {
        let mut query = Document::new();
        if let Some(op) = &filter.operation {
            query.insert("operation", op);
        }
        if let Some(actor) = &filter.actor {
            query.insert("actor", actor);
        }
        if let Some(t) = &filter.tenant_id {
            query.insert("tenant_id", t);
        }
        if let Some(p) = &filter.project_id {
            query.insert("project_id", p);
        }
        let limit = if filter.limit <= 0 { 100 } else { filter.limit };
        let skip = filter.offset.max(0) as u64;
        let mut cursor = self
            .admin_audit()
            .find(query)
            .sort(doc! { "created_at": -1 })
            .skip(skip)
            .limit(limit)
            .await
            .map_err(|e| mongo_err("list_admin_audit find", e))?;
        let mut out = Vec::new();
        while let Some(d) = cursor
            .try_next()
            .await
            .map_err(|e| mongo_err("list_admin_audit cursor", e))?
        {
            let mut row = doc_to_audit(&d)?;
            if filter.redact_request_json {
                row.request_json = serde_json::json!({"redacted": true});
            }
            out.push(row);
        }
        Ok(out)
    }

    async fn verify_admin_audit_chain(
        &self,
        limit: Option<i64>,
    ) -> SystemStoreResult<AdminAuditChainReport> {
        // Stream the chain oldest-to-newest one document at a time, feeding each
        // row to the SHARED verify step. We never collect the whole collection —
        // the cursor pulls documents lazily so a multi-million-row chain
        // verifies in constant memory.
        let mut cursor = self
            .admin_audit()
            .find(doc! {})
            .sort(doc! { "created_at": 1, "_id": 1 })
            .await
            .map_err(|e| mongo_err("verify_admin_audit_chain find", e))?;
        let mut previous_hash = String::new();
        let mut checked: i64 = 0;
        let max = match limit {
            Some(n) if n > 0 => Some(n),
            _ => None,
        };
        while let Some(d) = cursor
            .try_next()
            .await
            .map_err(|e| mongo_err("verify_admin_audit_chain cursor", e))?
        {
            if let Some(n) = max {
                if checked >= n {
                    break;
                }
            }
            let row = doc_to_audit(&d)?;
            match verify_admin_audit_chain_step(&row, &previous_hash, checked) {
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
