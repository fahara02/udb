//! `MongoDbCanonicalStore` — the MongoDB implementation of the
//! [`CanonicalStore`](super::CanonicalStore) base contract (B.9 phase 1).
//!
//! Unlike the SQL canonical stores, MongoDB has no server-side auto-increment,
//! so the monotone outbox sequence is materialised as a single counter
//! document (`{_id: "outbox_seq", seq: <i64>}`) inside the outbox collection.
//! Every `enqueue_outbox_event` first bumps that counter atomically
//! (`findOneAndUpdate({_id:"outbox_seq"}, {$inc:{seq:1}}, upsert, After)`) and
//! then inserts the event document carrying the assigned `event_seq`. The
//! durability token is the counter's current `seq` rendered as a decimal
//! string; `wait_for_token` polls the counter until it reaches the target.
//!
//! Advisory leases live in `udb_advisory_leases`, keyed by `lease_name` with a
//! unique index, and mirror the Postgres 6-case lease contract exactly:
//!   1. fresh lease           → acquire
//!   2. expired lease (any owner) → take over
//!   3. same-owner refresh    → acquire (heartbeat)
//!   4. different owner, live → deny
//!   5. release by non-owner  → no-op
//!   6. zero-ttl              → immediately expired, so a different owner can
//!      take over on the next acquire
//!
//! Expiry is stored as a `bson::DateTime` so the acquire filter can compare
//! `expires_at <= now` server-side (BSON datetimes order by their millisecond
//! value).
//!
//! PHASE 1 implements ONLY this base trait. The four system-store traits
//! (`ProjectionTaskStore` / `SagaStore` / `AdminAuditStore` /
//! `MigrationAuditStore`), the `CANONICAL_SYSTEM_STORE_BACKENDS` allowlist
//! entry, and runtime registration are deliberately deferred to phase 2.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use mongodb_driver::bson::{self, Document, doc};
use mongodb_driver::options::ReturnDocument;
use mongodb_driver::{Collection, Database, IndexModel};

use super::{CanonicalStore, DurabilityToken};

/// `_id` of the outbox sequence counter document.
const OUTBOX_SEQ_ID: &str = "outbox_seq";
/// Advisory-lease collection name (matches the SQL backends' table name).
const LEASE_COLLECTION: &str = "udb_advisory_leases";

pub struct MongoDbCanonicalStore {
    pub(super) db: Database,
    pub(super) instance_name: String,
    pub(super) outbox_collection: String,
}

/// Default collection names for the four system-store surfaces. Each lives in
/// the canonical store's database alongside the outbox + lease collections.
/// They mirror the SQL backends' table names (sans the `udb_system` schema
/// qualifier, which has no MongoDB analogue).
pub(super) const PROJECTION_COLLECTION: &str = "udb_projection_tasks";
pub(super) const SAGA_COLLECTION: &str = "udb_sagas";
pub(super) const ADMIN_AUDIT_COLLECTION: &str = "udb_admin_audit_log";
/// Serializer lock collection for the admin-audit hash chain. A single
/// `{_id:"lock"}` document is upserted+held inside the append transaction so
/// concurrent appenders serialize on the chain (the Mongo analogue of
/// `pg_advisory_xact_lock`).
pub(super) const ADMIN_AUDIT_LOCK_COLLECTION: &str = "udb_admin_audit_lock";
pub(super) const MIGRATION_RUNS_COLLECTION: &str = "udb_migration_runs";
pub(super) const MIGRATION_LEDGER_COLLECTION: &str = "udb_migration_op_ledger";
/// `_id` of the migration op-ledger sequence counter (the Mongo analogue of
/// the SQL `BIGSERIAL` ledger `id`). Lives in the ledger collection.
pub(super) const MIGRATION_OP_SEQ_ID: &str = "migration_op_seq";

impl MongoDbCanonicalStore {
    /// Construct from a native driver `Database` handle.
    pub fn new(
        db: Database,
        instance_name: impl Into<String>,
        outbox_collection: impl Into<String>,
    ) -> Self {
        Self {
            db,
            instance_name: instance_name.into(),
            outbox_collection: outbox_collection.into(),
        }
    }

    /// Convenience constructor that pulls the native `Database` off a
    /// [`MongoDbExecutor`](crate::runtime::executors::mongodb::MongoDbExecutor).
    /// Returns `None` when the executor is not on the native transport.
    pub fn from_executor(
        executor: &crate::runtime::executors::mongodb::MongoDbExecutor,
        instance_name: impl Into<String>,
        outbox_collection: impl Into<String>,
    ) -> Option<Self> {
        executor
            .native_database()
            .map(|db| Self::new(db, instance_name, outbox_collection))
    }

    /// Sibling system-store impls (`mongodb_projection.rs` etc.) reach the
    /// driver `Database` through this accessor; `self.db().client()` yields the
    /// `Client` for `start_session()`.
    pub(super) fn db(&self) -> &Database {
        &self.db
    }

    fn outbox(&self) -> Collection<Document> {
        self.db.collection::<Document>(&self.outbox_collection)
    }

    fn leases(&self) -> Collection<Document> {
        self.db.collection::<Document>(LEASE_COLLECTION)
    }

    /// Read the current outbox sequence counter (0 when the counter doc is
    /// absent — i.e. nothing has been enqueued yet).
    async fn current_seq(&self) -> Result<i64, String> {
        let counter = self
            .outbox()
            .find_one(doc! { "_id": OUTBOX_SEQ_ID })
            .await
            .map_err(|err| format!("mongodb outbox counter read failed: {err}"))?;
        Ok(counter
            .as_ref()
            .and_then(|doc| doc.get_i64("seq").ok())
            .unwrap_or(0))
    }

    /// `true` when `NamespaceExists`/"already exists" — createCollection is
    /// idempotent for our purposes.
    pub(super) fn is_namespace_exists(err: &mongodb_driver::error::Error) -> bool {
        let text = err.to_string();
        text.contains("NamespaceExists") || text.contains("already exists")
    }
}

#[async_trait]
impl CanonicalStore for MongoDbCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mongodb"
    }

    fn instance_name(&self) -> &str {
        &self.instance_name
    }

    async fn current_durability_token(&self) -> Result<DurabilityToken, String> {
        let seq = self.current_seq().await?;
        Ok(DurabilityToken::new("mongodb", seq.to_string()))
    }

    async fn wait_for_token(
        &self,
        token: &DurabilityToken,
        timeout: Duration,
    ) -> Result<bool, String> {
        if !token.is_for("mongodb") {
            return Err(format!(
                "MongoDbCanonicalStore cannot wait on a '{}' token",
                token.backend_label
            ));
        }
        let target: i64 = token
            .value
            .parse()
            .map_err(|_| format!("malformed mongodb durability token '{}'", token.value))?;
        let started = Instant::now();
        let poll = super::durability_poll_interval(timeout, super::MONGODB_DURABILITY_POLL_MS);
        loop {
            if self.current_seq().await? >= target {
                return Ok(true);
            }
            if started.elapsed() >= timeout {
                return Ok(false);
            }
            tokio::time::sleep(poll).await;
        }
    }

    async fn enqueue_outbox_event(
        &self,
        event_id: &str,
        topic: &str,
        partition_key: &str,
        payload: &serde_json::Value,
    ) -> Result<i64, String> {
        // 1. Atomically bump the counter and read the new value back. `upsert`
        //    creates the counter on first use; `ReturnDocument::After` yields
        //    the post-increment seq so concurrent writers never collide.
        let counter = self
            .outbox()
            .find_one_and_update(
                doc! { "_id": OUTBOX_SEQ_ID },
                doc! { "$inc": { "seq": 1_i64 } },
            )
            .upsert(true)
            .return_document(ReturnDocument::After)
            .await
            .map_err(|err| format!("mongodb outbox counter increment failed: {err}"))?;
        let seq = counter
            .as_ref()
            .and_then(|doc| doc.get_i64("seq").ok())
            .ok_or_else(|| "mongodb outbox counter returned no seq".to_string())?;

        // 2. Encode the JSON payload into BSON.
        let payload_bson = bson::to_bson(payload)
            .map_err(|err| format!("mongodb outbox payload encode failed: {err}"))?;

        // 3. Insert the event document carrying the assigned sequence.
        let created_at = bson::DateTime::from_millis(chrono::Utc::now().timestamp_millis());
        let event = doc! {
            "_id": event_id,
            "event_seq": seq,
            "topic": topic,
            "partition_key": partition_key,
            "payload": payload_bson,
            "created_at": created_at,
        };
        self.outbox()
            .insert_one(event)
            .await
            .map_err(|err| format!("mongodb outbox insert failed: {err}"))?;
        Ok(seq)
    }

    async fn outbox_max_seq(&self) -> Result<i64, String> {
        self.current_seq().await
    }

    async fn ensure_system_tables(&self) -> Result<(), String> {
        // createCollection is idempotent for our needs — tolerate NamespaceExists.
        match self.db.create_collection(&self.outbox_collection).await {
            Ok(_) => {}
            Err(err) if Self::is_namespace_exists(&err) => {}
            Err(err) => {
                return Err(format!(
                    "mongodb ensure_system_tables createCollection '{}' failed: {err}",
                    self.outbox_collection
                ));
            }
        }
        // Materialise the counter document if it is missing. `$setOnInsert`
        // keeps an existing counter untouched on idempotent re-runs.
        self.outbox()
            .find_one_and_update(
                doc! { "_id": OUTBOX_SEQ_ID },
                doc! { "$setOnInsert": { "seq": 0_i64 } },
            )
            .upsert(true)
            .return_document(ReturnDocument::After)
            .await
            .map_err(|err| format!("mongodb ensure_system_tables counter init failed: {err}"))?;
        Ok(())
    }

    async fn ensure_advisory_lease_table(&self) -> Result<(), String> {
        match self.db.create_collection(LEASE_COLLECTION).await {
            Ok(_) => {}
            Err(err) if Self::is_namespace_exists(&err) => {}
            Err(err) => {
                return Err(format!(
                    "mongodb ensure_advisory_lease_table createCollection failed: {err}"
                ));
            }
        }
        // Unique index on lease_name so the collection mirrors the SQL primary
        // key. createIndex is idempotent for an identical spec.
        let index = IndexModel::builder()
            .keys(doc! { "lease_name": 1 })
            .options(
                mongodb_driver::options::IndexOptions::builder()
                    .unique(true)
                    .build(),
            )
            .build();
        self.leases()
            .create_index(index)
            .await
            .map_err(|err| format!("mongodb advisory lease unique index failed: {err}"))?;
        Ok(())
    }

    async fn try_acquire_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, String> {
        // Atomic acquire mirroring the Postgres INSERT … ON CONFLICT DO UPDATE.
        // The filter encodes the two SAFE transitions; a live lease held by a
        // different owner matches NONE of them, so `find_one_and_update` with
        // `upsert` either updates our row OR (no match, no live row) inserts a
        // fresh one. A duplicate-key error means another writer inserted a live
        // row first → contention → Ok(false).
        let now = chrono::Utc::now();
        let now_bson = bson::DateTime::from_millis(now.timestamp_millis());
        let expires_at = bson::DateTime::from_millis(
            (now + chrono::Duration::from_std(ttl).unwrap_or_default()).timestamp_millis(),
        );

        // Match the lease ONLY when it is safe to (re)claim:
        //   * expired (expires_at <= now)  → takeover (covers zero-ttl)
        //   * same owner                   → refresh
        let filter = doc! {
            "lease_name": lease_name,
            "$or": [
                { "expires_at": { "$lte": now_bson } },
                { "owner_id": owner_id },
            ],
        };
        let update = doc! {
            "$set": { "owner_id": owner_id, "expires_at": expires_at },
            "$setOnInsert": { "lease_name": lease_name },
        };
        let outcome = self
            .leases()
            .find_one_and_update(filter, update)
            .upsert(true)
            .return_document(ReturnDocument::After)
            .await;
        match outcome {
            // Row updated/inserted and we own it.
            Ok(Some(doc)) => Ok(doc
                .get_str("owner_id")
                .map(|o| o == owner_id)
                .unwrap_or(false)),
            // upsert always returns the After document on success; None is
            // defensive.
            Ok(None) => Ok(false),
            Err(err) if Self::is_duplicate_key(&err) => {
                // A live, differently-owned lease already exists: the filter
                // didn't match it (not expired, not ours) and the upsert insert
                // collided on the unique lease_name index. That is exactly the
                // "different owner holds an unexpired lease" case → deny.
                Ok(false)
            }
            Err(err) => Err(format!("mongodb try_acquire_advisory_lease failed: {err}")),
        }
    }

    async fn release_advisory_lease(&self, lease_name: &str, owner_id: &str) -> Result<(), String> {
        // Owner-scoped delete: only the holder can release. No-op (deletedCount
        // 0) when we don't own it — idempotent.
        self.leases()
            .delete_one(doc! { "lease_name": lease_name, "owner_id": owner_id })
            .await
            .map_err(|err| format!("mongodb release_advisory_lease failed: {err}"))?;
        Ok(())
    }
}

impl MongoDbCanonicalStore {
    /// `true` when the error is a duplicate-key (E11000) write error — used by
    /// the lease acquire to map an upsert insert collision onto "denied".
    pub(super) fn is_duplicate_key(err: &mongodb_driver::error::Error) -> bool {
        let text = err.to_string();
        text.contains("E11000") || text.contains("duplicate key")
    }
}
