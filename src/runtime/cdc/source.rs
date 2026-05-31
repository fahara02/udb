//! Per-backend CDC source trait (C2 + C3).
//!
//! Pre-C2 the CDC engine in `runtime/cdc/engine_tail.rs` was hard-wired
//! to PostgreSQL logical replication — `LogicalReplicationStream` over
//! the Postgres replication protocol. Other backends could be CDC
//! *targets* (Kafka, Redis, ClickHouse via projections) but never CDC
//! *sources* — change data only flowed out of Postgres.
//!
//! This module supplies the abstract `CdcSource` contract so the
//! engine can host:
//!
//! - **`PostgresCdcSource`** — the existing WAL tailer adapted to the
//!   trait surface. C2 step 1.
//! - **`MongoCdcSource`** — uses the `mongodb` driver's change-stream
//!   API (`db.watch()`) when the `mongodb-native` feature is on. C2
//!   step 2.
//! - **`MysqlCdcSource`** — uses the MySQL binlog protocol. C3 step.
//!
//! Each source emits a stream of [`CdcEvent`] values carrying the
//! same structural fields the outbox engine already consumes (op,
//! tenant_id, project_id, payload). The engine doesn't need to know
//! which backend a stream came from — it consumes the abstract
//! `CdcEvent` and routes to Kafka / DLQ / projection workers exactly
//! as before.
//!
//! ## What stays in `engine_tail.rs`
//!
//! The Kafka producer, schema-registry interaction, and outbox table
//! advancement are backend-agnostic and stay in `engine_tail.rs`. The
//! refactor introduces the trait + adapter pattern; pre-existing
//! Postgres-specific code lives inside `PostgresCdcSource`.

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};

/// One change event surfaced by a CDC source. Backend-agnostic: the
/// engine doesn't care whether it came from a WAL record, a Mongo
/// change-stream document, or a MySQL binlog event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcEvent {
    /// Logical operation: `"insert"`, `"update"`, `"delete"`,
    /// `"truncate"` (when supported). Matches the wire tokens the
    /// outbox events use.
    pub op: String,
    /// Source-side identifier (schema.table for SQL backends,
    /// db.collection for Mongo). The engine surfaces this in the
    /// emitted Kafka topic and the schema-registry lookup.
    pub source: String,
    /// Active tenant (extracted from the changed row's
    /// `_tenant_id` / `tenant_id` column). Empty when the source row
    /// has no tenant column — single-tenant deployments.
    pub tenant_id: String,
    /// Active project (extracted from the changed row's
    /// `_project_id` / `project_id` column). Empty when none.
    pub project_id: String,
    /// The new row state (insert / update). `None` for delete events.
    pub after: Option<serde_json::Value>,
    /// The pre-image (update / delete). `None` for insert events.
    pub before: Option<serde_json::Value>,
    /// Source-specific offset (PG: LSN string, Mongo: resume token,
    /// MySQL: binlog file:position). The engine persists this in
    /// `udb_cdc_offsets` so a restart picks up where the previous
    /// process left off.
    pub source_offset: String,
    /// Unix-millis timestamp at the source. The engine uses this for
    /// staleness metrics and the schema-registry's
    /// `event_time` field.
    pub source_ts_unix_ms: i64,
}

impl CdcEvent {
    /// Construct an insert event.
    pub fn insert(
        source: impl Into<String>,
        after: serde_json::Value,
        source_offset: impl Into<String>,
    ) -> Self {
        let (tenant_id, project_id) = extract_context(&after);
        Self {
            op: "insert".to_string(),
            source: source.into(),
            tenant_id,
            project_id,
            after: Some(after),
            before: None,
            source_offset: source_offset.into(),
            source_ts_unix_ms: now_ms(),
        }
    }

    /// Construct an update event with before + after images.
    pub fn update(
        source: impl Into<String>,
        before: serde_json::Value,
        after: serde_json::Value,
        source_offset: impl Into<String>,
    ) -> Self {
        let (tenant_id, project_id) = extract_context(&after);
        Self {
            op: "update".to_string(),
            source: source.into(),
            tenant_id,
            project_id,
            after: Some(after),
            before: Some(before),
            source_offset: source_offset.into(),
            source_ts_unix_ms: now_ms(),
        }
    }

    /// Construct a delete event (only before image is available).
    pub fn delete(
        source: impl Into<String>,
        before: serde_json::Value,
        source_offset: impl Into<String>,
    ) -> Self {
        let (tenant_id, project_id) = extract_context(&before);
        Self {
            op: "delete".to_string(),
            source: source.into(),
            tenant_id,
            project_id,
            after: None,
            before: Some(before),
            source_offset: source_offset.into(),
            source_ts_unix_ms: now_ms(),
        }
    }
}

/// Pull tenant + project hints out of a row JSON. Accepts both the
/// `_tenant_id` / `_project_id` underscore-prefixed convention the
/// broker stamps on Mongo / Qdrant / Neo4j and the SQL-style
/// `tenant_id` / `project_id` column names. Empty when the row has
/// neither — the engine treats that as "global / unscoped".
fn extract_context(row: &serde_json::Value) -> (String, String) {
    let obj = row.as_object();
    let tenant_id = obj
        .and_then(|o| o.get("_tenant_id").or_else(|| o.get("tenant_id")))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let project_id = obj
        .and_then(|o| o.get("_project_id").or_else(|| o.get("project_id")))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    (tenant_id, project_id)
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// What every CDC source implements. `Send + Sync` because the engine
/// owns the source in a Tokio task. Methods are async because every
/// source involves real I/O (replication slot, change stream, binlog
/// dump).
#[async_trait]
pub trait CdcSource: Send + Sync {
    /// Stable backend label (`"postgres"`, `"mongodb"`, `"mysql"`).
    /// The engine uses this in metrics and the offset table.
    fn backend_label(&self) -> &str;

    /// Open a stream of `CdcEvent`s starting at the given offset.
    /// `from_offset` is empty on first start — the source picks its
    /// own "tail from now" semantic. On restart the engine passes the
    /// last persisted offset so the stream resumes exactly.
    ///
    /// The returned stream's lifetime is tied to `self`. The engine
    /// drops the stream on shutdown; the impl is expected to clean up
    /// (close the replication connection, dispose the change-stream
    /// cursor, etc.).
    async fn open(
        &self,
        from_offset: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<CdcEvent, String>> + Send>>, String>;

    /// Health probe — invoked by the broker's `/healthz` and the CDC
    /// status RPC. `Ok(())` means the source is reachable and the
    /// replication slot (or equivalent) is healthy.
    async fn health(&self) -> Result<(), String>;

    /// Optional capability check: does this source need a write-ahead
    /// log / replication slot that the operator must create
    /// out-of-band? PG returns true (operator must `CREATE PUBLICATION`
    /// + `CREATE_REPLICATION_SLOT`); Mongo + MySQL return false
    /// (change streams / binlog are server-side defaults).
    fn requires_replication_setup(&self) -> bool {
        false
    }
}

// ── Concrete sources ─────────────────────────────────────────────────────────
//
// Each source is feature-gated to the backend that hosts it.

/// Postgres CDC source — wraps the existing logical-replication tailer.
///
/// The actual WAL parsing lives in `runtime/cdc/engine_tail.rs::stream_cdc`
/// which is feature-gated to `kafka` because the producer + WAL parsing
/// share a dep graph. This adapter exposes the trait surface so the
/// engine can hold heterogeneous sources behind `Arc<dyn CdcSource>`.
#[cfg(feature = "kafka")]
pub struct PostgresCdcSource {
    pub dsn: String,
    pub publication: String,
    pub slot: String,
}

#[cfg(feature = "kafka")]
#[async_trait]
impl CdcSource for PostgresCdcSource {
    fn backend_label(&self) -> &str {
        "postgres"
    }

    fn requires_replication_setup(&self) -> bool {
        true
    }

    async fn open(
        &self,
        _from_offset: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<CdcEvent, String>> + Send>>, String> {
        // The engine_tail::stream_cdc function reads WAL via a
        // `LogicalReplicationStream`. Adapting that into a
        // `Stream<Item=CdcEvent>` involves translating each
        // `TupleData` into the CdcEvent shape. We delegate to a
        // dedicated helper in `engine_tail.rs::wal_stream_as_events`
        // (added in a later refactor) once the surface stabilises.
        //
        // For now this adapter returns an empty stream so the trait
        // can land + be tested independently of the WAL plumbing.
        // The engine continues to use the existing `stream_cdc`
        // entry point until the migration completes.
        Err(
            "PostgresCdcSource::open is not yet routed through the trait; \
             the engine still calls engine_tail::stream_cdc directly. \
             See runtime/cdc/source.rs for the migration plan."
                .into(),
        )
    }

    async fn health(&self) -> Result<(), String> {
        // Probe: open a non-replication connection to the DSN and
        // confirm `pg_replication_slots` shows the slot as active.
        use sqlx::Connection;
        let mut conn = sqlx::PgConnection::connect(&self.dsn)
            .await
            .map_err(|e| format!("postgres cdc health: connect failed: {e}"))?;
        let row: Option<(bool,)> =
            sqlx::query_as("SELECT active FROM pg_replication_slots WHERE slot_name = $1 LIMIT 1")
                .bind(&self.slot)
                .fetch_optional(&mut conn)
                .await
                .map_err(|e| format!("postgres cdc health: slot lookup failed: {e}"))?;
        match row {
            Some((true,)) => Ok(()),
            Some((false,)) => Err(format!("replication slot '{}' is inactive", self.slot)),
            None => Err(format!("replication slot '{}' does not exist", self.slot)),
        }
    }
}

/// MongoDB change-streams source — C2.
///
/// Uses the `mongodb` driver's `Collection::watch()` API (3.6+) when
/// the `mongodb-native` feature is compiled in. The change-stream
/// cursor surfaces `ChangeStreamEvent` documents which this adapter
/// translates to `CdcEvent` shape.
///
/// **Resume tokens**: Mongo's change-streams use opaque resume tokens
/// (BSON documents). We serialise them to JSON strings for storage in
/// the broker's `udb_cdc_offsets` table; on restart, deserialise back
/// and pass to `aggregate(["$changeStream": {resumeAfter: token}])`.
///
/// **Replica set / sharded cluster**: change streams require either a
/// replica set or a sharded cluster. The standalone `mongod`
/// deployment doesn't support them — the source surfaces a clear
/// error from `health()` when targeted at a standalone server.
#[cfg(feature = "mongodb-native")]
pub struct MongoCdcSource {
    pub uri: String,
    pub database: String,
    /// When `Some`, watches a specific collection. When `None`,
    /// watches the whole database (every collection produces events).
    pub collection: Option<String>,
}

#[cfg(feature = "mongodb-native")]
#[async_trait]
impl CdcSource for MongoCdcSource {
    fn backend_label(&self) -> &str {
        "mongodb"
    }

    async fn open(
        &self,
        from_offset: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<CdcEvent, String>> + Send>>, String> {
        use futures::StreamExt;
        use mongodb_driver::{Client, options::ChangeStreamOptions};

        let client = Client::with_uri_str(&self.uri)
            .await
            .map_err(|e| format!("mongo cdc connect failed: {e}"))?;
        let db = client.database(&self.database);

        // mongodb-driver 3.7 changed the watch() API: it now returns
        // a fluent ChangeStreamBuilder (no pipeline + opts args) and
        // resume_after takes a typed ResumeToken rather than a raw
        // BSON Document. ResumeToken is constructed by deserialising
        // from BSON, not by a direct From<Document> impl.
        let mut opts = ChangeStreamOptions::default();
        if !from_offset.is_empty() {
            let token_doc: mongodb_driver::bson::Document = serde_json::from_str(from_offset)
                .map_err(|e| format!("mongo cdc resume token parse failed: {e}"))?;
            let token: mongodb_driver::change_stream::event::ResumeToken =
                mongodb_driver::bson::from_document(token_doc)
                    .map_err(|e| format!("mongo cdc resume token deserialise failed: {e}"))?;
            opts.resume_after = Some(token);
        }

        let cs = match &self.collection {
            Some(coll) => {
                db.collection::<mongodb_driver::bson::Document>(coll)
                    .watch()
                    .with_options(opts)
                    .await
            }
            None => db.watch().with_options(opts).await,
        }
        .map_err(|e| format!("mongo cdc watch failed: {e}"))?;

        let source_label = format!(
            "{}.{}",
            self.database,
            self.collection.as_deref().unwrap_or("*")
        );

        // Translate each ChangeStreamEvent to CdcEvent. We hand-roll
        // the conversion because the driver's event type carries BSON
        // and our trait works in JSON.
        let stream = cs.map(move |evt_res| {
            let evt = evt_res.map_err(|e| format!("mongo cdc stream error: {e}"))?;
            let op = match evt.operation_type {
                mongodb_driver::change_stream::event::OperationType::Insert => "insert",
                mongodb_driver::change_stream::event::OperationType::Update => "update",
                mongodb_driver::change_stream::event::OperationType::Replace => "update",
                mongodb_driver::change_stream::event::OperationType::Delete => "delete",
                _ => "other",
            };
            let token = mongodb_driver::bson::to_document(&evt.id)
                .ok()
                .and_then(|d| serde_json::to_string(&d).ok())
                .unwrap_or_default();
            let after = evt
                .full_document
                .as_ref()
                .and_then(|d| mongodb_driver::bson::to_bson(d).ok())
                .and_then(|b| serde_json::to_value(b).ok());
            let before = evt
                .full_document_before_change
                .as_ref()
                .and_then(|d| mongodb_driver::bson::to_bson(d).ok())
                .and_then(|b| serde_json::to_value(b).ok());
            let (tenant_id, project_id) = extract_context(
                after
                    .as_ref()
                    .or(before.as_ref())
                    .unwrap_or(&serde_json::Value::Null),
            );
            Ok::<_, String>(CdcEvent {
                op: op.to_string(),
                source: source_label.clone(),
                tenant_id,
                project_id,
                after,
                before,
                source_offset: token,
                source_ts_unix_ms: now_ms(),
            })
        });

        Ok(Box::pin(stream))
    }

    async fn health(&self) -> Result<(), String> {
        use mongodb_driver::Client;
        use mongodb_driver::bson::doc;
        let client = Client::with_uri_str(&self.uri)
            .await
            .map_err(|e| format!("mongo cdc connect failed: {e}"))?;
        let db = client.database(&self.database);
        // `hello` returns `setName` for replica sets and an `msg=isdbgrid`
        // marker for sharded clusters. Standalone servers have neither.
        // mongodb-driver 3.7: run_command takes the command document
        // alone; options moved to a builder.
        let resp = db
            .run_command(doc! {"hello": 1})
            .await
            .map_err(|e| format!("mongo cdc hello failed: {e}"))?;
        let is_replica = resp.contains_key("setName");
        let is_sharded = resp
            .get_str("msg")
            .map(|m| m == "isdbgrid")
            .unwrap_or(false);
        if !is_replica && !is_sharded {
            return Err("mongo cdc requires a replica set or sharded cluster; \
                 standalone mongod does not support change streams"
                .into());
        }
        Ok(())
    }
}

/// MySQL binlog source — C3.
///
/// The MySQL binlog protocol (`COM_BINLOG_DUMP_GTID`) requires a
/// dedicated binary-protocol client beyond what `sqlx-mysql` provides.
/// Implementing it inline is a multi-week project (binlog event
/// parsing, GTID set arithmetic, row event decoding, FORMAT_DESCRIPTION
/// handling). This struct is the trait surface — operators can plug
/// in a real implementation via a feature flag without rewriting the
/// engine.
///
/// The default implementation reports `health()` against the MySQL
/// connection but `open()` returns a typed error so the engine can
/// surface the gap rather than silently emit no events.
#[cfg(feature = "mysql")]
pub struct MysqlBinlogSource {
    pub dsn: String,
    /// Server-id the broker registers as. MySQL requires every binlog
    /// reader to have a unique server-id; the operator picks one
    /// outside the existing replica set.
    pub server_id: u32,
}

#[cfg(feature = "mysql")]
#[async_trait]
impl CdcSource for MysqlBinlogSource {
    fn backend_label(&self) -> &str {
        "mysql"
    }

    async fn open(
        &self,
        _from_offset: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<CdcEvent, String>> + Send>>, String> {
        // The binlog protocol is server-side after `COM_BINLOG_DUMP_GTID`;
        // sqlx doesn't expose the raw protocol. To wire this, the
        // broker would either:
        //   1. Take a dependency on `mysql-binlog-connector-rs` (Apache 2,
        //      but adds ~50K lines to the dep graph), or
        //   2. Use the `Maxwell` / `Debezium` sidecar pattern and have
        //      this source read from Kafka.
        //
        // Option 2 is what production MySQL CDC deployments usually
        // pick because it gives you exactly-once + schema-evolution
        // for free. The trait stays — operators wire one of the two
        // implementations via a feature flag (`mysql-binlog-direct`
        // for option 1, `mysql-cdc-via-debezium` for option 2).
        Err(format!(
            "MysqlBinlogSource is a trait surface; no in-tree implementation. \
             For production MySQL CDC, deploy Debezium → Kafka and consume \
             events as a downstream Kafka source. Server-id reserved: {}",
            self.server_id
        ))
    }

    async fn health(&self) -> Result<(), String> {
        use sqlx::Connection;
        let mut conn = sqlx::MySqlConnection::connect(&self.dsn)
            .await
            .map_err(|e| format!("mysql cdc health: connect failed: {e}"))?;
        // Check `log_bin = ON` and `binlog_format = ROW` — both are
        // required for row-level binlog consumption.
        let row: (String, String) =
            sqlx::query_as("SELECT @@log_bin AS log_bin, @@binlog_format AS binlog_format")
                .fetch_one(&mut conn)
                .await
                .map_err(|e| format!("mysql cdc health: settings query failed: {e}"))?;
        if row.0 != "1" && row.0.to_lowercase() != "on" {
            return Err("mysql @@log_bin must be ON for CDC".into());
        }
        if !row.1.eq_ignore_ascii_case("ROW") {
            return Err(format!(
                "mysql @@binlog_format must be ROW for CDC; current: {}",
                row.1
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cdc_event_insert_extracts_tenant_from_after() {
        let evt = CdcEvent::insert(
            "public.orders",
            json!({"id": "abc", "_tenant_id": "acme", "_project_id": "p1"}),
            "0/A1B2C3",
        );
        assert_eq!(evt.op, "insert");
        assert_eq!(evt.tenant_id, "acme");
        assert_eq!(evt.project_id, "p1");
        assert!(evt.before.is_none());
        assert_eq!(evt.source_offset, "0/A1B2C3");
    }

    #[test]
    fn cdc_event_delete_extracts_tenant_from_before() {
        let evt = CdcEvent::delete(
            "public.orders",
            json!({"id": "abc", "tenant_id": "acme"}),
            "0/A1B2C3",
        );
        assert_eq!(evt.op, "delete");
        assert_eq!(evt.tenant_id, "acme");
        assert!(evt.after.is_none());
    }

    #[test]
    fn cdc_event_extract_context_handles_missing_fields() {
        let evt = CdcEvent::insert(
            "public.t",
            json!({"id": "x"}), // no tenant/project columns
            "off",
        );
        assert_eq!(evt.tenant_id, "");
        assert_eq!(evt.project_id, "");
    }

    /// D (2026-05-30): in-memory `CdcSource` for tests + non-live
    /// integration paths. Holds a pre-built sequence of events and
    /// replays them; honours `from_offset` by skipping events whose
    /// `source_offset` is less-than-or-equal-to the given marker.
    pub struct InMemoryCdcSource {
        pub label: String,
        pub events: Vec<CdcEvent>,
    }

    #[async_trait]
    impl CdcSource for InMemoryCdcSource {
        fn backend_label(&self) -> &str {
            &self.label
        }
        async fn open(
            &self,
            from_offset: &str,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<CdcEvent, String>> + Send>>, String> {
            let from = from_offset.to_string();
            let events: Vec<CdcEvent> = self
                .events
                .iter()
                .filter(|e| from.is_empty() || e.source_offset.as_str() > from.as_str())
                .cloned()
                .collect();
            let stream = futures::stream::iter(events.into_iter().map(Ok));
            Ok(Box::pin(stream))
        }
        async fn health(&self) -> Result<(), String> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn d_in_memory_source_open_streams_events_after_offset() {
        use futures::StreamExt;

        let source = InMemoryCdcSource {
            label: "test".into(),
            events: vec![
                CdcEvent::insert("t", json!({"id": "a"}), "1"),
                CdcEvent::insert("t", json!({"id": "b"}), "2"),
                CdcEvent::insert("t", json!({"id": "c"}), "3"),
            ],
        };
        // No offset: get every event.
        let mut s = source.open("").await.unwrap();
        let mut all = Vec::new();
        while let Some(evt) = s.next().await {
            all.push(evt.unwrap());
        }
        assert_eq!(all.len(), 3);
        // Offset = "2": only event "3".
        let mut s = source.open("2").await.unwrap();
        let mut after = Vec::new();
        while let Some(evt) = s.next().await {
            after.push(evt.unwrap());
        }
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].source_offset, "3");
    }

    #[test]
    fn d_in_memory_source_label_matches_constructor() {
        let s = InMemoryCdcSource {
            label: "fake".into(),
            events: vec![],
        };
        assert_eq!(s.backend_label(), "fake");
    }

    #[test]
    fn cdc_event_update_carries_both_before_and_after() {
        let evt = CdcEvent::update(
            "public.t",
            json!({"id": "x", "v": 1}),
            json!({"id": "x", "v": 2}),
            "off",
        );
        assert_eq!(evt.op, "update");
        assert_eq!(evt.before.unwrap()["v"], 1);
        assert_eq!(evt.after.unwrap()["v"], 2);
    }
}
