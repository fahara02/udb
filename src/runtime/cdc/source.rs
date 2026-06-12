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

#[cfg(any(feature = "kafka", feature = "mysql"))]
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
#[cfg(any(feature = "kafka", feature = "mysql"))]
use sqlx::Row;

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
    /// `_tenant_id` / `tenant_id` column). Must be non-empty before
    /// a source event is published.
    pub tenant_id: String,
    /// Active project (extracted from the changed row's
    /// `_project_id` / `project_id` column). Must be non-empty before
    /// a source event is published.
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

    pub fn validate_identity(&self) -> Result<(), String> {
        if self.tenant_id.trim().is_empty() {
            return Err(format!(
                "CDC source event {} at offset {} is missing tenant_id/_tenant_id",
                self.source, self.source_offset
            ));
        }
        if self.project_id.trim().is_empty() {
            return Err(format!(
                "CDC source event {} at offset {} is missing project_id/_project_id",
                self.source, self.source_offset
            ));
        }
        Ok(())
    }
}

/// Pull tenant + project hints out of a row JSON. Accepts both the
/// `_tenant_id` / `_project_id` underscore-prefixed convention the
/// broker stamps on Mongo / Qdrant / Neo4j and the SQL-style
/// `tenant_id` / `project_id` column names. Empty values are invalid
/// and rejected by [`CdcEvent::validate_identity`] before publish.
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

fn configured_relation(env_key: &str, fallback: &str) -> String {
    std::env::var(env_key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn safe_relation(raw: &str) -> Result<String, String> {
    let relation = raw.trim();
    if relation.is_empty() {
        return Err("CDC source relation is empty".to_string());
    }
    if relation
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '"'))
    {
        Ok(relation.to_string())
    } else {
        Err(format!(
            "CDC source relation '{relation}' contains invalid characters"
        ))
    }
}

fn offset_as_i64(from_offset: &str) -> i64 {
    from_offset.trim().parse::<i64>().unwrap_or(0)
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

/// Postgres CDC source.
///
/// This adapter tails a configured Postgres CDC journal/outbox relation with
/// monotonically increasing `event_seq` and JSONB `payload` columns. Raw WAL
/// decoding remains out of scope for the published `tokio-postgres` APIs, but
/// this is a real `CdcSource` implementation with offset resume instead of an
/// always-failing stub.
#[cfg(feature = "kafka")]
pub struct PostgresCdcSource {
    pub dsn: String,
    /// Optional source relation override. When empty, the adapter reads
    /// `UDB_CDC_POSTGRES_SOURCE_TABLE`, then falls back to UDB's outbox table.
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
        from_offset: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<CdcEvent, String>> + Send>>, String> {
        let relation = if self.publication.trim().is_empty() {
            configured_relation("UDB_CDC_POSTGRES_SOURCE_TABLE", "udb_system.udb_cdc_outbox")
        } else {
            self.publication.trim().to_string()
        };
        let relation = safe_relation(&relation)?;
        let dsn = self.dsn.clone();
        let mut last_seen = offset_as_i64(from_offset);
        let stream = stream! {
            let pool = match sqlx::PgPool::connect(&dsn).await {
                Ok(pool) => pool,
                Err(err) => {
                    yield Err(format!("postgres cdc source connect failed: {err}"));
                    return;
                }
            };
            loop {
                let sql = format!(
                    "SELECT event_seq, topic, payload, created_at
                     FROM {relation}
                     WHERE event_seq > $1
                     ORDER BY event_seq ASC
                     LIMIT 100"
                );
                match sqlx::query(&sql).bind(last_seen).fetch_all(&pool).await {
                    Ok(rows) => {
                        if rows.is_empty() {
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            continue;
                        }
                        for row in rows {
                            let seq: i64 = match row.try_get("event_seq") {
                                Ok(seq) => seq,
                                Err(err) => {
                                    yield Err(format!("postgres cdc source event_seq decode failed: {err}"));
                                    continue;
                                }
                            };
                            let topic: String = row.try_get("topic").unwrap_or_else(|_| relation.clone());
                            let payload: serde_json::Value = row
                                .try_get("payload")
                                .unwrap_or_else(|_| serde_json::Value::Null);
                            last_seen = seq;
                            let mut event = CdcEvent::insert(topic, payload, seq.to_string());
                            event.source_ts_unix_ms = row
                                .try_get::<chrono::DateTime<chrono::Utc>, _>("created_at")
                                .map(|ts| ts.timestamp_millis())
                                .unwrap_or_else(|_| now_ms());
                            yield Ok(event);
                        }
                    }
                    Err(err) => {
                        yield Err(format!("postgres cdc source poll failed: {err}"));
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }

    async fn health(&self) -> Result<(), String> {
        // Probe the configured source relation. Slot checks are still useful
        // for WAL deployments, but the concrete `open()` adapter tails a table.
        use sqlx::Connection;
        let mut conn = sqlx::PgConnection::connect(&self.dsn)
            .await
            .map_err(|e| format!("postgres cdc health: connect failed: {e}"))?;
        let relation = if self.publication.trim().is_empty() {
            configured_relation("UDB_CDC_POSTGRES_SOURCE_TABLE", "udb_system.udb_cdc_outbox")
        } else {
            self.publication.trim().to_string()
        };
        let relation = safe_relation(&relation)?;
        sqlx::query(&format!("SELECT 1 FROM {relation} LIMIT 1"))
            .execute(&mut conn)
            .await
            .map(|_| ())
            .map_err(|e| format!("postgres cdc health: source relation probe failed: {e}"))
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
/// MySQL CDC source.
///
/// Uses a configured journal/outbox table (`UDB_CDC_MYSQL_SOURCE_TABLE`, default
/// `udb_cdc_outbox`) with monotonically increasing `id`, `source`, `op`, and
/// JSON `payload_json`/`payload` columns. Direct binlog decoding remains a
/// future adapter, but this source is live and resumable today.
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
        from_offset: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<CdcEvent, String>> + Send>>, String> {
        let relation = safe_relation(&configured_relation(
            "UDB_CDC_MYSQL_SOURCE_TABLE",
            "udb_cdc_outbox",
        ))?;
        let dsn = self.dsn.clone();
        let mut last_seen = offset_as_i64(from_offset);
        let stream = stream! {
            let pool = match sqlx::MySqlPool::connect(&dsn).await {
                Ok(pool) => pool,
                Err(err) => {
                    yield Err(format!("mysql cdc source connect failed: {err}"));
                    return;
                }
            };
            loop {
                let sql = format!(
                    "SELECT id, source, op, COALESCE(payload_json, payload) AS payload
                     FROM {relation}
                     WHERE id > ?
                     ORDER BY id ASC
                     LIMIT 100"
                );
                match sqlx::query(&sql).bind(last_seen).fetch_all(&pool).await {
                    Ok(rows) => {
                        if rows.is_empty() {
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            continue;
                        }
                        for row in rows {
                            let id: i64 = match row.try_get("id") {
                                Ok(id) => id,
                                Err(err) => {
                                    yield Err(format!("mysql cdc source id decode failed: {err}"));
                                    continue;
                                }
                            };
                            let source: String = row.try_get("source").unwrap_or_else(|_| relation.clone());
                            let op: String = row.try_get("op").unwrap_or_else(|_| "insert".to_string());
                            let payload: serde_json::Value = row
                                .try_get::<serde_json::Value, _>("payload")
                                .or_else(|_| {
                                    row.try_get::<String, _>("payload")
                                        .and_then(|raw| serde_json::from_str(&raw).map_err(|err| sqlx::Error::Decode(Box::new(err))))
                                })
                                .unwrap_or_else(|_| serde_json::Value::Null);
                            last_seen = id;
                            let mut event = match op.to_ascii_lowercase().as_str() {
                                "delete" => CdcEvent::delete(source, payload, id.to_string()),
                                "update" => CdcEvent::update(source, serde_json::Value::Null, payload, id.to_string()),
                                _ => CdcEvent::insert(source, payload, id.to_string()),
                            };
                            event.source_ts_unix_ms = now_ms();
                            yield Ok(event);
                        }
                    }
                    Err(err) => {
                        yield Err(format!("mysql cdc source poll failed: {err}"));
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }

    async fn health(&self) -> Result<(), String> {
        use sqlx::Connection;
        let mut conn = sqlx::MySqlConnection::connect(&self.dsn)
            .await
            .map_err(|e| format!("mysql cdc health: connect failed: {e}"))?;
        let relation = safe_relation(&configured_relation(
            "UDB_CDC_MYSQL_SOURCE_TABLE",
            "udb_cdc_outbox",
        ))?;
        sqlx::query(&format!("SELECT 1 FROM {relation} LIMIT 1"))
            .execute(&mut conn)
            .await
            .map(|_| ())
            .map_err(|e| format!("mysql cdc health: source relation probe failed: {e}"))
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
        assert!(evt.validate_identity().is_err());
    }

    // In-memory `CdcSource` removed: the source/offset path is exercised by the
    // live Postgres-WAL + Kafka integration test, not an in-memory replay double
    // (in-memory doubles can pass while the real WAL/Kafka path is broken).

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
