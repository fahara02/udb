//! Live end-to-end test of the native auth event pipeline:
//! authn service → transactional outbox → CDC engine → Apache Kafka.
//!
//! This confirms the assumptions of the event-driven design against REAL
//! infrastructure (no in-memory doubles): a `create_user` call emits through the
//! real `OutboxAuthEventSink` into `udb_system.outbox_events`, the real
//! `CdcEngine::process_outbox_event` publishes the envelope to Kafka, and a real
//! Kafka consumer reads it back on the proto-declared topic.
//!
//! Run (after `docker compose -f docker-compose.integration.yml up -d --wait
//! postgres kafka`):
//!   UDB_LIVE_AUTH_TESTS=1 UDB_INTEGRATION_KAFKA_BROKERS=localhost:59192 \
//!     cargo test --lib events_live -- --ignored --nocapture

use super::support::*;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::authn::{
    PostgresApiKeyStore, PostgresSessionStore, PostgresUserStore, SessionStore,
};
use crate::runtime::cdc::{CdcConfig, CdcEngine};
use crate::runtime::metrics::NoopMetrics;
use crate::runtime::security::SecurityConfig;
use crate::runtime::service::auth_service::AuthnServiceImpl;
use std::sync::Arc;
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

const OUTBOX_RELATION: &str = "udb_system.outbox_events";

fn kafka_brokers() -> String {
    std::env::var("UDB_INTEGRATION_KAFKA_BROKERS")
        .or_else(|_| std::env::var("UDB_KAFKA_BROKERS"))
        .unwrap_or_else(|_| "localhost:59192".to_string())
}

/// Create the shared outbox table the CDC engine tails (same shape as the
/// system bootstrap / `prepare_outbox_envelope` insert).
async fn ensure_outbox_table(pool: &sqlx::PgPool) {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS udb_system")
        .execute(pool)
        .await
        .expect("create udb_system schema");
    sqlx::query("DROP TABLE IF EXISTS udb_system.outbox_events CASCADE")
        .execute(pool)
        .await
        .expect("drop outbox");
    sqlx::query(
        "CREATE TABLE udb_system.outbox_events ( \
            event_seq     BIGSERIAL PRIMARY KEY, \
            event_id      UUID NOT NULL UNIQUE, \
            topic         TEXT NOT NULL, \
            partition_key TEXT NOT NULL DEFAULT '', \
            payload       JSONB NOT NULL, \
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW() )",
    )
    .execute(pool)
    .await
    .expect("create outbox table");
}

#[tokio::test]
#[ignore = "requires live Postgres + Kafka; topic udb.authn.user.registered.v1 must exist. \
            UDB_LIVE_AUTH_TESTS=1 UDB_INTEGRATION_KAFKA_BROKERS=localhost:59192 \
            cargo test --lib events_live -- --ignored --nocapture"]
async fn auth_event_outbox_to_cdc_to_kafka_end_to_end() {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::{ClientConfig, Message};
    use rdkafka::{Offset, TopicPartitionList};

    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live UDB system catalog (CDC journal/DLQ tables)");
    ensure_outbox_table(&pool).await;

    // Real authn service: Postgres stores + the real outbox event sink (no Noop,
    // no in-memory). This is the production wiring.
    let sink = Arc::new(super::super::events::OutboxAuthEventSink::new(
        pool.clone(),
        OUTBOX_RELATION,
    ));
    let config = crate::runtime::authn::AuthnConfig {
        session_enabled: true,
        session_hash_secret: "live-auth-test-secret".to_string(),
        ..crate::runtime::authn::AuthnConfig::default()
    };
    let sessions: Arc<dyn SessionStore> = Arc::new(PostgresSessionStore::new(pool.clone(), ""));
    let authn = AuthnServiceImpl::with_stores(
        config,
        SecurityConfig::current(),
        sessions,
        Arc::new(PostgresApiKeyStore::new(pool.clone(), "")),
        Arc::new(PostgresUserStore::new(pool.clone(), "")),
    )
    .with_event_sink(sink);

    // ── Assumption 1: the service emits to the outbox ──────────────────────
    let suffix = Uuid::new_v4().simple().to_string();
    let created = authn
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("evt_{suffix}"),
            email: format!("evt_{suffix}@example.com"),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "acme".to_string(),
            full_name: "Event Live".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("create_user")
        .into_inner();
    let user = created.user.expect("user");

    let (event_id, topic, partition_key, payload): (Uuid, String, String, serde_json::Value) =
        sqlx::query_as(
            "SELECT event_id, topic, partition_key, payload FROM udb_system.outbox_events \
             WHERE topic = $1 ORDER BY event_seq DESC LIMIT 1",
        )
        .bind(super::super::events::topics::USER_REGISTERED)
        .fetch_one(&pool)
        .await
        .expect("outbox row written by create_user");
    assert_eq!(topic, "udb.authn.user.registered.v1");
    assert_eq!(partition_key, user.user_id, "partition key = document_id");
    assert_eq!(payload["event_type"], "udb.authn.user.registered.v1");
    assert_eq!(payload["event_id"], event_id.to_string());
    assert_eq!(payload["payload"]["user_id"], user.user_id);
    assert_eq!(
        payload["payload"]["email"],
        format!("evt_{suffix}@example.com")
    );

    // ── Assumption 2: the CDC engine publishes the outbox row to Kafka ─────
    let brokers = kafka_brokers();
    ensure_kafka_topic(&brokers, "udb.authn.user.registered.v1").await;
    let cdc_config = CdcConfig {
        outbox_table: "outbox_events".to_string(),
        ..CdcConfig::default()
    };
    let engine = CdcEngine::new(
        pool.clone(),
        None, // no redis idempotency guard for the test
        &brokers,
        live_pg_dsn(),
        Arc::new(NoopMetrics),
        cdc_config,
    )
    .expect("build CDC engine");
    engine
        .load_topic_policies()
        .await
        .expect("load live CDC topic policies");
    engine
        .process_outbox_event(
            event_id,
            topic.clone(),
            partition_key.clone(),
            payload.clone(),
            chrono::Utc::now(),
            0,
            None,
        )
        .await;
    let (kafka_partition, kafka_offset): (i32, i64) = sqlx::query_as(
        "SELECT kafka_partition, kafka_offset FROM udb_system.udb_cdc_event_journal \
         WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_one(&pool)
    .await
    .expect("CDC journal row with Kafka offset");

    // ── Assumption 2 (confirm): the event is on the Kafka topic ────────────
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", format!("udb-evt-test-{suffix}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("kafka consumer");
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(
            "udb.authn.user.registered.v1",
            kafka_partition,
            Offset::Offset(kafka_offset),
        )
        .expect("assign exact Kafka offset");
    consumer
        .assign(&assignment)
        .expect("assign Kafka partition");

    let mut found = false;
    for _ in 0..40 {
        if let Some(result) = consumer.poll(Duration::from_millis(500)) {
            let msg = result.expect("kafka message");
            if let Some(bytes) = msg.payload() {
                let value: serde_json::Value =
                    serde_json::from_slice(bytes).unwrap_or(serde_json::Value::Null);
                if value["event_id"] == event_id.to_string() {
                    assert_eq!(value["event_type"], "udb.authn.user.registered.v1");
                    assert_eq!(value["payload"]["user_id"], user.user_id);
                    found = true;
                    break;
                }
            }
        }
    }
    assert!(
        found,
        "the UserRegistered event must reach Kafka topic udb.authn.user.registered.v1"
    );

    cleanup_native_auth_db(&pool).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS udb_system.outbox_events CASCADE")
        .execute(&pool)
        .await;
}
