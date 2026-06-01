//! Live CDC tests that exercise failure/replay behavior against real Postgres
//! and real Kafka. These intentionally avoid in-memory CDC sources.

use super::support::*;
use crate::runtime::cdc::{CdcConfig, CdcEngine, CdcEnvelope};
use crate::runtime::metrics::NoopMetrics;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::Stream;
use uuid::Uuid;

fn kafka_brokers() -> String {
    std::env::var("UDB_INTEGRATION_KAFKA_BROKERS")
        .or_else(|_| std::env::var("UDB_KAFKA_BROKERS"))
        .unwrap_or_else(|_| "localhost:59192".to_string())
}

fn cdc_config_for_live_outbox(dlq_topic: impl Into<String>) -> CdcConfig {
    CdcConfig {
        outbox_table: "outbox_events".to_string(),
        dlq_topic: dlq_topic.into(),
        ..CdcConfig::default()
    }
}

async fn insert_outbox_envelope(
    pool: &sqlx::PgPool,
    event_id: Uuid,
    topic: &str,
    partition_key: &str,
    payload: serde_json::Value,
) {
    sqlx::query(
        "INSERT INTO udb_system.outbox_events (event_id, topic, partition_key, payload, created_at) \
         VALUES ($1, $2, $3, $4::JSONB, NOW())",
    )
    .bind(event_id)
    .bind(topic)
    .bind(partition_key)
    .bind(payload)
    .execute(pool)
    .await
    .expect("insert live CDC outbox event");
}

async fn dlq_record(pool: &sqlx::PgPool, event_id: Uuid) -> (String, String, serde_json::Value) {
    sqlx::query_as(
        "SELECT error_type, error_message, payload FROM udb_system.udb_cdc_dlq_events \
         WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await
    .expect("CDC DLQ row")
}

async fn outbox_count(pool: &sqlx::PgPool, event_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM udb_system.outbox_events WHERE event_id = $1")
        .bind(event_id)
        .fetch_one(pool)
        .await
        .expect("count outbox event")
}

async fn next_cdc_item(
    stream: &mut Pin<Box<dyn Stream<Item = Result<CdcEnvelope, tonic::Status>> + Send + 'static>>,
) -> CdcEnvelope {
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("CDC replay item should arrive")
        .expect("CDC replay stream should not end")
        .expect("CDC replay item should be ok")
}

#[tokio::test]
#[ignore = "requires live Postgres + Kafka. UDB_LIVE_AUTH_TESTS=1 \
            UDB_INTEGRATION_KAFKA_BROKERS=localhost:59192 cargo test --lib cdc_live -- --ignored --nocapture"]
async fn live_cdc_topic_policy_rejection_routes_to_dlq_and_acks() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live UDB system catalog");
    ensure_outbox_table(&pool).await;

    let brokers = kafka_brokers();
    let dlq_topic = format!("udb.cdc.dlq.{}.v1", Uuid::new_v4().simple());
    ensure_kafka_topic(&brokers, &dlq_topic).await;

    sqlx::query(
        "INSERT INTO udb_system.udb_topic_policy \
         (topic, owning_project, owning_service, schema_uri, enabled) \
         VALUES ($1, 'billing', 'authn', 'udb.authn.events.v1.Message', TRUE) \
         ON CONFLICT (topic) DO UPDATE SET enabled = TRUE, updated_at = NOW()",
    )
    .bind("udb.authn.allowed.only.v1")
    .execute(&pool)
    .await
    .expect("insert restrictive topic policy");

    let event_id = Uuid::new_v4();
    let user_id = Uuid::new_v4().to_string();
    let rejected_topic = "udb.authn.user.registered.v1";
    insert_outbox_envelope(
        &pool,
        event_id,
        rejected_topic,
        &user_id,
        serde_json::json!({
            "event_id": event_id.to_string(),
            "event_type": rejected_topic,
            "correlation_id": format!("cdc-policy:{event_id}"),
            "document_id": user_id,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "payload": {"user_id": user_id}
        }),
    )
    .await;

    let mut engine = CdcEngine::new(
        pool.clone(),
        None,
        &brokers,
        live_pg_dsn(),
        Arc::new(NoopMetrics),
        cdc_config_for_live_outbox(dlq_topic),
    )
    .expect("build CDC engine");
    engine
        .load_topic_policies()
        .await
        .expect("load live topic policies");
    engine
        .process_outbox_event(
            event_id,
            rejected_topic.to_string(),
            user_id,
            serde_json::json!({
                "event_id": event_id.to_string(),
                "event_type": rejected_topic,
                "correlation_id": format!("cdc-policy:{event_id}"),
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "payload": {}
            }),
            chrono::Utc::now(),
            77,
            None,
        )
        .await;

    let (error_type, error_message, payload) = dlq_record(&pool, event_id).await;
    assert_eq!(error_type, "TopicPolicyRejected");
    assert!(
        error_message.contains(rejected_topic),
        "DLQ message should name rejected topic: {error_message}"
    );
    assert_eq!(
        payload["failure_metadata"]["error_type"],
        "TopicPolicyRejected"
    );
    assert_eq!(outbox_count(&pool, event_id).await, 0);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + Kafka. UDB_LIVE_AUTH_TESTS=1 \
            UDB_INTEGRATION_KAFKA_BROKERS=localhost:59192 cargo test --lib cdc_live -- --ignored --nocapture"]
async fn live_cdc_mismatched_envelope_event_id_routes_to_dlq() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live UDB system catalog");
    ensure_outbox_table(&pool).await;

    let brokers = kafka_brokers();
    let dlq_topic = format!("udb.cdc.dlq.{}.v1", Uuid::new_v4().simple());
    ensure_kafka_topic(&brokers, &dlq_topic).await;

    let event_id = Uuid::new_v4();
    let payload_event_id = Uuid::new_v4();
    let topic = "udb.authn.user.registered.v1";
    let payload = serde_json::json!({
        "event_id": payload_event_id.to_string(),
        "event_type": topic,
        "correlation_id": format!("cdc-mismatch:{event_id}"),
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "payload": {"user_id": Uuid::new_v4().to_string()}
    });
    insert_outbox_envelope(&pool, event_id, topic, "mismatch-user", payload.clone()).await;

    let engine = CdcEngine::new(
        pool.clone(),
        None,
        &brokers,
        live_pg_dsn(),
        Arc::new(NoopMetrics),
        cdc_config_for_live_outbox(dlq_topic),
    )
    .expect("build CDC engine");
    engine
        .process_outbox_event(
            event_id,
            topic.to_string(),
            "mismatch-user".to_string(),
            payload,
            chrono::Utc::now(),
            88,
            None,
        )
        .await;

    let (error_type, error_message, payload) = dlq_record(&pool, event_id).await;
    assert_eq!(error_type, "EnvelopeEventIdMismatch");
    assert!(
        error_message.contains(&payload_event_id.to_string()),
        "DLQ message should include payload event id: {error_message}"
    );
    assert_eq!(
        payload["failure_metadata"]["error_type"],
        "EnvelopeEventIdMismatch"
    );
    assert_eq!(outbox_count(&pool, event_id).await, 0);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + Kafka. UDB_LIVE_AUTH_TESTS=1 \
            UDB_INTEGRATION_KAFKA_BROKERS=localhost:59192 cargo test --lib cdc_live -- --ignored --nocapture"]
async fn live_cdc_stream_replay_filters_by_scope_topic_and_anchor() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live UDB system catalog");
    ensure_outbox_table(&pool).await;

    let brokers = kafka_brokers();
    let engine = CdcEngine::new(
        pool.clone(),
        None,
        &brokers,
        live_pg_dsn(),
        Arc::new(NoopMetrics),
        cdc_config_for_live_outbox("udb.cdc.dlq.replay.v1"),
    )
    .expect("build CDC engine");

    let denied = match engine
        .stream_cdc(Vec::new(), "udb.authn.*".to_string(), None)
        .await
    {
        Ok(_) => panic!("missing CDC scope should be denied"),
        Err(status) => status,
    };
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);

    let anchor_id = Uuid::new_v4();
    let replay_id = Uuid::new_v4();
    let skipped_id = Uuid::new_v4();
    insert_outbox_envelope(
        &pool,
        anchor_id,
        "udb.authn.anchor.v1",
        "anchor",
        serde_json::json!({
            "event_id": anchor_id.to_string(),
            "event_type": "udb.authn.anchor.v1",
            "correlation_id": "cdc-replay-anchor",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "payload": {}
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    insert_outbox_envelope(
        &pool,
        skipped_id,
        "udb.notification.sent.v1",
        "skip",
        serde_json::json!({
            "event_id": skipped_id.to_string(),
            "event_type": "udb.notification.sent.v1",
            "correlation_id": "cdc-replay-skip",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "payload": {}
        }),
    )
    .await;
    insert_outbox_envelope(
        &pool,
        replay_id,
        "udb.authn.user.registered.v1",
        "replay",
        serde_json::json!({
            "event_id": replay_id.to_string(),
            "event_type": "udb.authn.user.registered.v1",
            "correlation_id": "cdc-replay-hit",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "payload": {"user_id": "replay"}
        }),
    )
    .await;

    let mut stream = engine
        .stream_cdc(
            vec!["udb:cdc:read".to_string()],
            "udb.authn.*".to_string(),
            Some(anchor_id.to_string()),
        )
        .await
        .expect("authorized CDC replay stream");
    let envelope = next_cdc_item(&mut stream).await;
    assert_eq!(envelope.event_id, replay_id.to_string());
    assert_eq!(envelope.topic, "udb.authn.user.registered.v1");
    assert_eq!(envelope.partition_key, "replay");

    cleanup_native_auth_db(&pool).await;
}
