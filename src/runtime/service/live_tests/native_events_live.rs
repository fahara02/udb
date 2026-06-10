//! §8 live conformance (final_task.md Phase 7 acceptance: "Tenant, notification,
//! storage, asset, and WebRTC event contracts match emitted Kafka topics").
//!
//! Each native-service mutation that declares an `emits` topic in its proto
//! `method_event_contract` must actually write the matching row to the
//! transactional outbox (`udb_system.outbox_events`). The notification and authn
//! services already have such tests (`notification_events_live.rs`,
//! `events_live.rs`); these close the gap for **storage / asset / webrtc** by
//! triggering a real mutation against the real Postgres-backed service (outbox
//! wired, no in-memory double) and asserting the declared versioned topic row.
//!
//! Run (after `docker compose ... up -d --wait postgres`):
//!   UDB_LIVE_AUTH_TESTS=1 cargo test --lib native_events_live -- --ignored --nocapture

use super::support::*;
use crate::proto::udb::core::asset::services::v1 as asset_pb;
use crate::proto::udb::core::asset::services::v1::asset_service_server::AssetService;
use crate::proto::udb::core::storage::services::v1 as storage_pb;
use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageService;
use crate::proto::udb::core::webrtc::services::v1 as webrtc_pb;
use crate::proto::udb::core::webrtc::services::v1::room_service_server::RoomService;
use tonic::Request;
use uuid::Uuid;

const OUTBOX_RELATION: &str = "udb_system.outbox_events";

/// Create the shared outbox table the CDC engine tails (same shape as the system
/// bootstrap / `prepare_outbox_envelope` insert and the auth-side live tests).
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

/// Assert the most-recent outbox row for `topic` exists and its envelope's
/// `event_type` matches the declared versioned topic. Returns the payload.
async fn assert_outbox_topic(pool: &sqlx::PgPool, topic: &str) -> serde_json::Value {
    let (event_id, got_topic, payload): (Uuid, String, serde_json::Value) = sqlx::query_as(
        "SELECT event_id, topic, payload FROM udb_system.outbox_events \
         WHERE topic = $1 ORDER BY event_seq DESC LIMIT 1",
    )
    .bind(topic)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|err| panic!("no outbox row written for declared topic {topic}: {err}"));
    assert_eq!(got_topic, topic);
    assert_eq!(
        payload["event_type"], topic,
        "envelope event_type must equal the versioned topic the proto `emits[]` declares"
    );
    assert_eq!(payload["event_id"], event_id.to_string());
    payload
}

#[tokio::test]
#[ignore = "requires live Postgres; UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_storage_finalize_emits_declared_topic -- --ignored --nocapture"]
async fn live_storage_finalize_emits_declared_topic() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    ensure_outbox_table(&pool).await;
    let tenant_id = Uuid::new_v4().to_string();

    let svc = storage_service(pool.clone()).with_outbox(Some(OUTBOX_RELATION.to_string()));
    let reg = svc
        .register_upload(Request::new(storage_pb::RegisterUploadRequest {
            tenant_id: tenant_id.clone(),
            filename: "report.pdf".to_string(),
            content_type: "application/pdf".to_string(),
            file_type: "DOCUMENT".to_string(),
            ..Default::default()
        }))
        .await
        .expect("register_upload")
        .into_inner();
    svc.finalize_upload(Request::new(storage_pb::FinalizeUploadRequest {
        tenant_id: tenant_id.clone(),
        file_id: reg.file_id.clone(),
        content_type: "application/pdf".to_string(),
        is_public: Some(true),
        size_bytes: 2048,
        ..Default::default()
    }))
    .await
    .expect("finalize_upload");

    // The §8-declared topic for FinalizeUpload (asset_service constant
    // STORAGE_FINALIZED_TOPIC / storage_service TOPIC_FILE_FINALIZED).
    assert_outbox_topic(&pool, "udb.storage.file.finalized.v1").await;
    cleanup_native_service_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_asset_register_emits_declared_topic -- --ignored --nocapture"]
async fn live_asset_register_emits_declared_topic() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    ensure_outbox_table(&pool).await;
    let tenant_id = Uuid::new_v4().to_string();
    let file_id = seed_storage_file(&pool, &tenant_id).await;

    let svc = asset_service(pool.clone()).with_outbox(Some(OUTBOX_RELATION.to_string()));
    svc.register_asset(Request::new(asset_pb::RegisterAssetRequest {
        tenant_id: tenant_id.clone(),
        file_id,
        name: "notes.txt".to_string(),
        media_type: "text".to_string(),
        ..Default::default()
    }))
    .await
    .expect("register_asset");

    assert_outbox_topic(&pool, "udb.asset.asset.registered.v1").await;
    cleanup_native_service_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_webrtc_create_room_emits_declared_topic -- --ignored --nocapture"]
async fn live_webrtc_create_room_emits_declared_topic() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    ensure_outbox_table(&pool).await;
    let tenant_id = Uuid::new_v4().to_string();

    let svc = webrtc_service(pool.clone()).with_outbox(Some(OUTBOX_RELATION.to_string()));
    let room = RoomService::create_room(
        &svc,
        Request::new(webrtc_pb::CreateRoomRequest {
            tenant_id: tenant_id.clone(),
            name: "standup".to_string(),
            max_participants: 10,
            ..Default::default()
        }),
    )
    .await
    .expect("create_room")
    .into_inner();
    assert!(!room.room_id.is_empty());

    assert_outbox_topic(&pool, "udb.webrtc.room.created.v1").await;
    cleanup_native_service_db(&pool).await;
}
