//! Live end-to-end test of the notification event pipeline:
//! notification service → transactional outbox → CDC engine → Apache Kafka
//! (the same Kafka the Spark `*_events_streaming` job consumes).
//!
//! Run (after `docker compose -f docker-compose.integration.yml up -d --wait
//! postgres kafka`):
//!   UDB_LIVE_AUTH_TESTS=1 UDB_INTEGRATION_KAFKA_BROKERS=localhost:59192 \
//!     cargo test --lib notification_events_live -- --ignored --nocapture

use super::support::*;
use crate::proto::udb::core::notification::entity::v1 as notif_entity_pb;
use crate::proto::udb::core::notification::services::v1 as notif_pb;
use crate::proto::udb::core::notification::services::v1::notification_service_server::NotificationService;
use crate::runtime::cdc::{CdcConfig, CdcEngine};
use crate::runtime::metrics::NoopMetrics;
use std::sync::Arc;
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

fn kafka_brokers() -> String {
    std::env::var("UDB_INTEGRATION_KAFKA_BROKERS")
        .or_else(|_| std::env::var("UDB_KAFKA_BROKERS"))
        .unwrap_or_else(|_| "localhost:59192".to_string())
}

#[tokio::test]
#[ignore = "requires live Postgres + Kafka; topic udb.notification.sent.v1 must exist. \
            UDB_LIVE_AUTH_TESTS=1 UDB_INTEGRATION_KAFKA_BROKERS=localhost:59192 \
            cargo test --lib notification_events_live -- --ignored --nocapture"]
async fn notification_event_outbox_to_cdc_to_kafka_end_to_end() {
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

    // Real notification service wired to the real outbox (no in-memory double).
    let tenant_id = seed_default_tenant(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "notify_evt", "CorrectHorse1!").await;
    seed_notification_subscriptions(&pool, &user.user_id, &tenant_id).await;
    let svc = notification_service_with_outbox(pool.clone()).await;

    svc.upsert_template(Request::new(notif_pb::UpsertTemplateRequest {
        event_type: "invoice.created".to_string(),
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        locale: "en".to_string(),
        subject_template: "Invoice".to_string(),
        body_template: "Body".to_string(),
        is_active: true,
        ..Default::default()
    }))
    .await
    .expect("upsert template");

    // ── Assumption 1: SendNotification emits to the outbox ─────────────────
    let sent = svc
        .send_notification(Request::new(notif_pb::SendNotificationRequest {
            event_type: "invoice.created".to_string(),
            recipient_id: user.user_id.clone(),
            recipient_address: user.email.clone(),
            tenant_id: tenant_id.clone(),
            channels: vec![notif_entity_pb::NotificationChannel::Email as i32],
            ..Default::default()
        }))
        .await
        .expect("send notification")
        .into_inner();
    assert_eq!(sent.logs.len(), 1);

    let (event_id, topic, partition_key, payload): (Uuid, String, String, serde_json::Value) =
        sqlx::query_as(
            "SELECT event_id, topic, partition_key, payload FROM udb_system.outbox_events \
             WHERE topic = $1 ORDER BY event_seq DESC LIMIT 1",
        )
        .bind("udb.notification.sent.v1")
        .fetch_one(&pool)
        .await
        .expect("outbox row written by send_notification");
    assert_eq!(topic, "udb.notification.sent.v1");
    assert_eq!(partition_key, user.user_id, "partition key = recipient id");
    assert_eq!(payload["event_type"], "udb.notification.sent.v1");
    assert_eq!(payload["event_id"], event_id.to_string());
    assert_eq!(payload["payload"]["recipient_id"], user.user_id);
    assert_eq!(payload["payload"]["tenant_id"], tenant_id);

    // ── Assumption 2: the CDC engine publishes the outbox row to Kafka ─────
    let brokers = kafka_brokers();
    ensure_kafka_topic(&brokers, "udb.notification.sent.v1").await;
    let cdc_config = CdcConfig {
        outbox_table: "outbox_events".to_string(),
        ..CdcConfig::default()
    };
    let engine = CdcEngine::new(
        pool.clone(),
        None,
        &brokers,
        live_pg_dsn(),
        Arc::new(NoopMetrics),
        cdc_config,
    )
    .expect("build CDC engine");
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
        .set(
            "group.id",
            format!("udb-notif-evt-{}", Uuid::new_v4().simple()),
        )
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("kafka consumer");
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(
            "udb.notification.sent.v1",
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
                    assert_eq!(value["event_type"], "udb.notification.sent.v1");
                    assert_eq!(value["payload"]["recipient_id"], user.user_id);
                    found = true;
                    break;
                }
            }
        }
    }
    assert!(
        found,
        "the NotificationSent event must reach Kafka topic udb.notification.sent.v1"
    );

    cleanup_native_auth_db(&pool).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS udb_system.outbox_events CASCADE")
        .execute(&pool)
        .await;
}

/// A RETRIED notification must land in the transactional outbox stamped
/// `retry=true` — proving the retry path goes through the strict transactional
/// sent-event chokepoint rather than a silent/best-effort no-op.
/// Driven entirely through the real `send_notification` / `retry_notification`
/// RPC handlers; the only raw SQL forces the log row to `FAILED` so retry is
/// eligible (delivery itself isn't exercised here).
#[tokio::test]
#[ignore = "requires live Postgres. \
            UDB_LIVE_AUTH_TESTS=1 \
            cargo test --lib notification_events_live -- --ignored --nocapture"]
async fn live_retry_notification_writes_outbox_event() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live UDB system catalog (CDC journal/DLQ tables)");
    ensure_outbox_table(&pool).await;

    // Real notification service wired to the real outbox (no in-memory double).
    let tenant_id = seed_default_tenant(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "notify_retry", "CorrectHorse1!").await;
    seed_notification_subscriptions(&pool, &user.user_id, &tenant_id).await;
    let svc = notification_service_with_outbox(pool.clone()).await;

    svc.upsert_template(Request::new(notif_pb::UpsertTemplateRequest {
        event_type: "invoice.created".to_string(),
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        locale: "en".to_string(),
        subject_template: "Invoice".to_string(),
        body_template: "Body".to_string(),
        is_active: true,
        ..Default::default()
    }))
    .await
    .expect("upsert template");

    // ── Step 1: a real send creates a log row (and its initial outbox event) ──
    let sent = svc
        .send_notification(Request::new(notif_pb::SendNotificationRequest {
            event_type: "invoice.created".to_string(),
            recipient_id: user.user_id.clone(),
            recipient_address: user.email.clone(),
            tenant_id: tenant_id.clone(),
            channels: vec![notif_entity_pb::NotificationChannel::Email as i32],
            ..Default::default()
        }))
        .await
        .expect("send notification")
        .into_inner();
    assert_eq!(sent.logs.len(), 1);
    let log_id = sent.logs[0].log_id.clone();
    assert!(!log_id.is_empty(), "send must return a log id");

    // ── Step 2: force the log row to FAILED so retry is eligible ─────────────
    // Drive the state through the proto-derived native model so table/column
    // names still come from UDB's own proto catalog (no hardcoded SQL identifiers).
    let log_model = crate::runtime::native_catalog::native_model(
        "udb.core.notification.entity.v1.NotificationLog",
        &["log_id", "status", "error_message"],
    );
    sqlx::query(&format!(
        "UPDATE {rel} SET {status} = 'FAILED', {error} = $2 WHERE {log_id} = $1::UUID",
        rel = log_model.relation,
        status = log_model.q("status"),
        error = log_model.q("error_message"),
        log_id = log_model.q("log_id"),
    ))
    .bind(&log_id)
    .bind("live delivery failure")
    .execute(&pool)
    .await
    .expect("mark notification failed through native model");

    // ── Step 3: retry via the REAL RPC, WITH the x-tenant-id metadata header ─
    // CRITICAL: `retry_notification` returns PermissionDenied without this header
    // (it requires tenant-scoped metadata), so omitting it would let the test pass
    // without ever reaching the outbox — the capability-lie trap. Attach it exactly
    // like the sibling `notification_live` retry tests do.
    let mut retry_req = Request::new(notif_pb::RetryNotificationRequest {
        log_id: log_id.clone(),
        ..Default::default()
    });
    retry_req
        .metadata_mut()
        .insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    let retried = svc
        .retry_notification(retry_req)
        .await
        .expect("retry notification")
        .into_inner()
        .log
        .expect("retried log");
    assert_eq!(retried.log_id, log_id);
    assert_eq!(retried.retry_count, 1, "retry bumps retry_count");
    assert_eq!(
        retried.status,
        notif_entity_pb::NotificationStatus::Pending as i32,
        "retry re-queues the notification to PENDING"
    );

    // ── Assertion: the NEWEST sent-event row is the retry, stamped retry=true ─
    let payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM udb_system.outbox_events \
         WHERE topic = $1 ORDER BY event_seq DESC LIMIT 1",
    )
    .bind("udb.notification.sent.v1")
    .fetch_one(&pool)
    .await
    .expect("outbox row written by retry_notification");
    assert_eq!(
        payload["payload"]["retry"], true,
        "the retried notification's outbox payload must be stamped retry=true"
    );
    assert_eq!(
        payload["payload"]["log_id"], log_id,
        "the retry outbox event must reference the retried log row"
    );
    let channels = payload["payload"]["channels"]
        .as_array()
        .expect("retry outbox payload carries a channels array");
    assert_eq!(
        channels.len(),
        1,
        "the retried single-channel notification's carrier must have exactly one channel"
    );

    cleanup_native_auth_db(&pool).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS udb_system.outbox_events CASCADE")
        .execute(&pool)
        .await;
}

/// Project scope supplied by a customer SDK in metadata must survive the entire
/// native notification path, and `ReportDelivery` must route from that canonical
/// NotificationLog project rather than a later caller-controlled fallback. This
/// is a real Postgres + outbox regression for both the SendNotification
/// metadata-only project loss and the former chained
/// `native_service_context(..., "").project_id` delivery defect.
#[tokio::test]
#[ignore = "requires live Postgres. \
            UDB_LIVE_AUTH_TESTS=1 \
            cargo test --lib live_report_delivery_uses_stored_project -- --ignored --nocapture"]
async fn live_report_delivery_uses_stored_project() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live UDB system catalog");
    ensure_outbox_table(&pool).await;

    let tenant_id = seed_default_tenant(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "notify_report_project", "CorrectHorse1!").await;
    seed_notification_subscriptions(&pool, &user.user_id, &tenant_id).await;
    let svc = notification_service_with_outbox(pool.clone()).await;
    svc.upsert_template(Request::new(notif_pb::UpsertTemplateRequest {
        event_type: "delivery.project.audit".to_string(),
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        locale: "en".to_string(),
        subject_template: "Project audit".to_string(),
        body_template: "Body".to_string(),
        is_active: true,
        ..Default::default()
    }))
    .await
    .expect("upsert template");

    let stored_project = "project-canonical";
    let mut send = Request::new(notif_pb::SendNotificationRequest {
        event_type: "delivery.project.audit".to_string(),
        recipient_id: user.user_id.clone(),
        recipient_address: user.email.clone(),
        tenant_id: tenant_id.clone(),
        // Match the normal SDK shape: authenticated project metadata is the
        // source of scope and the duplicate protobuf body field is omitted.
        project_id: String::new(),
        channels: vec![notif_entity_pb::NotificationChannel::Email as i32],
        ..Default::default()
    });
    send.metadata_mut()
        .insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    send.metadata_mut().insert(
        "x-udb-project-id",
        stored_project.parse().expect("project metadata"),
    );
    let sent = svc
        .send_notification(send)
        .await
        .expect("send notification")
        .into_inner();
    assert_eq!(sent.logs[0].project_id, stored_project);
    let log_id = sent.logs[0].log_id.clone();

    let mut wrong_channel = Request::new(notif_pb::ReportDeliveryRequest {
        tenant_id: tenant_id.clone(),
        log_id: log_id.clone(),
        channel: notif_entity_pb::NotificationChannel::Sms as i32,
        provider: "live-fixture".to_string(),
        status: notif_entity_pb::NotificationStatus::Delivered as i32,
        ..Default::default()
    });
    wrong_channel
        .metadata_mut()
        .insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    let mismatch = svc
        .report_delivery(wrong_channel)
        .await
        .expect_err("a delivery report cannot relabel the stored channel");
    assert_eq!(mismatch.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        mismatch.message(),
        "channel does not match notification log"
    );

    let mut report = Request::new(notif_pb::ReportDeliveryRequest {
        tenant_id: tenant_id.clone(),
        log_id: log_id.clone(),
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        provider: "live-fixture".to_string(),
        status: notif_entity_pb::NotificationStatus::Delivered as i32,
        provider_message_id: "provider-1".to_string(),
        ..Default::default()
    });
    report
        .metadata_mut()
        .insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    report.metadata_mut().insert(
        "x-udb-project-id",
        "project-attacker-controlled"
            .parse()
            .expect("project metadata"),
    );
    svc.report_delivery(report)
        .await
        .expect("report delivery through real handler");

    let payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM udb_system.outbox_events \
         WHERE topic = 'udb.notification.delivery.delivered.v1' \
           AND partition_key = $1 ORDER BY event_seq DESC LIMIT 1",
    )
    .bind(&log_id)
    .fetch_one(&pool)
    .await
    .expect("delivery outbox event");
    assert_eq!(payload["project_id"], stored_project);
    assert_eq!(payload["payload"]["project_id"], stored_project);
    let legacy_payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM udb_system.outbox_events \
         WHERE topic = 'udb.notification.delivered.v1' \
           AND partition_key = $1 ORDER BY event_seq DESC LIMIT 1",
    )
    .bind(&tenant_id)
    .fetch_one(&pool)
    .await
    .expect("legacy delivery outbox alias");
    assert_eq!(legacy_payload["project_id"], stored_project);
    assert_eq!(legacy_payload["payload"]["project_id"], stored_project);
    assert_eq!(legacy_payload["payload"]["log_id"], log_id);
    let attempt = crate::runtime::native_catalog::native_model(
        "udb.core.notification.entity.v1.NotificationDeliveryAttempt",
        &["notification_id", "channel"],
    );
    let relabeled_attempts: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {rel} WHERE {notification_id} = $1::UUID AND {channel} = 'SMS'",
        rel = attempt.relation,
        notification_id = attempt.q("notification_id"),
        channel = attempt.q("channel"),
    ))
    .bind(&log_id)
    .fetch_one(&pool)
    .await
    .expect("count relabeled delivery attempts");
    assert_eq!(relabeled_attempts, 0);

    cleanup_native_auth_db(&pool).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS udb_system.outbox_events CASCADE")
        .execute(&pool)
        .await;
}

/// A configured outbox failure must roll back the delivery-attempt row and the
/// parent log transition. Returning success with only one of those writes was a
/// false acknowledgement and caused durable state/event divergence.
#[tokio::test]
#[ignore = "requires live Postgres. \
            UDB_LIVE_AUTH_TESTS=1 \
            cargo test --lib live_report_delivery_rolls_back_on_outbox_failure -- --ignored --nocapture"]
async fn live_report_delivery_rolls_back_on_outbox_failure() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live UDB system catalog");
    ensure_outbox_table(&pool).await;

    let tenant_id = seed_default_tenant(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "notify_report_rollback", "CorrectHorse1!").await;
    seed_notification_subscriptions(&pool, &user.user_id, &tenant_id).await;
    let setup_svc = notification_service_with_outbox(pool.clone()).await;
    setup_svc
        .upsert_template(Request::new(notif_pb::UpsertTemplateRequest {
            event_type: "delivery.atomic.audit".to_string(),
            channel: notif_entity_pb::NotificationChannel::Email as i32,
            locale: "en".to_string(),
            subject_template: "Atomic audit".to_string(),
            body_template: "Body".to_string(),
            is_active: true,
            ..Default::default()
        }))
        .await
        .expect("upsert template");
    let sent = setup_svc
        .send_notification(Request::new(notif_pb::SendNotificationRequest {
            event_type: "delivery.atomic.audit".to_string(),
            recipient_id: user.user_id.clone(),
            recipient_address: user.email.clone(),
            tenant_id: tenant_id.clone(),
            project_id: "project-atomic".to_string(),
            channels: vec![notif_entity_pb::NotificationChannel::Email as i32],
            ..Default::default()
        }))
        .await
        .expect("send notification")
        .into_inner();
    let log_id = sent.logs[0].log_id.clone();

    let broken_svc = notification_service(pool.clone()).await.with_outbox(Some(
        "\"udb_system\".\"missing_notification_outbox\"".to_string(),
    ));
    let mut report = Request::new(notif_pb::ReportDeliveryRequest {
        tenant_id: tenant_id.clone(),
        log_id: log_id.clone(),
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        provider: "live-fixture".to_string(),
        status: notif_entity_pb::NotificationStatus::Delivered as i32,
        ..Default::default()
    });
    report
        .metadata_mut()
        .insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    let error = broken_svc
        .report_delivery(report)
        .await
        .expect_err("missing outbox must fail the atomic report");
    assert_eq!(error.code(), tonic::Code::Internal);

    let log = crate::runtime::native_catalog::native_model(
        "udb.core.notification.entity.v1.NotificationLog",
        &["log_id", "status"],
    );
    let status: String = sqlx::query_scalar(&format!(
        "SELECT {status}::TEXT FROM {rel} WHERE {log_id} = $1::UUID",
        rel = log.relation,
        status = log.q("status"),
        log_id = log.q("log_id"),
    ))
    .bind(&log_id)
    .fetch_one(&pool)
    .await
    .expect("read notification status");
    assert_eq!(status, "PENDING", "parent transition must roll back");
    let attempt = crate::runtime::native_catalog::native_model(
        "udb.core.notification.entity.v1.NotificationDeliveryAttempt",
        &["notification_id"],
    );
    let attempts: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {rel} WHERE {notification_id} = $1::UUID",
        rel = attempt.relation,
        notification_id = attempt.q("notification_id"),
    ))
    .bind(&log_id)
    .fetch_one(&pool)
    .await
    .expect("count delivery attempts");
    assert_eq!(attempts, 0, "attempt upsert must roll back");

    cleanup_native_auth_db(&pool).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS udb_system.outbox_events CASCADE")
        .execute(&pool)
        .await;
}

/// A multi-channel send is one customer request. Failure while preparing a later
/// channel must not leave earlier channel logs behind, and a configured outbox
/// failure must roll the entire typed log batch back.
#[tokio::test]
#[ignore = "requires live Postgres. \
            UDB_LIVE_AUTH_TESTS=1 \
            cargo test --lib live_send_notification_is_atomic_across_channels_and_outbox -- --ignored --nocapture"]
async fn live_send_notification_is_atomic_across_channels_and_outbox() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live UDB system catalog");
    ensure_outbox_table(&pool).await;

    let tenant_id = seed_default_tenant(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "notify_send_atomic", "CorrectHorse1!").await;
    seed_notification_subscriptions(&pool, &user.user_id, &tenant_id).await;
    let setup_svc = notification_service_with_outbox(pool.clone()).await;
    let event_type = "send.atomic.audit";
    setup_svc
        .upsert_template(Request::new(notif_pb::UpsertTemplateRequest {
            event_type: event_type.to_string(),
            channel: notif_entity_pb::NotificationChannel::Email as i32,
            locale: "en".to_string(),
            subject_template: "Atomic send".to_string(),
            body_template: "Body".to_string(),
            is_active: true,
            ..Default::default()
        }))
        .await
        .expect("upsert email template");

    let partial_error = setup_svc
        .send_notification(Request::new(notif_pb::SendNotificationRequest {
            event_type: event_type.to_string(),
            recipient_id: user.user_id.clone(),
            recipient_address: user.email.clone(),
            tenant_id: tenant_id.clone(),
            channels: vec![
                notif_entity_pb::NotificationChannel::Email as i32,
                notif_entity_pb::NotificationChannel::Sms as i32,
            ],
            ..Default::default()
        }))
        .await
        .expect_err("missing second-channel template must fail the whole send");
    assert_eq!(partial_error.code(), tonic::Code::NotFound);

    let log = crate::runtime::native_catalog::native_model(
        "udb.core.notification.entity.v1.NotificationLog",
        &["tenant_id", "event_type"],
    );
    let count_sql = format!(
        "SELECT COUNT(*) FROM {rel} WHERE {tenant_id} = $1 AND {event_type} = $2",
        rel = log.relation,
        tenant_id = log.q("tenant_id"),
        event_type = log.q("event_type"),
    );
    let partial_count: i64 = sqlx::query_scalar(&count_sql)
        .bind(&tenant_id)
        .bind(event_type)
        .fetch_one(&pool)
        .await
        .expect("count logs after later-channel failure");
    assert_eq!(
        partial_count, 0,
        "later-channel failure must not partially persist"
    );

    let broken_svc = notification_service(pool.clone()).await.with_outbox(Some(
        "\"udb_system\".\"missing_notification_outbox\"".to_string(),
    ));
    let outbox_error = broken_svc
        .send_notification(Request::new(notif_pb::SendNotificationRequest {
            event_type: event_type.to_string(),
            recipient_id: user.user_id.clone(),
            recipient_address: user.email.clone(),
            tenant_id: tenant_id.clone(),
            project_id: "project-send-atomic".to_string(),
            channels: vec![notif_entity_pb::NotificationChannel::Email as i32],
            ..Default::default()
        }))
        .await
        .expect_err("missing outbox must fail the atomic send");
    assert_eq!(outbox_error.code(), tonic::Code::Internal);
    let outbox_count: i64 = sqlx::query_scalar(&count_sql)
        .bind(&tenant_id)
        .bind(event_type)
        .fetch_one(&pool)
        .await
        .expect("count logs after outbox failure");
    assert_eq!(
        outbox_count, 0,
        "outbox failure must roll the log batch back"
    );

    cleanup_native_auth_db(&pool).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS udb_system.outbox_events CASCADE")
        .execute(&pool)
        .await;
}

/// The FAILED→PENDING transition, attempt reset, and retry event must either all
/// commit or all roll back. A missing outbox is a deterministic failure point.
#[tokio::test]
#[ignore = "requires live Postgres. \
            UDB_LIVE_AUTH_TESTS=1 \
            cargo test --lib live_retry_notification_rolls_back_on_outbox_failure -- --ignored --nocapture"]
async fn live_retry_notification_rolls_back_on_outbox_failure() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live UDB system catalog");
    ensure_outbox_table(&pool).await;

    let tenant_id = seed_default_tenant(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "notify_retry_atomic", "CorrectHorse1!").await;
    seed_notification_subscriptions(&pool, &user.user_id, &tenant_id).await;
    let setup_svc = notification_service_with_outbox(pool.clone()).await;
    let event_type = "retry.atomic.audit";
    setup_svc
        .upsert_template(Request::new(notif_pb::UpsertTemplateRequest {
            event_type: event_type.to_string(),
            channel: notif_entity_pb::NotificationChannel::Email as i32,
            locale: "en".to_string(),
            subject_template: "Atomic retry".to_string(),
            body_template: "Body".to_string(),
            is_active: true,
            ..Default::default()
        }))
        .await
        .expect("upsert retry template");
    let sent = setup_svc
        .send_notification(Request::new(notif_pb::SendNotificationRequest {
            event_type: event_type.to_string(),
            recipient_id: user.user_id.clone(),
            recipient_address: user.email.clone(),
            tenant_id: tenant_id.clone(),
            channels: vec![notif_entity_pb::NotificationChannel::Email as i32],
            ..Default::default()
        }))
        .await
        .expect("seed failed retry candidate")
        .into_inner();
    let log_id = sent.logs[0].log_id.clone();

    let log = crate::runtime::native_catalog::native_model(
        "udb.core.notification.entity.v1.NotificationLog",
        &["log_id", "status", "retry_count"],
    );
    sqlx::query(&format!(
        "UPDATE {rel} SET {status} = 'FAILED' WHERE {log_id} = $1::UUID",
        rel = log.relation,
        status = log.q("status"),
        log_id = log.q("log_id"),
    ))
    .bind(&log_id)
    .execute(&pool)
    .await
    .expect("mark notification failed");

    let broken_svc = notification_service(pool.clone()).await.with_outbox(Some(
        "\"udb_system\".\"missing_notification_outbox\"".to_string(),
    ));
    let mut retry = Request::new(notif_pb::RetryNotificationRequest {
        log_id: log_id.clone(),
        ..Default::default()
    });
    retry
        .metadata_mut()
        .insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    let error = broken_svc
        .retry_notification(retry)
        .await
        .expect_err("missing outbox must fail the atomic retry");
    assert_eq!(error.code(), tonic::Code::Internal);

    let (status, retry_count): (String, i32) = sqlx::query_as(&format!(
        "SELECT {status}::TEXT, {retry_count} FROM {rel} WHERE {log_id} = $1::UUID",
        rel = log.relation,
        status = log.q("status"),
        retry_count = log.q("retry_count"),
        log_id = log.q("log_id"),
    ))
    .bind(&log_id)
    .fetch_one(&pool)
    .await
    .expect("read rolled-back retry state");
    assert_eq!(status, "FAILED", "retry state transition must roll back");
    assert_eq!(retry_count, 0, "manual retry counter must roll back");

    cleanup_native_auth_db(&pool).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS udb_system.outbox_events CASCADE")
        .execute(&pool)
        .await;
}
