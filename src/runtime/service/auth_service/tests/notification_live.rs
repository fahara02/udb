use super::support::*;
use crate::proto::udb::core::notification::entity::v1 as notif_entity_pb;
use crate::proto::udb::core::notification::services::v1 as notif_pb;
use crate::proto::udb::core::notification::services::v1::notification_service_server::NotificationService;
use tonic::Request;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_notification_native_schema_from_proto -- --ignored --nocapture"]
async fn live_postgres_notification_native_schema_from_proto() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    assert_native_table_columns(
        &pool,
        "udb.core.notification.entity.v1.NotificationTemplate",
        &[
            "template_id",
            "event_type",
            "channel",
            "subject_template",
            "body_template",
            "locale",
            "is_active",
            "created_at",
            "updated_at",
            "deleted_at",
            "created_by",
            "deleted_by",
        ],
    )
    .await;
    assert_native_table_columns(
        &pool,
        "udb.core.notification.entity.v1.NotificationLog",
        &[
            "log_id",
            "template_id",
            "event_type",
            "channel",
            "recipient_id",
            "recipient_address",
            "tenant_id",
            "project_id",
            "resource_type",
            "resource_id",
            "resource_name",
            "correlation_id",
            "status",
            "error_message",
            "provider_message_id",
            "retry_count",
            "sent_at",
            "delivered_at",
            "created_at",
        ],
    )
    .await;
    assert_native_table_columns(
        &pool,
        "udb.core.notification.entity.v1.NotificationPreference",
        &[
            "preference_id",
            "user_id",
            "tenant_id",
            "channel",
            "event_type",
            "is_opted_out",
            "created_at",
            "updated_at",
            "created_by",
        ],
    )
    .await;

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_notification_service_crud_roundtrip -- --ignored --nocapture"]
async fn live_postgres_notification_service_crud_roundtrip() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let svc = notification_service(pool.clone()).await;

    // Default tenant seed + a real user, then seed subscriptions for every channel.
    let tenant_id = seed_default_tenant(&pool).await;
    let user = create_verified_user(&authn, "notify", "CorrectHorse1!").await;
    seed_notification_subscriptions(&pool, &user.user_id, &tenant_id).await;

    let prefs = svc
        .list_preferences(Request::new(notif_pb::ListPreferencesRequest {
            user_id: user.user_id.clone(),
            tenant_id: tenant_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("list preferences")
        .into_inner();
    assert_eq!(prefs.preferences.len(), 5, "one subscription per channel");

    let email_pref = svc
        .get_preference(Request::new(notif_pb::GetPreferenceRequest {
            user_id: user.user_id.clone(),
            tenant_id: tenant_id.clone(),
            channel: notif_entity_pb::NotificationChannel::Email as i32,
            event_type: String::new(),
        }))
        .await
        .expect("get preference")
        .into_inner()
        .preference
        .expect("preference");
    assert!(!email_pref.is_opted_out);

    // Template upsert is idempotent on (event_type, channel, locale).
    let event_type = "invoice.created";
    for body in ["v1 body", "v2 body"] {
        svc.upsert_template(Request::new(notif_pb::UpsertTemplateRequest {
            event_type: event_type.to_string(),
            channel: notif_entity_pb::NotificationChannel::Email as i32,
            locale: "en".to_string(),
            subject_template: "Invoice notice".to_string(),
            body_template: body.to_string(),
            is_active: true,
            ..Default::default()
        }))
        .await
        .expect("upsert template");
    }
    let mut get_template_req = Request::new(notif_pb::GetTemplateRequest {
        event_type: event_type.to_string(),
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        locale: "en".to_string(),
    });
    get_template_req
        .metadata_mut()
        .insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    let template = svc
        .get_template(get_template_req)
        .await
        .expect("get template")
        .into_inner()
        .template
        .expect("template");
    assert_eq!(template.body_template, "v2 body");

    // Send records one PENDING NotificationLog on the EMAIL channel.
    let sent = svc
        .send_notification(Request::new(notif_pb::SendNotificationRequest {
            event_type: event_type.to_string(),
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

    let mut get_req = Request::new(notif_pb::GetNotificationRequest {
        log_id: log_id.clone(),
    });
    get_req
        .metadata_mut()
        .insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    let got = svc
        .get_notification(get_req)
        .await
        .expect("get notification")
        .into_inner()
        .log
        .expect("log");
    assert_eq!(
        got.status,
        notif_entity_pb::NotificationStatus::Pending as i32
    );
    assert_eq!(got.retry_count, 0);

    let listed = svc
        .list_notifications(Request::new(notif_pb::ListNotificationsRequest {
            tenant_id: tenant_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("list notifications")
        .into_inner();
    assert!(listed.logs.iter().any(|l| l.log_id == log_id));

    let mut pending_retry_req = Request::new(notif_pb::RetryNotificationRequest {
        log_id: log_id.clone(),
        ..Default::default()
    });
    pending_retry_req
        .metadata_mut()
        .insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    let pending_retry = svc
        .retry_notification(pending_retry_req)
        .await
        .expect_err("pending notification should not be retried");
    assert_eq!(pending_retry.code(), tonic::Code::FailedPrecondition);

    // Retry is only valid after delivery has marked the log FAILED. The test
    // drives that state through the proto-derived native model so table/column
    // names still come from UDB's own proto catalog.
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

    // Retry bumps retry_count and re-queues a failed notification.
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
        .expect("log");
    assert_eq!(retried.retry_count, 1);
    assert_eq!(
        retried.status,
        notif_entity_pb::NotificationStatus::Pending as i32
    );

    let stats = svc
        .get_delivery_stats(Request::new(notif_pb::GetDeliveryStatsRequest {
            tenant_id: tenant_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("delivery stats")
        .into_inner();
    assert_eq!(stats.total_failed, 0);

    cleanup_native_auth_db(&pool).await;
}
