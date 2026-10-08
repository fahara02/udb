use super::support::*;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::udb::core::notification::entity::v1 as notif_entity_pb;
use crate::proto::udb::core::notification::services::v1 as notif_pb;
use crate::proto::udb::core::notification::services::v1::notification_service_server::NotificationService;
use tonic::Request;

#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn authn_reset_codes_are_queued_but_never_returned_by_notification_apis() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let notifier = std::sync::Arc::new(notification_service(pool.clone()).await);
    let authn = authn_service(pool.clone()).with_system_notifier(notifier.clone());
    let tenant = uuid::Uuid::new_v4().to_string();
    let user = create_verified_user_in(
        &authn,
        "auth-code-notification",
        "CorrectHorse1!",
        &tenant,
        "default",
    )
    .await;
    // No operator template exists: all three shipped code templates must
    // queue real, fully rendered delivery material through this notifier.
    let builtin_reset = authn
        .forgot_password(Request::new(authn_pb::ForgotPasswordRequest {
            identifier: user.email.clone(),
            ..Default::default()
        }))
        .await
        .expect("reset using shipped template")
        .into_inner();
    let builtin_otp = authn
        .send_otp(Request::new(authn_pb::SendOtpRequest {
            user_id: user.user_id.clone(),
            otp_type: crate::proto::udb::core::authn::entity::v1::OtpType::SensitiveOperation
                as i32,
            ..Default::default()
        }))
        .await
        .expect("OTP using shipped template")
        .into_inner();
    let log_model = crate::runtime::native_catalog::native_model(
        "udb.core.notification.entity.v1.NotificationLog",
        &["log_id", "rendered_body"],
    );
    for (event_type, code) in [
        ("authn.email_verification", String::new()),
        (
            "authn.password_reset",
            issued_test_otp_code(&builtin_reset.otp_id),
        ),
        ("authn.otp", issued_test_otp_code(&builtin_otp.otp_id)),
    ] {
        let listed = notifier
            .list_notifications(Request::new(notif_pb::ListNotificationsRequest {
                recipient_id: user.user_id.clone(),
                tenant_id: user.tenant_id.clone(),
                project_id: user.project_id.clone(),
                event_type: event_type.into(),
                ..Default::default()
            }))
            .await
            .expect("list shipped-template notification")
            .into_inner();
        assert_eq!(listed.logs.len(), 1, "{event_type}");
        let log = &listed.logs[0];
        assert!(
            log.template_id.is_empty(),
            "compiled template has no database FK"
        );
        let body: String = sqlx::query_scalar(&format!(
            "SELECT {body} FROM {relation} WHERE {id} = $1::UUID",
            body = log_model.q("rendered_body"),
            relation = log_model.relation,
            id = log_model.q("log_id"),
        ))
        .bind(&log.log_id)
        .fetch_one(&pool)
        .await
        .expect("actual built-in delivery body");
        assert!(!body.contains("{{"), "all template variables rendered");
        assert!(body.contains("expires in"), "expiry is communicated");
        if !code.is_empty() {
            assert!(body.contains(&code), "worker receives actual issued code");
            assert!(
                !format!("{listed:?}").contains(&code),
                "list redacts actual code"
            );
        }
    }
    notifier
        .upsert_template(Request::new(notif_pb::UpsertTemplateRequest {
            event_type: "authn.password_reset".into(),
            channel: notif_entity_pb::NotificationChannel::Email as i32,
            subject_template: "Reset code {{code}}".into(),
            body_template:
                "Hello {{user_name}}, use {{code}} within {{expires_in_minutes}} minutes".into(),
            tenant_id: user.tenant_id.clone(),
            project_id: user.project_id.clone(),
            is_active: true,
            ..Default::default()
        }))
        .await
        .expect("password-reset template");
    let reset = authn
        .forgot_password(Request::new(authn_pb::ForgotPasswordRequest {
            identifier: user.email.clone(),
            ..Default::default()
        }))
        .await
        .expect("forgot password")
        .into_inner();
    let code = issued_test_otp_code(&reset.otp_id);
    let list = notifier
        .list_notifications(Request::new(notif_pb::ListNotificationsRequest {
            recipient_id: user.user_id.clone(),
            tenant_id: user.tenant_id.clone(),
            project_id: user.project_id.clone(),
            event_type: "authn.password_reset".into(),
            ..Default::default()
        }))
        .await
        .expect("list reset notification")
        .into_inner();
    assert_eq!(
        list.logs.len(),
        2,
        "both shipped and tenant reset templates queue notifications"
    );
    assert!(
        !format!("{list:?}").contains(&code),
        "list cannot disclose the reset code"
    );
    let log_id = &list
        .logs
        .iter()
        .find(|log| !log.template_id.is_empty())
        .expect("tenant template takes precedence over the shipped default")
        .log_id;
    let model = crate::runtime::native_catalog::native_model(
        "udb.core.notification.entity.v1.NotificationLog",
        &["log_id", "rendered_subject", "rendered_body"],
    );
    let raw: (String, String) = sqlx::query_as(&format!(
        "SELECT {subject}, {body} FROM {relation} WHERE {id} = $1::UUID",
        subject = model.q("rendered_subject"),
        body = model.q("rendered_body"),
        relation = model.relation,
        id = model.q("log_id"),
    ))
    .bind(log_id)
    .fetch_one(&pool)
    .await
    .expect("queued delivery material");
    assert!(
        raw.0.contains(&code) && raw.1.contains(&code),
        "worker receives the actual code in subject and body"
    );
    let mut get = Request::new(notif_pb::GetNotificationRequest {
        log_id: log_id.clone(),
    });
    get.metadata_mut()
        .insert("x-tenant-id", user.tenant_id.parse().unwrap());
    get.metadata_mut()
        .insert("x-udb-project-id", user.project_id.parse().unwrap());
    let got = notifier
        .get_notification(get)
        .await
        .expect("get reset notification")
        .into_inner();
    assert!(
        !format!("{got:?}").contains(&code),
        "get cannot disclose the reset code"
    );
    let sent = notifier
        .send_notification(Request::new(notif_pb::SendNotificationRequest {
            event_type: "authn.password_reset".into(),
            recipient_id: user.user_id.clone(),
            recipient_address: user.email.clone(),
            tenant_id: user.tenant_id.clone(),
            project_id: user.project_id.clone(),
            variables: std::collections::HashMap::from([
                ("code".into(), code.clone()),
                ("user_name".into(), "Test".into()),
                ("expires_in_minutes".into(), "5".into()),
            ]),
            ..Default::default()
        }))
        .await
        .expect("send reset notification")
        .into_inner();
    assert!(
        !format!("{sent:?}").contains(&code),
        "send response cannot disclose the reset code"
    );
    // Once a terminal delivery has scrubbed its material, manual retry must
    // not resurrect a placeholder notification. The user requests a fresh code.
    let placeholder = "[redacted: this notification carried an authentication code]";
    sqlx::query(&format!(
        "UPDATE {relation} SET {status} = 'FAILED', {body} = $2 WHERE {id} = $1::UUID",
        relation = model.relation,
        status = model.q("status"),
        body = model.q("rendered_body"),
        id = model.q("log_id"),
    ))
    .bind(log_id)
    .bind(placeholder)
    .execute(&pool)
    .await
    .expect("terminal scrubbed notification");
    let mut retry = Request::new(notif_pb::RetryNotificationRequest {
        log_id: log_id.clone(),
        ..Default::default()
    });
    retry
        .metadata_mut()
        .insert("x-tenant-id", user.tenant_id.parse().unwrap());
    retry
        .metadata_mut()
        .insert("x-udb-project-id", user.project_id.parse().unwrap());
    let refused_retry = notifier
        .retry_notification(retry)
        .await
        .expect_err("scrubbed auth material cannot be retried");
    assert_eq!(refused_retry.code(), tonic::Code::FailedPrecondition);
    // An invite queues the password-reset template, refuses login with its
    // typed pending-state reason, and is activated by the actual emailed code.
    let invite_name = format!("notification-invite-{}", uuid::Uuid::new_v4());
    let invited = authn
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: invite_name.clone(),
            email: format!("{invite_name}@example.test"),
            tenant_id: user.tenant_id.clone(),
            project_id: user.project_id.clone(),
            full_name: "Invited User".into(),
            password_setup_required: true,
            ..Default::default()
        }))
        .await
        .expect("create password-setup invite")
        .into_inner();
    let invite_user = invited.user.expect("invited user");
    let pending = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: invite_user.email.clone(),
            password: "CorrectHorse1!".into(),
            ..Default::default()
        }))
        .await
        .expect_err("invite cannot log in before password setup");
    assert_eq!(
        crate::runtime::error_reasons::reason_of(&pending).as_deref(),
        Some("UDB_PASSWORD_SETUP_REQUIRED")
    );
    let invite_code = issued_test_otp_code(&invited.otp_id);
    let invite_logs = notifier
        .list_notifications(Request::new(notif_pb::ListNotificationsRequest {
            recipient_id: invite_user.user_id.clone(),
            tenant_id: user.tenant_id.clone(),
            project_id: user.project_id.clone(),
            event_type: "authn.password_reset".into(),
            ..Default::default()
        }))
        .await
        .expect("invite is routed to the password-reset template")
        .into_inner();
    assert_eq!(invite_logs.logs.len(), 1);
    let delivered_body: String = sqlx::query_scalar(&format!(
        "SELECT {body} FROM {relation} WHERE {id} = $1::UUID",
        body = model.q("rendered_body"),
        relation = model.relation,
        id = model.q("log_id"),
    ))
    .bind(&invite_logs.logs[0].log_id)
    .fetch_one(&pool)
    .await
    .expect("queued invite body");
    assert!(delivered_body.contains(&invite_code));
    authn
        .reset_password(Request::new(authn_pb::ResetPasswordRequest {
            otp_id: invited.otp_id,
            code: invite_code,
            new_password: "NewCorrectHorse1!".into(),
            ..Default::default()
        }))
        .await
        .expect("complete invite using actual queued code");
    let session = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: invite_user.email,
            password: "NewCorrectHorse1!".into(),
            ..Default::default()
        }))
        .await
        .expect("invited user can log in after setup")
        .into_inner();
    assert_eq!(session.user_id, invite_user.user_id);
    cleanup_native_auth_db(&pool).await;
}

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

    // SUPPRESSED is an opt-out terminal state, not a retry source. Exercise the
    // public preference + send + retry handlers so the runtime and descriptor
    // cannot drift back toward an unsafe SUPPRESSED → PENDING transition.
    svc.set_preference(Request::new(notif_pb::SetPreferenceRequest {
        user_id: user.user_id.clone(),
        tenant_id: tenant_id.clone(),
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        event_type: event_type.to_string(),
        is_opted_out: true,
        ..Default::default()
    }))
    .await
    .expect("opt out of invoice email");
    let suppressed = svc
        .send_notification(Request::new(notif_pb::SendNotificationRequest {
            event_type: event_type.to_string(),
            recipient_id: user.user_id.clone(),
            recipient_address: user.email.clone(),
            tenant_id: tenant_id.clone(),
            channels: vec![notif_entity_pb::NotificationChannel::Email as i32],
            ..Default::default()
        }))
        .await
        .expect("send suppressed notification")
        .into_inner()
        .logs
        .into_iter()
        .next()
        .expect("suppressed log");
    assert_eq!(
        suppressed.status,
        notif_entity_pb::NotificationStatus::Suppressed as i32
    );
    let mut suppressed_retry = Request::new(notif_pb::RetryNotificationRequest {
        log_id: suppressed.log_id.clone(),
        ..Default::default()
    });
    suppressed_retry
        .metadata_mut()
        .insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    let error = svc
        .retry_notification(suppressed_retry)
        .await
        .expect_err("suppressed notification must remain terminal");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    let mut get_suppressed = Request::new(notif_pb::GetNotificationRequest {
        log_id: suppressed.log_id.clone(),
    });
    get_suppressed
        .metadata_mut()
        .insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    let stored = svc
        .get_notification(get_suppressed)
        .await
        .expect("read suppressed notification")
        .into_inner()
        .log
        .expect("stored suppressed log");
    assert_eq!(
        stored.status,
        notif_entity_pb::NotificationStatus::Suppressed as i32
    );
    assert_eq!(stored.retry_count, 0);

    cleanup_native_auth_db(&pool).await;
}
