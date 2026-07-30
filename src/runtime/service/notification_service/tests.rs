//! Unit guards for the native `NotificationService`: request-body cross-tenant
//! rejection, the field-violation shapes, the typed policy/schema/capability/
//! internal details with their `error-reason` trailers, the per-channel send
//! decision + emit-set computation, the hybrid-tenant template selection scope,
//! and the master-plan 9.13 delivery-adapter guards (credential redaction, the
//! reused SSRF guard, provider-config parsing). Copied verbatim from the former
//! god file's `tenant_scope_tests`; imports are explicit (no `use super::*`).

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};
use uuid::Uuid;

use crate::proto::udb::core::notification::entity::v1 as notif_entity_pb;
use crate::proto::udb::core::notification::services::v1 as notif_pb;
use crate::proto::udb::core::notification::services::v1::notification_service_server::NotificationService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::NotificationServiceImpl;
use super::config::{
    NOT_RETRYABLE_STATE, TEMPLATE_NOT_FOUND, VARIABLE_MISSING, delivery_event_topic,
};
#[cfg(feature = "http-client")]
use super::delivery::parse_notification_delivery_providers_json;
use super::delivery::{NotificationDeliveryProvider, ProviderAuth, ProviderCredential};
use super::errors::{
    notification_capability_status, notification_internal_status,
    notification_not_retryable_status, notification_required_field,
    notification_schema_not_found_status, notification_template_not_found_status,
    notification_tenant_metadata_required_status, status_with_reason,
};
use super::model::{
    channel_send_decision, deliverable_channels, notification_delivery_payload, status_to_db,
    template_locale_or_default, template_model, template_selection_sql,
};

fn decode_detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("typed detail trailer is present");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

fn assert_schema_not_found_detail(
    status: &Status,
    operation: &str,
    schema_code: &str,
    message: &str,
) {
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Schema as i32);
    assert_eq!(detail.backend, "notification");
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.capability_required, schema_code);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Internal as i32);
    assert_eq!(detail.backend, "notification");
    assert_eq!(detail.operation, operation);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

#[test]
fn notification_internal_status_carries_typed_detail() {
    assert_internal_detail(
        &notification_internal_status(
            "decode_notification_log",
            "decode notification log failed: missing column",
        ),
        "decode_notification_log",
        "decode notification log failed: missing column",
    );
}

/// A caller scoped to tenant-a must not send/list for another tenant by putting
/// a foreign tenant_id in the request BODY; the scope guard rejects this before
/// any pool/DB access (no Postgres needed).
#[tokio::test]
async fn list_notifications_rejects_cross_tenant_body() {
    let svc = NotificationServiceImpl::new(); // no pool, no channels (admit no-op)
    let mut request = Request::new(notif_pb::ListNotificationsRequest {
        tenant_id: "tenant-b".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .list_notifications(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn send_notification_missing_event_type_carries_field_violation() {
    let svc = NotificationServiceImpl::new(); // no runtime/pool; validation runs first
    let mut request = Request::new(notif_pb::SendNotificationRequest {
        tenant_id: "tenant-a".to_string(),
        event_type: " ".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

    let err = svc
        .send_notification(request)
        .await
        .expect_err("missing event_type must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "event_type is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "event_type");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty notification event type"
    );
}

#[tokio::test]
async fn report_delivery_missing_log_id_carries_field_violation() {
    let svc = NotificationServiceImpl::new(); // no pool; validation runs first
    let mut request = Request::new(notif_pb::ReportDeliveryRequest {
        tenant_id: "tenant-a".to_string(),
        log_id: " ".to_string(),
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        status: notif_entity_pb::NotificationStatus::Delivered as i32,
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

    let err = svc
        .report_delivery(request)
        .await
        .expect_err("missing log_id must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "log_id is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "log_id");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty notification log id"
    );
}

#[tokio::test]
async fn report_delivery_unspecified_status_carries_field_violation() {
    let svc = NotificationServiceImpl::new(); // no pool; validation runs first
    let mut request = Request::new(notif_pb::ReportDeliveryRequest {
        tenant_id: "tenant-a".to_string(),
        log_id: Uuid::new_v4().to_string(),
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        status: notif_entity_pb::NotificationStatus::Unspecified as i32,
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

    let err = svc
        .report_delivery(request)
        .await
        .expect_err("unspecified delivery status must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "a terminal delivery status (SENT|DELIVERED|FAILED|PENDING) is required"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "status");
    assert_eq!(
        detail.field_violations[0].description,
        "must be one of SENT, DELIVERED, FAILED, or PENDING"
    );
}

#[tokio::test]
async fn upsert_template_missing_event_type_carries_field_violation() {
    let svc = NotificationServiceImpl::new(); // no pool; validation runs first
    let request = Request::new(notif_pb::UpsertTemplateRequest {
        event_type: " ".to_string(),
        ..Default::default()
    });

    let err = svc
        .upsert_template(request)
        .await
        .expect_err("missing event_type must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "event_type is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "event_type");
}

#[test]
fn template_locale_too_long_carries_field_violation() {
    let err = template_locale_or_default("en-US-extra")
        .expect_err("oversized locale must fail before template lookup");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "locale must be 10 characters or fewer");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "locale");
    assert_eq!(
        detail.field_violations[0].description,
        "must be 10 characters or fewer"
    );
}

#[test]
fn set_preference_missing_tenant_status_carries_field_violation() {
    let err = notification_required_field(
        "tenant_id",
        "must be a non-empty tenant id",
        "tenant_id is required",
    );

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "tenant_id is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "tenant_id");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty tenant id"
    );
}

#[test]
fn delivery_channels_exclude_suppressed_logs() {
    let logs = vec![
        notif_entity_pb::NotificationLog {
            channel: notif_entity_pb::NotificationChannel::Email as i32,
            status: notif_entity_pb::NotificationStatus::Suppressed as i32,
            ..Default::default()
        },
        notif_entity_pb::NotificationLog {
            channel: notif_entity_pb::NotificationChannel::Sms as i32,
            status: notif_entity_pb::NotificationStatus::Pending as i32,
            ..Default::default()
        },
    ];

    assert_eq!(
        deliverable_channels(&logs),
        vec![notif_entity_pb::NotificationChannel::Sms as i32]
    );
}

#[test]
fn delivery_payload_marks_retry_events() {
    let payload = notification_delivery_payload(
        "log-1",
        "REVIEW_ASSIGNED",
        "user-1",
        "tenant-a",
        "project-a",
        &[notif_entity_pb::NotificationChannel::Email as i32],
        true,
    );

    assert_eq!(payload["retry"], true);
    assert_eq!(payload["channels"][0], "EMAIL");
    // TEST-46: the retry emit threads the retried log's id and EXACTLY its
    // one channel alongside the retry marker — the same
    // `notification_delivery_payload` call `retry_notification` makes via
    // `emit_sent_event(pool, &log.log_id, …, &[log.channel], true)`. The
    // outbox row itself (UPDATE … RETURNING → enqueue) is asserted by the
    // env-gated live pipeline suite.
    assert_eq!(payload["log_id"], "log-1");
    assert_eq!(payload["channels"].as_array().map(Vec::len), Some(1));
}

#[test]
fn variable_missing_status_carries_reason_and_field_violation() {
    let status = status_with_reason(
        crate::runtime::executor_utils::invalid_argument_fields(
            "template variable 'ResourceName' is required but was not provided",
            [(
                "variables.ResourceName",
                "template variable is required but was not provided",
            )],
        ),
        VARIABLE_MISSING,
        &[("error-variable", "ResourceName")],
    );

    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        status
            .metadata()
            .get("error-reason")
            .and_then(|v| v.to_str().ok()),
        Some(VARIABLE_MISSING)
    );
    assert_eq!(
        status
            .metadata()
            .get("error-variable")
            .and_then(|v| v.to_str().ok()),
        Some("ResourceName")
    );
    let detail = decode_detail(&status);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "variables.ResourceName");
    assert_eq!(
        detail.field_violations[0].description,
        "template variable is required but was not provided"
    );
}

#[test]
fn notification_missing_setup_capabilities_carry_typed_detail() {
    for (operation, capability, message) in [
        (
            "native_entity_dispatch",
            "runtime_native_entity_dispatch",
            "notification service requires runtime native entity dispatch",
        ),
        (
            "postgres_store",
            "postgres_store",
            "notification service requires a Postgres-backed store (no PG pool configured)",
        ),
    ] {
        let err = notification_capability_status(operation, capability, message);
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(err.message(), message);
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "notification");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability);
        assert!(!detail.retryable);
    }
}

#[test]
fn notification_not_found_statuses_carry_schema_detail() {
    for (operation, schema_code, message) in [
        (
            "get_notification",
            "notification_not_found",
            "notification not found",
        ),
        (
            "get_template",
            "notification_template_not_found",
            "template not found",
        ),
        (
            "get_preference",
            "notification_preference_not_found",
            "preference not found",
        ),
    ] {
        assert_schema_not_found_detail(
            &notification_schema_not_found_status(operation, schema_code, message),
            operation,
            schema_code,
            message,
        );
    }
}

#[test]
fn notification_template_not_found_status_keeps_reason_and_schema_detail() {
    let err = notification_template_not_found_status(
        "send_notification",
        "no active notification template for event 'A' channel 'EMAIL' locale 'en-US'",
    );
    assert_eq!(
        err.metadata()
            .get("error-reason")
            .and_then(|value| value.to_str().ok()),
        Some(TEMPLATE_NOT_FOUND)
    );
    assert_schema_not_found_detail(
        &err,
        "send_notification",
        "notification_template_not_found",
        "no active notification template for event 'A' channel 'EMAIL' locale 'en-US'",
    );
}

#[test]
fn retry_not_retryable_state_carries_policy_detail_and_reason() {
    let err = notification_not_retryable_status();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "notification not found or not in a retryable (FAILED) state"
    );
    assert_eq!(
        err.metadata()
            .get("error-reason")
            .and_then(|value| value.to_str().ok()),
        Some(NOT_RETRYABLE_STATE)
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.operation, "retry_notification");
    assert_eq!(detail.policy_decision_id, "notification_not_retryable");
    assert!(!detail.retryable);
}

#[test]
fn tenant_metadata_required_status_carries_permission_denied_policy_detail() {
    for operation in [
        "get_notification",
        "retry_notification",
        "get_template",
        "list_templates",
    ] {
        let err = notification_tenant_metadata_required_status(operation);
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_eq!(err.message(), "tenant-scoped metadata is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, "tenant_metadata_required");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }
}

/// TEST-44: an opted-out (recipient, channel) produces the SUPPRESSED row
/// decision and is excluded from the outbox emit set — driving the REAL
/// per-channel decision (`channel_send_decision`) and the REAL emit-set
/// computation (`deliverable_channels`) that `send_notification` runs; the
/// DB-side preference lookup feeding `opted_out` is covered by the
/// env-gated live suite.
#[test]
fn opted_out_channel_is_suppressed_and_excluded_from_emit_set() {
    use notif_entity_pb::{NotificationChannel as C, NotificationStatus as S};

    assert_eq!(
        channel_send_decision(true),
        ("SUPPRESSED", S::Suppressed as i32)
    );
    assert_eq!(channel_send_decision(false), ("PENDING", S::Pending as i32));

    // Per-channel logs exactly as send_notification records them: EMAIL is
    // opted out, SMS is not — only SMS may enter the emit set.
    let logs: Vec<notif_entity_pb::NotificationLog> = [(C::Email, true), (C::Sms, false)]
        .into_iter()
        .map(|(channel, opted_out)| notif_entity_pb::NotificationLog {
            channel: channel as i32,
            status: channel_send_decision(opted_out).1,
            ..Default::default()
        })
        .collect();
    assert_eq!(deliverable_channels(&logs), vec![C::Sms as i32]);
}

/// TEST-45: template tenant scoping — the real selection query (the one
/// `get_template` executes) only admits the platform-global default
/// (`tenant_id IS NULL`) or the CALLER's bound tenant (`$4`), so with two
/// overrides in the table a tenant-B override can never be selected for a
/// tenant-A caller, and the caller's own override outranks the global
/// default.
#[test]
fn template_selection_scopes_overrides_to_the_caller_tenant() {
    let m = template_model();
    let sql = template_selection_sql(&m);
    let tenant = m.q("tenant_id");
    assert!(
        sql.contains(&format!("({tenant} IS NULL OR {tenant} = $4)")),
        "selection must only admit the global default or the caller's tenant: {sql}"
    );
    assert!(
        sql.contains(&format!("ORDER BY {tenant} NULLS LAST LIMIT 1")),
        "the caller's own override must outrank the global default: {sql}"
    );
    assert_eq!(
        sql.matches("$4").count(),
        1,
        "the bound caller tenant must be the only tenant-shaped input: {sql}"
    );
}

// ── master-plan 9.13 delivery adapters ─────────────────────────────────────

/// TEST-9.13a: a caller scoped to tenant-a cannot ReportDelivery for tenant-b
/// by putting a foreign tenant_id in the request BODY — rejected before any
/// pool/DB access (no Postgres needed).
#[tokio::test]
async fn report_delivery_rejects_cross_tenant_body() {
    let svc = NotificationServiceImpl::new(); // no pool, no channels (admit no-op)
    let mut request = Request::new(notif_pb::ReportDeliveryRequest {
        tenant_id: "tenant-b".to_string(),
        log_id: Uuid::new_v4().to_string(),
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        status: notif_entity_pb::NotificationStatus::Delivered as i32,
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .report_delivery(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

/// TEST-9.13b: the terminal delivery event name is
/// `udb.notification.delivery.<status>.v1`, derived from the SAME status
/// mapping `ReportDelivery` records, and an unknown/0 status fails closed.
#[test]
fn delivery_event_topic_names_the_status() {
    use notif_entity_pb::NotificationStatus as S;
    assert_eq!(status_to_db(S::Delivered as i32), "DELIVERED");
    assert_eq!(status_to_db(S::Failed as i32), "FAILED");
    assert_eq!(status_to_db(S::Unspecified as i32), "UNSPECIFIED");
    assert_eq!(
        delivery_event_topic(status_to_db(S::Delivered as i32)),
        "udb.notification.delivery.delivered.v1"
    );
    assert_eq!(
        delivery_event_topic(status_to_db(S::Failed as i32)),
        "udb.notification.delivery.failed.v1"
    );
    assert_eq!(
        delivery_event_topic(status_to_db(S::Sent as i32)),
        "udb.notification.delivery.sent.v1"
    );
}

/// TEST-9.13c: provider credentials never appear in a Debug string — neither
/// the decrypted credential in flight nor the wrapped credential in the
/// provider config (the redaction doctrine).
#[test]
fn provider_credentials_never_appear_in_debug() {
    let canary = "udb-provider-secret-9f3a2c";
    let credential = ProviderCredential(canary.to_string());
    let rendered = format!("{credential:?}");
    assert!(
        !rendered.contains(canary),
        "ProviderCredential Debug leaked the secret: {rendered}"
    );
    assert!(rendered.contains("[redacted]"));

    let provider = NotificationDeliveryProvider {
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        provider: "SES".to_string(),
        endpoint_url: "https://email.example.com/send".to_string(),
        wrapped_credential: canary.to_string(),
        body_template: None,
        auth: ProviderAuth::Bearer,
        idempotency_header: "Idempotency-Key".to_string(),
        message_id_header: None,
        message_id_json_path: None,
    };
    let rendered = format!("{provider:?}");
    assert!(
        !rendered.contains(canary),
        "NotificationDeliveryProvider Debug leaked the wrapped credential: {rendered}"
    );
    assert!(rendered.contains("[redacted]"));
}

/// TEST-9.13d: a provider URL pointing at an internal/cloud-metadata address is
/// SSRF-rejected at delivery time by the SAME guard the webhook lane uses
/// (reused, not re-implemented); a public https provider URL is accepted.
#[test]
fn provider_url_ssrf_rejected() {
    use crate::runtime::service::webhook_service::validate_webhook_target_url;
    for blocked in [
        "https://169.254.169.254/send",  // cloud metadata
        "https://127.0.0.1/send",        // loopback
        "http://email.example.com/send", // cleartext
    ] {
        let err = validate_webhook_target_url(blocked)
            .expect_err(&format!("provider URL must be SSRF-rejected: {blocked}"));
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "for {blocked}");
    }
    validate_webhook_target_url("https://email.example.com/send")
        .expect("a public https provider URL should be accepted");
}

#[cfg(feature = "http-client")]
#[test]
fn delivery_provider_json_accepts_names_and_redacts_credentials() {
    let providers = parse_notification_delivery_providers_json(
        r#"[
            {
                "channel": "EMAIL",
                "provider": "SES",
                "endpoint_url": "https://notify.example.com/email",
                "wrapped_credential": "udb-aead:v1:secret"
            },
            {
                "channel": "sms",
                "provider": "TWILIO",
                "url": "https://notify.example.com/sms",
                "credential": "udb-aead:v1:sms"
            },
            {
                "channel": "bad",
                "provider": "",
                "endpoint_url": "",
                "wrapped_credential": ""
            }
        ]"#,
    );
    assert_eq!(providers.len(), 2);
    assert_eq!(
        providers[0].channel,
        notif_entity_pb::NotificationChannel::Email as i32
    );
    assert_eq!(
        providers[1].channel,
        notif_entity_pb::NotificationChannel::Sms as i32
    );
    let rendered = format!("{:?}", providers[0]);
    assert!(!rendered.contains("udb-aead:v1:secret"));
    assert!(rendered.contains("[redacted]"));
    // Absent `auth` defaults to Bearer; the idempotency header defaults on.
    assert_eq!(providers[0].auth, ProviderAuth::Bearer);
    assert_eq!(providers[0].idempotency_header, "Idempotency-Key");
}

/// Each supported `auth` scheme parses into its typed [`ProviderAuth`], carrying
/// only the NON-secret scheme parameters (the secret stays the sealed
/// `wrapped_credential`). Unknown/absent schemes fall back to Bearer, and the
/// idempotency / message-id knobs parse alongside.
#[cfg(feature = "http-client")]
#[test]
fn delivery_provider_json_parses_each_auth_scheme() {
    let providers = parse_notification_delivery_providers_json(
        r#"[
            {
                "channel": "EMAIL", "provider": "SES",
                "endpoint_url": "https://n.example.com/e", "wrapped_credential": "c1",
                "auth": "bearer"
            },
            {
                "channel": "SMS", "provider": "TWILIO",
                "endpoint_url": "https://n.example.com/s", "wrapped_credential": "c2",
                "auth": { "scheme": "api_key", "header": "X-Api-Key" },
                "idempotency_header": "X-Idem",
                "message_id_header": "X-Message-Id"
            },
            {
                "channel": "PUSH", "provider": "FCM",
                "endpoint_url": "https://n.example.com/p", "wrapped_credential": "c3",
                "auth": { "scheme": "basic", "username": "svc" },
                "idempotency_header": ""
            },
            {
                "channel": "WEBHOOK", "provider": "HOOK",
                "endpoint_url": "https://n.example.com/h", "wrapped_credential": "c4",
                "auth": { "scheme": "hmac", "header": "X-Signature" },
                "message_id_json_path": "data.id"
            },
            {
                "channel": "IN_APP", "provider": "INAPP",
                "endpoint_url": "https://n.example.com/i", "wrapped_credential": "c5",
                "auth": { "scheme": "totally-unknown" }
            }
        ]"#,
    );
    assert_eq!(providers.len(), 5);

    assert_eq!(providers[0].auth, ProviderAuth::Bearer);

    assert_eq!(
        providers[1].auth,
        ProviderAuth::ApiKey {
            header: "X-Api-Key".to_string()
        }
    );
    assert_eq!(providers[1].idempotency_header, "X-Idem");
    assert_eq!(
        providers[1].message_id_header.as_deref(),
        Some("X-Message-Id")
    );

    assert_eq!(
        providers[2].auth,
        ProviderAuth::Basic {
            username: "svc".to_string()
        }
    );
    // An explicit empty string opts out of the idempotency header.
    assert_eq!(providers[2].idempotency_header, "");

    assert_eq!(
        providers[3].auth,
        ProviderAuth::Hmac {
            header: "X-Signature".to_string()
        }
    );
    assert_eq!(
        providers[3].message_id_json_path.as_deref(),
        Some("data.id")
    );

    // An unrecognized scheme fails safe to Bearer.
    assert_eq!(providers[4].auth, ProviderAuth::Bearer);
}

#[test]
fn log_transition_allowed_prev_is_a_forward_only_state_machine() {
    use super::store::log_transition_allowed_prev;
    // Success terminals move the log forward from an allowed prior state...
    assert_eq!(log_transition_allowed_prev("SENT"), &["PENDING"]);
    assert_eq!(
        log_transition_allowed_prev("DELIVERED"),
        &["PENDING", "SENT"]
    );
    // ...FAILED is handled by the retry model, not this forward transition, so it
    // yields no allowed prior state (callers can pass any status safely).
    assert!(log_transition_allowed_prev("FAILED").is_empty());
    assert!(log_transition_allowed_prev("SUPPRESSED").is_empty());
    assert!(log_transition_allowed_prev("PENDING").is_empty());
}
