//! Unit guards for the native `NotificationService`: request-body cross-tenant
//! rejection, the field-violation shapes, the typed policy/schema/capability/
//! internal details with their `error-reason` trailers, the per-channel send
//! decision + transactional event computation, the hybrid-tenant template selection scope,
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
    legacy_delivery_event_topic,
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
use super::events::{
    NotificationDeliveryEvent, delivery_event_payloads, sent_event_transaction_op,
    suppressed_event_transaction_op,
};
use super::model::{
    channel_send_decision, delivery_attempt_model, log_model, notification_sent_payload,
    preference_model, status_to_db, template_locale_or_default, template_model,
    template_selection_sql,
};
use super::store::{
    recipient_opted_out_sql, reset_delivery_attempts_sql, suppress_log_if_pending_sql,
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
async fn report_delivery_unspecified_channel_carries_field_violation() {
    let svc = NotificationServiceImpl::new(); // no pool; validation runs first
    let mut request = Request::new(notif_pb::ReportDeliveryRequest {
        tenant_id: "tenant-a".to_string(),
        log_id: Uuid::new_v4().to_string(),
        channel: notif_entity_pb::NotificationChannel::Unspecified as i32,
        provider: "fixture".to_string(),
        status: notif_entity_pb::NotificationStatus::Delivered as i32,
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

    let err = svc
        .report_delivery(request)
        .await
        .expect_err("unspecified delivery channel must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "channel is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "channel");
}

#[tokio::test]
async fn report_delivery_empty_provider_carries_field_violation() {
    let svc = NotificationServiceImpl::new(); // no pool; validation runs first
    let mut request = Request::new(notif_pb::ReportDeliveryRequest {
        tenant_id: "tenant-a".to_string(),
        log_id: Uuid::new_v4().to_string(),
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        provider: " ".to_string(),
        status: notif_entity_pb::NotificationStatus::Delivered as i32,
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

    let err = svc
        .report_delivery(request)
        .await
        .expect_err("empty delivery provider must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "provider is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "provider");
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
fn delivery_payload_marks_retry_events() {
    let log = notif_entity_pb::NotificationLog {
        log_id: "log-1".to_string(),
        template_id: "template-1".to_string(),
        event_type: "REVIEW_ASSIGNED".to_string(),
        recipient_id: "user-1".to_string(),
        tenant_id: "tenant-a".to_string(),
        project_id: "project-a".to_string(),
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        correlation_id: "corr-1".to_string(),
        ..Default::default()
    };
    let payload = notification_sent_payload(&log, true);

    assert_eq!(payload["retry"], true);
    assert_eq!(payload["channel"], "EMAIL");
    assert_eq!(payload["channels"][0], "EMAIL");
    assert_eq!(payload["log_id"], "log-1");
    assert_eq!(payload["template_id"], "template-1");
    assert_eq!(payload["recipient_ref"], "user-1");
    assert_eq!(payload["recipient_id"], "user-1");
    assert_eq!(payload["correlation_id"], "corr-1");
    assert_eq!(payload["channels"].as_array().map(Vec::len), Some(1));
}

#[test]
fn initial_sent_event_transaction_step_preserves_customer_scope() {
    let log = notif_entity_pb::NotificationLog {
        log_id: "log-atomic".to_string(),
        template_id: "template-atomic".to_string(),
        event_type: "ORDER_READY".to_string(),
        recipient_id: "recipient-1".to_string(),
        tenant_id: "tenant-a".to_string(),
        project_id: "project-a".to_string(),
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        correlation_id: "corr-atomic".to_string(),
        ..Default::default()
    };
    let op = sent_event_transaction_op(Some("\"udb_system\".\"outbox_events\""), &log)
        .expect("build sent-event transaction step")
        .expect("configured outbox produces a transaction step");

    let crate::runtime::core::native_store::NativeEntityTransactionOp::Outbox(write) = op else {
        panic!("sent-event helper must return the canonical outbox operation");
    };
    assert_eq!(write.topic, "udb.notification.sent.v1");
    assert_eq!(write.partition_key, "recipient-1");
    assert_eq!(write.envelope["tenant_id"], "tenant-a");
    assert_eq!(write.envelope["project_id"], "project-a");
    assert_eq!(write.envelope["payload"]["log_id"], "log-atomic");
    assert_eq!(write.envelope["payload"]["recipient_ref"], "recipient-1");
    assert_eq!(write.envelope["payload"]["channel"], "EMAIL");
    assert_eq!(write.envelope["payload"]["retry"], false);
}

#[test]
fn suppression_event_transaction_step_is_tenant_partitioned_and_redacted() {
    let log = notif_entity_pb::NotificationLog {
        log_id: "log-suppressed".to_string(),
        template_id: "template-suppressed".to_string(),
        event_type: "ORDER_READY".to_string(),
        recipient_id: "recipient-1".to_string(),
        recipient_address: "secret@example.invalid".to_string(),
        tenant_id: "tenant-a".to_string(),
        project_id: "project-a".to_string(),
        channel: notif_entity_pb::NotificationChannel::Email as i32,
        rendered_body: "private body".to_string(),
        ..Default::default()
    };
    let op = suppressed_event_transaction_op(Some("\"udb_system\".\"outbox_events\""), &log)
        .expect("build suppression event transaction step")
        .expect("configured outbox produces a transaction step");

    let crate::runtime::core::native_store::NativeEntityTransactionOp::Outbox(write) = op else {
        panic!("suppression helper must return the canonical outbox operation");
    };
    assert_eq!(write.topic, "udb.notification.suppressed.v1");
    assert_eq!(write.partition_key, "tenant-a");
    assert_eq!(write.envelope["tenant_id"], "tenant-a");
    assert_eq!(write.envelope["project_id"], "project-a");
    assert_eq!(
        write.envelope["payload"]["suppression_reason"],
        "USER_OPT_OUT"
    );
    let encoded = write.envelope.to_string();
    assert!(!encoded.contains("secret@example.invalid"));
    assert!(!encoded.contains("private body"));
}

#[test]
fn delivery_alias_payloads_match_the_public_v1_messages() {
    let event = NotificationDeliveryEvent {
        log_id: "log-delivery",
        template_id: "template-delivery",
        event_type: "ORDER_READY",
        tenant_id: "tenant-a",
        project_id: "project-a",
        correlation_id: "corr-delivery",
        channel_db: "EMAIL",
        provider: "fixture-provider",
        status_db: "FAILED",
        provider_message_id: "provider-message-1",
        error_detail: "provider timeout",
        retry_attempt: 2,
        will_retry: true,
    };
    let (canonical, failed) = delivery_event_payloads(&event);
    let failed = failed.expect("FAILED has a public compatibility payload");

    assert_eq!(canonical["provider"], "fixture-provider");
    assert_eq!(canonical["provider_message_id"], "provider-message-1");
    assert_eq!(failed["log_id"], "log-delivery");
    assert_eq!(failed["template_id"], "template-delivery");
    assert_eq!(failed["event_type"], "ORDER_READY");
    assert_eq!(failed["channel"], "EMAIL");
    assert_eq!(failed["tenant_id"], "tenant-a");
    assert_eq!(failed["project_id"], "project-a");
    assert_eq!(failed["error_code"], "DELIVERY_FAILED");
    assert_eq!(failed["error_detail"], "provider timeout");
    assert_eq!(failed["retry_attempt"], 2);
    assert_eq!(failed["will_retry"], true);
    assert_eq!(failed["correlation_id"], "corr-delivery");
    assert!(failed["occurred_at"].as_str().is_some());
    assert!(failed.get("provider").is_none());
    assert!(failed.get("provider_message_id").is_none());

    let delivered_event = NotificationDeliveryEvent {
        status_db: "DELIVERED",
        error_detail: "",
        retry_attempt: 0,
        will_retry: false,
        ..event
    };
    let (_, delivered) = delivery_event_payloads(&delivered_event);
    let delivered = delivered.expect("DELIVERED has a public compatibility payload");
    assert_eq!(delivered["log_id"], "log-delivery");
    assert_eq!(delivered["channel"], "EMAIL");
    assert_eq!(delivered["tenant_id"], "tenant-a");
    assert_eq!(delivered["project_id"], "project-a");
    assert_eq!(delivered["correlation_id"], "corr-delivery");
    assert!(delivered["occurred_at"].as_str().is_some());
    assert!(delivered.get("template_id").is_none());
    assert!(delivered.get("provider").is_none());
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
/// per-channel decision (`channel_send_decision`) and the same PENDING-log
/// selection that `send_notification` uses for per-channel sent events; the
/// DB-side preference lookup feeding `opted_out` is covered by the env-gated
/// live suite.
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
    let deliverable_channels: Vec<i32> = logs
        .iter()
        .filter(|log| log.status == S::Pending as i32)
        .map(|log| log.channel)
        .collect();
    assert_eq!(deliverable_channels, vec![C::Sms as i32]);
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
    assert_eq!(
        legacy_delivery_event_topic(status_to_db(S::Failed as i32)),
        Some("udb.notification.failed.v1")
    );
    assert_eq!(
        legacy_delivery_event_topic(status_to_db(S::Delivered as i32)),
        Some("udb.notification.delivered.v1")
    );
    assert_eq!(
        legacy_delivery_event_topic(status_to_db(S::Sent as i32)),
        None,
        "the initial notification.sent event is not a delivery-status alias"
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

/// The manual-retry attempt reset grants a FRESH bounded-retry budget: it zeroes
/// `attempt_count` (so the next worker pass can't instantly exhaust and re-emit the
/// dead-letter event) and is tenant-scoped. This is the fix for the double
/// dead-letter after RetryNotification — without the reset a resurrected log is
/// re-scanned with `attempt_count` still at the ceiling.
#[test]
fn reset_delivery_attempts_grants_a_fresh_budget_tenant_scoped() {
    let m = delivery_attempt_model();
    let sql = reset_delivery_attempts_sql(&m);
    // The budget is reset to zero — a full retry cycle, never an instant exhaust.
    assert!(
        sql.contains(&format!("{} = 0", m.q("attempt_count"))),
        "reset must zero attempt_count: {sql}"
    );
    assert!(
        sql.contains(&format!("{} = 'PENDING'", m.q("status"))),
        "reset must clear the terminal FAILED attempt state: {sql}"
    );
    // Scoped by notification id AND tenant (never resets another tenant's rows).
    assert!(
        sql.contains(&format!("{} = $1::UUID", m.q("notification_id"))),
        "reset must key on the notification id: {sql}"
    );
    assert!(
        sql.contains(&format!("{} = $2", m.q("tenant_id"))),
        "reset must be tenant-scoped: {sql}"
    );
}

/// The delivery-time opt-out lookup prefers the event-specific preference over the
/// tenant-wide default, and is scoped to (user_id, tenant, channel) — so an opt-out
/// recorded after send suppresses delivery on the worker/retry path.
#[test]
fn recipient_opted_out_prefers_event_specific_and_is_scoped() {
    let m = preference_model();
    let sql = recipient_opted_out_sql(&m);
    // Event-specific preference OR the tenant-wide ('') default...
    let event_type = m.q("event_type");
    assert!(
        sql.contains(&format!("({event_type} = $4 OR {event_type} = '')")),
        "opt-out must consider the event-specific and global preferences: {sql}"
    );
    // ...with the event-specific row preferred (sorts first).
    assert!(
        sql.contains(&format!("ORDER BY ({event_type} = $4) DESC")),
        "the event-specific preference must outrank the tenant-wide default: {sql}"
    );
    // Keyed on the recipient user id, tenant, and channel.
    assert!(
        sql.contains(&format!("{} = $1::UUID", m.q("user_id"))),
        "{sql}"
    );
    assert!(sql.contains(&format!("{} = $2", m.q("tenant_id"))), "{sql}");
    assert!(sql.contains(&format!("{} = $3", m.q("channel"))), "{sql}");
}

/// Suppression only ever moves a still-PENDING log to SUPPRESSED (never downgrades
/// a SENT/DELIVERED notification), and is tenant-scoped.
#[test]
fn suppress_only_moves_pending_logs_tenant_scoped() {
    let m = log_model();
    let sql = suppress_log_if_pending_sql(&m);
    assert!(
        sql.contains(&format!("{} = 'SUPPRESSED'", m.q("status"))),
        "suppression must set the terminal SUPPRESSED state: {sql}"
    );
    assert!(
        sql.contains(&format!("{} = 'PENDING'", m.q("status"))),
        "suppression must be gated on PENDING (never downgrade SENT/DELIVERED): {sql}"
    );
    assert!(
        sql.contains(&format!("{} = $1::UUID", m.q("log_id"))),
        "{sql}"
    );
    assert!(sql.contains(&format!("{} = $2", m.q("tenant_id"))), "{sql}");
}
