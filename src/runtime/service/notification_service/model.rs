//! Manifest-driven native models, enum<->db converters, read projections, JSON and
//! `PgRow` decoders, the `{{placeholder}}` renderer, and the per-channel send
//! decision for the native `NotificationService`. Extracted verbatim from the
//! former god file — the enum short-names, the projection column order, and the
//! row/JSON mappings are byte-for-byte identical.

use sqlx::Row;
use tonic::Status;

use crate::proto::udb::core::notification::entity::v1 as notif_entity_pb;
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::config::{
    DELIVERY_ATTEMPT_MSG, LOG_MSG, PREFERENCE_MSG, TEMPLATE_LOCALE_MAX_CHARS, TEMPLATE_MSG,
};
use super::errors::{notification_internal_status, notification_required_field};

pub(crate) fn log_model() -> NativeModel {
    native_model(
        LOG_MSG,
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
            "rendered_subject",
            "rendered_body",
        ],
    )
}

pub(crate) fn template_model() -> NativeModel {
    native_model(
        TEMPLATE_MSG,
        &[
            "template_id",
            "event_type",
            "channel",
            "subject_template",
            "body_template",
            "locale",
            "is_active",
            "created_by",
            // Hybrid tenant model (F4.3): NULL = platform-global default,
            // non-null = per-tenant override.
            "tenant_id",
        ],
    )
}

pub(crate) fn preference_model() -> NativeModel {
    native_model(
        PREFERENCE_MSG,
        &[
            "preference_id",
            "user_id",
            "tenant_id",
            "channel",
            "event_type",
            "is_opted_out",
            "created_by",
        ],
    )
}

// ── enum<->db (stored as VARCHAR via proto_enum) ──────────────────────────────

pub(crate) fn channel_to_db(value: i32) -> &'static str {
    use notif_entity_pb::NotificationChannel as C;
    match C::try_from(value).unwrap_or(C::Unspecified) {
        C::Email => "EMAIL",
        C::Sms => "SMS",
        C::Push => "PUSH",
        C::InApp => "IN_APP",
        C::Webhook => "WEBHOOK",
        C::Unspecified => "UNSPECIFIED",
    }
}

pub(crate) fn channel_from_db(value: &str) -> i32 {
    use notif_entity_pb::NotificationChannel as C;
    match value {
        "EMAIL" => C::Email as i32,
        "SMS" => C::Sms as i32,
        "PUSH" => C::Push as i32,
        "IN_APP" => C::InApp as i32,
        "WEBHOOK" => C::Webhook as i32,
        _ => C::Unspecified as i32,
    }
}

pub(crate) fn status_from_db(value: &str) -> i32 {
    use notif_entity_pb::NotificationStatus as S;
    match value {
        "PENDING" => S::Pending as i32,
        "SENT" => S::Sent as i32,
        "DELIVERED" => S::Delivered as i32,
        "FAILED" => S::Failed as i32,
        "SUPPRESSED" => S::Suppressed as i32,
        _ => S::Unspecified as i32,
    }
}

/// Inverse of [`status_from_db`]: a delivery status enum → its stored short name.
/// `UNSPECIFIED` is returned for an unknown/0 status so the `ReportDelivery`
/// handler can fail closed on a caller that omits a terminal status.
pub(crate) fn status_to_db(value: i32) -> &'static str {
    use notif_entity_pb::NotificationStatus as S;
    match S::try_from(value).unwrap_or(S::Unspecified) {
        S::Pending => "PENDING",
        S::Sent => "SENT",
        S::Delivered => "DELIVERED",
        S::Failed => "FAILED",
        S::Suppressed => "SUPPRESSED",
        S::Unspecified => "UNSPECIFIED",
    }
}

/// Per-channel send decision: an opted-out (recipient, channel) preference is
/// recorded as a SUPPRESSED log row — kept for audit/delivery stats but never
/// part of the delivery emit set — while everything else queues as PENDING.
/// Returns `(db_status, proto_status)`; the single decision point
/// `send_notification` applies per channel.
pub(crate) fn channel_send_decision(opted_out: bool) -> (&'static str, i32) {
    if opted_out {
        (
            "SUPPRESSED",
            notif_entity_pb::NotificationStatus::Suppressed as i32,
        )
    } else {
        (
            "PENDING",
            notif_entity_pb::NotificationStatus::Pending as i32,
        )
    }
}

/// Payload for the public per-channel `NotificationSentEvent`. Keep the former
/// `recipient_id` and `channels[]` keys as additive compatibility fields for
/// raw-JSON consumers while also satisfying the descriptor's singular
/// `recipient_ref`/`channel` schema.
pub(crate) fn notification_sent_payload(
    log: &notif_entity_pb::NotificationLog,
    retry: bool,
) -> serde_json::Value {
    let channel = channel_to_db(log.channel);
    serde_json::json!({
        "log_id": log.log_id,
        "template_id": log.template_id,
        "event_type": log.event_type,
        "channel": channel,
        "channels": [channel],
        "recipient_ref": log.recipient_id,
        "recipient_id": log.recipient_id,
        "tenant_id": log.tenant_id,
        "project_id": log.project_id,
        "correlation_id": log.correlation_id,
        "occurred_at": chrono::Utc::now().to_rfc3339(),
        "retry": retry,
    })
}

/// Payload for the public per-channel `NotificationSuppressedEvent`. Recipient
/// addresses and rendered content are deliberately excluded.
pub(crate) fn notification_suppressed_payload(
    log: &notif_entity_pb::NotificationLog,
    reason: &str,
) -> serde_json::Value {
    serde_json::json!({
        "log_id": log.log_id,
        "template_id": log.template_id,
        "event_type": log.event_type,
        "channel": channel_to_db(log.channel),
        "recipient_ref": log.recipient_id,
        "tenant_id": log.tenant_id,
        "project_id": log.project_id,
        "suppression_reason": reason,
        "correlation_id": log.correlation_id,
        "occurred_at": chrono::Utc::now().to_rfc3339(),
    })
}

pub(crate) fn template_locale_or_default(locale: &str) -> Result<String, Status> {
    let locale = locale.trim();
    if locale.is_empty() {
        return Ok("en".to_string());
    }
    if locale.chars().count() > TEMPLATE_LOCALE_MAX_CHARS {
        return Err(notification_required_field(
            "locale",
            "must be 10 characters or fewer",
            "locale must be 10 characters or fewer",
        ));
    }
    Ok(locale.to_string())
}

pub(crate) fn json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

pub(crate) fn json_string_field(
    row: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> String {
    row.get(field)
        .and_then(|value| match value {
            serde_json::Value::String(value) => Some(value.clone()),
            serde_json::Value::Number(value) => Some(value.to_string()),
            serde_json::Value::Bool(value) => Some(value.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

pub(crate) fn json_bool_field(
    row: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> bool {
    row.get(field)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

pub(crate) fn json_i32_field(row: &serde_json::Map<String, serde_json::Value>, field: &str) -> i32 {
    row.get(field)
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or(0)
}

pub(crate) fn json_i64_field(row: &serde_json::Map<String, serde_json::Value>, field: &str) -> i64 {
    row.get(field)
        .and_then(|value| match value {
            serde_json::Value::Number(value) => value.as_i64(),
            serde_json::Value::String(value) => value.parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or(0)
}

pub(crate) fn log_from_json(row: &serde_json::Value) -> notif_entity_pb::NotificationLog {
    let row = json_object(row);
    notif_entity_pb::NotificationLog {
        log_id: json_string_field(row, "log_id"),
        template_id: json_string_field(row, "template_id"),
        event_type: json_string_field(row, "event_type"),
        channel: channel_from_db(&json_string_field(row, "channel")),
        recipient_id: json_string_field(row, "recipient_id"),
        recipient_address: json_string_field(row, "recipient_address"),
        tenant_id: json_string_field(row, "tenant_id"),
        project_id: json_string_field(row, "project_id"),
        resource_type: json_string_field(row, "resource_type"),
        resource_id: json_string_field(row, "resource_id"),
        resource_name: json_string_field(row, "resource_name"),
        correlation_id: json_string_field(row, "correlation_id"),
        status: status_from_db(&json_string_field(row, "status")),
        error_message: json_string_field(row, "error_message"),
        provider_message_id: json_string_field(row, "provider_message_id"),
        retry_count: json_i32_field(row, "retry_count"),
        rendered_subject: json_string_field(row, "rendered_subject"),
        rendered_body: json_string_field(row, "rendered_body"),
        ..Default::default()
    }
}

pub(crate) fn preference_from_json_row(
    row: &serde_json::Value,
) -> notif_entity_pb::NotificationPreference {
    let row = json_object(row);
    notif_entity_pb::NotificationPreference {
        preference_id: json_string_field(row, "preference_id"),
        user_id: json_string_field(row, "user_id"),
        tenant_id: json_string_field(row, "tenant_id"),
        channel: channel_from_db(&json_string_field(row, "channel")),
        event_type: json_string_field(row, "event_type"),
        is_opted_out: json_bool_field(row, "is_opted_out"),
        created_by: json_string_field(row, "created_by"),
        ..Default::default()
    }
}

pub(crate) fn template_from_json_row(
    row: &serde_json::Value,
) -> notif_entity_pb::NotificationTemplate {
    let row = json_object(row);
    notif_entity_pb::NotificationTemplate {
        template_id: json_string_field(row, "template_id"),
        event_type: json_string_field(row, "event_type"),
        channel: channel_from_db(&json_string_field(row, "channel")),
        subject_template: json_string_field(row, "subject_template"),
        body_template: json_string_field(row, "body_template"),
        locale: json_string_field(row, "locale"),
        is_active: json_bool_field(row, "is_active"),
        created_by: json_string_field(row, "created_by"),
        tenant_id: json_string_field(row, "tenant_id"),
        ..Default::default()
    }
}

/// Render a `{{placeholder}}` template against the request `variables` map.
/// Substitution is minimal and consistent with the template comment
/// (`'{{.ResourceName}} requires review'`): every `{{ ... }}` token is trimmed,
/// an optional leading `.` is stripped (Go-template dotted form), and the result
/// is looked up in `variables`. A token with no value yields `Err(field_name)`
/// so the caller can fail-closed with `VARIABLE_MISSING` naming that field. No
/// regex dependency: a single forward scan over the bytes.
pub(crate) fn render_template(
    template: &str,
    variables: &std::collections::HashMap<String, String>,
) -> Result<String, String> {
    let mut out = String::with_capacity(template.len());
    let bytes = template.as_bytes();
    let mut i = 0;
    while i < template.len() {
        if i + 1 < template.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            if let Some(rel_end) = template[i + 2..].find("}}") {
                let raw = &template[i + 2..i + 2 + rel_end];
                let key = raw.trim().trim_start_matches('.').trim();
                match variables.get(key) {
                    Some(value) => out.push_str(value),
                    None => return Err(key.to_string()),
                }
                i += 2 + rel_end + 2;
                continue;
            }
        }
        // Not a placeholder start (or unterminated `{{`): copy this char verbatim.
        let ch = template[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    Ok(out)
}

// ── projections + row mappers ─────────────────────────────────────────────────

pub(crate) fn log_select_projection(m: &NativeModel) -> String {
    [
        m.text("log_id"),
        m.text_or_empty("template_id"),
        m.select("event_type"),
        m.text_or_empty("channel"),
        m.text_or_empty("recipient_id"),
        m.text_or_empty("recipient_address"),
        m.text_or_empty("tenant_id"),
        m.text_or_empty("project_id"),
        m.text_or_empty("resource_type"),
        m.text_or_empty("resource_id"),
        m.text_or_empty("resource_name"),
        m.text_or_empty("correlation_id"),
        m.text_or_empty("status"),
        m.text_or_empty("error_message"),
        m.text_or_empty("provider_message_id"),
        m.select("retry_count"),
    ]
    .join(", ")
}

pub(crate) fn log_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<notif_entity_pb::NotificationLog, Status> {
    let map = |e: sqlx::Error| {
        notification_internal_status(
            "decode_notification_log",
            format!("decode notification log failed: {e}"),
        )
    };
    Ok(notif_entity_pb::NotificationLog {
        log_id: row.try_get("log_id").map_err(map)?,
        template_id: row.try_get("template_id").map_err(map)?,
        event_type: row.try_get("event_type").map_err(map)?,
        channel: channel_from_db(&row.try_get::<String, _>("channel").map_err(map)?),
        recipient_id: row.try_get("recipient_id").map_err(map)?,
        recipient_address: row.try_get("recipient_address").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        project_id: row.try_get("project_id").map_err(map)?,
        resource_type: row.try_get("resource_type").map_err(map)?,
        resource_id: row.try_get("resource_id").map_err(map)?,
        resource_name: row.try_get("resource_name").map_err(map)?,
        correlation_id: row.try_get("correlation_id").map_err(map)?,
        status: status_from_db(&row.try_get::<String, _>("status").map_err(map)?),
        error_message: row.try_get("error_message").map_err(map)?,
        provider_message_id: row.try_get("provider_message_id").map_err(map)?,
        retry_count: row.try_get("retry_count").map_err(map)?,
        ..Default::default()
    })
}

pub(crate) fn template_select_projection(m: &NativeModel) -> String {
    [
        m.text("template_id"),
        m.select("event_type"),
        m.text_or_empty("channel"),
        m.text_or_empty("subject_template"),
        m.select("body_template"),
        m.text_or_empty("locale"),
        m.select("is_active"),
        m.text_or_empty("created_by"),
        m.text_or_empty("tenant_id"),
    ]
    .join(", ")
}

/// Build the `GetTemplate` selection query (hybrid tenant model, F4.3):
/// candidate rows are restricted to the platform-global default
/// (`tenant_id IS NULL`) or the CALLER's own tenant override (bound as `$4`) —
/// a foreign tenant's override can never match — and `NULLS LAST` prefers the
/// caller's override over the global default. Extracted so the tenant scoping
/// of the selection is unit-testable without a live Postgres.
#[cfg(test)]
pub(crate) fn template_selection_sql(m: &NativeModel) -> String {
    format!(
        "SELECT {projection} FROM {rel} \
         WHERE {event_type} = $1 AND {channel} = $2 AND {locale} = $3 AND {deleted} IS NULL \
           AND ({tenant_id} IS NULL OR {tenant_id} = $4) \
         ORDER BY {tenant_id} NULLS LAST LIMIT 1",
        projection = template_select_projection(m),
        rel = m.relation,
        event_type = m.q("event_type"),
        channel = m.q("channel"),
        locale = m.q("locale"),
        deleted = m.q("deleted_at"),
        tenant_id = m.q("tenant_id"),
    )
}

pub(crate) fn template_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<notif_entity_pb::NotificationTemplate, Status> {
    let map = |e: sqlx::Error| {
        notification_internal_status("decode_template", format!("decode template failed: {e}"))
    };
    Ok(notif_entity_pb::NotificationTemplate {
        template_id: row.try_get("template_id").map_err(map)?,
        event_type: row.try_get("event_type").map_err(map)?,
        channel: channel_from_db(&row.try_get::<String, _>("channel").map_err(map)?),
        subject_template: row.try_get("subject_template").map_err(map)?,
        body_template: row.try_get("body_template").map_err(map)?,
        locale: row.try_get("locale").map_err(map)?,
        is_active: row.try_get("is_active").map_err(map)?,
        created_by: row.try_get("created_by").map_err(map)?,
        // text_or_empty() coalesces a NULL (global default) tenant_id to "".
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        ..Default::default()
    })
}

pub(crate) fn preference_select_projection(m: &NativeModel) -> String {
    [
        m.text("preference_id"),
        m.text_or_empty("user_id"),
        m.text_or_empty("tenant_id"),
        m.text_or_empty("channel"),
        m.text_or_empty("event_type"),
        m.select("is_opted_out"),
        m.text_or_empty("created_by"),
    ]
    .join(", ")
}

pub(crate) fn preference_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<notif_entity_pb::NotificationPreference, Status> {
    let map = |e: sqlx::Error| {
        notification_internal_status(
            "decode_preference",
            format!("decode preference failed: {e}"),
        )
    };
    Ok(notif_entity_pb::NotificationPreference {
        preference_id: row.try_get("preference_id").map_err(map)?,
        user_id: row.try_get("user_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        channel: channel_from_db(&row.try_get::<String, _>("channel").map_err(map)?),
        event_type: row.try_get("event_type").map_err(map)?,
        is_opted_out: row.try_get("is_opted_out").map_err(map)?,
        created_by: row.try_get("created_by").map_err(map)?,
        ..Default::default()
    })
}

// ── delivery-attempt (9.13) model / projection / mappers ──────────────────────

pub(crate) fn delivery_attempt_model() -> NativeModel {
    native_model(
        DELIVERY_ATTEMPT_MSG,
        &[
            "attempt_id",
            "notification_id",
            "tenant_id",
            "channel",
            "provider",
            "status",
            "attempt_count",
            "last_error",
            "provider_message_id",
            "created_at",
            "updated_at",
        ],
    )
}

pub(crate) fn delivery_attempt_select_projection(m: &NativeModel) -> String {
    [
        m.text("attempt_id"),
        m.text("notification_id"),
        m.text("tenant_id"),
        m.text_or_empty("channel"),
        m.text_or_empty("provider"),
        m.text_or_empty("status"),
        m.select("attempt_count"),
        m.text_or_empty("last_error"),
        m.text_or_empty("provider_message_id"),
        format!(
            "EXTRACT(EPOCH FROM {})::BIGINT AS created_at_epoch",
            m.q("created_at")
        ),
        format!(
            "EXTRACT(EPOCH FROM {})::BIGINT AS updated_at_epoch",
            m.q("updated_at")
        ),
    ]
    .join(", ")
}

pub(crate) fn epoch_to_ts(epoch: Option<i64>) -> Option<prost_types::Timestamp> {
    epoch.map(|seconds| prost_types::Timestamp { seconds, nanos: 0 })
}

pub(crate) fn delivery_attempt_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<notif_entity_pb::NotificationDeliveryAttempt, Status> {
    let map = |e: sqlx::Error| {
        notification_internal_status(
            "decode_delivery_attempt",
            format!("decode delivery attempt failed: {e}"),
        )
    };
    Ok(notif_entity_pb::NotificationDeliveryAttempt {
        attempt_id: row.try_get("attempt_id").map_err(map)?,
        notification_id: row.try_get("notification_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        channel: channel_from_db(&row.try_get::<String, _>("channel").map_err(map)?),
        provider: row.try_get("provider").map_err(map)?,
        status: status_from_db(&row.try_get::<String, _>("status").map_err(map)?),
        attempt_count: row.try_get("attempt_count").map_err(map)?,
        last_error: row.try_get("last_error").map_err(map)?,
        provider_message_id: row.try_get("provider_message_id").map_err(map)?,
        created_at: epoch_to_ts(
            row.try_get::<Option<i64>, _>("created_at_epoch")
                .map_err(map)?,
        ),
        updated_at: epoch_to_ts(
            row.try_get::<Option<i64>, _>("updated_at_epoch")
                .map_err(map)?,
        ),
        ..Default::default()
    })
}
