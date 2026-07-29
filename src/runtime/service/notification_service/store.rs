//! Neutral-IR query/record builders, the opt-out preference lookup, and the shared
//! `NotificationDeliveryAttempt` upsert for the native `NotificationService`.
//! Extracted verbatim — the `LogicalRead`/`LogicalFilter`/`LogicalAggregate`/
//! `LogicalRecord` shapes, the hybrid-tenant template scope, and the
//! `(notification_id, channel, provider)` conflict-keyed upsert are byte-for-byte
//! identical to the former god file.

use sqlx::PgPool;
use tonic::Status;
use uuid::Uuid;

use crate::ir::{
    AggregateExpr, AggregateFunc, ComparisonOp, LogicalAggregate, LogicalFilter, LogicalPagination,
    LogicalProjection, LogicalRead, LogicalRecord, LogicalSort, LogicalValue, NullOrder,
    SortDirection,
};
use crate::proto::udb::core::notification::entity::v1 as notif_entity_pb;
use crate::runtime::DataBrokerRuntime;

use super::super::native_helpers::parse_uuid;
use super::config::{LOG_MSG, PREFERENCE_MSG, TEMPLATE_MSG};
use super::model::{
    channel_to_db, delivery_attempt_model, delivery_attempt_select_projection, log_model,
    preference_from_json_row,
};

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

fn logical_optional_string(value: &str) -> LogicalValue {
    if value.trim().is_empty() {
        LogicalValue::Null
    } else {
        logical_string(value.to_string())
    }
}

fn eq_filter(field: &str, value: impl Into<String>) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: field.to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(value),
    }
}

pub(crate) fn notification_log_filter(
    tenant_id: &str,
    project_id: &str,
    recipient_id: &str,
    event_type: &str,
    channel: &str,
    status: &str,
    resource_type: &str,
    resource_id: &str,
) -> LogicalFilter {
    let mut filters = Vec::new();
    if !recipient_id.trim().is_empty() {
        filters.push(eq_filter("recipient_id", recipient_id.trim()));
    }
    if !tenant_id.trim().is_empty() {
        filters.push(eq_filter("tenant_id", tenant_id.trim()));
    }
    if !event_type.trim().is_empty() {
        filters.push(eq_filter("event_type", event_type.trim()));
    }
    if !channel.trim().is_empty() {
        filters.push(eq_filter("channel", channel.trim()));
    }
    if !status.trim().is_empty() {
        filters.push(eq_filter("status", status.trim()));
    }
    if !project_id.trim().is_empty() {
        filters.push(eq_filter("project_id", project_id.trim()));
    }
    if !resource_type.trim().is_empty() {
        filters.push(eq_filter("resource_type", resource_type.trim()));
    }
    if !resource_id.trim().is_empty() {
        filters.push(eq_filter("resource_id", resource_id.trim()));
    }
    LogicalFilter::And(filters)
}

pub(crate) fn notification_log_list_read(
    filter: LogicalFilter,
    offset: u64,
    limit: u32,
) -> LogicalRead {
    LogicalRead {
        message_type: LOG_MSG.to_string(),
        filter: Some(filter),
        projection: Some(log_projection()),
        sort: vec![LogicalSort {
            field: "created_at".to_string(),
            direction: SortDirection::Desc,
            nulls: NullOrder::Default,
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

pub(crate) fn delivery_stats_aggregate(tenant_id: &str, event_type: &str) -> LogicalAggregate {
    let filter = notification_log_filter(tenant_id, "", "", event_type, "", "", "", "");
    LogicalAggregate {
        message_type: LOG_MSG.to_string(),
        filter: Some(filter),
        group_by: vec!["channel".to_string(), "status".to_string()],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Count,
            field: "*".to_string(),
            alias: "n".to_string(),
        }],
        having: None,
        sort: vec![LogicalSort {
            field: "channel".to_string(),
            direction: SortDirection::Asc,
            nulls: NullOrder::Default,
        }],
        pagination: None,
    }
}

fn log_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "log_id".to_string(),
        "template_id".to_string(),
        "event_type".to_string(),
        "channel".to_string(),
        "recipient_id".to_string(),
        "recipient_address".to_string(),
        "tenant_id".to_string(),
        "project_id".to_string(),
        "resource_type".to_string(),
        "resource_id".to_string(),
        "resource_name".to_string(),
        "correlation_id".to_string(),
        "status".to_string(),
        "error_message".to_string(),
        "provider_message_id".to_string(),
        "retry_count".to_string(),
        "rendered_subject".to_string(),
        "rendered_body".to_string(),
    ])
}

fn template_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "template_id".to_string(),
        "event_type".to_string(),
        "channel".to_string(),
        "subject_template".to_string(),
        "body_template".to_string(),
        "locale".to_string(),
        "is_active".to_string(),
        "created_by".to_string(),
        "tenant_id".to_string(),
    ])
}

fn preference_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "preference_id".to_string(),
        "user_id".to_string(),
        "tenant_id".to_string(),
        "channel".to_string(),
        "event_type".to_string(),
        "is_opted_out".to_string(),
        "created_by".to_string(),
    ])
}

pub(crate) fn notification_log_read(log_id: &str, tenant_id: &str) -> LogicalRead {
    LogicalRead {
        message_type: LOG_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            eq_filter("log_id", log_id.to_string()),
            eq_filter("tenant_id", tenant_id.to_string()),
        ])),
        projection: Some(log_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

pub(crate) fn template_scope_filter(
    tenant_id: &str,
    event_type: &str,
    channel: &str,
    locale: Option<&str>,
    active_only: bool,
) -> LogicalFilter {
    let mut filters = vec![LogicalFilter::Or(vec![
        LogicalFilter::IsNull("tenant_id".to_string()),
        eq_filter("tenant_id", tenant_id.to_string()),
    ])];
    if !event_type.trim().is_empty() {
        filters.push(eq_filter("event_type", event_type.trim()));
    }
    if !channel.trim().is_empty() {
        filters.push(eq_filter("channel", channel.trim()));
    }
    if let Some(locale) = locale.filter(|value| !value.trim().is_empty()) {
        filters.push(eq_filter("locale", locale.trim()));
    }
    if active_only {
        filters.push(LogicalFilter::Comparison {
            field: "is_active".to_string(),
            op: ComparisonOp::Eq,
            value: LogicalValue::Bool(true),
        });
    }
    filters.push(LogicalFilter::IsNull("deleted_at".to_string()));
    LogicalFilter::And(filters)
}

pub(crate) fn template_read(filter: LogicalFilter, offset: u64, limit: u32) -> LogicalRead {
    LogicalRead {
        message_type: TEMPLATE_MSG.to_string(),
        filter: Some(filter),
        projection: Some(template_projection()),
        sort: vec![
            LogicalSort {
                field: "event_type".to_string(),
                direction: SortDirection::Asc,
                nulls: NullOrder::Default,
            },
            LogicalSort {
                field: "channel".to_string(),
                direction: SortDirection::Asc,
                nulls: NullOrder::Default,
            },
            LogicalSort {
                field: "locale".to_string(),
                direction: SortDirection::Asc,
                nulls: NullOrder::Default,
            },
            LogicalSort {
                field: "tenant_id".to_string(),
                direction: SortDirection::Asc,
                nulls: NullOrder::Last,
            },
        ],
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

fn preference_filter(
    user_id: &str,
    tenant_id: &str,
    channel: i32,
    event_type: &str,
) -> LogicalFilter {
    LogicalFilter::And(vec![
        eq_filter("user_id", user_id.to_string()),
        eq_filter("tenant_id", tenant_id.to_string()),
        eq_filter("channel", channel_to_db(channel).to_string()),
        eq_filter("event_type", event_type.to_string()),
    ])
}

pub(crate) fn preference_read(
    user_id: &str,
    tenant_id: &str,
    channel: i32,
    event_type: &str,
) -> LogicalRead {
    LogicalRead {
        message_type: PREFERENCE_MSG.to_string(),
        filter: Some(preference_filter(user_id, tenant_id, channel, event_type)),
        projection: Some(preference_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

pub(crate) fn preference_list_filter(user_id: &str, tenant_id: &str) -> LogicalFilter {
    let mut filters = vec![eq_filter("user_id", user_id.to_string())];
    if !tenant_id.trim().is_empty() {
        filters.push(eq_filter("tenant_id", tenant_id.to_string()));
    }
    LogicalFilter::And(filters)
}

pub(crate) fn preference_list_read(filter: LogicalFilter, offset: u64, limit: u32) -> LogicalRead {
    LogicalRead {
        message_type: PREFERENCE_MSG.to_string(),
        filter: Some(filter),
        projection: Some(preference_projection()),
        sort: vec![LogicalSort {
            field: "channel".to_string(),
            direction: SortDirection::Asc,
            nulls: NullOrder::Default,
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

pub(crate) fn notification_log_record(
    log: &notif_entity_pb::NotificationLog,
    status_db: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("log_id".to_string(), logical_string(log.log_id.clone()));
    record.insert(
        "template_id".to_string(),
        logical_optional_string(&log.template_id),
    );
    record.insert(
        "event_type".to_string(),
        logical_string(log.event_type.clone()),
    );
    record.insert(
        "channel".to_string(),
        logical_string(channel_to_db(log.channel)),
    );
    record.insert(
        "recipient_id".to_string(),
        logical_optional_string(&log.recipient_id),
    );
    record.insert(
        "recipient_address".to_string(),
        logical_string(log.recipient_address.clone()),
    );
    record.insert(
        "tenant_id".to_string(),
        logical_string(log.tenant_id.clone()),
    );
    record.insert(
        "project_id".to_string(),
        logical_string(log.project_id.clone()),
    );
    record.insert(
        "resource_type".to_string(),
        logical_string(log.resource_type.clone()),
    );
    record.insert(
        "resource_id".to_string(),
        logical_string(log.resource_id.clone()),
    );
    record.insert(
        "resource_name".to_string(),
        logical_string(log.resource_name.clone()),
    );
    record.insert(
        "correlation_id".to_string(),
        logical_string(log.correlation_id.clone()),
    );
    record.insert("status".to_string(), logical_string(status_db.to_string()));
    record.insert(
        "error_message".to_string(),
        logical_string(log.error_message.clone()),
    );
    record.insert(
        "provider_message_id".to_string(),
        logical_string(log.provider_message_id.clone()),
    );
    record.insert(
        "retry_count".to_string(),
        LogicalValue::Int(log.retry_count as i64),
    );
    record.insert(
        "rendered_subject".to_string(),
        logical_optional_string(&log.rendered_subject),
    );
    record.insert(
        "rendered_body".to_string(),
        logical_optional_string(&log.rendered_body),
    );
    record
}

pub(crate) async fn is_notification_opted_out(
    runtime: &DataBrokerRuntime,
    context: &crate::RequestContext,
    recipient_id: &str,
    tenant_id: &str,
    channel: i32,
    event_type: &str,
) -> Result<bool, Status> {
    if recipient_id.trim().is_empty() {
        return Ok(false);
    }
    let user_id = parse_uuid("recipient_id", recipient_id)?.to_string();
    for candidate_event in [event_type, ""] {
        let rows = runtime
            .native_entity_read_for_service(
                "notification",
                context,
                preference_read(&user_id, tenant_id, channel, candidate_event),
            )
            .await?;
        if let Some(row) = rows.first() {
            return Ok(preference_from_json_row(row).is_opted_out);
        }
    }
    Ok(false)
}

/// Upsert the single `NotificationDeliveryAttempt` row keyed on
/// (notification_id, channel, provider): set the terminal status, BUMP
/// `attempt_count`, record `last_error`/`provider_message_id`, and stamp the
/// updated timestamp. Shared by the `ReportDelivery` handler and the delivery
/// worker so the durable delivery record has ONE writer shape. Returns the stored
/// row (the handler maps it to the response; the worker ignores it).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_delivery_attempt(
    pool: &PgPool,
    notification_id: Uuid,
    tenant_id: &str,
    channel_db: &str,
    provider: &str,
    status_db: &str,
    last_error: &str,
    provider_message_id: &str,
) -> Result<Option<sqlx::postgres::PgRow>, sqlx::Error> {
    let m = delivery_attempt_model();
    let rel = m.relation.clone();
    let projection = delivery_attempt_select_projection(&m);
    sqlx::query(&format!(
        "INSERT INTO {rel} AS existing \
         ({attempt_id}, {notification_id}, {tenant_id}, {channel}, {provider}, {status}, {attempt_count}, {last_error}, {provider_message_id}, {updated_at}) \
         VALUES (gen_random_uuid(), $1::UUID, $2, $3, $4, $5, 1, NULLIF($6, ''), NULLIF($7, ''), now()) \
         ON CONFLICT ({notification_id}, {channel}, {provider}) \
         DO UPDATE SET {status} = EXCLUDED.{status}, \
                       {attempt_count} = existing.{attempt_count} + 1, \
                       {last_error} = EXCLUDED.{last_error}, \
                       {provider_message_id} = COALESCE(EXCLUDED.{provider_message_id}, existing.{provider_message_id}), \
                       {tenant_id} = EXCLUDED.{tenant_id}, \
                       {updated_at} = now() \
         RETURNING {projection}",
        attempt_id = m.q("attempt_id"),
        notification_id = m.q("notification_id"),
        tenant_id = m.q("tenant_id"),
        channel = m.q("channel"),
        provider = m.q("provider"),
        status = m.q("status"),
        attempt_count = m.q("attempt_count"),
        last_error = m.q("last_error"),
        provider_message_id = m.q("provider_message_id"),
        updated_at = m.q("updated_at"),
    ))
    .bind(notification_id)
    .bind(tenant_id)
    .bind(channel_db)
    .bind(provider)
    .bind(status_db)
    .bind(last_error)
    .bind(provider_message_id)
    .fetch_optional(pool)
    .await
}

/// Transition a `NotificationLog.status` when a terminal delivery outcome is
/// recorded, but ONLY from an allowed prior state (`allowed_prev`). This is the
/// forward state machine `PENDING → SENT → DELIVERED`: a later attempt can never
/// downgrade a delivered notification, and the `(log_id, tenant_id)` predicate
/// keeps the write tenant-scoped. Idempotent — a no-op when the log is already
/// in a further state or belongs to another tenant. Returns whether a row moved.
///
/// Before this, the log stayed `PENDING` forever after a real send, so
/// `GetNotification`/`GetDeliveryStats` never reflected actual deliveries. The
/// FAILED transition is deliberately NOT handled here — it belongs with the
/// bounded-retry model (max-attempts + backoff) rather than marking a log failed
/// on the first transient error.
pub(crate) async fn transition_log_status(
    pool: &PgPool,
    log_id: Uuid,
    tenant_id: &str,
    new_status: &str,
    allowed_prev: &[&str],
) -> Result<bool, sqlx::Error> {
    if allowed_prev.is_empty() {
        return Ok(false);
    }
    let m = log_model();
    let rel = m.relation.clone();
    let prev: Vec<String> = allowed_prev.iter().map(|state| state.to_string()).collect();
    let result = sqlx::query(&format!(
        "UPDATE {rel} SET {status} = $1 \
         WHERE {log_id} = $2::UUID AND {tenant_id} = $3 AND {status} = ANY($4)",
        status = m.q("status"),
        log_id = m.q("log_id"),
        tenant_id = m.q("tenant_id"),
    ))
    .bind(new_status)
    .bind(log_id)
    .bind(tenant_id)
    .bind(&prev)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// The allowed prior states for a forward transition to `status_db`. Only the
/// success terminals move the log here; `FAILED` returns an empty slice (handled
/// by the retry model), so callers can pass any delivery status safely.
pub(crate) fn log_transition_allowed_prev(status_db: &str) -> &'static [&'static str] {
    match status_db {
        "DELIVERED" => &["PENDING", "SENT"],
        "SENT" => &["PENDING"],
        _ => &[],
    }
}
