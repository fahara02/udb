//! Native `NotificationService` — proto-driven Postgres CRUD over the UDB-owned
//! `udb_notification.{notification_logs,notification_templates,notification_preferences}`
//! tables. Like `auth_service`/`tenant_service`: no in-memory store, identifiers
//! resolved from the embedded proto manifest via [`NativeModel`].
//!
//! This is the control-plane surface (persist + query notification state,
//! templates, and preferences, and aggregate delivery stats). Actual outbound
//! delivery (SES/Twilio/FCM/webhook) is performed by separate delivery adapters;
//! `SendNotification` records the intent as a `NotificationLog` row.

use std::sync::Arc;

use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::notification::entity::v1 as notif_entity_pb;
use crate::proto::udb::core::notification::services::v1 as notif_pb;
use crate::proto::udb::core::notification::services::v1::notification_service_server::NotificationService;
use crate::runtime::channels::{ChannelManager, OperationChannel};
use crate::runtime::native_catalog::{NativeModel, native_model};

pub use crate::proto::udb::core::notification::services::v1::notification_service_server::NotificationServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    admit_on as native_admit_on, metadata_tenant_id, native_page_response, native_page_window,
    parse_uuid, validate_request_scope, validate_request_tenant,
};

const LOG_MSG: &str = "udb.core.notification.entity.v1.NotificationLog";
const TEMPLATE_MSG: &str = "udb.core.notification.entity.v1.NotificationTemplate";
const PREFERENCE_MSG: &str = "udb.core.notification.entity.v1.NotificationPreference";

pub struct NotificationServiceImpl {
    pg_pool: Option<PgPool>,
    /// Schema-qualified outbox table (`udb_system.outbox_events`) the CDC engine
    /// tails → Apache Kafka → the Spark streaming consumer. `None` = no emit.
    outbox_relation: Option<String>,
    /// Per-tenant fair-admission manager (the SAME one the data plane uses via
    /// `execute_with_channel_scoped`). Mutating/listing RPCs acquire a per-tenant
    /// budget through this so one tenant can't starve the shared control plane.
    /// `None` only in bare unit-test construction (no runtime wired) —
    /// `build_notification_service` always wires it in production.
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
}

/// Kafka topic for the "notification sent" domain event.
const NOTIFICATION_SENT_TOPIC: &str = "udb.notification.sent.v1";

impl NotificationServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Wire the shared per-tenant fair-admission manager (same one the data plane
    /// uses) so control-plane RPCs are bounded per tenant. No-op (`None`) leaves
    /// admission disabled for bare unit-test construction.
    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    /// Wire the transactional outbox so `SendNotification` publishes a domain
    /// event to Kafka (via the CDC relay). `relation` is the schema-qualified
    /// table, e.g. `"udb_system"."outbox_events"` (`CdcConfig::outbox_relation`).
    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "notification service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }

    /// Best-effort: enqueue a "notification sent" event into the shared native
    /// outbox envelope (top-level tenant/project for CDC routing). Never fails
    /// the RPC.
    async fn emit_sent_event(
        &self,
        pool: &PgPool,
        log_id: &str,
        event_type: &str,
        recipient_id: &str,
        tenant_id: &str,
        project_id: &str,
        channels: &[i32],
        retry: bool,
    ) {
        super::native_helpers::enqueue_outbox_event(
            pool,
            self.outbox_relation.as_deref(),
            NOTIFICATION_SENT_TOPIC,
            recipient_id,
            tenant_id,
            project_id,
            notification_delivery_payload(
                log_id,
                event_type,
                recipient_id,
                tenant_id,
                project_id,
                channels,
                retry,
            ),
            Some(&self.metrics),
        )
        .await;
    }
}

impl Default for NotificationServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

fn log_model() -> NativeModel {
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
        ],
    )
}

fn template_model() -> NativeModel {
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

fn preference_model() -> NativeModel {
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

fn channel_to_db(value: i32) -> &'static str {
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

fn channel_from_db(value: &str) -> i32 {
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

fn status_from_db(value: &str) -> i32 {
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

/// Per-channel send decision: an opted-out (recipient, channel) preference is
/// recorded as a SUPPRESSED log row — kept for audit/delivery stats but never
/// part of the delivery emit set — while everything else queues as PENDING.
/// Returns `(db_status, proto_status)`; the single decision point
/// `send_notification` applies per channel.
fn channel_send_decision(opted_out: bool) -> (&'static str, i32) {
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

fn notification_delivery_payload(
    log_id: &str,
    event_type: &str,
    recipient_id: &str,
    tenant_id: &str,
    project_id: &str,
    channels: &[i32],
    retry: bool,
) -> serde_json::Value {
    serde_json::json!({
        "log_id": log_id,
        "event_type": event_type,
        "recipient_id": recipient_id,
        "tenant_id": tenant_id,
        "project_id": project_id,
        "channels": channels.iter().map(|c| channel_to_db(*c)).collect::<Vec<_>>(),
        "retry": retry,
    })
}

fn deliverable_channels(logs: &[notif_entity_pb::NotificationLog]) -> Vec<i32> {
    let pending = notif_entity_pb::NotificationStatus::Pending as i32;
    logs.iter()
        .filter(|log| log.status == pending)
        .map(|log| log.channel)
        .collect()
}

async fn is_notification_opted_out(
    pool: &PgPool,
    recipient_id: &str,
    tenant_id: &str,
    channel: i32,
    event_type: &str,
) -> Result<bool, Status> {
    if recipient_id.trim().is_empty() {
        return Ok(false);
    }
    let user_id = parse_uuid("recipient_id", recipient_id)?;
    let m = preference_model();
    let rel = m.relation.clone();
    sqlx::query_scalar::<_, bool>(&format!(
        "SELECT COALESCE(( \
             SELECT {is_opted_out} FROM {rel} \
             WHERE {user_id} = $1::UUID AND {tenant_id} = $2 AND {channel} = $3 \
               AND {event_type} IN ($4, '') \
             ORDER BY CASE WHEN {event_type} = $4 THEN 0 ELSE 1 END \
             LIMIT 1 \
         ), FALSE)",
        is_opted_out = m.q("is_opted_out"),
        user_id = m.q("user_id"),
        tenant_id = m.q("tenant_id"),
        channel = m.q("channel"),
        event_type = m.q("event_type"),
    ))
    .bind(user_id)
    .bind(tenant_id)
    .bind(channel_to_db(channel))
    .bind(event_type)
    .fetch_one(pool)
    .await
    .map_err(|err| Status::internal(format!("notification preference lookup failed: {err}")))
}

// ── projections + row mappers ─────────────────────────────────────────────────

fn log_select_projection(m: &NativeModel) -> String {
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

fn log_from_row(row: &sqlx::postgres::PgRow) -> Result<notif_entity_pb::NotificationLog, Status> {
    let map = |e: sqlx::Error| Status::internal(format!("decode notification log failed: {e}"));
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

fn template_select_projection(m: &NativeModel) -> String {
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
fn template_selection_sql(m: &NativeModel) -> String {
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

fn template_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<notif_entity_pb::NotificationTemplate, Status> {
    let map = |e: sqlx::Error| Status::internal(format!("decode template failed: {e}"));
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

fn preference_select_projection(m: &NativeModel) -> String {
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

fn preference_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<notif_entity_pb::NotificationPreference, Status> {
    let map = |e: sqlx::Error| Status::internal(format!("decode preference failed: {e}"));
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

#[tonic::async_trait]
impl NotificationService for NotificationServiceImpl {
    async fn send_notification(
        &self,
        request: Request<notif_pb::SendNotificationRequest>,
    ) -> Result<Response<notif_pb::SendNotificationResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
        if req.event_type.trim().is_empty() {
            return Err(Status::invalid_argument("event_type is required"));
        }
        // Per-tenant fair admission (Write budget) so one tenant's send flood
        // can't starve the shared control plane.
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "notification",
            OperationChannel::Write,
            &req.tenant_id,
            None,
        )
        .await?;
        let pool = self.require_pool()?;
        let m = log_model();
        let rel = m.relation.clone();
        // Default to EMAIL when the caller did not pin channels; one log per channel.
        let channels = if req.channels.is_empty() {
            vec![notif_entity_pb::NotificationChannel::Email as i32]
        } else {
            req.channels.clone()
        };
        let mut logs = Vec::with_capacity(channels.len());
        for channel in channels.iter().copied() {
            let opted_out = is_notification_opted_out(
                pool,
                &req.recipient_id,
                &req.tenant_id,
                channel,
                &req.event_type,
            )
            .await?;
            let (status_db, status_pb) = channel_send_decision(opted_out);
            let log_id = Uuid::new_v4().to_string();
            sqlx::query(&format!(
                "INSERT INTO {rel} \
                 ({log_id}, {event_type}, {channel}, {recipient_id}, {recipient_address}, \
                  {tenant_id}, {project_id}, {resource_type}, {resource_id}, {resource_name}, \
                  {correlation_id}, {status}, {retry_count}) \
                 VALUES ($1::UUID, $2, $3, NULLIF($4, '')::UUID, $5, $6, $7, $8, $9, $10, $11, $12, 0)",
                log_id = m.q("log_id"),
                event_type = m.q("event_type"),
                channel = m.q("channel"),
                recipient_id = m.q("recipient_id"),
                recipient_address = m.q("recipient_address"),
                tenant_id = m.q("tenant_id"),
                project_id = m.q("project_id"),
                resource_type = m.q("resource_type"),
                resource_id = m.q("resource_id"),
                resource_name = m.q("resource_name"),
                correlation_id = m.q("correlation_id"),
                status = m.q("status"),
                retry_count = m.q("retry_count"),
            ))
            .bind(&log_id)
            .bind(&req.event_type)
            .bind(channel_to_db(channel))
            .bind(&req.recipient_id)
            .bind(&req.recipient_address)
            .bind(&req.tenant_id)
            .bind(&req.project_id)
            .bind(&req.resource_type)
            .bind(&req.resource_id)
            .bind(&req.resource_name)
            .bind(&req.correlation_id)
            .bind(status_db)
            .execute(pool)
            .await
            .map_err(|err| Status::internal(format!("send notification failed: {err}")))?;
            logs.push(notif_entity_pb::NotificationLog {
                log_id,
                event_type: req.event_type.clone(),
                channel,
                recipient_id: req.recipient_id.clone(),
                recipient_address: req.recipient_address.clone(),
                tenant_id: req.tenant_id.clone(),
                project_id: req.project_id.clone(),
                correlation_id: req.correlation_id.clone(),
                status: status_pb,
                ..Default::default()
            });
        }
        let delivery_channels = deliverable_channels(&logs);
        if !delivery_channels.is_empty() {
            let primary_log_id = logs
                .iter()
                .find(|log| log.status == notif_entity_pb::NotificationStatus::Pending as i32)
                .map(|log| log.log_id.clone())
                .unwrap_or_default();
            self.emit_sent_event(
                pool,
                &primary_log_id,
                &req.event_type,
                &req.recipient_id,
                &req.tenant_id,
                &req.project_id,
                &delivery_channels,
                false,
            )
            .await;
        }
        Ok(Response::new(notif_pb::SendNotificationResponse { logs }))
    }

    async fn get_notification(
        &self,
        request: Request<notif_pb::GetNotificationRequest>,
    ) -> Result<Response<notif_pb::GetNotificationResponse>, Status> {
        let metadata = request.metadata().clone();
        let scoped_tenant = metadata_tenant_id(&metadata)
            .ok_or_else(|| Status::permission_denied("tenant-scoped metadata is required"))?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "notification",
            OperationChannel::Read,
            &scoped_tenant,
            None,
        )
        .await?;
        let req = request.into_inner();
        let log_id = parse_uuid("log_id", &req.log_id)?;
        let pool = self.require_pool()?;
        let m = log_model();
        let rel = m.relation.clone();
        let projection = log_select_projection(&m);
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} WHERE {log_id} = $1::UUID AND {tenant_id} = $2",
            log_id = m.q("log_id"),
            tenant_id = m.q("tenant_id"),
        ))
        .bind(log_id)
        .bind(&scoped_tenant)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("get notification failed: {err}")))?;
        let log = match row {
            Some(row) => Some(log_from_row(&row)?),
            None => return Err(Status::not_found("notification not found")),
        };
        Ok(Response::new(notif_pb::GetNotificationResponse { log }))
    }

    async fn list_notifications(
        &self,
        request: Request<notif_pb::ListNotificationsRequest>,
    ) -> Result<Response<notif_pb::ListNotificationsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "notification",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        let pool = self.require_pool()?;
        let m = log_model();
        let rel = m.relation.clone();
        let projection = log_select_projection(&m);
        let page = native_page_window(req.page.as_ref(), 50);
        let channel = if req.channel == 0 {
            String::new()
        } else {
            channel_to_db(req.channel).to_string()
        };
        let status = if req.status == 0 {
            String::new()
        } else {
            // Map status enum to its stored short name for filtering.
            match notif_entity_pb::NotificationStatus::try_from(req.status) {
                Ok(notif_entity_pb::NotificationStatus::Pending) => "PENDING",
                Ok(notif_entity_pb::NotificationStatus::Sent) => "SENT",
                Ok(notif_entity_pb::NotificationStatus::Delivered) => "DELIVERED",
                Ok(notif_entity_pb::NotificationStatus::Failed) => "FAILED",
                Ok(notif_entity_pb::NotificationStatus::Suppressed) => "SUPPRESSED",
                _ => "",
            }
            .to_string()
        };
        let rows = sqlx::query(&format!(
            "SELECT {projection}, COUNT(*) OVER() AS total_count FROM {rel} \
             WHERE ($1 = '' OR {recipient}::TEXT = $1) \
               AND ($2 = '' OR {tenant} = $2) \
               AND ($3 = '' OR {event} = $3) \
               AND ($4 = '' OR {channel} = $4) \
               AND ($5 = '' OR {status} = $5) \
               AND ($6 = '' OR {project} = $6) \
               AND ($7 = '' OR {resource_type} = $7) \
               AND ($8 = '' OR {resource_id} = $8) \
             ORDER BY {created} DESC LIMIT $9 OFFSET $10",
            recipient = m.q("recipient_id"),
            tenant = m.q("tenant_id"),
            event = m.q("event_type"),
            channel = m.q("channel"),
            status = m.q("status"),
            project = m.q("project_id"),
            resource_type = m.q("resource_type"),
            resource_id = m.q("resource_id"),
            created = m.q("created_at"),
        ))
        .bind(&req.recipient_id)
        .bind(&req.tenant_id)
        .bind(&req.event_type)
        .bind(&channel)
        .bind(&status)
        .bind(&req.project_id)
        .bind(&req.resource_type)
        .bind(&req.resource_id)
        .bind(page.limit_i64())
        .bind(page.offset_i64())
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("list notifications failed: {err}")))?;
        let total: i64 = rows
            .first()
            .and_then(|r| r.try_get("total_count").ok())
            .unwrap_or(0);
        let mut logs = Vec::with_capacity(rows.len());
        for row in &rows {
            logs.push(log_from_row(row)?);
        }
        Ok(Response::new(notif_pb::ListNotificationsResponse {
            logs,
            page: Some(native_page_response(req.page.as_ref(), total, 50)),
        }))
    }

    async fn retry_notification(
        &self,
        request: Request<notif_pb::RetryNotificationRequest>,
    ) -> Result<Response<notif_pb::RetryNotificationResponse>, Status> {
        let metadata = request.metadata().clone();
        let scoped_tenant = metadata_tenant_id(&metadata)
            .ok_or_else(|| Status::permission_denied("tenant-scoped metadata is required"))?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "notification",
            OperationChannel::Write,
            &scoped_tenant,
            None,
        )
        .await?;
        let req = request.into_inner();
        let log_id = parse_uuid("log_id", &req.log_id)?;
        let pool = self.require_pool()?;
        let m = log_model();
        let rel = m.relation.clone();
        let projection = log_select_projection(&m);
        // Only failed (or suppressed) notifications are retryable — re-queuing a
        // PENDING/SENT/DELIVERED row would double-send. Guard in the WHERE so a
        // non-failed row yields no update.
        let row = sqlx::query(&format!(
            "UPDATE {rel} SET {status} = 'PENDING', {retry} = {retry} + 1 \
             WHERE {log_id} = $1::UUID AND {tenant_id} = $2 AND {status} IN ('FAILED','SUPPRESSED') \
             RETURNING {projection}",
            status = m.q("status"),
            retry = m.q("retry_count"),
            log_id = m.q("log_id"),
            tenant_id = m.q("tenant_id"),
        ))
        .bind(log_id)
        .bind(&scoped_tenant)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("retry notification failed: {err}")))?;
        let log = match row {
            Some(row) => log_from_row(&row)?,
            None => {
                return Err(Status::failed_precondition(
                    "notification not found or not in a retryable (FAILED) state",
                ));
            }
        };
        self.emit_sent_event(
            pool,
            &log.log_id,
            &log.event_type,
            &log.recipient_id,
            &log.tenant_id,
            &log.project_id,
            &[log.channel],
            true,
        )
        .await;
        Ok(Response::new(notif_pb::RetryNotificationResponse {
            log: Some(log),
        }))
    }

    async fn upsert_template(
        &self,
        request: Request<notif_pb::UpsertTemplateRequest>,
    ) -> Result<Response<notif_pb::UpsertTemplateResponse>, Status> {
        let req = request.into_inner();
        if req.event_type.trim().is_empty() {
            return Err(Status::invalid_argument("event_type is required"));
        }
        // Platform-global template write (no body tenant); bound on the shared
        // base Write budget so a template-write flood can't starve the control plane.
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "notification",
            OperationChannel::Write,
            "",
            None,
        )
        .await?;
        let pool = self.require_pool()?;
        let m = template_model();
        let rel = m.relation.clone();
        let locale = if req.locale.trim().is_empty() {
            "en".to_string()
        } else {
            req.locale.clone()
        };
        let projection = template_select_projection(&m);
        // Hybrid tenant model (F4.3): this control-plane write path has no tenant
        // scope in the request, so it always writes a platform-global default
        // (tenant_id = NULL). The unique index stays on (event_type, channel) so
        // global upserts keep deduping correctly (Postgres treats NULLs as
        // distinct, so a tenant_id-bearing unique index would break ON CONFLICT
        // for global rows). TODO: when a per-tenant override write path lands,
        // split this into partial unique indexes — (event_type, channel) WHERE
        // tenant_id IS NULL for globals and (event_type, channel, tenant_id)
        // WHERE tenant_id IS NOT NULL for overrides — and bind the caller tenant.
        let row = sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({template_id}, {event_type}, {channel}, {subject}, {body}, {locale}, {is_active}, {tenant_id}) \
             VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, NULL) \
             ON CONFLICT ({event_type}, {channel}) \
             DO UPDATE SET {subject} = EXCLUDED.{subject}, {body} = EXCLUDED.{body}, \
                           {locale} = EXCLUDED.{locale}, {is_active} = EXCLUDED.{is_active} \
             RETURNING {projection}",
            template_id = m.q("template_id"),
            event_type = m.q("event_type"),
            channel = m.q("channel"),
            subject = m.q("subject_template"),
            body = m.q("body_template"),
            locale = m.q("locale"),
            is_active = m.q("is_active"),
            tenant_id = m.q("tenant_id"),
        ))
        .bind(&req.event_type)
        .bind(channel_to_db(req.channel))
        .bind(&req.subject_template)
        .bind(&req.body_template)
        .bind(&locale)
        .bind(req.is_active)
        .fetch_one(pool)
        .await
        .map_err(|err| Status::internal(format!("upsert template failed: {err}")))?;
        Ok(Response::new(notif_pb::UpsertTemplateResponse {
            template: Some(template_from_row(&row)?),
        }))
    }

    async fn get_template(
        &self,
        request: Request<notif_pb::GetTemplateRequest>,
    ) -> Result<Response<notif_pb::GetTemplateResponse>, Status> {
        let metadata = request.metadata().clone();
        let scoped_tenant = metadata_tenant_id(&metadata)
            .ok_or_else(|| Status::permission_denied("tenant-scoped metadata is required"))?;
        let req = request.into_inner();
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "notification",
            OperationChannel::Read,
            &scoped_tenant,
            None,
        )
        .await?;
        let pool = self.require_pool()?;
        let m = template_model();
        let locale = if req.locale.trim().is_empty() {
            "en".to_string()
        } else {
            req.locale.clone()
        };
        let row = sqlx::query(&template_selection_sql(&m))
            .bind(&req.event_type)
            .bind(channel_to_db(req.channel))
            .bind(&locale)
            .bind(&scoped_tenant)
            .fetch_optional(pool)
            .await
            .map_err(|err| Status::internal(format!("get template failed: {err}")))?;
        let template = match row {
            Some(row) => Some(template_from_row(&row)?),
            None => return Err(Status::not_found("template not found")),
        };
        Ok(Response::new(notif_pb::GetTemplateResponse { template }))
    }

    async fn list_templates(
        &self,
        request: Request<notif_pb::ListTemplatesRequest>,
    ) -> Result<Response<notif_pb::ListTemplatesResponse>, Status> {
        let metadata = request.metadata().clone();
        let scoped_tenant = metadata_tenant_id(&metadata)
            .ok_or_else(|| Status::permission_denied("tenant-scoped metadata is required"))?;
        let req = request.into_inner();
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "notification",
            OperationChannel::Read,
            &scoped_tenant,
            None,
        )
        .await?;
        let pool = self.require_pool()?;
        let m = template_model();
        let rel = m.relation.clone();
        let projection = template_select_projection(&m);
        let page = native_page_window(req.page.as_ref(), 50);
        let channel = if req.channel == 0 {
            String::new()
        } else {
            channel_to_db(req.channel).to_string()
        };
        let rows = sqlx::query(&format!(
            "SELECT {projection}, COUNT(*) OVER() AS total_count FROM {rel} \
             WHERE {deleted} IS NULL \
               AND ($1 = '' OR {event_type} = $1) \
               AND ($2 = '' OR {channel} = $2) \
               AND (NOT $3 OR {is_active} = TRUE) \
               AND ({tenant_id} IS NULL OR {tenant_id} = $4) \
             ORDER BY {event_type}, {channel}, {locale}, {tenant_id} NULLS LAST LIMIT $5 OFFSET $6",
            deleted = m.q("deleted_at"),
            event_type = m.q("event_type"),
            channel = m.q("channel"),
            is_active = m.q("is_active"),
            tenant_id = m.q("tenant_id"),
            locale = m.q("locale"),
        ))
        .bind(&req.event_type)
        .bind(&channel)
        .bind(req.active_only)
        .bind(&scoped_tenant)
        .bind(page.limit_i64())
        .bind(page.offset_i64())
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("list templates failed: {err}")))?;
        let total: i64 = rows
            .first()
            .and_then(|r| r.try_get("total_count").ok())
            .unwrap_or(0);
        let mut templates = Vec::with_capacity(rows.len());
        for row in &rows {
            templates.push(template_from_row(row)?);
        }
        Ok(Response::new(notif_pb::ListTemplatesResponse {
            templates,
            page: Some(native_page_response(req.page.as_ref(), total, 50)),
        }))
    }

    async fn get_delivery_stats(
        &self,
        request: Request<notif_pb::GetDeliveryStatsRequest>,
    ) -> Result<Response<notif_pb::GetDeliveryStatsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "notification",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        let pool = self.require_pool()?;
        let m = log_model();
        let rel = m.relation.clone();
        // Per-channel aggregation, honoring the optional tenant/event filters and
        // the optional [date_from, date_to] window (YYYY-MM-DD, `to` inclusive of
        // the whole day). `sent` counts successful hand-offs (SENT|DELIVERED).
        let rows = sqlx::query(&format!(
            "SELECT {channel} AS channel, \
               COUNT(*) FILTER (WHERE {status} IN ('SENT','DELIVERED')) AS sent, \
               COUNT(*) FILTER (WHERE {status} = 'DELIVERED') AS delivered, \
               COUNT(*) FILTER (WHERE {status} = 'FAILED') AS failed, \
               COUNT(*) FILTER (WHERE {status} = 'SUPPRESSED') AS suppressed \
             FROM {rel} \
             WHERE ($1 = '' OR {tenant} = $1) AND ($2 = '' OR {event} = $2) \
               AND ($3 = '' OR {created} >= $3::date) \
               AND ($4 = '' OR {created} < ($4::date + 1)) \
             GROUP BY {channel} \
             ORDER BY {channel}",
            channel = m.q("channel"),
            status = m.q("status"),
            tenant = m.q("tenant_id"),
            event = m.q("event_type"),
            created = m.q("created_at"),
        ))
        .bind(&req.tenant_id)
        .bind(&req.event_type)
        .bind(&req.date_from)
        .bind(&req.date_to)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("delivery stats failed: {err}")))?;

        let (mut total_sent, mut total_delivered, mut total_failed) = (0i64, 0i64, 0i64);
        let mut by_channel = Vec::with_capacity(rows.len());
        for row in &rows {
            let channel: String = row.try_get("channel").unwrap_or_default();
            let sent: i64 = row.try_get("sent").unwrap_or(0);
            let delivered: i64 = row.try_get("delivered").unwrap_or(0);
            let failed: i64 = row.try_get("failed").unwrap_or(0);
            let suppressed: i64 = row.try_get("suppressed").unwrap_or(0);
            total_sent += sent;
            total_delivered += delivered;
            total_failed += failed;
            by_channel.push(notif_pb::ChannelStats {
                channel: channel_from_db(&channel),
                sent,
                delivered,
                failed,
                suppressed,
                delivery_rate: if sent > 0 {
                    delivered as f64 / sent as f64
                } else {
                    0.0
                },
            });
        }
        let overall_delivery_rate = if total_sent > 0 {
            total_delivered as f64 / total_sent as f64
        } else {
            0.0
        };
        Ok(Response::new(notif_pb::GetDeliveryStatsResponse {
            total_sent,
            total_delivered,
            total_failed,
            overall_delivery_rate,
            by_channel,
        }))
    }

    async fn set_preference(
        &self,
        request: Request<notif_pb::SetPreferenceRequest>,
    ) -> Result<Response<notif_pb::SetPreferenceResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "notification",
            OperationChannel::Write,
            &req.tenant_id,
            None,
        )
        .await?;
        let user_id = parse_uuid("user_id", &req.user_id)?;
        // `tenant_id` is a VARCHAR(120) NOT NULL slug/id — bind it as text, not a
        // UUID (the column is not UUID-typed), and reject empty to honor NOT NULL.
        if req.tenant_id.trim().is_empty() {
            return Err(Status::invalid_argument("tenant_id is required"));
        }
        let pool = self.require_pool()?;
        let m = preference_model();
        let rel = m.relation.clone();
        let projection = preference_select_projection(&m);
        let row = sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({preference_id}, {user_id}, {tenant_id}, {channel}, {event_type}, {is_opted_out}) \
             VALUES (gen_random_uuid(), $1::UUID, $2, $3, $4, $5) \
             ON CONFLICT ({user_id}, {channel}, {event_type}) \
             DO UPDATE SET {is_opted_out} = EXCLUDED.{is_opted_out} \
             RETURNING {projection}",
            preference_id = m.q("preference_id"),
            user_id = m.q("user_id"),
            tenant_id = m.q("tenant_id"),
            channel = m.q("channel"),
            event_type = m.q("event_type"),
            is_opted_out = m.q("is_opted_out"),
        ))
        .bind(user_id)
        .bind(&req.tenant_id)
        .bind(channel_to_db(req.channel))
        .bind(&req.event_type)
        .bind(req.is_opted_out)
        .fetch_one(pool)
        .await
        .map_err(|err| Status::internal(format!("set preference failed: {err}")))?;
        Ok(Response::new(notif_pb::SetPreferenceResponse {
            preference: Some(preference_from_row(&row)?),
        }))
    }

    async fn get_preference(
        &self,
        request: Request<notif_pb::GetPreferenceRequest>,
    ) -> Result<Response<notif_pb::GetPreferenceResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "notification",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        let user_id = parse_uuid("user_id", &req.user_id)?;
        let pool = self.require_pool()?;
        let m = preference_model();
        let rel = m.relation.clone();
        let projection = preference_select_projection(&m);
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} \
             WHERE {user_id} = $1::UUID AND {tenant_id} = $2 \
               AND {channel} = $3 AND {event_type} = $4",
            user_id = m.q("user_id"),
            tenant_id = m.q("tenant_id"),
            channel = m.q("channel"),
            event_type = m.q("event_type"),
        ))
        .bind(user_id)
        .bind(&req.tenant_id)
        .bind(channel_to_db(req.channel))
        .bind(&req.event_type)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("get preference failed: {err}")))?;
        let preference = match row {
            Some(row) => Some(preference_from_row(&row)?),
            None => return Err(Status::not_found("preference not found")),
        };
        Ok(Response::new(notif_pb::GetPreferenceResponse {
            preference,
        }))
    }

    async fn list_preferences(
        &self,
        request: Request<notif_pb::ListPreferencesRequest>,
    ) -> Result<Response<notif_pb::ListPreferencesResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "notification",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        let user_id = parse_uuid("user_id", &req.user_id)?;
        let pool = self.require_pool()?;
        let m = preference_model();
        let rel = m.relation.clone();
        let projection = preference_select_projection(&m);
        let page = native_page_window(req.page.as_ref(), 50);
        let rows = sqlx::query(&format!(
            "SELECT {projection}, COUNT(*) OVER() AS total_count FROM {rel} \
             WHERE {user_id} = $1::UUID AND ($2 = '' OR {tenant_id} = $2) \
             ORDER BY {channel} LIMIT $3 OFFSET $4",
            user_id = m.q("user_id"),
            tenant_id = m.q("tenant_id"),
            channel = m.q("channel"),
        ))
        .bind(user_id)
        .bind(&req.tenant_id)
        .bind(page.limit_i64())
        .bind(page.offset_i64())
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("list preferences failed: {err}")))?;
        let total: i64 = rows
            .first()
            .and_then(|r| r.try_get("total_count").ok())
            .unwrap_or(0);
        let mut preferences = Vec::with_capacity(rows.len());
        for row in &rows {
            preferences.push(preference_from_row(row)?);
        }
        Ok(Response::new(notif_pb::ListPreferencesResponse {
            preferences,
            page: Some(native_page_response(req.page.as_ref(), total, 50)),
        }))
    }
}

#[cfg(test)]
mod tenant_scope_tests {
    use super::*;
    use tonic::metadata::MetadataValue;

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
}

impl DataBrokerService {
    /// Build the native `NotificationService`, wired to the broker's Postgres pool.
    pub(crate) fn build_notification_service(&self) -> NotificationServiceImpl {
        let runtime = self.runtime.load_full();
        let pg_pool = runtime.pg_pool().ok().cloned();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        NotificationServiceImpl::new()
            .with_postgres(pg_pool)
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
