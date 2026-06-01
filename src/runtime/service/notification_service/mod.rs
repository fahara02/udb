//! Native `NotificationService` — proto-driven Postgres CRUD over the UDB-owned
//! `udb_notification.{notification_logs,notification_templates,notification_preferences}`
//! tables. Like `auth_service`/`tenant_service`: no in-memory store, identifiers
//! resolved from the embedded proto manifest via [`NativeModel`].
//!
//! This is the control-plane surface (persist + query notification state,
//! templates, and preferences, and aggregate delivery stats). Actual outbound
//! delivery (SES/Twilio/FCM/webhook) is performed by separate delivery adapters;
//! `SendNotification` records the intent as a `NotificationLog` row.

use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::common::v1 as common_pb;
use crate::proto::udb::core::notification::entity::v1 as notif_entity_pb;
use crate::proto::udb::core::notification::services::v1 as notif_pb;
use crate::proto::udb::core::notification::services::v1::notification_service_server::NotificationService;
use crate::runtime::native_catalog::{NativeModel, native_model};

pub use crate::proto::udb::core::notification::services::v1::notification_service_server::NotificationServiceServer;

use super::DataBrokerService;

const LOG_MSG: &str = "udb.core.notification.entity.v1.NotificationLog";
const TEMPLATE_MSG: &str = "udb.core.notification.entity.v1.NotificationTemplate";
const PREFERENCE_MSG: &str = "udb.core.notification.entity.v1.NotificationPreference";

pub struct NotificationServiceImpl {
    pg_pool: Option<PgPool>,
    /// Schema-qualified outbox table (`udb_system.outbox_events`) the CDC engine
    /// tails → Apache Kafka → the Spark streaming consumer. `None` = no emit.
    outbox_relation: Option<String>,
}

/// Kafka topic for the "notification sent" domain event.
const NOTIFICATION_SENT_TOPIC: &str = "udb.notification.sent.v1";

impl NotificationServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            outbox_relation: None,
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
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

    /// Best-effort: enqueue a "notification sent" event into the shared outbox
    /// (→ CDC → Kafka → Spark). Uses the same envelope shape as the broker's
    /// `prepare_outbox_envelope` / auth `OutboxAuthEventSink` so the CDC engine
    /// forwards it unchanged. Never fails the RPC.
    async fn emit_sent_event(
        &self,
        pool: &PgPool,
        log_id: &str,
        event_type: &str,
        recipient_id: &str,
        tenant_id: &str,
        channels: &[i32],
    ) {
        let Some(rel) = self.outbox_relation.as_deref() else {
            return;
        };
        let event_id = Uuid::new_v4().to_string();
        let envelope = serde_json::json!({
            "event_id": event_id,
            "event_type": NOTIFICATION_SENT_TOPIC,
            "correlation_id": log_id,
            "document_id": recipient_id,
            "payload": {
                "log_id": log_id,
                "event_type": event_type,
                "recipient_id": recipient_id,
                "tenant_id": tenant_id,
                "channels": channels.iter().map(|c| channel_to_db(*c)).collect::<Vec<_>>(),
            },
        });
        let sql = format!(
            "INSERT INTO {rel} (event_id, topic, partition_key, payload, created_at) \
             VALUES ($1::UUID, $2, $3, $4::JSONB, NOW())"
        );
        if let Err(err) = sqlx::query(&sql)
            .bind(&event_id)
            .bind(NOTIFICATION_SENT_TOPIC)
            .bind(recipient_id)
            .bind(envelope.to_string())
            .execute(pool)
            .await
        {
            tracing::warn!(topic = NOTIFICATION_SENT_TOPIC, error = %err, "notification outbox enqueue failed");
        }
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

fn parse_uuid(field: &str, value: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(value.trim())
        .map_err(|_| Status::invalid_argument(format!("{field} must be a valid UUID")))
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
    ]
    .join(", ")
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

fn page_size_of(page: &Option<common_pb::PageRequest>) -> (i64, i64) {
    let p = page.as_ref();
    let size = p
        .map(|x| x.page_size)
        .filter(|&n| n > 0)
        .unwrap_or(50)
        .min(500) as i64;
    let num = p.map(|x| x.page).filter(|&n| n > 0).unwrap_or(1) as i64;
    (size, (num - 1).max(0) * size)
}

/// Build a `PageResponse` from the requested page and the true total row count
/// (obtained via `COUNT(*) OVER()` on the list query). Mirrors the auth
/// service's `page_response` shape so clients get consistent pagination metadata.
fn page_response_of(page: &Option<common_pb::PageRequest>, total: i64) -> common_pb::PageResponse {
    let page_number = page
        .as_ref()
        .map(|p| p.page)
        .filter(|&n| n > 0)
        .unwrap_or(1);
    let page_size = page
        .as_ref()
        .map(|p| p.page_size)
        .filter(|&n| n > 0)
        .unwrap_or(50);
    let total_pages = if total <= 0 {
        0
    } else {
        ((total as i32) + page_size - 1) / page_size
    };
    common_pb::PageResponse {
        page: page_number,
        page_size,
        total_items: total,
        total_pages,
        next_page_token: String::new(),
        total_count: total,
        has_next: page_number < total_pages,
        has_previous: page_number > 1 && total_pages > 0,
    }
}

#[tonic::async_trait]
impl NotificationService for NotificationServiceImpl {
    async fn send_notification(
        &self,
        request: Request<notif_pb::SendNotificationRequest>,
    ) -> Result<Response<notif_pb::SendNotificationResponse>, Status> {
        let req = request.into_inner();
        if req.event_type.trim().is_empty() {
            return Err(Status::invalid_argument("event_type is required"));
        }
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
            let log_id = Uuid::new_v4().to_string();
            sqlx::query(&format!(
                "INSERT INTO {rel} \
                 ({log_id}, {event_type}, {channel}, {recipient_id}, {recipient_address}, \
                  {tenant_id}, {project_id}, {resource_type}, {resource_id}, {resource_name}, \
                  {correlation_id}, {status}, {retry_count}) \
                 VALUES ($1::UUID, $2, $3, NULLIF($4, '')::UUID, $5, $6, $7, $8, $9, $10, $11, 'PENDING', 0)",
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
                status: notif_entity_pb::NotificationStatus::Pending as i32,
                ..Default::default()
            });
        }
        // Publish the "notification sent" event to the outbox → CDC → Kafka.
        let primary_log_id = logs.first().map(|l| l.log_id.clone()).unwrap_or_default();
        self.emit_sent_event(
            pool,
            &primary_log_id,
            &req.event_type,
            &req.recipient_id,
            &req.tenant_id,
            &channels,
        )
        .await;
        Ok(Response::new(notif_pb::SendNotificationResponse { logs }))
    }

    async fn get_notification(
        &self,
        request: Request<notif_pb::GetNotificationRequest>,
    ) -> Result<Response<notif_pb::GetNotificationResponse>, Status> {
        let req = request.into_inner();
        let log_id = parse_uuid("log_id", &req.log_id)?;
        let pool = self.require_pool()?;
        let m = log_model();
        let rel = m.relation.clone();
        let projection = log_select_projection(&m);
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} WHERE {log_id} = $1::UUID",
            log_id = m.q("log_id"),
        ))
        .bind(log_id)
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
        let req = request.into_inner();
        let pool = self.require_pool()?;
        let m = log_model();
        let rel = m.relation.clone();
        let projection = log_select_projection(&m);
        let (limit, offset) = page_size_of(&req.page);
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
        .bind(limit)
        .bind(offset)
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
            page: Some(page_response_of(&req.page, total)),
        }))
    }

    async fn retry_notification(
        &self,
        request: Request<notif_pb::RetryNotificationRequest>,
    ) -> Result<Response<notif_pb::RetryNotificationResponse>, Status> {
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
             WHERE {log_id} = $1::UUID AND {status} IN ('FAILED','SUPPRESSED') \
             RETURNING {projection}",
            status = m.q("status"),
            retry = m.q("retry_count"),
            log_id = m.q("log_id"),
        ))
        .bind(log_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("retry notification failed: {err}")))?;
        let log = match row {
            Some(row) => Some(log_from_row(&row)?),
            None => {
                return Err(Status::failed_precondition(
                    "notification not found or not in a retryable (FAILED) state",
                ));
            }
        };
        Ok(Response::new(notif_pb::RetryNotificationResponse { log }))
    }

    async fn upsert_template(
        &self,
        request: Request<notif_pb::UpsertTemplateRequest>,
    ) -> Result<Response<notif_pb::UpsertTemplateResponse>, Status> {
        let req = request.into_inner();
        if req.event_type.trim().is_empty() {
            return Err(Status::invalid_argument("event_type is required"));
        }
        let pool = self.require_pool()?;
        let m = template_model();
        let rel = m.relation.clone();
        let locale = if req.locale.trim().is_empty() {
            "en".to_string()
        } else {
            req.locale.clone()
        };
        let projection = template_select_projection(&m);
        let row = sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({template_id}, {event_type}, {channel}, {subject}, {body}, {locale}, {is_active}) \
             VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6) \
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
        let req = request.into_inner();
        let pool = self.require_pool()?;
        let m = template_model();
        let rel = m.relation.clone();
        let locale = if req.locale.trim().is_empty() {
            "en".to_string()
        } else {
            req.locale.clone()
        };
        let projection = template_select_projection(&m);
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} \
             WHERE {event_type} = $1 AND {channel} = $2 AND {locale} = $3 AND {deleted} IS NULL",
            event_type = m.q("event_type"),
            channel = m.q("channel"),
            locale = m.q("locale"),
            deleted = m.q("deleted_at"),
        ))
        .bind(&req.event_type)
        .bind(channel_to_db(req.channel))
        .bind(&locale)
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
        let req = request.into_inner();
        let pool = self.require_pool()?;
        let m = template_model();
        let rel = m.relation.clone();
        let projection = template_select_projection(&m);
        let (limit, offset) = page_size_of(&req.page);
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
             ORDER BY {event_type} LIMIT $4 OFFSET $5",
            deleted = m.q("deleted_at"),
            event_type = m.q("event_type"),
            channel = m.q("channel"),
            is_active = m.q("is_active"),
        ))
        .bind(&req.event_type)
        .bind(&channel)
        .bind(req.active_only)
        .bind(limit)
        .bind(offset)
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
            page: Some(page_response_of(&req.page, total)),
        }))
    }

    async fn get_delivery_stats(
        &self,
        request: Request<notif_pb::GetDeliveryStatsRequest>,
    ) -> Result<Response<notif_pb::GetDeliveryStatsResponse>, Status> {
        let req = request.into_inner();
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
        let req = request.into_inner();
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
        let req = request.into_inner();
        let user_id = parse_uuid("user_id", &req.user_id)?;
        let pool = self.require_pool()?;
        let m = preference_model();
        let rel = m.relation.clone();
        let projection = preference_select_projection(&m);
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} \
             WHERE {user_id} = $1::UUID AND {channel} = $2 AND {event_type} = $3",
            user_id = m.q("user_id"),
            channel = m.q("channel"),
            event_type = m.q("event_type"),
        ))
        .bind(user_id)
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
        let req = request.into_inner();
        let user_id = parse_uuid("user_id", &req.user_id)?;
        let pool = self.require_pool()?;
        let m = preference_model();
        let rel = m.relation.clone();
        let projection = preference_select_projection(&m);
        let (limit, offset) = page_size_of(&req.page);
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
        .bind(limit)
        .bind(offset)
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
            page: Some(page_response_of(&req.page, total)),
        }))
    }
}

impl DataBrokerService {
    /// Build the native `NotificationService`, wired to the broker's Postgres pool.
    pub(crate) fn build_notification_service(&self) -> NotificationServiceImpl {
        let runtime = self.runtime.load_full();
        let pg_pool = runtime.pg_pool().ok().cloned();
        let outbox = runtime.config().cdc.outbox_relation();
        NotificationServiceImpl::new()
            .with_postgres(pg_pool)
            .with_outbox(Some(outbox))
    }
}
