//! Native `NotificationService` — proto-driven Postgres CRUD over the UDB-owned
//! `udb_notification.{notification_logs,notification_templates,notification_preferences}`
//! tables. Like `auth_service`/`tenant_service`: no in-memory store, identifiers
//! resolved from the embedded proto manifest via [`NativeModel`].
//!
//! This is the control-plane surface (persist + query notification state,
//! templates, and preferences, and aggregate delivery stats). Actual outbound
//! delivery (SES/Twilio/FCM/webhook) is performed by separate delivery adapters;
//! `SendNotification` records the intent as a `NotificationLog` row.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{
    AggregateExpr, AggregateFunc, ComparisonOp, ConflictStrategy, LogicalAggregate, LogicalFilter,
    LogicalPagination, LogicalProjection, LogicalRead, LogicalRecord, LogicalSort, LogicalValue,
    NullOrder, SortDirection,
};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::notification::entity::v1 as notif_entity_pb;
use crate::proto::udb::core::notification::services::v1 as notif_pb;
use crate::proto::udb::core::notification::services::v1::notification_service_server::NotificationService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, OperationChannel};
use crate::runtime::native_catalog::{NativeModel, native_model};

pub use crate::proto::udb::core::notification::services::v1::notification_service_server::NotificationServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    NativeEventContext, admit_on as native_admit_on, enqueue_outbox_event_with_context,
    metadata_tenant_id, native_page_response, native_page_window, native_service_context,
    parse_uuid, validate_request_scope, validate_request_tenant,
};

const LOG_MSG: &str = "udb.core.notification.entity.v1.NotificationLog";
const TEMPLATE_MSG: &str = "udb.core.notification.entity.v1.NotificationTemplate";
const PREFERENCE_MSG: &str = "udb.core.notification.entity.v1.NotificationPreference";
/// Native message id for the master-plan 9.13 delivery-status record.
const DELIVERY_ATTEMPT_MSG: &str = "udb.core.notification.entity.v1.NotificationDeliveryAttempt";
const TEMPLATE_LOCALE_MAX_CHARS: usize = 10;
pub(crate) const NOTIFICATION_DELIVERY_BATCH: i64 = 200;
const DEFAULT_NOTIFICATION_DELIVERY_INTERVAL_SECS: u64 = 30;
const NOTIFICATION_DELIVERY_INTERVAL_ENV: &str = "UDB_NOTIFICATION_DELIVERY_INTERVAL_SECS";
const NOTIFICATION_DELIVERY_PROVIDERS_ENV: &str = "UDB_NOTIFICATION_DELIVERY_PROVIDERS_JSON";

// Stable machine-readable error reasons (google.rpc.ErrorInfo-style `reason`),
// attached to the returned `Status` metadata under `error-reason` so SDK clients
// can branch on a fixed code instead of free-text messages. Only reasons with a
// LIVE failure path are fired here. `TEMPLATE_INACTIVE`/`PROVIDER_UNAVAILABLE`
// have no reachable site and are intentionally NOT defined.
/// No template matched the (event_type, channel, locale, tenant) scope on send.
const TEMPLATE_NOT_FOUND: &str = "TEMPLATE_NOT_FOUND";
/// A `{{placeholder}}` in the matched template had no value in `req.variables`.
const VARIABLE_MISSING: &str = "VARIABLE_MISSING";
/// Reserved: recipient opted out of the (channel, event) — currently handled as a
/// SUPPRESSED log row, not an error, so no live site fires this reason yet.
#[allow(dead_code)]
const RECIPIENT_OPTED_OUT: &str = "RECIPIENT_OPTED_OUT";
/// Retry was requested for a notification that is not in a retryable state.
const NOT_RETRYABLE_STATE: &str = "NOT_RETRYABLE_STATE";

/// Test-only sentinel (TODO 04.4.2.2): when `test_mode_enabled()` is true AND a
/// `SendNotification` request carries this exact value in `resource_type`, each
/// per-channel log is written as FAILED so `retry_notification` has a real
/// retryable row to exercise. Unreachable in production: the env gate defaults
/// false, so production sends ignore this value entirely.
const TEST_FORCE_FAILED_SENTINEL: &str = "__perf_force_failed__";

/// Attach a stable machine-readable `reason` (and optional metadata pairs) to a
/// `Status` without changing its gRPC `Code`. The reason rides in the trailing
/// `error-reason` metadata key; extra `(key, value)` pairs (e.g. the offending
/// variable name) ride alongside it. Bad ASCII keys/values are skipped so this
/// can never itself fail.
fn status_with_reason(mut status: Status, reason: &str, extra: &[(&str, &str)]) -> Status {
    let md = status.metadata_mut();
    if let Ok(value) = reason.parse::<tonic::metadata::MetadataValue<_>>() {
        md.insert("error-reason", value);
    }
    for (key, raw) in extra {
        if let (Ok(name), Ok(value)) = (
            key.parse::<tonic::metadata::MetadataKey<_>>(),
            raw.parse::<tonic::metadata::MetadataValue<_>>(),
        ) {
            md.insert(name, value);
        }
    }
    status
}

/// Whether the notification test harness is enabled (resolved once). Fail-closed
/// default: production never sets `UDB_NOTIFICATION_TEST_MODE`, so the test-only
/// forced-FAILED send path below is unreachable in production. Mirrors the exact
/// shape of `auth_service::authn::webauthn_softauth::test_mode_enabled`.
fn test_mode_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("UDB_NOTIFICATION_TEST_MODE")
            .map(|v| {
                let v = v.trim();
                v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes")
            })
            .unwrap_or(false)
    })
}

pub struct NotificationServiceImpl {
    pg_pool: Option<PgPool>,
    /// Runtime handle for P4 native-entity data-plane operations. Straightforward
    /// notification CRUD persists as typed proto entities through the native
    /// catalog, neutral IR compiler, and selected backend executor.
    runtime: Option<Arc<DataBrokerRuntime>>,
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

fn notification_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "notification",
        operation,
        capability_required,
        message,
    )
}

fn notification_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("notification", operation, message)
}

fn notification_policy_status_with_reason(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: &'static str,
    reason: &'static str,
) -> Status {
    status_with_reason(
        crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message),
        reason,
        &[],
    )
}

fn notification_policy_status_with_code(
    code: tonic::Code,
    operation: &'static str,
    policy_decision_id: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        code,
        operation,
        policy_decision_id,
        message,
    )
}

fn notification_tenant_metadata_required_status(operation: &'static str) -> Status {
    notification_policy_status_with_code(
        tonic::Code::PermissionDenied,
        operation,
        "tenant_metadata_required",
        "tenant-scoped metadata is required",
    )
}

fn notification_not_retryable_status() -> Status {
    notification_policy_status_with_reason(
        "retry_notification",
        "notification_not_retryable",
        "notification not found or not in a retryable (FAILED) state",
        NOT_RETRYABLE_STATE,
    )
}

fn notification_schema_not_found_status(
    operation: &'static str,
    schema_code: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "notification",
        operation,
        schema_code,
        message,
    )
}

fn notification_template_not_found_status(
    operation: &'static str,
    message: impl Into<String>,
) -> Status {
    status_with_reason(
        notification_schema_not_found_status(operation, "notification_template_not_found", message),
        TEMPLATE_NOT_FOUND,
        &[],
    )
}

/// Kafka topic for the "notification sent" domain event.
const NOTIFICATION_SENT_TOPIC: &str = "udb.notification.sent.v1";

impl NotificationServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    /// Wire the runtime used for typed native-entity notification persistence.
    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    /// Typed notification entity operations fail closed when no runtime is wired.
    fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            notification_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "notification service requires runtime native entity dispatch",
            )
        })
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
            notification_capability_status(
                "postgres_store",
                "postgres_store",
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

    /// Best-effort: publish a terminal delivery outcome (master-plan 9.13) to
    /// `udb.notification.delivery.<status>.v1` with the compliance envelope. The
    /// payload carries only routing/outcome metadata — never provider secrets.
    /// Never fails the RPC.
    #[allow(clippy::too_many_arguments)]
    async fn emit_delivery_event(
        &self,
        pool: &PgPool,
        log_id: &str,
        tenant_id: &str,
        project_id: &str,
        channel_db: &str,
        provider: &str,
        status_db: &str,
        provider_message_id: &str,
    ) {
        let outcome = if status_db == "FAILED" {
            "failure"
        } else {
            "allow"
        };
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            &delivery_event_topic(status_db),
            log_id,
            tenant_id,
            project_id,
            serde_json::json!({
                "log_id": log_id,
                "tenant_id": tenant_id,
                "project_id": project_id,
                "channel": channel_db,
                "provider": provider,
                "status": status_db,
                "provider_message_id": provider_message_id,
            }),
            NativeEventContext {
                operation: "notification.deliver".to_string(),
                outcome: outcome.to_string(),
                target_resource: log_id.to_string(),
                ..NativeEventContext::default()
            },
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
            "rendered_subject",
            "rendered_body",
        ],
    )
}

pub(crate) fn notification_delivery_interval() -> Duration {
    static INTERVAL: OnceLock<Duration> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        Duration::from_secs(
            std::env::var(NOTIFICATION_DELIVERY_INTERVAL_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_NOTIFICATION_DELIVERY_INTERVAL_SECS),
        )
    })
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

/// Inverse of [`status_from_db`]: a delivery status enum → its stored short name.
/// `UNSPECIFIED` is returned for an unknown/0 status so the `ReportDelivery`
/// handler can fail closed on a caller that omits a terminal status.
fn status_to_db(value: i32) -> &'static str {
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

/// The versioned dot-topic a terminal delivery outcome is published on
/// (master-plan 9.13): `udb.notification.delivery.<status>.v1`, e.g.
/// `udb.notification.delivery.delivered.v1`. Pure + unit-tested.
fn delivery_event_topic(status_db: &str) -> String {
    format!(
        "udb.notification.delivery.{}.v1",
        status_db.to_ascii_lowercase()
    )
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

fn notification_log_filter(
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

fn notification_log_list_read(filter: LogicalFilter, offset: u64, limit: u32) -> LogicalRead {
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

fn delivery_stats_aggregate(tenant_id: &str, event_type: &str) -> LogicalAggregate {
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

fn notification_log_read(log_id: &str, tenant_id: &str) -> LogicalRead {
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

fn template_scope_filter(
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

fn template_locale_or_default(locale: &str) -> Result<String, Status> {
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

fn notification_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

fn template_read(filter: LogicalFilter, offset: u64, limit: u32) -> LogicalRead {
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

fn preference_read(user_id: &str, tenant_id: &str, channel: i32, event_type: &str) -> LogicalRead {
    LogicalRead {
        message_type: PREFERENCE_MSG.to_string(),
        filter: Some(preference_filter(user_id, tenant_id, channel, event_type)),
        projection: Some(preference_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

fn preference_list_filter(user_id: &str, tenant_id: &str) -> LogicalFilter {
    let mut filters = vec![eq_filter("user_id", user_id.to_string())];
    if !tenant_id.trim().is_empty() {
        filters.push(eq_filter("tenant_id", tenant_id.to_string()));
    }
    LogicalFilter::And(filters)
}

fn preference_list_read(filter: LogicalFilter, offset: u64, limit: u32) -> LogicalRead {
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

fn json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

fn json_string_field(row: &serde_json::Map<String, serde_json::Value>, field: &str) -> String {
    row.get(field)
        .and_then(|value| match value {
            serde_json::Value::String(value) => Some(value.clone()),
            serde_json::Value::Number(value) => Some(value.to_string()),
            serde_json::Value::Bool(value) => Some(value.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

fn json_bool_field(row: &serde_json::Map<String, serde_json::Value>, field: &str) -> bool {
    row.get(field)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn json_i32_field(row: &serde_json::Map<String, serde_json::Value>, field: &str) -> i32 {
    row.get(field)
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or(0)
}

fn json_i64_field(row: &serde_json::Map<String, serde_json::Value>, field: &str) -> i64 {
    row.get(field)
        .and_then(|value| match value {
            serde_json::Value::Number(value) => value.as_i64(),
            serde_json::Value::String(value) => value.parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or(0)
}

fn log_from_json(row: &serde_json::Value) -> notif_entity_pb::NotificationLog {
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

fn preference_from_json_row(row: &serde_json::Value) -> notif_entity_pb::NotificationPreference {
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

fn template_from_json_row(row: &serde_json::Value) -> notif_entity_pb::NotificationTemplate {
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

fn notification_log_record(
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

/// Render a `{{placeholder}}` template against the request `variables` map.
/// Substitution is minimal and consistent with the template comment
/// (`'{{.ResourceName}} requires review'`): every `{{ ... }}` token is trimmed,
/// an optional leading `.` is stripped (Go-template dotted form), and the result
/// is looked up in `variables`. A token with no value yields `Err(field_name)`
/// so the caller can fail-closed with `VARIABLE_MISSING` naming that field. No
/// regex dependency: a single forward scan over the bytes.
fn render_template(
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

async fn is_notification_opted_out(
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
#[cfg(test)]
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

fn delivery_attempt_model() -> NativeModel {
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

fn delivery_attempt_select_projection(m: &NativeModel) -> String {
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

fn epoch_to_ts(epoch: Option<i64>) -> Option<prost_types::Timestamp> {
    epoch.map(|seconds| prost_types::Timestamp { seconds, nanos: 0 })
}

fn delivery_attempt_from_row(
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

/// Upsert the single `NotificationDeliveryAttempt` row keyed on
/// (notification_id, channel, provider): set the terminal status, BUMP
/// `attempt_count`, record `last_error`/`provider_message_id`, and stamp the
/// updated timestamp. Shared by the `ReportDelivery` handler and the delivery
/// worker so the durable delivery record has ONE writer shape. Returns the stored
/// row (the handler maps it to the response; the worker ignores it).
#[allow(clippy::too_many_arguments)]
async fn write_delivery_attempt(
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
            return Err(notification_required_field(
                "event_type",
                "must be a non-empty notification event type",
                "event_type is required",
            ));
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
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &req.tenant_id, &req.project_id);
        // Default to EMAIL when the caller did not pin channels; one log per channel.
        let channels = if req.channels.is_empty() {
            vec![notif_entity_pb::NotificationChannel::Email as i32]
        } else {
            req.channels.clone()
        };
        let locale = template_locale_or_default(&req.locale)?;
        let mut logs = Vec::with_capacity(channels.len());
        for channel in channels.iter().copied() {
            // Resolve the active template for this (event_type, channel, locale,
            // tenant) scope. Identity/tenant come from the verified context only.
            let channel_db = channel_to_db(channel);
            let template_filter = template_scope_filter(
                &req.tenant_id,
                &req.event_type,
                channel_db,
                Some(&locale),
                true,
            );
            let template_rows = runtime
                .native_entity_read_for_service(
                    "notification",
                    &context,
                    template_read(template_filter, 0, 1),
                )
                .await?;
            let template = match template_rows.first() {
                Some(row) => template_from_json_row(row),
                None => {
                    return Err(notification_template_not_found_status(
                        "send_notification",
                        format!(
                            "no active notification template for event '{}' channel '{}' locale '{}'",
                            req.event_type, channel_db, locale
                        ),
                    ));
                }
            };
            // Render subject/body against the request variables; an unsatisfied
            // `{{placeholder}}` fails closed naming the missing variable.
            let rendered_subject = render_template(&template.subject_template, &req.variables)
                .map_err(|field| {
                    status_with_reason(
                        crate::runtime::executor_utils::invalid_argument_fields(
                            format!("template variable '{field}' is required but was not provided"),
                            [(
                                format!("variables.{field}"),
                                "template variable is required but was not provided",
                            )],
                        ),
                        VARIABLE_MISSING,
                        &[("error-variable", field.as_str())],
                    )
                })?;
            let rendered_body =
                render_template(&template.body_template, &req.variables).map_err(|field| {
                    status_with_reason(
                        crate::runtime::executor_utils::invalid_argument_fields(
                            format!("template variable '{field}' is required but was not provided"),
                            [(
                                format!("variables.{field}"),
                                "template variable is required but was not provided",
                            )],
                        ),
                        VARIABLE_MISSING,
                        &[("error-variable", field.as_str())],
                    )
                })?;
            let opted_out = is_notification_opted_out(
                runtime,
                &context,
                &req.recipient_id,
                &req.tenant_id,
                channel,
                &req.event_type,
            )
            .await?;
            let (mut status_db, mut status_pb) = channel_send_decision(opted_out);
            // Test-only forced-FAILED path (TODO 04.4.2.2): gated false in prod.
            let mut error_message = String::new();
            if test_mode_enabled() && req.resource_type == TEST_FORCE_FAILED_SENTINEL {
                status_db = "FAILED";
                status_pb = notif_entity_pb::NotificationStatus::Failed as i32;
                error_message = "forced FAILED by UDB_NOTIFICATION_TEST_MODE harness".to_string();
            }
            let log_id = Uuid::new_v4().to_string();
            let log = notif_entity_pb::NotificationLog {
                log_id,
                template_id: template.template_id.clone(),
                event_type: req.event_type.clone(),
                channel,
                recipient_id: req.recipient_id.clone(),
                recipient_address: req.recipient_address.clone(),
                tenant_id: req.tenant_id.clone(),
                project_id: req.project_id.clone(),
                resource_type: req.resource_type.clone(),
                resource_id: req.resource_id.clone(),
                resource_name: req.resource_name.clone(),
                correlation_id: req.correlation_id.clone(),
                status: status_pb,
                error_message,
                rendered_subject,
                rendered_body,
                ..Default::default()
            };
            runtime
                .native_entity_write_for_service(
                    "notification",
                    &context,
                    LOG_MSG,
                    notification_log_record(&log, status_db),
                    ConflictStrategy::Error,
                )
                .await?;
            logs.push(log);
        }
        let delivery_channels = deliverable_channels(&logs);
        if !delivery_channels.is_empty() {
            if let Some(pool) = self.pg_pool.as_ref() {
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
        }
        Ok(Response::new(notif_pb::SendNotificationResponse { logs }))
    }

    async fn get_notification(
        &self,
        request: Request<notif_pb::GetNotificationRequest>,
    ) -> Result<Response<notif_pb::GetNotificationResponse>, Status> {
        let metadata = request.metadata().clone();
        let scoped_tenant = metadata_tenant_id(&metadata)
            .ok_or_else(|| notification_tenant_metadata_required_status("get_notification"))?;
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
        let log_id = parse_uuid("log_id", &req.log_id)?.to_string();
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &scoped_tenant, "");
        let rows = runtime
            .native_entity_read_for_service(
                "notification",
                &context,
                notification_log_read(&log_id, &scoped_tenant),
            )
            .await?;
        let log = match rows.first() {
            Some(row) => Some(log_from_json(row)),
            None => {
                return Err(notification_schema_not_found_status(
                    "get_notification",
                    "notification_not_found",
                    "notification not found",
                ));
            }
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
        let filter = notification_log_filter(
            &req.tenant_id,
            &req.project_id,
            &req.recipient_id,
            &req.event_type,
            &channel,
            &status,
            &req.resource_type,
            &req.resource_id,
        );
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &req.tenant_id, &req.project_id);
        let total = runtime
            .native_entity_count_for_service(
                "notification",
                &context,
                LOG_MSG,
                Some(filter.clone()),
            )
            .await?;
        let rows = runtime
            .native_entity_read_for_service(
                "notification",
                &context,
                notification_log_list_read(filter, page.offset as u64, page.limit as u32),
            )
            .await?;
        let logs = rows.iter().map(log_from_json).collect();
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
            .ok_or_else(|| notification_tenant_metadata_required_status("retry_notification"))?;
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
        // Transitional: retry needs conditional update plus retry_count + 1 with
        // RETURNING. The current typed write helper cannot express increments or
        // status-gated updates, so this path remains capability-gated to Postgres.
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
        .map_err(|err| {
            notification_internal_status(
                "retry_notification_update",
                format!("retry notification failed: {err}"),
            )
        })?;
        let log = match row {
            Some(row) => log_from_row(&row)?,
            None => {
                return Err(notification_not_retryable_status());
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

    async fn report_delivery(
        &self,
        request: Request<notif_pb::ReportDeliveryRequest>,
    ) -> Result<Response<notif_pb::ReportDeliveryResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // Tenant comes from the verified claim: a foreign tenant_id in the body is
        // rejected before any store access (mirrors the rest of the service).
        validate_request_tenant(&metadata, &req.tenant_id)?;
        if req.log_id.trim().is_empty() {
            return Err(notification_required_field(
                "log_id",
                "must be a non-empty notification log id",
                "log_id is required",
            ));
        }
        // Fail closed on a caller that omits a terminal delivery status.
        let status_db = status_to_db(req.status);
        if status_db == "UNSPECIFIED" {
            return Err(notification_required_field(
                "status",
                "must be one of SENT, DELIVERED, FAILED, or PENDING",
                "a terminal delivery status (SENT|DELIVERED|FAILED|PENDING) is required",
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "notification",
            OperationChannel::Write,
            &req.tenant_id,
            None,
        )
        .await?;
        let log_id = parse_uuid("log_id", &req.log_id)?;
        let channel_db = channel_to_db(req.channel);
        let provider = req.provider.trim();
        let pool = self.require_pool()?;
        // Upsert the durable per-(notification, channel, provider) delivery record.
        let row = write_delivery_attempt(
            pool,
            log_id,
            &req.tenant_id,
            channel_db,
            provider,
            status_db,
            &req.error_message,
            &req.provider_message_id,
        )
        .await
        .map_err(|err| {
            notification_internal_status(
                "report_delivery_attempt",
                format!("report delivery failed: {err}"),
            )
        })?;
        let attempt = row.as_ref().map(delivery_attempt_from_row).transpose()?;
        // Emit `udb.notification.delivery.<status>.v1` (best-effort; no secrets).
        let project_id = native_service_context(&metadata, &req.tenant_id, "").project_id;
        self.emit_delivery_event(
            pool,
            &req.log_id,
            &req.tenant_id,
            &project_id,
            channel_db,
            provider,
            status_db,
            &req.provider_message_id,
        )
        .await;
        Ok(Response::new(notif_pb::ReportDeliveryResponse { attempt }))
    }

    async fn upsert_template(
        &self,
        request: Request<notif_pb::UpsertTemplateRequest>,
    ) -> Result<Response<notif_pb::UpsertTemplateResponse>, Status> {
        let req = request.into_inner();
        if req.event_type.trim().is_empty() {
            return Err(notification_required_field(
                "event_type",
                "must be a non-empty notification event type",
                "event_type is required",
            ));
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
        // Transitional: global template upsert conflicts on (event_type, channel),
        // not the manifest primary key, and returns the stored row.
        let pool = self.require_pool()?;
        let m = template_model();
        let rel = m.relation.clone();
        let locale = template_locale_or_default(&req.locale)?;
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
        .map_err(|err| {
            notification_internal_status(
                "upsert_template_query",
                format!("upsert template failed: {err}"),
            )
        })?;
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
            .ok_or_else(|| notification_tenant_metadata_required_status("get_template"))?;
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
        let locale = template_locale_or_default(&req.locale)?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &scoped_tenant, "");
        let filter = template_scope_filter(
            &scoped_tenant,
            &req.event_type,
            channel_to_db(req.channel),
            Some(&locale),
            false,
        );
        let rows = runtime
            .native_entity_read_for_service("notification", &context, template_read(filter, 0, 1))
            .await?;
        let template = match rows.first() {
            Some(row) => Some(template_from_json_row(row)),
            None => {
                return Err(notification_template_not_found_status(
                    "get_template",
                    "template not found",
                ));
            }
        };
        Ok(Response::new(notif_pb::GetTemplateResponse { template }))
    }

    async fn list_templates(
        &self,
        request: Request<notif_pb::ListTemplatesRequest>,
    ) -> Result<Response<notif_pb::ListTemplatesResponse>, Status> {
        let metadata = request.metadata().clone();
        let scoped_tenant = metadata_tenant_id(&metadata)
            .ok_or_else(|| notification_tenant_metadata_required_status("list_templates"))?;
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
        let page = native_page_window(req.page.as_ref(), 50);
        let channel = if req.channel == 0 {
            String::new()
        } else {
            channel_to_db(req.channel).to_string()
        };
        let filter = template_scope_filter(
            &scoped_tenant,
            &req.event_type,
            &channel,
            None,
            req.active_only,
        );
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &scoped_tenant, "");
        let total = runtime
            .native_entity_count_for_service(
                "notification",
                &context,
                TEMPLATE_MSG,
                Some(filter.clone()),
            )
            .await?;
        let rows = runtime
            .native_entity_read_for_service(
                "notification",
                &context,
                template_read(filter, page.offset as u64, page.limit as u32),
            )
            .await?;
        let templates = rows.iter().map(template_from_json_row).collect();
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
        if req.date_from.trim().is_empty() && req.date_to.trim().is_empty() {
            let runtime = self.require_runtime()?;
            let context = native_service_context(&metadata, &req.tenant_id, "");
            let rows = runtime
                .native_entity_aggregate_for_service(
                    "notification",
                    &context,
                    delivery_stats_aggregate(&req.tenant_id, &req.event_type),
                )
                .await?;
            let (mut total_sent, mut total_delivered, mut total_failed) = (0i64, 0i64, 0i64);
            let mut by_channel = std::collections::BTreeMap::<i32, notif_pb::ChannelStats>::new();
            for row in &rows {
                let row = json_object(row);
                let channel = channel_from_db(&json_string_field(row, "channel"));
                let status = json_string_field(row, "status");
                let n = json_i64_field(row, "n");
                let entry = by_channel
                    .entry(channel)
                    .or_insert_with(|| notif_pb::ChannelStats {
                        channel,
                        ..Default::default()
                    });
                match status.as_str() {
                    "SENT" => {
                        entry.sent += n;
                        total_sent += n;
                    }
                    "DELIVERED" => {
                        entry.sent += n;
                        entry.delivered += n;
                        total_sent += n;
                        total_delivered += n;
                    }
                    "FAILED" => {
                        entry.failed += n;
                        total_failed += n;
                    }
                    "SUPPRESSED" => {
                        entry.suppressed += n;
                    }
                    _ => {}
                }
            }
            let mut by_channel = by_channel.into_values().collect::<Vec<_>>();
            for entry in &mut by_channel {
                entry.delivery_rate = if entry.sent > 0 {
                    entry.delivered as f64 / entry.sent as f64
                } else {
                    0.0
                };
            }
            let overall_delivery_rate = if total_sent > 0 {
                total_delivered as f64 / total_sent as f64
            } else {
                0.0
            };
            return Ok(Response::new(notif_pb::GetDeliveryStatsResponse {
                total_sent,
                total_delivered,
                total_failed,
                overall_delivery_rate,
                by_channel,
            }));
        }
        // Transitional: date-window filters require backend date casts; the
        // no-window per-channel aggregate uses LogicalAggregate above.
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
        .map_err(|err| {
            notification_internal_status(
                "delivery_stats_query",
                format!("delivery stats failed: {err}"),
            )
        })?;

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
            return Err(notification_required_field(
                "tenant_id",
                "must be a non-empty tenant id",
                "tenant_id is required",
            ));
        }
        // Transitional: public upsert conflicts on (user_id, channel, event_type),
        // not the manifest primary key. Keep exact Postgres semantics until the
        // typed write helper can target alternate unique keys.
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
        .map_err(|err| {
            notification_internal_status(
                "set_preference_query",
                format!("set preference failed: {err}"),
            )
        })?;
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
        let user_id = parse_uuid("user_id", &req.user_id)?.to_string();
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &req.tenant_id, "");
        let rows = runtime
            .native_entity_read_for_service(
                "notification",
                &context,
                preference_read(&user_id, &req.tenant_id, req.channel, &req.event_type),
            )
            .await?;
        let preference = match rows.first() {
            Some(row) => Some(preference_from_json_row(row)),
            None => {
                return Err(notification_schema_not_found_status(
                    "get_preference",
                    "notification_preference_not_found",
                    "preference not found",
                ));
            }
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
        let page = native_page_window(req.page.as_ref(), 50);
        let user_id = user_id.to_string();
        let filter = preference_list_filter(&user_id, &req.tenant_id);
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &req.tenant_id, "");
        let total = runtime
            .native_entity_count_for_service(
                "notification",
                &context,
                PREFERENCE_MSG,
                Some(filter.clone()),
            )
            .await?;
        let rows = runtime
            .native_entity_read_for_service(
                "notification",
                &context,
                preference_list_read(filter, page.offset as u64, page.limit as u32),
            )
            .await?;
        let preferences = rows.iter().map(preference_from_json_row).collect();
        Ok(Response::new(notif_pb::ListPreferencesResponse {
            preferences,
            page: Some(native_page_response(req.page.as_ref(), total, 50)),
        }))
    }
}

// ── leader-elected delivery worker (master-plan 9.13) ─────────────────────────
//
// The control-plane `SendNotification` path records each channel as a PENDING
// NotificationLog (the intent) and emits `udb.notification.sent.v1` — that
// EXISTING emission stays the source of intents. This worker is the delivery
// half: it drains queued intents, fetches each provider's credential through the
// VAULT transit seam (`DataBrokerRuntime::decrypt_secret_at_rest`, decrypt at
// use, never logged), SSRF-revalidates the provider URL at delivery time (reusing
// `webhook_service::resolve_and_validate_target`), attempts delivery per channel
// adapter, and writes a `NotificationDeliveryAttempt` via the SAME
// `write_delivery_attempt` upsert the `ReportDelivery` handler uses.
//
// `serve()` spawns [`run_notification_delivery_worker_once`] under
// `singleton::WORKER_NOTIFICATION_DELIVERY` when `http-client` is built. Provider
// configuration is intentionally generic and envelope-based:
// `UDB_NOTIFICATION_DELIVERY_PROVIDERS_JSON` is resolved once and contains only
// {channel, provider, endpoint_url, wrapped_credential}; provider-specific SDKs
// stay in sidecars.

/// A decrypted provider credential in flight. Redacting `Debug` from day one so
/// the secret never leaks into a log line or panic (the redaction doctrine,
/// mirroring `vault_service::PlaintextSecret`).
#[allow(dead_code)]
struct ProviderCredential(String);

impl std::fmt::Debug for ProviderCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ProviderCredential")
            .field(&"[redacted]")
            .finish()
    }
}

/// One configured delivery provider for a channel. `wrapped_credential` is the
/// vault-sealed API key (the `udb-aead:` envelope `encrypt_secret_at_rest`
/// produces); it is decrypted ONLY at the moment of use and never stored
/// plaintext or logged.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct NotificationDeliveryProvider {
    pub channel: i32,
    pub provider: String,
    /// The provider's HTTPS API endpoint (SSRF-validated at delivery time).
    pub endpoint_url: String,
    pub wrapped_credential: String,
}

// Redacting Debug so a provider config never leaks its wrapped credential.
impl std::fmt::Debug for NotificationDeliveryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotificationDeliveryProvider")
            .field("channel", &self.channel)
            .field("provider", &self.provider)
            .field("endpoint_url", &self.endpoint_url)
            .field("wrapped_credential", &"[redacted]")
            .finish()
    }
}

#[cfg(feature = "http-client")]
fn parse_delivery_channel(value: &serde_json::Value) -> Option<i32> {
    match value {
        serde_json::Value::Number(n) => n.as_i64().and_then(|v| i32::try_from(v).ok()),
        serde_json::Value::String(raw) => {
            let raw = raw.trim();
            if let Ok(n) = raw.parse::<i32>() {
                return Some(n);
            }
            let normalized = raw.to_ascii_uppercase().replace('-', "_");
            let channel = match normalized.as_str() {
                "EMAIL" => notif_entity_pb::NotificationChannel::Email,
                "SMS" => notif_entity_pb::NotificationChannel::Sms,
                "PUSH" => notif_entity_pb::NotificationChannel::Push,
                "IN_APP" => notif_entity_pb::NotificationChannel::InApp,
                "WEBHOOK" => notif_entity_pb::NotificationChannel::Webhook,
                _ => return None,
            };
            Some(channel as i32)
        }
        _ => None,
    }
}

#[cfg(feature = "http-client")]
fn parse_notification_delivery_providers_json(raw: &str) -> Vec<NotificationDeliveryProvider> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw.trim()) else {
        return Vec::new();
    };
    let Some(items) = value.as_array() else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            let obj = item.as_object()?;
            let channel = parse_delivery_channel(obj.get("channel")?)?;
            let provider = obj
                .get("provider")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_string();
            let endpoint_url = obj
                .get("endpoint_url")
                .or_else(|| obj.get("url"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_string();
            let wrapped_credential = obj
                .get("wrapped_credential")
                .or_else(|| obj.get("credential"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_string();
            if provider.is_empty() || endpoint_url.is_empty() || wrapped_credential.is_empty() {
                return None;
            }
            Some(NotificationDeliveryProvider {
                channel,
                provider,
                endpoint_url,
                wrapped_credential,
            })
        })
        .collect()
}

#[cfg(feature = "http-client")]
fn notification_delivery_providers() -> &'static Vec<NotificationDeliveryProvider> {
    static PROVIDERS: OnceLock<Vec<NotificationDeliveryProvider>> = OnceLock::new();
    PROVIDERS.get_or_init(|| {
        std::env::var(NOTIFICATION_DELIVERY_PROVIDERS_ENV)
            .ok()
            .map(|raw| parse_notification_delivery_providers_json(&raw))
            .unwrap_or_default()
    })
}

/// One queued notification intent (a PENDING NotificationLog) the worker drains.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct NotificationDeliveryIntent {
    pub log_id: String,
    pub tenant_id: String,
    pub channel: i32,
    pub recipient_address: String,
    pub rendered_subject: String,
    pub rendered_body: String,
}

#[cfg(feature = "http-client")]
fn intent_from_row(row: &sqlx::postgres::PgRow) -> Result<NotificationDeliveryIntent, String> {
    let channel_db: String = row
        .try_get("channel")
        .map_err(|err| format!("decode notification delivery channel failed: {err}"))?;
    Ok(NotificationDeliveryIntent {
        log_id: row
            .try_get("log_id")
            .map_err(|err| format!("decode notification delivery log id failed: {err}"))?,
        tenant_id: row
            .try_get("tenant_id")
            .map_err(|err| format!("decode notification delivery tenant failed: {err}"))?,
        channel: channel_from_db(&channel_db),
        recipient_address: row
            .try_get("recipient_address")
            .map_err(|err| format!("decode notification delivery recipient failed: {err}"))?,
        rendered_subject: row
            .try_get("rendered_subject")
            .map_err(|err| format!("decode notification delivery subject failed: {err}"))?,
        rendered_body: row
            .try_get("rendered_body")
            .map_err(|err| format!("decode notification delivery body failed: {err}"))?,
    })
}

#[cfg(feature = "http-client")]
async fn load_notification_delivery_intents(
    pool: &PgPool,
    batch: i64,
) -> Result<Vec<NotificationDeliveryIntent>, String> {
    let log = log_model();
    let attempt = delivery_attempt_model();
    let log_rel = log.relation.clone();
    let attempt_rel = attempt.relation.clone();
    let limit = batch.max(1);
    let rows = sqlx::query(&format!(
        "SELECT \
            l.{log_id}::TEXT AS log_id, \
            l.{tenant_id}::TEXT AS tenant_id, \
            l.{channel}::TEXT AS channel, \
            COALESCE(l.{recipient_address}::TEXT, '') AS recipient_address, \
            COALESCE(l.{rendered_subject}::TEXT, '') AS rendered_subject, \
            COALESCE(l.{rendered_body}::TEXT, '') AS rendered_body \
         FROM {log_rel} l \
         WHERE l.{status} = $1 \
           AND COALESCE(l.{tenant_id}::TEXT, '') <> '' \
           AND COALESCE(l.{recipient_address}::TEXT, '') <> '' \
           AND NOT EXISTS ( \
               SELECT 1 FROM {attempt_rel} a \
               WHERE a.{attempt_notification_id} = l.{log_id} \
                 AND a.{attempt_channel} = l.{channel} \
                 AND a.{attempt_status} IN ($2, $3) \
           ) \
         ORDER BY l.{created_at} ASC, l.{log_id} ASC \
         LIMIT $4",
        log_id = log.q("log_id"),
        tenant_id = log.q("tenant_id"),
        channel = log.q("channel"),
        recipient_address = log.q("recipient_address"),
        rendered_subject = log.q("rendered_subject"),
        rendered_body = log.q("rendered_body"),
        status = log.q("status"),
        created_at = log.q("created_at"),
        attempt_notification_id = attempt.q("notification_id"),
        attempt_channel = attempt.q("channel"),
        attempt_status = attempt.q("status"),
    ))
    .bind("PENDING")
    .bind("SENT")
    .bind("DELIVERED")
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load notification delivery intents failed: {err}"))?;

    rows.iter().map(intent_from_row).collect()
}

/// Write the terminal `NotificationDeliveryAttempt` (via the shared
/// `write_delivery_attempt` upsert) and emit `udb.notification.delivery.<status>.v1`.
/// Best-effort: a journal/emit failure is logged, never aborts the pass. The
/// `last_error` argument is a delivery diagnostic — NEVER a credential.
#[cfg(feature = "http-client")]
#[allow(clippy::too_many_arguments)]
async fn record_attempt_outcome(
    pool: &PgPool,
    outbox_relation: Option<&str>,
    metrics: Option<&Arc<dyn MetricsRecorder>>,
    notification_id: Uuid,
    intent: &NotificationDeliveryIntent,
    provider: &str,
    status_db: &str,
    last_error: &str,
    provider_message_id: &str,
) {
    let channel_db = channel_to_db(intent.channel);
    if let Err(err) = write_delivery_attempt(
        pool,
        notification_id,
        &intent.tenant_id,
        channel_db,
        provider,
        status_db,
        last_error,
        provider_message_id,
    )
    .await
    {
        tracing::warn!(log_id = %intent.log_id, error = %err, "notification delivery attempt write failed");
        return;
    }
    let outcome = if status_db == "FAILED" {
        "failure"
    } else {
        "allow"
    };
    enqueue_outbox_event_with_context(
        pool,
        outbox_relation,
        &delivery_event_topic(status_db),
        &intent.log_id,
        &intent.tenant_id,
        "",
        serde_json::json!({
            "log_id": intent.log_id,
            "tenant_id": intent.tenant_id,
            "channel": channel_db,
            "provider": provider,
            "status": status_db,
            "provider_message_id": provider_message_id,
        }),
        NativeEventContext {
            operation: "notification.deliver".to_string(),
            outcome: outcome.to_string(),
            target_resource: intent.log_id.clone(),
            ..NativeEventContext::default()
        },
        metrics,
    )
    .await;
}

/// Run ONE leader pass of notification delivery (the consumer shape `serve()`
/// spawns under `NativeWorkerHost::spawn_while_leader(WORKER_NOTIFICATION_DELIVERY)`).
/// For each queued intent: find the channel's provider, SSRF-revalidate its URL at
/// delivery time (DNS rebinding), decrypt its credential through the vault transit
/// seam (used, never logged), attempt delivery, and write a
/// `NotificationDeliveryAttempt` + emit the terminal event. Returns the count
/// delivered. Best-effort per intent: one failing provider never aborts the pass.
#[cfg(feature = "http-client")]
#[cfg_attr(test, allow(dead_code))]
#[allow(dead_code)]
pub(crate) async fn run_notification_delivery_once(
    http: &reqwest::Client,
    runtime: &DataBrokerRuntime,
    pool: &PgPool,
    outbox_relation: Option<&str>,
    providers: &[NotificationDeliveryProvider],
    intents: &[NotificationDeliveryIntent],
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) -> Result<u64, String> {
    let mut delivered: u64 = 0;
    for intent in intents {
        let Ok(notification_id) = Uuid::parse_str(intent.log_id.trim()) else {
            continue;
        };
        // The provider configured for this channel.
        let Some(provider) = providers.iter().find(|p| p.channel == intent.channel) else {
            record_attempt_outcome(
                pool,
                outbox_relation,
                metrics,
                notification_id,
                intent,
                "",
                "FAILED",
                "no delivery provider configured for channel",
                "",
            )
            .await;
            continue;
        };
        // SSRF guard at DELIVERY time (reuse the webhook resolver; defeats DNS
        // rebinding). A blocked target never becomes deliverable, so fail closed.
        if let Err(err) = crate::runtime::service::webhook_service::resolve_and_validate_target(
            &provider.endpoint_url,
        )
        .await
        {
            record_attempt_outcome(
                pool,
                outbox_relation,
                metrics,
                notification_id,
                intent,
                &provider.provider,
                "FAILED",
                err.message(),
                "",
            )
            .await;
            continue;
        }
        // Decrypt the provider credential ONLY at use; never log it.
        let credential = match runtime.decrypt_secret_at_rest(&provider.wrapped_credential) {
            Ok(secret) => ProviderCredential(secret),
            Err(_) => {
                record_attempt_outcome(
                    pool,
                    outbox_relation,
                    metrics,
                    notification_id,
                    intent,
                    &provider.provider,
                    "FAILED",
                    "provider credential unavailable (vault sealed?)",
                    "",
                )
                .await;
                continue;
            }
        };
        // Attempt delivery: POST the rendered message to the provider API,
        // authenticated with the decrypted credential (used, never logged).
        let outcome = http
            .post(provider.endpoint_url.as_str())
            .bearer_auth(credential.0.as_str())
            .json(&serde_json::json!({
                "to": intent.recipient_address,
                "subject": intent.rendered_subject,
                "body": intent.rendered_body,
            }))
            .send()
            .await;
        match outcome {
            Ok(resp) if resp.status().is_success() => {
                let provider_message_id = resp
                    .headers()
                    .get("x-provider-message-id")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                record_attempt_outcome(
                    pool,
                    outbox_relation,
                    metrics,
                    notification_id,
                    intent,
                    &provider.provider,
                    "SENT",
                    "",
                    &provider_message_id,
                )
                .await;
                delivered = delivered.saturating_add(1);
            }
            Ok(resp) => {
                let status = resp.status();
                record_attempt_outcome(
                    pool,
                    outbox_relation,
                    metrics,
                    notification_id,
                    intent,
                    &provider.provider,
                    "FAILED",
                    &format!("provider returned status {status}"),
                    "",
                )
                .await;
            }
            Err(err) => {
                record_attempt_outcome(
                    pool,
                    outbox_relation,
                    metrics,
                    notification_id,
                    intent,
                    &provider.provider,
                    "FAILED",
                    &err.to_string(),
                    "",
                )
                .await;
            }
        }
    }
    Ok(delivered)
}

/// Run one leader-owned delivery pass from durable `NotificationLog` rows.
/// Provider config is resolved once from `UDB_NOTIFICATION_DELIVERY_PROVIDERS_JSON`.
/// If no providers are configured, the worker leaves queued intents untouched so a
/// later sidecar/provider rollout can drain them instead of poisoning attempts.
#[cfg(feature = "http-client")]
pub(crate) async fn run_notification_delivery_worker_once(
    http: &reqwest::Client,
    runtime: Arc<DataBrokerRuntime>,
    pool: &PgPool,
    outbox_relation: Option<&str>,
    batch: i64,
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) -> Result<i64, String> {
    let providers = notification_delivery_providers();
    if providers.is_empty() {
        return Ok(0);
    }
    let intents = load_notification_delivery_intents(pool, batch).await?;
    let deliverable = intents
        .into_iter()
        .filter(|intent| {
            providers
                .iter()
                .any(|provider| provider.channel == intent.channel)
        })
        .collect::<Vec<_>>();
    let delivered = run_notification_delivery_once(
        http,
        runtime.as_ref(),
        pool,
        outbox_relation,
        providers,
        &deliverable,
        metrics,
    )
    .await?;
    Ok(i64::try_from(delivered).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tenant_scope_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::metadata::MetadataValue;

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
    }
}

impl DataBrokerService {
    /// Build the native `NotificationService`, wired to the broker's Postgres pool.
    pub(crate) fn build_notification_service(&self) -> NotificationServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam (extend_udb.md):
        // the backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("notification", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        NotificationServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
