//! Shared helpers for the native data-plane services (storage / asset / webrtc /
//! tenant / notification). Extracted to remove per-service duplication of UUID
//! parsing, JSON defaulting, and outbox-event emission, and to enforce **one
//! canonical outbox envelope** so the CDC engine routes every native event the
//! same way (tenant_id / project_id at the envelope top level).

use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::{Status, metadata::MetadataMap};
use uuid::Uuid;

use super::auth_service::events::{
    ComplianceEnvelope, build_native_compliance_envelope, enterprise_audit_mode,
    validate_native_compliance,
};
use crate::metrics::MetricsRecorder;
use crate::proto::udb::core::common::v1 as common_pb;
use crate::runtime::channels::{ChannelManager, ChannelPermit, OperationChannel};

/// Maximum number of rows any native-service list RPC will return in one page —
/// the shared pagination cap so no tenant can request an unbounded scan. Callers
/// clamp the requested page size to this.
pub(crate) const MAX_LIST_ROWS: i64 = 500;

pub(crate) const DEFAULT_OBJECT_BACKEND: &str = "minio";
pub(crate) const DEFAULT_OBJECT_BUCKET: &str = "udb-storage";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NativePageWindow {
    pub limit: usize,
    pub offset: usize,
    pub page: i32,
    pub page_size: i32,
}

impl NativePageWindow {
    pub(crate) fn limit_i64(self) -> i64 {
        self.limit as i64
    }

    pub(crate) fn offset_i64(self) -> i64 {
        self.offset as i64
    }
}

pub(crate) fn native_page_window(
    page: Option<&common_pb::PageRequest>,
    default_page_size: i32,
) -> NativePageWindow {
    let default_page_size = default_page_size.clamp(1, MAX_LIST_ROWS as i32);
    let page_number = page.map(|p| p.page).filter(|p| *p > 0).unwrap_or(1);
    let page_size = page
        .map(|p| p.page_size)
        .filter(|s| *s > 0)
        .unwrap_or(default_page_size)
        .min(MAX_LIST_ROWS as i32);
    let limit = page_size as usize;
    let offset = (page_number as usize)
        .saturating_sub(1)
        .saturating_mul(limit);
    NativePageWindow {
        limit,
        offset,
        page: page_number,
        page_size,
    }
}

pub(crate) fn native_page_response(
    page: Option<&common_pb::PageRequest>,
    total_items: i64,
    default_page_size: i32,
) -> common_pb::PageResponse {
    let window = native_page_window(page, default_page_size);
    let total_pages = if total_items <= 0 {
        0
    } else {
        ((total_items as i32) + window.page_size - 1) / window.page_size
    };
    common_pb::PageResponse {
        page: window.page,
        page_size: window.page_size,
        total_items,
        total_pages,
        next_page_token: String::new(),
        total_count: total_items,
        has_next: window.page < total_pages,
        has_previous: window.page > 1 && total_pages > 0,
    }
}

pub(crate) fn storage_object_defaults(
    backend: Option<String>,
    bucket: Option<String>,
) -> (String, String) {
    (
        backend
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_OBJECT_BACKEND.to_string()),
        bucket
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_OBJECT_BUCKET.to_string()),
    )
}

pub(crate) async fn admit_on(
    channels: Option<&ChannelManager>,
    metrics: &Arc<dyn MetricsRecorder>,
    service_label: &str,
    op: OperationChannel,
    tenant: &str,
    project: Option<&str>,
) -> Result<Option<ChannelPermit>, Status> {
    let Some(channels) = channels else {
        return Ok(None);
    };
    let tenant_label = if tenant.trim().is_empty() {
        None
    } else {
        Some(tenant)
    };
    let project_label = project
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("default");
    let tenant_hash = super::tenant_hash_label(tenant);
    match channels
        .acquire_fair_with_backpressure(op, tenant_label, project, None, None, op.default_cost())
        .await
    {
        Ok(permit) => {
            metrics.record_fair_admission(
                project_label,
                &tenant_hash,
                service_label,
                "default",
                op.as_str(),
                "accepted",
            );
            metrics.add_fair_cost(
                project_label,
                &tenant_hash,
                service_label,
                "default",
                op.as_str(),
                f64::from(op.default_cost()),
            );
            Ok(Some(permit))
        }
        Err(err) => {
            metrics.inc_channel_rejected(op.as_str());
            metrics.record_fair_admission(
                project_label,
                &tenant_hash,
                service_label,
                "default",
                op.as_str(),
                "rejected",
            );
            Err(err)
        }
    }
}

/// Execute ONE streamed batch item under the full shared channel discipline
/// (FIX-77, second half): per-item fair admission (the permit is held for the
/// duration of the item and dropped on return), the `udb_channel_inflight`
/// gauge, the per-item deadline timeout, the `udb_channel_latency` histogram,
/// and the timeout → `DeadlineExceeded` status mapping (with
/// `udb_channel_timeouts` accounting). This is the streaming-RPC counterpart
/// of `DataBrokerService::execute_with_channel_scoped`; all metric labels are
/// `op.as_str()` ("read" / "write" / "vector"), exactly the strings the batch
/// handlers previously hardcoded inline.
pub(crate) async fn execute_stream_batch_item<T, Fut>(
    channels: &ChannelManager,
    metrics: &Arc<dyn MetricsRecorder>,
    context: &crate::RequestContext,
    op: OperationChannel,
    backend: &'static str,
    fut: Fut,
) -> Result<T, Status>
where
    Fut: std::future::Future<Output = Result<T, Status>>,
{
    let _permit = super::admit_stream_batch_item(channels, metrics, context, op, backend).await?;
    metrics.inc_channel_inflight(op.as_str());
    let start = Instant::now();

    let res = tokio::time::timeout(
        Duration::from_secs(channels.deadline_secs(op, Some(backend))),
        fut,
    )
    .await;

    metrics.dec_channel_inflight(op.as_str());
    metrics.observe_channel_latency(op.as_str(), start.elapsed().as_secs_f64());

    match res {
        Ok(result) => result,
        Err(_) => {
            metrics.inc_channel_timeout(op.as_str());
            Err(Status::deadline_exceeded(format!(
                "{} channel timeout",
                op.as_str()
            )))
        }
    }
}

/// Parse a required UUID field, mapping failures to `InvalidArgument`.
pub(crate) fn parse_uuid(field: &str, value: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(value.trim())
        .map_err(|_| Status::invalid_argument(format!("{field} must be a valid UUID")))
}

/// Normalize a JSON string column input: blank → `{}` so JSONB binds stay valid.
pub(crate) fn non_empty_json(value: &str) -> String {
    let v = value.trim();
    if v.is_empty() {
        "{}".to_string()
    } else {
        v.to_string()
    }
}

fn metadata_value<'a>(metadata: &'a MetadataMap, name: &str) -> Option<&'a str> {
    metadata
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn bearer_claims(metadata: &MetadataMap) -> Option<crate::runtime::security::SecurityClaims> {
    let token = metadata_value(metadata, "authorization")?.strip_prefix("Bearer ")?;
    crate::runtime::security::validate_bearer_token(
        &crate::runtime::security::SecurityConfig::current(),
        token,
    )
    .ok()
}

/// Descriptor-driven native services carry tenant/project ids in decoded request
/// bodies. The tower method-security layer can enforce metadata and bearer
/// claims before tonic decodes the protobuf, but it cannot see body fields.
/// Native handlers call this helper immediately after decode to close that
/// bypass: a caller scoped to tenant/project A cannot smuggle tenant/project B in
/// the request body.
pub(crate) fn validate_request_scope(
    metadata: &MetadataMap,
    request_tenant_id: &str,
    request_project_id: &str,
) -> Result<(), Status> {
    let request_tenant_id = request_tenant_id.trim();
    let request_project_id = request_project_id.trim();
    if request_tenant_id.is_empty() {
        return Err(Status::invalid_argument("tenant_id is required"));
    }

    if let Some(header_tenant) = metadata_value(metadata, "x-tenant-id")
        && header_tenant != request_tenant_id
    {
        return Err(Status::permission_denied(
            "request tenant_id must match x-tenant-id",
        ));
    }
    let header_project = metadata_value(metadata, "x-udb-project-id")
        .or_else(|| metadata_value(metadata, "x-project-id"));
    if let Some(header_project) = header_project
        && !request_project_id.is_empty()
        && header_project != request_project_id
    {
        return Err(Status::permission_denied(
            "request project_id must match project metadata",
        ));
    }

    if let Some(claims) = bearer_claims(metadata) {
        if let Some(claim_tenant) = claims.tenant_id.as_deref().map(str::trim)
            && !claim_tenant.is_empty()
            && claim_tenant != request_tenant_id
        {
            return Err(Status::permission_denied(
                "request tenant_id must match bearer token tenant",
            ));
        }
        if let Some(claim_project) = claims.project_id.as_deref().map(str::trim)
            && !claim_project.is_empty()
            && !request_project_id.is_empty()
            && claim_project != request_project_id
        {
            return Err(Status::permission_denied(
                "request project_id must match bearer token project",
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_request_tenant(
    metadata: &MetadataMap,
    request_tenant_id: &str,
) -> Result<(), Status> {
    validate_request_scope(metadata, request_tenant_id, "")
}

pub(crate) fn metadata_tenant_id(metadata: &MetadataMap) -> Option<String> {
    metadata_value(metadata, "x-tenant-id")
        .map(ToString::to_string)
        .or_else(|| {
            bearer_claims(metadata)
                .and_then(|claims| claims.tenant_id)
                .map(|tenant| tenant.trim().to_string())
                .filter(|tenant| !tenant.is_empty())
        })
}

/// Compliance context a native data-plane service can attach to an outbox event
/// so the durable audit row carries the SAME field set as the auth lane
/// (Phase 10 telemetry coherence). All fields are optional: a service supplies
/// `actor`/`operation`/`outcome` at minimum; `decision_id`/`policy_version`/
/// `auth_method`/`trace_id`/`span_id` are filled when available (the OTel layer
/// populates trace ids from request context). An empty default reproduces the
/// pre-Phase-10 minimal envelope (best-effort, no enterprise enforcement).
#[derive(Clone, Default)]
pub(crate) struct NativeEventContext {
    pub actor: String,
    pub operation: String,
    pub outcome: String,
    pub reason_code: String,
    pub correlation_id: String,
    pub decision_id: String,
    pub policy_version: String,
    pub auth_method: String,
    pub trace_id: String,
    pub span_id: String,
    pub target_resource: String,
}

impl NativeEventContext {
    fn into_envelope(self) -> (ComplianceEnvelope, String) {
        let correlation = self.correlation_id;
        (
            ComplianceEnvelope {
                actor: self.actor,
                operation: self.operation,
                outcome: self.outcome,
                reason_code: self.reason_code,
                decision_id: self.decision_id,
                policy_version: self.policy_version,
                auth_method: self.auth_method,
                trace_id: self.trace_id,
                span_id: self.span_id,
                target_resource: self.target_resource,
                ..ComplianceEnvelope::default()
            },
            correlation,
        )
    }
}

/// Enqueue a domain event into the shared transactional outbox (`→ CDC → Kafka`)
/// using the **unified compliance envelope** (Phase 10): the same field set the
/// auth lane emits (`event_id`/`event_type`/`correlation_id`/`document_id` +
/// top-level `tenant_id`/`project_id` + actor/operation/outcome/trace +
/// CloudEvents attributes + redaction metadata + domain `payload`).
///
/// Best-effort: never fails the caller; a no-op when no outbox relation is
/// configured. Tenant-scoped native topics fail closed before insert when
/// `tenant_id` is missing, so CDC never sees an unscoped native event. This path
/// writes directly to the configured Postgres outbox relation; startup asserts
/// that write receipts do not read `outbox_max_seq` from a different default
/// `SystemStores` counter while this PG path is active. This thin wrapper keeps
/// the legacy 7-arg signature working for existing callers; it emits an empty
/// compliance context (no enterprise enforcement). Callers that have
/// actor/operation context should use [`enqueue_outbox_event_with_context`].
pub(crate) async fn enqueue_outbox_event(
    pool: &PgPool,
    relation: Option<&str>,
    topic: &str,
    partition_key: &str,
    tenant_id: &str,
    project_id: &str,
    payload: serde_json::Value,
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) {
    enqueue_outbox_event_with_context(
        pool,
        relation,
        topic,
        partition_key,
        tenant_id,
        project_id,
        payload,
        NativeEventContext::default(),
        metrics,
    )
    .await;
}

pub(crate) async fn emit_payload_event(
    pool: &PgPool,
    relation: Option<&str>,
    topic: &str,
    partition_key: &str,
    payload: serde_json::Value,
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) {
    let tenant_id = payload
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let project_id = payload
        .get("project_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    enqueue_outbox_event(
        pool,
        relation,
        topic,
        partition_key,
        &tenant_id,
        &project_id,
        payload,
        metrics,
    )
    .await;
}

/// Derive `(operation, resource)` from a versioned dot topic so native events
/// carry the same `operation`/`target_resource` fields the auth lane sets, without
/// every call site spelling them out. For `udb.storage.file.created.v1` this is
/// `("created", "file")`; the trailing `vN` and leading `udb` segments are dropped
/// and the last segment is the verb, the one before it the resource.
fn topic_operation_and_resource(topic: &str) -> (String, String) {
    let mut segs: Vec<&str> = topic.split('.').filter(|s| !s.is_empty()).collect();
    if let Some(last) = segs.last()
        && last.len() >= 2
        && last.starts_with('v')
        && last[1..].chars().all(|c| c.is_ascii_digit())
    {
        segs.pop();
    }
    if segs.first() == Some(&"udb") {
        segs.remove(0);
    }
    let operation = segs.last().copied().unwrap_or_default().to_string();
    let resource = if segs.len() >= 2 {
        segs[segs.len() - 2].to_string()
    } else {
        String::new()
    };
    (operation, resource)
}

/// Enriched native outbox enqueue (Phase 10 telemetry coherence). Same as
/// [`enqueue_outbox_event`] but carries a [`NativeEventContext`] so the durable
/// audit row gets actor/operation/outcome/decision/trace, and — when a
/// `MetricsRecorder` is wired — records `udb_outbox_enqueue_failures_total{path}`
/// on insert failure. In enterprise audit mode (`UDB_ENTERPRISE_AUDIT=1`) a
/// security-sensitive native event missing tenant/actor/operation/correlation is
/// rejected BEFORE insert (parity with the auth lane's fail-closed validation),
/// so a non-compliant native record never reaches the audit trail.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn enqueue_outbox_event_with_context(
    pool: &PgPool,
    relation: Option<&str>,
    topic: &str,
    partition_key: &str,
    tenant_id: &str,
    project_id: &str,
    payload: serde_json::Value,
    ctx: NativeEventContext,
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) {
    let Some(rel) = relation else {
        return;
    };
    if crate::runtime::cdc::tenant_scoped_topic(topic) && tenant_id.trim().is_empty() {
        tracing::warn!(
            topic,
            "refusing to enqueue tenant-scoped native event without tenant_id"
        );
        return;
    }

    let (mut env, correlation) = ctx.into_envelope();
    // Phase 10 (telemetry coherence): auto-populate the compliance envelope from
    // the ambient context so EVERY native event carries the same field set without
    // each call site threading it. The OTel task-local supplies trace/correlation;
    // the dotted topic supplies operation + resource. Explicit context values
    // (set by the caller) always win. Actor/auth_method come only from the
    // method-security request principal; a principal-less native event stays
    // empty and, in enterprise audit mode, is rejected by validation below.
    let trace = crate::runtime::otel::current_trace_context();
    if env.trace_id.is_empty() {
        env.trace_id = trace.trace_id.clone();
    }
    if env.span_id.is_empty() {
        env.span_id = trace.span_id.clone();
    }
    // Actor + auth_method: the authenticated principal scoped by the method-security
    // layer. The actor is what makes native events pass enterprise-mode fail-closed
    // validation (which requires a non-empty actor on security-sensitive topics).
    if env.actor.is_empty() {
        env.actor = crate::runtime::otel::current_actor();
    }
    if env.auth_method.is_empty() {
        env.auth_method = crate::runtime::otel::current_auth_method();
    }
    // decision_id + policy_version: the method-security gate authorization that
    // permitted this request (its id and the contract revision it enforced), so a
    // native event is traceable to the authorization decision behind it.
    if env.decision_id.is_empty() {
        env.decision_id = crate::runtime::otel::current_decision_id();
    }
    if env.policy_version.is_empty() {
        env.policy_version = crate::runtime::otel::current_policy_revision();
    }
    let (derived_op, derived_resource) = topic_operation_and_resource(topic);
    if env.operation.is_empty() {
        env.operation = derived_op;
    }
    if env.target_resource.is_empty() {
        env.target_resource = derived_resource;
    }
    if env.outcome.is_empty() {
        env.outcome = "success".to_string();
    }
    let correlation = if !correlation.trim().is_empty() {
        correlation
    } else if !trace.correlation_id.trim().is_empty() {
        trace.correlation_id.clone()
    } else {
        partition_key.to_string()
    };

    // Phase 10: in enterprise audit mode, reject a security-sensitive native
    // event lacking required compliance fields BEFORE it is enqueued. The native
    // envelope always carries redaction metadata, so `redaction_present = true`.
    if enterprise_audit_mode() {
        if let Err(missing) = validate_native_compliance(
            topic,
            tenant_id,
            &env.actor,
            &env.operation,
            &correlation,
            true,
        ) {
            tracing::warn!(topic, error = %missing,
                "refusing to enqueue non-compliant native event (enterprise audit mode)");
            if let Some(m) = metrics {
                m.inc_outbox_enqueue_failures_total("native_compliance");
            }
            return;
        }
    }

    let event_id = Uuid::new_v4().to_string();
    let envelope = build_native_compliance_envelope(
        &event_id,
        topic,
        partition_key,
        tenant_id,
        project_id,
        &env,
        &correlation,
        "none",
        1,
        &[],
        payload,
    );
    let sql = format!(
        "INSERT INTO {rel} (event_id, topic, partition_key, payload, created_at) \
         VALUES ($1::UUID, $2, $3, $4::JSONB, NOW())"
    );
    if let Err(err) = sqlx::query(&sql)
        .bind(&event_id)
        .bind(topic)
        .bind(partition_key)
        .bind(envelope.to_string())
        .execute(pool)
        .await
    {
        tracing::warn!(topic, error = %err, "native outbox enqueue failed");
        if let Some(m) = metrics {
            m.inc_outbox_enqueue_failures_total("native");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    fn metadata_with(name: &'static str, value: &'static str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert(name, MetadataValue::from_static(value));
        metadata
    }

    #[test]
    fn topic_operation_and_resource_parses_versioned_dot_topics() {
        assert_eq!(
            topic_operation_and_resource("udb.storage.file.created.v1"),
            ("created".to_string(), "file".to_string())
        );
        assert_eq!(
            topic_operation_and_resource("udb.webrtc.room.closed.v2"),
            ("closed".to_string(), "room".to_string())
        );
        // No version suffix, no udb prefix.
        assert_eq!(
            topic_operation_and_resource("tenant.created"),
            ("created".to_string(), "tenant".to_string())
        );
    }

    #[test]
    fn request_scope_rejects_header_tenant_mismatch() {
        let metadata = metadata_with("x-tenant-id", "tenant-a");
        let err = validate_request_tenant(&metadata, "tenant-b")
            .expect_err("body tenant must match request metadata");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn request_scope_rejects_header_project_mismatch() {
        let metadata = metadata_with("x-udb-project-id", "project-a");
        let err = validate_request_scope(&metadata, "tenant-a", "project-b")
            .expect_err("body project must match request metadata");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn request_scope_accepts_matching_metadata() {
        let mut metadata = MetadataMap::new();
        metadata.insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        metadata.insert("x-udb-project-id", MetadataValue::from_static("project-a"));
        validate_request_scope(&metadata, "tenant-a", "project-a")
            .expect("matching tenant/project metadata should pass");
    }

    #[test]
    fn native_context_maps_into_unified_envelope() {
        // The native context populates the shared ComplianceEnvelope, which the
        // shared builder renders with the same field set as the auth lane.
        let ctx = NativeEventContext {
            actor: "svc-asset".to_string(),
            operation: "run_pipeline".to_string(),
            outcome: "success".to_string(),
            correlation_id: "corr-7".to_string(),
            decision_id: "dec-1".to_string(),
            trace_id: "0af7651916cd43dd8448eb211c80319c".to_string(),
            span_id: "b7ad6b7169203331".to_string(),
            ..NativeEventContext::default()
        };
        let (env, correlation) = ctx.into_envelope();
        assert_eq!(correlation, "corr-7");
        let envelope = build_native_compliance_envelope(
            "11111111-1111-4111-8111-111111111111",
            "udb.asset.pipeline.completed.v1",
            "asset-1",
            "acme",
            "proj-1",
            &env,
            &correlation,
            "none",
            1,
            &[],
            serde_json::json!({ "asset_id": "a1" }),
        );
        assert_eq!(envelope["actor"], serde_json::json!("svc-asset"));
        assert_eq!(envelope["operation"], serde_json::json!("run_pipeline"));
        assert_eq!(envelope["outcome"], serde_json::json!("success"));
        assert_eq!(envelope["decision_id"], serde_json::json!("dec-1"));
        assert_eq!(
            envelope["trace_id"],
            serde_json::json!("0af7651916cd43dd8448eb211c80319c")
        );
        assert_eq!(envelope["correlation_id"], serde_json::json!("corr-7"));
        // CDC contract fields preserved so existing rows + tailer keep decoding.
        assert_eq!(
            envelope["event_type"],
            serde_json::json!("udb.asset.pipeline.completed.v1")
        );
        assert_eq!(envelope["tenant_id"], serde_json::json!("acme"));
        assert_eq!(envelope["redaction_mode"], serde_json::json!("none"));
    }
}
