//! Shared helpers for the native data-plane services (storage / asset / webrtc /
//! tenant / notification). Extracted to remove per-service duplication of UUID
//! parsing, JSON defaulting, and outbox-event emission, and to enforce **one
//! canonical outbox envelope** so the CDC engine routes every native event the
//! same way (tenant_id / project_id at the envelope top level).

use sqlx::PgPool;
use std::collections::BTreeSet;
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

pub(crate) fn update_mask_path_set(
    mask: Option<&prost_types::FieldMask>,
    allowed_paths: &[&str],
) -> Result<Option<BTreeSet<String>>, Status> {
    let Some(mask) = mask else {
        return Ok(None);
    };
    let allowed: BTreeSet<&str> = allowed_paths.iter().copied().collect();
    let mut paths = BTreeSet::new();
    for path in &mask.paths {
        if !allowed.contains(path.as_str()) {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                "update_mask contains unsupported path",
                [(
                    "update_mask",
                    format!(
                        "unsupported path `{path}`; allowed paths: {}",
                        allowed_paths.join(", ")
                    ),
                )],
            ));
        }
        paths.insert(path.clone());
    }
    Ok(Some(paths))
}

pub(crate) fn update_mask_allows(
    mask: &Option<BTreeSet<String>>,
    field: &str,
    legacy_present: bool,
) -> bool {
    mask.as_ref()
        .map_or(legacy_present, |paths| paths.contains(field))
}

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

pub(crate) fn native_offset_page_window(
    page: i32,
    page_size: i32,
    page_token: &str,
    default_page_size: i32,
) -> NativePageWindow {
    let default_page_size = default_page_size.clamp(1, MAX_LIST_ROWS as i32);
    let page_size = page_size
        .gt(&0)
        .then_some(page_size)
        .unwrap_or(default_page_size)
        .min(MAX_LIST_ROWS as i32);
    let token_offset = page_token
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|offset| *offset > 0);
    let offset = token_offset.unwrap_or_else(|| {
        let page_number = page.max(1) as usize;
        page_number
            .saturating_sub(1)
            .saturating_mul(page_size as usize)
    });
    let page_number = (offset / page_size as usize) + 1;
    NativePageWindow {
        limit: page_size as usize,
        offset,
        page: page_number as i32,
        page_size,
    }
}

pub(crate) fn native_next_page_token(offset: usize, limit: usize, returned: usize) -> String {
    if returned >= limit && limit > 0 {
        offset.saturating_add(limit).to_string()
    } else {
        String::new()
    }
}

pub(crate) fn native_next_page_token_for_total(offset: usize, limit: usize, total: i64) -> String {
    if limit > 0 && (offset as i64).saturating_add(limit as i64) < total {
        offset.saturating_add(limit).to_string()
    } else {
        String::new()
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
        next_page_token: native_next_page_token_for_total(window.offset, window.limit, total_items),
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
    async fn record_admission_usage(service_label: &str, op: OperationChannel, tenant: &str) {
        let Some(pool) = super::metering_service::admission_metering_pool() else {
            return;
        };
        let method = super::metering_service::admission_metering_method(service_label, op.as_str());
        let _ = super::metering_service::record_usage(
            &pool,
            tenant,
            "",
            &method,
            super::metering_service::ADMISSION_METERING_UNIT,
            i64::from(op.default_cost()),
            super::metering_service::now_unix(),
        )
        .await;
    }

    let Some(channels) = channels else {
        record_admission_usage(service_label, op, tenant).await;
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
            record_admission_usage(service_label, op, tenant).await;
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
            Err(crate::runtime::executor_utils::deadline_exceeded_status(
                backend,
                format!("{} channel", op.as_str()),
                crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                format!("{} channel timeout", op.as_str()),
            ))
        }
    }
}

/// Parse a required UUID field, mapping failures to `InvalidArgument`.
pub(crate) fn parse_uuid(field: &str, value: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(value.trim()).map_err(|_| {
        crate::runtime::executor_utils::invalid_argument_fields(
            format!("{field} must be a valid UUID"),
            [(field, "must be a valid UUID")],
        )
    })
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

/// 01.6.1.2 — the validated claim tenant, sourced ONLY from the method-security
/// context (no bearer re-parse). `None` when no context is installed (loopback) or
/// the claim carries no tenant.
fn claim_context_tenant_id() -> Option<String> {
    if !crate::runtime::service::method_security::claim_context_present() {
        return None;
    }
    let tenant = crate::runtime::service::method_security::current_claim_context()
        .tenant_id
        .trim()
        .to_string();
    (!tenant.is_empty()).then_some(tenant)
}

/// 01.6.1.2 — the validated claim project, sourced ONLY from the method-security
/// context (no bearer re-parse). `None` when no context is installed or the claim
/// carries no project.
fn claim_context_project_id() -> Option<String> {
    if !crate::runtime::service::method_security::claim_context_present() {
        return None;
    }
    let project = crate::runtime::service::method_security::current_claim_context()
        .project_id
        .trim()
        .to_string();
    (!project.is_empty()).then_some(project)
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
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "tenant_id is required",
            [("tenant_id", "must be a non-empty tenant id")],
        ));
    }

    if let Some(header_tenant) = metadata_value(metadata, "x-tenant-id")
        && header_tenant != request_tenant_id
    {
        return Err(native_scope_policy_status(
            "tenant_metadata_mismatch",
            "request tenant_id must match x-tenant-id",
        ));
    }
    let header_project = metadata_value(metadata, "x-udb-project-id")
        .or_else(|| metadata_value(metadata, "x-project-id"));
    if let Some(header_project) = header_project
        && !request_project_id.is_empty()
        && header_project != request_project_id
    {
        return Err(native_scope_policy_status(
            "project_metadata_mismatch",
            "request project_id must match project metadata",
        ));
    }

    // 01.6.1.2 — identity comes from the SINGLE validated claim the method-security
    // tower already installed for this request; do NOT re-parse the bearer here
    // (that was a second JWT-parse site outside method-security). On the in-process
    // loopback path no claim context is installed and there is no bearer to smuggle,
    // so the header checks above suffice.
    if crate::runtime::service::method_security::claim_context_present() {
        let claim = crate::runtime::service::method_security::current_claim_context();
        let claim_tenant = claim.tenant_id.trim();
        if !claim_tenant.is_empty() && claim_tenant != request_tenant_id {
            return Err(native_scope_policy_status(
                "tenant_claim_mismatch",
                "request tenant_id must match bearer token tenant",
            ));
        }
        let claim_project = claim.project_id.trim();
        if !claim_project.is_empty()
            && !request_project_id.is_empty()
            && claim_project != request_project_id
        {
            return Err(native_scope_policy_status(
                "project_claim_mismatch",
                "request project_id must match bearer token project",
            ));
        }
    }
    Ok(())
}

fn native_scope_policy_status(
    policy_decision_id: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        "native_request_scope",
        policy_decision_id,
        message,
    )
}

pub(crate) fn validate_request_tenant(
    metadata: &MetadataMap,
    request_tenant_id: &str,
) -> Result<(), Status> {
    validate_request_scope(metadata, request_tenant_id, "")
}

/// Resolve the request's tenant, VALIDATED CLAIM FIRST. The tower requires
/// `x-tenant-id` to equal the claim tenant with no empty-claim exemption, so
/// the two can never disagree on an authenticated request; consulting the claim
/// first means this stays true even if that gate is ever relaxed. The header
/// remains the source on the in-process loopback path, where no claim context
/// is installed and there is no bearer to smuggle.
pub(crate) fn metadata_tenant_id(metadata: &MetadataMap) -> Option<String> {
    claim_context_tenant_id()
        .or_else(|| metadata_value(metadata, "x-tenant-id").map(ToString::to_string))
}

/// Resolve the request's project, VALIDATED CLAIM FIRST.
///
/// The method-security tower rejects a project header that contradicts a
/// non-empty claim project, so the two agree whenever the token is
/// project-scoped. They can still diverge when the token carries NO project
/// claim, and this helper's result is compiled into an isolation predicate
/// (`context_predicates`) — so the claim is consulted first and the header is
/// only a fallback for a token that does not name a project at all. That keeps
/// a tenant-scoped token's ability to select a project, while making it
/// impossible for a header to displace a project the caller actually proved.
///
/// Note the deliberate asymmetry with the tenant equivalent: `x-tenant-id` must
/// equal the claim tenant with no empty-claim exemption, so tenant selection by
/// header is not possible at all.
pub(crate) fn metadata_project_id(metadata: &MetadataMap) -> Option<String> {
    claim_context_project_id().or_else(|| {
        metadata_value(metadata, "x-udb-project-id")
            .or_else(|| metadata_value(metadata, "x-project-id"))
            .map(ToString::to_string)
    })
}

/// The one request-context builder for native services (P6.1). Empty `project_id`
/// ⇒ resolve from metadata (x-udb-project-id / x-project-id / bearer claim); a
/// non-empty `project_id` is used verbatim. Sets tenant/project/correlation only;
/// all other `RequestContext` fields default. Subsumes the six former per-service
/// `*_context` copies — including the asset variant, which previously skipped the
/// metadata fallback (latent empty-project inconsistency, now unified).
pub(crate) fn native_service_context(
    metadata: &MetadataMap,
    tenant_id: &str,
    project_id: &str,
) -> crate::RequestContext {
    crate::RequestContext {
        tenant_id: tenant_id.to_string(),
        project_id: if project_id.trim().is_empty() {
            metadata_project_id(metadata).unwrap_or_default()
        } else {
            project_id.to_string()
        },
        correlation_id: metadata
            .get("x-correlation-id")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string(),
        ..crate::RequestContext::default()
    }
}

/// Validate the request body's tenant/project identity against metadata and the
/// already-verified bearer claim, then build the native runtime context from
/// that same scope. Keeping the check and construction in one helper prevents
/// handlers from validating only the tenant and subsequently trusting an
/// unrelated body project.
pub(crate) fn validated_native_service_context(
    metadata: &MetadataMap,
    tenant_id: &str,
    project_id: &str,
) -> Result<crate::RequestContext, Status> {
    validate_request_scope(metadata, tenant_id, project_id)?;
    Ok(native_service_context(metadata, tenant_id, project_id))
}

/// Build a native context for a PROJECT-SCOPED entity, resolving the project
/// from the validated request scope ([`metadata_project_id`]: claim first,
/// header only for a token that names no project).
///
/// This is the explicit spelling of what `native_service_context(md, tenant,
/// "")` used to do implicitly. That empty-string literal is now banned outright
/// (see `native_context_posture_tests`): it read as "no project" while actually
/// meaning "infer one, possibly from a request header", and the difference
/// shipped three silent read defects. Choosing between this and
/// [`tenant_only_native_service_context`] is now a decision each handler states
/// rather than one the argument list hides.
pub(crate) fn project_scoped_native_service_context(
    metadata: &MetadataMap,
    tenant_id: &str,
) -> crate::RequestContext {
    crate::RequestContext {
        tenant_id: tenant_id.to_string(),
        project_id: metadata_project_id(metadata).unwrap_or_default(),
        correlation_id: metadata
            .get("x-correlation-id")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string(),
        ..crate::RequestContext::default()
    }
}

/// Build a native context for tenant-scoped services whose backing entities are
/// not project-scoped. Unlike [`native_service_context`], this deliberately does
/// not fall back to `x-udb-project-id`; UUID-typed nullable `project_id` columns
/// must not receive human project tokens as compiler-injected predicates.
pub(crate) fn tenant_only_native_service_context(
    metadata: &MetadataMap,
    tenant_id: &str,
) -> crate::RequestContext {
    crate::RequestContext {
        tenant_id: tenant_id.to_string(),
        project_id: String::new(),
        correlation_id: metadata
            .get("x-correlation-id")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string(),
        ..crate::RequestContext::default()
    }
}

#[cfg(test)]
mod native_service_context_tests {
    use super::{native_service_context, tenant_only_native_service_context};
    use tonic::metadata::MetadataMap;

    #[test]
    fn empty_project_falls_back_to_metadata() {
        let mut md = MetadataMap::new();
        md.insert("x-udb-project-id", "proj-from-md".parse().unwrap());
        let ctx = native_service_context(&md, "tenant-1", "");
        assert_eq!(ctx.project_id, "proj-from-md");
        assert_eq!(ctx.tenant_id, "tenant-1");
    }

    #[test]
    fn non_empty_project_used_verbatim() {
        let mut md = MetadataMap::new();
        md.insert("x-udb-project-id", "proj-from-md".parse().unwrap());
        let ctx = native_service_context(&md, "tenant-1", "explicit-proj");
        assert_eq!(ctx.project_id, "explicit-proj");
    }

    #[test]
    fn reads_correlation_id() {
        let mut md = MetadataMap::new();
        md.insert("x-correlation-id", "corr-9".parse().unwrap());
        let ctx = native_service_context(&md, "tenant-1", "p");
        assert_eq!(ctx.correlation_id, "corr-9");
    }

    #[test]
    fn tenant_only_context_does_not_fall_back_to_project_metadata() {
        let mut md = MetadataMap::new();
        md.insert("x-udb-project-id", "proj-from-md".parse().unwrap());
        let ctx = tenant_only_native_service_context(&md, "tenant-1");
        assert_eq!(ctx.tenant_id, "tenant-1");
        assert_eq!(ctx.project_id, "");
    }
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
/// Why [`build_enriched_outbox_envelope`] refused to produce an envelope.
/// `TenantScopeMissing` mirrors the historical silent-skip (warn, no failure
/// metric); `Compliance` mirrors the enterprise-audit fail-closed rejection
/// (warn + `native_compliance` failure metric).
pub(crate) enum OutboxEnvelopeReject {
    TenantScopeMissing,
    Compliance(String),
}

impl std::fmt::Display for OutboxEnvelopeReject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TenantScopeMissing => {
                write!(f, "tenant-scoped native event is missing tenant_id")
            }
            Self::Compliance(missing) => {
                write!(f, "native event failed compliance validation: {missing}")
            }
        }
    }
}

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
    let (event_uuid, envelope) = match build_enriched_outbox_envelope(
        topic,
        partition_key,
        tenant_id,
        project_id,
        ctx,
        payload,
    ) {
        Ok(built) => built,
        Err(OutboxEnvelopeReject::TenantScopeMissing) => {
            tracing::warn!(
                topic,
                "refusing to enqueue tenant-scoped native event without tenant_id"
            );
            return;
        }
        Err(OutboxEnvelopeReject::Compliance(missing)) => {
            tracing::warn!(topic, error = %missing,
                "refusing to enqueue non-compliant native event (enterprise audit mode)");
            if let Some(m) = metrics {
                m.inc_outbox_enqueue_failures_total("native_compliance");
            }
            return;
        }
    };
    // ONE shared insert path (the auth lane uses the SAME `insert_outbox_row`), which
    // takes `event_id: Uuid` — so the two lanes can NEVER again diverge on the `$1`
    // bind type and re-introduce the sqlx prepared-statement collision that poisoned
    // the pooled connection (§4). Native emission stays best-effort (WARN, not fatal).
    if let Err(err) = crate::runtime::cdc::insert_outbox_row(
        pool,
        rel,
        event_uuid,
        topic,
        partition_key,
        &envelope,
    )
    .await
    {
        tracing::warn!(topic, error = %err, "native outbox enqueue failed");
        if let Some(m) = metrics {
            m.inc_outbox_enqueue_failures_total("native");
        }
    }
}

/// STRICT transactional native outbox enqueue for accounting-grade events
/// (e.g. embedding metering): runs the SAME envelope enrichment + compliance
/// validation as [`enqueue_outbox_event_with_context`], but inserts through the
/// caller's executor/transaction and PROPAGATES every failure instead of
/// swallowing it — so a billing event is either durably enqueued atomically
/// with the caller's write or the caller's transaction rolls back. A `None`
/// relation (outbox not configured for this deployment) is a no-op `Ok`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn enqueue_outbox_event_in_tx<'c, E>(
    executor: E,
    relation: Option<&str>,
    topic: &str,
    partition_key: &str,
    tenant_id: &str,
    project_id: &str,
    payload: serde_json::Value,
    ctx: NativeEventContext,
) -> Result<(), String>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let Some(rel) = relation else {
        return Ok(());
    };
    let (event_uuid, envelope) =
        build_enriched_outbox_envelope(topic, partition_key, tenant_id, project_id, ctx, payload)
            .map_err(|reject| reject.to_string())?;
    crate::runtime::cdc::insert_outbox_row(
        executor,
        rel,
        event_uuid,
        topic,
        partition_key,
        &envelope,
    )
    .await
    .map_err(|err| format!("outbox insert failed: {err}"))
}

/// Build an outbox step for
/// `DataBrokerRuntime::native_entity_transaction_for_service`.
/// Envelope construction stays in this shared chokepoint; the typed native-store
/// transaction owns only execution/commit. `None` preserves the established
/// no-outbox deployment posture without introducing a fake event operation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn native_transaction_outbox_op(
    relation: Option<&str>,
    topic: &str,
    partition_key: &str,
    tenant_id: &str,
    project_id: &str,
    payload: serde_json::Value,
    ctx: NativeEventContext,
) -> Result<Option<crate::runtime::core::native_store::NativeEntityTransactionOp>, String> {
    let Some(relation) = relation else {
        return Ok(None);
    };
    let (event_id, envelope) =
        build_enriched_outbox_envelope(topic, partition_key, tenant_id, project_id, ctx, payload)
            .map_err(|reject| reject.to_string())?;
    Ok(Some(
        crate::runtime::core::native_store::NativeEntityTransactionOp::Outbox(
            crate::runtime::core::native_store::NativeOutboxTransactionWrite {
                relation: relation.to_string(),
                event_id,
                topic: topic.to_string(),
                partition_key: partition_key.to_string(),
                envelope,
            },
        ),
    ))
}

/// Shared envelope enrichment for the native outbox lanes: ambient
/// trace/actor/decision autofill, dotted-topic operation/resource derivation,
/// and enterprise-audit compliance validation. Both the best-effort pool path
/// and the strict transactional path build through here so they can never
/// diverge on envelope shape or validation.
pub(crate) fn build_enriched_outbox_envelope(
    topic: &str,
    partition_key: &str,
    tenant_id: &str,
    project_id: &str,
    ctx: NativeEventContext,
    payload: serde_json::Value,
) -> Result<(Uuid, serde_json::Value), OutboxEnvelopeReject> {
    if crate::runtime::cdc::tenant_scoped_topic(topic) && tenant_id.trim().is_empty() {
        return Err(OutboxEnvelopeReject::TenantScopeMissing);
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
            return Err(OutboxEnvelopeReject::Compliance(missing));
        }
    }

    let event_uuid = Uuid::new_v4();
    let event_id = event_uuid.to_string();
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
    Ok((event_uuid, envelope))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::metadata::MetadataValue;

    fn metadata_with(name: &'static str, value: &'static str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert(name, MetadataValue::from_static(value));
        metadata
    }

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
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
        assert_eq!(err.message(), "request tenant_id must match x-tenant-id");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, "native_request_scope");
        assert_eq!(detail.policy_decision_id, "tenant_metadata_mismatch");
    }

    #[test]
    fn request_scope_missing_tenant_carries_field_violation() {
        let metadata = MetadataMap::new();
        let err = validate_request_tenant(&metadata, "  ").expect_err("tenant is required");
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
    fn parse_uuid_invalid_value_carries_field_violation() {
        let err = parse_uuid("user_id", "not-a-uuid").expect_err("invalid UUID must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "user_id must be a valid UUID");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "user_id");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a valid UUID"
        );
    }

    #[test]
    fn request_scope_rejects_header_project_mismatch() {
        let metadata = metadata_with("x-udb-project-id", "project-a");
        let err = validate_request_scope(&metadata, "tenant-a", "project-b")
            .expect_err("body project must match request metadata");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_eq!(
            err.message(),
            "request project_id must match project metadata"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, "native_request_scope");
        assert_eq!(detail.policy_decision_id, "project_metadata_mismatch");
    }

    #[tokio::test]
    async fn request_scope_rejects_claim_tenant_mismatch_with_policy_detail() {
        let claim = crate::runtime::service::method_security::test_claim_context(
            "user-1",
            "tenant-a",
            "",
            &["udb:native:write"],
            &[],
        );
        crate::runtime::service::method_security::scope_claim_context_for_test(claim, async {
            let err = validate_request_tenant(&MetadataMap::new(), "tenant-b")
                .expect_err("body tenant must match bearer claim tenant");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
            assert_eq!(
                err.message(),
                "request tenant_id must match bearer token tenant"
            );
            let detail = decode_detail(&err);
            assert_eq!(detail.kind, ErrorKind::Policy as i32);
            assert_eq!(detail.operation, "native_request_scope");
            assert_eq!(detail.policy_decision_id, "tenant_claim_mismatch");
        })
        .await;
    }

    #[tokio::test]
    async fn request_scope_rejects_claim_project_mismatch_with_policy_detail() {
        let claim = crate::runtime::service::method_security::test_claim_context(
            "user-1",
            "tenant-a",
            "project-a",
            &["udb:native:write"],
            &[],
        );
        crate::runtime::service::method_security::scope_claim_context_for_test(claim, async {
            let err = validate_request_scope(&MetadataMap::new(), "tenant-a", "project-b")
                .expect_err("body project must match bearer claim project");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
            assert_eq!(
                err.message(),
                "request project_id must match bearer token project"
            );
            let detail = decode_detail(&err);
            assert_eq!(detail.kind, ErrorKind::Policy as i32);
            assert_eq!(detail.operation, "native_request_scope");
            assert_eq!(detail.policy_decision_id, "project_claim_mismatch");
        })
        .await;
    }

    #[tokio::test]
    async fn validated_native_context_rejects_body_project_before_construction() {
        let claim = crate::runtime::service::method_security::test_claim_context(
            "user-1",
            "tenant-a",
            "project-a",
            &["udb:native:read"],
            &[],
        );
        crate::runtime::service::method_security::scope_claim_context_for_test(claim, async {
            let err =
                validated_native_service_context(&MetadataMap::new(), "tenant-a", "project-b")
                    .expect_err("a body project cannot displace the verified claim project");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
            let detail = decode_detail(&err);
            assert_eq!(detail.policy_decision_id, "project_claim_mismatch");
        })
        .await;
    }

    #[test]
    fn validated_native_context_preserves_matching_project() {
        let mut metadata = MetadataMap::new();
        metadata.insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        metadata.insert("x-udb-project-id", MetadataValue::from_static("project-a"));
        let context = validated_native_service_context(&metadata, "tenant-a", "project-a")
            .expect("matching native scope should construct a context");
        assert_eq!(context.tenant_id, "tenant-a");
        assert_eq!(context.project_id, "project-a");
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
