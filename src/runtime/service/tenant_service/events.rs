//! Best-effort tenant lifecycle event emission into the shared transactional
//! outbox, plus the two no-secrets event payload builders. Extracted verbatim;
//! `emit_event` takes `svc` where the trait method took `&self`.

use super::super::native_helpers::{
    NativeEventContext, OutboxEnvelopeReject, build_enriched_outbox_envelope,
    enqueue_outbox_event_with_context,
};
use super::TenantServiceImpl;

/// Best-effort tenant-event emission into the shared transactional outbox
/// (`→ CDC → Kafka`). `operation` is the contract-declared event type for the
/// emitting RPC (e.g. [`super::config::EVENT_TYPE_TENANT_CREATED`]); it lands in
/// the compliance envelope so the durable row is traceable to its RPC contract.
/// No-op when no PG pool / outbox relation is configured (mirrors the other
/// native services' best-effort emit posture).
pub(crate) async fn emit_event(
    svc: &TenantServiceImpl,
    topic: &str,
    operation: &str,
    partition_key: &str,
    tenant_id: &str,
    payload: serde_json::Value,
) {
    let Some(pool) = svc.pg_pool.as_ref() else {
        return;
    };
    enqueue_outbox_event_with_context(
        pool,
        svc.outbox_relation.as_deref(),
        topic,
        partition_key,
        tenant_id,
        "",
        payload,
        NativeEventContext {
            operation: operation.to_string(),
            target_resource: tenant_id.to_string(),
            ..NativeEventContext::default()
        },
        Some(&svc.metrics),
    )
    .await;
}

/// [`emit_event`], but the outbox row is inserted through the CALLER'S
/// transaction so the tenant row and its lifecycle event commit together.
///
/// `emit_event` runs after the write has already committed, which leaves a
/// window: if the insert fails, the tenant exists with no event, and — unlike
/// the webhook journal — nothing re-derives it, so a downstream provisioning
/// consumer silently never learns the tenant was created.
///
/// The two failure classes are deliberately NOT treated alike, because only one
/// of them is that window:
///
/// - An INFRASTRUCTURE failure (the insert itself) is propagated, so the
///   caller's transaction rolls the tenant row back too. Row and event are then
///   all-or-nothing and the window is closed.
/// - An ENVELOPE REJECTION (`udb.tenant.*` is a security-sensitive prefix, so
///   enterprise audit mode demands a fully populated compliance envelope) keeps
///   the pre-existing best-effort posture: warn, count, and let the write stand.
///   Failing the RPC here would convert a long-standing silent event drop into
///   a hard CreateTenant outage on exactly the deployments running enterprise
///   audit mode. That is a policy gap to close on its own terms, not a
///   side-effect of making the write atomic.
pub(crate) async fn emit_event_in_tx(
    svc: &TenantServiceImpl,
    executor: &mut sqlx::PgConnection,
    topic: &str,
    operation: &str,
    partition_key: &str,
    tenant_id: &str,
    payload: serde_json::Value,
) -> Result<(), String> {
    let Some(relation) = svc.outbox_relation.as_deref() else {
        return Ok(());
    };
    let context = NativeEventContext {
        operation: operation.to_string(),
        target_resource: tenant_id.to_string(),
        ..NativeEventContext::default()
    };
    let (event_id, envelope) =
        match build_enriched_outbox_envelope(topic, partition_key, tenant_id, "", context, payload)
        {
            Ok(built) => built,
            Err(reject) => {
                let bucket = match reject {
                    OutboxEnvelopeReject::TenantScopeMissing => "native_tenant_scope",
                    OutboxEnvelopeReject::Compliance(_) => "native_compliance",
                };
                tracing::warn!(
                    topic,
                    error = %reject,
                    "refusing to enqueue non-compliant tenant event; the tenant write still stands"
                );
                svc.metrics.inc_outbox_enqueue_failures_total(bucket);
                return Ok(());
            }
        };
    crate::runtime::cdc::insert_outbox_row(
        &mut *executor,
        relation,
        event_id,
        topic,
        partition_key,
        &envelope,
    )
    .await
    .map_err(|err| format!("outbox insert failed: {err}"))
}

/// Immutable audit emit for the PRIVILEGED cross-tenant admin purge (Bug #2).
/// Unlike [`emit_event`] this threads the VERIFIED caller (`actor`) and the
/// per-action authorization `decision_id` into the compliance envelope so the
/// durable audit row attributes the destructive cross-tenant action to a real,
/// authorized identity — never a spoofable body value. Best-effort (same posture
/// as the other tenant emits); the durable ledger row is the authoritative
/// outcome record even when no outbox relation is configured. The payload carries
/// identifiers, per-table counts, the human reason, and the outcome id — no
/// tenant config/branding bodies or secrets.
pub(crate) async fn emit_admin_purge_audit(
    svc: &TenantServiceImpl,
    target_tenant_id: &str,
    verified_actor: &str,
    decision_id: &str,
    payload: serde_json::Value,
) {
    let Some(pool) = svc.pg_pool.as_ref() else {
        return;
    };
    enqueue_outbox_event_with_context(
        pool,
        svc.outbox_relation.as_deref(),
        super::config::TOPIC_TENANT_ADMIN_PURGED,
        target_tenant_id,
        target_tenant_id,
        "",
        payload,
        NativeEventContext {
            actor: verified_actor.to_string(),
            operation: super::config::EVENT_TYPE_TENANT_ADMIN_PURGE.to_string(),
            outcome: "success".to_string(),
            decision_id: decision_id.to_string(),
            target_resource: target_tenant_id.to_string(),
            ..NativeEventContext::default()
        },
        Some(&svc.metrics),
    )
    .await;
}

/// Lifecycle event payload for tenant create/update: identifiers + status ONLY.
/// Deliberately excludes `config`/`branding` bodies and any credential material —
/// the outbox payload must never carry tenant secrets.
pub(crate) fn tenant_lifecycle_event_payload(
    tenant_id: &str,
    code: &str,
    status: &str,
) -> serde_json::Value {
    serde_json::json!({
        "tenant_id": tenant_id,
        "code": code,
        "status": status,
    })
}

/// Config-update event payload: tenant + config KEY only. The config VALUE is
/// deliberately omitted (it may carry secrets; same no-secrets rule as above).
pub(crate) fn tenant_config_event_payload(tenant_id: &str, config_key: &str) -> serde_json::Value {
    serde_json::json!({
        "tenant_id": tenant_id,
        "config_key": config_key,
    })
}
