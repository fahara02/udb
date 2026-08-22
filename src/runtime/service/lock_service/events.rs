//! The per-mutation outbox event emission and the shared `udb.lock.*` domain
//! payload for the native `LockService`. Extracted verbatim; `emit_lock_event`
//! takes `svc` where the method took `&self`.

use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_with_context};
use super::LockServiceImpl;

/// The domain payload every `udb.lock.*` event carries — shared between the
/// RPC emit path ([`emit_lock_event`]) and the expiry reaper
/// ([`super::workers::run_lock_expiry_once`]) so the two lanes never drift.
pub(crate) fn lock_event_payload(
    tenant_id: &str,
    project_id: &str,
    lock_name: &str,
    owner_id: &str,
    fencing_token: i64,
) -> serde_json::Value {
    serde_json::json!({
        "tenant_id": tenant_id,
        "project_id": project_id,
        "lock_name": lock_name,
        "owner_id": owner_id,
        "fencing_token": fencing_token,
    })
}

/// Build the lock event as a transaction step, so the lock-row write and its
/// event commit together via
/// [`crate::runtime::core::DataBrokerRuntime::native_entity_write_co_commit_for_service`].
///
/// `None` means nothing to co-commit. Unlike [`emit_lock_event`], a missing pool
/// or relation is NOT logged as a dropped event here: the caller falls back to
/// that function, which does the logging and metrics.
#[allow(clippy::too_many_arguments)]
pub(crate) fn lock_event_transaction_op(
    svc: &LockServiceImpl,
    topic: &str,
    partition_key: &str,
    tenant_id: &str,
    project_id: &str,
    lock_name: &str,
    owner_id: &str,
    fencing_token: i64,
) -> Option<crate::runtime::core::native_store::NativeEntityTransactionOp> {
    match super::super::native_helpers::native_transaction_outbox_op(
        svc.outbox_relation.as_deref(),
        topic,
        partition_key,
        tenant_id,
        project_id,
        lock_event_payload(tenant_id, project_id, lock_name, owner_id, fencing_token),
        NativeEventContext {
            target_resource: lock_name.to_string(),
            ..NativeEventContext::default()
        },
    ) {
        Ok(op) => op,
        Err(reject) => {
            tracing::error!(
                topic,
                lock_name,
                tenant_id,
                error = %reject,
                "lock event dropped: envelope rejected"
            );
            svc.metrics.inc_outbox_enqueue_failures_total("native");
            None
        }
    }
}

/// Emit a per-mutation versioned dot-topic outbox event (best-effort fallback).
///
/// The at-least-once-minus window this used to document is CLOSED on Postgres:
/// acquire/renew/release now build the event with
/// [`lock_event_transaction_op`] and commit it with the lock row through
/// `native_entity_write_co_commit_for_service`.
///
/// This path remains for the target that cannot be atomic — the outbox table is
/// Postgres, so a lock whose native store resolves elsewhere has its row and its
/// event in different databases. There, the lock row has already committed when
/// this runs and the outbox insert is a SEPARATE statement, so a crash (or a
/// missing pool/relation) between the two loses the event while keeping the state
/// change. Drops are never silent: both local drop paths log at error level with
/// the lock id/topic and count in
/// `udb_outbox_enqueue_failures_total{path="native"}`; an insert failure inside
/// the shared enqueue helper records the same counter (it does not surface a
/// `Result` to this call site).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn emit_lock_event(
    svc: &LockServiceImpl,
    topic: &str,
    partition_key: &str,
    tenant_id: &str,
    project_id: &str,
    lock_id: &str,
    lock_name: &str,
    owner_id: &str,
    fencing_token: i64,
) {
    let Some(pool) = svc.pg_pool.as_ref() else {
        tracing::error!(
            topic,
            lock_id,
            lock_name,
            tenant_id,
            "lock event dropped: no outbox Postgres pool configured for the lock native store"
        );
        svc.metrics.inc_outbox_enqueue_failures_total("native");
        return;
    };
    if svc.outbox_relation.is_none() {
        tracing::error!(
            topic,
            lock_id,
            lock_name,
            tenant_id,
            "lock event dropped: no outbox relation configured"
        );
        svc.metrics.inc_outbox_enqueue_failures_total("native");
        return;
    }
    enqueue_outbox_event_with_context(
        pool,
        svc.outbox_relation.as_deref(),
        topic,
        partition_key,
        tenant_id,
        project_id,
        lock_event_payload(tenant_id, project_id, lock_name, owner_id, fencing_token),
        NativeEventContext {
            target_resource: lock_name.to_string(),
            ..NativeEventContext::default()
        },
        Some(&svc.metrics),
    )
    .await;
}
