//! The `udb.messaging.inbox.appended.v1` outbox event. It is written in the SAME
//! transaction as the inbox row it announces, so an entry is never visible
//! without its wake-up event and an event never names an entry that rolled back.

use sqlx::PgConnection;
use tonic::Status;

use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_in_tx};
use super::config::TOPIC_INBOX_APPENDED;
use super::errors::messaging_internal_status;

/// The event payload: who has a new entry and its position, nothing else. No
/// group, kind, sender or content leaves the inbox through the event pipeline.
pub(crate) fn inbox_appended_payload(
    tenant_id: &str,
    device_id: &str,
    seq: i64,
) -> serde_json::Value {
    serde_json::json!({
        "tenant_id": tenant_id,
        "device_id": device_id,
        "seq": seq,
    })
}

/// Partition by device so one device's wake-ups stay ordered.
pub(crate) fn inbox_partition_key(tenant_id: &str, device_id: &str) -> String {
    format!("messaging:{tenant_id}:{device_id}")
}

/// Enqueue the event inside the caller's transaction; a failure fails the
/// transaction (the entry and its event commit together or not at all). A
/// deployment with no outbox relation configured enqueues nothing.
pub(crate) async fn emit_inbox_appended(
    conn: &mut PgConnection,
    outbox_relation: Option<&str>,
    tenant_id: &str,
    project_id: &str,
    device_id: &str,
    seq: i64,
) -> Result<(), Status> {
    enqueue_outbox_event_in_tx(
        &mut *conn,
        outbox_relation,
        TOPIC_INBOX_APPENDED,
        &inbox_partition_key(tenant_id, device_id),
        tenant_id,
        project_id,
        inbox_appended_payload(tenant_id, device_id, seq),
        NativeEventContext {
            target_resource: format!("messaging.inbox/{device_id}"),
            ..NativeEventContext::default()
        },
    )
    .await
    .map_err(|err| {
        messaging_internal_status(
            "inbox_appended_outbox",
            format!("inbox event enqueue failed: {err}"),
        )
    })
}
