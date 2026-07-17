//! Manifest-driven native models, read projections, and row decoders for the
//! native `WebhookService`. Extracted verbatim: the signing secret is NEVER
//! selected (OUTPUT_VIEW_STORAGE_ONLY) and the delivery timestamp is projected as
//! an epoch bigint that `epoch_to_ts` maps back to a proto `Timestamp`.

use sqlx::Row;

use crate::proto::udb::core::webhook::entity::v1 as webhook_entity_pb;
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::config::{DELIVERY_MSG, ENDPOINT_MSG};
use super::errors::webhook_internal_status;

pub(crate) fn endpoint_model() -> NativeModel {
    native_model(
        ENDPOINT_MSG,
        &[
            "endpoint_id",
            "tenant_id",
            "url",
            "topic_pattern",
            "signing_secret",
            "active",
            "description",
            "max_attempts",
            "metadata_json",
            "deleted_at",
            "deleted_by",
        ],
    )
}

pub(crate) fn delivery_model() -> NativeModel {
    native_model(
        DELIVERY_MSG,
        &[
            "delivery_id",
            "tenant_id",
            "endpoint_id",
            "event_id",
            "topic",
            "status",
            "attempt_count",
            "response_status",
            "signature",
            "last_error",
            "payload_json",
            "delivered_at",
        ],
    )
}

/// Projection for endpoint reads. The signing secret is NEVER selected — it is
/// OUTPUT_VIEW_STORAGE_ONLY and must never leave the store after creation.
pub(crate) fn endpoint_select_projection(m: &NativeModel) -> String {
    [
        m.text("endpoint_id"),
        m.text("tenant_id"),
        m.text("url"),
        m.text_or_empty("topic_pattern"),
        m.text_or_empty("description"),
        m.select("active"),
        m.select("max_attempts"),
        m.text_or_empty("metadata_json"),
    ]
    .join(", ")
}

pub(crate) fn endpoint_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<webhook_entity_pb::WebhookEndpoint, tonic::Status> {
    let map = |e: sqlx::Error| {
        webhook_internal_status(
            "decode_webhook_endpoint",
            format!("decode webhook endpoint failed: {e}"),
        )
    };
    Ok(webhook_entity_pb::WebhookEndpoint {
        endpoint_id: row.try_get("endpoint_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        url: row.try_get("url").map_err(map)?,
        topic_pattern: row.try_get("topic_pattern").map_err(map)?,
        description: row.try_get("description").map_err(map)?,
        active: row.try_get("active").map_err(map)?,
        max_attempts: row.try_get("max_attempts").map_err(map)?,
        metadata_json: row.try_get("metadata_json").map_err(map)?,
        // signing_secret intentionally left empty: STORAGE_ONLY, never surfaced.
        signing_secret: String::new(),
        ..Default::default()
    })
}

pub(crate) fn delivery_select_projection(m: &NativeModel) -> String {
    [
        m.text("delivery_id"),
        m.text("tenant_id"),
        m.text("endpoint_id"),
        m.text_or_empty("event_id"),
        m.text_or_empty("topic"),
        m.text_or_empty("status"),
        m.select("attempt_count"),
        m.select("response_status"),
        m.text_or_empty("signature"),
        m.text_or_empty("last_error"),
        m.text_or_empty("payload_json"),
        format!(
            "EXTRACT(EPOCH FROM {})::BIGINT AS delivered_at_epoch",
            m.q("delivered_at")
        ),
    ]
    .join(", ")
}

pub(crate) fn epoch_to_ts(epoch: Option<i64>) -> Option<prost_types::Timestamp> {
    epoch.map(|seconds| prost_types::Timestamp { seconds, nanos: 0 })
}

pub(crate) fn delivery_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<webhook_entity_pb::WebhookDelivery, tonic::Status> {
    let map = |e: sqlx::Error| {
        webhook_internal_status(
            "decode_webhook_delivery",
            format!("decode webhook delivery failed: {e}"),
        )
    };
    Ok(webhook_entity_pb::WebhookDelivery {
        delivery_id: row.try_get("delivery_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        endpoint_id: row.try_get("endpoint_id").map_err(map)?,
        event_id: row.try_get("event_id").map_err(map)?,
        topic: row.try_get("topic").map_err(map)?,
        status: row.try_get("status").map_err(map)?,
        attempt_count: row.try_get("attempt_count").map_err(map)?,
        response_status: row.try_get("response_status").map_err(map)?,
        signature: row.try_get("signature").map_err(map)?,
        last_error: row.try_get("last_error").map_err(map)?,
        payload_json: row.try_get("payload_json").map_err(map)?,
        delivered_at: epoch_to_ts(
            row.try_get::<Option<i64>, _>("delivered_at_epoch")
                .map_err(map)?,
        ),
        ..Default::default()
    })
}
