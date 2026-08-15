//! The `Subscribe` RPC handler — tenant-scoped initial snapshot (mediated IR read)
//! followed by the bounded delta stream. Extracted from the trait impl; `mod.rs`
//! delegates one line to it.

use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::ir::{LogicalPagination, LogicalRead};
use crate::proto::udb::core::livequery::services::v1 as lq_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{admit_on as native_admit_on, validated_native_service_context};
use super::LiveQueryServiceImpl;
use super::budget::try_acquire_stream_slot;
use super::config::{
    SERVICE_ID, buffer_events, clamp_snapshot_limit, default_snapshot_limit, max_snapshot_limit,
    resume_replay_limit,
};
use super::errors::livequery_required_field;
use super::predicate::{
    build_user_filter, parse_resume_cursor, resolve_source, row_object, snapshot_filter,
};
use super::stream::{LiveQueryStream, run_delta_forward};

/// Request-metadata header carrying the durable-resume cursor: the client's
/// last-delivered `LiveQueryChange.event_id`. Metadata (not a proto field) so
/// resume needs no contract change — a fresh subscription simply omits it.
const RESUME_CURSOR_HEADER: &str = "x-udb-livequery-resume";

pub(crate) async fn subscribe(
    svc: &LiveQueryServiceImpl,
    request: Request<lq_pb::SubscribeRequest>,
) -> Result<Response<LiveQueryStream>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // Scope guard FIRST: body tenant/project must match the verified claim and
    // request metadata before either value reaches the runtime context.
    let tenant_id = req.tenant_id.trim().to_string();
    let message_type = req.message_type.trim().to_string();
    let project_id = req.project_id.trim().to_string();
    let context = validated_native_service_context(&metadata, &tenant_id, &project_id)?;
    if message_type.is_empty() {
        return Err(livequery_required_field(
            "message_type",
            "must be a non-empty source message type",
            "message_type is required",
        ));
    }
    // Fail closed BEFORE any read/stream if the source has no tenant column.
    let source = resolve_source(&message_type)?;
    let user_filter = build_user_filter(&req.filters)?;

    // Durable-resume cursor (optional): the client's last-delivered event_id,
    // carried in request metadata so resume needs no proto change. Absent/blank =
    // a fresh, non-resuming subscription.
    let resume_cursor = parse_resume_cursor(
        metadata
            .get(RESUME_CURSOR_HEADER)
            .and_then(|value| value.to_str().ok()),
    );

    // Rate-bound new subscriptions per tenant at entry; the permit is dropped
    // immediately (not held across the long-lived stream) so it cannot block.
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        SERVICE_ID,
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;

    // Concurrency budget (distinct from the rate bound above): cap the
    // tenant's ACTIVE long-lived streams before any read/stream resources
    // are committed. The slot is released by the guard's Drop when the
    // stream ends, on every path.
    let stream_slot = try_acquire_stream_slot(&tenant_id)?;

    let runtime = svc.require_runtime()?;

    // Subscribe to the delta feed BEFORE the snapshot so events committed
    // during the snapshot are buffered on the broadcast receiver (not lost in
    // the subscribe→read gap); any overlap with snapshot rows is de-duplicated
    // client-side by event_id.
    let delta_rx = svc.cdc_engine.as_ref().map(|cdc| cdc.subscribe());

    // Durable resume: when the client presented a resume cursor AND a live CDC
    // feed exists, read the missed change events from the durable CDC journal
    // (tenant re-checked at read time with the canonical scope predicate) so they
    // can be replayed to the client BEFORE the broadcast hand-off — closing the
    // reconnect gap the in-process broadcast ring cannot span. Bounded by
    // `resume_replay_limit()`; a larger gap is closed by a subsequent re-resume
    // from the client's new last-delivered cursor.
    let resume_replay = match (resume_cursor.as_deref(), svc.cdc_engine.as_ref()) {
        (Some(cursor), Some(cdc)) => {
            cdc.journal_replay_for_scope(
                &source.cdc_topic,
                &tenant_id,
                &project_id,
                cursor,
                i64::from(resume_replay_limit()),
            )
            .await
        }
        _ => Vec::new(),
    };

    let limit = clamp_snapshot_limit(
        req.snapshot_limit,
        default_snapshot_limit(),
        max_snapshot_limit(),
    );
    let read = LogicalRead {
        message_type: message_type.clone(),
        filter: Some(snapshot_filter(
            &source.tenant_field,
            &tenant_id,
            user_filter.clone(),
        )),
        projection: None,
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(limit)),
    };
    // MEDIATED snapshot: IR query plan + read-fence (carried on the context),
    // tenant predicate injected server-side. Never a raw query.
    let rows = runtime
        .native_entity_read_for_service(SERVICE_ID, &context, read)
        .await?;
    // PII masking for BOTH the snapshot and the live deltas. The relational read
    // path masks these columns; LiveQuery shipped the raw row image, so a
    // subscriber with only `udb:stream` saw them in clear text.
    let can_read_pii = context
        .scopes
        .iter()
        .any(|scope| scope == "udb:pii:read" || scope == "udb:*" || scope == "*");
    let masked_columns: Vec<String> = if can_read_pii {
        Vec::new()
    } else {
        crate::runtime::native_catalog::native_manifest()
            .tables
            .iter()
            .find(|table| {
                crate::runtime::projection::message_type_matches(&table.message_name, &message_type)
            })
            .map(|table| {
                table
                    .columns
                    .iter()
                    .filter(|c| c.security.is_pii || c.security.mask_in_logs)
                    .map(|c| c.column_name.clone())
                    .collect()
            })
            .unwrap_or_default()
    };
    let rows_json: Vec<String> = rows
        .iter()
        .map(|row| {
            let masked = super::predicate::mask_row(row_object(row), &masked_columns);
            serde_json::to_string(&masked).unwrap_or_else(|_| "{}".to_string())
        })
        .collect();
    let snapshot = lq_pb::SubscribeResponse {
        payload: Some(lq_pb::subscribe_response::Payload::Snapshot(
            lq_pb::LiveQuerySnapshot {
                row_count: rows_json.len() as i64,
                rows_json,
            },
        )),
        error: None,
    };

    let (tx, mut rx) = mpsc::channel::<Result<lq_pb::SubscribeResponse, Status>>(buffer_events());
    // The snapshot is the FIRST frame (the channel is empty, so this fits).
    let _ = tx.try_send(Ok(snapshot));

    // Spawn the bounded delta forwarder when a live CDC feed exists; otherwise
    // drop the sender so the stream ends cleanly after the snapshot. The
    // budget slot travels with the forwarder task (released when the task
    // exits); without a live feed the stream is snapshot-only and ends
    // immediately, so the slot is released right here.
    if let Some(rx_delta) = delta_rx {
        tokio::spawn(run_delta_forward(
            rx_delta,
            tx,
            tenant_id,
            project_id,
            source.cdc_topic,
            masked_columns,
            user_filter,
            resume_replay,
            svc.metrics.clone(),
            stream_slot,
        ));
    } else {
        drop(tx);
        drop(stream_slot);
    }

    let stream = async_stream::try_stream! {
        while let Some(item) = rx.recv().await {
            let frame = item?;
            yield frame;
        }
    };
    Ok(Response::new(Box::pin(stream)))
}
