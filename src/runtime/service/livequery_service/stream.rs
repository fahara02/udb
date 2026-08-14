//! The bounded delta forwarder: the spawned task that filters CDC events against
//! the subscriber's tenant scope (fail closed) and IR predicate, forwarding
//! survivors as `Change` frames over a BOUNDED channel and closing the stream
//! with `resource_exhausted` on broadcast lag or a saturated subscriber.

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tokio::time::{Instant, Interval, MissedTickBehavior, interval_at};
use tokio_stream::Stream;
use tonic::Status;

use crate::ir::LogicalFilter;
use crate::metrics::MetricsRecorder;
use crate::proto::udb::core::livequery::services::v1 as lq_pb;

use super::budget::{StreamSlot, active_stream_count};
use super::config::livequery_keepalive_interval;
use super::errors::livequery_backpressure_status;
use super::predicate::{
    change_frame, change_row, event_matches_tenant_scope, filter_matches_row, keepalive_frame,
    topic_matches_source,
};

/// Await the next keepalive tick, or park forever when keepalives are disabled
/// (`None`). Kept total so the `tokio::select!` arm compiles whether or not a
/// cadence is configured; the `if keepalive.is_some()` guard on the arm means the
/// `pending` path is never actually polled when disabled.
async fn next_keepalive_tick(keepalive: &mut Option<Interval>) {
    match keepalive.as_mut() {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

/// The streamed frame type: an initial snapshot then a stream of change deltas.
pub(crate) type LiveQueryStream =
    Pin<Box<dyn Stream<Item = Result<lq_pb::SubscribeResponse, Status>> + Send + 'static>>;

/// Drive the bounded delta forwarder: subscribe to the CDC broadcast feed, drop
/// every event that fails the fail-closed tenant re-check or the IR predicate,
/// and forward survivors as `Change` frames. Closes the stream with
/// `resource_exhausted` on broadcast lag or a saturated subscriber channel.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_delta_forward(
    mut rx: broadcast::Receiver<crate::cdc::CdcEnvelope>,
    tx: mpsc::Sender<Result<lq_pb::SubscribeResponse, Status>>,
    tenant_id: String,
    project_id: String,
    cdc_topic: String,
    // Columns to mask in every forwarded row image (empty when the subscriber
    // holds udb:pii:read). Resolved once at subscribe time from the manifest.
    masked_columns: Vec<String>,
    user_filter: Option<LogicalFilter>,
    // Durable-resume backlog: change events the client missed while disconnected,
    // read from the CDC journal and already tenant re-checked at read time. Drained
    // (backpressure-aware) to the client BEFORE the live feed, then dropped.
    resume_replay: Vec<crate::cdc::CdcEnvelope>,
    // Delta-path metrics sink (per-outcome counters + per-tenant active gauge).
    metrics: Arc<dyn MetricsRecorder>,
    // Owned for the lifetime of the live stream; dropping it (on ANY exit path
    // of this task — normal break, error close, abort) releases this
    // subscription's per-tenant active-stream budget slot.
    stream_slot: StreamSlot,
) {
    // Reflect this newly-active stream in the per-tenant gauge (the acquirer
    // already counted the slot before this task was spawned).
    metrics.set_livequery_active_streams(&tenant_id, active_stream_count(&tenant_id) as i64);

    // Durable resume: replay the missed journalled deltas first. Uses the
    // backpressure-aware async send (not the live loop's close-on-Full) so a large
    // reconnect backlog is delivered rather than dropped; a closed channel (client
    // gone) ends the task. Each replayed frame carries its `event_id` so the client
    // dedups it against the snapshot / live feed and can advance its resume cursor.
    let mut ended = false;
    for envelope in resume_replay {
        let payload = match serde_json::from_str::<serde_json::Value>(&envelope.payload_json) {
            Ok(value) => value,
            // Fail closed: an opaque journal payload cannot be proven in-scope.
            Err(_) => {
                metrics.record_livequery_delta_dropped(&tenant_id, "scope");
                continue;
            }
        };
        // Predicate parity with the live loop: the subscriber's IR `user_filter`
        // must gate replayed deltas the same way it gates live ones, or a resume
        // would leak rows the client explicitly filtered out. Tenant scope was
        // already re-checked at journal read time, so only the predicate is applied
        // here — build the change row exactly as the live loop does and drop a
        // non-match with the same "filter" outcome label.
        if let Some(filter) = user_filter.as_ref() {
            let row = change_row(&payload);
            if !filter_matches_row(filter, &row) {
                metrics.record_livequery_delta_dropped(&tenant_id, "filter");
                continue;
            }
        }
        match tx
            .send(Ok(change_frame(&envelope, &payload, &masked_columns)))
            .await
        {
            Ok(()) => metrics.record_livequery_delta_forwarded(&tenant_id),
            Err(_) => {
                ended = true;
                break;
            }
        }
    }

    if !ended {
        run_live_loop(
            &mut rx,
            &tx,
            &tenant_id,
            &project_id,
            &cdc_topic,
            &masked_columns,
            user_filter.as_ref(),
            metrics.as_ref(),
        )
        .await;
    }

    // Stream ended (every path converges here): release the budget slot, THEN
    // reflect the decremented per-tenant active-stream count in the gauge.
    drop(stream_slot);
    metrics.set_livequery_active_streams(&tenant_id, active_stream_count(&tenant_id) as i64);
}

/// The live broadcast-forward loop, split out so [`run_delta_forward`] owns the
/// resume replay + slot/gauge lifecycle while this owns the per-event fan-out.
async fn run_live_loop(
    rx: &mut broadcast::Receiver<crate::cdc::CdcEnvelope>,
    tx: &mpsc::Sender<Result<lq_pb::SubscribeResponse, Status>>,
    tenant_id: &str,
    project_id: &str,
    cdc_topic: &str,
    masked_columns: &[String],
    user_filter: Option<&LogicalFilter>,
    metrics: &dyn MetricsRecorder,
) {
    // Optional idle-stream keepalive: on a busy stream real deltas keep the
    // connection warm and the timer just resets; on a silent stream this puts a
    // heartbeat frame on the wire every cadence so an LB idle timeout does not
    // reap a healthy subscription. First tick is one full period out (a fresh
    // stream just sent its snapshot), and missed ticks are skipped (no burst
    // after a busy spell) rather than queued.
    let mut keepalive: Option<Interval> = livequery_keepalive_interval().map(|period| {
        let mut ticker = interval_at(Instant::now() + period, period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker
    });
    loop {
        // Wake on subscriber hang-up too: without `tx.closed()` a disconnected
        // client whose source entity never mutates would park this task (and
        // pin its budget slot) on the broadcast feed indefinitely.
        let received = tokio::select! {
            _ = tx.closed() => break,
            _ = next_keepalive_tick(&mut keepalive), if keepalive.is_some() => {
                // Best-effort heartbeat. A FULL channel means the subscriber is
                // actively receiving deltas (not idle), so a dropped keepalive is
                // harmless — never close the stream on a heartbeat. Only a CLOSED
                // channel (client gone) ends the loop.
                match tx.try_send(Ok(keepalive_frame())) {
                    Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => continue,
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
            received = rx.recv() => received,
        };
        // Delta-path metrics: per-outcome counters recorded at each labelled site
        // below (forwarded / dropped-by-scope / dropped-by-filter / backpressure /
        // lag). The per-tenant active-stream gauge is owned by `run_delta_forward`.
        match received {
            Ok(envelope) => {
                if !topic_matches_source(&envelope.topic, cdc_topic) {
                    // Not this subscription's source entity — feed noise from another
                    // entity on the shared broadcast, not a dropped matching delta.
                    continue;
                }
                // Fail closed if the payload cannot be inspected: an opaque event
                // cannot be proven to belong to this tenant (a scope failure).
                let payload =
                    match serde_json::from_str::<serde_json::Value>(&envelope.payload_json) {
                        Ok(value) => value,
                        Err(_) => {
                            metrics.record_livequery_delta_dropped(tenant_id, "scope");
                            continue;
                        }
                    };
                // SECURITY: per-event tenant-scope re-check — a tenant-less or
                // foreign event is dropped here, never streamed.
                if !event_matches_tenant_scope(&envelope.topic, &payload, tenant_id, project_id) {
                    metrics.record_livequery_delta_dropped(tenant_id, "scope");
                    continue;
                }
                let row = change_row(&payload);
                if let Some(filter) = user_filter {
                    if !filter_matches_row(filter, &row) {
                        metrics.record_livequery_delta_dropped(tenant_id, "filter");
                        continue;
                    }
                }
                match tx.try_send(Ok(change_frame(&envelope, &payload, masked_columns))) {
                    Ok(()) => metrics.record_livequery_delta_forwarded(tenant_id),
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        metrics.record_livequery_delta_dropped(tenant_id, "backpressure");
                        let _ = tx
                            .send(Err(livequery_backpressure_status(
                                "subscriber_channel",
                                "live query subscriber too slow; stream closed",
                            )))
                            .await;
                        break;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                metrics.record_livequery_delta_dropped(tenant_id, "lag");
                let _ = tx
                    .send(Err(livequery_backpressure_status(
                        "delta feed lag",
                        "live query delta feed lagged; stream closed",
                    )))
                    .await;
                break;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}
