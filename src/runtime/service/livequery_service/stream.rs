//! The bounded delta forwarder: the spawned task that filters CDC events against
//! the subscriber's tenant scope (fail closed) and IR predicate, forwarding
//! survivors as `Change` frames over a BOUNDED channel and closing the stream
//! with `resource_exhausted` on broadcast lag or a saturated subscriber.

use std::pin::Pin;

use tokio::sync::{broadcast, mpsc};
use tokio::time::{Instant, Interval, MissedTickBehavior, interval_at};
use tokio_stream::Stream;
use tonic::Status;

use crate::ir::LogicalFilter;
use crate::proto::udb::core::livequery::services::v1 as lq_pb;

use super::budget::StreamSlot;
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
pub(crate) async fn run_delta_forward(
    mut rx: broadcast::Receiver<crate::cdc::CdcEnvelope>,
    tx: mpsc::Sender<Result<lq_pb::SubscribeResponse, Status>>,
    tenant_id: String,
    project_id: String,
    cdc_topic: String,
    user_filter: Option<LogicalFilter>,
    // Owned for the lifetime of the live stream; dropping it (on ANY exit path
    // of this task — normal break, error close, abort) releases this
    // subscription's per-tenant active-stream budget slot.
    _stream_slot: StreamSlot,
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
        // TODO(leader): delta-path metrics — record per-outcome counters
        // (forwarded / dropped-by-scope / dropped-by-filter / backpressure / lag)
        // and a per-tenant active-stream gauge at the labelled points below, once
        // the new `MetricsRecorder` methods land (metrics.rs is leader-owned; the
        // recorder is already reachable via `svc.metrics` at the call site).
        match received {
            Ok(envelope) => {
                if !topic_matches_source(&envelope.topic, &cdc_topic) {
                    continue;
                }
                // Fail closed if the payload cannot be inspected: an opaque event
                // cannot be proven to belong to this tenant.
                let payload =
                    match serde_json::from_str::<serde_json::Value>(&envelope.payload_json) {
                        Ok(value) => value,
                        Err(_) => continue,
                    };
                // SECURITY: per-event tenant-scope re-check — a tenant-less or
                // foreign event is dropped here, never streamed.
                if !event_matches_tenant_scope(&envelope.topic, &payload, &tenant_id, &project_id) {
                    continue;
                }
                let row = change_row(&payload);
                if let Some(filter) = user_filter.as_ref() {
                    if !filter_matches_row(filter, &row) {
                        continue;
                    }
                }
                match tx.try_send(Ok(change_frame(&envelope, &payload))) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
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
