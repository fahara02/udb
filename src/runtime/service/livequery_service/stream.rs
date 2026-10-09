//! The bounded delta forwarder: the spawned task that filters CDC events against
//! the subscriber's tenant scope (fail closed) and IR predicate, forwarding
//! survivors as `Change` frames over a BOUNDED channel and closing the stream
//! with `resource_exhausted` on broadcast lag or a saturated subscriber.

use std::collections::{HashSet, VecDeque};
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
use super::shared_tail::{FeedBatch, Watermark, envelope_watermark};

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

/// Private catch-up scans a lagging subscriber runs before it re-joins the
/// shared feed; a backlog larger than this is finished on the next batch.
const MAX_CATCH_UP_SCANS: usize = 64;

/// How many event ids the forwarder remembers to de-duplicate the broadcast
/// fast path against the journal backstop.
const DEDUP_WINDOW: usize = 16_384;

/// The cross-replica delta source. The in-process broadcast is fed ONLY on the
/// replica that holds the CDC tailer lease; every other replica's broadcast is
/// silent, so a subscriber connected there used to receive its snapshot and
/// then nothing. The tailer journals every event (in shared Postgres) BEFORE
/// broadcasting it, so tailing the journal delivers the same deltas on every
/// replica. The broadcast stays as the low-latency fast path; the two are
/// de-duplicated by `event_id`.
///
/// The journal is scanned by ONE shared poller per (topic, tenant, project)
/// ([`super::shared_tail`]); this subscriber applies the poller's batches from
/// its own watermark and only scans privately to catch up after a lag or a
/// resume from an older cursor.
pub(crate) struct JournalTail {
    pub(crate) cdc: Arc<crate::cdc::CdcEngine>,
    /// Journal position delivered up to; batches apply strictly after it.
    pub(crate) watermark: Watermark,
    /// In-scope rows per private catch-up scan.
    pub(crate) batch: i64,
    /// The shared poller's batches for this subscriber's scope.
    pub(crate) feed: broadcast::Receiver<Arc<FeedBatch>>,
}

/// Await the next shared-feed batch, or park forever without a journal tail.
async fn next_feed_batch(
    journal_tail: &mut Option<JournalTail>,
) -> Result<Arc<FeedBatch>, broadcast::error::RecvError> {
    match journal_tail.as_mut() {
        Some(tail) => tail.feed.recv().await,
        None => std::future::pending().await,
    }
}

/// Bounded FIFO set of delivered event ids.
#[derive(Default)]
pub(crate) struct SeenEvents {
    ids: HashSet<String>,
    order: VecDeque<String>,
}

impl SeenEvents {
    /// `true` the first time `event_id` is offered, `false` for a repeat.
    pub(crate) fn first_sighting(&mut self, event_id: &str) -> bool {
        if event_id.is_empty() {
            // Nothing to de-duplicate on; deliver.
            return true;
        }
        if !self.ids.insert(event_id.to_string()) {
            return false;
        }
        self.order.push_back(event_id.to_string());
        while self.order.len() > DEDUP_WINDOW {
            if let Some(evicted) = self.order.pop_front() {
                self.ids.remove(&evicted);
            }
        }
        true
    }
}

/// Outcome of offering one event to the subscriber.
enum Forwarded {
    /// Delivered, filtered out, or skipped — keep streaming.
    Continue,
    /// The subscriber is gone or the stream was closed with an error.
    Stop,
}

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
    resume_replay: Vec<crate::runtime::cdc::journal::JournalEntry>,
    // Delta-path metrics sink (per-outcome counters + per-tenant active gauge).
    metrics: Arc<dyn MetricsRecorder>,
    // Owned for the lifetime of the live stream; dropping it (on ANY exit path
    // of this task — normal break, error close, abort) releases this
    // subscription's per-tenant active-stream budget slot.
    stream_slot: StreamSlot,
    // Cross-replica journal tail. The subscribe handler refuses the stream when
    // the journal head cannot be read, so a live stream always has one; `None`
    // only in unit harnesses that drive the forwarder without a journal.
    journal_tail: Option<JournalTail>,
) {
    // Reflect this newly-active stream in the per-tenant gauge (the acquirer
    // already counted the slot before this task was spawned).
    metrics.set_livequery_active_streams(&tenant_id, active_stream_count(&tenant_id) as i64);
    let mut seen = SeenEvents::default();

    // Durable resume: replay the missed journalled deltas first. Uses the
    // backpressure-aware async send (not the live loop's close-on-Full) so a large
    // reconnect backlog is delivered rather than dropped; a closed channel (client
    // gone) ends the task. Each replayed frame carries its `event_id` so the client
    // dedups it against the snapshot / live feed and can advance its resume cursor.
    let mut ended = false;
    for entry in resume_replay {
        let envelope = entry.envelope;
        // The handler carries the complete scanned prefix, including foreign
        // rows, into JournalTail. Drain replay before accepting feed batches.
        if !seen.first_sighting(&envelope.event_id) {
            continue;
        }
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
            journal_tail,
            &mut seen,
        )
        .await;
    }

    // Stream ended (every path converges here): release the budget slot, THEN
    // reflect the decremented per-tenant active-stream count in the gauge.
    drop(stream_slot);
    metrics.set_livequery_active_streams(&tenant_id, active_stream_count(&tenant_id) as i64);
}

/// The live forward loop, split out so [`run_delta_forward`] owns the resume
/// replay + slot/gauge lifecycle while this owns the per-event fan-out. Events
/// arrive from two sources — the in-process broadcast (fast path, fed only on
/// the CDC leader) and the durable journal tail (every replica) — and are
/// de-duplicated by `event_id` before the shared scope/filter/forward path.
#[allow(clippy::too_many_arguments)]
async fn run_live_loop(
    rx: &mut broadcast::Receiver<crate::cdc::CdcEnvelope>,
    tx: &mpsc::Sender<Result<lq_pb::SubscribeResponse, Status>>,
    tenant_id: &str,
    project_id: &str,
    cdc_topic: &str,
    masked_columns: &[String],
    user_filter: Option<&LogicalFilter>,
    metrics: &dyn MetricsRecorder,
    mut journal_tail: Option<JournalTail>,
    seen: &mut SeenEvents,
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
    // Once the broadcast closes (or lags) the journal tail alone carries the
    // stream when it is configured.
    // Production subscribers consume one shared ordered journal feed. Direct
    // local fan-out could overtake an older remote event and poison UUID resume.
    // Unit harnesses without a journal retain their existing broadcast path.
    let mut broadcast_open = journal_tail.is_none();
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
            batch = next_feed_batch(&mut journal_tail), if journal_tail.is_some() => {
                match apply_feed_batch(
                    batch,
                    journal_tail.as_mut(),
                    seen,
                    tx,
                    tenant_id,
                    project_id,
                    cdc_topic,
                    masked_columns,
                    user_filter,
                    metrics,
                )
                .await
                {
                    Forwarded::Continue => continue,
                    Forwarded::Stop => break,
                }
            }
            received = rx.recv(), if broadcast_open => received,
        };
        // Delta-path metrics: per-outcome counters recorded at each labelled site
        // (forwarded / dropped-by-scope / dropped-by-filter / backpressure /
        // lag). The per-tenant active-stream gauge is owned by `run_delta_forward`.
        match received {
            Ok(envelope) => {
                if !seen.first_sighting(&envelope.event_id) {
                    continue;
                }
                if let Forwarded::Stop = forward_event(
                    &envelope,
                    tx,
                    tenant_id,
                    project_id,
                    cdc_topic,
                    masked_columns,
                    user_filter,
                    metrics,
                )
                .await
                {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                metrics.record_livequery_delta_dropped(tenant_id, "lag");
                if journal_tail.is_some() {
                    // The journal tail backfills whatever the bounded broadcast
                    // dropped; keep streaming from it alone.
                    broadcast_open = false;
                    continue;
                }
                let _ = tx
                    .send(Err(livequery_backpressure_status(
                        "delta feed lag",
                        "live query delta feed lagged; stream closed",
                    )))
                    .await;
                break;
            }
            Err(broadcast::error::RecvError::Closed) => {
                if journal_tail.is_some() {
                    broadcast_open = false;
                    continue;
                }
                break;
            }
        }
    }
}

/// Close the stream because the journal cannot be read. Swallowing it would
/// leave this subscriber on the broadcast alone, which on every replica but the
/// CDC tailer's leader is silent — indistinguishable from "nothing changed".
/// The client resumes from its last delivered event id once the journal is
/// readable again.
async fn close_journal_unavailable(
    tx: &mpsc::Sender<Result<lq_pb::SubscribeResponse, Status>>,
    cdc_topic: &str,
    error: &str,
) -> Forwarded {
    tracing::warn!(
        topic = %cdc_topic,
        error = %error,
        "live query stream closed: the CDC journal cannot be read"
    );
    let _ = tx
        .send(Err(super::errors::livequery_journal_unavailable_status(
            "journal_tail",
        )))
        .await;
    Forwarded::Stop
}

/// Apply one shared-feed batch. When the batch starts past this subscriber's
/// watermark (it lagged, or resumed from an older cursor), first catch up with
/// private scans; the batch's events are then applied only past the watermark,
/// and the watermark only advances to the batch end once the two are contiguous,
/// so the shared feed never opens a gap or replays what was delivered.
#[allow(clippy::too_many_arguments)]
async fn apply_feed_batch(
    batch: Result<Arc<FeedBatch>, broadcast::error::RecvError>,
    journal_tail: Option<&mut JournalTail>,
    seen: &mut SeenEvents,
    tx: &mpsc::Sender<Result<lq_pb::SubscribeResponse, Status>>,
    tenant_id: &str,
    project_id: &str,
    cdc_topic: &str,
    masked_columns: &[String],
    user_filter: Option<&LogicalFilter>,
    metrics: &dyn MetricsRecorder,
) -> Forwarded {
    let Some(tail) = journal_tail else {
        return Forwarded::Continue;
    };
    let batch = match batch {
        Ok(batch) => batch,
        // Missed batches are recovered by the catch-up the next batch triggers.
        Err(broadcast::error::RecvError::Lagged(_)) => return Forwarded::Continue,
        Err(broadcast::error::RecvError::Closed) => {
            return close_journal_unavailable(tx, cdc_topic, "journal feed closed").await;
        }
    };
    if let Some(error) = batch.error.as_deref() {
        return close_journal_unavailable(tx, cdc_topic, error).await;
    }
    let mut pending: Vec<crate::runtime::cdc::journal::JournalEntry> = Vec::new();
    if batch.from > tail.watermark {
        // Private catch-up from this subscriber's own watermark.
        for _ in 0..MAX_CATCH_UP_SCANS {
            metrics.record_livequery_journal_scan("catch_up");
            let scan = tail
                .cdc
                .try_journal_scan_after(
                    cdc_topic,
                    tenant_id,
                    project_id,
                    tail.watermark,
                    tail.batch,
                )
                .await;
            match scan {
                Ok((events, Some(last))) => {
                    pending.extend(events);
                    tail.watermark = last;
                    if tail.watermark >= batch.to {
                        break;
                    }
                }
                Ok((_, None)) => break,
                Err(err) => return close_journal_unavailable(tx, cdc_topic, &err).await,
            }
        }
    }
    if batch.from <= tail.watermark {
        pending.extend(
            batch
                .events
                .iter()
                .filter(|envelope| envelope_watermark(envelope) > tail.watermark)
                .cloned(),
        );
        if batch.to > tail.watermark {
            tail.watermark = batch.to;
        }
    }
    for entry in pending {
        let envelope = entry.envelope;
        if !seen.first_sighting(&envelope.event_id) {
            continue;
        }
        if let Forwarded::Stop = forward_event(
            &envelope,
            tx,
            tenant_id,
            project_id,
            cdc_topic,
            masked_columns,
            user_filter,
            metrics,
        )
        .await
        {
            return Forwarded::Stop;
        }
    }
    Forwarded::Continue
}

/// Scope-check, filter and forward one change event (shared by the broadcast
/// and journal paths).
#[allow(clippy::too_many_arguments)]
async fn forward_event(
    envelope: &crate::cdc::CdcEnvelope,
    tx: &mpsc::Sender<Result<lq_pb::SubscribeResponse, Status>>,
    tenant_id: &str,
    project_id: &str,
    cdc_topic: &str,
    masked_columns: &[String],
    user_filter: Option<&LogicalFilter>,
    metrics: &dyn MetricsRecorder,
) -> Forwarded {
    if !topic_matches_source(&envelope.topic, cdc_topic) {
        // Not this subscription's source entity — feed noise from another
        // entity on the shared broadcast, not a dropped matching delta.
        return Forwarded::Continue;
    }
    // Fail closed if the payload cannot be inspected: an opaque event cannot be
    // proven to belong to this tenant (a scope failure).
    let payload = match serde_json::from_str::<serde_json::Value>(&envelope.payload_json) {
        Ok(value) => value,
        Err(_) => {
            metrics.record_livequery_delta_dropped(tenant_id, "scope");
            return Forwarded::Continue;
        }
    };
    // SECURITY: per-event tenant-scope re-check — a tenant-less or foreign event
    // is dropped here, never streamed.
    if !event_matches_tenant_scope(&envelope.topic, &payload, tenant_id, project_id) {
        metrics.record_livequery_delta_dropped(tenant_id, "scope");
        return Forwarded::Continue;
    }
    let row = change_row(&payload);
    if let Some(filter) = user_filter {
        if !filter_matches_row(filter, &row) {
            metrics.record_livequery_delta_dropped(tenant_id, "filter");
            return Forwarded::Continue;
        }
    }
    match tx.try_send(Ok(change_frame(envelope, &payload, masked_columns))) {
        Ok(()) => {
            metrics.record_livequery_delta_forwarded(tenant_id);
            Forwarded::Continue
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            metrics.record_livequery_delta_dropped(tenant_id, "backpressure");
            let _ = tx
                .send(Err(livequery_backpressure_status(
                    "subscriber_channel",
                    "live query subscriber too slow; stream closed",
                )))
                .await;
            Forwarded::Stop
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Forwarded::Stop,
    }
}

#[cfg(test)]
mod dedup_tests {
    use super::SeenEvents;

    /// The broadcast fast path and the journal backstop carry the same events;
    /// each must reach the subscriber exactly once.
    #[test]
    fn seen_events_delivers_each_event_once() {
        let mut seen = SeenEvents::default();
        assert!(seen.first_sighting("e1"));
        assert!(!seen.first_sighting("e1"));
        assert!(seen.first_sighting("e2"));
        // An id-less event cannot be de-duplicated; it is delivered.
        assert!(seen.first_sighting(""));
        assert!(seen.first_sighting(""));
    }

    #[test]
    fn seen_events_window_is_bounded() {
        let mut seen = SeenEvents::default();
        for i in 0..(super::DEDUP_WINDOW + 10) {
            assert!(seen.first_sighting(&format!("e{i}")));
        }
        assert!(seen.ids.len() <= super::DEDUP_WINDOW);
        assert_eq!(seen.ids.len(), seen.order.len());
        // The newest ids are still remembered.
        let newest = format!("e{}", super::DEDUP_WINDOW + 9);
        assert!(!seen.first_sighting(&newest));
    }
}
